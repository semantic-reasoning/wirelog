#!/usr/bin/env python3
"""Self-test for ci-run-telemetry.py (#1570).

Drives the analyzer over committed fixtures, so every verdict is exercised
without a network, a token, `gh`, or a build directory.  The fixtures under
`ci-telemetry-fixtures/` are trimmed captures of real runs of this repository's
`CI PR` workflow plus three hand-written shapes the real captures do not
contain:

  run-success       run 34514413925, 18 jobs across 16 distinct runners,
                    hosted and self-hosted mixed in one run -- the ordinary
                    case, not an edge case
  run-skip-cascade  run 34571608167: a build failure skips five of fifteen
                    jobs; three of them are stamped with a completion one
                    second before their own start
  run-clock-skew    run 34579784069: a self-hosted runner whose steps start
                    before the job the server clock says they belong to
  run-queued        hand-written: a run still in flight
  run-missing-data  hand-written: a job with no steps, and a job that never
                    started but carries a completion
  run-rerun         hand-written: `run_started_at` diverging from `created_at`

The invariants matter more than any single number.  A metric that was not
measured must say so: the failure this guards against is a phase that matched
no step reporting zero seconds, which reads as a phase that ran instantly.
"""

from __future__ import annotations

import contextlib
import copy
import importlib.util
import io
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
TOOL_PATH = SCRIPT_DIR / "ci-run-telemetry.py"
FIXTURES = SCRIPT_DIR / "ci-telemetry-fixtures"


def load_tool():
    spec = importlib.util.spec_from_file_location("ci_run_telemetry", TOOL_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


tool = load_tool()


def fixture(name: str):
    run = json.loads((FIXTURES / f"{name}.json").read_text())
    jobs = json.loads((FIXTURES / f"{name}-jobs.json").read_text())
    return run, jobs


def walk_metrics(node, path=""):
    """Yield every (path, metric object) in a report."""
    if isinstance(node, dict):
        if "status" in node and "seconds" in node:
            yield path, node
            return
        for key, value in node.items():
            yield from walk_metrics(value, f"{path}.{key}" if path else key)
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from walk_metrics(value, f"{path}[{index}]")


ALL_FIXTURES = ("run-success", "run-skip-cascade", "run-clock-skew",
                "run-queued", "run-missing-data", "run-rerun")


class MetricInvariants(unittest.TestCase):
    """The rules that hold for every report, whatever the run looked like."""

    def test_no_negative_and_no_unevidenced_zero(self):
        for name in ALL_FIXTURES:
            report = tool.analyze(*fixture(name))
            for path, metric in walk_metrics(report):
                with self.subTest(fixture=name, metric=path):
                    seconds = metric["seconds"]
                    if seconds is not None:
                        self.assertGreaterEqual(
                            seconds, 0,
                            f"{path} reports a negative duration")
                    if metric["status"] in ("measured", "partial"):
                        self.assertTrue(
                            metric["evidence"],
                            f"{path} claims a measurement with no evidence")
                    else:
                        self.assertIsNone(
                            seconds,
                            f"{path} is {metric['status']} but carries seconds")
                        self.assertTrue(
                            metric["reason"],
                            f"{path} is {metric['status']} with no reason")

    def test_schema_and_required_keys(self):
        report = tool.analyze(*fixture("run-success"))
        self.assertEqual(report["schema"], "wirelog.ci-telemetry/1")
        for key in ("tool_version", "generated_at", "run_id", "run_attempt",
                    "workflow", "event", "status", "conclusion", "head_sha",
                    "head_branch", "t0", "t0_source", "job_counts",
                    "populations", "jobs", "metrics", "required_checks",
                    "critical_path", "anomalies", "unmapped_steps",
                    "unusable_steps"):
            self.assertIn(key, report)
        for key in ("skipped_steps", "unusable_steps", "unmapped_steps"):
            self.assertIn(key, report["jobs"][0])

    def test_unaccounted_closes_the_job_wall(self):
        report = tool.analyze(*fixture("run-success"))
        for job in report["jobs"]:
            wall = job["metrics"]["job_wall"]
            if wall["status"] != "measured":
                continue
            phases = sum(m["seconds"] for m in job["phases"].values()
                         if m["status"] == "measured")
            with self.subTest(job=job["name"]):
                self.assertEqual(
                    phases + job["unaccounted"]["seconds"], wall["seconds"],
                    "phase totals plus unaccounted must equal the job wall")


class SkipCascade(unittest.TestCase):
    """A skipped job is stamped with a completion before its own start.  It
    must produce no durations rather than zeroes or negatives."""

    def setUp(self):
        self.report = tool.analyze(*fixture("run-skip-cascade"))

    def test_counts_lead_with_what_actually_ran(self):
        self.assertEqual(self.report["job_counts"],
                         {"total": 15, "executed": 10, "skipped": 5})

    def test_skipped_jobs_have_no_durations(self):
        skipped = [j for j in self.report["jobs"] if j["conclusion"] == "skipped"]
        self.assertEqual(len(skipped), 5)
        for job in skipped:
            for key, metric in job["metrics"].items():
                with self.subTest(job=job["name"], metric=key):
                    self.assertEqual(metric["status"], "unavailable")
                    self.assertIn("skipped", metric["reason"])
            for phase, metric in job["phases"].items():
                with self.subTest(job=job["name"], phase=phase):
                    self.assertIsNone(metric["seconds"])

    def test_first_failure_latency_is_measured(self):
        metric = self.report["metrics"]["first_failure_latency"]
        self.assertEqual(metric["status"], "measured")
        self.assertEqual(metric["evidence"]["job"], "Build / ubuntu-latest / gcc")
        self.assertEqual(metric["seconds"], 1499)
        # The step matters: a size gate failing at minute 25 is #1573's case,
        # and the job name alone does not distinguish it from a test failure.
        self.assertEqual(metric["evidence"]["failed_steps"], ["Check binary size"])

    def test_run_wall_clock_ignores_skipped_completions(self):
        # The latest completed_at in this run (07:34:52) belongs to a skipped
        # job that never ran; the latest executed completion is 07:34:51.
        self.assertEqual(self.report["metrics"]["run_wall_clock"]["seconds"],
                         2704)

    def test_a_skipped_job_reports_no_release_latency(self):
        by_name = {j["name"]: j for j in self.report["jobs"]}
        metric = by_name["Build / ubuntu-24.04-arm / gcc"]["metrics"][
            "scheduler_release_latency"]
        self.assertEqual(metric["status"], "unavailable")
        self.assertIn("skipped", metric["reason"])


class ReleaseLatency(unittest.TestCase):
    """`analyze` takes the dependency graph as a parameter; unit 2 supplies it
    from the workflow files.  These cases drive it with a literal graph so the
    branches are exercised now rather than on first contact with real data."""

    def test_latency_is_measured_from_the_last_dependency_to_finish(self):
        run, jobs = fixture("run-skip-cascade")
        needs = {"lint / EditorConfig check": ["RC changelog freeze gate"]}
        report = tool.analyze(run, jobs, needs)
        by_name = {j["name"]: j for j in report["jobs"]}
        metric = by_name["lint / EditorConfig check"]["metrics"][
            "scheduler_release_latency"]
        # rc-changelog-freeze completed 06:50:17 and released the lint job,
        # whose creation is stamped the same second.
        self.assertEqual(metric["seconds"], 0)
        self.assertEqual(metric["evidence"]["released_by"],
                         "RC changelog freeze gate")

    def test_the_latest_dependency_wins_when_there_are_several(self):
        run, jobs = fixture("run-skip-cascade")
        needs = {"lint / uncrustify check": ["Docs / policy validation",
                                             "RC changelog freeze gate"]}
        report = tool.analyze(run, jobs, needs)
        by_name = {j["name"]: j for j in report["jobs"]}
        metric = by_name["lint / uncrustify check"]["metrics"][
            "scheduler_release_latency"]
        # Docs finished 06:50:04, rc-changelog-freeze 06:50:17; only the later
        # one can have released the job.
        self.assertEqual(metric["evidence"]["released_by"],
                         "RC changelog freeze gate")

    def test_a_skipped_dependency_makes_the_latency_unavailable(self):
        run, jobs = fixture("run-skip-cascade")
        needs = {"Build / ubuntu-latest / gcc": [
            "mbedtls-enabled / ubuntu-latest / gcc"]}
        report = tool.analyze(run, jobs, needs)
        by_name = {j["name"]: j for j in report["jobs"]}
        metric = by_name["Build / ubuntu-latest / gcc"]["metrics"][
            "scheduler_release_latency"]
        self.assertEqual(metric["status"], "unavailable")
        self.assertIn("batched", metric["reason"])

    def test_a_dependency_absent_from_the_run_is_named(self):
        run, jobs = fixture("run-skip-cascade")
        needs = {"Build / ubuntu-latest / gcc": ["a job that never ran"]}
        report = tool.analyze(run, jobs, needs)
        by_name = {j["name"]: j for j in report["jobs"]}
        metric = by_name["Build / ubuntu-latest / gcc"]["metrics"][
            "scheduler_release_latency"]
        self.assertEqual(metric["status"], "unavailable")
        self.assertIn("a job that never ran", metric["reason"])


class ClockSkew(unittest.TestCase):
    """Step timestamps come from the runner, job timestamps from the server.
    The offset is reported; it is never folded into a duration."""

    def setUp(self):
        self.report = tool.analyze(*fixture("run-clock-skew"))
        self.by_name = {j["name"]: j for j in self.report["jobs"]}

    def test_offset_is_reported_as_an_anomaly(self):
        skews = [a for a in self.report["anomalies"]
                 if a["kind"] == "runner_clock_skew"]
        self.assertTrue(skews, "a step starting before its job must be reported")
        for item in skews:
            self.assertLess(item["offset_seconds"], -tool.CLOCK_SKEW_TOLERANCE_S)
            self.assertNotRegex(item["runner_name"] or "", r"^GitHub Actions \d+$")

    def test_queue_delay_stays_on_the_server_clock(self):
        job = self.by_name["lint / uncrustify check"]
        # created 08:43:11 -> started 08:43:14 on the server clock.  The first
        # step claims 08:43:04, before the job was created; taking the earlier
        # value would report a negative queue delay and inflate the wall time.
        self.assertEqual(job["metrics"]["queue_delay"]["seconds"], 3)
        self.assertEqual(job["clock_offset_estimate"]["offset_seconds"], -10)

    def test_steps_never_extend_outside_the_corrected_window(self):
        for job in self.report["jobs"]:
            offset = job["clock_offset_estimate"]
            wall = job["metrics"]["job_wall"]
            if offset["status"] != "measured" or wall["status"] != "measured":
                continue
            accounted = sum(m["seconds"] for m in job["phases"].values()
                            if m["status"] == "measured")
            with self.subTest(job=job["name"]):
                self.assertLessEqual(
                    accounted, wall["seconds"] - offset["offset_seconds"])


class InFlightAndMissingData(unittest.TestCase):
    def test_queued_run_invents_nothing(self):
        report = tool.analyze(*fixture("run-queued"))
        by_name = {j["name"]: j for j in report["jobs"]}
        queued = by_name["TSan / ubuntu-latest / gcc"]
        self.assertEqual(queued["metrics"]["queue_delay"]["status"], "unavailable")
        self.assertEqual(queued["metrics"]["job_wall"]["status"], "unavailable")
        running = by_name["Build / ubuntu-latest / gcc"]
        self.assertEqual(running["metrics"]["queue_delay"]["seconds"], 12)
        self.assertEqual(running["metrics"]["job_wall"]["status"], "unavailable")
        self.assertEqual(report["metrics"]["run_wall_clock"]["status"], "partial")

    def test_missing_fields_are_reported_not_guessed(self):
        report = tool.analyze(*fixture("run-missing-data"))
        by_name = {j["name"]: j for j in report["jobs"]}
        no_steps = by_name["Detect build-triggering changes"]
        for phase, metric in no_steps["phases"].items():
            with self.subTest(phase=phase):
                self.assertEqual(metric["status"], "unavailable")
        self.assertEqual(no_steps["clock_offset_estimate"]["status"], "unavailable")
        never_started = by_name["Build / ubuntu-latest / gcc"]
        self.assertEqual(never_started["metrics"]["queue_delay"]["status"],
                         "unavailable")
        self.assertEqual(report["t0_source"], "created_at")

    def test_rerun_anchors_on_run_started_at(self):
        report = tool.analyze(*fixture("run-rerun"))
        self.assertEqual(report["t0_source"], "run_started_at")
        self.assertEqual(report["t0"], "2026-09-10T10:00:00Z")
        job = report["jobs"][0]
        # Anchoring on created_at would have reported an hour of upstream wait.
        self.assertEqual(job["metrics"]["upstream_elapsed"]["seconds"], 1)


class PhaseAttribution(unittest.TestCase):
    def test_every_fixture_step_name_is_classified_deliberately(self):
        unmapped = set()
        for name in ALL_FIXTURES:
            _, jobs = fixture(name)
            for job in jobs["jobs"]:
                for step in job.get("steps") or []:
                    step_name = step["name"]
                    if (tool.classify_step(step_name) == "other"
                            and step_name not in tool.KNOWN_UNMAPPED):
                        unmapped.add(step_name)
        self.assertEqual(
            unmapped, set(),
            "every observed step name must map to a phase or be listed in "
            "KNOWN_UNMAPPED; a new one landing in `other` is a silent gap")

    def test_tool_specific_installs_belong_to_their_gate(self):
        # Downloading a gate's own tool is that gate's cost.  Pooling it into
        # deps_install would overstate what #1571 reads as dependency setup.
        for step, phase in (("Install clang-tidy (pinned major)", "tidy"),
                            ("Install syft", "sbom"),
                            ("Install editorconfig-checker", "lint"),
                            ("Install uncrustify", "lint")):
            with self.subTest(step=step):
                self.assertEqual(tool.classify_step(step), phase)
        self.assertEqual(tool.classify_step("Install dependencies (Linux)"),
                         "deps_install")

    def test_post_steps_are_not_merged_into_the_phase_they_clean_up(self):
        self.assertEqual(tool.classify_step("Setup sccache"), "cache_setup")
        self.assertEqual(tool.classify_step("Post Setup sccache"), "post_cleanup")
        self.assertEqual(tool.classify_step("Run actions/checkout@v5"), "checkout")
        self.assertEqual(tool.classify_step("Post Run actions/checkout@v5"),
                         "post_cleanup")

    def test_tsan_native_test_step_is_not_lost(self):
        self.assertEqual(
            tool.classify_step("Policy smoke (native threads under TSan)"),
            "tests")

    def test_a_renamed_step_reports_unavailable_not_zero(self):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        renamed = 0
        for job in mutated["jobs"]:
            if job["name"] != "Build / ubuntu-latest / gcc":
                continue
            for step in job["steps"]:
                if step["name"] == "Test":
                    step["name"] = "Verify build outputs"
                    renamed += 1
        self.assertEqual(renamed, 1, "fixture must contain the step to rename")
        report = tool.analyze(run, mutated)
        job = next(j for j in report["jobs"]
                   if j["name"] == "Build / ubuntu-latest / gcc")
        self.assertEqual(job["phases"]["tests"]["status"], "unavailable")
        self.assertIn("Verify build outputs", report["unmapped_steps"])


class SkewedRunners(unittest.TestCase):
    """The residual between the job window and the steps inside it is the one
    place a clock offset can still surface as a duration.  It must not."""

    STEP_START = "2026-09-11T08:34:54Z"

    def _job_with_step_span(self, span_seconds):
        run, jobs = fixture("run-clock-skew")
        mutated = copy.deepcopy(jobs)
        job = next(j for j in mutated["jobs"]
                   if j["name"] == "RC changelog freeze gate")
        # created 08:33:37, started 08:35:04, completed 08:35:14: a ten-second
        # server-clock window on a runner whose clock trails by ten seconds,
        # so its steps can span more than the window they sit in.
        end = (datetime.fromisoformat(self.STEP_START.replace("Z", "+00:00"))
               + timedelta(seconds=span_seconds))
        job["steps"] = [{"name": "Set up job", "number": 1,
                         "status": "completed", "conclusion": "success",
                         "started_at": self.STEP_START,
                         "completed_at": end.isoformat().replace("+00:00", "Z")}]
        return tool.analyze(run, mutated), job

    def test_a_step_span_inside_the_window_is_an_ordinary_residual(self):
        report, _ = self._job_with_step_span(5)
        job = next(j for j in report["jobs"]
                   if j["name"] == "RC changelog freeze gate")
        self.assertEqual(job["unaccounted"]["status"], "measured")
        self.assertEqual(job["unaccounted"]["seconds"], 5)

    def test_steps_spanning_more_than_the_window_are_not_a_duration(self):
        report, _ = self._job_with_step_span(11)
        job = next(j for j in report["jobs"]
                   if j["name"] == "RC changelog freeze gate")
        residual = job["unaccounted"]
        self.assertEqual(residual["status"], "anomalous")
        self.assertIsNone(residual["seconds"])
        self.assertEqual(residual["raw_seconds"], -1)
        # A reader scanning the summary must see it, not only the JSON.
        kinds = {a["kind"] for a in report["anomalies"]}
        self.assertIn("steps_exceed_job_wall", kinds)
        self.assertIn("steps_exceed_job_wall", tool.render_markdown(report))

    def test_a_negative_duration_can_never_be_constructed(self):
        with self.assertRaises(ValueError):
            tool.measured(-1, {"from": "a", "to": "b"})
        with self.assertRaises(ValueError):
            tool.measured(1, {})


class UnusableSteps(unittest.TestCase):
    """A step that matched a phase but carried no usable interval must not be
    reported as a phase no step matched; that reason would be false."""

    def _with_broken_test_step(self, started, completed):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        job = next(j for j in mutated["jobs"]
                   if j["name"] == "Build / ubuntu-latest / gcc")
        for step in job["steps"]:
            if step["name"] == "Test":
                step["started_at"], step["completed_at"] = started, completed
        report = tool.analyze(run, mutated)
        return next(j for j in report["jobs"]
                    if j["name"] == "Build / ubuntu-latest / gcc")

    def test_a_backwards_step_is_named_not_silently_dropped(self):
        job = self._with_broken_test_step("2026-09-10T19:50:00Z",
                                          "2026-09-10T19:49:00Z")
        self.assertEqual(job["phases"]["tests"]["status"], "unavailable")
        self.assertIn("no usable interval", job["phases"]["tests"]["reason"])
        self.assertEqual([u["step"] for u in job["unusable_steps"]], ["Test"])
        self.assertEqual(job["unusable_steps"][0]["raw_seconds"], -60)

    def test_a_step_without_timestamps_is_named_too(self):
        job = self._with_broken_test_step(None, None)
        self.assertIn("no usable interval", job["phases"]["tests"]["reason"])
        self.assertEqual([u["step"] for u in job["unusable_steps"]], ["Test"])

    def test_an_unusable_step_reaches_the_run_summary(self):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        job = next(j for j in mutated["jobs"]
                   if j["name"] == "Build / ubuntu-latest / gcc")
        for step in job["steps"]:
            if step["name"] == "Test":
                step["completed_at"] = step["started_at"]
                step["started_at"] = "2026-09-10T23:59:59Z"
        report = tool.analyze(run, mutated)
        self.assertIn("Test", report["unusable_steps"])
        kinds = {a["kind"] for a in report["anomalies"]}
        self.assertIn("unusable_step", kinds)
        self.assertIn("Test", tool.render_markdown(report))


class InFlightRunLevelMetrics(unittest.TestCase):
    def test_every_run_level_figure_is_a_lower_bound_until_the_run_ends(self):
        report = tool.analyze(*fixture("run-clock-skew"))
        self.assertEqual(report["status"], "in_progress")
        for key in ("run_wall_clock", "pr_feedback"):
            with self.subTest(metric=key):
                metric = report["metrics"][key]
                self.assertEqual(metric["status"], "partial")
                self.assertIn("lower bound", metric["reason"])
        failure = report["metrics"]["first_failure_latency"]
        self.assertEqual(failure["status"], "unavailable")
        self.assertIn("still in progress", failure["reason"])

    def test_a_completed_failure_is_final_even_mid_run(self):
        run, jobs = fixture("run-skip-cascade")
        in_flight = copy.deepcopy(run)
        in_flight["status"], in_flight["conclusion"] = "in_progress", None
        report = tool.analyze(in_flight, jobs)
        # A job still running can only complete later, so it can never produce
        # an earlier first failure: this value does not move.
        self.assertEqual(report["metrics"]["first_failure_latency"]["status"],
                         "measured")
        self.assertEqual(report["metrics"]["run_wall_clock"]["status"],
                         "partial")

    def test_a_finished_run_reports_final_figures(self):
        report = tool.analyze(*fixture("run-success"))
        self.assertEqual(report["metrics"]["run_wall_clock"]["status"],
                         "measured")
        self.assertEqual(report["metrics"]["pr_feedback"]["status"], "measured")


class CriticalPathPlaceholder(unittest.TestCase):
    """Unit 2 fills this in.  Until then it must not look like a result."""

    def test_placeholder_names_the_missing_work_and_drift_is_not_empty(self):
        path = tool.analyze(*fixture("run-success"))["critical_path"]
        for key in ("observed", "graph"):
            self.assertEqual(path[key]["status"], "unavailable")
            self.assertIn("no dependency graph was supplied",
                          path[key]["reason"])
        # An empty list would read as "compared the graph, found no drift".
        self.assertIsNone(path["graph_drift"])


class SkippedSteps(unittest.TestCase):
    """An `if:`-skipped step is stamped started == completed.  Counted, it adds
    a 0 to its phase and its name to the evidence; a phase whose steps were all
    skipped would then report a measured zero -- a phase that never ran,
    presented as instant.  The success fixture carries twenty of them."""

    def test_a_skipped_step_does_not_feed_the_phase_it_matched(self):
        report = tool.analyze(*fixture("run-success"))
        job = next(j for j in report["jobs"]
                   if j["name"] == "Sanitizers / ubuntu-latest / gcc")
        phase = job["phases"]["deps_install"]
        # The macOS install is skipped on a Linux runner; only the Linux one
        # actually installed anything.
        self.assertEqual(phase["evidence"]["matched_steps"],
                         ["Install dependencies (Linux)"])
        self.assertIn("Install dependencies (macOS)",
                      [item["step"] for item in job["skipped_steps"]])

    def test_a_wholly_skipped_phase_is_unavailable_not_zero(self):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        job = next(j for j in mutated["jobs"]
                   if j["name"] == "Build / windows-latest / msvc")
        renamed = 0
        for step in job["steps"]:
            if step["name"].startswith("Test"):
                step["conclusion"] = "skipped"
                step["completed_at"] = step["started_at"]
                renamed += 1
        self.assertGreater(renamed, 0, "fixture must contain Test steps")
        report = tool.analyze(run, mutated)
        entry = next(j for j in report["jobs"]
                     if j["name"] == "Build / windows-latest / msvc")
        phase = entry["phases"]["tests"]
        self.assertEqual(phase["status"], "unavailable")
        self.assertIn("skipped", phase["reason"])
        row = next(line for line in tool.render_markdown(report).splitlines()
                   if line.startswith("| Build / windows-latest / msvc |"))
        self.assertNotIn("| 0 |", row)
        self.assertIn("| unavailable |", row)

    def test_skipped_steps_are_not_anomalies(self):
        report = tool.analyze(*fixture("run-success"))
        kinds = {a["kind"] for a in report["anomalies"]}
        self.assertNotIn("unusable_step", kinds)
        self.assertNotIn("skipped_step", kinds)


class ReportPairing(unittest.TestCase):
    def test_a_mismatched_saved_pair_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / "raw").mkdir()
            run, _ = fixture("run-success")
            _, other_jobs = fixture("run-clock-skew")
            (out / "raw" / "run-999.json").write_text(json.dumps(run))
            (out / "raw" / "run-999-jobs.json").write_text(json.dumps(other_jobs))
            # Without the guard this published a confident 14-hour run wall.
            self.assertEqual(
                tool.main(["report", "--run-id", "999", "--out", tmp]), 1)
            self.assertFalse((out / "run-999.json").exists())


class FailureConclusions(unittest.TestCase):
    def test_a_timed_out_job_counts_as_the_first_failure(self):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        job = next(j for j in mutated["jobs"]
                   if j["name"] == "Build / ubuntu-latest / gcc")
        job["conclusion"] = "timed_out"
        report = tool.analyze(run, mutated)
        metric = report["metrics"]["first_failure_latency"]
        self.assertEqual(metric["status"], "measured")
        self.assertEqual(metric["evidence"]["job"], "Build / ubuntu-latest / gcc")

    def test_a_cancelled_job_is_not_a_failure_but_is_acknowledged(self):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            job["conclusion"] = "cancelled"
        report = tool.analyze(run, mutated)
        # A run cancelled by concurrency did not fail; counting it would put
        # scheduling noise into a failure metric.  But "no failed job" alone
        # would hide that cancellations were seen at all.
        metric = report["metrics"]["first_failure_latency"]
        self.assertEqual(metric["status"], "unavailable")
        self.assertIn("cancelled", metric["reason"])

    def test_a_timeout_says_why_no_step_is_named(self):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        job = next(j for j in mutated["jobs"]
                   if j["name"] == "Build / ubuntu-latest / gcc")
        job["conclusion"] = "timed_out"
        for step in job["steps"]:
            step["conclusion"] = "cancelled"
        report = tool.analyze(run, mutated)
        evidence = report["metrics"]["first_failure_latency"]["evidence"]
        # A timed-out job stamps its steps cancelled, so an empty step list is
        # expected; the conclusion says so instead of reading as a job that
        # failed with no failing step.
        self.assertEqual(evidence["failed_steps"], [])
        self.assertEqual(evidence["conclusion"], "timed_out")


class GraphAbsence(unittest.TestCase):
    def test_the_missing_graph_is_named_and_not_confused_with_a_root(self):
        report = tool.analyze(*fixture("run-success"))
        for job in report["jobs"]:
            metric = job["metrics"]["scheduler_release_latency"]
            with self.subTest(job=job["name"]):
                self.assertEqual(metric["status"], "unavailable")
                self.assertIn("no dependency graph was supplied",
                              metric["reason"])

    def test_a_root_job_is_named_as_a_root_once_a_graph_exists(self):
        run, jobs = fixture("run-success")
        # A genuine root is named in the graph with an empty dependency list.
        report = tool.analyze(run, jobs, {
            "Detect build-triggering changes": [],
            "lint / uncrustify check": ["lint / EditorConfig check"]})
        by_name = {j["name"]: j for j in report["jobs"]}
        root = by_name["Detect build-triggering changes"]["metrics"][
            "scheduler_release_latency"]
        self.assertIn("root job", root["reason"])

    def test_a_job_missing_from_the_graph_is_not_called_a_root(self):
        run, jobs = fixture("run-success")
        report = tool.analyze(run, jobs, {"lint / uncrustify check":
                                          ["lint / EditorConfig check"]})
        by_name = {j["name"]: j for j in report["jobs"]}
        # Absence from a partial graph is an extractor gap, not rootness; the
        # two were conflated and 17 of 18 jobs claimed to be roots.
        absent = by_name["Sanitizers / ubuntu-latest / gcc"]["metrics"][
            "scheduler_release_latency"]
        self.assertIn("absent from the supplied dependency graph",
                      absent["reason"])
        self.assertNotIn("root job", absent["reason"])

    def test_summability_is_stated_on_every_job_in_every_state(self):
        # The flag must never be absent: a missing key would have to be
        # interpreted, and on a skipped job there is nothing to interpret it
        # from.  run-skip-cascade carries five skipped jobs.
        for name in ("run-success", "run-skip-cascade"):
            report = tool.analyze(*fixture(name))
            for job in report["jobs"]:
                with self.subTest(fixture=name, job=job["name"]):
                    self.assertIs(
                        job["metrics"]["upstream_elapsed"]["additive"], False)
                    self.assertIs(
                        job["metrics"]["scheduler_release_latency"]["additive"],
                        True)


class DependencyGraph(unittest.TestCase):
    """The graph is read from the workflow files, never hard-coded: #1574 will
    change it, and a copy here would drift exactly when that happens."""

    WORKFLOW = SCRIPT_DIR.parent.parent / ".github" / "workflows" / "ci-pr.yml"

    def setUp(self):
        self.graph = tool.extract_graph(self.WORKFLOW)

    def test_the_reusable_lint_workflow_is_expanded_into_its_jobs(self):
        # `lint` is `uses: ./.github/workflows/lint-pr.yml`; the API never
        # reports a job by that name, so an unexpanded node would match
        # nothing and sever the edge between the freeze gate and every build.
        self.assertNotIn("lint", self.graph)
        self.assertIn("lint / EditorConfig check", self.graph)
        self.assertIn("lint / uncrustify check", self.graph)
        self.assertEqual(self.graph["lint / uncrustify check"],
                         ["lint / EditorConfig check"])
        # The caller's own dependency lands on the callee's root.
        self.assertEqual(self.graph["lint / EditorConfig check"],
                         ["RC changelog freeze gate"])
        # The caller's dependents attach to the callee's sink.
        self.assertIn("lint / uncrustify check",
                      self.graph["Build / ubuntu-latest / gcc"])

    def test_every_job_has_an_entry_and_roots_are_explicit(self):
        # A job missing from the graph must be a gap, never mistaken for a
        # root, which is why roots carry an explicit empty list.
        self.assertEqual(len(self.graph), 12)
        self.assertEqual(self.graph["Detect build-triggering changes"], [])
        self.assertEqual(self.graph["RC changelog freeze gate"], [])

    def test_the_default_workflow_path_resolves_from_any_directory(self):
        # It is anchored to the script, not the working directory, because the
        # tool runs from a build directory under meson.  Asserted explicitly:
        # report() degrades gracefully when the file is missing, so a wrong
        # default would otherwise cost the critical path in silence.
        self.assertTrue(Path(tool.DEFAULT_WORKFLOW).is_file(),
                        f"{tool.DEFAULT_WORKFLOW} does not exist")
        self.assertEqual(Path(tool.DEFAULT_WORKFLOW),
                         self.WORKFLOW.resolve())
        self.assertEqual(len(tool.extract_graph(tool.DEFAULT_WORKFLOW)), 12)

    def test_the_on_section_is_not_read_as_jobs(self):
        for spurious in ("pull_request", "workflow_call", "lint / workflow_call"):
            self.assertNotIn(spurious, self.graph)

    def test_every_node_resolves_against_a_real_run(self):
        _, jobs = fixture("run-success")
        names = [j["name"] for j in jobs["jobs"]]
        edges, drift = tool.match_graph_to_jobs(self.graph, names)
        self.assertEqual(drift["nodes_without_job"], [])
        self.assertEqual(drift["jobs_without_node"], [])
        self.assertEqual(set(edges), set(names))

    def test_matrix_placeholders_match_expanded_names(self):
        pattern = tool.name_pattern("Build / ${{ matrix.os }} / ${{ m }}")
        self.assertTrue(pattern.match("Build / windows-latest / msvc"))
        self.assertFalse(pattern.match("Sanitizers / ubuntu-latest / gcc"))

    def test_an_unreadable_needs_form_is_refused_not_read_as_a_root(self):
        # Block lists and wrapped flow lists are legal YAML this parser does
        # not read.  Returning [] would call the job a root and sever its
        # edges with nothing in the drift report to show for it.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".github" / "workflows").mkdir(parents=True)
            source = self.WORKFLOW.read_text().replace(
                "    needs: [sanitizer-matrix, build-scope]",
                "    needs:\n      - sanitizer-matrix\n      - build-scope", 1)
            (root / ".github" / "workflows" / "ci-pr.yml").write_text(source)
            (root / ".github" / "workflows" / "lint-pr.yml").write_text(
                (self.WORKFLOW.parent / "lint-pr.yml").read_text())
            with self.assertRaises(tool.WorkflowParseError) as caught:
                tool.extract_graph(root / ".github" / "workflows" / "ci-pr.yml")
            self.assertIn("sever", str(caught.exception))

    def test_a_literal_node_wins_over_a_matrix_pattern(self):
        # Otherwise correctness depends on which job happens to be declared
        # first, and #1574 reorders jobs.
        graph = {"Build / ${{ matrix.os }} / ${{ matrix.compiler }}": ["Wide"],
                 "Build / ubuntu-latest / gcc": ["Narrow"],
                 "Wide": [], "Narrow": []}
        jobs = ["Build / ubuntu-latest / gcc", "Build / macos-latest / clang",
                "Wide", "Narrow"]
        edges, drift = tool.match_graph_to_jobs(graph, jobs)
        self.assertEqual(edges["Build / ubuntu-latest / gcc"], ["Narrow"])
        self.assertEqual(edges["Build / macos-latest / clang"], ["Wide"])
        self.assertEqual(drift["nodes_without_job"], [])

    def test_placeholder_text_does_not_count_as_specificity(self):
        # Length alone would let a node whose placeholder name happens to be
        # long outrank a shorter but more literal one.
        graph = {"${{ matrix.a_very_long_placeholder_name }}": ["Wide"],
                 "Build / ${{ os }}": ["Narrow"], "Wide": [], "Narrow": []}
        edges, _ = tool.match_graph_to_jobs(
            graph, ["Build / linux", "Wide", "Narrow"])
        self.assertEqual(edges["Build / linux"], ["Narrow"])

    def test_a_dependency_never_claims_a_job_that_resolved_elsewhere(self):
        # Expanding a dependency by re-matching its pattern let a node claim
        # jobs owned by a more specific node, which could make a job its own
        # dependency and leave a cycle with nothing in the drift report.
        graph = {"Sanitizers / ${{ matrix.os }} / ${{ matrix.compiler }}": [],
                 "Sanitizers / tsan / ${{ matrix.compiler }}":
                     ["Sanitizers / ${{ matrix.os }} / ${{ matrix.compiler }}"]}
        jobs = ["Sanitizers / ubuntu-latest / gcc", "Sanitizers / tsan / gcc"]
        edges, _ = tool.match_graph_to_jobs(graph, jobs)
        for job, deps in edges.items():
            with self.subTest(job=job):
                self.assertNotIn(job, deps, "a job cannot depend on itself")
        self.assertEqual(edges["Sanitizers / tsan / gcc"],
                         ["Sanitizers / ubuntu-latest / gcc"])

    def test_drift_is_reported_when_the_run_and_the_graph_disagree(self):
        edges, drift = tool.match_graph_to_jobs(
            {"A job that never ran": []}, ["Some other job"])
        self.assertEqual(drift["nodes_without_job"], ["A job that never ran"])
        self.assertEqual(drift["jobs_without_node"], ["Some other job"])


class CriticalPath(unittest.TestCase):
    WORKFLOW = SCRIPT_DIR.parent.parent / ".github" / "workflows" / "ci-pr.yml"

    def _analyze(self, name):
        run, jobs = fixture(name)
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in jobs["jobs"]])
        return tool.analyze(run, jobs, edges, drift)

    def test_the_walk_follows_dependencies_not_the_nearest_finisher(self):
        path = self._analyze("run-success")["critical_path"]["observed"]
        chain = path["evidence"]["chain"]
        self.assertEqual(chain[-1], "TSan / ubuntu-latest / clang")
        # The nearest job to finish before TSan started was a macOS matrix
        # build that TSan does not depend on; taking it would have produced a
        # plausible and wrong path.
        self.assertNotIn("Build / macos-latest / clang", chain)
        self.assertIn("Sanitizers / ubuntu-latest / clang", chain)
        self.assertEqual(chain[0], "RC changelog freeze gate")

    def test_the_observed_path_counts_queueing_and_the_graph_path_does_not(self):
        paths = self._analyze("run-success")["critical_path"]
        # The gap between them is the queueing on the path, which is the
        # measurement #1574 needs before it reorders the graph.
        self.assertGreater(paths["observed"]["seconds"],
                           paths["graph"]["seconds"])

    def test_a_skip_cascade_whose_chain_avoids_it_stays_measured(self):
        # Every edge on the reported chain is known, so the number is final.
        # Flagging it because some unrelated edge elsewhere was unavailable
        # would attach a caveat that does not apply to the value shown.
        paths = self._analyze("run-skip-cascade")["critical_path"]
        self.assertEqual(paths["graph"]["status"], "measured")
        self.assertNotIn("Build / ubuntu-24.04-arm / gcc",
                         paths["graph"]["evidence"]["chain"])

    def _with_skipped(self, job_name):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            if job["name"] == job_name:
                job["conclusion"] = "skipped"
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        return tool.analyze(run, mutated, edges, drift)

    def test_a_chain_through_a_skipped_dependency_degrades_and_names_it(self):
        paths = self._with_skipped("lint / EditorConfig check")["critical_path"]
        graph = paths["graph"]
        self.assertEqual(graph["status"], "partial")
        self.assertIn("node weights only", graph["reason"])
        self.assertIsNotNone(graph["seconds"])
        # The caveat names the edges it applies to, so a reader can see which
        # part of the number is node weight alone.
        self.assertIn("RC changelog freeze gate -> lint / EditorConfig check",
                      graph["evidence"]["unusable_edges"])

    def test_the_observed_walk_stops_rather_than_inventing_a_hop(self):
        paths = self._with_skipped("lint / EditorConfig check")["critical_path"]
        observed = paths["observed"]
        self.assertEqual(observed["status"], "partial")
        self.assertIn("did not run", observed["reason"])
        # It stopped at the job whose dependency was skipped instead of
        # walking through it at zero weight.
        self.assertEqual(observed["evidence"]["chain"][0],
                         "lint / uncrustify check")

    def test_a_job_outside_the_graph_stops_the_walk_as_partial(self):
        run, jobs = fixture("run-success")
        graph = tool.extract_graph(self.WORKFLOW)
        del graph["TSan / ubuntu-latest / ${{ matrix.compiler }}"]
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in jobs["jobs"]])
        report = tool.analyze(run, jobs, edges, drift)
        observed = report["critical_path"]["observed"]
        # Before this, a job with no node got an empty edge list, which is
        # indistinguishable from a root, and the walk reported a confident
        # one-hop path a third the length of the real one.
        self.assertEqual(observed["status"], "partial")
        self.assertIn("not in the dependency graph", observed["reason"])
        self.assertIn("TSan", drift["jobs_without_node"][0])

    def test_a_run_missing_declared_jobs_is_flagged_not_reported_flat(self):
        # The one-job rerun fixture walks cleanly to its root, but eleven jobs
        # the graph declares never appear; a bare number would read as a
        # complete path over a complete run.
        report = self._analyze("run-rerun")
        observed = report["critical_path"]["observed"]
        self.assertEqual(observed["status"], "partial")
        self.assertIn("absent from this run", observed["reason"])
        self.assertEqual(len(report["critical_path"]["graph_drift"]
                             ["nodes_without_job"]), 11)

    def test_a_hop_the_run_did_not_time_is_not_summed_as_zero(self):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            if job["name"] == "Sanitizers / ubuntu-latest / clang":
                job["started_at"] = None     # completed, but never started
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        observed = tool.analyze(run, mutated, edges,
                                drift)["critical_path"]["observed"]
        # Summing that hop as zero reported 5206 against a true 10362 and
        # called it measured: a 50 per cent error presented as final.
        self.assertEqual(observed["status"], "partial")
        self.assertIn("not timed", observed["reason"])
        self.assertEqual(observed["evidence"]["unmeasured_hops"],
                         ["Sanitizers / ubuntu-latest / clang"])

    def test_an_untimed_node_on_the_graph_chain_is_named_too(self):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            # The lint segment is the only route from the freeze gate to the
            # builds, so the longest path cannot route around it the way it
            # routes around a zero-weight leaf.
            if job["name"] == "lint / uncrustify check":
                job["started_at"] = None
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        path = tool.analyze(run, mutated, edges, drift)["critical_path"]["graph"]
        self.assertEqual(path["status"], "partial")
        self.assertIn("not timed", path["reason"])
        self.assertIn("lint / uncrustify check",
                      path["evidence"]["unmeasured_nodes"])

    def test_an_untimed_job_off_the_graph_chain_still_qualifies_the_total(self):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            # A leaf the longest path legitimately routes around.  Weighted at
            # zero during selection, it is precisely the node that ends up off
            # the winning chain, so a caveat scoped to the chain never fires.
            if job["name"] == "Sanitizers / ubuntu-latest / clang":
                job["started_at"] = None
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        path = tool.analyze(run, mutated, edges, drift)["critical_path"]["graph"]
        # Previously measured 4431 on a chain that had quietly substituted a
        # different sanitizer job for the untimed one.
        self.assertEqual(path["status"], "partial")
        self.assertIn("routed around", path["reason"])
        self.assertEqual(path["evidence"]["untimed_jobs_off_chain"],
                         ["Sanitizers / ubuntu-latest / clang"])
        self.assertNotIn("Sanitizers / ubuntu-latest / clang",
                         path["evidence"]["chain"])

    def test_an_untimed_last_finisher_is_not_walked_past_in_silence(self):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            # The observed walk starts at the last job to finish that was
            # timed, so untiming the actual last finisher moves the start
            # down one branch and the job never appears on the result.
            if job["name"] == "TSan / ubuntu-latest / clang":
                job["started_at"] = None
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        path = tool.analyze(run, mutated, edges,
                            drift)["critical_path"]["observed"]
        self.assertEqual(path["status"], "partial")
        self.assertEqual(path["evidence"]["untimed_jobs_off_chain"],
                         ["TSan / ubuntu-latest / clang"])
        self.assertNotIn("TSan / ubuntu-latest / clang",
                         path["evidence"]["chain"])

    def test_a_skipped_job_is_never_counted_as_untimed(self):
        # A skipped job has no duration and that is the right answer, so the
        # run-wide caveat must not fire on the skip cascade and turn every
        # such run into a qualified number.
        report = self._analyze("run-skip-cascade")
        self.assertGreater(report["job_counts"]["skipped"], 0)
        for key in ("observed", "graph"):
            path = report["critical_path"][key]
            self.assertEqual(path["status"], "measured")
            self.assertNotIn("untimed_jobs_off_chain", path["evidence"])

    def test_a_graph_that_resolved_to_nothing_is_not_called_a_missing_graph(self):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            job["name"] = "renamed: " + job["name"]
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        self.assertEqual(edges, {})
        path = tool.analyze(run, mutated, edges, drift)["critical_path"]
        # A wholesale rename is the loudest possible drift signal.  Reporting
        # it as "no graph was supplied" would read as a missing argument.
        self.assertIn("see drift", path["graph"]["reason"])
        self.assertNotIn("was supplied", path["graph"]["reason"])
        self.assertIn("absent from this run", path["observed"]["reason"])
        self.assertIsNotNone(path["graph_drift"])
        self.assertEqual(len(path["graph_drift"]["jobs_without_node"]),
                         len(mutated["jobs"]))

    def test_the_observed_total_says_what_it_omits(self):
        path = self._analyze("run-success")["critical_path"]["observed"]
        self.assertIn("lower bound", path["evidence"]["note"])
        # Per-hop weights are published, not just the chain, so the total can
        # be checked rather than taken on trust.
        hops = path["evidence"]["hops"]
        self.assertEqual(
            sum((h["queue_delay"] or 0) + (h["job_wall"] or 0) for h in hops),
            path["seconds"])

    def test_without_a_graph_both_paths_say_so(self):
        report = tool.analyze(*fixture("run-success"))
        path = report["critical_path"]
        for key in ("observed", "graph"):
            self.assertEqual(path[key]["status"], "unavailable")
            self.assertIn("no dependency graph was supplied",
                          path[key]["reason"])
        self.assertIsNone(path["graph_drift"])


class Aggregation(unittest.TestCase):
    def _reports(self):
        return [tool.analyze(*fixture(name))
                for name in ("run-success", "run-skip-cascade")]

    def test_cells_are_job_and_population_never_pooled(self):
        doc = tool.aggregate_reports(self._reports())
        self.assertFalse(doc["pooled"])
        # One run spans both, which is why a run-level filter would refuse
        # every real input.
        self.assertIn("hosted", doc["populations"])
        self.assertIn("self-hosted", doc["populations"])
        for cell in doc["cells"]:
            self.assertIn("population", cell)
            self.assertIn("conclusion", cell)

    def test_a_cell_spanning_several_self_hosted_machines_says_so(self):
        # Hosted runners are fungible ephemeral VMs, so pooling them is right.
        # Self-hosted boxes are not, and the fixtures show the same job landing
        # on different ones between runs.
        first = tool.analyze(*fixture("run-skip-cascade"))
        # The clock-skew capture is the same workflow on different machines;
        # give it the same conclusion so the two land in one cell, which is
        # what happens across a real baseline window.
        second = tool.analyze(*fixture("run-clock-skew"))
        second["conclusion"] = first["conclusion"]
        doc = tool.aggregate_reports([first, second])
        mixed = [c for c in doc["cells"] if c["mixed_machines"]]
        self.assertTrue(mixed, "the fixtures move a job between machines")
        for cell in mixed:
            self.assertEqual(cell["population"], "self-hosted")
            self.assertGreater(len(cell["runner_names"]), 1)

    def test_a_hosted_cell_is_not_flagged_as_mixed(self):
        doc = tool.aggregate_reports([tool.analyze(*fixture("run-success"))])
        hosted = [c for c in doc["cells"] if c["population"] == "hosted"]
        self.assertTrue(hosted)
        for cell in hosted:
            self.assertFalse(cell["mixed_machines"])

    def test_failed_and_successful_runs_are_kept_apart(self):
        doc = tool.aggregate_reports(self._reports())
        conclusions = {cell["conclusion"] for cell in doc["cells"]}
        self.assertEqual(conclusions, {"success", "failure"})

    def test_n_is_per_metric_so_a_skipped_job_contributes_to_none(self):
        doc = tool.aggregate_reports(self._reports())
        skipped = [c for c in doc["cells"] if c["population"] == "none"]
        self.assertTrue(skipped, "the failed run skipped five jobs")
        for cell in skipped:
            for name, stats in cell["metrics"].items():
                with self.subTest(job=cell["job"], metric=name):
                    self.assertEqual(stats["n"], 0)
                    self.assertIsNone(stats["median"])

    def test_an_unknown_attempt_is_not_called_a_rerun(self):
        reports = self._reports()
        reports[0] = dict(reports[0], run_attempt=None)
        doc = tool.aggregate_reports(reports)
        self.assertEqual([e["reason"] for e in doc["excluded"]],
                         ["run_attempt unknown"])

    def test_a_rerun_is_excluded_and_named(self):
        reports = self._reports() + [tool.analyze(*fixture("run-rerun"))]
        doc = tool.aggregate_reports(reports)
        self.assertEqual([e["reason"] for e in doc["excluded"]],
                         ["run_attempt > 1"])

    def test_percentiles_are_nearest_rank_and_say_when_they_are_the_maximum(self):
        self.assertEqual(tool.percentile_nearest_rank([1, 2, 3, 4], 0.5), 2)
        self.assertEqual(tool.percentile_nearest_rank([1, 2, 3, 4], 0.95), 4)
        self.assertIsNone(tool.percentile_nearest_rank([], 0.5))
        summary = tool.summarize([5, 1, 3])
        self.assertEqual(summary["median"], 3)
        self.assertTrue(summary["p95_is_sample_max"])


class Populations(unittest.TestCase):
    def test_one_run_spans_several_runner_populations(self):
        report = tool.analyze(*fixture("run-success"))
        self.assertEqual(set(report["populations"]), {"hosted", "self-hosted"})
        self.assertTrue(report["populations"]["self-hosted"])

    def test_labels_are_not_a_source_of_truth(self):
        _, jobs = fixture("run-success")
        disguised = [j for j in jobs["jobs"]
                     if j.get("runner_name") == "semantic-reasoning-i401-6-2"]
        self.assertTrue(disguised, "fixture must keep the disguised runner")
        self.assertIn("ubuntu-latest", disguised[0]["labels"])
        self.assertEqual(tool.population_of(disguised[0]), "self-hosted")

    def test_windows_runner_is_self_hosted(self):
        self.assertEqual(
            tool.population_of({"runner_name": "B2A1"}), "self-hosted")
        self.assertEqual(
            tool.population_of({"runner_name": "GitHub Actions 1000054606"}),
            "hosted")


class RequiredChecks(unittest.TestCase):
    def test_unqueryable_required_set_is_recorded_not_faked(self):
        report = tool.analyze(*fixture("run-success"))
        metric = report["metrics"]["total_required_check_completion"]
        self.assertEqual(metric["status"], "unavailable")
        self.assertIn("404", metric["reason"])
        self.assertEqual(report["required_checks"]["surrogate"], "pr_feedback")
        evidence = report["metrics"]["pr_feedback"]["evidence"]
        self.assertEqual(evidence["source"], "pinned-list")
        # On this workflow every executed job is a pinned check, so pr_feedback
        # and run_wall_clock coincide by construction.  The count says so, so a
        # reader does not mistake the agreement for corroboration.
        self.assertEqual(evidence["non_pinned_executed"], 0)
        self.assertEqual(report["metrics"]["pr_feedback"]["seconds"],
                         report["metrics"]["run_wall_clock"]["seconds"])


class FetchBudget(unittest.TestCase):
    """`fetch` is the only networked path.  Its transport is injectable, so
    these cases assert the request list and the retry policy offline."""

    def test_two_requests_for_a_single_page_run(self):
        calls = []

        def transport(args):
            calls.append(args[-1])
            if "/jobs" in args[-1]:
                return json.dumps({"total_count": 1, "jobs": [{"name": "a"}]}).encode()
            return json.dumps({"id": 7}).encode()

        budget = tool.Budget(tool.DEFAULT_MAX_REQUESTS)
        run, jobs = tool.fetch_run("o/r", 7, budget, transport, lambda _s: None)
        self.assertEqual(budget.used, 2)
        self.assertEqual(run["id"], 7)
        self.assertEqual(jobs["total_count"], 1)
        self.assertEqual(calls[0], "repos/o/r/actions/runs/7")
        self.assertIn("filter=latest", calls[1])

    def test_budget_is_a_hard_stop(self):
        def transport(args):
            return json.dumps({"id": 1, "total_count": 0, "jobs": []}).encode()

        budget = tool.Budget(1)
        with self.assertRaises(RuntimeError) as caught:
            tool.fetch_run("o/r", 7, budget, transport, lambda _s: None)
        self.assertIn("budget exhausted", str(caught.exception))

    def test_rate_limit_is_retried_and_other_errors_are_not(self):
        attempts = {"n": 0}
        slept = []

        def flaky(args):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise RuntimeError("HTTP 403: API rate limit exceeded")
            return json.dumps({"id": 1}).encode()

        budget = tool.Budget(10)
        self.assertEqual(
            tool.api_get("p", budget, flaky, slept.append)["id"], 1)
        self.assertEqual(slept, [1])

        def fatal(args):
            raise RuntimeError("HTTP 404: Not Found")

        with self.assertRaises(RuntimeError):
            tool.api_get("p", tool.Budget(10), fatal, slept.append)
        self.assertEqual(slept, [1], "a 404 must not be retried")


class Rendering(unittest.TestCase):
    def test_markdown_names_skipped_jobs_and_survives_a_failed_run(self):
        report = tool.analyze(*fixture("run-skip-cascade"))
        text = tool.render_markdown(report)
        self.assertIn("10 executed, 5 skipped", text)
        self.assertIn("first_failure_latency", text)
        self.assertNotIn("| 0 |", text.split("Per job")[0])

    WORKFLOW = SCRIPT_DIR.parent.parent / ".github" / "workflows" / "ci-pr.yml"

    def test_the_summary_says_the_three_totals_are_not_independent(self):
        run, jobs = fixture("run-success")
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in jobs["jobs"]])
        report = tool.analyze(run, jobs, edges, drift)
        text = tool.render_markdown(report)
        # All three appear adjacent in the table; without this a reader sees
        # 10366 / 10366 / 10362 and counts three agreeing measurements.
        self.assertIn("not three independent measurements", text)
        self.assertIn("by construction", text)
        self.assertIn("lower bound", text)

    def test_an_unreadable_workflow_is_diagnosed_once_not_twice(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / "raw").mkdir()
            run, jobs = fixture("run-success")
            (out / "raw" / f"run-{run['id']}.json").write_text(json.dumps(run))
            (out / "raw" / f"run-{run['id']}-jobs.json").write_text(
                json.dumps(jobs))
            captured = io.StringIO()
            with contextlib.redirect_stderr(captured):
                self.assertEqual(tool.main(
                    ["report", "--run-id", str(run["id"]), "--out", tmp,
                     "--workflow", str(out / "absent.yml")]), 0)
            text = captured.getvalue()
            # The file could not be read, so nothing is known about how many
            # jobs it declares.  Saying it declares none is a second claim,
            # and a false one.
            self.assertIn("no dependency graph", text)
            self.assertNotIn("declares no jobs", text)

    def test_report_command_writes_both_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / "raw").mkdir()
            run, jobs = fixture("run-success")
            (out / "raw" / f"run-{run['id']}.json").write_text(json.dumps(run))
            (out / "raw" / f"run-{run['id']}-jobs.json").write_text(json.dumps(jobs))
            self.assertEqual(
                tool.main(["report", "--run-id", str(run["id"]), "--out", tmp]), 0)
            self.assertTrue((out / f"run-{run['id']}.json").exists())
            self.assertTrue((out / f"run-{run['id']}.md").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
