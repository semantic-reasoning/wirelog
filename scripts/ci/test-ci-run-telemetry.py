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

import copy
import importlib.util
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
            self.assertIn("unit 2", path[key]["reason"])
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
                self.assertIn("unit 2", metric["reason"])

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
