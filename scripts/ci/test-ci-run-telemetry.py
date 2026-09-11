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
    run = json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))
    jobs = json.loads(
        (FIXTURES / f"{name}-jobs.json").read_text(encoding="utf-8"))
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

    def _did_not_run_as(self, conclusion, name=None):
        """A run-success job restamped the way a job that never ran is."""
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = name or "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = conclusion
                job["started_at"] = job["created_at"]
                job["completed_at"] = job["created_at"]
                job["steps"] = []
        return victim, tool.analyze(run, mutated)

    def test_the_cli_line_names_the_same_division_as_the_report(self):
        # Two renderings of one fact.  The CLI line is what an operator sees
        # without opening the artefact, so it cannot be the stale one.
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / "raw").mkdir()
            run, jobs = fixture("run-success")
            mutated = copy.deepcopy(jobs)
            for job in mutated["jobs"]:
                if job["name"] == "Sanitizers / ubuntu-latest / clang":
                    job["conclusion"] = "startup_failure"
                    job["started_at"] = job["created_at"]
                    job["completed_at"] = job["created_at"]
                    job["steps"] = []
            (out / "raw" / f"run-{run['id']}.json").write_text(
                json.dumps(run), encoding="utf-8")
            (out / "raw" / f"run-{run['id']}-jobs.json").write_text(
                json.dumps(mutated), encoding="utf-8")
            captured = io.StringIO()
            with contextlib.redirect_stdout(captured):
                self.assertEqual(tool.main(
                    ["report", "--run-id", str(run["id"]), "--out", tmp]), 0)
        self.assertIn("17 executed, 1 failed to start", captured.getvalue())

    def test_a_run_where_nothing_ran_still_reads_correctly(self):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            job["conclusion"] = "cancelled"
            job["started_at"] = job["created_at"]
            job["completed_at"] = job["created_at"]
            job["steps"] = []
        report = tool.analyze(run, mutated)
        text = tool.render_markdown(report)
        self.assertIn(f"0 executed, {len(mutated['jobs'])} cancelled before "
                      f"running", text)
        self.assertEqual(report["metrics"]["run_wall_clock"]["status"],
                         "unavailable")

    def test_the_summary_names_the_conclusion_the_job_actually_had(self):
        # A runner image that failed to boot means a gate never covered the
        # change.  Reporting it as somebody hitting cancel sends the reader
        # to the wrong question, and the table below carries no conclusion
        # at all, so this line is their only signal.
        for conclusion, label in (("cancelled", "cancelled before running"),
                                  ("stale", "stale"),
                                  ("startup_failure", "failed to start")):
            with self.subTest(conclusion=conclusion):
                _, report = self._did_not_run_as(conclusion)
                text = tool.render_markdown(report)
                self.assertIn(f"1 {label}", text)
                # The caption has to cover this conclusion too: the table
                # row carries six blanks and no conclusion at all.
                # Scoped to the table it sits under, because the JSON does
                # carry one duration for such a job: its release latency
                # ends at the job's creation, not at anything the job did.
                self.assertIn("has no durations in this table", text)
                self.assertNotIn("no durations at all", text)
                self.assertNotIn("A job that was skipped, or cancelled", text)
                for other in ("cancelled before running", "stale",
                              "failed to start"):
                    if other != label:
                        self.assertNotIn(f"1 {other}", text)

    def test_a_conclusion_with_no_label_is_named_as_itself(self):
        counts = {"executed": 2, "skipped": 0,
                  "did_not_run_by_conclusion": {"brand_new": 1}}
        self.assertEqual(tool.job_count_phrase(counts),
                         "2 executed, 1 brand_new")

    def test_every_no_run_conclusion_has_a_reason_and_a_label(self):
        # Adding a conclusion to one table and not the other would fall
        # silently back to the generic sentence or to the raw key.
        self.assertEqual(set(tool.NO_EXECUTION_REASONS),
                         set(tool.NO_RUN_LABELS))
        # Two conclusions sharing a term would render "1 stale, 1 stale",
        # which the reader cannot take apart.
        labels = list(tool.NO_RUN_LABELS.values())
        self.assertEqual(len(set(labels)), len(labels), labels)
        # And a conclusion neither table has met still gets a true sentence
        # and a term naming itself, rather than borrowing another's.
        self.assertEqual(tool.no_run_label("a_conclusion_from_the_future"),
                         "a_conclusion_from_the_future")
        self.assertIn("a_conclusion_from_the_future",
                      tool.no_execution_reason("a_conclusion_from_the_future"))
        self.assertIn("no execution",
                      tool.no_execution_reason("a_conclusion_from_the_future"))

    def test_each_no_run_conclusion_says_what_happened_in_its_own_words(self):
        sentences = list(tool.NO_EXECUTION_REASONS.values())
        self.assertEqual(len(set(sentences)), len(sentences), sentences)
        self.assertEqual(tool.NO_EXECUTION_REASONS, {
            "skipped": "job skipped; no execution",
            "cancelled": "job cancelled before it ran; no execution",
            "stale": "job stale; no execution",
            "startup_failure": "job failed to start; no execution",
            "action_required": "job awaiting approval; no execution",
        })
        self.assertEqual(tool.NO_RUN_LABELS, {
            "skipped": "skipped",
            "cancelled": "cancelled before running",
            "stale": "stale",
            "startup_failure": "failed to start",
            "action_required": "awaiting approval",
        })

    def test_a_cancellation_still_reaches_the_first_failure_sentence(self):
        # Once a job that did not run leaves the executed list, scanning
        # that list for cancellations stops finding them and the reader is
        # told the run simply had no failure.
        _, report = self._did_not_run_as("cancelled")
        metric = report["metrics"]["first_failure_latency"]
        self.assertEqual(metric["status"], "unavailable")
        self.assertIn("cancelled jobs were seen", metric["reason"])

    def test_a_job_that_did_not_run_cannot_stretch_the_pr_feedback(self):
        # Both run-level spans end at the last job that actually ran; a
        # never-run job's completion stamp is bookkeeping.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "cancelled"
                job["created_at"] = "2030-01-01T00:00:00Z"
                job["started_at"] = "2030-01-01T00:00:00Z"
                job["completed_at"] = "2030-01-01T00:00:00Z"
                job["steps"] = []
        metrics = tool.analyze(run, mutated)["metrics"]
        self.assertEqual(metrics["run_wall_clock"]["seconds"], 10366)
        self.assertEqual(metrics["pr_feedback"]["seconds"], 10366)

    def test_a_job_that_did_not_run_keeps_its_release_latency(self):
        # Deliberate: that metric ends at the job's *creation*, which
        # happens whether or not the job then dispatches.  Only the skip
        # cascade is carved out, because GitHub batches its creation stamps.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "cancelled"
                job["started_at"] = job["created_at"]
                job["completed_at"] = job["created_at"]
                job["steps"] = []
        workflow = SCRIPT_DIR.parent.parent / ".github" / "workflows" / \
            "ci-pr.yml"
        graph = tool.extract_graph(workflow)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        entry = next(job for job in tool.analyze(run, mutated, edges,
                                                 drift)["jobs"]
                     if job["name"] == victim)
        self.assertFalse(entry["ran"])
        self.assertEqual(
            entry["metrics"]["scheduler_release_latency"]["status"],
            "measured")

    def test_a_job_level_conclusion_that_may_precede_dispatch_is_weighed(self):
        # `neutral` and `action_required` are the two GitHub documents on the
        # job itself, and approval is by definition something that happens
        # before a dispatch.  Stamped the never-run way they used to publish
        # queue 0 and wall 0 and count as executed.
        run, jobs = fixture("run-success")
        for conclusion in ("neutral", "action_required"):
            with self.subTest(conclusion=conclusion):
                mutated = copy.deepcopy(jobs)
                victim = "Sanitizers / ubuntu-latest / clang"
                for job in mutated["jobs"]:
                    if job["name"] == victim:
                        job["conclusion"] = conclusion
                        job["started_at"] = job["created_at"]
                        job["completed_at"] = job["created_at"]
                        job["steps"] = []
                report = tool.analyze(run, mutated)
                entry = next(job for job in report["jobs"]
                             if job["name"] == victim)
                self.assertFalse(entry["ran"])
                self.assertIsNone(entry["metrics"]["job_wall"]["seconds"])
                self.assertEqual(report["job_counts"]["never_ran"], 1)

    def test_a_conclusion_this_tool_has_not_met_is_weighed_not_assumed(self):
        # The rule is closed the other way round: only an outcome that proves
        # execution skips the evidence.  A conclusion GitHub adds after this
        # was written must not default to "ran" and publish a zero.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "a_conclusion_from_the_future"
                job["started_at"] = job["created_at"]
                job["completed_at"] = job["created_at"]
                job["steps"] = []
        report = tool.analyze(run, mutated)
        entry = next(job for job in report["jobs"] if job["name"] == victim)
        self.assertFalse(entry["ran"])
        self.assertIn("a_conclusion_from_the_future",
                      entry["metrics"]["job_wall"]["reason"])
        self.assertIn("1 a_conclusion_from_the_future",
                      tool.render_markdown(report))

    def test_an_outcome_only_execution_reaches_is_never_weighed(self):
        # A successful job ran, whatever its stamps say.  Asking the evidence
        # there could only erase real seconds.
        run, jobs = fixture("run-success")
        for conclusion in tool.CONCLUSIONS_THAT_RAN:
            with self.subTest(conclusion=conclusion):
                mutated = copy.deepcopy(jobs)
                victim = "Sanitizers / ubuntu-latest / clang"
                for job in mutated["jobs"]:
                    if job["name"] == victim:
                        job["conclusion"] = conclusion
                        job["started_at"] = job["created_at"]
                        job["completed_at"] = job["created_at"]
                        job["steps"] = []
                entry = next(job for job in tool.analyze(run, mutated)["jobs"]
                             if job["name"] == victim)
                self.assertTrue(entry["ran"])

    def test_a_run_mixing_no_run_conclusions_names_each_kind_once(self):
        # One table names every term, skipped included.  A second spelling
        # beside it would double the skipped term on any run carrying a
        # skipped job, and a run carrying two kinds pins their order too.
        run, jobs = fixture("run-skip-cascade")
        mutated = copy.deepcopy(jobs)
        victim = next(job["name"] for job in mutated["jobs"]
                      if job["conclusion"] != "skipped")
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "startup_failure"
                job["started_at"] = job["created_at"]
                job["completed_at"] = job["created_at"]
                job["steps"] = []
        counts = tool.analyze(run, mutated)["job_counts"]
        self.assertEqual(tool.job_count_phrase(counts),
                         "9 executed, 1 failed to start, 5 skipped")

    def test_the_summary_terms_do_not_reorder_with_the_job_list(self):
        # Three distinct no-run conclusions in one run.  Without a sort the
        # phrase follows whatever order the API returned the jobs in.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victims = ["Sanitizers / ubuntu-latest / clang",
                   "Sanitizers / ubuntu-latest / gcc",
                   "Sanitizers / macos-latest / clang"]
        for name, conclusion in zip(victims, ("startup_failure", "stale",
                                              "cancelled")):
            for job in mutated["jobs"]:
                if job["name"] == name:
                    job["conclusion"] = conclusion
                    job["started_at"] = job["created_at"]
                    job["completed_at"] = job["created_at"]
                    job["steps"] = []
        forward = tool.job_count_phrase(
            tool.analyze(run, mutated)["job_counts"])
        reversed_jobs = {"jobs": list(reversed(mutated["jobs"]))}
        backward = tool.job_count_phrase(
            tool.analyze(run, reversed_jobs)["job_counts"])
        self.assertEqual(forward, backward)
        self.assertEqual(
            forward,
            "15 executed, 1 cancelled before running, 1 failed to start, "
            "1 stale")
        # Alphabetical by the term printed, not by GitHub's conclusion name,
        # which would read as arbitrary order to anyone looking at the line.

    def test_an_evidence_decided_conclusion_keeps_its_evidence(self):
        # `stale` and `startup_failure` are documented on the run, not the
        # job.  If one lands on a job that really ran, deciding it from the
        # conclusion alone would erase its seconds from the run wall clock
        # with nothing saying so.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "TSan / ubuntu-latest / clang"      # the last to finish
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "stale"
                self.assertTrue(job["steps"])
        report = tool.analyze(run, mutated)
        entry = next(job for job in report["jobs"] if job["name"] == victim)
        self.assertTrue(entry["ran"])
        self.assertEqual(report["metrics"]["run_wall_clock"]["seconds"], 10366)

    def test_a_job_that_did_not_run_is_not_counted_as_executed(self):
        # The header sat directly above a caption excusing the job and a row
        # of six blanks below it.  One test decides all three.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "cancelled"
                job["started_at"] = job["created_at"]
                job["completed_at"] = job["created_at"]
                job["steps"] = []
        report = tool.analyze(run, mutated)
        counts = report["job_counts"]
        self.assertEqual(counts["executed"], len(mutated["jobs"]) - 1)
        self.assertEqual(counts["never_ran"], 1)
        self.assertEqual(counts["skipped"], 0)
        text = tool.render_markdown(report)
        self.assertIn("17 executed, 1 cancelled before running", text)
        self.assertNotIn("0 skipped", text)

    def test_counts_lead_with_what_actually_ran(self):
        self.assertEqual(self.report["job_counts"],
                         {"total": 15, "executed": 10, "skipped": 5,
                          "never_ran": 0,
                          "did_not_run_by_conclusion": {"skipped": 5}})

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
            (out / "raw" / "run-999.json").write_text(json.dumps(run),
                                                      encoding="utf-8")
            (out / "raw" / "run-999-jobs.json").write_text(
                json.dumps(other_jobs), encoding="utf-8")
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
            source = self.WORKFLOW.read_text(encoding="utf-8").replace(
                "    needs: [sanitizer-matrix, build-scope]",
                "    needs:\n      - sanitizer-matrix\n      - build-scope", 1)
            (root / ".github" / "workflows" / "ci-pr.yml").write_text(
                source, encoding="utf-8")
            (root / ".github" / "workflows" / "lint-pr.yml").write_text(
                (self.WORKFLOW.parent / "lint-pr.yml").read_text(
                    encoding="utf-8"), encoding="utf-8")
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

    def test_the_observed_walk_does_not_step_through_a_cancelled_job(self):
        # A job cancelled before it ran carries a completion stamp and no
        # duration.  Walking through it at zero weight drops the gap it
        # represents, which is the same reason a skipped job is excluded.
        run, jobs = fixture("run-success")
        victim = "Sanitizers / ubuntu-latest / clang"
        mutated = self._cancelled_before_starting(jobs, victim)
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        path = tool.analyze(run, mutated, edges,
                            drift)["critical_path"]["observed"]
        self.assertNotIn(victim, path["evidence"]["chain"])
        # The walk routes around it rather than stopping: the same job still
        # ends the path, reached through a dependency that did run.
        self.assertEqual(path["evidence"]["chain"][-1],
                         "TSan / ubuntu-latest / clang")
        self.assertIn("Sanitizers / macos-latest / clang",
                      path["evidence"]["chain"])

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

    def test_an_untimed_job_on_the_chain_is_not_also_called_off_chain(self):
        # The two keys mean different things: one says the total is short by
        # this hop, the other says the chain itself may be wrong.  Collapsing
        # them would make the second unreadable.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            if job["name"] == "lint / uncrustify check":
                job["started_at"] = None
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        paths = tool.analyze(run, mutated, edges, drift)["critical_path"]
        for key in ("observed", "graph"):
            metric = paths[key]
            self.assertIn("lint / uncrustify check", metric["evidence"]["chain"])
            self.assertNotIn("untimed_jobs_off_chain", metric["evidence"])

    def test_a_run_with_no_jobs_still_distinguishes_drift_from_no_graph(self):
        # Only `nodes_without_job` is populated here, so the empty-edges
        # branch cannot lean on `jobs_without_node` alone.
        run, _ = fixture("run-success")
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(graph, [])
        self.assertEqual(drift["jobs_without_node"], [])
        self.assertTrue(drift["nodes_without_job"])
        path = tool.analyze(run, {"jobs": []}, edges, drift)["critical_path"]
        self.assertIn("see drift", path["graph"]["reason"])

    def test_a_job_still_running_is_not_reported_as_missing_data(self):
        # Every unfinished job in a live run is untimed by definition.
        # Calling that missing data makes the caveat fire on every live run
        # with an alarm it does not deserve, which is how a reader learns to
        # skip it.
        run, jobs = fixture("run-queued")
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in jobs["jobs"]])
        paths = tool.analyze(run, jobs, edges, drift)["critical_path"]
        for key in ("observed", "graph"):
            metric = paths[key]
            self.assertEqual(metric["status"], "partial")
            self.assertIn("have not finished", metric["reason"])
            self.assertNotIn("reported no duration", metric["reason"])
            self.assertTrue(metric["evidence"]["unfinished_jobs_off_chain"])
            self.assertNotIn("untimed_jobs_off_chain", metric["evidence"])

    def test_a_job_with_no_creation_time_says_that_and_not_something_else(self):
        # queue_delay measures from creation.  Reporting the missing start
        # when the creation stamp is what is absent is the same false claim
        # the job_wall reason was fixed for.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        mutated["jobs"][0]["created_at"] = None
        report = tool.analyze(run, mutated)
        self.assertEqual(report["jobs"][0]["metrics"]["queue_delay"]["reason"],
                         "job has no creation time")
        self.assertIn("job has no creation time",
                      tool.render_markdown(report))

    def test_one_job_gets_one_story_about_its_missing_start(self):
        # queue_delay ends where job_wall begins, so both describe the same
        # absent stamp.  Two adjacent bullets disagreeing about it is a
        # contradiction the reader can see without leaving the page.
        run, jobs = fixture("run-queued")
        report = tool.analyze(run, jobs)
        queued = [job for job in report["jobs"]
                  if job["started_at"] is None
                  and job["conclusion"] != "skipped"]
        self.assertTrue(queued)
        for job in queued:
            self.assertEqual(job["metrics"]["queue_delay"]["reason"],
                             job["metrics"]["job_wall"]["reason"])
            self.assertEqual(job["metrics"]["queue_delay"]["reason"],
                             "job has not started yet")
        text = tool.render_markdown(report)
        self.assertNotIn("job never started", text)

    def test_a_completed_job_is_settled_whatever_the_run_status_says(self):
        # The run's readability decides nothing about a job that is already
        # done: it will not start now.
        run, jobs = fixture("run-success")
        unreadable = copy.deepcopy(run)
        unreadable["status"] = "something-new"
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["started_at"] = None
                job["steps"] = []
                job["conclusion"] = None   # nothing here says it executed
        entry = next(job for job in tool.analyze(unreadable, mutated)["jobs"]
                     if job["name"] == victim)
        self.assertEqual(entry["status"], "completed")
        self.assertEqual(entry["metrics"]["job_wall"]["reason"],
                         "job never started")

    def test_a_started_in_flight_job_in_an_unreadable_run_says_which(self):
        # Start stamp present, completion absent, job status in flight, run
        # status unreadable.  The tool knows the job did not report a
        # completion and does not know whether the run is over, so it says
        # the second thing rather than asserting the first.
        run, jobs = fixture("run-queued")
        unreadable = copy.deepcopy(run)
        unreadable["status"] = "something-new"
        mutated = copy.deepcopy(jobs)
        victim = None
        for job in mutated["jobs"]:
            if job["status"] == "in_progress" and job["started_at"]:
                victim = job["name"]
                job["completed_at"] = None
                job["steps"] = []
                break
        self.assertIsNotNone(victim)
        entry = next(job for job in tool.analyze(unreadable, mutated)["jobs"]
                     if job["name"] == victim)
        reason = entry["metrics"]["job_wall"]["reason"]
        self.assertIn("is not one this tool recognises", reason)
        self.assertNotIn("stopped without reporting", reason)
        # The start stamp is present and printed two fields away, so the
        # sentence has to be about the end that is actually missing.
        self.assertIsNotNone(entry["started_at"])
        self.assertIn("no completion time", reason)
        self.assertNotIn("no start stamp", reason)

    def test_an_unreadable_run_status_settles_nothing_about_a_job(self):
        # The report says twice, at run level, that the run may still be
        # going.  Telling the reader a queued job in it never started is the
        # contradiction this whole unit is about, one level up.
        run, jobs = fixture("run-queued")
        unreadable = copy.deepcopy(run)
        unreadable["status"] = "something-new"
        report = tool.analyze(unreadable, jobs)
        text = tool.render_markdown(report)
        self.assertNotIn("job never started", text)
        self.assertNotIn("job has not started yet", text)
        waiting = [job for job in report["jobs"]
                   if job["started_at"] is None and job["ran"]]
        self.assertTrue(waiting)
        for job in waiting:
            for metric in ("queue_delay", "job_wall"):
                self.assertIn("is not one this tool recognises",
                              job["metrics"][metric]["reason"])

    def test_a_stalled_job_is_not_told_its_start_may_still_arrive(self):
        # The walkers already call this job missing data.  A cell telling
        # the reader it may still start contradicts the caveat above it.
        run, jobs = fixture("run-queued")
        finished = copy.deepcopy(run)
        finished["status"] = "completed"
        report = tool.analyze(finished, jobs)
        stalled = [job for job in report["jobs"]
                   if job["status"] in tool.IN_FLIGHT_STATUSES
                   and job["started_at"] is None]
        self.assertTrue(stalled)
        for job in stalled:
            for metric in ("queue_delay", "job_wall"):
                self.assertEqual(job["metrics"][metric]["reason"],
                                 "job never started")

    def test_a_cancelled_job_that_ran_without_steps_keeps_its_seconds(self):
        # run-missing-data ships a successful job with sixty seconds of wall
        # time and an empty step list, so "it reported no steps" cannot mean
        # "it never ran".  Flip only its conclusion and the seconds must
        # survive.
        run, jobs = fixture("run-missing-data")
        mutated = copy.deepcopy(jobs)
        victim = "Detect build-triggering changes"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                self.assertEqual(job["steps"], [])
                job["conclusion"] = "cancelled"
        entry = next(job for job in tool.analyze(run, mutated)["jobs"]
                     if job["name"] == victim)
        self.assertTrue(entry["ran"])
        self.assertFalse(tool.absent_by_design(entry))
        self.assertEqual(entry["metrics"]["job_wall"]["seconds"], 60)

    def test_a_step_that_ran_outranks_a_span_that_says_otherwise(self):
        # A job cancelled inside its dispatch second is stamped start ==
        # completion at GitHub's one-second resolution, which the span rule
        # reads as never-run.  Its step is the only thing that tells the two
        # apart, so the step has to be asked first.  The zero wall time it
        # then reports is true: the step evidence establishes it ran.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "cancelled"
                job["completed_at"] = job["started_at"]
                step = job["steps"][0]
                job["steps"] = [dict(step,
                                     completed_at=step["started_at"])]
        entry = next(job for job in tool.analyze(run, mutated)["jobs"]
                     if job["name"] == victim)
        self.assertTrue(entry["ran"])
        self.assertFalse(tool.absent_by_design(entry))

    def test_a_step_of_no_duration_is_still_evidence(self):
        # Most steps finish inside a second: 100 of the 299 non-skipped steps
        # in the shipped captures are stamped started == completed.  Reading
        # those as unusable would discard the commonest shape there is.
        _, jobs = fixture("run-success")
        template = next(job["steps"][0] for job in jobs["jobs"]
                        if job["name"] == "Sanitizers / ubuntu-latest / clang")
        instant = dict(template, completed_at=template["started_at"])
        self.assertEqual(tool.first_step_start({"steps": [instant]}),
                         template["started_at"])

    def test_a_start_stamped_at_creation_is_not_a_dispatch(self):
        # The equality is the bookkeeping signature itself: a job that never
        # ran is stamped as starting the moment it was created.  Reading it
        # as a dispatch publishes queue_delay 0 for a job that never waited
        # on a runner, and lists it as missing data besides.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "cancelled"
                job["started_at"] = job["created_at"]
                job["completed_at"] = None
                job["steps"] = []
        entry = next(job for job in tool.analyze(run, mutated)["jobs"]
                     if job["name"] == victim)
        self.assertFalse(entry["ran"])
        self.assertIsNone(entry["metrics"]["queue_delay"]["seconds"])

    def test_a_weighed_conclusion_with_never_run_stamps_reads_as_never_run(
            self):
        # The conclusion decides only who is asked.  `skipped` and an
        # outcome that proves execution settle it without the stamps;
        # every other conclusion is weighed, and these are the stamps a job
        # that never ran carries.  None of these subjects is settled by its
        # conclusion: each of them, stamped a different way, can read as
        # having run -- which is what the tests below this one show.
        run, jobs = fixture("run-success")
        # Named rather than looped over the constant, which would shrink
        # with it and prove nothing.
        self.assertEqual(tool.NEVER_DISPATCHED, ("skipped",))
        self.assertEqual(tool.CONCLUSIONS_THAT_RAN,
                         ("success", "failure", "timed_out"))
        for conclusion in ("stale", "startup_failure", "cancelled",
                           "neutral", "action_required",
                           "a_conclusion_from_the_future"):
            with self.subTest(conclusion=conclusion):
                mutated = copy.deepcopy(jobs)
                victim = "Sanitizers / ubuntu-latest / clang"
                for job in mutated["jobs"]:
                    if job["name"] == victim:
                        job["conclusion"] = conclusion
                        job["started_at"] = job["created_at"]
                        job["completed_at"] = job["created_at"]
                        job["steps"] = []
                entry = next(job for job in tool.analyze(run, mutated)["jobs"]
                             if job["name"] == victim)
                self.assertFalse(entry["ran"])
                self.assertIn("no execution",
                              entry["metrics"]["job_wall"]["reason"])

    def test_a_positive_span_alone_proves_the_job_ran(self):
        # Picked up with no queue delay, cancelled, no steps: the span
        # between its own start and completion is the only evidence left.
        run, jobs = fixture("run-missing-data")
        mutated = copy.deepcopy(jobs)
        victim = "Detect build-triggering changes"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "cancelled"
                job["created_at"] = job["started_at"]   # no queue delay
                job["steps"] = []
        entry = next(job for job in tool.analyze(run, mutated)["jobs"]
                     if job["name"] == victim)
        self.assertTrue(entry["ran"])
        self.assertEqual(entry["metrics"]["job_wall"]["seconds"], 60)

    def test_a_start_later_than_creation_alone_proves_the_job_ran(self):
        # Cancelled with its completion stamp lost and no steps.  The only
        # trace left is that the start came after the creation, which a job
        # that never ran does not have: it is stamped as starting when it
        # was created.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "cancelled"
                job["completed_at"] = None
                job["steps"] = []
                self.assertNotEqual(job["started_at"], job["created_at"])
        entry = next(job for job in tool.analyze(run, mutated)["jobs"]
                     if job["name"] == victim)
        self.assertTrue(entry["ran"])
        self.assertFalse(tool.absent_by_design(entry))
        self.assertEqual(entry["metrics"]["queue_delay"]["status"], "measured")

    def test_a_job_cancelled_while_it_waited_did_not_run(self):
        # Stamped start == completion at the moment of cancellation, with a
        # real queue delay before it.  Counting the later start as dispatch
        # would publish a measured zero, which reads as a job that ran
        # instantly -- the misreading this whole rule exists to prevent.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "cancelled"
                job["completed_at"] = job["started_at"]
                job["steps"] = []
        entry = next(job for job in tool.analyze(run, mutated)["jobs"]
                     if job["name"] == victim)
        self.assertFalse(entry["ran"])
        self.assertTrue(tool.absent_by_design(entry))
        self.assertIsNone(entry["metrics"]["job_wall"]["seconds"])

    def test_a_step_still_running_is_evidence_the_job_ran(self):
        # Cancelled mid-step: the step has a start and no completion, which
        # is exactly the shape a cancellation leaves behind.  Requiring both
        # stamps would discard the only evidence there is.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "cancelled"
                job["started_at"] = None
                first = job["steps"][0]
                job["steps"] = [dict(first, status="in_progress",
                                     conclusion=None, completed_at=None)]
        entry = next(job for job in tool.analyze(run, mutated)["jobs"]
                     if job["name"] == victim)
        self.assertTrue(entry["ran"])
        self.assertIn("its first step ran at",
                      entry["metrics"]["job_wall"]["reason"])

    def test_a_step_with_only_a_completion_is_not_evidence(self):
        # No start stamp to cite, so nothing says when the job began.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "cancelled"
                job["completed_at"] = job["started_at"]
                job["steps"] = [dict(job["steps"][0], started_at=None)]
        entry = next(job for job in tool.analyze(run, mutated)["jobs"]
                     if job["name"] == victim)
        self.assertFalse(entry["ran"])

    def test_a_skipped_or_backwards_step_is_not_evidence(self):
        # A skipped step is stamped started == completed and never ran, and
        # a step whose completion precedes its start has stamps the same
        # report calls unusable.  Citing either would publish a time the
        # report disowns two sections later.
        _, jobs = fixture("run-success")
        template = next(job["steps"][0] for job in jobs["jobs"]
                        if job["name"] == "Sanitizers / ubuntu-latest / clang")
        self.assertIsNone(tool.first_step_start(
            {"steps": [dict(template, conclusion="skipped")]}))
        self.assertIsNone(tool.first_step_start(
            {"steps": [dict(template, started_at="2026-09-10T19:30:00Z",
                            completed_at="2026-09-10T19:20:00Z")]}))
        self.assertEqual(tool.first_step_start({"steps": [template]}),
                         template["started_at"])

    def test_the_earliest_step_is_the_one_reported(self):
        _, jobs = fixture("run-success")
        steps = next(job["steps"] for job in jobs["jobs"]
                     if job["name"] == "Sanitizers / ubuntu-latest / clang")
        self.assertGreater(len(steps), 1)
        timed = [step["started_at"] for step in steps
                 if step.get("started_at") and step.get("completed_at")
                 and step.get("conclusion") != "skipped"]
        self.assertEqual(
            tool.first_step_start({"steps": list(reversed(steps))}),
            min(timed))

    def test_a_cancelled_job_that_never_ran_is_not_saved_by_a_dead_step(self):
        # The mirror: one step carrying no usable stamps is not evidence the
        # runner picked the job up, and taking it as evidence prints zeroes
        # under a caption saying the job has no durations.
        run, jobs = fixture("run-skip-cascade")
        mutated = copy.deepcopy(jobs)
        victim = None
        for job in mutated["jobs"]:
            if job["conclusion"] == "skipped" and not (job.get("steps") or []):
                victim = job["name"]
                job["conclusion"] = "cancelled"
                job["steps"] = [{"name": "Set up job", "number": 1,
                                 "status": "queued", "conclusion": None,
                                 "started_at": None, "completed_at": None}]
                break
        self.assertIsNotNone(victim)
        entry = next(job for job in tool.analyze(run, mutated)["jobs"]
                     if job["name"] == victim)
        self.assertFalse(entry["ran"])
        self.assertTrue(tool.absent_by_design(entry))

    def test_a_report_without_the_ran_flag_is_not_excused(self):
        # A report written before the flag existed carries no answer.  The
        # safe reading is that the job ran, so its cells are explained rather
        # than silently dropped.
        report = tool.analyze(*fixture("run-success"))
        entry = copy.deepcopy(report["jobs"][0])
        entry.pop("ran")
        self.assertFalse(tool.absent_by_design(entry))

    def test_a_cancelled_job_stamped_the_way_a_skipped_one_is_reports_nothing(
            self):
        # The shipped skip cascade shows what GitHub does to a job that did
        # not run: started_at equal to created_at, and a completion equal to
        # them or one second earlier.  Flip one such job's conclusion to
        # cancelled and nothing else, and the old rule printed 0 | 0 | 0
        # under a caption saying it has no durations.
        run, jobs = fixture("run-skip-cascade")
        mutated = copy.deepcopy(jobs)
        victim = None
        for job in mutated["jobs"]:
            if job["conclusion"] == "skipped" and not (job.get("steps") or []):
                victim = job["name"]
                self.assertEqual(job["started_at"], job["created_at"])
                job["conclusion"] = "cancelled"
                break
        self.assertIsNotNone(victim)
        report = tool.analyze(run, mutated)
        entry = next(job for job in report["jobs"] if job["name"] == victim)
        self.assertTrue(tool.absent_by_design(entry))
        self.assertIn("cancelled before it ran",
                      entry["metrics"]["job_wall"]["reason"])
        row = next(line for line in tool.render_markdown(report).splitlines()
                   if line.startswith(f"| {victim} |"))
        cells = [part.strip() for part in row.strip().strip("|").split("|")][2:]
        self.assertEqual([value for value in cells if value.isdigit()], [],
                         row)

    def test_a_cancelled_job_that_ran_still_qualifies_the_critical_path(self):
        # The predicate has three call sites and the block is only one.  A
        # job that ran and lost its wall time must still make the path say
        # so, or a total gets published as final with that job missing.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "cancelled"
                job["started_at"] = None       # ran; the stamp is gone
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        path = tool.analyze(run, mutated, edges, drift)["critical_path"]["graph"]
        self.assertEqual(path["status"], "partial")
        self.assertIn(victim, path["evidence"]["untimed_jobs_off_chain"])

    def test_a_cancelled_job_whose_runner_picked_it_up_is_not_excused(self):
        # Steps are the evidence that the runner executed the job.  A
        # cancellation that landed after that point leaves real durations,
        # and the caption does not cover them.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "cancelled"
                self.assertTrue(job["steps"])
        entry = next(job for job in tool.analyze(run, mutated)["jobs"]
                     if job["name"] == victim)
        self.assertFalse(tool.absent_by_design(entry))
        self.assertEqual(entry["metrics"]["job_wall"]["status"], "measured")

    def test_a_cancelled_job_that_lost_a_stamp_is_not_excused(self):
        # A start stamp with no completion is a job that ran and lost a
        # stamp, not one that never ran.  Excusing it prints seconds in its
        # own row under a caption saying it has none, and drops its
        # degraded cells from the block.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "cancelled"
                job["completed_at"] = None
        report = tool.analyze(run, mutated)
        entry = next(job for job in report["jobs"]
                     if job["name"] == victim)
        self.assertFalse(tool.absent_by_design(entry))
        self.assertEqual(entry["metrics"]["queue_delay"]["status"], "measured")
        block = tool.render_markdown(report).split(
            "Why a number above is not final")[-1]
        self.assertIn(f"{victim} / job_wall", block)

    def test_a_cancelled_job_whose_steps_ran_is_not_excused(self):
        # The mirror case: no start stamp, but its steps carry real spans,
        # so phase seconds are printed for a job the caption would excuse.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "cancelled"
                job["started_at"] = None
        report = tool.analyze(run, mutated)
        entry = next(job for job in report["jobs"]
                     if job["name"] == victim)
        self.assertEqual(entry["phases"]["compile"]["status"], "measured")
        self.assertFalse(tool.absent_by_design(entry))

    def test_a_completed_job_with_no_completion_time_says_so(self):
        # "job has not completed" contradicts the job's own status.  It
        # completed; what it did not do is say when.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        mutated["jobs"][0]["completed_at"] = None
        report = tool.analyze(run, mutated)
        self.assertEqual(report["jobs"][0]["metrics"]["job_wall"]["reason"],
                         "job reported no completion time")

    def test_a_started_job_in_a_finished_run_is_not_told_it_may_finish(self):
        # The start stamp is present, so `interval` reports the absent end.
        # The existing tests all null the start, so this half went unseen.
        run, jobs = fixture("run-queued")
        finished = copy.deepcopy(run)
        finished["status"] = "completed"
        mutated = copy.deepcopy(jobs)
        started = [job for job in mutated["jobs"]
                   if job["status"] == "in_progress" and job["started_at"]]
        self.assertTrue(started)
        for job in started:
            job["completed_at"] = None
        for job in tool.analyze(finished, mutated)["jobs"]:
            if job["status"] == "in_progress" and job["started_at"]:
                self.assertEqual(
                    job["metrics"]["job_wall"]["reason"],
                    "job stopped without reporting a completion time")

    def test_a_started_job_with_an_unreadable_status_says_which(self):
        run, jobs = fixture("run-queued")
        finished = copy.deepcopy(run)
        finished["status"] = "completed"
        mutated = copy.deepcopy(jobs)
        victim = None
        for job in mutated["jobs"]:
            if job["status"] == "in_progress" and job["started_at"]:
                victim = job["name"]
                job["status"] = "zzz"
                job["completed_at"] = None
                break
        self.assertIsNotNone(victim)
        entry = next(job for job in tool.analyze(finished, mutated)["jobs"]
                     if job["name"] == victim)
        self.assertIn("is not one this tool recognises",
                      entry["metrics"]["job_wall"]["reason"])

    def test_an_unreadable_status_is_not_told_it_never_started(self):
        # The loud branch is right, but the sentence should not assert a
        # fact about a state the tool has just said it cannot read.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        mutated["jobs"][0]["status"] = "something-new"
        mutated["jobs"][0]["started_at"] = None
        reason = tool.analyze(run, mutated)["jobs"][0]["metrics"]["job_wall"][
            "reason"]
        self.assertIn("not one this tool recognises", reason)
        self.assertNotIn("never started", reason)

    def test_a_finished_job_in_a_live_run_is_not_told_it_may_still_start(self):
        # The run is going, but this job is done.  Only the job's own status
        # decides whether its start may still arrive.
        run, jobs = fixture("run-success")
        live = copy.deepcopy(run)
        live["status"] = "in_progress"
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["started_at"] = None
                job["steps"] = []      # no step evidence either
                job["conclusion"] = None
        report = tool.analyze(live, mutated)
        entry = next(job for job in report["jobs"] if job["name"] == victim)
        self.assertEqual(entry["status"], "completed")
        self.assertEqual(entry["metrics"]["job_wall"]["reason"],
                         "job never started")
        # And the walkers must file it as missing data, not as work still
        # going, even though the run around it is going.
        running, untimed = tool.untimed_executed_jobs(
            report["jobs"], run_status="in_progress")
        self.assertIn(victim, untimed)
        self.assertNotIn(victim, running)

    def test_a_job_whose_steps_ran_is_not_told_it_never_started(self):
        # The job-level start stamp is gone but its own steps are stamped
        # and their seconds are printed two columns away.  "Never started"
        # is refuted by the same row that carries it.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["started_at"] = None
                self.assertTrue(job["steps"])
        entry = next(job for job in tool.analyze(run, mutated)["jobs"]
                     if job["name"] == victim)
        reason = entry["metrics"]["job_wall"]["reason"]
        self.assertIn("its first step ran at", reason)
        self.assertNotIn("never started", reason)
        self.assertEqual(entry["phases"]["compile"]["status"], "measured")

    def test_a_job_still_queued_has_not_never_started(self):
        # "never" is a finality claim.  A job waiting in the queue may start
        # at any moment, and a reader who checks finds the tool overstating.
        run, jobs = fixture("run-queued")
        report = tool.analyze(run, jobs)
        waiting = [job for job in report["jobs"]
                   if job["status"] in tool.IN_FLIGHT_STATUSES
                   and job["started_at"] is None]
        self.assertTrue(waiting)
        for job in waiting:
            self.assertEqual(job["metrics"]["job_wall"]["reason"],
                             "job has not started yet")
        # A job that did start and has not finished is a different fact, and
        # the running fixture carries one.
        running = [job for job in report["jobs"]
                   if job["status"] == "in_progress" and job["started_at"]]
        self.assertTrue(running)
        for job in running:
            self.assertEqual(job["metrics"]["job_wall"]["reason"],
                             "job has not completed")

    def _held_for_approval(self, run_status):
        run, jobs = fixture("run-success")
        run = copy.deepcopy(run)
        run["status"] = run_status
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            if job["name"] == "Sanitizers / ubuntu-latest / clang":
                job["status"] = "waiting"
                job["conclusion"] = None
                job["started_at"] = None
                job["completed_at"] = None
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        return tool.analyze(run, mutated, edges, drift)["critical_path"]

    def test_a_job_held_for_approval_is_not_missing_data(self):
        # `waiting` is an ordinary job at an environment protection rule.
        # Treating it as missing data would fire on every approval gate.
        paths = self._held_for_approval("in_progress")
        # Both walkers, because each asks the question separately and a
        # one-sided assertion let one of them drift back once already.
        for key in ("observed", "graph"):
            evidence = paths[key]["evidence"]
            self.assertIn("Sanitizers / ubuntu-latest / clang",
                          evidence["unfinished_jobs_off_chain"], key)
            self.assertNotIn("untimed_jobs_off_chain", evidence, key)

    def test_a_job_still_waiting_in_a_finished_run_is_missing_data(self):
        # The same status in a run the API calls completed describes a job
        # that stopped without saying so.  Quietly calling it work in
        # progress would promise a number that is never coming.
        paths = self._held_for_approval("completed")
        for key in ("observed", "graph"):
            evidence = paths[key]["evidence"]
            self.assertIn("Sanitizers / ubuntu-latest / clang",
                          evidence["untimed_jobs_off_chain"], key)
            self.assertNotIn("unfinished_jobs_off_chain", evidence, key)

    def test_an_unreadable_run_status_reaches_the_failure_latency_too(self):
        # The same sentence class one metric over: saying no failure has
        # happened *yet* asserts the run is going, which the tool declines
        # to say about a status it cannot read.
        run, jobs = fixture("run-success")
        unreadable = copy.deepcopy(run)
        unreadable["status"] = "something-new"
        metric = tool.analyze(unreadable, jobs)["metrics"][
            "first_failure_latency"]
        self.assertEqual(metric["status"], "unavailable")
        self.assertIn("is not one this tool recognises", metric["reason"])
        self.assertNotIn("still in progress", metric["reason"])

    def test_an_unreadable_run_status_still_downgrades_the_run_totals(self):
        # The two questions about the run pull in opposite directions and
        # both must err loud.  A status the tool cannot read might mean the
        # run is unfinished, so the totals become lower bounds; it is not
        # evidence a stalled job will move, so the walkers call it stopped.
        run, jobs = fixture("run-success")
        unreadable = copy.deepcopy(run)
        unreadable["status"] = "something-new"
        report = tool.analyze(unreadable, jobs)
        for key in ("run_wall_clock", "pr_feedback"):
            metric = report["metrics"][key]
            self.assertEqual(metric["status"], "partial", key)
            self.assertIn("may not be final", metric["reason"], key)
            # ...and it says which of the two it is, rather than claiming a
            # run is in progress two functions after declining to say so.
            self.assertNotIn("still in progress", metric["reason"], key)

    def test_a_run_status_the_tool_cannot_read_is_not_read_as_running(self):
        # An unreadable run status is no evidence that a stalled job will
        # ever move.  Reporting it as work in progress promises a number
        # nobody can wait for.
        paths = self._held_for_approval("something-new")
        for key in ("observed", "graph"):
            self.assertIn("Sanitizers / ubuntu-latest / clang",
                          paths[key]["evidence"]["untimed_jobs_off_chain"],
                          key)

    def test_the_in_flight_statuses_are_the_ones_github_documents(self):
        # Looping over the constant would shrink with it.  The list is a
        # claim about GitHub's job status enum, so assert the claim.
        self.assertEqual(tool.IN_FLIGHT_STATUSES,
                         ("queued", "in_progress", "waiting", "requested",
                          "pending"))
        # And the readable sets, which decide whether a sentence about a
        # job's or a run's future is settled.  Widening either by accident
        # would make the tool assert where it should hedge.
        self.assertEqual(tool.KNOWN_JOB_STATUSES,
                         tool.IN_FLIGHT_STATUSES + ("completed",))
        self.assertEqual(tool.KNOWN_RUN_STATUSES, tool.KNOWN_JOB_STATUSES)
        self.assertEqual(tool.CONCLUSIONS_THAT_RAN,
                         ("success", "failure", "timed_out"))

    def test_every_in_flight_status_takes_the_quiet_branch(self):
        # Dropping any one of them would report an ordinary live job as
        # missing data, and only `waiting` was pinned before.
        for status in tool.IN_FLIGHT_STATUSES:
            with self.subTest(status=status):
                run, jobs = fixture("run-success")
                run = copy.deepcopy(run)
                run["status"] = "in_progress"
                mutated = copy.deepcopy(jobs)
                for job in mutated["jobs"]:
                    if job["name"] == "Sanitizers / ubuntu-latest / clang":
                        job["status"] = status
                        job["conclusion"] = None
                        job["started_at"] = None
                        job["completed_at"] = None
                running, untimed = tool.untimed_executed_jobs(
                    tool.analyze(run, mutated)["jobs"], run_status="in_progress")
                self.assertIn("Sanitizers / ubuntu-latest / clang", running)
                self.assertEqual(untimed, [])

    def test_a_job_that_completed_without_a_start_is_not_called_unfinished(self):
        # run-missing-data ships exactly this shape: status completed, a
        # completion stamp, no start.  Saying it has not completed puts two
        # contradictory sentences in one report, and the reader who checks
        # finds the tool wrong, which costs more than saying nothing.
        run, jobs = fixture("run-missing-data")
        report = tool.analyze(run, jobs)
        victim = next(job for job in report["jobs"]
                      if job["started_at"] is None)
        self.assertEqual(victim["status"], "completed")
        # It concluded successfully, so it ran; the report says which fact
        # is missing rather than claiming the job never started.
        self.assertEqual(victim["metrics"]["job_wall"]["reason"],
                         "job reported no start stamp; it concluded 'success'")
        self.assertNotIn("job has not completed", tool.render_markdown(report))
        self.assertNotIn("job never started", tool.render_markdown(report))

    def test_a_job_that_failed_without_starting_is_still_missing_data(self):
        # Failure is an outcome, not an excuse from having a duration.  A job
        # that failed with no start stamp is data the run lost, and widening
        # the excused list to cover it would hide exactly that.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            if job["name"] == "Sanitizers / ubuntu-latest / clang":
                job["conclusion"] = "failure"
                job["started_at"] = None
                # No step evidence, so only the conclusion check can keep
                # this job out of the excused set.
                job["steps"] = []
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        path = tool.analyze(run, mutated, edges, drift)["critical_path"]["graph"]
        self.assertIn("Sanitizers / ubuntu-latest / clang",
                      path["evidence"]["untimed_jobs_off_chain"])

    @staticmethod
    def _cancelled_before_starting(jobs, name):
        """The shape GitHub actually produces: the job is completed and
        cancelled, it has no start stamp, and it ran no steps.  Nulling the
        start alone would leave step durations behind, which describes a job
        that ran and lost a stamp -- a different thing entirely."""
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            if job["name"] == name:
                job["status"] = "completed"
                job["conclusion"] = "cancelled"
                job["started_at"] = None
                job["steps"] = []
        return mutated

    def test_a_job_cancelled_before_it_started_is_not_missing_data(self):
        # It has no duration because it never ran, like a skipped job, and it
        # will never acquire one.  Both alarms would be false.
        run, jobs = fixture("run-success")
        mutated = self._cancelled_before_starting(
            jobs, "Sanitizers / ubuntu-latest / clang")
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        paths = tool.analyze(run, mutated, edges, drift)["critical_path"]
        for key in ("observed", "graph"):
            evidence = paths[key]["evidence"]
            self.assertNotIn("untimed_jobs_off_chain", evidence)
            self.assertNotIn("unfinished_jobs_off_chain", evidence)

    def test_a_status_the_tool_does_not_know_takes_the_loud_branch(self):
        # An unreadable status is data the tool cannot interpret.  Presenting
        # it as an ordinary running job would be a guess in the quiet
        # direction, which is the one that understates.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            if job["name"] == "Sanitizers / ubuntu-latest / clang":
                job["status"] = "something-new"
                job["started_at"] = None
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        path = tool.analyze(run, mutated, edges, drift)["critical_path"]["graph"]
        self.assertIn("Sanitizers / ubuntu-latest / clang",
                      path["evidence"]["untimed_jobs_off_chain"])

    def test_a_finished_job_with_no_duration_is_still_missing_data(self):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            if job["name"] == "Sanitizers / ubuntu-latest / clang":
                job["started_at"] = None    # completed, never started
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        path = tool.analyze(run, mutated, edges, drift)["critical_path"]["graph"]
        self.assertIn("reported no duration", path["reason"])
        self.assertNotIn("have not finished", path["reason"])

    def test_the_observed_reason_names_both_ways_it_can_be_wrong(self):
        # The walk selects by completion timestamp, so an untimed job can
        # either sit above the start it chose or sit on a branch it never
        # entered.  Naming only one leaves the other unstated.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            if job["name"] == "TSan / ubuntu-latest / clang":
                job["started_at"] = None
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        reason = tool.analyze(run, mutated, edges,
                              drift)["critical_path"]["observed"]["reason"]
        # The opening clause matters as much as the tail: the bucket holds
        # jobs whose status could not be read, which are not finished.
        self.assertIn("reported no duration", reason)
        self.assertNotIn("finished job", reason)
        self.assertIn("below the true end of the path", reason)
        self.assertIn("longer branch it never entered", reason)

    def test_a_job_is_not_told_the_critical_path_cannot_be_walked(self):
        # Read against one job, that sentence reverses cause and effect: the
        # latency is unknown because the job's dependencies are, not because
        # a path elsewhere could not be walked.
        report = tool.analyze(*fixture("run-success"))
        metric = report["jobs"][0]["metrics"]["scheduler_release_latency"]
        self.assertEqual(metric["status"], "unavailable")
        self.assertIn("dependencies are unknown", metric["reason"])
        self.assertNotIn("critical path", metric["reason"])

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

    def test_the_aggregate_note_covers_every_job_that_did_not_run(self):
        # The per-job caption was widened to cover cancelled-before-run
        # jobs; the aggregate note describes the same exclusion and has to
        # say the same thing.
        doc = tool.aggregate_reports(self._reports())
        text = tool.render_aggregate_markdown(doc)
        self.assertIn("a job that did not run contributes to none", text)

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
            (out / "raw" / f"run-{run['id']}.json").write_text(
                json.dumps(run), encoding="utf-8")
            (out / "raw" / f"run-{run['id']}-jobs.json").write_text(
                json.dumps(jobs), encoding="utf-8")
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

    def test_the_markdown_says_why_a_number_is_not_final(self):
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"]:
            if job["name"] == "Sanitizers / ubuntu-latest / clang":
                job["started_at"] = None
        graph = tool.extract_graph(self.WORKFLOW)
        edges, drift = tool.match_graph_to_jobs(
            graph, [j["name"] for j in mutated["jobs"]])
        text = tool.render_markdown(
            tool.analyze(run, mutated, edges, drift))
        # A "(partial)" cell with no reason beside it is a flag a reader
        # learns to ignore.  The whole point of the degradation is the cause.
        self.assertIn("Why a number above is not final", text)
        self.assertIn("routed around", text)
        self.assertIn("critical_path.graph", text)

    @staticmethod
    def _row_values(text, job_name):
        """One job's printed cells, read back out of the rendered table."""
        table = text.split("Per job, seconds")[1]
        headings = [part.strip() for part in
                    table.splitlines()[2].strip().strip("|").split("|")][2:]
        for line in table.splitlines():
            if line.startswith(f"| {job_name} |"):
                cells = [part.strip() for part in
                         line.strip().strip("|").split("|")][2:]
                # zip would silently drop a column the header does not
                # declare, which is the drift this whole reading exists to
                # catch.
                if len(cells) != len(headings):
                    raise AssertionError(
                        f"{job_name}: {len(cells)} cells under "
                        f"{len(headings)} headings")
                return dict(zip(headings, cells))
        raise AssertionError(f"no row for {job_name}")

    def _degraded_cells(self, report, text):
        """Every per-job cell the rendered table shows as not a measurement.

        Read out of the markdown rather than out of `job_cells`, because
        `job_cells` is what is under test: deriving the expectation from it
        would prove only that the block covers itself."""
        found = []
        for job in report["jobs"]:
            if job["conclusion"] == "skipped":
                continue
            for heading, value in self._row_values(text, job["name"]).items():
                if not value.isdigit():
                    found.append((job["name"], heading))
        return found

    def test_every_degraded_cell_in_the_table_is_explained(self):
        # The block exists to stop `(partial)` being a flag with no cause.
        # Explaining a metric that has no column while leaving the printed
        # ones bare would be the same failure wearing a longer report.
        graph = tool.extract_graph(self.WORKFLOW)
        for name in ("run-success", "run-skip-cascade", "run-clock-skew",
                     "run-queued", "run-missing-data", "run-rerun"):
            run, jobs = fixture(name)
            edges, drift = tool.match_graph_to_jobs(
                graph, [j["name"] for j in jobs["jobs"]])
            for arguments in ((run, jobs), (run, jobs, edges, drift)):
                report = tool.analyze(*arguments)
                text = tool.render_markdown(report)
                block = text.split("Why a number above is not final")[-1]
                for job_name, heading in self._degraded_cells(report, text):
                    column = dict((head, key) for key, _, head
                                  in tool.JOB_COLUMNS)[heading]
                    self.assertTrue(
                        f"{job_name} / {column}" in block
                        or f"`{column}` on " in block,
                        f"{name}: nothing explains {job_name} / {heading}")

    def test_the_printed_row_is_exactly_what_the_block_reasons_about(self):
        # The table and the block are built from one declaration precisely so
        # they cannot drift.  Pin that against the rendered text: add a
        # seventh cell to the row by hand and this fails, which is the only
        # way the block can silently stop covering a column.
        run, jobs = fixture("run-success")
        report = tool.analyze(run, jobs)
        text = tool.render_markdown(report)
        for job in report["jobs"]:
            printed = self._row_values(text, job["name"])
            expected = {heading: tool.cell(metric)
                        for (_, _, heading), (_, metric)
                        in zip(tool.JOB_COLUMNS, tool.job_cells(job))}
            self.assertEqual(printed, expected)
        self.assertEqual(len(tool.JOB_COLUMNS), len(tool.job_cells(
            report["jobs"][0])))

    def test_a_reason_spanning_more_than_one_column_is_not_collapsed(self):
        # "4 values are anomalous" names neither a job nor a number.  Four
        # specific lines are longer and worth more.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        for job in mutated["jobs"][:2]:
            job["created_at"] = "2025-01-01T00:01:00Z"
            job["started_at"] = "2025-01-01T00:00:00Z"
            job["completed_at"] = "2024-12-31T23:59:00Z"
        text = tool.render_markdown(tool.analyze(run, mutated))
        block = text.split("Why a number above is not final")[-1]
        self.assertNotIn("values are anomalous", block)
        for name in (job["name"] for job in mutated["jobs"][:2]):
            self.assertIn(f"`{name} / queue_delay` is anomalous", block)
            self.assertIn(f"`{name} / job_wall` is anomalous", block)

    def test_a_degraded_number_with_no_recorded_cause_still_appears(self):
        # The block's whole purpose is that no degraded cell is bare.  A
        # metric whose reason went missing must say that, not vanish.
        run, jobs = fixture("run-success")
        report = tool.analyze(run, jobs)
        report["jobs"][0]["metrics"]["job_wall"] = {
            "seconds": 3, "status": "partial", "reason": None, "evidence": {}}
        text = tool.render_markdown(report)
        self.assertIn("no cause was recorded", text)

    def test_a_small_group_is_named_job_by_job_not_counted(self):
        # The collapse threshold is a legibility tradeoff, not an invariant.
        # Pin both sides of it so it cannot be retuned unnoticed.
        run, jobs = fixture("run-missing-data")
        text = tool.render_markdown(tool.analyze(run, jobs))
        # Three jobs share the configure reason, which is the threshold, so
        # each is still named.  Five would collapse; run-clock-skew shows it.
        self.assertEqual(text.count("no step matched phase configure"), 3)
        self.assertNotIn("on 3 jobs is", text)
        skewed, skewed_jobs = fixture("run-clock-skew")
        wide = tool.render_markdown(tool.analyze(skewed, skewed_jobs))
        self.assertIn("`configure` on 5 jobs is", wide)
        self.assertEqual(wide.count("no step matched phase configure"), 1)
        # Four is the other side of the boundary.  Without it the threshold
        # is pinned only to the interval [3, 4] and can be retuned unseen.
        four = copy.deepcopy(skewed_jobs)
        first = four["jobs"][0]
        first["steps"].append({
            "name": "Configure", "number": 99, "status": "completed",
            "conclusion": "success", "started_at": first["started_at"],
            "completed_at": first["started_at"]})
        narrow = tool.render_markdown(tool.analyze(skewed, four))
        self.assertIn("`configure` on 4 jobs is", narrow)

    def test_the_block_never_explains_a_metric_with_no_column(self):
        # scheduler_release_latency and upstream_elapsed are computed and
        # published in the JSON, but the markdown prints neither, so a line
        # about them explains a number the reader cannot see.
        run, jobs = fixture("run-success")
        report = tool.analyze(run, jobs)
        block = "\n".join(
            tool.degradation_block(report, report["critical_path"]))
        for hidden in ("scheduler_release_latency", "upstream_elapsed"):
            self.assertNotIn(hidden, block)

    def test_the_published_columns_are_the_ones_declared(self):
        # Header, row and block agreeing with each other says nothing about
        # which columns exist.  Dropping one shrinks the published report and
        # every consistency check still passes, so pin the set itself.
        self.assertEqual(
            tuple(heading for _, _, heading in tool.JOB_COLUMNS),
            ("queue", "wall", "configure", "compile", "tests", "unaccounted"))
        text = tool.render_markdown(tool.analyze(*fixture("run-success")))
        self.assertIn(
            "| job | population | queue | wall | configure | compile | "
            "tests | unaccounted |", text)

    def test_a_job_cancelled_after_it_ran_is_not_excused(self):
        # It has real durations, so the caption's excuse does not cover it
        # and its degraded cells still need explaining.  Excusing it also
        # publishes a count a reader can check against the table and find
        # one short.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["conclusion"] = "cancelled"
                job["steps"] = [step for step in job["steps"]
                                if tool.classify_step(step["name"])
                                != "compile"]
        report = tool.analyze(run, mutated)
        self.assertEqual(
            report["jobs"][[j["name"] for j in report["jobs"]].index(victim)]
            ["metrics"]["job_wall"]["status"], "measured")
        text = tool.render_markdown(report)
        block = text.split("Why a number above is not final")[-1]
        self.assertIn("`compile` on 6 jobs is", block)

    def test_a_job_excused_from_durations_is_excused_from_the_block(self):
        # A job cancelled before it ran has no durations for the same reason
        # a skipped one does not.  Six true bullets per such job crowd out
        # the ones that are not absent by design.
        run, jobs = fixture("run-success")
        mutated = copy.deepcopy(jobs)
        victim = "Sanitizers / ubuntu-latest / clang"
        for job in mutated["jobs"]:
            if job["name"] == victim:
                job["status"] = "completed"
                job["conclusion"] = "cancelled"
                job["started_at"] = None
                job["steps"] = []
        text = tool.render_markdown(tool.analyze(run, mutated))
        block = text.split("Why a number above is not final")[-1]
        self.assertNotIn(victim, block)
        self.assertIn("1 cancelled before running", text)

    def test_the_block_follows_the_table_it_explains(self):
        text = tool.render_markdown(tool.analyze(*fixture("run-success")))
        self.assertLess(text.index("Per job, seconds"),
                        text.index("Why a number above is not final"))

    def test_one_reason_repeated_across_jobs_collapses_to_one_line(self):
        run, jobs = fixture("run-success")
        text = tool.render_markdown(tool.analyze(run, jobs))
        # Eighteen jobs, five without a configure step.  Five identical
        # sentences would bury every line that is not repeated.
        self.assertIn("`configure` on 5 jobs is unavailable", text)
        self.assertEqual(text.count("no step matched phase configure"), 1)

    def test_the_block_carries_the_run_level_rows_too(self):
        run, jobs = fixture("run-success")
        text = tool.render_markdown(tool.analyze(run, jobs))
        self.assertIn("`total_required_check_completion` is unavailable", text)
        self.assertIn("`first_failure_latency` is unavailable", text)

    def test_the_markdown_does_not_repeat_itself_once_per_skipped_job(self):
        text = tool.render_markdown(tool.analyze(*fixture("run-skip-cascade")))
        self.assertEqual(text.count("job skipped; no execution"), 0)

    def test_every_file_the_tool_reads_or_writes_names_its_encoding(self):
        # Job names come straight from the API and are not ASCII by rule, so
        # a text call that takes the locale encoding decodes correctly on the
        # developer's machine and raises on a runner with a different one.
        # Asserting the discipline is deterministic; asserting the symptom
        # would need a non-UTF-8 locale the test host may not have.
        import pathlib
        read_text, write_text = pathlib.Path.read_text, pathlib.Path.write_text
        fetch_run = tool.fetch_run
        seen = []

        def record(kind, original):
            def wrapper(self, *args, **kwargs):
                seen.append((kind, str(self), kwargs.get("encoding")))
                return original(self, *args, **kwargs)
            return wrapper

        run, jobs = fixture("run-success")
        jobs = copy.deepcopy(jobs)
        jobs["jobs"][0]["name"] = "Docs / política ✔ validation"
        with tempfile.TemporaryDirectory() as tmp:
            try:
                pathlib.Path.read_text = record("read", read_text)
                pathlib.Path.write_text = record("write", write_text)
                tool.fetch_run = lambda *a, **k: (run, jobs)
                # `fetch` writes the raw API payloads, which is where a
                # non-ASCII job name first reaches the disk.  A test that
                # skips it does not assert what it claims to.
                self.assertEqual(tool.main(
                    ["fetch", "--run-id", str(run["id"]), "--out", tmp,
                     "--repo", "o/r"]), 0)
                self.assertEqual(tool.main(
                    ["report", "--run-id", str(run["id"]), "--out", tmp]), 0)
                self.assertEqual(tool.main(
                    ["aggregate", "--out", tmp, "--label", "b"]), 0)
            finally:
                pathlib.Path.read_text = read_text
                pathlib.Path.write_text = write_text
                tool.fetch_run = fetch_run
        kinds = {kind for kind, _, _ in seen}
        self.assertEqual(kinds, {"read", "write"}, seen)
        written = {name for kind, name, _ in seen if kind == "write"}
        self.assertTrue(any(name.endswith(".md") for name in written), written)
        self.assertTrue(any("raw" in name for name in written), written)
        self.assertEqual([entry for entry in seen if entry[2] != "utf-8"], [])

    def test_report_command_writes_both_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / "raw").mkdir()
            run, jobs = fixture("run-success")
            (out / "raw" / f"run-{run['id']}.json").write_text(
                json.dumps(run), encoding="utf-8")
            (out / "raw" / f"run-{run['id']}-jobs.json").write_text(
                json.dumps(jobs), encoding="utf-8")
            self.assertEqual(
                tool.main(["report", "--run-id", str(run["id"]), "--out", tmp]), 0)
            self.assertTrue((out / f"run-{run['id']}.json").exists())
            self.assertTrue((out / f"run-{run['id']}.md").exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
