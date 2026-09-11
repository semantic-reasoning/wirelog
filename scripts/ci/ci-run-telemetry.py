#!/usr/bin/env python3
"""Measure PR CI queue time and per-phase duration from run metadata (#1570).

PR #1533 exposed slow feedback that status polling could not explain, because a
pending check does not say whether its job is waiting for a runner or running.
This tool separates those two and attributes the rest of a job's wall time to
named phases, so #1571 (cache), #1572 (duplicate compilation), #1573 (early
size failure) and #1574 (job graph) argue from measurement, not guesswork.

It reads what the API already records -- `runs/{id}` and `runs/{id}/jobs` --
and is therefore an *offline analyzer*: nothing is added to a workflow.  A job
cannot observe its own run's end, so run-level completion and the critical path
are only computable after the fact; and an added step in every job would change
the durations being measured.  Publishing the report automatically from a
`workflow_run` trigger is deliberately out of scope and tracked separately, so
until that lands these numbers are available to whoever runs this script,
not to the pull request author.

What it deliberately does NOT do:

  * It is not a gate.  It never exits 77, it is never registered as a test that
    checks anything, and it never enters a workflow.  Only its self-test is
    registered with Meson.
  * It does not measure an individual gate's wall time against its own timeout;
    that is #1487 and it reads `meson-logs/testlog.json`, not the Actions API.
  * It does not change the job graph.  It measures the one that exists.

Timestamps come from two clocks.  Job-level `created_at`/`started_at`/
`completed_at` are GitHub's server clock; step timestamps are the runner's.  On
a self-hosted runner those disagree -- this repository has one whose steps
start up to ten seconds before the job the server says they belong to.  Every
duration here is therefore computed on the server clock alone; the runner
offset is reported separately and never folded into a duration.

Every metric is an object, never a bare number, and a metric that was not
measured says so with a reason instead of reporting zero:

    {"seconds": 137, "status": "measured", "reason": null, "evidence": {...}}
    {"seconds": null, "status": "unavailable", "reason": "job skipped; ..."}

`status: "measured"` requires non-empty evidence, so a phase that matched no
step reports `unavailable` rather than a zero that looks like a fast phase.
`partial` carries a value that is real but not final (a run still in flight);
`unavailable` and `anomalous` never carry one.  `clock_offset_estimate` is the
one signed quantity here and uses `offset_seconds`, so it is never mistaken for
a duration.

Usage:
  ci-run-telemetry.py fetch  --run-id ID [--run-id ID ...] [--out DIR]
                            [--max-requests N] [--repo OWNER/NAME]
  ci-run-telemetry.py report --run-id ID [--run-id ID ...] [--out DIR]

`fetch` costs two requests for a run whose jobs fit one page, plus any
retries, which are charged to the same budget.  `--out` defaults to
`ci-telemetry/`, which the repository ignores.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = "wirelog.ci-telemetry/1"
TOOL_VERSION = "1"

# A runner GitHub operates is named "GitHub Actions <n>".  Everything else is
# self-hosted.  Labels are NOT a source of truth: this repository runs a
# self-hosted runner that carries the `ubuntu-latest` label, so a population
# split on labels silently pools two different machines.
HOSTED_RUNNER_RE = re.compile(r"^GitHub Actions \d+$")

# Runner and server clocks may disagree by this much before it is reported as
# skew rather than ordinary second-granularity rounding.
CLOCK_SKEW_TOLERANCE_S = 2

# A dependent job is created when the last job it needs completes.  Allow the
# same rounding slack when checking that relation.
RELEASE_TOLERANCE_S = 2

GRAPH_PENDING = ("needs-graph extraction is not implemented in this tool "
                 "version; see #1570 unit 2")

DEFAULT_OUT_DIR = "ci-telemetry"
JOBS_PER_PAGE = 100
DEFAULT_MAX_REQUESTS = 40
RETRY_SLEEPS_S = (1, 2, 4, 8)
RETRYABLE_RE = re.compile(
    r"rate limit|secondary rate|abuse detection|HTTP 5\d\d|502|503|504",
    re.IGNORECASE,
)

# Phase attribution is by API step name, which is not the same set as the
# `- name:` values in the workflow files: the runner injects `Set up job` and
# `Complete job`, an unnamed `- uses:` step becomes `Run <action>`, and every
# action with a post phase adds `Post <name>`.  The self-test asserts this
# table against the step names in the committed fixtures, not against the YAML.
PHASE_EXACT = {
    "Set up job": "runner_setup",
    "Complete job": "post_cleanup",
    "Setup sccache": "cache_setup",
    # Ahead of the generic Install rule below: these install a specific gate's
    # tool and belong to that gate's phase, not to dependency installation.
    "Install clang-tidy (pinned major)": "tidy",
    "Install syft": "sbom",
    "Install editorconfig-checker": "lint",
    "Install uncrustify": "lint",
    "clang-tidy ratchet gate": "tidy",
    "SBOM snapshot gate": "sbom",
    "Check binary size": "binary_size",
    # tsan-native's only test step; it matches no Test* pattern.
    "Policy smoke (native threads under TSan)": "tests",
    "Path A example compile-check": "compile",
    "Run editorconfig-checker": "lint",
    "Run uncrustify (hard gate)": "lint",
    "Detect source and build-definition changes": "policy",
    "Enforce RC changelog freeze policy": "policy",
    "Validate support policy": "policy",
    "Verify enabled compile flag": "policy",
}

# Ordered: the first match wins, and `Post *` precedes everything so a post
# phase is never merged into the phase it cleans up after.
PHASE_PATTERNS = (
    (re.compile(r"^Post "), "post_cleanup"),
    (re.compile(r"^Run actions/checkout@"), "checkout"),
    (re.compile(r"^Install "), "deps_install"),
    (re.compile(r"^Configure\b"), "configure"),
    (re.compile(r"^Build\b"), "compile"),
    (re.compile(r"^Test\b"), "tests"),
)

# Steps that are real but belong to no phase: sub-second diagnostics and a
# publication step.  Listed so the coverage self-test stays exhaustive -- a new
# unmapped step name fails it instead of silently landing in `other`.
KNOWN_UNMAPPED = frozenset({
    "Record bash version",
    "Verify tool version",
    "Publish results",
})

PHASES = (
    "runner_setup", "checkout", "cache_setup", "deps_install", "configure",
    "compile", "tests", "tidy", "sbom", "binary_size", "lint", "policy",
    "post_cleanup", "other",
)

# The checks a pull request waits on.  The required set is not queryable in
# this repository (see `required_checks` in the report), so this list is the
# declared surrogate and is reported under its own name.
PINNED_CHECK_PREFIXES = (
    "Detect build-triggering changes", "Docs / policy validation",
    "RC changelog freeze gate", "lint / ", "Build / ", "Sanitizers / ",
    "mbedtls-enabled / ", "TSan / ", "TSan-native / ",
)


def die(message: str) -> int:
    print(f"ci-run-telemetry: error: {message}", file=sys.stderr)
    return 1


# ---------------------------------------------------------------------------
# Metric objects
# ---------------------------------------------------------------------------

def measured(seconds: int, evidence: dict) -> dict:
    """A value the data supports.  Evidence is mandatory: it is what stops a
    phase that matched no step from reporting a zero that reads as 'fast'.
    A negative duration is refused here rather than published, so the rule
    holds for every caller instead of at each call site."""
    if not evidence:
        raise ValueError("measured metric requires evidence")
    if seconds < 0:
        raise ValueError("measured metric cannot be negative")
    return {"seconds": seconds, "status": "measured", "reason": None,
            "evidence": evidence}


def unavailable(reason: str) -> dict:
    return {"seconds": None, "status": "unavailable", "reason": reason,
            "evidence": {}}


def anomalous(reason: str, raw_seconds: int, evidence: dict) -> dict:
    """Arithmetic the data contradicts.  The raw value is kept so the
    contradiction can be inspected, but it is never presented as a duration."""
    return {"seconds": None, "status": "anomalous", "reason": reason,
            "evidence": evidence, "raw_seconds": raw_seconds}


def parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ts_key(value, missing_last: bool = False):
    """Sort key over ISO timestamps.  An unparseable one sorts first, or last
    when @missing_last, so it never wins a min()/max() by accident."""
    parsed = parse_ts(value)
    if parsed is not None:
        return parsed
    bound = datetime.max if missing_last else datetime.min
    return bound.replace(tzinfo=timezone.utc)


def non_additive(metric: dict) -> dict:
    """Mark a value that must not be summed along a path.  `upstream_elapsed`
    includes every upstream job's own queueing, so adding it up double-counts;
    saying so in the object keeps a reader from doing it.  Set in every state,
    including unavailable, so a missing key never has to be interpreted."""
    metric["additive"] = False
    return metric


def additive(metric: dict) -> dict:
    """Mark the one per-job wait that may be summed along a path."""
    metric["additive"] = True
    return metric


def provisional(metric: dict, in_flight: bool) -> dict:
    """Downgrade a measurement taken while the run is still going.  The value
    is real but it is a lower bound, and saying so is the difference between a
    baseline and a number that quietly understates."""
    if in_flight and metric["status"] == "measured":
        metric["status"] = "partial"
        metric["reason"] = "run still in progress; value is a lower bound"
    return metric


def interval(start_iso, end_iso, missing_reason: str) -> dict:
    """Seconds between two server-clock timestamps, or why they do not yield
    one.  A negative interval is reported as anomalous, never clamped to 0."""
    start, end = parse_ts(start_iso), parse_ts(end_iso)
    if start is None or end is None:
        return unavailable(missing_reason)
    seconds = math.floor((end - start).total_seconds())
    evidence = {"from": start_iso, "to": end_iso}
    if seconds < 0:
        return anomalous("end precedes start", seconds, evidence)
    return measured(seconds, evidence)


# ---------------------------------------------------------------------------
# Per-job analysis
# ---------------------------------------------------------------------------

def classify_step(name: str) -> str:
    if name in PHASE_EXACT:
        return PHASE_EXACT[name]
    for pattern, phase in PHASE_PATTERNS:
        if pattern.search(name):
            return phase
    return "other"


def population_of(job: dict) -> str:
    runner = job.get("runner_name")
    if not runner:
        return "none"
    return "hosted" if HOSTED_RUNNER_RE.match(runner) else "self-hosted"


def is_skipped(job: dict) -> bool:
    return job.get("conclusion") == "skipped"


def job_phases(job: dict) -> tuple:
    """Phase totals plus the steps that fed each one.  Returns
    (phases, unmapped_step_names, unusable_steps, skipped_steps,
    accounted_seconds)."""
    totals: dict = {}
    evidence: dict = {}
    matched_phases = set()
    unmapped = []
    unusable = []
    skipped = []
    accounted = 0
    for step in job.get("steps") or []:
        name = step.get("name") or ""
        phase = classify_step(name)
        if phase == "other" and name not in KNOWN_UNMAPPED:
            unmapped.append(name)
        if step.get("conclusion") == "skipped":
            # A skipped step is stamped started == completed.  Counting it
            # would add a 0 to the phase and put its name in the evidence, and
            # a phase whose steps were all skipped would then report a
            # measured zero -- a phase that never ran, presented as instant.
            skipped.append({"step": name, "phase": phase})
            continue
        matched_phases.add(phase)
        start = parse_ts(step.get("started_at"))
        end = parse_ts(step.get("completed_at"))
        if start is None or end is None:
            unusable.append({"step": name, "phase": phase,
                             "reason": "step has no usable interval"})
            continue
        seconds = math.floor((end - start).total_seconds())
        if seconds < 0:
            unusable.append({"step": name, "phase": phase,
                             "reason": "step ends before it starts",
                             "raw_seconds": seconds})
            continue
        totals[phase] = totals.get(phase, 0) + seconds
        evidence.setdefault(phase, []).append(name)
        accounted += seconds
    phases = {}
    for phase in PHASES:
        if phase in totals:
            phases[phase] = measured(totals[phase],
                                     {"matched_steps": evidence[phase]})
        elif phase in matched_phases:
            # Steps did match; their timestamps were unusable.  Saying "no step
            # matched" here would be false and would hide the real problem.
            phases[phase] = unavailable(
                f"steps matched phase {phase} but carried no usable interval")
        elif any(item["phase"] == phase for item in skipped):
            phases[phase] = unavailable(
                f"every step matching phase {phase} was skipped")
        else:
            phases[phase] = unavailable(f"no step matched phase {phase}")
    return phases, unmapped, unusable, skipped, accounted


def analyze_job(job: dict, t0_iso: str, completed_by_name: dict) -> dict:
    """One job's timing.  Skipped jobs get no durations at all: GitHub stamps
    them with a completion one second before their own start, and a zero there
    would read as a job that ran instantly."""
    name = job.get("name") or ""
    entry = {
        "name": name,
        "status": job.get("status"),
        "conclusion": job.get("conclusion"),
        "population": population_of(job),
        "runner_name": job.get("runner_name"),
        "labels": job.get("labels") or [],
        "created_at": job.get("created_at"),
        "started_at": job.get("started_at"),
        "completed_at": job.get("completed_at"),
    }

    if is_skipped(job):
        reason = "job skipped; no execution"
        entry["metrics"] = {
            "upstream_elapsed": non_additive(unavailable(reason)),
            "queue_delay": unavailable(reason),
            "job_wall": unavailable(reason),
        }
        entry["phases"] = {p: unavailable(reason) for p in PHASES}
        entry["unmapped_steps"] = []
        entry["unusable_steps"] = []
        entry["skipped_steps"] = []
        entry["unaccounted"] = unavailable(reason)
        entry["clock_offset_estimate"] = {
            "offset_seconds": None, "status": "unavailable", "reason": reason,
            "evidence": {},
        }
        entry["runner_first_step_at"] = None
        return entry

    entry["metrics"] = {
        # Everything upstream of this job, including every upstream job's own
        # queueing.  Useful for ordering jobs, but NOT additive along a path.
        "upstream_elapsed": non_additive(
            interval(t0_iso, job.get("created_at"),
                     "run or job creation time missing")),
        "queue_delay": interval(job.get("created_at"), job.get("started_at"),
                                "job never started"),
        "job_wall": interval(job.get("started_at"), job.get("completed_at"),
                             "job has not completed"),
    }

    phases, unmapped, unusable, skipped, accounted = job_phases(job)
    entry["phases"] = phases
    entry["unmapped_steps"] = unmapped
    entry["unusable_steps"] = unusable
    # Normal, not anomalous: a skipped step is an `if:` doing its job.  Kept
    # out of anomalies so it cannot dilute the signals that are.
    entry["skipped_steps"] = skipped

    wall = entry["metrics"]["job_wall"]
    if wall["status"] != "measured":
        entry["unaccounted"] = unavailable("job wall time unavailable")
    elif accounted > wall["seconds"]:
        # Steps are timed by the runner, the job window by the server.  On a
        # skewed runner the steps can span more than the window, and the
        # residual is then evidence of that skew, not a duration.
        entry["unaccounted"] = anomalous(
            "accounted step time exceeds the job wall",
            wall["seconds"] - accounted,
            {"job_wall": wall["seconds"], "accounted": accounted})
    else:
        entry["unaccounted"] = measured(
            wall["seconds"] - accounted,
            {"job_wall": wall["seconds"], "accounted": accounted})

    # The runner's own first step cannot precede the runner picking the job up,
    # so a negative offset is the two clocks disagreeing, not a fast start.
    starts = [parse_ts(s.get("started_at")) for s in (job.get("steps") or [])]
    starts = [s for s in starts if s is not None]
    job_start = parse_ts(job.get("started_at"))
    if starts and job_start is not None:
        offset = math.floor((min(starts) - job_start).total_seconds())
        first_at = min(starts).isoformat().replace("+00:00", "Z")
        entry["runner_first_step_at"] = first_at
        entry["clock_offset_estimate"] = {
            "offset_seconds": offset, "status": "measured", "reason": None,
            "evidence": {"job_started_at": job.get("started_at"),
                         "first_step_started_at": first_at},
        }
    else:
        entry["runner_first_step_at"] = None
        entry["clock_offset_estimate"] = {
            "offset_seconds": None, "status": "unavailable",
            "reason": "no step start recorded", "evidence": {},
        }
    return entry


def release_latency(job: dict, jobs_by_name: dict, needs: dict) -> dict:
    """How long after its dependencies finished the job was created.  This is
    the only per-job wait that may be summed along a path."""
    name = job.get("name") or ""
    if not needs:
        return unavailable("dependency graph not supplied; see #1570 unit 2")
    if name not in needs:
        # Not the same as having no dependencies: the extractor may simply
        # have missed this job, and calling that a root would hide the gap.
        return unavailable("job absent from the supplied dependency graph")
    predecessors = needs[name]
    if not predecessors:
        return unavailable("root job; nothing released it")
    completions = []
    for dep in predecessors:
        dep_job = jobs_by_name.get(dep)
        if dep_job is None:
            return unavailable(f"dependency {dep!r} absent from the run")
        if is_skipped(dep_job):
            # GitHub materializes a whole skip cascade in one second, so the
            # timestamps carry no ordering information at all.
            return unavailable("released by skipped dependency; creation batched")
        completions.append((dep, dep_job.get("completed_at")))
    latest_name, latest_iso = max(completions,
                                  key=lambda pair: ts_key(pair[1]))
    metric = interval(latest_iso, job.get("created_at"),
                      "dependency or job creation time missing")
    if metric["status"] == "measured":
        metric["evidence"]["released_by"] = latest_name
    return metric


# ---------------------------------------------------------------------------
# Run-level analysis
# ---------------------------------------------------------------------------

def pinned_check(name: str) -> bool:
    return any(name.startswith(prefix) for prefix in PINNED_CHECK_PREFIXES)


def analyze(run: dict, jobs_doc: dict, needs: dict | None = None) -> dict:
    needs = needs or {}
    jobs = jobs_doc.get("jobs") or []
    t0_iso = run.get("run_started_at") or run.get("created_at")
    t0_source = "run_started_at" if run.get("run_started_at") else "created_at"

    jobs_by_name = {j.get("name"): j for j in jobs}
    analyzed = [analyze_job(j, t0_iso, jobs_by_name) for j in jobs]
    for entry, job in zip(analyzed, jobs):
        if is_skipped(job):
            entry["metrics"]["scheduler_release_latency"] = additive(
                unavailable("job skipped; no execution"))
        else:
            entry["metrics"]["scheduler_release_latency"] = additive(
                release_latency(job, jobs_by_name, needs))

    executed = [j for j in jobs if not is_skipped(j)]
    skipped = [j for j in jobs if is_skipped(j)]
    # Until the run ends, every run-level figure is a lower bound: a check that
    # has not reported cannot raise it, and #1574 would quote it as final.
    in_flight = run.get("status") != "completed"

    ends = [j.get("completed_at") for j in executed if j.get("completed_at")]
    if ends:
        last = max(ends, key=ts_key)
        run_wall = provisional(
            interval(t0_iso, last, "no executed job has completed"), in_flight)
    else:
        run_wall = unavailable("no executed job has completed")

    # `cancelled` is deliberately excluded: a run cancelled by concurrency did
    # not fail, and counting it would put scheduling noise in a failure metric.
    failures = [j for j in executed
                if j.get("conclusion") in ("failure", "timed_out")
                and j.get("completed_at")]
    if failures:
        first = min(failures,
                    key=lambda j: ts_key(j["completed_at"], missing_last=True))
        # Not downgraded while the run is in flight: a job still running can
        # only complete later, so it cannot produce an earlier first failure.
        first_failure = interval(t0_iso, first["completed_at"],
                                 "failing job has no completion time")
        if first_failure["status"] == "measured":
            first_failure["evidence"]["job"] = first.get("name")
            # Which step failed is what tells #1573 whether the size gate fired
            # late; the job name alone does not.  A timed-out job stamps its
            # steps `cancelled`, so an empty list there is expected and is
            # labelled rather than left looking like a job that failed with no
            # failing step.
            first_failure["evidence"]["failed_steps"] = [
                s.get("name") for s in (first.get("steps") or [])
                if s.get("conclusion") in ("failure", "timed_out")]
            first_failure["evidence"]["conclusion"] = first.get("conclusion")
    elif in_flight:
        first_failure = unavailable("run still in progress; no failure yet")
    elif any(j.get("conclusion") == "cancelled" for j in executed):
        first_failure = unavailable(
            "no job failed; cancelled jobs were seen and excluded")
    else:
        first_failure = unavailable("run has no failed job")

    pinned_ends = [j.get("completed_at") for j in executed
                   if pinned_check(j.get("name") or "") and j.get("completed_at")]
    if pinned_ends:
        last_pinned = max(pinned_ends, key=ts_key)
        pr_feedback = provisional(
            interval(t0_iso, last_pinned, "no pinned check completed"),
            in_flight)
        if pr_feedback["status"] in ("measured", "partial"):
            pr_feedback["evidence"]["source"] = "pinned-list"
            # When every executed job is a pinned check this equals
            # run_wall_clock by construction, not by corroboration.  Say so, so
            # the two are not read as independent agreeing measurements.
            pr_feedback["evidence"]["non_pinned_executed"] = sum(
                1 for j in executed if not pinned_check(j.get("name") or ""))
    else:
        pr_feedback = unavailable("no pinned check completed")

    populations: dict = {}
    for entry in analyzed:
        populations.setdefault(entry["population"], []).append(entry["name"])

    anomalies = []
    for entry in analyzed:
        offset = entry.get("clock_offset_estimate") or {}
        if (offset.get("status") == "measured"
                and abs(offset["offset_seconds"]) > CLOCK_SKEW_TOLERANCE_S):
            anomalies.append({
                "kind": "runner_clock_skew", "job": entry["name"],
                "runner_name": entry["runner_name"],
                "offset_seconds": offset["offset_seconds"],
            })
        for key, metric in entry["metrics"].items():
            if metric.get("status") == "anomalous":
                anomalies.append({
                    "kind": "negative_interval", "job": entry["name"],
                    "metric": key, "raw_seconds": metric.get("raw_seconds"),
                    "reason": metric.get("reason"),
                })
        residual = entry.get("unaccounted") or {}
        if residual.get("status") == "anomalous":
            anomalies.append({
                "kind": "steps_exceed_job_wall", "job": entry["name"],
                "raw_seconds": residual.get("raw_seconds"),
                "reason": residual.get("reason"),
            })
        for item in entry.get("unusable_steps") or []:
            anomalies.append({
                "kind": "unusable_step", "job": entry["name"],
                "step": item.get("step"), "reason": item.get("reason"),
            })

    unmapped = sorted({name for entry in analyzed
                       for name in entry["unmapped_steps"]})
    unusable = sorted({item["step"] for entry in analyzed
                       for item in entry.get("unusable_steps") or []})

    return {
        "schema": SCHEMA,
        "tool_version": TOOL_VERSION,
        "generated_at": now_iso(),
        "run_id": run.get("id"),
        "run_attempt": run.get("run_attempt"),
        "workflow": run.get("name"),
        "event": run.get("event"),
        "status": run.get("status"),
        "conclusion": run.get("conclusion"),
        "head_sha": run.get("head_sha"),
        "head_branch": run.get("head_branch"),
        "t0": t0_iso,
        "t0_source": t0_source,
        "job_counts": {"total": len(jobs), "executed": len(executed),
                       "skipped": len(skipped)},
        "populations": {k: sorted(v) for k, v in sorted(populations.items())},
        "jobs": analyzed,
        "metrics": {
            "run_wall_clock": run_wall,
            "first_failure_latency": first_failure,
            "pr_feedback": pr_feedback,
            "total_required_check_completion": unavailable(
                "no required_status_checks rule (rulesets 13129249, 13670289); "
                "branches/main/protection -> 404"),
        },
        "required_checks": {
            "source": "unavailable",
            "surrogate": "pr_feedback",
            "reason": "the required set is not queryable in this repository",
        },
        "critical_path": {
            "observed": unavailable(GRAPH_PENDING),
            "graph": unavailable(GRAPH_PENDING),
            # Not [].  An empty list here would read as "compared, no drift".
            "graph_drift": None,
        },
        "anomalies": anomalies,
        "unmapped_steps": unmapped,
        "unusable_steps": unusable,
    }


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def cell(metric: dict) -> str:
    if metric.get("status") == "measured":
        return str(metric["seconds"])
    if metric.get("status") == "partial":
        return f"{metric['seconds']} (partial)"
    return metric.get("status", "unavailable")


def render_markdown(report: dict) -> str:
    counts = report["job_counts"]
    lines = [
        f"# CI timing for run {report['run_id']} ({report['workflow']})",
        "",
        f"- head `{report['head_sha']}` on `{report['head_branch']}`, "
        f"attempt {report['run_attempt']}, conclusion `{report['conclusion']}`",
        f"- t0 `{report['t0']}` (from `{report['t0_source']}`)",
        f"- jobs: {counts['total']} ({counts['executed']} executed, "
        f"{counts['skipped']} skipped)",
        "",
        "Run-level, seconds:",
        "",
        "| metric | value |",
        "| --- | --- |",
    ]
    for key, metric in report["metrics"].items():
        lines.append(f"| {key} | {cell(metric)} |")
    lines += [
        "",
        "Per job, seconds. A skipped job has no durations at all.",
        "",
        "| job | population | queue | wall | configure | compile | tests | "
        "unaccounted |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for job in report["jobs"]:
        m, p = job["metrics"], job["phases"]
        lines.append(
            f"| {job['name']} | {job['population']} | {cell(m['queue_delay'])} "
            f"| {cell(m['job_wall'])} | {cell(p['configure'])} "
            f"| {cell(p['compile'])} | {cell(p['tests'])} "
            f"| {cell(job['unaccounted'])} |")
    if report["anomalies"]:
        lines += ["", "Anomalies:", ""]
        for item in report["anomalies"]:
            lines.append(f"- `{item['kind']}` on {item['job']}: "
                         + json.dumps({k: v for k, v in item.items()
                                       if k not in ("kind", "job")}))
    if report["unmapped_steps"]:
        lines += ["", "Steps matching no phase (their seconds land in `other`):",
                  ""]
        for name in report["unmapped_steps"]:
            lines.append(f"- {name}")
    if report["unusable_steps"]:
        lines += ["", "Steps whose own timestamps are unusable (their seconds "
                  "are in no phase):", ""]
        for name in report["unusable_steps"]:
            lines.append(f"- {name}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def run_gh(args: list) -> bytes:
    """The single point where this tool touches the network.  The self-test
    replaces it, so no test needs `gh`, a token, or a network."""
    proc = subprocess.run(["gh", *args], capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.decode("utf-8", "replace").strip())
    return proc.stdout


class Budget:
    """A hard cap on API requests, so a bad argument cannot turn this into a
    crawler.  Retries are charged too, so one logical request can cost up to
    five: the cap bounds traffic, not distinct endpoints."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.used = 0

    def spend(self) -> None:
        if self.used >= self.limit:
            raise RuntimeError(
                f"API request budget exhausted after {self.used} requests "
                f"(--max-requests {self.limit})")
        self.used += 1


def api_get(path: str, budget: Budget, transport=run_gh, sleep=time.sleep) -> dict:
    last = None
    for attempt, pause in enumerate((0, *RETRY_SLEEPS_S)):
        if pause:
            print(f"ci-run-telemetry: retrying in {pause}s ({last})",
                  file=sys.stderr)
            sleep(pause)
        budget.spend()
        try:
            return json.loads(transport(["api", path]))
        except FileNotFoundError as exc:
            raise RuntimeError(f"gh is not installed or not on PATH: {exc}")
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"gh returned a body that is not JSON: {exc}")
        except RuntimeError as exc:
            last = str(exc)
            if not RETRYABLE_RE.search(last):
                raise
    raise RuntimeError(f"giving up after {len(RETRY_SLEEPS_S)} retries: {last}")


def fetch_run(repo: str, run_id: int, budget: Budget, transport=run_gh,
              sleep=time.sleep) -> tuple:
    run = api_get(f"repos/{repo}/actions/runs/{run_id}", budget, transport, sleep)
    jobs: list = []
    page = 1
    while True:
        doc = api_get(
            f"repos/{repo}/actions/runs/{run_id}/jobs"
            f"?per_page={JOBS_PER_PAGE}&filter=latest&page={page}",
            budget, transport, sleep)
        page_jobs = doc.get("jobs") or []
        jobs.extend(page_jobs)
        # A short page is the end of pagination.  total_count is not a safe
        # stop: under filter=latest it can over-report (it counts earlier
        # attempts), and trusting it to under-report would truncate the run.
        if len(page_jobs) < JOBS_PER_PAGE:
            break
        page += 1
    return run, {"total_count": len(jobs), "jobs": jobs}


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

def raw_paths(out: Path, run_id) -> tuple:
    return (out / "raw" / f"run-{run_id}.json",
            out / "raw" / f"run-{run_id}-jobs.json")


def cmd_fetch(args) -> int:
    out = Path(args.out)
    (out / "raw").mkdir(parents=True, exist_ok=True)
    budget = Budget(args.max_requests)
    repo = args.repo or os.environ.get("GITHUB_REPOSITORY")
    if not repo:
        try:
            repo = json.loads(run_gh(["repo", "view", "--json", "nameWithOwner"]))[
                "nameWithOwner"]
        except Exception as exc:  # noqa: BLE001 - reported, not swallowed
            return die(f"could not determine the repository: {exc}")
    for run_id in args.run_id:
        try:
            run, jobs = fetch_run(repo, run_id, budget)
        except (RuntimeError, OSError) as exc:
            return die(str(exc))
        run_path, jobs_path = raw_paths(out, run_id)
        run_path.write_text(json.dumps(run, indent=2, sort_keys=True) + "\n")
        jobs_path.write_text(json.dumps(jobs, indent=2, sort_keys=True) + "\n")
        print(f"ci-run-telemetry: saved run {run_id} "
              f"({jobs['total_count']} jobs, {budget.used} requests so far)")
    return 0


def cmd_report(args) -> int:
    out = Path(args.out)
    for run_id in args.run_id:
        run_path, jobs_path = raw_paths(out, run_id)
        if not run_path.exists() or not jobs_path.exists():
            return die(f"no saved responses for run {run_id} under {out}/raw "
                       f"(run `fetch` first)")
        run = json.loads(run_path.read_text())
        if run.get("id") is not None and run.get("id") != run_id:
            return die(f"{run_path} holds run {run.get('id')}, not {run_id}; "
                       f"the saved pair does not belong together")
        report = analyze(run, json.loads(jobs_path.read_text()))
        (out / f"run-{run_id}.json").write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n")
        (out / f"run-{run_id}.md").write_text(render_markdown(report))
        counts = report["job_counts"]
        print(f"ci-run-telemetry: run {run_id}: {counts['executed']} executed, "
              f"{counts['skipped']} skipped, "
              f"{len(report['anomalies'])} anomalies")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="ci-run-telemetry.py",
        description="Measure PR CI queue time and per-phase duration (#1570).")
    sub = parser.add_subparsers(dest="command", required=True)

    fetch = sub.add_parser("fetch", help="save the API responses for a run")
    fetch.add_argument("--run-id", type=int, action="append", required=True,
                       help="workflow run id; repeat for several runs")
    fetch.add_argument("--out", default=DEFAULT_OUT_DIR,
                       help=f"output directory (default {DEFAULT_OUT_DIR}/)")
    fetch.add_argument("--repo", help="OWNER/NAME; defaults to the checkout")
    fetch.add_argument("--max-requests", type=int, default=DEFAULT_MAX_REQUESTS,
                       help="hard cap on API requests, retries included "
                            f"(default {DEFAULT_MAX_REQUESTS})")
    fetch.set_defaults(func=cmd_fetch)

    report = sub.add_parser("report", help="analyze saved responses")
    report.add_argument("--run-id", type=int, action="append", required=True,
                        help="workflow run id; repeat for several runs")
    report.add_argument("--out", default=DEFAULT_OUT_DIR,
                        help=f"output directory (default {DEFAULT_OUT_DIR}/)")
    report.set_defaults(func=cmd_report)

    args = parser.parse_args(argv)
    if getattr(args, "max_requests", 1) < 1:
        return die("--max-requests must be at least 1")
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
