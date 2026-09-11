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
                             [--workflow FILE]
  ci-run-telemetry.py aggregate [--run-id ID ...] [--out DIR] [--label NAME]

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

GRAPH_NOT_SUPPLIED = ("no dependency graph was supplied, so the critical "
                      "path cannot be walked")

DEFAULT_OUT_DIR = "ci-telemetry"
# Resolved from this file, not the working directory: the default has to hold
# when the tool runs from a build directory or anywhere else.
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_WORKFLOW = str(REPO_ROOT / ".github" / "workflows" / "ci-pr.yml")
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


def interval(start_iso, end_iso, missing_reason: str,
             no_start_reason: str = None) -> dict:
    """Seconds between two server-clock timestamps, or why they do not yield
    one.  A negative interval is reported as anomalous, never clamped to 0.

    @missing_reason names the absent end.  @no_start_reason names the absent
    start where the two are different facts, and they usually are: a job that
    completed without a start stamp is not a job that has not completed, and
    telling the reader the second is worse than telling them nothing, because
    they can check it and find it false."""
    start, end = parse_ts(start_iso), parse_ts(end_iso)
    if start is None and no_start_reason:
        return unavailable(no_start_reason)
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
            # A step that has not finished is not a broken step; saying so
            # would drown the ones that are.
            reason = ("step has not finished"
                      if step.get("status") != "completed"
                      else "step has no usable interval")
            unusable.append({"step": name, "phase": phase, "reason": reason,
                             "finished": step.get("status") == "completed"})
            continue
        seconds = math.floor((end - start).total_seconds())
        if seconds < 0:
            unusable.append({"step": name, "phase": phase,
                             "reason": "step ends before it starts",
                             "raw_seconds": seconds, "finished": True})
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
            "queue_delay": additive(unavailable(reason)),
            "job_wall": additive(unavailable(reason)),
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
        # Summable along a path, and the critical path does sum them.
        "queue_delay": additive(
            interval(job.get("created_at"), job.get("started_at"),
                     "job never started", "job has no creation time")),
        "job_wall": additive(
            interval(job.get("started_at"), job.get("completed_at"),
                     "job has not completed", "job never started")),
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
        # Not GRAPH_NOT_SUPPLIED: that sentence explains why a critical path
        # cannot be walked, and read against a job it reverses cause and
        # effect.  What is missing here is the job's own dependencies.
        return unavailable("no dependency graph was supplied, so this job's "
                           "dependencies are unknown")
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
# Dependency graph
# ---------------------------------------------------------------------------
#
# Read from the workflow files rather than hard-coded: #1574 is going to change
# this graph, and a copy in here would drift exactly when it matters.  Parsed
# by pinned regex because the repository has no YAML parser and adding one for
# a measurement instrument is not worth the dependency.

JOB_KEY_RE = re.compile(r"^  ([A-Za-z0-9_-]+):\s*$", re.MULTILINE)
NAME_RE = re.compile(r"^    name:\s*(.+?)\s*$", re.MULTILINE)
NEEDS_LIST_RE = re.compile(r"^    needs:\s*\[(.*?)\]\s*$", re.MULTILINE)
NEEDS_ONE_RE = re.compile(r"^    needs:\s*([A-Za-z0-9_-]+)\s*$", re.MULTILINE)
NEEDS_ANY_RE = re.compile(r"^    needs:", re.MULTILINE)


class WorkflowParseError(Exception):
    """A `needs:` this parser cannot read.  Refused rather than returned as an
    empty list, because an empty list means "root job" and a job wrongly called
    a root severs an edge without leaving any trace in the drift report."""
USES_LOCAL_RE = re.compile(r"^    uses:\s*(\./[^\s]+)\s*$", re.MULTILINE)
EXPRESSION_RE = re.compile(r"\$\{\{.*?\}\}")


JOBS_SECTION_RE = re.compile(r"^jobs:\s*$", re.MULTILINE)
TOP_LEVEL_KEY_RE = re.compile(r"^[A-Za-z0-9_-]+:", re.MULTILINE)


def split_jobs(text: str) -> dict:
    """Map job key to its block, within the `jobs:` section only.  Scoping
    matters: `on:` also carries two-space keys, and without this `pull_request`
    and `workflow_call` are read as jobs."""
    section = JOBS_SECTION_RE.search(text)
    if not section:
        return {}
    start = section.end()
    following = TOP_LEVEL_KEY_RE.search(text, start)
    text = text[start:following.start() if following else len(text)]
    keys = [(m.group(1), m.start()) for m in JOB_KEY_RE.finditer(text)]
    blocks = {}
    for index, (key, start) in enumerate(keys):
        end = keys[index + 1][1] if index + 1 < len(keys) else len(text)
        blocks[key] = text[start:end]
    return blocks


def parse_needs(block: str, job_key: str = "?") -> list:
    match = NEEDS_LIST_RE.search(block)
    if match:
        return [item.strip() for item in match.group(1).split(",")
                if item.strip()]
    match = NEEDS_ONE_RE.search(block)
    if match:
        return [match.group(1)]
    if NEEDS_ANY_RE.search(block):
        raise WorkflowParseError(
            f"job {job_key!r} declares needs in a form this parser cannot "
            f"read (only `needs: a` and `needs: [a, b]` on one line); "
            f"reading it as a root would sever its edges silently")
    return []


def name_pattern(display_name: str):
    """A matrix job's display name carries `${{ ... }}` placeholders; the API
    reports them expanded.  Match on the literal parts around them."""
    parts = [re.escape(part) for part in EXPRESSION_RE.split(display_name)]
    return re.compile("^" + ".+".join(parts) + "$")


def extract_graph(workflow_path) -> dict:
    """Nodes keyed by API display name, each mapping to the display names it
    needs.  A job that calls a reusable workflow is expanded into that
    workflow's jobs, named `<caller key> / <callee display name>` the way the
    API reports them: without this the `lint` node matches nothing, its edge is
    severed, and the graph misses the segment between the freeze gate and every
    build.  Every job gets an entry -- an empty list for a genuine root -- so a
    job missing from the result is a gap, never a root."""
    workflow_path = Path(workflow_path)
    text = workflow_path.read_text(encoding="utf-8")
    blocks = split_jobs(text)

    display: dict = {}
    expansion: dict = {}
    for key, block in blocks.items():
        local = USES_LOCAL_RE.search(block)
        if local:
            # removeprefix, not lstrip: lstrip takes a character set and
            # would eat the leading dot of `.github`.
            relative = local.group(1).removeprefix("./")
            callee = workflow_path.parent.parent.parent / relative
            callee_blocks = split_jobs(
                Path(callee).read_text(encoding="utf-8"))
            inner = {}
            for sub_key, sub_block in callee_blocks.items():
                sub_name = NAME_RE.search(sub_block)
                inner[sub_key] = (f"{key} / {sub_name.group(1)}"
                                  if sub_name else f"{key} / {sub_key}")
            expansion[key] = {
                "names": inner,
                "needs": {sub: parse_needs(sub_block, sub)
                          for sub, sub_block in callee_blocks.items()},
            }
            continue
        name = NAME_RE.search(block)
        display[key] = name.group(1) if name else key

    graph: dict = {}
    for key, block in blocks.items():
        outer_needs = parse_needs(block, key)
        if key in expansion:
            inner = expansion[key]
            for sub, sub_name in inner["names"].items():
                edges = [inner["names"][dep] for dep in inner["needs"][sub]
                         if dep in inner["names"]]
                if not inner["needs"][sub]:
                    # A callee root inherits the caller's dependencies.
                    edges = [resolve_edge(dep, display, expansion)
                             for dep in outer_needs]
                    edges = [e for group in edges for e in group]
                graph[sub_name] = edges
            continue
        edges = [resolve_edge(dep, display, expansion) for dep in outer_needs]
        graph[display[key]] = [e for group in edges for e in group]
    return graph


def resolve_edge(dep_key: str, display: dict, expansion: dict) -> list:
    """An edge onto a reusable-workflow call lands on that workflow's sinks --
    the jobs nothing inside it depends on."""
    if dep_key in expansion:
        inner = expansion[dep_key]
        depended_on = {d for deps in inner["needs"].values() for d in deps}
        return [inner["names"][sub] for sub in inner["names"]
                if sub not in depended_on]
    return [display[dep_key]] if dep_key in display else []


def match_graph_to_jobs(graph: dict, job_names: list) -> tuple:
    """Resolve graph node names, which may carry matrix placeholders, against
    the names the API actually reported.  Returns (edges by job name, drift)."""
    # Most specific first: a matrix node such as `Build / .+ / .+` would
    # otherwise shadow the literal `Build / ubuntu-latest / gcc` whenever it
    # happened to be declared earlier, and #1574 reorders jobs.
    # R7: placeholder text adds length without adding specificity, so measure
    # the literal part only.
    ordered = sorted(graph, key=lambda node: (node.count("${{"),
                                              -len(EXPRESSION_RE.sub("", node))))
    patterns = [(node, name_pattern(node)) for node in ordered]
    resolved: dict = {}
    node_of_job: dict = {}
    matched_nodes = set()
    for job_name in job_names:
        for node, pattern in patterns:
            if pattern.match(job_name):
                matched_nodes.add(node)
                node_of_job[job_name] = node
                resolved[job_name] = set(graph[node])
                break
    # Which jobs each node owns, decided once by the specificity rule above.
    # Expanding a dependency by re-matching its pattern would let it claim
    # jobs that resolved to a different node, which can make a job its own
    # dependency and leave a cycle behind with nothing in the drift report.
    jobs_of_node: dict = {}
    for job_name, node in node_of_job.items():
        jobs_of_node.setdefault(node, []).append(job_name)

    edges: dict = {}
    for job_name in job_names:
        if job_name not in resolved:
            # Deliberately absent, not empty: an empty list is a root, and the
            # observed walk has to be able to tell the two apart.
            continue
        concrete = set()
        for dep_node in resolved[job_name]:
            concrete.update(jobs_of_node.get(dep_node, []))
        concrete.discard(job_name)
        edges[job_name] = sorted(concrete)
    drift = {
        "jobs_without_node": sorted(n for n in job_names if n not in resolved),
        "nodes_without_job": sorted(set(graph) - matched_nodes),
    }
    return edges, drift


def _seconds(metric: dict):
    return metric["seconds"] if metric.get("status") in ("measured",
                                                         "partial") else None


# A job with one of these conclusions has no duration because it did not run,
# not because the run lost the data.  Reporting it as missing data is the same
# false alarm as reporting a job that is merely still going.
NO_DURATION_BY_DESIGN = ("skipped", "cancelled")

# The job statuses that mean the job may still produce a duration.  GitHub
# uses exactly these two before `completed`.
IN_FLIGHT_STATUSES = ("queued", "in_progress")


def untimed_executed_jobs(analyzed: list) -> tuple:
    """Jobs that ran, or may still run, and that the run did not time, split
    into the ones that have not finished and the ones that finished anyway.
    A job that was skipped or cancelled before it started has no duration by
    design and is in neither list.

    The split is what the caller needs to say something true.  A job still
    running is ordinary and makes the answer a lower bound; a job that
    finished without a usable duration is missing data.  Calling the first
    the second turns every live run into an alarm, which is how a caveat
    stops being read.

    Both walkers can pick a chain that excludes such a job, for different
    reasons.  The graph walk compares weights and an untimed node weighs
    zero, so the longest path routes around it.  The observed walk compares
    completion timestamps and starts at the last job it could time, so an
    untimed later finisher is stepped over and a hole on an unvisited branch
    is never seen.  Either way the surviving chain carries no trace of it,
    which is why both lists are collected run-wide rather than along the
    chain."""
    running, untimed = [], []
    for entry in analyzed:
        if entry.get("conclusion") in NO_DURATION_BY_DESIGN:
            continue
        if _seconds(entry["metrics"]["job_wall"]) is not None:
            continue
        # Only the two statuses that actually mean "not done yet" take the
        # quiet branch.  A status this tool does not recognise is data it
        # cannot read, which belongs with missing data rather than with an
        # ordinary live job.
        target = running if entry.get("status") in IN_FLIGHT_STATUSES \
            else untimed
        target.append(entry["name"])
    return sorted(running), sorted(untimed)


def observed_path(analyzed: list, edges: dict, drift: dict) -> dict:
    """Walk back from the last job to finish, hop by hop, through its actual
    dependencies.  Taking the nearest earlier finisher instead would be wrong
    and quietly so: on a real run it picks a macOS matrix build as the
    predecessor of a TSan job that does not depend on it.  Where the graph
    cannot resolve a hop the walk stops and says the path is partial rather
    than attaching a plausible neighbour."""
    by_name = {entry["name"]: entry for entry in analyzed}
    finished = [e for e in analyzed
                if _seconds(e["metrics"]["job_wall"]) is not None
                and e.get("completed_at")]
    if not finished:
        return unavailable("no job has completed")
    current = max(finished, key=lambda e: ts_key(e["completed_at"]))
    chain = [current["name"]]
    truncated = None
    seen = {current["name"]}
    while True:
        if current["name"] not in edges:
            truncated = f"{current['name']} is not in the dependency graph"
            break
        candidates = [by_name[name] for name in edges[current["name"]]
                      if name in by_name and name not in seen]
        # A skipped job has a completion stamp but no duration, and walking
        # through it at zero weight would drop the very gap it represents.
        candidates = [c for c in candidates if c.get("completed_at")
                      and c["conclusion"] != "skipped"]
        if not candidates:
            if edges[current["name"]]:
                truncated = (f"{current['name']}'s dependencies did not run")
            break
        current = max(candidates, key=lambda e: ts_key(e["completed_at"]))
        seen.add(current["name"])
        chain.append(current["name"])
    chain.reverse()
    total = sum(
        (_seconds(by_name[n]["metrics"]["queue_delay"]) or 0)
        + (_seconds(by_name[n]["metrics"]["job_wall"]) or 0) for n in chain)
    hops = [{"job": name,
             "queue_delay": _seconds(by_name[name]["metrics"]["queue_delay"]),
             "job_wall": _seconds(by_name[name]["metrics"]["job_wall"])}
            for name in chain]
    # A hop the run did not time contributes nothing to the sum, so the total
    # is short by however long that job actually took.  Name it rather than
    # letting `or 0` present the shortfall as a measurement.
    unmeasured = [hop["job"] for hop in hops
                  if hop["queue_delay"] is None or hop["job_wall"] is None]
    metric = measured(total, {
        "chain": chain, "hops": hops,
        "weights": "queue_delay + job_wall per hop",
        "note": ("a lower bound on run wall clock: it omits the first job's "
                 "upstream wait and every scheduler release latency"),
    })
    if unmeasured:
        metric["evidence"]["unmeasured_hops"] = unmeasured
    # The walk starts at the last job to finish that the run actually timed,
    # so an untimed job that finished later is skipped over silently and the
    # chain below it is reported as if it were the whole path.
    on_chain = set(chain)
    running, untimed = untimed_executed_jobs(analyzed)
    off_running = [name for name in running if name not in on_chain]
    off_untimed = [name for name in untimed if name not in on_chain]
    if off_running:
        metric["evidence"]["unfinished_jobs_off_chain"] = off_running
    if off_untimed:
        metric["evidence"]["untimed_jobs_off_chain"] = off_untimed
    reasons = []
    if unmeasured:
        reasons.append(f"{len(unmeasured)} hop(s) on this chain were not "
                       f"timed and contribute nothing to the total")
    if off_running:
        reasons.append(f"{len(off_running)} job(s) elsewhere in the run have "
                       f"not finished, so the walk started below the end of "
                       f"the path and the total is a lower bound")
    if off_untimed:
        reasons.append(f"{len(off_untimed)} finished job(s) elsewhere in the "
                       f"run were not timed, so the walk may have started "
                       f"below the true end of the path, or one of them may "
                       f"lie on a longer branch it never entered")
    if truncated:
        reasons.append(f"walk stopped: {truncated}")
    if drift.get("nodes_without_job"):
        # The run does not contain jobs the declared graph expects, so this
        # path may be short because those jobs are absent rather than because
        # the chain ends here.
        reasons.append(f"{len(drift['nodes_without_job'])} declared jobs are "
                       f"absent from this run; see drift")
    if reasons:
        metric["status"] = "partial"
        metric["reason"] = "; ".join(reasons)
    return metric


def graph_path(analyzed: list, edges: dict, drift: dict) -> dict:
    """Longest path over the declared graph, weighting each node by its wall
    time and each edge by the wait between the dependency finishing and the
    dependent being created.  Inside a skip cascade every edge weight is
    unavailable, so the path falls back to node weights alone and says so
    rather than disappearing."""
    by_name = {entry["name"]: entry for entry in analyzed}
    if not edges:
        # A graph that resolved to nothing is not the same as no graph, and
        # saying the latter hides a rename that matched none of the run.
        if drift and (drift.get("jobs_without_node")
                      or drift.get("nodes_without_job")):
            return unavailable("the declared graph resolved to none of this "
                               "run's jobs; see drift")
        return unavailable(GRAPH_NOT_SUPPLIED)
    best: dict = {}
    unusable_edges: set = set()

    def visit(name, stack):
        if name in best:
            return best[name]
        if name in stack:            # a cycle cannot occur in a needs DAG,
            return 0, [name]         # but never spin if the data says one does
        stack = stack | {name}
        # Every name reaching here is a by_name key: the seeds are its keys
        # and the recursion below skips a dep that is not one.
        node_weight = _seconds(by_name[name]["metrics"]["job_wall"]) or 0
        best_total, best_chain = node_weight, [name]
        for dep in edges.get(name, []):
            if dep not in by_name:
                continue
            dep_total, dep_chain = visit(dep, stack)
            edge = by_name[name]["metrics"]["scheduler_release_latency"]
            edge_weight = _seconds(edge)
            if edge_weight is None:
                edge_weight = 0
            total = dep_total + edge_weight + node_weight
            if total > best_total:
                best_total, best_chain = total, dep_chain + [name]
        best[name] = (best_total, best_chain)
        return best[name]

    results = [visit(name, frozenset()) for name in by_name]
    if not results:
        return unavailable("no job to walk")
    total, chain = max(results, key=lambda pair: pair[0])
    unmeasured_nodes = [name for name in chain
                        if _seconds(by_name[name]["metrics"]["job_wall"])
                        is None]
    # Only the edges on the chain being reported can qualify this number.
    for index, name in enumerate(chain[1:], start=1):
        if _seconds(by_name[name]["metrics"]["scheduler_release_latency"]) is None:
            unusable_edges.add(f"{chain[index - 1]} -> {name}")
    metric = measured(total, {"chain": chain,
                              "weights": "job_wall per node, "
                                         "scheduler_release_latency per edge"})
    reasons = []
    if unmeasured_nodes:
        reasons.append(f"{len(unmeasured_nodes)} job(s) on this chain were "
                       f"not timed and contribute nothing to the total")
        metric["evidence"]["unmeasured_nodes"] = unmeasured_nodes
    # An untimed node weighs zero during selection, so the longest path can
    # be routed around it and come back clean.  The hole is off the chain
    # precisely because it was scored as free.
    on_chain = set(chain)
    running, untimed = untimed_executed_jobs(analyzed)
    off_running = [name for name in running if name not in on_chain]
    off_untimed = [name for name in untimed if name not in on_chain]
    if off_running:
        reasons.append(f"{len(off_running)} job(s) off this chain have not "
                       f"finished, so they weigh nothing yet and the longest "
                       f"path may move once they do")
        metric["evidence"]["unfinished_jobs_off_chain"] = off_running
    if off_untimed:
        reasons.append(f"{len(off_untimed)} finished job(s) off this chain "
                       f"were not timed, so the longest path may have been "
                       f"routed around them")
        metric["evidence"]["untimed_jobs_off_chain"] = off_untimed
    if unusable_edges:
        reasons.append("edge weights unavailable on this chain; node weights "
                       "only (a skip cascade batches job creation)")
        metric["evidence"]["unusable_edges"] = sorted(unusable_edges)
    if drift["jobs_without_node"] or drift["nodes_without_job"]:
        reasons.append("the declared graph and the run disagree; see drift")
    if reasons:
        metric["status"] = "partial"
        metric["reason"] = "; ".join(reasons)
    return metric


# ---------------------------------------------------------------------------
# Run-level analysis
# ---------------------------------------------------------------------------

def pinned_check(name: str) -> bool:
    return any(name.startswith(prefix) for prefix in PINNED_CHECK_PREFIXES)


def analyze(run: dict, jobs_doc: dict, needs: dict | None = None,
            drift: dict | None = None) -> dict:
    """@needs is the resolved per-job dependency map and @drift is where it and
    the run disagree; both come from match_graph_to_jobs."""
    # None means no graph was offered.  An empty mapping means one was and it
    # resolved to nothing, which is a finding rather than an absence.
    graph_supplied = needs is not None
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
            # Only a finished step with unusable timestamps is anomalous.
            if not item.get("finished"):
                continue
            anomalies.append({
                "kind": "unusable_step", "job": entry["name"],
                "step": item.get("step"), "reason": item.get("reason"),
            })

    if graph_supplied:
        drift = drift or {"jobs_without_node": [], "nodes_without_job": []}
        critical_path = {
            "observed": observed_path(analyzed, needs, drift),
            "graph": graph_path(analyzed, needs, drift),
            "graph_drift": drift,
        }
    else:
        critical_path = {
            "observed": unavailable(GRAPH_NOT_SUPPLIED),
            "graph": unavailable(GRAPH_NOT_SUPPLIED),
            # Not [].  An empty value here would read as "compared, no drift".
            "graph_drift": None,
        }

    unmapped = sorted({name for entry in analyzed
                       for name in entry["unmapped_steps"]})
    unusable = sorted({item["step"] for entry in analyzed
                       for item in entry.get("unusable_steps") or []
                       if item.get("finished")})

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
        "critical_path": critical_path,
        "anomalies": anomalies,
        "unmapped_steps": unmapped,
        "unusable_steps": unusable,
    }


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------
#
# Cells are (job name, runner population).  No run in this repository is
# single-population -- one run spans GitHub-hosted runners and several
# self-hosted ones, and the same job lands on different ones between runs -- so
# a run-level population filter would refuse every real input and a pooled
# median would average two different machines.  "Hosted-runner baseline"
# therefore means hosted-runner job samples.

AGGREGATED_METRICS = ("queue_delay", "job_wall", "upstream_elapsed")


def percentile_nearest_rank(sorted_values: list, fraction: float):
    """Nearest-rank, pinned so the doc and the tool cannot drift.  With a small
    sample this returns the maximum, which is why the report says so rather
    than presenting it as an estimate."""
    if not sorted_values:
        return None
    rank = math.ceil(fraction * len(sorted_values))
    return sorted_values[max(rank, 1) - 1]


def summarize(values: list) -> dict:
    ordered = sorted(values)
    return {
        "n": len(ordered),
        "median": percentile_nearest_rank(ordered, 0.5),
        "p95": percentile_nearest_rank(ordered, 0.95),
        "max": ordered[-1] if ordered else None,
        "p95_is_sample_max": len(ordered) < 20,
    }


def aggregate_reports(reports: list) -> dict:
    """Per-(job, population) cells across runs, kept separate by conclusion so
    a failed run's truncated timings never dilute a successful one's."""
    cells: dict = {}
    excluded = []
    runs = []
    for report in reports:
        conclusion = report.get("conclusion") or report.get("status")
        attempt = report.get("run_attempt")
        if attempt is None:
            excluded.append({"run_id": report.get("run_id"),
                             "reason": "run_attempt unknown"})
            continue
        if attempt != 1:
            excluded.append({"run_id": report.get("run_id"),
                             "reason": "run_attempt > 1"})
            continue
        runs.append({"run_id": report.get("run_id"), "conclusion": conclusion,
                     "head_sha": report.get("head_sha")})
        for job in report.get("jobs") or []:
            key = f"{job['name']}\u0000{job['population']}\u0000{conclusion}"
            cell = cells.setdefault(key, {
                "job": job["name"], "population": job["population"],
                "conclusion": conclusion, "runner_names": set(),
                "samples": {name: [] for name in AGGREGATED_METRICS},
            })
            if job.get("runner_name"):
                cell["runner_names"].add(job["runner_name"])
            for name in AGGREGATED_METRICS:
                value = _seconds(job["metrics"][name])
                if value is not None:
                    cell["samples"][name].append(value)
    summary = []
    for cell in cells.values():
        summary.append({
            "job": cell["job"], "population": cell["population"],
            "conclusion": cell["conclusion"],
            # Hosted runners are fungible ephemeral machines; self-hosted ones
            # are not, so a cell spanning several of them is a mixture and has
            # to say so rather than hide behind the population label.
            "runner_names": sorted(cell["runner_names"]),
            "mixed_machines": len(cell["runner_names"]) > 1
                              and cell["population"] == "self-hosted",
            # n is per metric, not per cell: a skipped job contributes to
            # neither, and a failed run truncates some metrics and not others.
            "metrics": {name: summarize(values)
                        for name, values in cell["samples"].items()},
        })
    summary.sort(key=lambda c: (c["conclusion"], c["job"], c["population"]))
    # "none" is the absence of a runner, not a population of one.
    populations = sorted({c["population"] for c in summary} - {"none"})
    return {
        "schema": SCHEMA,
        "generated_at": now_iso(),
        "runs": runs,
        "excluded": excluded,
        "populations": populations,
        "pooled": False,
        "note": ("cells are (job, population, conclusion); populations are "
                 "never pooled because one run spans several and the same job "
                 "moves between them.  Within self-hosted, a cell may still "
                 "span several machines; `mixed_machines` says which do"),
        "cells": summary,
    }


def render_aggregate_markdown(doc: dict) -> str:
    lines = [
        f"# CI telemetry baseline over {len(doc['runs'])} runs",
        "",
        f"- populations: {', '.join(doc['populations']) or 'none'}; "
        f"never pooled",
        f"- excluded: {len(doc['excluded'])}",
        "",
        "Seconds. `n` is per metric: a skipped job contributes to none, and a "
        "failed run truncates some.",
        "",
        "| conclusion | job | population | queue n | queue med | queue p95 | "
        "wall n | wall med | wall p95 | mixed machines |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    def num(value):
        # A cell with no samples prints an em dash, not "None" and not 0.
        return "--" if value is None else str(value)

    for cell in doc["cells"]:
        q, w = cell["metrics"]["queue_delay"], cell["metrics"]["job_wall"]
        if all(stats["n"] == 0 for stats in cell["metrics"].values()):
            # A cell with no samples in any metric is a job that only ever
            # skipped; it is counted below rather than given a row of dashes.
            continue
        lines.append(
            f"| {cell['conclusion']} | {cell['job']} | {cell['population']} "
            f"| {q['n']} | {num(q['median'])} | {num(q['p95'])} "
            f"| {w['n']} | {num(w['median'])} | {num(w['p95'])} "
            f"| {'yes' if cell['mixed_machines'] else ''} |")
    empty = sum(1 for c in doc["cells"]
                if all(stats["n"] == 0 for stats in c["metrics"].values()))
    if empty:
        lines += ["", f"{empty} further cells contributed no sample to any "
                  "metric (jobs that only ever skipped)."]
    if any(c["metrics"]["queue_delay"]["p95_is_sample_max"]
           for c in doc["cells"] if c["metrics"]["queue_delay"]["n"]):
        lines += ["", "Both columns are nearest-rank, so `med` is a sample "
                  "value rather than the midpoint of two. Where n < 20 the "
                  "p95 column is the sample maximum, not a percentile "
                  "estimate."]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def cell(metric: dict) -> str:
    if metric.get("status") == "measured":
        return str(metric["seconds"])
    if metric.get("status") == "partial":
        return f"{metric['seconds']} (partial)"
    return metric.get("status", "unavailable")


# The per-job table, declared once: (metric name, where it lives on the job
# entry, column heading).  Both the table and the block that explains it are
# built from this, because they have to agree.  A reason printed for a metric
# with no column explains a number the reader cannot see, and a column with no
# reason is the bare `(partial)` the block exists to defeat; either happens the
# moment the two lists are written out separately.
JOB_COLUMNS = (
    ("queue_delay", "metrics", "queue"),
    ("job_wall", "metrics", "wall"),
    ("configure", "phases", "configure"),
    ("compile", "phases", "compile"),
    ("tests", "phases", "tests"),
    ("unaccounted", None, "unaccounted"),
)

# How many entries sharing one reason are still worth naming one by one.  The
# threshold keys on group size, so it does nothing on a run with fewer jobs
# than this, where the block is at its longest relative to its table.
COLLAPSE_AT = 3


def job_cells(job: dict) -> list:
    """The (column, metric) pairs this job contributes to the per-job table,
    in the order the table prints them."""
    return [(name, job[source][name] if source else job[name])
            for name, source, _ in JOB_COLUMNS]


def degradation_block(report: dict, path: dict) -> list:
    """Why any number printed above is not final.  A cell reading `partial`
    or `unavailable` with nothing saying why is a flag a reader learns to
    skip, and the caveat the two walkers raise is worth nothing once that has
    happened.

    Two things would defeat it on their own.  Explaining a metric that has no
    column tells the reader about a number they cannot see while leaving the
    ones they can see bare, so the selection is exactly `cell()`'s output:
    the run-level table, both critical paths, and each job's six columns.  And
    one reason repeated once per job buries every line that is not repeated,
    so identical reasons collapse into a single line carrying the count.  A
    skipped job is left out for the same reason: its durations are absent by
    design and the table header says so once."""
    def wanted(metric):
        # Deliberately not `and metric.get("reason")`: a degraded metric that
        # carries no reason is the bare `(partial)` this block exists to
        # defeat, so it must appear and say that its cause was not recorded.
        return metric.get("status") != "measured"

    qualified = [(key, metric) for key, metric in report["metrics"].items()
                 if wanted(metric)]
    qualified += [(f"critical_path.{key}", path[key])
                  for key in ("observed", "graph") if wanted(path[key])]
    for job in report["jobs"]:
        if job["conclusion"] == "skipped":
            continue
        qualified += [(f"{job['name']} / {column}", metric)
                      for column, metric in job_cells(job) if wanted(metric)]
    if not qualified:
        return []

    groups: dict = {}
    for key, metric in qualified:
        reason = metric.get("reason") or "no cause was recorded"
        groups.setdefault((metric["status"], reason), []).append(key)
    lines = ["", "Why a number above is not final:", ""]
    for (status, reason), keys in groups.items():
        # A handful is worth naming individually; a wall of them is not.
        if len(keys) <= COLLAPSE_AT:
            lines += [f"- `{key}` is {status}: {reason}" for key in keys]
            continue
        # Collapse only what can still be named.  Entries sharing a reason
        # across different columns have no shared subject, and "4 values are
        # anomalous" tells the reader neither which jobs nor which numbers,
        # which is worse than four specific lines.
        columns = {key.rsplit(" / ", 1)[-1] for key in keys}
        if len(columns) != 1:
            lines += [f"- `{key}` is {status}: {reason}" for key in keys]
            continue
        lines.append(f"- `{columns.pop()}` on {len(keys)} jobs is "
                     f"{status}: {reason}")
    return lines


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
    path = report["critical_path"]
    caveats = []
    feedback = report["metrics"].get("pr_feedback") or {}
    if (feedback.get("evidence") or {}).get("non_pinned_executed") == 0:
        caveats.append("`pr_feedback` equals `run_wall_clock` by construction "
                       "here, not by corroboration: every executed job is a "
                       "pinned check.")
    note = (path["observed"].get("evidence") or {}).get("note")
    if note:
        caveats.append(f"`critical_path.observed` is {note}.")
    for key in ("observed", "graph"):
        metric = path[key]
        lines.append(f"| critical_path.{key} | {cell(metric)} |")
    chain = (path["observed"].get("evidence") or {}).get("chain")
    if chain:
        hops = {hop["job"]: hop
                for hop in (path["observed"].get("evidence") or {}).get(
                    "hops") or []}
        lines += ["", "Critical path, in order. Seconds are queue then wall:",
                  ""]
        for name in chain:
            hop = hops.get(name) or {}
            lines.append(f"- {name}"
                         f" ({hop.get('queue_delay')} / {hop.get('job_wall')})")
    if caveats:
        lines += ["", "These are not three independent measurements:", ""]
        lines += [f"- {text}" for text in caveats]
    drift = path.get("graph_drift")
    if drift and (drift.get("jobs_without_node")
                  or drift.get("nodes_without_job")):
        lines += ["", "The declared graph and the run disagree:", ""]
        for name in drift.get("jobs_without_node") or []:
            lines.append(f"- job with no node: {name}")
        for name in drift.get("nodes_without_job") or []:
            lines.append(f"- node with no job: {name}")
    lines += [
        "",
        "Per job, seconds. A skipped job has no durations at all.",
        "",
        "| job | population | "
        + " | ".join(heading for _, _, heading in JOB_COLUMNS) + " |",
        "| --- | --- | " + " | ".join("---" for _ in JOB_COLUMNS) + " |",
    ]
    for job in report["jobs"]:
        values = " | ".join(cell(metric) for _, metric in job_cells(job))
        lines.append(f"| {job['name']} | {job['population']} | {values} |")
    lines += degradation_block(report, path)
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
        run_path.write_text(json.dumps(run, indent=2, sort_keys=True) + "\n",
                            encoding="utf-8")
        jobs_path.write_text(json.dumps(jobs, indent=2, sort_keys=True)
                             + "\n", encoding="utf-8")
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
        run = json.loads(run_path.read_text(encoding="utf-8"))
        if run.get("id") is not None and run.get("id") != run_id:
            return die(f"{run_path} holds run {run.get('id')}, not {run_id}; "
                       f"the saved pair does not belong together")
        jobs_doc = json.loads(jobs_path.read_text(encoding="utf-8"))
        edges, drift = None, None
        if args.workflow:
            try:
                graph = extract_graph(args.workflow)
            except (OSError, WorkflowParseError) as exc:
                # A missing workflow costs the critical path, not the report:
                # the per-job timings are the bulk of the value and they do
                # not depend on the graph.
                print(f"ci-run-telemetry: no dependency graph "
                      f"({exc}); the critical path will say so",
                      file=sys.stderr)
                graph = None
            else:
                if not graph:
                    # Only reachable when the file parsed: an unreadable
                    # workflow has already said why, and saying it declares
                    # no jobs on top of that would be a second, false claim.
                    print("ci-run-telemetry: the workflow declares no jobs; "
                          "the critical path will say so", file=sys.stderr)
            if graph:
                names = [j.get("name") for j in jobs_doc.get("jobs") or []]
                edges, drift = match_graph_to_jobs(graph, names)
        report = analyze(run, jobs_doc, edges, drift)
        (out / f"run-{run_id}.json").write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8")
        (out / f"run-{run_id}.md").write_text(render_markdown(report),
                                              encoding="utf-8")
        counts = report["job_counts"]
        print(f"ci-run-telemetry: run {run_id}: {counts['executed']} executed, "
              f"{counts['skipped']} skipped, "
              f"{len(report['anomalies'])} anomalies")
    return 0


def cmd_aggregate(args) -> int:
    out = Path(args.out)
    if args.run_id:
        paths = [out / f"run-{run_id}.json" for run_id in args.run_id]
        missing = [str(path) for path in paths if not path.exists()]
        if missing:
            return die(f"no report at {', '.join(missing)} (run `report` first)")
    else:
        paths = sorted(path for path in out.glob("run-*.json")
                       if path.name != f"{args.label}.json")
    if not paths:
        return die(f"no reports under {out} (run `report` first)")
    reports = [json.loads(path.read_text(encoding="utf-8"))
               for path in paths]
    doc = aggregate_reports(reports)
    (out / f"{args.label}.json").write_text(
        json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (out / f"{args.label}.md").write_text(render_aggregate_markdown(doc),
                                          encoding="utf-8")
    print(f"ci-run-telemetry: {args.label}: {len(doc['runs'])} runs, "
          f"{len(doc['cells'])} cells, populations "
          f"{', '.join(doc['populations']) or 'none'}")
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
    report.add_argument("--workflow", default=DEFAULT_WORKFLOW,
                        help="workflow file the dependency graph is read from "
                             f"(default {DEFAULT_WORKFLOW}); pass an empty "
                             "value to skip the critical path")
    report.set_defaults(func=cmd_report)

    aggregate = sub.add_parser(
        "aggregate", help="combine reports into per-(job, population) cells")
    aggregate.add_argument("--run-id", type=int, action="append",
                           help="restrict to these runs; default every report "
                                "in the output directory")
    aggregate.add_argument("--out", default=DEFAULT_OUT_DIR,
                           help=f"output directory (default {DEFAULT_OUT_DIR}/)")
    aggregate.add_argument("--label", default="baseline",
                           help="name for the written files (default baseline)")
    aggregate.set_defaults(func=cmd_aggregate)

    args = parser.parse_args(argv)
    if getattr(args, "max_requests", 1) < 1:
        return die("--max-requests must be at least 1")
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
