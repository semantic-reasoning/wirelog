#!/usr/bin/env python3
"""Verify that paired perf binaries and correctness logs are comparable."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


PROFILE_OPTIONS = (
    "buildtype", "optimization", "b_lto", "b_sanitize", "c_args",
    "c_link_args", "wirelog_log_max_level", "tests", "mbedTLS", "threads",
)
SAME_SOURCES = ("bench/bench_flowlog.c", "bench/bench_crdt_workload.h")
GATE_SOURCES = ("tests/test_crdt_perf_gate.c", "tests/test_cspa_perf_gate.c")
FIXTURES = (
    "bench/data/crdt/Insert_input.csv", "bench/data/crdt/Remove_input.csv",
    "bench/data/cspa/assign.csv", "bench/data/cspa/dereference.csv",
)
BINARIES = ("bench/bench_flowlog", "tests/test_crdt_perf_gate",
            "tests/test_cspa_perf_gate")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_output(source: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(source), *args], text=True, encoding="utf-8").strip()


def side_record(source: Path, build: Path, expected_sha: str) -> dict:
    actual_sha = git_output(source, "rev-parse", "HEAD")
    if actual_sha != expected_sha:
        raise ValueError(f"{source}: checkout SHA {actual_sha} != {expected_sha}")
    if git_output(source, "status", "--porcelain", "--untracked-files=no"):
        raise ValueError(f"{source}: tracked source files are dirty")
    options_path = build / "meson-info/intro-buildoptions.json"
    compiler_path = build / "meson-info/intro-compilers.json"
    options = {item["name"]: item["value"] for item in json.loads(
        options_path.read_text(encoding="utf-8"))}
    absent = set(PROFILE_OPTIONS) - set(options)
    if absent:
        raise ValueError(f"{build}: missing profile options {sorted(absent)}")
    compilers = json.loads(compiler_path.read_text(encoding="utf-8"))
    compiler = compilers["host"]["c"]
    compiler_id = {key: compiler[key] for key in
                   ("id", "version", "full_version", "linker_id", "exelist")}
    return {"source_sha": actual_sha,
            "source_tree": git_output(source, "rev-parse", "HEAD^{tree}"),
            "profile": {key: options[key] for key in PROFILE_OPTIONS},
            "compiler": compiler_id,
            "source_sha256": {name: sha256(source / name) for name in
                              SAME_SOURCES + GATE_SOURCES},
            "fixture_sha256": {name: sha256(source / name) for name in FIXTURES},
            "binary_sha256": {name: sha256(build / name) for name in BINARIES}}


def verify_pair(base_source: Path, base_build: Path, base_sha: str,
                candidate_source: Path, candidate_build: Path,
                candidate_sha: str, output: Path) -> dict:
    if (not re.fullmatch(r"[0-9a-f]{40}", base_sha)
        or not re.fullmatch(r"[0-9a-f]{40}", candidate_sha)
        or base_sha == candidate_sha):
        raise ValueError("base/candidate SHA must be distinct full lowercase commit IDs")
    report = {"schema_version": 1, "status": "incomplete"}
    try:
        report["base"] = side_record(base_source, base_build, base_sha)
        report["candidate"] = side_record(candidate_source, candidate_build,
                                          candidate_sha)
        base, candidate = report["base"], report["candidate"]
        for key in ("profile", "compiler", "fixture_sha256"):
            if base[key] != candidate[key]:
                raise ValueError(f"base/candidate {key} differ")
        for name in SAME_SOURCES:
            if base["source_sha256"][name] != candidate["source_sha256"][name]:
                raise ValueError(f"base/candidate benchmark source differs: {name}")
        report["status"] = "comparable"
    except (KeyError, OSError, subprocess.CalledProcessError, ValueError) as error:
        report["status"] = "rejected"
        report["reason"] = str(error)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n",
                      encoding="utf-8")
    if report["status"] != "comparable":
        raise ValueError(report["reason"])
    return report


def check_correctness(workload: str, log: str) -> None:
    if workload == "crdt":
        patterns = (r"test_crdt_perf_gate: correctness OK",
                    r"result\s+=\s+104851\s+\(expected 104851\)",
                    r"iterations\s+=\s+14148(?:\s|$)")
    elif workload == "cspa":
        patterns = (r"test_cspa_perf_gate: correctness OK\s+"
                    r"\(tuples=20381 iters=6\)",)
    else:
        raise ValueError(f"unknown workload: {workload}")
    for pattern in patterns:
        if not re.search(pattern, log):
            raise ValueError(f"{workload} correctness evidence missing: {pattern}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    profile = sub.add_parser("profile")
    for option in ("base-source", "base-build", "base-sha", "candidate-source",
                   "candidate-build", "candidate-sha", "output"):
        profile.add_argument("--" + option, required=True,
                             type=Path if option.endswith(("source", "build")) or option == "output" else str)
    correctness = sub.add_parser("correctness")
    correctness.add_argument("--workload", choices=("crdt", "cspa"), required=True)
    correctness.add_argument("--log", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "profile":
            verify_pair(args.base_source, args.base_build, args.base_sha,
                        args.candidate_source, args.candidate_build,
                        args.candidate_sha, args.output)
        else:
            check_correctness(args.workload, args.log.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        print(f"verify-paired-builds: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
