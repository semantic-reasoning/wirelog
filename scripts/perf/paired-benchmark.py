#!/usr/bin/env python3
"""Collect serialized, paired CRDT/CSPA evidence on a tagged Linux runner.

This is diagnostic evidence, not a performance gate. Both binaries must come
from builds with the same resolved profile; the workflow records that profile.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
from datetime import datetime, timezone


HEADER = ("workload\tnodes\tedges\tworkers\trepeat\tmin_ms\tmedian_ms"
          "\tmax_ms\tpeak_rss_kb\ttuples\titerations\tstatus")
WORKLOADS = {
    "crdt": ("crdt", "--data-crdt", 2152328, 14148),
    "cspa-fast": ("cspa", "--data-cspa", 20381, 6),
}
FIXTURES = (
    "crdt/Insert_input.csv", "crdt/Remove_input.csv",
    "cspa/assign.csv", "cspa/dereference.csv",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_host(cpu: int) -> dict[str, str]:
    values = {"time_utc": datetime.now(timezone.utc).isoformat()}
    frequency_root = f"/sys/devices/system/cpu/cpu{cpu}/cpufreq"
    for name, path in {
        "loadavg": "/proc/loadavg",
        "cpu_pressure": "/proc/pressure/cpu",
        "cgroup_cpu_stat": "/sys/fs/cgroup/cpu.stat",
        "governor": f"{frequency_root}/scaling_governor",
        "frequency_khz": f"{frequency_root}/scaling_cur_freq",
        "frequency_min_khz": f"{frequency_root}/scaling_min_freq",
        "frequency_max_khz": f"{frequency_root}/scaling_max_freq",
    }.items():
        try:
            values[name] = Path(path).read_text(encoding="utf-8").strip()
        except OSError:
            values[name] = "unavailable"
    return values


def order(samples: int, first: str) -> list[tuple[int, str]]:
    """Alternate the leading side; an odd tail is inverted on a repeat run."""
    if samples < 1 or first not in ("base", "candidate"):
        raise ValueError("samples must be positive and first must name a side")
    other = "candidate" if first == "base" else "base"
    return [(i, side) for i in range(samples)
            for side in ((first, other) if i % 2 == 0 else (other, first))]


def parse_tsv(stdout: str, workload: str) -> dict[str, int | float | str]:
    lines = stdout.strip().splitlines()
    if len(lines) != 2 or lines[0] != HEADER:
        raise ValueError("missing or malformed bench TSV header/row")
    fields = lines[1].split("\t")
    if len(fields) != 12:
        raise ValueError("bench TSV row must have 12 columns")
    expected_name, _, expected_tuples, expected_iterations = WORKLOADS[workload]
    if fields[0] != expected_name or fields[3:5] != ["1", "1"] or fields[11] != "OK":
        raise ValueError("wrong workload, worker count, repeat count, or status")
    try:
        times = [float(value) for value in fields[5:8]]
        tuples, iterations = int(fields[9]), int(fields[10])
    except ValueError as error:
        raise ValueError("invalid bench timing or result") from error
    if not all(math.isfinite(value) and value > 0 for value in times):
        raise ValueError("nonpositive or nonfinite bench timing")
    if times[0] != times[1] or times[1] != times[2]:
        raise ValueError("repeat=1 timings disagree")
    if (tuples, iterations) != (expected_tuples, expected_iterations):
        raise ValueError(f"wrong result: tuples={tuples}, iterations={iterations}")
    return {"workload": workload, "elapsed_ms": times[1], "tuples": tuples,
            "iterations": iterations}


def append_event(stream, event: dict) -> None:
    stream.write(json.dumps(event, sort_keys=True) + "\n")
    stream.flush()
    os.fsync(stream.fileno())


def run_once(binary: Path, workload: str, data_root: Path, cpu: int,
             timeout: int) -> dict:
    _, data_option, _, _ = WORKLOADS[workload]
    command = ["taskset", "-c", str(cpu), str(binary), "--workload", workload,
               data_option, str(data_root / ("crdt" if workload == "crdt" else "cspa")),
               "--workers", "1", "--repeat", "1"]
    before = read_host(cpu)
    try:
        result = subprocess.run(command, capture_output=True, text=True,
                                encoding="utf-8", errors="replace",
                                timeout=timeout, check=False)
        event = {"command": command, "exit_code": result.returncode,
                 "stdout": result.stdout, "stderr": result.stderr}
    except subprocess.TimeoutExpired as error:
        event = {"command": command, "timeout_seconds": timeout,
                 "stdout": error.stdout.decode("utf-8", "replace") if isinstance(error.stdout, bytes) else error.stdout,
                 "stderr": error.stderr.decode("utf-8", "replace") if isinstance(error.stderr, bytes) else error.stderr}
    except OSError as error:
        event = {"command": command, "exit_code": 127, "stdout": "",
                 "stderr": str(error)}
    event["host_before"] = before
    event["host_after"] = read_host(cpu)
    return event


def summarize(times: list[float]) -> dict[str, float]:
    mean = statistics.fmean(times)
    return {"median_ms": statistics.median(times),
            "cov_percent": 100 * statistics.pstdev(times) / mean}


def collect(args: argparse.Namespace) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", args.base_sha) or not re.fullmatch(
            r"[0-9a-f]{40}", args.candidate_sha):
        raise ValueError("both source SHAs must be full lowercase commit IDs")
    if args.base_sha == args.candidate_sha:
        raise ValueError("base and candidate must differ")
    if args.cpu not in os.sched_getaffinity(0):
        raise ValueError(f"CPU {args.cpu} is outside this process's affinity")
    if args.samples < 1 or args.timeout < 1:
        raise ValueError("samples and timeout must be positive")
    binaries = {"base": args.base_binary.resolve(),
                "candidate": args.candidate_binary.resolve()}
    for binary in binaries.values():
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError(f"missing executable benchmark: {binary}")
    fixtures = {}
    for relative in FIXTURES:
        path = args.data_root / relative
        if not path.is_file():
            raise ValueError(f"missing fixture: {path}")
        fixtures[relative] = sha256(path)
    if args.out_dir.exists():
        raise ValueError(f"evidence directory already exists: {args.out_dir}")
    args.out_dir.mkdir(parents=True)
    metadata = {"schema_version": 1, "mode": "diagnostic_only",
                "source_sha": {"base": args.base_sha, "candidate": args.candidate_sha},
                "binary_sha256": {side: sha256(path) for side, path in binaries.items()},
                "fixture_sha256": fixtures, "cpu": args.cpu, "samples_per_side": args.samples,
                "first": args.first, "host": read_host(args.cpu)}
    (args.out_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    samples: dict[str, dict[str, list[float]]] = {
        workload: {"base": [], "candidate": []} for workload in WORKLOADS}
    status = "DIAGNOSTIC"
    reason = "all samples valid; no timing verdict defined"
    with (args.out_dir / "attempts.jsonl").open("w", encoding="utf-8") as stream:
        for workload in WORKLOADS:
            sequence = [(None, "base"), (None, "candidate")]
            sequence += order(args.samples, args.first)
            for index, side in sequence:
                phase = "warmup" if index is None else "sample"
                event = {"phase": phase, "workload": workload, "side": side,
                         "index": index}
                event.update(run_once(binaries[side], workload, args.data_root,
                                      args.cpu, args.timeout))
                if "timeout_seconds" in event:
                    status, reason = "INCONCLUSIVE", f"{workload} {side} {phase} timed out"
                elif event["exit_code"] != 0:
                    status, reason = "CORRECTNESS_FAILURE", f"{workload} {side} {phase} exited nonzero"
                else:
                    try:
                        parsed = parse_tsv(event["stdout"], workload)
                        event["parsed"] = parsed
                        if index is not None:
                            samples[workload][side].append(parsed["elapsed_ms"])
                    except ValueError as error:
                        status, reason = "CORRECTNESS_FAILURE", str(error)
                append_event(stream, event)
                if status != "DIAGNOSTIC":
                    break
            if status != "DIAGNOSTIC":
                break
    summary = {"schema_version": 1, "status": status, "reason": reason,
               "workloads": {}}
    for workload, sides in samples.items():
        if all(len(values) == args.samples for values in sides.values()):
            base = summarize(sides["base"])
            candidate = summarize(sides["candidate"])
            summary["workloads"][workload] = {
                "base": base, "candidate": candidate,
                "candidate_over_base_median": candidate["median_ms"] / base["median_ms"]}
        else:
            summary["workloads"][workload] = {
                "sample_count": {side: len(values) for side, values in sides.items()}}
    (args.out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--base-binary", type=Path, required=True)
    parser.add_argument("--candidate-binary", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--cpu", type=int, required=True)
    parser.add_argument("--first", choices=("base", "candidate"), default="base")
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    args.samples = 9
    try:
        result = collect(args)
    except (OSError, ValueError) as error:
        print(f"paired-benchmark: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "DIAGNOSTIC" else 1


if __name__ == "__main__":
    raise SystemExit(main())
