#!/usr/bin/env python3
"""Run bench_flowlog portfolio workloads and write structured artifacts."""

from __future__ import annotations

import argparse
import base64
import json
import math
import os
import platform
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
DEFAULT_WORKERS = "1,8,16"


WORKLOADS: dict[str, dict[str, Any]] = {
    "tc": {
        "args": ["--data", "graph_100.csv"],
        "data": ["graph_100.csv"],
    },
    "reach": {
        "args": ["--data", "graph_100.csv"],
        "data": ["graph_100.csv"],
    },
    "cc": {
        "args": ["--data", "graph_100.csv"],
        "data": ["graph_100.csv"],
    },
    "sssp": {
        "args": [
            "--data",
            "graph_100.csv",
            "--data-weighted",
            "graph_100_weighted.csv",
        ],
        "data": ["graph_100.csv", "graph_100_weighted.csv"],
    },
    "sg": {
        "args": ["--data", "graph_100.csv"],
        "data": ["graph_100.csv"],
    },
    "bipartite": {
        "args": ["--data", "graph_100.csv"],
        "data": ["graph_100.csv"],
    },
    "andersen": {
        "args": ["--data-andersen", "andersen"],
        "data": ["andersen"],
    },
    "dyck": {
        "args": ["--data-dyck", "dyck"],
        "data": ["dyck"],
    },
    "cspa-fast": {
        "args": ["--data-cspa", "cspa"],
        "data": ["cspa"],
    },
    "cspa": {
        "args": ["--data-cspa", "cspa"],
        "data": ["cspa"],
        "parser": "cspa_incremental_tsv",
    },
    "csda": {
        "args": ["--data-csda", "csda"],
        "data": ["csda"],
    },
    "galen": {
        "args": ["--data-galen", "galen"],
        "data": ["galen"],
    },
    "polonius": {
        "args": ["--data-polonius", "polonius"],
        "data": ["polonius"],
    },
    "ddisasm": {
        "args": ["--data-ddisasm", "ddisasm"],
        "data": ["ddisasm"],
    },
    "crdt": {
        "args": ["--data-crdt", "crdt"],
        "data": ["crdt"],
    },
    "doop": {
        "args": ["--data-doop", "doop"],
        "data": ["doop"],
    },
}

README_FULL_WORKLOADS = [
    "tc",
    "reach",
    "cc",
    "sssp",
    "sg",
    "bipartite",
    "andersen",
    "dyck",
    "cspa-fast",
    "cspa",
    "csda",
    "galen",
    "polonius",
    "ddisasm",
    "crdt",
    "doop",
]

TIERS = {
    "smoke": {
        "description": "cheap local validation tier",
        "workloads": ["reach", "sssp"],
        "excluded_reason": "smoke tier keeps local validation fast",
    },
    "light": {
        "description": "graph-only tier without heavy portfolio workloads",
        "workloads": ["tc", "reach", "cc", "sssp", "sg", "bipartite"],
        "excluded_reason": "light tier excludes analysis, CRDT, and DOOP workloads",
    },
    "readme-full": {
        "description": "README-scale portfolio: 15 table rows plus cspa incremental command",
        "workloads": README_FULL_WORKLOADS,
        "excluded_reason": None,
    },
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_workers(value: str) -> list[int]:
    workers: list[int] = []
    for part in value.split(","):
        text = part.strip()
        if not text:
            continue
        try:
            parsed = int(text, 10)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"invalid worker count: {text}") from exc
        if parsed <= 0:
            raise argparse.ArgumentTypeError("worker counts must be positive")
        if parsed not in workers:
            workers.append(parsed)
    if not workers:
        raise argparse.ArgumentTypeError("at least one worker count is required")
    return workers


def positive_int(value: str) -> int:
    try:
        parsed = int(value, 10)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def resolve_under(root: Path, value: str) -> str:
    return str(root / value)


def run_git(repo_root: Path, args: list[str]) -> str | None:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=repo_root,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
        )
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


def git_metadata(repo_root: Path) -> dict[str, Any]:
    sha = run_git(repo_root, ["rev-parse", "HEAD"])
    dirty_status = run_git(repo_root, ["status", "--porcelain"])
    return {
        "sha": sha,
        "dirty": None if dirty_status is None else bool(dirty_status),
    }


def tier_definition(tier: str, workloads: list[str]) -> dict[str, Any]:
    excluded = [name for name in README_FULL_WORKLOADS if name not in workloads]
    tier_info = TIERS.get(tier, {})
    return {
        "tier": tier,
        "description": tier_info.get("description", "custom workload list"),
        "workloads": workloads,
        "workload_count": len(workloads),
        "readme_command_count": len(README_FULL_WORKLOADS),
        "excluded_workloads": excluded,
        "excluded_reason": tier_info.get("excluded_reason") if excluded else None,
    }


def build_command(
    bench: Path,
    data_root: Path,
    workload: str,
    workers: int,
    repeat: int,
) -> list[str] | None:
    spec = WORKLOADS.get(workload)
    if spec is None:
        return None

    data_args: list[str] = []
    raw_args = spec["args"]
    for index, arg in enumerate(raw_args):
        if index % 2 == 0:
            data_args.append(arg)
        else:
            data_args.append(resolve_under(data_root, arg))

    command = [
        str(bench),
        "--workload",
        workload,
        *data_args,
        "--workers",
        str(workers),
        "--repeat",
        str(repeat),
    ]
    if spec.get("parser", "json") == "json":
        command.extend(["--format", "json"])
    return command


def missing_data_paths(data_root: Path, workload: str) -> list[str]:
    spec = WORKLOADS.get(workload)
    if spec is None:
        return []
    missing: list[str] = []
    for rel in spec["data"]:
        path = data_root / rel
        if not path.exists():
            missing.append(str(path))
    return missing


def parse_bench_json(stdout: str) -> tuple[dict[str, Any] | None, str | None]:
    text = stdout.strip()
    if not text:
        return None, "empty stdout"
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        return None, f"invalid JSON stdout: {exc}"
    if not isinstance(parsed, dict):
        return None, "bench JSON root is not an object"
    return parsed, None


def parse_bench_tsv(stdout: str) -> tuple[dict[str, Any] | None, str | None]:
    columns = [
        "workload",
        "nodes",
        "edges",
        "workers",
        "repeat",
        "min_ms",
        "median_ms",
        "max_ms",
        "peak_rss_kb",
        "tuples",
        "iterations",
        "status",
    ]
    lines = [line for line in stdout.splitlines() if line.strip()]
    if lines and lines[0].split("\t") == columns:
        lines.pop(0)
    if len(lines) != 1:
        return None, f"expected one benchmark TSV row, found {len(lines)}"
    fields = lines[0].split("\t")
    if len(fields) != len(columns):
        return None, f"benchmark TSV row has {len(fields)} fields, expected {len(columns)}"
    row = dict(zip(columns, fields, strict=True))
    if row["status"] != "OK":
        return None, f"benchmark TSV status is {row['status']!r}"
    try:
        for name in ("nodes", "edges"):
            if row[name] != "-" and parse_int(row[name]) < 0:
                raise ValueError(f"{name} must be nonnegative or '-'")
        workers = parse_int(row["workers"])
        repeat = parse_int(row["repeat"])
        peak_rss_kb = parse_int(row["peak_rss_kb"])
        tuples = parse_int(row["tuples"])
        iterations = parse_int(row["iterations"])
        timings = [parse_float(row[name]) for name in ("min_ms", "median_ms", "max_ms")]
    except ValueError as exc:
        return None, f"invalid benchmark TSV metric: {exc}"
    if workers <= 0 or repeat <= 0 or peak_rss_kb < 0 or tuples < 0 or iterations < 0:
        return None, "benchmark TSV counts must be positive or nonnegative"
    if any(not math.isfinite(value) or value < 0 for value in timings):
        return None, "benchmark TSV timings must be finite nonnegative numbers"
    if timings != sorted(timings):
        return None, "benchmark TSV must satisfy min <= median <= max"
    return {
        "workload": row["workload"],
        "workers": workers,
        "repeat": repeat,
        "tuples": tuples,
        "iterations": iterations,
        "peak_rss_kb": peak_rss_kb,
        "wall_time_ms": dict(zip(("min", "median", "max"), timings, strict=True)),
        "bench_status": row["status"],
    }, None


def parse_int(value: str) -> int:
    return int(value, 10)


def parse_float(value: str) -> float:
    return float(value)


def parse_cspa_incremental_tsv(stdout: str) -> tuple[dict[str, Any] | None, str | None]:
    columns = [
        ("workload", str),
        ("facts", parse_int),
        ("baseline_ms", parse_float),
        ("initial_ms", parse_float),
        ("insert_ms", parse_float),
        ("reeval_ms", parse_float),
        ("speedup", parse_float),
        ("tuples_before", parse_int),
        ("tuples_after", parse_int),
        ("iters_before", parse_int),
        ("iters_after", parse_int),
        ("peak_rss_kb", parse_int),
        ("bench_status", str),
    ]

    rows = [line for line in stdout.splitlines() if line.startswith("cspa_incr\t")]
    if len(rows) > 1:
        return None, f"expected one cspa_incr TSV row, found {len(rows)}"
    for line in rows:
        fields = line.split("\t")
        if len(fields) != len(columns):
            return None, f"cspa_incr row has {len(fields)} fields, expected {len(columns)}"

        parsed: dict[str, Any] = {}
        try:
            for (name, converter), value in zip(columns, fields, strict=True):
                parsed[name] = converter(value)
        except ValueError as exc:
            return None, f"invalid cspa_incr field: {exc}"
        if parsed["workload"] != "cspa_incr" or parsed["bench_status"] != "OK":
            return None, "cspa_incr row has unexpected workload or status"
        for name in (
            "facts",
            "tuples_before",
            "tuples_after",
            "iters_before",
            "iters_after",
            "peak_rss_kb",
        ):
            if parsed[name] < 0:
                return None, f"cspa_incr field {name} is negative"
        for name in (
            "baseline_ms",
            "initial_ms",
            "insert_ms",
            "reeval_ms",
            "speedup",
        ):
            if not math.isfinite(parsed[name]) or parsed[name] < 0:
                return None, f"cspa_incr field {name} is not a finite nonnegative number"
        return parsed, None

    return None, "missing cspa_incr TSV row"


def parse_bench_output(
    parser: str,
    stdout: str,
) -> tuple[dict[str, Any] | None, str | None, str]:
    if parser == "json":
        parsed, error = parse_bench_json(stdout)
        if error is None:
            return parsed, None, "json_parse_failed"
        tsv, tsv_error = parse_bench_tsv(stdout)
        if tsv_error is None:
            return tsv, None, "json_parse_failed"
        return None, f"JSON: {error}; TSV: {tsv_error}", "bench_output_parse_failed"
    if parser == "cspa_incremental_tsv":
        parsed, error = parse_cspa_incremental_tsv(stdout)
        return parsed, error, "tsv_parse_failed"
    return None, f"unknown parser: {parser}", "parse_failed"


def validate_bench_json(
    parsed: dict[str, Any], workload: str, workers: int, repeat: int
) -> str | None:
    if parsed.get("workload") != workload:
        return f"bench workload identity mismatch: expected {workload!r}"
    actual_workers = parsed.get("workers")
    actual_repeat = parsed.get("repeat")
    if (
        isinstance(actual_workers, bool)
        or not isinstance(actual_workers, int)
        or actual_workers != workers
        or isinstance(actual_repeat, bool)
        or not isinstance(actual_repeat, int)
        or actual_repeat != repeat
    ):
        return "bench worker/repeat identity mismatch"
    for name in ("tuples", "iterations", "peak_rss_kb"):
        value = parsed.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return f"bench field {name} must be a nonnegative integer"
    wall = parsed.get("wall_time_ms")
    if not isinstance(wall, dict):
        return "bench wall_time_ms must be an object"
    samples = [wall.get(name) for name in ("min", "median", "max")]
    if any(
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
        for value in samples
    ):
        return "bench wall_time_ms min/median/max must be finite nonnegative numbers"
    if samples != sorted(samples):
        return "bench wall_time_ms must satisfy min <= median <= max"
    return None


def summarize_bench(parsed: dict[str, Any] | None) -> dict[str, Any]:
    if parsed is None:
        return {}
    if parsed.get("workload") == "cspa_incr":
        reeval_ms = parsed.get("reeval_ms")
        return {
            "bench_workload": parsed.get("workload"),
            "tuples": parsed.get("tuples_after"),
            "iterations": parsed.get("iters_after"),
            "peak_rss_kb": parsed.get("peak_rss_kb"),
            "median_ms": reeval_ms,
            "reeval_ms": reeval_ms,
            "speedup": parsed.get("speedup"),
        }
    wall = parsed.get("wall_time_ms")
    median_ms = wall.get("median") if isinstance(wall, dict) else None
    return {
        "bench_workload": parsed.get("workload"),
        "tuples": parsed.get("tuples"),
        "iterations": parsed.get("iterations"),
        "peak_rss_kb": parsed.get("peak_rss_kb"),
        "median_ms": median_ms,
        # Issue #1380: session-accounted peak next to the OS peak so the
        # nightly artifact carries the memory baseline per (workload, W).
        "ledger_peak_bytes": parsed.get("ledger_peak_bytes"),
        "ledger_worker_peak_max_bytes": parsed.get("ledger_worker_peak_max_bytes"),
        "ledger_subsys_peak_bytes": parsed.get("ledger_subsys_peak_bytes"),
    }


def make_skip_record(
    workload: str,
    workers: int,
    repeat: int,
    command: list[str] | None,
    reason: str,
    detail: Any = None,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "skip",
        "reason": reason,
        "detail": detail,
        "workload": workload,
        "workers": workers,
        "repeat": repeat,
        "command": command,
        "started_at": utc_now(),
        "ended_at": utc_now(),
        "duration_sec": 0.0,
        "return_code": None,
        "bench": None,
        "summary": {},
    }


def run_one(
    bench: Path,
    data_root: Path,
    workload: str,
    workers: int,
    repeat: int,
) -> dict[str, Any]:
    spec = WORKLOADS.get(workload)
    command = build_command(bench, data_root, workload, workers, repeat)
    if command is None:
        return make_skip_record(workload, workers, repeat, None, "unknown_workload")

    if not bench.exists():
        return make_skip_record(
            workload,
            workers,
            repeat,
            command,
            "missing_bench_binary",
            str(bench),
        )
    if not bench.is_file() or not bench.stat().st_mode & 0o111:
        return make_skip_record(
            workload,
            workers,
            repeat,
            command,
            "bench_binary_not_executable",
            str(bench),
        )

    missing = missing_data_paths(data_root, workload)
    if missing:
        return make_skip_record(
            workload,
            workers,
            repeat,
            command,
            "missing_data",
            missing,
        )

    started = utc_now()
    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout = proc.stdout.decode("utf-8", errors="replace")
        stderr = proc.stderr.decode("utf-8", errors="replace")
        duration = time.monotonic() - t0
        ended = utc_now()
    except OSError as exc:
        duration = time.monotonic() - t0
        return {
            "schema_version": SCHEMA_VERSION,
            "status": "fail",
            "reason": "exec_error",
            "detail": str(exc),
            "workload": workload,
            "workers": workers,
            "repeat": repeat,
            "command": command,
            "started_at": started,
            "ended_at": utc_now(),
            "duration_sec": round(duration, 6),
            "return_code": None,
            "bench": None,
            "summary": {},
        }

    parser = spec.get("parser", "json") if spec else "json"
    parsed, parse_error, parse_reason = parse_bench_output(parser, stdout)
    if parse_error is None and isinstance(parsed, dict):
        if parser == "json":
            parse_error = validate_bench_json(parsed, workload, workers, repeat)
        elif parsed.get("workload") != "cspa_incr":
            parse_error = "bench workload identity mismatch: expected 'cspa_incr'"
        if parse_error is not None:
            parse_reason = "invalid_bench_record"
    bench_status = parsed.get("bench_status") if isinstance(parsed, dict) else None
    parser_ok = parse_error is None and (bench_status in (None, "OK"))
    status = "ok" if proc.returncode == 0 and parser_ok else "fail"
    reason = None
    if proc.returncode != 0:
        reason = "bench_failed"
    elif parse_error is not None:
        reason = parse_reason
    elif bench_status not in (None, "OK"):
        reason = "bench_status_not_ok"

    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "reason": reason,
        "detail": parse_error,
        "workload": workload,
        "workers": workers,
        "repeat": repeat,
        "command": command,
        "started_at": started,
        "ended_at": ended,
        "duration_sec": round(duration, 6),
        "return_code": proc.returncode,
        "bench": parsed,
        "summary": summarize_bench(parsed),
        "stderr_tail": stderr[-4000:] if stderr else "",
        "stdout_tail": stdout[-4000:] if status != "ok" else "",
        "raw_stdout": stdout,
        "raw_stderr": stderr,
        "raw_stdout_base64": base64.b64encode(proc.stdout).decode("ascii"),
        "raw_stderr_base64": base64.b64encode(proc.stderr).decode("ascii"),
    }


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
            f.write("\n")


def append_jsonl_durable(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as f:
        json.dump(value, f, indent=2, sort_keys=True)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    temporary.replace(path)


def tsv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value).replace("\t", " ").replace("\n", " ")


def write_tsv(path: Path, records: list[dict[str, Any]]) -> None:
    columns = [
        "status",
        "reason",
        "workload",
        "workers",
        "repeat",
        "return_code",
        "duration_sec",
        "median_ms",
        "tuples",
        "iterations",
        "peak_rss_kb",
        "ledger_peak_bytes",
        "ledger_worker_peak_max_bytes",
        "command",
    ]
    with path.open("w", encoding="utf-8") as f:
        f.write("\t".join(columns))
        f.write("\n")
        for record in records:
            summary = record.get("summary") or {}
            row = {
                "status": record.get("status"),
                "reason": record.get("reason"),
                "workload": record.get("workload"),
                "workers": record.get("workers"),
                "repeat": record.get("repeat"),
                "return_code": record.get("return_code"),
                "duration_sec": record.get("duration_sec"),
                "median_ms": summary.get("median_ms"),
                "tuples": summary.get("tuples"),
                "iterations": summary.get("iterations"),
                "peak_rss_kb": summary.get("peak_rss_kb"),
                "ledger_peak_bytes": summary.get("ledger_peak_bytes"),
                "ledger_worker_peak_max_bytes": summary.get(
                    "ledger_worker_peak_max_bytes"
                ),
                "command": " ".join(record.get("command") or []),
            }
            f.write("\t".join(tsv_value(row[col]) for col in columns))
            f.write("\n")


def flag_worker_result_mismatches(records: list[dict[str, Any]]) -> None:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for record in records:
        if record.get("status") == "ok":
            grouped.setdefault((record["workload"], record["repeat"]), []).append(record)
    for (workload, repeat), group in grouped.items():
        signatures = {
            (record["summary"].get("tuples"), record["summary"].get("iterations"))
            for record in group
        }
        missing_counts = any(None in signature for signature in signatures)
        if len(group) > 1 and (missing_counts or len(signatures) != 1):
            detail = "tuple/iteration counts differ across worker counts"
            for record in group:
                record["status"] = "fail"
                record["reason"] = "worker_result_mismatch"
                record["detail"] = detail


def build_manifest(
    args: argparse.Namespace,
    repo_root: Path,
    data_root: Path,
    out_dir: Path,
    workloads: list[str],
    started_at: str,
    ended_at: str,
    duration_sec: float,
    records: list[dict[str, Any]],
    run_status: str,
) -> dict[str, Any]:
    status_counts = Counter(record["status"] for record in records)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "wirelog-flowlog-portfolio",
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_sec": round(duration_sec, 6),
        "run_status": run_status,
        "run_complete": run_status == "complete",
        "repo": git_metadata(repo_root),
        "repo_root": str(repo_root),
        "data_root": str(data_root),
        "output_dir": str(out_dir),
        "bench_path": str(args.bench),
        "bench_exists": args.bench.exists(),
        "bench_executable": args.bench.is_file() and bool(args.bench.stat().st_mode & 0o111)
        if args.bench.exists()
        else False,
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "processor": platform.processor(),
        },
        "python": {
            "version": platform.python_version(),
            "executable": sys.executable,
        },
        "args": {
            "tier": args.tier,
            "workers": args.workers,
            "repeat": args.repeat,
            "workloads": args.workload,
        },
        "tier_definition": tier_definition(args.tier, workloads),
        "record_count": len(records),
        "status_counts": dict(sorted(status_counts.items())),
        "artifacts": {
            "manifest": "manifest.json",
            "portfolio_jsonl": "portfolio.jsonl",
            "portfolio_tsv": "portfolio.tsv",
            "failures_jsonl": "failures.jsonl",
            "progress_jsonl": "progress.jsonl",
        },
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run bench_flowlog portfolio workloads and write manifest.json, "
            "portfolio.jsonl, portfolio.tsv, and failures.jsonl artifacts."
        )
    )
    parser.add_argument(
        "--bench",
        type=Path,
        default=Path("build/bench/bench_flowlog"),
        help="bench_flowlog binary path (default: build/bench/bench_flowlog)",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="repository root used for git metadata and default data root",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help="bench data root (default: REPO_ROOT/bench/data)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("perf-artifacts/portfolio"),
        help="artifact output directory (default: perf-artifacts/portfolio)",
    )
    parser.add_argument(
        "--workers",
        type=parse_workers,
        default=parse_workers(DEFAULT_WORKERS),
        help=f"comma-separated worker counts (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--repeat",
        type=positive_int,
        default=5,
        help="bench_flowlog repeat count for each command (default: 5)",
    )
    parser.add_argument(
        "--tier",
        choices=["smoke", "light", "readme-full", "full"],
        default="smoke",
        help=(
            "workload tier: smoke, light, or readme-full/full. "
            "Heavy README-scale runs are explicit. (default: smoke)"
        ),
    )
    parser.add_argument(
        "--workload",
        action="append",
        default=None,
        help="override tier with one workload name; may be repeated",
    )
    args = parser.parse_args(argv)

    args.repo_root = args.repo_root.resolve()
    args.data_root = (
        args.data_root.resolve() if args.data_root else args.repo_root / "bench" / "data"
    )
    args.out_dir = args.out_dir.resolve()
    args.bench = args.bench.resolve()
    if args.tier == "full":
        args.tier = "readme-full"
    return args


def selected_workloads(args: argparse.Namespace) -> list[str]:
    if args.workload:
        workloads: list[str] = []
        for name in args.workload:
            if name not in workloads:
                workloads.append(name)
        return workloads
    return list(TIERS[args.tier]["workloads"])


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    workloads = selected_workloads(args)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    artifact_names = (
        "manifest.json",
        "manifest.json.tmp",
        "portfolio.jsonl",
        "portfolio.tsv",
        "failures.jsonl",
        "progress.jsonl",
    )
    existing = [name for name in artifact_names if (args.out_dir / name).exists()]
    if existing:
        print(
            f"refusing to overwrite existing portfolio artifacts in {args.out_dir}: "
            + ", ".join(existing),
            file=sys.stderr,
        )
        return 2

    started_at = utc_now()
    t0 = time.monotonic()
    records: list[dict[str, Any]] = []
    progress_path = args.out_dir / "progress.jsonl"
    progress_path.write_text("", encoding="utf-8")
    write_json_atomic(
        args.out_dir / "manifest.json",
        build_manifest(
            args, args.repo_root, args.data_root, args.out_dir, workloads,
            started_at, started_at, 0.0, records, "running",
        ),
    )
    for workload in workloads:
        for workers in args.workers:
            record = run_one(args.bench, args.data_root, workload, workers, args.repeat)
            records.append(record)
            append_jsonl_durable(progress_path, record)
            print(
                f"{record['status']}: workload={workload} workers={workers} "
                f"reason={record.get('reason') or '-'}",
                file=sys.stderr,
            )

    flag_worker_result_mismatches(records)

    ended_at = utc_now()
    duration_sec = time.monotonic() - t0
    failures = [record for record in records if record["status"] != "ok"]
    manifest = build_manifest(
        args,
        args.repo_root,
        args.data_root,
        args.out_dir,
        workloads,
        started_at,
        ended_at,
        duration_sec,
        records,
        "complete",
    )

    write_jsonl(args.out_dir / "portfolio.jsonl", records)
    write_tsv(args.out_dir / "portfolio.tsv", records)
    write_jsonl(args.out_dir / "failures.jsonl", failures)
    write_json_atomic(args.out_dir / "manifest.json", manifest)

    if any(record["status"] == "fail" for record in records):
        return 1
    if any(record["status"] == "skip" for record in records):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
