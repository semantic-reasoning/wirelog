#!/usr/bin/env python3
"""Run bench_flowlog portfolio workloads and write structured artifacts."""

from __future__ import annotations

import argparse
import json
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

    return [
        str(bench),
        "--workload",
        workload,
        *data_args,
        "--workers",
        str(workers),
        "--repeat",
        str(repeat),
        "--format",
        "json",
    ]


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


def summarize_bench(parsed: dict[str, Any] | None) -> dict[str, Any]:
    if parsed is None:
        return {}
    wall = parsed.get("wall_time_ms")
    median_ms = wall.get("median") if isinstance(wall, dict) else None
    return {
        "bench_workload": parsed.get("workload"),
        "tuples": parsed.get("tuples"),
        "iterations": parsed.get("iterations"),
        "peak_rss_kb": parsed.get("peak_rss_kb"),
        "median_ms": median_ms,
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
            text=True,
            encoding="utf-8",
            errors="replace",
        )
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

    parsed, parse_error = parse_bench_json(proc.stdout)
    status = "ok" if proc.returncode == 0 and parse_error is None else "fail"
    reason = None
    if proc.returncode != 0:
        reason = "bench_failed"
    elif parse_error is not None:
        reason = "json_parse_failed"

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
        "stderr_tail": proc.stderr[-4000:] if proc.stderr else "",
        "stdout_tail": proc.stdout[-4000:] if status != "ok" else "",
    }


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
            f.write("\n")


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
                "command": " ".join(record.get("command") or []),
            }
            f.write("\t".join(tsv_value(row[col]) for col in columns))
            f.write("\n")


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
) -> dict[str, Any]:
    status_counts = Counter(record["status"] for record in records)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "wirelog-flowlog-portfolio",
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_sec": round(duration_sec, 6),
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

    started_at = utc_now()
    t0 = time.monotonic()
    records: list[dict[str, Any]] = []
    for workload in workloads:
        for workers in args.workers:
            record = run_one(args.bench, args.data_root, workload, workers, args.repeat)
            records.append(record)
            print(
                f"{record['status']}: workload={workload} workers={workers} "
                f"reason={record.get('reason') or '-'}",
                file=sys.stderr,
            )

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
    )

    (args.out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_jsonl(args.out_dir / "portfolio.jsonl", records)
    write_tsv(args.out_dir / "portfolio.tsv", records)
    write_jsonl(args.out_dir / "failures.jsonl", failures)

    if any(record["status"] == "fail" for record in records):
        return 1
    if any(record["status"] == "skip" for record in records):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
