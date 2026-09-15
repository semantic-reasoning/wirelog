#!/usr/bin/env python3
"""Run one corpus-continuous hosted libFuzzer campaign shard."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from campaign_common import TARGETS, corpus_digest  # noqa: E402


BINARIES = {
    "parser": "parser_fuzz",
    "csv_reader": "csv_reader_fuzz",
    "intern": "intern_fuzz",
    "compound_arena": "compound_arena_fuzz",
}


def timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--duration-seconds", type=int, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", type=int, required=True)
    return parser.parse_args()


def kill_process_group(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def run_target(args: argparse.Namespace, target: str) -> dict[str, object]:
    input_corpus = args.input_root / target / "corpus"
    target_root = args.output_root / target
    output_corpus = target_root / "corpus"
    artifacts = target_root / "artifacts"
    logs = target_root / "logs"
    log_file = logs / "libfuzzer.log"

    if not input_corpus.is_dir():
        raise RuntimeError(f"missing input corpus for {target}: {input_corpus}")
    binary = args.build_dir / "tests" / BINARIES[target]
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError(f"missing fuzz binary for {target}: {binary}")

    output_corpus.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(input_corpus, output_corpus)
    artifacts.mkdir(parents=True, exist_ok=True)
    logs.mkdir(parents=True, exist_ok=True)
    input_digest = corpus_digest(input_corpus)
    start = time.time()
    start_timestamp = timestamp()
    command = [
        str(binary),
        str(output_corpus),
        f"-max_total_time={args.duration_seconds}",
        f"-artifact_prefix={artifacts}/",
        "-print_final_stats=1",
    ]
    status = 0
    timed_out = False
    with log_file.open("wb") as log:
        process = subprocess.Popen(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            process.wait(timeout=args.duration_seconds + 300)
            status = process.returncode
        except subprocess.TimeoutExpired:
            timed_out = True
            kill_process_group(process)
            status = 124
    end = time.time()
    end_timestamp = timestamp()
    # Wall time includes process startup and teardown.  Never credit those
    # seconds as fuzzing time; a successful shard receives at most the amount
    # requested from libFuzzer.
    observed = min(args.duration_seconds, max(0, int(end - start)))
    crash_count = sum(1 for path in artifacts.rglob("*") if path.is_file())
    output_digest = corpus_digest(output_corpus)
    return {
        "schema": "wirelog.fuzz-campaign/1",
        "campaign_id": args.campaign_id,
        "target": target,
        "shard_index": args.shard_index,
        "shard_count": args.shard_count,
        "commit": args.commit,
        "run_id": args.run_id,
        "run_attempt": args.run_attempt,
        "requested_fuzz_seconds": args.duration_seconds,
        "observed_fuzz_seconds": observed,
        "exit_status": status,
        "timed_out": timed_out,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "input_corpus_sha256": input_digest,
        "output_corpus_sha256": output_digest,
        "crash_artifact_count": crash_count,
        "log_file": str(log_file.relative_to(args.output_root)),
    }


def main() -> int:
    args = parse_args()
    if args.shard_index < 1 or args.shard_index > args.shard_count:
        raise SystemExit("shard index must be within shard count")
    if args.shard_count < 1 or args.duration_seconds < 1:
        raise SystemExit("shard count and duration must be positive")
    args.output_root.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, object]] = []
    overall = 0
    for target in TARGETS:
        try:
            result = run_target(args, target)
        except Exception as error:  # keep machine-readable evidence on failure
            result = {
                "schema": "wirelog.fuzz-campaign/1",
                "campaign_id": args.campaign_id,
                "target": target,
                "shard_index": args.shard_index,
                "shard_count": args.shard_count,
                "commit": args.commit,
                "run_id": args.run_id,
                "run_attempt": args.run_attempt,
                "requested_fuzz_seconds": args.duration_seconds,
                "observed_fuzz_seconds": 0,
                "exit_status": 1,
                "timed_out": False,
                "error": str(error),
                "crash_artifact_count": 0,
            }
            overall = 1
        if result.get("exit_status") != 0 or result.get("timed_out"):
            overall = 1
        results.append(result)
        (args.output_root / "metadata").mkdir(parents=True, exist_ok=True)
        (args.output_root / "metadata" / f"{target}.json").write_text(
            json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
    manifest = {
        "schema": "wirelog.fuzz-campaign-shard/1",
        "campaign_id": args.campaign_id,
        "shard_index": args.shard_index,
        "shard_count": args.shard_count,
        "commit": args.commit,
        "run_id": args.run_id,
        "run_attempt": args.run_attempt,
        "overall_exit_status": overall,
        "targets": results,
    }
    (args.output_root / "shard-manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    return overall


if __name__ == "__main__":
    raise SystemExit(main())
