#!/usr/bin/env python3
"""Fail-closed verifier for corpus-continuous hosted fuzz evidence."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from campaign_common import TARGETS, corpus_digest  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-root", type=Path, required=True)
    parser.add_argument("--seed-root", type=Path, required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--duration-seconds", type=int, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", type=int, required=True)
    parser.add_argument("--report", type=Path, required=True)
    return parser.parse_args()


def verify(args: argparse.Namespace) -> dict[str, object]:
    errors: list[str] = []
    records: dict[tuple[int, str], tuple[dict[str, object], Path]] = {}
    for metadata in sorted(args.evidence_root.glob("shard-*/shard-output/metadata/*.json")):
        try:
            record = json.loads(metadata.read_text(encoding="utf-8"))
            key = (int(record["shard_index"]), str(record["target"]))
        except (ValueError, KeyError, json.JSONDecodeError) as error:
            errors.append(f"invalid metadata {metadata}: {error}")
            continue
        if key in records:
            errors.append(f"duplicate attempt for shard={key[0]} target={key[1]}")
        records[key] = (record, metadata.parent.parent)

    expected_keys = {
        (shard, target)
        for shard in range(1, args.shard_count + 1)
        for target in TARGETS
    }
    missing = sorted(expected_keys - records.keys())
    extra = sorted(records.keys() - expected_keys)
    errors.extend(f"missing shard/target coverage: {key}" for key in missing)
    errors.extend(f"unexpected shard/target coverage: {key}" for key in extra)

    seed_digests: dict[str, str] = {}
    for target in TARGETS:
        try:
            seed_digests[target] = corpus_digest(args.seed_root / target)
        except ValueError as error:
            errors.append(str(error))

    target_reports: list[dict[str, object]] = []
    for target in TARGETS:
        previous_output = seed_digests.get(target)
        total_observed = 0
        for shard in range(1, args.shard_count + 1):
            item = records.get((shard, target))
            if item is None:
                continue
            record, output_root = item
            prefix = f"shard={shard} target={target}"
            for field, expected in (
                ("campaign_id", args.campaign_id),
                ("commit", args.commit),
                ("run_id", args.run_id),
                ("run_attempt", args.run_attempt),
                ("shard_count", args.shard_count),
                ("requested_fuzz_seconds", args.duration_seconds),
            ):
                if record.get(field) != expected:
                    errors.append(f"{prefix}: {field} does not match campaign")
            if record.get("exit_status") != 0:
                errors.append(f"{prefix}: nonzero exit status")
            if record.get("timed_out"):
                errors.append(f"{prefix}: timed out or cancelled")
            if record.get("crash_artifact_count") != 0:
                errors.append(f"{prefix}: crash artifacts present")
            try:
                observed = int(record["observed_fuzz_seconds"])
                total_observed += observed
            except (KeyError, TypeError, ValueError):
                errors.append(f"{prefix}: invalid observed duration")
                observed = 0
            input_digest = record.get("input_corpus_sha256")
            if input_digest != previous_output:
                errors.append(f"{prefix}: input corpus digest does not continue prior output")
            corpus = output_root / target / "corpus"
            try:
                actual_output = corpus_digest(corpus)
            except ValueError as error:
                errors.append(f"{prefix}: {error}")
                actual_output = None
            if actual_output != record.get("output_corpus_sha256"):
                errors.append(f"{prefix}: output corpus digest is not reproducible")
            previous_output = record.get("output_corpus_sha256")
        target_reports.append({
            "target": target,
            "observed_fuzz_seconds": total_observed,
            "required_fuzz_seconds": 86400,
            "passed": total_observed >= 86400,
        })
        if total_observed < 86400:
            errors.append(
                f"target={target}: only {total_observed} observed fuzz seconds; 86400 required"
            )

    return {
        "schema": "wirelog.fuzz-campaign-report/1",
        "campaign_id": args.campaign_id,
        "commit": args.commit,
        "run_id": args.run_id,
        "run_attempt": args.run_attempt,
        "shard_count": args.shard_count,
        "targets": target_reports,
        "overall_exit_status": 0 if not errors else 1,
        "errors": errors,
        "verified_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }


def main() -> int:
    args = parse_args()
    report = verify(args)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    return int(report["overall_exit_status"])


if __name__ == "__main__":
    raise SystemExit(main())
