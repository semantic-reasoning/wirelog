#!/usr/bin/env bash
#
# Copyright (C) CleverPlant
# Licensed under LGPL-3.0
# For commercial licenses, contact: inquiry@cleverplant.com
#
# Run one hosted libFuzzer target for one bounded shard and emit verifier
# schema v2 evidence for that shard.

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/../.." && pwd -P)

if command -v python3 >/dev/null 2>&1 \
        && python3 -c 'import sys; raise SystemExit(sys.version_info < (3, 9))'
then
    python=python3
elif command -v python >/dev/null 2>&1 \
        && python -c 'import sys; raise SystemExit(sys.version_info < (3, 9))'
then
    python=python
else
    echo "ERROR: Python 3.9 or newer is required" >&2
    exit 127
fi

WIRELOG_REPO_ROOT=$repo_root exec "$python" - "$@" <<'PY'
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TARGET_BINARIES = {
    "parser": "parser_fuzz",
    "csv_reader": "csv_reader_fuzz",
    "intern": "intern_fuzz",
    "compound_arena": "compound_arena_fuzz",
}

CAMPAIGN_PATH_RE = re.compile(r"[A-Za-z0-9._-]+")
GIT_COMMIT_RE = re.compile(r"[0-9a-f]{40}")
SHA256_RE = re.compile(r"[0-9a-f]{64}")


def load_verifier(repo_root: Path):
    verifier = repo_root / "scripts" / "fuzz" / "verify-hosted-campaign.py"
    spec = importlib.util.spec_from_file_location(
        "wirelog_verify_hosted_campaign", verifier)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load verifier support: {verifier}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parser_error(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(2)


def parse_duration(value: str) -> int:
    if not value:
        parser_error("--duration must not be empty")
    suffix = value[-1]
    if suffix in "smh":
        number = value[:-1]
        scale = {"s": 1, "m": 60, "h": 3600}[suffix]
    else:
        number = value
        scale = 1
    if not number.isdigit():
        parser_error("--duration must be integer seconds or use s/m/h suffix")
    seconds = int(number) * scale
    if seconds <= 0:
        parser_error("--duration must be greater than zero")
    return seconds


def positive_int(value: str, field: str) -> int:
    if not value.isdigit() or int(value) <= 0:
        parser_error(f"{field} must be a positive integer")
    return int(value)


def hash_file_hex(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_tree_sha256(root: Path) -> str:
    files: list[tuple[bytes, Path]] = []
    pending = [root]
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as scan:
            for entry in scan:
                path = Path(entry.path)
                if entry.is_symlink():
                    raise RuntimeError(f"evidence tree contains symlink: {path}")
                if entry.is_dir(follow_symlinks=False):
                    pending.append(path)
                elif entry.is_file(follow_symlinks=False):
                    relative = path.relative_to(root).as_posix().encode("utf-8")
                    files.append((relative, path))
                else:
                    raise RuntimeError(
                        f"evidence tree contains non-regular entry: {path}")
    digest = hashlib.sha256()
    digest.update(b"wirelog-hosted-fuzz-artifact-sha256-v1\0")
    for relative, path in sorted(files, key=lambda item: item[0]):
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(bytes.fromhex(hash_file_hex(path)))
    return digest.hexdigest()


def timestamp() -> datetime:
    return datetime.now(timezone.utc)


def format_timestamp(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def chmod_read_only(path: Path) -> None:
    for current, dirs, files in os.walk(path):
        current_path = Path(current)
        for name in files:
            file_path = current_path / name
            file_path.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        for name in dirs:
            dir_path = current_path / name
            dir_path.chmod(
                stat.S_IRUSR | stat.S_IXUSR
                | stat.S_IRGRP | stat.S_IXGRP
                | stat.S_IROTH | stat.S_IXOTH)
    path.chmod(
        stat.S_IRUSR | stat.S_IXUSR
        | stat.S_IRGRP | stat.S_IXGRP
        | stat.S_IROTH | stat.S_IXOTH)


def ensure_writable(path: Path) -> None:
    for current, dirs, files in os.walk(path):
        current_path = Path(current)
        for name in dirs:
            dir_path = current_path / name
            dir_path.chmod(dir_path.stat().st_mode | stat.S_IWUSR)
        for name in files:
            file_path = current_path / name
            file_path.chmod(file_path.stat().st_mode | stat.S_IWUSR)
        current_path.chmod(current_path.stat().st_mode | stat.S_IWUSR)


def relative_to_evidence(path: Path, evidence_root: Path) -> str:
    return path.relative_to(evidence_root).as_posix()


def contains_path(parent: Path, child: Path) -> bool:
    return child == parent or child.is_relative_to(parent)


def paths_overlap(left: Path, right: Path) -> bool:
    return contains_path(left, right) or contains_path(right, left)


def confined_to_evidence_root(
    path: Path, evidence_root: Path, field: str,
) -> Path:
    try:
        resolved = path.resolve(strict=False)
    except (OSError, RuntimeError) as exc:
        parser_error(f"{field} cannot be resolved: {exc}")
    if not contains_path(evidence_root, resolved):
        parser_error(f"{field} must stay within --output-root")
    return resolved


def default_git_commit(repo_root: Path) -> str:
    env_value = os.environ.get("GITHUB_SHA", "")
    if GIT_COMMIT_RE.fullmatch(env_value):
        return env_value
    try:
        value = subprocess.check_output(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        parser_error("--git-commit is required outside a git checkout")
    if not GIT_COMMIT_RE.fullmatch(value):
        parser_error("--git-commit must be a lowercase 40-character hex ID")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run one hosted libFuzzer target shard and emit one JSON evidence "
            "record compatible with scripts/fuzz/verify-hosted-campaign.py."
        ))
    parser.add_argument("--build-dir", required=True)
    parser.add_argument("--target", required=True,
                        choices=sorted(TARGET_BINARIES))
    parser.add_argument("--input-corpus", required=True)
    parser.add_argument("--output-root", required=True,
                        help="evidence root for shard corpora and logs")
    parser.add_argument("--duration", required=True,
                        help="integer seconds, or s/m/h suffix")
    parser.add_argument("--campaign-id", required=True,
                        help="path-safe campaign identifier")
    parser.add_argument("--shard-index", required=True)
    parser.add_argument("--shard-count", required=True)
    parser.add_argument("--git-commit")
    parser.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID", "1"))
    parser.add_argument(
        "--run-attempt", default=os.environ.get("GITHUB_RUN_ATTEMPT", "1"))
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--build-sha256", required=True)
    parser.add_argument("--artifact-name")
    parser.add_argument("--artifact-id")
    parser.add_argument("--artifact-sha256")
    parser.add_argument("--record-output",
                        help="optional path to write the JSON record")
    parser.add_argument("--timeout-grace", default="30",
                        help="seconds allowed after --duration before kill")
    return parser


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.duration_seconds = parse_duration(args.duration)
    args.timeout_grace_seconds = positive_int(args.timeout_grace,
                                              "--timeout-grace")
    args.shard_index_int = positive_int(args.shard_index, "--shard-index")
    args.shard_count_int = positive_int(args.shard_count, "--shard-count")
    args.run_id_int = positive_int(args.run_id, "--run-id")
    args.run_attempt_int = positive_int(args.run_attempt, "--run-attempt")
    if args.shard_index_int > args.shard_count_int:
        parser_error("--shard-index must not exceed --shard-count")
    if not CAMPAIGN_PATH_RE.fullmatch(args.campaign_id):
        parser_error("--campaign-id must match [A-Za-z0-9._-]+")
    if not args.build_id.strip() or args.build_id != args.build_id.strip():
        parser_error("--build-id must be a nonempty trimmed string")
    if not SHA256_RE.fullmatch(args.build_sha256):
        parser_error("--build-sha256 must be a lowercase SHA-256 hex digest")
    if args.artifact_sha256 is not None \
            and not SHA256_RE.fullmatch(args.artifact_sha256):
        parser_error("--artifact-sha256 must be a lowercase SHA-256 hex digest")
    if args.artifact_name is not None and (
            not args.artifact_name.strip()
            or args.artifact_name != args.artifact_name.strip()):
        parser_error("--artifact-name must be a nonempty trimmed string")
    if args.artifact_id is not None:
        args.artifact_id_int = positive_int(args.artifact_id, "--artifact-id")
    else:
        ordinal = sorted(TARGET_BINARIES).index(args.target) + 1
        args.artifact_id_int = ordinal * 1_000_000 + args.shard_index_int
    return args


def count_crash_artifacts(artifact_dir: Path) -> int:
    count = 0
    for path in artifact_dir.rglob("*"):
        if path.is_file() and not path.is_symlink():
            count += 1
    return count


def main(argv: list[str]) -> int:
    repo_root = Path(os.environ["WIRELOG_REPO_ROOT"]).resolve()
    verifier = load_verifier(repo_root)
    args = parse_args(argv)

    build_dir = Path(args.build_dir).resolve()
    input_corpus = Path(args.input_corpus).resolve()
    evidence_root = Path(args.output_root).resolve()

    if not build_dir.is_dir():
        parser_error(f"build directory not found: {build_dir}")
    if not input_corpus.is_dir():
        parser_error(f"input corpus directory not found: {input_corpus}")

    try:
        verifier.canonical_corpus_sha256(input_corpus)
    except verifier.CorpusEvidenceError as exc:
        parser_error(f"input corpus is not valid verifier evidence: {exc}")

    binary = build_dir / "tests" / TARGET_BINARIES[args.target]
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser_error(f"target binary not found or not executable: {binary}")

    git_commit = args.git_commit or default_git_commit(repo_root)
    if not GIT_COMMIT_RE.fullmatch(git_commit):
        parser_error("--git-commit must be a lowercase 40-character hex ID")

    shard_rel = Path(
        "corpora",
        args.campaign_id,
        args.target,
        f"shard-{args.shard_index_int}",
    )
    shard_root = evidence_root / shard_rel
    input_snapshot = shard_root / "input"
    output_corpus = shard_root / "output"
    crash_dir = shard_root / "crashes"
    log_dir = shard_root / "logs"
    log_file = log_dir / "target.log"

    if paths_overlap(input_corpus, output_corpus):
        parser_error(
            "input corpus and shard output corpus must not overlap")
    if shard_root.exists():
        parser_error(f"shard evidence directory already exists: {shard_root}")

    evidence_root.mkdir(parents=True, exist_ok=True)
    evidence_root = evidence_root.resolve(strict=True)
    record_output: Path | None = None
    if args.record_output:
        record_output = Path(args.record_output)
        if not record_output.is_absolute():
            record_output = evidence_root / record_output
        record_output = confined_to_evidence_root(
            record_output, evidence_root, "--record-output")

    log_dir.mkdir(parents=True)
    crash_dir.mkdir()
    shutil.copytree(input_corpus, input_snapshot)
    shutil.copytree(input_snapshot, output_corpus)
    chmod_read_only(input_snapshot)
    ensure_writable(output_corpus)

    artifact_prefix = str(crash_dir) + os.sep
    command = [
        str(binary),
        str(output_corpus),
        f"-max_total_time={args.duration_seconds}",
        f"-artifact_prefix={artifact_prefix}",
    ]
    start = timestamp()
    timed_out = False
    with log_file.open("wb") as log:
        try:
            completed = subprocess.run(
                command,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=args.duration_seconds + args.timeout_grace_seconds,
                check=False,
            )
            exit_status = completed.returncode
        except subprocess.TimeoutExpired:
            timed_out = True
            exit_status = 124
            log.write(b"\nwirelog hosted fuzz helper: target timed out\n")
    end = timestamp()

    crash_count = count_crash_artifacts(crash_dir)
    clean = exit_status == 0 and not timed_out and crash_count == 0
    elapsed_seconds = max(0, int((end - start).total_seconds()))
    observed_seconds = min(args.duration_seconds, elapsed_seconds)
    artifact_name = args.artifact_name or (
        f"fuzz-{args.campaign_id}-{args.target}-"
        f"shard-{args.shard_index_int}-run-{args.run_id_int}-"
        f"attempt-{args.run_attempt_int}"
    )

    input_digest = verifier.canonical_corpus_sha256(input_snapshot)
    output_digest = verifier.canonical_corpus_sha256(output_corpus)
    artifact_digest = args.artifact_sha256 or canonical_tree_sha256(shard_root)
    termination_reason = (
        "completed" if clean else "timeout" if timed_out else "failed")

    record: dict[str, Any] = {
        "schema_version": 2,
        "campaign_id": args.campaign_id,
        "target": args.target,
        "shard_index": args.shard_index_int,
        "shard_count": args.shard_count_int,
        "git_commit": git_commit,
        "run_id": args.run_id_int,
        "run_attempt": args.run_attempt_int,
        "workflow_conclusion": "success" if clean else "failure",
        "job_conclusion": "success" if clean else "failure",
        "termination_reason": termination_reason,
        "requested_fuzz_seconds": args.duration_seconds,
        "observed_fuzz_seconds": observed_seconds,
        "exit_status": exit_status,
        "start_timestamp": format_timestamp(start),
        "end_timestamp": format_timestamp(end),
        "artifact_name": artifact_name,
        "artifact_id": args.artifact_id_int,
        "artifact_sha256": artifact_digest,
        "build_id": args.build_id,
        "build_sha256": args.build_sha256,
        "binary_sha256": hash_file_hex(binary),
        "input_corpus_path": relative_to_evidence(
            input_snapshot, evidence_root),
        "output_corpus_path": relative_to_evidence(
            output_corpus, evidence_root),
        "input_corpus_sha256": input_digest,
        "output_corpus_sha256": output_digest,
        "crash_artifact_count": crash_count,
    }

    payload = json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
    if record_output is not None:
        record_output.parent.mkdir(parents=True, exist_ok=True)
        record_output.write_text(payload, encoding="utf-8")
    sys.stdout.write(payload)

    if clean:
        return 0
    if timed_out:
        return 124
    return exit_status if exit_status != 0 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except BrokenPipeError:
        raise SystemExit(1)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
PY
