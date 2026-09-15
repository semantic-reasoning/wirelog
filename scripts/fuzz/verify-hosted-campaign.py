#!/usr/bin/env python3
"""Verify corpus-continuous hosted libFuzzer campaign evidence.

Corpus digests use the ``wirelog-corpus-sha256-v1`` canonical format.  The
digest starts with that ASCII string followed by a NUL byte.  Each regular
file is then processed in ascending order of its UTF-8 encoded, POSIX-style
path relative to the corpus directory.  For each file, the digest receives
the path byte length as an unsigned 64-bit big-endian integer, the path
bytes, and the 32 raw bytes of the file content's SHA-256 digest.  Directories
do not contribute.  Symlinks and non-regular filesystem entries are rejected.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any


SCHEMA_VERSION = 2
MINIMUM_FUZZ_SECONDS = 86_400
TARGETS = ("parser", "csv_reader", "intern", "compound_arena")
CORPUS_DIGEST_DOMAIN = b"wirelog-corpus-sha256-v1\0"

REQUIRED_FIELDS = (
    "schema_version",
    "campaign_id",
    "target",
    "shard_index",
    "shard_count",
    "git_commit",
    "run_id",
    "run_attempt",
    "workflow_conclusion",
    "job_conclusion",
    "termination_reason",
    "requested_fuzz_seconds",
    "observed_fuzz_seconds",
    "exit_status",
    "start_timestamp",
    "end_timestamp",
    "artifact_name",
    "artifact_id",
    "artifact_sha256",
    "build_id",
    "build_sha256",
    "binary_sha256",
    "input_corpus_path",
    "output_corpus_path",
    "input_corpus_sha256",
    "output_corpus_sha256",
    "crash_artifact_count",
)

GIT_COMMIT_RE = re.compile(r"[0-9a-f]{40}")
SHA256_RE = re.compile(r"[0-9a-f]{64}")


class EvidenceInputError(Exception):
    """Evidence could not be loaded as a collection of shard records."""


class CorpusEvidenceError(Exception):
    """A corpus path cannot be used as trustworthy campaign evidence."""


class JsonArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise EvidenceInputError(message)


def error(code: str, message: str, **context: Any) -> dict[str, Any]:
    return {"code": code, "message": message, **context}


def is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def is_sha256(value: Any) -> bool:
    return isinstance(value, str) and SHA256_RE.fullmatch(value) is not None


def parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except (OverflowError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed


def is_canonical_relative_path(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    path = PurePosixPath(value)
    return (
        not path.is_absolute()
        and path.parts
        and all(part not in ("", ".", "..") for part in path.parts)
        and path.as_posix() == value
    )


def validate_record(record: Any, position: int) -> list[dict[str, Any]]:
    problems: list[dict[str, Any]] = []
    where = {"record": position}
    if not isinstance(record, dict):
        return [error("invalid_record", "shard record must be an object",
                      **where)]

    missing = [field for field in REQUIRED_FIELDS if field not in record]
    if missing:
        problems.append(error(
            "missing_fields",
            "shard record is missing required fields",
            fields=missing,
            **where,
        ))
        return problems

    if record["schema_version"] != SCHEMA_VERSION:
        problems.append(error(
            "unsupported_schema",
            f"schema_version must be {SCHEMA_VERSION}",
            value=record["schema_version"],
            **where,
        ))

    for field in ("campaign_id", "artifact_name", "build_id"):
        value = record[field]
        if not isinstance(value, str) or not value.strip() \
                or value != value.strip():
            problems.append(error(
                f"invalid_{field}",
                f"{field} must be a nonempty trimmed string",
                value=value, **where))

    target = record["target"]
    if target not in TARGETS:
        problems.append(error(
            "invalid_target",
            f"target must be one of: {', '.join(TARGETS)}",
            value=target,
            **where,
        ))

    shard_count = record["shard_count"]
    shard_index = record["shard_index"]
    if not is_int(shard_count) or shard_count <= 0:
        problems.append(error(
            "invalid_shard_count", "shard_count must be a positive integer",
            value=shard_count, **where))
    if not is_int(shard_index) or shard_index <= 0:
        problems.append(error(
            "invalid_shard_index", "shard_index must be a positive integer",
            value=shard_index, **where))
    elif is_int(shard_count) and shard_count > 0 \
            and shard_index > shard_count:
        problems.append(error(
            "invalid_shard_index", "shard_index exceeds shard_count",
            value=shard_index, shard_count=shard_count, **where))

    if not isinstance(record["git_commit"], str) \
            or GIT_COMMIT_RE.fullmatch(record["git_commit"]) is None:
        problems.append(error(
            "invalid_git_commit",
            "git_commit must be a lowercase 40-character hexadecimal ID",
            **where,
        ))

    for field in ("run_id", "run_attempt", "artifact_id",
                  "requested_fuzz_seconds"):
        if not is_int(record[field]) or record[field] <= 0:
            problems.append(error(
                f"invalid_{field}", f"{field} must be a positive integer",
                value=record[field], **where))
    for field in ("observed_fuzz_seconds", "crash_artifact_count"):
        if not is_int(record[field]) or record[field] < 0:
            problems.append(error(
                f"invalid_{field}",
                f"{field} must be a nonnegative integer",
                value=record[field], **where,
            ))
    if not is_int(record["exit_status"]):
        problems.append(error(
            "invalid_exit_status", "exit_status must be an integer",
            value=record["exit_status"], **where))
    elif record["exit_status"] != 0:
        problems.append(error(
            "nonzero_exit_status",
            f"shard exited with status {record['exit_status']}",
            exit_status=record["exit_status"], **where))

    for field in ("artifact_sha256", "build_sha256", "binary_sha256",
                  "input_corpus_sha256", "output_corpus_sha256"):
        if not is_sha256(record[field]):
            problems.append(error(
                f"invalid_{field}",
                f"{field} must be a lowercase SHA-256 hexadecimal digest",
                **where,
            ))

    for field in ("input_corpus_path", "output_corpus_path"):
        if not is_canonical_relative_path(record[field]):
            problems.append(error(
                f"invalid_{field}",
                f"{field} must be a canonical relative POSIX path",
                value=record[field], **where,
            ))

    workflow_conclusion = record["workflow_conclusion"]
    job_conclusion = record["job_conclusion"]
    termination_reason = record["termination_reason"]
    completion_values = (workflow_conclusion, job_conclusion,
                         termination_reason)
    lowered = {
        value.lower() for value in completion_values if isinstance(value, str)
    }
    if lowered & {"cancelled", "canceled"}:
        problems.append(error(
            "campaign_cancelled", "campaign evidence reports cancellation",
            **where))
    if lowered & {"timeout", "timed_out", "timed-out"}:
        problems.append(error(
            "campaign_timed_out", "campaign evidence reports a timeout",
            **where))
    if workflow_conclusion != "success":
        problems.append(error(
            "invalid_workflow_conclusion",
            "workflow_conclusion must be success",
            value=workflow_conclusion, **where))
    if job_conclusion != "success":
        problems.append(error(
            "invalid_job_conclusion", "job_conclusion must be success",
            value=job_conclusion, **where))
    if termination_reason != "completed":
        problems.append(error(
            "invalid_termination_reason",
            "termination_reason must be completed",
            value=termination_reason, **where))

    successful_signals = (
        workflow_conclusion == "success",
        job_conclusion == "success",
        termination_reason == "completed",
        record["exit_status"] == 0,
    )
    if any(successful_signals) and not all(successful_signals):
        problems.append(error(
            "contradictory_completion",
            "workflow, job, termination, and exit status disagree",
            **where,
        ))

    start = parse_timestamp(record["start_timestamp"])
    end = parse_timestamp(record["end_timestamp"])
    if start is None:
        problems.append(error(
            "invalid_start_timestamp",
            "start_timestamp must be an ISO-8601 timestamp with a timezone",
            **where,
        ))
    if end is None:
        problems.append(error(
            "invalid_end_timestamp",
            "end_timestamp must be an ISO-8601 timestamp with a timezone",
            **where,
        ))
    if start is not None and end is not None:
        if end <= start:
            problems.append(error(
                "invalid_timestamp_order",
                "end_timestamp must be later than start_timestamp",
                **where,
            ))
        elif is_int(record["observed_fuzz_seconds"]) \
                and record["observed_fuzz_seconds"] \
                > (end - start).total_seconds():
            problems.append(error(
                "observed_exceeds_elapsed",
                "observed_fuzz_seconds exceeds the shard time interval",
                observed_fuzz_seconds=record["observed_fuzz_seconds"],
                elapsed_seconds=(end - start).total_seconds(),
                **where,
            ))

    if is_int(record["observed_fuzz_seconds"]) \
            and is_int(record["requested_fuzz_seconds"]) \
            and record["observed_fuzz_seconds"] \
            > record["requested_fuzz_seconds"]:
        problems.append(error(
            "observed_exceeds_requested",
            "observed_fuzz_seconds exceeds requested_fuzz_seconds",
            observed_fuzz_seconds=record["observed_fuzz_seconds"],
            requested_fuzz_seconds=record["requested_fuzz_seconds"],
            **where,
        ))
    return problems


def base_report() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "ok": False,
        "campaign_id": None,
        "git_commit": None,
        "run_id": None,
        "run_attempt": None,
        "build_id": None,
        "build_sha256": None,
        "minimum_observed_fuzz_seconds": MINIMUM_FUZZ_SECONDS,
        "targets": {},
        "errors": [],
    }


def sort_errors(problems: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(problems, key=lambda item: (
        item["code"],
        str(item.get("target", "")),
        int(item.get("shard_index", 0)),
        int(item.get("record", 0)),
        item["message"],
    ))


def hash_file(path: Path) -> bytes:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as exc:
        raise CorpusEvidenceError(f"cannot read corpus file {path}: {exc}") \
            from exc
    return digest.digest()


def canonical_corpus_sha256(corpus: Path) -> str:
    """Return the documented canonical SHA-256 for one corpus directory."""
    files: list[tuple[bytes, Path]] = []
    pending = [corpus]
    while pending:
        directory = pending.pop()
        try:
            with os.scandir(directory) as scan:
                entries = list(scan)
        except OSError as exc:
            raise CorpusEvidenceError(
                f"cannot enumerate corpus directory {directory}: {exc}") \
                from exc
        for entry in entries:
            path = Path(entry.path)
            try:
                if entry.is_symlink():
                    raise CorpusEvidenceError(
                        f"corpus contains a symlink: {path}")
                if entry.is_dir(follow_symlinks=False):
                    pending.append(path)
                    continue
                if not entry.is_file(follow_symlinks=False):
                    raise CorpusEvidenceError(
                        f"corpus contains a non-regular entry: {path}")
                relative = path.relative_to(corpus).as_posix().encode("utf-8")
            except (OSError, UnicodeError, ValueError) as exc:
                raise CorpusEvidenceError(
                    f"cannot inspect corpus entry {path}: {exc}") from exc
            files.append((relative, path))

    digest = hashlib.sha256()
    digest.update(CORPUS_DIGEST_DOMAIN)
    for relative, path in sorted(files, key=lambda item: item[0]):
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(hash_file(path))
    return digest.hexdigest()


def corpus_directory(evidence_root: Path, value: str) -> Path:
    lexical = evidence_root.joinpath(*PurePosixPath(value).parts)
    current = evidence_root
    try:
        for part in PurePosixPath(value).parts:
            current = current / part
            if current.is_symlink():
                raise CorpusEvidenceError(
                    f"corpus path traverses a symlink: {value}")
        resolved = lexical.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise CorpusEvidenceError(
            f"corpus directory does not exist: {value}: {exc}") from exc
    if not resolved.is_relative_to(evidence_root):
        raise CorpusEvidenceError(
            f"corpus path escapes evidence root: {value}")
    if not resolved.is_dir():
        raise CorpusEvidenceError(
            f"corpus path is not a directory: {value}")
    return resolved


def verify_corpora(
    records: list[dict[str, Any]], evidence_root: Path,
) -> tuple[dict[tuple[str, int, str], str], list[dict[str, Any]]]:
    actual: dict[tuple[str, int, str], str] = {}
    problems: list[dict[str, Any]] = []
    for position, record in enumerate(records, start=1):
        identity = {
            "record": position,
            "target": record["target"],
            "shard_index": record["shard_index"],
        }
        for role in ("input", "output"):
            path_field = f"{role}_corpus_path"
            digest_field = f"{role}_corpus_sha256"
            try:
                directory = corpus_directory(evidence_root, record[path_field])
                observed = canonical_corpus_sha256(directory)
            except CorpusEvidenceError as exc:
                problems.append(error(
                    "invalid_corpus_evidence", str(exc), corpus_role=role,
                    corpus_path=record[path_field], **identity))
                continue
            actual[(record["target"], record["shard_index"], role)] = observed
            if record[digest_field] != observed:
                problems.append(error(
                    "corpus_hash_mismatch",
                    f"manifest {role} corpus digest does not match evidence",
                    corpus_role=role,
                    corpus_path=record[path_field],
                    expected=observed,
                    actual=record[digest_field],
                    **identity,
                ))
    return actual, problems


def verify(records: list[Any], evidence_root: Path) -> dict[str, Any]:
    report = base_report()
    problems: list[dict[str, Any]] = []
    try:
        root = evidence_root.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        report["errors"] = [error(
            "invalid_evidence_root",
            f"evidence root does not exist: {evidence_root}: {exc}",
        )]
        return report
    if not root.is_dir():
        report["errors"] = [error(
            "invalid_evidence_root",
            f"evidence root is not a directory: {evidence_root}",
        )]
        return report

    for position, record in enumerate(records, start=1):
        problems.extend(validate_record(record, position))
    if problems:
        report["errors"] = sort_errors(problems)
        return report

    typed_records: list[dict[str, Any]] = records
    actual_corpora, corpus_problems = verify_corpora(typed_records, root)
    problems.extend(corpus_problems)

    campaign_ids = {record["campaign_id"] for record in typed_records}
    commits = {record["git_commit"] for record in typed_records}
    attempts = {(record["run_id"], record["run_attempt"])
                for record in typed_records}
    build_identities = {(record["build_id"], record["build_sha256"])
                        for record in typed_records}

    if len(campaign_ids) == 1:
        report["campaign_id"] = next(iter(campaign_ids))
    elif len(campaign_ids) > 1:
        problems.append(error(
            "mixed_campaign",
            "evidence contains more than one campaign_id",
            campaign_ids=sorted(campaign_ids),
        ))

    if len(commits) == 1:
        report["git_commit"] = next(iter(commits))
    elif len(commits) > 1:
        problems.append(error(
            "mixed_git_commit",
            "evidence contains more than one git_commit",
            git_commits=sorted(commits),
        ))

    if len(attempts) == 1:
        report["run_id"], report["run_attempt"] = next(iter(attempts))
    elif len(attempts) > 1:
        problems.append(error(
            "duplicate_run_attempt",
            "evidence contains more than one workflow run/attempt identity",
            run_attempts=[{"run_id": item[0], "run_attempt": item[1]}
                          for item in sorted(attempts)],
        ))

    if len(build_identities) == 1:
        report["build_id"], report["build_sha256"] = \
            next(iter(build_identities))
    elif len(build_identities) > 1:
        problems.append(error(
            "mixed_build_provenance",
            "evidence contains more than one build identity/digest",
            builds=[{"build_id": item[0], "build_sha256": item[1]}
                    for item in sorted(build_identities)],
        ))

    artifact_ids = Counter(record["artifact_id"] for record in typed_records)
    artifact_names = Counter(
        record["artifact_name"] for record in typed_records)
    for artifact_id, count in artifact_ids.items():
        if count > 1:
            problems.append(error(
                "duplicate_artifact_identity",
                "immutable artifact_id appears more than once",
                artifact_id=artifact_id, occurrences=count,
            ))
    for artifact_name, count in artifact_names.items():
        if count > 1:
            problems.append(error(
                "duplicate_artifact_name",
                "artifact_name appears more than once",
                artifact_name=artifact_name, occurrences=count,
            ))

    corpus_paths = Counter(
        record[field]
        for record in typed_records
        for field in ("input_corpus_path", "output_corpus_path")
    )
    for corpus_path, count in corpus_paths.items():
        if count > 1:
            problems.append(error(
                "reused_corpus_path",
                "each shard input/output must be an independent snapshot",
                corpus_path=corpus_path, occurrences=count,
            ))

    shard_counts = {record["shard_count"] for record in typed_records}
    if len(shard_counts) > 1:
        problems.append(error(
            "mixed_shard_count",
            "evidence contains inconsistent shard_count values",
            shard_counts=sorted(shard_counts),
        ))

    for record in typed_records:
        identity = {
            "target": record["target"],
            "shard_index": record["shard_index"],
        }
        if record["crash_artifact_count"] != 0:
            problems.append(error(
                "crash_artifacts",
                "shard produced crash artifacts",
                crash_artifact_count=record["crash_artifact_count"],
                **identity,
            ))

    for target in TARGETS:
        target_records = [record for record in typed_records
                          if record["target"] == target]
        observed = sum(record["observed_fuzz_seconds"]
                       for record in target_records)
        requested = sum(record["requested_fuzz_seconds"]
                        for record in target_records)
        target_counts = {record["shard_count"] for record in target_records}
        expected_count = (next(iter(target_counts))
                          if len(target_counts) == 1 else None)
        observed_indexes = [record["shard_index"]
                            for record in target_records]
        binary_digests = {record["binary_sha256"]
                          for record in target_records}
        report["targets"][target] = {
            "expected_shards": expected_count,
            "observed_shards": sorted(set(observed_indexes)),
            "shard_records": len(target_records),
            "requested_fuzz_seconds": requested,
            "observed_fuzz_seconds": observed,
            "binary_sha256": (next(iter(binary_digests))
                              if len(binary_digests) == 1 else None),
        }

        if not target_records:
            problems.append(error(
                "missing_target", "campaign has no evidence for target",
                target=target))
        elif expected_count is None:
            problems.append(error(
                "mixed_shard_count",
                "target has inconsistent shard_count values",
                target=target,
                shard_counts=sorted(target_counts),
            ))
        else:
            if len(binary_digests) > 1:
                problems.append(error(
                    "mixed_binary_provenance",
                    "target shards report different binary digests",
                    target=target,
                    binary_sha256=sorted(binary_digests),
                ))

            counts = Counter(observed_indexes)
            for shard_index in range(1, expected_count + 1):
                if counts[shard_index] == 0:
                    problems.append(error(
                        "missing_shard", "target shard is missing",
                        target=target, shard_index=shard_index,
                    ))
                elif counts[shard_index] > 1:
                    problems.append(error(
                        "duplicate_shard",
                        "target shard appears more than once",
                        target=target, shard_index=shard_index,
                        occurrences=counts[shard_index],
                    ))

            complete = (set(counts) == set(range(1, expected_count + 1))
                        and all(count == 1 for count in counts.values()))
            if complete:
                ordered = sorted(target_records,
                                 key=lambda record: record["shard_index"])
                for previous, current in zip(ordered, ordered[1:]):
                    previous_end = parse_timestamp(previous["end_timestamp"])
                    current_start = parse_timestamp(current["start_timestamp"])
                    assert previous_end is not None
                    assert current_start is not None
                    if current_start < previous_end:
                        problems.append(error(
                            "overlapping_intervals",
                            "target shard intervals overlap",
                            target=target,
                            shard_index=current["shard_index"],
                            previous_end=previous["end_timestamp"],
                            current_start=current["start_timestamp"],
                        ))

                    previous_digest = actual_corpora.get((
                        target, previous["shard_index"], "output"))
                    current_digest = actual_corpora.get((
                        target, current["shard_index"], "input"))
                    if previous_digest is not None \
                            and current_digest is not None \
                            and previous_digest != current_digest:
                        problems.append(error(
                            "corpus_digest_mismatch",
                            "shard input corpus does not match prior output "
                            "corpus",
                            target=target,
                            shard_index=current["shard_index"],
                            expected=previous_digest,
                            actual=current_digest,
                        ))

        if observed < MINIMUM_FUZZ_SECONDS:
            problems.append(error(
                "insufficient_observed_fuzz_seconds",
                "target has less than 86400 observed fuzz seconds",
                target=target,
                observed_fuzz_seconds=observed,
                required_fuzz_seconds=MINIMUM_FUZZ_SECONDS,
            ))

    report["errors"] = sort_errors(problems)
    report["ok"] = not report["errors"]
    return report


def load_records(paths: list[Path]) -> list[Any]:
    records: list[Any] = []
    for path in paths:
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError) as exc:
            raise EvidenceInputError(f"cannot read {path}: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise EvidenceInputError(
                f"invalid JSON in {path}: line {exc.lineno} column {exc.colno}"
            ) from exc
        if isinstance(document, list):
            records.extend(document)
        else:
            records.append(document)
    return records


def emit(report: dict[str, Any]) -> None:
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))


def main(argv: list[str] | None = None) -> int:
    parser = JsonArgumentParser(description=__doc__)
    parser.add_argument(
        "--evidence-root",
        required=True,
        type=Path,
        help="root containing corpus directories referenced by the manifests",
    )
    parser.add_argument(
        "evidence",
        nargs="+",
        type=Path,
        help="JSON file containing one shard record or an array of records",
    )
    try:
        args = parser.parse_args(argv)
        records = load_records(args.evidence)
    except EvidenceInputError as exc:
        report = base_report()
        report["errors"] = [error("invalid_input", str(exc))]
        emit(report)
        return 2

    report = verify(records, args.evidence_root)
    emit(report)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
