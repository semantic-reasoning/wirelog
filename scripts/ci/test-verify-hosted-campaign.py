#!/usr/bin/env python3
"""Deterministic fixture tests for the hosted fuzz campaign verifier."""

from __future__ import annotations

import contextlib
import copy
import hashlib
import importlib.util
import io
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Callable


SCRIPT_DIR = Path(__file__).resolve().parent
VERIFIER = SCRIPT_DIR.parent / "fuzz" / "verify-hosted-campaign.py"
TARGETS = ("parser", "csv_reader", "intern", "compound_arena")
CORPUS_DIGEST_DOMAIN = b"wirelog-corpus-sha256-v1\0"


def load_verifier():
    spec = importlib.util.spec_from_file_location(
        "verify_hosted_campaign", VERIFIER)
    assert spec is not None and spec.loader is not None, VERIFIER
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


verifier = load_verifier()


def digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def corpus_digest(corpus: Path) -> str:
    files = []
    for path in corpus.rglob("*"):
        if path.is_file():
            relative = path.relative_to(corpus).as_posix().encode("utf-8")
            files.append((relative, path))
    result = hashlib.sha256()
    result.update(CORPUS_DIGEST_DOMAIN)
    for relative, path in sorted(files, key=lambda item: item[0]):
        result.update(len(relative).to_bytes(8, "big"))
        result.update(relative)
        result.update(hashlib.sha256(path.read_bytes()).digest())
    return result.hexdigest()


def write_corpus(root: Path, relative: str, files: dict[str, bytes]) -> str:
    corpus = root / relative
    corpus.mkdir(parents=True)
    for name, content in files.items():
        path = corpus / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return corpus_digest(corpus)


def complete_campaign(root: Path) -> list[dict]:
    records = []
    build_sha256 = digest("build-artifact-content")
    for target_number, target in enumerate(TARGETS, start=1):
        shard_corpora = (
            (
                {"seed": f"{target}:seed".encode()},
                {
                    "seed": f"{target}:seed".encode(),
                    "nested/generated-1": f"{target}:generated:1".encode(),
                },
            ),
            (
                {
                    "nested/generated-1": f"{target}:generated:1".encode(),
                    "seed": f"{target}:seed".encode(),
                },
                {
                    "seed": f"{target}:seed".encode(),
                    "nested/generated-1": f"{target}:generated:1".encode(),
                    "generated-2": f"{target}:generated:2".encode(),
                },
            ),
        )
        for shard_index, (input_files, output_files) in enumerate(
                shard_corpora, start=1):
            base = f"corpora/{target}/shard-{shard_index}"
            input_path = f"{base}/input"
            output_path = f"{base}/output"
            input_sha256 = write_corpus(root, input_path, input_files)
            output_sha256 = write_corpus(root, output_path, output_files)
            start_hour = 0 if shard_index == 1 else 12
            start_minute = 0 if shard_index == 1 else 5
            end_day = 1 if shard_index == 1 else 2
            end_hour = 12 if shard_index == 1 else 0
            end_minute = 0 if shard_index == 1 else 5
            artifact_id = 10_000 + target_number * 10 + shard_index
            records.append({
                "schema_version": 2,
                "campaign_id": "campaign-fixture",
                "target": target,
                "shard_index": shard_index,
                "shard_count": 2,
                "git_commit": "a" * 40,
                "run_id": 123456,
                "run_attempt": 1,
                "workflow_conclusion": "success",
                "job_conclusion": "success",
                "termination_reason": "completed",
                "requested_fuzz_seconds": 43200,
                "observed_fuzz_seconds": 43200,
                "exit_status": 0,
                "start_timestamp": (
                    f"2026-09-01T{start_hour:02d}:{start_minute:02d}:00Z"
                ),
                "end_timestamp": (
                    f"2026-09-{end_day:02d}T{end_hour:02d}:"
                    f"{end_minute:02d}:00Z"
                ),
                "artifact_name": (
                    f"fuzz-campaign-fixture-{target}-shard-{shard_index}-"
                    "run-123456-attempt-1"
                ),
                "artifact_id": artifact_id,
                "artifact_sha256": digest(f"artifact:{artifact_id}"),
                "build_id": "build-artifact-9001",
                "build_sha256": build_sha256,
                "binary_sha256": digest(f"binary:{target}"),
                "input_corpus_path": input_path,
                "output_corpus_path": output_path,
                "input_corpus_sha256": input_sha256,
                "output_corpus_sha256": output_sha256,
                "crash_artifact_count": 0,
            })
    return records


Mutation = Callable[[list[dict], Path], None]


class VerifierFixtures(unittest.TestCase):
    def run_fixture(
        self, mutate: Mutation | None = None,
    ) -> tuple[int, dict]:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            records = complete_campaign(root)
            if mutate is not None:
                mutate(records, root)
            evidence = root / "evidence.json"
            evidence.write_text(json.dumps(records), encoding="utf-8")
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                result = verifier.main([
                    "--evidence-root", str(root), str(evidence),
                ])
        return result, json.loads(stdout.getvalue())

    def assert_failure(self, code: str, mutate: Mutation) -> dict:
        result, report = self.run_fixture(mutate)
        self.assertEqual(result, 1, report)
        self.assertFalse(report["ok"])
        self.assertIn(code, {item["code"] for item in report["errors"]})
        return report

    def test_argument_failure_is_json(self) -> None:
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            result = verifier.main([])
        report = json.loads(stdout.getvalue())
        self.assertEqual(result, 2, report)
        self.assertFalse(report["ok"])
        self.assertEqual(report["errors"][0]["code"], "invalid_input")

    def test_complete_campaign_succeeds(self) -> None:
        result, report = self.run_fixture()
        self.assertEqual(result, 0, report)
        self.assertTrue(report["ok"])
        self.assertEqual(report["errors"], [])
        self.assertEqual(report["campaign_id"], "campaign-fixture")
        self.assertEqual(report["git_commit"], "a" * 40)
        self.assertEqual(report["run_id"], 123456)
        self.assertEqual(report["run_attempt"], 1)
        self.assertEqual(report["build_id"], "build-artifact-9001")
        for target in TARGETS:
            self.assertEqual(
                report["targets"][target]["observed_fuzz_seconds"], 86400)
            self.assertEqual(
                report["targets"][target]["binary_sha256"],
                digest(f"binary:{target}"),
            )

    def test_missing_shard_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[:] = [
                record for record in records
                if not (record["target"] == "parser"
                        and record["shard_index"] == 2)
            ]

        self.assert_failure("missing_shard", mutate)

    def test_duplicate_target_shard_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            duplicate = copy.deepcopy(records[0])
            duplicate["artifact_name"] += "-duplicate"
            duplicate["artifact_id"] += 1_000_000
            records.append(duplicate)

        self.assert_failure("duplicate_shard", mutate)

    def test_failed_shard_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[0]["exit_status"] = 1
            records[0]["job_conclusion"] = "failure"
            records[0]["termination_reason"] = "failed"

        self.assert_failure("nonzero_exit_status", mutate)

    def test_crash_artifact_fails(self) -> None:
        self.assert_failure(
            "crash_artifacts",
            lambda records, _root: records[0].update(
                crash_artifact_count=1),
        )

    def test_mixed_commit_and_campaign_fail(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[0]["git_commit"] = "b" * 40
            records[0]["campaign_id"] = "other-campaign"

        report = self.assert_failure("mixed_git_commit", mutate)
        self.assertIn("mixed_campaign",
                      {item["code"] for item in report["errors"]})

    def test_corpus_handoff_mismatch_fails(self) -> None:
        def mutate(records: list[dict], root: Path) -> None:
            path = root / records[1]["input_corpus_path"] / "seed"
            path.write_bytes(b"not-the-prior-output")
            records[1]["input_corpus_sha256"] = corpus_digest(path.parent)

        self.assert_failure("corpus_digest_mismatch", mutate)

    def test_forged_corpus_digest_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[0]["output_corpus_sha256"] = digest("forged")

        self.assert_failure("corpus_hash_mismatch", mutate)

    def test_altered_corpus_fails(self) -> None:
        def mutate(records: list[dict], root: Path) -> None:
            corpus = root / records[0]["output_corpus_path"]
            (corpus / "seed").write_bytes(b"altered-after-manifest")

        self.assert_failure("corpus_hash_mismatch", mutate)

    def test_missing_corpus_fails(self) -> None:
        def mutate(records: list[dict], root: Path) -> None:
            shutil.rmtree(root / records[0]["input_corpus_path"])

        self.assert_failure("invalid_corpus_evidence", mutate)

    def test_reused_corpus_path_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[1]["input_corpus_path"] = records[0]["output_corpus_path"]

        self.assert_failure("reused_corpus_path", mutate)

    def test_duplicate_workflow_attempt_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[0]["run_attempt"] = 2

        self.assert_failure("duplicate_run_attempt", mutate)

    def test_86399_observed_seconds_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[1]["observed_fuzz_seconds"] = 43199

        report = self.assert_failure(
            "insufficient_observed_fuzz_seconds", mutate)
        parser = report["targets"]["parser"]
        self.assertEqual(parser["observed_fuzz_seconds"], 86399)

    def test_observed_duration_above_request_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[0]["observed_fuzz_seconds"] = 43201

        self.assert_failure("observed_exceeds_requested", mutate)

    def test_negative_observed_duration_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[0]["observed_fuzz_seconds"] = -1

        self.assert_failure("invalid_observed_fuzz_seconds", mutate)

    def test_observed_duration_above_elapsed_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[0]["end_timestamp"] = "2026-09-01T11:59:59Z"

        self.assert_failure("observed_exceeds_elapsed", mutate)

    def test_cancellation_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[0]["workflow_conclusion"] = "cancelled"
            records[0]["job_conclusion"] = "cancelled"
            records[0]["termination_reason"] = "cancelled"

        self.assert_failure("campaign_cancelled", mutate)

    def test_timeout_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[0]["job_conclusion"] = "timed_out"
            records[0]["termination_reason"] = "timeout"

        self.assert_failure("campaign_timed_out", mutate)

    def test_missing_artifact_identity_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            del records[0]["artifact_id"]

        self.assert_failure("missing_fields", mutate)

    def test_binary_and_build_mismatch_fail(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[1]["binary_sha256"] = digest("other-binary")
            records[1]["build_id"] = "build-artifact-9002"
            records[1]["build_sha256"] = digest("other-build")

        report = self.assert_failure("mixed_binary_provenance", mutate)
        self.assertIn("mixed_build_provenance",
                      {item["code"] for item in report["errors"]})

    def test_missing_completion_field_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            del records[0]["job_conclusion"]

        self.assert_failure("missing_fields", mutate)

    def test_contradictory_completion_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[0]["termination_reason"] = "failed"

        self.assert_failure("contradictory_completion", mutate)

    def test_overlapping_intervals_fail(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[1]["start_timestamp"] = "2026-09-01T11:59:59Z"

        self.assert_failure("overlapping_intervals", mutate)

    def test_malformed_interval_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[0]["end_timestamp"] = records[0]["start_timestamp"]

        self.assert_failure("invalid_timestamp_order", mutate)

    def test_old_schema_fails(self) -> None:
        def mutate(records: list[dict], _root: Path) -> None:
            records[0]["schema_version"] = 1

        self.assert_failure("unsupported_schema", mutate)


if __name__ == "__main__":
    unittest.main()
