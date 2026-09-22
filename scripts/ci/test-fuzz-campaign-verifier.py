#!/usr/bin/env python3
"""Fixture coverage for the hosted fuzz campaign verifier."""

from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FUZZ = ROOT / "scripts" / "fuzz"
VERIFY = FUZZ / "verify-campaign.py"
COMMON = FUZZ / "campaign_common.py"
TARGETS = ("parser", "csv_reader", "intern", "compound_arena")
SHARDS = 24
COMMIT = "0123456789abcdef0123456789abcdef01234567"


def load_common():
    spec = importlib.util.spec_from_file_location("campaign_common", COMMON)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


common = load_common()


class CampaignVerifierFixtures(unittest.TestCase):
    def make_fixture(self):
        temp = Path(tempfile.mkdtemp(prefix="wirelog-fuzz-campaign."))
        self.addCleanup(shutil.rmtree, temp)
        seed = temp / "seed"
        evidence = temp / "evidence"
        seed.mkdir()
        for target in TARGETS:
            target_seed = seed / target
            target_seed.mkdir()
            (target_seed / "seed").write_text(f"{target}\n", encoding="utf-8")
        previous = {
            target: common.corpus_digest(seed / target) for target in TARGETS
        }
        for shard in range(1, SHARDS + 1):
            output_root = evidence / f"shard-{shard:02d}" / "shard-output"
            metadata_root = output_root / "metadata"
            metadata_root.mkdir(parents=True)
            for target in TARGETS:
                corpus = output_root / target / "corpus"
                corpus.mkdir(parents=True)
                (corpus / "seed").write_text(
                    f"{target}-{shard}\n", encoding="utf-8"
                )
                output_digest = common.corpus_digest(corpus)
                record = {
                    "schema": "wirelog.fuzz-campaign/1",
                    "campaign_id": "fixture-campaign",
                    "target": target,
                    "shard_index": shard,
                    "shard_count": SHARDS,
                    "commit": COMMIT,
                    "run_id": "12345",
                    "run_attempt": 1,
                    "requested_fuzz_seconds": 3600,
                    "observed_fuzz_seconds": 3600,
                    "exit_status": 0,
                    "timed_out": False,
                    "input_corpus_sha256": previous[target],
                    "output_corpus_sha256": output_digest,
                    "crash_artifact_count": 0,
                }
                (metadata_root / f"{target}.json").write_text(
                    json.dumps(record), encoding="utf-8"
                )
                previous[target] = output_digest
        return temp, seed, evidence

    def verify(self, temp, seed, evidence, mutate=None):
        if mutate:
            mutate(evidence)
        report = temp / "report.json"
        command = [
            "python3", str(VERIFY),
            "--evidence-root", str(evidence),
            "--seed-root", str(seed),
            "--campaign-id", "fixture-campaign",
            "--shard-count", str(SHARDS),
            "--duration-seconds", "3600",
            "--commit", COMMIT,
            "--run-id", "12345",
            "--run-attempt", "1",
            "--report", str(report),
        ]
        result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8")
        return result.returncode, json.loads(report.read_text(encoding="utf-8"))

    def test_complete_campaign(self):
        temp, seed, evidence = self.make_fixture()
        status, report = self.verify(temp, seed, evidence)
        self.assertEqual(status, 0)
        self.assertEqual(report["overall_exit_status"], 0)

    def test_missing_shard(self):
        temp, seed, evidence = self.make_fixture()
        self.assert_failure(temp, seed, evidence, lambda root: (
            (root / "shard-24" / "shard-output" / "metadata" / "parser.json").unlink()
        ), "missing shard/target coverage")

    def test_failed_shard(self):
        temp, seed, evidence = self.make_fixture()
        self.assert_failure(temp, seed, evidence, lambda root: self.edit(
            root / "shard-03" / "shard-output" / "metadata" / "intern.json",
            exit_status=1,
        ), "nonzero exit status")

    def test_cancelled_shard(self):
        temp, seed, evidence = self.make_fixture()
        self.assert_failure(temp, seed, evidence, lambda root: self.edit(
            root / "shard-04" / "shard-output" / "metadata" / "csv_reader.json",
            timed_out=True,
        ), "timed out or cancelled")

    def test_crash_artifact(self):
        temp, seed, evidence = self.make_fixture()
        self.assert_failure(temp, seed, evidence, lambda root: self.edit(
            root / "shard-05" / "shard-output" / "metadata" / "parser.json",
            crash_artifact_count=1,
        ), "crash artifacts present")

    def test_mixed_commit(self):
        temp, seed, evidence = self.make_fixture()
        self.assert_failure(temp, seed, evidence, lambda root: self.edit(
            root / "shard-06" / "shard-output" / "metadata" / "parser.json",
            commit="fedcba9876543210fedcba9876543210fedcba98",
        ), "commit does not match campaign")

    def test_digest_mismatch(self):
        temp, seed, evidence = self.make_fixture()
        self.assert_failure(temp, seed, evidence, lambda root: self.edit(
            root / "shard-07" / "shard-output" / "metadata" / "intern.json",
            input_corpus_sha256="0" * 64,
        ), "input corpus digest does not continue prior output")

    def test_duplicate_attempt(self):
        temp, seed, evidence = self.make_fixture()

        def duplicate(root):
            source = root / "shard-08" / "shard-output" / "metadata" / "parser.json"
            shutil.copyfile(source, source.with_name("parser-retry.json"))

        self.assert_failure(temp, seed, evidence, duplicate, "duplicate attempt")

    def test_86399_seconds_is_not_enough(self):
        temp, seed, evidence = self.make_fixture()

        def shorten(root):
            self.edit(
                root / "shard-24" / "shard-output" / "metadata" / "parser.json",
                observed_fuzz_seconds=3599,
            )

        self.assert_failure(temp, seed, evidence, shorten, "target=parser: only 86399")

    @staticmethod
    def edit(path: Path, **changes):
        record = json.loads(path.read_text(encoding="utf-8"))
        record.update(changes)
        path.write_text(json.dumps(record), encoding="utf-8")

    def assert_failure(self, temp, seed, evidence, mutate, message):
        status, report = self.verify(temp, seed, evidence, mutate)
        self.assertNotEqual(status, 0)
        self.assertEqual(report["overall_exit_status"], 1)
        self.assertTrue(any(message in error for error in report["errors"]))


if __name__ == "__main__":
    unittest.main()
