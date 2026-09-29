#!/usr/bin/env python3
"""Contract tests for paired build and correctness evidence verification."""

import importlib.util
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("verify-paired-builds.py")
SPEC = importlib.util.spec_from_file_location("verify_paired_builds", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def record(sha):
    return {
        "source_sha": sha, "source_tree": sha,
        "profile": {"buildtype": "release", "c_args": []},
        "compiler": {"id": "gcc", "version": "13"},
        "source_sha256": {name: "same" for name in
                          MODULE.SAME_SOURCES + MODULE.GATE_SOURCES},
        "fixture_sha256": {name: "same" for name in MODULE.FIXTURES},
        "binary_sha256": {name: sha for name in MODULE.BINARIES},
    }


class PairedBuildVerification(unittest.TestCase):
    BASE = "a" * 40
    CANDIDATE = "b" * 40

    def run_pair(self, base, candidate):
        with TemporaryDirectory() as tmp:
            output = Path(tmp) / "preflight.json"
            with patch.object(MODULE, "side_record", side_effect=(base, candidate)):
                try:
                    MODULE.verify_pair(Path("base"), Path("build-base"), self.BASE,
                                       Path("candidate"), Path("build-candidate"),
                                       self.CANDIDATE, output)
                except ValueError:
                    return output.read_text(encoding="utf-8")
                return output.read_text(encoding="utf-8")

    def test_matching_contract_accepts_distinct_binaries(self):
        self.assertIn('"status": "comparable"',
                      self.run_pair(record(self.BASE), record(self.CANDIDATE)))

    def test_changed_fixture_rejected_and_recorded(self):
        base, candidate = record(self.BASE), record(self.CANDIDATE)
        candidate["fixture_sha256"][MODULE.FIXTURES[0]] = "changed"
        report = self.run_pair(base, candidate)
        self.assertIn('"status": "rejected"', report)
        self.assertIn("fixture_sha256 differ", report)

    def test_changed_profile_rejected(self):
        base, candidate = record(self.BASE), record(self.CANDIDATE)
        candidate["profile"]["c_args"] = ["-O0"]
        self.assertIn("profile differ", self.run_pair(base, candidate))

    def test_changed_benchmark_source_rejected(self):
        base, candidate = record(self.BASE), record(self.CANDIDATE)
        candidate["source_sha256"][MODULE.SAME_SOURCES[0]] = "changed"
        self.assertIn("benchmark source differs", self.run_pair(base, candidate))

    def test_gate_output_requires_expected_result(self):
        MODULE.check_correctness("crdt", "test_crdt_perf_gate: correctness OK\n"
                                 "result = 104851 (expected 104851)\n"
                                 "iterations = 14148\n")
        MODULE.check_correctness("cspa", "test_cspa_perf_gate: correctness OK "
                                 "(tuples=20381 iters=6)\n")
        with self.assertRaises(ValueError):
            MODULE.check_correctness("crdt", "test_crdt_perf_gate: correctness OK\n"
                                     "result = 104851 (expected 104851)\n"
                                     "iterations = 14147\n")
        with self.assertRaises(ValueError):
            MODULE.check_correctness("cspa", "test_cspa_perf_gate: correctness OK "
                                     "(tuples=20381 iters=5)\n")


if __name__ == "__main__":
    unittest.main()
