#!/usr/bin/env python3
"""Contract tests for the tagged-runner paired diagnostic collector."""

import argparse
import json
import os
from pathlib import Path
import runpy
import tempfile
import unittest


MODULE = runpy.run_path(str(Path(__file__).with_name("paired-benchmark.py")))
SHA_BASE = "a" * 40
SHA_CANDIDATE = "b" * 40


class PairedBenchmarkTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for name in MODULE["FIXTURES"]:
            path = self.root / "data" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture\n", encoding="utf-8")
        self.cpu = min(os.sched_getaffinity(0))

    def binary(self, name="bench", wrong=False):
        path = self.root / name
        crdt_tuples = 0 if wrong else 2152328
        path.write_text(
            "#!/bin/sh\n"
            "case \"$*\" in\n"
            "  *'--workload crdt '*) "
            "printf 'crdt\\t-\\t-\\t1\\t1\\t10.0\\t10.0\\t10.0\\t100\\t"
            f"{crdt_tuples}\\t14148\\tOK\\n' ;;\n"
            "  *'--workload cspa-fast '*) "
            "printf 'cspa\\t-\\t-\\t1\\t1\\t2.0\\t2.0\\t2.0\\t100\\t"
            "20381\\t6\\tOK\\n' ;;\n"
            "  *) exit 3 ;;\n"
            "esac\n",
            encoding="utf-8",
        )
        # The real benchmark prints a header before its TSV row.
        text = path.read_text(encoding="utf-8")
        text = text.replace('case "$*" in',
                            "printf '" + MODULE["HEADER"].replace("\t", "\\t")
                            + "\\n'\ncase \"$*\" in")
        path.write_text(text, encoding="utf-8")
        path.chmod(0o755)
        return path

    def args(self, base, candidate, samples=1):
        return argparse.Namespace(
            base_sha=SHA_BASE, candidate_sha=SHA_CANDIDATE,
            base_binary=base, candidate_binary=candidate,
            data_root=self.root / "data", out_dir=self.root / "evidence",
            cpu=self.cpu, samples=samples, first="base", timeout=5,
        )

    def test_nine_pair_order_is_balanced_and_repeat_can_invert(self):
        order = MODULE["order"](9, "base")
        self.assertEqual(len(order), 18)
        self.assertEqual(sum(side == "base" for _, side in order), 9)
        self.assertEqual(order[:4], [(0, "base"), (0, "candidate"),
                                     (1, "candidate"), (1, "base")])
        self.assertEqual(MODULE["order"](9, "candidate")[0], (0, "candidate"))

    def test_parser_rejects_wrong_result_and_malformed_timing(self):
        header = MODULE["HEADER"]
        row = "crdt\t-\t-\t1\t1\t10.0\t10.0\t10.0\t100\t2152328\t14148\tOK"
        self.assertEqual(MODULE["parse_tsv"](header + "\n" + row, "crdt")["elapsed_ms"], 10.0)
        with self.assertRaisesRegex(ValueError, "wrong result"):
            MODULE["parse_tsv"](header + "\n" + row.replace("2152328", "0"), "crdt")
        with self.assertRaisesRegex(ValueError, "nonpositive or nonfinite"):
            MODULE["parse_tsv"](header + "\n" + row.replace("10.0", "nan"), "crdt")
        with self.assertRaisesRegex(ValueError, "malformed"):
            MODULE["parse_tsv"](row, "crdt")

    def test_success_keeps_every_warmup_and_sample_without_timing_verdict(self):
        binary = self.binary()
        summary = MODULE["collect"](self.args(binary, binary))
        self.assertEqual(summary["status"], "DIAGNOSTIC")
        self.assertEqual(summary["workloads"]["crdt"]["candidate_over_base_median"], 1.0)
        self.assertEqual(summary["workloads"]["cspa-fast"]["base"]["median_ms"], 2.0)
        events = [json.loads(line) for line in
                  (self.root / "evidence" / "attempts.jsonl").read_text(encoding="utf-8").splitlines()]
        self.assertEqual(len(events), 8)
        self.assertEqual([event["phase"] for event in events],
                         ["warmup", "warmup", "sample", "sample"] * 2)
        self.assertTrue(all("host_before" in event and "host_after" in event for event in events))

    def test_wrong_result_is_failure_and_durable(self):
        binary = self.binary(wrong=True)
        summary = MODULE["collect"](self.args(binary, binary))
        self.assertEqual(summary["status"], "CORRECTNESS_FAILURE")
        self.assertIn("wrong result", summary["reason"])
        self.assertEqual(len((self.root / "evidence" / "attempts.jsonl").read_text(
            encoding="utf-8").splitlines()), 1)

    def test_timeout_is_inconclusive_and_durable(self):
        binary = self.binary()
        scope = MODULE["collect"].__globals__
        original = scope["run_once"]
        try:
            scope["run_once"] = lambda *_: {
                "timeout_seconds": 5, "stdout": "", "stderr": "",
                "host_before": {}, "host_after": {},
            }
            summary = MODULE["collect"](self.args(binary, binary))
        finally:
            scope["run_once"] = original
        self.assertEqual(summary["status"], "INCONCLUSIVE")
        event = json.loads((self.root / "evidence" / "attempts.jsonl").read_text(
            encoding="utf-8").splitlines()[0])
        self.assertEqual(event["timeout_seconds"], 5)

    def test_rerun_cannot_replace_existing_evidence(self):
        binary = self.binary()
        args = self.args(binary, binary)
        MODULE["collect"](args)
        path = args.out_dir / "attempts.jsonl"
        original = path.read_bytes()
        with self.assertRaisesRegex(ValueError, "already exists"):
            MODULE["collect"](args)
        self.assertEqual(path.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
