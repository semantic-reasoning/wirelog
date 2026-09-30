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
        path.write_text(
            "#!/bin/sh\n"
            "case \"$*\" in\n"
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

    def probe(self, name="probe", wrong=False):
        path = self.root / name
        record = {"schema_version": 1, "measurement": "crdt_perf_gate_single_run",
                  "workload": "crdt", "fixture": "full", "workers": 1,
                  "elapsed_ms": 10.0, "result": 0 if wrong else 104851,
                  "expected": 104851, "aggregate": 2152328,
                  "iterations": 14148, "status": "OK"}
        path.write_text("#!/bin/sh\nprintf '%s\\n' '" + json.dumps(record) + "'\n", encoding="utf-8")
        path.chmod(0o755)
        return path

    def args(self, base, candidate, samples=1, wrong=False):
        return argparse.Namespace(
            base_sha=SHA_BASE, candidate_sha=SHA_CANDIDATE,
            base_binary=base, candidate_binary=candidate,
            base_crdt_binary=self.probe("base-probe", wrong),
            candidate_crdt_binary=self.probe("candidate-probe", wrong),
            base_data_root=self.root / "data", candidate_data_root=self.root / "data", out_dir=self.root / "evidence",
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
        row = "cspa\t-\t-\t1\t1\t10.0\t10.0\t10.0\t100\t20381\t6\tOK"
        self.assertEqual(MODULE["parse_tsv"](header + "\n" + row, "cspa-fast")["elapsed_ms"], 10.0)
        with self.assertRaisesRegex(ValueError, "wrong result"):
            MODULE["parse_tsv"](header + "\n" + row.replace("20381", "0"), "cspa-fast")
        with self.assertRaisesRegex(ValueError, "nonpositive or nonfinite"):
            MODULE["parse_tsv"](header + "\n" + row.replace("10.0", "nan"), "cspa-fast")
        with self.assertRaisesRegex(ValueError, "malformed"):
            MODULE["parse_tsv"](row, "cspa-fast")

    def test_crdt_bench_tsv_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "requires the gate single-run JSON probe"):
            MODULE["parse_tsv"]("anything", "crdt")

    def test_probe_parser_is_strict(self):
        record = json.loads(self.probe().read_text(encoding="utf-8").split("'", 3)[3].rsplit("'", 1)[0])
        parse = MODULE["parse_crdt_probe"]
        self.assertEqual(parse(json.dumps(record))["elapsed_ms"], 10.0)
        for key, value in (("result", 0), ("expected", 0), ("iterations", 0),
                           ("workers", True), ("schema_version", 2),
                           ("fixture", "small"), ("status", "FAIL"),
                           ("elapsed_ms", float("nan")), ("elapsed_ms", float("inf")),
                           ("elapsed_ms", 0), ("aggregate", True)):
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                parse(json.dumps(dict(record, **{key: value})))
        for text in ("{}", "[]", "bad", json.dumps(record) + "\n{}",
                     json.dumps(dict(record, extra=1)),
                     json.dumps(record).replace('"workers": 1', '"workers": 1, "workers": 1')):
            with self.subTest(text=text), self.assertRaises(ValueError):
                parse(text)

    def test_nonzero_probe_is_failure_even_with_valid_stdout(self):
        binary = self.binary()
        args = self.args(binary, binary)
        with args.base_crdt_binary.open("a", encoding="utf-8") as stream:
            stream.write("exit 9\n")
        summary = MODULE["collect"](args)
        self.assertEqual(summary["status"], "CORRECTNESS_FAILURE")
        event = json.loads((args.out_dir / "attempts.jsonl").read_text(encoding="utf-8").splitlines()[0])
        self.assertEqual(event["exit_code"], 9)
        self.assertIn('"status": "OK"', event["stdout"])

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
        metadata = json.loads((self.root / "evidence" / "metadata.json").read_text(encoding="utf-8"))
        self.assertEqual(metadata["binary_sha256"]["base"]["crdt"],
                         MODULE["sha256"](self.root / "base-probe"))
        self.assertEqual(metadata["binary_sha256"]["base"]["cspa-fast"],
                         MODULE["sha256"](binary))
        self.assertTrue(all("--workload" not in event["command"]
                            for event in events if event["workload"] == "crdt"))
        self.assertTrue(all("--workload" in event["command"]
                            for event in events if event["workload"] == "cspa-fast"))

    def test_each_probe_uses_revision_local_fixture(self):
        binary = self.binary()
        args = self.args(binary, binary)
        candidate_data = self.root / "candidate-data"
        for relative in MODULE["FIXTURES"]:
            path = candidate_data / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture\n", encoding="utf-8")
        args.candidate_data_root = candidate_data
        for side in ("base", "candidate"):
            probe = getattr(args, side + "_crdt_binary")
            expected = getattr(args, side + "_data_root") / "crdt"
            text = probe.read_text(encoding="utf-8").replace("#!/bin/sh\n", "#!/bin/sh\n"
                + f'[ "$WIRELOG_CRDT_DATA_DIR" = "{expected}" ] || exit 8\n'
                + '[ "$WIRELOG_CRDT_PROBE" = 1 ] || exit 8\n'
                + '[ "$WIRELOG_CRDT_SMALL" = 0 ] || exit 8\n')
            probe.write_text(text, encoding="utf-8")
        self.assertEqual(MODULE["collect"](args)["status"], "DIAGNOSTIC")

    def test_mismatched_revision_fixture_rejected_before_collection(self):
        binary = self.binary()
        args = self.args(binary, binary)
        candidate_data = self.root / "candidate-data"
        for relative in MODULE["FIXTURES"]:
            path = candidate_data / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("changed\n", encoding="utf-8")
        args.candidate_data_root = candidate_data
        with self.assertRaisesRegex(ValueError, "fixture hashes differ"):
            MODULE["collect"](args)
        self.assertFalse(args.out_dir.exists())

    def test_wrong_result_is_failure_and_durable(self):
        binary = self.binary(wrong=True)
        summary = MODULE["collect"](self.args(binary, binary, wrong=True))
        self.assertEqual(summary["status"], "CORRECTNESS_FAILURE")
        self.assertIn("result", summary["reason"])
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
