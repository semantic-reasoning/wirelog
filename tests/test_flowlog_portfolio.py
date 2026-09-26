#!/usr/bin/env python3
"""Regression tests for structured FlowLog portfolio result validation."""

from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "perf" / "run-flowlog-portfolio.py"
SPEC = importlib.util.spec_from_file_location("flowlog_portfolio", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
portfolio = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(portfolio)


class PortfolioResultTests(unittest.TestCase):
    def valid_json(self) -> dict[str, object]:
        return {
            "workload": "reach",
            "workers": 8,
            "repeat": 5,
            "tuples": 100,
            "iterations": 98,
            "peak_rss_kb": 3000,
            "wall_time_ms": {"min": 1.0, "median": 1.2, "max": 1.5},
        }

    def test_valid_json_record(self) -> None:
        self.assertIsNone(portfolio.validate_bench_json(self.valid_json(), "reach", 8, 5))

    def test_rejects_empty_and_identity_mismatch(self) -> None:
        self.assertIsNotNone(portfolio.validate_bench_json({}, "reach", 8, 5))
        record = self.valid_json()
        record["workers"] = 1
        self.assertIn("identity", portfolio.validate_bench_json(record, "reach", 8, 5) or "")
        for field, malformed in (("workers", True), ("repeat", 5.0)):
            record = self.valid_json()
            record[field] = malformed
            self.assertIn(
                "identity",
                portfolio.validate_bench_json(record, "reach", 8, 5) or "",
            )

    def test_rejects_missing_metrics_and_nonfinite_timings(self) -> None:
        record = self.valid_json()
        del record["iterations"]
        self.assertIn("iterations", portfolio.validate_bench_json(record, "reach", 8, 5) or "")
        record = self.valid_json()
        record["wall_time_ms"] = {"min": 1.0, "median": float("nan"), "max": 2.0}
        self.assertIn("finite", portfolio.validate_bench_json(record, "reach", 8, 5) or "")
        record = self.valid_json()
        record["wall_time_ms"] = {"min": 2.0, "median": 1.0, "max": 3.0}
        self.assertIn("min <= median", portfolio.validate_bench_json(record, "reach", 8, 5) or "")
        record = self.valid_json()
        record["tuples"] = -1
        self.assertIn("nonnegative integer", portfolio.validate_bench_json(record, "reach", 8, 5) or "")

    def test_rejects_cspa_nonfinite_tsv_fields(self) -> None:
        valid = "cspa_incr\t1\t10\t9\t0.1\t2\t5\t100\t110\t4\t5\t3000\tOK"
        parsed, error = portfolio.parse_cspa_incremental_tsv(valid)
        self.assertIsNone(error)
        self.assertEqual(parsed["tuples_after"], 110)
        invalid = valid.replace("\t2\t5\t100", "\tnan\t5\t100")
        parsed, error = portfolio.parse_cspa_incremental_tsv(invalid)
        self.assertIsNone(parsed)
        self.assertIn("finite", error or "")
        parsed, error = portfolio.parse_cspa_incremental_tsv(valid + "\n" + valid)
        self.assertIsNone(parsed)
        self.assertIn("one cspa_incr", error or "")

    def test_parses_standard_tsv_emitted_despite_json_request(self) -> None:
        header = "workload\tnodes\tedges\tworkers\trepeat\tmin_ms\tmedian_ms\tmax_ms\tpeak_rss_kb\ttuples\titerations\tstatus"
        row = "dyck\t-\t100\t8\t5\t10.0\t11.0\t12.0\t3000\t2100\t8\tOK"
        parsed, error, _ = portfolio.parse_bench_output("json", row)
        self.assertIsNone(error)
        self.assertEqual(parsed["workload"], "dyck")
        self.assertEqual(parsed["workers"], 8)
        self.assertEqual(parsed["tuples"], 2100)
        parsed, error = portfolio.parse_bench_tsv(header + "\n" + row + "\n")
        self.assertIsNone(error)
        self.assertEqual(parsed["wall_time_ms"]["median"], 11.0)
        self.assertIsNone(portfolio.validate_bench_json(parsed, "dyck", 8, 5))
        self.assertIn(
            "identity",
            portfolio.validate_bench_json(parsed, "dyck", 1, 5) or "",
        )

    def test_rejects_invalid_standard_tsv_rows(self) -> None:
        header = "workload\tnodes\tedges\tworkers\trepeat\tmin_ms\tmedian_ms\tmax_ms\tpeak_rss_kb\ttuples\titerations\tstatus"
        row = "dyck\t-\t100\t8\t5\t10.0\t11.0\t12.0\t3000\t2100\t8\tOK"
        cases = (
            (row.replace("\tOK", "\tFAIL"), "status"),
            (row.replace("\t100\t", "\tbad\t"), "invalid benchmark TSV metric"),
            (row.replace("\t10.0\t11.0", "\tnan\t11.0"), "finite"),
            (row.replace("\t10.0\t11.0\t12.0", "\t12.0\t11.0\t13.0"), "min <= median"),
        )
        for invalid, expected in cases:
            parsed, error = portfolio.parse_bench_tsv(header + "\n" + invalid)
            self.assertIsNone(parsed)
            self.assertIn(expected, error or "")
        parsed, error = portfolio.parse_bench_tsv(header + "\n" + row + "\n" + row)
        self.assertIsNone(parsed)
        self.assertIn("one benchmark TSV row", error or "")

    def test_interruption_keeps_completed_record_and_incomplete_manifest(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            out_dir = root / "out"
            record = {
                "status": "ok", "workload": "reach", "workers": 1, "repeat": 1,
                "return_code": 0, "duration_sec": 0.1, "summary": {},
                "command": [], "raw_stdout": "completed evidence",
            }
            args = [
                "--repo-root", str(root), "--data-root", str(root),
                "--out-dir", str(out_dir), "--bench", str(root / "bench"),
                "--workers", "1,8", "--repeat", "1", "--workload", "reach",
            ]
            with mock.patch.object(
                portfolio, "run_one", side_effect=[record, KeyboardInterrupt]
            ):
                with self.assertRaises(KeyboardInterrupt):
                    portfolio.main(args)
            manifest = json.loads((out_dir / "manifest.json").read_text(encoding="utf-8"))
            progress = [
                json.loads(line)
                for line in (out_dir / "progress.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            self.assertFalse(manifest["run_complete"])
            self.assertEqual(len(progress), 1)
            self.assertEqual(progress[0]["raw_stdout"], "completed evidence")

    def test_final_artifact_failure_does_not_publish_completion(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            out_dir = root / "out"
            record = {
                "status": "ok", "workload": "reach", "workers": 1, "repeat": 1,
                "return_code": 0, "duration_sec": 0.1, "summary": {}, "command": [],
            }
            args = [
                "--repo-root", str(root), "--data-root", str(root),
                "--out-dir", str(out_dir), "--bench", str(root / "bench"),
                "--workers", "1", "--repeat", "1", "--workload", "reach",
            ]
            with mock.patch.object(portfolio, "run_one", return_value=record):
                with mock.patch.object(portfolio, "write_tsv", side_effect=OSError("disk full")):
                    with self.assertRaisesRegex(OSError, "disk full"):
                        portfolio.main(args)
            manifest = json.loads((out_dir / "manifest.json").read_text(encoding="utf-8"))
            self.assertFalse(manifest["run_complete"])

    def test_existing_artifacts_are_preserved_when_reuse_is_rejected(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            out_dir = root / "out"
            out_dir.mkdir()
            progress = out_dir / "progress.jsonl"
            progress.write_text("previous evidence\n", encoding="utf-8")
            args = [
                "--repo-root", str(root), "--data-root", str(root),
                "--out-dir", str(out_dir), "--bench", str(root / "bench"),
                "--workers", "1", "--repeat", "1", "--workload", "reach",
            ]
            self.assertEqual(portfolio.main(args), 2)
            self.assertEqual(progress.read_text(encoding="utf-8"), "previous evidence\n")

    def test_flags_cross_worker_result_mismatch(self) -> None:
        records = [
            {"status": "ok", "workload": "reach", "repeat": 5,
             "workers": 1, "summary": {"tuples": 100, "iterations": 98}},
            {"status": "ok", "workload": "reach", "repeat": 5,
             "workers": 8, "summary": {"tuples": 99, "iterations": 98}},
        ]
        portfolio.flag_worker_result_mismatches(records)
        self.assertTrue(all(record["status"] == "fail" for record in records))
        self.assertTrue(all(record["reason"] == "worker_result_mismatch" for record in records))


if __name__ == "__main__":
    unittest.main()
