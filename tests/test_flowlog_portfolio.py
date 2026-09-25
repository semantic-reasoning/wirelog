#!/usr/bin/env python3
"""Regression tests for structured FlowLog portfolio result validation."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

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
