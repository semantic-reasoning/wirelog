#!/usr/bin/env python3
"""Strict evidence parsing for the TDD execution audit (#1378)."""
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import subprocess
import tempfile
import copy
import argparse
import os

RUNTIME_BINS = []

SPEC = importlib.util.spec_from_file_location(
    "audit", Path(__file__).resolve().parents[1] / "scripts/perf/audit-tdd-execution.py"
)
audit = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(audit)


def frame(pairs, plan=1, expected=1, scope="full", mask="ffffffffffffffff"):
    return (f"TDD snapshot begin plan_count={plan} expected_count={expected} scope={scope} "
            f"affected_mask={mask} requested_workers=8\n" + pairs
            + f"TDD snapshot complete evaluated_count={expected} rc=0\n")


def evidence(framed=True, **changes):
    fields = dict(idx="0", rel="r", recursive="1", use_tdd="1", fallback="none",
                  requested_workers="8", strategy="bdx", selected_workers="8",
                  submitted_tasks="16", completed_rounds="2", replay="none",
                  replay_rc="0", rc="0", worker_delta_rows="20")
    fields.update({k: "0.001" for k in audit.TIMINGS})
    fields.update(changes)
    if fields["completed_rounds"] == "0":
        fields.update(worker_delta_rows="0", worker_min_ms="0", worker_max_ms="0", worker_sum_ms="0")
    decision = {key: fields[key] for key in ("rel", "recursive", "use_tdd", "fallback")}
    for key in audit.DECISION_FLAGS + audit.DECISION_COUNTS:
        decision.setdefault(key, "1")
    decision["idb_atoms"] = "2"
    pair = ("TDD decision " + " ".join(f"{k}={v}" for k, v in decision.items()) + "\n"
            + "TDD stratum " + " ".join(f"{k}={v}" for k, v in fields.items()) + "\n")
    return frame(pair) if framed else pair


class AuditTests(unittest.TestCase):
    def test_complete_pair_prefix_is_not_complete_inventory(self):
        root = Path(__file__).resolve().parents[1]
        inventory = json.loads((root / "docs/tdd-execution-audit/inventory.json").read_text())
        record = copy.deepcopy(next(r for r in inventory["runs"] if r.get("workload") == "polonius"
                                    and r.get("requested_workers") == 8))
        lines = record["logs"][".stderr"]["text"].splitlines()
        end = next(i for i, line in enumerate(lines) if line.startswith("TDD stratum "))
        record["logs"][".stderr"]["text"] = "\n".join(lines[:end + 1]) + "\n"
        record["disposition"] = "completed"
        audit.validate_record(record, "polonius", 8, 1983, 23, record["configuration"])
        self.assertEqual(record["disposition"], "invalid_evidence")

    def test_missing_analysis_is_invalid(self):
        for key in ("snapshot", "exchange", "safe", "global_read_candidate", "self_join", "idb_atoms",
                    "segments", "segment_seed", "segment_global_read", "segment_unsafe",
                    "segment_max_idb", "segment_max_joins", "single_key_aligned"):
            text = "\n".join(" ".join(token for token in line.split() if not token.startswith(key + "="))
                             if line.startswith("TDD decision ") else line
                             for line in evidence().splitlines()) + "\n"
            with self.subTest(key=key), self.assertRaises(ValueError):
                audit.parse_strata(text, 8)

    def test_committed_evidence_matches_full_logs(self):
        root = Path(__file__).resolve().parents[1]
        inventory = json.loads((root / "docs/tdd-execution-audit/inventory.json").read_text())
        expected = {"cspa-fast": (20381, 6), "galen": (5568, 23),
                    "polonius": (1983, 23), "ddisasm": (704, 0), "crdt": (2152328, 14148)}
        measured = [r for r in inventory["runs"] if r["disposition"] == "measured_verified"]
        self.assertEqual(len(measured), 20)
        self.assertEqual({(r["workload"], r["configuration"], r["requested_workers"]) for r in measured},
                         {(w, c, n) for w in expected for c in ("default", "unfused") for n in (1, 8)})
        self.assertEqual(len(inventory["controls"]), 2)
        for record in measured + inventory["controls"]:
            for log in record["logs"].values():
                self.assertEqual(audit.hashlib.sha256(log["text"].encode()).hexdigest(), log["sha256"])
            workers = record.get("requested_workers", 8)
            workload = record.get("workload", "tdd-bdx")
            tuples, iterations = expected.get(workload, (4950, 7))
            self.assertEqual(audit.parse_result(record["logs"][".stdout"]["text"], workload,
                                                workers, tuples, iterations), record["result"])
            self.assertEqual(audit.parse_strata(record["logs"][".stderr"]["text"], workers), record["strata"])
        deferred = [r for r in inventory["runs"] if r["workload"] == "doop"]
        self.assertEqual(len(deferred), 1)
        self.assertEqual(deferred[0]["disposition"], "not_measured")
        self.assertNotIn("strata", deferred[0])
        self.assertTrue(all(r["equal"] for r in inventory["iteration_comparisons"]))

    def test_failed_or_timed_out_process_is_not_rejected_plan(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "run"
            for error, expected in ((subprocess.TimeoutExpired(["bench"], 1), "timeout"),
                                    (OSError("cannot launch"), "execution_error")):
                with patch.object(audit.subprocess, "run", side_effect=error):
                    result = audit.run_process(["bench"], {}, prefix, 1, directory)
                self.assertEqual(result["disposition"], expected)
                self.assertNotIn("strata", result)
                self.assertIn("text", result["logs"][".stderr"])
            with patch.object(audit.subprocess, "run", return_value=subprocess.CompletedProcess(["bench"], 7)):
                result = audit.run_process(["bench"], {}, prefix, 1, directory)
            self.assertEqual(result["disposition"], "execution_error")
            self.assertEqual(result["returncode"], 7)

    def test_manifest_uses_release_relative_filename_format(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "input.csv").write_text("1,2\n")
            manifest, files = audit.data_manifest(root)
            expected = audit.hashlib.sha256(
                (audit.sha(root / "input.csv") + "  input.csv\n").encode()).hexdigest()
            self.assertEqual(manifest, expected)
            self.assertEqual(list(files), ["input.csv"])

    def test_actual_dispatch_and_replay(self):
        self.assertEqual(audit.parse_strata(evidence(), 8)[0]["disposition"], "tdd_executed")
        row = audit.parse_strata(evidence(strategy="owner", replay="owner_tiny_frontier"), 8)[0]
        self.assertEqual(row["disposition"], "tdd_then_serial_replay")
        self.assertEqual(row["dispatch_width"], 8)
        overflow = audit.parse_strata(evidence(strategy="global_read", replay="global_read_overflow", replay_rc="75"), 8)[0]
        self.assertEqual(overflow["replay_rc"], 75)

    def test_adaptive_is_not_rejected(self):
        row = audit.parse_strata(evidence(selected_workers="1", submitted_tasks="0",
                                          completed_rounds="0"), 8)[0]
        self.assertEqual(row["disposition"], "adaptively_narrowed")
        self.assertEqual(row["dispatch_width"], 0)

    def test_multiple_strata_and_reset(self):
        rejected = evidence(framed=False, idx="1", rel="s", use_tdd="0", fallback="unsafe_plan",
                            strategy="none", selected_workers="0", submitted_tasks="0",
                            completed_rounds="0")
        rows = audit.parse_strata(frame(evidence(framed=False) + rejected, plan=2, expected=2), 8)
        self.assertEqual([r["disposition"] for r in rows], ["tdd_executed", "not_eligible"])

    def test_frame_scopes_and_pair_loss(self):
        stable = frame("", plan=3, expected=0, scope="stable", mask="0")
        self.assertEqual(audit.parse_strata(stable, 8, require_full=False), [])
        pairs = evidence(framed=False, idx="0") + evidence(framed=False, idx="2", rel="t")
        affected = frame(pairs, plan=3, expected=2, scope="affected", mask="5")
        self.assertEqual([r["idx"] for r in audit.parse_strata(affected, 8, require_full=False)], [0, 2])
        wide = frame(evidence(framed=False) + evidence(framed=False, idx="64", rel="s")
                     + evidence(framed=False, idx="65", rel="t"), plan=66, expected=3,
                     scope="affected", mask="1")
        self.assertEqual([r["idx"] for r in audit.parse_strata(wide, 8, require_full=False)], [0, 64, 65])
        for bad in (stable, affected, frame(evidence(framed=False), plan=2, expected=2),
                    evidence().replace("expected_count=1", "expected_count=2"),
                    evidence().replace("evaluated_count=1", "evaluated_count=0"),
                    evidence().replace("scope=full", "scope=unknown"),
                    evidence() + evidence(framed=False), evidence(framed=False)):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                audit.parse_strata(bad, 8)

    def test_analysis_domains_and_pair_order(self):
        for key, value in (("snapshot", "2"), ("safe", "bad"), ("idb_atoms", "-1"),
                           ("segments", "no"), ("fallback", "unknown")):
            lines = evidence().splitlines()
            tokens = lines[1].split()
            lines[1] = " ".join(f"{key}={value}" if token.startswith(key + "=") else token for token in tokens)
            with self.subTest(key=key), self.assertRaises(ValueError):
                audit.parse_strata("\n".join(lines) + "\n", 8)
        lines = evidence().splitlines()
        lines[1], lines[2] = lines[2], lines[1]
        with self.assertRaises(ValueError):
            audit.parse_strata("\n".join(lines) + "\n", 8)

    def test_runtime_frames(self):
        if not RUNTIME_BINS:
            self.skipTest("supply --runtime-bin for real snapshot boundary coverage")
        env = {k: v for k, v in os.environ.items() if not k.startswith("WIRELOG_")}
        env["WIRELOG_TDD_DECISION_DEBUG"] = "1"
        for binary in RUNTIME_BINS:
            proc = subprocess.run([binary, "--audit-frames"], env=env, capture_output=True,
                                  text=True, timeout=30, check=True)
            frames = ["TDD snapshot begin " + s for s in proc.stderr.split("TDD snapshot begin ")[1:]]
            self.assertEqual(len(frames), 3)
            rows = [audit.parse_strata(s, 8, require_full=False) for s in frames]
            self.assertEqual([[r["idx"] for r in snapshot] for snapshot in rows], [[0, 1, 2], [0, 2], []])
            for option in ("--audit-error", "--audit-partial"):
                proc = subprocess.run([binary, option], env=env, capture_output=True,
                                      text=True, timeout=30, check=True)
                self.assertIn("TDD snapshot begin ", proc.stderr)
                self.assertNotIn("TDD snapshot complete ", proc.stderr)
                with self.assertRaises(ValueError):
                    audit.parse_strata(proc.stderr, 8)
            proc = subprocess.run([binary, "--audit-frames-off"], env={k: v for k, v in env.items()
                                  if k != "WIRELOG_TDD_DECISION_DEBUG"}, capture_output=True,
                                  text=True, timeout=30, check=True)
            self.assertNotIn("TDD ", proc.stderr)

    def test_missing_duplicate_truncated_and_malformed(self):
        for text in ("", evidence() + evidence(), evidence().splitlines()[0] + "\n",
                     evidence().replace(" rc=0", " rc=bad"), evidence().rstrip(),
                     evidence().replace("worker_sum_ms=0.001", "worker_sum_ms=nan"),
                     evidence().replace(" rel=r", " rel=r rel=other", 1)):
            with self.subTest(text=text), self.assertRaises(ValueError):
                audit.parse_strata(text, 8)

    def test_inconsistent_success_rejected(self):
        for changes in (dict(submitted_tasks="15"), dict(rc="12"),
                        dict(requested_workers="1"), dict(replay="unknown"),
                        dict(strategy="none"), dict(idx="1"),
                        dict(replay="global_read_overflow"), dict(replay="owner_tiny_frontier")):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                audit.parse_strata(evidence(**changes), 8)

    def test_timeout_retains_partial_logs(self):
        def timeout(*args, **kwargs):
            kwargs["stderr"].write("partial diagnostic\n")
            raise subprocess.TimeoutExpired(args[0], 1)
        with tempfile.TemporaryDirectory() as directory, patch.object(audit.subprocess, "run", side_effect=timeout):
            result = audit.run_process(["bench"], {}, Path(directory) / "run", 1, directory)
        self.assertEqual(result["disposition"], "timeout")
        self.assertEqual(result["logs"][".stderr"]["text"], "partial diagnostic\n")

    def test_full_stderr_not_tail(self):
        rows = audit.parse_strata(evidence() + "unrelated diagnostic\n" * 1000, 8)
        self.assertEqual(len(rows), 1)

    def test_json_alias_and_worker_specific_iterations(self):
        row = dict(workload="cspa", workers=8, repeat=1, tuples=20381,
                   iterations=6, peak_rss_kb=123, wall_time_ms={"median": 1.0})
        self.assertEqual(audit.parse_result(json.dumps(row), "cspa-fast", 8, 20381, 6)["tuples"], 20381)
        with self.assertRaises(ValueError):
            audit.parse_result(json.dumps(row), "galen", 8, 20381, 6)
        row["tuples"] = 1
        with self.assertRaises(ValueError):
            audit.parse_result(json.dumps(row), "cspa-fast", 8, 20381, 6)
        tsv = "ddisasm\t-\t412\t8\t1\t1.0\t1.0\t1.0\t123\t704\t19\tOK\n"
        self.assertEqual(audit.parse_result(tsv, "ddisasm", 8, 704, 0)["iterations"], 19)
        for bad in (tsv + tsv, tsv.replace("OK", "FAIL"), tsv.replace("\t19\t", "\t-1\t")):
            with self.assertRaises(ValueError):
                audit.parse_result(bad, "ddisasm", 8, 704, 0)
        with self.assertRaises(ValueError):
            audit.parse_result(tsv.replace("\t8\t", "\t1\t"), "ddisasm", 1, 704, 0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--runtime-bin", action="append", default=[])
    options, remaining = parser.parse_known_args()
    RUNTIME_BINS = options.runtime_bin
    unittest.main(argv=[__file__] + remaining)
