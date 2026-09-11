#!/usr/bin/env python3
"""Mock observer failures and exercise real, lightweight child processes."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location(
    "diagnose", Path(__file__).with_name("diagnose-arm-sanitizer-timeouts.py"))
diagnose = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(diagnose)


class DiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def child(self, code, deadline=2):
        return diagnose.run_child([sys.executable, "-c", code], deadline,
                                  self.root, os.environ.copy(),
                                  diagnose.observer_env())

    def test_allowlist(self):
        self.assertEqual(diagnose.TARGETS, {
            "test_compaction": 30, "test_diff_join": 30,
            "test_col_rel_deep_copy": 30, "test_tdd_decision_stats": 300,
            "test_tdd_decision_stats_nofusion": 300})

    def test_pass_and_file_logs(self):
        result = self.child("print('profile output')")
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["rc"], 0)
        self.assertIn("profile output", (self.root / "stdout.log").read_text())

    def test_nonzero(self):
        result = self.child("import sys; print('error', file=sys.stderr); sys.exit(7)")
        self.assertEqual((result["status"], result["rc"]), ("fail", 7))
        self.assertIn("error", (self.root / "stderr.log").read_text())

    def test_timeout_kills_and_reaps(self):
        result = self.child("import time; time.sleep(20)", 0.2)
        self.assertEqual(result["status"], "timeout")
        self.assertEqual(result["rc"], -9)
        with self.assertRaises(ProcessLookupError):
            os.kill(result["pid"], 0)

    def test_slow_observer_does_not_extend_deadline(self):
        with mock.patch.object(diagnose, "observer_command", return_value=[
                sys.executable, "-c", "import time; time.sleep(20)"]):
            started = time.monotonic()
            result = self.child("import time; time.sleep(20)", 0.2)
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(result["status"], "timeout")
        self.assertEqual(result["observer_status"], "interrupted")

    def test_observer_failure_separate(self):
        with mock.patch.object(diagnose, "observer_command", return_value=[
                sys.executable, "-c", "raise SystemExit(3)"]):
            result = self.child("import time; time.sleep(0.3)")
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["observer_status"], "fail")

    def test_missing_gdb(self):
        with mock.patch.object(diagnose.subprocess, "run",
                               side_effect=FileNotFoundError("gdb missing")):
            event = diagnose.observe_command(["gdb"], self.root / "gdb.log", {})
        self.assertEqual(event["status"], "unavailable")

    def test_attach_denied(self):
        with mock.patch.object(diagnose.subprocess, "run", return_value=
                               subprocess.CompletedProcess(["gdb"], 1)):
            event = diagnose.observe_command(["gdb"], self.root / "gdb.log", {})
        self.assertEqual((event["status"], event["rc"]), ("fail", 1))

    def test_observer_command_timeout(self):
        with mock.patch.object(diagnose.subprocess, "run", side_effect=
                               subprocess.TimeoutExpired(["gdb"], 2)):
            event = diagnose.observe_command(["gdb"], self.root / "gdb.log", {})
        self.assertEqual(event["status"], "timeout")

    def test_proc_unavailable(self):
        with mock.patch.object(Path, "read_text", side_effect=PermissionError("denied")):
            event = diagnose.snapshot_proc(123, self.root / "proc.log")
        self.assertEqual(event["status"], "unavailable")
        self.assertIn("denied", (self.root / "proc.log").read_text())

    def test_environment_separation(self):
        with mock.patch.dict(os.environ, {"LD_LIBRARY_PATH": "/sanitized",
                "LD_PRELOAD": "/bad.so", "ASAN_OPTIONS": "test", "SECRET": "hidden"}):
            observer = diagnose.observer_env()
            child = diagnose.child_env(self.root)
        for key in ("LD_LIBRARY_PATH", "LD_PRELOAD", "ASAN_OPTIONS", "SECRET"):
            self.assertNotIn(key, observer)
        self.assertNotIn("LD_PRELOAD", child)
        self.assertNotIn("/sanitized", child["LD_LIBRARY_PATH"])
        self.assertIn(str(self.root), child["LD_LIBRARY_PATH"])

    def test_manifest_selected_config(self):
        with mock.patch.dict(os.environ, {"SECRET": "never-record"}), \
                mock.patch.object(diagnose.subprocess, "run", return_value=
                                  subprocess.CompletedProcess([], 0, "abc123\n")):
            manifest = diagnose.manifest(self.root)
        self.assertEqual(manifest["sha"], "abc123")
        self.assertEqual(manifest["targets"], diagnose.TARGETS)
        self.assertNotIn("never-record", json.dumps(manifest))
        self.assertEqual(manifest["meson"]["b_sanitize"], "address,undefined")


if __name__ == "__main__":
    unittest.main()
