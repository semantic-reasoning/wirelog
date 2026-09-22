#!/usr/bin/env python3
"""Self-test for check-python-encoding.py (#1650)."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest
import sys

HERE = Path(__file__).resolve().parent
GATE = HERE / "check-python-encoding.py"
ROOT = HERE.parents[1]


def load_gate():
    spec = importlib.util.spec_from_file_location("check_python_encoding", GATE)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class PythonEncodingGateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gate = load_gate()

    def test_repository_is_clean(self):
        self.assertEqual([], self.gate.scan(ROOT))

    def test_deliberate_text_operations_are_reported(self):
        source = """
import subprocess
from pathlib import Path

def bad(path):
    path.read_text()
    path.write_text('x')
    path.open('w')
    open(path)
    subprocess.run(['tool'], text=True)
"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.py"
            path.write_text(source, encoding="utf-8")
            messages = [item.message for item in self.gate.scan_file(path)]
        self.assertEqual(5, len(messages))
        self.assertTrue(any("read_text" in message for message in messages))
        self.assertTrue(any("write_text" in message for message in messages))
        self.assertTrue(any("Path.open" in message for message in messages))
        self.assertTrue(any("open()" in message for message in messages))
        self.assertTrue(any("subprocess" in message for message in messages))

    def test_binary_and_explicit_encoding_are_allowed(self):
        source = """
import subprocess
from pathlib import Path

def good(path):
    path.read_text(encoding='utf-8')
    path.write_text('x', encoding='utf-8')
    path.open('rb')
    open(path, 'wb')
    subprocess.run(['tool'], text=True, encoding='utf-8')
"""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "good.py"
            path.write_text(source, encoding="utf-8")
            self.assertEqual([], self.gate.scan_file(path))

    def test_audit_uses_strict_utf8_for_committed_logs(self):
        audit = (ROOT / "scripts/perf/audit-tdd-execution.py").read_text(encoding="utf-8")
        self.assertIn('read_text(encoding="utf-8", errors="strict")', audit)
        self.assertIn('disposition="invalid_evidence"', audit)


if __name__ == "__main__":
    unittest.main()
