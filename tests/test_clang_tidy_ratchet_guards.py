#!/usr/bin/env python3
"""Unit tests for the clang-tidy ratchet's fail-closed guards."""

import importlib.util
import pathlib
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/ci/check-clang-tidy-ratchet.py"
SPEC = importlib.util.spec_from_file_location("clang_tidy_ratchet", SCRIPT)
assert SPEC and SPEC.loader
RATCHET = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RATCHET)


class RatchetGuardTests(unittest.TestCase):
    def test_config_values_are_compared_but_user_is_ignored(self):
        baseline = RATCHET.normalized_config_lines(
            "User: one\nHeaderFilterRegex: 'wirelog/.*'\n")
        same = RATCHET.normalized_config_lines(
            "User: another\nHeaderFilterRegex: 'wirelog/.*'\n")
        changed = RATCHET.normalized_config_lines(
            "User: another\nHeaderFilterRegex: ''\n")
        self.assertEqual(baseline, same)
        self.assertNotEqual(baseline, changed)

    def test_nolint_summary_parser(self):
        self.assertEqual(
            RATCHET.parse_nolint_count(
                "Suppressed 2 warnings (2 NOLINT).\n"), 2)
        self.assertEqual(
            RATCHET.parse_nolint_count(
                "Suppressed 1 warning (1 NOLINTNEXTLINE).\n"), 1)
        self.assertEqual(
            RATCHET.parse_nolint_count(
                "Suppressed 2 warnings (1 in non-user code, 1 NOLINT).\n"),
            1)
        self.assertEqual(RATCHET.parse_nolint_count("other stderr\n"), 0)

    def test_nolint_increase_fails_and_decrease_is_allowed(self):
        old_dir = RATCHET.BASELINE_DIR
        with tempfile.TemporaryDirectory() as temp:
            RATCHET.BASELINE_DIR = pathlib.Path(temp)
            path = pathlib.Path(temp) / "llvm-22.nolint-counts.txt"
            path.write_text("wirelog/example.c 1\n", encoding="utf-8")
            source = ROOT / "wirelog/example.c"
            increased = RATCHET.Result(source, 0, "", "", [],
                                       nolint_count=2)
            decreased = RATCHET.Result(source, 0, "", "", [],
                                       nolint_count=0)
            self.assertEqual(
                RATCHET.check_nolint_baseline([increased], "22"), 1)
            self.assertEqual(
                RATCHET.check_nolint_baseline([decreased], "22"), 0)
        RATCHET.BASELINE_DIR = old_dir


if __name__ == "__main__":
    unittest.main()
