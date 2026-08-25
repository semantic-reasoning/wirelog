#!/usr/bin/env python3
"""Unit tests for the clang-tidy ratchet's fail-closed guards."""

import importlib.util
import pathlib
import re
import tempfile
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/ci/check-clang-tidy-ratchet.py"
SPEC = importlib.util.spec_from_file_location("clang_tidy_ratchet", SCRIPT)
assert SPEC and SPEC.loader
RATCHET = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RATCHET)


class RatchetGuardTests(unittest.TestCase):
    def test_effective_checks_are_canonical_and_sorted(self):
        self.assertEqual(
            RATCHET.effective_checks_from_output(
                "Enabled checks:\n  z-check\n  a-check\n"),
            ["a-check", "z-check"])

    def test_effective_checks_reject_malformed_output(self):
        with self.assertRaises(RATCHET.GateError):
            RATCHET.effective_checks_from_output(
                "Enabled checks:\n  bad name\n")

    @mock.patch.object(RATCHET.subprocess, "run")
    def test_regenerate_can_probe_an_unlisted_major(self, run):
        run.return_value = RATCHET.subprocess.CompletedProcess(
            ["clang-tidy"], 0, "LLVM version 99.0.0\n", "")
        self.assertEqual(RATCHET.check_version("clang-tidy", False), "99")
        with self.assertRaises(RATCHET.SkipGate):
            RATCHET.check_version("clang-tidy", True)

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

    def test_allowlisted_sources_with_nolint_have_a_baseline_key(self):
        """Every allowlisted source carrying a NOLINT marker must appear in
        the baseline.

        The baseline is only consulted for allowlisted targets, so a file
        promoted after the baseline was written arrives with suppressions
        the baseline does not know about and the ratchet fails on main --
        which is how wirelog/ir/program.c broke the gate.  This asserts
        presence only, never a count, so it cannot go stale when a
        toolchain bump changes how many warnings a marker suppresses.
        """
        marker = re.compile(r"//\s*NOLINT(NEXTLINE|BEGIN|END)?\b")
        allowlist = ROOT / "scripts/ci/clang-tidy-allowlist.txt"
        baseline = RATCHET.read_nolint_baseline("22")
        missing = []
        for raw in allowlist.read_text(encoding="utf-8").splitlines():
            entry = raw.strip()
            if not entry or entry.startswith("#"):
                continue
            source = ROOT / entry
            if not source.is_file():
                continue
            text = source.read_text(encoding="utf-8", errors="replace")
            if marker.search(text) and entry not in baseline:
                missing.append(entry)
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
