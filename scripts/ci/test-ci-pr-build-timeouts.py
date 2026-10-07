#!/usr/bin/env python3
"""Keep platform-specific CI build budgets explicit and reviewable.

This is intentionally a small structural check rather than a general YAML
parser.  The build-matrix block has a stable layout, and checking that layout
without a third-party dependency keeps the ABI contract portable to Windows.
"""

from pathlib import Path
import re
import unittest


WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/ci-pr.yml"
EXPECTED_TIMEOUTS = {
    "ubuntu-latest": 60,
    "macos-latest": 60,
    "windows-latest": 120,
}


def build_matrix(text: str) -> str:
    match = re.search(r"^  build-matrix:\n(.*?)(?=^  \S|\Z)",
                      text, re.MULTILINE | re.DOTALL)
    assert match, "missing build-matrix job"
    return match[1]


def validate(text: str) -> None:
    block = build_matrix(text)
    timeout_lines = re.findall(r"^    timeout-minutes: (.+)$", block,
                               re.MULTILINE)
    assert timeout_lines == ["${{ matrix.timeout_minutes }}"], \
        "build-matrix must use one matrix-specific timeout expression"

    entries = re.findall(
        r"^          - os: ([^\n]+)\n((?:            [^\n]*\n)*)",
        block, re.MULTILINE)
    operating_systems = [operating_system for operating_system, _ in entries]
    assert len(set(operating_systems)) == len(operating_systems), \
        "build-matrix must not contain duplicate platform entries"
    assert len(entries) == len(EXPECTED_TIMEOUTS), \
        "build-matrix must contain exactly one entry per platform"
    actual = {}
    for operating_system, body in entries:
        timeout = re.search(r"^            timeout_minutes: (\d+)$", body,
                            re.MULTILINE)
        assert timeout, f"{operating_system} matrix entry lacks a timeout"
        actual[operating_system] = int(timeout[1])
    assert actual == EXPECTED_TIMEOUTS, \
        f"unexpected build-matrix timeout mapping: {actual!r}"


class CiPrBuildTimeoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = WORKFLOW.read_text(encoding="utf-8")

    def test_actual_workflow(self):
        validate(self.text)

    def test_global_timeout_is_rejected(self):
        mutated = self.text.replace(
            "timeout-minutes: ${{ matrix.timeout_minutes }}",
            "timeout-minutes: 90")
        self.assertNotEqual(mutated, self.text)
        with self.assertRaisesRegex(AssertionError, "matrix-specific"):
            validate(mutated)

    def test_missing_windows_budget_is_rejected(self):
        mutated = re.sub(r"\n            timeout_minutes: 120(?=\n\n    steps:)",
                         "", self.text)
        self.assertNotEqual(mutated, self.text)
        with self.assertRaisesRegex(AssertionError, "lacks a timeout"):
            validate(mutated)

    def test_windows_budget_must_remain_distinct(self):
        mutated = self.text.replace("timeout_minutes: 120", "timeout_minutes: 60")
        self.assertNotEqual(mutated, self.text)
        with self.assertRaisesRegex(AssertionError, "unexpected"):
            validate(mutated)

    def test_duplicate_platform_entry_is_rejected(self):
        entry = ("          - os: windows-latest\n"
                 "            compiler: msvc\n"
                 "            cc: cl\n"
                 "            timeout_minutes: 120\n")
        mutated = self.text.replace(entry, entry + entry)
        self.assertNotEqual(mutated, self.text)
        with self.assertRaisesRegex(AssertionError, "duplicate"):
            validate(mutated)

    def test_linux_budget_cannot_be_relaxed_accidentally(self):
        mutated = self.text.replace("timeout_minutes: 60", "timeout_minutes: 90", 1)
        self.assertNotEqual(mutated, self.text)
        with self.assertRaisesRegex(AssertionError, "unexpected"):
            validate(mutated)


if __name__ == "__main__":
    unittest.main()
