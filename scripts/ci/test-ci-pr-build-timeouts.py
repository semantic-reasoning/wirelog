#!/usr/bin/env python3
"""Keep CI budgets and the production/test LTO boundary explicit.

This is intentionally a small structural check rather than a general YAML
parser.  The build-matrix block has a stable layout, and checking that layout
without a third-party dependency keeps the ABI contract portable to Windows.
"""

from pathlib import Path
import re
import unittest


WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/ci-pr.yml"
TEST_MESON = WORKFLOW.parents[2] / "tests/meson.build"
PROJECT_MESON = WORKFLOW.parents[2] / "meson.build"
EXPECTED_TIMEOUTS = {
    "ubuntu-26.04": 90,
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


def library(text: str, name: str) -> str:
    """Read one balanced static_library call, ignoring comments and strings."""
    match = re.search(r"^" + re.escape(name) + r" = static_library\(",
                      text, re.MULTILINE)
    assert match, f"missing {name} archive"
    depth, quote, comment = 1, None, False
    for offset in range(match.end(), len(text)):
        char = text[offset]
        if comment:
            comment = char != "\n"
        elif quote:
            if char == quote:
                quote = None
        elif char == "#":
            comment = True
        elif char in "\"'":
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if not depth:
                return text[match.start():offset + 1]
    raise AssertionError(f"unterminated {name} archive")


def validate_archives(text: str) -> None:
    for name in ("testlib_prod", "testlib_prod_memory_hooks",
                 "radix_workspace_hook_lib"):
        block = library(text, name)
        options = re.search(r"override_options:\s*\[([^]]*)\]", block)
        assert options, f"{name} lacks target overrides"
        assert "'b_lto=false'" in options[1], f"{name} must use native objects"
        assert "'unity=off'" in options[1], f"{name} must preserve extraction"
    memory = library(text, "testlib_prod_memory_hooks")
    assert "testlib_prod.extract_objects(testlib_prod_memory_common_src)" in memory, \
        "MEMORY archive must extract only common objects"
    assert re.search(r"^testlib_prod_no_lto = testlib_prod$", text, re.MULTILINE), \
        "allocation wrappers must alias the native archive"
    assert not re.search(r"static_library\(\s*['\"]testlib_prod_no_lto['\"]", text), \
        "do not rebuild the duplicate native archive"
    for links in re.findall(r"link_with:\s*\[([^]]*)\]", text):
        names = set(re.findall(r"\b\w+\b", links))
        assert not {"testlib_prod", "testlib_prod_memory_hooks"} <= names, \
            "MEMORY fixtures must not also link the original governor/control"


def validate_profiles(workflow: str, project: str) -> None:
    primary = re.search(r"^  build-primary:\n(.*?)(?=^  \S|\Z)", workflow,
                        re.MULTILINE | re.DOTALL)
    assert primary, "missing primary production job"
    assert "-Db_lto=" not in primary[1], "primary production profile must retain LTO"
    defaults = re.search(r"default_options:\s*\[(.*?)\]", project, re.DOTALL)
    assert defaults and "'b_lto=true'" in defaults[1], \
        "production default must retain LTO"
    sanitizer = re.search(r"^  sanitizer-matrix:\n(.*?)(?=^  \S|\Z)", workflow,
                          re.MULTILINE | re.DOTALL)
    assert sanitizer, "missing sanitizer job"
    configure = re.search(r"      - name: Configure with sanitizers\n(.*?)(?=      - name:|\Z)",
                          sanitizer[1], re.DOTALL)
    assert configure and "-Db_lto=false" in configure[1], \
        "sanitizer Configure must explicitly disable LTO"


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

    def change_platform_budget(self, operating_system, old, new):
        pattern = (r"(^          - os: " + re.escape(operating_system)
                   + r"\n(?:            [^\n]*\n)*?"
                   + r"            timeout_minutes: )" + str(old) + r"(?=\n)")
        mutated, count = re.subn(pattern, lambda match: match[1] + str(new),
                                 self.text, flags=re.MULTILINE)
        self.assertEqual(count, 1)
        self.assertNotEqual(mutated, self.text)
        return mutated

    def test_linux_budget_cannot_revert_to_60(self):
        mutated = self.change_platform_budget("ubuntu-26.04", 90, 60)
        with self.assertRaisesRegex(AssertionError, "unexpected"):
            validate(mutated)

    def test_linux_budget_cannot_be_relaxed_accidentally(self):
        mutated = self.change_platform_budget("ubuntu-26.04", 90, 120)
        with self.assertRaisesRegex(AssertionError, "unexpected"):
            validate(mutated)

    def test_macos_budget_remains_60(self):
        mutated = self.change_platform_budget("macos-latest", 60, 90)
        with self.assertRaisesRegex(AssertionError, "unexpected"):
            validate(mutated)


class CiPrLtoCostTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")
        cls.tests = TEST_MESON.read_text(encoding="utf-8")
        cls.project = PROJECT_MESON.read_text(encoding="utf-8")

    def test_actual_profiles_and_archives(self):
        validate_archives(self.tests)
        validate_profiles(self.workflow, self.project)

    def test_each_archive_requires_native_objects(self):
        for name in ("testlib_prod", "testlib_prod_memory_hooks",
                     "radix_workspace_hook_lib"):
            with self.subTest(archive=name):
                block = library(self.tests, name)
                mutated = self.tests.replace(block, block.replace("'b_lto=false'", "'b_lto=true'"))
                with self.assertRaisesRegex(AssertionError, "native objects"):
                    validate_archives(mutated)

    def test_unity_extraction_cannot_be_enabled(self):
        mutated = self.tests.replace("'unity=off'", "'unity=on'", 1)
        with self.assertRaisesRegex(AssertionError, "preserve extraction"):
            validate_archives(mutated)

    def test_memory_replacements_cannot_extract_originals(self):
        mutated = self.tests.replace("extract_objects(testlib_prod_memory_common_src)",
                                     "extract_objects(testlib_prod_src)")
        with self.assertRaisesRegex(AssertionError, "only common"):
            validate_archives(mutated)

    def test_duplicate_native_archive_is_rejected(self):
        mutated = self.tests + "\nunused = static_library('testlib_prod_no_lto', [])\n"
        with self.assertRaisesRegex(AssertionError, "duplicate native"):
            validate_archives(mutated)

    def test_memory_fixture_cannot_link_both_implementations(self):
        mutated = self.tests.replace("link_with: [testlib_prod_memory_hooks]",
                                     "link_with: [testlib_prod_memory_hooks, testlib_prod]", 1)
        with self.assertRaisesRegex(AssertionError, "original governor/control"):
            validate_archives(mutated)

    def test_sanitizer_requires_its_own_override(self):
        mutated = self.workflow.replace("          -Db_lto=false\n", "", 1)
        self.assertNotEqual(mutated, self.workflow)
        with self.assertRaisesRegex(AssertionError, "sanitizer Configure"):
            validate_profiles(mutated, self.project)

    def test_primary_override_is_rejected(self):
        mutated = self.workflow.replace(
            "run: meson setup builddir -Dtests=true -DmbedTLS=disabled",
            "run: meson setup builddir -Dtests=true -DmbedTLS=disabled -Db_lto=false", 1)
        with self.assertRaisesRegex(AssertionError, "primary production"):
            validate_profiles(mutated, self.project)

    def test_production_default_cannot_be_disabled(self):
        mutated = self.project.replace("'b_lto=true'", "'b_lto=false'", 1)
        with self.assertRaisesRegex(AssertionError, "production default"):
            validate_profiles(self.workflow, mutated)


if __name__ == "__main__":
    unittest.main()
