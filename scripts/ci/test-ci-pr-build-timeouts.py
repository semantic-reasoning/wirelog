#!/usr/bin/env python3
"""Keep CI budgets, the production/test LTO boundary and the clang-tidy
ratchet's exclusive scheduling explicit.

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
    "macos-latest": 60,
    "windows-latest": 120,
}
# Issue #2397: Linux Clang runs as SHARD_COUNT `meson test --slice K/N` jobs
# with a per-shard job budget, a shorter test-step budget (so the evidence
# steps still run after a test-step timeout) and a collecting job that keeps
# the `Build / ubuntu-26.04 / clang` context.  Raise SHARD_COUNT and the
# workflow's count sites together; docs/CI_TELEMETRY.md records why the count
# is what it is.
SHARD_COUNT = 5
SHARD_JOB_TIMEOUT = 60
SHARD_TEST_STEP_TIMEOUT = 40
COLLECT_TIMEOUT = 10
SHARD_JOB = "build-linux-clang-shards"
COLLECT_JOB = "build-linux-clang"


def job_block(text: str, name: str) -> str:
    match = re.search(r"^  " + re.escape(name) + r":\n(.*?)(?=^  \S|\Z)",
                      text, re.MULTILINE | re.DOTALL)
    assert match, f"missing {name} job"
    return match[1]


def build_matrix(text: str) -> str:
    return job_block(text, "build-matrix")


def step_block(job: str, name: str) -> str:
    starts = [m.start() for m in re.finditer(
        r"^      - name: " + re.escape(name) + r"\n", job, re.MULTILINE)]
    assert len(starts) == 1, f"expected exactly one '{name}' step"
    rest = job[starts[0]:]
    end = re.search(r"^      - ", rest[1:], re.MULTILINE)
    return rest if not end else rest[:end.start() + 1]


def validate_linux_clang_shards(text: str) -> None:
    n = SHARD_COUNT
    assert "ubuntu" not in build_matrix(text), \
        "Linux Clang must not also run unsharded in build-matrix"
    shards = job_block(text, SHARD_JOB)
    collect = job_block(text, COLLECT_JOB)
    for name, block in ((SHARD_JOB, shards), (COLLECT_JOB, collect)):
        assert "continue-on-error" not in block, \
            f"{name} must not use continue-on-error"
        assert "--repeat" not in block and "retry" not in block.lower(), \
            f"{name} must not retry tests"
    assert re.search(r"^    name: Build / ubuntu-26\.04 / clang / shard "
                     r"\$\{\{ matrix\.shard \}\} of " + str(n) + r"$",
                     shards, re.MULTILINE), "shard job name must state the shard count"
    assert re.findall(r"^    timeout-minutes: (\d+)$", shards, re.MULTILINE) \
        == [str(SHARD_JOB_TIMEOUT)], f"shard jobs must keep a {SHARD_JOB_TIMEOUT}-minute budget"
    assert re.search(r"^      fail-fast: false$", shards, re.MULTILINE), \
        "one failing shard must not cancel the others"
    shard_list = re.findall(r"^        shard: \[([^]]*)\]$", shards, re.MULTILINE)
    assert shard_list == [", ".join(str(k) for k in range(1, n + 1))], \
        f"shard matrix must be exactly 1..{n}"
    test = step_block(shards, "Test (Linux Clang shard)")
    assert re.findall(r"^        run: (.*)$", test, re.MULTILINE) == [
        "meson test -C builddir --print-errorlogs --slice ${{ matrix.shard }}/" + str(n)], \
        "shard test step must run the whole default suite sliced, nothing else"
    assert re.findall(r"^        timeout-minutes: (\d+)$", test, re.MULTILINE) \
        == [str(SHARD_TEST_STEP_TIMEOUT)], "shard test step needs its own shorter budget"
    assert SHARD_TEST_STEP_TIMEOUT < SHARD_JOB_TIMEOUT
    for flag in ("--suite", "--no-suite", "--exclude", "--timeout-multiplier",
                 "--setup"):
        assert flag not in shards, f"shard job must not filter tests with {flag}"
    plan = step_block(shards, "Plan Linux Clang shard")
    assert re.findall(r"--count (\S+)", plan) == [str(n)] \
        and "--shard ${{ matrix.shard }}" in plan, \
        "plan step must record this shard of the same count"
    verify = step_block(shards, "Verify shard coverage")
    assert "check-test-shards.py verify" in verify, "verify step must run the checker"
    assert "always()" in verify, "verify must also run after a test-step failure"
    walltime = step_block(shards, "Shell gate walltime budget (Linux Clang shard)")
    assert 'WIRELOG_WALLTIME_REQUIRED: "1"' in walltime \
        and not re.search(r"^        if:", walltime, re.MULTILINE), \
        "shard walltime gate must be required and unconditional"
    upload = step_block(shards, "Upload shard evidence")
    assert "if: ${{ always() }}" in upload and "upload-artifact" in upload \
        and "linux-clang-shard-${{ matrix.shard }}" in upload, \
        "shard evidence must upload under its shard name even on failure"

    assert re.search(r"^    name: Build / ubuntu-26\.04 / clang$", collect,
                     re.MULTILINE), "collecting job must keep the stable context name"
    assert re.search(r"^    needs: \[" + SHARD_JOB + r", build-scope\]$", collect,
                     re.MULTILINE), "collecting job must need every shard"
    condition = re.findall(r"^    if: (.*)$", collect, re.MULTILINE)
    assert len(condition) == 1 and "!cancelled()" in condition[0], \
        "collecting job must run, not skip, when a shard fails"
    assert re.findall(r"^    timeout-minutes: (\d+)$", collect, re.MULTILINE) \
        == [str(COLLECT_TIMEOUT)], "collecting job needs its own budget"
    gate = step_block(collect, "Require every shard to succeed")
    assert "needs." + SHARD_JOB + ".result" in gate and "!= success" in gate, \
        "collecting job must fail unless every shard succeeded"
    aggregate = step_block(collect, "Aggregate shard coverage")
    assert re.findall(r"--count (\S+)", aggregate) == [str(n)], \
        "collecting job must prove the same shard count"
    download = step_block(collect, "Download shard evidence")
    assert re.findall(r"^          pattern: (.*)$", download, re.MULTILINE) \
        == ["linux-clang-shard-*"] \
        and re.findall(r"^          path: (.*)$", download, re.MULTILINE) \
        == ["shard-evidence"] \
        and aggregate.rstrip().endswith(" shard-evidence"), \
        "collecting job must download every shard's upload into the folder it aggregates"


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
    validate_linux_clang_shards(text)


def call_end(text: str, start: int):
    """Return the offset of the ')' that closes a call whose body starts at
    start, ignoring comments and strings; None if it never closes."""
    depth, quote, comment = 1, None, False
    for offset in range(start, len(text)):
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
                return offset
    return None


def library(text: str, name: str) -> str:
    """Read one balanced static_library call, ignoring comments and strings."""
    match = re.search(r"^" + re.escape(name) + r" = static_library\(",
                      text, re.MULTILINE)
    assert match, f"missing {name} archive"
    end = call_end(text, match.end())
    if end is None:
        raise AssertionError(f"unterminated {name} archive")
    return text[match.start():end + 1]


def code_only(block: str) -> str:
    """Drop meson '#' comments and blank string contents, keeping quotes."""
    kept, quote, comment = [], None, False
    for char in block:
        if comment:
            if char != "\n":
                continue
            comment = False
        elif quote:
            if char == quote:
                quote = None
            else:
                char = " "
        elif char == "#":
            comment = True
            continue
        elif char in "\"'":
            quote = char
        kept.append(char)
    return "".join(kept)


def validate_tidy_scheduling(text: str) -> None:
    """#2397: the ratchet runs min(8, ncpu) clang-tidy processes by default,
    so it must not share the runner with other tests."""
    openers = list(re.finditer(r"^[ \t]*test\('clang_tidy_ratchet',",
                               text, re.MULTILINE))
    assert len(openers) == 1, "expected exactly one clang_tidy_ratchet test"
    end = call_end(text, openers[0].end())
    assert end is not None, "unterminated clang_tidy_ratchet test"
    block = code_only(text[openers[0].start():end + 1])
    assert re.search(r"\bis_parallel\s*:\s*false\b", block), \
        "clang_tidy_ratchet must stay is_parallel: false (#2397)"


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

    def test_macos_budget_remains_60(self):
        mutated = self.change_platform_budget("macos-latest", 60, 90)
        with self.assertRaisesRegex(AssertionError, "unexpected"):
            validate(mutated)


# The mutation tests below derive every anchor from SHARD_COUNT, so a bump is
# one constant here; N - 1 must still be a valid shard count.
assert SHARD_COUNT >= 2
N = str(SHARD_COUNT)
FEWER = str(SHARD_COUNT - 1)
SHARD_LIST = ", ".join(str(k) for k in range(1, SHARD_COUNT + 1))
SLICE = "--slice ${{ matrix.shard }}/"
PLAN_COUNT = "--shard ${{ matrix.shard }} --count "
AGGREGATE_COUNT = "aggregate --count "
JOB_NAME = "clang / shard ${{ matrix.shard }} of "


class LinuxClangShardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = WORKFLOW.read_text(encoding="utf-8")

    def mutate(self, old, new, count=1):
        self.assertEqual(self.text.count(old), count, old)
        mutated = self.text.replace(old, new)
        self.assertNotEqual(mutated, self.text)
        return mutated

    def rejects(self, mutated, message):
        with self.assertRaisesRegex(AssertionError, message):
            validate(mutated)

    def test_actual_workflow(self):
        validate_linux_clang_shards(self.text)

    def test_dropping_a_shard_is_rejected(self):
        self.rejects(self.mutate("shard: [" + SHARD_LIST + "]",
                                 "shard: [" + SHARD_LIST.rsplit(", ", 1)[0] + "]"),
                     re.escape("exactly 1.." + N))

    def test_slice_count_must_agree(self):
        self.rejects(self.mutate(SLICE + N + "\n", SLICE + FEWER + "\n"),
                     "sliced")

    def test_plan_count_must_agree(self):
        self.rejects(self.mutate(PLAN_COUNT + N + " ", PLAN_COUNT + FEWER + " "),
                     "same count")

    def test_aggregate_count_must_agree(self):
        self.rejects(self.mutate(AGGREGATE_COUNT + N + "\n",
                                 AGGREGATE_COUNT + FEWER + "\n"),
                     "same shard count")

    def test_plan_count_must_match_exactly(self):
        self.rejects(self.mutate(PLAN_COUNT + N + " ", PLAN_COUNT + N + "0 "),
                     "same count")

    def test_aggregate_count_must_match_exactly(self):
        self.rejects(self.mutate(AGGREGATE_COUNT + N + "\n",
                                 AGGREGATE_COUNT + N + "1\n"),
                     "same shard count")

    def test_job_name_count_must_agree(self):
        self.rejects(self.mutate(JOB_NAME + N + "\n", JOB_NAME + FEWER + "\n"),
                     "state the shard count")

    def test_download_must_match_upload_name(self):
        self.rejects(self.mutate("pattern: linux-clang-shard-*",
                                 "pattern: linux-clang-evidence-*"),
                     "download every shard")

    def test_download_path_must_match_aggregate_folder(self):
        self.rejects(self.mutate("          path: shard-evidence\n\n",
                                 "          path: evidence\n\n"),
                     "download every shard")

    def test_shard_walltime_gate_must_be_unconditional(self):
        self.rejects(self.mutate(
            "      - name: Shell gate walltime budget (Linux Clang shard)\n",
            "      - name: Shell gate walltime budget (Linux Clang shard)\n"
            "        if: false\n"),
            "unconditional")

    def test_test_filter_is_rejected(self):
        self.rejects(self.mutate("--print-errorlogs --slice",
                                 "--print-errorlogs --no-suite tidy --slice"),
                     "sliced")

    def test_collecting_context_name_is_pinned(self):
        self.rejects(self.mutate("    name: Build / ubuntu-26.04 / clang\n",
                                 "    name: Build / ubuntu-26.04 / clang / all\n"),
                     "stable context")

    def test_collecting_job_must_not_skip(self):
        self.rejects(self.mutate("if: ${{ !cancelled() && needs.build-scope",
                                 "if: ${{ needs.build-scope"),
                     "run, not skip")

    def test_collecting_job_must_check_shard_result(self):
        self.rejects(self.mutate('if [ "$SHARDS_RESULT" != success ]; then',
                                 'if [ "$SHARDS_RESULT" = cancelled ]; then'),
                     "every shard succeeded")

    def test_continue_on_error_is_rejected(self):
        self.rejects(self.mutate(
            "      - name: Test (Linux Clang shard)\n",
            "      - name: Test (Linux Clang shard)\n        continue-on-error: true\n"),
            "continue-on-error")

    def test_shard_budget_is_pinned(self):
        self.rejects(self.mutate("    timeout-minutes: 60\n    strategy:\n"
                                 "      fail-fast: false\n      matrix:\n"
                                 "        shard:",
                                 "    timeout-minutes: 90\n    strategy:\n"
                                 "      fail-fast: false\n      matrix:\n"
                                 "        shard:"),
                     "60-minute")

    def test_test_step_budget_is_pinned(self):
        self.rejects(self.mutate("        timeout-minutes: 40\n",
                                 "        timeout-minutes: 60\n"),
                     "shorter budget")

    def test_fail_fast_is_rejected(self):
        block = job_block(self.text, SHARD_JOB)
        mutated = self.text.replace(block, block.replace("fail-fast: false",
                                                         "fail-fast: true"))
        self.assertNotEqual(mutated, self.text)
        self.rejects(mutated, "cancel the others")

    def test_walltime_gate_must_stay_required(self):
        block = job_block(self.text, SHARD_JOB)
        mutated = self.text.replace(block, block.replace(
            'WIRELOG_WALLTIME_REQUIRED: "1"', 'WIRELOG_WALLTIME_REQUIRED: "0"'))
        self.assertNotEqual(mutated, self.text)
        self.rejects(mutated, "walltime gate must be required")

    def test_evidence_upload_must_survive_failure(self):
        block = job_block(self.text, SHARD_JOB)
        mutated = self.text.replace(block, block.replace(
            "        if: ${{ always() }}\n        uses: actions/upload-artifact",
            "        uses: actions/upload-artifact"))
        self.assertNotEqual(mutated, self.text)
        self.rejects(mutated, "even on failure")

    def test_linux_cannot_return_to_build_matrix(self):
        block = build_matrix(self.text)
        entry = "          # macOS - Clang\n"
        self.assertEqual(block.count(entry), 1)
        mutated = self.text.replace(block, block.replace(
            entry, "          - os: ubuntu-26.04\n"
            "            compiler: clang\n            cc: clang\n"
            "            timeout_minutes: 90\n\n" + entry))
        self.assertNotEqual(mutated, self.text)
        with self.assertRaisesRegex(AssertionError, "one entry per platform"):
            validate(mutated)
        with self.assertRaisesRegex(AssertionError, "unsharded"):
            validate_linux_clang_shards(mutated)


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


class TidySchedulingTests(unittest.TestCase):
    FLAG = "priority: 100,\n       is_parallel: false)"
    MESSAGE = "must stay is_parallel: false"

    @classmethod
    def setUpClass(cls):
        cls.tests = TEST_MESON.read_text(encoding="utf-8")

    def mutate(self, text, old, new):
        self.assertEqual(text.count(old), 1, old)
        mutated = text.replace(old, new)
        self.assertNotEqual(mutated, text)
        return mutated

    def rejects(self, mutated):
        with self.assertRaisesRegex(AssertionError, self.MESSAGE):
            validate_tidy_scheduling(mutated)

    def test_actual_ratchet_scheduling(self):
        validate_tidy_scheduling(self.tests)

    def test_parallel_ratchet_is_rejected(self):
        self.rejects(self.mutate(self.tests, self.FLAG, "priority: 100)"))

    def test_explicit_true_is_rejected(self):
        self.rejects(self.mutate(self.tests, self.FLAG,
                                 "priority: 100,\n       is_parallel: true)"))

    def test_commented_flag_is_rejected(self):
        self.rejects(self.mutate(
            self.tests, self.FLAG,
            "priority: 100,\n       # is_parallel: false\n       )"))

    def test_flag_in_a_string_is_rejected(self):
        self.rejects(self.mutate(
            self.tests, self.FLAG,
            "priority: 100,\n       env: ['is_parallel: false'])"))

    def test_flag_on_sibling_does_not_count(self):
        mutated = self.mutate(self.tests, self.FLAG, "priority: 100)")
        mutated = self.mutate(mutated, "timeout: 300,\n       priority: 100)",
                              "timeout: 300,\n       priority: 100,\n"
                              "       is_parallel: false)")
        self.rejects(mutated)


if __name__ == "__main__":
    unittest.main()
