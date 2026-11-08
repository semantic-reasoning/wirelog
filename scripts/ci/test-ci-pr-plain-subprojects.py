#!/usr/bin/env python3
"""Keep the "sanitizer builds keep the bundled subprojects plain" contract.

Under ASan/UBSan/TSan the third-party subprojects (nanoarrow, and the
bundled xxHash fallback) do not need instrumentation: they are already
covered by the plain build legs, and instrumenting them only adds compile
time and sanitizer noise from code we do not maintain.  The plain
subproject options are therefore derived from the existing `b_sanitize`
meson option: when a sanitizer is enabled (anything other than `none` or
`memory`), meson.build passes `b_sanitize=none` to the nanoarrow and
xxHash subproject calls while wirelog itself stays fully instrumented.

Why not a dedicated meson option (e.g. `plain_subprojects`): the early
binary-size gate (Issue #1573) requires the tested merge to carry a
production profile identical to the base branch, and a new project option
changes the captured option set and fails the gate.  Deriving the plain
options from `b_sanitize` keeps the default production recipe
(b_sanitize=none) evaluating to the identical subproject options.

Assumption this check encodes: MSan (b_sanitize=memory,
tier1-sanitizers.yml) requires every linked module to be instrumented, so
the conditional must keep `memory` out of the plain treatment.

This is intentionally a small structural check rather than a general
Meson/YAML parser (same convention as test-ci-pr-sanitizer-examples.py).
"""

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]
MESON_BUILD = ROOT / "meson.build"
MESON_OPTIONS = ROOT / "meson_options.txt"
WORKFLOW = ROOT / ".github/workflows/ci-pr.yml"

SAN_VAR = "_b_sanitize"
PLAIN_VAR = "_plain_subproject_options"


def validate_meson(text: str) -> None:
    """Pin the b_sanitize-derived plain subproject wiring in meson.build."""
    assert re.search(
        r"^\s*" + SAN_VAR + r"\s*=\s*get_option\('b_sanitize'\)\s*$",
        text,
        re.M,
    ), (
        f"{SAN_VAR} must be read from the existing b_sanitize option "
        f"(no new project option: the binary-size gate fails on a changed "
        f"production profile)"
    )
    assert re.search(
        r"^\s*" + PLAIN_VAR + r"\s*=\s*\['b_sanitize=none'\]\s*$",
        text,
        re.M,
    ), f"sanitizer builds must pass 'b_sanitize=none' to the bundled subprojects"
    assert re.search(
        r"^\s*" + PLAIN_VAR + r"\s*=\s*\[\]\s*$",
        text,
        re.M,
    ), "plain (b_sanitize=none/memory) builds must keep the default subproject options"
    assert re.search(
        r"if\s+" + SAN_VAR + r"\s*!=\s*'none'\s*and\s*not\s*"
        + SAN_VAR + r"\.contains\('memory'\)",
        text,
    ), (
        "the plain treatment must apply to every sanitizer except "
        "none/memory: MSan requires every linked module instrumented"
    )
    assert re.search(
        r"subproject\('nanoarrow',\s*default_options:\s*\[[^\]]*\]\s*\+\s*"
        + PLAIN_VAR + r"\s*\)",
        text,
    ), "the nanoarrow subproject call must apply the plain subproject options"
    assert re.search(
        r"subproject\('xxhash',\s*default_options:\s*" + PLAIN_VAR + r"\s*\)",
        text,
    ), "the xxhash subproject call must apply the plain subproject options"


def validate_no_new_option(options_text: str) -> None:
    assert "plain_subprojects" not in options_text, (
        "do not reintroduce a dedicated plain_subprojects meson option: a "
        "new project option changes the production profile captured by the "
        "early binary-size gate (Issue #1573) and fails the base/head "
        "comparison"
    )


def setup_blocks(text: str):
    """Yield (build_dir, [flags]) for every `meson setup ...` invocation."""
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        m = re.search(r"meson setup\s+(\S+)", lines[i])
        if m:
            build_dir = m.group(1)
            flags = re.findall(r"-D[A-Za-z0-9_]+=[^\s\\]+", lines[i])
            j = i + 1
            while j < len(lines):
                nxt = lines[j].strip()
                if nxt.startswith("-D"):
                    flags += re.findall(r"-D[A-Za-z0-9_]+=[^\s\\]+", nxt)
                    j += 1
                elif nxt == "":
                    break
                elif nxt.startswith("#"):
                    j += 1
                    continue
                else:
                    break
            yield build_dir, flags
            i = j
        else:
            i += 1


def b_sanitize_value(flags):
    for f in flags:
        if f.startswith("-Db_sanitize="):
            return f[len("-Db_sanitize="):]
    return None


def validate_workflow(text: str) -> None:
    """Pin that the PR CI sanitizer legs still drive the plain treatment.

    The plain subproject options are a function of b_sanitize, so the
    contract on the workflow side is: the sanitizer legs enable a
    sanitizer (that is what makes the subprojects plain), and the plain
    production legs (including the size-gate job) keep the default
    b_sanitize so the production recipe stays gate-identical.
    """
    blocks = list(setup_blocks(text))
    assert blocks, "no `meson setup` invocations found in ci-pr.yml"

    sanitized = [(d, f) for d, f in blocks if b_sanitize_value(f) not in (None, "none", "")]
    assert sanitized, (
        "no sanitizer `meson setup` block found in ci-pr.yml: the plain "
        "subproject treatment (b_sanitize-derived) has no driver"
    )
    plain = [(d, f) for d, f in blocks if b_sanitize_value(f) is None]
    assert plain, (
        "no non-sanitizer `meson setup` block found in ci-pr.yml: the "
        "plain production legs (including the binary-size gate job) are "
        "missing"
    )


class PlainSubprojectsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.meson_text = MESON_BUILD.read_text(encoding="utf-8")
        cls.options_text = MESON_OPTIONS.read_text(encoding="utf-8")
        cls.workflow_text = WORKFLOW.read_text(encoding="utf-8")

    def test_meson_wiring(self):
        validate_meson(self.meson_text)

    def test_no_dedicated_option(self):
        validate_no_new_option(self.options_text)

    def test_ci_driver(self):
        validate_workflow(self.workflow_text)

    def test_nanoarrow_plain_application_removed_is_rejected(self):
        mutated = self.meson_text.replace(
            "testing=disabled'] + _plain_subproject_options)",
            "testing=disabled'])",
            1,
        )
        self.assertNotEqual(mutated, self.meson_text)
        with self.assertRaisesRegex(AssertionError, "nanoarrow"):
            validate_meson(mutated)

    def test_memory_exclusion_removed_is_rejected(self):
        mutated = self.meson_text.replace(
            "and not _b_sanitize.contains('memory')",
            "",
            1,
        )
        self.assertNotEqual(mutated, self.meson_text)
        with self.assertRaisesRegex(AssertionError, "MSan"):
            validate_meson(mutated)

    def test_dedicated_option_reintroduced_is_rejected(self):
        mutated = self.options_text + "\noption(\n  'plain_subprojects',\n)\n"
        self.assertNotEqual(mutated, self.options_text)
        with self.assertRaisesRegex(AssertionError, "plain_subprojects"):
            validate_no_new_option(mutated)

    def test_sanitizer_legs_removed_are_rejected(self):
        mutated = re.sub(
            r"^\s*-Db_sanitize=[^\s\\]*\s*\\?\s*$", "", self.workflow_text, flags=re.M
        )
        mutated = re.sub(r"-Db_sanitize=[^\s\\]+", "", mutated)
        self.assertNotEqual(mutated, self.workflow_text)
        with self.assertRaisesRegex(AssertionError, "driver"):
            validate_workflow(mutated)


if __name__ == "__main__":
    unittest.main()
