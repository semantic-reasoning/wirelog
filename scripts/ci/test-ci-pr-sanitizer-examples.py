#!/usr/bin/env python3
"""Keep the "sanitizer legs do not build examples" contract in ci-pr.yml.

The examples/ demos each re-compile the full wirelog_sources, so building
them under ASan/UBSan/TSan instrumentation is expensive for little value:
the example suites (golden-diff and demo smoke tests) are already exercised
by the non-sanitized build legs.  The sanitizer legs therefore configure with
-Dexamples=false (the `examples` meson option, default true).

This is intentionally a small structural check rather than a general YAML
parser.  Each sanitizer leg's `meson setup` block has a stable layout, and
checking that layout without a third-party dependency keeps the contract
portable (the same reason as test-ci-pr-sccache-stats-daemon.py).

The check is bidirectional:
  * every `meson setup` that enables a sanitizer (b_sanitize != none/auto)
    MUST also pass -Dexamples=false; and
  * a plain (non-sanitizer) `meson setup` MUST NOT pass -Dexamples=false,
    so the plain legs keep building (and testing) the examples.
"""

from pathlib import Path
import re
import unittest


WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/ci-pr.yml"


def setup_blocks(text: str):
    """Yield (build_dir, [flags]) for every `meson setup ...` invocation.

    The setup command and its -D flags live on consecutive lines (with or
    without trailing backslashes); collect them as one logical command.
    """
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        m = re.search(r"meson setup\s+(\S+)", line)
        if m:
            build_dir = m.group(1)
            flags = []
            j = i
            # Same line may already carry flags.
            flags += re.findall(r"-D[A-Za-z0-9_]+=[^\s\\]+", line)
            j = i + 1
            while j < len(lines):
                nxt = lines[j].strip()
                if nxt.startswith("-D"):
                    flags += re.findall(r"-D[A-Za-z0-9_]+=[^\s\\]+", nxt)
                    j += 1
                elif nxt in ("", ) or nxt.startswith("#") or nxt.startswith("env:") or nxt.startswith("CC:") or nxt.startswith("CXX:"):
                    if nxt == "":
                        break
                    if nxt.startswith("#"):
                        j += 1
                        continue
                    break
                else:
                    break
            yield build_dir, flags
            i = j
        else:
            i += 1


def sanitizer_flags(flags):
    for f in flags:
        if f.startswith("-Db_sanitize="):
            value = f[len("-Db_sanitize="):]
            return value not in ("none", "auto", "")
    return False


def validate(text: str) -> None:
    blocks = list(setup_blocks(text))
    assert blocks, "no `meson setup` invocations found in ci-pr.yml"

    sanitized = [(d, f) for d, f in blocks if sanitizer_flags(f)]
    assert sanitized, "no sanitizer `meson setup` blocks found (expected some)"

    for build_dir, flags in sanitized:
        assert "-Dexamples=false" in flags, (
            f"sanitizer setup for {build_dir} must pass -Dexamples=false "
            f"to skip the expensive examples/ demos "
            f"(found flags: {flags!r})"
        )

    for build_dir, flags in blocks:
        if not sanitizer_flags(flags):
            assert "-Dexamples=false" not in flags, (
                f"non-sanitizer setup for {build_dir} must NOT disable "
                f"examples; the plain legs are what build and test them "
                f"(found flags: {flags!r})"
            )


class CiPrSanitizerExamplesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = WORKFLOW.read_text(encoding="utf-8")

    def test_actual_workflow(self):
        validate(self.text)

    def test_sanitizer_leg_without_examples_flag_is_rejected(self):
        # Remove the flag from exactly one sanitizer leg -- its own line, not
        # the explanatory comment that also mentions the literal -- and expect
        # rejection.
        kept = []
        removed = False
        for line in self.text.splitlines(keepends=True):
            if not removed and line.strip() == "-Dexamples=false":
                removed = True
                continue
            kept.append(line)
        self.assertTrue(removed, "no standalone -Dexamples=false flag line found")
        mutated = "".join(kept)
        self.assertNotEqual(mutated, self.text)
        with self.assertRaisesRegex(AssertionError, "-Dexamples=false"):
            validate(mutated)

    def test_plain_leg_disabling_examples_is_rejected(self):
        # Force a plain (non-sanitizer) leg to disable examples and expect
        # rejection, so the plain legs keep covering the examples suites.
        plain = re.search(
            r"meson setup builddir[^\n]*\n(?:\s+-D[A-Za-z0-9_]+=[^\n]*\n)*",
            self.text,
        )
        self.assertIsNotNone(plain, "no plain `meson setup builddir` block found")
        block = plain.group(0)
        self.assertFalse(
            re.search(r"b_sanitize", block),
            "expected a non-sanitizer block, but it sets b_sanitize",
        )
        mutated = self.text.replace(block, block.rstrip("\n") + " -Dexamples=false\n", 1)
        self.assertNotEqual(mutated, self.text)
        with self.assertRaisesRegex(AssertionError, "must NOT"):
            validate(mutated)


if __name__ == "__main__":
    unittest.main()
