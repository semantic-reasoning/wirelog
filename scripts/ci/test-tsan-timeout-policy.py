#!/usr/bin/env python3
"""Keep the POSIX TSan timeout multiplier identical across every workflow.

Issue #1728.  Runtime POSIX TSan legs run the whole suite under a sanitizer
that makes some tests approach their declared Meson budget, so they run with
an explicit timeout multiplier.  The multiplier only helps if every leg
carries it and none drifts, so this gate pins the literal command text.

Like its sibling scripts/ci/test-ci-pr-build-timeouts.py this is deliberately
a structural check rather than a general YAML parser: it keeps the abi suite
free of a third-party dependency on Windows, and the thing being asserted is
literal command text that a YAML round-trip would normalise away.

Two separate build directories matter and must not be confused:

  builddir-tsan          runtime POSIX TSan, whole suite, multiplier REQUIRED
  builddir-tsan-native   compile smoke with -Dthreads=native, one test,
                         multiplier FORBIDDEN (#1728 preserves it unchanged)

`-` is not a word character, so a `\\b` anchor after "builddir-tsan" would
also match "builddir-tsan-native" and this gate would scale the very leg it
exists to protect.  The negative lookahead below is load-bearing.
"""

from pathlib import Path
import re
import unittest


REPO = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO / ".github/workflows"
THREADING_DOC = REPO / "docs/THREADING.md"

MULTIPLIER = 4
# Meson accepts both spellings.  main uses the long one; accept either so a
# maintainer who reaches for `-t` is not blocked, but pin the VALUE so the
# legs cannot drift apart or away from the documented number.
MULTIPLIER_RE = re.compile(r"(?:--timeout-multiplier[= ]|-t )(\S+)")
RUNTIME_COMMAND = (
    f"meson test -C builddir-tsan --timeout-multiplier {MULTIPLIER} "
    "--print-errorlogs")
NATIVE_COMMAND = (
    "meson test -C builddir-tsan-native threading_doc --print-errorlogs")

# One runtime invocation per gating workflow.  A workflow absent from this
# mapping must contribute none, so a new unscaled leg written in the
# canonical form cannot appear quietly.  Equivalent spellings a shell would
# accept -- `-C ./builddir-tsan`, a quoted path, `cd` then `meson test` --
# are outside the pattern by construction; the contract here is that an
# existing leg cannot be reverted, not that every possible way to write a
# new one is policed.
RUNTIME_LEGS = {
    "ci-pr.yml": 1,
    "ci-main.yml": 1,
    "tier1-sanitizers.yml": 1,
}

RUNTIME_RE = re.compile(r"meson test -C builddir-tsan(?![\w-])[^\n]*")
NATIVE_RE = re.compile(r"meson test -C builddir-tsan-native[^\n]*")


def runtime_invocations(text: str) -> list:
    return RUNTIME_RE.findall(text)


def validate(sources: dict, doc: str) -> None:
    """@sources maps workflow basename to its text; @doc is THREADING.md."""
    counts = {}
    for name, text in sorted(sources.items()):
        found = runtime_invocations(text)
        if found:
            counts[name] = len(found)
        for command in found:
            # Distinguish the two ways a leg goes wrong, so the failure names
            # the actual defect: a reverted leg carries no multiplier at all,
            # a drifted one carries the wrong number.
            multiplier = MULTIPLIER_RE.search(command)
            assert multiplier, (
                f"{name}: unscaled runtime TSan invocation, every leg must "
                f"carry the timeout multiplier {MULTIPLIER}: {command!r}")
            assert multiplier[1] == str(MULTIPLIER), (
                f"{name}: runtime TSan multiplier must be exactly "
                f"{MULTIPLIER}, found {multiplier[1]!r}; update "
                f"docs/THREADING.md together with the workflows")

            for required in ("-C builddir-tsan", "--print-errorlogs"):
                assert required in command, (
                    f"{name}: runtime TSan invocation lost {required!r}: "
                    f"{command!r}")

    assert counts == RUNTIME_LEGS, (
        f"expected exactly one runtime TSan invocation per gating workflow "
        f"{RUNTIME_LEGS!r}, found {counts!r}")

    native = [command
              for name, text in sorted(sources.items())
              for command in NATIVE_RE.findall(text)
              if name == "ci-pr.yml"]
    elsewhere = [name
                 for name, text in sorted(sources.items())
                 if name != "ci-pr.yml" and NATIVE_RE.search(text)]
    assert not elsewhere, (
        f"native compile-smoke leg must live only in ci-pr.yml, also found "
        f"in {elsewhere!r}")
    assert len(native) == 1, (
        f"missing native compile-smoke leg in ci-pr.yml, found {native!r}")
    assert native[0] == NATIVE_COMMAND, (
        f"native compile-smoke leg must not be scaled and must not change: "
        f"{native[0]!r}")

    # Acceptance criterion 1 asks for the same *documented* policy.  Pin the
    # multiplier in the doc from the same constant, so prose and workflows
    # cannot drift apart silently.
    assert f"`--timeout-multiplier {MULTIPLIER}`" in doc, (
        f"docs/THREADING.md must document the {MULTIPLIER} multiplier")


def read_sources() -> dict:
    return {path.name: path.read_text(encoding="utf-8")
            for path in sorted(WORKFLOW_DIR.glob("*.yml"))}


class TsanTimeoutPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sources = read_sources()
        cls.doc = THREADING_DOC.read_text(encoding="utf-8")

    def mutate(self, name, old, new, count=1):
        sources = dict(self.sources)
        assert old in sources[name], f"fixture drift: {old!r} absent"
        sources[name] = sources[name].replace(old, new, count)
        return sources

    def test_actual_workflows(self):
        validate(self.sources, self.doc)

    def test_unscaled_revert_is_rejected(self):
        sources = self.mutate("tier1-sanitizers.yml", RUNTIME_COMMAND,
                              RUNTIME_COMMAND.replace(f" --timeout-multiplier {MULTIPLIER}", ""))
        with self.assertRaisesRegex(AssertionError, "unscaled"):
            validate(sources, self.doc)

    def test_partial_revert_in_one_workflow_is_rejected(self):
        sources = self.mutate("ci-main.yml", RUNTIME_COMMAND,
                              RUNTIME_COMMAND.replace(f" --timeout-multiplier {MULTIPLIER}", ""))
        with self.assertRaisesRegex(AssertionError, "unscaled"):
            validate(sources, self.doc)

    def test_different_multiplier_is_rejected(self):
        sources = self.mutate("ci-pr.yml", RUNTIME_COMMAND,
                              RUNTIME_COMMAND.replace(f"--timeout-multiplier {MULTIPLIER}", "-t 5"))
        with self.assertRaisesRegex(AssertionError, "exactly"):
            validate(sources, self.doc)

    def test_missing_leg_is_rejected(self):
        sources = self.mutate("ci-main.yml", RUNTIME_COMMAND, "true")
        with self.assertRaisesRegex(AssertionError, "one runtime TSan"):
            validate(sources, self.doc)

    def test_extra_unscaled_leg_is_rejected(self):
        sources = dict(self.sources)
        sources["ci-nightly-invented.yml"] = (
            "        run: meson test -C builddir-tsan --print-errorlogs\n")
        with self.assertRaisesRegex(AssertionError, "unscaled"):
            validate(sources, self.doc)

    def test_native_smoke_must_not_be_scaled(self):
        sources = self.mutate(
            "ci-pr.yml", NATIVE_COMMAND,
            NATIVE_COMMAND.replace(
                "threading_doc",
                f"threading_doc --timeout-multiplier {MULTIPLIER}"))
        with self.assertRaisesRegex(AssertionError, "must not be scaled"):
            validate(sources, self.doc)

    def test_native_smoke_must_exist(self):
        sources = self.mutate("ci-pr.yml", NATIVE_COMMAND, "true")
        with self.assertRaisesRegex(AssertionError, "missing native"):
            validate(sources, self.doc)

    def test_native_builddir_is_not_counted_as_runtime(self):
        """The lookahead must keep builddir-tsan-native out of the runtime set."""
        self.assertEqual(
            runtime_invocations(
                "meson test -C builddir-tsan-native threading_doc"), [])

    def test_unrelated_tsan_builddir_is_not_policed(self):
        """perf-nightly.yml runs TSan in build-stress-tsan; leave it alone."""
        for name, text in self.sources.items():
            if name != "perf-nightly.yml":
                continue
            self.assertEqual(runtime_invocations(text), [],
                             "perf-nightly.yml must not be policed")

    def test_doc_drift_is_rejected(self):
        with self.assertRaisesRegex(AssertionError, "must document"):
            validate(self.sources, self.doc.replace(
                f"`--timeout-multiplier {MULTIPLIER}`", "`-t 2`"))


if __name__ == "__main__":
    unittest.main()
