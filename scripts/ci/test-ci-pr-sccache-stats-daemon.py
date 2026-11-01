#!/usr/bin/env python3
"""Keep the sccache stats-daemon contract in ci-pr.yml explicit and reviewable.

sccache v0.19.0 auto-spawns an in-memory daemon (127.0.0.1:4226) that holds
ALL cache statistics; `sccache --show-stats` (run by the sccache-action post
step) connects to that daemon.  When no daemon is alive it reports the
all-zero summary `0% - 0 hits, 0 misses, 0 errors` -- the exact signature
observed in the TSan/ubuntu-26.04/gcc summary.  The daemon exits after a 600s
idle timeout by default, and the sanitizer legs (tsan, sanitizer-matrix) run
a ~20-minute test phase after their last compile, so the daemon is long dead
by the post step and their REAL hit rate is never reported.

This is intentionally a small structural check rather than a general YAML
parser.  The top-level env block has a stable layout, and checking that
layout without a third-party dependency keeps the ABI contract portable to
Windows.
"""

from pathlib import Path
import re
import unittest


WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/ci-pr.yml"


def top_level_env(text: str) -> str:
    match = re.search(r"^env:\n((?:  [^\n]*\n)*)", text, re.MULTILINE)
    assert match, "missing top-level env block"
    return match[1]


def validate(text: str) -> None:
    env = top_level_env(text)

    gha = re.search(r'^  SCCACHE_GHA_ENABLED: "([^"]+)"$', env, re.MULTILINE)
    assert gha, "missing SCCACHE_GHA_ENABLED in the top-level env block"
    assert gha[1] == "true", "SCCACHE_GHA_ENABLED must stay enabled"

    idle = re.findall(r'^  SCCACHE_IDLE_TIMEOUT: "([^"]+)"$',
                      env, re.MULTILINE)
    assert idle == ["0"], (
        "SCCACHE_IDLE_TIMEOUT must pin the daemon to stay alive for the "
        f"whole job so the sanitizer legs' real hit rate is reported "
        f"(expected exactly [\"0\"], found {idle!r})")


class CiPrSccacheStatsDaemonTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = WORKFLOW.read_text(encoding="utf-8")

    def test_actual_workflow(self):
        validate(self.text)

    def test_idle_timeout_line_is_rejected_when_removed(self):
        mutated = self.text.replace('\n  SCCACHE_IDLE_TIMEOUT: "0"', "")
        self.assertNotEqual(mutated, self.text)
        with self.assertRaisesRegex(AssertionError, "SCCACHE_IDLE_TIMEOUT"):
            validate(mutated)

    def test_idle_timeout_default_is_rejected(self):
        mutated = self.text.replace('SCCACHE_IDLE_TIMEOUT: "0"',
                                    'SCCACHE_IDLE_TIMEOUT: "600"')
        self.assertNotEqual(mutated, self.text)
        with self.assertRaisesRegex(AssertionError, "SCCACHE_IDLE_TIMEOUT"):
            validate(mutated)

    def test_gha_backend_must_stay_enabled(self):
        mutated = self.text.replace('SCCACHE_GHA_ENABLED: "true"',
                                    'SCCACHE_GHA_ENABLED: "false"')
        self.assertNotEqual(mutated, self.text)
        with self.assertRaisesRegex(AssertionError, "SCCACHE_GHA_ENABLED"):
            validate(mutated)


if __name__ == "__main__":
    unittest.main()
