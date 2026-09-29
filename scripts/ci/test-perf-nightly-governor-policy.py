#!/usr/bin/env python3
"""Keep the governor exception limited to Linux perf-nightly execution."""

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]
NIGHTLY = ROOT / ".github/workflows/perf-nightly.yml"
REQUIRED = ROOT / ".github/workflows/perf-suite-required.yml"
RELEASE = ROOT / ".github/workflows/release-tag.yml"
MARKER = "WIRELOG_PERF_NIGHTLY: '1'"


def step(workflow: str, name: str) -> str:
    matches = re.findall(
        rf"^      - name: {re.escape(name)}\n(.*?)(?=^      - |^  \S|\Z)",
        workflow, re.MULTILINE | re.DOTALL,
    )
    assert len(matches) == 1, f"expected exactly one step: {name}"
    return matches[0]


def validate(nightly: str, required: str, release: str) -> None:
    linux = step(nightly, "Run perf suite (Linux)")
    assert "if: runner.os == 'Linux'" in linux
    assert "WIRELOG_PERF_GATE: '1'" in linux
    assert MARKER in linux, "Linux nightly must opt out of governor eligibility"
    assert "taskset -c 0 meson test -C build-perf --suite perf" in linux
    assert nightly.count(MARKER) == 1, "marker must belong only to Linux perf suite"
    assert MARKER not in step(nightly, "Run perf suite (Windows)")
    assert MARKER not in required and MARKER not in release, \
        "required and release paths must retain governor eligibility"

    observe = step(nightly, "Set cpufreq governor (Linux, best-effort)")
    assert "scaling_governor" in observe
    assert "cpupower frequency-set -g performance" in observe

    capture = step(nightly, "Capture Linux perf suite evidence")
    assert "always() && runner.os == 'Linux'" in capture
    for evidence in ("github.sha", "github.run_id", "RUNNER_NAME",
                     "taskset -pc $$", "scaling_governor", "mode=nightly",
                     "governor_eligibility=bypassed", "meson-logs/testlog.txt"):
        assert evidence in capture, f"missing nightly evidence: {evidence}"
    assert 'echo "governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"' in capture, \
        "nightly artifact must record the observed governor"
    assert "echo 'governor=unavailable'" in capture, \
        "nightly artifact must record an unavailable governor"
    upload = step(nightly, "Upload Linux perf suite evidence")
    assert "always() && runner.os == 'Linux'" in upload
    assert "perf-artifacts/linux-suite" in upload
    assert "if-no-files-found: error" in upload


class GovernorPolicyContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.nightly = NIGHTLY.read_text(encoding="utf-8")
        cls.required = REQUIRED.read_text(encoding="utf-8")
        cls.release = RELEASE.read_text(encoding="utf-8")

    def test_workflow(self):
        validate(self.nightly, self.required, self.release)

    def test_global_marker_fails(self):
        changed = self.nightly.replace(
            "WIRELOG_PERF_GATE: '1'\n        run:",
            "WIRELOG_PERF_GATE: '1'\n          " + MARKER + "\n        run:",
            1,
        )
        with self.assertRaises(AssertionError):
            validate(changed, self.required, self.release)

    def test_missing_affinity_fails(self):
        changed = self.nightly.replace(
            "taskset -c 0 meson test -C build-perf --suite perf",
            "meson test -C build-perf --suite perf",
            1,
        )
        with self.assertRaises(AssertionError):
            validate(changed, self.required, self.release)

    def test_missing_capture_observation_fails(self):
        changed = self.nightly.replace(
            'echo "governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"',
            "echo 'governor=omitted'", 1,
        )
        with self.assertRaisesRegex(AssertionError, "observed governor"):
            validate(changed, self.required, self.release)


if __name__ == "__main__":
    unittest.main()
