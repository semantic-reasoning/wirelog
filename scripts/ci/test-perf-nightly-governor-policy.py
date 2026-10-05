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
RUNNER = ROOT / "scripts/ci/run-perf-nightly-linux.sh"


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
    assert "run-perf-nightly-linux.sh" in linux
    runner = RUNNER.read_text(encoding="utf-8")
    assert "WIRELOG_PERF_GATE=1 WIRELOG_PERF_REQUIRE=1" in runner
    assert "WIRELOG_PERF_GATE=1 WIRELOG_PERF_NIGHTLY=1" in runner, \
        "the legacy trace suite retains its existing governor policy"
    assert MARKER not in step(nightly, "Run perf suite (Windows)")
    assert MARKER not in required and MARKER not in release, \
        "required and release paths must retain governor eligibility"

    observe = step(nightly, "Set cpufreq governor (Linux, best-effort)")
    assert "scaling_governor" in observe
    assert "cpupower frequency-set -g performance" in observe

    before = step(nightly, "Capture Linux perf host before suite")
    assert (nightly.index("      - name: Capture Linux perf host before suite")
            < nightly.index("      - name: Run perf suite (Linux)")
            < nightly.index("      - name: Capture Linux perf suite evidence")), \
        "Linux perf evidence must be captured before and after the suite in order"
    assert "if: runner.os == 'Linux'" in before
    for evidence in ("host-before.txt", "RUNNER_NAME", "requested_affinity=0",
                     "boot_id=", "loadavg=", "cpu.stat", "/proc/pressure/cpu",
                     "scaling_governor", "scaling_cur_freq", "scaling_min_freq",
                     "scaling_max_freq", "cpuinfo_cur_freq",
                     'echo "$name=unavailable"'):
        assert evidence in before, f"missing pre-suite evidence: {evidence}"

    capture = step(nightly, "Capture Linux perf suite evidence")
    assert "always() && runner.os == 'Linux'" in capture
    for evidence in ("github.sha", "github.run_id", "RUNNER_NAME",
                     "taskset -pc $$", "scaling_governor", "mode=nightly",
                     "governor_eligibility=required", "meson-logs/testlog.txt",
                     "source_tree=", "requested_affinity=0", "boot_id=",
                     "cpu_model=", "loadavg=", "cpu.stat", "/proc/pressure/cpu",
                     "scaling_cur_freq", "scaling_min_freq", "scaling_max_freq",
                     "cpuinfo_cur_freq", "sha256.txt",
                     "test_crdt_perf_gate", "test_cspa_perf_gate",
                     "bench/data/crdt/Insert_input.csv",
                     "bench/data/crdt/Remove_input.csv",
                     "bench/data/cspa/assign.csv",
                     "bench/data/cspa/dereference.csv"):
        assert evidence in capture, f"missing nightly evidence: {evidence}"
    assert 'echo "governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"' in capture, \
        "nightly artifact must record the observed governor"
    assert "echo 'governor=unavailable'" in capture, \
        "nightly artifact must record an unavailable governor"
    assert 'echo "$name=unavailable"' in capture, \
        "nightly artifact must record an unavailable CPU frequency"
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
            "run-perf-nightly-linux.sh", "missing-runner.sh", 1,
        )
        with self.assertRaises(AssertionError):
            validate(changed, self.required, self.release)

    def test_missing_affinity_fails(self):
        changed = self.nightly.replace(
            "run-perf-nightly-linux.sh", "meson test --suite perf",
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

    def test_missing_frequency_fallback_fails(self):
        prefix, marker, suffix = self.nightly.rpartition('echo "$name=unavailable"')
        assert marker
        changed = prefix + 'echo "$name=omitted"' + suffix
        with self.assertRaisesRegex(AssertionError, "unavailable CPU frequency"):
            validate(changed, self.required, self.release)

    def test_missing_before_snapshot_fails(self):
        changed = self.nightly.replace(
            "host-before.txt", "host-before-omitted.txt", 1,
        )
        with self.assertRaisesRegex(AssertionError, "pre-suite evidence"):
            validate(changed, self.required, self.release)

    def test_before_snapshot_after_suite_fails(self):
        before_name = "      - name: Capture Linux perf host before suite\n"
        suite_name = "      - name: Run perf suite (Linux)\n"
        after_name = "      - name: Capture Linux perf suite evidence\n"
        start = self.nightly.index(before_name)
        end = self.nightly.index(suite_name, start)
        before_block = self.nightly[start:end]
        changed = self.nightly[:start] + self.nightly[end:]
        changed = changed.replace(after_name, before_block + after_name, 1)
        with self.assertRaisesRegex(AssertionError, "in order"):
            validate(changed, self.required, self.release)


if __name__ == "__main__":
    unittest.main()
