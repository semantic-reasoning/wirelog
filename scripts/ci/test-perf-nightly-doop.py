#!/usr/bin/env python3
"""Verify the authoritative perf-nightly DOOP wiring is still active."""

from pathlib import Path
import re
import unittest


WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/perf-nightly.yml"
RUNNER = Path(__file__).resolve().parent / "run-perf-stable-linux.sh"


def job(text: str, name: str) -> str:
    match = re.search(
        rf"^  {re.escape(name)}:\n(.*?)(?=^  \S|\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    assert match, f"missing job: {name}"
    return match[1]


def step(text: str, name: str) -> str:
    matches = re.findall(
        rf"^      - name: {re.escape(name)}\n(.*?)(?=^      - |^  \S|\Z)",
        text,
        re.MULTILINE | re.DOTALL,
    )
    assert len(matches) == 1, f"expected exactly one step: {name}"
    return matches[0]


def field(body: str, key: str) -> str:
    match = re.search(
        rf"^        {re.escape(key)}:(?: [|>])?\n((?:          [^\n]*\n)+)",
        body,
        re.MULTILINE,
    )
    if match:
        return match[1]
    inline = re.search(rf"^        {re.escape(key)}: ([^\n]+)$", body,
                       re.MULTILINE)
    assert inline, f"missing {key} block"
    return inline[1] + "\n"


def validate(text: str, runner_text: str | None = None) -> None:
    if runner_text is None:
        runner_text = RUNNER.read_text(encoding="utf-8")
    stable = job(text, "perf-stable")
    assert re.search(
        r"^    runs-on: \[self-hosted, Linux, X64, perf\]$",
        stable, re.MULTILINE), \
        "authoritative DOOP path must run on the perf runner"
    assert "continue-on-error" not in stable, \
        "authoritative DOOP path must not hide failures"
    assert "github.repository == 'semantic-reasoning/wirelog'" in stable
    assert "github.ref == 'refs/heads/main'" in stable
    assert "group: wirelog-perf-linux" in stable
    assert "queue: max" in stable and "cancel-in-progress: false" in stable

    host = field(step(stable, "Verify perf runner capacity"), "run")
    assert "taskset -pc $$" in host, \
        "perf path must verify an actual permitted CPU affinity"

    wrapper = step(stable, "Run correctness and DOOP gates in the local perf image")
    assert "run-perf-stable-linux.sh" in field(wrapper, "run"), \
        "DOOP must use its local image wrapper"
    target = field(wrapper, "env")
    assert "WL_DOOP_PERF_GATE_TARGET_MS: ${{ vars.WL_DOOP_PERF_GATE_TARGET_MS }}" in target
    assert "image inspect" in runner_text and "run --pull=never" in runner_text
    assert '--user "$(id -u):$(id -g)"' in runner_text
    assert "--cpuset-cpus" not in runner_text, \
        "W=8 DOOP must retain the runner's full eligible CPU set"
    assert "bench/data/doop/download.sh" in runner_text
    assert "crdt_correctness_full cspa_correctness" in runner_text
    assert 'taskset -c "$affinity"' in runner_text, \
        "DOOP must retain the full available affinity for W=8"
    assert "doop_w8_gate" in runner_text and "--logbase doop-testlog" in runner_text, \
        "DOOP timing gate must remain registered and produce its isolated log"
    assert "check-doop-perf-gate-execution.sh" in runner_text
    assert runner_text.index("crdt_correctness_full cspa_correctness") \
        < runner_text.index('taskset -c "$affinity"')
    assert "set +e" in runner_text, \
        "the W=8 DOOP gate must still run when correctness fails"
    evidence = field(step(stable, "Capture DOOP execution evidence"), "run")
    assert '"workers": 8' in evidence and '"repeat": 5' in evidence
    assert '"governor": "%s"' in evidence, \
        "host evidence must retain the observed governor"
    assert '"mode": "strict-tagged"' in evidence, \
        "host evidence must report the governor-independent tagged mode"
    assert '"timing": "enforced"' in evidence, \
        "tagged mode must retain enforced timing evidence"
    upload = field(step(stable, "Upload DOOP execution evidence"), "with")
    assert "perf-artifacts/doop" in upload


class PerfNightlyDoopWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = WORKFLOW.read_text(encoding="utf-8")

    def test_actual_workflow(self):
        validate(self.text)

    def test_doop_removal_fails(self):
        runner = RUNNER.read_text(encoding="utf-8").replace(
            "doop_w8_gate", "", 1)
        with self.assertRaisesRegex(AssertionError, "timing gate"):
            validate(self.text, runner)

    def test_single_cpu_affinity_fails(self):
        runner = RUNNER.read_text(encoding="utf-8").replace(
            'taskset -c "$affinity"', 'taskset -c 0', 1)
        with self.assertRaisesRegex(AssertionError, "full available affinity"):
            validate(self.text, runner)

    def test_doop_always_run_guard_fails_when_removed(self):
        mutated = self.text.replace(
            "run-perf-stable-linux.sh", "missing-runner.sh", 1)
        with self.assertRaisesRegex(AssertionError, "local image wrapper"):
            validate(mutated)


if __name__ == "__main__":
    unittest.main()
