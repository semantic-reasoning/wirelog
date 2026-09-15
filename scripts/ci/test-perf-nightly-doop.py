#!/usr/bin/env python3
"""Verify the authoritative perf-nightly DOOP wiring is still active."""

from pathlib import Path
import re
import unittest


WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/perf-nightly.yml"


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


def validate(text: str) -> None:
    stable = job(text, "perf-stable")
    assert re.search(r"^    runs-on: ubuntu-latest$", stable, re.MULTILINE), \
        "authoritative DOOP path must run on a hosted Ubuntu runner"
    assert "self-hosted" not in stable, \
        "authoritative DOOP path must not require a self-hosted runner"
    assert "continue-on-error" not in stable, \
        "authoritative DOOP path must not hide failures"
    install = field(step(stable, "Install dependencies"), "run")
    assert "curl" in install and "unzip" in install, \
        "stable runner must install DOOP download tools"

    host = field(step(stable, "Verify hosted DOOP capacity"), "run")
    assert "taskset -pc $$" in host, \
        "hosted path must verify an actual permitted CPU affinity"

    download = field(step(stable, "Download pinned DOOP dataset"), "run")
    assert download.strip() == "bench/data/doop/download.sh", \
        "stable path must download the pinned DOOP dataset"

    evaluator = step(stable, "Run evaluator correctness gates")
    evaluator_run = field(evaluator, "run")
    assert "crdt_correctness_full cspa_correctness" in evaluator_run
    assert "doop_w8_gate" not in evaluator_run, \
        "W=1 evaluator invocation must not hide DOOP on CPU 0"

    doop = step(stable, "Run DOOP W=8 timing gate")
    assert "if: always()" in doop, \
        "DOOP must run even when an earlier correctness gate fails"
    doop_env = field(doop, "env")
    for variable in (
        "WIRELOG_PERF_GATE: '1'",
        "WIRELOG_PERF_REQUIRE: '1'",
        "WIRELOG_DOOP_PERF_MODE: required-hosted",
        "WL_DOOP_PERF_GATE_TARGET_MS: ${{ vars.WL_DOOP_PERF_GATE_TARGET_MS }}",
    ):
        assert variable in doop_env, f"DOOP step must set {variable}"
    doop_run = field(doop, "run")
    assert 'taskset -c "$doop_affinity" meson test -C build-perf-error doop_w8_gate' in doop_run, \
        "DOOP must run with the verified permitted CPU affinity"
    assert "--logbase doop-testlog" in doop_run
    assert "doop-testlog.txt >>" in doop_run
    assert 'exit "$rc"' in doop_run

    verify = field(step(stable, "Verify DOOP gate executed"), "run")
    assert "check-doop-perf-gate-execution.sh" in verify
    evidence = field(step(stable, "Capture DOOP execution evidence"), "run")
    assert '"workers": 8' in evidence and '"repeat": 5' in evidence
    upload = field(step(stable, "Upload DOOP execution evidence"), "with")
    assert "perf-artifacts/doop" in upload


class PerfNightlyDoopWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = WORKFLOW.read_text(encoding="utf-8")

    def test_actual_workflow(self):
        validate(self.text)

    def test_doop_removal_fails(self):
        mutated = self.text.replace(
            'taskset -c "$doop_affinity" meson test -C build-perf-error doop_w8_gate',
            'taskset -c "$doop_affinity" meson test -C build-perf-error',
        )
        with self.assertRaisesRegex(AssertionError, "DOOP must"):
            validate(mutated)

    def test_single_cpu_affinity_fails(self):
        mutated = self.text.replace(
            'taskset -c "$doop_affinity" meson test -C build-perf-error doop_w8_gate',
            "taskset -c 0 meson test -C build-perf-error doop_w8_gate")
        with self.assertRaisesRegex(AssertionError, "DOOP must"):
            validate(mutated)

    def test_doop_always_run_guard_fails_when_removed(self):
        mutated = self.text.replace(
            "      - name: Run DOOP W=8 timing gate\n        if: always()\n",
            "      - name: Run DOOP W=8 timing gate\n")
        with self.assertRaisesRegex(AssertionError, "DOOP must run"):
            validate(mutated)


if __name__ == "__main__":
    unittest.main()
