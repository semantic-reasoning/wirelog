#!/usr/bin/env python3
"""Contract and evidence-classification tests for hosted PR calibration."""

from pathlib import Path
from contextlib import redirect_stdout
import io
import json
import re
import tempfile
import unittest

import hosted_perf_calibration as calibration


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/perf-hosted-calibration.yml"
EXPECTED = {
    "pr_number": "2085",
    "base_sha": "b" * 40,
    "head_sha": "h" * 40,
    "merge_sha": "m" * 40,
    "state": "open",
    "base_repo": "semantic-reasoning/wirelog",
    "base_ref": "main",
}
ORDINAL_SIDES = {
    "01": ("base", "head"), "02": ("head", "base"),
    "03": ("base", "head"), "04": ("head", "base"),
    "05": ("base", "base"), "06": ("head", "head"),
}


def gate_log(gate: str, *, miss: bool = False, wrong: bool = False) -> str:
    test_name = f"test_{gate}_perf_gate"
    if wrong:
        return (f"{test_name}: FAIL: trial 1 correctness sentinel mismatch\n"
                "result: exit status 1\n")
    if gate == "crdt":
        result = 0 if wrong else 104851
        correctness = (f"{test_name}: trials=9 workers=1\n"
                       f"  result = {result} (expected 104851)\n"
                       "  iterations = 6\n")
        target = 38120
    else:
        tuples = 0 if wrong else 20381
        correctness = (f"{test_name}: trials=9 workers=1\n"
                       f"  tuples = {tuples}/20,381\n"
                       "  iterations = 6/6\n")
        target = 2050
    start = target + 20 if miss else target - 100
    trials = [start + index * 10 for index in range(9)]
    raw = " ".join(f"{value:.1f}" for value in trials)
    median = trials[len(trials) // 2]
    mean = sum(trials) / len(trials)
    ending = (f"{test_name}: FAIL: median 110.0 ms exceeds target {target} ms "
              "(regression)\n" if miss else f"{test_name} OK\n")
    if miss:
        ending = (f"{test_name}: FAIL: median {median:.1f} ms exceeds target "
                  f"{target} ms (regression)\n")
    status = 1 if miss else 0
    return (f"{test_name}: raw_ms = {raw}\n"
            f"  mean_ms = {mean:.1f}\n  stdev_ms = 3.0 (CoV 0.100%)\n"
            f"  median_ms = {median:.1f} (target {target})\n"
            f"{correctness}{ending}result: exit status {status}\n")


def result_for(gate: str, *, status: str = "pass", target: int | None = None) -> dict:
    output = gate_log(gate, miss=status == "target_miss")
    exit_code = 1 if status == "target_miss" else 0
    if status == "skip":
        output = (f"test_{gate}_perf_gate: SKIP: governor unavailable\n"
                  "result: exit status 77\n")
        exit_code = 0
    elif status == "correctness_failed":
        output, exit_code = gate_log(gate, wrong=True), 1
    parsed = calibration.parse_gate(gate, exit_code, output)
    parsed["evidence_complete"] = True
    parsed["host-before"] = {"profile_sha256": calibration.PROFILE_SHA256}
    parsed["host-after"] = {"profile_sha256": calibration.PROFILE_SHA256}
    if target is not None:
        parsed["target_ms"] = target
    return parsed


def full_arms() -> dict[str, dict]:
    arms = {}
    for ordinal, sides in ORDINAL_SIDES.items():
        runs = []
        for index, side in enumerate(sides, start=1):
            sha = EXPECTED["base_sha" if side == "base" else "merge_sha"]
            runs.append({
                "run_number": index,
                "side": side,
                "source_ref": "base" if side == "base" else "pull-merge",
                "source_sha": sha,
                "source_status": "",
                "identity": EXPECTED,
                "host": {
                    "profile_sha256": calibration.PROFILE_SHA256,
                    "host_profile": ("ubuntu-24.04/20260101", "x86_64", "Hosted CPU",
                                     "gcc 14.1", "1.12.0", "6.8.0"),
                },
                "gates": {gate: result_for(gate) for gate in calibration.GATES},
            })
        arms[ordinal] = {"ordinal": ordinal, "identity": EXPECTED, "runs": runs}
    return arms


class HostedPerfWorkflowContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def test_manual_read_only_hosted_contract(self):
        triggers = self.workflow.split("permissions:", 1)[0]
        self.assertIn("workflow_dispatch:", triggers)
        for forbidden in ("pull_request:", "push:", "schedule:"):
            self.assertNotIn(forbidden, triggers)
        self.assertIn("contents: read", self.workflow)
        self.assertIn("pull-requests: read", self.workflow)
        self.assertNotIn("secrets.", self.workflow)
        self.assertNotRegex(self.workflow, r"(?m)^\s+(?:id-token|contents|pull-requests):\s+write$")
        self.assertNotIn("self-hosted", self.workflow)
        self.assertNotRegex(self.workflow, r"(?m)^\s+.*perf\s*\]$")
        for name in ("resolve-identity", "calibration-arm", "aggregate"):
            block = re.search(
                rf"(?m)^  {re.escape(name)}:\n(.*?)(?=^  [\w-]+:|\Z)",
                self.workflow, re.S)
            self.assertIsNotNone(block, name)
            self.assertIn("runs-on: ubuntu-latest", block[1])
            self.assertIn("github.repository == 'semantic-reasoning/wirelog'", block[1])
            self.assertIn("github.ref == 'refs/heads/main'", block[1])

    def test_pr_and_merge_sha_are_frozen_and_rechecked(self):
        self.assertIn("gh api \"repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}\"", self.workflow)
        self.assertIn('refs/pull/${PR_NUMBER}/merge', self.workflow)
        self.assertIn("[[ \"$merge_ref\" == \"$merge_sha\" ]]", self.workflow)
        self.assertIn("ref: ${{ matrix.first == 'base' && needs.resolve-identity.outputs.base_sha || needs.resolve-identity.outputs.merge_sha }}", self.workflow)
        self.assertIn("ref: ${{ matrix.second == 'base' && needs.resolve-identity.outputs.base_sha || needs.resolve-identity.outputs.merge_sha }}", self.workflow)
        self.assertIn("current-identity.json", self.workflow)
        self.assertIn("github.workflow_sha", self.workflow)
        self.assertIn("persist-credentials: false", self.workflow)

    def test_six_ordered_serialized_pairs_and_partial_artifacts(self):
        matrix = self.workflow.split("        include:\n", 1)[1].split("    steps:", 1)[0]
        entries = re.findall(
            r"- ordinal: '([0-9]{2})'\n\s+first: (base|head)\n\s+second: (base|head)",
            matrix)
        self.assertEqual([(ordinal, first, second)
                          for ordinal, first, second in entries],
                         [(key, *sides) for key, sides in ORDINAL_SIDES.items()])
        self.assertIn("fail-fast: false", self.workflow)
        self.assertIn("max-parallel: 1", self.workflow)
        self.assertGreaterEqual(self.workflow.count("name: Upload arm evidence"), 1)
        self.assertIn("if: always()", self.workflow)
        self.assertIn("stdout.log", self.workflow)
        self.assertIn("stderr.log", self.workflow)
        self.assertIn("capture-host", self.workflow)

    def test_profile_and_actual_serial_gate_contract(self):
        self.assertIn("CC: gcc", self.workflow)
        for token in ("--buildtype=release", "-Dwirelog_log_max_level=trace",
                      "-Dtests=true", "-DmbedTLS=disabled",
                      "test_crdt_perf_gate", "test_cspa_perf_gate",
                      "WIRELOG_PERF_GATE=1", "taskset -c 0", "TMPDIR=$HOME/.tmp"):
            self.assertIn(token, self.workflow)
        required = (ROOT / ".github/workflows/perf-suite-required.yml").read_text(
            encoding="utf-8")
        self.assertIn("runs-on: ubuntu-latest", required)
        self.assertNotIn("self-hosted", required)

    def test_ineligible_aggregate_fails_but_upload_still_runs(self):
        aggregate = self.workflow.split("\n  aggregate:\n", 1)[1]
        classify = aggregate.split("- name: Classify complete or partial evidence", 1)[1]
        self.assertIn("set -euo pipefail", classify)
        upload = aggregate.split("- name: Upload aggregate result and logs", 1)[1]
        self.assertIn("if: always()", upload)


class HostedPerfParserTests(unittest.TestCase):
    def test_timed_crdt_pass_and_target_miss_use_summary_correctness(self):
        passed = calibration.parse_gate("crdt", 0, gate_log("crdt"))
        miss = calibration.parse_gate("crdt", 1, gate_log("crdt", miss=True))
        self.assertEqual(passed["status"], "pass")
        self.assertEqual(len(passed["raw_ms"]), 9)
        self.assertEqual(passed["median_ms"], 38060.0)
        self.assertEqual(passed["cov_percent"], [0.1])
        self.assertTrue(passed["correctness_ok"])
        self.assertEqual(miss["status"], "target_miss")
        wrong_gold = gate_log("crdt").replace(
            "result = 104851 (expected 104851)", "result = 1 (expected 1)")
        self.assertFalse(calibration.parse_gate("crdt", 0, wrong_gold)["correctness_ok"])

    def test_timed_cspa_summary_and_correctness_failure(self):
        passed = calibration.parse_gate("cspa", 0, gate_log("cspa"))
        wrong = calibration.parse_gate("cspa", 1, gate_log("cspa", miss=True, wrong=True))
        self.assertEqual(passed["status"], "pass")
        self.assertTrue(passed["correctness_ok"])
        self.assertEqual(wrong["status"], "correctness_failed")
        self.assertFalse(wrong["correctness_ok"])

    def test_skip_or_missing_raw_sample_is_no_measurement(self):
        skipped = calibration.parse_gate(
            "crdt", 0, "test_crdt_perf_gate: SKIP: governor unavailable\n"
            "result: exit status 77\n")
        missing = calibration.parse_gate("cspa", 0, "no raw output")
        self.assertEqual(skipped["status"], "no_measurement")
        self.assertEqual(skipped["exit_code"], 77)
        self.assertEqual(missing["status"], "no_measurement")


class HostedPerfCampaignClassificationTests(unittest.TestCase):
    def run_aggregate_cli(self, root: Path, arms: dict,
                          current: dict = EXPECTED) -> tuple[int, dict]:
        root.mkdir(parents=True, exist_ok=True)
        identity_path = root / "identity.json"
        current_path = root / "current.json"
        artifact_root = root / "downloaded"
        identity_path.write_text(json.dumps(EXPECTED), encoding="utf-8")
        current_path.write_text(json.dumps(current), encoding="utf-8")
        artifact_root.mkdir()
        for ordinal, arm in arms.items():
            folder = artifact_root / f"hosted-calibration-{ordinal}"
            folder.mkdir()
            (folder / f"{ordinal}.json").write_text(
                json.dumps(arm), encoding="utf-8")
        output_path = root / "result.json"
        with redirect_stdout(io.StringIO()):
            exit_code = calibration.main([
                "aggregate", "--identity", str(identity_path),
                "--current-identity", str(current_path),
                "--evidence", str(artifact_root), "--output", str(output_path),
            ])
        return exit_code, json.loads(output_path.read_text(encoding="utf-8"))

    def test_downloaded_artifact_layout_loads_six_two_run_ordinals(self):
        cache = Path.home() / ".cache"
        cache.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="wirelog-hosted-calibration-",
                                         dir=cache) as temporary:
            root = Path(temporary)
            for ordinal, arm in full_arms().items():
                folder = root / f"hosted-calibration-run-{ordinal}"
                folder.mkdir()
                (folder / f"{ordinal}.json").write_text(
                    calibration.json.dumps(arm), encoding="utf-8")
            loaded = calibration.load_arms(root)
            self.assertEqual(set(loaded), set(ORDINAL_SIDES))
            self.assertEqual(len(loaded["01"]["runs"]), 2)
            self.assertEqual(
                calibration.classify_campaign(EXPECTED, EXPECTED, loaded)["classification"],
                "eligible")

    def test_two_run_pairs_and_target_miss_are_eligible(self):
        arms = full_arms()
        arms["01"]["runs"][0]["gates"]["crdt"] = result_for("crdt", status="target_miss")
        result = calibration.classify_campaign(EXPECTED, EXPECTED, arms)
        self.assertEqual(result["classification"], "eligible")
        self.assertEqual(len(result["arms"]["01"]["runs"]), 2)

    def test_missing_ordinal_is_incomplete(self):
        arms = full_arms()
        del arms["06"]
        result = calibration.classify_campaign(EXPECTED, EXPECTED, arms)
        self.assertEqual(result["classification"], "incomplete")
        self.assertFalse(result["complete"])

    def test_missing_ordinal_preserves_stale_identity(self):
        arms = full_arms()
        del arms["06"]
        changed = dict(EXPECTED, head_sha="x" * 40)
        result = calibration.classify_campaign(EXPECTED, changed, arms)
        self.assertEqual(result["classification"], "incomplete")
        self.assertTrue(result["stale"])
        arms = full_arms()
        del arms["06"]
        arms["01"]["runs"][0]["source_sha"] = "x" * 40
        result = calibration.classify_campaign(EXPECTED, EXPECTED, arms)
        self.assertEqual(result["classification"], "incomplete")
        self.assertTrue(result["stale"])

    def test_missing_gate_log_is_incomplete(self):
        arms = full_arms()
        arms["01"]["runs"][0]["gates"]["crdt"]["evidence_complete"] = False
        result = calibration.classify_campaign(EXPECTED, EXPECTED, arms)
        self.assertEqual(result["classification"], "incomplete")

    def test_skip_and_correctness_failure_are_ineligible(self):
        arms = full_arms()
        arms["01"]["runs"][0]["gates"]["cspa"] = result_for("cspa", status="skip")
        result = calibration.classify_campaign(EXPECTED, EXPECTED, arms)
        self.assertEqual(result["classification"], "ineligible")
        self.assertFalse(result["eligible"])
        arms = full_arms()
        arms["02"]["runs"][1]["gates"]["crdt"] = result_for(
            "crdt", status="correctness_failed")
        result = calibration.classify_campaign(EXPECTED, EXPECTED, arms)
        self.assertEqual(result["classification"], "ineligible")

    def test_profile_mismatch_and_stale_identity_are_ineligible(self):
        arms = full_arms()
        arms["03"]["runs"][0]["host"]["profile_sha256"] = "different"
        result = calibration.classify_campaign(EXPECTED, EXPECTED, arms)
        self.assertEqual(result["classification"], "ineligible")
        arms = full_arms()
        arms["04"]["runs"][0]["host"]["host_profile"] = (
            "ubuntu-24.04/20260101", "x86_64", "Different CPU",
            "gcc 14.1", "1.12.0", "6.8.0")
        result = calibration.classify_campaign(EXPECTED, EXPECTED, arms)
        self.assertEqual(result["classification"], "ineligible")
        arms = full_arms()
        changed = dict(EXPECTED, head_sha="x" * 40)
        result = calibration.classify_campaign(EXPECTED, changed, arms)
        self.assertEqual(result["classification"], "ineligible")
        self.assertTrue(result["stale"])

    def test_missing_none_and_malformed_host_profiles_are_safe_and_ineligible(self):
        for malformed in (None, "ubuntu/CPU", ["only", "five", "fields"], 42):
            with self.subTest(profile=malformed):
                arms = full_arms()
                arms["01"]["runs"][0]["host"]["host_profile"] = malformed
                result = calibration.classify_campaign(EXPECTED, EXPECTED, arms)
                self.assertEqual(result["classification"], "ineligible")
        arms = full_arms()
        del arms["01"]["runs"][0]["host"]
        result = calibration.classify_campaign(EXPECTED, EXPECTED, arms)
        self.assertEqual(result["classification"], "ineligible")

    def test_aggregate_command_exit_matches_eligibility(self):
        cache = Path.home() / ".cache"
        cache.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="wirelog-hosted-aggregate-",
                                         dir=cache) as temporary:
            root = Path(temporary)
            arms = full_arms()
            arms["01"]["runs"][0]["gates"]["crdt"] = result_for(
                "crdt", status="target_miss")
            code, result = self.run_aggregate_cli(root / "eligible", arms)
            self.assertEqual(code, 0)
            self.assertEqual(result["classification"], "eligible")

        with tempfile.TemporaryDirectory(prefix="wirelog-hosted-aggregate-",
                                         dir=cache) as temporary:
            root = Path(temporary)
            arms = full_arms()
            arms["01"]["runs"][0]["gates"]["cspa"] = result_for(
                "cspa", status="skip")
            code, result = self.run_aggregate_cli(root / "ineligible", arms)
            self.assertEqual(code, 1)
            self.assertEqual(result["classification"], "ineligible")

        with tempfile.TemporaryDirectory(prefix="wirelog-hosted-aggregate-",
                                         dir=cache) as temporary:
            root = Path(temporary)
            arms = full_arms()
            del arms["06"]
            code, result = self.run_aggregate_cli(root / "incomplete", arms)
            self.assertEqual(code, 1)
            self.assertEqual(result["classification"], "incomplete")


if __name__ == "__main__":
    unittest.main()
