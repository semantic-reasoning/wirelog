#!/usr/bin/env python3
"""Static contract checks for the hosted corpus-continuous fuzz workflow."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "fuzz-evidence-hosted.yml"
VERIFIER = ROOT / "scripts" / "fuzz" / "verify-campaign.py"


class HostedFuzzWorkflowContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text = WORKFLOW.read_text(encoding="utf-8")

    def test_exactly_24_sequential_stages(self):
        stages = [
            int(value)
            for value in re.findall(r"^  shard-([0-9]{2}):$", self.text, re.MULTILINE)
        ]
        self.assertEqual(stages, list(range(1, 25)))
        for value in range(2, 25):
            previous = f"shard-{value - 1:02d}"
            current = f"shard-{value:02d}"
            block = self.text.split(f"  {current}:\n", 1)[1].split(
                f"  shard-{value + 1:02d}:\n" if value < 24 else "  verify:\n", 1
            )[0]
            self.assertIn(f"needs: [validate, {previous}]", block)
            expected = (
                "name: fuzz-campaign-${{ needs.validate.outputs.campaign_id }}-run-${{ github.run_id }}-shard-"
                + f"{value:02d}"
                + "-attempt-${{ github.run_attempt }}"
            )
            self.assertIn(expected, block)

    def test_each_stage_is_hosted_and_below_six_hour_limit(self):
        self.assertNotRegex(self.text, r"runs-on:\s*\$\{{")
        self.assertEqual(self.text.count("runs-on: ubuntu-latest"), 26)
        self.assertEqual(self.text.count("timeout-minutes: 300"), 24)
        self.assertEqual(self.text.count("actions/upload-artifact@v7"), 25)
        self.assertIn("permissions:\n  contents: read", self.text)

    def test_artifact_identity_and_fail_closed_verification(self):
        self.assertIn("github.run_attempt", self.text)
        self.assertIn("github.run_id", self.text)
        self.assertIn("needs: [validate, shard-24]", self.text)
        self.assertIn("if: ${{ always() }}", self.text)
        self.assertIn("--input-dir evidence/shard-24/shard-output", self.text)
        self.assertIn("--out-dir fuzz-evidence/minimized", self.text)
        self.assertIn("overall_exit_status", VERIFIER.read_text(encoding="utf-8"))
        self.assertIn("duplicate attempt", VERIFIER.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
