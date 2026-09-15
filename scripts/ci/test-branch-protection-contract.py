#!/usr/bin/env python3
"""Self-test the repository's branch-protection preparation contract.

This is deliberately a targeted static scan.  The repository does not need a
YAML dependency just to protect a small set of names and relationships, and a
full YAML parser would make this gate harder to run on every supported runner.
"""

from __future__ import annotations

import re
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CI_PR = Path(".github/workflows/ci-pr.yml")
CLA_WORKFLOW = Path(".github/workflows/cla-required.yml")
LINT_PR = Path(".github/workflows/lint-pr.yml")
PERF_WORKFLOW = Path(".github/workflows/perf-suite-required.yml")
CODEOWNERS = Path(".github/CODEOWNERS")
RELEASE_DOC = Path("docs/RELEASE_PROCESS.md")


class ContractError(AssertionError):
    """A repository contract assertion failed."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ContractError(message)


def read(root: Path, relative: Path) -> str:
    path = root / relative
    require(path.is_file(), f"missing contract input: {relative}")
    return path.read_text(encoding="utf-8")


def strip_yaml_comments(text: str) -> str:
    """Remove YAML comments while preserving live, line-oriented content."""
    live_lines = []
    for line in text.splitlines():
        quote = None
        escaped = False
        comment_at = None
        for index, character in enumerate(line):
            if escaped:
                escaped = False
                continue
            if character == "\\" and quote == '"':
                escaped = True
                continue
            if character in "'\"":
                if quote is None:
                    quote = character
                elif quote == character:
                    quote = None
                continue
            if character == "#" and quote is None and (
                index == 0 or line[index - 1].isspace()
            ):
                comment_at = index
                break
        if comment_at is not None:
            line = line[:comment_at].rstrip()
        if line.strip():
            live_lines.append(line)
    return "\n".join(live_lines)


def workflow_block(text: str, heading: str) -> str:
    """Return a top-level workflow block without attempting YAML parsing."""
    text = strip_yaml_comments(text)
    match = re.search(
        rf"(?ms)^  {re.escape(heading)}:\s*$"
        rf"(.*?)(?=^  [A-Za-z0-9_-]+:\s*$|\Z)",
        text,
    )
    require(match is not None, f"workflow is missing {heading!r}")
    return match.group(0)


def pull_request_block(text: str) -> str:
    text = strip_yaml_comments(text)
    match = re.search(
        r"(?ms)^  pull_request:\s*(.*?)(?=^  [A-Za-z0-9_-]+:\s*$|\Z)",
        text,
    )
    require(match is not None, "workflow is missing a pull_request trigger")
    return match.group(0)


def yaml_field(block: str, field: str) -> list[str]:
    """Return live, four-space-indented fields from a workflow job block."""
    return re.findall(rf"(?m)^    {re.escape(field)}:\s*(.*?)\s*$", block)


def branch_names(text: str) -> set[str]:
    trigger = pull_request_block(text)
    match = re.search(r"(?m)^    branches:\s*\[([^]]+)\]\s*$", trigger)
    require(match is not None, "pull_request trigger must declare branches")
    return {part.strip().strip("'\"") for part in match.group(1).split(",")}


def assert_branches(text: str, workflow_name: str) -> None:
    branches = branch_names(text)
    require(
        {"main", "1.0"}.issubset(branches),
        f"{workflow_name}: pull_request branches must include main and 1.0",
    )


def check_codeowners(root: Path) -> None:
    text = read(root, CODEOWNERS)
    catch_all = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        fields = stripped.split()
        if fields and fields[0] == "*":
            catch_all.append(fields)
    require(
        any(len(fields) > 1 for fields in catch_all),
        ".github/CODEOWNERS must contain a repo-wide '*' catch-all owner",
    )


def check_branches(root: Path) -> None:
    assert_branches(read(root, CI_PR), "ci-pr.yml")
    assert_branches(read(root, CLA_WORKFLOW), "cla-required.yml")


def check_cla_workflow(root: Path) -> None:
    text = read(root, CLA_WORKFLOW)
    job = workflow_block(text, "cla-signoff-gate")
    require(re.search(r"(?m)^    name:\s*CLA signoff gate\s*$", job) is not None,
            "CLA gate display name changed")
    require(re.search(r"(?m)^      CLA_CONTEXT:\s*license/cla\s*$", job) is not None,
            "CLA_CONTEXT must be license/cla")
    require("statuses[]" in job, "CLA gate must inspect hosted commit statuses")
    require(re.search(
        r"(?m)^\s+status_url=\"https://api\.github\.com/repos/"
        r"\$\{REPOSITORY\}/commits/\$\{HEAD_SHA\}/status\"\s*$", job
    ) is not None,
            "CLA gate must query the PR head SHA status endpoint")
    require("--fail" in job and "--max-time" in job,
            "CLA status requests must fail closed on transport errors")
    require(re.search(r"(?m)^\s+set -euo pipefail\s*$", job) is not None,
            "CLA gate must use strict shell failure semantics")
    require(re.search(
        r"(?m)^\s+if \[ \"\$\{PR_AUTHOR_ASSOCIATION\}\" = \"OWNER\" \]",
        job,
    ) is not None,
            "CLA owner bypass must be explicit and narrowly scoped")

    require(re.search(r"(?m)^\s+success\)\s*$", job) is not None
            and re.search(r"success\).*?exit 0", job, re.S),
            "CLA success must be the only successful hosted-status path")
    require(re.search(r"(?m)^\s+pending\)\s*$", job) is not None,
            "CLA gate must recognize pending hosted status")
    require(re.search(r"(?m)^\s+missing\)\s*$", job) is not None,
            "CLA gate must recognize missing hosted status")
    require(re.search(r"(?m)^\s+failure\|error\)\s*$", job) is not None
            and re.search(r"failure\|error\).*?exit 1", job, re.S),
            "CLA failure and error statuses must fail")
    require("unexpected state" in job and re.search(r"\*\).*?exit 1", job, re.S),
            "CLA unexpected statuses must fail closed")
    for state in ("pending", "missing"):
        state_block = re.search(rf"{state}\)(.*?);;", job, re.S)
        require(state_block is not None and "exit 0" not in state_block.group(1),
                f"CLA {state} status must not pass")
    require("Timed out after" in job and re.search(r"latest state.*?\n\s*exit 1", job, re.S),
            "CLA timeout for missing or pending status must fail closed")


CONCRETE_BUILD_JOBS = (
    "build-primary",
    "build-arm",
    "build-matrix",
    "build-mbedtls",
    "sanitizer-matrix",
    "tsan",
    "tsan-native",
)


def check_ci_pr(root: Path) -> None:
    text = read(root, CI_PR)
    build_scope = workflow_block(text, "build-scope")
    require(re.search(
        r"(?m)^      build-required:\s*\$\{\{ steps\.scope\.outputs\.build-required \}\}\s*$",
        build_scope,
    ) is not None,
            "build-scope must publish its build-required output")
    require(re.search(
        r'(?m)^\s+echo "build-required=true" >> "\$GITHUB_OUTPUT"\s*$',
        build_scope,
    ) is not None,
            "build-scope must emit build-required=true")

    jobs_text = text.split("\njobs:", 1)[1] if "\njobs:" in text else ""
    job_names = re.findall(r"(?m)^  ([A-Za-z0-9_-]+):\s*$", jobs_text)
    require("build-scope" in job_names, "ci-pr must define the shared build-scope job")
    for job_name in job_names:
        if job_name == "build-scope":
            continue
        job = workflow_block(text, job_name)
        require(not re.search(r"(?m)^\s+paths(?:-ignore)?:", job),
                f"{job_name} must not have a job-level path filter")

    for job_name in CONCRETE_BUILD_JOBS:
        job = workflow_block(text, job_name)
        if_lines = yaml_field(job, "if")
        require(len(if_lines) == 1 and
                "needs.build-scope.outputs.build-required == 'true'" in if_lines[0],
                f"{job_name} must share the build-scope gate")


def check_lint_contexts(root: Path) -> None:
    caller = workflow_block(read(root, CI_PR), "lint")
    reusable = read(root, LINT_PR)
    require("uses: ./.github/workflows/lint-pr.yml" in caller,
            "ci-pr lint caller must use lint-pr.yml")

    caller_name_match = re.search(r"(?m)^    name:\s*(.+?)\s*$", caller)
    caller_name = caller_name_match.group(1) if caller_name_match else "lint"
    editor = workflow_block(reusable, "editorconfig-check")
    uncrustify = workflow_block(reusable, "uncrustify-check")
    editor_name = re.search(r"(?m)^    name:\s*(.+?)\s*$", editor)
    uncrustify_name = re.search(r"(?m)^    name:\s*(.+?)\s*$", uncrustify)
    require(editor_name is not None and uncrustify_name is not None,
            "lint reusable jobs must have stable display names")
    contexts = {
        f"{caller_name} / {editor_name.group(1)}",
        f"{caller_name} / {uncrustify_name.group(1)}",
    }
    require(
        contexts == {"lint / EditorConfig check", "lint / uncrustify check"},
        "lint caller/reusable names must compose into both required contexts",
    )


def check_perf_workflow(root: Path) -> None:
    text = strip_yaml_comments(read(root, PERF_WORKFLOW))
    trigger = pull_request_block(text)
    require(re.search(r"(?m)^    paths:\s*$", trigger) is not None,
            "perf-suite-required must remain pull-request path-filtered")
    require(re.search(r"(?m)^\s+- ['\"]?wirelog/columnar/ops\.c['\"]?\s*$", trigger),
            "perf-suite-required path filter lost its compact-runs surface")
    require(not re.search(r"(?m)^\s*WIRELOG_PERF_REQUIRE(?:\s*:|=)", text),
            "path-filtered perf-suite-required must not set WIRELOG_PERF_REQUIRE")
    require(re.search(r"(?m)^\s*WIRELOG_PERF_GATE:\s*['\"]?1['\"]?\s*$", text),
            "perf-suite-required must invoke the perf gate explicitly")


def check_release_docs(root: Path) -> None:
    text = read(root, RELEASE_DOC)
    normalized = re.sub(r"\s+", " ", text)
    required = (
        "#### Phase A — preparatory (repo-side readiness only)",
        "#### Phase B — final RC1 cutover (last moment)",
        "Do **not** configure GitHub branch protection yet",
        "final #746 acceptance remains gated on Phase B",
        "do not rely on path-filtered contexts for branch protection",
        "dedicated always-emitting release perf context",
        "WIRELOG_PERF_REQUIRE=1",
        "not fully present in this repository state yet",
        "Final #746 acceptance is deferred",
    )
    for needle in required:
        require(needle in normalized,
                f"release docs missing branch/perf contract text: {needle!r}")

    live_claims = re.compile(
        r"(?i)\b(?:branch protection|protection rules)\s+"
        r"(?:is|are|has been|have been)\s+"
        r"(?:currently|now|already|live\s+and)?\s*"
        r"(?:active|enabled|live|in place)\b"
    )
    require(not live_claims.search(text),
            "release docs must not claim live branch protection")


def check_contract(root: Path) -> None:
    """Check the repository contract and raise ContractError on drift."""
    check_codeowners(root)
    check_branches(root)
    check_cla_workflow(root)
    check_ci_pr(root)
    check_lint_contexts(root)
    check_perf_workflow(root)
    check_release_docs(root)


CONTRACT_FILES = (
    CODEOWNERS,
    CI_PR,
    CLA_WORKFLOW,
    LINT_PR,
    PERF_WORKFLOW,
    RELEASE_DOC,
)


class BranchProtectionContractTests(unittest.TestCase):
    """Mutation tests ensure each important assertion is actually live."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="branch-contract-")
        self.root = Path(self.temp.name)
        for relative in CONTRACT_FILES:
            destination = self.root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / relative, destination)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def assert_mutation_fails(self, relative: Path, mutate, message: str) -> None:
        path = self.root / relative
        path.write_text(mutate(path.read_text(encoding="utf-8")), encoding="utf-8")
        with self.assertRaisesRegex(ContractError, message):
            check_contract(self.root)

    def test_current_contract(self) -> None:
        check_contract(ROOT)

    def test_missing_1_0_branch_is_rejected(self) -> None:
        self.assert_mutation_fails(
            CI_PR,
            lambda text: text.replace('branches: [main, "1.0"]',
                                      "branches: [main, legacy]", 1),
            "branches must include main and 1\\.0",
        )

    def test_renamed_cla_gate_is_rejected(self) -> None:
        self.assert_mutation_fails(
            CLA_WORKFLOW,
            lambda text: text.replace("cla-signoff-gate", "cla-signoff-renamed", 1),
            "missing.*cla-signoff-gate",
        )

    def test_false_build_required_is_rejected(self) -> None:
        self.assert_mutation_fails(
            CI_PR,
            lambda text: text.replace('echo "build-required=true"',
                                      'echo "build-required=false"'),
            "emit build-required=true",
        )

    def test_always_if_with_stale_comment_is_rejected(self) -> None:
        def mutate(text: str) -> str:
            old_if = "    if: ${{ needs.build-scope.outputs.build-required == 'true' }}"
            start = text.index("  build-primary:")
            end = text.index("  build-arm:", start)
            job = text[start:end]
            self.assertIn(old_if, job)
            job = job.replace(
                old_if,
                "    if: ${{ always() }}\n"
                "    # if: ${{ needs.build-scope.outputs.build-required == 'true' }}",
                1,
            )
            return text[:start] + job + text[end:]

        self.assert_mutation_fails(
            CI_PR,
            mutate,
            "build-primary must share the build-scope gate",
        )

    def test_missing_lint_job_is_rejected(self) -> None:
        self.assert_mutation_fails(
            CI_PR,
            lambda text: text.replace("  lint:\n", "  lint-renamed:\n", 1),
            "missing.*'lint'",
        )

    def test_stale_docs_perf_contract_is_rejected(self) -> None:
        self.assert_mutation_fails(
            RELEASE_DOC,
            lambda text: text.replace("path-filtered contexts", "path-aware contexts", 1),
            "path-filtered contexts",
        )


def main() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(
        BranchProtectionContractTests
    )
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
