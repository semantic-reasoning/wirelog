#!/usr/bin/env python3
"""Self-test for check-shell-gate-timeouts.py (#1464).

Drives the gate over synthetic introspection files in a temporary build
directory, so every verdict (offender, pass, vacuity floor, skip, required
escalation) is exercised without a real Meson build.  When a real build
directory is present, one case also checks that the gate classifies exactly
the tests check-bash-constructs.py's seed scan would, so the shared helper
cannot drift from its only other consumer.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
GATE = SCRIPT_DIR / "check-shell-gate-timeouts.py"
BASH_CONSTRUCTS = SCRIPT_DIR / "check-bash-constructs.py"


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None, path
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses resolve their module here
    spec.loader.exec_module(module)
    return module


def shell_test(name: str, timeout: int, suite: str = "wirelog:abi",
               shape: str = "find_program") -> dict:
    script = f"/src/scripts/ci/{name}.sh"
    cmd = [script] if shape == "find_program" else ["/usr/bin/bash", script]
    return {"name": name, "suite": [suite], "timeout": timeout, "cmd": cmd}


def py_test(name: str, timeout: int) -> dict:
    return {"name": name, "suite": ["wirelog:abi"], "timeout": timeout,
            "cmd": ["/usr/bin/python3", f"/src/scripts/ci/{name}.py"]}


class GateCase(unittest.TestCase):
    def setUp(self) -> None:
        self.gate = load(GATE, "_shell_gate_timeouts")
        # The synthetic-introspection cases assert verdicts, not the platform
        # skip, so pin a supported platform for the module under test; the
        # skip itself is exercised by test_unsupported_platform_skips.
        self.saved_platform = self.gate.sys.platform
        self.gate.sys.platform = "linux"
        self.tmp = tempfile.TemporaryDirectory()
        self.build = Path(self.tmp.name) / "build"
        (self.build / "meson-info").mkdir(parents=True)
        self.environ = dict(os.environ)
        os.environ.pop("WIRELOG_ABI_REQUIRED", None)

    def tearDown(self) -> None:
        self.gate.sys.platform = self.saved_platform
        os.environ.clear()
        os.environ.update(self.environ)
        self.tmp.cleanup()

    def write(self, tests: list[dict]) -> None:
        (self.build / "meson-info" / "intro-tests.json").write_text(
            json.dumps(tests), encoding="utf-8")

    def run_gate(self, build: Path | None = None) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = self.gate.main(["gate", str(build or self.build)])
        return rc, out.getvalue(), err.getvalue()

    def test_default_timeout_fails_by_name(self) -> None:
        self.write([shell_test("check-good", 120), shell_test("check-bad", 30)])
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("check-bad (suite wirelog:abi): timeout 30", err)
        self.assertNotIn("check-good", err)

    def test_both_registration_shapes_are_classified(self) -> None:
        self.write([shell_test("check-fp", 30, shape="find_program"),
                    shell_test("check-args", 30, shape="args")])
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("check-fp", err)
        self.assertIn("check-args", err)

    def test_non_shell_tests_are_ignored(self) -> None:
        self.write([py_test("gate-py", 30), shell_test("check-ok", 120)])
        rc, out, _ = self.run_gate()
        self.assertEqual(rc, 0)
        self.assertIn("ok 1 shell-seeded tests", out)

    def test_explicit_non_default_values_pass(self) -> None:
        self.write([shell_test("check-a", 90), shell_test("check-b", 180),
                    shell_test("check-c", 3600, suite="wirelog:perf")])
        rc, out, _ = self.run_gate()
        self.assertEqual(rc, 0)
        self.assertIn("ok 3 shell-seeded tests", out)

    def test_rule_is_suite_agnostic(self) -> None:
        self.write([shell_test("check-sbom", 30, suite="wirelog:sbom")])
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("check-sbom (suite wirelog:sbom)", err)

    def test_non_positive_timeout_fails(self) -> None:
        self.write([shell_test("check-inf", 0), shell_test("check-neg", -1)])
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("check-inf (suite wirelog:abi): timeout 0 disables", err)
        self.assertIn("check-neg (suite wirelog:abi): timeout -1 disables", err)

    def test_vacuity_floor(self) -> None:
        self.write([py_test("only-python", 120)])
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("no shell-seeded tests", err)

    def test_missing_intro_skips(self) -> None:
        rc, out, _ = self.run_gate()
        self.assertEqual(rc, 77)
        self.assertIn("SKIP", out)

    def test_missing_build_dir_skips(self) -> None:
        rc, out, _ = self.run_gate(Path(self.tmp.name) / "absent")
        self.assertEqual(rc, 77)
        self.assertIn("build directory missing", out)

    def test_required_turns_skip_into_failure(self) -> None:
        os.environ["WIRELOG_ABI_REQUIRED"] = "1"
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("gate required but would skip", err)

    def test_unsupported_platform_skips(self) -> None:
        self.write([shell_test("check-ok", 120)])
        saved = self.gate.sys.platform
        self.gate.sys.platform = "win32"
        try:
            rc, out, _ = self.run_gate()
        finally:
            self.gate.sys.platform = saved
        self.assertEqual(rc, 77)
        self.assertIn("Linux and macOS only", out)

    def test_usage(self) -> None:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = self.gate.main(["gate"])
        self.assertEqual(rc, 1)
        self.assertIn("usage", err.getvalue())

    def test_classifier_matches_bash_constructs_seed_scan(self) -> None:
        """The gate's classification, resolved to script paths, must equal
        the seed set check-bash-constructs.py derives from the same
        introspection, so the two gates cannot drift apart."""
        build = Path(os.environ.get("WIRELOG_SHELL_GATE_BUILD", "build"))
        intro = build / "meson-info" / "intro-tests.json"
        if not intro.is_file():
            self.skipTest("no real build introspection available")
        root = SCRIPT_DIR.parents[1]
        constructs = load(BASH_CONSTRUCTS, "_bash_constructs_selftest")
        expected = constructs.seeds_from_intro(intro, root)
        resolved = set()
        for _, script in self.gate.shell_seeds(intro):
            path = Path(script)
            if not path.is_absolute():
                path = root / path
            resolved.add(path.resolve(strict=False))
        self.assertEqual(sorted(resolved), expected)
        self.assertGreater(len(expected), 0)


if __name__ == "__main__":
    unittest.main()
