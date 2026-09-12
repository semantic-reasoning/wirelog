#!/usr/bin/env python3
"""Self-test for check-shell-gate-walltime.py (#1487).

Drives the gate over a synthetic introspection file and a synthetic Meson
testlog (JSON Lines) in a temporary build directory, so every verdict --
pass, over-budget fail, missing-log skip, required escalation, vacuity floor,
and fraction handling -- is exercised without a real ``meson test`` run.

The join key is the test ``command``, so one case pins the gate to the
command-join and not the ``name`` join: a testlog whose ``name`` disagrees
with the introspection name (exactly what Meson 1.12 writes: a *pretty* name)
must still be measured, and a testlog that only matches by name must not be.
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
GATE = SCRIPT_DIR / "check-shell-gate-walltime.py"


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


def testlog_entry(cmd: list[str], duration: float,
                  name: str | None = None) -> dict:
    """A Meson testlog line. ``name`` is the *pretty* name unless given."""
    return {"name": name if name is not None else "pretty - " + cmd[-1].split("/")[-1],
            "result": "OK", "is_fail": False, "duration": duration,
            "returncode": 0, "command": cmd}


class GateCase(unittest.TestCase):
    def setUp(self) -> None:
        self.gate = load(GATE, "_shell_gate_walltime")
        self.tmp = tempfile.TemporaryDirectory()
        self.build = Path(self.tmp.name) / "build"
        (self.build / "meson-info").mkdir(parents=True)
        (self.build / "meson-logs").mkdir(parents=True)
        self.environ = dict(os.environ)
        os.environ.pop("WIRELOG_WALLTIME_REQUIRED", None)

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self.environ)
        self.tmp.cleanup()

    def write_intro(self, tests: list[dict]) -> None:
        (self.build / "meson-info" / "intro-tests.json").write_text(
            json.dumps(tests), encoding="utf-8")

    def write_testlog(self, entries: list[dict]) -> None:
        lines = "".join(json.dumps(e) + "\n" for e in entries)
        (self.build / "meson-logs" / "testlog.json").write_text(
            lines, encoding="utf-8")

    def run_gate(self, build: Path | None = None,
                 fraction: float | None = None) -> tuple[int, str, str]:
        out, err = io.StringIO(), io.StringIO()
        argv = ["gate", str(build or self.build)]
        if fraction is not None:
            argv += ["--fraction", str(fraction)]
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = self.gate.main(argv)
        return rc, out.getvalue(), err.getvalue()

    # -- pass ---------------------------------------------------------------
    def test_within_budget_passes(self) -> None:
        self.write_intro([shell_test("check-fast", 120)])
        self.write_testlog([testlog_entry(shell_test("check-fast", 120)["cmd"], 3.0)])
        rc, out, _ = self.run_gate()
        self.assertEqual(rc, 0)
        self.assertIn("ok 1 executed shell-seeded tests", out)

    def test_non_shell_tests_are_ignored(self) -> None:
        self.write_intro([py_test("gate-py", 120), shell_test("check-ok", 120)])
        self.write_testlog([
            testlog_entry(shell_test("check-ok", 120)["cmd"], 60.0),
            testlog_entry(py_test("gate-py", 120)["cmd"], 999.0),
        ])
        rc, out, _ = self.run_gate()
        self.assertEqual(rc, 0)
        self.assertIn("ok 1 executed shell-seeded tests", out)

    # -- fail ---------------------------------------------------------------
    def test_over_budget_fails_by_name_with_both_numbers(self) -> None:
        self.write_intro([shell_test("check-slow", 120), shell_test("check-fast", 120)])
        self.write_testlog([
            testlog_entry(shell_test("check-slow", 120)["cmd"], 80.0),
            testlog_entry(shell_test("check-fast", 120)["cmd"], 2.0),
        ])
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("check-slow (suite wirelog:abi): duration 80.00s", err)
        self.assertIn("66.7% of its 120s timeout", err)
        self.assertNotIn("check-fast", err)

    def test_worst_run_is_judged(self) -> None:
        self.write_intro([shell_test("check-spike", 120)])
        cmd = shell_test("check-spike", 120)["cmd"]
        self.write_testlog([
            testlog_entry(cmd, 2.0),
            testlog_entry(cmd, 90.0),
        ])
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("check-spike", err)
        self.assertIn("90.00s", err)

    def test_non_positive_timeout_fails(self) -> None:
        self.write_intro([shell_test("check-neg", 0)])
        self.write_testlog([testlog_entry(shell_test("check-neg", 0)["cmd"], 1.0)])
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("check-neg", err)
        self.assertIn("disables the walltime budget", err)

    # -- join key -----------------------------------------------------------
    def test_command_join_not_name(self) -> None:
        """The testlog's pretty name does not equal the introspection name,
        yet the duration must still be attributed (Meson 1.12 behaviour)."""
        self.write_intro([shell_test("check-pretty", 120)])
        self.write_testlog([
            testlog_entry(shell_test("check-pretty", 120)["cmd"], 90.0,
                          name="abi - wirelog:check-pretty")])
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("check-pretty", err)

    def test_name_only_match_is_not_measured(self) -> None:
        """A testlog line whose command is a different seed but whose name
        happens to equal the registered name must NOT be attributed: a
        name-join would silently measure the wrong test."""
        self.write_intro([shell_test("check-real", 120)])
        other_cmd = ["/usr/bin/bash", "/src/scripts/ci/check-other.sh"]
        self.write_testlog([testlog_entry(other_cmd, 90.0, name="check-real")])
        rc, out, _ = self.run_gate()
        # No seed command was measured -> vacuity skip, not a false pass.
        self.assertEqual(rc, 77)
        self.assertIn("registered but none ran", out)

    # -- skip routes --------------------------------------------------------
    def test_missing_testlog_skips(self) -> None:
        self.write_intro([shell_test("check-any", 120)])
        rc, out, _ = self.run_gate()
        self.assertEqual(rc, 77)
        self.assertIn("SKIP", out)

    def test_missing_build_dir_skips(self) -> None:
        rc, out, _ = self.run_gate(build=Path(self.tmp.name) / "absent")
        self.assertEqual(rc, 77)
        self.assertIn("build directory missing", out)

    def test_no_shell_seeds_skips(self) -> None:
        self.write_intro([py_test("only-python", 120)])
        self.write_testlog([testlog_entry(py_test("only-python", 120)["cmd"], 1.0)])
        rc, out, _ = self.run_gate()
        self.assertEqual(rc, 77)
        self.assertIn("no shell-seeded tests", out)

    def test_seeds_registered_but_none_ran_skips(self) -> None:
        self.write_intro([shell_test("check-idle", 120)])
        self.write_testlog([])
        rc, out, _ = self.run_gate()
        self.assertEqual(rc, 77)
        self.assertIn("registered but none ran", out)

    def test_corrupt_testlog_fails(self) -> None:
        self.write_intro([shell_test("check-any", 120)])
        (self.build / "meson-logs" / "testlog.json").write_text(
            "{not valid json\n", encoding="utf-8")
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("cannot evaluate", err)

    # -- required escalation -----------------------------------------------
    def test_required_turns_skip_into_failure(self) -> None:
        self.write_intro([shell_test("check-any", 120)])
        os.environ["WIRELOG_WALLTIME_REQUIRED"] = "1"
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("gate required but would skip", err)

    # -- fraction handling --------------------------------------------------
    def test_fraction_override(self) -> None:
        self.write_intro([shell_test("check-edge", 120)])
        self.write_testlog([testlog_entry(shell_test("check-edge", 120)["cmd"], 70.0)])
        # 70/120 = 58.3%: over the 50% default, within a 75% budget.
        rc_fail, _, _ = self.run_gate()
        self.assertEqual(rc_fail, 1)
        rc_pass, out, _ = self.run_gate(fraction=0.75)
        self.assertEqual(rc_pass, 0)
        self.assertIn("75%", out)

    def test_invalid_fraction_fails(self) -> None:
        self.write_intro([shell_test("check-any", 120)])
        self.write_testlog([testlog_entry(shell_test("check-any", 120)["cmd"], 1.0)])
        for bad in (0.0, 1.5, -1.0):
            rc, _, err = self.run_gate(fraction=bad)
            self.assertEqual(rc, 1)
            self.assertIn("--fraction must be in (0, 1]", err)

    def test_fraction_one_is_allowed(self) -> None:
        self.write_intro([shell_test("check-at", 120)])
        self.write_testlog([testlog_entry(shell_test("check-at", 120)["cmd"], 119.0)])
        rc, out, _ = self.run_gate(fraction=1.0)
        self.assertEqual(rc, 0)
        self.assertIn("ok 1 executed", out)

    def test_usage(self) -> None:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                self.gate.main(["gate"])
                rc = 0
            except SystemExit as exc:  # argparse exits on a missing positional
                rc = exc.code if isinstance(exc.code, int) else 2
        self.assertEqual(rc, 2)
        self.assertIn("usage", err.getvalue())


if __name__ == "__main__":
    unittest.main()
