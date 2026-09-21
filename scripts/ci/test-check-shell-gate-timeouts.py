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


def real_py_test(name: str, timeout: int) -> dict:
    return {"name": name, "suite": ["wirelog:abi"], "timeout": timeout,
            "cmd": [sys.executable, str(SCRIPT_DIR / name)]}


def powershell_test(name: str, timeout: int, script: str | None = None) -> dict:
    """Mirror the shape tests/meson.build actually registers (#1489).

    Two tokens sit between the interpreter and ``-File``, and a build-root
    argument follows the script, so the gate's ``-File`` scan is exercised at
    an interior index rather than at ``cmd[1]``.  argv[0] stays a bare
    ``pwsh.exe``: the gate splits it with ``Path(...).name``, which does not
    split backslashes under POSIX, so a Windows-style argv[0] would classify
    on the Windows leg only and go red on Linux and macOS.
    """
    return {"name": name, "suite": ["wirelog:abi"], "timeout": timeout,
            "cmd": ["pwsh.exe", "-NoProfile", "-ExecutionPolicy", "Bypass",
                    "-File", script or f"/src/scripts/ci/{name}.ps1",
                    "/build"]}


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
        self.write([real_py_test("test-check-shell-gate-timeouts.py", 30), shell_test("check-ok", 120)])
        rc, out, _ = self.run_gate()
        self.assertEqual(rc, 0)
        self.assertIn("ok 1 process-spawning tests (shell, PowerShell, Python)", out)

    def test_python_process_gate_default_fails(self) -> None:
        self.write([real_py_test("check-bash-constructs.py", 30)])
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("check-bash-constructs.py", err)

    def test_powershell_default_fails(self) -> None:
        self.write([powershell_test("check-abi-symbols-windows", 30)])
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("check-abi-symbols-windows", err)

    def test_explicit_non_default_values_pass(self) -> None:
        self.write([shell_test("check-a", 90), shell_test("check-b", 180),
                    shell_test("check-c", 3600, suite="wirelog:perf")])
        rc, out, _ = self.run_gate()
        self.assertEqual(rc, 0)
        self.assertIn("ok 3 process-spawning tests (shell, PowerShell, Python)", out)

    def test_darwin_platform_passes_shell_gate(self) -> None:
        self.write([shell_test("check-macos", 120)])
        saved = self.gate.sys.platform
        self.gate.sys.platform = "darwin"
        try:
            rc, out, _ = self.run_gate()
        finally:
            self.gate.sys.platform = saved
        self.assertEqual(rc, 0)
        self.assertIn("ok 1 process-spawning tests (shell, PowerShell, Python)", out)

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
        self.assertIn("no process-spawning tests (shell, PowerShell, Python)", err)

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

    def test_windows_platform_runs(self) -> None:
        self.write([powershell_test("check-ok", 120)])
        saved = self.gate.sys.platform
        self.gate.sys.platform = "win32"
        try:
            rc, out, _ = self.run_gate()
        finally:
            self.gate.sys.platform = saved
        self.assertEqual(rc, 0)
        self.assertIn("ok 1 process-spawning tests (shell, PowerShell, Python)", out)

    def test_unsupported_platform_skips(self) -> None:
        self.write([shell_test("check-ok", 120)])
        saved = self.gate.sys.platform
        self.gate.sys.platform = "freebsd"
        try:
            rc, out, _ = self.run_gate()
        finally:
            self.gate.sys.platform = saved
        self.assertEqual(rc, 77)
        self.assertIn("Linux, macOS, and Windows only", out)

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

    def test_powershell_seed_stays_out_of_bash_closure(self) -> None:
        """A .ps1 registration is this gate's alone, never the ratchet's.

        #1489 requires that widening this gate to PowerShell does not widen
        check-bash-constructs.py, which would lex PowerShell as Bash.  The
        real-build case above cannot observe that: a Linux introspection
        registers no .ps1 at all, so the constraint would hold there whether
        or not it were true.  Drive a synthetic root holding every shape and
        pin the split at the shared classifier and at its caller.
        """
        # resolve(): check-bash-constructs.py confines seeds with
        # Path.relative_to against the root as passed, and macOS tempdirs
        # resolve /var -> /private/var.  An unresolved root would raise
        # "introspection seed escapes source root" on macOS only.
        root = Path(self.tmp.name).resolve() / "src"
        (root / "scripts" / "ci").mkdir(parents=True)
        sh_script = root / "scripts" / "ci" / "seed.sh"
        ps_script = root / "scripts" / "ci" / "gate.ps1"
        sh_script.write_text("#!/bin/sh\n", encoding="utf-8")
        ps_script.write_text("# PowerShell\n", encoding="utf-8")

        ps_entry = powershell_test("ps-gate", 30, script=str(ps_script))
        # Both shapes the gate recognises: behind ``-File``, and as argv[0]
        # the way find_program() registers a script.  Pinning only the first
        # would leave the ratchet's argv[0] ``endswith`` tuple free to grow a
        # ``.ps1`` with no test noticing.
        bare_entry = {"name": "ps-bare", "suite": ["wirelog:abi"],
                      "timeout": 30, "cmd": [str(ps_script), "/build"]}
        sh_entry = {"name": "sh-gate", "suite": ["wirelog:abi"],
                    "timeout": 30, "cmd": ["/usr/bin/bash", str(sh_script)]}
        self.write([sh_entry, ps_entry, bare_entry])

        # The timeout gate owns all three: each is an offender, by name.
        rc, _, err = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertIn("sh-gate", err)
        self.assertIn("ps-gate", err)
        self.assertIn("ps-bare", err)

        # The ratchet owns only the .sh.  Exact equality, not membership, so a
        # .ps1 entering its closure fails here instead of passing unnoticed.
        intro = self.build / "meson-info" / "intro-tests.json"
        constructs = load(BASH_CONSTRUCTS, "_bash_constructs_ps1_closure")
        self.assertEqual(constructs.seeds_from_intro(intro, root), [sh_script])
        self.assertIsNone(constructs.shell_seed_from_cmd(ps_entry["cmd"]))
        self.assertIsNone(constructs.shell_seed_from_cmd(bare_entry["cmd"]))
        self.assertEqual([s for _, s in self.gate.shell_seeds(intro)],
                         [str(sh_script)])


if __name__ == "__main__":
    unittest.main()
