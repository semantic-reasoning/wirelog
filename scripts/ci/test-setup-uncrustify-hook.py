#!/usr/bin/env python3
"""Self-test for scripts/setup_uncrustify_hook.py (#1464).

The installer used to open the hook file without an encoding and write a
body containing non-ASCII characters, so on a console whose locale codec
could not encode them (cp949 on Windows) the write raised after ``open()``
had already truncated the file: the hook was left empty and non-executable
and every ``git commit`` failed.  meson.build runs the installer with
``check: false``, so nobody saw the error.

This test drives the installer's hook writer in a subprocess under a C
locale with PEP 538/540 locale coercion disabled, which is the only way to
reproduce the defect from a UTF-8 host, and asserts the hook is non-empty,
executable and decodes as UTF-8.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1]

DRIVER = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import setup_uncrustify_hook as hook
ok, message = hook.setup_pre_commit_hook(Path(sys.argv[2]))
print(message)
sys.exit(0 if ok else 1)
"""


class HookInstallCase(unittest.TestCase):
    def install(self, env_extra: dict[str, str]) -> tuple[int, Path, str]:
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        root = Path(tmp)
        (root / ".git" / "hooks").mkdir(parents=True)
        env = dict(os.environ)
        env.update(env_extra)
        proc = subprocess.run(
            [sys.executable, "-c", DRIVER, str(SCRIPTS_DIR), str(root)],
            env=env, capture_output=True, text=True, errors="replace")
        return proc.returncode, root / ".git" / "hooks" / "pre-commit", \
            proc.stdout + proc.stderr

    def assert_hook_ok(self, rc: int, hook: Path, output: str) -> None:
        self.assertEqual(rc, 0, output)
        self.assertTrue(hook.is_file(), output)
        body = hook.read_bytes()
        self.assertGreater(len(body), 0, "hook was truncated to zero bytes")
        text = body.decode("utf-8")
        self.assertIn("uncrustify", text)
        if os.name != "nt":
            self.assertTrue(hook.stat().st_mode & stat.S_IXUSR, "hook not executable")

    def test_installs_under_utf8_locale(self) -> None:
        self.assert_hook_ok(*self.install({"PYTHONIOENCODING": "utf-8"}))

    def test_installs_under_c_locale_without_coercion(self) -> None:
        """The defect's environment: an ASCII console with locale coercion off."""
        self.assert_hook_ok(*self.install({
            "LC_ALL": "C", "LANG": "C", "PYTHONUTF8": "0",
            "PYTHONCOERCECLOCALE": "0", "PYTHONIOENCODING": "ascii",
        }))

    def test_existing_hook_is_left_alone(self) -> None:
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        root = Path(tmp)
        hooks = root / ".git" / "hooks"
        hooks.mkdir(parents=True)
        (hooks / "pre-commit").write_text("#!/bin/sh\n# runs uncrustify already\n",
                                          encoding="utf-8")
        proc = subprocess.run(
            [sys.executable, "-c", DRIVER, str(SCRIPTS_DIR), str(root)],
            capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("already contains", proc.stdout)


if __name__ == "__main__":
    unittest.main()
