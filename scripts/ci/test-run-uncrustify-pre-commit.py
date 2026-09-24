#!/usr/bin/env python3
"""Integration tests for the generated transactional Uncrustify hook."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = ROOT / "scripts/ci/run-uncrustify-pre-commit.py"
SETUP_PATH = ROOT / "scripts/setup_uncrustify_hook.py"
REAL_GIT = shutil.which("git")


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TransactionalHookCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="wirelog-hook-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "repo with spaces"
        self.root.mkdir()
        self.formatter_dir = Path(self.temp.name) / "bin"
        self.formatter_dir.mkdir()
        formatter_script = self.formatter_dir / "fake_uncrustify.py"
        formatter_script.write_text(
            "import os, pathlib, sys\n"
            "args = sys.argv\n"
            "source = pathlib.Path(args[args.index('-f') + 1])\n"
            "data = source.read_bytes()\n"
            "if os.environ.get('FAKE_REPLACE_SOURCE'):\n"
            "    temp = source.with_name(source.name + '.replacement')\n"
            "    temp.write_bytes(data)\n"
            "    os.replace(temp, source)\n"
            "if source.name in ('bad.c', 'z-bad.c'):\n"
            "    sys.stdout.buffer.write(b'partial formatter output')\n"
            "    raise SystemExit(23)\n"
            "sys.stdout.buffer.write(data + b'\\nformatted')\n",
            encoding="utf-8")
        if os.name == "nt":
            self.fake_formatter = self.formatter_dir / "uncrustify.cmd"
            self.fake_formatter.write_text(
                f'@echo off\r\n"{sys.executable}" "{formatter_script}" %*\r\n'
                "exit /b %ERRORLEVEL%\r\n",
                encoding="utf-8")
        else:
            self.fake_formatter = self.formatter_dir / "uncrustify"
            self.fake_formatter.write_text(
                "#!/bin/sh\n"
                f'exec "{sys.executable}" "{formatter_script}" "$@"\n',
                encoding="utf-8")
            self.fake_formatter.chmod(0o755)
        self.env = os.environ.copy()
        self.env["PATH"] = str(self.formatter_dir) + os.pathsep + self.env["PATH"]
        self.env["TMPDIR"] = str(Path(self.temp.name) / "tmp")
        Path(self.env["TMPDIR"]).mkdir()
        self._git("init", "-q")
        self._git("config", "user.name", "Hook Test")
        self._git("config", "user.email", "hook-test@example.invalid")
        (self.root / "uncrustify.cfg").write_text("# fake config\n", encoding="utf-8")
        scripts = self.root / "scripts" / "ci"
        scripts.mkdir(parents=True)
        shutil.copy2(RUNNER_PATH, scripts / RUNNER_PATH.name)
        setup = load_module("setup_uncrustify_hook_test", SETUP_PATH)
        ok, detail = setup.setup_pre_commit_hook(self.root)
        self.assertTrue(ok, detail)
        hook = self.root / ".git" / "hooks" / "pre-commit"
        self.assertEqual(hook.read_text(encoding="utf-8"), setup.get_uncrustify_check_script())
        if os.name != "nt":
            self.assertTrue(hook.stat().st_mode & stat.S_IXUSR)
        self.hook_body = hook.read_text(encoding="utf-8")

    def _git(self, *args: str, env: dict[str, str] | None = None,
             check: bool = True, input: bytes | None = None) -> subprocess.CompletedProcess:
        return subprocess.run([REAL_GIT, "-C", str(self.root), *args], env=env or self.env,
                              input=input, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              check=check)

    def _stage(self, files: dict[str, bytes]) -> None:
        for relative, data in files.items():
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            self._git("add", "--", relative)

    def _tree(self) -> bytes:
        return self._git("write-tree").stdout

    def _snapshot(self, relative_paths: list[str]) -> tuple[bytes, dict[str, bytes]]:
        return self._tree(), {relative: (self.root / relative).read_bytes()
                              for relative in relative_paths}

    def _commit(self, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
        return self._git("commit", "-m", "hook test", env=env, check=False)

    def _assert_no_temporary_files(self) -> None:
        self.assertEqual(list((self.root / ".git").glob(".wirelog-index-*")), [])
        self.assertFalse(Path(str(self.root / ".git" / "index") + ".lock").exists())
        self.assertEqual(list(Path(self.env["TMPDIR"]).glob("wirelog-uncrustify-*")), [])
        for pattern in (".wirelog-backup-*", ".wirelog-formatted-*"):
            leftovers = list(self.root.rglob(pattern))
            self.assertEqual(leftovers, [], f"temporary hook files remain: {leftovers}")

    def test_partial_output_and_nonzero_exit_preserve_batch(self) -> None:
        self._stage({"bad.c": b"original bad\n", "good.c": b"original good\n"})
        before = self._snapshot(["bad.c", "good.c"])
        result = self._commit()
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(b"uncrustify failed for bad.c", result.stderr)
        self.assertEqual(self._snapshot(["bad.c", "good.c"]), before)
        self._assert_no_temporary_files()

    def test_later_formatter_failure_does_not_apply_earlier_output(self) -> None:
        self._stage({"a.c": b"original a\n", "z-bad.c": b"original bad\n"})
        before = self._snapshot(["a.c", "z-bad.c"])
        result = self._commit()
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self._snapshot(["a.c", "z-bad.c"]), before)
        self._assert_no_temporary_files()

    def test_success_formats_and_stages_unusual_filename(self) -> None:
        filename = "line\nbreak.c" if os.name != "nt" else "line break.c"
        relative = f"space name/{filename}"
        self._stage({relative: b"original\n", "second.c": b"second\n"})
        result = self._commit()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        formatted = b"original\n\nformatted"
        self.assertEqual((self.root / relative).read_bytes(), formatted)
        committed = self._git("show", f"HEAD:{relative}").stdout
        self.assertEqual(committed, formatted)
        self.assertEqual((self.root / "second.c").read_bytes(), b"second\n\nformatted")
        self.assertEqual(self._git("show", "HEAD:second.c").stdout, b"second\n\nformatted")
        self._assert_no_temporary_files()

    def test_renames_use_destination_extension_for_formatting(self) -> None:
        self._stage({"old.c": b"c source\n"})
        self.assertEqual(self._commit().returncode, 0)

        self._git("mv", "old.c", "new.c")
        self.assertIn(b"R100\told.c\tnew.c", self._git(
            "diff", "--cached", "--name-status", "--find-renames").stdout)
        self.assertEqual(self._commit().returncode, 0)
        c_to_c = b"c source\n\nformatted\nformatted"
        self.assertEqual(self._git("show", "HEAD:new.c").stdout, c_to_c)

        self._stage({"legacy.txt": b"text source\n"})
        self.assertEqual(self._commit().returncode, 0)
        self._git("mv", "legacy.txt", "migrated.c")
        self.assertIn(b"R100\tlegacy.txt\tmigrated.c", self._git(
            "diff", "--cached", "--name-status", "--find-renames").stdout)
        self.assertEqual(self._commit().returncode, 0)
        self.assertEqual(self._git("show", "HEAD:migrated.c").stdout,
                         b"text source\n\nformatted")

        self._git("mv", "new.c", "retired.txt")
        self.assertIn(b"R100\tnew.c\tretired.txt", self._git(
            "diff", "--cached", "--name-status", "--find-renames").stdout)
        self.assertEqual(self._commit().returncode, 0)
        self.assertEqual(self._git("show", "HEAD:retired.txt").stdout, c_to_c)
        self._assert_no_temporary_files()

    def test_partial_staging_is_rejected_without_staging_worktree_edits(self) -> None:
        path = self.root / "partial.c"
        path.write_bytes(b"staged version\n")
        blob = self._git("hash-object", "-w", "--stdin", input=b"staged version\n").stdout.strip().decode()
        self._git("update-index", "--add", "--cacheinfo", f"100644,{blob},partial.c")
        path.write_bytes(b"staged version\nunstaged edit\n")
        before = self._snapshot(["partial.c"])
        result = self._commit()
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(b"has unstaged changes", result.stderr)
        self.assertEqual(self._snapshot(["partial.c"]), before)
        self._assert_no_temporary_files()

    def test_symlink_source_is_rejected(self) -> None:
        target = self.root / "target.c"
        target.write_bytes(b"target\n")
        try:
            (self.root / "link.c").symlink_to("target.c")
        except (NotImplementedError, OSError) as error:
            self.skipTest(f"symlinks are unavailable: {error}")
        self._git("add", "--", "target.c", "link.c")
        before = self._tree()
        result = self._commit()
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self._tree(), before)
        self.assertTrue((self.root / "link.c").is_symlink())
        self._assert_no_temporary_files()

    def test_source_identity_change_during_formatting_is_rejected(self) -> None:
        self._stage({"identity.c": b"original\n"})
        before = self._snapshot(["identity.c"])
        env = self.env.copy()
        env["FAKE_REPLACE_SOURCE"] = "1"
        result = self._commit(env)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(b"source changed while formatting", result.stderr)
        self.assertEqual(self._snapshot(["identity.c"]), before)
        self._assert_no_temporary_files()

    def test_git_add_failure_rolls_back_worktree_and_keeps_real_index(self) -> None:
        self._stage({"a.c": b"original a\n", "b.c": b"original b\n"})
        before = self._snapshot(["a.c", "b.c"])
        runner = load_module("uncrustify_runner_add_test", RUNNER_PATH)
        real_git = runner.git
        add_calls = 0

        def fail_second_add(root, args, *, env=None, check=True, capture=False):
            nonlocal add_calls
            if "add" in args:
                add_calls += 1
                if add_calls == 2:
                    return subprocess.CompletedProcess(args, 41, stdout=b"", stderr=b"injected")
            return real_git(root, args, env=env, check=check, capture=capture)

        previous_cwd = Path.cwd()
        previous_env = os.environ.copy()
        try:
            os.chdir(self.root)
            os.environ.update(self.env)
            with mock.patch.object(runner, "git", side_effect=fail_second_add):
                with open(os.devnull, "w", encoding="utf-8") as sink:
                    with contextlib.redirect_stderr(sink):
                        self.assertEqual(runner.run(), 1)
        finally:
            os.chdir(previous_cwd)
            os.environ.clear()
            os.environ.update(previous_env)
        self.assertEqual(add_calls, 2)
        self.assertEqual(self._snapshot(["a.c", "b.c"]), before)
        self._assert_no_temporary_files()

    def test_worktree_apply_failure_rolls_back_earlier_replacement(self) -> None:
        self._stage({"a.c": b"original a\n", "b.c": b"original b\n"})
        before = self._snapshot(["a.c", "b.c"])
        runner = load_module("uncrustify_runner_test", RUNNER_PATH)
        real_replace = os.replace
        failed = False

        def fail_second_apply(source, destination):
            nonlocal failed
            if Path(destination).resolve(strict=False) == (self.root / "b.c").resolve(strict=False) \
                    and not failed:
                failed = True
                raise OSError("injected replacement failure")
            return real_replace(source, destination)

        previous_cwd = Path.cwd()
        previous_env = os.environ.copy()
        previous_tempdir = runner.tempfile.tempdir
        try:
            os.chdir(self.root)
            os.environ.update(self.env)
            runner.tempfile.tempdir = self.env["TMPDIR"]
            with mock.patch.object(os, "replace", side_effect=fail_second_apply):
                error_output = io.StringIO()
                with contextlib.redirect_stderr(error_output):
                    self.assertEqual(runner.run(), 1)
        finally:
            os.chdir(previous_cwd)
            os.environ.clear()
            os.environ.update(previous_env)
            runner.tempfile.tempdir = previous_tempdir
        self.assertTrue(failed, error_output.getvalue())
        self.assertEqual(self._snapshot(["a.c", "b.c"]), before)
        self._assert_no_temporary_files()

    def test_copy_and_temporary_file_failures_leave_state_unchanged(self) -> None:
        self._stage({"copy.c": b"original\n"})
        before = self._snapshot(["copy.c"])
        runner = load_module("uncrustify_runner_copy_test", RUNNER_PATH)
        previous_cwd = Path.cwd()
        previous_env = os.environ.copy()
        previous_tempdir = runner.tempfile.tempdir
        try:
            os.chdir(self.root)
            os.environ.update(self.env)
            runner.tempfile.tempdir = self.env["TMPDIR"]
            with mock.patch.object(runner.shutil, "copyfile", side_effect=OSError("copy failed")):
                with open(os.devnull, "w", encoding="utf-8") as sink:
                    with contextlib.redirect_stderr(sink):
                        self.assertEqual(runner.run(), 1)
            with mock.patch.object(runner.tempfile, "mkstemp", side_effect=OSError("temp failed")):
                with open(os.devnull, "w", encoding="utf-8") as sink:
                    with contextlib.redirect_stderr(sink):
                        self.assertEqual(runner.run(), 1)
        finally:
            os.chdir(previous_cwd)
            os.environ.clear()
            os.environ.update(previous_env)
            runner.tempfile.tempdir = previous_tempdir
        self.assertEqual(self._snapshot(["copy.c"]), before)
        self._assert_no_temporary_files()

    def test_cleanup_failure_after_index_install_keeps_state_consistent(self) -> None:
        self._stage({"cleanup.c": b"original\n"})
        runner = load_module("uncrustify_runner_cleanup_test", RUNNER_PATH)
        previous_cwd = Path.cwd()
        previous_env = os.environ.copy()
        previous_tempdir = runner.tempfile.tempdir
        try:
            os.chdir(self.root)
            os.environ.update(self.env)
            runner.tempfile.tempdir = self.env["TMPDIR"]
            with mock.patch.object(runner, "_cleanup", side_effect=OSError("injected cleanup failure")):
                with open(os.devnull, "w", encoding="utf-8") as sink:
                    with contextlib.redirect_stderr(sink):
                        self.assertEqual(runner.run(), 0)
        finally:
            os.chdir(previous_cwd)
            os.environ.clear()
            os.environ.update(previous_env)
            runner.tempfile.tempdir = previous_tempdir
        formatted = b"original\n\nformatted"
        self.assertEqual((self.root / "cleanup.c").read_bytes(), formatted)
        self.assertEqual(self._git("show", ":cleanup.c").stdout, formatted)


if __name__ == "__main__":
    unittest.main()
