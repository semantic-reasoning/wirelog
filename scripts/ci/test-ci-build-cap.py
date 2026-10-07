#!/usr/bin/env python3
"""CI Ninja cap contracts; opt in to the tiny Meson fixture with --integration."""

import importlib.util
import json
import os
from pathlib import Path
import shutil
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
LAUNCHER = ROOT / '.github/actions/setup-meson/ninja-cap.py'
spec = importlib.util.spec_from_file_location('ninja_cap', LAUNCHER)
cap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cap)
INTEGRATION = '--integration' in sys.argv
if INTEGRATION:
    sys.argv.remove('--integration')


def temp_directory():
    # CI fixtures follow the perf helpers' HOME temp policy on every platform.
    parent = Path.home() / '.tmp'
    parent.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(prefix='ci-build-cap-', dir=parent)


class LauncherContract(unittest.TestCase):
    def test_available_affinity(self):
        for count in (1, 2, 7, 8, 16, 64):
            with self.subTest(count=count), patch.object(
                    cap.os, 'sched_getaffinity', return_value=set(range(count)), create=True):
                self.assertEqual(cap.available_jobs(), min(count, 8))

    def test_cpu_fallback_and_unavailable(self):
        for count in (None, 0, 1, 2, 7, 8, 16, 64):
            with self.subTest(count=count), patch.object(
                    cap.os, 'sched_getaffinity', side_effect=OSError, create=True), patch.object(
                    cap.os, 'process_cpu_count', return_value=count, create=True):
                self.assertEqual(cap.available_jobs(), min(max(count or 1, 1), 8))
        with patch.object(cap.os, 'sched_getaffinity', None, create=True), patch.object(
                cap.os, 'process_cpu_count', return_value=3, create=True):
            self.assertEqual(cap.available_jobs(), 3)
        with patch.object(cap, 'os', SimpleNamespace(cpu_count=lambda: 2)):
            self.assertEqual(cap.available_jobs(), 2)

    def test_ninja_jobs_forms_and_clusters(self):
        cases = [
            ([], ['-j', '8']),
            (['-C', 'a b', 'target'], ['-j', '8', '-C', 'a b', 'target']),
            (['-j', '2'], ['-j', '2']),
            (['-j7'], ['-j', '7']),
            (['-j8'], ['-j', '8']),
            (['-j64'], ['-j', '8']),
            (['-j0'], ['-j', '8']),
            (['-vj64'], ['-j', '8', '-v']),
            (['-nvj', '2'], ['-j', '2', '-nv']),
            (['-j2', '-j0', '-j64'], ['-j', '2']),
            (['-j64', '-j2'], ['-j', '2']),
            (['--', '-j64'], ['-j', '8', '--', '-j64']),
            (['-f', '-j64', '-n'], ['-j', '8', '-f', '-j64', '-n']),
            (['-vC-j64'], ['-j', '8', '-vC-j64']),
            (['-t', 'commands', '-j64'], ['-j', '8', '-t', 'commands', '-j64']),
            (['-tcommands', '-j64'], ['-j', '8', '-tcommands', '-j64']),
            (['--jobs=64'], ['-j', '8', '--jobs=64']),
            (['--version'], ['-j', '8', '--version']),
        ]
        for before, after in cases:
            with self.subTest(before=before):
                self.assertEqual(cap.bounded_args(before, 8), after)
        self.assertEqual(cap.bounded_args(['-j64'], 2), ['-j', '2'])

    def test_malformed_jobs_fail_closed(self):
        for args in (['-j'], ['-j-1'], ['-jabc'], ['-j', '1.5'],
                     ['-j64v'], ['-qj64'], ['-j', '８'], ['-j', '']):
            with self.subTest(args=args), self.assertRaises(ValueError):
                cap.bounded_args(args, 8)

    def test_backend_selection_preserves_and_reuses_inherited_ninja(self):
        # The fixture executable also works as a real .exe on Windows.
        backend = str(Path(sys.executable).resolve())
        with patch.dict(os.environ, {'NINJA': backend}, clear=True):
            self.assertEqual(cap.resolve_backend(), backend)
        with patch.dict(os.environ, {'NINJA': str(LAUNCHER), cap.REAL_ENV: backend}, clear=True):
            self.assertEqual(cap.resolve_backend(), backend)
        for env in ({'NINJA': 'missing-wirelog-ninja'}, {'NINJA': str(LAUNCHER)},
                    {'NINJA': str(LAUNCHER), cap.REAL_ENV: str(LAUNCHER)}):
            with self.subTest(env=env), patch.dict(os.environ, env, clear=True), self.assertRaises(ValueError):
                cap.resolve_backend()
        with patch.dict(os.environ, {}, clear=True), patch.object(
                cap.shutil, 'which', return_value=backend) as which:
            self.assertEqual(cap.resolve_backend(), backend)
            which.assert_called_with('ninja')

    def test_main_forwards_bounded_argv_to_exact_backend(self):
        backend = str(Path(sys.executable).resolve())
        command = [backend, '-j', '8', '-C', 'space path', '-v', 'target']
        for platform in ('nt', 'posix'):
            with self.subTest(platform=platform), patch.dict(
                    os.environ, {cap.REAL_ENV: backend}, clear=True), patch.object(
                    cap, 'available_jobs', return_value=8), patch.object(
                    cap.os, 'execv') as execute, patch.object(
                    cap.subprocess, 'run', return_value=SimpleNamespace(returncode=17)) as run:
                # Replace only the launcher's reference; changing os.name globally
                # would make pathlib construct paths for the wrong host platform.
                with patch.object(cap, 'os', SimpleNamespace(
                        name=platform, environ=os.environ, execv=execute)):
                    self.assertEqual(cap.main(['-C', 'space path', '-vj64', 'target']),
                                     17 if platform == 'nt' else 0)
                if platform == 'nt':
                    run.assert_called_once_with(command)
                    execute.assert_not_called()
                else:
                    execute.assert_called_once_with(backend, command)
                    run.assert_not_called()
        for backend in ('', 'ninja', str(LAUNCHER), str(ROOT / 'missing-ninja')):
            with self.subTest(backend=backend), patch.dict(
                    os.environ, {cap.REAL_ENV: backend}, clear=True), patch('sys.stderr'):
                self.assertEqual(cap.main([]), 2)

    def test_real_ninja_waits_and_propagates_status(self):
        ninja = shutil.which('ninja')
        self.assertIsNotNone(ninja, 'CI setup must install Ninja for the lifecycle contract')
        with temp_directory() as temp:
            for status in (0, 1):
                with self.subTest(worker_status=status):
                    root = Path(temp) / f'build path with spaces {status}'
                    root.mkdir()
                    worker = root / 'marker worker.py'
                    worker.write_text(
                        'import pathlib, sys, time\n'
                        'root = pathlib.Path(__file__).parent\n'
                        'print("worker stdout", flush=True)\n'
                        'print("worker stderr", file=sys.stderr, flush=True)\n'
                        '(root / "started").touch()\n'
                        'deadline = time.monotonic() + 10\n'
                        'while not (root / "release").exists():\n'
                        '    if time.monotonic() >= deadline:\n'
                        '        (root / "completed").touch()\n'
                        '        sys.exit(99)\n'
                        '    time.sleep(0.01)\n'
                        '(root / "completed").touch()\n'
                        'sys.exit(int(sys.argv[1]))\n', encoding='utf-8')
                    argv = [sys.executable, str(worker), str(status)]
                    command = (subprocess.list2cmdline(argv) if os.name == 'nt'
                               else shlex.join(argv)).replace('$', '$$')
                    (root / 'build.ninja').write_text(
                        f'rule marker\n  command = {command}\nbuild marker: marker\n',
                        encoding='utf-8')
                    env = dict(os.environ, WIRELOG_NINJA_REAL=str(Path(ninja).resolve()))
                    # File-backed streams cannot hide early launcher exit by keeping
                    # captured pipe handles open in an independently running child.
                    with (root / 'stdout.log').open('w', encoding='utf-8') as stdout, (
                            root / 'stderr.log').open('w', encoding='utf-8') as stderr:
                        process = subprocess.Popen(
                            [sys.executable, str(LAUNCHER)], cwd=root, env=env,
                            stdout=stdout, stderr=stderr)
                        try:
                            deadline = time.monotonic() + 10
                            while not (root / 'started').exists():
                                self.assertLess(time.monotonic(), deadline,
                                                'Ninja worker did not start')
                                time.sleep(0.01)
                            self.assertIsNone(process.poll(),
                                              'launcher exited while Ninja was running')
                            (root / 'release').touch()
                            self.assertEqual(process.wait(timeout=10), 0 if status == 0 else 1)
                            self.assertTrue((root / 'completed').exists())
                        finally:
                            # Release the worker even when detecting an early exit.
                            (root / 'release').touch()
                            try:
                                process.wait(timeout=10)
                            except subprocess.TimeoutExpired:
                                process.terminate()
                                process.wait(timeout=5)
                            deadline = time.monotonic() + 10
                            while (root / 'started').exists() and not (root / 'completed').exists():
                                self.assertLess(time.monotonic(), deadline,
                                                'Ninja worker did not finish during cleanup')
                                time.sleep(0.01)
                    output = ((root / 'stdout.log').read_text(encoding='utf-8') +
                              (root / 'stderr.log').read_text(encoding='utf-8'))
                    self.assertIn('worker stdout', output)
                    self.assertIn('worker stderr', output)

    def test_common_action_and_container_wiring(self):
        action = (ROOT / '.github/actions/setup-meson/action.yml').read_text(encoding='utf-8')
        self.assertIn('${{ github.action_path }}/ninja-cap.py', action)
        self.assertIn("Join-Path '${{ github.action_path }}' 'ninja-cap.py'", action)
        self.assertEqual(action.count('--wirelog-resolve-backend'), 2)
        self.assertIn('"$real_ninja" --version', action)
        self.assertIn('& $realNinja --version', action)
        self.assertIn('>> "$GITHUB_ENV"', action)
        self.assertIn('$env:GITHUB_ENV', action)
        for name in ('run-perf-stable-linux.sh', 'run-perf-nightly-linux.sh'):
            text = (ROOT / 'scripts/ci' / name).read_text(encoding='utf-8')
            self.assertIn('export WIRELOG_NINJA_REAL="$(command -v ninja)"', text)
            self.assertIn('export NINJA="$repo_root/.github/actions/setup-meson/ninja-cap.py"', text)
            self.assertLess(text.index('export NINJA='), text.index('meson setup '))
            self.assertIn('"$WIRELOG_NINJA_REAL" --version', text)
        for name in ('android.yml', 'ios.yml'):
            self.assertIn('uses: ./.github/actions/setup-meson', (ROOT / '.github/workflows' / name).read_text(encoding='utf-8'))
        diagnose = (ROOT / '.github/workflows/diagnose-1575-arm.yml').read_text(encoding='utf-8')
        self.assertIn('meson compile -C builddir-san test_compaction test_diff_join', diagnose)
        self.assertNotIn('ninja -C', diagnose)

    @unittest.skipIf(os.name == 'nt', 'POSIX executable fixture; native Windows is validated by CI')
    def test_exec_streams_cwd_status_and_signal(self):
        with temp_directory() as temp:
            root = Path(temp)
            backend = root / 'fake ninja.py'
            backend.write_text('#!' + sys.executable + '\n'
                               'import json, os, signal, sys\n'
                               'print(json.dumps({"argv": sys.argv[1:], "cwd": os.getcwd()}), flush=True)\n'
                               'print("backend stderr", file=sys.stderr, flush=True)\n'
                               'if os.environ.get("FAKE_SIGNAL"): os.kill(os.getpid(), signal.SIGTERM)\n'
                               'sys.exit(17)\n', encoding='utf-8')
            backend.chmod(0o755)
            env = dict(os.environ, WIRELOG_NINJA_REAL=str(backend))
            result = subprocess.run([sys.executable, str(LAUNCHER), '-vj64', 'target'],
                                    cwd=root, env=env, capture_output=True, text=True, encoding='utf-8', timeout=10)
            self.assertEqual(result.returncode, 17)
            recorded = json.loads(result.stdout)
            self.assertEqual(recorded['argv'], ['-j', str(cap.available_jobs()), '-v', 'target'])
            self.assertEqual(recorded['cwd'], str(root))
            self.assertEqual(result.stderr, 'backend stderr\n')
            env['FAKE_SIGNAL'] = '1'
            result = subprocess.run([sys.executable, str(LAUNCHER)], cwd=root, env=env,
                                    capture_output=True, text=True, encoding='utf-8', timeout=10)
            self.assertEqual(result.returncode, -signal.SIGTERM)


@unittest.skipUnless(INTEGRATION, 'opt in with --integration (Meson 1.12.0 and Ninja required)')
class MesonIntegration(unittest.TestCase):
    def test_windows_python_shebang_resolution(self):
        from mesonbuild.programs import ExternalProgram
        from mesonbuild import mesonlib
        with patch.object(mesonlib, 'is_windows', return_value=True):
            self.assertEqual(ExternalProgram._shebang_to_cmd(str(LAUNCHER)),
                             mesonlib.python_command + [str(LAUNCHER)])

    @unittest.skipIf(os.name == 'nt', 'recording backend uses a POSIX shebang')
    def test_setup_compile_and_implicit_test_rebuild(self):
        from mesonbuild.coredata import version
        self.assertEqual(version, '1.12.0')
        ninja = shutil.which('ninja')
        self.assertIsNotNone(ninja)
        with temp_directory() as temp:
            root = Path(temp)
            source = root / 'source'
            source.mkdir()
            home = root / 'home'
            home.mkdir()
            tmp = home / '.tmp'
            tmp.mkdir()
            trace = root / 'ninja.jsonl'
            backend = root / 'record-ninja.py'
            backend.write_text('#!' + sys.executable + '\n'
                               'import json, os, sys\n'
                               'with open(os.environ["CAP_TRACE"], "a", encoding="utf-8") as log:\n'
                               '    log.write(json.dumps(sys.argv[1:]) + "\\n")\n'
                               'os.execv(os.environ["CAP_REAL"], [os.environ["CAP_REAL"], *sys.argv[1:]])\n', encoding='utf-8')
            backend.chmod(0o755)
            (source / 'meson.build').write_text("project('cap-fixture', 'c')\n"
                                                "prog = executable('cap-test', 'main.c')\n"
                                                "test('cap-test', prog)\n", encoding='utf-8')
            c_source = source / 'main.c'
            c_source.write_text('int main(void) { return 0; }\n', encoding='utf-8')
            build = root / 'build'
            env = dict(os.environ, HOME=str(home), TMPDIR=str(tmp), NINJA=str(LAUNCHER),
                       WIRELOG_NINJA_REAL=str(backend), CAP_TRACE=str(trace), CAP_REAL=ninja)
            command = [sys.executable, '-m', 'mesonbuild.mesonmain']

            def run(args):
                result = subprocess.run(command + args, env=env, cwd=root,
                                        text=True, encoding='utf-8', capture_output=True, timeout=60)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                lines = [json.loads(line) for line in trace.read_text(encoding='utf-8').splitlines()]
                trace.write_text('', encoding='utf-8')
                self.assertTrue(lines, 'Meson bypassed the configured Ninja launcher')
                for argv in lines:
                    self.assertEqual(argv[:2], ['-j', str(cap.available_jobs())])
                    self.assertEqual(argv.count('-j'), 1)
                return lines

            setup = run(['setup', str(build), str(source)])
            self.assertTrue(any('--version' in argv for argv in setup))
            compile_calls = run(['compile', '-C', str(build), '-j', '64'])
            self.assertTrue(any('-C' in argv for argv in compile_calls))
            # Removing the output guarantees meson test has to rebuild it.
            (build / 'cap-test').unlink()
            tests = run(['test', '-C', str(build), '--print-errorlogs'])
            self.assertTrue(any('--version' not in argv for argv in tests))
            self.assertTrue((build / 'cap-test').exists())


if __name__ == '__main__':
    unittest.main()
