#!/usr/bin/env python3
"""Collector v1 provenance, durability and complete schedule contracts."""
import argparse
import ctypes
import json
import os
from os import open as open_fd  # File-descriptor open; no text encoding applies.
from pathlib import Path
import runpy
import select
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

M = runpy.run_path(str(Path(__file__).with_name('paired-collector-v1.py')))
G = M['collect'].__globals__


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        args = dict(mode='aa_control', base_sha='a' * 40, candidate_sha='a' * 40,
                    cpu=min(os.sched_getaffinity(0)), timeout=1, out_dir=self.root / 'output')
        for side in M['SIDES']:
            source = self.root / (side + '-source')
            build = self.root / (side + '-build')
            subprocess.run(['git', 'init', '-q', str(source)], check=True)
            for name in set(M['CONTRACT_SOURCES'] + M['VERIFY']['GATE_SOURCES'] + M['VERIFY']['FIXTURES']):
                path = source / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('"WIRELOG_CRDT_PROBE"\n', encoding='utf-8')
            subprocess.run(['git', '-C', str(source), 'add', '.'], check=True)
            subprocess.run(['git', '-C', str(source), '-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'commit', '-qm', 'fixture'], check=True)
            args[side + '_sha'] = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True, encoding='utf-8').strip()
            for name in M['VERIFY']['BINARIES']:
                path = build / name
                path.parent.mkdir(parents=True, exist_ok=True)
                if 'test_crdt' in name:
                    record = dict(schema_version=1, measurement='crdt_perf_gate_single_run', workload='crdt', fixture='full', workers=1, expected=104851, result=104851, aggregate=2152328, iterations=14148, status='OK', elapsed_ms=10)
                    text = "#!/bin/sh\nprintf '%s\\n' '" + json.dumps(record) + "'\n"
                else:
                    text = "#!/bin/sh\nprintf '%s\\n' '" + M['LEGACY']['HEADER'] + "' 'cspa\t-\t-\t1\t1\t2\t2\t2\t100\t20381\t6\tOK'\n"
                path.write_text(text, encoding='utf-8')
                path.chmod(0o755)
            info = build / 'meson-info'
            info.mkdir()
            (info / 'intro-buildoptions.json').write_text(json.dumps([dict(name=k, value=True if k == 'tests' else [] if k == 'c_args' else 'release') for k in M['VERIFY']['PROFILE_OPTIONS']]), encoding='utf-8')
            (info / 'intro-compilers.json').write_text(json.dumps(dict(host=dict(c=dict(id='gcc', version='1', full_version='gcc 1', linker_id='ld', exelist=['cc'])))), encoding='utf-8')
            log = self.root / (side + '.log')
            log.write_text(side + ' build completed\n', encoding='utf-8')
            args.update({side + '_source': source, side + '_build': build, side + '_build_log': log})
        # Commits have identical tree/content but timestamps can differ.
        subprocess.run(['git', '-C', str(args['candidate_source']), 'fetch', '-q', str(args['base_source']), args['base_sha']], check=True)
        subprocess.run(['git', '-C', str(args['candidate_source']), 'reset', '--hard', '-q', args['base_sha']], check=True)
        args['candidate_sha'] = args['base_sha']
        self.args = argparse.Namespace(**args)

    def read(self, name):
        return json.loads((self.args.out_dir / name).read_text(encoding='utf-8'))

    def test_complete_aa_and_exact_schedule(self):
        self.assertEqual(M['collect'](self.args), 0)
        campaign = self.read('campaign-v1.json')
        self.assertEqual(len(campaign['attempts']), 80)
        self.assertEqual([dict((k, e[k]) for k in M['schedule']()[0]) for e in campaign['attempts']], M['schedule']())
        for order in ('AB', 'BA'):
            for workload in M['EXPECTED']:
                events = [e for e in campaign['attempts'] if e['block_id'] == order and e['workload'] == workload]
                self.assertEqual([e['sequence'] for e in events], list(range(20)))
                self.assertEqual([e['side'] for e in events], list(M['SIDES'] if order == 'AB' else M['SIDES'][::-1]) * 10)
        report = self.read('evaluation-report.json')
        self.assertEqual(report['status'], 'COMPLETE_VALID')
        self.assertEqual(self.read('preflight.json')['theoretical_launch_timeout_ceiling_seconds'], 80)
        self.assertNotEqual(campaign['manifest']['build_provenance']['base']['build_instance_id'], campaign['manifest']['build_provenance']['candidate']['build_instance_id'])
        with self.assertRaisesRegex(ValueError, 'already exists'):
            M['collect'](self.args)

    def test_comparison_and_preflight(self):
        subprocess.run(['git', '-C', str(self.args.candidate_source), '-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'commit', '--allow-empty', '-qm', 'candidate'], check=True)
        self.args.candidate_sha = subprocess.check_output(['git', '-C', str(self.args.candidate_source), 'rev-parse', 'HEAD'], text=True, encoding='utf-8').strip()
        self.args.mode = 'comparison'
        self.assertEqual(M['collect'](self.args), 0)

    def test_explicit_v1_cli_dispatch(self):
        command = [str(Path(__file__).parents[2] / '.venv/bin/python'),
                   str(Path(__file__).with_name('paired-benchmark.py')), 'v1']
        for key, value in vars(self.args).items():
            command += ['--' + key.replace('_', '-'), str(value)]
        result = subprocess.run(command, capture_output=True, text=True, encoding='utf-8')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.read('collection-status.json')['status'], 'COMPLETE_VALID')

    def test_failed_parse_continues_and_nonzero_is_correctness_failure(self):
        original = G['execute']
        count = 0
        def execute(*args):
            nonlocal count
            raw = original(*args)
            count += 1
            if count == 1:
                raw['stdout'] = 'bad'
            if count == 2:
                raw['exit_code'] = 9
            return raw
        with patch.dict(G, execute=execute):
            self.assertEqual(M['collect'](self.args), 1)
        self.assertEqual(count, 80)
        self.assertEqual(len(self.read('campaign-v1.json')['attempts']), 79)
        self.assertEqual(len((self.args.out_dir / 'raw-attempts.jsonl').read_text(encoding='utf-8').splitlines()), 80)

    def test_unparseable_timeout_is_incomplete(self):
        with patch.dict(G, execute=lambda *_: dict(stdout='', stderr='timeout', exit_code=-9, timed_out=True, host_before={}, host_after={})):
            self.assertEqual(M['collect'](self.args), 3)
        self.assertEqual(self.read('collection-status.json')['status'], 'INCOMPLETE')
        self.assertEqual(len((self.args.out_dir / 'raw-attempts.jsonl').read_text(encoding='utf-8').splitlines()), 80)

    def test_drift_rejects_campaign(self):
        original = G['execute']
        count = 0
        def execute(*args):
            nonlocal count
            count += 1
            if count == 80:
                self.args.base_build_log.write_text('drift', encoding='utf-8')
            return original(*args)
        with patch.dict(G, execute=execute), self.assertRaisesRegex(ValueError, 'drift'):
            M['collect'](self.args)
        self.assertFalse((self.args.out_dir / 'campaign-v1.json').exists())
        self.assertEqual(self.read('collection-status.json')['status'], 'INVALID_EVIDENCE')

    def test_interruption_retains_fsynced_journal(self):
        original = G['execute']
        count = 0
        def execute(*args):
            nonlocal count
            count += 1
            if count == 3:
                raise KeyboardInterrupt
            return original(*args)
        with patch.dict(G, execute=execute), patch('os.fsync', wraps=os.fsync) as sync, self.assertRaises(KeyboardInterrupt):
            M['collect'](self.args)
        self.assertGreaterEqual(sync.call_count, 4)
        self.assertEqual(len((self.args.out_dir / 'raw-attempts.jsonl').read_text(encoding='utf-8').splitlines()), 2)
        self.assertFalse((self.args.out_dir / 'campaign-v1.json').exists())

    def test_mismatch_rejected_before_launch(self):
        path = self.args.candidate_build / 'meson-info/intro-compilers.json'
        path.write_text(path.read_text(encoding='utf-8').replace('gcc 1', 'gcc 2'), encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'compiler'):
            M['collect'](self.args)
        self.assertFalse(self.args.out_dir.exists())

    def test_typed_parse_and_unavailable_telemetry(self):
        result = dict(schema_version=1, measurement='crdt_perf_gate_single_run', workload='crdt', fixture='full', workers=1, expected=104851, result=0, aggregate=2, iterations=0, status='FAIL', elapsed_ms=10)
        self.assertEqual(M['parse_result'](json.dumps(result), 'crdt')[1]['observed']['result'], 0)
        for key, value in [('result', True), ('aggregate', -1), ('elapsed_ms', float('nan')), ('workers', True)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                M['parse_result'](json.dumps(dict(result, **{key: value})), 'crdt')
        self.assertTrue(all(v['value'] is None and v['unavailable_reason'] for v in M['telemetry']({}).values()))
        self.assertEqual(M['profile_value'](True), 'true')
        self.assertEqual(M['profile_value']([]), '[]')

    def test_process_timeout_is_bounded_and_retains_partial_output(self):
        binary = self.root / 'timeout-probe'
        binary.write_text("#!/bin/sh\nprintf 'partial\\n'\nsleep 30 &\nwait\n", encoding='utf-8')
        binary.chmod(0o755)
        raw = M['execute'](binary, 'crdt', self.root, self.args.cpu, 1)
        self.assertTrue(raw['timed_out'])
        self.assertIn(raw['exit_code'], (-signal.SIGTERM, -signal.SIGKILL))
        self.assertEqual(raw['stdout'], 'partial\n')

    def test_wrong_typed_result_is_correctness_failure(self):
        path = self.args.base_build / 'tests/test_crdt_perf_gate'
        path.write_text(path.read_text(encoding='utf-8').replace('"aggregate": 2152328', '"aggregate": 2152327'), encoding='utf-8')
        self.assertEqual(M['collect'](self.args), 1)
        self.assertEqual(self.read('collection-status.json')['status'], 'CORRECTNESS_FAILURE')
        self.assertEqual(len(self.read('campaign-v1.json')['attempts']), 80)

    def test_fixture_and_profile_mismatch_are_preflight_rejections(self):
        path = self.args.candidate_source / M['VERIFY']['FIXTURES'][0]
        path.write_text('wrong fixture', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'dirty'):
            M['collect'](self.args)
        self.assertFalse(self.args.out_dir.exists())
        subprocess.run(['git', '-C', str(self.args.candidate_source), 'checkout', '--', '.'], check=True)
        path = self.args.candidate_build / 'meson-info/intro-buildoptions.json'
        path.write_text(path.read_text(encoding='utf-8').replace('"value": true', '"value": false'), encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'profile'):
            M['collect'](self.args)

    def test_termination_signals_persist_active_launch_and_reap_group(self):
        for iteration, number in ((i, s) for i in range(5) for s in (signal.SIGTERM, signal.SIGHUP)):
            with self.subTest(signal=number, iteration=iteration):
                self.args.out_dir = self.root / f'output-{number}-{iteration}'
                ready = self.root / f'ready-{number}-{iteration}'
                os.mkfifo(ready)
                read_fd = open_fd(ready, os.O_RDONLY | os.O_NONBLOCK)
                probe = self.args.base_build / 'tests/test_crdt_perf_gate'
                probe.write_text('#!' + sys.executable + '\n' + '''
import os, signal, subprocess, sys
child = None
def stop(number, frame):
    if child is not None:
        child.wait(timeout=2)
    sys.exit(128 + number)
signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGHUP, stop)
child = subprocess.Popen([sys.executable, '-c',
    "import signal; print('child-ready', flush=True); signal.pause()"],
    stdout=subprocess.PIPE, text=True)
assert child.stdout.readline() == 'child-ready\\n'
print('active partial output', flush=True)
print('active partial stderr', file=sys.stderr, flush=True)
with open(os.environ['WIRELOG_TEST_READY'], 'w') as stream:
    stream.write(str(os.getpid()) + ' ' + str(child.pid) + '\\n')
signal.pause()
''', encoding='utf-8')
                command = [sys.executable, str(Path(__file__).with_name('paired-benchmark.py')), 'v1']
                for key, value in vars(self.args).items():
                    command += ['--' + key.replace('_', '-'), str(value)]
                environment = dict(os.environ, WIRELOG_TEST_READY=str(ready))
                collector = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                             text=True, env=environment, encoding='utf-8')
                group = None
                try:
                    self.assertTrue(select.select([read_fd], [], [], 10)[0], 'benchmark did not become ready')
                    group, descendant = map(int, os.read(read_fd, 1024).decode().split())
                    os.kill(collector.pid, number)
                    stdout, stderr = collector.communicate(timeout=10)
                    self.assertEqual(collector.returncode, -number, (stdout, stderr))
                    with self.assertRaises(ProcessLookupError):
                        os.killpg(group, 0)
                    for pid in (group, descendant):
                        self.assertFalse(Path(f'/proc/{pid}').exists())
                    lines = (self.args.out_dir / 'raw-attempts.jsonl').read_text(encoding='utf-8').splitlines()
                    self.assertEqual(len(lines), 1)
                    raw = json.loads(lines[0])
                    self.assertTrue(raw['interrupted'])
                    self.assertEqual(raw['signal_number'], number)
                    self.assertEqual(raw['signal_name'], signal.Signals(number).name)
                    self.assertEqual(raw['stdout'], 'active partial output\n')
                    self.assertEqual(raw['stderr'], 'active partial stderr\n')
                    self.assertEqual(raw['exit_code'], 128 + signal.SIGTERM)
                    self.assertTrue(raw['host_before']['time_utc'])
                    self.assertTrue(raw['host_after']['time_utc'])
                    self.assertFalse((self.args.out_dir / 'campaign-v1.json').exists())
                    self.assertFalse((self.args.out_dir / 'evaluation-report.json').exists())
                finally:
                    os.close(read_fd)
                    if group is not None:
                        try:
                            os.killpg(group, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                    if collector.poll() is None:
                        collector.kill()
                    collector.communicate(timeout=5)

    def test_library_signal_handlers_restore_and_preserve_cause(self):
        previous = {number: signal.getsignal(number) for number in M['TERMINATION_SIGNALS']}
        with self.assertRaises(M['TerminationRequested']) as requested:
            with M['termination_handlers']():
                os.kill(os.getpid(), signal.SIGHUP)
        self.assertEqual(requested.exception.signum, signal.SIGHUP)
        self.assertEqual(previous, {number: signal.getsignal(number) for number in previous})

    def test_secondary_cleanup_error_keeps_keyboard_interrupt_record(self):
        original_cleanup = G['cleanup_process_group']
        original_wait = subprocess.Popen.wait
        interrupted = False

        def wait(process, *args, **kwargs):
            nonlocal interrupted
            if not interrupted:
                interrupted = True
                raise KeyboardInterrupt
            return original_wait(process, *args, **kwargs)

        def cleanup(process, raw):
            original_cleanup(process, raw)
            raise OSError('secondary cleanup error')

        # Interrupt the direct wait at the active process cleanup boundary.
        with patch.object(subprocess.Popen, 'wait', wait), patch.dict(G, cleanup_process_group=cleanup):
            with self.assertRaises(M['InterruptedLaunch']) as result:
                M['execute'](self.args.base_build / 'tests/test_crdt_perf_gate', 'crdt',
                             self.args.base_source / 'bench/data', self.args.cpu, 1)
        self.assertIsInstance(result.exception.cause, KeyboardInterrupt)
        self.assertEqual(result.exception.raw['interruption'], 'KeyboardInterrupt')
        self.assertIn('secondary cleanup error', result.exception.raw['cleanup_error'])

    def test_binary_capture_replaces_invalid_utf8(self):
        probe = self.root / 'binary-output'
        probe.write_text('#!' + sys.executable + '\nimport os\nos.write(1, b"out\\xff\\n")\nos.write(2, b"err\\xfe\\n")\n', encoding='utf-8')
        probe.chmod(0o755)
        raw = M['execute'](probe, 'crdt', self.root, self.args.cpu, 1)
        self.assertEqual(raw['stdout'], 'out\ufffd\n')
        self.assertEqual(raw['stderr'], 'err\ufffd\n')
        self.assertEqual(raw['exit_code'], 0)

    def test_exited_leader_still_cleans_live_descendant(self):
        # Adopt the orphan for this Linux integration test so it can be reaped
        # here instead of relying on the container's PID 1 zombie handling.
        libc = ctypes.CDLL(None, use_errno=True)
        previous = ctypes.c_int()
        self.assertEqual(libc.prctl(37, ctypes.byref(previous), 0, 0, 0), 0)
        self.assertEqual(libc.prctl(36, 1, 0, 0, 0), 0)
        probe = self.root / 'orphan-probe'
        probe.write_text('#!' + sys.executable + '''
import os, signal
child = os.fork()
if child == 0:
    signal.pause()
    os._exit(0)
os.write(1, (str(os.getpid()) + ' ' + str(child) + '\\n').encode())
os._exit(0)
''', encoding='utf-8')
        probe.chmod(0o755)
        descendant = None
        try:
            raw = M['execute'](probe, 'crdt', self.root, self.args.cpu, 1)
            group, descendant = map(int, raw['stdout'].split())
            self.assertEqual(raw['exit_code'], 0)
            pidfd = os.pidfd_open(descendant)
            try:
                self.assertTrue(select.select([pidfd], [], [], 2)[0], 'descendant survived cleanup')
            finally:
                os.close(pidfd)
            _, status = os.waitpid(descendant, 0)
            self.assertTrue(os.WIFSIGNALED(status))
            with self.assertRaises(ProcessLookupError):
                os.killpg(group, 0)
        finally:
            if descendant is not None:
                try:
                    os.kill(descendant, signal.SIGKILL)
                    os.waitpid(descendant, 0)
                except (ProcessLookupError, ChildProcessError):
                    pass
            self.assertEqual(libc.prctl(36, previous.value, 0, 0, 0), 0)

    def test_popen_boundary_signal_restores_mask_and_handlers(self):
        previous_handlers = {n: signal.getsignal(n) for n in M['TERMINATION_SIGNALS']}
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
        original_popen = subprocess.Popen

        def popen(*args, **kwargs):
            process = original_popen(*args, **kwargs)
            os.kill(os.getpid(), signal.SIGHUP)
            return process

        with M['termination_handlers'](), patch.object(subprocess, 'Popen', popen):
            with self.assertRaises(M['InterruptedLaunch']) as result:
                M['execute'](self.args.base_build / 'tests/test_crdt_perf_gate', 'crdt',
                             self.root, self.args.cpu, 1)
        self.assertIsInstance(result.exception.cause, M['TerminationRequested'])
        self.assertEqual(result.exception.raw['signal_number'], signal.SIGHUP)
        self.assertEqual(previous_mask, signal.pthread_sigmask(signal.SIG_BLOCK, set()))
        self.assertEqual(previous_handlers, {n: signal.getsignal(n) for n in previous_handlers})

    def test_first_control_interrupt_during_cleanup_wait_escapes(self):
        original_wait = subprocess.Popen.wait
        for error in (M['TerminationRequested'](signal.SIGTERM), KeyboardInterrupt()):
            with self.subTest(error=type(error).__name__):
                count = 0
                process_seen = None

                def wait(process, *args, **kwargs):
                    nonlocal count, process_seen
                    count += 1
                    process_seen = process
                    if count == 2:
                        raise error
                    return original_wait(process, *args, **kwargs)

                with patch.object(subprocess.Popen, 'wait', wait):
                    with self.assertRaises(M['InterruptedLaunch']) as result:
                        M['execute'](self.args.base_build / 'tests/test_crdt_perf_gate',
                                     'crdt', self.root, self.args.cpu, 1)
                self.assertIs(result.exception.cause, error)
                raw = result.exception.raw
                self.assertTrue(raw['interrupted'])
                self.assertEqual(raw['interruption'], type(error).__name__)
                self.assertIn('crdt_perf_gate_single_run', raw['stdout'])
                self.assertTrue(raw['host_before']['time_utc'])
                self.assertTrue(raw['host_after']['time_utc'])
                with self.assertRaises(ProcessLookupError):
                    os.killpg(process_seen.pid, 0)

    def test_first_signal_during_secondary_cleanup_remains_control_flow(self):
        original_wait = subprocess.Popen.wait
        primary = KeyboardInterrupt()
        secondary = M['TerminationRequested'](signal.SIGHUP)
        count = 0

        def wait(process, *args, **kwargs):
            nonlocal count
            count += 1
            if count == 2:
                raise primary
            if count == 3:
                raise secondary
            return original_wait(process, *args, **kwargs)

        with patch.object(subprocess.Popen, 'wait', wait):
            with self.assertRaises(M['InterruptedLaunch']) as result:
                M['execute'](self.args.base_build / 'tests/test_crdt_perf_gate',
                             'crdt', self.root, self.args.cpu, 1)
        self.assertIs(result.exception.primary_cause, primary)
        self.assertIs(result.exception.cause, secondary)
        self.assertEqual(result.exception.raw['primary_interruption'], 'KeyboardInterrupt')
        self.assertEqual(result.exception.raw['signal_number'], signal.SIGHUP)
        self.assertNotIn('cleanup_error', result.exception.raw)

    def test_ordinary_cleanup_wait_failures_are_diagnostics(self):
        for error in (subprocess.TimeoutExpired('probe', 1), OSError('reap failed')):
            with self.subTest(error=type(error).__name__):
                process = Mock(pid=123, returncode=0)
                process.wait.side_effect = error
                process.poll.return_value = 0
                raw = {}
                with patch('os.killpg', side_effect=ProcessLookupError):
                    M['cleanup_process_group'](process, raw)
                self.assertIn(type(error).__name__, raw['cleanup_error'])
                self.assertEqual(raw['exit_code'], 0)

    def test_cli_first_signal_during_cleanup_journals_and_resignals(self):
        wrapper = self.root / 'cleanup-signal-cli.py'
        wrapper.write_text('''import runpy, signal, subprocess, os
module = runpy.run_path(''' + repr(str(Path(__file__).with_name('paired-collector-v1.py'))) + ''')
original = subprocess.Popen.wait
counts = {}
def wait(process, *args, **kwargs):
    if process.args[0] == 'taskset':
        counts[process.pid] = counts.get(process.pid, 0) + 1
        if counts[process.pid] == 2:
            os.kill(os.getpid(), signal.SIGTERM)
    return original(process, *args, **kwargs)
subprocess.Popen.wait = wait
raise SystemExit(module['main']())
''', encoding='utf-8')
        command = [sys.executable, str(wrapper)]
        for key, value in vars(self.args).items():
            command += ['--' + key.replace('_', '-'), str(value)]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
        try:
            stdout, stderr = process.communicate(timeout=10)
            self.assertEqual(process.returncode, -signal.SIGTERM, (stdout, stderr))
            lines = (self.args.out_dir / 'raw-attempts.jsonl').read_text(encoding='utf-8').splitlines()
            self.assertEqual(len(lines), 1)
            raw = json.loads(lines[0])
            self.assertTrue(raw['interrupted'])
            self.assertEqual(raw['signal_number'], signal.SIGTERM)
            self.assertIn('crdt_perf_gate_single_run', raw['stdout'])
            self.assertFalse((self.args.out_dir / 'campaign-v1.json').exists())
            self.assertFalse((self.args.out_dir / 'evaluation-report.json').exists())
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)

    def test_collector_cleanup_keyboard_interrupt_journals_active_launch(self):
        original_wait = subprocess.Popen.wait
        counts = {}

        def wait(process, *args, **kwargs):
            if process.args[0] == 'taskset':
                counts[process.pid] = counts.get(process.pid, 0) + 1
                if counts[process.pid] == 2:
                    raise KeyboardInterrupt
            return original_wait(process, *args, **kwargs)

        with patch.object(subprocess.Popen, 'wait', wait), self.assertRaises(KeyboardInterrupt):
            M['collect'](self.args)
        lines = (self.args.out_dir / 'raw-attempts.jsonl').read_text(encoding='utf-8').splitlines()
        self.assertEqual(len(lines), 1)
        raw = json.loads(lines[0])
        self.assertEqual(raw['interruption'], 'KeyboardInterrupt')
        self.assertIn('crdt_perf_gate_single_run', raw['stdout'])
        self.assertFalse((self.args.out_dir / 'campaign-v1.json').exists())


if __name__ == '__main__':
    unittest.main()
