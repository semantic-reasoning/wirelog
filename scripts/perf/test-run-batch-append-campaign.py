#!/usr/bin/env python3
"""Runner journal, strict output, signal, and process-group cleanup tests."""
import json
import os
from pathlib import Path
import runpy
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

PERF = Path(__file__).parent
RUNNER = runpy.run_path(str(PERF / 'collect-batch-append-campaign.py'))
BASE = runpy.run_path(str(PERF / 'test-collect-batch-append-campaign.py'))
CAL_TEST = runpy.run_path(str(PERF / 'test-calibrate-batch-append.py'))


class CpuProbe:
    def __init__(self, proc, tick=0):
        self.proc = proc
        self.tick = tick
        self.calls = 0

    def snapshot(self):
        self.calls += 1
        value = CAL_TEST['FakeProbe']().snapshot()
        value['cpu_psi_some_total_usec'] += 200_000 * self.calls
        value['cgroup_v2_cpu']['nr_throttled'] += self.calls
        value['cgroup_v2_cpu']['throttled_usec'] += 5 * self.calls
        value['sibling_cpu_ticks'] = {'1': {'total': 1000 + self.tick,
                                             'busy': 500 + self.tick}}
        return value


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.fixture = BASE['CollectionTests'](
            'test_comparison_admits_216_rows_with_empty_durable_journal_and_no_launch')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.runner = RUNNER
        self.fallback_patchers = []
        freezer = self.runner['FREEZER']
        maps = (freezer['PLAN']['FALLBACKS'], freezer['PROFILE']['FALLBACKS'],
                freezer['CAL']['PROFILE']['PLAN']['FALLBACKS'],
                freezer['CAL']['PROFILE']['FALLBACKS'])
        for api in maps:
            def fake_validate(root, module=api):
                module['outer_status'](root)
                return {'schema': module['SCHEMA'],
                        'outer_ignored_paths': list(module['IGNORED_ROOTS']),
                        'nanoarrow': {'commit': module['NANO_COMMIT'],
                                      'tree': module['NANO_TREE'],
                                      'manifest_sha256': 'a' * 64},
                        'xxhash': {'archive_sha256': module['XX_ARCHIVE_SHA256'],
                                   'manifest_sha256': 'b' * 64}}
            patcher = patch.dict(api, validate_fallbacks=fake_validate)
            patcher.start()
            self.fallback_patchers.append(patcher)
            self.addCleanup(patcher.stop)
        self.freeze_root = self.fixture.freeze_path.parent
        self.output = self.fixture.output
        self.args = self.fixture.args
        (self.fixture.root / 'loadavg').write_text('0.10 0.20 0.30 1/2 3\n', encoding='ascii')
        (self.fixture.root / 'self').mkdir(exist_ok=True)
        (self.fixture.root / 'self/cgroup').write_text('0::/fixture\n', encoding='ascii')
        self.collection, self.preflight = self.runner['collect'](
            self.args, host_probe=CAL_TEST['FakeProbe']())
        self.original_admit = self.runner['admit']

    def _run_with_fake_processes(self):
        calls = []
        started_flags = []
        passed_rows = []

        class Process:
            def __init__(inner, argv, **kwargs):
                inner.argv = argv
                inner.kwargs = kwargs
                inner.pid = 123456
                inner.returncode = None
                calls.append(list(argv))
                passed_rows.append((list(argv), kwargs['cwd'], kwargs['env']))
                journal = self.output / 'journal.jsonl'
                inner.started_visible = bool(journal.exists() and journal.stat().st_size
                                              and json.loads(journal.read_text(
                                                  encoding='utf-8').splitlines()[-1])['event'] == 'started')
                started_flags.append(inner.started_visible)
                case = argv[argv.index('--case') + 1]
                iterations = int(argv[argv.index('--iterations') + 1])
                inner.stdout = CAL_TEST['stdout_for'](case, iterations, 300_000_000)
                inner.stderr = b''

            def communicate(inner, timeout=None):
                inner.returncode = 0
                return inner.stdout, inner.stderr

            def poll(inner):
                return inner.returncode

        def probe_factory(cpu):
            return CpuProbe(self.fixture.root)

        # Full admission ran before this test. Keep every subsequent pre/post
        # check on the same immutable admitted artifact while exercising the
        # runner's repeated call sites and journal transitions.
        def stable_admit(args, expected_host=None, host_probe=None):
            return self.collection, self.preflight

        with patch.dict(self.runner['run_campaign'].__globals__, admit=stable_admit), \
                patch.object(self.runner['os'], 'fsync', return_value=None):
            result_path, status = self.runner['run_campaign'](
                self.args, popen_factory=Process,
                affinity_getter=lambda pid: {0}, probe_factory=probe_factory,
                host_probe=CAL_TEST['FakeProbe']())
        return result_path, status, calls, started_flags, passed_rows

    def test_runs_exact_frozen_schedule_and_durably_records_every_row(self):
        result_path, status, calls, started_flags, passed_rows = self._run_with_fake_processes()
        count = self.preflight['planned_command_count']
        self.assertEqual(status['status'], 'complete_capture')
        self.assertEqual(status['benchmark_launches_performed'], count)
        self.assertEqual(status['planned_command_count'], count)
        self.assertEqual(status['eligibility_summary'], dict(eligible=0, ineligible=count))
        self.assertEqual(len(calls), count)
        self.assertEqual(len(started_flags), count)
        self.assertTrue(all(started_flags))
        events = [json.loads(line) for line in (self.output / 'journal.jsonl').read_text(
            encoding='utf-8').splitlines()]
        self.assertEqual(len(events), count * 2)
        self.assertTrue(all(events[index * 2]['event'] == 'started'
                            and events[index * 2 + 1]['event'] == 'result'
                            for index in range(count)))
        self.assertEqual([event['command_index'] for event in events[::2]], list(range(count)))
        first = events[0]
        result = events[1]
        self.assertEqual(first['command_row']['argv'], calls[0])
        self.assertEqual(passed_rows[0][0], first['command_row']['argv'])
        self.assertEqual(passed_rows[0][1], first['command_row']['cwd'])
        self.assertEqual(passed_rows[0][2], first['command_row']['environment'])
        self.assertEqual(first['timeout_seconds'], 300)
        self.assertIn('load_average', result['host_before'])
        self.assertIn('cgroup_identity', result['host_after'])
        self.assertTrue(any('SMT sibling telemetry unavailable' in item
                            for item in result['host_eligibility']['diagnostics']))
        self.assertEqual(result['stdout_sha256'], RUNNER['digest'](
            (self.output / result['stdout_path']).read_bytes()))
        self.assertEqual(result_path, self.output / 'status.json')
        self.assertFalse(RUNNER['FREEZER']['contains_verdict_key'](status))

    def test_runner_keeps_default_mode_as_admission_only(self):
        common = [
            '--mode', 'comparison', '--overlay-patch', str(self.fixture.overlay),
            '--freeze-artifact', str(self.fixture.freeze_path), '--output-dir', str(self.output),
            '--plan-a', str(self.fixture.fixture.plan_a_path),
            '--profile-a', str(self.fixture.fixture.profile_a_path),
            '--calibration-a', str(self.fixture.fixture.cal_a),
            '--plan-b', str(self.fixture.fixture.plan_b_path),
            '--profile-b', str(self.fixture.fixture.profile_b_path)]
        self.assertFalse(self.runner['parser']().parse_args(common).run)
        self.assertTrue(self.runner['parser']().parse_args(common + ['--run']).run)
        selected = []
        stub_artifact = dict(planned_command_count=216)

        def admission(args):
            selected.append('admission')
            return self.output, stub_artifact

        def execution(args):
            selected.append('execution')
            self.fail('default CLI must not launch commands')

        with patch.dict(self.runner['main'].__globals__, collect=admission,
                        run_campaign=execution):
            self.assertEqual(self.runner['main'](common), 0)
        self.assertEqual(selected, ['admission'])

    def test_journal_append_fsyncs_each_record(self):
        path = self.fixture.root / 'journal-test.jsonl'
        path.write_bytes(b'')
        path.chmod(0o600)
        with patch.object(self.runner['os'], 'fsync', wraps=os.fsync) as sync:
            self.runner['append_journal'](path, dict(event='started', command_index=0))
        self.assertEqual(sync.call_count, 1)
        self.assertEqual(json.loads(path.read_text(encoding='utf-8'))['event'], 'started')

    def test_concurrent_or_retried_run_lock_is_refused_before_spawn(self):
        RUNNER['write_durable'](self.output / RUNNER['RUN_LOCK'], b'{"pid":1}\n')
        with self.assertRaisesRegex(RUNNER['RunnerError'], 'lock already exists'):
            self.runner['run_campaign'](
                self.args, popen_factory=lambda *a, **k: self.fail('launched'),
                host_probe=CAL_TEST['FakeProbe']())

    def test_signal_persists_process_result_and_incomplete_status(self):
        freezer = self.runner['FREEZER']
        case = self.preflight['frozen_preflight']['commands'][0]['case']
        iterations = self.preflight['frozen_preflight']['commands'][0]['iterations']

        class InterruptedProcess:
            def __init__(inner, argv, **kwargs):
                inner.pid = 123457
                inner.returncode = -15
                inner.stdout = CAL_TEST['stdout_for'](case, iterations, 300_000_000)
                inner.stderr = b'interrupted'
                signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)

            def communicate(inner, timeout=None):
                return inner.stdout, inner.stderr

            def poll(inner):
                return inner.returncode

        def stable_admit(args, expected_host=None, host_probe=None):
            return self.collection, self.preflight

        def mark_interrupted(signum, _frame):
            freezer['CAL']['_INTERRUPTED_SIGNAL'] = signum

        with patch.dict(self.runner['run_campaign'].__globals__, admit=stable_admit), \
                patch.object(self.runner['os'], 'fsync', return_value=None), \
                patch.dict(freezer['CAL'], terminate_active_process=mark_interrupted,
                           terminate_group=lambda process: (process.stdout, process.stderr)):
            with self.assertRaises(RUNNER['RunnerError']):
                self.runner['run_campaign'](
                    self.args, popen_factory=InterruptedProcess,
                    affinity_getter=lambda pid: {0},
                    probe_factory=lambda cpu: CpuProbe(self.fixture.root),
                    host_probe=CAL_TEST['FakeProbe']())
        status = json.loads((self.output / 'status.json').read_text(encoding='utf-8'))
        events = [json.loads(line) for line in (self.output / 'journal.jsonl').read_text(
            encoding='utf-8').splitlines()]
        self.assertEqual(status['status'], 'incomplete_capture')
        self.assertEqual(status['failure']['category'], 'interrupted')
        self.assertEqual([event['event'] for event in events], ['started', 'result'])
        self.assertTrue((self.output / events[1]['stdout_path']).is_file())

    def test_process_failure_aborts_at_fixed_index_and_keeps_raw_result(self):
        calls = []
        row = self.preflight['frozen_preflight']['commands'][0]

        class FailedProcess:
            def __init__(inner, argv, **kwargs):
                calls.append(argv)
                inner.pid = 123458
                inner.returncode = 7
                inner.stdout = CAL_TEST['stdout_for'](
                    row['case'], row['iterations'], 300_000_000)
                inner.stderr = b'failed deliberately'

            def communicate(inner, timeout=None):
                return inner.stdout, inner.stderr

            def poll(inner):
                return inner.returncode

        def stable_admit(args, expected_host=None, host_probe=None):
            return self.collection, self.preflight

        with patch.dict(self.runner['run_campaign'].__globals__, admit=stable_admit), \
                patch.object(self.runner['os'], 'fsync', return_value=None):
            with self.assertRaisesRegex(RUNNER['RunnerError'], 'benchmark exited'):
                self.runner['run_campaign'](
                    self.args, popen_factory=FailedProcess,
                    affinity_getter=lambda pid: {0},
                    probe_factory=lambda cpu: CpuProbe(self.fixture.root),
                    host_probe=CAL_TEST['FakeProbe']())
        self.assertEqual(len(calls), 1)
        status = json.loads((self.output / 'status.json').read_text(encoding='utf-8'))
        events = [json.loads(line) for line in (self.output / 'journal.jsonl').read_text(
            encoding='utf-8').splitlines()]
        self.assertEqual(status['status'], 'incomplete_capture')
        self.assertEqual(status['failure']['category'], 'process')
        self.assertEqual(status['failure']['command_index'], 0)
        self.assertEqual([event['event'] for event in events], ['started', 'result'])
        self.assertTrue((self.output / events[1]['stderr_path']).is_file())

    def test_provenance_drift_after_row_zero_aborts_before_row_one(self):
        calls = []
        row = self.preflight['frozen_preflight']['commands'][0]

        class Process:
            def __init__(inner, argv, **kwargs):
                calls.append(argv)
                inner.pid = 123459
                inner.returncode = 0
                inner.stdout = CAL_TEST['stdout_for'](
                    row['case'], row['iterations'], 300_000_000)
                inner.stderr = b''

            def communicate(inner, timeout=None):
                return inner.stdout, inner.stderr

            def poll(inner):
                return inner.returncode

        admission_calls = [0]

        def drifted_admit(args, expected_host=None, host_probe=None):
            admission_calls[0] += 1
            if admission_calls[0] == 4:
                raise RUNNER['CollectionError']('binary hash changed after command zero')
            return self.collection, self.preflight

        with patch.dict(self.runner['run_campaign'].__globals__, admit=drifted_admit), \
                patch.object(self.runner['os'], 'fsync', return_value=None):
            with self.assertRaisesRegex(RUNNER['RunnerError'], 'binary hash changed'):
                self.runner['run_campaign'](
                    self.args, popen_factory=Process, affinity_getter=lambda pid: {0},
                    probe_factory=lambda cpu: CpuProbe(self.fixture.root),
                    host_probe=CAL_TEST['FakeProbe']())
        self.assertEqual(len(calls), 1)
        status = json.loads((self.output / 'status.json').read_text(encoding='utf-8'))
        events = [json.loads(line) for line in (self.output / 'journal.jsonl').read_text(
            encoding='utf-8').splitlines()]
        self.assertEqual(status['status'], 'incomplete_capture')
        self.assertEqual(status['failure']['category'], 'provenance')
        self.assertEqual(status['failure']['command_index'], 0)
        self.assertEqual([event['event'] for event in events], ['started', 'result'])

    def test_host_identity_drift_is_fatal_after_recording_row(self):
        row = self.preflight['frozen_preflight']['commands'][0]
        calls = [0]

        class Process:
            def __init__(inner, argv, **kwargs):
                inner.pid = 123460
                inner.returncode = 0
                inner.stdout = CAL_TEST['stdout_for'](
                    row['case'], row['iterations'], 300_000_000)
                inner.stderr = b''

            def communicate(inner, timeout=None):
                return inner.stdout, inner.stderr

            def poll(inner):
                return inner.returncode

        class DriftingProbe(CpuProbe):
            def snapshot(inner):
                value = super(DriftingProbe, inner).snapshot()
                calls[0] += 1
                if calls[0] == 2:
                    (inner.proc / 'self/cgroup').write_text('0::/changed\n', encoding='ascii')
                return value

        def stable_admit(args, expected_host=None, host_probe=None):
            return self.collection, self.preflight

        with patch.dict(self.runner['run_campaign'].__globals__, admit=stable_admit), \
                patch.object(self.runner['os'], 'fsync', return_value=None):
            with self.assertRaisesRegex(RUNNER['RunnerError'], 'host_identity failure'):
                self.runner['run_campaign'](
                    self.args, popen_factory=Process, affinity_getter=lambda pid: {0},
                    probe_factory=lambda cpu: DriftingProbe(self.fixture.root),
                    host_probe=CAL_TEST['FakeProbe']())
        status = json.loads((self.output / 'status.json').read_text(encoding='utf-8'))
        self.assertEqual(status['failure']['category'], 'host_identity')

    def test_aa_control_executes_exactly_108_frozen_rows_in_order(self):
        aa_suite = BASE['CollectionTests'](
            'test_aa_pre_admits_exactly_108_rows_with_origin_calibration')
        aa_suite.setUp()
        self.addCleanup(aa_suite.doCleanups)
        aa_suite.test_aa_pre_admits_exactly_108_rows_with_origin_calibration()
        self.args = aa_suite.args
        self.args.output_dir = Path(self.args.freeze_artifact).parent / 'runner-collection'
        self.output = self.args.output_dir
        self.collection, self.preflight = self.runner['collect'](
            self.args, host_probe=CAL_TEST['FakeProbe']())
        _, status, calls, _, passed_rows = self._run_with_fake_processes()
        rows = self.preflight['frozen_preflight']['commands']
        self.assertEqual(status['status'], 'complete_capture')
        self.assertEqual(len(rows), 108)
        self.assertEqual(calls, [row['argv'] for row in rows])
        self.assertEqual([record[1] for record in passed_rows], [row['cwd'] for row in rows])
        self.assertEqual([record[2] for record in passed_rows],
                         [row['environment'] for row in rows])
        starts = [json.loads(line) for line in (self.output / 'journal.jsonl').read_text(
            encoding='utf-8').splitlines()[::2]]
        self.assertEqual([row['command_index'] for row in starts], list(range(108)))


class ProcessGroupTests(unittest.TestCase):
    def test_timeout_kills_descendant_that_keeps_pipes_open(self):
        home_tmp = Path.home() / '.tmp'
        home_tmp.mkdir(mode=0o700, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=home_tmp) as directory:
            pidfile = Path(directory) / 'child.pid'
            code = ('import subprocess,sys,time; '
                    'p=subprocess.Popen([sys.executable,"-c","import time; time.sleep(60)"]); '
                    'open(sys.argv[1],"w").write(str(p.pid)); time.sleep(60)')
            environment = os.environ.copy()
            environment['HOME'] = directory
            environment['TMPDIR'] = directory
            result = RUNNER['FREEZER']['CAL']['launch_process'](
                [sys.executable, '-c', code, str(pidfile)], directory, environment,
                0.15, selected_cpu=None)
            self.assertTrue(result['timed_out'])
            self.assertTrue(pidfile.exists())
            child_pid = int(pidfile.read_text(encoding='ascii'))
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                try:
                    stat_text = Path(f'/proc/{child_pid}/stat').read_text(encoding='ascii')
                    if stat_text.split(') ', 1)[1].startswith('Z'):
                        break
                except (FileNotFoundError, ProcessLookupError):
                    break
                time.sleep(0.02)
            else:
                self.fail('descendant remained alive after process-group cleanup')


if __name__ == '__main__':
    unittest.main()
