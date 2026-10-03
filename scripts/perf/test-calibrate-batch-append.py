#!/usr/bin/env python3
"""Calibration parser, host qualification, durability, and no-candidate tests."""
import json
import os
from pathlib import Path
import runpy
import subprocess
import tempfile
import unittest
from unittest.mock import patch

PERF = Path(__file__).parent
PARSER = runpy.run_path(str(PERF / 'batch_append_output_v2.py'))
CAL = runpy.run_path(str(PERF / 'calibrate-batch-append.py'))
EXEC_TESTS = runpy.run_path(str(PERF / 'test-prepare-batch-append-execution.py'))


def stdout_for(case, iterations, append_total_ns):
    dims = PARSER['CASES'][case]
    per_call = append_total_ns / iterations
    fields = PARSER['SUCCESS']
    sample = (
        f"sample\tcontract=wirelog.batch-append-benchmark.v2\tcase={case}\tindex=0"
        f"\titerations={iterations}\tappend_total_ns={append_total_ns}"
        f"\treset_total_ns={iterations}\tappend_ns_per_call={per_call:.3f}"
        f"\treset_ns_per_call=1.000" + ''.join(f'\t{k}={v}' for k, v in fields.items()))
    header = ('bench_batch_append\tcontract=wirelog.batch-append-benchmark.v2'
              '\treset=nrows-only-before-each-call\tinput=disjoint\tgovernor=off'
              '\treserved_capacity=512')
    summary = (f'summary\tcontract=wirelog.batch-append-benchmark.v2\tcase={case}'
               f'\tmedian_append_ns_per_call={per_call:.3f}'
               f'\tmin_append_ns_per_call={per_call:.3f}'
               f'\tmax_append_ns_per_call={per_call:.3f}'
               '\tcov_append_percent=0.000\tmean_reset_ns_per_call=1.000')
    case_line = (f"case\tcontract=wirelog.batch-append-benchmark.v2\tname={case}"
                 f"\tcolumns={dims['columns']}\trows_per_call={dims['rows_per_call']}"
                 f"\tcapacity={dims['capacity']}\titerations={iterations}"
                 '\tsamples=1\twarmups=2' + ''.join(f'\t{k}={v}' for k, v in fields.items()))
    return (header + '\n' + sample + '\n' + summary + '\n' + case_line + '\n').encode()


class FakeProbe:
    def snapshot(self):
        return dict(timestamp_utc=1, kernel='test-kernel', online_cpus=[0, 1],
                    affinity_cpus=[0, 1], selected_cpu=0, governor='performance',
                    frequency_khz=2000000, model='fixture cpu', microcode='0x1',
                    cpu_psi_some_total_usec=100, cgroup_v2_cpu=dict(
                        nr_throttled=2, throttled_usec=100), smt_siblings=[1],
                    sibling_cpu_ticks={'1': dict(total=1000, busy=500)})


class FakeProcess:
    next_outputs = []
    calls = []
    started_flags = []
    tamper_path = None
    tamper_payload = b'tampered while benchmark ran'

    def __init__(self, argv, **kwargs):
        self.argv = list(argv)
        self.kwargs = kwargs
        self.pid = 987654
        self.returncode = None
        self.stdout = b''
        self.stderr = b''
        self.calls.append(self.argv)
        home = Path(kwargs['env']['HOME'])
        self.started_exists_at_spawn = any(home.parent.glob('*-started.json'))
        self.started_flags.append(self.started_exists_at_spawn)
        index = self.argv.index('--case')
        case = self.argv[index + 1]
        iteration_index = self.argv.index('--iterations')
        iterations = int(self.argv[iteration_index + 1])
        total = self.next_outputs.pop(0)
        self.stdout = stdout_for(case, iterations, total)
        if self.tamper_path:
            self.tamper_path.write_bytes(self.tamper_payload)
            self.tamper_path = None

    def communicate(self, timeout=None):
        self.returncode = 0
        return self.stdout, self.stderr

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.returncode = 0
        return 0


class CalibrationTests(unittest.TestCase):
    def setUp(self):
        home_tmp = Path.home() / '.tmp'
        home_tmp.mkdir(mode=0o700, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(dir=home_tmp)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        fixture_class = EXEC_TESTS['ExecutionPreflightTests']
        self.fixture = fixture_class('test_valid_comparison_and_aa_artifacts_are_profile_only')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        os.chmod(self.fixture.paths['base']['build'] / 'bench/bench_batch_append', 0o755)
        self.plan_path = self.fixture.plan_path
        self.overlay = self.fixture.overlay
        self.profile = self.root / 'profile-evidence'
        profile = CAL['PROFILE']['build_artifact'](
            self.plan_path, self.overlay,
            self.fixture.paths['base']['build'], self.fixture.paths['candidate']['build'])
        CAL['PROFILE']['write_atomic'](self.profile, profile)
        self.profile_path = self.profile / 'execution-preflight.json'
        FakeProcess.calls = []
        FakeProcess.started_flags = []
        FakeProcess.tamper_path = None
        FakeProcess.tamper_payload = b'tampered while benchmark ran'

    def evidence_dir(self, suffix):
        return self.root / f'calibration-{suffix}'

    def run_calibration(self, suffix, totals):
        FakeProcess.next_outputs = list(totals)
        return CAL['calibrate'](self.plan_path, self.profile_path, self.overlay,
                                self.evidence_dir(suffix), timeout_seconds=5,
                                probe=FakeProbe(), popen_factory=FakeProcess)

    def test_parser_accepts_exact_success_and_rejects_malformed_contract(self):
        valid = stdout_for('32x256', 10000, 300_000_000)
        parsed = PARSER['parse_output'](valid, '32x256', 10000)
        self.assertEqual(parsed['append_total_ns'], 300_000_000)
        bad_case = valid.replace(b'columns=32', b'columns=31')
        with self.assertRaises(PARSER['OutputError']):
            PARSER['parse_output'](bad_case, '32x256', 10000)
        bad_success = valid.replace(b'denied=0', b'denied=1')
        with self.assertRaises(PARSER['OutputError']):
            PARSER['parse_output'](bad_success, '32x256', 10000)
        with self.assertRaises(PARSER['OutputError']):
            PARSER['parse_output'](valid + b'extra\n', '32x256', 10000)
        with self.assertRaises(PARSER['OutputError']):
            PARSER['parse_output'](valid.replace(b'iterations=10000', b'iterations=9999'),
                                   '32x256', 10000)

    def test_host_telemetry_and_eligibility_thresholds(self):
        root = self.root / 'host-fixture'
        proc, sysfs = root / 'proc', root / 'sys'
        (sysfs / 'devices/system/cpu/cpu0/topology').mkdir(parents=True)
        (sysfs / 'devices/system/cpu/cpu0/cpufreq').mkdir()
        (sysfs / 'devices/system/cpu').mkdir(exist_ok=True)
        (sysfs / 'fs/cgroup/test').mkdir(parents=True)
        (proc / 'self').mkdir(parents=True)
        (proc / 'pressure').mkdir()
        (sysfs / 'devices/system/cpu/online').write_text('0-1\n')
        (sysfs / 'devices/system/cpu/cpu0/topology/thread_siblings_list').write_text('0,1\n')
        (sysfs / 'devices/system/cpu/cpu0/cpufreq/scaling_governor').write_text('performance\n')
        (sysfs / 'devices/system/cpu/cpu0/cpufreq/scaling_cur_freq').write_text('2000000\n')
        (proc / 'cpuinfo').write_text(
            'processor : 0\nmodel name : fixture cpu\nmicrocode : 0x1\n\n'
            'processor : 1\nmodel name : fixture cpu\nmicrocode : 0x1\n')
        (proc / 'self/cgroup').write_text('0::/test\n')
        (sysfs / 'fs/cgroup/test/cpu.stat').write_text(
            'nr_periods 3\nnr_throttled 0\nthrottled_usec 0\n')
        (proc / 'pressure/cpu').write_text('some avg10=0.00 avg60=0.00 avg300=0.00 total=100\n')
        (proc / 'stat').write_text('cpu1 10 0 10 80 0 0 0 0 0 0\n')
        probe = CAL['HostProbe'](proc, sysfs, affinity={0, 1})
        snapshot = probe.snapshot()
        self.assertEqual(snapshot['selected_cpu'], 0)
        self.assertEqual(snapshot['governor'], 'performance')
        self.assertEqual(snapshot['microcode'], '0x1')
        after = dict(snapshot, cpu_psi_some_total_usec=110,
                     sibling_cpu_ticks={1: dict(total=2000, busy=29)})
        after['cgroup_v2_cpu'] = dict(nr_throttled=0, throttled_usec=0)
        good = CAL['host_eligibility'](snapshot, after, 1_000_000_000)
        self.assertTrue(good['eligible'])
        self.assertLessEqual(good['smt_sibling']['busy_percent_by_cpu']['1'], 1.0)
        noisy = dict(after, cpu_psi_some_total_usec=20_000)
        self.assertFalse(CAL['host_eligibility'](snapshot, noisy, 1_000_000_000)['eligible'])
        throttled = dict(after, cgroup_v2_cpu=dict(nr_throttled=1, throttled_usec=1))
        self.assertFalse(CAL['host_eligibility'](snapshot, throttled, 1_000_000_000)['eligible'])

    def test_scaling_retains_attempts_and_publishes_only_after_three_cases(self):
        path, artifact = self.run_calibration('success', [150_000_000, 300_000_000,
                                                         280_000_000, 260_000_000])
        self.assertEqual(artifact['schema'], CAL['ARTIFACT_SCHEMA'])
        self.assertEqual(artifact['candidate_launches'], 0)
        self.assertEqual(artifact['benchmark_launches_performed'], 0)
        self.assertNotIn('performance_verdict', artifact)
        self.assertEqual(len(artifact['attempts']), 4)
        self.assertTrue(all(record['accepted'] for record in (
            artifact['attempts'][1], artifact['attempts'][2], artifact['attempts'][3])))
        first, second = artifact['attempts'][:2]
        self.assertEqual(first['started']['iterations'],
                         self.fixture.plan['benchmark_case_contract']['cases'][0]['default_iterations_start'])
        self.assertEqual(second['started']['iterations'], CAL['next_iterations'](
            first['started']['iterations'], 150_000_000))
        self.assertEqual(len(artifact['accepted_iteration_counts']), 3)
        self.assertTrue(all(call[0] == str(self.evidence_dir('success') /
                             'private-bin/bench_batch_append') for call in FakeProcess.calls))
        self.assertTrue(all('--warmups' in call and '--samples' in call for call in FakeProcess.calls))
        self.assertTrue(all(FakeProcess.started_flags))
        self.assertTrue(all(record['started']['environment']['LC_ALL'] == 'C'
                            and record['started']['cwd'] ==
                            str(self.fixture.paths['base']['build'])
                            and record['process_monotonic_finished_ns'] >=
                            record['process_monotonic_started_ns']
                            for record in artifact['attempts']))
        self.assertTrue(all(record['telemetry']['eligible'] for record in artifact['attempts']))
        self.assertTrue(path.is_file())
        for attempt in artifact['attempts']:
            evidence = path.parent / attempt['stdout_path']
            self.assertEqual(CAL['sha256'](evidence.read_bytes()), attempt['stdout_sha256'])
            stderr = path.parent / attempt['stderr_path']
            self.assertEqual(CAL['sha256'](stderr.read_bytes()), attempt['stderr_sha256'])

    def test_under_threshold_stops_after_six_attempts_without_artifact(self):
        FakeProcess.next_outputs = [100_000_000] * 18
        with self.assertRaisesRegex(CAL['CalibrationError'], 'did not reach'):
            CAL['calibrate'](self.plan_path, self.profile_path, self.overlay,
                            self.evidence_dir('short'), timeout_seconds=5,
                            probe=FakeProbe(), popen_factory=FakeProcess)
        output = self.evidence_dir('short')
        self.assertFalse((output / 'calibration.json').exists())
        self.assertEqual(len(list(output.glob('1x1-attempt-*-result.json'))), 6)

    def test_rehash_detects_binary_drift_and_preserves_diagnostic_without_publish(self):
        FakeProcess.next_outputs = [300_000_000]
        FakeProcess.tamper_path = self.fixture.paths['base']['build'] / 'bench/bench_batch_append'
        with self.assertRaisesRegex(CAL['CalibrationError'], 'original baseline binary changed'):
            CAL['calibrate'](self.plan_path, self.profile_path, self.overlay,
                            self.evidence_dir('drift'), timeout_seconds=5,
                            probe=FakeProbe(), popen_factory=FakeProcess)
        output = self.evidence_dir('drift')
        self.assertFalse((output / 'calibration.json').exists())
        self.assertTrue((output / '1x1-attempt-01-started.json').is_file())
        self.assertTrue((output / '1x1-attempt-01-stdout.bin').is_file())

    def test_plan_and_profile_hash_drift_are_rejected_before_publish(self):
        original_plan = self.plan_path.read_bytes()
        original_profile = self.profile_path.read_bytes()
        for label, target, payload, diagnostic in (
                ('plan', self.plan_path, original_plan + b' ', 'plan artifact changed'),
                ('profile', self.profile_path, original_profile + b' ', 'profile artifact changed')):
            with self.subTest(label=label):
                target.write_bytes(original_plan if label == 'plan' else original_profile)
                FakeProcess.next_outputs = [300_000_000]
                FakeProcess.tamper_path = target
                FakeProcess.tamper_payload = payload
                with self.assertRaisesRegex(CAL['CalibrationError'], diagnostic):
                    CAL['calibrate'](self.plan_path, self.profile_path, self.overlay,
                                    self.evidence_dir(f'{label}-drift'), timeout_seconds=5,
                                    probe=FakeProbe(), popen_factory=FakeProcess)
                self.assertFalse((self.evidence_dir(f'{label}-drift') /
                                  'calibration.json').exists())
                self.plan_path.write_bytes(original_plan)
                self.profile_path.write_bytes(original_profile)
        self.plan_path.write_bytes(original_plan)
        self.profile_path.write_bytes(original_profile)

    def test_next_count_formula_is_bounded_and_monotonic(self):
        self.assertEqual(CAL['next_iterations'](100, 300_000_000), 101)
        self.assertEqual(CAL['next_iterations'](100, 150_000_000), 200)
        self.assertEqual(CAL['next_iterations'](MAX := CAL['MAX_ITERATIONS'], 1), MAX)

    def test_timeout_terminates_and_reaps_the_process_group(self):
        result = CAL['launch_process'](
            ['/bin/sh', '-c', 'sleep 10 & wait'], self.root,
            dict(PATH='/usr/bin:/bin'), timeout_seconds=0.1)
        self.assertTrue(result['timed_out'])
        self.assertIsNotNone(result['exit_code'])
        self.assertLess(result['wall_ns'], 5_000_000_000)


if __name__ == '__main__':
    unittest.main()
