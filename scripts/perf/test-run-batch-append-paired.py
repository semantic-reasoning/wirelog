#!/usr/bin/env python3
"""Paired batch-append runner capture and offline evaluator decision tests."""
import json
from pathlib import Path
import runpy
import subprocess
import tempfile
import unittest

PERF = Path(__file__).parent
RUN = runpy.run_path(str(PERF / 'run-batch-append-paired.py'))
EVAL = runpy.run_path(str(PERF / 'evaluate-batch-append-paired.py'))
CAL_TESTS = runpy.run_path(str(PERF / 'test-calibrate-batch-append.py'))
PLAN = RUN['PLAN']
COUNTS = {'1x1': 1000, '1x256': 100, '32x256': 10}
BASE_NS = {'1x1': 150.0, '1x256': 500.0, '32x256': 6500.0}


class Args:
    def __init__(self, root, **overrides):
        self.base = str(root / 'bin-base')
        self.candidate = str(root / 'bin-candidate')
        self.base_source = str(root / 'src-base')
        self.candidate_source = str(root / 'src-candidate')
        self.base_label = 'main'
        self.candidate_label = 'branch'
        self.calibration = str(root / 'calibration.json')
        self.seed = 'test-seed'
        self.campaign_index = 0
        self.cpu = 0
        self.timeout_seconds = 30
        self.out = str(root / 'evidence')
        self.__dict__.update(overrides)


class FakeLauncher:
    """Return v2 benchmark output; ns/call is timing(side, case, order, pair_index)."""

    def __init__(self, seed, timing, affinity=None, on_call=None):
        self.schedule = PLAN['schedule'](seed)
        self.timing = timing
        self.affinity = affinity
        self.on_call = on_call
        self.calls = []

    def __call__(self, argv, cwd, env, timeout_seconds, selected_cpu=None):
        index = len(self.calls)
        self.calls.append(dict(argv=list(argv), cwd=cwd, env=env, cpu=selected_cpu))
        if self.on_call:
            self.on_call(index, argv)
        launch = self.schedule[index]
        side = Path(argv[0]).name.split('-', 1)[0]
        case = argv[argv.index('--case') + 1]
        iterations = int(argv[argv.index('--iterations') + 1])
        # stdout_for adds a 1 ns/call reset to the append time.
        per_call = self.timing(side, case, launch['pair_order'], launch['pair_index']) - 1.0
        stdout = CAL_TESTS['stdout_for'](case, iterations, round(per_call * iterations))
        return dict(stdout=stdout, stderr=b'', exit_code=0, timed_out=False, wall_ns=10**9,
                    spawn_error=None, interrupted_signal=None,
                    child_affinity_cpus=self.affinity if self.affinity is not None
                    else [selected_cpu])


def same(side, case, order, *_):
    return BASE_NS[case]


class PairedRunTests(unittest.TestCase):
    def setUp(self):
        home_tmp = Path.home() / '.tmp'
        home_tmp.mkdir(mode=0o700, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(dir=home_tmp)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for side in RUN['SIDES']:
            source = self.root / f'src-{side}'
            source.mkdir()
            self.git(source, 'init', '-q')
            (source / 'README').write_text(side, encoding='utf-8')
            self.git(source, 'add', 'README')
            self.git(source, '-c', 'user.name=t', '-c', 'user.email=t@e', 'commit', '-q',
                     '-m', side)
            (self.root / f'bin-{side}').write_bytes(f'binary {side}'.encode())
        self.write_calibration()

    @staticmethod
    def git(source, *args):
        subprocess.run(['git', '-C', str(source), *args], check=True, capture_output=True)

    def write_calibration(self, **overrides):
        value = dict(schema='wirelog.batch-append-calibration.v1', status='calibrated',
                     accepted_iteration_counts=dict(COUNTS))
        value.update(overrides)
        (self.root / 'calibration.json').write_text(json.dumps(value), encoding='utf-8')

    def collect(self, timing=same, name='evidence', launcher=None, **overrides):
        args = Args(self.root, out=str(self.root / name), **overrides)
        launcher = launcher or FakeLauncher(args.seed, timing)
        out, status = RUN['run'](args, probe=CAL_TESTS['FakeProbe'](), launcher=launcher,
                                 home=Path.home())
        return out, status, launcher

    def test_complete_capture_records_frozen_schedule_and_provenance(self):
        seen = []

        def on_call(index, argv):
            if index == 0:
                seen.append((self.root / 'evidence/schedule.json').is_file())
        launcher = FakeLauncher('test-seed', same, on_call=on_call)
        out, status, _ = self.collect(launcher=launcher)
        self.assertEqual(seen, [True])
        self.assertEqual(status['status'], 'complete_capture')
        self.assertEqual(status['benchmark_launches_performed'], 108)
        self.assertEqual(len(launcher.calls), 108)
        manifest = json.loads((out / 'manifest.json').read_text(encoding='utf-8'))
        for side in RUN['SIDES']:
            source = self.root / f'src-{side}'
            head = subprocess.run(['git', '-C', str(source), 'rev-parse', 'HEAD', 'HEAD^{tree}'],
                                  capture_output=True, text=True, encoding='utf-8',
                                  check=True).stdout.split()
            self.assertEqual([manifest['binaries'][side]['commit'],
                              manifest['binaries'][side]['tree']], head)
        schedule = json.loads((out / 'schedule.json').read_text(encoding='utf-8'))['commands']
        for call, row in zip(launcher.calls, schedule):
            self.assertEqual(call['argv'], row['argv'])
            self.assertEqual(call['cpu'], 0)
            self.assertTrue(call['argv'][0].startswith(str(out)))
            self.assertEqual(row['iterations'], COUNTS[row['case']])
        records = (out / 'journal.jsonl').read_text(encoding='utf-8').splitlines()
        self.assertEqual(len(records), 216)
        self.assertTrue(all(json.loads(r).get('host_eligibility', {'eligible': True})['eligible']
                            for r in records))

    def test_rejects_evidence_outside_home_or_inside_sources_or_existing(self):
        for out, message in ((Path('/tmp/wirelog-paired'), '/tmp'),
                             (self.root / 'src-base/evidence', 'outside source'),
                             (self.root, 'already exists')):
            with self.assertRaisesRegex(RUN['RunError'], message):
                RUN['run'](Args(self.root, out=str(out)), probe=CAL_TESTS['FakeProbe'](),
                           launcher=FakeLauncher('test-seed', same), home=Path.home())
        with self.assertRaisesRegex(RUN['RunError'], 'under HOME'):
            RUN['run'](Args(self.root), probe=CAL_TESTS['FakeProbe'](),
                       launcher=FakeLauncher('test-seed', same), home=self.root / 'src-base')

    def test_rejects_dirty_source_and_invalid_calibration(self):
        (self.root / 'src-candidate/untracked').write_text('x', encoding='utf-8')
        with self.assertRaisesRegex(RUN['RunError'], 'untracked'):
            self.collect()
        (self.root / 'src-candidate/untracked').unlink()
        self.write_calibration(status='attempted')
        with self.assertRaisesRegex(RUN['RunError'], 'calibrated'):
            self.collect()
        self.write_calibration(accepted_iteration_counts={'1x1': 5, '1x256': 5})
        with self.assertRaisesRegex(RUN['RunError'], 'iteration counts'):
            self.collect()
        self.assertFalse((self.root / 'evidence').exists())

    def test_binary_drift_aborts_without_launching(self):
        def tamper(index, argv):
            if index == 3:
                (self.root / 'evidence/candidate-bench_batch_append').write_bytes(b'changed')
        launcher = FakeLauncher('test-seed', same, on_call=tamper)
        with self.assertRaisesRegex(RUN['RunError'], 'binary drift'):
            self.collect(launcher=launcher)
        status = json.loads((self.root / 'evidence/status.json').read_text(encoding='utf-8'))
        self.assertEqual(status['status'], 'aborted')
        self.assertLess(len(launcher.calls), 108)

    def test_child_affinity_mismatch_is_recorded_ineligible(self):
        launcher = FakeLauncher('test-seed', same, affinity=[0, 1])
        out, status, _ = self.collect(launcher=launcher)
        self.assertEqual(status['status'], 'complete_capture')
        report = EVAL['analyze'](out)
        self.assertEqual(len(report['excluded_pairs']), 54)
        self.assertEqual(EVAL['qualify_aa'](report)['1x1']['qualified'], False)

    def aa(self, timing=same):
        args = dict(candidate=str(self.root / 'bin-base'))
        out, _, _ = self.collect(timing, name='aa', **args)
        return out

    def test_qualified_aa_lets_comparison_meet_thresholds(self):
        aa = EVAL['analyze'](self.aa())
        self.assertTrue(all(v['qualified'] for v in EVAL['qualify_aa'](aa).values()))
        out, _, _ = self.collect(
            lambda side, case, order, *_: BASE_NS[case] * (0.75 if side == 'candidate' else 1))
        decisions = EVAL['decide'](EVAL['analyze'](out), aa)
        self.assertEqual({k: v['outcome'] for k, v in decisions.items()},
                         dict.fromkeys(PLAN['CASES'], 'meets'))
        without = EVAL['decide'](EVAL['analyze'](out), None)
        self.assertEqual({v['outcome'] for v in without.values()}, {'diagnostic'})

    def test_unqualified_aa_case_makes_only_that_comparison_case_diagnostic(self):
        def noisy(side, case, order, *_):
            bump = 1.03 if case == '32x256' and side == 'candidate' and order == 'BA' else 1
            return BASE_NS[case] * bump
        aa = EVAL['analyze'](self.aa(noisy))
        qualification = EVAL['qualify_aa'](aa)
        self.assertFalse(qualification['32x256']['qualified'])
        self.assertTrue(qualification['1x256']['qualified'])
        out, _, _ = self.collect(
            lambda side, case, order, *_: BASE_NS[case] * (0.75 if side == 'candidate' else 1))
        decisions = EVAL['decide'](EVAL['analyze'](out), aa)
        self.assertEqual(decisions['32x256']['outcome'], 'diagnostic')
        self.assertEqual(decisions['1x256']['outcome'], 'meets')

    def test_aa_deviation_and_order_effect_limits_apply_separately(self):
        def deviation_only(side, case, order, *_):
            return BASE_NS[case] * (1.025 if case == '1x256' and side == 'candidate' else 1)

        def order_only(side, case, order, *_):
            bump = {'AB': 0.985, 'BA': 1.015}[order]
            return BASE_NS[case] * (bump if case == '1x1' and side == 'candidate' else 1)
        qualification = EVAL['qualify_aa'](EVAL['analyze'](self.aa(deviation_only)))
        self.assertEqual(qualification['1x256']['reasons'],
                         ['AB median |ratio-1| 2.500% > 2%', 'BA median |ratio-1| 2.500% > 2%'])
        self.assertTrue(qualification['1x1']['qualified'])
        out, _, _ = self.collect(order_only, name='aa-order',
                                 candidate=str(self.root / 'bin-base'))
        qualification = EVAL['qualify_aa'](EVAL['analyze'](out))
        self.assertEqual(len(qualification['1x1']['reasons']), 1)
        self.assertIn('order effect 3.000 pp', qualification['1x1']['reasons'][0])

    def test_thresholds_reject_small_gain_and_large_batch_regression(self):
        aa = EVAL['analyze'](self.aa())

        def timing(side, case, order, *_):
            factor = {'1x1': 0.95, '1x256': 1.0, '32x256': 1.05}[case]
            return BASE_NS[case] * (factor if side == 'candidate' else 1)
        out, _, _ = self.collect(timing)
        decisions = EVAL['decide'](EVAL['analyze'](out), aa)
        self.assertEqual(decisions['1x1']['outcome'], 'does_not_meet')
        self.assertFalse(decisions['1x1']['checks']['improvement_ge_10pct'])
        self.assertEqual(decisions['1x256']['outcome'], 'meets')
        self.assertEqual(decisions['32x256']['outcome'], 'does_not_meet')

    def test_single_row_gain_needs_interval_excluding_no_improvement(self):
        aa = EVAL['analyze'](self.aa())

        def mixed(side, case, order, pair_index):
            slow = case == '1x1' and side == 'candidate' and pair_index >= 5
            fast = side == 'candidate' and not slow
            return BASE_NS[case] * (1.2 if slow else 0.7 if fast else 1)
        out, _, _ = self.collect(mixed)
        decision = EVAL['decide'](EVAL['analyze'](out), aa)['1x1']
        self.assertTrue(decision['checks']['improvement_ge_10pct'])
        self.assertTrue(decision['checks']['savings_ge_25ns'])
        self.assertFalse(decision['checks']['ci_excludes_no_improvement'])
        self.assertEqual(decision['outcome'], 'does_not_meet')

    def test_aa_of_another_binary_does_not_qualify_comparison(self):
        out, _, _ = self.collect(name='aa', base=str(self.root / 'bin-candidate'))
        aa = EVAL['analyze'](out)
        self.assertTrue(all(v['qualified'] for v in EVAL['qualify_aa'](aa).values()))
        cmp_out, _, _ = self.collect()
        decisions = EVAL['decide'](EVAL['analyze'](cmp_out), aa)
        self.assertEqual({v['outcome'] for v in decisions.values()}, {'diagnostic'})
        self.assertEqual(decisions['1x1']['aa']['reasons'],
                         ['A/A binary is not the comparison base'])
        out, _, _ = self.collect(name='mixed')
        self.assertFalse(any(v['qualified'] for v in EVAL['qualify_aa'](
            EVAL['analyze'](out)).values()))

    def test_evaluator_rejects_schedule_or_binary_tampering(self):
        out, _, _ = self.collect()
        journal = out / 'journal.jsonl'
        original = journal.read_text(encoding='utf-8')
        for field, value, message in (('pair_order', 'XX', 'frozen schedule'),
                                      ('executable_sha256', '0' * 64, 'executable hash')):
            lines = original.splitlines()
            record = json.loads(lines[0])
            record['command_row'][field] = value
            lines[0] = json.dumps(record)
            journal.write_text('\n'.join(lines) + '\n', encoding='utf-8')
            with self.assertRaisesRegex(EVAL['EvaluationError'], message):
                EVAL['analyze'](out)
        journal.write_text(original + original.splitlines()[0] + '\n', encoding='utf-8')
        with self.assertRaisesRegex(EVAL['EvaluationError'], 'duplicate'):
            EVAL['analyze'](out)


if __name__ == '__main__':
    unittest.main()
