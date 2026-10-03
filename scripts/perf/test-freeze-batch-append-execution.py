#!/usr/bin/env python3
"""No-launch command-freeze provenance, calibration, and tamper tests."""
import json
import copy
import os
from pathlib import Path
import runpy
import tempfile
import unittest

PERF = Path(__file__).parent
FREEZER = runpy.run_path(str(PERF / 'freeze-batch-append-execution.py'))
PLAN = FREEZER['PLAN']
PROFILE = FREEZER['PROFILE']
CAL = FREEZER['CAL']
EXEC_TESTS = runpy.run_path(str(PERF / 'test-prepare-batch-append-execution.py'))
CAL_TESTS = runpy.run_path(str(PERF / 'test-calibrate-batch-append.py'))
FakeProcess = CAL_TESTS['FakeProcess']
FakeProbe = CAL_TESTS['FakeProbe']


class CommandFreezeTests(unittest.TestCase):
    def setUp(self):
        home_tmp = Path.home() / '.tmp'
        home_tmp.mkdir(mode=0o700, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(dir=home_tmp)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.fixture = EXEC_TESTS['ExecutionPreflightTests'](
            'test_valid_comparison_and_aa_artifacts_are_profile_only')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        for side in ('base', 'candidate'):
            os.chmod(self.fixture.paths[side]['build'] / 'bench/bench_batch_append', 0o755)
        self.overlay = self.fixture.overlay
        self.plan_a_path = self.root / 'plan-a.json'
        self.plan_a_path.write_text(json.dumps(self.fixture.plan, indent=2), encoding='utf-8')
        plan_b = self.make_plan('different-schedule-seed')
        self.plan_b_path = self.root / 'plan-b.json'
        self.plan_b_path.write_text(json.dumps(plan_b, indent=2), encoding='utf-8')
        self.plan_b_bytes = self.plan_b_path.read_bytes()
        self.profile_a_path, self.profile_a = self.make_profile(self.plan_a_path, 'profile-a')
        self.profile_b_path, self.profile_b = self.make_profile(self.plan_b_path, 'profile-b')
        self.cal_a = self.make_calibration(self.plan_a_path, self.profile_a_path, 'cal-a',
                                           [300_000_000] * 3)
        self.fake_call_count = len(FakeProcess.calls)

    def make_plan(self, seed, mode='comparison', product='pre'):
        args = type('Args', (), dict(
            mode=mode, aa_product=(product if mode == 'aa_control' else None), seed=seed,
            base_source=self.fixture.paths['base']['source'],
            candidate_source=self.fixture.paths['candidate']['source'],
            overlay_patch=self.overlay))()
        return PLAN['build_plan'](args)

    def make_profile(self, plan_path, name):
        artifact = PROFILE['build_artifact'](
            plan_path, self.overlay, self.fixture.paths['base']['build'],
            self.fixture.paths['candidate']['build'])
        output = self.root / name
        PROFILE['write_atomic'](output, artifact)
        return output / 'execution-preflight.json', artifact

    def make_calibration(self, plan_path, profile_path, name, totals):
        FakeProcess.next_outputs = list(totals)
        evidence = self.root / name
        CAL['calibrate'](plan_path, profile_path, self.overlay, evidence,
                         timeout_seconds=5, probe=FakeProbe(),
                         popen_factory=FakeProcess, affinity_getter=lambda _pid: {0})
        return evidence

    def freeze_comparison(self, output_name='frozen'):
        args = type('Args', (), dict(
            mode='comparison', overlay_patch=self.overlay,
            plan_a=self.plan_a_path, profile_a=self.profile_a_path,
            calibration_a=self.cal_a, plan_b=self.plan_b_path,
            profile_b=self.profile_b_path,
            output_dir=self.root / output_name))()
        return FREEZER['freeze'](args)

    def test_comparison_freezes_216_adjacent_rows_without_launches(self):
        before = len(FakeProcess.calls)
        path, artifact = self.freeze_comparison()
        self.assertEqual(before, len(FakeProcess.calls))
        self.assertEqual(artifact['schema'], FREEZER['SCHEMA'])
        self.assertEqual(artifact['planned_command_count'], 216)
        self.assertEqual(artifact['benchmark_launches_performed'], 0)
        self.assertNotIn('performance_verdict', artifact)
        self.assertFalse(FREEZER['contains_verdict_key'](artifact))
        self.assertTrue(path.is_file())
        self.assertEqual([row['command_index'] for row in artifact['commands']],
                         list(range(216)))
        self.assertEqual([sum(row['campaign_index'] == index for row in artifact['commands'])
                          for index in (0, 1)], [108, 108])
        for campaign in (0, 1):
            rows = [row for row in artifact['commands'] if row['campaign_index'] == campaign]
            self.assertEqual(len(rows), 108)
            for offset in range(0, len(rows), 2):
                first, second = rows[offset:offset + 2]
                self.assertEqual(first['pair_id'], second['pair_id'])
                expected_sides = ['base', 'candidate'] if first['pair_order'] == 'AB' \
                    else ['candidate', 'base']
                self.assertEqual([first['side'], second['side']], expected_sides)
                self.assertEqual(first['case'], second['case'])
                self.assertEqual(first['iterations'], second['iterations'])
                self.assertEqual(first['argv'][1:], second['argv'][1:])
                self.assertTrue(Path(first['executable']).is_absolute())
                self.assertEqual(first['executable_sha256'],
                                 FREEZER['sha256'](Path(first['executable']).read_bytes()))
                self.assertEqual(first['environment']['LC_ALL'], 'C')
                self.assertIn('cpu', first)
            balance = {}
            for offset in range(0, len(rows), 2):
                row = rows[offset]
                balance.setdefault((row['case'], row['pair_order']), 0)
                balance[(row['case'], row['pair_order'])] += 1
            self.assertEqual(set(balance.values()), {9})
        self.assertEqual(artifact['plans'][0]['accepted_iteration_counts'],
                         artifact['plans'][1]['accepted_iteration_counts'])

    def test_rejects_same_seed_and_tampered_profile(self):
        changed = json.loads(self.plan_a_path.read_text(encoding='utf-8'))
        changed['seed'] = self.fixture.plan['seed']
        changed['schedule']['launches'] = PLAN['schedule'](changed['seed'])
        with self.assertRaisesRegex(FREEZER['FreezeError'], 'distinct schedule seeds'):
            FREEZER['plan_semantic_copy'](self.fixture.plan, changed)

        changed_profile = json.loads(self.profile_b_path.read_text(encoding='utf-8'))
        changed_profile['candidate']['binary_sha256'] = '0' * 64
        self.profile_b_path.write_text(json.dumps(changed_profile), encoding='utf-8')
        with self.assertRaisesRegex(FREEZER['FreezeError'], 'profile artifact does not match'):
            self.freeze_comparison('tampered-profile')

    def test_rejects_raw_output_or_unlisted_calibration_evidence_tamper(self):
        stdout = self.cal_a / '1x1-attempt-01-stdout.bin'
        raw = stdout.read_bytes()
        stdout.write_bytes(raw + b'extra\n')
        with self.assertRaisesRegex(FREEZER['FreezeError'], 'raw output hash/path'):
            self.freeze_comparison('bad-stdout')
        stdout.write_bytes(raw)
        (self.cal_a / 'unlisted.tmp').write_bytes(b'extra')
        with self.assertRaisesRegex(FREEZER['FreezeError'], 'missing or extra files'):
            self.freeze_comparison('extra-file')

    def test_recomputes_and_rejects_ineligible_calibration_telemetry(self):
        calibration_path = self.cal_a / 'calibration.json'
        artifact = json.loads(calibration_path.read_text(encoding='utf-8'))
        attempt = artifact['attempts'][0]
        attempt['host_after']['cpu_psi_some_total_usec'] += 1_000_000
        result_path = self.cal_a / '1x1-attempt-01-result.json'
        result_path.write_text(json.dumps(attempt), encoding='utf-8')
        calibration_path.write_text(json.dumps(artifact), encoding='utf-8')
        with self.assertRaisesRegex(FREEZER['FreezeError'], 'telemetry eligibility'):
            self.freeze_comparison('ineligible-telemetry')

    def test_one_first_bound_calibration_counts_apply_to_both_plans(self):
        self.cal_a = self.make_calibration(
            self.plan_a_path, self.profile_a_path, 'cal-a-scaled',
            [150_000_000, 300_000_000, 300_000_000, 300_000_000])
        before = len(FakeProcess.calls)
        _, artifact = self.freeze_comparison('one-calibration')
        self.assertEqual(before, len(FakeProcess.calls))
        self.assertEqual(artifact['planned_command_count'], 216)
        self.assertEqual(artifact['plans'][0]['calibration_sha256'],
                         artifact['plans'][1]['calibration_sha256'])
        self.assertEqual(artifact['plans'][0]['accepted_iteration_counts'],
                         artifact['plans'][1]['accepted_iteration_counts'])
        by_campaign = [{(row['pair_id'], row['side']): row['iterations']
                        for row in artifact['commands'] if row['campaign_index'] == campaign}
                       for campaign in (0, 1)]
        self.assertEqual(by_campaign[0], by_campaign[1])

    def test_rejects_calibration_bound_to_second_plan(self):
        wrong_calibration = self.make_calibration(
            self.plan_b_path, self.profile_b_path, 'cal-bound-to-b', [300_000_000] * 3)
        self.cal_a = wrong_calibration
        with self.assertRaisesRegex(FREEZER['FreezeError'], 'exact plan/profile bytes'):
            self.freeze_comparison('cal-bound-to-b')

    def test_rejects_accepted_count_inconsistent_with_attempts(self):
        calibration_path = self.cal_a / 'calibration.json'
        artifact = json.loads(calibration_path.read_text(encoding='utf-8'))
        artifact['accepted_iteration_counts']['1x1'] += 1
        calibration_path.write_text(json.dumps(artifact), encoding='utf-8')
        with self.assertRaisesRegex(FREEZER['FreezeError'], 'accepted count differs'):
            self.freeze_comparison('count-tampered')

    def test_aa_pre_is_separately_mode_bound_and_freezes_108(self):
        aa_base = self.fixture.make_source('aa-freeze-base', 'pre')
        aa_candidate = self.fixture.make_source('aa-freeze-candidate', 'pre')
        plan = self.fixture.make_plan(aa_base, aa_candidate, mode='aa_control', aa_product='pre')
        plan_path = self.root / 'aa-plan.json'
        plan_path.write_text(json.dumps(plan), encoding='utf-8')
        base_build = self.fixture.make_build('aa-freeze-base', aa_base['source'])
        candidate_build = self.fixture.make_build('aa-freeze-candidate', aa_candidate['source'])
        for build in (base_build, candidate_build):
            os.chmod(build / 'bench/bench_batch_append', 0o755)
            (build / 'bench/bench_batch_append').write_bytes(
                Path(self.profile_a['base']['binary_path']).read_bytes())
        profile = PROFILE['build_artifact'](plan_path, self.overlay, base_build, candidate_build)
        profile_dir = self.root / 'aa-profile'
        PROFILE['write_atomic'](profile_dir, profile)
        profile_path = profile_dir / 'execution-preflight.json'
        before = len(FakeProcess.calls)
        args = type('Args', (), dict(
            mode='aa_control', overlay_patch=self.overlay, plan=plan_path,
            profile=profile_path, calibration=self.cal_a,
            calibration_origin_plan=self.plan_a_path,
            calibration_origin_profile=self.profile_a_path,
            output_dir=self.root / 'aa-frozen'))()
        _, artifact = FREEZER['freeze'](args)
        _, comparison_artifact = self.freeze_comparison('aa-reuse-origin-check')
        self.assertEqual(before, len(FakeProcess.calls))
        self.assertEqual(artifact['mode'], 'aa_control')
        self.assertEqual(artifact['aa_product'], 'pre')
        self.assertEqual(artifact['planned_command_count'], 108)
        self.assertEqual(artifact['benchmark_launches_performed'], 0)
        self.assertEqual(artifact['calibration_origin']['plan_sha256'],
                         FREEZER['sha256'](self.plan_a_path.read_bytes()))
        self.assertEqual(artifact['calibration_origin']['calibration_sha256'],
                         FREEZER['sha256']((self.cal_a / 'calibration.json').read_bytes()))
        self.assertEqual(artifact['plans'][0]['calibration_sha256'],
                         artifact['calibration_origin']['calibration_sha256'])
        self.assertEqual(artifact['plans'][0]['accepted_iteration_counts'],
                         json.loads((self.cal_a / 'calibration.json').read_text(
                             encoding='utf-8'))['accepted_iteration_counts'])
        self.assertEqual(artifact['calibration_origin']['calibration_sha256'],
                         comparison_artifact['plans'][0]['calibration_sha256'])
        self.assertEqual(artifact['plans'][0]['accepted_iteration_counts'],
                         comparison_artifact['plans'][0]['accepted_iteration_counts'])

    def test_aa_requires_exact_origin_and_rejects_target_binary_drift(self):
        aa_base = self.fixture.make_source('aa-origin-base', 'pre')
        aa_candidate = self.fixture.make_source('aa-origin-candidate', 'pre')
        plan = self.fixture.make_plan(aa_base, aa_candidate, mode='aa_control', aa_product='pre')
        plan_path = self.root / 'aa-origin-plan.json'
        plan_path.write_text(json.dumps(plan), encoding='utf-8')
        builds = [self.fixture.make_build('aa-origin-' + side, source['source'])
                  for side, source in (('base', aa_base), ('candidate', aa_candidate))]
        for build in builds:
            binary = build / 'bench/bench_batch_append'
            binary.write_bytes(Path(self.profile_a['base']['binary_path']).read_bytes())
            os.chmod(binary, 0o755)
        profile = PROFILE['build_artifact'](plan_path, self.overlay, *builds)
        profile_dir = self.root / 'aa-origin-profile'
        PROFILE['write_atomic'](profile_dir, profile)
        profile_path = profile_dir / 'execution-preflight.json'
        target_path, target_plan, target_hash = FREEZER['strict_plan'](plan_path, self.overlay)
        _, target_profile, target_profile_hash = FREEZER['profile_for_plan'](
            target_path, target_plan, self.overlay, profile_path)
        _, origin_plan, origin_hash = FREEZER['strict_plan'](self.plan_a_path, self.overlay)
        _, origin_profile, origin_profile_hash = FREEZER['profile_for_plan'](
            self.plan_a_path, origin_plan, self.overlay, self.profile_a_path)
        target = dict(plan=target_plan, profile=target_profile,
                      plan_path=target_path, plan_sha256=target_hash,
                      profile_path=profile_path, profile_sha256=target_profile_hash)
        origin = dict(plan=origin_plan, profile=origin_profile,
                      plan_path=self.plan_a_path, plan_sha256=origin_hash,
                      profile_path=self.profile_a_path, profile_sha256=origin_profile_hash)
        FREEZER['validate_aa_origin'](target, origin)
        for label, mutate in (
                ('source tree', lambda p: p['sources']['base'].__setitem__('product_tree', '0' * 40)),
                ('source identity', lambda p: p['sources']['candidate'].__setitem__(
                    'checkout_commit', '0' * 40)),
                ('overlay', lambda p: p['overlay'].__setitem__('sha256', '0' * 64)),
                ('helper', lambda p: p['unchanged_helpers'].__setitem__(
                    next(iter(p['unchanged_helpers'])), '0' * 64)),
                ('profile', lambda p: p['base'].__setitem__('profile_sha256', '0' * 64)),
                ('binary', lambda p: p['candidate'].__setitem__('binary_sha256', '0' * 64))):
            with self.subTest(drift=label):
                altered = copy.deepcopy(target)
                mutate(altered['plan'] if label in ('source tree', 'source identity',
                                                     'overlay', 'helper')
                       else altered['profile'])
                with self.assertRaises(FREEZER['FreezeError']):
                    FREEZER['validate_aa_origin'](altered, origin)
        args = type('Args', (), dict(
            mode='aa_control', overlay_patch=self.overlay, plan=plan_path,
            profile=profile_path, calibration=self.cal_a,
            calibration_origin_plan=None, calibration_origin_profile=None,
            output_dir=self.root / 'missing-origin'))()
        with self.assertRaisesRegex(FREEZER['FreezeError'], 'origin plan/profile'):
            FREEZER['freeze'](args)
        args.calibration_origin_plan = self.plan_b_path
        args.calibration_origin_profile = self.profile_b_path
        with self.assertRaises(FREEZER['FreezeError']):
            FREEZER['freeze'](args)

    def test_host_summary_schema_identity_timestamp_and_counters_are_reconstructed(self):
        calibration = json.loads((self.cal_a / 'calibration.json').read_text(encoding='utf-8'))
        original_host = calibration['host']
        attempts = calibration['attempts']
        mutations = (
            ('host identity', lambda host: host['initial'].__setitem__('model', 'tampered')),
            ('SMT topology', lambda host: host['final'].__setitem__('smt_siblings', [99])),
            ('required schema', lambda host: host['initial'].pop('cgroup_v2_cpu')),
            ('timestamps', lambda host: host['initial'].__setitem__('timestamp_utc', 2)),
            ('final timestamp', lambda host: host['final'].__setitem__('timestamp_utc', 0)),
            ('counter reset', lambda host: host['final'].__setitem__(
                'cpu_psi_some_total_usec', host['initial']['cpu_psi_some_total_usec'] - 1)),
            ('cgroup reset', lambda host: host['final']['cgroup_v2_cpu'].__setitem__(
                'nr_throttled', host['initial']['cgroup_v2_cpu']['nr_throttled'] - 1)),
            ('sibling reset', lambda host: host['final']['sibling_cpu_ticks']['1'].__setitem__(
                'total', host['initial']['sibling_cpu_ticks']['1']['total'] - 1)),
        )
        for expected, mutate in mutations:
            with self.subTest(expected=expected):
                host = copy.deepcopy(original_host)
                mutate(host)
                with self.assertRaises(FREEZER['FreezeError']):
                    FREEZER['validate_host_summaries'](host, attempts)

    def test_rejects_aa_post_and_output_overlaps(self):
        aa_base = self.fixture.make_source('aa-post-base', 'post')
        aa_candidate = self.fixture.make_source('aa-post-candidate', 'post')
        plan = self.fixture.make_plan(aa_base, aa_candidate, mode='aa_control', aa_product='post')
        plan_path = self.root / 'aa-post-plan.json'
        plan_path.write_text(json.dumps(plan), encoding='utf-8')
        with self.assertRaisesRegex(FREEZER['FreezeError'], 'only explicit product pre'):
            FREEZER['strict_plan'](plan_path, self.overlay)
        with self.assertRaises(FREEZER['FreezeError']):
            FREEZER['output_path'](self.fixture.paths['base']['source'], [dict(
                plan_path=self.plan_a_path, profile_path=self.profile_a_path,
                calibration=dict(evidence_path=str(self.cal_a)), plan=self.fixture.plan,
                profile=self.profile_a)])


if __name__ == '__main__':
    unittest.main()
