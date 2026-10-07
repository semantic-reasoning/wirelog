#!/usr/bin/env python3
"""No-launch admission, durable journal, and revalidation tests."""
import json
import os
from pathlib import Path
import runpy
import shutil
import tempfile
import unittest
from unittest.mock import patch

PERF = Path(__file__).parent
COLLECTOR = runpy.run_path(str(PERF / 'collect-batch-append-campaign.py'))
FREEZER = COLLECTOR['FREEZER']
FIXTURE_MODULE = runpy.run_path(str(PERF / 'test-freeze-batch-append-execution.py'))
FakeProcess = FIXTURE_MODULE['FakeProcess']
FakeProbe = runpy.run_path(str(PERF / 'test-calibrate-batch-append.py'))['FakeProbe']


def cleanup_nested_fixture(fixture):
    if not fixture.doCleanups():
        raise AssertionError('nested fixture cleanup failed')


class CollectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # This suite drives CommandFreezeTests.setUp() manually rather than
        # through unittest's class lifecycle. Bracket its shared fixture here.
        freeze_tests = FIXTURE_MODULE['CommandFreezeTests']
        freeze_tests.setUpClass()
        try:
            cls.extend_case_template(freeze_tests)
        except BaseException:
            freeze_tests.tearDownClass()
            raise

    @classmethod
    def extend_case_template(cls, freeze_tests):
        # Issue #2082: every test also needs the comparison freeze of that
        # case.  Freeze it once into the case directory and hand
        # CommandFreezeTests a pristine copy that already holds it; setUp()
        # then restores the freeze with the rest of the case.  The original
        # template is put back in tearDownClass().
        cls.saved_case_template = (freeze_tests.case_pristine,
                                   freeze_tests.case_snapshot)
        cls.case_tmp = tempfile.TemporaryDirectory(dir=Path.home() / '.tmp')
        try:
            builder = freeze_tests(
                'test_comparison_freezes_216_adjacent_rows_without_launches')
            try:
                builder.setUp()
                freeze_path, _ = builder.freeze_comparison('source-freeze')
                cls.freeze_relative = freeze_path.relative_to(builder.root)
                pristine = Path(cls.case_tmp.name) / 'pristine'
                shutil.copytree(builder.root, pristine, symlinks=True)
            finally:
                cleanup_nested_fixture(builder)
            snapshot = freeze_tests.snapshot_fixture(pristine)
        except BaseException:
            cls.case_tmp.cleanup()
            raise
        freeze_tests.case_pristine = pristine
        freeze_tests.case_snapshot = snapshot

    @classmethod
    def tearDownClass(cls):
        freeze_tests = FIXTURE_MODULE['CommandFreezeTests']
        try:
            freeze_tests.case_pristine, freeze_tests.case_snapshot = \
                cls.saved_case_template
            cls.case_tmp.cleanup()
        finally:
            freeze_tests.tearDownClass()

    def setUp(self):
        home_tmp = Path.home() / '.tmp'
        home_tmp.mkdir(mode=0o700, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(dir=home_tmp)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.fallback_patchers = []
        maps = (FREEZER['PLAN']['FALLBACKS'], FREEZER['PROFILE']['FALLBACKS'],
                FREEZER['CAL']['PROFILE']['PLAN']['FALLBACKS'],
                FREEZER['CAL']['PROFILE']['FALLBACKS'])
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
        self.fixture = FIXTURE_MODULE['CommandFreezeTests'](
            'test_comparison_freezes_216_adjacent_rows_without_launches')
        self.addCleanup(cleanup_nested_fixture, self.fixture)
        self.fixture.setUp()
        self.overlay = self.fixture.overlay
        self.freeze_path = self.fixture.root / self.freeze_relative
        self.output = self.freeze_path.parent / 'collection'
        self.args = type('Args', (), dict(
            mode='comparison', overlay_patch=self.overlay, freeze_artifact=self.freeze_path,
            output_dir=self.output,
            plan_a=self.fixture.plan_a_path, profile_a=self.fixture.profile_a_path,
            calibration_a=self.fixture.cal_a,
            plan_b=self.fixture.plan_b_path, profile_b=self.fixture.profile_b_path,
            plan=None, profile=None, calibration=None,
            calibration_origin_plan=None, calibration_origin_profile=None))()
        self.fake_calls_before = len(FakeProcess.calls)

    def test_nested_fixture_cleanup_failure_is_reported_after_all_cleanups(self):
        fixture = unittest.TestCase()
        callbacks = []
        fixture.addCleanup(callbacks.append, 'remaining')

        def fail_cleanup():
            raise RuntimeError('nested cleanup failed')

        fixture.addCleanup(fail_cleanup)
        with self.assertRaisesRegex(AssertionError, 'nested fixture cleanup failed'):
            cleanup_nested_fixture(fixture)
        self.assertEqual(callbacks, ['remaining'])

    def test_comparison_admits_216_rows_with_empty_durable_journal_and_no_launch(self):
        with patch('os.fsync', wraps=os.fsync) as sync:
            path, artifact = COLLECTOR['collect'](self.args, host_probe=FakeProbe())
        self.assertGreaterEqual(sync.call_count, 5)
        self.assertEqual(path, self.output)
        self.assertEqual(artifact['schema'], COLLECTOR['SCHEMA'])
        self.assertEqual(artifact['status'], 'ready')
        self.assertEqual(artifact['planned_command_count'], 216)
        self.assertEqual(artifact['benchmark_launches_performed'], 0)
        self.assertFalse(FREEZER['contains_verdict_key'](artifact))
        frozen = artifact['frozen_preflight']
        self.assertEqual(frozen['schema'], FREEZER['SCHEMA'])
        self.assertEqual(len(frozen['commands']), 216)
        self.assertEqual(len(frozen['input_sha256']), len(set(frozen['input_sha256'])))
        self.assertEqual((path / 'journal.jsonl').read_bytes(), b'')
        status = json.loads((path / 'status.json').read_text(encoding='utf-8'))
        preflight_bytes = (path / 'collector-preflight.json').read_bytes()
        self.assertEqual(status['status'], 'ready')
        self.assertEqual(status['benchmark_launches_performed'], 0)
        self.assertEqual(status['planned_command_count'], 216)
        self.assertEqual(status['preflight_sha256'], COLLECTOR['digest'](preflight_bytes))
        self.assertEqual(json.loads(preflight_bytes), json.loads(json.dumps(artifact)))
        original_rows = json.loads(self.freeze_path.read_text(encoding='utf-8'))['commands']
        self.assertEqual(frozen['commands'], original_rows)
        self.assertEqual(frozen['commands'][0]['environment']['HOME'],
                         original_rows[0]['environment']['HOME'])
        self.assertNotEqual(Path(frozen['commands'][0]['environment']['HOME']), path)
        self.assertEqual(artifact['command_freeze_sha256'],
                         COLLECTOR['digest'](self.freeze_path.read_bytes()))
        self.assertEqual(artifact['host_attestation']['selected_cpus'], [0])
        self.assertEqual(artifact['host_attestation']['smt_siblings_by_cpu'], {'0': [1]})
        self.assertEqual(len(FakeProcess.calls), self.fake_calls_before)

    def test_requires_current_frozen_host_identity_and_cpu_affinity(self):
        class WrongKernel:
            def snapshot(self):
                host = FakeProbe().snapshot()
                host['kernel'] = 'different-kernel'
                return host

        class MissingCpu:
            def snapshot(self):
                host = FakeProbe().snapshot()
                host['affinity_cpus'] = [1]
                return host

        with self.assertRaisesRegex(COLLECTOR['CollectionError'], 'host identity'):
            COLLECTOR['admit'](self.args, host_probe=WrongKernel())
        with self.assertRaisesRegex(COLLECTOR['CollectionError'], 'CPU 0'):
            COLLECTOR['admit'](self.args, host_probe=MissingCpu())

    def test_host_counters_and_timestamps_may_advance_during_revalidation(self):
        class AdvancingProbe:
            def __init__(self):
                self.calls = 0

            def snapshot(self):
                self.calls += 1
                host = FakeProbe().snapshot()
                host['timestamp_utc'] += self.calls
                host['cpu_psi_some_total_usec'] += 3 * self.calls
                host['cgroup_v2_cpu']['nr_throttled'] += self.calls
                host['cgroup_v2_cpu']['throttled_usec'] += 11 * self.calls
                host['frequency_khz'] += 100 * self.calls
                return host

        probe = AdvancingProbe()
        _, artifact = COLLECTOR['collect'](self.args, host_probe=probe)
        self.assertEqual(probe.calls, 3)
        initial = artifact['host_attestation']['initial']
        self.assertEqual(initial['timestamp_utc'], 2)
        self.assertEqual(initial['cpu_psi_some_total_usec'], 103)

    def test_collection_output_must_be_strictly_inside_freeze_directory(self):
        self.args.output_dir = self.freeze_path.parent.parent / 'sibling-collection'
        with self.assertRaisesRegex(COLLECTOR['CollectionError'], 'direct child'):
            COLLECTOR['admit'](self.args, host_probe=FakeProbe())
        nested_parent = self.freeze_path.parent / 'nested-parent'
        nested_parent.mkdir()
        self.args.output_dir = nested_parent / 'nested-collection'
        try:
            with self.assertRaisesRegex(COLLECTOR['CollectionError'], 'direct child'):
                COLLECTOR['admit'](self.args, host_probe=FakeProbe())
        finally:
            nested_parent.rmdir()

    def test_rejects_tampered_frozen_command_bytes(self):
        original = self.freeze_path.read_bytes()
        freeze = json.loads(original.decode('utf-8'))
        freeze['commands'][0]['argv'].append('--extra')
        self.freeze_path.write_text(json.dumps(freeze, sort_keys=True, indent=2) + '\n',
                                    encoding='utf-8')
        with self.assertRaisesRegex(COLLECTOR['CollectionError'], 'byte-for-byte'):
            COLLECTOR['collect'](self.args, host_probe=FakeProbe())
        self.freeze_path.write_bytes(original)

    def test_refuses_existing_outside_home_and_overlapping_output(self):
        self.output.mkdir()
        marker = self.output / 'keep'
        marker.write_text('existing', encoding='utf-8')
        with self.assertRaises(FileExistsError):
            COLLECTOR['collect'](self.args, host_probe=FakeProbe())
        self.assertEqual(marker.read_text(encoding='utf-8'), 'existing')
        marker.unlink()
        self.output.rmdir()
        self.args.output_dir = Path('/opt/collection-evidence')
        with self.assertRaises(COLLECTOR['CollectionError']):
            COLLECTOR['admit'](self.args, host_probe=FakeProbe())
        self.args.output_dir = self.fixture.fixture.paths['base']['source'] / 'inside-source'
        with self.assertRaises(COLLECTOR['CollectionError']):
            COLLECTOR['admit'](self.args, host_probe=FakeProbe())

    def test_post_publish_input_drift_removes_collection(self):
        original_plan = self.fixture.plan_a_path.read_bytes()
        real_rename = os.rename
        published = False

        def rename_then_mutate(source, destination):
            nonlocal published
            real_rename(source, destination)
            published = True
            self.fixture.plan_a_path.write_bytes(original_plan + b' ')

        try:
            with patch('os.rename', side_effect=rename_then_mutate):
                with self.assertRaises(COLLECTOR['CollectionError']):
                    COLLECTOR['collect'](self.args, host_probe=FakeProbe())
            self.assertTrue(published)
            self.assertFalse(self.output.exists())
        finally:
            self.fixture.plan_a_path.write_bytes(original_plan)

    def test_aa_pre_admits_exactly_108_rows_with_origin_calibration(self):
        aa_base = self.fixture.fixture.make_source('collector-aa-base', 'pre')
        aa_candidate = self.fixture.fixture.make_source('collector-aa-candidate', 'pre')
        plan = self.fixture.fixture.make_plan(aa_base, aa_candidate, mode='aa_control',
                                             aa_product='pre')
        plan_path = self.root / 'aa-plan.json'
        plan_path.write_text(json.dumps(plan, sort_keys=True), encoding='utf-8')
        builds = [self.fixture.fixture.make_build(f'collector-aa-{side}', source['source'])
                  for side, source in (('base', aa_base), ('candidate', aa_candidate))]
        for build in builds:
            binary = build / 'bench/bench_batch_append'
            binary.write_bytes(Path(self.fixture.profile_a['base']['binary_path']).read_bytes())
            binary.chmod(0o755)
        profile = FREEZER['PROFILE']['build_artifact'](
            plan_path, self.overlay, builds[0], builds[1])
        profile_dir = self.root / 'aa-profile'
        FREEZER['PROFILE']['write_atomic'](profile_dir, profile)
        freeze_args = type('Args', (), dict(
            mode='aa_control', overlay_patch=self.overlay,
            plan=plan_path, profile=profile_dir / 'execution-preflight.json',
            calibration=self.fixture.cal_a,
            calibration_origin_plan=self.fixture.plan_a_path,
            calibration_origin_profile=self.fixture.profile_a_path,
            output_dir=self.root / 'aa-freeze',
            plan_a=None, profile_a=None, calibration_a=None,
            plan_b=None, profile_b=None))()
        freeze_path, _ = FREEZER['freeze'](freeze_args)
        self.args = type('Args', (), dict(
            mode='aa_control', overlay_patch=self.overlay, freeze_artifact=freeze_path,
            output_dir=(self.root / 'aa-freeze' / 'collection'),
            plan=plan_path, profile=profile_dir / 'execution-preflight.json',
            calibration=self.fixture.cal_a,
            calibration_origin_plan=self.fixture.plan_a_path,
            calibration_origin_profile=self.fixture.profile_a_path,
            plan_a=None, profile_a=None, calibration_a=None,
            plan_b=None, profile_b=None))()
        _, artifact = COLLECTOR['collect'](self.args, host_probe=FakeProbe())
        self.assertEqual(artifact['planned_command_count'], 108)
        self.assertEqual(len(artifact['frozen_preflight']['commands']), 108)
        self.assertEqual(artifact['frozen_preflight']['calibration_origin']['calibration_sha256'],
                         artifact['frozen_preflight']['plans'][0]['calibration_sha256'])
        self.assertEqual(len(FakeProcess.calls), self.fake_calls_before)


if __name__ == '__main__':
    unittest.main()
