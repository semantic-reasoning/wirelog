#!/usr/bin/env python3
"""No-launch command-freeze provenance, calibration, and tamper tests."""
import json
import copy
import hashlib
import os
from pathlib import Path
import runpy
import shutil
import tempfile
import unittest
from unittest.mock import patch

PERF = Path(__file__).parent
FREEZER = runpy.run_path(str(PERF / 'freeze-batch-append-execution.py'))
PLAN = FREEZER['PLAN']
PROFILE = FREEZER['PROFILE']
CAL = FREEZER['CAL']
EXEC_TESTS = runpy.run_path(str(PERF / 'test-prepare-batch-append-execution.py'))
CAL_TESTS = runpy.run_path(str(PERF / 'test-calibrate-batch-append.py'))
FakeProcess = CAL_TESTS['FakeProcess']
FakeProbe = CAL_TESTS['FakeProbe']


class _FixtureContext:
    """Plain fixture helper for setup shared by the freeze test methods."""

    def __init__(self, root=None):
        self.root = root
        self.cleanups = []

    def addCleanup(self, function, *args, **kwargs):
        self.cleanups.append((function, args, kwargs))

    def assertTrue(self, value, message=None):
        if not value:
            raise AssertionError(message or 'fixture root must be below HOME')

    def doCleanups(self):
        cleanups = reversed(self.cleanups)
        self.cleanups = []
        first_error = None
        for function, args, kwargs in cleanups:
            try:
                function(*args, **kwargs)
            except BaseException as error:
                if first_error is None:
                    first_error = (error, error.__traceback__)
        if first_error is not None:
            error, traceback = first_error
            raise error.with_traceback(traceback)

    def __getattr__(self, name):
        method = getattr(EXEC_TESTS['ExecutionPreflightTests'], name, None)
        if callable(method):
            if name in ('git', 'git_bytes', 'all_keys'):
                return method
            return method.__get__(self, type(self))
        raise AttributeError(name)


class CommandFreezeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        home_tmp = Path.home() / '.tmp'
        home_tmp.mkdir(mode=0o700, exist_ok=True)
        cls.class_fallback_patchers = []
        for api in (FREEZER['PLAN']['FALLBACKS'], PROFILE['FALLBACKS'],
                    CAL['PROFILE']['PLAN']['FALLBACKS'], CAL['PROFILE']['FALLBACKS']):
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
            cls.class_fallback_patchers.append(patcher)
        cls.fixture = _FixtureContext()
        EXEC_TESTS['ExecutionPreflightTests'].setUp(cls.fixture)
        for side in ('base', 'candidate'):
            os.chmod(cls.fixture.paths[side]['build'] / 'bench/bench_batch_append', 0o755)
        cls.plan_a_path = cls.fixture.root / 'plan-a-template.json'
        cls.plan_a_path.write_text(json.dumps(cls.fixture.plan, indent=2), encoding='utf-8')
        args = type('Args', (), dict(
            mode='comparison', aa_product=None, seed='different-schedule-seed',
            base_source=cls.fixture.paths['base']['source'],
            candidate_source=cls.fixture.paths['candidate']['source'],
            overlay_patch=cls.fixture.overlay))()
        plan_b = PLAN['build_plan'](args)
        cls.plan_b_path = cls.fixture.root / 'plan-b-template.json'
        cls.plan_b_path.write_text(json.dumps(plan_b, indent=2), encoding='utf-8')
        cls.profile_templates = {}
        for name, plan_path in (('profile-a', cls.plan_a_path),
                                ('profile-b', cls.plan_b_path)):
            artifact = PROFILE['build_artifact'](
                plan_path, cls.fixture.overlay, cls.fixture.paths['base']['build'],
                cls.fixture.paths['candidate']['build'])
            output = cls.fixture.root / f'{name}-template'
            PROFILE['write_atomic'](output, artifact)
            cls.profile_templates[name] = (output / 'execution-preflight.json').read_bytes()
        cls.shared_fixture_snapshot = cls.snapshot_fixture(cls.fixture.root)
        # Issue #2082: every test starts from the same private case directory --
        # its own plan and profile copies and a calibration of plan A -- and
        # building it, the calibration above all, is most of each setUp().  It
        # is built once here at a fixed path, so the paths recorded inside the
        # evidence stay valid, and setUp() restores that path from a pristine
        # copy before every test.  Each test still owns a fresh case directory;
        # the pristine copy is proven unchanged after every test.
        case_tmp = tempfile.TemporaryDirectory(dir=home_tmp)
        cls.fixture.addCleanup(case_tmp.cleanup)
        cls.case_root = Path(case_tmp.name) / 'case'
        cls.case_pristine = Path(case_tmp.name) / 'pristine'
        cls.case_root.mkdir(mode=0o700)
        cls.build_case(cls.case_root)
        shutil.copytree(cls.case_root, cls.case_pristine, symlinks=True)
        shutil.rmtree(cls.case_root)
        cls.case_snapshot = cls.snapshot_fixture(cls.case_pristine)

    @classmethod
    def build_case(cls, root):
        plan_a = root / 'plan-a.json'
        plan_a.write_bytes(cls.plan_a_path.read_bytes())
        plan_b = root / 'plan-b.json'
        plan_b.write_bytes(cls.plan_b_path.read_bytes())
        profile_a, _ = cls.copy_profile('profile-a', plan_a)
        cls.copy_profile('profile-b', plan_b)
        saved = {name: copy.deepcopy(getattr(FakeProcess, name))
                 for name in ('calls', 'started_flags', 'next_outputs', 'tamper_path',
                              'tamper_payload', 'interrupt_on_start')}
        try:
            FakeProcess.calls = []
            FakeProcess.started_flags = []
            FakeProcess.next_outputs = [300_000_000] * 3
            FakeProcess.tamper_path = None
            FakeProcess.tamper_payload = b'tampered while benchmark ran'
            FakeProcess.interrupt_on_start = False
            CAL['calibrate'](plan_a, profile_a, cls.fixture.overlay, root / 'cal-a',
                             timeout_seconds=5, probe=FakeProbe(),
                             popen_factory=FakeProcess, affinity_getter=lambda _pid: {0})
        finally:
            for name, value in saved.items():
                setattr(FakeProcess, name, value)

    @classmethod
    def tearDownClass(cls):
        first_error = None
        try:
            if cls.snapshot_fixture(cls.fixture.root) != cls.shared_fixture_snapshot:
                raise AssertionError(
                    'class-scoped freeze fixture changed during the test class')
        except BaseException as error:
            first_error = (error, error.__traceback__)
        try:
            cls.fixture.doCleanups()
        except BaseException as error:
            if first_error is None:
                first_error = (error, error.__traceback__)
        for patcher in reversed(cls.class_fallback_patchers):
            try:
                patcher.stop()
            except BaseException as error:
                if first_error is None:
                    first_error = (error, error.__traceback__)
        if first_error is not None:
            error, traceback = first_error
            raise error.with_traceback(traceback)

    def setUp(self):
        case_root = self.__class__.case_root
        # One case directory per process: a second live fixture would share
        # it, so refuse rather than overwrite the first one's private state.
        if case_root.exists():
            raise AssertionError(f'case directory {case_root} is already in use; '
                                 'clean up the enclosing fixture first')
        self.addCleanup(shutil.rmtree, case_root, True)
        shutil.copytree(self.__class__.case_pristine, case_root, symlinks=True)
        self.addCleanup(self.assert_case_template_unchanged)
        self.root = case_root
        self.fallback_patchers = []
        for api in (FREEZER['PLAN']['FALLBACKS'], PROFILE['FALLBACKS'],
                    CAL['PROFILE']['PLAN']['FALLBACKS'], CAL['PROFILE']['FALLBACKS']):
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
        self.addCleanup(self.assert_shared_fixture_unchanged)
        self.fixture = _FixtureContext(self.root)
        self.fixture.paths = self.__class__.fixture.paths
        self.fixture.overlay = self.__class__.fixture.overlay
        self.fixture.plan = self.__class__.fixture.plan
        for side in ('base', 'candidate'):
            os.chmod(self.fixture.paths[side]['build'] / 'bench/bench_batch_append', 0o755)
        self.overlay = self.fixture.overlay
        self.plan_a_path = self.root / 'plan-a.json'
        self.plan_b_path = self.root / 'plan-b.json'
        self.plan_b_bytes = self.plan_b_path.read_bytes()
        self.profile_a_path = self.root / 'profile-a/execution-preflight.json'
        self.profile_a = json.loads(self.profile_a_path.read_text(encoding='utf-8'))
        self.profile_b_path = self.root / 'profile-b/execution-preflight.json'
        self.profile_b = json.loads(self.profile_b_path.read_text(encoding='utf-8'))
        self._fake_process_state = {
            name: copy.deepcopy(getattr(FakeProcess, name))
            for name in ('calls', 'started_flags', 'next_outputs', 'tamper_path',
                         'tamper_payload', 'interrupt_on_start')}
        FakeProcess.calls = []
        FakeProcess.started_flags = []
        FakeProcess.next_outputs = []
        FakeProcess.tamper_path = None
        FakeProcess.tamper_payload = b'tampered while benchmark ran'
        FakeProcess.interrupt_on_start = False
        self.addCleanup(self.restore_fake_process)
        self.cal_a = self.root / 'cal-a'
        self.fake_call_count = len(FakeProcess.calls)

    @staticmethod
    def snapshot_fixture(root):
        snapshot = {}
        for path in sorted(root.rglob('*')):
            relative = path.relative_to(root).as_posix()
            info = path.lstat()
            if path.is_symlink():
                snapshot[relative] = ('symlink', os.readlink(path))
            elif path.is_dir():
                snapshot[relative] = ('directory', info.st_mode & 0o777)
            elif path.is_file():
                snapshot[relative] = ('file', info.st_mode & 0o777,
                                      hashlib.sha256(path.read_bytes()).hexdigest())
            else:
                snapshot[relative] = ('special', info.st_mode)
        return snapshot

    def assert_shared_fixture_unchanged(self):
        # Compared with the class-level snapshot.  While every earlier test
        # passed this check, a mismatch here belongs to this test.
        if self.snapshot_fixture(self.__class__.fixture.root) != \
                self.__class__.shared_fixture_snapshot:
            raise AssertionError('class-scoped freeze fixture changed during a test')

    def assert_case_template_unchanged(self):
        if self.snapshot_fixture(self.__class__.case_pristine) != \
                self.__class__.case_snapshot:
            raise AssertionError('pristine case directory changed during a test')

    @staticmethod
    def copy_profile(name, plan_path):
        template = CommandFreezeTests.profile_templates[name]
        artifact = json.loads(template)
        artifact['plan_path'] = str(plan_path.resolve())
        output = plan_path.parent / name
        output.mkdir(mode=0o700)
        target = output / 'execution-preflight.json'
        target.write_text(json.dumps(artifact, sort_keys=True, indent=2,
                                     ensure_ascii=False, allow_nan=False) + '\n',
                          encoding='utf-8')
        return target, artifact

    def restore_fake_process(self):
        for name, value in self._fake_process_state.items():
            setattr(FakeProcess, name, value)


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

    def test_rejects_consistently_forged_success_with_missing_sibling_counters(self):
        calibration_path = self.cal_a / 'calibration.json'
        artifact = json.loads(calibration_path.read_text(encoding='utf-8'))
        for summary in artifact['host'].values():
            summary['sibling_cpu_ticks'] = {}
        for record in artifact['attempts']:
            started = record['started']
            started['host_before']['sibling_cpu_ticks'] = {}
            record['host_after']['sibling_cpu_ticks'] = {}
            record['eligible'] = True
            record['accepted'] = True
            record['telemetry']['eligible'] = True
            prefix = f"{started['case']}-attempt-{started['attempt_index']:02d}"
            for suffix, value in (('started', started), ('result', record)):
                path = self.cal_a / f'{prefix}-{suffix}.json'
                path.write_text(json.dumps(value, sort_keys=True, indent=2,
                                           ensure_ascii=False, allow_nan=False) + '\n',
                                encoding='utf-8')
        calibration_path.write_text(json.dumps(artifact, sort_keys=True, indent=2,
                                               ensure_ascii=False, allow_nan=False) + '\n',
                                    encoding='utf-8')
        with self.assertRaisesRegex(FREEZER['FreezeError'], 'sibling tick coverage'):
            self.freeze_comparison('forged-missing-sibling')

    def test_recomputes_and_rejects_forged_success_with_zero_sibling_delta(self):
        calibration_path = self.cal_a / 'calibration.json'
        artifact = json.loads(calibration_path.read_text(encoding='utf-8'))
        counters = {'1': {'total': 1000, 'busy': 500}}
        for summary in artifact['host'].values():
            summary['sibling_cpu_ticks'] = copy.deepcopy(counters)
        for record in artifact['attempts']:
            started = record['started']
            started['host_before']['sibling_cpu_ticks'] = copy.deepcopy(counters)
            record['host_after']['sibling_cpu_ticks'] = copy.deepcopy(counters)
            prefix = f"{started['case']}-attempt-{started['attempt_index']:02d}"
            for suffix, value in (('started', started), ('result', record)):
                path = self.cal_a / f'{prefix}-{suffix}.json'
                path.write_text(json.dumps(value, sort_keys=True, indent=2,
                                           ensure_ascii=False, allow_nan=False) + '\n',
                                encoding='utf-8')
        calibration_path.write_text(json.dumps(artifact, sort_keys=True, indent=2,
                                               ensure_ascii=False, allow_nan=False) + '\n',
                                    encoding='utf-8')
        with self.assertRaisesRegex(FREEZER['FreezeError'], 'telemetry eligibility'):
            self.freeze_comparison('forged-zero-sibling-delta')

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
            ('timestamps', lambda host: host['initial'].__setitem__('timestamp_utc', 3)),
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


class FixtureContextTests(unittest.TestCase):
    def test_cleanup_stack_runs_after_a_cleanup_fails(self):
        context = _FixtureContext()
        completed = []
        context.addCleanup(completed.append, 'first')

        def fail_cleanup():
            completed.append('failure')
            raise RuntimeError('cleanup failed')

        context.addCleanup(fail_cleanup)
        context.addCleanup(completed.append, 'last')
        with self.assertRaisesRegex(RuntimeError, 'cleanup failed'):
            context.doCleanups()
        self.assertEqual(completed, ['last', 'failure', 'first'])


if __name__ == '__main__':
    unittest.main()
