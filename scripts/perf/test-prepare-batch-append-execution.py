#!/usr/bin/env python3
"""Profile-only preflight provenance, Meson, drift, and durability tests."""
import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import tempfile
import unittest
from unittest.mock import patch

PERF = Path(__file__).parent
PLAN = runpy.run_path(str(PERF / 'prepare-batch-append-campaign.py'))
M = runpy.run_path(str(PERF / 'prepare-batch-append-execution.py'))


class ExecutionPreflightTests(unittest.TestCase):
    def setUp(self):
        home_tmp = Path.home() / '.tmp'
        home_tmp.mkdir(mode=0o700, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(dir=home_tmp)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.assertTrue(self.root.is_relative_to(Path.home().resolve()))
        self.paths = {}
        self.paths['base'] = self.make_source('base', 'pre')
        self.paths['candidate'] = self.make_source('candidate', 'post')
        self.overlay = self.root / 'benchmark-overlay.patch'
        self.overlay.write_bytes(self.git_bytes(
            self.paths['base']['source'], 'diff', '--binary',
            'HEAD', '--', *PLAN['OVERLAY_PATHS']))
        self.plan_path = self.root / 'campaign-plan.json'
        self.plan = self.make_plan(self.paths['base'], self.paths['candidate'])
        self.plan_path.write_text(json.dumps(self.plan, indent=2), encoding='utf-8')
        for side in ('base', 'candidate'):
            self.paths[side]['build'] = self.make_build(side, self.paths[side]['source'])
            info = self.paths[side]['build'] / 'meson-info'
            self.paths[side]['metadata_bytes'] = {
                path.name: path.read_bytes() for path in info.iterdir()}

    @staticmethod
    def git(source, *args):
        return subprocess.check_output(['git', '-C', str(source), *args],
                                       text=True, encoding='utf-8').strip()

    @staticmethod
    def git_bytes(source, *args):
        return subprocess.check_output(['git', '-C', str(source), *args])

    def make_source(self, side, product):
        source = self.root / f'{side}-source'
        source.mkdir()
        self.git(source, 'init', '-q')
        self.git(source, 'config', 'user.name', 'Preflight Test')
        self.git(source, 'config', 'user.email', 'preflight@example.invalid')
        git_dir = Path(self.git(source, 'rev-parse', '--git-dir'))
        if not git_dir.is_absolute():
            git_dir = (source / git_dir).resolve()
        alternates = git_dir / 'objects/info/alternates'
        revision = PLAN['load_revision_manifest']()[0]
        anchor = revision['anchor']['commit']
        anchor_ref = PLAN['ANCHOR_REF']
        self.git(source, 'fetch', '--no-tags', str(PLAN['ANCHOR_BUNDLE_PATH']), anchor_ref)
        fetched_commit = self.git(source, 'rev-parse', 'FETCH_HEAD')
        fetched_tree = self.git(source, 'rev-parse', 'FETCH_HEAD^{tree}')
        if fetched_commit != anchor or fetched_tree != revision['anchor']['tree']:
            raise AssertionError('fixture bundle differs from the pinned anchor commit/tree')
        if alternates.exists():
            raise AssertionError('fixture repository must not use Git object alternates')
        self.git(source, 'cat-file', '-e', f'{anchor}^{{commit}}')
        self.git(source, 'update-ref', 'refs/heads/fixture', anchor)
        self.git(source, 'checkout', '-q', '--detach', anchor)
        if product == 'post':
            commit = self.git(source, 'commit-tree', revision['product']['post_tree'],
                              '-p', anchor, '-m', 'synthetic product')
            self.git(source, 'update-ref', 'refs/heads/fixture', commit)
            self.git(source, 'checkout', '-q', '--detach', commit)
        self.write_overlay(source)
        return dict(source=source,
                    commit=self.git(source, 'rev-parse', 'HEAD'),
                    tree=self.git(source, 'rev-parse', 'HEAD^{tree}'),
                    product=product)

    def write_overlay(self, source):
        contents = {
            'bench/bench_batch_append.c': '/* benchmark source */\n',
            'bench/meson.build': 'benchmark target fixture\n',
            'tests/meson.build': 'benchmark smoke registration fixture\n',
            'tests/test_bench_batch_append.c': '/* smoke */\n',
        }
        for name, content in contents.items():
            path = source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding='utf-8')
        self.git(source, 'add', *PLAN['OVERLAY_PATHS'])

    def make_plan(self, base, candidate, mode='comparison', aa_product='pre'):
        args = type('Args', (), dict(
            mode=mode, aa_product=(aa_product if mode == 'aa_control' else None),
            seed='execution-test-seed',
            base_source=base['source'], candidate_source=candidate['source'],
            overlay_patch=self.overlay))()
        return PLAN['build_plan'](args)

    def make_build(self, side, source):
        build = self.root / 'build-storage' / f'{side}-build'
        (build / 'meson-info').mkdir(parents=True)
        (build / 'bench').mkdir()
        binary = build / 'bench/bench_batch_append'
        binary.write_bytes(f'fixture benchmark {side}\n'.encode())
        (build / 'build.ninja').write_text(
            'ninja_required_version = 1.3\n'
            'build bench/bench_batch_append: phony\n'
            'build all: phony\n'
            'default all\n', encoding='utf-8')
        info = build / 'meson-info'
        metadata = {
            'meson-info.json': dict(directories=dict(source=str(source), build=str(build))),
            'intro-buildoptions.json': [
                dict(name='buildtype', value='release', section='core',
                     machine='any', type='combo'),
                dict(name='c_args', value=['-O2', '-mavx2'], section='core',
                     machine='host', type='array'),
            ],
            'intro-compilers.json': dict(host=dict(c=dict(
                id='gcc', exelist=['/bin/true'], linker_exelist=['/bin/true'],
                version='test-gcc', full_version='test-gcc 1.0', linker_id='ld.fake'))),
            'intro-machines.json': dict(host=dict(system='linux', cpu_family='x86_64',
                                                   cpu='test-cpu', endian='little')),
            'intro-dependencies.json': [dict(name='fake-dep', found=True,
                                              version='1.0', compile_args=['-I/fake'])],
            'intro-targets.json': [dict(
                name='bench_batch_append', id='fake@@bench_batch_append@exe',
                type='executable', defined_in=str(source / 'bench/meson.build'),
                filename=[str(binary)], build_by_default=True,
                target_sources=[
                    dict(language='c', machine='host', compiler=['/bin/true'],
                         parameters=['-I' + str(source / 'bench'), '-O2', '-mavx2'],
                         sources=[str(source / 'bench/bench_batch_append.c'),
                                  str(source / 'wirelog/columnar/relation.c')]),
                    dict(language=None, machine='host', compiler=None,
                         parameters=['-Wl,--as-needed', '-pthread'], sources=[]),
                ])],
        }
        for filename, value in metadata.items():
            (info / filename).write_text(json.dumps(value), encoding='utf-8')
        (build / 'meson-logs').mkdir()
        (build / 'meson-logs/meson-log.txt').write_text(
            f'Meson fixture configure for {side}\n', encoding='utf-8')
        (build / 'compile_commands.json').write_text('[]\n', encoding='utf-8')
        return build

    def artifact(self, **overrides):
        values = dict(plan_path_arg=self.plan_path, overlay_arg=self.overlay,
                      base_build_arg=self.paths['base']['build'],
                      candidate_build_arg=self.paths['candidate']['build'])
        values.update(overrides)
        return M['build_artifact'](**values)

    def write_plan(self, plan):
        self.plan_path.write_text(json.dumps(plan, indent=2), encoding='utf-8')

    @staticmethod
    def all_keys(value):
        if isinstance(value, dict):
            result = set(value)
            for item in value.values():
                result.update(ExecutionPreflightTests.all_keys(item))
            return result
        if isinstance(value, list):
            result = set()
            for item in value:
                result.update(ExecutionPreflightTests.all_keys(item))
            return result
        return set()

    def edit_metadata(self, side, filename, edit):
        path = self.paths[side]['build'] / 'meson-info' / filename
        value = json.loads(path.read_text(encoding='utf-8'))
        edit(value)
        path.write_text(json.dumps(value), encoding='utf-8')

    def test_valid_comparison_and_aa_artifacts_are_profile_only(self):
        def snapshot(root):
            return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
                    for path in root.rglob('*') if path.is_file()}
        before = {side: snapshot(self.paths[side]['build'])
                  for side in ('base', 'candidate')}
        with patch('subprocess.run', wraps=subprocess.run) as run:
            comparison = self.artifact()
        self.assertEqual(before, {side: snapshot(self.paths[side]['build'])
                                  for side in ('base', 'candidate')})
        self.assertTrue(comparison['source_build_verified'])
        self.assertEqual(comparison['status'], 'not_executable')
        self.assertTrue(comparison['not_executable'])
        self.assertEqual(comparison['benchmark_launches_performed'], 0)
        self.assertNotIn('performance_verdict', comparison)
        self.assertNotIn('calibration', comparison)
        self.assertNotIn('execution_argv', comparison)
        self.assertFalse({'calibration', 'iterations', 'argv', 'performance_verdict'}
                         & self.all_keys(comparison))
        self.assertEqual(comparison['base']['profile_sha256'],
                         comparison['candidate']['profile_sha256'])
        self.assertNotEqual(comparison['base']['binary_sha256'],
                            comparison['candidate']['binary_sha256'])
        invoked = [call.args[0] for call in run.call_args_list]
        binary = comparison['base']['binary_path']
        self.assertFalse(any(any(str(arg) == binary for arg in command)
                             for command in invoked if isinstance(command, list)))

        for product in ('pre', 'post'):
            aa_base = self.make_source(f'aa-{product}-base', product)
            aa_candidate = self.make_source(f'aa-{product}-candidate', product)
            aa_plan = self.make_plan(aa_base, aa_candidate, mode='aa_control',
                                     aa_product=product)
            aa_plan_path = self.root / f'aa-{product}-plan.json'
            aa_plan_path.write_text(json.dumps(aa_plan), encoding='utf-8')
            aa_base_build = self.make_build(f'aa-{product}-base', aa_base['source'])
            aa_build = self.make_build(f'aa-{product}-candidate', aa_candidate['source'])
            aa_artifact = M['build_artifact'](aa_plan_path, self.overlay,
                                             aa_base_build, aa_build)
            self.assertEqual(aa_artifact['mode'], 'aa_control')
            self.assertEqual(aa_artifact['status'], 'not_executable')
            self.assertTrue(aa_artifact['not_executable'])

    def test_rejects_v2_plan(self):
        changed = dict(self.plan)
        changed['schema'] = 'wirelog.batch-append-plan.v2'
        changed['schema_version'] = 2
        self.write_plan(changed)
        with self.assertRaisesRegex(M['PreflightError'], 'schema v3'):
            self.artifact()

    def test_rejects_plan_schedule_tampering_and_helper_drift(self):
        changed = json.loads(self.plan_path.read_text(encoding='utf-8'))
        first = changed['schedule']['launches'][0]
        first['side'] = 'candidate' if first['side'] == 'base' else 'base'
        self.write_plan(changed)
        with self.assertRaisesRegex(M['PreflightError'], 'frozen schedule'):
            self.artifact()
        self.write_plan(self.plan)
        helper = self.paths['base']['source'] / 'bench/bench_util.h'
        helper.write_text('drifted helper\n', encoding='utf-8')
        with self.assertRaisesRegex(M['PreflightError'], 'changes'):
            self.artifact()

    def test_rejects_shallow_and_ignored_or_untracked_sources(self):
        source = self.paths['candidate']['source']
        ignored = source / 'ignored-local-file'
        ignored.write_text('must reject\n', encoding='utf-8')
        with self.assertRaisesRegex(M['PreflightError'], 'ignored, or untracked'):
            self.artifact()
        ignored.unlink()
        self.git(source, 'update-ref', 'refs/heads/shallow-test',
                 self.git(source, 'rev-parse', 'HEAD'))
        (source / '.git/shallow').write_text(self.git(source, 'rev-parse', 'HEAD') + '\n',
                                              encoding='utf-8')
        with self.assertRaisesRegex(M['PreflightError'], 'must not be shallow'):
            self.artifact()

    def test_rejects_mesons_source_build_mapping_mismatch(self):
        self.edit_metadata('candidate', 'meson-info.json',
                           lambda meta: meta['directories'].__setitem__('source', '/wrong/source'))
        with self.assertRaisesRegex(M['PreflightError'], 'source/build mapping mismatch'):
            self.artifact()

    def test_rejects_option_compiler_cpu_and_dependency_profile_mismatch(self):
        edits = (
            ('intro-buildoptions.json',
             lambda data: data[0].__setitem__('value', 'debug')),
            ('intro-compilers.json',
             lambda data: data['host']['c'].__setitem__('full_version', 'different compiler')),
            ('intro-targets.json',
             lambda data: data[0]['target_sources'][0]['parameters'].__setitem__(2, '-mno-avx')),
            ('intro-dependencies.json',
             lambda data: data[0].__setitem__('version', '2.0')),
        )
        for filename, edit in edits:
            with self.subTest(filename=filename):
                path = self.paths['candidate']['build'] / 'meson-info' / filename
                path.write_bytes(self.paths['candidate']['metadata_bytes'][filename])
                self.edit_metadata('candidate', filename, edit)
                with self.assertRaisesRegex(M['PreflightError'], 'profiles differ'):
                    self.artifact()

    def test_rejects_binary_drift_between_snapshots(self):
        original = M['inspect_side']
        calls = 0

        def drift_binary(source, build):
            nonlocal calls
            result = original(source, build)
            calls += 1
            if calls == 2:
                Path(result['binary_path']).write_bytes(b'changed during inspection')
            return result

        with patch.dict(M['build_artifact'].__globals__, inspect_side=drift_binary):
            with self.assertRaisesRegex(M['PreflightError'], 'drifted during preflight'):
                self.artifact()

    def test_rejects_source_helper_drift_between_snapshots(self):
        original = M['inspect_side']
        calls = 0

        def drift_helper(source, build):
            nonlocal calls
            result = original(source, build)
            calls += 1
            if calls == 2:
                helper = self.paths['candidate']['source'] / 'bench/bench_util.h'
                helper.write_text('drift during preflight\n', encoding='utf-8')
            return result

        with patch.dict(M['build_artifact'].__globals__, inspect_side=drift_helper):
            with self.assertRaisesRegex(M['PreflightError'], 'changes'):
                self.artifact()

    def test_rejects_pending_rebuild_and_overlapping_or_outside_paths(self):
        build = self.paths['candidate']['build']
        (build / 'build.ninja').write_text(
            'rule pending\n  command = true\nbuild pending-output: pending\ndefault pending-output\n',
            encoding='utf-8')
        with self.assertRaisesRegex(M['PreflightError'], 'pending work'):
            self.artifact()
        (build / 'build.ninja').write_text(
            'ninja_required_version = 1.3\nbuild all: phony\ndefault all\n',
            encoding='utf-8')
        with self.assertRaisesRegex(M['PreflightError'], 'outside source worktrees'):
            self.artifact(candidate_build_arg=self.paths['candidate']['source'] / 'build')
        with self.assertRaisesRegex(M['PreflightError'], 'under HOME'):
            self.artifact(base_build_arg=Path('/opt/build'))

    def test_requires_clean_benchmark_target_even_when_default_is_clean(self):
        build = self.paths['candidate']['build']
        graph = build / 'build.ninja'
        graph.write_text('ninja_required_version = 1.3\nbuild all: phony\ndefault all\n',
                         encoding='utf-8')
        self.assertRegex(M['assert_no_pending_rebuild'](build), r'^[0-9a-f]{64}$')
        with self.assertRaisesRegex(M['PreflightError'], 'pending work'):
            self.artifact()
        graph.write_text(
            'ninja_required_version = 1.3\n'
            'rule stale\n  command = true\n'
            'build bench/bench_batch_append: stale bench/stale-input.c\n'
            'build all: phony\ndefault all\n', encoding='utf-8')
        (build / 'bench/stale-input.c').write_text('stale input\n', encoding='utf-8')
        with self.assertRaisesRegex(M['PreflightError'], 'pending work'):
            self.artifact()

    def test_atomic_home_artifact_refuses_existing_and_records_hashes(self):
        from unittest.mock import patch as mock_patch
        artifact = self.artifact()
        output = self.root / 'artifact'
        with mock_patch('os.fsync', wraps=os.fsync) as sync, \
             mock_patch('os.replace', wraps=os.replace) as replace:
            M['write_atomic'](output, artifact)
        self.assertGreaterEqual(sync.call_count, 3)
        replace.assert_called_once()
        stored = json.loads((output / 'execution-preflight.json').read_text(encoding='utf-8'))
        self.assertEqual(stored, artifact)
        self.assertEqual(stored['plan_sha256'], hashlib.sha256(
            self.plan_path.read_bytes()).hexdigest())
        with self.assertRaises(FileExistsError):
            M['write_atomic'](output, artifact)
        with self.assertRaisesRegex(M['PreflightError'], 'under HOME'):
            M['write_atomic'](Path('/opt/evidence'), artifact)
        base_build = Path(artifact['base']['build_root'])
        candidate_build = Path(artifact['candidate']['build_root'])
        with self.assertRaisesRegex(M['PreflightError'], 'outside build directories'):
            M['write_atomic'](base_build, artifact)
        with self.assertRaisesRegex(M['PreflightError'], 'outside build directories'):
            M['write_atomic'](base_build / 'nested-output', artifact)
        with self.assertRaisesRegex(M['PreflightError'], 'outside build directories'):
            M['write_atomic'](candidate_build.parent, artifact)


if __name__ == '__main__':
    unittest.main()
