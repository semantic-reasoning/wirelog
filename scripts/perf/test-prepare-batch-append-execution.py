#!/usr/bin/env python3
"""Profile-only preflight provenance, Meson, drift, and durability tests."""
import hashlib
import io
import json
import os
from pathlib import Path
import runpy
import stat
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

PERF = Path(__file__).parent
PLAN = runpy.run_path(str(PERF / 'prepare-batch-append-campaign.py'))
M = runpy.run_path(str(PERF / 'prepare-batch-append-execution.py'))
REAL_VALIDATE_FALLBACKS = M['FALLBACKS']['validate_fallbacks']
REAL_PLAN_VALIDATE_FALLBACKS = PLAN['FALLBACKS']['validate_fallbacks']


class ExecutionPreflightTests(unittest.TestCase):
    def setUp(self):
        home_tmp = Path.home() / '.tmp'
        home_tmp.mkdir(mode=0o700, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(dir=home_tmp)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.assertTrue(self.root.is_relative_to(Path.home().resolve()))
        def fake_validate(root):
            M['FALLBACKS']['outer_status'](root)
            return {'schema': M['FALLBACKS']['SCHEMA'],
                    'outer_ignored_paths': list(M['FALLBACKS']['IGNORED_ROOTS']),
                    'nanoarrow': {'commit': M['FALLBACKS']['NANO_COMMIT'],
                                  'tree': M['FALLBACKS']['NANO_TREE'],
                                  'manifest_sha256': 'a' * 64},
                    'xxhash': {'archive_sha256': M['FALLBACKS']['XX_ARCHIVE_SHA256'],
                               'manifest_sha256': 'b' * 64}}
        self.fallback_patcher = patch.dict(M['FALLBACKS'], validate_fallbacks=fake_validate)
        self.plan_fallback_patcher = patch.dict(PLAN['FALLBACKS'], validate_fallbacks=fake_validate)
        self.fallback_patcher.start()
        self.plan_fallback_patcher.start()
        self.addCleanup(self.fallback_patcher.stop)
        self.addCleanup(self.plan_fallback_patcher.stop)
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
            self.git(source, 'apply', '--index', str(PLAN['PRODUCT_PATCH_PATH']))
            tree = self.git(source, 'write-tree')
            if tree != revision['product']['post_tree']:
                raise AssertionError('fixture product patch did not reconstruct pinned tree')
            commit = self.git(source, 'commit-tree', tree,
                              '-p', anchor, '-m', 'synthetic product')
            self.git(source, 'update-ref', 'refs/heads/fixture', commit)
            self.git(source, 'checkout', '-q', '--detach', commit)
        self.write_overlay(source)
        for ignored in M['FALLBACKS']['IGNORED_ROOTS']:
            (source / ignored.rstrip('/')).mkdir(parents=True, exist_ok=True)
        return dict(source=source,
                    commit=self.git(source, 'rev-parse', 'HEAD'),
                    tree=self.git(source, 'rev-parse', 'HEAD^{tree}'),
                    product=product)

    def install_real_fallbacks(self, source):
        subprojects = PLAN['REPOSITORY_ROOT'] / 'subprojects'
        for name in ('nanoarrow', 'xxHash-0.8.4', 'packagecache'):
            target = source / 'subprojects' / name
            shutil.rmtree(target)
            shutil.copytree(subprojects / name, target, symlinks=True,
                            copy_function=shutil.copy2)

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

    def test_real_fallbacks_bind_pre_and_post_plan_and_profile(self):
        fallback_api = M['FALLBACKS']
        fake_validate = fallback_api['validate_fallbacks']
        fallback_api['validate_fallbacks'] = REAL_VALIDATE_FALLBACKS
        plan_api = PLAN['FALLBACKS']
        fake_plan_validate = plan_api['validate_fallbacks']
        plan_api['validate_fallbacks'] = REAL_PLAN_VALIDATE_FALLBACKS
        self.addCleanup(lambda: fallback_api.__setitem__('validate_fallbacks', fake_validate))
        self.addCleanup(lambda: plan_api.__setitem__('validate_fallbacks', fake_plan_validate))
        for side in ('base', 'candidate'):
            self.install_real_fallbacks(self.paths[side]['source'])
        plan = self.make_plan(self.paths['base'], self.paths['candidate'])
        self.plan_path.write_text(json.dumps(plan), encoding='utf-8')
        artifact = self.artifact()
        self.assertEqual(plan['sources']['base']['fallback_dependencies']['schema'],
                         fallback_api['SCHEMA'])
        for side in ('base', 'candidate'):
            deps = artifact['fallback_dependencies'][side]
            self.assertEqual(deps, plan['sources'][side]['fallback_dependencies'])
            self.assertEqual(deps['nanoarrow']['commit'], fallback_api['NANO_COMMIT'])
            self.assertEqual(deps['xxhash']['archive_sha256'],
                             fallback_api['XX_ARCHIVE_SHA256'])
            self.assertTrue(deps['nanoarrow']['manifest_sha256'])
            self.assertTrue(deps['xxhash']['manifest_sha256'])
        self.assertEqual(artifact['mode'], 'comparison')
        self.assertEqual(artifact['fallback_dependencies']['base']['nanoarrow']['tree'],
                         fallback_api['NANO_TREE'])
        self.assertEqual(artifact['fallback_dependencies']['candidate']['nanoarrow']['tree'],
                         fallback_api['NANO_TREE'])
        # The fixed pre and synthetic post product source profiles share the
        # same independently verified fallback identities.
        self.assertEqual(plan['sources']['base']['fallback_dependencies'],
                         plan['sources']['candidate']['fallback_dependencies'])

        nested_link = self.paths['base']['source'] / 'subprojects/nanoarrow/python/subprojects/arrow-nanoarrow'
        original_link = os.readlink(nested_link)
        nested_link.unlink()
        nested_link.symlink_to('../../../escape')
        with self.assertRaisesRegex(M['PreflightError'], 'symlink differs|escapes'):
            self.artifact()
        nested_link.unlink()
        nested_link.symlink_to(original_link)
        nested_link.unlink()
        nested_link.symlink_to('.')
        with self.assertRaisesRegex(PLAN['PlanError'], 'symlink differs|resolve to checkout root'):
            self.make_plan(self.paths['base'], self.paths['candidate'])
        nested_link.unlink()
        nested_link.symlink_to(original_link)

        def rejects(pattern=''):
            with self.assertRaises(PLAN['FALLBACKS']['FallbackError']) as caught:
                REAL_PLAN_VALIDATE_FALLBACKS(self.paths['base']['source'])
            if pattern:
                self.assertRegex(str(caught.exception), pattern)

        nested_root = self.paths['base']['source'] / 'subprojects/nanoarrow'
        regular_link = nested_root / 'python/subprojects/arrow-nanoarrow'
        regular_link.unlink()
        regular_link.write_text('../..', encoding='utf-8')
        rejects('tracked symlink')
        regular_link.unlink()
        regular_link.symlink_to('../..')
        nested_git = PLAN['FALLBACKS']['git']
        tracked = nested_git(nested_root, 'ls-files', '-s', '-z', binary=True)
        tracked_files = []
        for record in tracked.split(b'\0'):
            if not record:
                continue
            metadata, path_bytes = record.split(b'\t', 1)
            mode, _, _ = metadata.decode('ascii').split()
            if mode in ('100644', '100755'):
                tracked_files.append(path_bytes.decode('utf-8'))
        tracked_path = nested_root / tracked_files[0]
        tracked_name = tracked_files[0]
        tracked_bytes = nested_git(nested_root, 'show', f'HEAD:{tracked_name}', binary=True)
        tracked_mode = stat.S_IMODE(tracked_path.stat().st_mode)
        nested_git(nested_root, 'update-index', '--assume-unchanged', tracked_name)
        tracked_path.unlink()
        tracked_path.write_bytes(tracked_bytes + b' assume-unchanged mutation')
        tracked_path.chmod(tracked_mode)
        try:
            rejects('tracked file differs from Git blob')
        finally:
            tracked_path.unlink()
            tracked_path.write_bytes(tracked_bytes)
            tracked_path.chmod(tracked_mode)
            nested_git(nested_root, 'update-index', '--no-assume-unchanged', tracked_name)

        # Simulate mutation after the blob-verification loop but before the final
        # filesystem walk. Assume-unchanged keeps nested Git status clean, so the
        # physical walk must detect the race from exact manifest tuples.
        nested_git(nested_root, 'update-index', '--assume-unchanged', tracked_name)
        fallback_globals = REAL_PLAN_VALIDATE_FALLBACKS.__globals__
        original_walk = fallback_globals['walk_tree']
        mutated_during_walk = False

        def mutate_before_walk(root, **kwargs):
            nonlocal mutated_during_walk
            if Path(root) == nested_root and not mutated_during_walk:
                tracked_path.unlink()
                tracked_path.write_bytes(tracked_bytes + b' race mutation')
                tracked_path.chmod(tracked_mode)
                mutated_during_walk = True
            return original_walk(root, **kwargs)

        try:
            with patch.dict(fallback_globals, walk_tree=mutate_before_walk):
                rejects('exact manifest')
            self.assertTrue(mutated_during_walk)
        finally:
            if tracked_path.exists():
                tracked_path.unlink()
            tracked_path.write_bytes(tracked_bytes)
            tracked_path.chmod(tracked_mode)
            nested_git(nested_root, 'update-index', '--no-assume-unchanged', tracked_name)
        extra_link = nested_root / 'extra-link'
        extra_link.symlink_to('.')
        rejects('additional files')
        extra_link.unlink()
        marker = nested_root / '.meson-subproject-wrap-hash.txt'
        marker_bytes = marker.read_bytes()
        marker.unlink()
        marker.write_bytes(b'bad marker\n')
        rejects('marker')
        marker.unlink()
        marker.write_bytes(marker_bytes)
        marker.chmod(0o600)
        rejects('marker type/mode')
        marker.chmod(0o644)
        xx_marker = self.paths['base']['source'] / 'subprojects/xxHash-0.8.4/.meson-subproject-wrap-hash.txt'
        xx_marker_bytes = xx_marker.read_bytes()
        xx_marker.unlink()
        xx_marker.write_bytes(b'bad marker\n')
        rejects('marker')
        xx_marker.unlink()
        xx_marker.write_bytes(xx_marker_bytes)
        xx_marker.chmod(0o600)
        rejects('mode differs')
        xx_marker.chmod(0o644)

        xx_root = self.paths['base']['source'] / 'subprojects/xxHash-0.8.4'
        extracted = xx_root / 'xxhash.c'
        extracted_mode = stat.S_IMODE(extracted.stat().st_mode)
        extracted_bytes = extracted.read_bytes()
        extracted.unlink()
        extracted.write_bytes(extracted_bytes)
        extracted.chmod(extracted_mode ^ 0o100)
        rejects('mode differs')
        extracted.unlink()
        extracted.write_bytes(extracted_bytes + b' drift')
        extracted.chmod(extracted_mode)
        rejects('bytes differ')
        extracted.unlink()
        xxhash_source = PLAN['REPOSITORY_ROOT'] / 'subprojects/xxHash-0.8.4/xxhash.c'
        shutil.copy2(xxhash_source, extracted)

        archive = self.paths['base']['source'] / 'subprojects/packagecache/xxHash-0.8.4.tar.gz'
        canonical_archive = PLAN['REPOSITORY_ROOT'] / 'subprojects/packagecache/xxHash-0.8.4.tar.gz'
        cache = archive.parent
        cache.chmod(0o700)
        rejects('packagecache directory type/mode')
        cache.chmod(0o755)
        archive_bytes = archive.read_bytes()
        archive.unlink()
        archive.write_bytes(archive_bytes)
        archive.chmod(0o644)
        rejects('archive type/mode')
        archive.unlink()
        shutil.copy2(canonical_archive, archive)
        archive.unlink()
        archive.write_bytes(canonical_archive.read_bytes() + b'drift')
        archive.chmod(0o600)
        rejects('archive SHA-256')
        archive.unlink()
        shutil.copy2(canonical_archive, archive)
        extra_cache = archive.parent / 'extra.archive'
        extra_cache.write_bytes(b'extra')
        rejects('exactly the pinned regular archive')
        extra_cache.unlink()
        unrelated = self.paths['base']['source'] / 'subprojects/unrelated'
        unrelated.mkdir()
        rejects('unapproved.*path')
        unrelated.rmdir()

        patch_path = (self.paths['base']['source'] /
                      'subprojects/packagefiles/xxhash-0.8.4/meson.build')
        patch_bytes = patch_path.read_bytes()
        patch_path.write_bytes(patch_bytes + b' drift')
        rejects('unapproved.*path')
        patch_path.write_bytes(patch_bytes)

        globals_dict = REAL_PLAN_VALIDATE_FALLBACKS.__globals__
        with patch.dict(globals_dict, NANO_COMMIT='0' * 40):
            rejects('revision differs')
        with patch.dict(globals_dict, NANO_TREE='0' * 40):
            rejects('identity mismatch')
        with patch.dict(globals_dict, XX_ARCHIVE_SHA256='0' * 64):
            rejects('archive identity')

        revision = self.git(self.paths['base']['source'], 'rev-parse', 'HEAD')
        wraps = globals_dict['check_wraps'](self.paths['base']['source'], revision)
        unsafe_archives = (
            ('traversal', [('../outside', tarfile.REGTYPE, '')]),
            ('absolute', [('/outside', tarfile.REGTYPE, '')]),
            ('duplicate', [('xxHash-0.8.4/duplicate', tarfile.REGTYPE, ''),
                           ('xxHash-0.8.4/duplicate', tarfile.REGTYPE, '')]),
            ('link', [('xxHash-0.8.4/link', tarfile.SYMTYPE, '../../outside')]),
            ('special', [('xxHash-0.8.4/device', tarfile.CHRTYPE, '')]),
        )
        for case, members in unsafe_archives:
            payload = io.BytesIO()
            with tarfile.open(fileobj=payload, mode='w:gz') as archive_file:
                root_info = tarfile.TarInfo('xxHash-0.8.4/')
                root_info.type = tarfile.DIRTYPE
                root_info.mode = 0o755
                archive_file.addfile(root_info)
                for member_name, member_type, linkname in members:
                    info = tarfile.TarInfo(member_name)
                    info.type = member_type
                    info.mode = 0o644
                    info.linkname = linkname
                    data = b'x' if member_type == tarfile.REGTYPE else b''
                    info.size = len(data)
                    archive_file.addfile(info, io.BytesIO(data) if data else None)
            archive_path = self.root / 'unsafe.tar.gz'
            archive_path.write_bytes(payload.getvalue())
            wrapped = json.loads(json.dumps(wraps))
            wrapped['xxhash']['archive_sha256'] = hashlib.sha256(
                payload.getvalue()).hexdigest()
            with self.subTest(unsafe_archive=case), \
                 patch.dict(globals_dict, XX_ARCHIVE_SHA256=wrapped['xxhash']['archive_sha256']):
                with self.assertRaises(PLAN['FALLBACKS']['FallbackError']):
                    globals_dict['archive_expected'](
                        self.paths['base']['source'], revision, archive_path, wrapped)

        marker_bytes = marker.read_bytes()
        calls = 0
        def mutate_after_initial_snapshot(root):
            nonlocal calls
            result = REAL_VALIDATE_FALLBACKS(root)
            calls += 1
            if calls == 6:
                marker.unlink()
                marker.write_bytes(b'mutated between snapshots\n')
            return result
        with patch.dict(M['FALLBACKS'], validate_fallbacks=mutate_after_initial_snapshot):
            with self.assertRaisesRegex(M['PreflightError'], 'marker'):
                self.artifact()
        marker.unlink()
        marker.write_bytes(marker_bytes)

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
        with self.assertRaisesRegex(M['PreflightError'], 'unapproved.*path'):
            self.artifact()

    def test_rejects_shallow_and_ignored_or_untracked_sources(self):
        source = self.paths['candidate']['source']
        ignored = source / 'ignored-local-file'
        ignored.write_text('must reject\n', encoding='utf-8')
        with self.assertRaisesRegex(M['PreflightError'], 'unapproved.*path'):
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
            with self.assertRaisesRegex(M['PreflightError'], 'unapproved.*path'):
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

    def test_atomic_artifact_is_removed_when_fallback_drifts_after_replace(self):
        from unittest.mock import patch as mock_patch
        artifact = self.artifact()
        output = self.root / 'post-replace-fallback-drift'
        original_replace = os.replace
        published = False

        def drift_after_replace(root):
            module = M['FALLBACKS']
            module['outer_status'](root)
            result = {'schema': module['SCHEMA'],
                      'outer_ignored_paths': list(module['IGNORED_ROOTS']),
                      'nanoarrow': {'commit': module['NANO_COMMIT'],
                                    'tree': module['NANO_TREE'],
                                    'manifest_sha256': 'a' * 64},
                      'xxhash': {'archive_sha256': module['XX_ARCHIVE_SHA256'],
                                 'manifest_sha256': 'b' * 64}}
            if published:
                result['nanoarrow']['manifest_sha256'] = 'c' * 64
            return result

        # The plan loader and profile loader use separate runpy namespaces.
        def plan_drift_after_replace(root):
            module = PLAN['FALLBACKS']
            module['outer_status'](root)
            result = {'schema': module['SCHEMA'],
                      'outer_ignored_paths': list(module['IGNORED_ROOTS']),
                      'nanoarrow': {'commit': module['NANO_COMMIT'],
                                    'tree': module['NANO_TREE'],
                                    'manifest_sha256': 'a' * 64},
                      'xxhash': {'archive_sha256': module['XX_ARCHIVE_SHA256'],
                                 'manifest_sha256': 'b' * 64}}
            if published:
                result['nanoarrow']['manifest_sha256'] = 'c' * 64
            return result

        def replace_then_mark_drift(source, destination):
            nonlocal published
            original_replace(source, destination)
            published = True

        with mock_patch.dict(M['FALLBACKS'], validate_fallbacks=drift_after_replace), \
             mock_patch.dict(PLAN['FALLBACKS'],
                             validate_fallbacks=plan_drift_after_replace), \
             mock_patch('os.replace', side_effect=replace_then_mark_drift):
            with self.assertRaisesRegex(M['PreflightError'], 'provenance'):
                M['write_atomic'](output, artifact)
        self.assertTrue(published)
        self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
