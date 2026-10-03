#!/usr/bin/env python3
"""Provenance and frozen-schedule contract for the batch-append plan builder."""
import hashlib
import json
import os
from pathlib import Path
import runpy
import subprocess
import tempfile
import unittest
from unittest.mock import patch

M = runpy.run_path(str(Path(__file__).with_name('prepare-batch-append-campaign.py')))


class PreparePlanTests(unittest.TestCase):
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
        self.fallback_patcher.start()
        self.addCleanup(self.fallback_patcher.stop)
        self.overlay_paths = M['OVERLAY_PATHS']
        self.source_data = {'base': self.make_source('base', 'pre'),
                            'candidate': self.make_source('candidate', 'post')}
        patch_path = self.root / 'overlay.patch'
        patch_path.write_bytes(self.git_bytes(self.source_data['base'],
                                              'diff', '--binary', 'HEAD',
                                              '--', *self.overlay_paths))
        self.patch = patch_path
        self.args = dict(mode='comparison', seed='unit-test-seed',
                         base_source=self.source_data['base'],
                         candidate_source=self.source_data['candidate'], aa_product=None,
                         overlay_patch=patch_path, output_dir=self.root / 'out')

    @staticmethod
    def git(source, *args):
        return subprocess.check_output(['git', '-C', str(source), *args],
                                       text=True, encoding='utf-8').strip()

    @staticmethod
    def git_bytes(source, *args):
        return subprocess.check_output(['git', '-C', str(source), *args])

    def make_source(self, name, product):
        source = self.root / f'{name}-source'
        source.mkdir()
        self.git(source, 'init', '-q')
        self.git(source, 'config', 'user.name', 'Plan Test')
        self.git(source, 'config', 'user.email', 'plan@example.invalid')
        git_dir = Path(self.git(source, 'rev-parse', '--git-dir'))
        if not git_dir.is_absolute():
            git_dir = (source / git_dir).resolve()
        alternates = git_dir / 'objects/info/alternates'
        revision = M['load_revision_manifest']()[0]
        anchor = revision['anchor']['commit']
        anchor_ref = M['ANCHOR_REF']
        self.git(source, 'fetch', '--no-tags', str(M['ANCHOR_BUNDLE_PATH']), anchor_ref)
        fetched_commit = self.git(source, 'rev-parse', 'FETCH_HEAD')
        fetched_tree = self.git(source, 'rev-parse', 'FETCH_HEAD^{tree}')
        self.assertEqual(fetched_commit, anchor)
        self.assertEqual(fetched_tree, revision['anchor']['tree'])
        self.assertFalse(alternates.exists())
        self.git(source, 'cat-file', '-e', f'{anchor}^{{commit}}')
        self.git(source, 'update-ref', 'refs/heads/fixture', anchor)
        self.git(source, 'checkout', '-q', '--detach', anchor)
        if product == 'post':
            tree = revision['product']['post_tree']
            synthetic = self.git(source, 'commit-tree', tree, '-p', anchor,
                                 '-m', 'synthetic product')
            self.git(source, 'update-ref', 'refs/heads/fixture', synthetic)
            self.git(source, 'checkout', '-q', '--detach', synthetic)
        self.add_overlay(source)
        for ignored in M['FALLBACKS']['IGNORED_ROOTS']:
            (source / ignored.rstrip('/')).mkdir(parents=True, exist_ok=True)
        return source

    def add_overlay(self, source):
        contents = {
            'bench/bench_batch_append.c': '/* benchmark fixture */\n',
            'bench/meson.build': 'benchmark target fixture\n',
            'tests/meson.build': 'smoke test fixture\n',
            'tests/test_bench_batch_append.c': '/* smoke fixture */\n',
        }
        for path, text in contents.items():
            file_path = source / path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(text, encoding='utf-8')
        self.git(source, 'add', *self.overlay_paths)

    def plan(self, **overrides):
        values = dict(self.args)
        values.update(overrides)
        return M['build_plan'](type('Args', (), values)())

    @staticmethod
    def all_keys(value):
        if isinstance(value, dict):
            result = set(value)
            for item in value.values():
                result.update(PreparePlanTests.all_keys(item))
            return result
        if isinstance(value, list):
            result = set()
            for item in value:
                result.update(PreparePlanTests.all_keys(item))
            return result
        return set()

    def test_frozen_schedule_is_reproducible_balanced_and_adjacent(self):
        plan = self.plan()
        repeated = self.plan()
        launches = plan['schedule']['launches']
        self.assertEqual(launches, repeated['schedule']['launches'])
        self.assertEqual(len(launches), 108)
        self.assertEqual([item['launch_index'] for item in launches], list(range(108)))
        self.assertEqual(plan['schedule']['warmups_per_process'], 2)
        self.assertEqual(plan['schedule']['measured_samples_per_process'], 1)
        self.assertEqual(plan['scope']['benchmark_launches_performed'], 0)
        self.assertNotIn('performance_verdict', self.all_keys(plan))
        pairs = [launches[index:index + 2] for index in range(0, len(launches), 2)]
        self.assertEqual(len(pairs), 54)
        for first, second in pairs:
            self.assertEqual(first['pair_id'], second['pair_id'])
            self.assertEqual(first['case'], second['case'])
            self.assertNotEqual(first['side'], second['side'])
        for case in M['CASES']:
            observed = [pair[0] for pair in pairs if pair[0]['case'] == case]
            self.assertEqual(len(observed), 18)
            self.assertEqual(sum(row['pair_order'] == 'AB' for row in observed), 9)
            self.assertEqual(sum(row['pair_order'] == 'BA' for row in observed), 9)
        self.assertNotEqual(launches, M['schedule']('another-seed'))

    def test_plan_records_manifest_checkout_product_overlay_and_execution_identity(self):
        plan = self.plan()
        self.assertEqual(plan['schema'], M['SCHEMA'])
        self.assertEqual(plan['schema_version'], 3)
        self.assertEqual(plan['benchmark_output_contract'], M['OUTPUT_CONTRACT'])
        self.assertNotIn('performance_verdict', self.all_keys(plan))
        expected_hash = hashlib.sha256(self.patch.read_bytes()).hexdigest()
        self.assertEqual(plan['overlay']['sha256'], expected_hash)
        self.assertEqual(plan['revision_manifest']['sha256'], M['MANIFEST_SHA256'])
        base, candidate = plan['sources']['base'], plan['sources']['candidate']
        self.assertEqual(base['checkout_kind'], 'direct_anchor')
        self.assertEqual(base['checkout_commit'], plan['revision_manifest']['anchor']['commit'])
        self.assertEqual(base['checkout_tree'], plan['revision_manifest']['product']['pre_tree'])
        self.assertEqual(candidate['checkout_kind'], 'synthetic_product')
        self.assertEqual(candidate['checkout_tree'], plan['revision_manifest']['product']['post_tree'])
        self.assertEqual(base['execution_tree'], self.git(self.source_data['base'], 'write-tree'))
        self.assertNotEqual(base['execution_tree'], base['checkout_tree'])
        self.assertEqual(candidate['product_delta'], base['product_delta'])
        self.assertEqual(base['product_tree'], plan['revision_manifest']['product']['pre_tree'])
        self.assertEqual(candidate['product_tree'], plan['revision_manifest']['product']['post_tree'])
        helpers = plan['unchanged_helpers']
        self.assertEqual(tuple(helpers), M['HELPER_PATHS'])
        self.assertEqual(base['helper_sources'], helpers)
        self.assertEqual(candidate['helper_sources'], helpers)
        for metadata in helpers.values():
            self.assertTrue(metadata['git_blob_oid'])
            self.assertEqual(len(metadata['sha256']), 64)
        cases = plan['benchmark_case_contract']
        self.assertIn('unresolved until later baseline calibration',
                      cases['iteration_counts'])
        self.assertEqual(cases['cases'], [
            dict(name='1x1', ncols=1, rows_per_call=1, capacity=512,
                 default_iterations_start=2000000,
                 iterations_status='starting point; unresolved until baseline calibration'),
            dict(name='1x256', ncols=1, rows_per_call=256, capacity=512,
                 default_iterations_start=10000,
                 iterations_status='starting point; unresolved until baseline calibration'),
            dict(name='32x256', ncols=32, rows_per_call=256, capacity=512,
                 default_iterations_start=10000,
                 iterations_status='starting point; unresolved until baseline calibration'),
        ])

    def test_mode_identity_is_symmetric_for_comparison_and_aa(self):
        for product in ('pre', 'post'):
            base = self.make_source(f'aa-{product}-base', product)
            candidate = self.make_source(f'aa-{product}-candidate', product)
            aa = self.plan(mode='aa_control', aa_product=product,
                           base_source=base, candidate_source=candidate)
            self.assertEqual(aa['aa_product'], product)
            self.assertEqual(aa['sources']['base']['product_tree'],
                             aa['sources']['candidate']['product_tree'])
        comparison = self.plan()
        self.assertNotEqual(comparison['sources']['base']['product_tree'],
                            comparison['sources']['candidate']['product_tree'])
        with self.assertRaisesRegex(M['PlanError'], 'explicitly select'):
            self.plan(mode='aa_control')

    def test_rejects_different_fallback_dependency_identities_between_sides(self):
        original = M['FALLBACKS']['validate_fallbacks']
        calls = 0

        def side_specific(root):
            nonlocal calls
            value = original(root)
            calls += 1
            if calls == 2:
                value = json.loads(json.dumps(value))
                value['nanoarrow']['manifest_sha256'] = 'e' * 64
            return value

        with patch.dict(M['FALLBACKS'], validate_fallbacks=side_specific):
            with self.assertRaisesRegex(M['PlanError'], 'fallback dependency identities differ'):
                self.plan()

    def test_rejects_mismatched_overlay_and_dirty_worktree(self):
        source = self.source_data['candidate']
        (source / 'bench/meson.build').write_text('tampered\n', encoding='utf-8')
        self.git(source, 'add', 'bench/meson.build')
        with self.assertRaisesRegex(M['PlanError'], 'does not exactly match'):
            self.plan()
        (source / 'bench/meson.build').write_text('benchmark target fixture\n', encoding='utf-8')
        self.git(source, 'add', 'bench/meson.build')
        (source / 'README.local').write_text('untracked\n', encoding='utf-8')
        with self.assertRaisesRegex(M['PlanError'], 'untracked'):
            self.plan()

    def test_rejects_wrong_parent_count_and_product_tree(self):
        revision = M['load_revision_manifest']()[0]
        anchor = revision['anchor']['commit']
        pre_tree = revision['product']['pre_tree']
        post_tree = revision['product']['post_tree']
        for case in ('wrong-parent', 'merge-parent-count', 'wrong-product-tree'):
            source = self.make_source(f'invalid-{case}', 'post')
            if case == 'wrong-parent':
                intervening = self.git(source, 'commit-tree', pre_tree, '-p', anchor,
                                       '-m', 'intervening')
                commit = self.git(source, 'commit-tree', post_tree, '-p', intervening,
                                  '-m', 'wrong parent')
            elif case == 'merge-parent-count':
                intervening = self.git(source, 'commit-tree', pre_tree, '-p', anchor,
                                       '-m', 'second parent')
                commit = self.git(source, 'commit-tree', post_tree, '-p', anchor,
                                  '-p', intervening, '-m', 'merge')
            else:
                commit = self.git(source, 'commit-tree', pre_tree, '-p', anchor,
                                  '-m', 'wrong product tree')
            self.git(source, 'update-ref', 'refs/heads/fixture', commit)
            self.git(source, 'checkout', '-q', '--force', '--detach', commit)
            self.add_overlay(source)
            with self.subTest(case=case), self.assertRaises(M['PlanError']):
                self.plan(candidate_source=source)

    def test_rejects_manifest_tamper_and_product_overlay_path_overlap(self):
        from unittest.mock import patch
        globals_dict = M['load_revision_manifest'].__globals__
        with patch.dict(globals_dict, MANIFEST_SHA256='0' * 64):
            with self.assertRaisesRegex(M['PlanError'], 'pinned hash'):
                self.plan()
        untracked = self.root / 'untracked-manifest.json'
        untracked.write_bytes(M['MANIFEST_PATH'].read_bytes())
        with patch.dict(globals_dict, MANIFEST_PATH=untracked):
            with self.assertRaisesRegex(M['PlanError'], 'must be tracked'):
                self.plan()
        with patch.dict(M['load_revision_manifest'].__globals__, OVERLAY_PATHS=M['OVERLAY_PATHS'] +
                        ('wirelog/columnar/relation.c',)):
            with self.assertRaisesRegex(M['PlanError'], 'overlap'):
                M['load_revision_manifest']()

        json_module = globals_dict['json']
        original_loads = json_module.loads
        def bad_paths(payload):
            value = original_loads(payload)
            value['product']['paths'][0] = 'wirelog/columnar/not_relation.c'
            return value
        with patch.object(json_module, 'loads', side_effect=bad_paths):
            with self.assertRaisesRegex(M['PlanError'], 'identities or exact paths'):
                M['load_revision_manifest']()

        subprocess_module = globals_dict['subprocess']
        original_run = subprocess_module.run
        def bad_status(command, *args, **kwargs):
            result = original_run(command, *args, **kwargs)
            if 'diff' in command and '--name-status' in command:
                return subprocess_module.CompletedProcess(
                    command, 0, stdout='M\twirelog/columnar/relation.c\n', stderr='')
            return result
        with patch.object(subprocess_module, 'run', side_effect=bad_status):
            with self.assertRaisesRegex(M['PlanError'], 'path/status/hash'):
                M['load_revision_manifest']()

    def test_rejects_extra_product_tree_path(self):
        from unittest.mock import patch
        subprocess_module = M['load_revision_manifest'].__globals__['subprocess']
        original_run = subprocess_module.run
        def extra_path(command, *args, **kwargs):
            result = original_run(command, *args, **kwargs)
            if 'diff' in command and '--name-status' in command:
                return subprocess_module.CompletedProcess(
                    command, 0,
                    stdout=('M\ttests/test_relation_generations.c\n'
                            'M\twirelog/columnar/relation.c\n'
                            'A\tunexpected/product.c\n'), stderr='')
            return result
        with patch.object(subprocess_module, 'run', side_effect=extra_path):
            with self.assertRaisesRegex(M['PlanError'], 'path/status/hash'):
                M['load_revision_manifest']()

    def test_writes_durable_plan_only_to_fresh_directory(self):
        from unittest.mock import patch
        plan = self.plan()
        with patch('os.fsync', wraps=os.fsync) as sync, \
             patch('os.replace', wraps=os.replace) as replace:
            M['write_fresh'](self.args['output_dir'], plan)
        self.assertGreaterEqual(sync.call_count, 3)
        replace.assert_called_once()
        output = self.args['output_dir'] / 'campaign-plan.json'
        loaded = json.loads(output.read_text(encoding='utf-8'))
        self.assertEqual(loaded, plan)
        with self.assertRaises(FileExistsError):
            M['write_fresh'](self.args['output_dir'], plan)
        with self.assertRaisesRegex(M['PlanError'], 'under HOME'):
            M['write_fresh'](Path('/opt/wirelog-evidence'), plan)

    def test_plan_publication_rejects_fallback_mutation_between_snapshots(self):
        plan = self.plan()
        original = M['FALLBACKS']['validate_fallbacks']
        calls = 0

        def drift(root):
            nonlocal calls
            result = original(root)
            calls += 1
            if calls in (3, 4):
                result = json.loads(json.dumps(result))
                result['nanoarrow']['manifest_sha256'] = 'f' * 64
            return result

        args = type('Args', (), dict(self.args))()
        output = self.root / 'drifting-plan'
        with patch.dict(M['FALLBACKS'], validate_fallbacks=drift):
            with self.assertRaisesRegex(M['PlanError'], 'changed during plan publication'):
                M['write_fresh'](output, plan, args)
        self.assertFalse(output.exists())

    def test_plan_publication_removes_artifact_when_fallback_drifts_after_replace(self):
        plan = self.plan()
        original_validate = M['FALLBACKS']['validate_fallbacks']
        original_replace = os.replace
        published = False

        def drift_after_replace(root):
            result = original_validate(root)
            if published:
                result = json.loads(json.dumps(result))
                result['xxhash']['manifest_sha256'] = 'd' * 64
            return result

        def replace_then_mark_drift(source, destination):
            nonlocal published
            original_replace(source, destination)
            published = True

        args = type('Args', (), dict(self.args))()
        output = self.root / 'post-replace-drift'
        with patch.dict(M['FALLBACKS'], validate_fallbacks=drift_after_replace), \
             patch('os.replace', side_effect=replace_then_mark_drift):
            with self.assertRaisesRegex(M['PlanError'], 'changed during plan publication'):
                M['write_fresh'](output, plan, args)
        self.assertTrue(published)
        self.assertFalse(output.exists())


if __name__ == '__main__':
    unittest.main()
