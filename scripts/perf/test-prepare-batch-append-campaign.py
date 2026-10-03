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

M = runpy.run_path(str(Path(__file__).with_name('prepare-batch-append-campaign.py')))


class PreparePlanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=os.environ['TMPDIR'])
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.overlay_paths = M['OVERLAY_PATHS']
        self.source_data = {}
        for side in ('base', 'candidate'):
            source = self.root / f'{side}-source'
            source.mkdir()
            self.git(source, 'init', '-q')
            self.git(source, 'config', 'user.name', 'Plan Test')
            self.git(source, 'config', 'user.email', 'plan@example.invalid')
            for path in ('bench/meson.build', 'tests/meson.build'):
                (source / path).parent.mkdir(parents=True, exist_ok=True)
                (source / path).write_text('baseline\n', encoding='utf-8')
            self.git(source, 'add', '.')
            self.git(source, 'commit', '-qm', 'upstream fixture')
            self.git(source, 'commit', '--allow-empty', '-qm', 'wrapper fixture')
            wrapper = self.git(source, 'rev-parse', 'HEAD')
            upstream_tree = self.git(source, 'rev-parse', f'{wrapper}^{{tree}}')
            self.source_data[side] = (source, wrapper, upstream_tree)
        for side, (source, _wrapper, _tree) in self.source_data.items():
            self.add_overlay(source)
        patch = self.root / 'overlay.patch'
        patch.write_bytes(self.git_bytes(self.source_data['base'][0],
                                         'diff', '--binary', self.source_data['base'][1],
                                         '--', *self.overlay_paths))
        self.patch = patch
        self.args = dict(mode='comparison', seed='unit-test-seed',
                         base_source=self.source_data['base'][0],
                         base_wrapper=self.source_data['base'][1],
                         base_upstream_tree=self.source_data['base'][2],
                         candidate_source=self.source_data['candidate'][0],
                         candidate_wrapper=self.source_data['candidate'][1],
                         candidate_upstream_tree=self.source_data['candidate'][2],
                         overlay_patch=patch, output_dir=self.root / 'out')

    @staticmethod
    def git(source, *args):
        return subprocess.check_output(['git', '-C', str(source), *args],
                                       text=True, encoding='utf-8').strip()

    @staticmethod
    def git_bytes(source, *args):
        return subprocess.check_output(['git', '-C', str(source), *args])

    def add_overlay(self, source):
        contents = {
            'bench/bench_batch_append.c': '/* benchmark fixture */\n',
            'bench/meson.build': 'baseline\nbenchmark target\n',
            'tests/meson.build': 'baseline\nsmoke test\n',
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
        self.assertEqual(plan['scope']['performance_verdict'], 'not produced')
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

    def test_plan_records_wrapper_overlay_and_resulting_tree(self):
        plan = self.plan()
        self.assertEqual(plan['schema'], M['SCHEMA'])
        expected_hash = hashlib.sha256(self.patch.read_bytes()).hexdigest()
        self.assertEqual(plan['overlay']['sha256'], expected_hash)
        self.assertEqual(plan['upstream']['base']['overlay_sha256'], expected_hash)
        self.assertNotEqual(plan['upstream']['base']['measured_tree'],
                            plan['upstream']['base']['upstream_tree'])

    def test_aa_control_requires_same_upstream_tree(self):
        source = self.root / 'different-source'
        subprocess.run(['git', 'clone', '-q', str(self.source_data['base'][0]), str(source)], check=True)
        self.git(source, 'config', 'user.name', 'Plan Test')
        self.git(source, 'config', 'user.email', 'plan@example.invalid')
        (source / 'README.local').write_text('different upstream\n', encoding='utf-8')
        self.git(source, 'add', 'README.local')
        self.git(source, 'commit', '-qm', 'different upstream')
        wrapper = self.git(source, 'rev-parse', 'HEAD')
        tree = self.git(source, 'rev-parse', f'{wrapper}^{{tree}}')
        self.add_overlay(source)
        patch = self.root / 'overlay.patch'
        # The same exact patch applies because the required overlay files match.
        source_patch = self.git_bytes(source, 'diff', '--binary', wrapper, '--', *self.overlay_paths)
        self.assertEqual(source_patch, self.patch.read_bytes())
        comparison = self.plan(candidate_source=source, candidate_wrapper=wrapper,
                               candidate_upstream_tree=tree)
        self.assertNotEqual(comparison['upstream']['base']['upstream_tree'],
                            comparison['upstream']['candidate']['upstream_tree'])
        with self.assertRaisesRegex(M['PlanError'], 'identical upstream trees'):
            self.plan(mode='aa_control', candidate_source=source,
                      candidate_wrapper=wrapper, candidate_upstream_tree=tree)

    def test_rejects_mismatched_overlay_and_dirty_worktree(self):
        source = self.source_data['candidate'][0]
        (source / 'bench/meson.build').write_text('tampered\n', encoding='utf-8')
        self.git(source, 'add', 'bench/meson.build')
        with self.assertRaisesRegex(M['PlanError'], 'does not exactly match'):
            self.plan()
        (source / 'bench/meson.build').write_text('baseline\nbenchmark target\n', encoding='utf-8')
        self.git(source, 'add', 'bench/meson.build')
        (source / 'README.local').write_text('untracked\n', encoding='utf-8')
        with self.assertRaisesRegex(M['PlanError'], 'untracked'):
            self.plan()

    def test_rejects_invalid_supplied_wrapper(self):
        with self.assertRaisesRegex(M['PlanError'], 'HEAD does not match'):
            self.plan(base_wrapper='0' * 40)

    def test_writes_durable_plan_only_to_fresh_directory(self):
        from unittest.mock import patch
        plan = self.plan()
        with patch('os.fsync', wraps=os.fsync) as sync:
            M['write_fresh'](self.args['output_dir'], plan)
        self.assertGreaterEqual(sync.call_count, 3)
        output = self.args['output_dir'] / 'campaign-plan.json'
        loaded = json.loads(output.read_text(encoding='utf-8'))
        self.assertEqual(loaded, plan)
        with self.assertRaises(FileExistsError):
            M['write_fresh'](self.args['output_dir'], plan)


if __name__ == '__main__':
    unittest.main()
