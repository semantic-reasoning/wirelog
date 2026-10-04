#!/usr/bin/env python3
"""Focused tests for the pinned #1913 source materializer."""
from pathlib import Path
import runpy
import subprocess
import tempfile
import unittest
from unittest.mock import patch


M = runpy.run_path(str(Path(__file__).with_name('materialize-1913-diagnostic.py')))
G = M['materialize'].__globals__
TEST_PATH = M['TEST_PATH']
TEST_TMP = Path.home() / '.cache' / 'wirelog-test-tmp'
TEST_TMP.mkdir(parents=True, exist_ok=True)


class MaterializerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=TEST_TMP)
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name) / 'repo'
        self.repo.mkdir()
        self.git('init', '-q')
        self.git('config', 'user.name', 'Test')
        self.git('config', 'user.email', 'test@example.invalid')
        (self.repo / 'seed.txt').write_text('seed\n', encoding='utf-8')
        self.git('add', 'seed.txt')
        self.git('commit', '-qm', 'seed')
        seed = self.git('rev-parse', 'HEAD')
        source_file = self.repo / TEST_PATH
        source_file.parent.mkdir(parents=True)
        source_file.write_text('old probe\n', encoding='utf-8')
        self.git('add', TEST_PATH)
        self.git('commit', '-qm', 'base source')
        self.base = self.git('rev-parse', 'HEAD')
        (self.repo / 'unrelated.txt').write_text('same\n', encoding='utf-8')
        self.git('add', 'unrelated.txt')
        self.git('commit', '-qm', 'second parent')
        second_parent = self.git('rev-parse', 'HEAD')
        self.git('checkout', '-q', self.base)
        self.git('commit', '--allow-empty', '-qm', 'candidate source')
        # Model the pinned candidate merge with its base as first parent.
        candidate_tree = self.git('rev-parse', 'HEAD^{tree}')
        candidate = self.git('commit-tree', candidate_tree, '-p', self.base,
                             '-p', second_parent, input='candidate merge\n')
        probe_file = Path(self.temp.name) / 'probe.c'
        probe_file.write_text('WIRELOG_CRDT_PROBE crdt_perf_gate_single_run\n', encoding='utf-8')
        probe_blob = self.git('hash-object', '-w', str(probe_file))
        self.pins = {
            'base': dict(sha=self.base, tree=self.git('rev-parse', f'{self.base}^{{tree}}'),
                         parents=(seed,)),
            'candidate': dict(sha=candidate, tree=candidate_tree,
                              parents=(self.base, second_parent)),
        }
        self.expected_old_blob = self.git('rev-parse', f'{self.base}:{TEST_PATH}')
        self.probe_blob = probe_blob

    def git(self, *args, input=None):
        result = subprocess.run(['git', '-C', str(self.repo), *args], input=input,
                                text=True if input is not None else None,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                check=True)
        return result.stdout.decode().strip() if isinstance(result.stdout, bytes) else result.stdout.strip()

    def patch_pins(self):
        return patch.dict(G, SOURCES=self.pins, PROBE_BLOB=self.probe_blob,
                          EXPECTED_OLD_TEST_BLOB=self.expected_old_blob)

    def test_wrapper_has_exact_pinned_parent_and_single_probe_change(self):
        with self.patch_pins():
            result = M['materialize'](self.repo, 'candidate')
        self.assertEqual(result['wrapper_parent'], self.pins['candidate']['sha'])
        self.assertEqual(result['changed_path'], TEST_PATH)
        self.assertEqual(result['probe_blob'], self.probe_blob)
        self.assertEqual(self.git('show', f"{result['wrapper_sha']}:{TEST_PATH}"),
                         'WIRELOG_CRDT_PROBE crdt_perf_gate_single_run')
        self.assertEqual(self.git('diff-tree', '--no-commit-id', '--name-status', '-r',
                                  result['wrapper_parent'], result['wrapper_sha']),
                         f'M\t{TEST_PATH}')
        self.assertEqual(self.git('status', '--porcelain=v1'), '')

    def test_wrong_pin_tree_rejected(self):
        bad = dict(self.pins, candidate=dict(self.pins['candidate'], tree='0' * 40))
        with patch.dict(G, SOURCES=bad, PROBE_BLOB=self.probe_blob,
                        EXPECTED_OLD_TEST_BLOB=self.expected_old_blob):
            with self.assertRaisesRegex(ValueError, 'tree or parent provenance'):
                M['materialize'](self.repo, 'candidate')

    def test_dirty_repository_rejected(self):
        (self.repo / 'untracked').write_text('no\n', encoding='utf-8')
        with self.patch_pins():
            with self.assertRaisesRegex(ValueError, 'tracked or untracked'):
                M['materialize'](self.repo, 'base')

    def test_unrecognized_side_rejected_before_git_operations(self):
        with self.assertRaisesRegex(ValueError, 'side must be'):
            M['materialize'](self.repo, 'arbitrary')


if __name__ == '__main__':
    unittest.main()
