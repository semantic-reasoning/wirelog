#!/usr/bin/env python3
"""Mutation tests for fail-closed #1913 diagnostic evidence validation."""
import copy
import hashlib
import json
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


M = runpy.run_path(str(Path(__file__).with_name('validate-1913-diagnostic.py')))
G = M['validate'].__globals__
MAT = G['MATERIALIZER']
MAT_G = MAT['materialize'].__globals__
TEST_PATH = M['TEST_PATH']
TIMER_SOURCES = M['TIMER_SOURCES']
TEST_TMP = Path.home() / '.cache' / 'wirelog-test-tmp'
TEST_TMP.mkdir(parents=True, exist_ok=True)


class ValidatorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=TEST_TMP)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / 'repo'
        self.repo.mkdir()
        self.git('init', '-q')
        self.git('config', 'user.name', 'Test')
        self.git('config', 'user.email', 'test@example.invalid')
        (self.repo / 'seed').write_text('seed\n', encoding='utf-8')
        self.git('add', 'seed')
        self.git('commit', '-qm', 'seed')
        seed = self.git('rev-parse', 'HEAD')
        for source in TIMER_SOURCES:
            source_path = self.repo / source
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text(f'fixture source {source}\n', encoding='utf-8')
        test_path = self.repo / TEST_PATH
        test_path.parent.mkdir(parents=True, exist_ok=True)
        test_path.write_text('old probe\n', encoding='utf-8')
        self.git('add', *TIMER_SOURCES)
        self.git('commit', '-qm', 'baseline')
        base = self.git('rev-parse', 'HEAD')
        (self.repo / 'second-parent').write_text('second\n', encoding='utf-8')
        self.git('add', 'second-parent')
        self.git('commit', '-qm', 'second parent')
        second_parent = self.git('rev-parse', 'HEAD')
        candidate_tree = self.git('rev-parse', f'{base}^{{tree}}')
        candidate = self.git('commit-tree', candidate_tree, '-p', base,
                             '-p', second_parent, input='candidate\n')
        probe_path = self.root / 'probe.c'
        probe_path.write_text('WIRELOG_CRDT_PROBE crdt_perf_gate_single_run\n', encoding='utf-8')
        probe_blob = self.git('hash-object', '-w', str(probe_path))
        (self.repo / 'probe.c').write_text(probe_path.read_text(encoding='utf-8'), encoding='utf-8')
        self.git('add', 'probe.c')
        self.git('commit', '-qm', 'probe source')
        self.pins = {
            'base': dict(sha=base, tree=self.git('rev-parse', f'{base}^{{tree}}'),
                         parents=(seed,)),
            'candidate': dict(sha=candidate, tree=candidate_tree,
                              parents=(base, second_parent)),
        }
        self.old_blob = self.git('rev-parse', f'{base}:{TEST_PATH}')
        self.probe_blob = probe_blob
        self.materialization_dir = self.root / 'materialization'
        self.evidence_dir = self.root / 'evidence'
        self.materialization_dir.mkdir()
        self.evidence_dir.mkdir()
        with patch.dict(MAT, SOURCES=self.pins, PROBE_BLOB=probe_blob,
                        EXPECTED_OLD_TEST_BLOB=self.old_blob), \
             patch.dict(MAT_G, SOURCES=self.pins, PROBE_BLOB=probe_blob,
                        EXPECTED_OLD_TEST_BLOB=self.old_blob):
            self.materializations = {
                side: MAT['materialize'](self.repo, side) for side in ('base', 'candidate')
            }
        self.pin_patches = (patch.dict(MAT, SOURCES=self.pins, PROBE_BLOB=probe_blob,
                                       EXPECTED_OLD_TEST_BLOB=self.old_blob),
                            patch.dict(MAT_G, SOURCES=self.pins, PROBE_BLOB=probe_blob,
                                       EXPECTED_OLD_TEST_BLOB=self.old_blob))
        for pin_patch in self.pin_patches:
            pin_patch.start()
            self.addCleanup(pin_patch.stop)
        self.addCleanup(self.temp.cleanup)
        for side, record in self.materializations.items():
            self.write(self.materialization_dir / f'{side}.json', record)
        self.make_evidence()

    def git(self, *args, input=None):
        result = subprocess.run(['git', '-C', str(self.repo), *args], input=input,
                                text=input is not None, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, check=True)
        return result.stdout.decode().strip() if isinstance(result.stdout, bytes) else result.stdout.strip()

    @staticmethod
    def write(path, value):
        path.write_text(json.dumps(value, sort_keys=True, indent=2) + '\n', encoding='utf-8')

    def timer_hashes(self, wrapper):
        return {name: hashlib.sha256(self.git_bytes('show', f'{wrapper}:{name}')).hexdigest()
                for name in TIMER_SOURCES}

    def git_bytes(self, *args):
        return subprocess.check_output(['git', '-C', str(self.repo), *args],
                                       stderr=subprocess.PIPE)

    def make_evidence(self):
        sources = {side: self.materializations[side]['wrapper_sha']
                   for side in ('base', 'candidate')}
        profiles = {side: {'buildtype': 'release'} for side in ('base', 'candidate')}
        build_provenance = {
            side: dict(build_instance_id='build-' + side, source_sha=sources[side],
                       profile=profiles[side], build_log_sha256=('a' if side == 'base' else 'b') * 64)
            for side in ('base', 'candidate')
        }
        timer = self.timer_hashes(sources['base'])
        self.mat_base = copy.deepcopy(self.materializations['base'])
        records = {}
        for side in ('base', 'candidate'):
            source_hashes = dict(timer)
            source_hashes['tests/test_cspa_perf_gate.c'] = 'c' * 64
            records[side] = dict(
                source_sha=sources[side],
                source_tree=self.materializations[side]['wrapper_tree'],
                source_sha256=source_hashes,
                timer_contract_sha256=timer,
            )
        workloads = {
            'crdt': dict(binary_sha256={'base': 'd' * 64, 'candidate': 'e' * 64},
                         fixture_sha256={'base': {'crdt': 'f' * 64},
                                         'candidate': {'crdt': 'f' * 64}},
                         timer=M['CRDT_TIMER'], expected=M['EXPECTED']['crdt']),
            'cspa-fast': dict(binary_sha256={'base': '1' * 64, 'candidate': '2' * 64},
                              fixture_sha256={'base': {'cspa': '3' * 64},
                                              'candidate': {'cspa': '3' * 64}},
                              timer=M['CSPA_TIMER'], expected=M['EXPECTED']['cspa-fast']),
        }
        manifest = dict(sources=sources, profiles=profiles, workloads=workloads,
                        host_id='host', cpu=0, build_provenance=build_provenance)
        campaign = dict(schema_version=1, campaign_id='campaign-1913', manifest=manifest,
                        campaign_mode='comparison', blocks=[dict(block_id='AB', order='AB'),
                                                            dict(block_id='BA', order='BA')],
                        attempts=[])
        host = {name: dict(value=None, unavailable_reason='fixture')
                for name in ('load_one', 'cpu_pressure_some_total_us',
                             'cgroup_throttled_us', 'frequency_khz', 'governor')}
        for order in ('AB', 'BA'):
            for workload, spec in workloads.items():
                side_order = ('base', 'candidate') if order == 'AB' else ('candidate', 'base')
                for sequence in range(20):
                    campaign['attempts'].append(dict(
                        campaign_id=campaign['campaign_id'], manifest_sha256=M['EVALUATOR']['digest'](manifest),
                        block_id=order, workload=workload, sequence=sequence,
                        phase='warmup' if sequence < 2 else 'sample',
                        pair_index=None if sequence < 2 else (sequence - 2) // 2,
                        side=side_order[sequence % 2], elapsed_ms=1.0, exit_code=0,
                        timed_out=False, correctness=dict(status='OK', observed=spec['expected']),
                        host_before=host, host_after=host))
        campaign_path = self.evidence_dir / 'campaign-v1.json'
        self.write(campaign_path, campaign)
        evaluation = M['EVALUATOR']['evaluate'](
            campaign, hashlib.sha256(campaign_path.read_bytes()).hexdigest())
        self.assertEqual(evaluation['status'], 'COMPLETE_VALID', evaluation)
        preflight = dict(schema_version=1, mode='comparison', qualify_aa=False,
                         campaign_id=campaign['campaign_id'], manifest=manifest,
                         records=records, artifacts={}, paths={}, schedule=[],
                         theoretical_launch_timeout_ceiling_seconds=80 * 180,
                         launcher_sha256=None)
        self.write(self.evidence_dir / 'preflight.json', preflight)
        self.write(self.evidence_dir / 'collection-status.json',
                   dict(status='COMPLETE_VALID', evaluator_exit=0))
        self.write(self.evidence_dir / 'evaluation-report.json', evaluation)

    def load(self, name):
        return json.loads((self.evidence_dir / name).read_text(encoding='utf-8'))

    def save(self, name, value):
        self.write(self.evidence_dir / name, value)

    def assert_rejected(self):
        with self.assertRaises((KeyError, OSError, subprocess.CalledProcessError,
                                TypeError, ValueError)):
            M['validate'](self.repo, self.materialization_dir, self.evidence_dir)

    def test_valid_completed_pair(self):
        report = M['validate'](self.repo, self.materialization_dir, self.evidence_dir)
        self.assertEqual(report['status'], 'VALID')
        self.assertEqual(report['campaign_evaluation'], 'COMPLETE_VALID')

    def test_swapped_side_records_rejected(self):
        base = self.materialization_dir / 'base.json'
        candidate = self.materialization_dir / 'candidate.json'
        base_value, candidate_value = json.loads(base.read_text()), json.loads(candidate.read_text())
        self.write(base, candidate_value)
        self.write(candidate, base_value)
        self.assert_rejected()

    def test_altered_wrapper_source_probe_and_tree_rejected(self):
        mutations = (
            ('wrapper_sha', '0' * 40), ('source_sha', '1' * 40),
            ('probe_blob', '2' * 40), ('wrapper_tree', '3' * 40),
            ('wrapper_parent', '4' * 40),
        )
        for field, value in mutations:
            with self.subTest(field=field):
                record = json.loads((self.materialization_dir / 'base.json').read_text())
                record[field] = value
                self.write(self.materialization_dir / 'base.json', record)
                self.assert_rejected()
                self.write(self.materialization_dir / 'base.json', self.mat_base)

    def test_missing_materialization_or_manifest_rejected(self):
        candidate = self.materialization_dir / 'candidate.json'
        saved = candidate.read_text(encoding='utf-8')
        candidate.unlink()
        self.assert_rejected()
        candidate.write_text(saved, encoding='utf-8')
        for filename in ('campaign-v1.json', 'preflight.json'):
            path = self.evidence_dir / filename
            saved = path.read_text(encoding='utf-8')
            document = json.loads(saved)
            document.pop('manifest', None)
            self.write(path, document)
            self.assert_rejected()
            path.write_text(saved, encoding='utf-8')

    def test_timer_identity_mismatch_rejected(self):
        preflight = self.load('preflight.json')
        preflight['records']['candidate']['timer_contract_sha256'][TEST_PATH] = '9' * 64
        self.save('preflight.json', preflight)
        self.assert_rejected()

    def test_missing_and_partial_collector_outputs_rejected(self):
        status = self.evidence_dir / 'collection-status.json'
        saved_status = status.read_text(encoding='utf-8')
        status.unlink()
        self.assert_rejected()
        status.write_text(saved_status, encoding='utf-8')
        campaign = self.load('campaign-v1.json')
        campaign['attempts'].pop()
        self.save('campaign-v1.json', campaign)
        self.assert_rejected()

    def test_malformed_document_roots_write_structured_invalid_report(self):
        cases = (
            ('campaign-v1.json', []),
            ('collection-status.json', []),
            ('evaluation-report.json', []),
        )
        for filename, malformed in cases:
            with self.subTest(filename=filename):
                path = self.evidence_dir / filename
                original = path.read_text(encoding='utf-8')
                path.write_text(json.dumps(malformed), encoding='utf-8')
                output = self.root / (filename + '.invalid.json')
                result = subprocess.run([
                    sys.executable,
                    str(Path(__file__).with_name('validate-1913-diagnostic.py')),
                    '--repository', str(self.repo),
                    '--materialization-dir', str(self.materialization_dir),
                    '--evidence-dir', str(self.evidence_dir),
                    '--output', str(output),
                ], capture_output=True, text=True, encoding='utf-8')
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertEqual(json.loads(output.read_text(encoding='utf-8'))['status'], 'INVALID')
                path.write_text(original, encoding='utf-8')


if __name__ == '__main__':
    unittest.main()
