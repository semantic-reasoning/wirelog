#!/usr/bin/env python3
"""Exercise strict evidence rejection and the hosted correctness contract."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('checker', ROOT / 'scripts/ci/check-required-correctness.py')
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)
EXECUTABLE = sys.argv.pop(1) if len(sys.argv) > 1 else None


class Evidence(unittest.TestCase):
    def setUp(self):
        self.records = [dict(name='wirelog:' + name, result='OK', returncode=0,
                             stdout='', stderr='') for name in checker.TESTS]
        self.records[0]['stdout'] = 'test_crdt_perf_gate: correctness OK\nresult = 104851 (expected 104851)'
        self.records[1]['stdout'] = 'test_cspa_perf_gate: correctness OK (tuples=20381 iters=6)'

    def validate(self, records):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'testlog.json'
            path.write_text(''.join(json.dumps(r) + '\n' for r in records), encoding='utf-8')
            checker.validate(path)

    def test_valid_names_and_gold(self):
        self.validate(self.records)
        for record in self.records:
            del record['stderr']  # Meson omits empty stderr.
            record['name'] = 'perf - ' + record['name']
        self.validate(self.records)

    def test_reject_incomplete_or_unsuccessful_evidence(self):
        cases = [self.records[:-1], self.records + [self.records[0]],
                 self.records + [dict(name='wirelog:other', result='OK', returncode=0)]]
        for field, value in [('result', 'SKIP'), ('result', 'FAIL'), ('returncode', 1),
                             ('returncode', False), ('stderr', None),
                             ('name', 'crdt_correctness_full'), ('stdout', ''),
                             ('stdout', 'median_ms=1')]:
            records = copy.deepcopy(self.records)
            records[0][field] = value
            cases.append(records)
        records = copy.deepcopy(self.records)
        records[1]['stdout'] = 'test_cspa_perf_gate: correctness OK (tuples=20380 iters=6)'
        cases.append(records)
        for records in cases:
            with self.subTest(records=records), self.assertRaises(ValueError):
                self.validate(records)

    def test_malformed_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'testlog.json'
            path.write_text('{broken\n', encoding='utf-8')
            with self.assertRaises(ValueError):
                checker.validate(path)

    def test_workflow_has_exact_correctness_surface(self):
        workflow = (ROOT / '.github/workflows/perf-suite-required.yml').read_text(encoding='utf-8')
        command = workflow.split('meson test -C build-perf', 1)[1].split('\n          python3', 1)[0]
        selected = command.replace('\\\n', ' ').split()
        self.assertEqual([word for word in selected if word in checker.TESTS], list(checker.TESTS))
        paths = re.findall(r"      - '([^']+)'", workflow)
        self.assertEqual(paths[:4],
                         ['wirelog/columnar/ops.c', 'wirelog/columnar/internal.h',
                          '.github/workflows/perf-suite-required.yml', 'tests/test_crdt_perf_gate.c'])
        # Issue #2084: the compaction code lives in merge.c; the coverage of
        # the rest of the surface is proven by test-branch-protection-contract.py.
        self.assertIn('wirelog/columnar/merge.c', paths)
        self.assertIn('name: Perf Suite Required', workflow)
        self.assertIn('  perf-gate:', workflow)
        self.assertIn('name: Perf Suite (col_rel_compact_runs / heap surfaces)', workflow)
        self.assertIn('runs-on: ubuntu-latest', workflow)
        for forbidden in ('--suite perf', 'WIRELOG_PERF_GATE', 'taskset', 'cpupower', 'linux-tools'):
            self.assertNotIn(forbidden, workflow)
        self.assertIn('WIRELOG_CRDT_FULL_CORRECTNESS', workflow)
        self.assertIn('--no-rebuild', workflow)
        self.assertIn('performance_status=not_evaluated', workflow)
        meson = (ROOT / 'tests/meson.build').read_text(encoding='utf-8')
        self.assertNotIn('WIRELOG_CRDT_FULL_CORRECTNESS=1', meson)

    @unittest.skipUnless(EXECUTABLE, 'compiled runtime fixture supplied by Meson')
    def test_runtime_opt_in_is_scoped_to_correctness_only(self):
        base = {k: v for k, v in os.environ.items() if not k.startswith('WIRELOG_')}
        for settings in ({'WIRELOG_GATE_CORRECTNESS_ONLY': '1'},
                         {'WIRELOG_CRDT_FULL_CORRECTNESS': '1'}):
            result = subprocess.run([EXECUTABLE], env=base | settings,
                                    capture_output=True, text=True, encoding='utf-8', timeout=10)
            self.assertEqual(result.returncode, 77, result.stderr)


if __name__ == '__main__':
    unittest.main()
