#!/usr/bin/env python3
"""Require complete, successful Meson correctness evidence for the hosted job."""
import json
from pathlib import Path
import re
import sys

TESTS = ('crdt_correctness_full', 'cspa_correctness', 'consolidate_kway_merge',
         'compaction', 'k_fusion_merge', 'tdd_sorted_merge_atomic', 'radix_sort',
         'lftj', 'lftj_integration')


def validate(path: Path) -> None:
    records = {}
    for line in path.read_text(encoding='utf-8').splitlines():
        record = json.loads(line)
        if not isinstance(record, dict):
            raise ValueError('record must be an object')
        # Meson 1.12 renders names as "suite - project:test", or "project:test".
        name = record.get('name', '')
        if not isinstance(name, str):
            raise ValueError('invalid test name')
        match = re.fullmatch(r'(?:[^\n]+ - )?wirelog:([a-z_]+)', name)
        if not match:
            raise ValueError(f'unrecognized Meson name: {name}')
        name = match.group(1)
        if name not in TESTS or name in records:
            raise ValueError(f'unexpected or duplicate test: {name}')
        if (record.get('result') != 'OK'
                or type(record.get('returncode')) is not int
                or record['returncode'] != 0):
            raise ValueError(f'unsuccessful test: {name}')
        if not all(isinstance(record.get(key, ''), str) for key in ('stdout', 'stderr')):
            raise ValueError(f'malformed output: {name}')
        output = record.get('stdout', '') + record.get('stderr', '')
        if any(marker in output for marker in ('median_ms', 'performance_verdict=',
                                               'test_crdt_perf_gate OK',
                                               'test_cspa_perf_gate OK')):
            raise ValueError(f'timing verdict in correctness evidence: {name}')
        records[name] = output
    if set(records) != set(TESTS):
        raise ValueError('missing required correctness tests')
    if ('test_crdt_perf_gate: correctness OK' not in records['crdt_correctness_full']
            or not re.search(r'result\s*=\s*104851 \(expected 104851\)',
                             records['crdt_correctness_full'])):
        raise ValueError('missing CRDT gold correctness evidence')
    if 'test_cspa_perf_gate: correctness OK (tuples=20381 iters=6)' not in records['cspa_correctness']:
        raise ValueError('missing CSPA gold correctness evidence')


def main() -> int:
    try:
        validate(Path(sys.argv[1]))
    except (IndexError, OSError, ValueError, TypeError) as error:
        print(f'check-required-correctness: FAIL: {error}', file=sys.stderr)
        return 1
    print('correctness_status=pass')
    print('performance_status=not_evaluated')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
