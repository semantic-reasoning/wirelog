#!/usr/bin/env python3
"""Freeze exact B/M source SHAs and the current policy digest before runs."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

HERE = Path(__file__).resolve().parent
POLICY_PATH = HERE / 'paired-bm-policy-v1.json'
POLICY_SHA256 = '523029143376f1765e920d6932181cc049831dcebddd7531ffa70cc713848e55'


def freeze(base_sha, candidate_sha):
    if (not re.fullmatch(r'[0-9a-f]{40}', base_sha) or
            not re.fullmatch(r'[0-9a-f]{40}', candidate_sha) or
            base_sha == candidate_sha):
        raise ValueError('source SHAs must be distinct full lowercase commit IDs')
    policy_sha = hashlib.sha256(POLICY_PATH.read_bytes()).hexdigest()
    if policy_sha != POLICY_SHA256:
        raise ValueError('policy bytes differ from the frozen v1 policy digest')
    root = HERE.parents[1]
    trees = {}
    for side, revision in (('base', base_sha), ('candidate', candidate_sha)):
        tree = subprocess.check_output(
            ['git', '-C', str(root), 'rev-parse', f'{revision}^{{tree}}'],
            text=True, encoding='utf-8').strip()
        if not re.fullmatch(r'[0-9a-f]{40}', tree):
            raise ValueError(f'{side} source does not resolve to a full Git tree ID')
        trees[side] = tree
    return dict(schema_version=1, plan_id='tagged-runner-bm-repeat-v1',
                base_sha=base_sha, candidate_sha=candidate_sha,
                base_tree_sha=trees['base'], candidate_tree_sha=trees['candidate'],
                policy_id='tagged-runner-bm-eligibility-v1',
                policy_sha256=policy_sha,
                campaign_mode='comparison', workloads=['crdt', 'cspa-fast'],
                attempts=['attempt-1', 'attempt-2'],
                note='Commit and publish this plan before collecting either attempt.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-sha', required=True)
    parser.add_argument('--candidate-sha', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        document = freeze(args.base_sha, args.candidate_sha)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open('x', encoding='utf-8') as stream:
            json.dump(document, stream, sort_keys=True, indent=2)
            stream.write('\n')
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f'freeze-paired-bm-plan: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
