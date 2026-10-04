#!/usr/bin/env python3
"""Fail closed unless #1913 provenance and completed campaign agree."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import runpy
import subprocess
import sys


HERE = Path(__file__).resolve().parent
MATERIALIZER = runpy.run_path(str(HERE / 'materialize-1913-diagnostic.py'))
EVALUATOR = runpy.run_path(str(HERE / 'evaluate-paired-campaign.py'))
SIDES = ('base', 'candidate')
TEST_PATH = MATERIALIZER['TEST_PATH']
TIMER_SOURCES = (
    'bench/bench_flowlog.c',
    'bench/bench_crdt_workload.h',
    TEST_PATH,
    'tests/test_perf_util.h',
    'bench/bench_util.h',
)
CRDT_TIMER = {
    'id': 'crdt_perf_gate_single_run',
    'unit': 'ms',
    'scope': 'run_crdt_once_: full pipeline, one worker',
}
CSPA_TIMER = {
    'id': 'bench_flowlog_repeat_1',
    'unit': 'ms',
    'scope': 'run_pipeline_count: full pipeline, one worker',
}
EXPECTED = {
    'crdt': {'result': 104851, 'aggregate': 2152328, 'iterations': 14148},
    'cspa-fast': {'tuples': 20381, 'iterations': 6},
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def keys(value, expected, name):
    require(type(value) is dict and set(value) == set(expected), f'{name}: wrong fields')


def git(repo, *args):
    return subprocess.check_output(
        ['git', '-C', str(repo), *args], stderr=subprocess.PIPE
    ).decode('utf-8').strip()


def git_bytes(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args], stderr=subprocess.PIPE)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def read_json(path):
    def unique_object(pairs):
        record = {}
        for key, value in pairs:
            if key in record:
                raise ValueError(f'duplicate JSON key: {key}')
            record[key] = value
        return record

    return json.loads(path.read_text(encoding='utf-8'), object_pairs_hook=unique_object,
                      parse_constant=lambda value: (_ for _ in ()).throw(
                          ValueError(f'invalid JSON constant: {value}')))


def commit_info(repo, sha):
    headers = git(repo, 'cat-file', '-p', sha).split('\n\n', 1)[0].splitlines()
    tree = next((line[5:] for line in headers if line.startswith('tree ')), None)
    parents = [line[7:] for line in headers if line.startswith('parent ')]
    require(tree is not None, f'{sha}: not a commit')
    return tree, parents


def validate_materialization(repo, directory, side):
    source = MATERIALIZER['SOURCES'][side]
    record = read_json(directory / f'{side}.json')
    keys(record, ('side', 'source_sha', 'source_tree', 'source_parents',
                  'source_probe_blob', 'probe_blob', 'wrapper_sha', 'wrapper_tree',
                  'wrapper_parent', 'changed_path'), f'{side} materialization')
    require(record['side'] == side, f'{side}: swapped materialization side')
    require(record['source_sha'] == source['sha'], f'{side}: source SHA differs from pin')
    require(record['source_tree'] == source['tree'], f'{side}: source tree differs from pin')
    require(record['source_parents'] == list(source['parents']),
            f'{side}: source parents differ from pin')
    require(record['source_probe_blob'] == MATERIALIZER['EXPECTED_OLD_TEST_BLOB'],
            f'{side}: original probe blob differs from pin')
    require(record['probe_blob'] == MATERIALIZER['PROBE_BLOB'],
            f'{side}: diagnostic probe blob differs from pin')
    require(record['changed_path'] == TEST_PATH, f'{side}: wrong changed path')
    require(type(record['wrapper_sha']) is str and
            re.fullmatch(r'[0-9a-f]{40}', record['wrapper_sha']) is not None,
            f'{side}: invalid wrapper SHA')
    require(record['wrapper_parent'] == source['sha'], f'{side}: wrong wrapper parent')
    source_tree, source_parents = commit_info(repo, source['sha'])
    require((source_tree, source_parents) == (source['tree'], list(source['parents'])),
            f'{side}: source commit provenance mismatch')
    require(git(repo, 'rev-parse', f'{source["sha"]}:{TEST_PATH}') ==
            MATERIALIZER['EXPECTED_OLD_TEST_BLOB'], f'{side}: original test blob mismatch')
    wrapper_tree, wrapper_parents = commit_info(repo, record['wrapper_sha'])
    require(wrapper_tree == record['wrapper_tree'], f'{side}: wrapper tree differs from commit')
    require(wrapper_parents == [source['sha']], f'{side}: wrapper commit parent mismatch')
    changes = git(repo, 'diff-tree', '--no-commit-id', '--name-status', '-r',
                   source['sha'], record['wrapper_sha'])
    require(changes == f'M\t{TEST_PATH}', f'{side}: wrapper changes unexpected paths')
    require(git(repo, 'ls-tree', record['wrapper_sha'], TEST_PATH) ==
            f'100644 blob {MATERIALIZER["PROBE_BLOB"]}\t{TEST_PATH}',
            f'{side}: wrapper mode or probe blob mismatch')
    probe = git_bytes(repo, 'cat-file', 'blob', MATERIALIZER['PROBE_BLOB'])
    require(b'WIRELOG_CRDT_PROBE' in probe and b'crdt_perf_gate_single_run' in probe,
            'pinned probe blob contents are incompatible')
    return record, sha256(probe)


def validate(repo, materialization_dir, evidence_dir):
    repo = Path(repo).resolve()
    materialization_dir = Path(materialization_dir)
    evidence_dir = Path(evidence_dir)
    records = {side: validate_materialization(repo, materialization_dir, side)
               for side in SIDES}
    materialized = {side: records[side][0] for side in SIDES}
    probe_hashes = {side: records[side][1] for side in SIDES}
    require(materialized['base']['wrapper_sha'] != materialized['candidate']['wrapper_sha'],
            'base and candidate wrappers are not distinct')

    preflight_path = evidence_dir / 'preflight.json'
    campaign_path = evidence_dir / 'campaign-v1.json'
    status_path = evidence_dir / 'collection-status.json'
    evaluation_path = evidence_dir / 'evaluation-report.json'
    for path in (preflight_path, campaign_path, status_path, evaluation_path):
        require(path.is_file(), f'missing required collector artifact: {path.name}')
    preflight = read_json(preflight_path)
    campaign = read_json(campaign_path)
    collection_status = read_json(status_path)
    evaluation_report = read_json(evaluation_path)
    keys(campaign, ('schema_version', 'campaign_id', 'manifest', 'blocks',
                    'attempts', 'campaign_mode'), 'campaign')
    keys(preflight, ('schema_version', 'mode', 'qualify_aa', 'campaign_id',
                     'manifest', 'records', 'artifacts', 'paths', 'schedule',
                     'theoretical_launch_timeout_ceiling_seconds', 'launcher_sha256'),
         'preflight')
    require(type(collection_status) is dict, 'collection status must be an object')
    require(type(evaluation_report) is dict, 'evaluation report must be an object')
    require(preflight['schema_version'] == 1 and preflight['mode'] == 'comparison' and
            preflight['qualify_aa'] is False, 'preflight mode is not descriptive comparison')
    require(preflight['manifest'] == campaign.get('manifest'),
            'preflight and campaign manifests differ')
    require(preflight['campaign_id'] == campaign.get('campaign_id'),
            'preflight and campaign IDs differ')
    require(campaign.get('campaign_mode') == 'comparison',
            'collector campaign mode is not comparison')
    manifest = campaign['manifest']
    keys(manifest, ('sources', 'profiles', 'workloads', 'host_id', 'cpu', 'build_provenance'),
         'campaign manifest')
    keys(manifest['sources'], SIDES, 'campaign sources')
    preflight_records = preflight['records']
    keys(preflight_records, SIDES, 'preflight source records')
    keys(manifest['build_provenance'], SIDES, 'campaign build provenance')
    expected_probe_sha256 = probe_hashes['base']
    require(probe_hashes['candidate'] == expected_probe_sha256,
            'probe content hash differs by side')
    for side in SIDES:
        materialization = materialized[side]
        wrapper_sha = materialization['wrapper_sha']
        wrapper_tree = materialization['wrapper_tree']
        source_record = preflight_records[side]
        require(type(source_record) is dict, f'{side}: preflight source record must be an object')
        require(manifest['sources'][side] == wrapper_sha,
                f'{side}: campaign source SHA differs from wrapper')
        build = manifest['build_provenance'][side]
        require(type(build) is dict and build.get('source_sha') == wrapper_sha,
                f'{side}: build source SHA differs from wrapper')
        require(source_record.get('source_sha') == wrapper_sha and
                source_record.get('source_tree') == wrapper_tree,
                f'{side}: preflight source SHA/tree differs from wrapper')
        source_hashes = source_record.get('source_sha256')
        contract_hashes = source_record.get('timer_contract_sha256')
        require(type(source_hashes) is dict and type(contract_hashes) is dict,
                f'{side}: missing source/timer identity')
        require(source_hashes.get(TEST_PATH) == expected_probe_sha256 and
                contract_hashes.get(TEST_PATH) == expected_probe_sha256,
                f'{side}: CRDT timer source hash differs from pinned probe')
    for path in TIMER_SOURCES:
        base_hash = preflight_records['base']['timer_contract_sha256'].get(path)
        candidate_hash = preflight_records['candidate']['timer_contract_sha256'].get(path)
        require(type(base_hash) is str and re.fullmatch(r'[0-9a-f]{64}', base_hash),
                f'missing timer source hash: {path}')
        require(base_hash == candidate_hash, f'timer source differs between sides: {path}')
        for side in SIDES:
            content = git_bytes(repo, 'show', f'{materialized[side]["wrapper_sha"]}:{path}')
            require(sha256(content) == preflight_records[side]['timer_contract_sha256'][path],
                    f'{side}: timer source hash does not match Git tree: {path}')
    workloads = manifest['workloads']
    keys(workloads, ('crdt', 'cspa-fast'), 'campaign workloads')
    require(type(workloads['crdt']) is dict and type(workloads['cspa-fast']) is dict,
            'campaign workload records must be objects')
    require(workloads['crdt'].get('timer') == CRDT_TIMER and
            workloads['crdt'].get('expected') == EXPECTED['crdt'],
            'CRDT timer or correctness contract differs from #1913')
    require(workloads['cspa-fast'].get('timer') == CSPA_TIMER and
            workloads['cspa-fast'].get('expected') == EXPECTED['cspa-fast'],
            'CSPA descriptive workload contract changed')
    require(collection_status.get('status') == 'COMPLETE_VALID' and
            collection_status.get('evaluator_exit') == 0,
            'collector status is incomplete or invalid')
    require(type(campaign.get('attempts')) is list and len(campaign['attempts']) == 80,
            'campaign is partial; expected all 80 launches')
    raw_campaign = campaign_path.read_bytes()
    expected_evaluation = EVALUATOR['evaluate'](campaign, sha256(raw_campaign))
    require(expected_evaluation['status'] == 'COMPLETE_VALID',
            f'campaign evaluator rejected evidence: {expected_evaluation["reason"]}')
    require(evaluation_report.get('status') == 'COMPLETE_VALID' and
            evaluation_report.get('input_sha256') == sha256(raw_campaign) and
            evaluation_report.get('manifest_sha256') == expected_evaluation['manifest_sha256'],
            'evaluation report does not match complete campaign')
    return {
        'schema_version': 1,
        'status': 'VALID',
        'campaign_id': campaign['campaign_id'],
        'sources': {side: materialized[side]['source_sha'] for side in SIDES},
        'wrappers': {side: materialized[side]['wrapper_sha'] for side in SIDES},
        'wrapper_trees': {side: materialized[side]['wrapper_tree'] for side in SIDES},
        'crdt_timer': CRDT_TIMER,
        'crdt_timer_source_sha256': expected_probe_sha256,
        'campaign_evaluation': 'COMPLETE_VALID',
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', type=Path, required=True)
    parser.add_argument('--materialization-dir', type=Path, required=True)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        result = validate(args.repository, args.materialization_dir, args.evidence_dir)
    except (KeyError, OSError, subprocess.CalledProcessError, TypeError, ValueError) as error:
        result = {'schema_version': 1, 'status': 'INVALID', 'reason': str(error)}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + '\n',
                               encoding='utf-8')
        print(f'validate-1913-diagnostic: {error}', file=sys.stderr)
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + '\n',
                           encoding='utf-8')
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
