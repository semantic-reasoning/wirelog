#!/usr/bin/env python3
"""Freeze a provenance-checked batch-append schedule; never launches benchmarks."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

SCHEMA = 'wirelog.batch-append-plan.v2'
CASES = ('1x1', '1x256', '32x256')
PAIR_ORDERS = ('AB', 'BA')
PAIRS_PER_ORDER = 9
HELPER_PATHS = ('bench/bench_util.h', 'tests/test_perf_util.h')
CASE_CONTRACT = (
    dict(name='1x1', ncols=1, rows_per_call=1, capacity=512,
         default_iterations_start=2000000),
    dict(name='1x256', ncols=1, rows_per_call=256, capacity=512,
         default_iterations_start=10000),
    dict(name='32x256', ncols=32, rows_per_call=256, capacity=512,
         default_iterations_start=10000),
)
OUTPUT_CONTRACT = dict(
    schema='wirelog.batch-append-benchmark.v2',
    schema_version=2,
    records=dict(
        header=['contract', 'reset', 'input', 'governor', 'reserved_capacity'],
        sample=['contract', 'case', 'index', 'iterations', 'append_total_ns', 'reset_total_ns',
                'append_ns_per_call', 'reset_ns_per_call'],
        case=['contract', 'name', 'columns', 'rows_per_call', 'capacity', 'iterations',
              'samples', 'warmups'],
        summary=['contract', 'case', 'median_append_ns_per_call',
                 'min_append_ns_per_call', 'max_append_ns_per_call',
                 'cov_append_percent', 'mean_reset_ns_per_call']),
    success_fields=['denied=0', 'row_count_check=OK', 'capacity_check=OK',
                    'value_check=OK', 'distinct_input_probe=OK', 'status=OK'])
OVERLAY_PATHS = (
    'bench/bench_batch_append.c',
    'bench/meson.build',
    'tests/meson.build',
    'tests/test_bench_batch_append.c',
)


class PlanError(ValueError):
    pass


def git(source, *args):
    result = subprocess.run(['git', '-C', str(source), *args],
                            capture_output=True, text=True, encoding='utf-8')
    if result.returncode:
        raise PlanError(f'git {args[0]} failed in {source}: {result.stderr.strip()}')
    return result.stdout.strip()


def git_bytes(source, *args):
    result = subprocess.run(['git', '-C', str(source), *args],
                            capture_output=True, check=False)
    if result.returncode:
        raise PlanError(f'git {args[0]} failed in {source}: {result.stderr.decode(errors="replace").strip()}')
    return result.stdout


def safe_path(value, label):
    path = Path(value).expanduser().resolve()
    if path == Path('/tmp') or Path('/tmp') in path.parents:
        raise PlanError(f'{label} must not be under /tmp')
    if path == Path('/dev/shm') or Path('/dev/shm') in path.parents:
        raise PlanError(f'{label} must not be under /dev/shm')
    return path


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def source_provenance(label, source_arg, wrapper_arg, upstream_tree_arg, overlay_bytes):
    source = safe_path(source_arg, f'{label} source')
    top = Path(git(source, 'rev-parse', '--show-toplevel')).resolve()
    if top != source:
        raise PlanError(f'{label} source must be the Git worktree root')
    if git(source, 'rev-parse', '--is-shallow-repository') != 'false':
        raise PlanError(f'{label} source must contain complete Git history')

    wrapper = wrapper_arg.lower()
    expected_tree = upstream_tree_arg.lower()
    if len(wrapper) != 40 or any(c not in '0123456789abcdef' for c in wrapper):
        raise PlanError(f'{label} wrapper must be a full 40-character commit ID')
    if len(expected_tree) != 40 or any(c not in '0123456789abcdef' for c in expected_tree):
        raise PlanError(f'{label} upstream tree must be a full 40-character tree ID')
    if git(source, 'rev-parse', 'HEAD') != wrapper:
        raise PlanError(f'{label} HEAD does not match the supplied wrapper commit')
    parents = git(source, 'show', '-s', '--format=%P', wrapper).split()
    if len(parents) != 1:
        raise PlanError(f'{label} wrapper commit must have exactly one parent')
    parent = parents[0]
    git(source, 'cat-file', '-e', f'{parent}^{{commit}}')
    wrapper_tree = git(source, 'rev-parse', f'{wrapper}^{{tree}}')
    parent_tree = git(source, 'rev-parse', f'{parent}^{{tree}}')
    if parent_tree != wrapper_tree or wrapper_tree != expected_tree:
        raise PlanError(
            f'{label} parent tree, wrapper tree, and declared upstream tree must match '
            f'(parent={parent_tree}, wrapper={wrapper_tree}, upstream={expected_tree})')

    helpers = {}
    for path in HELPER_PATHS:
        blob_oid = git(source, 'rev-parse', f'{wrapper}:{path}')
        blob = git_bytes(source, 'show', f'{wrapper}:{path}')
        helpers[path] = dict(git_blob_oid=blob_oid, sha256=sha256(blob))

    unstaged = subprocess.run(['git', '-C', str(source), 'diff', '--quiet'], check=False)
    if unstaged.returncode != 0:
        raise PlanError(f'{label} worktree has unstaged changes')
    untracked = git(source, 'ls-files', '--others', '--exclude-standard')
    if untracked:
        raise PlanError(f'{label} worktree has untracked files')
    changed = git(source, 'diff', '--cached', '--name-only', wrapper).splitlines()
    if tuple(sorted(changed)) != tuple(sorted(OVERLAY_PATHS)):
        raise PlanError(f'{label} staged overlay paths differ from the required benchmark overlay')
    applied = subprocess.run(
        ['git', '-C', str(source), 'diff', '--cached', '--binary', wrapper, '--', *OVERLAY_PATHS],
        capture_output=True, check=False)
    if applied.returncode:
        raise PlanError(f'{label} could not read staged overlay diff')
    if applied.stdout != overlay_bytes:
        raise PlanError(f'{label} staged overlay does not exactly match the supplied overlay patch')
    measured_tree = git(source, 'write-tree')
    return dict(source_root=str(source), wrapper_commit=wrapper,
                wrapper_parent_commit=parent, wrapper_parent_tree=parent_tree,
                wrapper_tree=wrapper_tree, upstream_tree=expected_tree,
                measured_tree=measured_tree,
                overlay_sha256=sha256(overlay_bytes), helper_sources=helpers)


def schedule(seed):
    """Stable hash-sort order; independent of Python's random implementation."""
    pairs = []
    for case in CASES:
        for order in PAIR_ORDERS:
            for index in range(PAIRS_PER_ORDER):
                pairs.append(dict(case=case, order=order, pair_index=index,
                                  pair_id=f'{case}:{order}:{index:02d}'))
    key = b'wirelog-batch-append-plan-v2\0' + seed.encode('utf-8') + b'\0'
    pairs.sort(key=lambda pair: (hashlib.sha256(key + pair['pair_id'].encode('ascii')).digest(),
                                 pair['pair_id']))
    launches = []
    for pair in pairs:
        sides = ('base', 'candidate') if pair['order'] == 'AB' else ('candidate', 'base')
        for side in sides:
            launches.append(dict(launch_index=len(launches), pair_id=pair['pair_id'],
                                 case=pair['case'], pair_order=pair['order'],
                                 pair_index=pair['pair_index'], side=side,
                                 warmups=2, measured_samples=1))
    return launches


def build_plan(args):
    overlay = safe_path(args.overlay_patch, 'overlay patch').read_bytes()
    if not overlay:
        raise PlanError('overlay patch is empty')
    parsed = subprocess.run(['git', 'apply', '--numstat', str(safe_path(args.overlay_patch, 'overlay patch'))],
                            capture_output=True, text=True, encoding='utf-8')
    if parsed.returncode:
        raise PlanError(f'overlay patch is not a valid Git patch: {parsed.stderr.strip()}')
    patch_paths = tuple(sorted(line.split('\t', 2)[2] for line in parsed.stdout.splitlines()
                               if len(line.split('\t', 2)) == 3))
    if patch_paths != tuple(sorted(OVERLAY_PATHS)):
        raise PlanError('overlay patch paths differ from the required benchmark overlay')
    base = source_provenance('base', args.base_source, args.base_wrapper,
                             args.base_upstream_tree, overlay)
    candidate = source_provenance('candidate', args.candidate_source,
                                  args.candidate_wrapper, args.candidate_upstream_tree,
                                  overlay)
    same_upstream = base['upstream_tree'] == candidate['upstream_tree']
    if args.mode == 'aa_control' and not same_upstream:
        raise PlanError('A/A control requires identical upstream trees')
    if args.mode == 'comparison' and same_upstream:
        raise PlanError('comparison requires different upstream trees')
    if base['helper_sources'] != candidate['helper_sources']:
        raise PlanError('benchmark helper Git blobs and SHA-256 values must match on both sides')
    launches = schedule(args.seed)
    if len(launches) != 108:
        raise PlanError('internal schedule size error')
    case_contract = [dict(case, iterations_status='starting point; unresolved until baseline calibration')
                     for case in CASE_CONTRACT]
    return dict(schema=SCHEMA, schema_version=2, mode=args.mode, seed=args.seed,
                seed_algorithm='SHA-256 hash-sort of pair IDs; key prefix wirelog-batch-append-plan-v2',
                created_utc=datetime.now(timezone.utc).isoformat(),
                upstream=dict(base=base, candidate=candidate),
                overlay=dict(sha256=sha256(overlay), paths=list(OVERLAY_PATHS)),
                unchanged_helpers=base['helper_sources'],
                benchmark_case_contract=dict(cases=case_contract,
                    iteration_counts='starting points only; unresolved until later baseline calibration'),
                benchmark_output_contract=OUTPUT_CONTRACT,
                schedule=dict(case_order=list(CASES), pairs_per_case=18,
                              pairs_per_order=PAIRS_PER_ORDER, launch_count=len(launches),
                              pairing='adjacent calls; each pair has one baseline and one candidate launch',
                              warmups_per_process=2, measured_samples_per_process=1,
                              launches=launches),
                scope=dict(benchmark_launches_performed=0,
                           build_profile_validation='deferred',
                           host_qualification='deferred'))


def write_fresh(output_arg, plan):
    output = safe_path(output_arg, 'output directory')
    home = Path.home().resolve()
    if output != home and home not in output.parents:
        raise PlanError('output directory must be under HOME')
    if not output.parent.is_dir():
        raise PlanError('output parent directory must already exist')
    output.mkdir(mode=0o700, exist_ok=False)
    payload = (json.dumps(plan, indent=2, sort_keys=True, ensure_ascii=False,
                          allow_nan=False) + '\n').encode('utf-8')
    path = output / 'campaign-plan.json'
    temporary = output / 'campaign-plan.json.tmp'
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        for directory in (output, output.parent):
            dir_fd = os.open(directory, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
    except BaseException:
        for artifact in (temporary, path):
            try:
                artifact.unlink()
            except FileNotFoundError:
                pass
        try:
            output.rmdir()
        except OSError:
            pass
        raise


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument('--mode', choices=('comparison', 'aa_control'), required=True)
    result.add_argument('--seed', required=True, help='recorded UTF-8 schedule seed')
    result.add_argument('--base-source', required=True)
    result.add_argument('--base-wrapper', required=True)
    result.add_argument('--base-upstream-tree', required=True)
    result.add_argument('--candidate-source', required=True)
    result.add_argument('--candidate-wrapper', required=True)
    result.add_argument('--candidate-upstream-tree', required=True)
    result.add_argument('--overlay-patch', required=True)
    result.add_argument('--output-dir', required=True)
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        plan = build_plan(args)
        write_fresh(args.output_dir, plan)
    except (OSError, PlanError, UnicodeError) as error:
        print(f'prepare-batch-append-campaign: {error}', file=sys.stderr)
        return 2
    print(f"Frozen {plan['schedule']['launch_count']} launches in {Path(args.output_dir) / 'campaign-plan.json'}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
