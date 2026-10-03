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

SCHEMA = 'wirelog.batch-append-plan.v3'
SCHEMA_VERSION = 3
MANIFEST_PATH = Path(__file__).with_name('batch-append-revision-manifest-v1.json')
MANIFEST_SHA256 = 'be21dfe6eb2c34a7b4b855d74e0020fa68b87cc570901ef41c09ba395ee69ad8'
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
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


def load_revision_manifest():
    try:
        manifest_relative = MANIFEST_PATH.resolve().relative_to(REPOSITORY_ROOT)
        tracked = subprocess.run(
            ['git', '-C', str(REPOSITORY_ROOT), 'ls-files', '--error-unmatch', '--',
             manifest_relative.as_posix()], check=True, capture_output=True,
            text=True, encoding='utf-8')
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        raise PlanError('revision manifest must be tracked in the harness checkout') from error
    try:
        raw = MANIFEST_PATH.read_bytes()
    except OSError as error:
        raise PlanError(f'cannot read tracked revision manifest: {error}') from error
    if sha256(raw) != MANIFEST_SHA256:
        raise PlanError('revision manifest bytes do not match the code-pinned hash')
    try:
        manifest = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise PlanError(f'invalid revision manifest JSON: {error}') from error
    paths = ['tests/test_relation_generations.c', 'wirelog/columnar/relation.c']
    if type(manifest) is not dict:
        raise PlanError('revision manifest must be a JSON object')
    anchor, product = manifest.get('anchor'), manifest.get('product')
    if manifest.get('schema') != \
            'wirelog.batch-append-revision-manifest.v1' or type(anchor) is not dict \
            or type(product) is not dict:
        raise PlanError('unsupported or malformed revision manifest')
    if anchor.get('commit') != '64bd12f499b357d60d1928d5dba08326af58426f' \
            or anchor.get('tree') != '4c10e21ab4ba034dbc50abd92a59d22e8b975130' \
            or product.get('pre_tree') != anchor.get('tree') \
            or product.get('post_tree') != '9fa822ba32dd3b89e71d0158efa27422e49a06d4' \
            or product.get('paths') != paths:
        raise PlanError('revision manifest identities or exact paths are invalid')
    if set(product['paths']) & set(OVERLAY_PATHS):
        raise PlanError('revision product paths overlap benchmark overlay paths')
    try:
        changed = subprocess.run(
            ['git', '-C', str(REPOSITORY_ROOT), 'diff', '--name-status',
             product['pre_tree'], product['post_tree'], '--', *product['paths']],
            check=True, capture_output=True, text=True, encoding='utf-8').stdout.splitlines()
        diff = subprocess.run(
            ['git', '-C', str(REPOSITORY_ROOT), 'diff', '--binary',
             product['pre_tree'], product['post_tree'], '--', *product['paths']],
            check=True, capture_output=True).stdout
    except (OSError, subprocess.SubprocessError) as error:
        raise PlanError(f'cannot verify revision product delta: {error}') from error
    if changed != [f'M\t{path}' for path in paths] \
            or sha256(diff) != product.get('diff_sha256'):
        raise PlanError('revision product delta does not match manifest path/status/hash')
    return manifest, sha256(raw)


def source_provenance(label, source_arg, kind, manifest, overlay_bytes):
    source = safe_path(source_arg, f'{label} source')
    top = Path(git(source, 'rev-parse', '--show-toplevel')).resolve()
    if top != source:
        raise PlanError(f'{label} source must be the Git worktree root')
    if git(source, 'rev-parse', '--is-shallow-repository') != 'false':
        raise PlanError(f'{label} source must contain complete Git history')

    checkout_commit = git(source, 'rev-parse', 'HEAD')
    checkout_tree = git(source, 'rev-parse', f'{checkout_commit}^{{tree}}')
    anchor = manifest['anchor']
    product = manifest['product']
    if kind == 'pre':
        if checkout_commit != anchor['commit'] or checkout_tree != product['pre_tree']:
            raise PlanError(f'{label} pre checkout must be the direct fixed anchor commit/tree')
        checkout_kind = 'direct_anchor'
        product_tree = product['pre_tree']
    elif kind == 'post':
        parents = git(source, 'show', '-s', '--format=%P', checkout_commit).split()
        if len(parents) != 1:
            raise PlanError(f'{label} synthetic post checkout must have exactly one parent')
        if parents[0] != anchor['commit']:
            raise PlanError(f'{label} synthetic post checkout parent must be the fixed anchor')
        parent_tree = git(source, 'rev-parse', f'{parents[0]}^{{tree}}')
        if parent_tree != product['pre_tree']:
            raise PlanError(f'{label} synthetic post parent tree does not match product pre tree')
        if checkout_tree != product['post_tree']:
            raise PlanError(f'{label} synthetic post checkout tree does not match product post tree')
        checkout_kind = 'synthetic_product'
        product_tree = product['post_tree']
    else:
        raise PlanError(f'invalid {label} product selection')

    helpers = {}
    for path in HELPER_PATHS:
        blob_oid = git(source, 'rev-parse', f'{checkout_commit}:{path}')
        blob = git_bytes(source, 'show', f'{checkout_commit}:{path}')
        helpers[path] = dict(git_blob_oid=blob_oid, sha256=sha256(blob))

    unstaged = subprocess.run(['git', '-C', str(source), 'diff', '--quiet'], check=False)
    if unstaged.returncode != 0:
        raise PlanError(f'{label} worktree has unstaged changes')
    status = git(source, 'status', '--porcelain', '--untracked-files=all',
                 '--ignored=matching', '--', '.', ':!bench/bench_batch_append.c',
                 ':!bench/meson.build', ':!tests/meson.build',
                 ':!tests/test_bench_batch_append.c')
    if status:
        raise PlanError(f'{label} source has tracked, ignored, or untracked changes outside overlay')
    changed = git(source, 'diff', '--cached', '--name-only', checkout_commit).splitlines()
    if tuple(sorted(changed)) != tuple(sorted(OVERLAY_PATHS)):
        raise PlanError(f'{label} staged overlay paths differ from the required benchmark overlay')
    applied = subprocess.run(
        ['git', '-C', str(source), 'diff', '--cached', '--binary', checkout_commit,
         '--', *OVERLAY_PATHS],
        capture_output=True, check=False)
    if applied.returncode:
        raise PlanError(f'{label} could not read staged overlay diff')
    if applied.stdout != overlay_bytes:
        raise PlanError(f'{label} staged overlay does not exactly match the supplied overlay patch')
    execution_tree = git(source, 'write-tree')
    return dict(source_root=str(source), checkout_kind=checkout_kind,
                checkout_commit=checkout_commit, checkout_tree=checkout_tree,
                anchor_commit=anchor['commit'], anchor_tree=anchor['tree'],
                product_tree=product_tree,
                product_delta=dict(from_tree=product['pre_tree'], to_tree=product['post_tree'],
                                   paths=product['paths'], diff_sha256=product['diff_sha256']),
                execution_tree=execution_tree, helper_sources=helpers)


def schedule(seed):
    """Stable hash-sort order; independent of Python's random implementation."""
    pairs = []
    for case in CASES:
        for order in PAIR_ORDERS:
            for index in range(PAIRS_PER_ORDER):
                pairs.append(dict(case=case, order=order, pair_index=index,
                                  pair_id=f'{case}:{order}:{index:02d}'))
    key = b'wirelog-batch-append-plan-v3\0' + seed.encode('utf-8') + b'\0'
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
    if type(args.seed) is not str or not args.seed:
        raise PlanError('schedule seed must be a non-empty string')
    overlay = safe_path(args.overlay_patch, 'overlay patch').read_bytes()
    base_source = safe_path(args.base_source, 'base source')
    manifest, manifest_hash = load_revision_manifest()
    if not overlay:
        raise PlanError('overlay patch is empty')
    parsed = subprocess.run(
        ['git', 'apply', '--numstat', str(safe_path(args.overlay_patch, 'overlay patch'))],
        cwd=base_source, capture_output=True, text=True, encoding='utf-8')
    if parsed.returncode:
        raise PlanError(f'overlay patch is not a valid Git patch: {parsed.stderr.strip()}')
    patch_paths = tuple(sorted(line.split('\t', 2)[2] for line in parsed.stdout.splitlines()
                               if len(line.split('\t', 2)) == 3))
    if patch_paths != tuple(sorted(OVERLAY_PATHS)):
        raise PlanError('overlay patch paths differ from the required benchmark overlay')
    if args.mode == 'comparison':
        if getattr(args, 'aa_product', None) is not None:
            raise PlanError('A/A product selection is only valid in aa_control mode')
        base_kind, candidate_kind = 'pre', 'post'
    elif args.mode == 'aa_control':
        selection = getattr(args, 'aa_product', None)
        if selection not in ('pre', 'post'):
            raise PlanError('A/A control must explicitly select product pre or post')
        base_kind = candidate_kind = selection
    else:
        raise PlanError('invalid campaign mode')
    base = source_provenance('base', args.base_source, base_kind, manifest, overlay)
    candidate = source_provenance('candidate', args.candidate_source,
                                  candidate_kind, manifest, overlay)
    if args.mode == 'aa_control' and base['product_tree'] != candidate['product_tree']:
        raise PlanError('A/A control requires identical product trees')
    if base['helper_sources'] != candidate['helper_sources']:
        raise PlanError('benchmark helper Git blobs and SHA-256 values must match on both sides')
    launches = schedule(args.seed)
    if len(launches) != 108:
        raise PlanError('internal schedule size error')
    case_contract = [dict(case, iterations_status='starting point; unresolved until baseline calibration')
                     for case in CASE_CONTRACT]
    return dict(schema=SCHEMA, schema_version=SCHEMA_VERSION, mode=args.mode,
                aa_product=getattr(args, 'aa_product', None), seed=args.seed,
                seed_algorithm='SHA-256 hash-sort of pair IDs; key prefix wirelog-batch-append-plan-v3',
                created_utc=datetime.now(timezone.utc).isoformat(),
                revision_manifest=dict(path=MANIFEST_PATH.name, sha256=manifest_hash,
                                       anchor=manifest['anchor'], product=manifest['product']),
                sources=dict(base=base, candidate=candidate),
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
    result.add_argument('--aa-product', choices=('pre', 'post'))
    result.add_argument('--seed', required=True, help='recorded UTF-8 schedule seed')
    result.add_argument('--base-source', required=True)
    result.add_argument('--candidate-source', required=True)
    result.add_argument('--overlay-patch', required=True)
    result.add_argument('--output-dir', required=True)
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    if args.mode == 'comparison' and args.aa_product is not None:
        parser().error('--aa-product is only valid with --mode aa_control')
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
