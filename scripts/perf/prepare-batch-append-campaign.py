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
import runpy
import tempfile

SCHEMA = 'wirelog.batch-append-plan.v3'
SCHEMA_VERSION = 3
MANIFEST_PATH = Path(__file__).with_name('batch-append-revision-manifest-v1.json')
MANIFEST_SHA256 = 'be21dfe6eb2c34a7b4b855d74e0020fa68b87cc570901ef41c09ba395ee69ad8'
PRODUCT_PATCH_PATH = Path(__file__).with_name('batch-append-product-delta-v1.patch')
PRODUCT_PATCH_SHA256 = '059b699ee5a802b13a54f9e7e6bc04cc043b9f95aa311294801593d3fc7f977d'
ANCHOR_BUNDLE_PATH = Path(__file__).with_name('batch-append-anchor-v1.bundle')
ANCHOR_BUNDLE_SIZE = 9853557
ANCHOR_BUNDLE_SHA256 = '20bac73d57b374248c32a05bc1f02e20a035da4585c123ce291d28fb8461c995'
ANCHOR_REF = 'refs/heads/batch-append-anchor'
ANCHOR_COMMIT = '64bd12f499b357d60d1928d5dba08326af58426f'
ANCHOR_TREE = '4c10e21ab4ba034dbc50abd92a59d22e8b975130'
VERIFIED_PRODUCT_DELTAS = set()
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
FALLBACKS = runpy.run_path(str(Path(__file__).with_name('batch_append_fallbacks.py')))
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


def verify_product_patch(product):
    expected_paths = ['tests/test_relation_generations.c', 'wirelog/columnar/relation.c']
    try:
        patch_relative = PRODUCT_PATCH_PATH.resolve().relative_to(REPOSITORY_ROOT)
        patch_raw = PRODUCT_PATCH_PATH.read_bytes()
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        raise PlanError('product delta patch must be a repository input') from error
    if patch_relative.as_posix() != 'scripts/perf/batch-append-product-delta-v1.patch':
        raise PlanError('product delta patch path is not the pinned repository input')
    if sha256(patch_raw) != PRODUCT_PATCH_SHA256 \
            or product.get('diff_sha256') != PRODUCT_PATCH_SHA256:
        raise PlanError('product delta patch bytes do not match the code-pinned hash')
    try:
        patch_lines = patch_raw.decode('utf-8').splitlines()
    except UnicodeError as error:
        raise PlanError('product delta patch must be UTF-8 text') from error

    expected_headers = [f'diff --git a/{path} b/{path}' for path in expected_paths]
    actual_headers = [line for line in patch_lines if line.startswith('diff --git ')]
    if actual_headers != expected_headers:
        raise PlanError('product delta patch must modify exactly the pinned paths')
    forbidden_headers = ('old mode ', 'new mode ', 'new file mode ', 'deleted file mode ',
                         'rename from ', 'rename to ', 'copy from ', 'copy to ')
    if any(line.startswith(forbidden_headers) for line in patch_lines):
        raise PlanError('product delta patch cannot rename paths or change file modes')
    for path in expected_paths:
        if patch_lines.count(f'--- a/{path}') != 1 \
                or patch_lines.count(f'+++ b/{path}') != 1:
            raise PlanError('product delta patch has unsafe or unexpected file paths')

    try:
        bundle_relative = ANCHOR_BUNDLE_PATH.resolve().relative_to(REPOSITORY_ROOT)
        bundle_raw = ANCHOR_BUNDLE_PATH.read_bytes()
    except (OSError, ValueError) as error:
        raise PlanError('anchor bundle must be a repository input') from error
    if bundle_relative.as_posix() != 'scripts/perf/batch-append-anchor-v1.bundle' \
            or len(bundle_raw) != ANCHOR_BUNDLE_SIZE \
            or sha256(bundle_raw) != ANCHOR_BUNDLE_SHA256:
        raise PlanError('anchor bundle bytes do not match the code-pinned identity')
    verification_key = (str(REPOSITORY_ROOT.resolve()), str(PRODUCT_PATCH_PATH.resolve()),
                        PRODUCT_PATCH_SHA256, str(ANCHOR_BUNDLE_PATH.resolve()),
                        len(bundle_raw), ANCHOR_BUNDLE_SHA256,
                        product.get('pre_tree'), product.get('post_tree'),
                        product.get('diff_sha256'))
    if verification_key in VERIFIED_PRODUCT_DELTAS:
        return
    home_tmp = safe_path(Path.home() / '.tmp', 'revision verification temporary directory')
    home_tmp.mkdir(mode=0o700, exist_ok=True)
    with tempfile.TemporaryDirectory(
            dir=home_tmp, prefix=f'batch-append-tree-{os.getpid()}-') as name:
        isolated = Path(name)
        repo = isolated / 'repo'
        repo.mkdir(mode=0o700)
        subprocess.run(['git', '-C', str(repo), 'init', '-q'], check=True,
                       capture_output=True)
        env = os.environ.copy()
        env['GIT_OPTIONAL_LOCKS'] = '0'

        def isolated_git(*args, text=True):
            result = subprocess.run(
                ['git', '-C', str(repo), *args], env=env,
                capture_output=True, check=False, text=text,
                encoding='utf-8' if text else None)
            if result.returncode:
                stderr = result.stderr if text else result.stderr.decode(errors='replace')
                raise PlanError(f'cannot reconstruct pinned product delta: {stderr.strip()}')
            return result.stdout

        bundle_check = subprocess.run(['git', 'bundle', 'verify', str(ANCHOR_BUNDLE_PATH)],
                                      cwd=repo, capture_output=True, text=True,
                                      encoding='utf-8', check=False)
        if bundle_check.returncode or 'records a complete history' not in bundle_check.stdout:
            raise PlanError('anchor bundle must contain complete history without prerequisites')
        refs = subprocess.run(['git', 'bundle', 'list-heads', str(ANCHOR_BUNDLE_PATH)],
                              cwd=repo, capture_output=True, text=True,
                              encoding='utf-8', check=False)
        if refs.returncode or refs.stdout.strip().splitlines() != [f'{ANCHOR_COMMIT} {ANCHOR_REF}']:
            raise PlanError('anchor bundle must advertise exactly the pinned anchor ref')
        isolated_git('fetch', '--no-tags', str(ANCHOR_BUNDLE_PATH), ANCHOR_REF)
        if isolated_git('rev-parse', 'FETCH_HEAD').strip() != ANCHOR_COMMIT \
                or isolated_git('rev-parse', 'FETCH_HEAD^{tree}').strip() != ANCHOR_TREE:
            raise PlanError('anchor bundle commit/tree differs from the pinned identity')
        isolated_git('read-tree', product['pre_tree'])
        isolated_git('apply', '--cached', '--check', str(PRODUCT_PATCH_PATH))
        isolated_git('apply', '--cached', str(PRODUCT_PATCH_PATH))
        reconstructed_tree = isolated_git('write-tree').strip()
        if reconstructed_tree != product.get('post_tree'):
            raise PlanError('reconstructed product tree does not match pinned post tree')

        changed = isolated_git(
            'diff', '--no-renames', '--no-ext-diff', '--no-textconv', '--name-status',
            product['pre_tree'], reconstructed_tree).splitlines()
        if changed != [f'M\t{path}' for path in expected_paths]:
            raise PlanError('reconstructed product delta has unexpected paths or statuses')
        raw = isolated_git(
            'diff', '--no-renames', '--no-ext-diff', '--no-textconv', '--raw',
            product['pre_tree'], reconstructed_tree).splitlines()
        if len(raw) != len(expected_paths):
            raise PlanError('reconstructed product delta has unexpected raw entries')
        for line, path in zip(raw, expected_paths, strict=True):
            header, actual_path = line.split('\t', 1)
            fields = header.split()
            if actual_path != path or fields[0:2] != [':100644', '100644'] \
                    or fields[-1] != 'M':
                raise PlanError('reconstructed product delta changed a path type or mode')
        diff = isolated_git(
            'diff', '--no-renames', '--no-ext-diff', '--no-textconv', '--binary',
            product['pre_tree'], reconstructed_tree, text=False)
        if sha256(diff) != product.get('diff_sha256'):
            raise PlanError('reconstructed product delta does not match pinned patch hash')
    VERIFIED_PRODUCT_DELTAS.add(verification_key)


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
    verify_product_patch(product)
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
    try:
        fallback_dependencies = FALLBACKS['validate_fallbacks'](source)
    except FALLBACKS['FallbackError'] as error:
        raise PlanError(f'{label} fallback dependency provenance failed: {error}') from error
    return dict(source_root=str(source), checkout_kind=checkout_kind,
                checkout_commit=checkout_commit, checkout_tree=checkout_tree,
                anchor_commit=anchor['commit'], anchor_tree=anchor['tree'],
                product_tree=product_tree,
                product_delta=dict(from_tree=product['pre_tree'], to_tree=product['post_tree'],
                                   paths=product['paths'], diff_sha256=product['diff_sha256']),
                execution_tree=execution_tree, helper_sources=helpers,
                fallback_dependencies=fallback_dependencies)


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
    if FALLBACKS['canonical'](base['fallback_dependencies']) != \
            FALLBACKS['canonical'](candidate['fallback_dependencies']):
        raise PlanError('base and candidate fallback dependency identities differ')
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


def write_fresh(output_arg, plan, args=None):
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
        if args is not None:
            current = build_plan(args)
            current.pop('created_utc', None)
            expected = dict(plan)
            expected.pop('created_utc', None)
            if current != expected:
                raise PlanError('source/fallback provenance changed before plan publication')
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
        if args is not None:
            current = build_plan(args)
            current.pop('created_utc', None)
            expected = dict(plan)
            expected.pop('created_utc', None)
            if current != expected:
                raise PlanError('source/fallback provenance changed during plan publication')
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
        write_fresh(args.output_dir, plan, args)
    except (OSError, PlanError, UnicodeError) as error:
        print(f'prepare-batch-append-campaign: {error}', file=sys.stderr)
        return 2
    print(f"Frozen {plan['schedule']['launch_count']} launches in {Path(args.output_dir) / 'campaign-plan.json'}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
