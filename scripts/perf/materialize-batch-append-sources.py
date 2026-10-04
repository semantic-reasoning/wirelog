#!/usr/bin/env python3
"""Create disposable, provenance-pinned base and candidate benchmark sources."""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys
import runpy

HERE = Path(__file__).resolve().parent
campaign = runpy.run_path(str(HERE / 'prepare-batch-append-campaign.py'))
fallbacks = runpy.run_path(str(HERE / 'batch_append_fallbacks.py'))


def run(root, *args, env=None):
    result = subprocess.run(['git', '-C', str(root), *args], env=env,
                            capture_output=True, text=True, encoding='utf-8')
    if result.returncode:
        raise ValueError(f'git {args[0]} failed: {result.stderr.strip()}')
    return result.stdout.strip()


def under_home(value, label):
    path = campaign['safe_path'](value, label)
    home = Path.home().resolve()
    if path == home or home not in path.parents:
        raise ValueError(f'{label} must be a child of HOME')
    return path


def copy_fallbacks(seed, target):
    subprojects = target / 'subprojects'
    subprojects.mkdir(exist_ok=True)
    nano_source = seed / 'subprojects/nanoarrow'
    nano_target = subprojects / 'nanoarrow'
    subprocess.run(['git', 'clone', '--no-local', '--no-hardlinks',
                    str(nano_source), str(nano_target)], check=True,
                   capture_output=True, text=True, encoding='utf-8')
    run(nano_target, 'checkout', '--detach', fallbacks['NANO_COMMIT'])
    wrap = fallbacks['check_wraps'](seed, run(seed, 'rev-parse', 'HEAD'))
    (nano_target / fallbacks['NANO_MARKER']).write_bytes(
        (wrap['nanoarrow']['wrap_sha256'] + '\n').encode('ascii'))
    os.chmod(nano_target / fallbacks['NANO_MARKER'], 0o644)
    shutil.copytree(seed / 'subprojects/xxHash-0.8.4',
                    subprojects / 'xxHash-0.8.4', symlinks=True)
    shutil.copytree(seed / 'subprojects/packagecache',
                    subprojects / 'packagecache', symlinks=True)


def materialize(output_root, patch, overlay, fallback_seed, seed_mode,
                mode='comparison', aa_product=None):
    output_root = under_home(output_root, 'output root')
    fallback_seed = campaign['safe_path'](fallback_seed, 'fallback seed')
    caller_root = Path(campaign['REPOSITORY_ROOT']).resolve()
    if output_root == caller_root or caller_root in output_root.parents \
            or output_root in caller_root.parents:
        raise ValueError('output root must not overlap the caller checkout')
    if output_root == fallback_seed or fallback_seed in output_root.parents \
            or output_root in fallback_seed.parents:
        raise ValueError('fallback seed and output root must not overlap')
    if not output_root.parent.is_dir():
        raise ValueError('output parent must already exist')
    seed_identity = fallbacks['validate_fallback_seed'](fallback_seed, seed_mode)
    manifest, _ = campaign['load_revision_manifest']()
    overlay_raw = campaign['safe_path'](overlay, 'overlay patch').read_bytes()
    patch_path = campaign['safe_path'](patch, 'product patch')
    if patch_path != campaign['PRODUCT_PATCH_PATH'].resolve():
        raise ValueError('product patch must be the pinned repository input')
    if not overlay_raw:
        raise ValueError('overlay patch is empty')
    if mode == 'comparison':
        if aa_product is not None:
            raise ValueError('A/A product selection is only valid in aa_control mode')
    elif mode == 'aa_control':
        if aa_product not in ('pre', 'post'):
            raise ValueError('A/A materialization requires product pre or post')
    else:
        raise ValueError('invalid materialization mode')
    parsed_overlay = subprocess.run(
        ['git', 'apply', '--numstat', str(campaign['safe_path'](overlay, 'overlay patch'))],
        cwd=campaign['REPOSITORY_ROOT'], capture_output=True, text=True, encoding='utf-8')
    if parsed_overlay.returncode:
        raise ValueError(f'overlay patch is invalid: {parsed_overlay.stderr.strip()}')
    overlay_paths = tuple(sorted(line.split('\t', 2)[2]
                                 for line in parsed_overlay.stdout.splitlines()
                                 if len(line.split('\t', 2)) == 3))
    if overlay_paths != tuple(sorted(campaign['OVERLAY_PATHS'])):
        raise ValueError('overlay patch paths differ from the required benchmark overlay')

    output_root.mkdir(mode=0o700, exist_ok=False)
    try:
        bundle_path = campaign['ANCHOR_BUNDLE_PATH'].resolve()
        for label in ('base', 'candidate'):
            target = output_root / f'{label}-source'
            target.mkdir(mode=0o700)
            subprocess.run(['git', '-C', str(target), 'init', '-q'], check=True,
                           capture_output=True, text=True, encoding='utf-8')
            run(target, 'fetch', '--no-tags', str(bundle_path), campaign['ANCHOR_REF'])
            run(target, 'checkout', '--detach', campaign['ANCHOR_COMMIT'])
            make_post = (mode == 'comparison' and label == 'candidate') \
                or (mode == 'aa_control' and aa_product == 'post')
            if make_post:
                run(target, 'apply', '--index', str(patch_path))
                post_tree = run(target, 'write-tree')
                if post_tree != manifest['product']['post_tree']:
                    raise ValueError('product patch did not reproduce the pinned candidate tree')
                env = os.environ.copy()
                env.update(GIT_AUTHOR_NAME='Wirelog pinned candidate',
                           GIT_AUTHOR_EMAIL='wirelog-campaign@example.invalid',
                           GIT_COMMITTER_NAME='Wirelog pinned candidate',
                           GIT_COMMITTER_EMAIL='wirelog-campaign@example.invalid',
                           GIT_AUTHOR_DATE='2026-01-01T00:00:00+00:00',
                           GIT_COMMITTER_DATE='2026-01-01T00:00:00+00:00')
                candidate = run(target, 'commit-tree', post_tree,
                                '-p', campaign['ANCHOR_COMMIT'],
                                '-m', 'Pinned batch append product candidate', env=env)
                run(target, 'checkout', '--detach', candidate)
            run(target, 'apply', '--index', str(campaign['safe_path'](
                overlay, 'overlay patch')))
            copy_fallbacks(fallback_seed, target)
            observed = fallbacks['validate_fallbacks'](target)
            if (target / 'subprojects/nanoarrow/.git/objects/info/alternates').exists():
                raise ValueError('materialized nanoarrow checkout must not use object alternates')
            seed_nested = dict(schema=seed_identity['schema'],
                               outer_ignored_paths=seed_identity['outer_ignored_paths'],
                               nanoarrow=seed_identity['nanoarrow'], xxhash=seed_identity['xxhash'])
            if fallbacks['canonical'](observed) != fallbacks['canonical'](seed_nested):
                raise ValueError('materialized fallback identities differ from validated seed')
        if any((output_root / f'{label}-source/.git/objects/info/alternates').exists()
               for label in ('base', 'candidate')):
            raise ValueError('materialized repositories must not use object alternates')
        if fallbacks['canonical'](fallbacks['validate_fallback_seed'](
                fallback_seed, seed_mode)) != fallbacks['canonical'](seed_identity):
            raise ValueError('fallback seed changed during materialization')
    except BaseException:
        shutil.rmtree(output_root)
        raise
    return output_root


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument('--output-root', required=True)
    result.add_argument('--overlay-patch', required=True)
    result.add_argument('--fallback-seed', required=True)
    result.add_argument('--fallback-seed-mode', choices=('clean', 'overlay_staged'), required=True)
    result.add_argument('--mode', choices=('comparison', 'aa_control'), default='comparison')
    result.add_argument('--aa-product', choices=('pre', 'post'))
    result.add_argument('--product-patch', default=str(campaign['PRODUCT_PATCH_PATH']))
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        output = materialize(args.output_root, args.product_patch,
                             args.overlay_patch, args.fallback_seed,
                             args.fallback_seed_mode, args.mode, args.aa_product)
    except (OSError, ValueError, subprocess.SubprocessError,
            campaign['PlanError'], fallbacks['FallbackError']) as error:
        print(f'materialize-batch-append-sources: {error}', file=sys.stderr)
        return 2
    print(f'Materialized base and candidate sources in {output}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
