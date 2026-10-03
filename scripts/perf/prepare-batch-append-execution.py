#!/usr/bin/env python3
"""Validate plan-v2 source/build provenance and freeze a profile-only artifact.

This tool never builds or launches the benchmark. Calibration acceptance and
execution-argv generation are deliberately deferred to a later contract.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys

HERE = Path(__file__).resolve().parent
PLAN = runpy.run_path(str(HERE / 'prepare-batch-append-campaign.py'))
ARTIFACT_SCHEMA = 'wirelog.batch-append-profile.v1'
MESON_FILES = ('meson-info.json', 'intro-buildoptions.json',
               'intro-compilers.json', 'intro-machines.json', 'intro-targets.json',
               'intro-dependencies.json')


class PreflightError(ValueError):
    pass


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf-8')


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise PreflightError(f'duplicate JSON key: {key}')
        result[key] = value
    return result


def reject_constant(value):
    raise PreflightError(f'non-JSON constant: {value}')


def load_json(path):
    try:
        return json.loads(path.read_text(encoding='utf-8'),
                          object_pairs_hook=unique_object,
                          parse_constant=reject_constant)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise PreflightError(f'cannot read JSON {path}: {error}') from error


def under_home(path, label):
    result = PLAN['safe_path'](path, label)
    home = Path.home().resolve()
    if result != home and home not in result.parents:
        raise PreflightError(f'{label} must be under HOME')
    return result


def outside_sources(path, sources, label, root_label='source worktrees'):
    for source in sources:
        if path == source or source in path.parents or path in source.parents:
            raise PreflightError(f'{label} must be outside {root_label}')


def validate_plan(path, overlay_patch):
    raw = path.read_bytes()
    plan = load_json(path)
    if type(plan) is not dict or plan.get('schema') != 'wirelog.batch-append-plan.v2' \
            or plan.get('schema_version') != 2:
        raise PreflightError('execution preflight requires batch-append plan schema v2')
    if plan.get('benchmark_output_contract') != PLAN['OUTPUT_CONTRACT']:
        raise PreflightError('plan benchmark output contract is unsupported')
    upstream = plan.get('upstream')
    if type(upstream) is not dict or set(upstream) != {'base', 'candidate'}:
        raise PreflightError('plan has invalid source identities')
    for side in ('base', 'candidate'):
        if type(upstream[side]) is not dict:
            raise PreflightError(f'plan has invalid {side} source identity')
        for key in ('source_root', 'wrapper_commit', 'upstream_tree'):
            if type(upstream[side].get(key)) is not str or not upstream[side][key]:
                raise PreflightError(f'plan has invalid {side} {key}')
        source = Path(upstream[side]['source_root']).resolve()
        verify_source_checkout(source)
    args = argparse.Namespace(
        mode=plan.get('mode'), seed=plan.get('seed'),
        base_source=upstream['base'].get('source_root'),
        base_wrapper=upstream['base'].get('wrapper_commit'),
        base_upstream_tree=upstream['base'].get('upstream_tree'),
        candidate_source=upstream['candidate'].get('source_root'),
        candidate_wrapper=upstream['candidate'].get('wrapper_commit'),
        candidate_upstream_tree=upstream['candidate'].get('upstream_tree'),
        overlay_patch=overlay_patch)
    try:
        recomputed = PLAN['build_plan'](args)
    except PLAN['PlanError'] as error:
        raise PreflightError(f'plan/source provenance revalidation failed: {error}') from error
    recomputed.pop('created_utc')
    supplied = dict(plan)
    supplied.pop('created_utc', None)
    if supplied != recomputed:
        raise PreflightError('plan provenance or frozen schedule does not match its sources')
    return plan, sha256(raw)


def verify_source_checkout(source):
    try:
        shallow = subprocess.run(['git', '-C', str(source), 'rev-parse',
                                  '--is-shallow-repository'],
                                 check=True, capture_output=True, text=True,
                                 encoding='utf-8', timeout=30).stdout.strip()
        status = subprocess.run(['git', '-C', str(source), 'status', '--porcelain',
                                 '--untracked-files=all', '--ignored=matching', '--',
                                 '.', ':!bench/bench_batch_append.c',
                                 ':!bench/meson.build', ':!tests/meson.build',
                                 ':!tests/test_bench_batch_append.c'],
                                check=True, capture_output=True, text=True,
                                encoding='utf-8', timeout=30).stdout
    except (OSError, subprocess.SubprocessError) as error:
        raise PreflightError(f'cannot verify source checkout {source}: {error}') from error
    if shallow != 'false':
        raise PreflightError(f'source checkout must not be shallow: {source}')
    if status:
        raise PreflightError(f'source checkout has tracked, ignored, or untracked changes: {source}')


def normalize(value, source_root, build_root):
    if isinstance(value, str):
        # Replace longer roots first, including paths embedded in compiler flags.
        for root, replacement in sorted(((str(build_root), '$BUILD'),
                                        (str(source_root), '$SOURCE')),
                                       key=lambda pair: len(pair[0]), reverse=True):
            value = value.replace(root, replacement)
        return value
    if isinstance(value, list):
        return [normalize(item, source_root, build_root) for item in value]
    if isinstance(value, dict):
        return {key: normalize(item, source_root, build_root)
                for key, item in sorted(value.items())}
    return value


def read_meson_profile(source_root, build_root):
    info = build_root / 'meson-info'
    files = {name: info / name for name in MESON_FILES}
    raw = {name: path.read_bytes() for name, path in files.items()}
    metadata = {name: load_json(path) for name, path in files.items()}
    directories = metadata['meson-info.json'].get('directories', {})
    if Path(directories.get('source', '')).resolve() != source_root \
            or Path(directories.get('build', '')).resolve() != build_root:
        raise PreflightError(f'Meson source/build mapping mismatch in {build_root}')

    options = metadata['intro-buildoptions.json']
    if type(options) is not list or not options or any(type(item) is not dict or 'name' not in item for item in options):
        raise PreflightError(f'incomplete Meson build options in {build_root}')
    names = [item['name'] for item in options]
    if len(names) != len(set(names)):
        raise PreflightError(f'duplicate Meson build option in {build_root}')
    options = sorted(options, key=lambda item: item['name'])

    compilers = metadata['intro-compilers.json']
    machines = metadata['intro-machines.json']
    dependencies = metadata['intro-dependencies.json']
    targets = metadata['intro-targets.json']
    if type(compilers) is not dict or 'host' not in compilers:
        raise PreflightError(f'missing host compiler metadata in {build_root}')
    if type(machines) is not dict or 'host' not in machines:
        raise PreflightError(f'missing host machine metadata in {build_root}')
    if type(targets) is not list:
        raise PreflightError(f'invalid Meson target metadata in {build_root}')
    matches = [target for target in targets if target.get('name') == 'bench_batch_append']
    if len(matches) != 1:
        raise PreflightError(f'expected one bench_batch_append target in {build_root}')
    target = matches[0]
    expected_defined = str(source_root / 'bench/meson.build')
    expected_binary = str(build_root / 'bench/bench_batch_append')
    if target.get('type') != 'executable' or target.get('defined_in') != expected_defined \
            or target.get('filename') != [expected_binary]:
        raise PreflightError(f'benchmark target source/build mapping mismatch in {build_root}')
    sources = target.get('target_sources')
    if type(sources) is not list or not sources:
        raise PreflightError(f'benchmark target has no source/compiler metadata in {build_root}')
    if not any(str(source_root / 'bench/bench_batch_append.c') in item.get('sources', [])
               for item in sources if type(item) is dict):
        raise PreflightError(f'benchmark source is absent from target metadata in {build_root}')

    toolchain_executables = []
    for machine_name, language_map in sorted(compilers.items()):
        if type(language_map) is not dict:
            continue
        for language, compiler in sorted(language_map.items()):
            if type(compiler) is not dict:
                continue
            for field in ('exelist', 'linker_exelist'):
                command = compiler.get(field, [])
                if not command:
                    continue
                if type(command) is not list:
                    raise PreflightError(f'invalid {field} in compiler metadata')
                for token in command:
                    resolved = Path(token).resolve() if Path(token).is_absolute() else None
                    if resolved is None:
                        found = shutil.which(token)
                        resolved = Path(found).resolve() if found else None
                    if resolved is None or not resolved.is_file():
                        raise PreflightError(f'cannot resolve compiler tool {token!r}')
                    toolchain_executables.append(dict(
                        machine=machine_name, language=language, command_field=field,
                        command=command, path=str(resolved),
                        sha256=sha256(resolved.read_bytes())))

    normalized = dict(
        options=normalize(options, source_root, build_root),
        compilers=normalize(compilers, source_root, build_root),
        machines=normalize(machines, source_root, build_root),
        dependencies=normalize(dependencies, source_root, build_root),
        toolchain_executables=toolchain_executables,
        target=normalize(target, source_root, build_root))
    cpu_flags = []
    link_flags = []
    for source in sources:
        for flag in source.get('parameters', []):
            if flag.startswith(('/arch:', '-march', '-mcpu', '-mtune', '-mavx',
                                '-msse', '-mno-', '-mfma', '-m64', '-m32',
                                '-mabi=', '-mfloat-abi=')):
                cpu_flags.append(flag)
        if source.get('language') is None:
            link_flags.extend(source.get('parameters', []))
    normalized_profile = dict(normalized)
    normalized_profile['cpu_flags'] = normalize(cpu_flags, source_root, build_root)
    normalized_profile['linker_flags'] = normalize(link_flags, source_root, build_root)
    build_evidence = {}
    for label, evidence_path in (
            ('meson_log', build_root / 'meson-logs/meson-log.txt'),
            ('compile_commands', build_root / 'compile_commands.json')):
        try:
            build_evidence[label] = sha256(evidence_path.read_bytes())
        except OSError as error:
            raise PreflightError(f'missing build evidence {evidence_path}: {error}') from error
    return dict(source_root=str(source_root), build_root=str(build_root),
                source_build_mapping=dict(source='$SOURCE', build='$BUILD'),
                resolved_build_options=normalized['options'],
                compiler_metadata=normalized['compilers'],
                machine_metadata=normalized['machines'],
                dependency_metadata=normalized['dependencies'],
                toolchain_executables=toolchain_executables,
                normalized_target=normalized['target'],
                cpu_flags=normalize(cpu_flags, source_root, build_root),
                linker_flags=normalize(link_flags, source_root, build_root),
                introspection_sha256={name: sha256(content) for name, content in raw.items()},
                build_evidence_sha256=build_evidence,
                profile_sha256=sha256(canonical(normalized_profile)),
                binary_path=expected_binary,
                binary_sha256=sha256(Path(expected_binary).read_bytes()),
                ninja_no_pending_work=True)


def assert_no_pending_rebuild(build_root, target=None):
    command = ['ninja', '-C', str(build_root), '-n']
    if target is not None:
        command.append(target)
    try:
        result = subprocess.run(command,
                                capture_output=True, text=True, encoding='utf-8',
                                timeout=30, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise PreflightError(f'cannot verify no-pending-rebuild state in {build_root}: {error}') from error
    output_lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if result.returncode != 0 or not output_lines \
            or output_lines[-1] != 'ninja: no work to do.':
        raise PreflightError(f'build has pending work or Ninja dry-run failed in {build_root}')
    return sha256(result.stdout.encode('utf-8'))


def inspect_side(source, build):
    default_dry_run_hash = assert_no_pending_rebuild(build)
    target_dry_run_hash = assert_no_pending_rebuild(build, 'bench/bench_batch_append')
    result = read_meson_profile(source, build)
    if assert_no_pending_rebuild(build) != default_dry_run_hash \
            or assert_no_pending_rebuild(build, 'bench/bench_batch_append') != target_dry_run_hash:
        raise PreflightError(f'Ninja dry-run evidence changed during inspection in {build}')
    result['ninja_dry_run_stdout_sha256'] = dict(
        default=default_dry_run_hash, benchmark_target=target_dry_run_hash)
    return result


def build_artifact(plan_path_arg, overlay_arg, base_build_arg, candidate_build_arg):
    plan_path = under_home(plan_path_arg, 'plan')
    overlay = under_home(overlay_arg, 'overlay patch')
    base_build = under_home(base_build_arg, 'base build')
    candidate_build = under_home(candidate_build_arg, 'candidate build')
    plan, plan_hash = validate_plan(plan_path, overlay)
    sources = [Path(plan['upstream'][side]['source_root']).resolve()
               for side in ('base', 'candidate')]
    for evidence_path, label in ((plan_path, 'plan'), (overlay, 'overlay patch')):
        outside_sources(evidence_path, sources, label)
    for build in (base_build, candidate_build):
        outside_sources(build, sources, 'build directory')
        if not build.is_dir():
            raise PreflightError(f'build directory does not exist: {build}')
    if base_build == candidate_build or base_build in candidate_build.parents \
            or candidate_build in base_build.parents:
        raise PreflightError('base and candidate builds must be distinct, non-overlapping directories')
    base = inspect_side(sources[0], base_build)
    candidate = inspect_side(sources[1], candidate_build)
    if base['profile_sha256'] != candidate['profile_sha256']:
        raise PreflightError('base and candidate normalized build profiles differ')
    if sha256(plan_path.read_bytes()) != plan_hash:
        raise PreflightError('plan changed during profile preflight')
    # Repeat source, overlay, and helper verification after all profile reads.
    plan_after, plan_hash_after = validate_plan(plan_path, overlay)
    if plan_hash_after != plan_hash or plan_after != plan:
        raise PreflightError('plan or source provenance drifted during profile preflight')
    base_after = inspect_side(sources[0], base_build)
    candidate_after = inspect_side(sources[1], candidate_build)
    if base_after != base or candidate_after != candidate:
        raise PreflightError('Meson profile or benchmark binary drifted during preflight')
    return dict(schema=ARTIFACT_SCHEMA,
                status='not_executable', source_build_verified=True,
                not_executable=True,
                plan_sha256=plan_hash, mode=plan['mode'],
                overlay_sha256=plan['overlay']['sha256'],
                base=base, candidate=candidate,
                profile_sha256=base['profile_sha256'],
                benchmark_launches_performed=0)


def write_atomic(output_arg, artifact):
    output = under_home(output_arg, 'artifact output directory')
    sources = [Path(artifact[side]['source_root']).resolve()
               for side in ('base', 'candidate')]
    outside_sources(output, sources, 'artifact output directory')
    builds = [Path(artifact[side]['build_root']).resolve()
              for side in ('base', 'candidate')]
    outside_sources(output, builds, 'artifact output directory', 'build directories')
    if not output.parent.is_dir():
        raise PreflightError('artifact output parent must already exist')
    output.mkdir(mode=0o700, exist_ok=False)
    target = output / 'execution-preflight.json'
    temporary = output / 'execution-preflight.json.tmp'
    payload = (json.dumps(artifact, sort_keys=True, indent=2,
                          ensure_ascii=False, allow_nan=False) + '\n').encode('utf-8')
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        for directory in (output, output.parent):
            fd = os.open(directory, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    except BaseException:
        for path in (temporary, target):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        try:
            output.rmdir()
        except OSError:
            pass
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', required=True)
    parser.add_argument('--overlay-patch', required=True)
    parser.add_argument('--base-build', required=True)
    parser.add_argument('--candidate-build', required=True)
    parser.add_argument('--output-dir', required=True)
    args = parser.parse_args(argv)
    try:
        artifact = build_artifact(args.plan, args.overlay_patch,
                                  args.base_build, args.candidate_build)
        write_atomic(args.output_dir, artifact)
    except (OSError, PreflightError, PLAN['PlanError']) as error:
        print(f'prepare-batch-append-execution: {error}', file=sys.stderr)
        return 2
    print(f"Profile artifact ready: {Path(args.output_dir) / 'execution-preflight.json'}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
