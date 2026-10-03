#!/usr/bin/env python3
"""Admit a frozen batch-append campaign and create its durable empty journal."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import secrets
import shutil
import sys

HERE = Path(__file__).resolve().parent
FREEZER = runpy.run_path(str(HERE / 'freeze-batch-append-execution.py'))
SCHEMA = 'wirelog.batch-append-campaign-collection.v1'
STATUS_SCHEMA = 'wirelog.batch-append-campaign-status.v1'


class CollectionError(ValueError):
    pass


def digest(data):
    return hashlib.sha256(data).hexdigest()


def strict_args(args):
    mode = args.mode
    if mode == 'comparison':
        plans = (args.plan_a, args.plan_b)
        profiles = (args.profile_a, args.profile_b)
        calibrations = (args.calibration_a,)
        origin = None
    elif mode == 'aa_control':
        plans = (args.plan,)
        profiles = (args.profile,)
        calibrations = (args.calibration,)
        origin = (args.calibration_origin_plan, args.calibration_origin_profile)
    else:
        raise CollectionError('unsupported campaign mode')
    required = [args.overlay_patch, args.output_dir, args.freeze_artifact,
                *plans, *profiles, *calibrations]
    if origin is not None:
        required.extend(origin)
    if any(value is None for value in required):
        raise CollectionError('required collection input is missing')
    if mode == 'comparison' and any(getattr(args, name, None) is not None
                                    for name in ('plan', 'profile', 'calibration',
                                                 'calibration_origin_plan',
                                                 'calibration_origin_profile')):
        raise CollectionError('comparison mode received A/A-only inputs')
    if mode == 'aa_control' and any(getattr(args, name, None) is not None
                                    for name in ('plan_a', 'profile_a', 'calibration_a',
                                                 'plan_b', 'profile_b')):
        raise CollectionError('A/A mode received comparison-only inputs')
    return plans, profiles, calibrations, origin


def frozen_artifact(mode, normalized, input_hashes, freeze_root):
    rows = FREEZER['command_rows'](mode, normalized, freeze_root)
    expected = 108 if mode == 'aa_control' else 216
    if len(rows) != expected:
        raise CollectionError(f'frozen row count must be exactly {expected}')
    value = dict(schema=FREEZER['SCHEMA'], schema_version=1,
                 status='commands_frozen', mode=mode,
                 aa_product=(normalized[0]['plan'].get('aa_product')
                             if mode == 'aa_control' else None),
                 planned_command_count=len(rows), benchmark_launches_performed=0,
                 input_sha256=input_hashes,
                 plans=[dict(plan_sha256=item['plan_sha256'], seed=item['plan']['seed'],
                             profile_sha256=item['profile_sha256'],
                             calibration_sha256=item['calibration']['calibration_sha256'],
                             accepted_iteration_counts=item['calibration']['accepted_iteration_counts'])
                        for item in normalized],
                 commands=rows)
    if mode == 'aa_control':
        origin_record = normalized[0]['calibration_origin']
        value['calibration_origin'] = dict(
            plan_sha256=origin_record['plan_sha256'],
            profile_sha256=origin_record['profile_sha256'],
            calibration_sha256=origin_record['calibration']['calibration_sha256'],
            source_tree=origin_record['plan']['sources']['base']['product_tree'],
            execution_tree=origin_record['plan']['sources']['base']['execution_tree'],
            binary_sha256=origin_record['profile']['base']['binary_sha256'])
    return value


def host_compatible(snapshot, normalized, frozen, host_probe):
    stable = ('kernel', 'model', 'microcode')
    selected = set()
    topology = {}
    for row in frozen['commands']:
        cpu = row['cpu']['selected_cpu']
        if type(cpu) is not int or cpu not in snapshot['online_cpus'] \
                or cpu not in snapshot['affinity_cpus'] \
                or cpu not in row['cpu']['online'] or cpu not in row['cpu']['affinity']:
            raise CollectionError(f'frozen CPU {cpu} is not currently online and affinity-compatible')
        if any(row['host'][key] != snapshot[key] for key in stable):
            raise CollectionError('current host identity differs from frozen command identity')
        item = normalized[row['campaign_index']]
        calibrated = item['calibration']['attempt_by_case'][row['case']]['started']['host_before']
        if cpu not in topology:
            if cpu == snapshot['selected_cpu']:
                topology[cpu] = snapshot['smt_siblings']
            elif hasattr(host_probe, 'sys'):
                path = host_probe.sys / f'devices/system/cpu/cpu{cpu}/topology/thread_siblings_list'
                try:
                    topology[cpu] = FREEZER['CAL']['parse_cpu_list'](
                        path.read_text(encoding='ascii'))
                except (OSError, FREEZER['CAL']['CalibrationError']) as error:
                    raise CollectionError(f'current SMT topology unavailable for CPU {cpu}: {error}') from error
            else:
                raise CollectionError(f'current SMT topology unavailable for CPU {cpu}')
        if calibrated['smt_siblings'] != topology[cpu]:
            raise CollectionError('current CPU topology differs from calibration host topology')
        selected.add(cpu)
    return dict(selected_cpus=sorted(selected),
                smt_siblings_by_cpu={str(cpu): topology[cpu] for cpu in sorted(topology)})


def host_identity(snapshot):
    fields = ('kernel', 'model', 'microcode', 'online_cpus', 'affinity_cpus',
              'selected_cpu', 'smt_siblings')
    return {field: snapshot[field] for field in fields}


def stable_admission(artifact):
    value = dict(artifact)
    host = dict(value['host_attestation'])
    host['initial'] = host_identity(host['initial'])
    value['host_attestation'] = host
    return value


def admit(args, expected_host=None, host_probe=None):
    plans, profiles, calibrations, origin = strict_args(args)
    try:
        overlay, normalized, input_hashes = FREEZER['validate_inputs'](
            args.mode, args.overlay_patch, plans, profiles, calibrations,
            calibration_origin=origin)
        freeze_path = FREEZER['under_home'](args.freeze_artifact, 'command-freeze artifact')
        if freeze_path.name != 'command-freeze.json':
            raise CollectionError('freeze artifact must be named command-freeze.json')
        freeze_root = freeze_path.parent
        FREEZER['output_path'](freeze_root, normalized, overlay)
        raw_freeze = freeze_path.read_bytes()
        supplied = FREEZER['load_json'](freeze_path)
        rebuilt = frozen_artifact(args.mode, normalized, input_hashes, freeze_root)
        rebuilt_bytes = (json.dumps(rebuilt, sort_keys=True, indent=2,
                                    ensure_ascii=False, allow_nan=False) + '\n').encode('utf-8')
        if raw_freeze != rebuilt_bytes or supplied != rebuilt:
            raise CollectionError('command-freeze artifact differs byte-for-byte from validated inputs')
        output = FREEZER['output_path'](args.output_dir, normalized, overlay)
        if output.parent != freeze_root:
            raise CollectionError('collection output must be a fresh direct child of the command-freeze directory')
    except (FREEZER['FreezeError'], FREEZER['PROFILE']['PreflightError'],
            FREEZER['PLAN']['PlanError'], FREEZER['CAL']['CalibrationError'],
            OSError) as error:
        raise CollectionError(f'campaign admission failed: {error}') from error
    host_probe = host_probe or FREEZER['CAL']['HostProbe']()
    try:
        current_host = host_probe.snapshot()
    except FREEZER['CAL']['CalibrationError'] as error:
        raise CollectionError(f'current host identity/topology unavailable: {error}') from error
    cpu_topology = host_compatible(current_host, normalized, supplied, host_probe)
    if expected_host is not None and host_identity(current_host) != host_identity(expected_host):
        raise CollectionError('host identity/topology changed during collection admission')
    if FREEZER['contains_verdict_key'](supplied):
        raise CollectionError('frozen command contract contains a verdict field')
    hashes = dict(input_hashes)
    hashes[str(freeze_path)] = digest(raw_freeze)
    artifact = dict(schema=SCHEMA, schema_version=1, status='ready',
                    mode=args.mode, planned_command_count=supplied['planned_command_count'],
                    benchmark_launches_performed=0, input_sha256=hashes,
                    command_freeze_path=str(freeze_path),
                    command_freeze_sha256=digest(raw_freeze),
                    host_attestation=dict(purpose='identity/topology and CPU availability only',
                                          initial=current_host, **cpu_topology),
                    frozen_preflight=supplied)
    return output, artifact


def write_durable(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def fsync_directory(path):
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def remove_tree(path):
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass


def collect(args, host_probe=None):
    host_probe = host_probe or FREEZER['CAL']['HostProbe']()
    output, artifact = admit(args, host_probe=host_probe)
    initial_host = artifact['host_attestation']['initial']
    if output.exists() or output.is_symlink():
        raise FileExistsError(output)
    stage = output.parent / f'.{output.name}.collect-{secrets.token_hex(12)}'
    if output.parent != output.parent.resolve():
        raise CollectionError('collection output parent must resolve without symlinks')
    stage.mkdir(mode=0o700, exist_ok=False)
    preflight_bytes = (json.dumps(artifact, sort_keys=True, indent=2,
                                  ensure_ascii=False, allow_nan=False) + '\n').encode('utf-8')
    status = dict(schema=STATUS_SCHEMA, status='ready', benchmark_launches_performed=0,
                  planned_command_count=artifact['planned_command_count'],
                  preflight_sha256=digest(preflight_bytes))
    status_bytes = (json.dumps(status, sort_keys=True, indent=2,
                               ensure_ascii=False, allow_nan=False) + '\n').encode('utf-8')
    published = False
    try:
        write_durable(stage / 'collector-preflight.json', preflight_bytes)
        write_durable(stage / 'status.json', status_bytes)
        write_durable(stage / 'journal.jsonl', b'')
        fsync_directory(stage)

        # Recompute every validated input and the complete command vector before
        # making the staged directory visible at its requested path.
        before_output, before = admit(args, expected_host=initial_host, host_probe=host_probe)
        if before_output != output or stable_admission(before) != stable_admission(artifact):
            raise CollectionError('inputs changed while preparing collection directory')
        if output.exists() or output.is_symlink():
            raise FileExistsError(output)
        os.rename(stage, output)
        published = True
        fsync_directory(output.parent)

        # Repeat validation after publication; a failed race must leave no artifact.
        after_output, after = admit(args, expected_host=initial_host, host_probe=host_probe)
        if after_output != output or stable_admission(after) != stable_admission(artifact):
            raise CollectionError('inputs changed during collection publication')
    except BaseException:
        remove_tree(stage)
        if published:
            remove_tree(output)
        raise
    return output, artifact


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument('--mode', choices=('comparison', 'aa_control'), required=True)
    result.add_argument('--overlay-patch', required=True)
    result.add_argument('--freeze-artifact', required=True,
                        help='immutable command-freeze.json to admit without rewriting commands')
    result.add_argument('--output-dir', required=True)
    result.add_argument('--plan-a')
    result.add_argument('--profile-a')
    result.add_argument('--calibration-a')
    result.add_argument('--plan-b')
    result.add_argument('--profile-b')
    result.add_argument('--plan')
    result.add_argument('--profile')
    result.add_argument('--calibration')
    result.add_argument('--calibration-origin-plan')
    result.add_argument('--calibration-origin-profile')
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        output, artifact = collect(args)
    except (OSError, CollectionError) as error:
        print(f'collect-batch-append-campaign: {error}', file=sys.stderr)
        return 2
    print(f"Admitted {artifact['planned_command_count']} frozen commands in {output}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
