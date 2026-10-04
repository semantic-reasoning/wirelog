#!/usr/bin/env python3
"""Admit a frozen batch-append campaign and create its durable empty journal."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import runpy
import secrets
import shutil
import signal
import stat
import time
import sys

HERE = Path(__file__).resolve().parent
FREEZER = runpy.run_path(str(HERE / 'freeze-batch-append-execution.py'))
SCHEMA = 'wirelog.batch-append-campaign-collection.v1'
STATUS_SCHEMA = 'wirelog.batch-append-campaign-status.v1'
RUN_TIMEOUT_SECONDS = 300
RUN_LOCK = '.run.lock'


class CollectionError(ValueError):
    pass


class RunnerError(ValueError):
    def __init__(self, message, category='host_provenance'):
        super().__init__(message)
        self.category = category


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


def atomic_json(path, value):
    data = (json.dumps(value, sort_keys=True, indent=2,
                       ensure_ascii=False, allow_nan=False) + '\n').encode('utf-8')
    temporary = path.parent / f'.{path.name}.{secrets.token_hex(8)}.tmp'
    try:
        write_durable(temporary, data)
        os.replace(temporary, path)
        fsync_directory(path.parent)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def append_journal(path, record):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
        raise RunnerError('journal is not a mode-0600 regular file')
    data = (json.dumps(record, sort_keys=True, separators=(',', ':'),
                       ensure_ascii=False, allow_nan=False) + '\n').encode('utf-8')
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | getattr(os, 'O_NOFOLLOW', 0))
    try:
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise RunnerError('short journal write')
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def write_status(path, status):
    atomic_json(path, status)


def read_collection(output, args, expected_preflight=None, host_probe=None):
    preflight_path = output / 'collector-preflight.json'
    status_path = output / 'status.json'
    journal_path = output / 'journal.jsonl'
    try:
        for path in (preflight_path, status_path, journal_path):
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
                raise RunnerError(f'collection input is not a mode-0600 regular file: {path.name}')
        preflight_raw = preflight_path.read_bytes()
        preflight = FREEZER['load_json'](preflight_path)
        status = FREEZER['load_json'](status_path)
        journal_info = journal_path.lstat()
    except (OSError, FREEZER['FreezeError']) as error:
        raise RunnerError(f'collection evidence is incomplete: {error}') from error
    expected_keys = {'schema', 'schema_version', 'status', 'mode',
                     'planned_command_count', 'benchmark_launches_performed',
                     'input_sha256', 'command_freeze_path', 'command_freeze_sha256',
                     'host_attestation', 'frozen_preflight'}
    if type(preflight) is not dict or set(preflight) != expected_keys \
            or preflight.get('schema') != SCHEMA or preflight.get('schema_version') != 1 \
            or preflight.get('status') != 'ready' or preflight.get('benchmark_launches_performed') != 0:
        raise RunnerError('collector preflight schema/status is invalid')
    if FREEZER['contains_verdict_key'](preflight):
        raise RunnerError('collector preflight contains a verdict field')
    if type(status) is not dict or set(status) != {
            'schema', 'status', 'benchmark_launches_performed',
            'planned_command_count', 'preflight_sha256'} \
            or status.get('schema') != STATUS_SCHEMA or status.get('status') != 'ready' \
            or status.get('benchmark_launches_performed') != 0 \
            or status.get('planned_command_count') != preflight['planned_command_count'] \
            or status.get('preflight_sha256') != digest(preflight_raw):
        raise RunnerError('collection status is not fresh and ready')
    if not stat.S_ISREG(journal_info.st_mode) or stat.S_IMODE(journal_info.st_mode) != 0o600 \
            or journal_info.st_size != 0:
        raise RunnerError('collection journal is non-empty or invalid; resume is forbidden')
    entries = {path.name for path in output.iterdir()}
    required_entries = {'collector-preflight.json', 'status.json', 'journal.jsonl'}
    if entries not in (required_entries, required_entries | {RUN_LOCK}):
        raise RunnerError('collection directory contains unexpected files')
    if RUN_LOCK in entries:
        lock_info = (output / RUN_LOCK).lstat()
        if not stat.S_ISREG(lock_info.st_mode) or stat.S_IMODE(lock_info.st_mode) != 0o600:
            raise RunnerError('run lock is not a mode-0600 regular file')
    if expected_preflight is not None and stable_admission(preflight) != \
            stable_admission(expected_preflight):
        raise RunnerError('collection preflight differs from the active inputs')
    current_output, current = admit(args, expected_host=preflight['host_attestation']['initial'],
                                    host_probe=host_probe)
    if current_output != output or stable_admission(current) != stable_admission(preflight):
        raise RunnerError('current provenance differs from collection preflight')
    rows = preflight['frozen_preflight'].get('commands')
    expected_count = 108 if preflight['mode'] == 'aa_control' else 216
    if type(rows) is not list or len(rows) != expected_count \
            or preflight['planned_command_count'] != expected_count:
        raise RunnerError(f'collection must contain exactly {expected_count} frozen rows')
    return preflight, preflight_raw, status, journal_path, rows


def prepare_frozen_run_dirs(row, index, freeze_root):
    parent = freeze_root / f'run-{index:03d}'
    home = parent / 'home'
    tmp = parent / 'tmp'
    if row['environment'].get('HOME') != str(home) \
            or row['environment'].get('TMPDIR') != str(tmp):
        raise RunnerError(f'command {index} HOME/TMPDIR does not match frozen paths')
    if parent.exists() or home.exists() or tmp.exists() or parent.is_symlink():
        raise RunnerError(f'frozen per-command directories already exist for command {index}')
    parent.mkdir(mode=0o700)
    home.mkdir(mode=0o700)
    tmp.mkdir(mode=0o700)
    fsync_directory(home)
    fsync_directory(tmp)
    fsync_directory(parent)
    fsync_directory(freeze_root)
    return home, tmp


def per_cpu_probe(cpu, probe_factory=None):
    return probe_factory(cpu) if probe_factory is not None else FREEZER['CAL']['HostProbe'](
        affinity={cpu})


def enriched_snapshot(probe):
    snapshot = probe.snapshot()
    try:
        load_values = (probe.proc / 'loadavg').read_text(encoding='ascii').split()
        load_average = [float(value) for value in load_values[:3]]
        cgroup_identity = (probe.proc / 'self/cgroup').read_text(encoding='ascii').strip()
        if len(load_average) != 3 or any(not math.isfinite(value) or value < 0
                                         for value in load_average) or not cgroup_identity:
            raise ValueError('missing load average or cgroup identity')
        snapshot['load_average'] = load_average
        snapshot['cgroup_identity'] = cgroup_identity
    except (OSError, ValueError) as error:
        raise FREEZER['CAL']['CalibrationError'](
            f'load average/cgroup identity telemetry unavailable: {error}') from error
    return snapshot


def eligibility(before, after, wall_ns, row):
    diagnostics = []
    if before is None:
        diagnostics.append('required per-CPU pre-launch telemetry unavailable')
    if after is None:
        diagnostics.append('required per-CPU post-launch telemetry unavailable')
    if before is None or after is None:
        return dict(eligible=False, diagnostics=diagnostics,
                    cpu_psi_some_total_delta_usec=None,
                    cpu_psi_some_total_over_wall=None, cgroup_v2_delta=None,
                    smt_sibling=None)
    identity_fields = ('kernel', 'model', 'microcode', 'selected_cpu', 'online_cpus',
                       'affinity_cpus', 'smt_siblings', 'cgroup_identity')
    changed = [key for key in identity_fields if before[key] != after[key]]
    if changed:
        diagnostics.append('host/CPU/cgroup identity changed: ' + ', '.join(changed))
    if before['governor'] != after['governor']:
        diagnostics.append('CPU governor changed during benchmark process')
    for key in ('kernel', 'model', 'microcode', 'governor'):
        if row['host'][key] != before[key]:
            diagnostics.append(f'frozen {key} differs from pre-launch host telemetry')
    if row['cpu']['selected_cpu'] != before['selected_cpu']:
        diagnostics.append('per-CPU telemetry selected a different CPU than the frozen row')
    checked_result = FREEZER['CAL']['host_eligibility'](before, after, wall_ns)
    sibling = checked_result.get('smt_sibling') or {}
    unmeasurable = sibling.get('unmeasurable_cpus', [])
    if unmeasurable:
        diagnostics.append('SMT sibling telemetry unavailable for CPUs: ' +
                           ', '.join(str(cpu) for cpu in unmeasurable))
    diagnostics.extend(checked_result['diagnostics'])
    checked_result['eligible'] = not diagnostics
    checked_result['diagnostics'] = diagnostics
    checked_result['identity_drift'] = bool(changed) or any(
        row['host'][key] != before[key] for key in ('kernel', 'model', 'microcode')) \
        or row['cpu']['selected_cpu'] != before['selected_cpu'] \
        or row['cpu']['selected_cpu'] not in before['online_cpus'] \
        or row['cpu']['selected_cpu'] not in before['affinity_cpus'] \
        or row['cpu']['selected_cpu'] not in row['cpu']['online'] \
        or row['cpu']['selected_cpu'] not in row['cpu']['affinity']
    return checked_result


def run_campaign(args, *, popen_factory=None, affinity_getter=None, probe_factory=None,
                 host_probe=None):
    output = FREEZER['under_home'](args.output_dir, 'collection directory')
    host_probe = host_probe or FREEZER['CAL']['HostProbe']()
    preflight, preflight_raw, status, journal, rows = read_collection(
        output, args, host_probe=host_probe)
    if output.parent != Path(preflight['command_freeze_path']).resolve().parent:
        raise RunnerError('collection directory is not a direct child of its frozen command directory')
    CAL = FREEZER['CAL']
    CAL['_INTERRUPTED_SIGNAL'] = None
    CAL['_LAST_RUN_SIGNAL'] = None
    previous_handlers = {}
    def capture_signal(signum, frame):
        CAL['terminate_active_process'](signum, frame)
    try:
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            previous_handlers[signum] = signal.signal(signum, capture_signal)
    except (OSError, ValueError) as error:
        for signum, previous in previous_handlers.items():
            signal.signal(signum, previous)
        raise RunnerError(f'cannot install campaign signal handlers before claiming run: {error}',
                          'signal_setup') from error

    lock_path = output / RUN_LOCK
    lock_value = dict(schema='wirelog.batch-append-run-lock.v1', pid=os.getpid(),
                      created_monotonic_ns=time.monotonic_ns())
    run_id = secrets.token_hex(16)
    launched = 0
    eligible_count = 0
    ineligible_count = 0
    failure_index = None
    claimed = False
    current_status = dict(schema=STATUS_SCHEMA, status='running',
                          benchmark_launches_performed=0,
                          planned_command_count=len(rows),
                          preflight_sha256=digest(preflight_raw), run_id=run_id,
                          started_monotonic_ns=time.monotonic_ns())

    def revalidate():
        saved = (output / 'collector-preflight.json').read_bytes()
        if digest(saved) != digest(preflight_raw):
            raise RunnerError('collector preflight bytes changed during execution')
        current_output, current = admit(args, expected_host=preflight['host_attestation']['initial'],
                                        host_probe=host_probe)
        if current_output != output or stable_admission(current) != stable_admission(preflight):
            raise RunnerError('source/profile/fallback/binary provenance drifted')

    def incomplete(category, index, error):
        return dict(schema=STATUS_SCHEMA, status='incomplete_capture',
                    benchmark_launches_performed=launched,
                    planned_command_count=len(rows), preflight_sha256=digest(preflight_raw),
                    run_id=run_id, failure=dict(category=category, command_index=index,
                                                error=str(error)))

    try:
        if CAL.get('_INTERRUPTED_SIGNAL') is not None:
            raise RunnerError(f'interrupted by signal {CAL["_INTERRUPTED_SIGNAL"]}', 'interrupted')
        try:
            fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError as error:
            raise RunnerError(
                'campaign run lock already exists; concurrent/retry/resume is forbidden') from error
        claimed = True
        with os.fdopen(fd, 'wb') as stream:
            stream.write((json.dumps(lock_value, sort_keys=True) + '\n').encode('utf-8'))
            stream.flush()
            os.fsync(stream.fileno())
        fsync_directory(output)
        if CAL.get('_INTERRUPTED_SIGNAL') is not None:
            raise RunnerError(f'interrupted by signal {CAL["_INTERRUPTED_SIGNAL"]}', 'interrupted')
        write_status(output / 'status.json', current_status)
        if CAL.get('_INTERRUPTED_SIGNAL') is not None:
            raise RunnerError(f'interrupted by signal {CAL["_INTERRUPTED_SIGNAL"]}', 'interrupted')
        revalidate()
        for index, row in enumerate(rows):
            failure_index = index
            if CAL.get('_INTERRUPTED_SIGNAL') is not None:
                raise RunnerError(f'interrupted by signal {CAL["_INTERRUPTED_SIGNAL"]}')
            revalidate()
            if row['command_index'] != index:
                raise RunnerError('frozen command indices are not a complete ordered sequence')
            selected_cpu = row['cpu']['selected_cpu']
            home, tmp = prepare_frozen_run_dirs(
                row, index, Path(preflight['command_freeze_path']).resolve().parent)
            before_probe = None
            try:
                before_probe = per_cpu_probe(selected_cpu, probe_factory)
                host_before = enriched_snapshot(before_probe)
                before_error = None
            except (CAL['CalibrationError'], OSError, ValueError) as error:
                host_before = None
                before_error = str(error)
            started_mono = time.monotonic_ns()
            started = dict(event='started', run_id=run_id, command_index=index,
                           row_sha256=digest(json.dumps(row, sort_keys=True,
                                                       separators=(',', ':')).encode('utf-8')),
                           input_sha256=preflight['input_sha256'],
                           collector_preflight_sha256=digest(preflight_raw),
                           command_row=row, monotonic_started_ns=started_mono,
                           timeout_seconds=RUN_TIMEOUT_SECONDS,
                           host_before=host_before, host_before_error=before_error,
                           created_home=str(home), created_tmpdir=str(tmp))
            append_journal(journal, started)
            result = CAL['launch_process'](
                row['argv'], row['cwd'], row['environment'], RUN_TIMEOUT_SECONDS,
                popen_factory=(popen_factory or __import__('subprocess').Popen),
                selected_cpu=selected_cpu,
                affinity_getter=(affinity_getter or os.sched_getaffinity))
            if result.get('interrupted_signal') is not None:
                CAL['_LAST_RUN_SIGNAL'] = result['interrupted_signal']
            if result.get('spawn_error') is None:
                launched += 1
            stdout_path = output / f'command-{index:03d}.stdout.bin'
            stderr_path = output / f'command-{index:03d}.stderr.bin'
            write_durable(stdout_path, result['stdout'])
            write_durable(stderr_path, result['stderr'])
            fsync_directory(output)
            try:
                if before_probe is None:
                    raise CAL['CalibrationError']('per-CPU probe could not be created')
                host_after = enriched_snapshot(before_probe)
                after_error = None
            except (CAL['CalibrationError'], OSError, ValueError) as error:
                host_after = None
                after_error = str(error)
            telemetry = eligibility(host_before, host_after, result['wall_ns'], row)
            parse_error = None
            parsed = None
            try:
                parsed = FREEZER['OUTPUT']['parse_output'](
                    result['stdout'], row['case'], row['iterations'], warmups=2, samples=1)
            except FREEZER['OUTPUT']['OutputError'] as error:
                parse_error = str(error)
            provenance_error = None
            try:
                revalidate()
            except Exception as error:
                provenance_error = str(error)
            process_error = None
            if result['timed_out']:
                process_error = 'benchmark process timed out and process group was terminated'
            elif result.get('interrupted_signal') is not None:
                process_error = f"interrupted by signal {result['interrupted_signal']}"
            elif result.get('spawn_error') is not None:
                process_error = f"benchmark process could not start: {result['spawn_error']}"
            elif result.get('child_affinity_error') is not None:
                process_error = f"child CPU affinity unavailable: {result['child_affinity_error']}"
            elif result.get('child_affinity_cpus') != [selected_cpu]:
                process_error = 'child CPU affinity does not match the selected frozen CPU'
            elif result['exit_code'] != 0:
                process_error = f"benchmark exited with status {result['exit_code']}"
            identity_error = ('host/CPU/cgroup identity changed during command'
                              if telemetry.get('identity_drift') else None)
            fatal = process_error or parse_error or provenance_error or identity_error
            finished = dict(event='result', run_id=run_id, command_index=index,
                            row_sha256=started['row_sha256'],
                            input_sha256=preflight['input_sha256'],
                            monotonic_finished_ns=result['monotonic_finished_ns'],
                            wall_ns=result['wall_ns'],
                            exit_code=result['exit_code'], timed_out=result['timed_out'],
                            spawn_error=result.get('spawn_error'),
                            interrupted_signal=result.get('interrupted_signal'),
                            child_affinity_cpus=result.get('child_affinity_cpus'),
                            child_affinity_error=result.get('child_affinity_error'),
                            stdout_path=stdout_path.name, stdout_sha256=digest(result['stdout']),
                            stderr_path=stderr_path.name, stderr_sha256=digest(result['stderr']),
                            host_before=host_before, host_before_error=before_error,
                            host_after=host_after, host_after_error=after_error,
                            host_eligibility=telemetry, parsed=parsed,
                            process_error=process_error, output_error=parse_error,
                            provenance_error=provenance_error,
                            identity_error=identity_error)
            append_journal(journal, finished)
            if telemetry['eligible']:
                eligible_count += 1
            else:
                ineligible_count += 1
            if fatal:
                category = ('process' if process_error else
                            'correctness_output' if parse_error else
                            'host_identity' if identity_error else 'provenance')
                if result.get('interrupted_signal') is not None:
                    category = 'interrupted'
                raise RunnerError(f'{category} failure at command {index}: {fatal}', category)
        failure_index = None
        if CAL.get('_INTERRUPTED_SIGNAL') is not None:
            raise RunnerError(f'interrupted by signal {CAL["_INTERRUPTED_SIGNAL"]}', 'interrupted')
        revalidate()
        if CAL.get('_INTERRUPTED_SIGNAL') is not None:
            raise RunnerError(f'interrupted by signal {CAL["_INTERRUPTED_SIGNAL"]}', 'interrupted')
        complete = dict(schema=STATUS_SCHEMA, status='complete_capture',
                        benchmark_launches_performed=launched,
                        planned_command_count=len(rows),
                        preflight_sha256=digest(preflight_raw), run_id=run_id,
                        eligibility_summary=dict(eligible=eligible_count,
                                                 ineligible=ineligible_count))
        write_status(output / 'status.json', complete)
        return output / 'status.json', complete
    except BaseException as error:
        category = ('interrupted' if CAL.get('_INTERRUPTED_SIGNAL') is not None
                    or CAL.get('_LAST_RUN_SIGNAL') is not None else
                    error.category if isinstance(error, RunnerError) else
                    'host_provenance' if isinstance(error, CollectionError) else 'evidence_or_process')
        failed = incomplete(category, failure_index, error)
        if claimed:
            try:
                write_status(output / 'status.json', failed)
            except BaseException:
                pass
        if isinstance(error, RunnerError):
            raise
        raise RunnerError(f'campaign capture failed: {error}') from error
    finally:
        for signum, previous in previous_handlers.items():
            signal.signal(signum, previous)


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
    result.add_argument('--run', action='store_true',
                        help='execute the already admitted frozen schedule once')
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        output, artifact = (run_campaign(args) if args.run else collect(args))
    except (OSError, CollectionError, RunnerError) as error:
        print(f'collect-batch-append-campaign: {error}', file=sys.stderr)
        if isinstance(error, RunnerError) and error.category == 'interrupted':
            signum = (FREEZER['CAL'].get('_INTERRUPTED_SIGNAL')
                      or FREEZER['CAL'].get('_LAST_RUN_SIGNAL'))
            return 128 + signum if signum else 130
        return 2
    action = 'Captured' if args.run else 'Admitted'
    print(f"{action} {artifact['planned_command_count']} frozen commands in {output}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
