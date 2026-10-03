#!/usr/bin/env python3
"""Freeze provenance-bound paired benchmark commands without launching them."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import stat
import sys

HERE = Path(__file__).resolve().parent
PLAN = runpy.run_path(str(HERE / 'prepare-batch-append-campaign.py'))
PROFILE = runpy.run_path(str(HERE / 'prepare-batch-append-execution.py'))
CAL = runpy.run_path(str(HERE / 'calibrate-batch-append.py'))
OUTPUT = runpy.run_path(str(HERE / 'batch_append_output_v2.py'))
SCHEMA = 'wirelog.batch-append-command-freeze.v1'
CAL_SCHEMA = 'wirelog.batch-append-calibration.v1'
CASE_NAMES = ('1x1', '1x256', '32x256')
MIN_APPEND_NS = 250_000_000
MAX_ATTEMPTS = 6
BASE_TREE = '4c10e21ab4ba034dbc50abd92a59d22e8b975130'


class FreezeError(ValueError):
    pass


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def load_json(path):
    try:
        return json.loads(Path(path).read_text(encoding='utf-8'),
                          object_pairs_hook=PROFILE['unique_object'],
                          parse_constant=PROFILE['reject_constant'])
    except (OSError, UnicodeError, json.JSONDecodeError,
            PROFILE['PreflightError']) as error:
        raise FreezeError(f'cannot read strict JSON {path}: {error}') from error


def raw_hash(path):
    try:
        return sha256(Path(path).read_bytes())
    except OSError as error:
        raise FreezeError(f'cannot hash {path}: {error}') from error


def under_home(path, label):
    try:
        result = PROFILE['under_home'](path, label)
    except PROFILE['PreflightError'] as error:
        raise FreezeError(str(error)) from error
    return result


def exact_keys(value, expected, label):
    if type(value) is not dict or set(value) != set(expected):
        raise FreezeError(f'{label} fields differ from the required schema')


def contains_verdict_key(value):
    forbidden = {'performance_verdict', 'performance_ratio', 'ratio', 'ratios',
                 'verdict', 'verdict_status'}
    if type(value) is dict:
        return any(key in forbidden or contains_verdict_key(item)
                   for key, item in value.items())
    if type(value) is list:
        return any(contains_verdict_key(item) for item in value)
    return False


def strict_plan(path, overlay):
    path = under_home(path, 'campaign plan')
    try:
        plan, digest = PROFILE['validate_plan'](path, overlay)
    except (PROFILE['PreflightError'], PLAN['PlanError']) as error:
        raise FreezeError(f'plan/source revalidation failed: {error}') from error
    if plan['mode'] == 'aa_control' and plan.get('aa_product') != 'pre':
        raise FreezeError('A/A command freeze supports only explicit product pre')
    if plan['sources']['base']['product_tree'] != BASE_TREE:
        raise FreezeError('calibration and command execution require fixed pre baseline tree')
    return path, plan, digest


def profile_for_plan(plan_path, plan, overlay, profile_path):
    profile_path = under_home(profile_path, 'execution profile')
    profile_raw = profile_path.read_bytes()
    profile = load_json(profile_path)
    if type(profile) is not dict or profile.get('schema') != PROFILE['ARTIFACT_SCHEMA'] \
            or profile.get('status') != 'not_executable' \
            or profile.get('source_build_verified') is not True \
            or profile.get('not_executable') is not True \
            or profile.get('benchmark_launches_performed') != 0:
        raise FreezeError('expected a profile-v2 not-executable preflight artifact')
    if profile.get('plan_sha256') != sha256(plan_path.read_bytes()):
        raise FreezeError('profile does not bind exact plan bytes')
    if profile.get('mode') != plan['mode']:
        raise FreezeError('profile mode does not match plan')
    for side in ('base', 'candidate'):
        if type(profile.get(side)) is not dict:
            raise FreezeError(f'profile is missing {side} build evidence')
    try:
        expected = PROFILE['build_artifact'](
            plan_path, overlay, profile['base']['build_root'],
            profile['candidate']['build_root'])
    except (OSError, PROFILE['PreflightError'], PLAN['PlanError']) as error:
        raise FreezeError(f'profile/source/build revalidation failed: {error}') from error
    if profile != expected:
        raise FreezeError('profile artifact does not match current sources/builds/binaries')
    for side in ('base', 'candidate'):
        binary = Path(profile[side]['binary_path'])
        try:
            mode = binary.stat().st_mode
        except OSError as error:
            raise FreezeError(f'{side} benchmark binary is unavailable: {error}') from error
        if not stat.S_ISREG(mode) or not mode & 0o111:
            raise FreezeError(f'{side} benchmark binary is not an executable regular file')
    return profile_path, profile, sha256(profile_raw)


def profile_semantic_copy(profile_a, profile_b):
    left, right = dict(profile_a), dict(profile_b)
    left.pop('plan_sha256', None)
    right.pop('plan_sha256', None)
    left.pop('plan_path', None)
    right.pop('plan_path', None)
    if left != right:
        raise FreezeError('second profile differs beyond its bound plan hash')


def plan_semantic_copy(plan_a, plan_b):
    left, right = json.loads(json.dumps(plan_a)), json.loads(json.dumps(plan_b))
    for plan in (left, right):
        plan.pop('created_utc', None)
        plan.pop('seed', None)
        plan['schedule'].pop('launches', None)
    if left != right:
        raise FreezeError('second plan differs beyond seed, derived schedule, or creation time')
    if plan_a['seed'] == plan_b['seed']:
        raise FreezeError('comparison plans must use distinct schedule seeds')


def evidence_snapshot(root):
    """Hash every file, reject symlinks/special files, and return a relative map."""
    root = Path(root).resolve()
    result = {}
    for path in sorted(root.rglob('*')):
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise FreezeError(f'calibration evidence contains symlink {path}')
        if stat.S_ISREG(info.st_mode):
            result[path.relative_to(root).as_posix()] = sha256(path.read_bytes())
        elif not stat.S_ISDIR(info.st_mode):
            raise FreezeError(f'calibration evidence contains special file {path}')
    return result


def host_identity(host):
    return {key: host[key] for key in (
        'kernel', 'model', 'microcode', 'online_cpus', 'affinity_cpus', 'selected_cpu',
        'smt_siblings')}


def validate_host_snapshot(host, label):
    exact_keys(host, ('timestamp_utc', 'kernel', 'online_cpus', 'affinity_cpus',
                      'selected_cpu', 'governor', 'frequency_khz', 'model', 'microcode',
                      'cpu_psi_some_total_usec', 'cgroup_v2_cpu', 'smt_siblings',
                      'sibling_cpu_ticks'), label)
    if type(host['timestamp_utc']) is not int or host['timestamp_utc'] <= 0:
        raise FreezeError(f'{label} timestamp is invalid')
    for key in ('kernel', 'governor', 'model', 'microcode'):
        if type(host[key]) is not str or not host[key]:
            raise FreezeError(f'{label} {key} is invalid')
    for key in ('online_cpus', 'affinity_cpus', 'smt_siblings'):
        value = host[key]
        if type(value) is not list or any(type(cpu) is not int or cpu < 0 for cpu in value) \
                or value != sorted(set(value)):
            raise FreezeError(f'{label} {key} is invalid')
    if type(host['selected_cpu']) is not int or host['selected_cpu'] not in host['online_cpus'] \
            or host['selected_cpu'] not in host['affinity_cpus'] \
            or host['selected_cpu'] in host['smt_siblings']:
        raise FreezeError(f'{label} selected CPU/topology is invalid')
    if type(host['frequency_khz']) is not int or host['frequency_khz'] <= 0:
        raise FreezeError(f'{label} frequency is invalid')
    if type(host['cpu_psi_some_total_usec']) is not int or host['cpu_psi_some_total_usec'] < 0:
        raise FreezeError(f'{label} PSI counter is invalid')
    exact_keys(host['cgroup_v2_cpu'], ('nr_throttled', 'throttled_usec'),
               f'{label} cgroup counters')
    if any(type(value) is not int or value < 0 for value in host['cgroup_v2_cpu'].values()):
        raise FreezeError(f'{label} cgroup counters are invalid')
    ticks = host['sibling_cpu_ticks']
    if type(ticks) is not dict:
        raise FreezeError(f'{label} SMT sibling ticks are invalid')
    for cpu, counters in ticks.items():
        try:
            parsed_cpu = int(cpu)
        except (TypeError, ValueError) as error:
            raise FreezeError(f'{label} SMT CPU key is invalid') from error
        if str(parsed_cpu) != cpu or parsed_cpu not in host['smt_siblings']:
            raise FreezeError(f'{label} SMT CPU key is invalid')
        exact_keys(counters, ('total', 'busy'), f'{label} SMT counters')
        if any(type(value) is not int or value < 0 for value in counters.values()) \
                or counters['busy'] > counters['total']:
            raise FreezeError(f'{label} SMT counters are invalid')


def validate_host_summaries(host_summary, attempts):
    exact_keys(host_summary, ('initial', 'final'), 'calibration host summary')
    initial, final = host_summary['initial'], host_summary['final']
    validate_host_snapshot(initial, 'initial host summary')
    validate_host_snapshot(final, 'final host summary')
    snapshots = [initial]
    for index, attempt in enumerate(attempts):
        started, result = attempt['started'], attempt
        before, after = started['host_before'], result['host_after']
        validate_host_snapshot(before, f'attempt {index + 1} host-before')
        validate_host_snapshot(after, f'attempt {index + 1} host-after')
        snapshots.extend((before, after))
    snapshots.append(final)
    baseline_identity = host_identity(snapshots[0])
    for snapshot in snapshots[1:]:
        if host_identity(snapshot) != baseline_identity:
            raise FreezeError('host identity or SMT topology changed across calibration')
    if initial['timestamp_utc'] > snapshots[1]['timestamp_utc'] \
            or final['timestamp_utc'] < snapshots[-2]['timestamp_utc']:
        raise FreezeError('host summary timestamps do not bracket calibration attempts')
    tick_keys = set(snapshots[0]['sibling_cpu_ticks'])
    for previous, current in zip(snapshots, snapshots[1:]):
        if current['timestamp_utc'] < previous['timestamp_utc']:
            raise FreezeError('host telemetry timestamp moved backwards')
        if current['cpu_psi_some_total_usec'] < previous['cpu_psi_some_total_usec']:
            raise FreezeError('CPU PSI counter reset across calibration')
        if set(current['sibling_cpu_ticks']) != tick_keys:
            raise FreezeError('SMT sibling telemetry availability changed across calibration')
        for key in ('nr_throttled', 'throttled_usec'):
            if current['cgroup_v2_cpu'][key] < previous['cgroup_v2_cpu'][key]:
                raise FreezeError(f'cgroup counter {key} reset across calibration')
        for cpu in tick_keys:
            for key in ('total', 'busy'):
                if current['sibling_cpu_ticks'][cpu][key] < previous['sibling_cpu_ticks'][cpu][key]:
                    raise FreezeError(f'SMT sibling {key} counter reset across calibration')


def read_attempt_file(path):
    return load_json(path)


def host_for_eligibility(host):
    normalized = dict(host)
    ticks = host.get('sibling_cpu_ticks')
    if type(ticks) is not dict:
        raise FreezeError('SMT sibling tick telemetry is malformed')
    try:
        converted = {int(cpu): value for cpu, value in ticks.items()}
    except (TypeError, ValueError) as error:
        raise FreezeError('SMT sibling CPU identifiers are malformed') from error
    if len(converted) != len(ticks) or any(str(cpu) != key for key, cpu in
                                             ((key, int(key)) for key in ticks)):
        raise FreezeError('SMT sibling CPU identifiers are duplicated or non-canonical')
    normalized['sibling_cpu_ticks'] = converted
    return normalized


def validate_calibration(plan_path, plan, plan_hash, profile_path, profile,
                         profile_hash, overlay, evidence_arg):
    evidence = under_home(evidence_arg, 'calibration evidence directory')
    if not evidence.is_dir():
        raise FreezeError(f'calibration evidence directory does not exist: {evidence}')
    sources = [Path(plan['sources'][side]['source_root']).resolve()
               for side in ('base', 'candidate')]
    builds = [Path(profile[side]['build_root']).resolve()
              for side in ('base', 'candidate')]
    for path, label, roots in ((evidence, 'calibration evidence', sources),
                               (evidence, 'calibration evidence', builds)):
        PROFILE['outside_sources'](path, roots, label,
                                   'source worktrees' if roots is sources else 'build directories')
    cal_path = evidence / 'calibration.json'
    cal_raw = cal_path.read_bytes()
    artifact = load_json(cal_path)
    exact_keys(artifact, ('schema', 'status', 'plan_sha256', 'profile_sha256',
                          'revision_manifest_sha256', 'base_product_tree',
                          'baseline_binary_sha256', 'private_binary_sha256', 'host',
                          'accepted_iteration_counts', 'attempts', 'candidate_launches'),
               'calibration artifact')
    if artifact['schema'] != CAL_SCHEMA or artifact['status'] != 'calibrated' \
            or type(artifact['candidate_launches']) is not int \
            or artifact['candidate_launches'] != 0:
        raise FreezeError('calibration artifact status or candidate launch count is invalid')
    if artifact['plan_sha256'] != plan_hash or artifact['profile_sha256'] != profile_hash:
        raise FreezeError('calibration is not bound to these exact plan/profile bytes')
    if artifact['revision_manifest_sha256'] != plan['revision_manifest']['sha256'] \
            or artifact['base_product_tree'] != BASE_TREE:
        raise FreezeError('calibration revision manifest or baseline product tree differs')
    try:
        CAL['validate_bound_inputs'](plan_path, profile_path, overlay,
                                     plan_hash, profile_hash,
                                     profile['base']['binary_sha256'])
    except (CAL['CalibrationError'], PROFILE['PreflightError'], PLAN['PlanError']) as error:
        raise FreezeError(f'calibration-bound input revalidation failed: {error}') from error
    base_binary = Path(profile['base']['binary_path']).resolve()
    if raw_hash(base_binary) != artifact['baseline_binary_sha256'] \
            or artifact['baseline_binary_sha256'] != profile['base']['binary_sha256']:
        raise FreezeError('original baseline binary hash differs from profile/calibration')
    private_binary = evidence / 'private-bin/bench_batch_append'
    try:
        private_info = private_binary.lstat()
    except OSError as error:
        raise FreezeError(f'private baseline copy is missing: {error}') from error
    if not stat.S_ISREG(private_info.st_mode) or not private_info.st_mode & 0o111 \
            or raw_hash(private_binary) != artifact['private_binary_sha256'] \
            or artifact['private_binary_sha256'] != artifact['baseline_binary_sha256']:
        raise FreezeError('private baseline executable/hash does not match original baseline')
    expected_counts = artifact['accepted_iteration_counts']
    if type(expected_counts) is not dict or set(expected_counts) != set(CASE_NAMES):
        raise FreezeError('calibration must accept exactly the three benchmark cases')
    if any(type(value) is not int or value <= 0 for value in expected_counts.values()):
        raise FreezeError('calibration accepted counts must be positive integers')
    if type(artifact['attempts']) is not list or not artifact['attempts']:
        raise FreezeError('calibration artifact has no attempts')

    expected_paths = {'calibration.json', 'private-home', 'private-tmp', 'private-bin',
                      'private-bin/bench_batch_append'}
    for fixed_dir in ('private-home', 'private-tmp'):
        if not (evidence / fixed_dir).is_dir() or any((evidence / fixed_dir).iterdir()):
            raise FreezeError(f'{fixed_dir} must be an existing empty private directory')
    if not (evidence / 'private-bin').is_dir():
        raise FreezeError('private-bin directory is missing')

    case_iterations = {case: None for case in CASE_NAMES}
    attempt_by_case = {case: [] for case in CASE_NAMES}
    common_host_identity = None
    positions = {case: index for index, case in enumerate(CASE_NAMES)}
    previous_case_position = -1
    for attempt in artifact['attempts']:
        if type(attempt) is not dict or type(attempt.get('started')) is not dict:
            raise FreezeError('calibration attempt record is malformed')
        started = attempt['started']
        case = started.get('case')
        if case not in positions or positions[case] < previous_case_position:
            raise FreezeError('calibration case attempts are not in canonical case order')
        previous_case_position = positions[case]
        attempt_by_case[case].append(attempt)

    all_attempts = []
    env_root = evidence
    expected_environment = dict(PATH='/usr/bin:/bin', HOME=str(env_root / 'private-home'),
                                TMPDIR=str(env_root / 'private-tmp'), LANG='C',
                                LC_ALL='C', TZ='UTC')
    for case in CASE_NAMES:
        records = attempt_by_case[case]
        if not records or len(records) > MAX_ATTEMPTS:
            raise FreezeError(f'{case} must have between one and six attempts')
        case_contract = next((item for item in plan['benchmark_case_contract']['cases']
                              if item.get('name') == case), None)
        if type(case_contract) is not dict:
            raise FreezeError(f'plan case contract is missing {case}')
        expected_n = case_contract.get('default_iterations_start')
        if type(expected_n) is not int or expected_n <= 0:
            raise FreezeError(f'plan starting count is invalid for {case}')
        for index, record in enumerate(records, start=1):
            started = record['started']
            exact_keys(started, ('schema', 'case', 'attempt_index', 'iterations', 'argv', 'cwd',
                                 'environment', 'plan_sha256', 'profile_sha256',
                                 'original_binary_sha256', 'private_binary_sha256',
                                 'monotonic_started_ns', 'host_before'),
                       f'{case} started record')
            if started['schema'] != 'wirelog.batch-append-calibration-attempt.v1' \
                    or started['case'] != case or type(started['attempt_index']) is not int \
                    or started['attempt_index'] != index \
                    or type(started['iterations']) is not int or started['iterations'] != expected_n:
                raise FreezeError(f'{case} attempt order/count chain is invalid')
            if started['plan_sha256'] != plan_hash or started['profile_sha256'] != profile_hash \
                    or started['original_binary_sha256'] != artifact['baseline_binary_sha256'] \
                    or started['private_binary_sha256'] != artifact['private_binary_sha256']:
                raise FreezeError(f'{case} started record is bound to another input/binary')
            expected_argv = [str(private_binary), '--case', case,
                             '--iterations', str(expected_n), '--warmups', '2', '--samples', '1']
            if started['argv'] != expected_argv or started['cwd'] != \
                    str(Path(profile['base']['build_root']).resolve()) \
                    or started['environment'] != expected_environment:
                raise FreezeError(f'{case} argv/cwd/environment differs from calibration contract')
            if type(started['host_before']) is not dict:
                raise FreezeError(f'{case} host-before telemetry is malformed')
            host_before = started['host_before']
            validate_host_snapshot(host_before, f'{case} host-before telemetry')
            if host_before.get('selected_cpu') not in host_before.get('affinity_cpus', []) \
                    or host_before.get('selected_cpu') not in host_before.get('online_cpus', []):
                raise FreezeError(f'{case} selected CPU was not online and allowed')
            before_identity = host_identity(host_before)
            if common_host_identity is None:
                common_host_identity = before_identity
            elif before_identity != common_host_identity:
                raise FreezeError('host/CPU identity changed across calibration attempts')

            prefix = f'{case}-attempt-{index:02d}'
            started_path = evidence / f'{prefix}-started.json'
            result_path = evidence / f'{prefix}-result.json'
            stdout_path = evidence / f'{prefix}-stdout.bin'
            stderr_path = evidence / f'{prefix}-stderr.bin'
            stored_started = read_attempt_file(started_path)
            stored_result = read_attempt_file(result_path)
            if stored_started != started or stored_result != record:
                raise FreezeError(f'{case} attempt JSON differs from calibration artifact')
            expected_paths.update(path.name for path in
                                  (started_path, result_path, stdout_path, stderr_path))
            stdout = stdout_path.read_bytes()
            stderr = stderr_path.read_bytes()
            if sha256(stdout) != record.get('stdout_sha256') \
                    or sha256(stderr) != record.get('stderr_sha256') \
                    or record.get('stdout_path') != stdout_path.name \
                    or record.get('stderr_path') != stderr_path.name:
                raise FreezeError(f'{case} raw output hash/path mismatch')
            exact_keys(record, ('started', 'process_monotonic_started_ns',
                                'process_monotonic_finished_ns', 'stdout_path',
                                'stdout_sha256', 'stderr_path', 'stderr_sha256', 'exit_code',
                                'timed_out', 'wall_ns', 'spawn_error', 'interrupted_signal',
                                'child_affinity_cpus', 'child_affinity_error', 'host_after',
                                'telemetry', 'eligible', 'accepted', 'append_total_ns',
                                'diagnostic', 'parsed'), f'{case} result record')
            if record['exit_code'] != 0 or record['timed_out'] is not False \
                    or record['spawn_error'] is not None or record['interrupted_signal'] is not None \
                    or record['diagnostic'] is not None:
                raise FreezeError(f'{case} calibration process did not succeed cleanly')
            if record['child_affinity_error'] is not None \
                    or record['child_affinity_cpus'] != [host_before['selected_cpu']]:
                raise FreezeError(f'{case} launched child affinity was not verified')
            start_ns = record['process_monotonic_started_ns']
            finish_ns = record['process_monotonic_finished_ns']
            if type(start_ns) is not int or type(finish_ns) is not int \
                    or type(record['wall_ns']) is not int \
                    or finish_ns < start_ns or record['wall_ns'] != finish_ns - start_ns \
                    or started['monotonic_started_ns'] > start_ns:
                raise FreezeError(f'{case} monotonic timing evidence is inconsistent')
            try:
                parsed = OUTPUT['parse_output'](stdout, case, expected_n,
                                                warmups=2, samples=1)
            except OUTPUT['OutputError'] as error:
                raise FreezeError(f'{case} raw stdout violates benchmark-v2 contract: {error}') from error
            if record['parsed'] != parsed or record['append_total_ns'] != parsed['append_total_ns']:
                raise FreezeError(f'{case} parsed output does not match raw benchmark stdout')
            if type(record['host_after']) is not dict:
                raise FreezeError(f'{case} post-attempt telemetry is missing')
            validate_host_snapshot(record['host_after'], f'{case} host-after telemetry')
            if host_identity(record['host_after']) != before_identity:
                raise FreezeError(f'{case} host/CPU identity changed during calibration process')
            telemetry = CAL['host_eligibility'](
                host_for_eligibility(host_before), host_for_eligibility(record['host_after']),
                record['wall_ns'])
            if telemetry != record['telemetry'] or telemetry['eligible'] is not True \
                    or record['eligible'] is not True:
                raise FreezeError(f'{case} telemetry eligibility cannot be reconstructed')
            expected_accept = index == len(records)
            if record['accepted'] is not expected_accept:
                raise FreezeError(f'{case} calibration did not stop at its first accepted attempt')
            if expected_accept:
                if parsed['append_total_ns'] < MIN_APPEND_NS:
                    raise FreezeError(f'{case} final attempt is below the acceptance threshold')
                case_iterations[case] = expected_n
            else:
                if parsed['append_total_ns'] >= MIN_APPEND_NS:
                    raise FreezeError(f'{case} calibration continued after first eligible acceptance')
                expected_n = CAL['next_iterations'](expected_n, parsed['append_total_ns'])
            all_attempts.append(record)
        if case_iterations[case] != expected_counts[case]:
            raise FreezeError(f'{case} accepted count differs from reconstructed attempts')

    if all_attempts != artifact['attempts']:
        raise FreezeError('calibration attempts are not a canonical three-case sequence')
    validate_host_summaries(artifact['host'], artifact['attempts'])
    actual_paths = {path.relative_to(evidence).as_posix() for path in evidence.rglob('*')}
    if actual_paths != expected_paths:
        raise FreezeError('calibration evidence directory has missing or extra files')
    evidence_hashes = evidence_snapshot(evidence)
    return dict(evidence_path=str(evidence), calibration_path=str(cal_path),
                calibration_sha256=sha256(cal_raw), evidence_files_sha256=evidence_hashes,
                accepted_iteration_counts=case_iterations,
                host_identity=common_host_identity,
                attempt_by_case={case: next(record for record in artifact['attempts']
                                            if record['started']['case'] == case
                                            and record['accepted'] is True)
                                 for case in CASE_NAMES})


def validate_aa_origin(target, origin):
    target_plan, target_profile = target['plan'], target['profile']
    origin_plan, origin_profile = origin['plan'], origin['profile']
    if origin_plan.get('mode') != 'comparison' or target_plan.get('mode') != 'aa_control' \
            or target_plan.get('aa_product') != 'pre':
        raise FreezeError('A/A calibration origin must be comparison-A and target must be A/A-pre')
    if target_plan['revision_manifest'] != origin_plan['revision_manifest'] \
            or target_plan['overlay'] != origin_plan['overlay'] \
            or target_plan['unchanged_helpers'] != origin_plan['unchanged_helpers'] \
            or target_plan['benchmark_case_contract'] != origin_plan['benchmark_case_contract'] \
            or target_plan['benchmark_output_contract'] != origin_plan['benchmark_output_contract']:
        raise FreezeError('A/A target differs from calibration origin manifest/overlay/helpers/contracts')
    for side in ('base', 'candidate'):
        source = target_plan['sources'][side]
        if source['product_tree'] != BASE_TREE:
            raise FreezeError('A/A target source is not the pre product tree')
        compared = dict(source)
        compared.pop('source_root', None)
        expected = dict(origin_plan['sources']['base'])
        expected.pop('source_root', None)
        if compared != expected:
            raise FreezeError('A/A target source provenance differs from calibration base')
        if target_profile[side]['profile_sha256'] != origin_profile['base']['profile_sha256']:
            raise FreezeError('A/A target normalized build profile differs from calibration origin')
        if target_profile[side]['binary_sha256'] != origin_profile['base']['binary_sha256']:
            raise FreezeError('A/A target binary differs from calibrated baseline binary')
    if target_profile['profile_sha256'] != origin_profile['profile_sha256']:
        raise FreezeError('A/A target profile fingerprint differs from calibration origin')


def validate_inputs(mode, overlay_arg, plan_paths, profile_paths, calibration_dirs,
                    calibration_origin=None):
    overlay = under_home(overlay_arg, 'overlay patch')
    expected_inputs = 2 if mode == 'comparison' else 1
    if len(plan_paths) != expected_inputs or len(profile_paths) != expected_inputs \
            or len(calibration_dirs) != 1:
        raise FreezeError('comparison requires two plans/profiles and one calibration; A/A requires one of each')
    normalized = []
    input_paths = {str(overlay): raw_hash(overlay)}
    for plan_arg, profile_arg in zip(plan_paths, profile_paths):
        plan_path, plan, plan_hash = strict_plan(plan_arg, overlay)
        if plan['mode'] != mode:
            raise FreezeError('requested freeze mode differs from plan mode')
        profile_path, profile, profile_hash = profile_for_plan(
            plan_path, plan, overlay, profile_arg)
        roots = [Path(plan['sources'][side]['source_root']).resolve()
                 for side in ('base', 'candidate')]
        for path in (plan_path, profile_path):
            try:
                PROFILE['outside_sources'](path, roots, 'command-freeze input')
            except PROFILE['PreflightError'] as error:
                raise FreezeError(str(error)) from error
        input_paths[str(plan_path)] = plan_hash
        input_paths[str(profile_path)] = profile_hash
        normalized.append(dict(plan_path=plan_path, plan=plan, plan_sha256=plan_hash,
                               profile_path=profile_path, profile=profile,
                               profile_sha256=profile_hash))
    if mode == 'comparison':
        plan_semantic_copy(normalized[0]['plan'], normalized[1]['plan'])
        profile_semantic_copy(normalized[0]['profile'], normalized[1]['profile'])
    elif mode == 'aa_control':
        if normalized[0]['plan'].get('aa_product') != 'pre':
            raise FreezeError('A/A freeze requires one explicit aa_control plan selecting pre')
        if calibration_origin is None or len(calibration_origin) != 2 \
                or any(value is None for value in calibration_origin):
            raise FreezeError('A/A freeze requires exact calibration-origin plan/profile inputs')
        origin_plan_path, origin_plan, origin_plan_hash = strict_plan(calibration_origin[0], overlay)
        origin_profile_path, origin_profile, origin_profile_hash = profile_for_plan(
            origin_plan_path, origin_plan, overlay, calibration_origin[1])
        if origin_plan['mode'] != 'comparison':
            raise FreezeError('calibration origin must be a comparison plan')
        roots = [Path(origin_plan['sources'][side]['source_root']).resolve()
                 for side in ('base', 'candidate')]
        for path in (origin_plan_path, origin_profile_path):
            try:
                PROFILE['outside_sources'](path, roots, 'calibration origin input')
            except PROFILE['PreflightError'] as error:
                raise FreezeError(str(error)) from error
        origin = dict(plan_path=origin_plan_path, plan=origin_plan,
                      plan_sha256=origin_plan_hash, profile_path=origin_profile_path,
                      profile=origin_profile, profile_sha256=origin_profile_hash)
        validate_aa_origin(normalized[0], origin)
        input_paths[str(origin_plan_path)] = origin_plan_hash
        input_paths[str(origin_profile_path)] = origin_profile_hash
        normalized[0]['calibration_origin'] = origin
    else:
        raise FreezeError('unsupported freeze mode')
    calibration_owner = normalized[0]['calibration_origin'] if mode == 'aa_control' else normalized[0]
    calibration = validate_calibration(
        calibration_owner['plan_path'], calibration_owner['plan'], calibration_owner['plan_sha256'],
        calibration_owner['profile_path'], calibration_owner['profile'],
        calibration_owner['profile_sha256'], overlay, calibration_dirs[0])
    try:
        PROFILE['outside_sources'](
            Path(calibration['evidence_path']),
            [Path(calibration_owner['plan']['sources'][side]['source_root']).resolve()
             for side in ('base', 'candidate')], 'command-freeze input')
    except PROFILE['PreflightError'] as error:
        raise FreezeError(str(error)) from error
    input_paths.update({str(Path(calibration['evidence_path']) / name): digest
                        for name, digest in calibration['evidence_files_sha256'].items()})
    normalized[0]['calibration'] = calibration
    if mode == 'aa_control':
        normalized[0]['calibration_origin']['calibration'] = calibration
    if mode == 'comparison':
        normalized[1]['calibration'] = calibration
    return overlay, normalized, input_paths


def command_rows(mode, normalized, output_root):
    rows = []
    for campaign_index, item in enumerate(normalized):
        plan = item['plan']
        profile = item['profile']
        counts = item['calibration']['accepted_iteration_counts']
        per_case = item['calibration']['attempt_by_case']
        for launch in plan['schedule']['launches']:
            side = launch['side']
            source = plan['sources'][side]
            build = profile[side]
            if raw_hash(build['binary_path']) != build['binary_sha256']:
                raise FreezeError(f'{side} binary changed while constructing command rows')
            case = launch['case']
            host = per_case[case]['started']['host_before']
            command_index = len(rows)
            home = output_root / f'run-{command_index:03d}' / 'home'
            tmp = output_root / f'run-{command_index:03d}' / 'tmp'
            env = dict(PATH='/usr/bin:/bin', HOME=str(home), TMPDIR=str(tmp),
                       LANG='C', LC_ALL='C', TZ='UTC')
            iterations = counts[case]
            argv = [build['binary_path'], '--case', case, '--iterations', str(iterations),
                    '--warmups', '2', '--samples', '1']
            rows.append(dict(
                command_index=command_index, campaign_index=campaign_index,
                plan_sha256=item['plan_sha256'], profile_sha256=item['profile_sha256'],
                calibration_sha256=item['calibration']['calibration_sha256'],
                pair_id=launch['pair_id'], pair_index=launch['pair_index'],
                pair_order=launch['pair_order'], case=case, side=side,
                executable=build['binary_path'], executable_sha256=build['binary_sha256'],
                argv=argv, cwd=build['build_root'], environment=env,
                cpu=dict(selected_cpu=host['selected_cpu'], affinity=host['affinity_cpus'],
                         online=host['online_cpus']),
                host=dict(kernel=host['kernel'], model=host['model'],
                          microcode=host['microcode'], governor=host['governor'],
                          frequency_khz=host['frequency_khz']),
                product_identity=dict(checkout_commit=source['checkout_commit'],
                                      checkout_tree=source['checkout_tree'],
                                      product_tree=source['product_tree'],
                                      execution_tree=source['execution_tree']),
                iterations=iterations, warmups=2, measured_samples=1))
    expected_count = 108 if mode == 'aa_control' else 216
    if len(rows) != expected_count:
        raise FreezeError(f'internal command count mismatch: expected {expected_count}')
    for campaign_index in range(len(normalized)):
        campaign = [row for row in rows if row['campaign_index'] == campaign_index]
        if len(campaign) != 108:
            raise FreezeError('each plan must produce exactly 108 command rows')
        balance = {(case, order): 0 for case in CASE_NAMES for order in ('AB', 'BA')}
        for offset in range(0, len(campaign), 2):
            first, second = campaign[offset:offset + 2]
            order = first['pair_order']
            sides = ('base', 'candidate') if order == 'AB' else ('candidate', 'base')
            if first['pair_id'] != second['pair_id'] or first['case'] != second['case'] \
                    or second['pair_order'] != order or (first['side'], second['side']) != sides \
                    or first['iterations'] != second['iterations']:
                raise FreezeError('paired command rows are not adjacent and balanced')
            balance[(first['case'], order)] += 1
        if set(balance.values()) != {9}:
            raise FreezeError('each case must contain nine adjacent AB and nine adjacent BA pairs')
    return rows


def output_path(output_arg, normalized, overlay_path=None):
    output = under_home(output_arg, 'command-freeze output directory')
    if not output.parent.is_dir():
        raise FreezeError('command-freeze output parent must already exist')
    inputs = []
    if overlay_path is not None:
        inputs.append(Path(overlay_path))
    for item in normalized:
        inputs.extend((item['plan_path'], item['profile_path'],
                       Path(item['calibration']['evidence_path'])))
        if 'calibration_origin' in item:
            origin = item['calibration_origin']
            inputs.extend((origin['plan_path'], origin['profile_path']))
            inputs.extend(Path(origin['plan']['sources'][side]['source_root'])
                          for side in ('base', 'candidate'))
            inputs.extend(Path(origin['profile'][side]['build_root'])
                          for side in ('base', 'candidate'))
        inputs.extend(Path(item['plan']['sources'][side]['source_root'])
                      for side in ('base', 'candidate'))
        inputs.extend(Path(item['profile'][side]['build_root']) for side in ('base', 'candidate'))
    for path in inputs:
        resolved = path.resolve()
        if output == resolved or output in resolved.parents or resolved in output.parents:
            raise FreezeError(f'output directory overlaps input evidence/source/build: {resolved}')
    return output


def publish(output, artifact):
    output.mkdir(mode=0o700, exist_ok=False)
    target = output / 'command-freeze.json'
    temporary = output / 'command-freeze.json.tmp'
    payload = (json.dumps(artifact, sort_keys=True, indent=2, ensure_ascii=False,
                          allow_nan=False) + '\n').encode('utf-8')
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
    return target


def freeze(args):
    mode = args.mode
    if mode == 'comparison':
        plans = (args.plan_a, args.plan_b)
        profiles = (args.profile_a, args.profile_b)
        calibrations = (args.calibration_a,)
    else:
        plans = (args.plan,)
        profiles = (args.profile,)
        calibrations = (args.calibration,)
    calibration_origin = ((args.calibration_origin_plan, args.calibration_origin_profile)
                          if mode == 'aa_control' else None)
    overlay, normalized, input_hashes = validate_inputs(
        mode, args.overlay_patch, plans, profiles, calibrations,
        calibration_origin=calibration_origin)
    output = output_path(args.output_dir, normalized, overlay)
    rows = command_rows(mode, normalized, output)
    artifact = dict(schema=SCHEMA, schema_version=1, status='commands_frozen', mode=mode,
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
        origin = normalized[0]['calibration_origin']
        artifact['calibration_origin'] = dict(
            plan_sha256=origin['plan_sha256'], profile_sha256=origin['profile_sha256'],
            calibration_sha256=origin['calibration']['calibration_sha256'],
            source_tree=origin['plan']['sources']['base']['product_tree'],
            execution_tree=origin['plan']['sources']['base']['execution_tree'],
            binary_sha256=origin['profile']['base']['binary_sha256'])
    if contains_verdict_key(artifact):
        raise FreezeError('command artifact must not include performance verdict fields')
    # Revalidate every input and raw evidence file immediately before publish.
    _, _, final_hashes = validate_inputs(mode, args.overlay_patch, plans, profiles, calibrations,
                                         calibration_origin=calibration_origin)
    if final_hashes != input_hashes:
        raise FreezeError('plan/profile/calibration inputs changed before command publication')
    artifact['input_sha256'] = final_hashes
    path = publish(output, artifact)
    return path, artifact


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument('--mode', choices=('comparison', 'aa_control'), required=True)
    result.add_argument('--overlay-patch', required=True)
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
    if args.mode == 'comparison':
        required = (args.plan_a, args.profile_a, args.calibration_a,
                    args.plan_b, args.profile_b)
        if any(value is None for value in required) or any(
                value is not None for value in (args.plan, args.profile, args.calibration,
                                                 args.calibration_origin_plan,
                                                 args.calibration_origin_profile)):
            parser().error('comparison requires plan/profile A and B plus calibration A only')
    elif any(value is None for value in (args.plan, args.profile, args.calibration,
                                         args.calibration_origin_plan,
                                         args.calibration_origin_profile)) \
            or any(value is not None for value in (
                args.plan_a, args.profile_a, args.calibration_a,
                args.plan_b, args.profile_b)):
        parser().error('aa_control requires target --plan/--profile, origin plan/profile, and calibration')
    try:
        path, artifact = freeze(args)
    except (OSError, FreezeError, PROFILE['PreflightError'], PLAN['PlanError'],
            CAL['CalibrationError']) as error:
        print(f'freeze-batch-append-execution: {error}', file=sys.stderr)
        return 2
    print(f"Froze {artifact['planned_command_count']} commands in {path}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
