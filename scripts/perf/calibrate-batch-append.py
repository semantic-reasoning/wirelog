#!/usr/bin/env python3
"""Calibrate baseline batch-append iteration counts; never launch candidate."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import runpy
import shutil
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
PROFILE = runpy.run_path(str(HERE / 'prepare-batch-append-execution.py'))
OUTPUT = runpy.run_path(str(HERE / 'batch_append_output_v2.py'))
ARTIFACT_SCHEMA = 'wirelog.batch-append-calibration.v1'
PROFILE_SCHEMA = 'wirelog.batch-append-profile.v2'
MIN_APPEND_NS = 250_000_000
TARGET_APPEND_NS = 300_000_000
MAX_ITERATIONS = 100_000_000
MAX_ATTEMPTS_PER_CASE = 6
CASE_NAMES = ('1x1', '1x256', '32x256')
_ACTIVE_PROCESS = None


class CalibrationError(ValueError):
    pass


class CalibrationInterrupted(CalibrationError):
    pass


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def load_json(path):
    try:
        return json.loads(path.read_text(encoding='utf-8'),
                          object_pairs_hook=PROFILE['unique_object'],
                          parse_constant=PROFILE['reject_constant'])
    except (OSError, UnicodeError, json.JSONDecodeError,
            PROFILE['PreflightError']) as error:
        raise CalibrationError(f'cannot read strict JSON {path}: {error}') from error


def fsync_directory(path):
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_exclusive(path, payload, mode=0o600):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    fsync_directory(path.parent)


def write_json_exclusive(path, value):
    payload = (json.dumps(value, sort_keys=True, indent=2,
                          ensure_ascii=False, allow_nan=False) + '\n').encode('utf-8')
    write_exclusive(path, payload)
    return sha256(payload)


def write_json_atomic(path, value):
    temporary = path.with_name(path.name + '.tmp')
    payload = (json.dumps(value, sort_keys=True, indent=2,
                          ensure_ascii=False, allow_nan=False) + '\n').encode('utf-8')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise
    return sha256(payload)


def hash_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def parse_cpu_list(value):
    cpus = set()
    for item in value.strip().split(','):
        if not item:
            continue
        first, separator, last = item.partition('-')
        if not first.isdecimal() or (separator and not last.isdecimal()):
            raise CalibrationError(f'invalid Linux CPU list {value!r}')
        lower = int(first)
        upper = int(last) if separator else lower
        if upper < lower or upper - lower > 65536:
            raise CalibrationError(f'invalid Linux CPU range {item!r}')
        cpus.update(range(lower, upper + 1))
    if not cpus:
        raise CalibrationError('Linux CPU list is empty')
    return sorted(cpus)


def read_key_values(path):
    values = {}
    for line in Path(path).read_text(encoding='ascii').splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[1].isdecimal():
            values[fields[0]] = int(fields[1])
    return values


def psi_total(path):
    for line in Path(path).read_text(encoding='ascii').splitlines():
        if line.startswith('some '):
            match = re.search(r'(?:^|\s)total=(\d+)(?:\s|$)', line)
            if match:
                return int(match.group(1))
    raise CalibrationError('CPU PSI some.total telemetry is unavailable')


def cgroup_cpu_stat(proc_cgroup, cgroup_root):
    relative = None
    for line in Path(proc_cgroup).read_text(encoding='ascii').splitlines():
        hierarchy, controllers, path = line.split(':', 2)
        if hierarchy == '0' and not controllers:
            relative = path.lstrip('/')
            break
    if relative is None:
        raise CalibrationError('unified cgroup v2 membership is unavailable')
    stats_path = Path(cgroup_root) / relative / 'cpu.stat'
    values = read_key_values(stats_path)
    if 'nr_throttled' not in values or 'throttled_usec' not in values:
        raise CalibrationError('cgroup v2 CPU throttling counters are unavailable')
    return dict(nr_throttled=values['nr_throttled'],
                throttled_usec=values['throttled_usec'])


def proc_cpu_ticks(proc_stat, cpu_ids):
    result = {}
    wanted = {f'cpu{cpu}' for cpu in cpu_ids}
    for line in Path(proc_stat).read_text(encoding='ascii').splitlines():
        fields = line.split()
        if fields and fields[0] in wanted and len(fields) >= 9:
            counters = [int(value) for value in fields[1:9]]
            result[int(fields[0][3:])] = dict(
                total=sum(counters), busy=sum(counters[:3]) + sum(counters[5:8]))
    return result


def cpuinfo_for(cpuinfo_path, cpu_id):
    current = {}
    found = None
    for line in Path(cpuinfo_path).read_text(encoding='utf-8').splitlines() + ['']:
        if not line.strip():
            if current.get('processor') == str(cpu_id):
                found = current
                break
            current = {}
        elif ':' in line:
            key, value = line.split(':', 1)
            current[key.strip()] = value.strip()
    if found is None:
        raise CalibrationError(f'CPU model/microcode telemetry unavailable for CPU {cpu_id}')
    model = found.get('model name') or found.get('Hardware')
    microcode = found.get('microcode')
    if not model or not microcode:
        raise CalibrationError('CPU model or microcode telemetry is unavailable')
    return dict(model=model, microcode=microcode)


class HostProbe:
    def __init__(self, proc_root='/proc', sys_root='/sys', affinity=None):
        self.proc = Path(proc_root)
        self.sys = Path(sys_root)
        self.affinity = affinity

    def snapshot(self):
        online = parse_cpu_list((self.sys / 'devices/system/cpu/online').read_text())
        try:
            affinity = sorted(self.affinity if self.affinity is not None
                              else os.sched_getaffinity(0))
        except (AttributeError, OSError) as error:
            raise CalibrationError(f'CPU affinity telemetry is unavailable: {error}') from error
        allowed = sorted(set(online) & set(affinity))
        if not allowed:
            raise CalibrationError('no online CPU is in the process affinity mask')
        cpu = allowed[0]
        cpu_root = self.sys / f'devices/system/cpu/cpu{cpu}'
        topology = cpu_root / 'topology/thread_siblings_list'
        siblings = sorted(set(parse_cpu_list(topology.read_text())) - {cpu}) \
            if topology.is_file() else []
        cpufreq = cpu_root / 'cpufreq'
        governor_path = cpufreq / 'scaling_governor'
        frequency_path = cpufreq / 'scaling_cur_freq'
        if not frequency_path.is_file():
            frequency_path = cpufreq / 'cpuinfo_cur_freq'
        try:
            governor = governor_path.read_text(encoding='ascii').strip()
            frequency_khz = int(frequency_path.read_text(encoding='ascii').strip())
        except (OSError, ValueError) as error:
            raise CalibrationError(f'CPU governor/frequency telemetry unavailable: {error}') from error
        if not governor or frequency_khz <= 0:
            raise CalibrationError('CPU governor/frequency telemetry is invalid')
        model = cpuinfo_for(self.proc / 'cpuinfo', cpu)
        return dict(
            timestamp_utc=time.time_ns(), kernel=platform.release(),
            online_cpus=online, affinity_cpus=affinity, selected_cpu=cpu,
            governor=governor, frequency_khz=frequency_khz, **model,
            cpu_psi_some_total_usec=psi_total(self.proc / 'pressure/cpu'),
            cgroup_v2_cpu=cgroup_cpu_stat(self.proc / 'self/cgroup', self.sys / 'fs/cgroup'),
            smt_siblings=siblings,
            sibling_cpu_ticks=proc_cpu_ticks(self.proc / 'stat', siblings))


def host_eligibility(before, after, wall_ns):
    diagnostics = []
    if before['selected_cpu'] != after['selected_cpu'] \
            or before['selected_cpu'] not in after['online_cpus'] \
            or before['selected_cpu'] not in after['affinity_cpus']:
        diagnostics.append('selected CPU online/affinity changed during attempt')
    psi_delta = after['cpu_psi_some_total_usec'] - before['cpu_psi_some_total_usec']
    if psi_delta < 0 or wall_ns <= 0:
        diagnostics.append('CPU PSI or monotonic wall clock moved backwards')
        psi_ratio = None
    else:
        psi_ratio = psi_delta * 1000.0 / wall_ns
        if psi_ratio > 0.01:
            diagnostics.append(f'CPU PSI some.total/wall exceeded 1% ({psi_ratio:.6f})')
    cgroup_delta = {}
    for key in ('nr_throttled', 'throttled_usec'):
        delta = after['cgroup_v2_cpu'][key] - before['cgroup_v2_cpu'][key]
        cgroup_delta[key] = delta
        if delta != 0:
            diagnostics.append(f'cgroup v2 {key} changed by {delta}')
    siblings = before['smt_siblings']
    measured = {}
    unavailable = []
    for cpu in siblings:
        left = before['sibling_cpu_ticks'].get(cpu)
        right = after['sibling_cpu_ticks'].get(cpu)
        if left is None or right is None:
            unavailable.append(cpu)
            continue
        total_delta = right['total'] - left['total']
        busy_delta = right['busy'] - left['busy']
        if total_delta <= 0 or busy_delta < 0:
            unavailable.append(cpu)
            continue
        busy_percent = busy_delta * 100.0 / total_delta
        measured[str(cpu)] = busy_percent
        if busy_percent > 1.0:
            diagnostics.append(f'SMT sibling CPU {cpu} busy exceeded 1% ({busy_percent:.3f}%)')
    sibling_status = 'measured' if measured else 'none'
    sibling = dict(status=sibling_status, cpus=siblings,
                   busy_percent_by_cpu=measured,
                   absent_reason=('no measurable SMT sibling' if not measured else None),
                   unmeasurable_cpus=unavailable)
    return dict(eligible=not diagnostics, diagnostics=diagnostics,
                cpu_psi_some_total_delta_usec=psi_delta,
                cpu_psi_some_total_over_wall=psi_ratio,
                cgroup_v2_delta=cgroup_delta, smt_sibling=sibling)


def prepare_evidence_directory(path, plan, profile):
    output = PROFILE['under_home'](path, 'calibration evidence directory')
    sources = [Path(plan['sources'][side]['source_root']).resolve()
               for side in ('base', 'candidate')]
    builds = [Path(profile[side]['build_root']).resolve() for side in ('base', 'candidate')]
    PROFILE['outside_sources'](output, sources, 'calibration evidence directory')
    PROFILE['outside_sources'](output, builds, 'calibration evidence directory', 'build directories')
    if not output.parent.is_dir():
        raise CalibrationError('calibration evidence parent must already exist')
    output.mkdir(mode=0o700, exist_ok=False)
    fsync_directory(output.parent)
    return output


def validate_bound_inputs(plan_path, profile_path, overlay_path,
                          expected_plan_hash, expected_profile_hash,
                          expected_binary_hash):
    try:
        profile_raw = Path(profile_path).read_bytes()
        if sha256(profile_raw) != expected_profile_hash:
            raise CalibrationError('profile artifact changed during calibration')
        profile = load_json(profile_path)
        if type(profile) is not dict or profile.get('schema') != PROFILE_SCHEMA \
                or profile.get('status') != 'not_executable' \
                or profile.get('not_executable') is not True:
            raise CalibrationError('expected a profile-v2 not-executable artifact')
        plan, plan_hash = PROFILE['validate_plan'](Path(plan_path), Path(overlay_path))
        if plan_hash != expected_plan_hash or profile.get('plan_sha256') != expected_plan_hash:
            raise CalibrationError('plan artifact changed or profile does not bind this plan')
        if plan['sources']['base']['product_tree'] != \
                '4c10e21ab4ba034dbc50abd92a59d22e8b975130':
            raise CalibrationError('calibration baseline must select fixed product pre tree')
        if profile.get('revision_manifest_sha256') != plan['revision_manifest']['sha256']:
            raise CalibrationError('profile does not bind current revision manifest')
        binary = Path(profile['base']['binary_path'])
        expected_path = Path(profile['base']['build_root']) / 'bench/bench_batch_append'
        if binary != expected_path or profile['base'].get('source_root') != \
                plan['sources']['base']['source_root']:
            raise CalibrationError('profile baseline source/build target does not match plan')
        if profile['base']['binary_sha256'] != expected_binary_hash \
                or hash_file(binary) != expected_binary_hash:
            raise CalibrationError('original baseline binary changed during calibration')
        return plan, profile, binary
    except (OSError, KeyError, TypeError, PROFILE['PreflightError']) as error:
        raise CalibrationError(f'bound plan/profile revalidation failed: {error}') from error


def copy_baseline_binary(original, private_copy, expected_hash):
    private_copy.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(private_copy, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o700)
    try:
        with Path(original).open('rb') as source, os.fdopen(fd, 'wb') as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)
            target.flush()
            os.fsync(target.fileno())
        mode = Path(original).stat().st_mode & 0o111
        os.chmod(private_copy, 0o700 if mode else 0o600)
        if hash_file(private_copy) != expected_hash:
            raise CalibrationError('private baseline binary copy failed hash verification')
        fsync_directory(private_copy.parent)
    except BaseException:
        try:
            private_copy.unlink()
        except FileNotFoundError:
            pass
        raise


def terminate_group(process, grace_seconds=3):
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.communicate(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.communicate()
    if process.poll() is None:
        process.wait()


def launch_process(argv, cwd, env, timeout_seconds, popen_factory=subprocess.Popen,
                   selected_cpu=None):
    global _ACTIVE_PROCESS
    started_ns = time.monotonic_ns()
    try:
        options = dict(cwd=str(cwd), env=env, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, start_new_session=True)
        if selected_cpu is not None:
            options['preexec_fn'] = lambda: os.sched_setaffinity(0, {selected_cpu})
        process = popen_factory(argv, **options)
    except OSError as error:
        finished_ns = time.monotonic_ns()
        return dict(stdout=b'', stderr=str(error).encode(), exit_code=None,
                    timed_out=False, wall_ns=finished_ns - started_ns,
                    monotonic_started_ns=started_ns, monotonic_finished_ns=finished_ns,
                    spawn_error=str(error))
    _ACTIVE_PROCESS = process
    try:
        try:
            stdout, stderr = process.communicate(timeout=timeout_seconds)
            timed_out = False
        except subprocess.TimeoutExpired:
            terminate_group(process)
            stdout, stderr = process.communicate()
            timed_out = True
        finished_ns = time.monotonic_ns()
        return dict(stdout=stdout, stderr=stderr, exit_code=process.returncode,
                    timed_out=timed_out, wall_ns=finished_ns - started_ns,
                    monotonic_started_ns=started_ns, monotonic_finished_ns=finished_ns,
                    spawn_error=None)
    except BaseException:
        terminate_group(process)
        raise
    finally:
        _ACTIVE_PROCESS = None


def next_iterations(current, append_total_ns):
    scaled = (current * TARGET_APPEND_NS + append_total_ns - 1) // append_total_ns
    return min(MAX_ITERATIONS, max(current + 1, scaled))


def calibration_inputs(plan_path, profile_path, overlay_path):
    plan_path = PROFILE['under_home'](plan_path, 'campaign plan')
    profile_path = PROFILE['under_home'](profile_path, 'profile artifact')
    overlay_path = PROFILE['under_home'](overlay_path, 'overlay patch')
    plan_raw = Path(plan_path).read_bytes()
    profile_raw = Path(profile_path).read_bytes()
    plan, plan_hash = PROFILE['validate_plan'](Path(plan_path), Path(overlay_path))
    profile = load_json(profile_path)
    if type(profile) is not dict or profile.get('schema') != PROFILE_SCHEMA \
            or profile.get('status') != 'not_executable' or profile.get('not_executable') is not True \
            or profile.get('source_build_verified') is not True \
            or profile.get('benchmark_launches_performed') != 0:
        raise CalibrationError('input profile must be schema v2 and not executable')
    if type(profile.get('base')) is not dict or type(profile.get('candidate')) is not dict:
        raise CalibrationError('profile artifact is missing base/candidate evidence')
    if sha256(plan_raw) != plan_hash or profile.get('plan_sha256') != plan_hash:
        raise CalibrationError('profile and plan hashes do not match')
    if plan['sources']['base']['product_tree'] != \
            '4c10e21ab4ba034dbc50abd92a59d22e8b975130':
        raise CalibrationError('baseline calibration requires the pre product tree')
    if plan['mode'] == 'aa_control' and plan['aa_product'] != 'pre':
        raise CalibrationError('A/A calibration baseline must select product pre')
    binary = Path(profile['base']['binary_path'])
    expected_binary_hash = profile['base']['binary_sha256']
    if hash_file(binary) != expected_binary_hash:
        raise CalibrationError('baseline binary does not match profile hash')
    profile_hash = sha256(profile_raw)
    if profile.get('revision_manifest_sha256') != plan['revision_manifest']['sha256']:
        raise CalibrationError('profile manifest hash does not match plan')
    if profile['base'].get('product_tree') not in (None, plan['sources']['base']['product_tree']):
        raise CalibrationError('profile baseline product identity disagrees with plan')
    if profile.get('profile_sha256') != profile['base'].get('profile_sha256') \
            or profile['base'].get('source_root') != plan['sources']['base']['source_root']:
        raise CalibrationError('profile baseline identity/profile hash is inconsistent')
    for side in ('base', 'candidate'):
        if profile[side].get('profile_sha256') != profile.get('profile_sha256'):
            raise CalibrationError('profile sides do not share the verified build profile')
        if profile[side].get('source_root') != plan['sources'][side]['source_root']:
            raise CalibrationError(f'profile {side} source root does not match plan')
        PROFILE['under_home'](profile[side]['build_root'], f'{side} build directory')
    if not os.access(binary, os.X_OK):
        raise CalibrationError('profile baseline binary is not executable')
    sources = [Path(plan['sources'][side]['source_root']).resolve()
               for side in ('base', 'candidate')]
    builds = [Path(profile[side]['build_root']).resolve() for side in ('base', 'candidate')]
    for value, label in ((plan_path, 'campaign plan'), (profile_path, 'profile artifact'),
                         (overlay_path, 'overlay patch')):
        PROFILE['outside_sources'](value, sources, label)
        PROFILE['outside_sources'](value, builds, label, 'build directories')
    return plan, profile, plan_hash, profile_hash, expected_binary_hash


def calibrate(plan_path, profile_path, overlay_path, evidence_dir,
              timeout_seconds=300, probe=None, popen_factory=subprocess.Popen):
    plan, profile, plan_hash, profile_hash, binary_hash = calibration_inputs(
        plan_path, profile_path, overlay_path)
    output = prepare_evidence_directory(evidence_dir, plan, profile)
    private_home = output / 'private-home'
    private_tmp = output / 'private-tmp'
    private_binary = output / 'private-bin/bench_batch_append'
    for directory in (private_home, private_tmp):
        directory.mkdir(mode=0o700)
    fsync_directory(output)
    original_binary = Path(profile['base']['binary_path'])
    copy_baseline_binary(original_binary, private_binary, binary_hash)
    if probe is None:
        probe = HostProbe()
    initial_host = probe.snapshot()
    attempts = []
    accepted_counts = {}
    environment = dict(PATH='/usr/bin:/bin', HOME=str(private_home),
                       TMPDIR=str(private_tmp), LANG='C', LC_ALL='C', TZ='UTC')
    build_cwd = Path(profile['base']['build_root'])

    def verify():
        current_plan, current_profile, current_binary = validate_bound_inputs(
            plan_path, profile_path, overlay_path, plan_hash, profile_hash, binary_hash)
        if hash_file(private_binary) != binary_hash:
            raise CalibrationError('private baseline binary changed during calibration')
        return current_plan, current_profile, current_binary

    for case in CASE_NAMES:
        case_info = next((item for item in plan['benchmark_case_contract']['cases']
                          if item.get('name') == case), None)
        if type(case_info) is not dict:
            raise CalibrationError(f'plan is missing case contract {case}')
        iterations = case_info.get('default_iterations_start')
        if type(iterations) is not int or iterations <= 0:
            raise CalibrationError(f'plan has invalid starting iterations for {case}')
        accepted = False
        for attempt_index in range(MAX_ATTEMPTS_PER_CASE):
            current_plan, current_profile, _ = verify()
            before = probe.snapshot()
            argv = [str(private_binary), '--case', case, '--iterations', str(iterations),
                    '--warmups', '2', '--samples', '1']
            prefix = f'{case}-attempt-{attempt_index + 1:02d}'
            started = dict(schema='wirelog.batch-append-calibration-attempt.v1',
                           case=case, attempt_index=attempt_index + 1,
                           iterations=iterations, argv=argv, cwd=str(build_cwd),
                           environment=environment, plan_sha256=plan_hash,
                           profile_sha256=profile_hash, original_binary_sha256=binary_hash,
                           private_binary_sha256=hash_file(private_binary),
                           monotonic_started_ns=time.monotonic_ns(), host_before=before)
            write_json_exclusive(output / f'{prefix}-started.json', started)
            result = launch_process(argv, build_cwd, environment, timeout_seconds,
                                    popen_factory=popen_factory,
                                    selected_cpu=before['selected_cpu'])
            stdout_path = output / f'{prefix}-stdout.bin'
            stderr_path = output / f'{prefix}-stderr.bin'
            write_exclusive(stdout_path, result['stdout'])
            write_exclusive(stderr_path, result['stderr'])
            try:
                after = probe.snapshot()
                telemetry_error = None
            except CalibrationError as error:
                after = None
                telemetry_error = str(error)
            if telemetry_error:
                result_record = dict(
                    started=started, stdout_path=stdout_path.name,
                    stdout_sha256=sha256(result['stdout']), stderr_path=stderr_path.name,
                    stderr_sha256=sha256(result['stderr']), exit_code=result['exit_code'],
                    timed_out=result['timed_out'], wall_ns=result['wall_ns'],
                    spawn_error=result['spawn_error'], host_after=None, eligible=False,
                    accepted=False, append_total_ns=None,
                    diagnostic=f'required post-attempt host telemetry unavailable: {telemetry_error}')
                write_json_exclusive(output / f'{prefix}-result.json', result_record)
                raise CalibrationError(result_record['diagnostic'])
            eligibility = host_eligibility(before, after, result['wall_ns'])
            record = dict(started=started,
                          process_monotonic_started_ns=result['monotonic_started_ns'],
                          process_monotonic_finished_ns=result['monotonic_finished_ns'],
                          stdout_path=stdout_path.name,
                          stdout_sha256=sha256(result['stdout']),
                          stderr_path=stderr_path.name,
                          stderr_sha256=sha256(result['stderr']),
                          exit_code=result['exit_code'], timed_out=result['timed_out'],
                          wall_ns=result['wall_ns'], spawn_error=result['spawn_error'],
                          host_after=after, telemetry=eligibility,
                          eligible=eligibility['eligible'], accepted=False,
                          append_total_ns=None, diagnostic=None)
            try:
                if result['timed_out']:
                    raise CalibrationError('benchmark process timed out and was reaped')
                if result['spawn_error'] is not None:
                    raise CalibrationError(f'benchmark process could not start: {result["spawn_error"]}')
                if result['exit_code'] != 0:
                    raise CalibrationError(f'benchmark exited with status {result["exit_code"]}')
                parsed = OUTPUT['parse_output'](result['stdout'], case, iterations,
                                                warmups=2, samples=1)
                record['append_total_ns'] = parsed['append_total_ns']
                record['parsed'] = parsed
                if not eligibility['eligible']:
                    raise CalibrationError('; '.join(eligibility['diagnostics']))
                verify()
                if parsed['append_total_ns'] >= MIN_APPEND_NS:
                    record['accepted'] = True
                    accepted_counts[case] = iterations
                    accepted = True
            except (CalibrationError, OUTPUT['OutputError']) as error:
                record['diagnostic'] = str(error)
            write_json_exclusive(output / f'{prefix}-result.json', record)
            attempts.append(record)
            verify()
            if record['diagnostic']:
                raise CalibrationError(f'{case} attempt {attempt_index + 1} failed: {record["diagnostic"]}')
            if accepted:
                break
            if attempt_index + 1 >= MAX_ATTEMPTS_PER_CASE:
                break
            iterations = next_iterations(iterations, record['append_total_ns'])
        if not accepted:
            raise CalibrationError(f'{case} did not reach {MIN_APPEND_NS} ns in '
                                   f'{MAX_ATTEMPTS_PER_CASE} eligible attempts')

    verify()
    final_host = probe.snapshot()
    artifact = dict(
        schema=ARTIFACT_SCHEMA, status='calibrated', plan_sha256=plan_hash,
        profile_sha256=profile_hash, revision_manifest_sha256=plan['revision_manifest']['sha256'],
        base_product_tree=plan['sources']['base']['product_tree'],
        baseline_binary_sha256=binary_hash,
        private_binary_sha256=hash_file(private_binary),
        host=dict(initial=initial_host, final=final_host),
        accepted_iteration_counts=accepted_counts,
        attempts=attempts, candidate_launches=0,
        benchmark_launches_performed=0)
    write_json_atomic(output / 'calibration.json', artifact)
    return output / 'calibration.json', artifact


def terminate_active_process(signum, _frame):
    process = _ACTIVE_PROCESS
    if process is not None:
        terminate_group(process)
    raise CalibrationInterrupted(f'interrupted by signal {signum}; child process group reaped')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', required=True)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--overlay-patch', required=True)
    parser.add_argument('--evidence-dir', required=True)
    parser.add_argument('--timeout-seconds', type=int, default=300)
    args = parser.parse_args(argv)
    if args.timeout_seconds <= 0:
        parser.error('--timeout-seconds must be positive')
    previous = {}
    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.signal(signum, terminate_active_process)
    try:
        path, artifact = calibrate(args.plan, args.profile, args.overlay_patch,
                                   args.evidence_dir, args.timeout_seconds)
    except (OSError, KeyError, TypeError, CalibrationError, PROFILE['PreflightError']) as error:
        print(f'calibrate-batch-append: {error}', file=sys.stderr)
        return 2
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    print(f"Calibration accepted for {len(artifact['accepted_iteration_counts'])} cases: {path}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
