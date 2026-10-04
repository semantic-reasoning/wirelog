#!/usr/bin/env python3
"""Collect one immutable paired campaign; no performance verdict is defined."""
import argparse
from contextlib import ExitStack, contextmanager
import hashlib
import json
import math
import os
from os import open as open_fd  # File-descriptor open; no text encoding applies.
from pathlib import Path
import re
import runpy
import select
import signal
import subprocess
import sys
import threading
import tempfile
import time
import uuid

HERE = Path(__file__).resolve().parent
LEGACY = runpy.run_path(str(HERE / 'paired-benchmark.py'))
VERIFY = runpy.run_path(str(HERE / 'verify-paired-builds.py'))
EVALUATOR = runpy.run_path(str(HERE / 'evaluate-paired-campaign.py'))
AA_HOST = runpy.run_path(str(HERE / 'paired-aa-host.py'))
AA_LAUNCHER = HERE / 'paired-aa-affinity-launcher.py'
SIDES = ('base', 'candidate')
TIMERS = {
    'crdt': dict(id='crdt_perf_gate_single_run', unit='ms', scope='run_crdt_once_: full pipeline, one worker'),
    'cspa-fast': dict(id='bench_flowlog_repeat_1', unit='ms', scope='run_pipeline_count: full pipeline, one worker'),
}
EXPECTED = {'crdt': dict(result=104851, aggregate=2152328, iterations=14148),
            'cspa-fast': dict(tuples=20381, iterations=6)}
CONTRACT_SOURCES = VERIFY['SAME_SOURCES'] + ('bench/bench_util.h',)
TERMINATION_SIGNALS = {signal.SIGTERM, signal.SIGHUP}


class TerminationRequested(BaseException):
    def __init__(self, signum):
        self.signum = signum


@contextmanager
def termination_handlers():
    """Library calls in worker threads leave process signal ownership alone."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    previous = {number: signal.getsignal(number) for number in TERMINATION_SIGNALS}
    requested = False

    def request(number, frame):
        nonlocal requested
        if not requested:
            requested = True
            raise TerminationRequested(number)

    try:
        for number in TERMINATION_SIGNALS:
            signal.signal(number, request)
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def cleanup_process_group(process, raw):
    """Bound cleanup of the saved launch PGID, even after leader exit."""
    errors = []
    pgid = process.pid

    def send(number):
        try:
            os.killpg(pgid, number)
            return True
        except ProcessLookupError:
            return False
        except OSError as error:
            errors.append(f'killpg({number}): {error}')
            return False

    try:
        if send(signal.SIGTERM):
            deadline = time.monotonic() + 0.5
            while time.monotonic() < deadline:
                process.poll()
                if not send(0):
                    break
                time.sleep(min(0.01, max(0, deadline - time.monotonic())))
    finally:
        send(signal.SIGKILL)
    try:
        process.wait(timeout=1)
    except Exception as error:
        errors.append(f'reap: {type(error).__name__}: {error}')
    raw['exit_code'] = process.poll()
    if errors:
        raw['cleanup_error'] = '; '.join(errors)


class InterruptedLaunch(BaseException):
    """Carry the killed launch's raw record to the durable journal."""

    def __init__(self, raw, cause, primary_cause=None):
        self.raw, self.cause = raw, cause
        self.primary_cause = cause if primary_cause is None else primary_cause


def mark_interruption(raw, error):
    raw.update(interrupted=True, interruption=type(error).__name__)
    if isinstance(error, TerminationRequested):
        raw.update(signal_number=error.signum, signal_name=signal.Signals(error.signum).name)


def schedule():
    return [dict(block_id=order, workload=workload, sequence=n,
                 phase='warmup' if n < 2 else 'sample',
                 pair_index=None if n < 2 else (n - 2) // 2,
                 side=(SIDES if order == 'AB' else SIDES[::-1])[n % 2])
            for order in ('AB', 'BA') for workload in EXPECTED for n in range(20)]


def profile_value(value):
    if type(value) not in (bool, list, str, int):
        raise ValueError('unsupported resolved profile value')
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def telemetry(raw):
    def metric(name, parse):
        try:
            value = parse(raw[name])
            if isinstance(value, (int, float)) and (not math.isfinite(value) or value < 0):
                raise ValueError('negative or nonfinite')
            if isinstance(value, str) and (not value or value == 'unavailable'):
                raise ValueError('unavailable')
            return dict(value=value, unavailable_reason=None)
        except (KeyError, ValueError, IndexError, TypeError) as error:
            return dict(value=None, unavailable_reason=f'{name}: unavailable or malformed ({type(error).__name__})')
    def total(text):
        return int(re.search(r'^some .*\btotal=(\d+)', text, re.M)[1])
    def throttled(text):
        return int(re.search(r'^throttled_usec\s+(\d+)$', text, re.M)[1])
    return dict(load_one=metric('loadavg', lambda x: float(x.split()[0])),
                cpu_pressure_some_total_us=metric('cpu_pressure', total),
                cgroup_throttled_us=metric('cgroup_cpu_stat', throttled),
                frequency_khz=metric('frequency_khz', int),
                governor=metric('governor', str))


def parse_result(stdout, workload):
    if workload == 'crdt':
        record = json.loads(stdout, object_pairs_hook=EVALUATOR['unique_object'])
        identity = dict(schema_version=1, measurement='crdt_perf_gate_single_run', workload='crdt', fixture='full', workers=1, expected=104851)
        if type(record) is not dict or set(record) != set(identity) | {'elapsed_ms', 'result', 'aggregate', 'iterations', 'status'}:
            raise ValueError('wrong probe schema')
        if any(type(record[k]) is not type(v) or record[k] != v for k, v in identity.items()):
            raise ValueError('wrong probe identity')
        elapsed = record['elapsed_ms']
        observed = {k: record[k] for k in EXPECTED[workload]}
        status = record['status']
    else:
        lines = stdout.strip().splitlines()
        if len(lines) != 2 or lines[0] != LEGACY['HEADER']:
            raise ValueError('malformed TSV')
        fields = lines[1].split('\t')
        if len(fields) != 12 or fields[0] != 'cspa' or fields[3:5] != ['1', '1']:
            raise ValueError('wrong TSV identity')
        times = [float(x) for x in fields[5:8]]
        if times[0] != times[1] or times[1] != times[2]:
            raise ValueError('repeat=1 timings disagree')
        elapsed = times[1]
        observed = dict(tuples=int(fields[9]), iterations=int(fields[10]))
        status = fields[11]
    if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed <= 0:
        raise ValueError('invalid elapsed')
    if any(type(v) is not int or v < 0 for v in observed.values()) or status not in ('OK', 'FAIL'):
        raise ValueError('invalid correctness')
    return elapsed, dict(status=status, observed=observed)


def proc_affinity(pid):
    status = Path(f'/proc/{pid}/status').read_text(encoding='utf-8')
    match = re.search(r'^Cpus_allowed_list:\s*(\S+)$', status, re.M)
    if match is None:
        raise ValueError('child Cpus_allowed_list is unavailable')
    values = set()
    for part in match.group(1).split(','):
        if '-' in part:
            start, end = map(int, part.split('-', 1))
            values.update(range(start, end + 1))
        else:
            values.add(int(part))
    return sorted(values)


def _attempt_command(binary, workload, root, cpu, ready_fd=None, release_fd=None):
    args = []
    env = os.environ.copy()
    if workload == 'crdt':
        env.update(WIRELOG_CRDT_PROBE='1', WIRELOG_CRDT_SMALL='0', WIRELOG_CRDT_DATA_DIR=str(root / 'crdt'))
    else:
        args = ['--workload', workload, '--data-cspa', str(root / 'cspa'), '--workers', '1', '--repeat', '1']
    if ready_fd is None:
        command = ['taskset', '-c', str(cpu), str(binary)]
    else:
        command = [sys.executable, str(AA_LAUNCHER), '--cpu', str(cpu),
                   '--ready-fd', str(ready_fd), '--release-fd', str(release_fd),
                   str(binary)]
    command.extend(args)
    return command, env


def execute(binary, workload, root, cpu, timeout, *, qualify_aa=False,
            handshake_timeout=5):
    if threading.active_count() != 1:
        raise ValueError('benchmark launch requires a single-threaded Linux collector')
    ready_read = ready_write = release_read = release_write = None
    if qualify_aa:
        ready_read, ready_write = os.pipe()
        release_read, release_write = os.pipe()
    command, env = _attempt_command(binary, workload, root, cpu,
                                    ready_write if qualify_aa else None,
                                    release_read if qualify_aa else None)
    raw = dict(command=command, host_before={}, host_after={}, timed_out=False,
               stdout='', stderr='', exit_code=None)
    process = None
    benchmark_released = False
    cause = None
    primary_cause = None
    with ExitStack() as files:
        stdout_file = files.enter_context(tempfile.TemporaryFile(mode='w+b'))
        stderr_file = files.enter_context(tempfile.TemporaryFile(mode='w+b'))
        try:
            if not qualify_aa:
                raw['host_before'] = LEGACY['read_host'](cpu)
            previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, TERMINATION_SIGNALS)
            try:
                # Single-threaded child: only restore the inherited mask before exec.
                process = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file,
                                           env=env, start_new_session=True,
                                           pass_fds=((ready_write, release_read)
                                                     if qualify_aa else ()),
                                           preexec_fn=lambda: signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask))
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
            raw['process_group_id'] = process.pid
            if qualify_aa:
                os.close(ready_write)
                ready_write = None
                os.close(release_read)
                release_read = None
                readable, _, _ = select.select([ready_read], [], [], handshake_timeout)
                if not readable:
                    raw.update(prelaunch_rejected=True, launch_rejection='affinity_handshake_timeout')
                    raise TimeoutError('affinity launcher readiness timed out')
                message = os.read(ready_read, 4096)
                try:
                    ready = AA_HOST['strict_json'](message.decode('utf-8'))
                    child_affinity = ready['affinity']
                    observed = proc_affinity(process.pid)
                    if ready.get('pid') != process.pid or child_affinity != [cpu] or observed != [cpu]:
                        raise ValueError(f'child affinity is not the selected singleton CPU: {observed}')
                except (KeyError, UnicodeError, ValueError) as error:
                    raw.update(prelaunch_rejected=True, launch_rejection=f'affinity_verification_failed: {error}')
                    raise RuntimeError(raw['launch_rejection']) from error
                raw['launcher_ready'] = ready
                raw['aa_host_before'] = AA_HOST['read_snapshot'](cpu, child_affinity=observed)
                raw['host_before'] = LEGACY['read_host'](cpu)
                if release_write is None:
                    raise RuntimeError('affinity release pipe is unavailable')
                os.write(release_write, b'1')
                benchmark_released = True
                os.close(release_write)
                release_write = None
            else:
                raw['host_before'] = LEGACY['read_host'](cpu)
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                raw['timed_out'] = True
            # Normal leader exit can leave descendants alive in the saved group.
            cleanup_process_group(process, raw)
        except (RuntimeError, TimeoutError) as error:
            raw.update(stderr=f'{type(error).__name__}: {error}', exit_code=125)
            if process is not None:
                try:
                    cleanup_process_group(process, raw)
                except Exception as cleanup_error:
                    raw['cleanup_error'] = f'{type(cleanup_error).__name__}: {cleanup_error}'
        except OSError as error:
            raw.update(stderr=str(error), launch_error=f'{type(error).__name__}: {error}', exit_code=127)
            if qualify_aa and not benchmark_released:
                raw.update(prelaunch_rejected=True,
                           launch_rejection=f'affinity_handshake_io_error: {type(error).__name__}: {error}')
            if process is not None:
                try:
                    cleanup_process_group(process, raw)
                except Exception as cleanup_error:
                    raw['cleanup_error'] = f'{type(cleanup_error).__name__}: {cleanup_error}'
        except BaseException as error:
            cause = primary_cause = error
            mark_interruption(raw, error)
            if process is not None:
                try:
                    cleanup_process_group(process, raw)
                except Exception as cleanup_error:
                    raw['cleanup_error'] = f'{type(cleanup_error).__name__}: {cleanup_error}'
                except BaseException as interruption:
                    raw['primary_interruption'] = type(error).__name__
                    raw['secondary_interruption'] = type(interruption).__name__
                    if not isinstance(error, TerminationRequested):
                        cause = interruption
                        mark_interruption(raw, interruption)
                    # A first signal during the recovery path remains control
                    # flow. Retry idempotently before capturing and journaling.
                    try:
                        cleanup_process_group(process, raw)
                    except Exception as cleanup_error:
                        raw['cleanup_error'] = f'{type(cleanup_error).__name__}: {cleanup_error}'
                    except BaseException as further_interruption:
                        raw['further_interruption'] = type(further_interruption).__name__
                        if not isinstance(cause, TerminationRequested):
                            cause = further_interruption
                            mark_interruption(raw, further_interruption)
        finally:
            if qualify_aa:
                raw['aa_host_after'] = AA_HOST['read_snapshot'](
                    cpu, child_affinity=raw.get('launcher_ready', {}).get('affinity'))
                if raw.get('launcher_ready'):
                    raw['aa_host_after']['child_affinity']['observation'] = (
                        'carried_forward_from_verified_preexec_affinity')
                raw['host_after'] = LEGACY['read_host'](cpu)
            else:
                raw['host_after'] = LEGACY['read_host'](cpu)
            for key, output in (('stdout', stdout_file), ('stderr', stderr_file)):
                try:
                    output.flush()
                    os.fsync(output.fileno())
                    output.seek(0)
                    captured = output.read().decode('utf-8', errors='replace')
                    if captured or not raw[key]:
                        raw[key] = captured
                except OSError as error:
                    raw['capture_error'] = f'{key}: {error}'
            if process is not None:
                observed_exit_code = process.poll()
                if observed_exit_code is not None:
                    raw['exit_code'] = observed_exit_code
            for descriptor in (ready_read, ready_write, release_read, release_write):
                if descriptor is not None:
                    try:
                        os.close(descriptor)
                    except OSError:
                        pass
        if cause is not None:
            raise InterruptedLaunch(raw, cause, primary_cause) from cause
    return raw


def atomic_json(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('x', encoding='utf-8') as stream:
        json.dump(value, stream, sort_keys=True, indent=2, allow_nan=False)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    fd = open_fd(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def append_raw_attempt(stream, event, link_stream=None, ordinal=None):
    line = (json.dumps(event, sort_keys=True) + '\n').encode('utf-8')
    stream.buffer.write(line)
    stream.flush()
    os.fsync(stream.fileno())
    if link_stream is not None:
        link = dict(ordinal=ordinal, campaign_id=event['campaign_id'],
                    manifest_sha256=event['manifest_sha256'], block_id=event['block_id'],
                    workload=event['workload'], sequence=event['sequence'], side=event['side'],
                    raw_line_sha256=hashlib.sha256(line).hexdigest())
        link_stream.write(json.dumps(link, sort_keys=True) + '\n')
        link_stream.flush()
        os.fsync(link_stream.fileno())


def qualification_preflight(args, records, artifacts, binaries, sources, builds, logs,
                            campaign_id, manifest, planned):
    paths = {side: dict(source=str(sources[side]), build=str(builds[side]),
                        log=str(logs[side]),
                        executables={name: str(path.resolve())
                                     for name, path in binaries[side].items()})
             for side in SIDES}
    return dict(schema_version=1, mode=args.mode, qualify_aa=bool(args.qualify_aa),
                campaign_id=campaign_id, manifest=manifest,
                records=records, artifacts={str(path): digest for path, digest in artifacts.items()},
                paths=paths, schedule=planned,
                theoretical_launch_timeout_ceiling_seconds=len(planned) * args.timeout,
                launcher_sha256=LEGACY['sha256'](AA_LAUNCHER) if args.qualify_aa else None)


def reject_aa_preflight(args, document, status, reason, workloads):
    args.out_dir.mkdir(parents=True)
    atomic_json(args.out_dir / 'preflight.json', document)
    result = dict(schema_version=1, status=status, reason=reason,
                  benchmark_launches=0, workloads=workloads)
    atomic_json(args.out_dir / 'aa-qualification-preflight.json', result)
    atomic_json(args.out_dir / 'collection-status.json',
                dict(status='AA_PREFLIGHT_REJECTED', qualification_status=status,
                     reason=reason, benchmark_launches=0))
    return 4 if status == 'FAIL' else 5


def collect(args):
    with termination_handlers():
        return collect_campaign(args)


def collect_campaign(args):
    args.qualify_aa = bool(getattr(args, 'qualify_aa', False))
    if args.qualify_aa and args.mode != 'aa_control':
        raise ValueError('--qualify-aa is only valid with --mode aa_control')
    if args.out_dir.exists():
        raise ValueError('evidence directory already exists')
    if args.cpu not in os.sched_getaffinity(0) or not 1 <= args.timeout <= 3600:
        raise ValueError('invalid CPU or timeout (1..3600 seconds)')
    if args.mode == 'aa_control' and args.base_sha != args.candidate_sha or args.mode == 'comparison' and args.base_sha == args.candidate_sha:
        raise ValueError('source SHAs do not match campaign mode')
    records, artifacts, binaries, roots, logs, sources, builds = {}, {}, {}, {}, {}, {}, {}
    for side in SIDES:
        source, build = getattr(args, side + '_source').resolve(), getattr(args, side + '_build').resolve()
        sources[side], builds[side] = source, build
        sha = getattr(args, side + '_sha')
        if not re.fullmatch('[0-9a-f]{40}', sha):
            raise ValueError('invalid source SHA')
        records[side] = VERIFY['side_record'](source, build, sha)
        options = json.loads((build / 'meson-info/intro-buildoptions.json').read_text(encoding='utf-8'))
        records[side]['profile'] = {item['name']: item['value'] for item in options}
        records[side]['timer_contract_sha256'] = {
            name: LEGACY['sha256'](source / name) for name in CONTRACT_SOURCES}
        roots[side] = source / 'bench/data'
        binaries[side] = {'crdt': build / 'tests/test_crdt_perf_gate', 'cspa-fast': build / 'bench/bench_flowlog'}
        logs[side] = getattr(args, side + '_build_log').resolve()
        for path in [logs[side], *binaries[side].values(), *(source / p for p in CONTRACT_SOURCES + VERIFY['GATE_SOURCES'] + VERIFY['FIXTURES']), build / 'meson-info/intro-buildoptions.json', build / 'meson-info/intro-compilers.json']:
            artifacts[path] = LEGACY['sha256'](path)
        if any(not os.access(p, os.X_OK) for p in binaries[side].values()):
            raise ValueError('benchmark is not executable')
    if logs['base'] == logs['candidate'] or artifacts[logs['base']] == artifacts[logs['candidate']]:
        if not args.qualify_aa or logs['base'] != logs['candidate']:
            raise ValueError('distinct build log paths and hashes required')
    for key in ('profile', 'compiler', 'fixture_sha256'):
        if records['base'][key] != records['candidate'][key]:
            raise ValueError(f'mismatched {key}')
    for name in CONTRACT_SOURCES:
        if records['base']['timer_contract_sha256'][name] != records['candidate']['timer_contract_sha256'][name]:
            raise ValueError(f'timer/benchmark contract differs: {name}')
    profiles = {s: {k: profile_value(v) for k, v in records[s]['profile'].items()} for s in SIDES}
    host_id = Path('/etc/machine-id').read_text(encoding='utf-8').strip()
    planned = schedule()
    shared_paths = (sources['base'] == sources['candidate'] and builds['base'] == builds['candidate']
                    and logs['base'] == logs['candidate'])
    if args.qualify_aa:
        mismatch = []
        for workload in EXPECTED:
            base_hash = artifacts[binaries['base'][workload]]
            candidate_hash = artifacts[binaries['candidate'][workload]]
            if base_hash != candidate_hash:
                mismatch.append(dict(workload=workload, base_sha256=base_hash,
                                     candidate_sha256=candidate_hash))
        campaign_id = str(uuid.uuid4())
        preflight = qualification_preflight(args, records, artifacts, binaries, sources,
                                            builds, logs, campaign_id, {}, planned)
        if mismatch:
            return reject_aa_preflight(args, preflight, 'FAIL', 'binary_sha_mismatch', mismatch)
        if not shared_paths:
            return reject_aa_preflight(args, preflight, 'INCONCLUSIVE',
                                       'shared_build_path_identity_missing', [])
    instance_id = str(uuid.uuid4())
    build_ids = {side: (instance_id if args.qualify_aa and shared_paths else str(uuid.uuid4()))
                 for side in SIDES}
    manifest = dict(sources={s: records[s]['source_sha'] for s in SIDES}, profiles=profiles,
                    build_provenance={s: dict(build_instance_id=build_ids[s], source_sha=records[s]['source_sha'], profile=profiles[s], build_log_sha256=artifacts[logs[s]]) for s in SIDES},
                    host_id=host_id, cpu=args.cpu, workloads={})
    for workload in EXPECTED:
        prefix = 'bench/data/crdt/' if workload == 'crdt' else 'bench/data/cspa/'
        manifest['workloads'][workload] = dict(binary_sha256={s: artifacts[binaries[s][workload]] for s in SIDES}, fixture_sha256={s: {k: v for k, v in records[s]['fixture_sha256'].items() if k.startswith(prefix)} for s in SIDES}, timer=TIMERS[workload], expected=EXPECTED[workload])
    campaign_id = campaign_id if args.qualify_aa else str(uuid.uuid4())
    campaign = dict(schema_version=1, campaign_id=campaign_id, campaign_mode=args.mode, manifest=manifest,
                    blocks=[dict(block_id=o, order=o) for o in ('AB', 'BA')], attempts=[])
    preflight = qualification_preflight(args, records, artifacts, binaries, sources, builds,
                                        logs, campaign['campaign_id'], manifest, planned)
    args.out_dir.mkdir(parents=True)
    atomic_json(args.out_dir / 'preflight.json', preflight)
    for side in SIDES:
        (args.out_dir / f'{side}-build.log').write_bytes(logs[side].read_bytes())
    with (args.out_dir / 'raw-attempts.jsonl').open('x', encoding='utf-8') as stream:
      link_context = ((args.out_dir / 'aa-attempt-links.jsonl').open('x', encoding='utf-8')
                      if args.qualify_aa else None)
      try:
        for ordinal, item in enumerate(planned):
            try:
                execute_args = (dict(qualify_aa=True) if args.qualify_aa else {})
                raw = dict(item, **execute(binaries[item['side']][item['workload']], item['workload'], roots[item['side']], args.cpu, args.timeout, **execute_args))
            except InterruptedLaunch as error:
                raw = dict(item, **error.raw)
                if args.qualify_aa:
                    raw.update(ordinal=ordinal, campaign_id=campaign['campaign_id'],
                               manifest_sha256=EVALUATOR['digest'](manifest))
                append_raw_attempt(stream, raw, link_context, ordinal if args.qualify_aa else None)
                raise error.cause
            try:
                elapsed, correctness = parse_result(raw['stdout'], item['workload'])
                campaign['attempts'].append(dict(item, campaign_id=campaign['campaign_id'], manifest_sha256=EVALUATOR['digest'](manifest), elapsed_ms=elapsed, correctness=correctness, exit_code=raw['exit_code'], timed_out=raw['timed_out'], host_before=telemetry(raw['host_before']), host_after=telemetry(raw['host_after'])))
            except (ValueError, TypeError, KeyError) as error:
                raw['parse_error'] = str(error)
            if args.qualify_aa:
                raw.update(ordinal=ordinal, campaign_id=campaign['campaign_id'],
                           manifest_sha256=EVALUATOR['digest'](manifest))
            append_raw_attempt(stream, raw, link_context, ordinal if args.qualify_aa else None)
            if raw.get('prelaunch_rejected'):
                break
      finally:
        if link_context is not None:
            link_context.close()
    drift = [str(path) for path, digest in artifacts.items() if not path.is_file() or LEGACY['sha256'](path) != digest]
    for side in SIDES:
        source = getattr(args, side + '_source')
        if VERIFY['git_output'](source, 'rev-parse', 'HEAD') != records[side]['source_sha'] or VERIFY['git_output'](source, 'status', '--porcelain', '--untracked-files=no'):
            drift.append(str(source))
    if drift:
        atomic_json(args.out_dir / 'collection-status.json', dict(status='INVALID_EVIDENCE', reason='artifact/source drift', paths=drift))
        raise ValueError('artifact/source drift after collection')
    path = args.out_dir / 'campaign-v1.json'
    atomic_json(path, campaign)
    result = subprocess.run([sys.executable, str(HERE / 'evaluate-paired-campaign.py'), str(path)], capture_output=True, text=True, check=False, encoding='utf-8')
    (args.out_dir / 'evaluation-report.json').write_text(result.stdout, encoding='utf-8')
    atomic_json(args.out_dir / 'collection-status.json', dict(evaluator_exit=result.returncode, stderr=result.stderr, status=json.loads(result.stdout)['status']))
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('comparison', 'aa_control'), required=True)
    for side in SIDES:
        for name in ('source', 'build', 'build-log', 'sha'):
            parser.add_argument(f'--{side}-{name}', required=True, type=str if name == 'sha' else Path)
    parser.add_argument('--out-dir', type=Path, required=True)
    parser.add_argument('--cpu', type=int, required=True)
    parser.add_argument('--timeout', type=int, default=180)
    parser.add_argument('--qualify-aa', action='store_true',
                        help='collect the protected-main A/A qualification evidence')
    args = parser.parse_args()
    try:
        return collect(args)
    except TerminationRequested as error:
        signal.signal(error.signum, signal.SIG_DFL)
        os.kill(os.getpid(), error.signum)
        raise
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f'paired-collector-v1: {error}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
