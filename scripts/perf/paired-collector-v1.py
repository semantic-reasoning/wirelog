#!/usr/bin/env python3
"""Collect one immutable paired campaign; no performance verdict is defined."""
import argparse
from contextlib import ExitStack, contextmanager
import json
import math
import os
from pathlib import Path
import re
import runpy
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


def execute(binary, workload, root, cpu, timeout):
    if threading.active_count() != 1:
        raise ValueError('benchmark launch requires a single-threaded Linux collector')
    env = os.environ.copy()
    command = ['taskset', '-c', str(cpu), str(binary)]
    if workload == 'crdt':
        env.update(WIRELOG_CRDT_PROBE='1', WIRELOG_CRDT_SMALL='0', WIRELOG_CRDT_DATA_DIR=str(root / 'crdt'))
    else:
        command += ['--workload', workload, '--data-cspa', str(root / 'cspa'), '--workers', '1', '--repeat', '1']
    raw = dict(command=command, host_before={}, host_after={}, timed_out=False,
               stdout='', stderr='', exit_code=None)
    process = None
    cause = None
    primary_cause = None
    with ExitStack() as files:
        stdout_file = files.enter_context(tempfile.TemporaryFile(mode='w+b'))
        stderr_file = files.enter_context(tempfile.TemporaryFile(mode='w+b'))
        try:
            raw['host_before'] = LEGACY['read_host'](cpu)
            previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, TERMINATION_SIGNALS)
            try:
                # Single-threaded child: only restore the inherited mask before exec.
                process = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file,
                                           env=env, start_new_session=True,
                                           preexec_fn=lambda: signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask))
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                raw['timed_out'] = True
            # Normal leader exit can leave descendants alive in the saved group.
            cleanup_process_group(process, raw)
        except OSError as error:
            raw.update(stderr=str(error), exit_code=127)
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
                raw['exit_code'] = process.poll()
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
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def collect(args):
    with termination_handlers():
        return collect_campaign(args)


def collect_campaign(args):
    if args.out_dir.exists():
        raise ValueError('evidence directory already exists')
    if args.cpu not in os.sched_getaffinity(0) or not 1 <= args.timeout <= 3600:
        raise ValueError('invalid CPU or timeout (1..3600 seconds)')
    if args.mode == 'aa_control' and args.base_sha != args.candidate_sha or args.mode == 'comparison' and args.base_sha == args.candidate_sha:
        raise ValueError('source SHAs do not match campaign mode')
    records, artifacts, binaries, roots, logs = {}, {}, {}, {}, {}
    for side in SIDES:
        source, build = getattr(args, side + '_source').resolve(), getattr(args, side + '_build').resolve()
        sha = getattr(args, side + '_sha')
        if not re.fullmatch('[0-9a-f]{40}', sha):
            raise ValueError('invalid source SHA')
        records[side] = VERIFY['side_record'](source, build, sha)
        options = json.loads((build / 'meson-info/intro-buildoptions.json').read_text())
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
        raise ValueError('distinct build log paths and hashes required')
    for key in ('profile', 'compiler', 'fixture_sha256'):
        if records['base'][key] != records['candidate'][key]:
            raise ValueError(f'mismatched {key}')
    for name in CONTRACT_SOURCES:
        if records['base']['timer_contract_sha256'][name] != records['candidate']['timer_contract_sha256'][name]:
            raise ValueError(f'timer/benchmark contract differs: {name}')
    profiles = {s: {k: profile_value(v) for k, v in records[s]['profile'].items()} for s in SIDES}
    host_id = Path('/etc/machine-id').read_text().strip()
    manifest = dict(sources={s: records[s]['source_sha'] for s in SIDES}, profiles=profiles,
                    build_provenance={s: dict(build_instance_id=str(uuid.uuid4()), source_sha=records[s]['source_sha'], profile=profiles[s], build_log_sha256=artifacts[logs[s]]) for s in SIDES},
                    host_id=host_id, cpu=args.cpu, workloads={})
    for workload in EXPECTED:
        prefix = 'bench/data/crdt/' if workload == 'crdt' else 'bench/data/cspa/'
        manifest['workloads'][workload] = dict(binary_sha256={s: artifacts[binaries[s][workload]] for s in SIDES}, fixture_sha256={s: {k: v for k, v in records[s]['fixture_sha256'].items() if k.startswith(prefix)} for s in SIDES}, timer=TIMERS[workload], expected=EXPECTED[workload])
    campaign = dict(schema_version=1, campaign_id=str(uuid.uuid4()), campaign_mode=args.mode, manifest=manifest,
                    blocks=[dict(block_id=o, order=o) for o in ('AB', 'BA')], attempts=[])
    planned = schedule()
    args.out_dir.mkdir(parents=True)
    atomic_json(args.out_dir / 'preflight.json', dict(records=records, artifacts={str(k): v for k, v in artifacts.items()}, schedule=planned, campaign_id=campaign['campaign_id'], manifest=manifest, theoretical_launch_timeout_ceiling_seconds=len(planned) * args.timeout))
    for side in SIDES:
        (args.out_dir / f'{side}-build.log').write_bytes(logs[side].read_bytes())
    with (args.out_dir / 'raw-attempts.jsonl').open('x', encoding='utf-8') as stream:
        for item in planned:
            try:
                raw = dict(item, **execute(binaries[item['side']][item['workload']], item['workload'], roots[item['side']], args.cpu, args.timeout))
            except InterruptedLaunch as error:
                LEGACY['append_event'](stream, dict(item, **error.raw))
                raise error.cause
            try:
                elapsed, correctness = parse_result(raw['stdout'], item['workload'])
                campaign['attempts'].append(dict(item, campaign_id=campaign['campaign_id'], manifest_sha256=EVALUATOR['digest'](manifest), elapsed_ms=elapsed, correctness=correctness, exit_code=raw['exit_code'], timed_out=raw['timed_out'], host_before=telemetry(raw['host_before']), host_after=telemetry(raw['host_after'])))
            except (ValueError, TypeError, KeyError) as error:
                raw['parse_error'] = str(error)
            LEGACY['append_event'](stream, raw)
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
    result = subprocess.run([sys.executable, str(HERE / 'evaluate-paired-campaign.py'), str(path)], capture_output=True, text=True, check=False)
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
