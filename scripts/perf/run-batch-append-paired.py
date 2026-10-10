#!/usr/bin/env python3
"""Collect one paired batch-append campaign for two prebuilt binaries; no verdict.

The v1 campaign collector is pinned to the #2033 pre/post revision manifest.
This runner compares any two prebuilt bench_batch_append binaries (for example
main against an optimization branch) with the same host telemetry, eligibility
rules, process launcher, frozen hash-sort AB/BA schedule and strict v2 output
parser. It records raw evidence only; evaluate-batch-append-paired.py computes
ratios, confidence intervals and threshold outcomes offline.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import shutil
import signal
import subprocess
import sys

HERE = Path(__file__).resolve().parent
CAL = runpy.run_path(str(HERE / 'calibrate-batch-append.py'))
PLAN = runpy.run_path(str(HERE / 'prepare-batch-append-campaign.py'))
OUTPUT = runpy.run_path(str(HERE / 'batch_append_output_v2.py'))
SCHEMA = 'wirelog.batch-append-paired-run.v1'
CALIBRATION_SCHEMA = 'wirelog.batch-append-calibration.v1'
SIDES = ('base', 'candidate')
WARMUPS = 2
SAMPLES = 1


class RunError(ValueError):
    pass


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def append_json(path, record):
    with open(path, 'a', encoding='utf-8') as handle:
        handle.write(json.dumps(record, sort_keys=True) + '\n')
        handle.flush()
        os.fsync(handle.fileno())


def write_json(path, value):
    CAL['write_json_atomic'](path, value)


def evidence_path(value, home, sources):
    """A new directory under HOME, outside /tmp, /dev/shm and both sources."""
    try:
        path = PLAN['safe_path'](value, 'evidence directory')
    except PLAN['PlanError'] as error:
        raise RunError(str(error)) from error
    home = Path(home).resolve()
    if home not in path.parents:
        raise RunError('evidence directory must be under HOME')
    for source in sources:
        if path == source or source in path.parents:
            raise RunError(f'evidence directory must be outside source {source}')
    if path.exists():
        raise RunError('evidence directory already exists; campaigns are never resumed')
    return path


def git(source, *args):
    result = subprocess.run(['git', '-C', str(source), *args], capture_output=True,
                            text=True, check=False)
    if result.returncode != 0:
        raise RunError(f'git {" ".join(args)} failed in {source}: {result.stderr.strip()}')
    return result.stdout.strip()


def source_provenance(source):
    """Commit and tree of a clean checkout the binary was built from."""
    source = Path(source).resolve()
    if git(source, 'rev-parse', '--is-shallow-repository') != 'false':
        raise RunError(f'source {source} is a shallow clone')
    dirty = git(source, 'status', '--porcelain', '--untracked-files=all')
    if dirty:
        raise RunError(f'source {source} has uncommitted or untracked files')
    return dict(source_root=str(source), commit=git(source, 'rev-parse', 'HEAD'),
                tree=git(source, 'rev-parse', 'HEAD^{tree}'))


def load_calibration(path):
    calibration = CAL['load_json'](Path(path))
    if type(calibration) is not dict or calibration.get('schema') != CALIBRATION_SCHEMA \
            or calibration.get('status') != 'calibrated':
        raise RunError('calibration artifact is not a calibrated v1 artifact')
    counts = calibration.get('accepted_iteration_counts')
    if type(counts) is not dict or set(counts) != set(PLAN['CASES']) \
            or any(type(v) is not int or v <= 0 for v in counts.values()):
        raise RunError('calibration iteration counts are missing or invalid')
    return counts


def build_manifest(args, out, bins, counts, launches, env, probe):
    return dict(schema=SCHEMA, seed=args.seed,
                seed_algorithm='prepare-batch-append-campaign.schedule (v3 hash-sort)',
                campaign_index=args.campaign_index, cpu=args.cpu, binaries=bins,
                calibration=str(Path(args.calibration).resolve()),
                calibration_sha256=sha256_file(args.calibration),
                iteration_counts=counts, warmups=WARMUPS, samples=SAMPLES,
                timeout_seconds=args.timeout_seconds, environment=env,
                host_initial=probe.snapshot(), launches=len(launches),
                evidence_dir=str(out))


def run(args, probe=None, launcher=None, home=None):
    sources = {side: source_provenance(getattr(args, f'{side}_source')) for side in SIDES}
    out = evidence_path(args.out, home or os.environ.get('HOME', ''),
                        [Path(s['source_root']) for s in sources.values()])
    counts = load_calibration(args.calibration)
    if args.seed == '' or args.campaign_index < 0 or args.timeout_seconds <= 0:
        raise RunError('seed must be non-empty; campaign index and timeout must be valid')
    probe = probe or CAL['HostProbe'](affinity=[args.cpu])
    launcher = launcher or CAL['launch_process']
    out.mkdir(mode=0o700, parents=False)
    bins = {}
    for side in SIDES:
        original = Path(getattr(args, side)).resolve()
        target = out / f'{side}-bench_batch_append'
        shutil.copy2(original, target)
        bins[side] = dict(path=str(target), original=str(original),
                          sha256=sha256_file(target),
                          original_sha256=sha256_file(original),
                          label=getattr(args, f'{side}_label'), **sources[side])
        if bins[side]['sha256'] != bins[side]['original_sha256']:
            raise RunError(f'{side} binary changed while copying')
    launches = PLAN['schedule'](args.seed)
    home_dir, tmp_dir = out / 'private-home', out / 'private-tmp'
    home_dir.mkdir(mode=0o700)
    tmp_dir.mkdir(mode=0o700)
    env = dict(PATH='/usr/bin:/bin', HOME=str(home_dir), TMPDIR=str(tmp_dir), LC_ALL='C')
    write_json(out / 'manifest.json',
               build_manifest(args, out, bins, counts, launches, env, probe))
    rows = []
    for launch in launches:
        side = launch['side']
        iterations = counts[launch['case']]
        rows.append(dict(
            command_index=launch['launch_index'], campaign_index=args.campaign_index,
            case=launch['case'], pair_id=launch['pair_id'], pair_order=launch['pair_order'],
            pair_index=launch['pair_index'], side=side, iterations=iterations,
            executable_sha256=bins[side]['sha256'],
            argv=[bins[side]['path'], '--case', launch['case'], '--iterations',
                  str(iterations), '--warmups', str(WARMUPS), '--samples', str(SAMPLES)]))
    # The whole frozen schedule is durable before the first launch.
    write_json(out / 'schedule.json', dict(schema=SCHEMA, seed=args.seed, commands=rows))
    status = dict(status='running', benchmark_launches_performed=0,
                  planned_command_count=len(rows))
    write_json(out / 'status.json', status)
    journal = out / 'journal.jsonl'
    for row in rows:
        index = row['command_index']
        if sha256_file(row['argv'][0]) != row['executable_sha256']:
            status.update(status='aborted', abort_reason=f'binary drift before command {index}')
            write_json(out / 'status.json', status)
            raise RunError(status['abort_reason'])
        append_json(journal, dict(event='started', command_index=index, command_row=row))
        before = probe.snapshot()
        result = launcher(row['argv'], out, env, args.timeout_seconds, selected_cpu=args.cpu)
        after = probe.snapshot()
        eligibility = CAL['host_eligibility'](before, after, result['wall_ns'])
        (out / f'command-{index:03d}.stdout.bin').write_bytes(result['stdout'])
        (out / f'command-{index:03d}.stderr.bin').write_bytes(result['stderr'])
        parsed, output_error = None, None
        if result['exit_code'] == 0 and not result['timed_out']:
            try:
                parsed = OUTPUT['parse_output'](result['stdout'], row['case'], row['iterations'],
                                                warmups=WARMUPS, samples=SAMPLES)
            except OUTPUT['OutputError'] as error:
                output_error = str(error)
        if result.get('child_affinity_cpus') != [args.cpu]:
            eligibility['eligible'] = False
            eligibility['diagnostics'].append('child affinity mismatch')
        status['benchmark_launches_performed'] += 1
        append_json(journal, dict(
            event='result', command_index=index, exit_code=result['exit_code'],
            timed_out=result['timed_out'], wall_ns=result['wall_ns'],
            spawn_error=result.get('spawn_error'),
            interrupted_signal=result.get('interrupted_signal'),
            child_affinity_cpus=result.get('child_affinity_cpus'),
            stdout_sha256=hashlib.sha256(result['stdout']).hexdigest(),
            stderr_sha256=hashlib.sha256(result['stderr']).hexdigest(),
            parsed=parsed, output_error=output_error, host_before=before,
            host_after=after, host_eligibility=eligibility))
        if result.get('interrupted_signal') is not None or result.get('spawn_error'):
            reason = (f"signal {result['interrupted_signal']}"
                      if result.get('interrupted_signal') is not None
                      else f"spawn error: {result['spawn_error']}")
            status.update(status='aborted', abort_reason=f'command {index}: {reason}')
            write_json(out / 'status.json', status)
            raise RunError(status['abort_reason'])
        write_json(out / 'status.json', status)
    status['status'] = 'complete_capture'
    write_json(out / 'status.json', status)
    return out, status


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    for side in SIDES:
        result.add_argument(f'--{side}', required=True, help=f'{side} bench_batch_append binary')
        result.add_argument(f'--{side}-source', required=True,
                            help=f'clean checkout the {side} binary was built from')
        result.add_argument(f'--{side}-label', required=True)
    result.add_argument('--calibration', required=True,
                        help='calibrated batch-append-calibration v1 artifact')
    result.add_argument('--seed', required=True)
    result.add_argument('--campaign-index', type=int, required=True)
    result.add_argument('--cpu', type=int, required=True)
    result.add_argument('--timeout-seconds', type=int, default=600)
    result.add_argument('--out', required=True, help='new evidence directory under HOME')
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    previous = {signum: signal.signal(signum, CAL['terminate_active_process'])
                for signum in (signal.SIGINT, signal.SIGTERM)}
    try:
        out, status = run(args)
    except (OSError, RunError, CAL['CalibrationError']) as error:
        print(f'run-batch-append-paired: {error}', file=sys.stderr)
        return 2
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    print(f"{status['status']}: {status['benchmark_launches_performed']} launches in {out}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
