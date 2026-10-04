#!/usr/bin/env python3
"""Qualify protected-main paired A/A evidence against its frozen noise policy."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import runpy
import statistics
import struct
import sys

HERE = Path(__file__).resolve().parent
POLICY_PATH = HERE / 'paired-aa-policy-v1.json'
POLICY_SHA256 = '337a0d224300a93c565428e47a500cf38782bf1de26bc6b4c407f22364a65a6d'
FROZEN_PLAN_COMMIT = '83a1e047accf615dac17a20637c8ade9c0da48b1'
FROZEN_PLAN_SHA256 = '2dca39eb45290ffbb8ea949f4e55d52e37a20c838ce7e049a1fd2aee4338ffbd'
POLICY = json.loads(POLICY_PATH.read_text(encoding='utf-8'))
if (POLICY['source_plan']['commit'] != FROZEN_PLAN_COMMIT or
        POLICY['source_plan']['sha256'] != FROZEN_PLAN_SHA256):
    raise RuntimeError('policy snapshot does not match the frozen source plan identity')
HOST = runpy.run_path(str(HERE / 'paired-aa-host.py'))
COLLECTOR = runpy.run_path(str(HERE / 'paired-collector-v1.py'))
SIDES = ('base', 'candidate')
WORKLOADS = tuple(POLICY['campaign']['workloads'])


class InvalidEvidence(ValueError):
    pass


class HostIneligible(ValueError):
    pass


def require(condition, reason):
    if not condition:
        raise InvalidEvidence(reason)


def digest_bytes(value):
    return hashlib.sha256(value).hexdigest()


def digest_json(value):
    return digest_bytes((json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    allow_nan=False) + '\n').encode('utf-8'))


def _dict(value, name):
    require(type(value) is dict, f'{name}_malformed')
    return value


def _metric(snapshot, name):
    value = snapshot.get(name)
    require(type(value) is dict and set(value) >= {'value', 'unavailable_reason'},
            f'malformed_host_metric:{name}')
    if value['value'] is None or value['unavailable_reason'] is not None:
        raise HostIneligible(f'missing_telemetry:{name}')
    return value['value']


def _cgroup_shape(snapshot):
    item = snapshot.get('cgroup')
    require(type(item) is dict, 'malformed_cgroup')
    if item.get('value') is None or item.get('unavailable_reason') is not None:
        raise HostIneligible('missing_telemetry:cgroup')
    value = _dict(item['value'], 'cgroup')
    require(value.get('version') in ('v1', 'v2') and
            type(value.get('membership_path')) is str and
            type(value.get('mountpoint')) is str and type(value.get('mount_root')) is str and
            type(value.get('levels')) is list and bool(value['levels']),
            'malformed_cgroup')
    levels = []
    for level in value['levels']:
        level = _dict(level, 'cgroup_level')
        require(type(level.get('path')) is str and type(level.get('cpu_limit')) is dict and
                type(level.get('nr_throttled')) is int and level['nr_throttled'] >= 0 and
                type(level.get('throttle_time')) is int and level['throttle_time'] >= 0 and
                level.get('throttle_time_unit') == ('us' if value['version'] == 'v2' else 'ns'),
                'malformed_cgroup')
        limit = level['cpu_limit']
        if value['version'] == 'v2':
            require(set(limit) == {'quota', 'period_us'} and
                    (limit['quota'] == 'max' or
                     type(limit['quota']) is str and limit['quota'].isdigit()) and
                    type(limit['period_us']) is int and limit['period_us'] > 0,
                    'malformed_cgroup')
        else:
            require(set(limit) == {'quota_us', 'period_us'} and
                    type(limit['quota_us']) is int and type(limit['period_us']) is int and
                    limit['period_us'] > 0, 'malformed_cgroup')
        levels.append(level)
    return dict(version=value['version'], membership_path=value['membership_path'],
                mount_root=value.get('mount_root'), mountpoint=value.get('mountpoint'),
                levels=levels)


def _host_observation(snapshot, cpu):
    require(type(cpu) is int and type(snapshot) is dict and
            type(snapshot.get('monotonic_ns')) is int and snapshot['monotonic_ns'] >= 0 and
            type(snapshot.get('selected_cpu')) is int and snapshot['selected_cpu'] == cpu,
            'malformed_host_snapshot')
    machine = _metric(snapshot, 'machine_id')
    governor = _metric(snapshot, 'governor')
    collector_affinity = _metric(snapshot, 'collector_affinity')
    child_affinity = _metric(snapshot, 'child_affinity')
    some = _metric(snapshot, 'cpu_psi_some_avg10_percent')
    full = _metric(snapshot, 'cpu_psi_full_avg10_percent')
    memory_full = _metric(snapshot, 'memory_psi_full_avg10_percent')
    swap = _metric(snapshot, 'swap_pages')
    for value, label in ((some, 'cpu_psi_some_avg10_percent'),
                         (full, 'cpu_psi_full_avg10_percent'),
                         (memory_full, 'memory_psi_full_avg10_percent')):
        require(type(value) in (int, float) and math.isfinite(value) and value >= 0,
                f'malformed_host_metric:{label}')
    require(type(machine) is str and bool(machine) and type(governor) is str and bool(governor),
            'malformed_host_identity')
    require(type(collector_affinity) is list and all(type(item) is int for item in collector_affinity) and
            type(child_affinity) is list and all(type(item) is int for item in child_affinity) and
            len(set(collector_affinity)) == len(collector_affinity) and cpu in collector_affinity and
            child_affinity == [cpu], 'invalid_cpu_affinity')
    require(type(swap) is dict and set(swap) == {'pswpin', 'pswpout'} and
            all(type(v) is int and v >= 0 for v in swap.values()), 'malformed_swap_counters')
    cgroup = _cgroup_shape(snapshot)
    return dict(monotonic_ns=snapshot['monotonic_ns'], machine_id=machine,
                governor=governor, cpu_psi_some=some, cpu_psi_full=full,
                memory_psi_full=memory_full, swap=swap, cgroup=cgroup,
                collector_affinity=collector_affinity, child_affinity=child_affinity)


def _check_host(raw_events, cpu):
    identity = None
    cgroup_shape = None
    checked = 0
    violations = []
    for event in raw_events:
        ready = event.get('launcher_ready')
        require(type(ready) is dict and type(ready.get('pid')) is int and
                type(ready.get('affinity')) is list and
                all(type(item) is int for item in ready['affinity']) and
                ready.get('affinity') == [cpu] and
                type(event.get('process_group_id')) is int and
                event['process_group_id'] == ready['pid'], 'invalid_cpu_affinity')
        after_child = event.get('aa_host_after', {}).get('child_affinity', {})
        require(after_child.get('observation') ==
                'carried_forward_from_verified_preexec_affinity', 'invalid_cpu_affinity')
        before = _host_observation(event.get('aa_host_before'), cpu)
        after = _host_observation(event.get('aa_host_after'), cpu)
        require(before['monotonic_ns'] <= after['monotonic_ns'], 'host_monotonic_clock_regressed')
        for observation in (before, after):
            current_identity = (observation['machine_id'], observation['governor'],
                                tuple(observation['collector_affinity']))
            if identity is None:
                identity = current_identity
            if current_identity != identity:
                violations.append('host_identity_or_governor_drift')
            shape = (observation['cgroup']['version'], observation['cgroup']['membership_path'],
                     observation['cgroup']['mount_root'], observation['cgroup']['mountpoint'],
                     [(level['path'], level['cpu_limit'], level['throttle_time_unit'])
                      for level in observation['cgroup']['levels']])
            if cgroup_shape is None:
                cgroup_shape = shape
            if shape != cgroup_shape:
                violations.append('cgroup_path_or_limit_drift')
            if observation['cpu_psi_some'] > POLICY['host_eligibility']['cpu_psi_some_avg10_max_percent']:
                violations.append('cpu_psi_some_over_limit')
            if observation['cpu_psi_full'] > POLICY['host_eligibility']['cpu_psi_full_avg10_max_percent']:
                violations.append('cpu_psi_full_over_limit')
            if observation['memory_psi_full'] > POLICY['host_eligibility']['memory_psi_full_avg10_max_percent']:
                violations.append('memory_psi_full_over_limit')
        if before['swap'] != after['swap']:
            violations.append('swap_counter_delta')
        if len(before['cgroup']['levels']) != len(after['cgroup']['levels']):
            violations.append('cgroup_path_or_limit_drift')
        else:
            for old, new in zip(before['cgroup']['levels'], after['cgroup']['levels']):
                if (new['nr_throttled'] < old['nr_throttled'] or
                        new['throttle_time'] < old['throttle_time']):
                    violations.append('cgroup_counter_regressed')
                elif (new['nr_throttled'] != old['nr_throttled'] or
                      new['throttle_time'] != old['throttle_time']):
                    violations.append('cgroup_throttle_delta')
        checked += 1
    return dict(checked_attempts=checked, machine_id=identity[0] if identity else None,
                governor=identity[1] if identity else None,
                collector_affinity=list(identity[2]) if identity else None,
                violations=sorted(set(violations)))


def _bootstrap_word(workload, order, replicate, draw, retry):
    name = workload.encode('utf-8')
    payload = (b'wirelog-aa-bootstrap-v1\0' +
               struct.pack('>QH', POLICY['bootstrap']['seed'], len(name)) + name +
               order.encode('ascii') + struct.pack('>III', replicate, draw, retry))
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], 'big')


def _bootstrap_index(workload, order, replicate, draw):
    size = 9
    limit = (1 << 64) - ((1 << 64) % size)
    for retry in range(1 << 32):
        value = _bootstrap_word(workload, order, replicate, draw, retry)
        if value < limit:
            return value % size
    raise InvalidEvidence('bootstrap_rng_exhausted')


def _bootstrap(workload, by_order):
    resamples = POLICY['bootstrap']['resamples']
    medians = []
    for replicate in range(resamples):
        sample = []
        for order in ('AB', 'BA'):
            values = by_order[order]
            for draw in range(9):
                sample.append(values[_bootstrap_index(workload, order, replicate, draw)])
        medians.append(statistics.median(sample))
    medians.sort()
    low, high = POLICY['bootstrap']['percentile_indices']
    return medians[low], medians[high], (medians[high] - medians[low]) / 2


def _same_paths(preflight):
    paths = _dict(preflight.get('paths'), 'preflight_paths')
    require(set(paths) == set(SIDES), 'invalid_evidence')
    for side in SIDES:
        paths[side] = _dict(paths[side], 'preflight_side_paths')
    require(set(_dict(paths['base'].get('executables'), 'preflight_executables')) == set(WORKLOADS) and
            set(_dict(paths['candidate'].get('executables'), 'preflight_executables')) == set(WORKLOADS),
            'invalid_evidence')
    for key in ('source', 'build', 'log', 'executables'):
        require(paths['base'].get(key) == paths['candidate'].get(key),
                'shared_build_path_identity_missing')
    return paths


def _preflight(preflight, campaign):
    require(type(preflight.get('schema_version')) is int and
            preflight.get('schema_version') == 1 and preflight.get('mode') == 'aa_control' and
            preflight.get('qualify_aa') is True, 'invalid_evidence')
    manifest = campaign.get('manifest')
    require(type(manifest) is dict and preflight.get('manifest') == manifest,
            'preflight_manifest_mismatch')
    require(campaign.get('campaign_mode') == 'aa_control', 'invalid_campaign_mode')
    _same_paths(preflight)
    require(preflight['paths']['base']['source'] == preflight['paths']['candidate']['source'] and
            preflight['paths']['base']['build'] == preflight['paths']['candidate']['build'] and
            preflight['paths']['base']['log'] == preflight['paths']['candidate']['log'],
            'shared_build_path_identity_missing')
    require(manifest['sources']['base'] == manifest['sources']['candidate'] and
            manifest['build_provenance']['base']['build_instance_id'] ==
            manifest['build_provenance']['candidate']['build_instance_id'],
            'shared_build_identity_missing')
    records = _dict(preflight.get('records'), 'preflight_records')
    require(set(records) == set(SIDES), 'invalid_evidence')
    for side in SIDES:
        raw_profile = _dict(records[side].get('profile'), 'preflight_profile')
        resolved_profile = {name: COLLECTOR['profile_value'](value)
                            for name, value in raw_profile.items()}
        log_path = Path(preflight['paths'][side]['log']).resolve()
        log_hash = manifest['build_provenance'][side]['build_log_sha256']
        require(records[side].get('source_sha') == manifest['sources'][side] and
                resolved_profile == manifest['profiles'][side] and
                preflight['paths'][side]['log'] == preflight['paths']['base']['log'] and
                preflight.get('artifacts', {}).get(str(log_path)) == log_hash and
                log_path.is_file() and COLLECTOR['LEGACY']['sha256'](log_path) == log_hash,
                'preflight_provenance_mismatch')
    require(preflight.get('campaign_id') == campaign.get('campaign_id'), 'campaign_id_mismatch')
    require(preflight.get('launcher_sha256') == COLLECTOR['LEGACY']['sha256'](HERE / 'paired-aa-affinity-launcher.py'),
            'launcher_hash_mismatch')
    for workload in WORKLOADS:
        require(workload in manifest['workloads'] and
                manifest['workloads'][workload]['binary_sha256']['base'] ==
                manifest['workloads'][workload]['binary_sha256']['candidate'],
                'binary_sha_mismatch')
        executables = preflight['paths']['base'].get('executables', {})
        for side in SIDES:
            path = preflight['paths'][side].get('executables', {}).get(workload)
            require(type(path) is str and path, 'invalid_evidence')
            key = str(Path(path).resolve())
            binary_hash = manifest['workloads'][workload]['binary_sha256'][side]
            require(preflight.get('artifacts', {}).get(key) == binary_hash and
                    Path(key).is_file() and
                    COLLECTOR['LEGACY']['sha256'](Path(key)) == binary_hash and
                    preflight['paths'][side]['log'] == preflight['paths']['base']['log'] and
                    preflight.get('artifacts', {}).get(str(Path(preflight['paths'][side]['log']).resolve())) ==
                    manifest['build_provenance'][side]['build_log_sha256'] and
                    manifest['workloads'][workload]['binary_sha256'][side],
                    'preflight_binary_hash_mismatch')
    return manifest


def _read_evidence(evidence_dir, preflight, manifest):
    campaign_path = evidence_dir / 'campaign-v1.json'
    raw_path = evidence_dir / 'raw-attempts.jsonl'
    links_path = evidence_dir / 'aa-attempt-links.jsonl'
    require(campaign_path.is_file() and raw_path.is_file() and links_path.is_file(),
            'incomplete_evidence')
    campaign = HOST['strict_json'](campaign_path.read_text(encoding='utf-8'))
    require(campaign.get('manifest') == manifest, 'preflight_manifest_mismatch')
    evaluated = COLLECTOR['EVALUATOR']['evaluate'](
        campaign, digest_bytes(campaign_path.read_bytes()))
    require(evaluated['status'] in ('COMPLETE_VALID', 'CORRECTNESS_FAILURE'),
            'campaign_not_complete_valid')
    raw_bytes = raw_path.read_bytes()
    link_bytes = links_path.read_bytes()
    require(raw_bytes.endswith(b'\n') and link_bytes.endswith(b'\n'), 'incomplete_evidence')
    raw_lines = raw_bytes.splitlines(keepends=True)
    link_lines = link_bytes.splitlines()
    require(len(raw_lines) == len(link_lines) == 80, 'incomplete_evidence')
    planned = COLLECTOR['schedule']()
    supplied_schedule = preflight.get('schedule')
    require(len(planned) == 80 and type(supplied_schedule) is list and
            len(supplied_schedule) == 80 and
            all(type(actual) is dict and set(actual) == set(expected) and
                all(type(actual.get(key)) is type(value) and actual[key] == value
                    for key, value in expected.items())
                for actual, expected in zip(supplied_schedule, planned)),
            'schedule_mismatch')
    attempts = {}
    raw_events = []
    manifest_hash = COLLECTOR['EVALUATOR']['digest'](manifest)
    for ordinal, (raw_line, link_line, expected) in enumerate(zip(raw_lines, link_lines, planned)):
        raw = HOST['strict_json'](raw_line.decode('utf-8'))
        link = HOST['strict_json'](link_line.decode('utf-8'))
        link_keys = {'ordinal', 'campaign_id', 'manifest_sha256', 'block_id', 'workload',
                     'sequence', 'side', 'raw_line_sha256'}
        require(type(link) is dict and set(link) == link_keys and
                type(link.get('ordinal')) is int and
                link['ordinal'] == ordinal and
                link['raw_line_sha256'] == digest_bytes(raw_line), 'raw_attempt_hash_mismatch')
        link_schedule = ('block_id', 'workload', 'sequence', 'side')
        require(all(type(raw.get(key)) is type(value) and raw.get(key) == value
                    for key, value in expected.items()) and
                all(type(link.get(key)) is type(expected[key]) and
                    link.get(key) == expected[key] for key in link_schedule) and
                type(raw.get('ordinal')) is int and raw['ordinal'] == ordinal and
                raw.get('campaign_id') == campaign['campaign_id'] and
                raw.get('manifest_sha256') == manifest_hash and
                link.get('campaign_id') == campaign['campaign_id'] and
                link.get('manifest_sha256') == manifest_hash, 'attempt_mapping_mismatch')
        require(raw.get('prelaunch_rejected') is not True, 'prelaunch_rejected')
        require(type(raw.get('exit_code')) is int and type(raw.get('timed_out')) is bool,
                'malformed_process_result')
        elapsed, correctness = COLLECTOR['parse_result'](raw.get('stdout', ''), expected['workload'])
        process_failed = (raw['exit_code'] != 0 or raw['timed_out'] or
                          correctness['status'] != 'OK' or
                          correctness['observed'] != manifest['workloads'][expected['workload']]['expected'])
        key = (expected['block_id'], expected['workload'], expected['sequence'])
        attempts[key] = dict(elapsed_ms=elapsed, side=expected['side'], correctness=correctness,
                             exit_code=raw['exit_code'], timed_out=raw['timed_out'],
                             process_failed=process_failed)
        raw_events.append(raw)
    require(len(attempts) == 80 and len(campaign.get('attempts', [])) == 80,
            'incomplete_evidence')
    campaign_attempts = {}
    for item in campaign['attempts']:
        key = (item.get('block_id'), item.get('workload'), item.get('sequence'))
        require(key not in campaign_attempts, 'attempt_mapping_mismatch')
        campaign_attempts[key] = item
    require(set(campaign_attempts) == set(attempts), 'attempt_mapping_mismatch')
    for key, raw in attempts.items():
        item = campaign_attempts[key]
        require(item.get('elapsed_ms') == raw['elapsed_ms'] and
                item.get('side') == raw['side'] and
                item.get('correctness') == raw['correctness'] and
                item.get('exit_code') == raw['exit_code'] and
                item.get('timed_out') is raw['timed_out'],
                'campaign_raw_mapping_mismatch')
    failures = [key for key, item in attempts.items() if item['process_failed']]
    return campaign, raw_events, attempts, failures, evaluated['status']


def _statistics(attempts):
    result = {}
    for workload in WORKLOADS:
        by_order = {'AB': [], 'BA': []}
        for order in ('AB', 'BA'):
            block = order
            for pair in range(9):
                first = attempts[(block, workload, 2 + pair * 2)]
                second = attempts[(block, workload, 3 + pair * 2)]
                base = first if first['side'] == 'base' else second
                candidate = second if second['side'] == 'candidate' else first
                by_order[order].append(math.log(candidate['elapsed_ms']) -
                                       math.log(base['elapsed_ms']))
        combined = by_order['AB'] + by_order['BA']
        median = statistics.median(combined)
        lower, upper, half_width = _bootstrap(workload, by_order)
        result[workload] = dict(pair_log_ratios={order: values for order, values in by_order.items()},
                                median_log_ratio=median, ci_lower_log_ratio=lower,
                                ci_upper_log_ratio=upper, ci_half_width=half_width,
                                order_effect_log_ratio=(statistics.mean(by_order['AB']) -
                                                        statistics.mean(by_order['BA'])),
                                resamples=POLICY['bootstrap']['resamples'])
    return result


def qualify(evidence_dir, preflight_path):
    base_result = dict(schema_version=1, evaluator_version='1.0', policy_id=POLICY['policy_id'],
                       policy=POLICY,
                       policy_snapshot_sha256=digest_bytes(POLICY_PATH.read_bytes()),
                       frozen_plan_commit=FROZEN_PLAN_COMMIT,
                       frozen_plan_sha256=FROZEN_PLAN_SHA256,
                       status='INCONCLUSIVE', reason='invalid_evidence', workloads={}, host={})
    try:
        require(base_result['policy_snapshot_sha256'] == POLICY_SHA256,
                'policy_snapshot_hash_mismatch')
        preflight = HOST['strict_json'](preflight_path.read_text(encoding='utf-8'))
        early = evidence_dir / 'aa-qualification-preflight.json'
        if early.is_file():
            result = HOST['strict_json'](early.read_text(encoding='utf-8'))
            if result.get('status') == 'FAIL' and result.get('reason') == 'binary_sha_mismatch':
                require(type(result.get('benchmark_launches')) is int and
                        result.get('benchmark_launches') == 0 and
                        type(result.get('workloads')) is list and bool(result['workloads']),
                        'invalid_evidence')
                for item in result['workloads']:
                    require(type(item) is dict and item.get('workload') in WORKLOADS and
                            item.get('base_sha256') != item.get('candidate_sha256'),
                            'invalid_evidence')
                    paths = preflight.get('paths', {})
                    artifacts = preflight.get('artifacts', {})
                    binary_paths = []
                    for side in SIDES:
                        path = paths.get(side, {}).get('executables', {}).get(item['workload'])
                        binary_paths.append(str(Path(path).resolve()) if type(path) is str else '')
                        require(type(path) is str and
                                artifacts.get(str(Path(path).resolve())) == item[f'{side}_sha256'] and
                                Path(path).is_file() and
                                COLLECTOR['LEGACY']['sha256'](Path(path)) == item[f'{side}_sha256'],
                                'invalid_evidence')
                    require(binary_paths[0] != binary_paths[1], 'invalid_evidence')
                base_result.update(status='FAIL', reason='binary_sha_mismatch',
                                   preflight=result, benchmark_launches=0)
                return base_result
            if result.get('status') == 'INCONCLUSIVE':
                paths = preflight.get('paths', {})
                shared_identity_missing = any(
                    paths.get('base', {}).get(key) != paths.get('candidate', {}).get(key)
                    for key in ('source', 'build', 'log'))
                require(result.get('reason') == 'shared_build_path_identity_missing' and
                        result.get('workloads') == [] and
                        type(result.get('benchmark_launches')) is int and
                        result.get('benchmark_launches') == 0 and
                        preflight.get('mode') == 'aa_control' and
                        preflight.get('qualify_aa') is True and shared_identity_missing,
                        'invalid_evidence')
                base_result.update(reason=result.get('reason', 'invalid_evidence'),
                                   preflight=result, benchmark_launches=0)
                return base_result
        campaign_path = evidence_dir / 'campaign-v1.json'
        require(campaign_path.is_file(), 'incomplete_evidence')
        campaign = HOST['strict_json'](campaign_path.read_text(encoding='utf-8'))
        manifest = _preflight(preflight, campaign)
        campaign, raw_events, attempts, failures, evaluator_status = _read_evidence(
            evidence_dir, preflight, manifest)
        try:
            host = _check_host(raw_events, manifest['cpu'])
        except HostIneligible as error:
            host = dict(checked_attempts=0, machine_id=None, governor=None,
                        collector_affinity=None, violations=[str(error)])
        if host['machine_id'] != manifest['host_id']:
            host['violations'].append('host_id_mismatch')
        stats = _statistics(attempts)
        base_result.update(campaign_id=campaign['campaign_id'], host=host, workloads=stats,
                           campaign_evaluator_status=evaluator_status, benchmark_launches=80)
        if failures:
            base_result.update(status='FAIL', reason='process_or_correctness_failure',
                               failed_attempts=[list(key) for key in failures])
            return base_result
        if host['violations']:
            base_result.update(status='INCONCLUSIVE', reason='host_ineligible')
            return base_result
        widths = [row['ci_half_width'] for row in stats.values()]
        if any(width > POLICY['bootstrap']['max_ci_half_width'] for width in widths):
            base_result.update(status='INCONCLUSIVE', reason='wide_confidence_interval')
            return base_result
        if any(abs(row['median_log_ratio']) > POLICY['bootstrap']['max_abs_median_log_ratio']
               for row in stats.values()):
            base_result.update(status='FAIL', reason='aa_median_outside_noise_floor')
            return base_result
        base_result.update(status='PASS', reason='both_workloads_within_noise_floor')
        return base_result
    except HostIneligible as error:
        base_result.update(status='INCONCLUSIVE', reason='host_ineligible',
                           detail=str(error))
        return base_result
    except (OSError, UnicodeError, ValueError, TypeError, KeyError, IndexError, AttributeError,
            OverflowError, struct.error) as error:
        base_result.update(status='INCONCLUSIVE', reason='invalid_evidence',
                           detail=f'{type(error).__name__}: {error}')
        return base_result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--preflight', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = qualify(args.evidence_dir, args.preflight)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + '.tmp')
    with temporary.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, sort_keys=True, indent=2, allow_nan=False)
        stream.write('\n')
        stream.flush()
    temporary.replace(args.output)
    print(json.dumps(dict(status=result['status'], reason=result['reason']), sort_keys=True))
    return 0 if result['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
