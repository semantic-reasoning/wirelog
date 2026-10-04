#!/usr/bin/env python3
"""Qualify two frozen-plan B/M campaigns; never make a timing verdict."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import runpy
import statistics
import sys

HERE = Path(__file__).resolve().parent
POLICY_PATH = HERE / 'paired-bm-policy-v1.json'
POLICY = json.loads(POLICY_PATH.read_text(encoding='utf-8'))
POLICY_SHA256 = hashlib.sha256(POLICY_PATH.read_bytes()).hexdigest()
FROZEN_POLICY_SHA256 = '523029143376f1765e920d6932181cc049831dcebddd7531ffa70cc713848e55'
COLLECTOR = runpy.run_path(str(HERE / 'paired-collector-v1.py'))
EVALUATOR = COLLECTOR['EVALUATOR']
HOST = COLLECTOR['AA_HOST']
SIDES = ('base', 'candidate')
WORKLOADS = tuple(POLICY['campaign']['workloads'])
MISSING = object()


class InvalidEvidence(ValueError):
    pass


class MissingEvidence(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise InvalidEvidence(message)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def strict_json(path):
    try:
        return HOST['strict_json'](Path(path).read_text(encoding='utf-8'))
    except FileNotFoundError as error:
        raise MissingEvidence(f'missing:{Path(path).name}') from error


def entry_value(entry, name, numeric=False):
    if entry is MISSING:
        raise MissingEvidence(f'missing_host:{name}')
    require(type(entry) is dict and {'value', 'unavailable_reason'} <= set(entry) and
            set(entry) <= {'value', 'unavailable_reason', 'unit'},
            f'malformed_host:{name}')
    if 'unit' in entry:
        require(entry['unit'] is None or
                type(entry['unit']) is str and bool(entry['unit']),
                f'malformed_host:{name}_unit')
    value = entry['value']
    reason = entry['unavailable_reason']
    if value is None:
        require(type(reason) is str and bool(reason), f'malformed_host:{name}')
        raise MissingEvidence(f'missing_host:{name}')
    require(reason is None, f'malformed_host:{name}')
    if numeric:
        require(type(value) in (int, float) and math.isfinite(value) and value >= 0,
                f'malformed_host:{name}')
    return value


def cgroup_levels(value):
    require(type(value) is dict and value.get('version') in ('v1', 'v2') and
            set(value) == {'version', 'membership_path', 'mount_root', 'mountpoint', 'levels'} and
            type(value.get('membership_path')) is str and
            type(value.get('mount_root')) is str and type(value.get('mountpoint')) is str and
            type(value.get('levels')) is list and bool(value['levels']),
            'malformed_cgroup')
    require(value['membership_path'].startswith('/') and value['mount_root'].startswith('/') and
            value['mountpoint'].startswith('/') and '..' not in value['membership_path'].split('/'),
            'malformed_cgroup')
    levels = []
    for level in value['levels']:
        require(type(level) is dict and set(level) == {'path', 'cpu_limit', 'nr_throttled',
                'throttle_time', 'throttle_time_unit'} and type(level.get('path')) is str and
                type(level.get('nr_throttled')) is int and level['nr_throttled'] >= 0 and
                type(level.get('throttle_time')) is int and level['throttle_time'] >= 0 and
                level.get('throttle_time_unit') == ('us' if value['version'] == 'v2' else 'ns') and
                type(level.get('cpu_limit')) is dict,
                'malformed_cgroup')
        limit = level['cpu_limit']
        if value['version'] == 'v2':
            require(set(limit) == {'quota', 'period_us'} and
                    (limit['quota'] == 'max' or type(limit['quota']) is str and
                     limit['quota'].isdigit()) and type(limit['period_us']) is int and
                    limit['period_us'] > 0, 'malformed_cgroup')
        else:
            require(set(limit) == {'quota_us', 'period_us'} and
                    type(limit['quota_us']) is int and limit['quota_us'] >= -1 and
                    type(limit['period_us']) is int and
                    limit['period_us'] > 0, 'malformed_cgroup')
        levels.append((level['path'], level['nr_throttled'], level['throttle_time'], limit))
    return (value['version'], value.get('membership_path'), value.get('mount_root'),
            value.get('mountpoint'), levels)


def _attempt_host_checks(raw_events, manifest):
    violations = []
    cpu_count_seen = None
    machine_ids = set()
    for raw in raw_events:
        pair = []
        for boundary in ('bm_host_before', 'bm_host_after'):
            if boundary not in raw:
                raise MissingEvidence(f'missing_host_snapshot:{boundary}')
            snapshot = raw[boundary]
            require(type(snapshot) is dict, 'malformed_host_snapshot')
            machine_id = entry_value(snapshot.get('machine_id', MISSING), 'machine_id')
            require(type(machine_id) is str and bool(machine_id), 'malformed_host:machine_id')
            require(machine_id == manifest['host_id'], 'host_manifest_identity_mismatch')
            machine_ids.add(machine_id)
            cpus = entry_value(snapshot.get('logical_cpu_count', MISSING), 'logical_cpu_count')
            require(type(cpus) is int and cpus > 0, 'malformed_host:logical_cpu_count')
            if cpu_count_seen is None:
                cpu_count_seen = cpus
            elif cpu_count_seen != cpus:
                violations.append('logical_cpu_count_changed')
            load = entry_value(snapshot.get('load_one', MISSING), 'load_one', numeric=True)
            psi = entry_value(snapshot.get('cpu_psi_some_avg10_percent', MISSING),
                              'cpu_psi_some_avg10_percent', numeric=True)
            cg_entry = snapshot.get('cgroup', MISSING)
            cg = entry_value(cg_entry, 'cgroup')
            identity = cgroup_levels(cg)
            pair.append((identity, psi, load))
            if psi > POLICY['limits']['max_cpu_psi_some_avg10_percent']:
                violations.append('cpu_psi_some_over_limit')
            if load > cpus * POLICY['limits']['max_load_one_per_logical_cpu']:
                violations.append('load_over_logical_cpu_limit')
        before, after = pair
        if before[0][:4] != after[0][:4] or [x[0] for x in before[0][4]] != [x[0] for x in after[0][4]]:
            violations.append('effective_cgroup_changed')
        else:
            for old, new in zip(before[0][4], after[0][4]):
                if new[1] < old[1] or new[2] < old[2]:
                    raise InvalidEvidence('cgroup_throttle_counter_regressed')
                if new[3] != old[3]:
                    violations.append('effective_cgroup_changed')
                if new[1] - old[1] > POLICY['limits']['max_cgroup_nr_throttled_delta']:
                    violations.append('cgroup_throttle_delta')
    return dict(boundaries_checked=len(raw_events) * 2,
                logical_cpu_count=cpu_count_seen,
                machine_ids=sorted(machine_ids),
                violations=sorted(set(violations)))


def _validate_preflight(root, plan, campaign):
    preflight = strict_json(root / 'preflight.json')
    require(type(preflight) is dict and preflight.get('schema_version') == 1 and
            preflight.get('mode') == 'comparison' and preflight.get('qualify_bm') is True,
            'invalid_preflight')
    require(preflight.get('bm_plan_sha256') == plan['_sha256'] and
            preflight.get('campaign_id') == campaign['campaign_id'] and
            preflight.get('manifest') == campaign['manifest'], 'preflight_campaign_mismatch')
    manifest = campaign['manifest']
    require(manifest['sources'] == {'base': plan['base_sha'],
                                    'candidate': plan['candidate_sha']},
            'source_plan_mismatch')
    expected_workloads = COLLECTOR['EXPECTED']
    workload_records = manifest.get('workloads')
    require(type(workload_records) is dict and
            set(workload_records) == set(expected_workloads),
            'malformed_workload_contract')
    for workload, expected in expected_workloads.items():
        workload_record = workload_records[workload]
        require(type(workload_record) is dict, 'malformed_workload_contract')
        actual = workload_record.get('expected')
        require(type(actual) is dict and set(actual) == set(expected) and
                all(type(value) is int for value in actual.values()) and
                actual == expected, f'{workload}_expected_contract_mismatch')
    paths = preflight.get('paths')
    records = preflight.get('records')
    artifacts = preflight.get('artifacts')
    require(type(paths) is dict and set(paths) == set(SIDES) and
            type(records) is dict and set(records) == set(SIDES) and
            type(artifacts) is dict, 'invalid_preflight')
    require(all(type(path) is str and type(digest) is str and
                re.fullmatch(r'[0-9a-f]{64}', digest)
                for path, digest in artifacts.items()), 'malformed_artifact_inventory')
    source_hash_paths = tuple(dict.fromkeys(COLLECTOR['VERIFY']['SAME_SOURCES'] +
                                            COLLECTOR['VERIFY']['GATE_SOURCES']))
    timer_hash_paths = tuple(COLLECTOR['CONTRACT_SOURCES'])
    fixture_paths = tuple(COLLECTOR['VERIFY']['FIXTURES'])
    binary_paths = tuple(COLLECTOR['VERIFY']['BINARIES'])
    for side in SIDES:
        require(type(paths[side]) is dict and
                set(paths[side]) == {'source', 'build', 'log', 'executables'} and
                all(type(paths[side][key]) is str and bool(paths[side][key])
                    for key in ('source', 'build', 'log')) and
                type(paths[side]['executables']) is dict and
                set(paths[side]['executables']) == {'crdt', 'cspa-fast'} and
                all(type(path) is str and bool(path)
                    for path in paths[side]['executables'].values()),
                f'malformed_{side}_paths')
        record = records[side]
        record_keys = {'source_sha', 'source_tree', 'profile', 'compiler',
                       'source_sha256', 'fixture_sha256', 'binary_sha256',
                       'timer_contract_sha256'}
        require(type(record) is dict and set(record) == record_keys,
                f'malformed_{side}_record')
        require(type(record['source_sha']) is str and
                re.fullmatch(r'[0-9a-f]{40}', record['source_sha']) and
                type(record['source_tree']) is str and
                re.fullmatch(r'[0-9a-f]{40}', record['source_tree']) and
                record['source_tree'] == plan[f'{side}_tree_sha'],
                f'{side}_source_tree_mismatch')
        require(record.get('source_sha') == plan[f'{side}_sha'] and
                type(record['profile']) is dict and
                set(COLLECTOR['VERIFY']['PROFILE_OPTIONS']) <= set(record['profile']) and
                all(type(name) is str and bool(name) for name in record['profile']) and
                type(record['compiler']) is dict and
                set(record['compiler']) == {'id', 'version', 'full_version', 'linker_id', 'exelist'} and
                all(type(record['compiler'][key]) is str and bool(record['compiler'][key])
                    for key in ('id', 'version', 'full_version', 'linker_id')) and
                type(record['compiler']['exelist']) is list and
                bool(record['compiler']['exelist']) and
                all(type(item) is str and bool(item) for item in record['compiler']['exelist']),
                f'malformed_{side}_build_provenance')
        for field, expected_paths in (('source_sha256', source_hash_paths),
                                      ('fixture_sha256', fixture_paths),
                                      ('binary_sha256', binary_paths),
                                      ('timer_contract_sha256', timer_hash_paths)):
            inventory = record[field]
            require(type(inventory) is dict and set(inventory) == set(expected_paths) and
                    all(type(value) is str and re.fullmatch(r'[0-9a-f]{64}', value)
                        for value in inventory.values()),
                    f'malformed_{side}_{field}')
            for relative, digest in inventory.items():
                expected_path = str((Path(paths[side]['source']) / relative).resolve())
                if field == 'binary_sha256':
                    expected_path = str((Path(paths[side]['build']) / relative).resolve())
                require(artifacts.get(expected_path) == digest,
                        f'{side}_{field}_artifact_mismatch:{relative}')
        resolved = {name: COLLECTOR['profile_value'](value)
                    for name, value in record['profile'].items()}
        require(resolved == manifest['profiles'][side] and
                record['source_sha'] == manifest['build_provenance'][side]['source_sha'] and
                resolved == manifest['build_provenance'][side]['profile'],
                'profile_provenance_mismatch')
        meson_options_path = str((Path(paths[side]['build']) /
                                  'meson-info/intro-buildoptions.json').resolve())
        meson_compilers_path = str((Path(paths[side]['build']) /
                                    'meson-info/intro-compilers.json').resolve())
        for evidence_name, artifact_path in (
                (f'{side}-intro-buildoptions.json', meson_options_path),
                (f'{side}-intro-compilers.json', meson_compilers_path)):
            evidence_path = root / evidence_name
            if not evidence_path.is_file():
                raise MissingEvidence(f'missing:{evidence_name}')
            require(artifacts.get(artifact_path) == sha256(evidence_path.read_bytes()),
                    f'{side}_meson_evidence_hash_mismatch:{evidence_name}')
        options_document = strict_json(root / f'{side}-intro-buildoptions.json')
        require(type(options_document) is list, f'malformed_{side}_meson_build_options')
        options = {}
        for option in options_document:
            require(type(option) is dict and {'name', 'value'} <= set(option) and
                    type(option['name']) is str and bool(option['name']) and
                    option['name'] not in options,
                    f'malformed_{side}_meson_build_option')
            COLLECTOR['profile_value'](option['value'])
            options[option['name']] = option['value']
        require(set(COLLECTOR['VERIFY']['PROFILE_OPTIONS']) <= set(options) and
                options == record['profile'], f'{side}_meson_profile_content_mismatch')
        compilers_document = strict_json(root / f'{side}-intro-compilers.json')
        require(type(compilers_document) is dict and
                type(compilers_document.get('host')) is dict and
                type(compilers_document['host'].get('c')) is dict,
                f'malformed_{side}_meson_compiler_document')
        compiler = compilers_document['host']['c']
        compiler_identity = {}
        for key in ('id', 'version', 'full_version', 'linker_id', 'exelist'):
            require(key in compiler, f'missing_{side}_meson_compiler_field:{key}')
            compiler_identity[key] = compiler[key]
        require(compiler_identity == record['compiler'],
                f'{side}_meson_compiler_content_mismatch')
        log_hash = manifest['build_provenance'][side]['build_log_sha256']
        copied_log = root / f'{side}-build.log'
        try:
            actual_log = sha256(copied_log.read_bytes())
        except FileNotFoundError as error:
            raise MissingEvidence(f'missing:{copied_log.name}') from error
        require(actual_log == log_hash and artifacts.get(paths[side].get('log')) == log_hash,
                'build_log_provenance_mismatch')
        for workload in WORKLOADS:
            entry = manifest['workloads'][workload]
            binary_path = paths[side].get('executables', {}).get(workload)
            require(type(binary_path) is str and
                    artifacts.get(binary_path) == entry['binary_sha256'][side] and
                    record['binary_sha256'].get('bench/bench_flowlog' if workload == 'cspa-fast'
                                                 else 'tests/test_crdt_perf_gate') ==
                    entry['binary_sha256'][side], 'binary_provenance_mismatch')
            prefix = 'bench/data/cspa/' if workload == 'cspa-fast' else 'bench/data/crdt/'
            fixtures = {key: value for key, value in record['fixture_sha256'].items()
                        if key.startswith(prefix)}
            require(fixtures == entry['fixture_sha256'][side], 'fixture_provenance_mismatch')
    require(records['base']['compiler'] == records['candidate']['compiler'] and
            records['base']['profile'] == records['candidate']['profile'] and
            records['base']['fixture_sha256'] == records['candidate']['fixture_sha256'],
            'campaign_builds_not_comparable')
    for relative in source_hash_paths:
        if relative in COLLECTOR['VERIFY']['SAME_SOURCES']:
            require(records['base']['source_sha256'][relative] ==
                    records['candidate']['source_sha256'][relative],
                    f'cross_side_benchmark_source_mismatch:{relative}')
    require(records['base']['timer_contract_sha256'] ==
            records['candidate']['timer_contract_sha256'],
            'cross_side_timer_contract_mismatch')
    require(manifest['workloads']['crdt']['timer'] == COLLECTOR['TIMERS']['crdt'] and
            manifest['workloads']['cspa-fast']['timer'] == COLLECTOR['TIMERS']['cspa-fast'],
            'benchmark_timer_mismatch')
    return preflight


def _validate_raw_record(raw, item, ordinal):
    require(type(raw) is dict, 'malformed_raw_attempt_object')

    def required(name, value_type):
        if name not in raw:
            raise MissingEvidence(f'missing_raw_field:{name}')
        require(type(raw[name]) is value_type, f'malformed_raw_field:{name}')
        return raw[name]

    for name in ('block_id', 'workload', 'phase', 'side', 'campaign_id',
                 'manifest_sha256'):
        required(name, str)
    require(required('sequence', int) == item['sequence'],
            'raw_attempt_sequence_mismatch')
    pair_index = raw.get('pair_index', MISSING)
    if pair_index is MISSING:
        raise MissingEvidence('missing_raw_field:pair_index')
    require(pair_index is None or type(pair_index) is int,
            'malformed_raw_field:pair_index')
    require(required('ordinal', int) == ordinal, 'raw_attempt_ordinal_mismatch')
    command = required('command', list)
    require(all(type(part) is str for part in command), 'malformed_raw_command')
    required('stdout', str)
    required('stderr', str)
    required('exit_code', int)
    required('timed_out', bool)
    legacy_host_keys = {'time_utc', 'loadavg', 'cpu_pressure', 'cgroup_cpu_stat',
                        'governor', 'frequency_khz', 'frequency_min_khz',
                        'frequency_max_khz'}
    for boundary in ('host_before', 'host_after'):
        snapshot = required(boundary, dict)
        require(set(snapshot) == legacy_host_keys and
                all(type(value) is str for value in snapshot.values()),
                f'malformed_raw_{boundary}')
    for boundary in ('bm_host_before', 'bm_host_after'):
        if boundary not in raw:
            raise MissingEvidence(f'missing_raw_field:{boundary}')
        require(type(raw[boundary]) is dict, f'malformed_raw_{boundary}')

    bool_fields = ('prelaunch_rejected', 'interrupted')
    for name in bool_fields:
        if name in raw:
            require(type(raw[name]) is bool, f'malformed_raw_field:{name}')
    for name in ('process_group_id', 'signal_number'):
        if name in raw:
            require(type(raw[name]) is int, f'malformed_raw_field:{name}')
    for name in ('launch_rejection', 'launch_error', 'cleanup_error', 'capture_error',
                 'interruption', 'signal_name', 'primary_interruption',
                 'secondary_interruption', 'further_interruption', 'parse_error'):
        if name in raw:
            require(type(raw[name]) is str, f'malformed_raw_field:{name}')

    for key, value in item.items():
        if value is None:
            if key not in raw:
                raise MissingEvidence(f'missing_raw_field:{key}')
            require(raw[key] is None, f'malformed_raw_field:{key}')
        else:
            actual = required(key, type(value))
            require(actual == value, f'raw_attempt_identity_mismatch:{key}')
    require(pair_index == item['pair_index'], 'raw_attempt_pair_index_mismatch')
    return raw


def _read_campaign(root, plan):
    campaign_path = root / 'campaign-v1.json'
    raw_path = root / 'raw-attempts.jsonl'
    links_path = root / 'bm-attempt-links.jsonl'
    for path in (campaign_path, raw_path, links_path, root / 'evaluation-report.json',
                 root / 'collection-status.json'):
        if not path.is_file():
            raise MissingEvidence(f'missing:{path.name}')
    campaign_bytes = campaign_path.read_bytes()
    campaign = HOST['strict_json'](campaign_bytes.decode('utf-8'))
    require(type(campaign) is dict, 'malformed_campaign_object')
    evaluated = EVALUATOR['evaluate'](campaign, sha256(campaign_bytes))
    require(evaluated['status'] in ('COMPLETE_VALID', 'CORRECTNESS_FAILURE',
                                    'INCOMPLETE', 'INVALID_EVIDENCE'),
            'unknown_campaign_status')
    stored_evaluation = strict_json(root / 'evaluation-report.json')
    require(type(stored_evaluation) is dict, 'malformed_evaluation_object')
    require(stored_evaluation == evaluated, 'typed_evaluation_tampered')
    collection = strict_json(root / 'collection-status.json')
    require(type(collection) is dict, 'malformed_collection_status_object')
    require(collection.get('status') == evaluated['status'], 'collection_status_mismatch')
    preflight = _validate_preflight(root, plan, campaign)
    raw_bytes = raw_path.read_bytes()
    links_bytes = links_path.read_bytes()
    require(raw_bytes.endswith(b'\n') and links_bytes.endswith(b'\n'), 'incomplete_raw_evidence')
    raw_lines, link_lines = raw_bytes.splitlines(keepends=True), links_bytes.splitlines()
    planned = COLLECTOR['schedule']()
    if len(raw_lines) != 80 or len(link_lines) != 80:
        raise MissingEvidence('incomplete_raw_evidence')
    require(len(planned) == 80, 'collector_schedule_mismatch')
    manifest_digest = EVALUATOR['digest'](campaign['manifest'])
    typed = {(x['block_id'], x['workload'], x['sequence']): x
             for x in campaign['attempts']}
    raw_events = []
    raw_process_failure = False
    for ordinal, (line, link_line, item) in enumerate(zip(raw_lines, link_lines, planned)):
        raw = HOST['strict_json'](line.decode('utf-8'))
        link = HOST['strict_json'](link_line.decode('utf-8'))
        _validate_raw_record(raw, item, ordinal)
        require(type(link) is dict and link == dict(ordinal=ordinal,
                campaign_id=campaign['campaign_id'], manifest_sha256=manifest_digest,
                block_id=item['block_id'], workload=item['workload'],
                sequence=item['sequence'], side=item['side'], raw_line_sha256=sha256(line)),
                'raw_attempt_link_mismatch')
        require(all(raw.get(key) == value for key, value in item.items()) and
                raw.get('ordinal') == ordinal and
                raw.get('campaign_id') == campaign['campaign_id'] and
                raw.get('manifest_sha256') == manifest_digest,
                'raw_attempt_identity_mismatch')
        process_failed = (raw.get('exit_code') != 0 or raw.get('timed_out') is True or
                          raw.get('prelaunch_rejected') is True)
        raw_process_failure = raw_process_failure or process_failed
        key = (item['block_id'], item['workload'], item['sequence'])
        event = typed.get(key)
        if event is None and process_failed:
            raw_events.append(raw)
            continue
        if event is None:
            raise MissingEvidence('typed_attempt_missing')
        try:
            elapsed, correctness = COLLECTOR['parse_result'](raw.get('stdout', ''), item['workload'])
        except (ValueError, TypeError, KeyError):
            if process_failed:
                raw_events.append(raw)
                continue
            raise InvalidEvidence('unparseable_successful_raw_attempt')
        require(event['elapsed_ms'] == elapsed and event['correctness'] == correctness and
                event['exit_code'] == raw.get('exit_code') and
                event['timed_out'] is raw.get('timed_out') and
                event['host_before'] == COLLECTOR['telemetry'](raw.get('host_before', {})) and
                event['host_after'] == COLLECTOR['telemetry'](raw.get('host_after', {})),
                'raw_typed_attempt_mismatch')
        raw_events.append(raw)
    return campaign, preflight, evaluated, raw_events, raw_process_failure


def _campaign_stats(campaign):
    attempts = campaign['attempts']
    output = {}
    for workload in WORKLOADS:
        output[workload] = {}
        for side in SIDES:
            selected = [item['elapsed_ms'] for item in attempts
                        if item['workload'] == workload and item['side'] == side and
                        item['phase'] == 'sample']
            require(len(selected) == 18, 'incomplete_timed_samples')
            output[workload][side] = dict(median_ms=statistics.median(selected),
                                          sample_count=len(selected))
            for block in ('AB', 'BA'):
                by_block = [item['elapsed_ms'] for item in attempts
                            if item['workload'] == workload and item['side'] == side and
                            item['block_id'] == block and item['phase'] == 'sample']
                require(len(by_block) == 9, 'incomplete_timed_samples')
                cov = 100 * statistics.pstdev(by_block) / statistics.fmean(by_block)
                output[workload][side][f'cov_percent_{block}'] = cov
    return output


def qualify(plan_path, attempt_one, attempt_two):
    result = dict(schema_version=1, evaluator_version='1.0', policy_id=POLICY['policy_id'],
                  policy_sha256=POLICY_SHA256, status='INCONCLUSIVE', reason='missing_evidence',
                  eligibility='not established', attempts={}, workloads={},
                  cspa_absolute_gate='not evaluated by this diagnostic qualifier')
    try:
        require(POLICY_SHA256 == FROZEN_POLICY_SHA256, 'policy_snapshot_hash_mismatch')
        plan = strict_json(plan_path)
        require(type(plan) is dict and plan.get('schema_version') == 1 and
                plan.get('plan_id') == 'tagged-runner-bm-repeat-v1' and
                plan.get('policy_id') == POLICY['policy_id'] and
                plan.get('policy_sha256') == POLICY_SHA256 and
                plan.get('campaign_mode') == 'comparison' and
                plan.get('workloads') == list(WORKLOADS) and
                plan.get('attempts') == ['attempt-1', 'attempt-2'] and
                type(plan.get('base_sha')) is str and len(plan['base_sha']) == 40 and
                type(plan.get('candidate_sha')) is str and len(plan['candidate_sha']) == 40 and
                type(plan.get('base_tree_sha')) is str and
                re.fullmatch(r'[0-9a-f]{40}', plan['base_tree_sha']) and
                type(plan.get('candidate_tree_sha')) is str and
                re.fullmatch(r'[0-9a-f]{40}', plan['candidate_tree_sha']) and
                plan['base_sha'] != plan['candidate_sha'], 'invalid_plan')
        plan['_sha256'] = sha256(Path(plan_path).read_bytes())
        campaigns = []
        correctness_failed = False
        for label, path in (('attempt-1', attempt_one), ('attempt-2', attempt_two)):
            campaign, preflight, evaluated, raw_events, raw_failure = _read_campaign(Path(path), plan)
            result['attempts'][label] = dict(campaign_id=campaign['campaign_id'],
                evaluator_status=evaluated['status'], manifest_sha256=evaluated['manifest_sha256'])
            if evaluated['status'] == 'CORRECTNESS_FAILURE' or raw_failure:
                correctness_failed = True
                campaigns.append((campaign, preflight, None, None))
                continue
            if evaluated['status'] == 'INCOMPLETE':
                raise MissingEvidence('incomplete_typed_campaign')
            require(evaluated['status'] == 'COMPLETE_VALID', 'invalid_typed_campaign')
            host = _attempt_host_checks(raw_events, campaign['manifest'])
            stats = _campaign_stats(campaign)
            campaigns.append((campaign, preflight, host, stats))
        require(campaigns[0][0]['campaign_id'] != campaigns[1][0]['campaign_id'],
                'campaign_ids_not_independent')
        if correctness_failed:
            result.update(status='CORRECTNESS_FAILURE', reason='campaign_correctness_failure',
                          eligibility='not established')
            return result
        first, second = campaigns
        for campaign, _, _, _ in campaigns:
            require(campaign['campaign_mode'] == 'comparison', 'wrong_campaign_mode')
            require(campaign['manifest']['sources'] == {'base': plan['base_sha'],
                    'candidate': plan['candidate_sha']}, 'source_plan_mismatch')
        for key in ('sources', 'profiles'):
            require(first[0]['manifest'][key] == second[0]['manifest'][key],
                    f'campaign_{key}_mismatch')
        for workload in WORKLOADS:
            a = first[0]['manifest']['workloads'][workload]
            b = second[0]['manifest']['workloads'][workload]
            require(a['timer'] == b['timer'] and a['fixture_sha256'] == b['fixture_sha256'] and
                    a['expected'] == b['expected'], f'campaign_{workload}_contract_mismatch')
        for side in SIDES:
            first_record = first[1]['records'][side]
            second_record = second[1]['records'][side]
            require(first_record['compiler'] == second_record['compiler'] and
                    first_record['profile'] == second_record['profile'] and
                    first_record['source_tree'] == second_record['source_tree'] and
                    first_record['source_sha256'] == second_record['source_sha256'] and
                    first_record['fixture_sha256'] == second_record['fixture_sha256'] and
                    first_record['timer_contract_sha256'] ==
                    second_record['timer_contract_sha256'],
                    f'{side}_cross_campaign_build_contract_mismatch')
        result['host'] = {label: host for label, (_, _, host, _) in
                          zip(('attempt-1', 'attempt-2'), campaigns)}
        result['workloads'] = {}
        violations = []
        for campaign, _, host, stats in campaigns:
            violations.extend(host['violations'])
            for workload in WORKLOADS:
                for side in SIDES:
                    for suffix in ('AB', 'BA'):
                        key = f'cov_percent_{suffix}'
                        if stats[workload][side][key] > POLICY['limits']['max_cov_percent']:
                            violations.append(f'{workload}_{side}_{key}_over_limit')
        for workload in WORKLOADS:
            result['workloads'][workload] = {}
            for side in SIDES:
                medians = [item[3][workload][side]['median_ms'] for item in campaigns]
                result['workloads'][workload][side] = dict(
                    attempt_medians_ms=medians,
                    max_min_spread_ratio=max(medians) / min(medians))
                if max(medians) / min(medians) > POLICY['limits']['max_same_side_median_spread_ratio']:
                    violations.append(f'{workload}_{side}_attempt_spread_over_limit')
            ratios = [item[3][workload]['candidate']['median_ms'] /
                      item[3][workload]['base']['median_ms'] for item in campaigns]
            result['workloads'][workload]['candidate_base_median_ratios'] = ratios
            if max(ratios) / min(ratios) > POLICY['limits']['max_candidate_base_median_spread_ratio']:
                violations.append(f'{workload}_candidate_base_ratio_spread_over_limit')
        if violations:
            result.update(status='INCONCLUSIVE', reason='eligibility_criteria_not_met',
                          eligibility='ineligible for interpretation', violations=sorted(set(violations)))
        else:
            result.update(status='ELIGIBLE', reason='both complete campaigns meet predeclared eligibility',
                          eligibility='interpretation only; never a performance PASS')
        return result
    except MissingEvidence as error:
        result.update(status='INCONCLUSIVE', reason='missing_evidence', detail=str(error))
        return result
    except (InvalidEvidence, OSError, UnicodeError, ValueError, TypeError, KeyError,
            IndexError, OverflowError) as error:
        result.update(status='INVALID_EVIDENCE', reason='invalid_evidence',
                      detail=f'{type(error).__name__}: {error}')
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--attempt-one', type=Path, required=True)
    parser.add_argument('--attempt-two', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = qualify(args.plan, args.attempt_one, args.attempt_two)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, sort_keys=True, indent=2,
                                      allow_nan=False) + '\n', encoding='utf-8')
    print(json.dumps(dict(status=report['status'], reason=report['reason']), sort_keys=True))
    return 0 if report['status'] == 'ELIGIBLE' else 1


if __name__ == '__main__':
    raise SystemExit(main())
