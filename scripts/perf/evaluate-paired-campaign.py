#!/usr/bin/env python3
"""Evaluate immutable paired campaign v1 evidence offline; no timing verdict."""
from __future__ import annotations
import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
import sys

VERSION = '1.0'
SIDES = ('base', 'candidate')
TELEMETRY = {'load_one': (int, float), 'cpu_pressure_some_total_us': (int,),
             'cgroup_throttled_us': (int,), 'frequency_khz': (int,),
             'governor': (str,)}

class Invalid(ValueError):
    pass

def require(condition, message):
    if not condition:
        raise Invalid(message)

def keys(value, expected, name):
    require(type(value) is dict and set(value) == set(expected), f'{name}: wrong fields')

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    allow_nan=False).encode()).hexdigest()

def sha(value, length):
    return type(value) is str and re.fullmatch('[0-9a-f]{' + str(length) + '}', value)

def telemetry(value):
    keys(value, TELEMETRY, 'telemetry')
    for name, types in TELEMETRY.items():
        item = value[name]
        keys(item, ('value', 'unavailable_reason'), name)
        if item['value'] is None:
            require(type(item['unavailable_reason']) is str and bool(item['unavailable_reason']),
                    f'{name}: missing unavailable reason')
        else:
            require(item['unavailable_reason'] is None and type(item['value']) in types,
                    f'{name}: wrong type')
            if types != (str,):
                require(math.isfinite(item['value']) and item['value'] >= 0,
                        f'{name}: invalid number')
            else:
                require(bool(item['value']), f'{name}: empty string')

def evaluate(campaign, input_sha256):
    report = {'schema_version': 1, 'evaluator_version': VERSION,
              'input_sha256': input_sha256, 'status': 'INVALID_EVIDENCE',
              'reason': '', 'workloads': {}}
    try:
        keys(campaign, ('schema_version', 'campaign_id', 'manifest', 'blocks', 'attempts', 'campaign_mode'), 'campaign')
        require(type(campaign['schema_version']) is int and campaign['schema_version'] == 1,
                'unsupported campaign schema; legacy collector evidence is not campaign v1')
        cid = campaign['campaign_id']
        require(type(cid) is str and bool(cid), 'empty campaign ID')
        manifest = campaign['manifest']
        keys(manifest, ('sources', 'profiles', 'workloads', 'host_id', 'cpu', 'build_provenance'), 'manifest')
        mode = campaign['campaign_mode']
        require(mode in ('comparison', 'aa_control'), 'unknown campaign mode')
        keys(manifest['build_provenance'], SIDES, 'build provenance')
        keys(manifest['sources'], SIDES, 'sources')
        keys(manifest['profiles'], SIDES, 'profiles')
        for side in SIDES:
            require(sha(manifest['sources'][side], 40), 'invalid source SHA')
            profile = manifest['profiles'][side]
            require(type(profile) is dict and bool(profile) and
                    all(type(k) is str and bool(k) and type(v) is str and bool(v) for k, v in profile.items()),
                    'profile must contain resolved string settings')
        require(manifest['profiles']['base'] == manifest['profiles']['candidate'], 'profile mismatch')
        require((manifest['sources']['base'] != manifest['sources']['candidate']) == (mode == 'comparison'),
                'source SHA condition disagrees with campaign mode')
        for side in SIDES:
            build = manifest['build_provenance'][side]
            keys(build, ('build_instance_id', 'source_sha', 'profile', 'build_log_sha256'), 'build provenance')
            require(type(build['build_instance_id']) is str and bool(build['build_instance_id']) and
                    build['source_sha'] == manifest['sources'][side] and
                    build['profile'] == manifest['profiles'][side] and sha(build['build_log_sha256'], 64),
                    'invalid build provenance')
        require(manifest['build_provenance']['base']['build_instance_id'] !=
                manifest['build_provenance']['candidate']['build_instance_id'], 'build instance ID reused')
        require(type(manifest['host_id']) is str and bool(manifest['host_id']), 'missing host identity')
        require(type(manifest['cpu']) is int and manifest['cpu'] >= 0, 'invalid CPU')
        workloads = manifest['workloads']
        require(type(workloads) is dict and bool(workloads), 'missing workloads')
        for name, spec in workloads.items():
            require(type(name) is str and bool(name), 'invalid workload name')
            keys(spec, ('binary_sha256', 'fixture_sha256', 'timer', 'expected'), 'workload')
            keys(spec['binary_sha256'], SIDES, 'binary hashes')
            require(all(sha(v, 64) for v in spec['binary_sha256'].values()), 'invalid binary SHA')
            keys(spec['fixture_sha256'], SIDES, 'fixtures')
            for fixtures in spec['fixture_sha256'].values():
                require(type(fixtures) is dict and bool(fixtures) and
                        all(type(k) is str and bool(k) and sha(v, 64) for k, v in fixtures.items()),
                        'invalid fixture hashes')
            require(spec['fixture_sha256']['base'] == spec['fixture_sha256']['candidate'], 'fixture mismatch')
            keys(spec['timer'], ('id', 'unit', 'scope'), 'timer')
            require(spec['timer']['unit'] == 'ms' and all(type(v) is str and bool(v)
                    for v in spec['timer'].values()), 'invalid timer')
            require(type(spec['expected']) is dict and bool(spec['expected']) and
                    all(type(k) is str and bool(k) and type(v) is int and v >= 0 for k,v in spec['expected'].items()),
                    'invalid expected correctness')
        blocks = campaign['blocks']
        require(type(blocks) is list and len(blocks) == 2, 'exactly two blocks required')
        orders = {}
        for block in blocks:
            keys(block, ('block_id', 'order'), 'block')
            require(type(block['block_id']) is str and bool(block['block_id']) and
                    block['block_id'] not in orders, 'duplicate/invalid block ID')
            require(block['order'] in ('AB', 'BA'), 'invalid order')
            orders[block['block_id']] = block['order']
        require(set(orders.values()) == {'AB', 'BA'}, 'both AB and BA blocks required')
        identity = digest(manifest)
        report.update(campaign_id=cid, campaign_mode=mode, manifest=manifest, manifest_sha256=identity, blocks=blocks)
        require(type(campaign['attempts']) is list, 'attempts must be list')
        events = {}
        failures = []
        for event in campaign['attempts']:
            keys(event, ('campaign_id', 'manifest_sha256', 'block_id', 'workload', 'sequence',
                         'phase', 'pair_index', 'side', 'elapsed_ms', 'exit_code', 'timed_out',
                         'correctness', 'host_before', 'host_after'), 'attempt')
            require(event['campaign_id'] == cid and event['manifest_sha256'] == identity, 'identity mismatch')
            bid, name, seq = event['block_id'], event['workload'], event['sequence']
            require(type(bid) is str and bid in orders and type(name) is str and name in workloads,
                    'unknown block/workload')
            require(type(seq) is int and 0 <= seq < 20, 'invalid sequence index')
            key = (bid, name, seq)
            require(key not in events, 'duplicate sequence index')
            sides = SIDES if orders[bid] == 'AB' else SIDES[::-1]
            require(event['side'] == sides[seq % 2], 'wrong adjacent side order')
            require(event['phase'] == ('warmup' if seq < 2 else 'sample'), 'wrong phase')
            pair = event['pair_index']
            require(pair is None if seq < 2 else type(pair) is int and pair == (seq - 2)//2,
                    'invalid pair index')
            elapsed = event['elapsed_ms']
            require(type(elapsed) in (int, float) and math.isfinite(elapsed) and elapsed > 0,
                    'invalid elapsed time')
            require(type(event['exit_code']) is int and type(event['timed_out']) is bool,
                    'malformed process result')
            correctness = event['correctness']
            keys(correctness, ('status', 'observed'), 'correctness')
            require(correctness['status'] in ('OK', 'FAIL') and type(correctness['observed']) is dict,
                    'malformed correctness')
            expected = workloads[name]['expected']
            require(set(correctness['observed']) == set(expected) and
                    all(type(v) is int and v >= 0 for v in correctness['observed'].values()),
                    'malformed correctness observations')
            telemetry(event['host_before']); telemetry(event['host_after'])
            if event['exit_code'] != 0 or event['timed_out'] or correctness['status'] != 'OK' or correctness['observed'] != expected:
                failures.append(f'{bid}/{name}/{seq}: process or correctness failure')
            events[key] = event
        report['telemetry'] = [dict(block_id=k[0], workload=k[1], sequence=k[2],
                                    host_before=e['host_before'], host_after=e['host_after'])
                               for k,e in sorted(events.items())]
        if failures:
            report.update(status='CORRECTNESS_FAILURE', reason='; '.join(sorted(failures)))
            return report
        if len(events) != len(workloads) * 40:
            report.update(status='INCOMPLETE', reason='missing warmup or sample sequence indices')
            return report
        for name in sorted(workloads):
            result = {'blocks': {}}
            for bid in sorted(orders):
                samples = [events[bid, name, i] for i in range(2, 20)]
                side_stats = {}
                for side in SIDES:
                    times = [e['elapsed_ms'] for e in samples if e['side'] == side]
                    side_stats[side] = {'median_ms': statistics.median(times),
                                       'cov_percent': statistics.pstdev(times)/statistics.fmean(times)*100}
                ratios = []
                for i in range(0, 18, 2):
                    pair = {e['side']: e['elapsed_ms'] for e in samples[i:i+2]}
                    ratios.append(math.log(pair['candidate']) - math.log(pair['base']))
                result['blocks'][bid] = dict(order=orders[bid], sides=side_stats,
                        paired_log_ratios=ratios, median_log_ratio=statistics.median(ratios),
                        mean_log_ratio=statistics.fmean(ratios))
            by_order = {b['order']: b for b in result['blocks'].values()}
            result['order_effect_log_ratio'] = by_order['AB']['mean_log_ratio'] - by_order['BA']['mean_log_ratio']
            report['workloads'][name] = result
        report.update(status='COMPLETE_VALID', reason='complete valid descriptive evidence; no timing verdict')
    except (Invalid, TypeError, OverflowError, ValueError) as error:
        report.update(status='INVALID_EVIDENCE', reason=str(error), workloads={})
    return report

def unique_object(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise Invalid(f'duplicate JSON key: {key}')
        obj[key] = value
    return obj

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign', type=Path)
    args = parser.parse_args()
    try:
        raw = args.campaign.read_bytes()
        campaign = json.loads(raw, object_pairs_hook=unique_object,
                              parse_constant=lambda v: (_ for _ in ()).throw(Invalid('nonfinite JSON constant')))
    except (OSError, ValueError, UnicodeError) as error:
        print(f'evaluate-paired-campaign: {error}', file=sys.stderr)
        return 2
    result = evaluate(campaign, hashlib.sha256(raw).hexdigest())
    print(json.dumps(result, sort_keys=True, indent=2, allow_nan=False))
    return {'COMPLETE_VALID': 0, 'CORRECTNESS_FAILURE': 1,
            'INVALID_EVIDENCE': 2, 'INCOMPLETE': 3}[result['status']]

if __name__ == '__main__':
    sys.exit(main())
