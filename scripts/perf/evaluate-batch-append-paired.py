#!/usr/bin/env python3
"""Evaluate paired batch-append campaigns offline against the #2036 protocol.

One observation is a process-level append-plus-reset time per side. For each
case this reports raw pairs, per-side median and population CoV, paired
candidate/baseline ratios and ns/call savings, the AB-BA order effect in
percentage points, and 95% intervals from a stratified bootstrap of 10,000
complete-pair resamples within the AB/BA strata. Campaigns are never pooled.

An A/A collection qualifies a case when, in each stratum, the median
|ratio - 1| is at most 2% and the order effect is at most 2 pp. A comparison
case meets its threshold only when it is complete, its A/A collection
qualified the same case, and the threshold holds; otherwise it is diagnostic.
"""
import argparse
import json
from pathlib import Path
import random
import runpy
import statistics
import sys

HERE = Path(__file__).resolve().parent
PLAN = runpy.run_path(str(HERE / 'prepare-batch-append-campaign.py'))
SEED = 2036
RESAMPLES = 10000
# The #2036 evidence was captured by the pre-check-in copy of this runner,
# which wrote the same journal under an issue-specific manifest schema.
SCHEMAS = ('wirelog.batch-append-paired-run.v1', 'wirelog.issue-2036.paired-run.v1')
AA_MAX_DEVIATION_PERCENT = 2.0
AA_MAX_ORDER_EFFECT_PP = 2.0
LARGE_BATCH_MAX_UPPER_RATIO = 1.03
SINGLE_MIN_GAIN_PERCENT = 10.0
SINGLE_MIN_SAVINGS_NS = 25.0


class EvaluationError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise EvaluationError(message)


def load(collection):
    collection = Path(collection)
    manifest = json.loads((collection / 'manifest.json').read_text(encoding='utf-8'))
    status = json.loads((collection / 'status.json').read_text(encoding='utf-8'))
    require(manifest.get('schema') in SCHEMAS, f'{collection}: unknown manifest schema')
    rows = {}
    for number, line in enumerate((collection / 'journal.jsonl').read_text(
            encoding='utf-8').splitlines(), 1):
        record = json.loads(line)
        event = record.get('event')
        require(event in ('started', 'result'), f'journal line {number}: unknown event')
        slot = rows.setdefault(record['command_index'], {})
        require(event not in slot, f'journal line {number}: duplicate {event} record')
        slot[event] = record
    return manifest, status, rows


def check_schedule(manifest, rows):
    """Every journaled command matches the frozen schedule and its binary."""
    expected = PLAN['schedule'](manifest['seed'])
    for index, record in rows.items():
        require('started' in record, f'command {index}: result without started record')
        row = record['started']['command_row']
        require(0 <= index < len(expected), f'command {index}: outside the schedule')
        launch = expected[index]
        require(all(row[key] == launch[key] for key in ('case', 'pair_id', 'pair_order',
                                                        'pair_index', 'side')),
                f'command {index}: does not match the frozen schedule')
        require(row['campaign_index'] == manifest['campaign_index'],
                f'command {index}: campaign index mismatch')
        require(row['iterations'] == manifest['iteration_counts'][row['case']],
                f'command {index}: iteration count mismatch')
        require(row['executable_sha256'] == manifest['binaries'][row['side']]['sha256'],
                f'command {index}: executable hash mismatch')


def observation(started, result):
    row = started['command_row']
    parsed = (result or {}).get('parsed') or {}
    ok = (result is not None and result.get('exit_code') == 0 and not result.get('timed_out')
          and parsed.get('correctness', {}).get('status') == 'OK'
          and parsed.get('iterations') == row['iterations'])
    eligibility = (result or {}).get('host_eligibility') or {}
    total = reset = None
    if ok:
        total = (parsed['append_total_ns'] + parsed['reset_total_ns']) / row['iterations']
        reset = parsed['reset_total_ns'] / row['iterations']
    return dict(case=row['case'], pair_id=row['pair_id'], order=row['pair_order'],
                side=row['side'], ok=ok, eligible=bool(eligibility.get('eligible')),
                psi=eligibility.get('cpu_psi_some_total_over_wall'),
                diagnostics=eligibility.get('diagnostics'), ns_per_call=total,
                reset_ns_per_call=reset,
                frequency_khz=[((result or {}).get(k) or {}).get('frequency_khz')
                               for k in ('host_before', 'host_after')])


def cov(values):
    mean = statistics.fmean(values)
    return statistics.pstdev(values) / mean * 100 if mean else float('nan')


def stratified_ci(strata, statistic, rng):
    draws = []
    for _ in range(RESAMPLES):
        sample = []
        for pairs in strata:
            sample.extend(rng.choice(pairs) for _ in pairs)
        draws.append(statistic(sample))
    draws.sort()
    return [draws[int(0.025 * RESAMPLES)], draws[int(0.975 * RESAMPLES) - 1]]


def summarize(campaign, case, items):
    rng = random.Random(f'{SEED}:{campaign}:{case}')
    strata = {order: [p for p in items if p['order'] == order] for order in PLAN['PAIR_ORDERS']}
    populated = [pairs for pairs in strata.values() if pairs]
    gain = {order: (1 - statistics.median(p['ratio'] for p in pairs)) * 100
            for order, pairs in strata.items() if pairs}
    complete = all(len(strata[o]) == PLAN['PAIRS_PER_ORDER'] for o in PLAN['PAIR_ORDERS'])
    return dict(
        pairs=len(items), pairs_by_order={k: len(v) for k, v in strata.items()},
        complete=complete,
        raw=[{k: p[k] for k in ('pair_id', 'order', 'base', 'candidate', 'ratio', 'psi',
                                'base_reset', 'candidate_reset')} for p in items],
        base_median_ns=statistics.median(p['base'] for p in items),
        candidate_median_ns=statistics.median(p['candidate'] for p in items),
        base_cov_percent=cov([p['base'] for p in items]),
        candidate_cov_percent=cov([p['candidate'] for p in items]),
        base_reset_median_ns=statistics.median(p['base_reset'] for p in items),
        candidate_reset_median_ns=statistics.median(p['candidate_reset'] for p in items),
        median_ratio=statistics.median(p['ratio'] for p in items),
        median_savings_ns=statistics.median(p['savings'] for p in items),
        median_gain_percent_by_order=gain,
        order_effect_pp=(gain['AB'] - gain['BA']) if len(gain) == 2 else None,
        aa_median_abs_deviation_by_order={
            order: statistics.median(abs(p['ratio'] - 1) for p in pairs) * 100
            for order, pairs in strata.items() if pairs},
        max_psi=max(max(p['psi']) for p in items),
        ratio_ci95=stratified_ci(
            populated, lambda s: statistics.median(p['ratio'] for p in s), rng),
        savings_ci95_ns=stratified_ci(
            populated, lambda s: statistics.median(p['savings'] for p in s), rng))


def analyze(collection):
    manifest, status, rows = load(collection)
    check_schedule(manifest, rows)
    pairs = {}
    for index in sorted(rows):
        item = observation(rows[index]['started'], rows[index].get('result'))
        pairs.setdefault((item['case'], item['pair_id']), {})[item['side']] = item
    campaign = manifest['campaign_index']
    grouped, excluded = {}, []
    for (case, pair_id), sides in sorted(pairs.items()):
        base, cand = sides.get('base'), sides.get('candidate')
        if not (base and cand and base['ok'] and cand['ok']
                and base['eligible'] and cand['eligible']):
            excluded.append(dict(case=case, pair_id=pair_id, sides={
                side: item and {k: item[k] for k in ('ok', 'eligible', 'psi', 'diagnostics')}
                for side, item in (('base', base), ('candidate', cand))}))
            continue
        grouped.setdefault(case, []).append(dict(
            order=base['order'], pair_id=pair_id, base=base['ns_per_call'],
            candidate=cand['ns_per_call'], ratio=cand['ns_per_call'] / base['ns_per_call'],
            savings=base['ns_per_call'] - cand['ns_per_call'], psi=[base['psi'], cand['psi']],
            base_reset=base['reset_ns_per_call'], candidate_reset=cand['reset_ns_per_call']))
    return dict(collection=str(Path(collection).resolve()), schema=manifest['schema'],
                capture_status=status.get('status'),
                launches=status.get('benchmark_launches_performed'),
                planned=len(PLAN['schedule'](manifest['seed'])), campaign=campaign,
                seed=manifest['seed'], bootstrap_seed=SEED, resamples=RESAMPLES,
                binaries={side: {k: v for k, v in manifest['binaries'][side].items()
                                 if k in ('label', 'sha256', 'commit', 'tree')}
                          for side in ('base', 'candidate')},
                calibration_sha256=manifest['calibration_sha256'], cpu=manifest['cpu'],
                excluded_pairs=excluded,
                cases={case: summarize(campaign, case, items)
                       for case, items in sorted(grouped.items())})


def qualify_aa(report):
    """Per-case A/A qualification; both sides must be the same binary."""
    same = report['binaries']['base']['sha256'] == report['binaries']['candidate']['sha256']
    result = {}
    for case in PLAN['CASES']:
        entry = report['cases'].get(case)
        reasons = []
        if not same:
            reasons.append('A/A sides are different binaries')
        if report['capture_status'] != 'complete_capture':
            reasons.append(f"capture status {report['capture_status']}")
        if entry is None or not entry['complete']:
            reasons.append('incomplete or ineligible pairs')
        else:
            for order, value in entry['aa_median_abs_deviation_by_order'].items():
                if value > AA_MAX_DEVIATION_PERCENT:
                    reasons.append(f'{order} median |ratio-1| {value:.3f}% > 2%')
            if abs(entry['order_effect_pp']) > AA_MAX_ORDER_EFFECT_PP:
                reasons.append(f"order effect {entry['order_effect_pp']:.3f} pp > 2 pp")
        result[case] = dict(qualified=not reasons, reasons=reasons)
    return result


def decide(report, aa_report):
    """Threshold outcomes; a case without a matching qualified A/A is diagnostic."""
    aa = qualify_aa(aa_report) if aa_report else None
    if aa_report:
        if aa_report['binaries']['base']['sha256'] != report['binaries']['base']['sha256']:
            aa = {case: dict(qualified=False, reasons=['A/A binary is not the comparison base'])
                  for case in PLAN['CASES']}
        elif (aa_report['calibration_sha256'], aa_report['cpu']) \
                != (report['calibration_sha256'], report['cpu']):
            aa = {case: dict(qualified=False,
                             reasons=['A/A calibration or CPU differs from the comparison'])
                  for case in PLAN['CASES']}
    decisions = {}
    for case in PLAN['CASES']:
        entry = report['cases'].get(case)
        checks = dict(complete=bool(entry and entry['complete']
                                    and report['capture_status'] == 'complete_capture'),
                      aa_qualified=bool(aa and aa[case]['qualified']))
        if entry is not None and case == '1x1':
            checks.update(
                improvement_ge_10pct=(1 - entry['median_ratio']) * 100 >= SINGLE_MIN_GAIN_PERCENT,
                savings_ge_25ns=entry['median_savings_ns'] >= SINGLE_MIN_SAVINGS_NS,
                ci_excludes_no_improvement=entry['ratio_ci95'][1] < 1.0)
        elif entry is not None:
            checks.update(ratio_upper_ci_le_1_03=(
                entry['ratio_ci95'][1] <= LARGE_BATCH_MAX_UPPER_RATIO))
        if not checks['complete'] or not checks['aa_qualified']:
            outcome = 'diagnostic'
        else:
            outcome = 'meets' if all(checks.values()) else 'does_not_meet'
        decisions[case] = dict(outcome=outcome, checks=checks,
                               aa=aa[case] if aa else dict(qualified=False,
                                                           reasons=['no A/A collection']))
    return decisions


def line(case, entry):
    order = entry['order_effect_pp']
    return (f"{case:6s} n={entry['pairs']} {entry['pairs_by_order']} "
            f"base={entry['base_median_ns']:.2f}ns(cov {entry['base_cov_percent']:.2f}%) "
            f"cand={entry['candidate_median_ns']:.2f}ns(cov {entry['candidate_cov_percent']:.2f}%) "
            f"ratio={entry['median_ratio']:.4f} "
            f"CI[{entry['ratio_ci95'][0]:.4f},{entry['ratio_ci95'][1]:.4f}] "
            f"save={entry['median_savings_ns']:.2f}ns "
            f"CI[{entry['savings_ci95_ns'][0]:.2f},{entry['savings_ci95_ns'][1]:.2f}] "
            f"order={'n/a' if order is None else f'{order:.3f}'}pp "
            f"maxPSI={entry['max_psi']:.4f}")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('collection')
    parser.add_argument('--mode', choices=('aa', 'comparison'), required=True)
    parser.add_argument('--aa', help='A/A collection that qualifies the comparison base')
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args(argv)
    try:
        report = analyze(args.collection)
        if args.mode == 'aa':
            report['aa_qualification'] = qualify_aa(report)
        else:
            report['decisions'] = decide(report, analyze(args.aa) if args.aa else None)
    except (OSError, KeyError, TypeError, ValueError) as error:
        print(f'evaluate-batch-append-paired: {error}', file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(report, indent=1, sort_keys=True))
        return 0
    print(f"{report['collection']} capture={report['capture_status']} "
          f"launches={report['launches']}/{report['planned']} campaign={report['campaign']} "
          f"excluded_pairs={len(report['excluded_pairs'])}")
    for case, entry in report['cases'].items():
        text = line(case, entry)
        if args.mode == 'aa':
            qualification = report['aa_qualification'][case]
            text += f" aa_qualified={qualification['qualified']} {qualification['reasons']}"
        else:
            decision = report['decisions'][case]
            text += f" outcome={decision['outcome']}"
            if decision['outcome'] != 'meets':
                failed = [k for k, v in decision['checks'].items() if not v]
                text += f' failed={failed} aa={decision["aa"]["reasons"]}'
        print(text)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
