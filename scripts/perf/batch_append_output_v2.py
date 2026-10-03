"""Strict parser for one successful batch-append benchmark-v2 process."""
import math


class OutputError(ValueError):
    pass


CONTRACT = 'wirelog.batch-append-benchmark.v2'
SUCCESS = {
    'denied': '0',
    'row_count_check': 'OK',
    'capacity_check': 'OK',
    'value_check': 'OK',
    'distinct_input_probe': 'OK',
    'status': 'OK',
}
CASES = {
    '1x1': dict(columns=1, rows_per_call=1, capacity=512),
    '1x256': dict(columns=1, rows_per_call=256, capacity=512),
    '32x256': dict(columns=32, rows_per_call=256, capacity=512),
}
FIELDS = {
    'bench_batch_append': {'contract', 'reset', 'input', 'governor', 'reserved_capacity'},
    'sample': {'contract', 'case', 'index', 'iterations', 'append_total_ns',
               'reset_total_ns', 'append_ns_per_call', 'reset_ns_per_call', *SUCCESS},
    'summary': {'contract', 'case', 'median_append_ns_per_call',
                'min_append_ns_per_call', 'max_append_ns_per_call',
                'cov_append_percent', 'mean_reset_ns_per_call'},
    'case': {'contract', 'name', 'columns', 'rows_per_call', 'capacity',
             'iterations', 'samples', 'warmups', *SUCCESS},
}


def parse_record(line, expected_kind):
    fields = line.split('\t')
    if not fields or fields[0] != expected_kind:
        raise OutputError(f'expected {expected_kind} record')
    values = {}
    for field in fields[1:]:
        key, separator, value = field.partition('=')
        if not separator or not key or key in values:
            raise OutputError(f'malformed or duplicate field in {expected_kind} record')
        values[key] = value
    if set(values) != FIELDS[expected_kind]:
        raise OutputError(f'{expected_kind} record fields do not match output schema v2')
    if values.get('contract') != CONTRACT:
        raise OutputError(f'{expected_kind} record has unsupported contract')
    return values


def integer(value, label, minimum=0):
    if not value.isascii() or not value.isdecimal():
        raise OutputError(f'{label} must be a decimal integer')
    parsed = int(value)
    if parsed < minimum:
        raise OutputError(f'{label} is below its minimum')
    return parsed


def finite_number(value, label):
    try:
        parsed = float(value)
    except ValueError as error:
        raise OutputError(f'{label} must be numeric') from error
    if not math.isfinite(parsed) or parsed < 0:
        raise OutputError(f'{label} must be finite and non-negative')
    return parsed


def require_success(values, label):
    for key, expected in SUCCESS.items():
        if values.get(key) != expected:
            raise OutputError(f'{label} did not report {key}={expected}')


def parse_output(stdout, case_name, iterations, warmups=2, samples=1):
    if type(stdout) is not bytes or not stdout.endswith(b'\n'):
        raise OutputError('benchmark stdout must be complete newline-terminated bytes')
    try:
        text = stdout.decode('utf-8', errors='strict')
    except UnicodeDecodeError as error:
        raise OutputError('benchmark stdout is not UTF-8') from error
    lines = text.splitlines()
    if len(lines) != 4 or any(not line for line in lines):
        raise OutputError('expected exactly one header, sample, summary, and case record')
    header, sample, summary, case = [
        parse_record(line, kind) for line, kind in zip(
            lines, ('bench_batch_append', 'sample', 'summary', 'case'))]
    if header != dict(contract=CONTRACT, reset='nrows-only-before-each-call',
                      input='disjoint', governor='off', reserved_capacity='512'):
        raise OutputError('header does not match required benchmark-v2 configuration')
    if case_name not in CASES or type(iterations) is not int or iterations <= 0:
        raise OutputError('invalid expected case or iteration count')
    if sample['case'] != case_name or summary['case'] != case_name \
            or case['name'] != case_name:
        raise OutputError('benchmark case records do not match requested case')
    expected_case = CASES[case_name]
    for key, value in expected_case.items():
        if integer(case[key], f'case.{key}', 1) != value:
            raise OutputError(f'case.{key} does not match required dimensions')
    expected_counts = dict(iterations=iterations, samples=samples, warmups=warmups)
    for key, value in expected_counts.items():
        if integer(case[key], f'case.{key}', 1) != value:
            raise OutputError(f'case.{key} does not match requested process options')
    if integer(sample['index'], 'sample.index') != 0 \
            or integer(sample['iterations'], 'sample.iterations', 1) != iterations:
        raise OutputError('sample index or iterations do not match requested process options')
    require_success(sample, 'sample')
    require_success(case, 'case')
    append_total_ns = integer(sample['append_total_ns'], 'sample.append_total_ns', 1)
    reset_total_ns = integer(sample['reset_total_ns'], 'sample.reset_total_ns', 1)
    append_per_call = finite_number(sample['append_ns_per_call'], 'sample.append_ns_per_call')
    reset_per_call = finite_number(sample['reset_ns_per_call'], 'sample.reset_ns_per_call')
    summary_values = {key: finite_number(summary[key], f'summary.{key}') for key in (
        'median_append_ns_per_call', 'min_append_ns_per_call',
        'max_append_ns_per_call', 'cov_append_percent', 'mean_reset_ns_per_call')}
    if summary_values['min_append_ns_per_call'] > summary_values['median_append_ns_per_call'] \
            or summary_values['median_append_ns_per_call'] > summary_values['max_append_ns_per_call']:
        raise OutputError('summary min/median/max order is invalid')
    if any(abs(summary_values[key] - append_per_call) > 0.0005 for key in (
            'median_append_ns_per_call', 'min_append_ns_per_call',
            'max_append_ns_per_call')) \
            or abs(summary_values['mean_reset_ns_per_call'] - reset_per_call) > 0.0005 \
            or summary_values['cov_append_percent'] != 0.0:
        raise OutputError('one-sample summary does not match its sample record')
    return dict(case=case_name, iterations=iterations, append_total_ns=append_total_ns,
                reset_total_ns=reset_total_ns, append_ns_per_call=append_per_call,
                reset_ns_per_call=reset_per_call, summary=summary_values,
                correctness=dict(SUCCESS))
