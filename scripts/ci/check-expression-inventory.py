#!/usr/bin/env python3
"""Keep #2299's finite expression inventory aligned with decoder admission."""
import importlib.util
from pathlib import Path
import re
import sys


DIGEST_TAGS = {
    'WL_PLAN_EXPR_ARITH_' + algorithm + suffix
    for algorithm in ('MD5', 'SHA1', 'SHA256', 'SHA512')
    for suffix in ('', '_S')
}


HMAC_TAGS = {'WL_PLAN_EXPR_ARITH_HMAC_SHA256' + suffix
             for suffix in ('', '_SS', '_SI', '_IS')}


NUMERIC_UUID_TAGS = {'WL_PLAN_EXPR_ARITH_UUID4'} | {
    'WL_PLAN_EXPR_ARITH_UUID5' + suffix for suffix in ('', '_SS', '_SI', '_IS')}


def function_body(source: str, name: str) -> str:
    """Use the existing C masker so helper cases/comments cannot add support."""
    path = Path(__file__).with_name('ownership_doc_anchors.py')
    spec = importlib.util.spec_from_file_location('expression_c_mask', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    masked = module._mask_comments_and_strings(source)
    start = re.search(r'\b' + re.escape(name) + r'\s*\(', masked)
    if start is None:
        raise ValueError(f'missing function {name}')
    opening = masked.index('{', start.end())
    depth = 1
    for pos in range(opening + 1, len(masked)):
        depth += (masked[pos] == '{') - (masked[pos] == '}')
        if not depth:
            return masked[opening + 1:pos]
    raise ValueError(f'unclosed function {name}')


def check(root: Path) -> tuple[list[str], int]:
    header = (root / 'wirelog/exec_plan.h').read_text(encoding='utf-8')
    enum = header[header.index('WL_PLAN_EXPR_VAR ='):
                  header.index('} wl_plan_expr_tag_t;')]
    tags = set(re.findall(r'\b(WL_PLAN_EXPR_[A-Z0-9_]+)\s*(?==)', enum))
    source = (root / 'wirelog/columnar/expr.c').read_text(encoding='utf-8')
    raw = function_body(source, 'wl_columnar_expr_eval_run_ctx')
    decoder = function_body(source, 'wl_columnar_expr_attempt_decode')
    raw_tags = set(re.findall(r'case (WL_PLAN_EXPR_[A-Z0-9_]+)', raw))
    decoded = set(re.findall(r'case (WL_PLAN_EXPR_[A-Z0-9_]+)', decoder))
    scalar = function_body(source, 'wl_columnar_expr_scalar_decode')
    scalar_tags = set(re.findall(r'case (WL_PLAN_EXPR_[A-Z0-9_]+)', scalar))
    delegates = re.search(r'default:\s*return\s+'
                          r'wl_columnar_expr_attempt_decode\s*\(', scalar)
    ledger = (root / 'docs/implementation/2299/EXPRESSION_ACCEPTANCE.md').read_text(encoding='utf-8')
    rows = re.findall(r'^\| `(WL_PLAN_EXPR_[A-Z0-9_]+)` \| ([a-z-]+) \| ([a-z-]+) \| ([a-z0-9-]+) \|$',
                      ledger, re.M)
    errors = []
    recorded = {row[0]: row[1:] for row in rows}
    if len(rows) != len(recorded):
        errors.append('duplicate tag rows')
    if set(recorded) != tags:
        errors.append(f'tag inventory differs: {sorted(tags ^ set(recorded))}')
    if decoded - raw_tags or scalar_tags - raw_tags:
        errors.append('decoder admits raw-rejected tags')
    if not delegates:
        errors.append('scalar numeric delegation changed; review admission inventory')
    for tag, state, scalar_state, qualification in rows:
        expected = 'legacy-rejected' if tag not in raw_tags else 'open'
        if tag in raw_tags and tag in decoded:
            start = decoder.index('case ' + tag + ':')
            end = decoder.index('break;', start)
            clause = decoder[start:end]
            gated = re.search(r'if\s*\(\s*!raw_empty_setup\s*\)\s*'
                              r'return\s+ENOTSUP\s*;', clause)
            expected = 'empty-only' if gated else 'existing-numeric'
        if state != expected:
            errors.append(f'{tag}: {state}, source requires review of {expected}')
        scalar_expected = ('legacy-rejected' if tag not in raw_tags else
                           'admitted' if tag in scalar_tags or
                           (delegates and expected == 'existing-numeric') else 'open')
        if scalar_state != scalar_expected:
            errors.append(f'{tag}: scalar {scalar_state}, expected {scalar_expected}')
        valid_qualification = (qualification == 'open' or
                               qualification == 'baseline-numeric' and
                               expected == 'existing-numeric' or
                               qualification in {'unit4-sessions', 'unit5-sessions', 'unit5-decimal', 'unit5-digests', 'unit6-builders', 'unit6-selectors', 'unit6-replace'} and
                               tag in scalar_tags and scalar_expected == 'admitted' or
                               qualification == 'unit7-digests' and tag in DIGEST_TAGS and
                               scalar_expected == 'admitted' or
                               qualification == 'unit7-hmac' and tag in HMAC_TAGS and
                               scalar_expected == 'admitted' or
                               qualification == 'unit7-uuid-numeric' and tag in NUMERIC_UUID_TAGS and
                               scalar_expected == 'admitted' or
                               qualification == 'unit7-uuid-rfc' and tag == 'WL_PLAN_EXPR_STR_FN_UUID5_RFC' and
                               scalar_expected == 'admitted' or
                               qualification == 'legacy-rejected' and tag not in raw_tags)
        if not valid_qualification:
            errors.append(f'{tag}: invalid host qualification {qualification}')
    return errors, len(tags)


def main() -> int:
    try:
        errors, count = check(Path(__file__).resolve().parents[2])
    except (ValueError, OSError) as exc:
        print(f'check-expression-inventory: FAIL: {exc}', file=sys.stderr)
        return 1
    if errors:
        print('check-expression-inventory: FAIL: ' + '; '.join(errors),
              file=sys.stderr)
        return 1
    print(f'check-expression-inventory: OK; {count} tags accounted for')
    return 0


if __name__ == '__main__':
    sys.exit(main())
