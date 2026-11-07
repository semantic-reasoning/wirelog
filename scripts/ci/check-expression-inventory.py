#!/usr/bin/env python3
"""Keep #2299's finite expression inventory aligned with decoder admission."""
import importlib.util
from pathlib import Path
import re
import sys


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
    ledger = (root / 'docs/implementation/2299/EXPRESSION_ACCEPTANCE.md').read_text(encoding='utf-8')
    rows = re.findall(r'^\| `(WL_PLAN_EXPR_[A-Z0-9_]+)` \| ([a-z-]+) \|$',
                      ledger, re.M)
    errors = []
    if len(rows) != len(dict(rows)):
        errors.append('duplicate tag rows')
    if set(dict(rows)) != tags:
        errors.append(f'tag inventory differs: {sorted(tags ^ set(dict(rows)))}')
    for tag, state in rows:
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
