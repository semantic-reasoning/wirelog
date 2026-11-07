#!/usr/bin/env python3
"""Mutate admission and ledger separately: stale inventories must fail closed."""
import importlib.util
from pathlib import Path
import tempfile

script = Path(__file__).with_name('check-expression-inventory.py')
spec = importlib.util.spec_from_file_location('expression_inventory', script)
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
root = script.resolve().parents[2]
paths = ['wirelog/exec_plan.h', 'wirelog/columnar/expr.c',
         'docs/implementation/2299/EXPRESSION_ACCEPTANCE.md']
with tempfile.TemporaryDirectory(prefix='expression-inventory-') as folder:
    fixture = Path(folder)
    for path in paths:
        target = fixture / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text((root / path).read_text(encoding='utf-8'), encoding='utf-8')
    source_path = fixture / paths[1]
    original = source_path.read_text(encoding='utf-8')
    assert gate.check(fixture) == ([], 84), 'unmodified inventory'
    source_path.write_text(original.replace(
        'if (!raw_empty_setup) return ENOTSUP;', '', 1), encoding='utf-8')
    assert gate.check(fixture)[0], 'ordinary string admission must be detected'
    begin = original.index('    case WL_PLAN_EXPR_CMP_STR_EQ:',
                           original.index('wl_columnar_expr_attempt_decode('))
    end = original.index('        break;', begin) + len('        break;')
    source_path.write_text(original[:begin] + original[end:], encoding='utf-8')
    assert gate.check(fixture)[0], 'removed decoder cases must be detected'
    source_path.write_text(original + '\nvoid unrelated(int x) { switch (x) {'
                           'case WL_PLAN_EXPR_AGG_AVG: break;} }\n', encoding='utf-8')
    assert not gate.check(fixture)[0], 'unrelated helper is not raw support'
    scalar_start = original.index('wl_columnar_expr_scalar_decode(')
    scalar_case = original.index('case WL_PLAN_EXPR_VAR_STRING:', scalar_start)
    scalar_end = original.index('case WL_PLAN_EXPR_CONST_STR:', scalar_case)
    source_path.write_text(original[:scalar_case] + original[scalar_end:], encoding='utf-8')
    assert gate.check(fixture)[0], 'removed scalar admission must be detected'
    source_path.write_text(original, encoding='utf-8')
    ledger_path = fixture / paths[2]
    ledger = ledger_path.read_text(encoding='utf-8')
    row = '| `WL_PLAN_EXPR_AGG_AVG` | legacy-rejected | legacy-rejected | legacy-rejected |\n'
    assert row in ledger
    ledger_path.write_text(ledger + row, encoding='utf-8')
    assert gate.check(fixture)[0], 'duplicate rows must fail'
    ledger_path.write_text(ledger.replace(row, ''), encoding='utf-8')
    assert gate.check(fixture)[0], 'missing tags must fail'
print('test-check-expression-inventory: all cases passed')
