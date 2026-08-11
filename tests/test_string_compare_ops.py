#!/usr/bin/env python3
# test_string_compare_ops.py - symbol ordering on the compiled path (#966)
#
# Copyright (C) CleverPlant
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Usage: test_string_compare_ops.py <wirelog_cli_executable>
#
# #962 made ordering comparisons on symbols lexicographic by emitting
# WL_PLAN_EXPR_CMP_STR_LT..GTE.  Those opcodes were absent from
# col_expr_compile's accepted set, so any predicate containing one hit
# `default: return NULL` and the whole expression fell back to the
# interpreter.  #966 adds them to the compiled evaluator.
#
# This is a performance change, so there is no failing-before assertion to
# make: the interpreter was already correct and the compiled path must agree
# with it exactly.  What this test guards is that agreement -- it pins the
# observable semantics of all six operators so a divergence between the two
# implementations cannot land silently.  That matters more than usual here:
# the sweep during #962 found no program anywhere in this tree that compares
# symbols, so before this file nothing at all would have caught one.
#
# Two cases are chosen specifically because a plausible wrong implementation
# gets them wrong:
#
#   "Zebra" < "apple"     byte order, not case-insensitive collation
#                         ('Z' is 0x5A, 'a' is 0x61)
#   "apple10" < "apple9"  lexicographic, not numeric
#
# and one because it is the pre-#962 behaviour:
#
#   comparing by intern id would order by *insertion*, so with the facts
#   below in this order an id comparison makes "apple" < "banana" true and
#   "Zebra" < "apple" false.  The expected set here says otherwise.

import os
import subprocess
import sys
import tempfile

PROGRAM = '''\
.decl W(s: symbol, t: symbol)
.decl LT(s: symbol, t: symbol)
.decl GT(s: symbol, t: symbol)
.decl LTE(s: symbol, t: symbol)
.decl GTE(s: symbol, t: symbol)
.decl EQ(s: symbol, t: symbol)
.decl NEQ(s: symbol, t: symbol)
.output LT
.output GT
.output LTE
.output GTE
.output EQ
.output NEQ
W("apple","banana"). W("banana","apple"). W("apple","apple").
W("Zebra","apple"). W("apple10","apple9").
LT(s,t)  :- W(s,t), s < t.
GT(s,t)  :- W(s,t), s > t.
LTE(s,t) :- W(s,t), s <= t.
GTE(s,t) :- W(s,t), s >= t.
EQ(s,t)  :- W(s,t), s = t.
NEQ(s,t) :- W(s,t), s != t.
'''

EXPECTED = sorted([
    'EQ("apple", "apple")',
    'GT("banana", "apple")',
    'GTE("apple", "apple")',
    'GTE("banana", "apple")',
    'LT("apple10", "apple9")',
    'LT("apple", "banana")',
    'LT("Zebra", "apple")',
    'LTE("apple10", "apple9")',
    'LTE("apple", "apple")',
    'LTE("apple", "banana")',
    'LTE("Zebra", "apple")',
    'NEQ("apple10", "apple9")',
    'NEQ("apple", "banana")',
    'NEQ("banana", "apple")',
    'NEQ("Zebra", "apple")',
])


def main() -> int:
    if len(sys.argv) != 2:
        sys.stderr.write("usage: test_string_compare_ops.py <executable>\n")
        return 1
    exe = sys.argv[1]

    env = dict(os.environ)
    for k in ("WL_LOG", "WL_LOG_FILE"):
        env.pop(k, None)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "str6.dl")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(PROGRAM)
        r = subprocess.run([exe, path], capture_output=True, text=True,
                           timeout=60, env=env)

    if r.returncode != 0:
        sys.stderr.write("exit %d; stderr=%r\n" % (r.returncode, r.stderr))
        return 1

    got = sorted(l.strip() for l in r.stdout.replace("\r\n", "\n").split("\n")
                 if l.strip())

    if got != EXPECTED:
        missing = [x for x in EXPECTED if x not in got]
        extra = [x for x in got if x not in EXPECTED]
        if missing:
            sys.stderr.write("missing rows: %r\n" % missing)
        if extra:
            sys.stderr.write("unexpected rows: %r\n" % extra)
        return 1

    print("test_string_compare_ops: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
