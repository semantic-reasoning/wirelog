#!/usr/bin/env python3
# test_inline_slot_types.py - per-slot types for inline compounds (#1037)
#
# Copyright (C) CleverPlant
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Usage: test_inline_slot_types.py <wirelog_cli_executable>
#
# The fact path has always round-tripped a symbol through an inline compound
# slot: `p(1, "aa", "bb").` against `.decl p(id: int64, lbl: pair/2 inline)`
# evaluates correctly.  The `.input` path could not express the same data,
# because the `.decl` grammar records one type per *declared* column and
# `type_name_to_column_type()` maps any `functor/arity` spelling to int64 --
# so io_ctx.c typed every inline slot int64 and the integer reader refused
# "aa".  The failure was loud rather than silent, which is the right failure,
# but the data was unloadable.
#
# `f(t1, t2)` now names each slot's type.  `f/arity` is unchanged and still
# means every slot is int64.
#
# The controls are the point.  A change that simply made the reader intern
# anything non-numeric would satisfy the positive case while destroying the
# declaration's authority, so:
#
#   * the old `pair/2` spelling must STILL refuse a symbol file -- that is
#     what proves the new behaviour comes from the declaration rather than
#     from the data,
#   * an int64 slot declared explicitly must still reject a symbol,
#   * and a compound-free relation must be untouched.
#
# `string` and `symbol` are two spellings of one slot type.  Every case
# above spells it `symbol`, so one case spells it `string` -- otherwise
# nothing in the tree would notice if that spelling stopped decoding.

import os
import subprocess
import sys
import tempfile

NEW = '''\
.decl p(id: int64, lbl: pair(symbol, symbol) inline)
.decl o(a: int64, b: symbol, c: symbol)
.input p(filename="%s")
.output o
o(a,b,c) :- p(a, pair(b,c)).
'''

OLD = '''\
.decl p(id: int64, lbl: pair/2 inline)
.decl o(a: int64, b: symbol, c: symbol)
.input p(filename="%s")
.output o
o(a,b,c) :- p(a, pair(b,c)).
'''

# Explicit int64 slots: a symbol file must still be refused.
TYPED_INT = '''\
.decl p(id: int64, lbl: pair(int64, int64) inline)
.decl o(a: int64, b: int64, c: int64)
.input p(filename="%s")
.output o
o(a,b,c) :- p(a, pair(b,c)).
'''

# Mixed slots, and the numeric one is second so a naive implementation that
# applied slot 0's type to the whole compound would fail here.
MIXED = '''\
.decl p(id: int64, lbl: pair(symbol, int64) inline)
.decl o(a: int64, b: symbol, c: int64)
.input p(filename="%s")
.output o
o(a,b,c) :- p(a, pair(b,c)).
'''

# The `string` spelling of a symbol slot, mixed the same way as above so a
# pass cannot come from every slot happening to be interned.
STRING_SPELLING = '''\
.decl p(id: int64, lbl: pair(string, int64) inline)
.decl o(a: int64, b: symbol, c: int64)
.input p(filename="%s")
.output o
o(a,b,c) :- p(a, pair(b,c)).
'''

PLAIN = '''\
.decl q(a: int64, s: symbol)
.decl r(a: int64, s: symbol)
.input q(filename="%s")
.output r
r(a,s) :- q(a,s).
'''


def run(exe, program, tmpdir, name, rows):
    csv = os.path.join(tmpdir, name + ".csv")
    with open(csv, "w", encoding="utf-8") as fh:
        fh.write(rows)
    dl = os.path.join(tmpdir, name + ".dl")
    with open(dl, "w", encoding="utf-8") as fh:
        fh.write(program % csv)
    env = dict(os.environ)
    env.pop("WL_LOG", None)
    return subprocess.run([exe, dl], capture_output=True, text=True,
                          timeout=60, env=env)


def main() -> int:
    if len(sys.argv) != 2:
        sys.stderr.write("usage: test_inline_slot_types.py <executable>\n")
        return 1
    exe = sys.argv[1]
    ok = True

    def fail(msg):
        nonlocal ok
        sys.stderr.write(msg.rstrip() + "\n")
        ok = False

    with tempfile.TemporaryDirectory() as td:
        # The point of the issue.
        r = run(exe, NEW, td, "new", "1\taa\tbb\n")
        if 'o(1, "aa", "bb")' not in r.stdout:
            fail("declared symbol slots: expected o(1, \"aa\", \"bb\"); "
                 "stdout=%r stderr=%r" % (r.stdout.strip(), r.stderr.strip()))

        # Control: the declaration decides, not the data.
        r = run(exe, OLD, td, "old", "1\taa\tbb\n")
        if r.returncode == 0:
            fail("pair/2 must still refuse a symbol file -- otherwise the "
                 "new behaviour is coming from the data, not the .decl")

        # Control: explicit int64 slots reject symbols.
        r = run(exe, TYPED_INT, td, "ti", "1\taa\tbb\n")
        if r.returncode == 0:
            fail("pair(int64, int64) must refuse a symbol file")

        # Control: /arity still loads integers.
        r = run(exe, OLD, td, "oldint", "1\t2\t3\n")
        if r.returncode != 0:
            fail("pair/2 must still load an all-integer file; stderr=%r"
                 % r.stderr.strip())

        # Per-slot, not per-compound.
        r = run(exe, MIXED, td, "mix", "1\taa\t7\n")
        if 'o(1, "aa", 7)' not in r.stdout:
            fail("mixed slots: expected o(1, \"aa\", 7); stdout=%r stderr=%r"
                 % (r.stdout.strip(), r.stderr.strip()))

        # `string` names the same slot type as `symbol`.
        r = run(exe, STRING_SPELLING, td, "str", "1\tcc\t9\n")
        if 'o(1, "cc", 9)' not in r.stdout:
            fail("string slot spelling: expected o(1, \"cc\", 9); "
                 "stdout=%r stderr=%r"
                 % (r.stdout.strip(), r.stderr.strip()))

        # Control: no compound anywhere, unchanged.
        r = run(exe, PLAIN, td, "plain", "1\tzz\n")
        if 'r(1, "zz")' not in r.stdout:
            fail("compound-free relation changed; stdout=%r" % r.stdout.strip())

    if not ok:
        return 1
    print("test_inline_slot_types: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
