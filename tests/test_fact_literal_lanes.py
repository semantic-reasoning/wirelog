#!/usr/bin/env python3
# test_fact_literal_lanes.py - inline fact literals against their lanes (#2231)
#
# Copyright (C) CleverPlant
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Usage: test_fact_literal_lanes.py <wirelog_cli_executable>
#
# An integer literal in a float column was stored as its raw bits, so
# `g(1).` against `.decl g(x: float)` read back as 4.9e-324 and `g(-3).`,
# whose bits are a NaN, failed to load.  It is now stored as the binary64
# value of the integer, as an `.input` file's `1` already was.  A string in
# a float column, and a float in an `inline` slot that is not float or in a
# `side` handle column, used to be stored as an intern id or raw bits; they
# are now parse errors.  The easy facade refuses float relations, so these
# run through the CLI.  An int32 column now refuses a value outside 32
# bits, in an inline fact and in an `.input` file.

import os
import subprocess
import sys
import tempfile

CASES = [
    # (name, program, expected stdout lines or None for a parse error)
    ("integer in a float column",
     ".decl g(x: float)\n.decl p(x: float)\n.output p\n"
     "g(1).\np(x) :- g(x).\n", ["p(1)"]),
    (".decl after the fact",
     "g(1).\n.decl g(x: float)\n.decl p(x: float)\n.output p\n"
     "p(x) :- g(x).\n", ["p(1)"]),
    ("negative integer in a float column",
     ".decl g(x: float)\n.decl p(x: float)\n.output p\n"
     "g(-3).\np(x) :- g(x).\n", ["p(-3)"]),
    ("integer in a float column under a comparison",
     ".decl g(x: float)\n.decl p(x: float)\n.output p\n"
     "g(1).\np(x) :- g(x), x > 0.5.\n", ["p(1)"]),
    ("integer in an inline float slot",
     ".decl g(id: int64, v: f(float, int64) inline)\n"
     ".decl p(a: int64, b: float, c: int64)\n.output p\n"
     "g(1, 2, 3).\np(a, b, c) :- g(a, f(b, c)).\n", ["p(1, 2, 3)"]),
    ("string in a float column", ".decl g(x: float)\n.decl p(x: float)\n"
     ".output p\ng(\"a\").\np(x) :- g(x).\n", None),
    ("float in an inline int64 slot",
     ".decl g(id: int64, v: f(int64, int64) inline)\n"
     "g(1, 2.5, 3).\n", None),
    ("float in an untyped inline slot",
     ".decl g(id: int64, v: f/2 inline)\ng(1, 2.5, 3).\n", None),
    ("float in a side handle column",
     ".decl g(id: int64, v: f/2)\ng(1, 2.5).\n", None),
    # Rule-head constants follow the same rules.
    ("integer head constant in a float column",
     ".decl s(x: int64)\ns(1).\n.decl o(x: float)\n.output o\n"
     "o(1) :- s(_).\n", ["o(1)"]),
    ("negative integer head constant in a float column",
     ".decl s(x: int64)\ns(1).\n.decl o(x: float)\n.output o\n"
     "o(-3) :- s(_).\n", ["o(-3)"]),
    ("integer head constant read by a comparison",
     ".decl s(x: int64)\ns(1).\n.decl o(x: float)\n.decl p(x: int64)\n"
     ".output p\no(1) :- s(_).\np(9) :- o(x), x > 0.5.\n", ["p(9)"]),
    ("integer head constant in an inline float slot",
     ".decl s(x: int64)\ns(1).\n"
     ".decl o(id: int64, v: f(float, int64) inline)\n"
     ".decl p(a: int64, b: float, c: int64)\n.output p\n"
     "o(1, 2, 3) :- s(_).\np(a, b, c) :- o(a, f(b, c)).\n",
     ["p(1, 2, 3)"]),
    ("int32 head constant outside 32 bits",
     ".decl s(x: int64)\ns(1).\n.decl o(x: int32)\n.output o\n"
     "o(5000000000) :- s(_).\n", None),
]

# (name, program with %s for the input file, file contents, expected stdout
# lines or None for a failed run)
INPUT_CASES = [
    ("int32 input value in range",
     ".decl g(x: int32)\n.decl p(x: int32)\n.input g(filename=\"%s\")\n"
     ".output p\np(x) :- g(x).\n", "2147483647\n-2147483648\n",
     ["p(-2147483648)", "p(2147483647)"]),
    ("int32 input value out of range",
     ".decl g(x: int32)\n.decl p(x: int32)\n.input g(filename=\"%s\")\n"
     ".output p\np(x) :- g(x).\n", "5000000000\n", None),
]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: test_fact_literal_lanes.py <wirelog_cli>",
              file=sys.stderr)
        return 2
    exe = sys.argv[1]
    failures = 0
    with tempfile.TemporaryDirectory() as td:
        for i, (name, program, want) in enumerate(CASES):
            dl = os.path.join(td, "case%d.dl" % i)
            with open(dl, "w", encoding="utf-8") as f:
                f.write(program)
            r = subprocess.run([exe, dl], capture_output=True, text=True,
                               encoding="utf-8", timeout=60, check=False)
            if want is None:
                ok = r.returncode != 0 and "Parse error" in r.stderr
            else:
                got = sorted(l for l in r.stdout.splitlines() if l.strip())
                ok = r.returncode == 0 and got == sorted(want)
            status = "PASS" if ok else "FAIL"
            print("  %s: %s" % (status, name))
            if not ok:
                failures += 1
                print("    rc=%d stdout=%r stderr=%r"
                      % (r.returncode, r.stdout, r.stderr))
        for i, (name, program, rows, want) in enumerate(INPUT_CASES):
            data = os.path.join(td, "input%d.csv" % i)
            with open(data, "w", encoding="utf-8") as f:
                f.write(rows)
            dl = os.path.join(td, "input%d.dl" % i)
            with open(dl, "w", encoding="utf-8") as f:
                f.write(program % data)
            r = subprocess.run([exe, dl], capture_output=True, text=True,
                               encoding="utf-8", timeout=60, check=False)
            got = sorted(l for l in r.stdout.splitlines() if l.strip())
            if want is None:
                ok = r.returncode != 0 and not got
            else:
                ok = r.returncode == 0 and got == sorted(want)
            print("  %s: %s" % ("PASS" if ok else "FAIL", name))
            if not ok:
                failures += 1
                print("    rc=%d stdout=%r stderr=%r"
                      % (r.returncode, r.stdout, r.stderr))
    print("%d failure(s)" % failures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
