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
# run through the CLI.

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
    print("%d failure(s)" % failures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
