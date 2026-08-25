#!/usr/bin/env python3
# test_cli_parse_diagnostics.py - rejected programs explain themselves (#979)
#
# Copyright (C) CleverPlant
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Usage: test_cli_parse_diagnostics.py <wirelog_cli_executable>
#
# Every load failure used to reach the user as the bare two words "Parse
# error".  The rejection sites in wirelog/ir/program.c already compose good
# messages naming the relation, the count and the line, but they emit through
# WL_LOG(WL_LOG_SEC_PARSER, WL_LOG_ERROR, ...) and wl_log_thresholds defaults
# to WL_LOG_NONE (0).  The gate is (LVL) <= threshold and WL_LOG_ERROR is 1,
# so 1 <= 0 is false and the text was discarded unless the user had already
# set WL_LOG=PARSER:1 -- which requires knowing the answer to ask the
# question.  wirelog_parse_string() separately filled a 512-byte errbuf with
# the syntax error and let it go out of scope unread.
#
# So these cases assert on *stderr text with a scrubbed environment*.  WL_LOG
# and WL_LOG_FILE are removed from the child environment explicitly: inherited
# from a developer shell either would make every assertion below pass without
# the fix, which is the exact failure mode the issue describes.
#
# Two properties are asserted together throughout, and the second is the one
# that keeps the first honest:
#
#   1. the message names the thing that was rejected, and
#   2. the program is *still rejected* -- non-zero exit, nothing evaluated.
#
# Without (2) a "fix" that made the checks permissive and printed a warning
# would pass every text assertion here while silently reintroducing the
# wrong-answer classes those checks exist to stop (#973, #977, #920).

import os
import subprocess
import sys
import tempfile

# Rejections that must name their subject.  Each entry is
#   (label, program text, [substrings that must all appear in stderr])
# The substrings are deliberately the *identifiers from the program* rather
# than fixed prose, so rewording a diagnostic does not break the test but
# dropping the relation name does.
#
# Every identifier here is deliberately long and distinctive.  The first draft
# of this test used the natural short names from the issue -- a relation `t`
# and a variable `y` -- and the `t` case passed against the unfixed binary,
# because the pre-existing stderr "Parse error\nerror: execution failed"
# happens to contain a `t` (in "execution").  A one- or two-character needle
# cannot distinguish a real diagnostic from an incidental substring, so the
# assertion was vacuous in exactly the way this issue is about.  Keep these
# names long enough that an accidental match is not possible.
CASES = [
    (
        "fact arity",
        '.decl edge(a: int64, b: int64)\n.output edge\nedge(1).\n',
        ["edge"],
    ),
    (
        "rule head width",
        '.decl src(a: int64, b: int64)\n'
        '.decl narrow(a: int64)\n'
        '.output narrow\n'
        'src(1, 2).\n'
        'narrow(x, y) :- src(x, y).\n',
        ["narrow"],
    ),
    (
        "two aggregates in one head",
        '.decl val(g: int64, v: int64)\n'
        '.decl aggpair(g: int64, a: int64, b: int64)\n'
        '.output aggpair\n'
        'val(1, 5).  val(1, 9).\n'
        'aggpair(g, min(v), max(v)) :- val(g, v).\n',
        ["aggpair"],
    ),
    (
        "unsafe variable under negation",
        '.decl posrel(a: int64)\n'
        '.decl negrel(a: int64, b: int64)\n'
        '.decl outrel(a: int64)\n'
        '.output outrel\n'
        'posrel(1).\n'
        'outrel(x) :- posrel(x), !negrel(x, unboundvar).\n',
        ["unboundvar", "negrel"],
    ),
    (
        "__graph_metadata arity",
        '.decl __graph_metadata(a: int64, b: int64)\n'
        '.decl z(a: int64)\n'
        '.output z\n'
        'z(1).\n',
        ["__graph_metadata"],
    ),
    (
        "recursive count aggregate",
        '.decl plan_edge(x: int64, y: int64)\n'
        'plan_edge(1, 2).\n'
        '.decl plan_count(x: int64, n: int64)\n'
        'plan_count(1, 1).\n'
        'plan_count(y, count(n)) :- plan_count(x, n), plan_edge(x, y).\n',
        ["recursive aggregate", "count", "plan_count"],
    ),
    (
        "recursive sum aggregate",
        '.decl plan_sum_edge(x: int64, y: int64)\n'
        'plan_sum_edge(1, 2).\n'
        '.decl plan_sum(x: int64, n: int64)\n'
        'plan_sum(1, 1).\n'
        'plan_sum(y, sum(n)) :- plan_sum(x, n), plan_sum_edge(x, y).\n',
        ["recursive aggregate", "sum", "plan_sum"],
    ),
    (
        "recursive min aggregate same SCC",
        '.decl plan_edge(x: int64, y: int64)\n'
        'plan_edge(1, 2). plan_edge(2, 3).\n'
        '.decl plan_label(x: int64, l: int64)\n'
        'plan_label(x, min(x)) :- plan_edge(x, y).\n'
        'plan_label(y, min(y)) :- plan_edge(x, y).\n'
        '.decl plan_big(x: int64)\n'
        'plan_big(x) :- plan_label(x, l), l > 0.\n'
        'plan_label(x, min(9)) :- plan_big(x).\n',
        ["same stratum", "plan_label", "plan_big"],
    ),
]

# A syntactically malformed program.  This is the errbuf that
# wirelog_parse_string() filled and threw away, a different gap from the
# lowering stages above, so it gets its own case.
SYNTAX_CASE = '.decl e(a: int64, b: int64)\ne(1, 2)\n@@@ this is not datalog @@@\n'

# Control.  A valid program must keep exiting 0 and must not gain any
# diagnostic chatter on stderr.  Without this, a "fix" that unconditionally
# printed an error string would satisfy every assertion above.
VALID = (
    '.decl e(a: int64, b: int64)\n'
    '.decl reach(a: int64, b: int64)\n'
    '.output reach\n'
    'e(1, 2).  e(2, 3).\n'
    'reach(x, y) :- e(x, y).\n'
    'reach(x, z) :- reach(x, y), e(y, z).\n'
)


def _scrubbed_env():
    """Child env with the logger opt-ins removed.

    Inheriting WL_LOG=PARSER:1 (or a wildcard such as WL_LOG=*:5) from the
    developer's shell would route the rejection text to stderr through the
    logger and make these tests pass on an unfixed tree.
    """
    env = dict(os.environ)
    env.pop("WL_LOG", None)
    env.pop("WL_LOG_FILE", None)
    # The legacy presence-flag shim (#277) enables TRACE on its section for
    # any value, including "0", so it has to go too.
    env.pop("WL_DEBUG_JOIN", None)
    env.pop("WL_CONSOLIDATION_LOG", None)
    return env


def _run(exe, program_text, tmpdir, name):
    path = os.path.join(tmpdir, name + ".dl")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(program_text)
    return subprocess.run(
        [exe, path], capture_output=True, text=True, timeout=60,
        env=_scrubbed_env())


def _fail(msg):
    sys.stderr.write(msg if msg.endswith("\n") else msg + "\n")
    return False


def main() -> int:
    if len(sys.argv) != 2:
        sys.stderr.write("usage: test_cli_parse_diagnostics.py <executable>\n")
        return 1
    exe = sys.argv[1]
    ok = True

    with tempfile.TemporaryDirectory() as tmpdir:
        for i, (label, text, needles) in enumerate(CASES):
            r = _run(exe, text, tmpdir, "case%d" % i)

            # (2) still rejected.
            if r.returncode == 0:
                ok = _fail(
                    "%s: expected a non-zero exit (the program is invalid), "
                    "got 0. Diagnostics must not come at the cost of "
                    "accepting the program." % label)
                continue

            # (1) and it says what was wrong.
            for needle in needles:
                if needle not in r.stderr:
                    ok = _fail(
                        "%s: stderr does not name %r.\n"
                        "  stderr was: %r\n"
                        "  A rejected program must say which relation or "
                        "variable was rejected without WL_LOG being set."
                        % (label, needle, r.stderr.strip()))

        # Syntax errors: the discarded errbuf.
        #
        # wl_parser_parse_string() composes "line %u, col %u: %s (got %s)"
        # (parser.c:84) and wirelog_parse_string() drops it on the floor.
        # Asserting the position keywords is what makes this bite: an earlier
        # draft compared stderr for equality against the string "Parse error"
        # and passed against the unfixed binary, because the real output is
        # "Parse error\nerror: execution failed" -- not equal, so the
        # assertion never fired.  Require the detail to be present rather
        # than requiring the generic text to be absent.
        r = _run(exe, SYNTAX_CASE, tmpdir, "syntax")
        if r.returncode == 0:
            ok = _fail("syntax: expected a non-zero exit, got 0")
        else:
            for needle in ("line ", "col "):
                if needle not in r.stderr:
                    ok = _fail(
                        "syntax: stderr lacks %r, so the parser's error "
                        "buffer is still being discarded.\n"
                        "  stderr was: %r" % (needle, r.stderr.strip()))

        # Control: a valid program is unaffected.
        r = _run(exe, VALID, tmpdir, "valid")
        if r.returncode != 0:
            ok = _fail(
                "control: a valid program must still exit 0, got %d\n"
                "  stderr was: %r" % (r.returncode, r.stderr.strip()))
        if "error" in r.stderr.lower():
            ok = _fail(
                "control: a valid program must not print an error; "
                "stderr was %r" % r.stderr.strip())
        if "reach(1, 3)" not in r.stdout.replace("  ", " "):
            ok = _fail(
                "control: the valid program must still evaluate "
                "(reach(1, 3) missing); stdout was %r" % r.stdout.strip())

    if not ok:
        return 1
    print("test_cli_parse_diagnostics: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
