#!/usr/bin/env python3
"""Guard against freeing a program before the sessions built from its plan.

A ``wl_plan_t`` and every session created from it borrow the program's
intern table (the borrow note in ``wirelog/exec_plan_gen.c`` and the
executor contract in ``wirelog/wirelog.h``).  Test helpers used to free the
program right after ``wl_plan_from_program()``; that became a use after free
once session creation attaches the memory governor to ``plan->intern``
(Issue #1431), and nothing stopped a new helper from reintroducing the
pattern (Issue #1471).  ``tests/plan_fixture.h`` provides
``plan_fixture_hold()``, which keeps a program alive until exit.

The gate is a deterministic text scan over ``tests/`` and ``bench/`` (no C
parser).  For every function that calls ``wl_plan_from_program()`` or
``wl_plan_from_program_with_snapshot()`` and does not call
``plan_fixture_hold()``, it opens a region at the plan call and walks the
lines that follow.  A line is *unconditional* when it sits at the plan
call's brace depth and indentation and does not open a control statement,
so the plan-generation failure branch (``if (rc != 0) { free; return }``,
braced or not) is never unconditional.  The region ends at an unconditional
``return``/``continue``/``break``/``goto``, an unconditional
``wl_plan_free()`` of the plan, an unconditional session destroy, or when
the enclosing block closes.  Inside the region an unconditional
``wirelog_program_free()`` of the program is an offender when the plan is
still live: before the free the plan escaped (stored through an assignment,
returned, or handed to a session create), or after the free the plan is
referenced at all, other than by ``wl_plan_free()``.

Known limitations, deliberately accepted: a program freed under another
name, a free wrapped in a macro or split across lines, a plan built in a
nested block and used after that block closes, a plan handed to a storing
function other than session create, ``goto`` to a label that frees on the
success path, and ``#if 0`` blocks (the preprocessor is not evaluated).
A conditional ``return plan;`` followed by an unconditional free of the
program is reported even though the free is on the failure path; brace the
failure branch or hold the program.  The fix for a finding is always
``plan_fixture_hold()``.

Exit codes: 0 clean, 1 offenders or usage or vacuity (no plan-building helper
scanned), 77 when ``tests/`` or ``bench/`` is missing; ``WIRELOG_ABI_REQUIRED=1``
turns that skip into a failure.

Usage: check-program-lifetime.py <source-root>
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

PLAN_CALL = re.compile(r"\bwl_plan_from_program(?:_with_snapshot)?\s*\(")
# The program is the first argument and the plan the ``&plan`` out
# parameter (never ``&&``); a call without such an argument, for example
# ``wl_plan_from_program(prog, NULL)``, opens no region but still counts
# the function as a helper.
PLAN_ARGS = re.compile(
    r"\bwl_plan_from_program(?:_with_snapshot)?\s*\(\s*(\w+)\s*,[^;]*?"
    r"(?<!&)&(?!&)\s*(\w+)")
HOLD_CALL = re.compile(r"\bplan_fixture_hold\s*\(")
SESSION_CREATE = re.compile(r"\b(?:wl|col)_session_create\w*\s*\(")
SESSION_DESTROY = re.compile(r"\b(?:wl|col)_session_destroy\w*\s*\(")
CONTROL_WORD = re.compile(r"^\s*(?:if|else|for|while|do|switch|case|default)\b")
LABEL_LINE = re.compile(r"^\s*[A-Za-z_]\w*\s*:\s*$")
TERMINATOR = re.compile(r"^\s*(?:return|continue|break|goto)\b")
FUNCTION_NAME = re.compile(r"^\s*(?:static\s+)?[\w\s\*]*?\b([A-Za-z_]\w*)\s*\(")
SCAN_DIRS = ("tests", "bench")
SKIP_FILES = {"plan_fixture.h"}


def fail(message: str) -> int:
    print(f"check-program-lifetime: FAIL: {message}", file=sys.stderr)
    return 1


def skip(message: str) -> int:
    if os.environ.get("WIRELOG_ABI_REQUIRED") == "1":
        return fail(f"gate required but would skip: {message}")
    print(f"check-program-lifetime: SKIP: {message}")
    return 77


def strip_source(text: str) -> str:
    """Blank out comments and string/char literals, preserving newlines.

    A single pass keeps the states exclusive, so a ``//`` inside a string
    or a ``{`` inside a comment cannot leak into the brace count.
    """
    out = []
    i = 0
    n = len(text)
    state = "code"
    while i < n:
        c = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if state == "code":
            if c == "/" and nxt == "/":
                state = "line"
                out.append("  ")
                i += 2
                continue
            if c == "/" and nxt == "*":
                state = "block"
                out.append("  ")
                i += 2
                continue
            if c == '"':
                state = "string"
                out.append(" ")
                i += 1
                continue
            if c == "'":
                state = "char"
                out.append(" ")
                i += 1
                continue
            out.append(c)
            i += 1
            continue
        if state == "line":
            if c == "\n":
                state = "code"
                out.append(c)
            else:
                out.append(" ")
            i += 1
            continue
        if state == "block":
            if c == "*" and nxt == "/":
                state = "code"
                out.append("  ")
                i += 2
                continue
            out.append(c if c == "\n" else " ")
            i += 1
            continue
        # string or char literal: honour escapes, keep newlines
        if c == "\\" and i + 1 < n:
            out.append("  " if nxt != "\n" else " \n")
            i += 2
            continue
        if (state == "string" and c == '"') or (state == "char" and c == "'"):
            state = "code"
            out.append(" ")
            i += 1
            continue
        out.append(c if c == "\n" else " ")
        i += 1
    return "".join(out)


def indent_of(line: str) -> int:
    return len(line) - len(line.lstrip(" \t"))


def split_functions(lines: list[str]) -> list[tuple[str, int, int]]:
    """Return (name, first_line_index, last_line_index) per function body.

    A body opens at a ``{`` seen while the brace depth is zero.  The name is
    taken from the nearest earlier line, since the previous body closed, that
    looks like a declarator; ``?`` when none does.
    """
    functions = []
    depth = 0
    last_close = -1
    body_start = None
    name = "?"
    for index, line in enumerate(lines):
        for ch in line:
            if ch == "{":
                if depth == 0:
                    body_start = index
                    name = "?"
                    for back in range(index, last_close, -1):
                        match = FUNCTION_NAME.match(lines[back])
                        if match:
                            name = match.group(1)
                            break
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0 and body_start is not None:
                    functions.append((name, body_start, index))
                    body_start = None
                    last_close = index
                elif depth < 0:
                    depth = 0
    return functions


def depth_at_line_starts(lines: list[str]) -> list[int]:
    depths = []
    depth = 0
    for line in lines:
        depths.append(depth)
        depth += line.count("{") - line.count("}")
        if depth < 0:
            depth = 0
    return depths


def plan_call_args(lines: list[str], index: int) -> tuple[str, str] | None:
    joined = " ".join(item.strip() for item in lines[index:index + 4])
    match = PLAN_ARGS.search(joined)
    return (match.group(1), match.group(2)) if match else None


def references_plan(line: str, planv: str, ignore_plan_free: bool) -> bool:
    text = line
    if ignore_plan_free:
        text = re.sub(r"\bwl_plan_free\s*\(\s*%s\s*\)" % re.escape(planv), "",
                      text)
    return re.search(r"\b%s\b" % re.escape(planv), text) is not None


def escapes_plan(line: str, planv: str) -> bool:
    """True when the plan leaves the helper alive on this line.

    Before the free only real escapes count: the plan is stored somewhere
    (``*out = plan;``, ``ctx->plan = plan;``), returned, or handed to a
    session.  Reads such as ``ASSERT(plan == NULL)`` or ``plan->strata``
    are not escapes; after the free every reference counts instead.
    """
    name = re.escape(planv)
    return (SESSION_CREATE.search(line) is not None
            or re.search(r"=\s*%s\s*;" % name, line) is not None
            or re.search(r"\breturn\s+%s\b" % name, line) is not None)


def scan_function(lines: list[str], depths: list[int], name: str,
                  start: int, end: int, rel: str) -> tuple[bool, list[str]]:
    """Scan one function; return (is_helper, offender messages)."""
    body = "\n".join(lines[start:end + 1])
    if not PLAN_CALL.search(body):
        return False, []
    if HOLD_CALL.search(body):
        return True, []
    offenders = []
    for index in range(start, end + 1):
        if not PLAN_CALL.search(lines[index]):
            continue
        args = plan_call_args(lines, index)
        if not args:
            continue
        progv, planv = args
        region_depth = depths[index]
        region_indent = indent_of(lines[index])
        free_re = re.compile(r"\bwirelog_program_free\s*\(\s*%s\s*\)"
                             % re.escape(progv))
        plan_free_re = re.compile(r"\bwl_plan_free\s*\(\s*%s\s*\)"
                                  % re.escape(planv))
        escaped = False
        cursor = index + 1
        while cursor <= end:
            line = lines[cursor]
            if not line.strip():
                cursor += 1
                continue
            if depths[cursor] < region_depth:
                break
            unconditional = (depths[cursor] <= region_depth
                             and indent_of(line) <= region_indent
                             and not CONTROL_WORD.match(line)
                             and not LABEL_LINE.match(line))
            if unconditional and plan_free_re.search(line):
                break
            if unconditional and SESSION_DESTROY.search(line):
                break
            if unconditional and free_re.search(line):
                # The plan must be dead by now: no escape before the free,
                # no reference on this line, and none later in the region.
                later_live = references_plan(line, planv, True)
                probe = cursor + 1
                while probe <= end and not later_live:
                    probe_line = lines[probe]
                    if depths[probe] < region_depth:
                        break
                    probe_unconditional = (depths[probe] <= region_depth
                                           and indent_of(probe_line)
                                           <= region_indent
                                           and not CONTROL_WORD.match(
                                               probe_line)
                                           and not LABEL_LINE.match(
                                               probe_line))
                    if probe_unconditional and plan_free_re.search(probe_line):
                        break
                    if (SESSION_CREATE.search(probe_line)
                            or references_plan(probe_line, planv, True)):
                        later_live = True
                    if probe_unconditional and TERMINATOR.match(probe_line):
                        break
                    probe += 1
                if escaped or later_live:
                    offenders.append(
                        f"{rel}:{name}:{cursor + 1}: wirelog_program_free("
                        f"{progv}) while the plan built at line {index + 1} "
                        f"is still live; hold the program with "
                        f"plan_fixture_hold() (tests/plan_fixture.h)")
                break
            if escapes_plan(line, planv):
                escaped = True
            if unconditional and TERMINATOR.match(line):
                break
            cursor += 1
    return True, offenders


def scan_file(path: Path, rel: str) -> tuple[int, list[str]]:
    text = path.read_text(encoding="utf-8")
    lines = strip_source(text).split("\n")
    depths = depth_at_line_starts(lines)
    helpers = 0
    offenders: list[str] = []
    for name, start, end in split_functions(lines):
        is_helper, found = scan_function(lines, depths, name, start, end, rel)
        if is_helper:
            helpers += 1
        offenders.extend(found)
    return helpers, offenders


def scan_tree(root: Path) -> tuple[int, int, list[str]]:
    """Return (helpers scanned, files scanned, offender lines)."""
    helpers = 0
    files = 0
    offenders: list[str] = []
    for sub in SCAN_DIRS:
        base = root / sub
        if not base.is_dir():
            continue
        for path in sorted(base.rglob("*")):
            if path.suffix not in (".c", ".h") or path.name in SKIP_FILES:
                continue
            if not path.is_file():
                continue
            files += 1
            rel = path.relative_to(root).as_posix()
            count, found = scan_file(path, rel)
            helpers += count
            offenders.extend(found)
    return helpers, files, offenders


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        return fail("usage: check-program-lifetime.py <source-root>")
    root = Path(argv[1]).resolve()
    for sub in SCAN_DIRS:
        if not (root / sub).is_dir():
            return skip(f"{root / sub} is missing")
    try:
        helpers, files, offenders = scan_tree(root)
    except (OSError, UnicodeDecodeError) as exc:
        return fail(f"cannot scan {root}: {exc}")
    if helpers == 0:
        return fail("no plan-building helper was scanned; the scanner or the "
                    "test tree has changed shape")
    if offenders:
        return fail("\n".join(offenders))
    print(f"check-program-lifetime: ok {helpers} plan-building helpers in "
          f"{files} files, none free the program early")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
