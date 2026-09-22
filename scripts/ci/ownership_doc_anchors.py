#!/usr/bin/env python3
"""Index C function definitions so the ownership matrix can cite call sites.

The ownership gate (#1384) anchors every matrix row on a symbol rather than a
line number, because the files it cites churn and a line citation rots within
days.  Resolving ``file.c:symbol`` needs a *definition* index.

This deliberately does not reuse scripts/ci/threading_doc_anchors.py.  That
helper emits a site only where its ``ATOMIC`` regex matches, attributed to the
enclosing function, and its ``#N`` suffixes disambiguate repeated atomics
within one function rather than repeated definitions.  Whole files the
ownership matrix must cite contribute nothing to it -- cache.c, eval_stack.c,
filter.c and diff_arrangement.c yield zero anchors between them, while
col_mat_cache_pin_release is a real definition at cache.c:375.  Extending that
helper would change its extraction contract, not widen it, and
check-threading-doc.sh hard-requires ``^atomic_[A-Za-z0-9_]+$`` on its third
field, so the 155-row threading gate would be at risk for roughly sixty lines
of shared regex.  The comment/string masking below is therefore a
reimplementation, and threading_doc_anchors.py is not imported or edited.

``static`` definitions are indexed like any other: the retraction producer this
matrix documents is ``static``, and file-local pin helpers such as
mat_cache_evict_lru carry the semantics a row needs to cite.

Usage: ownership_doc_anchors.py <repo-root> [--dump]
Output: one ``file<TAB>line<TAB>symbol`` record per definition.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

FUNCTION = re.compile(
    r"\b([A-Za-z_]\w*)\s*\([^;{}]*\)\s*"
    r"(?:__[A-Za-z_]\w*\s*)*\{"
)
# `defined` is the preprocessor operator, not a callable: `#if defined(X)`
# followed by a brace on a later line otherwise indexes as a definition.
CONTROL = {"if", "for", "while", "switch", "catch", "sizeof", "return",
           "defined"}


def _mask_comments_and_strings(text: str) -> str:
    """Blank comments and literals, preserving offsets and newlines.

    Offsets must survive so a match position still maps to the original line,
    and newlines must survive so the line map stays correct.
    """
    out = list(text)
    i = 0
    state = "code"
    while i < len(out):
        c = out[i]
        n = out[i + 1] if i + 1 < len(out) else ""
        if state == "block":
            if c == "*" and n == "/":
                out[i] = out[i + 1] = " "
                i += 2
                state = "code"
                continue
            if c != "\n":
                out[i] = " "
            i += 1
            continue
        if state in {"string", "char"}:
            if c == "\\":
                if c != "\n":
                    out[i] = " "
                if i + 1 < len(out) and out[i + 1] != "\n":
                    out[i + 1] = " "
                i += 2
                continue
            if (state == "string" and c == '"') or (state == "char" and c == "'"):
                out[i] = " "
                state = "code"
            elif c != "\n":
                out[i] = " "
            i += 1
            continue
        if c == "/" and n == "/":
            out[i] = out[i + 1] = " "
            i += 2
            while i < len(out) and out[i] != "\n":
                out[i] = " "
                i += 1
            continue
        if c == "/" and n == "*":
            out[i] = out[i + 1] = " "
            i += 2
            state = "block"
            continue
        if c == '"':
            out[i] = " "
            state = "string"
        elif c == "'":
            out[i] = " "
            state = "char"
        i += 1
    return "".join(out)


def _mask_preprocessor(text: str) -> str:
    """Blank preprocessor directives, preserving offsets and newlines.

    A directive's parenthesised payload is not a parameter list, and the
    definition regex's ``[^;{}]*`` crosses newlines: in ops.c the value of
    ``#define WL_JOIN_PAIR_CACHE_MAX_BYTES (...)`` matched forward 1446
    characters to the opening brace of col_op_variable's body, swallowing that
    definition and several others.  The file indexed 7 definitions instead of
    20 and every anchor inside the swallowed span silently failed to resolve --
    a false negative, which is the failure this gate exists to prevent.
    Continuation lines are blanked with the directive that owns them.
    """
    out = []
    continued = False
    for raw in text.splitlines(keepends=True):
        stripped = raw.lstrip()
        directive = continued or stripped.startswith("#")
        if directive:
            body = raw.rstrip("\n")
            out.append(" " * len(body) + raw[len(body):])
            continued = body.rstrip().endswith("\\")
        else:
            out.append(raw)
    return "".join(out)


ATTRIBUTE = re.compile(
    r"(?<![A-Za-z0-9_])(?:__attribute__|__declspec)\s*\(")


def _mask_attributes(text: str) -> str:
    """Blank __attribute__((...)) and __declspec(...), preserving offsets.

    An attribute on its own line *before* the return type is matched by
    FUNCTION as though it were the definition, and its parenthesised payload
    then runs forward to the real definition's opening brace, swallowing it.
    Measured on wirelog/columnar/session.c: three definitions
    (wl_columnar_session_get_tdd_decision_stats and its two siblings) were
    absent from the index and bogus ``__attribute__`` entries stood in their
    place.  Adding the name to CONTROL does not help -- finditer resumes at
    match.end(), which is already past the swallowed definition, so the real
    one is never offered.

    This fails closed for a cited anchor, which simply will not resolve, but it
    fails *open* for the ambiguity check: a swallowed second definition makes a
    genuinely ambiguous symbol read as unique.  That is why it is masked rather
    than tolerated.
    """
    out = list(text)
    for match in ATTRIBUTE.finditer(text):
        # Start at the first paren of the match.  Starting one later leaves the
        # outer `)` of __attribute__((...)) unblanked.
        i = text.index("(", match.start())
        depth = 0
        closed = False
        while i < len(out):
            if out[i] == "(":
                depth += 1
            elif out[i] == ")":
                depth -= 1
                if depth == 0:
                    closed = True
                    break
            i += 1
        if not closed:
            # Unterminated: only reachable on C that would not compile.  Blank
            # nothing rather than masking every later definition in the file.
            continue
        for j in range(match.start(), i + 1):
            if out[j] != "\n":
                out[j] = " "
    return "".join(out)


def _line_of(starts: list[int], offset: int) -> int:
    lo, hi = 0, len(starts) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if starts[mid] <= offset:
            lo = mid
        else:
            hi = mid - 1
    return lo + 1


def definitions(source: Path, root: Path | None = None) -> list[tuple[str, int, str]]:
    """Return (path, line, symbol) for every function definition.

    The path is relative to the repository root when one is given.  Basenames
    are not unique under wirelog/ -- session.c exists both as wirelog/session.c
    and wirelog/columnar/session.c -- so diagnostics must name the file that
    actually holds the definition.  Anchors in the matrix stay basename-scoped
    because that is the existing convention of the table at docs/MEMORY.md,
    and the gate refuses any basename+symbol pair that resolves more than once.
    """
    clean = _mask_attributes(_mask_preprocessor(
        _mask_comments_and_strings(source.read_text(encoding="utf-8"))))
    starts = [0]
    starts.extend(i + 1 for i, c in enumerate(clean) if c == "\n")
    found: list[tuple[str, int, str]] = []
    for match in FUNCTION.finditer(clean):
        symbol = match.group(1)
        if symbol in CONTROL:
            continue
        name = source.relative_to(root).as_posix() if root else source.name
        found.append((name, _line_of(starts, match.start(1)), symbol))
    return found


def inventory(root: Path) -> list[tuple[str, int, str]]:
    records: list[tuple[str, int, str]] = []
    for path in sorted((root / "wirelog").rglob("*")):
        if path.suffix in {".c", ".h"} and path.is_file():
            records.extend(definitions(path, root))
    return records


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--dump", action="store_true")
    args = parser.parse_args(argv[1:])
    if not (args.root / "wirelog").is_dir():
        print(f"ownership_doc_anchors: no wirelog/ under {args.root}", file=sys.stderr)
        return 1
    records = inventory(args.root)
    if not records:
        print("ownership_doc_anchors: indexed no definitions", file=sys.stderr)
        return 1
    if args.dump:
        for name, line, symbol in records:
            print(f"{name}\t{line}\t{symbol}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
