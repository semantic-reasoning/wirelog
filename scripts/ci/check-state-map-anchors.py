#!/usr/bin/env python3
"""Verify every attribution the publication-cutoff state map makes (#1953).

The map in docs/SEMANTICS.md says, per session field, which functions write it
and which read it.  Each of those is an assertion about code, and successive
review rounds kept finding the same two defect classes: a named function that
exists but does not touch the field beside it, and a cell missing a function
that does.  So this gate checks both directions.

  * SOUNDNESS -- every function a cell names must really write, or really
    read, that field.
  * COMPLETENESS -- every function this gate can attribute the field to must
    be named.  Emptying a cell is therefore a failure, not a way to pass.

The completeness claim is bounded, and the bound is not a formality.  A
function counts when it is under wirelog/, which is all the symbol index walks;
when it has exactly one indexed definition, because otherwise there is no one
body to read; when the reference is not inside a preprocessor directive, which
the index blanks and this gate blanks with it -- the restore in
`tdd_worker_subpass_fn`'s `TDD_WORKER_RETURN()` macro is invisible for that
reason; and, for a qualified subject, when it reaches the field through a
pointer its own body declares as that struct -- the name immediately before
the field, reached through `->`, so `entry->rel->field` counts when `rel` is
such a pointer and `entry->owner->field` does not when `owner` is not.  Writing
the unbounded sentence instead
was a review finding: tests/ contains a write to `col_rel_t::base_nrows` that
this gate neither names nor wants to.

Because the cells are complete, the table needs no convention about which
functions are omitted and why.  An earlier revision carried one ("both
evaluating paths write every field below, and are named only where they also
read it") and it was false in six places -- prose the gate could not check,
justifying omissions the gate could not check either.

A subject may be written `struct_t::field` when two structs share a field
name.  The qualifier is load-bearing, not decoration: `base_nrows` exists on
both `col_rel_t` and `col_diff_arrangement_t`, and a gate that dropped the
qualifier credited a one-line arrangement function as a writer of the
relation's field.  For a qualified subject a function attributes the field
only when it reaches it through a pointer its own body declares as that
struct.  Any other function that touches a bare `field` must declare no
pointer to that struct at all, or the row cannot be partitioned and the gate
fails rather than guess.

The verdict column is NOT checked here, and cannot be: PRESERVE and COMMIT are
judgements about what a stopping return should do, not facts about the
current code.  They are reviewed by people.  This gate exists so that the
evidence those judgements rest on is not also a matter of opinion.

Function bodies come from scripts/ci/ownership_doc_anchors.py, the index the
ownership gate already uses, for the reason it records: a line citation rots
within days.  Comments are stripped before matching, so a rename that leaves
stale prose behind does not satisfy a claim -- with one shape it does not
cover: a `//` comment continued with a trailing backslash keeps the next line
inside the comment in C, and the masker treats that line as code.  No source
under wirelog/ does that today.
"""

from __future__ import annotations

import argparse
import functools
import importlib.util
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DOC = REPO / "docs/SEMANTICS.md"
INDEX = REPO / "scripts/ci/ownership_doc_anchors.py"
SECTION = "### Publication-cutoff state map"
# Which fields the bookkeeping blocks commit is a judgement about scope, like
# the verdict column, so it is written down rather than derived.  Pinning it is
# what makes DELETING a row a failure: emptying a cell already failed the
# completeness check, but a removed row simply left less to check and passed.
# It catches a row deleted or renamed here.  It cannot catch a field newly
# committed in a bookkeeping block: that gets no row and no complaint, because
# this gate looks for no boundary to those blocks.  One is findable on the
# snapshot path, where col_rel_compact_many sits just above the block and
# col_session_mem_sample just below it; the step path has no such pair, its
# compaction call is sixty-odd lines further up, past calls this comment will
# not try to count again, and its block is split by a relation-removal loop.
# Deriving from call sites would be one more thing to keep in step with the
# code, and a call added or moved inside a block would change the derived set
# with nothing to notice.
SUBJECTS = (
    "last_inserted_relation",
    "pending_input_change",
    "pending_full_input_eval",
    "has_evaluated",
    "snapshot_stable_valid",
    "delta_seeded",
    "last_removed_relation",
    "retraction_seeded",
    "plain_step_completion_pending",
    "plain_step_completion_phase",
    "plain_step_completion_step_context",
    "plain_step_completion_active",
    "col_rel_t::base_nrows",
)

IDENT = re.compile(r"`([A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)?)`")
# Comments, string literals and character literals are all masked, because both
# predicates are otherwise satisfiable by text that is not a code reference:
# `"field = %d"` contains a literal assignment and would satisfy writes(), and
# any prose mention such as `"field is stale"` would satisfy reads().
# A write is a plain assignment, a compound assignment, or an increment.  The
# first version accepted only `=`, which would have rejected a true `|=`
# attribution with "its body does not" -- a false alarm being the one outcome
# that teaches a reader to stop believing the gate.
ASSIGN = r"(?:=[^=]|[-+*/%&|^]=|<<=|>>=|\+\+|--)"


def die(msg: str) -> None:
    print(f"check-state-map-anchors: FAIL: {msg}", file=sys.stderr)
    raise SystemExit(1)


def section_rows(doc: Path) -> list:
    """Return the table rows of the map section as lists of cell strings."""
    lines = doc.read_text(encoding="utf-8").splitlines()
    start = next((i for i, l in enumerate(lines) if l.startswith(SECTION)), None)
    if start is None:
        die(f"{SECTION!r} not in {doc}")
    # A sibling ### section ends this one too; stopping only at ## would fold a
    # later sibling's table into this scan.
    end = next((i for i, l in enumerate(lines[start + 1:], start + 1)
                if l.startswith("## ") or l.startswith("### ")), len(lines))
    rows = []
    for line in lines[start:end]:
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 4 or set("".join(cells)) <= set("-: "):
            continue                      # header separator
        rows.append(cells)
    if not rows:
        die(f"the {SECTION!r} section has no table rows to check")
    return rows


# Memoised so the self-test can invoke main() repeatedly in one process: the
# index and the bodies are derived from the source tree, which no fixture edits,
# and rebuilding them per case cost more than every check in this file.
@functools.lru_cache(maxsize=1)
def function_index() -> dict:
    out = subprocess.run([sys.executable, str(INDEX), str(REPO), "--dump"],
                         capture_output=True, text=True, encoding="utf-8")
    if out.returncode != 0:
        die(f"{INDEX.name} --dump failed: {out.stderr.strip()}")
    idx = {}
    for line in out.stdout.splitlines():
        path, lineno, sym = line.split("\t")
        idx.setdefault(sym, []).append((path, int(lineno)))
    if not idx:
        die(f"{INDEX.name} indexed no functions")
    return idx


@functools.lru_cache(maxsize=1)
def _masker():
    """The masking state machine ownership_doc_anchors.py already uses.

    This gate had its own three-regex version, and no order of three
    independent regexes is right for C: comments-first mangles `"http://x"`,
    strings-first mangles `/* "unpaired */`, and neither handles a character
    literal, so `'{'` could run a body's brace walk past its closing brace.
    That file's masker handles those and documents why it preserves offsets and
    newlines -- which is what keeps a definition's recorded line number indexing
    the same line here, and what stops a comment between two halves of an
    identifier from fusing them.  Reusing it also stops this being the
    repository's third implementation of the same masking.

    All three of its stages are composed, in its own order, so this gate reads
    the text the index numbered.  Order matters beyond today's tree: running the
    attribute stage on unmasked text lets an `__attribute__((` inside a string
    literal paren-match forward and swallow a real write along with the closing
    brace.  Applying only the first stage left 111 bodies carrying a
    preprocessor line the index blanks, 13 of them a `#define`; dropping the
    attribute stage changes the masked text of 6 files, `session.c` among them.
    Neither changes a verdict in this tree today, so the fixtures in the
    self-test are the only thing between a partial masker and a silent pass.
    """
    spec = importlib.util.spec_from_file_location("wl_ownership_anchors", INDEX)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    # A rename fails closed either way -- the lambda below raises
    # AttributeError on the first file, and the self-test dies before reaching
    # the assertions that check these names exist.  This is for the message: a
    # named failure from the file that depends on the stages, rather than a
    # traceback from the file that did not change.
    missing = [n for n in ("_mask_comments_and_strings", "_mask_preprocessor",
                           "_mask_attributes") if not hasattr(mod, n)]
    if missing:
        die(f"{INDEX.name} no longer exports {', '.join(missing)}; this gate "
            f"masks with its stages so that both read the same text")
    return lambda src: mod._mask_attributes(
        mod._mask_preprocessor(mod._mask_comments_and_strings(src)))


def strip_comments(src: str) -> str:
    """Blank comments and literals, preserving offsets and newlines."""
    return _masker()(src)


@functools.lru_cache(maxsize=None)
def stripped_lines(path: str) -> tuple:
    """A file's lines with comments and string literals blanked out."""
    text = (REPO / path).read_text(encoding="utf-8", errors="replace")
    return tuple(strip_comments(text).splitlines())


def body(idx: dict, sym: str):
    """Source of sym's definition with comments stripped, or None."""
    sites = idx.get(sym)
    if not sites or len(sites) != 1:
        return None
    return one_body(*sites[0])


def one_body(path: str, lineno: int) -> str:
    # Walk the STRIPPED lines.  Counting braces first and stripping afterwards
    # was wrong in both directions, and worse in the one nobody looks at: a `}`
    # in a comment ended the walk early, while a `{` in a comment ended it late,
    # swallowed the next definition and credited this function with that one's
    # writes.  The balance check below cannot see the second case, because
    # swallowed text balances.
    lines = stripped_lines(path)
    text, depth, opened = [], 0, False
    for line in lines[lineno - 1:]:
        text.append(line)
        depth += line.count("{") - line.count("}")
        if "{" in line:
            opened = True
        if opened and depth <= 0:
            break
    src = "\n".join(text)
    if not braces_balance(src):
        die(f"{path}:{lineno}: the extracted body's braces do not balance, so "
            f"a brace in a comment or string truncated it")
    return src


def braces_balance(src: str) -> bool:
    """Did the brace walk end on a complete body?

    Now that the walk runs over stripped text this should hold by construction,
    so it is an invariant check rather than a filter: it catches a definition
    the index pointed at whose braces never close, such as one built by a macro
    the stripper does not expand.  No body in this tree fails it, so only a unit
    test reaches it; kept separate for that reason.
    """
    return src.count("{") == src.count("}")


def writes(src: str, field: str) -> bool:
    return re.search(rf"\b{re.escape(field)}\s*{ASSIGN}", src) is not None


def reads(src: str, field: str) -> bool:
    for m in re.finditer(rf"\b{re.escape(field)}\b", src):
        if not re.match(rf"\s*{ASSIGN}", src[m.end():]):
            return True
        # A compound assignment reads the field as well as writing it.
        if re.match(r"\s*(?:[-+*/%&|^]=|<<=|>>=|\+\+|--)", src[m.end():]):
            return True
    return False


def declares(src: str, struct: str) -> bool:
    """Does this body declare a pointer to `struct`?"""
    return re.search(rf"\b{re.escape(struct)}\s*\*", src) is not None


def touches_via(src: str, struct: str, field: str, write: bool) -> bool:
    """Does this body reach `field` through a pointer it declares as `struct`?"""
    roots = {n for n in re.findall(
        rf"\b{re.escape(struct)}\s*\*+\s*(?:const\s+)?(\w+)", src)
        if n != "const"}
    for root in sorted(roots):
        pat = rf"\b{re.escape(root)}\s*(?:\[[^\]]*\])?\s*->\s*{re.escape(field)}"
        for m in re.finditer(pat, src):
            after = src[m.end():]
            if write == bool(re.match(rf"\s*{ASSIGN}", after)):
                return True
    return False


@functools.lru_cache(maxsize=None)
def attributions(struct, field: str, write: bool) -> frozenset:
    """Every function this gate can attribute the field to."""
    bodies = all_bodies()
    role = "write" if write else "read"
    bare = {n for n, s in bodies.items()
            if (writes(s, field) if write else reads(s, field))}
    # A symbol with several definitions has no single body to read, so it is
    # absent from all_bodies() and would silently leave the expected set --
    # after which the gate would demand its REMOVAL from a cell that is right.
    # Fail closed on the one that matters: one that touches this field.
    ambiguous = ambiguous_touchers(ambiguous_bodies(), field, write)
    if ambiguous:
        die(f"{', '.join(ambiguous)}: defined more than once and {role}s "
            f"{field}, so the map cannot attribute it")
    if struct is None:
        return frozenset(bare)
    # A qualifier no body declares is a typo, not a narrowing: without this the
    # expected set would be empty and a row with two empty columns would pass.
    if not any(declares(s, struct) for s in bodies.values()):
        die(f"no body declares `{struct} *`, so the qualifier on "
            f"{struct}::{field} resolves to nothing")
    owned, unclear = partition_by_struct(bodies, bare, struct, field, write)
    if unclear:
        die(f"cannot tell which struct's {field} these {role}: "
            f"{', '.join(sorted(unclear))}")
    return frozenset(owned)


def ambiguous_touchers(multi: dict, field: str, write: bool) -> list:
    """Symbols with several definitions, any of which touches the field.

    Separate from attributions() for the same reason as partition_by_struct:
    no multiply-defined symbol in this tree touches a mapped field, so only a
    unit test reaches it, and a mutant that dropped the guard survived
    everything else.
    """
    return sorted(n for n, srcs in multi.items()
                  if any((writes(s, field) if write else reads(s, field))
                         for s in srcs))


def partition_by_struct(bodies, bare, struct: str, field: str, write: bool):
    """Split touchers of a bare field into this struct's and the unclassifiable.

    A body that reaches the field through a pointer it declares as `struct` is
    this struct's.  One that declares no such pointer is another struct's and is
    dropped.  One that declares the pointer but does not reach the field through
    it is neither, and is returned as unclear so the caller can fail rather than
    guess.  Kept separate from attributions() because no (struct, field) pair in
    this tree produces an unclear set, so only a unit test can reach it.
    """
    owned = {n for n in bare if touches_via(bodies[n], struct, field, write)}
    unclear = {n for n in bare - owned if declares(bodies[n], struct)}
    return owned, unclear


@functools.lru_cache(maxsize=1)
def ambiguous_bodies() -> dict:
    """Bodies of every symbol defined more than once, keyed by symbol."""
    idx = function_index()
    out = {}
    for sym, sites in idx.items():
        if len(sites) > 1:
            out[sym] = tuple(one_body(path, lineno) for path, lineno in sites)
    return out


@functools.lru_cache(maxsize=1)
def all_bodies() -> dict:
    idx = function_index()
    out = {}
    for sym in idx:
        src = body(idx, sym)
        if src is not None:
            out[sym] = src
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    # --doc lets the self-test point the gate at a fixture; the code it checks
    # is always the real tree.
    ap.add_argument("--doc", type=Path, default=DOC)
    args = ap.parse_args()

    rows = section_rows(args.doc)
    idx = function_index()
    bodies = all_bodies()
    problems, checked, subjects = [], 0, 0

    for cells in rows:
        subject = IDENT.findall(cells[0])
        if not subject:
            continue                      # no backticked field: the header
        if len(subject) != 1:
            problems.append(
                f"{'/'.join(subject)}: a row subject must name exactly one "
                f"field, and this one names {len(subject)}")
            continue
        struct, _, field = subject[0].rpartition("::")
        struct = struct or None
        subjects += 1
        for role, cell in (("write", cells[1]), ("read", cells[2])):
            listed = set(IDENT.findall(cell))
            expected = attributions(struct, field, role == "write")
            for sym in sorted(listed):
                if sym not in bodies:
                    n = len(idx.get(sym, ()))
                    problems.append(
                        f"{sym}: named in the {role} column of {field} but "
                        + (f"is defined {n} times, so which body is meant is "
                           "undecidable" if n else "no definition was indexed"))
                    continue
                checked += 1
                if sym not in expected:
                    problems.append(
                        f"{sym}: the map says it {role}s {field}, but its "
                        f"body does not")
            for sym in sorted(expected - listed):
                problems.append(
                    f"{sym}: {role}s {field} but the map's {role} column "
                    f"does not name it")

    # Before the subject comparison: a table that names no field at all is a
    # shape failure, and reporting thirteen missing rows instead would mask it
    # -- which it did, until the pin was added and this check stopped being
    # reachable for that fixture.
    if not subjects:
        die("no row named a field; the table shape must have changed")

    seen, repeated = set(), []
    for cells in rows:
        for subject in IDENT.findall(cells[0]):
            if subject in seen:
                repeated.append(subject)
            seen.add(subject)
    if repeated:
        problems.append(f"{', '.join(sorted(set(repeated)))}: more than one row "
                        f"for the same field")
    missing, extra = set(SUBJECTS) - seen, seen - set(SUBJECTS)
    if missing:
        problems.append(f"no row for {', '.join(sorted(missing))}, which "
                        f"SUBJECTS says the bookkeeping commits; drop it from "
                        f"SUBJECTS if the scope really shrank")
    if extra:
        problems.append(f"{', '.join(sorted(extra))}: a row subject not in "
                        f"SUBJECTS; add it there if the scope really grew")

    if problems:
        print(f"check-state-map-anchors: FAIL: {len(problems)} bad attribution"
              f"(s) in {SECTION!r} of {args.doc}:", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        return 1
    print(f"check-state-map-anchors: OK; {checked} attributions verified"
          f" across {subjects} fields, and each column checked for omissions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
