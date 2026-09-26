#!/usr/bin/env python3
"""Self-test for check-state-map-anchors.py.

A gate that cannot fail asserts nothing.  Each case below makes the gate fail on
purpose -- or, for the section-boundary case, refuse to be widened -- so a change
that guts the checker is caught here rather than by a reader trusting a stale
table.  Every fixture is written into a temporary directory; the real
docs/SEMANTICS.md is only ever read.

A mutation run against an earlier version of this file showed which predicates
were free to be replaced by `return True` with every case still passing:
`writes`, the assignment-target exclusion in `reads`, comment stripping, and the
non-unique-definition guard.  Those are asserted directly now, because a
document fixture cannot reach them -- no function in this tree mentions one of
these fields only in a comment, so there is nothing to inject for it.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
GATE = REPO / "scripts/ci/check-state-map-anchors.py"
DOC = REPO / "docs/SEMANTICS.md"
SECTION = "### Publication-cutoff state map"

failures = 0


def load_gate():
    spec = importlib.util.spec_from_file_location("gate", GATE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


GATE_MOD = None


def run(doc: Path) -> int:
    """Exit code the gate returns for this fixture.

    In-process, because the gate memoises a symbol index the fixtures never
    touch and each case would otherwise rebuild it -- two dozen rebuilds cost
    more than every assertion in this file.  run_cli() covers the command-line
    path separately, on a passing and on a failing document: asserting only the
    passing one let a deleted entry point return 0 for everything.
    """
    argv = sys.argv
    sys.argv = [str(GATE), "--doc", str(doc)]
    out, err = io.StringIO(), io.StringIO()
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            return GATE_MOD.main()
    except SystemExit as exit_:
        return exit_.code if isinstance(exit_.code, int) else 1
    finally:
        sys.argv = argv


def run_message(doc: Path):
    """The gate's (exit code, stderr) for this fixture.

    The cases below need the message as well as the code: when a fail-closed
    guard is bypassed, the row it would have stopped often still fails on the
    attributions that follow, so the code alone cannot say which check fired.
    They need the code as well as the message, because a guard downgraded from
    `die()` to a bare print keeps its message and returns 0 -- which is how an
    earlier version of the brace-guard case passed while the guard was broken.
    """
    argv, rc = sys.argv, 0
    sys.argv = [str(GATE), "--doc", str(doc)]
    out, err = io.StringIO(), io.StringIO()
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = GATE_MOD.main() or 0
    except SystemExit as exit_:
        rc = exit_.code if isinstance(exit_.code, int) else 1
    finally:
        sys.argv = argv
    return rc, err.getvalue()


def run_cli(doc: Path) -> int:
    return subprocess.run([sys.executable, str(GATE), "--doc", str(doc)],
                          capture_output=True, text=True,
                          encoding="utf-8").returncode


def expect(name: str, want, got) -> None:
    global failures
    if want == got:
        print(f"test-check-state-map-anchors: ok {name}")
    else:
        print(f"test-check-state-map-anchors: FAIL {name}"
              f" (want {want}, got {got})", file=sys.stderr)
        failures += 1


def unit_cases(g) -> None:
    """Predicates a document fixture cannot reach."""
    # writes(): a compound assignment and an increment are writes; a comparison
    # is not.  Left unasserted, writes() could be `return True`.
    for src, want in (("x = 1;", True), ("x |= 1;", True), ("x++;", True),
                      ("x <<= 1;", True), ("x >>= 1;", True),
                      ("x == 1;", False), ("x != 1;", False),
                      ("x <= 1;", False), ("y = x;", False)):
        expect(f"writes({src!r})", want, g.writes(src, "x"))
    # reads(): an assignment target is not a read, but a compound assignment is.
    for src, want in (("x == 1;", True), ("y = x;", True), ("x |= 1;", True),
                      ("x = 1;", False)):
        expect(f"reads({src!r})", want, g.reads(src, "x"))
    # Comments are stripped, so stale prose left behind by a rename cannot
    # satisfy a claim.
    expect("a write inside a block comment is not a write", False,
           g.writes(g.strip_comments("/* x = 1; */"), "x"))
    expect("a write inside a line comment is not a write", False,
           g.writes(g.strip_comments("// x = 1;\n"), "x"))
    expect("a field named only in a comment is not read", False,
           g.reads(g.strip_comments("/* see x for why */"), "x"))
    # A format string naming a field is not a reference to it either.
    expect("a field named only in a string literal is not read", False,
           g.reads(g.strip_comments('log("x = %d", n);'), "x"))
    expect("code outside the string still counts", True,
           g.reads(g.strip_comments('log("x = %d", x);'), "x"))
    # A non-unique definition must resolve to nothing rather than to one of them.
    idx = {"dup": [("a.c", 1), ("b.c", 1)], "sole": [("c.c", 1)]}
    expect("a symbol defined twice has no body", None, g.body(idx, "dup"))
    # touches_via(): the struct qualifier must actually discriminate.
    rel = "col_rel_t *r = x; r->base_nrows = r->nrows;"
    arr = "col_diff_arrangement_t *arr = x; arr->base_nrows = arr->current;"
    expect("a col_rel_t root attributes col_rel_t::base_nrows", True,
           g.touches_via(rel, "col_rel_t", "base_nrows", True))
    expect("an arrangement root does not", False,
           g.touches_via(arr, "col_rel_t", "base_nrows", True))
    # Vary the direction as well.  With both assertions asking write=True, the
    # write/read comparison inside touches_via could be `if True` and a body
    # that writes one struct's field while reading another's would be credited
    # to whichever the row named.
    expect("a write is not a read of the same root", False,
           g.touches_via(rel, "col_rel_t", "base_nrows", False))
    read_only = "col_rel_t *r = x; if (r->base_nrows > 0) return 1;"
    expect("a read is attributed as a read", True,
           g.touches_via(read_only, "col_rel_t", "base_nrows", False))
    expect("a read is not a write", False,
           g.touches_via(read_only, "col_rel_t", "base_nrows", True))
    # A subscripted root, and a const-qualified declaration: both are real
    # spellings in this tree and both were free to stop working unnoticed.
    expect("a subscripted root is attributed", True,
           g.touches_via("col_rel_t *rs; rs[0]->base_nrows = 0;",
                         "col_rel_t", "base_nrows", True))
    expect("a const-qualified root is attributed", True,
           g.touches_via("col_rel_t *const r = x; r->base_nrows = 0;",
                         "col_rel_t", "base_nrows", True))
    # declares() is what decides whether an unattributed toucher is another
    # struct's or an unclassifiable one.  Left unasserted it could be
    # `return False` and the fail-closed branch would never fire.
    expect("declares() sees a pointer declaration", True,
           g.declares("col_rel_t *r = x;", "col_rel_t"))
    expect("declares() sees a const pointer", True,
           g.declares("const col_rel_t * const r = x;", "col_rel_t"))
    expect("declares() does not see an unrelated struct", False,
           g.declares("col_diff_arrangement_t *a = x;", "col_rel_t"))
    # The fail-closed partition.  No (struct, field) pair in this tree yields an
    # unclear body, so a document fixture cannot reach this branch at all and a
    # mutant that made it return silently survived every other case.
    bodies = {
        "owns": "col_rel_t *r = x; r->base_nrows = 0;",
        "other": "col_diff_arrangement_t *a = x; a->base_nrows = 0;",
        "chained": "col_rel_t *r = x; (void)r; e->owner->base_nrows = 0;",
    }
    owned, unclear = g.partition_by_struct(
        bodies, set(bodies), "col_rel_t", "base_nrows", True)
    expect("the declared root is attributed", {"owns"}, owned)
    expect("another struct's root is dropped", False, "other" in unclear)
    expect("a chained root is unclear, not silently dropped", {"chained"},
           unclear)
    # And pin what a "chained root" actually means here: the match anchors on
    # the last name before the field, so a chain ending in a declared pointer IS
    # attributed.  The doc once said a chained root reaches nothing the gate
    # recognises, which is false for this shape.
    last_link = "col_rel_t *rel; entry->rel->base_nrows = 0;"
    expect("a chain ending in a declared pointer is attributed", True,
           g.touches_via(last_link, "col_rel_t", "base_nrows", True))
    # The other unreachable guard: a symbol that gained a second definition and
    # touches a mapped field would leave the expected set silently, after which
    # the gate would demand its removal from a cell that is right.
    multi = {"twice": ("has_evaluated = true;", "int x;"),
             "elsewhere": ("int y;", "int z;")}
    expect("a multiply-defined toucher is reported", ["twice"],
           g.ambiguous_touchers(multi, "has_evaluated", True))
    expect("a multiply-defined non-toucher is not", [],
           g.ambiguous_touchers(multi, "delta_seeded", True))
    # The ambiguity guard's real input.  Neutering ambiguous_bodies() to {}
    # passed the gate and every assertion, because the wiring case substitutes
    # the consumer and never reaches the producer.
    # Pin the delegation: re-inlining a local masker here would pass every
    # behavioural assertion below and quietly make this the repository's third
    # implementation again.
    index = importlib.util.module_from_spec(
        importlib.util.spec_from_file_location("wl_index_for_test", g.INDEX))
    index.__spec__.loader.exec_module(index)
    for stage in ("_mask_comments_and_strings", "_mask_preprocessor",
                  "_mask_attributes"):
        expect(f"the index still exports {stage}", True, hasattr(index, stage))
    # Identity, not behaviour: a verbatim copy of the three stages inside this
    # gate reproduced the old one-line comparison and survived the whole suite,
    # so that assertion pinned nothing.  The closure must hold the module this
    # gate loaded.
    cells = getattr(g._masker(), "__closure__", None) or ()
    held = [c.cell_contents for c in cells]
    expect("the gate's masker closes over the index module", True,
           any(getattr(m, "_mask_comments_and_strings", None) is not None
               and m.__spec__.origin == str(g.INDEX) for m in held))
    # And on a real file, not a twenty-character line on which two of the three
    # stages are no-ops.
    corpus = (g.REPO / "wirelog/columnar/session.c").read_text(
        encoding="utf-8", errors="replace")
    expect("the gate masks a real file exactly as the index does", True,
           g.strip_comments(corpus) == index._mask_attributes(
               index._mask_preprocessor(
                   index._mask_comments_and_strings(corpus))))

    expect("balanced braces pass", True, g.braces_balance("void f(void) { }"))
    expect("a truncated body is caught", False,
           g.braces_balance("void f(void) { if (x) {"))
    ambig = g.ambiguous_bodies()
    expect("the tree really has multiply-defined symbols", True, len(ambig) > 0)
    idx = g.function_index()
    expect("each is keyed to one body per definition", True,
           all(len(v) == len(idx[k]) >= 2 for k, v in ambig.items()))
    # The string mask is observable only through writes(): a format string
    # naming a field contains a literal assignment.
    expect("an assignment inside a string literal is not a write", False,
           g.writes(g.strip_comments('log("x = %d", n);'), "x"))


def extraction_cases(g) -> None:
    """one_body() against small C files, which no document fixture can reach.

    This is the change a whole round of review turned on and it had no test: a
    faithful revert to counting braces before stripping passed the gate and
    every case here.  Measured, the revert changes no body in this tree at all
    -- 5 of 1766 carry a brace inside a comment or string and all 5 balance
    within their own body, so no span moves.  These fixtures are therefore the
    only thing between that revert and a silent pass, which is the point: an
    earlier draft of this docstring claimed a blast radius of 310 bodies, a
    number taken from a review report rather than measured, and it does not
    reproduce.

    Each fixture puts a brace where it does not belong and asserts the walk
    still ends on the definition it started in -- in both directions, because
    the direction that ends the walk LATE is the silent one: the swallowed text
    balances, so braces_balance cannot see it, and the first function is
    credited with the second's write.
    """
    fixtures = {
        "brace_open_comment.c": (
            "void first(void)\n{\n    /* a stray { in a comment */\n"
            "    int a = 0;\n    (void)a;\n}\n\n"
            "void second(void)\n{\n    has_evaluated = true;\n}\n",
            "first", "has_evaluated", False),
        "brace_close_comment.c": (
            "void third(void)\n{\n    /* a stray } in a comment */\n"
            "    delta_seeded = true;\n}\n",
            "third", "delta_seeded", True),
        "brace_open_char.c": (
            "void fourth(void)\n{\n    char c = '{';\n    (void)c;\n}\n\n"
            "void fifth(void)\n{\n    has_evaluated = true;\n}\n",
            "fourth", "has_evaluated", False),
        "brace_close_char.c": (
            "void sixth(void)\n{\n    char c = '}';\n"
            "    delta_seeded = true;\n}\n",
            "sixth", "delta_seeded", True),
        "brace_in_string.c": (
            'void seventh(void)\n{\n    log("{");\n}\n\n'
            'void eighth(void)\n{\n    has_evaluated = true;\n}\n',
            "seventh", "has_evaluated", False),
        # The write is on the SAME line as the string: with the write below it
        # this fixture passed under the deleted regex stripper too, because a
        # line comment never reached the next line, so it discriminated nothing.
        "slashes_in_string.c": (
            'void ninth(void)\n{\n    log("http://x"); delta_seeded = true;\n}\n',
            "ninth", "delta_seeded", True),
        # The index blanks preprocessor lines and this gate composes that stage,
        # so both read the same text; with only the first stage the write below
        # would be visible.
        "preprocessor.c": (
            "void tenth(void)\n{\n#define WL_TMP_X has_evaluated = true;\n"
            "    int a = 0;\n    (void)a;\n}\n",
            "tenth", "has_evaluated", False),
        # The attribute stage, which six files under wirelog/ need, session.c
        # among them.  Dropping it changes no verdict in this tree, so only a
        # fixture keeps it in the composition.
        "attribute.c": (
            "void eleventh(void)\n{\n"
            "    int q __attribute__((aligned(delta_seeded = 1)));\n"
            "    (void)q;\n}\n",
            "eleventh", "delta_seeded", False),
        # And the ORDER: running the attribute stage before the literals are
        # masked lets an `__attribute__((` inside a string paren-match forward
        # and swallow the write after it, together with the closing brace.
        "attribute_in_string.c": (
            'void twelfth(void)\n{\n'
            '    log("__attribute__((x)"); delta_seeded = true;\n}\n',
            "twelfth", "delta_seeded", True),
    }
    real_repo = g.REPO
    with tempfile.TemporaryDirectory(prefix="wirelog-extract.") as tmp:
        d = Path(tmp)
        for name, (text, _, _, _) in fixtures.items():
            (d / name).write_text(text, encoding="utf-8")
        g.REPO = d
        # Keyed on the path string, so each fixture needs its own file name.
        g.stripped_lines.cache_clear()
        try:
            for name, (_, fn, field, want) in fixtures.items():
                try:
                    got = g.writes(g.one_body(name, 1), field)
                except SystemExit:
                    got = "the gate died"
                expect(f"{fn}() in {name} sees {field}: {want}", want, got)
        finally:
            g.REPO = real_repo
            g.stripped_lines.cache_clear()


def main() -> int:
    real = DOC.read_text(encoding="utf-8")
    if SECTION not in real:
        print("test-check-state-map-anchors: FAIL: the real document has no"
              f" {SECTION!r}, so these cases would prove nothing",
              file=sys.stderr)
        return 1

    global GATE_MOD
    GATE_MOD = load_gate()
    unit_cases(GATE_MOD)
    extraction_cases(GATE_MOD)

    with tempfile.TemporaryDirectory(prefix="wirelog-state-map.") as tmp:
        d = Path(tmp)
        seq = [0]

        # The positive control: unmodified, the gate must pass.  Without it a
        # checker that always failed would score every case below as a pass.
        good = d / "good.md"
        good.write_text(real, encoding="utf-8")
        expect("the unmodified document resolves", 0, run(good))
        # The one case that goes through the command line, so a gate that no
        # longer runs as a script cannot pass every other case in-process.
        expect("the unmodified document resolves on the command line", 0,
               run_cli(good))
        broken = d / "broken.md"
        broken.write_text(real.replace("`session_note_inserted_input`",
                                       "`session_note_inserted_input_gone`"),
                          encoding="utf-8")
        expect("a bad document fails on the command line too", 1,
               run_cli(broken))

        # Every case below edits a table cell, because only table rows carry
        # attributions.  An earlier draft mutated the prose around the table and
        # scored the gate's silence as a pass.
        def row_edit(name: str, old: str, new: str, want: int = 1) -> None:
            if real.count(old) != 1:
                expect(f"{name} [anchor matched {real.count(old)} times]",
                       "one anchor", f"{real.count(old)} anchors")
                return
            seq[0] += 1
            f = d / f"case{seq[0]}.md"
            f.write_text(real.replace(old, new), encoding="utf-8")
            expect(name, want, run(f))

        # A mistyped or renamed anchor must not slip through.
        # Anchored on the cell, not the bare name: the prose above the table
        # names this function too, and row_edit refuses an ambiguous anchor.
        row_edit("an unresolvable anchor fails",
                 "| `col_session_create_internal`, `col_session_remove`,",
                 "| `col_session_create_internal_renamed`,"
                 " `col_session_remove`,")

        # A symbol defined more than once must fail closed rather than resolve
        # to an arbitrary one of its definitions.
        row_edit("a symbol with several definitions fails closed",
                 "`col_session_create_internal`, ", "`wl_mutex_lock`, ")

        # Right name, wrong place: `expect` is defined in this very repository,
        # but only under tests/, which the library symbol index does not carry.
        row_edit("a symbol defined only under tests/ fails",
                 "`session_note_inserted_input` | `col_eval_stratum_tdd_recursive`",
                 "`expect` | `col_eval_stratum_tdd_recursive`")

        # The attribution check, read column: a function that exists, is
        # uniquely defined, and never touches the field beside it.
        row_edit("a real function that never reads the field fails",
                 "| `col_eval_stratum_tdd_recursive`, `col_session_snapshot_impl`,",
                 "| `arr_build_full`, `col_eval_stratum_tdd_recursive`,"
                 " `col_session_snapshot_impl`,")

        # The same for the write column, so `writes()` is not free to be
        # `return True` at this level either.
        row_edit("a real function that never writes the field fails",
                 "| `col_session_insert`, `col_session_snapshot_impl`,",
                 "| `arr_build_full`, `col_session_insert`,"
                 " `col_session_snapshot_impl`,")

        # The qualifier on `col_rel_t::base_nrows` is load-bearing:
        # `col_diff_arrangement_reset_delta` is, in full,
        # `arr->base_nrows = arr->current_nrows;` and touches no col_rel_t.  A
        # gate that dropped the qualifier credited it as a writer.
        # The write column is the one that ends at `..._deep_copy_governed` and
        # is followed by the read column; the read column ends at the verdict.
        row_edit("an arrangement function is not a col_rel_t writer",
                 "| `col_rel_t::base_nrows` | `col_rel_compact_impl`,",
                 "| `col_rel_t::base_nrows` | `col_diff_arrangement_reset_delta`,"
                 " `col_rel_compact_impl`,")
        row_edit("an arrangement function is not a col_rel_t reader",
                 "`wl_columnar_relation_deep_copy_governed` |"
                 " `col_rel_compact_impl`,",
                 "`wl_columnar_relation_deep_copy_governed` |"
                 " `col_diff_arrangement_has_delta`, `col_rel_compact_impl`,")

        # Completeness: dropping a real writer must fail.  Soundness alone would
        # let a cell be trimmed to whatever is convenient.
        row_edit("a dropped writer fails",
                 "`col_session_create_internal`, `col_session_remove`,",
                 "`col_session_remove`,")

        # And emptying a column outright must fail, not pass with less to check.
        row_edit("an emptied column fails",
                 "| `col_session_insert`, `col_session_snapshot_impl`,"
                 " `col_session_step_impl`, `session_note_inserted_input` |",
                 "| — |")

        # The two fail-closed guards are wired to main(), not merely present.
        # Nothing in this tree makes either fire -- no multiply-defined symbol
        # touches a mapped field, and no body declares a struct without
        # reaching the field through it -- so a mutant that deleted the call
        # site survived every document fixture.  Substituting the predicate is
        # the only way to reach the wiring without editing the source tree.
        def clear_caches():
            # Every memoised result downstream of the stubbed function, or the
            # stub is installed after the value it would have changed is already
            # cached.  The braces_balance case failed exactly that way: bodies
            # were built before the stub went in, so one_body never ran again.
            for cached in ("attributions", "all_bodies", "ambiguous_bodies"):
                getattr(GATE_MOD, cached).cache_clear()

        def with_stub(attr, stub, want, name):
            real_fn = getattr(GATE_MOD, attr)
            setattr(GATE_MOD, attr, stub)
            clear_caches()
            try:
                rc, err = run_message(good)
                expect(name, (1, True), (rc, want in err))
            finally:
                setattr(GATE_MOD, attr, real_fn)
                clear_caches()

        with_stub("braces_balance", lambda src: False,
                  "braces do not balance",
                  "an unbalanced extracted body stops the gate")
        with_stub("ambiguous_touchers", lambda multi, field, write: ["forced"],
                  "forced: defined more than once",
                  "a reported ambiguous toucher stops the gate")
        with_stub("partition_by_struct",
                  lambda bodies, bare, struct, field, write: (set(), {"forced"}),
                  "cannot tell which struct's",
                  "a reported unclear body stops the gate")

        # A qualifier no body declares would otherwise leave both expected
        # sets empty, so a row with two empty columns passed with exit 0 while
        # still counting toward the reported field total.
        qual = d / "qualifier.md"
        row = next(l for l in real.splitlines()
                   if l.startswith("| `has_evaluated` |"))
        qual.write_text(real.replace(
            row, "| `col_session_t::has_evaluated` | — | — | PRESERVE |"),
            encoding="utf-8")
        # By the message, not the code: this fixture also changes the subject
        # set, so the SUBJECTS pin objects first and the row would exit 1 even
        # with this guard deleted.
        expect("an unrecognised struct qualifier fails",
               (1, True),
               (lambda r: (r[0], "resolves to nothing" in r[1]))(
                   run_message(qual)))

        # And a qualifier that IS declared but is the wrong owner must reach the
        # fail-closed branch rather than narrow the row silently.
        row_edit("a qualifier that cannot partition the row fails",
                 "| `col_rel_t::base_nrows` |",
                 "| `wl_col_session_t::base_nrows` |")

        # Dropping a reader must fail for the same reason dropping a writer
        # does; the read direction was implemented but unasserted.
        row_edit("a dropped reader fails",
                 "| `col_eval_stratum_tdd_recursive`,"
                 " `col_session_snapshot_impl`, `col_session_step_impl`,"
                 " `session_note_inserted_input` |",
                 "| `col_eval_stratum_tdd_recursive`,"
                 " `col_session_snapshot_impl`, `col_session_step_impl` |")

        # An anchor ADDED rather than substituted is caught only by the
        # unresolvable-symbol branch, because completeness still sees every real
        # function named.  Every other case removes something as well.
        row_edit("an added unresolvable anchor fails",
                 "| `col_session_create_internal`,",
                 "| `wl_mutex_lock`, `col_session_create_internal`,")

        # A table with rows but no backticked subject must fail closed.  This
        # reaches the no-subject guard, which the empty-section fixture cannot:
        # that one is caught by the no-rows guard first.  So deleting the
        # no-subject guard alone is caught, here; deleting the no-rows guard
        # alone is not, because the no-subject guard then catches the empty
        # section instead.  Deleting both is caught.
        nosubj = d / "nosubject.md"
        nosubj.write_text(
            real[:real.index(SECTION)] + SECTION + "\n\n"
            "| field | written by | read by | verdict |\n"
            "|---|---|---|---|\n"
            "| has_evaluated | col_session_step_impl | none | PRESERVE |\n",
            encoding="utf-8")
        # Also by the message: with every subject unbackticked the pin reports
        # thirteen missing rows, which would mask a deleted no-subject guard.
        expect("a table whose subjects lost their backticks fails closed",
               (1, True),
               (lambda r: (r[0], "no row named a field" in r[1]))(
                   run_message(nosubj)))

        # Emptying a cell already failed; deleting the row did not, because a
        # shorter table simply had less to check.  SUBJECTS pins the scope.
        dropped = d / "droppedrow.md"
        row = next(l for l in real.splitlines()
                   if l.startswith("| `delta_seeded` |"))
        dropped.write_text(real.replace(row + "\n", ""), encoding="utf-8")
        expect("a deleted row fails",
               (1, True),
               (lambda r: (r[0], "drop it from SUBJECTS" in r[1]))(
                   run_message(dropped)))

        # Renaming a subject trips both halves of the SUBJECTS check, so it
        # cannot tell them apart; adding a row trips only the "not in SUBJECTS"
        # half, which otherwise survived deletion.
        row_edit("a subject outside SUBJECTS fails",
                 "| `snapshot_stable_valid` |", "| `snapshot_stale_valid` |")
        # The added row must be sound and complete on its own, or it fails for
        # a different reason and proves nothing: a field name that appears
        # nowhere has no writers and no readers, so empty columns are correct
        # and only the SUBJECTS check can object.
        added = d / "addedrow.md"
        row = next(l for l in real.splitlines()
                   if l.startswith("| `has_evaluated` |"))
        added.write_text(
            real.replace(row, row + "\n| `not_a_session_field_xyz` | — | — |"
                               " PRESERVE |"),
            encoding="utf-8")
        expect("a row for a field outside SUBJECTS fails",
               (1, True),
               (lambda r: (r[0], "add it there if the scope really grew" in r[1]))(
                   run_message(added)))

        # Two rows for one subject satisfied both halves of the set comparison.
        dup = d / "duplicate.md"
        drow = next(l for l in real.splitlines()
                    if l.startswith("| `has_evaluated` |"))
        dup.write_text(real.replace(drow, drow + "\n" + drow), encoding="utf-8")
        expect("a duplicated row fails", 1, run(dup))

        # By the message: the fixture names a second field that has its own
        # row, so the duplicate-row check added later fails it too and the case
        # passed with this guard deleted.
        multi = d / "multisubject.md"
        multi.write_text(
            real.replace("| `last_inserted_relation` |",
                         "| `last_inserted_relation` and `has_evaluated` |"),
            encoding="utf-8")
        expect("a subject naming two fields fails",
               (1, True),
               (lambda r: (r[0], "must name exactly one field" in r[1]))(
                   run_message(multi)))

        # A sibling ### section must not be folded into this one.  Under a
        # boundary that stopped only at "## ", the poisoned table below would be
        # scanned and the gate would fail -- so passing here is the assertion.
        sibling = d / "sibling.md"
        sibling.write_text(
            real.rstrip("\n")
            + "\n\n### Some later section\n\n"
              "| field | written by | read by | verdict |\n"
              "|---|---|---|---|\n"
              "| `has_evaluated` | `arr_build_full` | `arr_build_full` | no |\n",
            encoding="utf-8")
        expect("a later ### section is not folded in", 0, run(sibling))

        # Deleting the section must fail rather than pass with nothing to check.
        gone = d / "gone.md"
        gone.write_text(real[:real.index(SECTION)], encoding="utf-8")
        # By the message: with the section gone the no-subjects guard also
        # returns 1, so the exit code alone left this die() with no killing
        # mutant -- the one guard in this file not asserted the way the others
        # are. The section name as well as "not in": the SUBJECTS-extra message
        # also contains that phrase, and is unreachable from this fixture only
        # because every other table in the file is narrower than four columns.
        # The pin earns its place against two mutants an exit code cannot tell
        # apart -- one that rewords this message while keeping the guard, and
        # one that leaves `start` at zero so the scan runs from the top of the
        # file and a later check returns 1 instead.  A mutant that leaves
        # `start` as None reaches no later check at all: the next line adds to
        # it and raises.
        expect("a missing section fails closed",
               (1, True),
               (lambda r: (r[0], f"{SECTION!r} not in" in r[1]))(
                   run_message(gone)))

        # So must a section that names nothing.
        empty = d / "empty.md"
        empty.write_text(real[:real.index(SECTION)] + SECTION
                         + "\n\nNothing here names a symbol.\n",
                         encoding="utf-8")
        expect("a section naming no symbol fails closed", 1, run(empty))

    if failures:
        print(f"test-check-state-map-anchors: {failures} case(s) failed",
              file=sys.stderr)
        return 1
    print("test-check-state-map-anchors: all cases passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
