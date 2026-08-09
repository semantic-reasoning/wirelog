/*
 * test_input_arity.c - .input CSV arity validation tests (#977, #985)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * The built-in CSV adapter auto-detects the column count of an
 * integer-only file from its first line.  The caller then inserts the
 * rows at the *declared* arity of the relation, so a file whose width
 * disagrees with the .decl is packed at one stride and read back at
 * another: a heap over-read when the file is narrower than the .decl,
 * silently re-strided tuples when it is wider.
 *
 * Since #985 that stride is the relation's PHYSICAL width -- an `inline`
 * compound column expands to its full arity of slots -- so the declared
 * column count and the file's field count are no longer the same number for
 * every relation.  Cases 6 through 9 cover that.
 *
 * Cases 1 through 8 drive the real pipeline (parse -> plan -> load .input ->
 * evaluate) through wl_run_pipeline so both the columnar insert path
 * and the printed results are exercised.  Case 9 is the other CSV entry an
 * embedder has -- wirelog_load_facts_from_csv(), which computes its width
 * and column types itself and never builds an io_ctx -- so it is driven
 * through the public executor API instead.
 */

#include "../wirelog/cli/driver.h"
#include "../wirelog/intern.h"
#include "../wirelog/wirelog.h"

#include "test_tmpdir.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Helpers                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                            \
        do {                                      \
            tests_run++;                          \
            printf("  [%d] %s", tests_run, name); \
        } while (0)

#define PASS()                 \
        do {                       \
            tests_passed++;        \
            printf(" ... PASS\n"); \
        } while (0)

#define FAIL(msg)                         \
        do {                                  \
            tests_failed++;                   \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)

static int
write_text_file(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    if (!f)
        return -1;
    if (text && text[0] != '\0')
        fputs(text, f);
    fclose(f);
    return 0;
}

/*
 * Run the full pipeline on @src, capturing printed tuples into @outpath.
 * On success (*out_text != NULL) the caller must free(*out_text).
 * Returns the wl_run_pipeline return code, or -999 if the capture file
 * could not be opened.
 */
static int
run_pipeline_capture(const char *src, const char *outpath, char **out_text)
{
    *out_text = NULL;

    FILE *f = fopen(outpath, "w");
    if (!f)
        return -999;

    int rc = wl_run_pipeline(src, 1, false, false, 0, f);
    fclose(f);

    *out_text = wl_read_file(outpath);
    return rc;
}

static int
count_substr(const char *hay, const char *needle)
{
    int count = 0;
    const char *p = hay;
    while (p && (p = strstr(p, needle)) != NULL) {
        count++;
        p++;
    }
    return count;
}

/* ======================================================================== */
/* Case 1: narrow integer CSV against a wider .decl                         */
/* ======================================================================== */

/*
 * 200-row, 1-column CSV loaded into an 8-column relation.  Before the
 * fix the adapter detects ncols=1, allocates 256*1 int64 and the caller
 * inserts 200 rows at stride 8 -> heap-buffer-overflow READ of size 8 in
 * col_rel_row_copy_in (caught by ASan; silent garbage otherwise).
 * The load must fail instead.
 */
static void
test_narrow_csv_rejected(void)
{
    TEST("narrow integer CSV vs wider .decl is rejected");

    char csv_path[512];
    char out_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_arity_narrow.csv");
    test_tmppath(out_path, sizeof(out_path), "wl_arity_narrow_out.txt");

    /* 200 single-column rows */
    char csv[2048];
    size_t off = 0;
    for (int i = 0; i < 200; i++) {
        int n = snprintf(csv + off, sizeof(csv) - off, "%d\n", i);
        if (n < 0 || (size_t)n >= sizeof(csv) - off) {
            FAIL("csv buffer too small");
            return;
        }
        off += (size_t)n;
    }
    if (write_text_file(csv_path, csv) != 0) {
        FAIL("cannot write CSV fixture");
        return;
    }

    char src[2048];
    snprintf(src, sizeof(src),
        ".decl wide(a: int32, b: int32, c: int32, d: int32,"
        " e: int32, f: int32, g: int32, h: int32)\n"
        ".input wide(filename=\"%s\", delimiter=\",\")\n"
        ".decl seen(a: int32, b: int32, c: int32, d: int32,"
        " e: int32, f: int32, g: int32, h: int32)\n"
        "seen(A, B, C, D, E, F, G, H) :- "
        "wide(A, B, C, D, E, F, G, H).\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if (rc == 0) {
        FAIL("expected non-zero rc for 1-column CSV vs 8-column .decl");
        free(output);
        return;
    }

    free(output);
    PASS();
}

/* ======================================================================== */
/* Case 2: wide integer CSV against a narrower .decl                        */
/* ======================================================================== */

/*
 * A 3-column CSV loaded into a 2-column relation.  Before the fix this
 * exits 0 after emitting re-strided tuples (row 0 becomes (1,2), row 1
 * becomes (3,4) ...), silently corrupting the EDB.  The load must fail.
 */
static void
test_wide_csv_rejected(void)
{
    TEST("wide integer CSV vs narrower .decl is rejected");

    char csv_path[512];
    char out_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_arity_wide.csv");
    test_tmppath(out_path, sizeof(out_path), "wl_arity_wide_out.txt");

    if (write_text_file(csv_path, "1,2,3\n4,5,6\n7,8,9\n") != 0) {
        FAIL("cannot write CSV fixture");
        return;
    }

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl edge(x: int32, y: int32)\n"
        ".input edge(filename=\"%s\", delimiter=\",\")\n"
        ".decl reach(x: int32, y: int32)\n"
        "reach(X, Y) :- edge(X, Y).\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if (rc == 0) {
        char msg[256];
        snprintf(msg, sizeof(msg),
            "expected non-zero rc for 3-column CSV vs 2-column .decl; "
            "got 0 with output:\n%s", output ? output : "(null)");
        free(output);
        FAIL(msg);
        return;
    }

    free(output);
    PASS();
}

/* ======================================================================== */
/* Case 3: positive control - matching integer widths still evaluate        */
/* ======================================================================== */

static void
test_matching_width_evaluates(void)
{
    TEST("matching-width integer CSV loads and evaluates");

    char csv_path[512];
    char out_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_arity_match.csv");
    test_tmppath(out_path, sizeof(out_path), "wl_arity_match_out.txt");

    if (write_text_file(csv_path, "1,2\n2,3\n3,4\n") != 0) {
        FAIL("cannot write CSV fixture");
        return;
    }

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl edge(x: int32, y: int32)\n"
        ".input edge(filename=\"%s\", delimiter=\",\")\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(X, Y) :- edge(X, Y).\n"
        "tc(X, Z) :- tc(X, Y), edge(Y, Z).\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if (rc != 0 || !output) {
        char msg[128];
        snprintf(msg, sizeof(msg), "wl_run_pipeline returned %d", rc);
        free(output);
        FAIL(msg);
        return;
    }

    /* Transitive closure of the 1->2->3->4 chain: exactly six tuples. */
    static const char *expected[] = {
        "tc(1, 2)", "tc(1, 3)", "tc(1, 4)",
        "tc(2, 3)", "tc(2, 4)", "tc(3, 4)",
    };
    int ok = (count_substr(output, "tc(") == 6);
    for (size_t i = 0; ok && i < sizeof(expected) / sizeof(expected[0]); i++)
        ok = (count_substr(output, expected[i]) == 1);

    if (!ok) {
        char msg[512];
        snprintf(msg, sizeof(msg), "unexpected tc tuples:\n%s", output);
        free(output);
        FAIL(msg);
        return;
    }

    free(output);
    PASS();
}

/* ======================================================================== */
/* Case 4: positive control - string relations use the guarded path         */
/* ======================================================================== */

static void
test_symbol_relation_unaffected(void)
{
    TEST("symbol-bearing CSV still loads via the guarded path");

    char csv_path[512];
    char out_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_arity_sym.csv");
    test_tmppath(out_path, sizeof(out_path), "wl_arity_sym_out.txt");

    if (write_text_file(csv_path, "alice,bob\nbob,charlie\n") != 0) {
        FAIL("cannot write CSV fixture");
        return;
    }

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl parent(x: symbol, y: symbol)\n"
        ".input parent(filename=\"%s\", delimiter=\",\")\n"
        ".decl ancestor(x: symbol, y: symbol)\n"
        "ancestor(X, Y) :- parent(X, Y).\n"
        "ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if (rc != 0 || !output) {
        char msg[128];
        snprintf(msg, sizeof(msg), "wl_run_pipeline returned %d", rc);
        free(output);
        FAIL(msg);
        return;
    }

    static const char *expected[] = {
        "ancestor(\"alice\", \"bob\")",
        "ancestor(\"alice\", \"charlie\")",
        "ancestor(\"bob\", \"charlie\")",
    };
    int ok = (count_substr(output, "ancestor(") == 3);
    for (size_t i = 0; ok && i < sizeof(expected) / sizeof(expected[0]); i++)
        ok = (count_substr(output, expected[i]) == 1);

    if (!ok) {
        char msg[512];
        snprintf(msg, sizeof(msg), "unexpected ancestor tuples:\n%s", output);
        free(output);
        FAIL(msg);
        return;
    }

    free(output);
    PASS();
}

/* ======================================================================== */
/* Case 5: empty .input file remains legal                                  */
/* ======================================================================== */

/*
 * An empty file yields nrows == 0 and ncols == 0 with rc == 0.  There is
 * no row to mis-stride, and rejecting it would break programs whose
 * .input file is legitimately empty, so the arity check must not fire.
 */
static void
test_empty_csv_still_loads(void)
{
    TEST("empty .input CSV still loads (no rows, no error)");

    char csv_path[512];
    char out_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_arity_empty.csv");
    test_tmppath(out_path, sizeof(out_path), "wl_arity_empty_out.txt");

    if (write_text_file(csv_path, "") != 0) {
        FAIL("cannot write CSV fixture");
        return;
    }

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl edge(x: int32, y: int32)\n"
        ".input edge(filename=\"%s\", delimiter=\",\")\n"
        ".decl reach(x: int32, y: int32)\n"
        "reach(X, Y) :- edge(X, Y).\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if (rc != 0) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "empty .input file must still load; rc = %d", rc);
        free(output);
        FAIL(msg);
        return;
    }

    if (output && count_substr(output, "reach(") != 0) {
        char msg[512];
        snprintf(msg, sizeof(msg),
            "expected no reach tuples from empty input:\n%s", output);
        free(output);
        FAIL(msg);
        return;
    }

    free(output);
    PASS();
}

/* ======================================================================== */
/* Case 6: inline-compound .decl is fed at its PHYSICAL width (#985)        */
/* ======================================================================== */

/*
 * `.decl inp(id: int64, p: pair/2 inline, s: symbol)` is three declared
 * columns and four physical ones, so its `.input` file has four fields.
 *
 * Pre-fix wirelog_io_ctx_create_for_relation() published num_cols == 3 and
 * a three-entry col_types, so the reader consumed the first three fields
 * with the *third* declared type -- symbol -- applied to the compound's
 * second slot.  The 4-field file loaded with rc 0 and evaluated to
 * `outr(1, 7, 1, "pair")`: the 8 dropped, "aa" never read, `1` the intern
 * id minted for the string "8", and `"pair"` the reverse-intern of the
 * unwritten fourth slot's 0, which happened to be the functor name.  Four
 * columns, none of them the file's.
 *
 * This is therefore a wrong-answer test, not an error-message test, and it
 * asserts the values.  A three-field file for the same .decl (the shape a
 * logical reading of the header would suggest) is Case 7.
 */
static void
test_inline_compound_input_uses_physical_width(void)
{
    TEST("inline-compound .decl loads its .input at the physical width");

    char csv_path[512];
    char out_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_arity_inline.csv");
    test_tmppath(out_path, sizeof(out_path), "wl_arity_inline_out.txt");

    if (write_text_file(csv_path, "1,7,8,aa\n") != 0) {
        FAIL("cannot write CSV fixture");
        return;
    }

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl inp(id: int64, p: pair/2 inline, s: symbol)\n"
        ".input inp(filename=\"%s\", delimiter=\",\")\n"
        ".decl outr(a: int64, b: int64, c: int64, d: symbol)\n"
        ".output outr\n"
        "outr(A, B, C, D) :- inp(A, B, C, D).\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if (rc != 0 || !output) {
        char msg[128];
        snprintf(msg, sizeof(msg), "wl_run_pipeline returned %d", rc);
        free(output);
        FAIL(msg);
        return;
    }

    if (count_substr(output, "outr(") != 1
        || count_substr(output, "outr(1, 7, 8, \"aa\")") != 1) {
        char msg[512];
        snprintf(msg, sizeof(msg),
            "expected exactly outr(1, 7, 8, \"aa\"); got:\n%s", output);
        free(output);
        FAIL(msg);
        return;
    }

    free(output);
    PASS();
}

/* ======================================================================== */
/* Case 7: under-width .input against an inline-compound .decl             */
/* ======================================================================== */

/*
 * The same relation fed a three-field file -- one field per *declared*
 * column, which is exactly the mistake the old num_cols invited.  Pre-fix
 * this loaded with rc 0 and printed the same fabricated
 * `outr(1, 7, 1, "pair")` as the 4-field file did.
 *
 * The absence of any output line is asserted alongside the non-zero rc: a
 * change that keeps the load rejected but prints a partially-loaded row
 * first would otherwise pass on the rc alone.
 */
static void
test_inline_compound_under_width_input_rejected(void)
{
    TEST("under-width .input vs an inline-compound .decl is rejected");

    char csv_path[512];
    char out_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_arity_inline_narrow.csv");
    test_tmppath(out_path, sizeof(out_path),
        "wl_arity_inline_narrow_out.txt");

    if (write_text_file(csv_path, "1,7,8\n") != 0) {
        FAIL("cannot write CSV fixture");
        return;
    }

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl inp(id: int64, p: pair/2 inline, s: symbol)\n"
        ".input inp(filename=\"%s\", delimiter=\",\")\n"
        ".decl outr(a: int64, b: int64, c: int64, d: symbol)\n"
        ".output outr\n"
        "outr(A, B, C, D) :- inp(A, B, C, D).\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if (rc == 0) {
        char msg[512];
        snprintf(msg, sizeof(msg),
            "expected non-zero rc for a 3-field CSV vs a 4-column-physical "
            ".decl; got 0 with output:\n%s", output ? output : "(null)");
        free(output);
        FAIL(msg);
        return;
    }

    if (output && count_substr(output, "outr(") != 0) {
        char msg[512];
        snprintf(msg, sizeof(msg),
            "a rejected load must print nothing; got:\n%s", output);
        free(output);
        FAIL(msg);
        return;
    }

    free(output);
    PASS();
}

/* ======================================================================== */
/* Case 8: regression - a plain relation's num_cols stays logical           */
/* ======================================================================== */

/*
 * No compound column anywhere, so the physical width must equal the
 * declared one and a two-field file must still load.
 *
 * This case kills no mutant that the rest of the file does not already
 * kill.  Measured, mutating the per-column sum in
 * wl_ir_relation_physical_width() three ways:
 *
 *   phys += col->compound_arity                    Cases 3,4,5,6,8,9 fail
 *   phys += (kind == INLINE) ? 1 : compound_arity  Cases 3,4,5,6,8,9 fail
 *   phys += (kind != NONE) ? compound_arity : 1    all 9 pass
 *
 * The first two collapse every plain column to 0 -- compound_arity is 0
 * when there is no compound -- so they take this case down in company
 * rather than alone.  The third miscounts only `side` compounds, which no
 * case in this file declares; it is caught in tests/test_program.c by
 * test_fact_arity_side_compound_is_one_column() and
 * test_head_arity_compound_handle_form_rejected().  So nothing in the
 * "over-counting helper" class is uniquely caught here.
 *
 * What it is, and why it stays: a no-op pin on the path 99% of relations
 * take.  wl_ir_relation_physical_width() is the identity on a
 * compound-free relation, and this asserts that end to end -- through the
 * io_ctx, the adapter, and the columnar insert -- rather than by reading
 * the helper.  It is the tested form of the "no in-tree program changes
 * behaviour" claim in the CHANGELOG, it costs one CSV file and one join,
 * and it fails loudly if the physical width ever stops being derived from
 * columns[] and starts being stored or guessed.
 */
static void
test_plain_relation_width_unchanged(void)
{
    TEST("plain 2-column relation still loads a 2-field .input");

    char csv_path[512];
    char out_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_arity_plain.csv");
    test_tmppath(out_path, sizeof(out_path), "wl_arity_plain_out.txt");

    if (write_text_file(csv_path, "1,2\n2,3\n") != 0) {
        FAIL("cannot write CSV fixture");
        return;
    }

    char src[1024];
    snprintf(src, sizeof(src),
        ".decl edge(x: int64, y: int64)\n"
        ".input edge(filename=\"%s\", delimiter=\",\")\n"
        ".decl reach(x: int64, y: int64)\n"
        ".output reach\n"
        "reach(X, Y) :- edge(X, Y).\n",
        csv_path);

    char *output = NULL;
    int rc = run_pipeline_capture(src, out_path, &output);

    remove(csv_path);
    remove(out_path);

    if (rc != 0 || !output) {
        char msg[128];
        snprintf(msg, sizeof(msg), "wl_run_pipeline returned %d", rc);
        free(output);
        FAIL(msg);
        return;
    }

    if (count_substr(output, "reach(") != 2
        || count_substr(output, "reach(1, 2)") != 1
        || count_substr(output, "reach(2, 3)") != 1) {
        char msg[512];
        snprintf(msg, sizeof(msg),
            "expected exactly reach(1, 2) and reach(2, 3); got:\n%s", output);
        free(output);
        FAIL(msg);
        return;
    }

    free(output);
    PASS();
}

/* ======================================================================== */
/* Case 9: the public embedder entry -- wirelog_load_facts_from_csv (#985)  */
/* ======================================================================== */

/*
 * `wirelog_load_facts_from_csv()` (api_facade.c) is the CSV entry an
 * embedder reaches with no `.input` directive and no adapter registry
 * involved, and it builds its own width and its own col_types array rather
 * than going through wirelog_io_ctx_create_for_relation().  Both of those
 * are separate code from anything Cases 6-8 touch.
 *
 * The relation carries a trailing `symbol` column *after* the inline
 * compound, which is what makes the type array observable: the expansion
 * has to shift `s`'s STRING type from logical position 2 to physical
 * position 3.  Two independent reverts are killed here:
 *
 *   - the width.  Back at rel->column_count the reader is asked for 3
 *     columns, the 4-field file trips the per-line width check in
 *     wl_csv_read_file_via_ctx(), and the load returns false.
 *   - the type expansion.  Without the shift, physical slot 2 is typed
 *     STRING and slot 3 int64, so "8" is interned as a string and "aa" is
 *     handed to the integer parser.
 *
 * The relation is declared without `.input` so the load under test is the
 * explicit public call, not the executor's eager seeding.
 */
static void
test_public_api_csv_inline_compound(void)
{
    TEST("wirelog_load_facts_from_csv: inline compound + trailing symbol");

    char csv_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wl_arity_api_inline.csv");

    if (write_text_file(csv_path, "1,7,8,aa\n2,70,80,bb\n") != 0) {
        FAIL("cannot write CSV fixture");
        return;
    }

    static const char *src
        = ".decl inp(id: int64, p: pair/2 inline, s: symbol)\n"
        ".decl outr(a: int64, b: int64, c: int64, d: symbol)\n"
        ".output outr\n"
        "outr(A, B, C, D) :- inp(A, B, C, D).\n";

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        remove(csv_path);
        FAIL("wirelog_parse_string failed");
        return;
    }

    wirelog_executor_t *exec = wirelog_executor_create(prog, &err);
    if (!exec) {
        wirelog_program_free(prog);
        remove(csv_path);
        FAIL("wirelog_executor_create failed");
        return;
    }

    bool ok = wirelog_load_facts_from_csv(exec, "inp", csv_path, &err);
    remove(csv_path);
    if (!ok) {
        char msg[160];
        snprintf(msg, sizeof(msg),
            "load of a 4-field CSV into a 4-column-physical relation "
            "returned false (err=%d)", (int)err);
        wirelog_executor_free(exec);
        wirelog_program_free(prog);
        FAIL(msg);
        return;
    }

    wirelog_result_t *res = wirelog_evaluate(exec, &err);
    if (!res) {
        wirelog_executor_free(exec);
        wirelog_program_free(prog);
        FAIL("wirelog_evaluate returned NULL");
        return;
    }

    const wirelog_intern_t *in = wirelog_program_get_intern(prog);
    int64_t aa = in ? wl_intern_get(in, "aa") : -1;
    int64_t bb = in ? wl_intern_get(in, "bb") : -1;
    uint64_t rows = wirelog_result_relation_cardinality(res, "outr");
    const int64_t *data
        = (const int64_t *)wirelog_result_get_relation(res, "outr");

    int bad = 0;
    char msg[256];
    msg[0] = '\0';

    if (aa < 0 || bb < 0) {
        snprintf(msg, sizeof(msg),
            "the symbol column was not interned: aa=%lld bb=%lld",
            (long long)aa, (long long)bb);
        bad = 1;
    } else if (rows != 2 || !data) {
        snprintf(msg, sizeof(msg), "expected 2 outr rows, got %llu",
            (unsigned long long)rows);
        bad = 1;
    } else {
        /* Row order is not part of the contract; match either way. */
        const int64_t *r0 = &data[0];
        const int64_t *r1 = &data[4];
        if (r0[0] == 2) {
            const int64_t *t = r0;
            r0 = r1;
            r1 = t;
        }
        if (r0[0] != 1 || r0[1] != 7 || r0[2] != 8 || r0[3] != aa
            || r1[0] != 2 || r1[1] != 70 || r1[2] != 80 || r1[3] != bb) {
            snprintf(msg, sizeof(msg),
                "expected outr(1,7,8,\"aa\") and outr(2,70,80,\"bb\"); got "
                "(%lld,%lld,%lld,%lld) and (%lld,%lld,%lld,%lld)",
                (long long)r0[0], (long long)r0[1], (long long)r0[2],
                (long long)r0[3], (long long)r1[0], (long long)r1[1],
                (long long)r1[2], (long long)r1[3]);
            bad = 1;
        }
    }

    wirelog_result_free(res);
    wirelog_executor_free(exec);
    wirelog_program_free(prog);

    if (bad) {
        FAIL(msg);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== test_input_arity (Issues #977, #985) ===\n");

    test_narrow_csv_rejected();
    test_wide_csv_rejected();
    test_matching_width_evaluates();
    test_symbol_relation_unaffected();
    test_empty_csv_still_loads();
    test_inline_compound_input_uses_physical_width();
    test_inline_compound_under_width_input_rejected();
    test_plain_relation_width_unchanged();
    test_public_api_csv_inline_compound();

    printf("\n=== Results: %d run, %d passed, %d failed ===\n",
        tests_run, tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
