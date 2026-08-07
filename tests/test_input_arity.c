/*
 * test_input_arity.c - .input CSV arity validation tests (Issue #977)
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
 * These tests drive the real pipeline (parse -> plan -> load .input ->
 * evaluate) through wl_run_pipeline so both the columnar insert path
 * and the printed results are exercised.
 */

#include "../wirelog/cli/driver.h"

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
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== test_input_arity (Issue #977) ===\n");

    test_narrow_csv_rejected();
    test_wide_csv_rejected();
    test_matching_width_evaluates();
    test_symbol_relation_unaffected();
    test_empty_csv_still_loads();

    printf("\n=== Results: %d run, %d passed, %d failed ===\n",
        tests_run, tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
