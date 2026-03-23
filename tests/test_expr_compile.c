/*
 * test_expr_compile.c - Pre-compiled expression evaluator tests (Issue #298)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * TDD RED phase: end-to-end tests that verify the pre-compiled expression
 * evaluator produces correct results for FILTER and MAP operators.
 * The optimization pre-compiles bytecode once before the row loop to
 * eliminate per-row strtol() calls on variable names.
 *
 * Tests focus on expressions that previously fell through to the slow path:
 *   arithmetic + comparison: c1*10+n1 > c2*10+n2  (CRDT pattern)
 *   arithmetic map:          x*10+y                (computed column)
 *
 * These tests PASS once the col_expr_compile / col_eval_expr_compiled
 * optimization is implemented in wirelog/columnar/ops.c.
 */

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Helpers                                                              */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                      \
        do {                                \
            tests_run++;                    \
            printf("  [TEST] %-60s", name); \
            fflush(stdout);                 \
        } while (0)

#define PASS()             \
        do {                   \
            tests_passed++;    \
            printf(" PASS\n"); \
        } while (0)

#define FAIL(msg)                   \
        do {                            \
            tests_failed++;             \
            printf(" FAIL: %s\n", msg); \
        } while (0)

/* ======================================================================== */
/* Result Capture                                                            */
/* ======================================================================== */

#define MAX_ROWS  64
#define MAX_NCOLS 8

struct result_ctx {
    int64_t rows[MAX_ROWS][MAX_NCOLS];
    uint32_t ncols[MAX_ROWS];
    uint32_t count;
};

static void
capture_rows(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct result_ctx *ctx = (struct result_ctx *)user_data;
    (void)relation;
    if (ctx->count >= MAX_ROWS)
        return;
    uint32_t n = ncols < MAX_NCOLS ? ncols : MAX_NCOLS;
    ctx->ncols[ctx->count] = n;
    for (uint32_t i = 0; i < n; i++)
        ctx->rows[ctx->count][i] = row[i];
    ctx->count++;
}

/* ======================================================================== */
/* Helper: run a program and return all rows of a named relation            */
/* ======================================================================== */

static int
run_program(const char *src, const char *rel_name, struct result_ctx *out)
{
    (void)rel_name; /* capture_rows gets all output relations */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_t *sess = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    if (wl_session_load_facts(sess, prog) != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    out->count = 0;
    if (wl_session_snapshot(sess, capture_rows, out) != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return (int)out->count;
}

/* ======================================================================== */
/* Test 1: CRDT arithmetic filter -- c1*10+n1 > c2*10+n2                   */
/* ======================================================================== */

/*
 * Test that filter with arithmetic comparison (the CRDT hot path) correctly
 * retains rows where c1*10+n1 > c2*10+n2 and discards the rest.
 *
 * Input rows:
 *   (2, 5, 1, 8):  2*10+5=25 > 1*10+8=18  -> passes
 *   (1, 3, 1, 5):  1*10+3=13 > 1*10+5=15  -> fails (filtered out)
 *   (3, 0, 2, 9):  3*10+0=30 > 2*10+9=29  -> passes
 *
 * Expected output: 2 rows — (2,5,1,8) and (3,0,2,9)
 */
static void
test_filter_crdt_arithmetic(void)
{
    TEST("CRDT filter: c1*10+n1 > c2*10+n2 retains correct rows");

    const char *src =
        ".decl data(c1: int64, n1: int64, c2: int64, n2: int64)\n"
        "data(2, 5, 1, 8).\n"
        "data(1, 3, 1, 5).\n"
        "data(3, 0, 2, 9).\n"
        ".decl result(c1: int64, n1: int64, c2: int64, n2: int64)\n"
        "result(c1, n1, c2, n2) :- data(c1, n1, c2, n2),"
        " c1 * 10 + n1 > c2 * 10 + n2.\n";

    struct result_ctx out;
    int n = run_program(src, "result", &out);

    if (n < 0) {
        FAIL("evaluation failed");
        return;
    }
    if (n != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 2 rows, got %d", n);
        FAIL(buf);
        return;
    }

    /* Verify both passing rows are present (order may vary) */
    int found_row1 = 0, found_row2 = 0;
    for (int i = 0; i < n; i++) {
        if (out.rows[i][0] == 2 && out.rows[i][1] == 5)
            found_row1 = 1;
        if (out.rows[i][0] == 3 && out.rows[i][1] == 0)
            found_row2 = 1;
    }
    if (!found_row1 || !found_row2) {
        FAIL("wrong rows in output");
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Test 2: Arithmetic filter with constant -- x*2+3 > 10                   */
/* ======================================================================== */

/*
 * Filter: keep rows where x*2+3 > 10, i.e., x > 3.5, so x >= 4.
 *
 * Input: x = 2, 4, 6, 1
 * Expected output: x = 4, 6
 */
static void
test_filter_arithmetic_vs_constant(void)
{
    TEST("Arithmetic filter: x*2+3 > 10 keeps x >= 4");

    const char *src =
        ".decl a(x: int64)\n"
        "a(2). a(4). a(6). a(1).\n"
        ".decl r(x: int64)\n"
        "r(x) :- a(x), x * 2 + 3 > 10.\n";

    struct result_ctx out;
    int n = run_program(src, "r", &out);

    if (n < 0) {
        FAIL("evaluation failed");
        return;
    }
    if (n != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 2 rows, got %d", n);
        FAIL(buf);
        return;
    }

    int found4 = 0, found6 = 0;
    for (int i = 0; i < n; i++) {
        if (out.rows[i][0] == 4)
            found4 = 1;
        if (out.rows[i][0] == 6)
            found6 = 1;
    }
    if (!found4 || !found6) {
        FAIL("wrong rows in output");
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Test 3: MAP with arithmetic expression -- r(x*10+y) :- a(x,y)           */
/* ======================================================================== */

/*
 * Map operator computes x*10+y for each row.
 *
 * Input: (2,5), (3,7)
 * Expected output: 25, 37
 */
static void
test_map_arithmetic_expr(void)
{
    TEST("Map: r(x*10+y) :- a(x,y) computes correct values");

    const char *src =
        ".decl a(x: int64, y: int64)\n"
        "a(2, 5).\n"
        "a(3, 7).\n"
        ".decl r(z: int64)\n"
        "r(x * 10 + y) :- a(x, y).\n";

    struct result_ctx out;
    int n = run_program(src, "r", &out);

    if (n < 0) {
        FAIL("evaluation failed");
        return;
    }
    if (n != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 2 rows, got %d", n);
        FAIL(buf);
        return;
    }

    int found25 = 0, found37 = 0;
    for (int i = 0; i < n; i++) {
        if (out.rows[i][0] == 25)
            found25 = 1;
        if (out.rows[i][0] == 37)
            found37 = 1;
    }
    if (!found25 || !found37) {
        char buf[64];
        int64_t v0 = n > 0 ? out.rows[0][0] : -1;
        int64_t v1 = n > 1 ? out.rows[1][0] : -1;
        snprintf(buf, sizeof(buf), "got %" PRId64 ", %" PRId64, v0, v1);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Test 4: All rows pass arithmetic filter                                  */
/* ======================================================================== */

static void
test_filter_all_pass(void)
{
    TEST("Arithmetic filter: all rows pass when condition is always true");

    const char *src =
        ".decl a(x: int64)\n"
        "a(1). a(2). a(3).\n"
        ".decl r(x: int64)\n"
        "r(x) :- a(x), x * 1 + 0 > 0.\n"; /* always x > 0 for positive x */

    struct result_ctx out;
    int n = run_program(src, "r", &out);

    if (n != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 rows, got %d", n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Test 5: No rows pass arithmetic filter                                   */
/* ======================================================================== */

static void
test_filter_none_pass(void)
{
    TEST("Arithmetic filter: no rows pass when condition is always false");

    const char *src =
        ".decl a(x: int64)\n"
        "a(1). a(2). a(3).\n"
        ".decl r(x: int64)\n"
        "r(x) :- a(x), x * 0 + 0 > 5.\n"; /* always 0 > 5 = false */

    struct result_ctx out;
    int n = run_program(src, "r", &out);

    if (n != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0 rows, got %d", n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Test 6: CRDT double-arithmetic filter (laterSibling2 pattern)           */
/* ======================================================================== */

/*
 * Models the laterSibling2 rule body:
 *   c1*10+n1 > c2*10+n2,
 *   c2*10+n2 > c3*10+n3.
 *
 * Input 6-column rows (c1,n1,c2,n2,c3,n3):
 *   (3,0, 2,9, 2,5):  30>29 && 29>25 -> passes
 *   (3,0, 2,5, 2,9):  30>25 but 25>29 false -> fails
 *   (2,5, 1,8, 1,0):  25>18 && 18>10 -> passes
 *
 * Expected: 2 rows pass.
 */
static void
test_filter_crdt_double_arithmetic(void)
{
    TEST("CRDT double filter: c1*10+n1>c2*10+n2 && c2*10+n2>c3*10+n3");

    const char *src =
        ".decl data(c1: int64, n1: int64, c2: int64,"
        " n2: int64, c3: int64, n3: int64)\n"
        "data(3, 0, 2, 9, 2, 5).\n"
        "data(3, 0, 2, 5, 2, 9).\n"
        "data(2, 5, 1, 8, 1, 0).\n"
        ".decl result(c1: int64, n1: int64)\n"
        "result(c1, n1) :- data(c1, n1, c2, n2, c3, n3),"
        " c1 * 10 + n1 > c2 * 10 + n2,"
        " c2 * 10 + n2 > c3 * 10 + n3.\n";

    struct result_ctx out;
    int n = run_program(src, "result", &out);

    if (n < 0) {
        FAIL("evaluation failed");
        return;
    }
    if (n != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 2 rows, got %d", n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Test 7: Multiple arithmetic filters on same relation                     */
/* ======================================================================== */

/*
 * Two arithmetic filter conditions: x*2 > 6 AND x + y > 10.
 *
 * Input rows (x, y):
 *   (1, 12): x*2=2, 2>6 false  -> fails first condition
 *   (4,  5): x*2=8, 8>6 true; x+y=9, 9>10 false -> fails second
 *   (4,  8): x*2=8, 8>6 true; x+y=12, 12>10 true -> passes
 *   (5,  6): x*2=10,10>6 true; x+y=11, 11>10 true -> passes
 *
 * Expected: 2 rows — (4,8) and (5,6)
 */
static void
test_filter_multiple_arithmetic(void)
{
    TEST("Multiple arithmetic filters: x*2 > 6 AND x+y > 10");

    const char *src =
        ".decl a(x: int64, y: int64)\n"
        "a(1, 12).\n"
        "a(4,  5).\n"
        "a(4,  8).\n"
        "a(5,  6).\n"
        ".decl r(x: int64, y: int64)\n"
        "r(x, y) :- a(x, y), x * 2 > 6, x + y > 10.\n";

    struct result_ctx out;
    int n = run_program(src, "r", &out);

    if (n < 0) {
        FAIL("evaluation failed");
        return;
    }
    if (n != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 2 rows, got %d", n);
        FAIL(buf);
        return;
    }

    int found48 = 0, found56 = 0;
    for (int i = 0; i < n; i++) {
        if (out.rows[i][0] == 4 && out.rows[i][1] == 8)
            found48 = 1;
        if (out.rows[i][0] == 5 && out.rows[i][1] == 6)
            found56 = 1;
    }
    if (!found48 || !found56) {
        FAIL("wrong rows in output");
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
    printf("=== wirelog Pre-compiled Expression Evaluator Tests (Issue #298)"
        " ===\n\n");

    printf("--- FILTER: arithmetic comparison ---\n");
    test_filter_crdt_arithmetic();
    test_filter_arithmetic_vs_constant();
    test_filter_all_pass();
    test_filter_none_pass();

    printf("\n--- MAP: arithmetic expression ---\n");
    test_map_arithmetic_expr();

    printf("\n--- FILTER: complex arithmetic (CRDT patterns) ---\n");
    test_filter_crdt_double_arithmetic();
    test_filter_multiple_arithmetic();

    printf("\n=== Results: %d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf(" ===\n");

    return tests_failed > 0 ? 1 : 0;
}
