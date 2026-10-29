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
#include "../wirelog/columnar/internal.h"
#include "../wirelog/intern.h"
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

static void
test_arithmetic_precedence_results(void)
{
    static const struct {
        const char *expression;
        int64_t expected;
    } cases[] = {
        { "x + y * z", 14 }, { "x - y * z", 2 },
        { "x + y / z", 9 }, { "x - y / z", 7 },
        { "x + y % z", 9 }, { "x - y % z", 7 },
        { "x * y + z", 26 }, { "x / y - z", 0 },
        { "x % y + z", 4 }, { "x - y - z", 3 },
        { "24 / y / z", 4 }, { "24 / y * z", 16 },
        { "x % y * z", 4 }, { "x * y % z", 0 },
        { "20 - y * 4 + 18 / y % 4", 10 },
        { "x + -3 * z", 2 }, { "-8 + y * z", -2 },
        { "band(x + y * z, 15)", 14 },
        { "min(x + y * z)", 14 },
        { "sum(x + y * z)", 14 },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        TEST(cases[i].expression);
        char source[256];
        snprintf(source, sizeof(source),
            ".decl a(x: int64, y: int64, z: int64)\n"
            "a(8, 3, 2).\n.decl r(value: int64)\n"
            "r(%s) :- a(x, y, z).\n", cases[i].expression);
        struct result_ctx out;
        int n = run_program(source, "r", &out);
        if (n != 1 || out.ncols[0] != 1
            || out.rows[0][0] != cases[i].expected) {
            FAIL("wrong arithmetic result");
            continue;
        }
        PASS();
    }
}

static void
test_arithmetic_precedence_filters(void)
{
    static const char *conditions[] = {
        "x + y * z = 14", "14 = x + y * z",
        "x + y * z < 20", "20 > x + y * z",
        "8 + y * z = 14", "band(x, 15) + y * z = 14",
        "strlen(\"12345678\") + y * z = 14",
    };
    for (size_t i = 0; i < sizeof(conditions) / sizeof(conditions[0]); i++) {
        TEST(conditions[i]);
        char source[320];
        snprintf(source, sizeof(source),
            ".decl a(id: int64, x: int64, y: int64, z: int64)\n"
            "a(1, 8, 3, 2). a(2, 8, 3, 4).\n"
            ".decl r(id: int64)\nr(id) :- a(id, x, y, z), %s.\n",
            conditions[i]);
        struct result_ctx out;
        int n = run_program(source, "r", &out);
        if (n != 1 || out.ncols[0] != 1 || out.rows[0][0] != 1) {
            FAIL("expected only row id 1, not id 2");
            continue;
        }
        PASS();
    }
}

static void
test_float_arithmetic_precedence(void)
{
    TEST("float MAP and FILTER use multiplicative precedence");
    const char *source =
        ".decl a(x: float, y: float, z: float)\n"
        "a(8.0, 3.0, 2.0). a(8.0, 3.0, 4.0).\n"
        ".decl r(value: float)\n"
        "r(x + y * z) :- a(x, y, z), x + y * z = 14.0.\n";
    struct result_ctx out;
    int n = run_program(source, "r", &out);
    double value = 0.0;
    if (n == 1)
        memcpy(&value, &out.rows[0][0], sizeof(value));
    if (n != 1 || out.ncols[0] != 1 || value != 14.0) {
        FAIL("expected the single float result 14.0");
        return;
    }
    PASS();
}

static uint32_t string_operand(uint8_t *out, uint8_t tag, const char *text)
{
    uint16_t size = (uint16_t)strlen(text);
    out[0] = tag; memcpy(out + 1, &size, 2); memcpy(out + 3, text, size);
    return 3u + size;
}
static void test_string_result_classification(void)
{
    TEST("pure complete STRING/CAT classification and malformed refusal");
    wirelog_column_type_t source[] = {WIRELOG_TYPE_STRING, WIRELOG_TYPE_INT64};
    wirelog_column_type_t result = WIRELOG_TYPE_BOOL;
    uint8_t expression[2048];
    uint32_t size = string_operand(expression, WL_PLAN_EXPR_VAR, "col0");
    size += string_operand(expression+size, WL_PLAN_EXPR_CONST_STR, "(");
    expression[size++] = WL_PLAN_EXPR_STR_FN_CAT;
    size += string_operand(expression+size, WL_PLAN_EXPR_CONST_STR, "");
    expression[size++] = WL_PLAN_EXPR_STR_FN_CAT;
    if (!wl_columnar_expr_interpreted_result_type(expression, size, source, 2,
        &result) || result != WIRELOG_TYPE_STRING){
        FAIL("nested CAT type missing"); return;
    }
    for (unsigned variant = 0; variant < 13; variant++) {
        uint8_t bad[2048]; memcpy(bad, expression, size); uint32_t count = size;
        if (variant == 0) bad[count++] = WL_PLAN_EXPR_EXTENSION_CALL;
        if (variant == 1) bad[count++] = WL_PLAN_EXPR_CONST_STR;
        if (variant == 2) count = 2; /* Truncated length. */
        if (variant == 3) count = 6; /* Truncated column name. */
        if (variant == 4) {
            bad[0] = WL_PLAN_EXPR_VAR_STRING; bad[6] = '1';
        }                                                                     /* Actual INT64 lane. */
        if (variant == 5) bad[6] = '2'; /* Out of range. */
        if (variant == 6) bad[0] = WL_PLAN_EXPR_VAR_FLOAT;
        if (variant == 7) {
            bad[0] = WL_PLAN_EXPR_STR_FN_CAT; count = 1;
        }
        if (variant == 8) count += string_operand(bad+count,
                    WL_PLAN_EXPR_CONST_STR, "extra");
        if (variant == 9) bad[count-1] = WL_PLAN_EXPR_STR_FN_TO_UPPER;
        if (variant == 10) count = string_operand(bad, WL_PLAN_EXPR_VAR,
                    "col4294967296");
        if (variant == 11) count = string_operand(bad, WL_PLAN_EXPR_VAR,
                    "col00000000000000001");
        if (variant == 12) bad[3] = 'x';
        result = WIRELOG_TYPE_BOOL;
        if (wl_columnar_expr_interpreted_result_type(bad, count, source, 2,
            &result) || result != WIRELOG_TYPE_BOOL){
            FAIL("malformed/unknown expression classified"); return;
        }
    }
    expression[0] = WL_PLAN_EXPR_VAR_STRING;
    if (!wl_columnar_expr_interpreted_result_type(expression, size, source, 2,
        &result) ||
        wl_columnar_expr_interpreted_result_type(expression, size, NULL, 2,
        &result)){
        FAIL("explicit STRING reference provenance"); return;
    }
    for (unsigned extra = 0; extra < 2; extra++) {
        size = 0;
        for (unsigned i = 0; i < COL_FILTER_STACK + extra; i++)
            size += string_operand(expression+size, WL_PLAN_EXPR_CONST_STR, "");
        for (unsigned i = 1; i < COL_FILTER_STACK + extra;
            i++) expression[size++] = WL_PLAN_EXPR_STR_FN_CAT;
        if (wl_columnar_expr_interpreted_result_type(expression, size, source,
            2, &result) != (extra == 0)){
            FAIL("operand stack bound"); return;
        }
    }
    PASS();
}

static void test_builtin_result_contracts(void)
{
    TEST("all fixed builtin result types, arities and opaque refusal");
    const struct { uint8_t tag; unsigned arity; wirelog_column_type_t type;
    } cases[] = {
        {WL_PLAN_EXPR_ARITH_ADD, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_SUB, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_MUL, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_DIV, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_MOD, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_BAND, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_BOR, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_BXOR, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_SHL, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_SHR, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_HMAC_SHA256, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_UUID5, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_HMAC_SHA256_SS, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_HMAC_SHA256_SI, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_HMAC_SHA256_IS, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_UUID5_SS, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_UUID5_SI, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_UUID5_IS, 2, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_BNOT, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_HASH, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_CRC32_ETH, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_CRC32_CAST, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_MD5, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_SHA1, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_SHA256, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_SHA512, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_HASH_S, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_CRC32_ETH_S, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_CRC32_CAST_S, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_MD5_S, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_SHA1_S, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_SHA256_S, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_SHA512_S, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_STR_FN_STRLEN, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_STR_FN_STR_ORD, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_STR_FN_TO_NUMBER, 1, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_UUID4, 0, WIRELOG_TYPE_INT64},
        {WL_PLAN_EXPR_ARITH_FLOAT_ADD, 2, WIRELOG_TYPE_FLOAT},
        {WL_PLAN_EXPR_ARITH_FLOAT_SUB, 2, WIRELOG_TYPE_FLOAT},
        {WL_PLAN_EXPR_ARITH_FLOAT_MUL, 2, WIRELOG_TYPE_FLOAT},
        {WL_PLAN_EXPR_ARITH_FLOAT_DIV, 2, WIRELOG_TYPE_FLOAT},
        {WL_PLAN_EXPR_CMP_EQ, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_NEQ, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_LT, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_GT, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_LTE, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_GTE, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_FLOAT_EQ, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_FLOAT_NEQ, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_FLOAT_LT, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_FLOAT_GT, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_FLOAT_LTE, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_FLOAT_GTE, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_STR_EQ, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_STR_NEQ, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_STR_LT, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_STR_GT, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_STR_LTE, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_CMP_STR_GTE, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_STR_FN_CONTAINS, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_STR_FN_STR_PREFIX, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_STR_FN_STR_SUFFIX, 2, WIRELOG_TYPE_BOOL},
        {WL_PLAN_EXPR_STR_FN_TO_UPPER, 1, WIRELOG_TYPE_STRING},
        {WL_PLAN_EXPR_STR_FN_TO_LOWER, 1, WIRELOG_TYPE_STRING},
        {WL_PLAN_EXPR_STR_FN_TRIM, 1, WIRELOG_TYPE_STRING},
        {WL_PLAN_EXPR_STR_FN_TO_STRING, 1, WIRELOG_TYPE_STRING},
        {WL_PLAN_EXPR_STR_FN_CAT, 2, WIRELOG_TYPE_STRING},
        {WL_PLAN_EXPR_STR_FN_UUID5_RFC, 2, WIRELOG_TYPE_STRING},
        {WL_PLAN_EXPR_STR_FN_SUBSTR, 3, WIRELOG_TYPE_STRING},
        {WL_PLAN_EXPR_STR_FN_STR_REPLACE, 3, WIRELOG_TYPE_STRING},
    };
    uint8_t expression[1024]; wirelog_column_type_t result;
    for (unsigned i = 0; i < sizeof(cases)/sizeof(cases[0]); i++) {
        uint32_t size = 0;
        for (unsigned j = 0; j < cases[i].arity; j++) {
            expression[size++] = WL_PLAN_EXPR_CONST_INT;
            memset(expression+size, 0, 8); size += 8;
        }
        expression[size++] = cases[i].tag;
        result = WIRELOG_TYPE_UINT64;
        if (!wl_columnar_expr_interpreted_result_type(expression, size, NULL, 0,
            &result) || result != cases[i].type){
            FAIL("builtin result contract"); return;
        }
        if (cases[i].arity &&
            wl_columnar_expr_interpreted_result_type(expression+9, size-9, NULL,
            0, &result)){
            FAIL("operator underflow"); return;
        }
        /* Every STRING producer must keep a direct key in a zero-row MAP.
         * Even crypto tags are classified without invoking execution. */
        if (cases[i].type == WIRELOG_TYPE_STRING) {
            col_rel_t *input = col_rel_new_auto("in", 1),
                *output = col_rel_new_auto("out", 2);
            wl_col_session_t session = {0}; session.intern = wl_intern_create();
            wirelog_column_type_t string_type = WIRELOG_TYPE_STRING;
            uint8_t key[32];
            uint32_t key_size = string_operand(key, WL_PLAN_EXPR_VAR, "col0");
            wl_plan_expr_buffer_t exprs[] = {{.data = key, .size = key_size},
                                             {.data = expression,
                                              .size = size}};
            uint32_t indices[] = {0, 0};
            wl_plan_op_t map = {.op = WL_PLAN_OP_MAP, .project_count = 2,
                                .project_indices = indices, .map_expr_count = 2,
                                .map_exprs = exprs};
            bool ok = input && output && session.intern &&
                col_rel_set_column_types(input, &string_type, 1) == 0 &&
                col_op_map_output_types(&map, input, &session, NULL,
                    output) == 0 && output->column_types &&
                output->column_types[0] == WIRELOG_TYPE_STRING &&
                output->column_types[1] == WIRELOG_TYPE_STRING &&
                wl_intern_count(session.intern) == 0 && !output->nrows;
            col_rel_destroy(input); col_rel_destroy(output);
            wl_intern_free(session.intern);
            if (!ok) {
                FAIL("empty mixed MAP type or classifier side effect"); return;
            }
        }
    }
    const uint8_t opaque[] = {WL_PLAN_EXPR_EXTENSION_CALL,
                              WL_PLAN_EXPR_EXTENSION_CALL_ABI,
                              WL_PLAN_EXPR_AGG_COUNT, WL_PLAN_EXPR_AGG_SUM,
                              WL_PLAN_EXPR_AGG_MIN, WL_PLAN_EXPR_AGG_MAX,
                              WL_PLAN_EXPR_AGG_AVG, 0xff};
    for (unsigned i = 0; i < sizeof(opaque); i++) {
        uint32_t size = string_operand(expression, WL_PLAN_EXPR_CONST_STR,
                "uninterned");
        expression[size++] = opaque[i];
        expression[size++] = WL_PLAN_EXPR_STR_FN_TO_UPPER;
        result = WIRELOG_TYPE_UINT64;
        if (wl_columnar_expr_interpreted_result_type(expression, size, NULL, 0,
            &result) || result != WIRELOG_TYPE_UINT64){
            FAIL("opaque prefix rescued by known suffix"); return;
        }
    }
    /* Nullary UUID4 must respect the stack bound as a push. */
    memset(expression, WL_PLAN_EXPR_ARITH_UUID4, COL_FILTER_STACK+1);
    uint32_t size = COL_FILTER_STACK+1;
    for (unsigned i = 1; i < COL_FILTER_STACK+1;
        i++) expression[size++] = WL_PLAN_EXPR_ARITH_ADD;
    if (wl_columnar_expr_interpreted_result_type(expression, size, NULL, 0,
        &result)){
        FAIL("nullary stack overflow"); return;
    }
    const uint8_t values[] = {WL_PLAN_EXPR_CONST_INT, WL_PLAN_EXPR_CONST_FLOAT,
                              WL_PLAN_EXPR_BOOL};
    const wirelog_column_type_t expected[] = {WIRELOG_TYPE_INT64,
                                              WIRELOG_TYPE_FLOAT,
                                              WIRELOG_TYPE_BOOL};
    for (unsigned i = 0; i < 3; i++) {
        expression[0] = values[i]; memset(expression+1, 0, 8);
        size = i == 2 ? 2 : 9;
        if (!wl_columnar_expr_interpreted_result_type(expression, size, NULL, 0,
            &result) || result != expected[i]) {
            FAIL("constant type"); return;
        }
        for (uint32_t n = 0; n < size;
            n++) if (wl_columnar_expr_interpreted_result_type(expression, n,
                NULL, 0, &result)) {
                FAIL("truncated constant"); return;
            }
    }
    PASS();
}

static void test_empty_string_map_schema(void)
{
    TEST(
        "empty MAP keeps direct STRING beside interpreted CAT; unknown stays untyped");
    col_rel_t *input = col_rel_new_auto("input", 1);
    col_rel_t *output = col_rel_new_auto("output", 2);
    wl_col_session_t session = {0};
    wirelog_column_type_t types[] = {WIRELOG_TYPE_STRING};
    uint8_t direct[32], cat[64];
    uint32_t a = string_operand(direct, WL_PLAN_EXPR_VAR, "col0");
    memcpy(cat, direct, a); uint32_t b = a;
    b += string_operand(cat+b, WL_PLAN_EXPR_CONST_STR, "suffix");
    cat[b++] = WL_PLAN_EXPR_STR_FN_CAT;
    wl_plan_expr_buffer_t expressions[] = {{.data = direct, .size = a},
                                           {.data = cat, .size = b}};
    uint32_t indices[] = {0, 0};
    wl_plan_op_t map = {.op = WL_PLAN_OP_MAP, .project_count = 2,
                        .project_indices = indices, .map_expr_count = 2,
                        .map_exprs = expressions};
    bool ok = input && output && col_rel_set_column_types(input, types,
            1) == 0 &&
        col_op_map_output_types(&map, input, &session, NULL, output) == 0 &&
        output->column_types &&
        output->column_types[0] == WIRELOG_TYPE_STRING &&
        output->column_types[1] == WIRELOG_TYPE_STRING && !output->nrows;
    col_rel_destroy(output); output = col_rel_new_auto("unknown", 2);
    cat[b-1] = WL_PLAN_EXPR_EXTENSION_CALL;
    ok = ok && output && col_op_map_output_types(&map, input, &session, NULL,
            output) == 0 && !output->column_types;
    col_rel_destroy(output); col_rel_destroy(input);
    if (!ok) {
        FAIL("empty type publication or unknown fallback"); return;
    }
    PASS();
}

int
main(void)
{
    printf("=== wirelog Pre-compiled Expression Evaluator Tests (Issue #298)"
        " ===\n\n");

    test_string_result_classification();
    test_empty_string_map_schema();
    test_builtin_result_contracts();
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

    printf("\n--- Arithmetic precedence (Issue #1353) ---\n");
    test_arithmetic_precedence_results();
    test_arithmetic_precedence_filters();
    test_float_arithmetic_precedence();

    printf("\n=== Results: %d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf(" ===\n");

    return tests_failed > 0 ? 1 : 0;
}
