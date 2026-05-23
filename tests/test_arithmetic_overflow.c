/*
 * test_arithmetic_overflow.c - arithmetic evaluator overflow hardening (Issue #822)
 *
 * Verifies that postfix arithmetic evaluation fails closed on overflow and
 * division/modulus errors, and that overflow/invalid-arithmetic errors propagate
 * through MAP and REDUCE operators.
 */

#define _GNU_SOURCE

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                      \
        do {                               \
            tests_run++;                    \
            printf("  [TEST] %-60s", name); \
            fflush(stdout);                 \
        } while (0)

#define PASS()             \
        do {                   \
            tests_passed++;    \
            printf(" PASS\n"); \
        } while (0)

#define FAIL(msg)                          \
        do {                                   \
            tests_failed++;                     \
            printf(" FAIL: %s\n", (msg));       \
            return;                             \
        } while (0)

/* ------------------------------------------------------------------ */
/* Snapshot helpers                                                   */
/* ------------------------------------------------------------------ */

#define MAX_ROWS  16
#define MAX_NCOLS 4

struct result_ctx {
    int64_t rows[MAX_ROWS][MAX_NCOLS];
    uint32_t ncols[MAX_ROWS];
    uint32_t count;
};

static void
capture_rows(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    (void)relation;
    struct result_ctx *ctx = (struct result_ctx *)user_data;
    if (ctx->count >= MAX_ROWS)
        return;

    uint32_t copied_cols = ncols < MAX_NCOLS ? ncols : MAX_NCOLS;
    ctx->ncols[ctx->count] = copied_cols;
    for (uint32_t i = 0; i < copied_cols; i++)
        ctx->rows[ctx->count][i] = row[i];
    ctx->count++;
}

static int
run_snapshot(const char *src, struct result_ctx *out)
{
    memset(out, 0, sizeof(*out));

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

    int rc = wl_session_snapshot(sess, capture_rows, out);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc;
}

static void
test_filter_addition_overflow_rejects_row(void)
{
    TEST("compiled filter x + 1 > 0 rejects INT64_MAX");

    const char *src =
        ".decl a(x: int64)\n"
        "a(9223372036854775807).\n"
        ".decl r(x: int64)\n"
        "r(x) :- a(x), x + 1 > 0.\n";

    struct result_ctx out;
    int rc = run_snapshot(src, &out);
    if (rc != 0) {
        FAIL("snapshot returned error");
        return;
    }
    if (out.count != 0) {
        FAIL("expected 0 rows");
        return;
    }
    PASS();
}

static void
test_filter_division_by_zero_rejects_row(void)
{
    TEST("compiled filter x / 0 rejects row");

    const char *src =
        ".decl a(x: int64)\n"
        "a(1).\n"
        ".decl r(x: int64)\n"
        "r(x) :- a(x), x / 0 > 0.\n";

    struct result_ctx out;
    int rc = run_snapshot(src, &out);
    if (rc != 0) {
        FAIL("snapshot returned error");
        return;
    }
    if (out.count != 0) {
        FAIL("expected 0 rows");
        return;
    }
    PASS();
}

static void
test_filter_modulo_by_zero_rejects_row(void)
{
    TEST("compiled filter x % 0 rejects row");

    const char *src =
        ".decl a(x: int64)\n"
        "a(1).\n"
        ".decl r(x: int64)\n"
        "r(x) :- a(x), x % 0 > 0.\n";

    struct result_ctx out;
    int rc = run_snapshot(src, &out);
    if (rc != 0) {
        FAIL("snapshot returned error");
        return;
    }
    if (out.count != 0) {
        FAIL("expected 0 rows");
        return;
    }
    PASS();
}

static void
test_filter_invalid_shift_rejects_row(void)
{
    TEST("compiled filter x << 64 rejects row");

    const char *src =
        ".decl a(x: int64)\n"
        "a(1).\n"
        ".decl r(x: int64)\n"
        "r(x) :- a(x), bshl(x, 64) > 0.\n";

    struct result_ctx out;
    int rc = run_snapshot(src, &out);
    if (rc != 0) {
        FAIL("snapshot returned error");
        return;
    }
    if (out.count != 0) {
        FAIL("expected 0 rows");
        return;
    }
    PASS();
}

static void
test_map_head_overflow_fails_hard(void)
{
    TEST("map head r(x+1) with INT64_MAX returns ERANGE");

    const char *src =
        ".decl a(x: int64)\n"
        "a(9223372036854775807).\n"
        ".decl r(z: int64)\n"
        "r(x + 1) :- a(x).\n";

    struct result_ctx out;
    int rc = run_snapshot(src, &out);
    if (rc != ERANGE) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected ERANGE, got %d", rc);
        FAIL(msg);
        return;
    }
    PASS();
}

static void
test_reduce_sum_accumulation_overflow_fails_hard(void)
{
    TEST("reduce agg sum accumulation overflows in same group");

    const char *src =
        ".decl a(g: int64, x: int64)\n"
        "a(1, 9223372036854775807).\n"
        "a(1, 1).\n"
        ".decl r(g: int64, s: int64)\n"
        "r(g, sum(x)) :- a(g, x).\n";

    struct result_ctx out;
    int rc = run_snapshot(src, &out);
    if (rc != ERANGE) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected ERANGE, got %d", rc);
        FAIL(msg);
        return;
    }
    PASS();
}

static void
test_slow_filter_to_number_expression_rejects_row(void)
{
    TEST("slow filter fallback via to_number handles overflow");

    const char *src =
        ".decl a(x: int64)\n"
        "a(1).\n"
        ".decl r(x: int64)\n"
        "r(x) :- a(x), to_number(\"9223372036854775807\") + 1 > 0.\n";

    struct result_ctx out;
    int rc = run_snapshot(src, &out);
    if (rc != 0) {
        FAIL("snapshot returned error");
        return;
    }
    if (out.count != 0) {
        FAIL("expected 0 rows");
        return;
    }
    PASS();
}

static void
test_filter_to_number_range_error_rejects_row(void)
{
    TEST("filter to_number out-of-range rejects row");

    const char *src =
        ".decl a(x: int64)\n"
        "a(1).\n"
        ".decl r(x: int64)\n"
        "r(x) :- a(x), to_number(\"9223372036854775808\") > 0.\n";

    struct result_ctx out;
    int rc = run_snapshot(src, &out);
    if (rc != 0) {
        FAIL("snapshot returned error");
        return;
    }
    if (out.count != 0) {
        FAIL("expected 0 rows");
        return;
    }
    PASS();
}

static void
test_map_to_number_range_error_fails_hard(void)
{
    TEST("map head to_number out-of-range returns ERANGE");

    const char *src =
        ".decl a(x: int64)\n"
        "a(1).\n"
        ".decl r(z: int64)\n"
        "r(to_number(\"9223372036854775808\")) :- a(x).\n";

    struct result_ctx out;
    int rc = run_snapshot(src, &out);
    if (rc != ERANGE) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected ERANGE, got %d", rc);
        FAIL(msg);
        return;
    }
    PASS();
}

static void
test_reduce_to_number_range_error_fails_hard(void)
{
    TEST("reduce agg to_number out-of-range returns ERANGE");

    const char *src =
        ".decl a(g: int64)\n"
        "a(1).\n"
        ".decl r(g: int64, s: int64)\n"
        "r(g, sum(to_number(\"9223372036854775808\"))) :- a(g).\n";

    struct result_ctx out;
    int rc = run_snapshot(src, &out);
    if (rc != ERANGE) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected ERANGE, got %d", rc);
        FAIL(msg);
        return;
    }
    PASS();
}

static void
test_reduce_expression_overflow_fails_hard(void)
{
    TEST("reduce agg expression overflow returns error");

    const char *src =
        ".decl a(g: int64, x: int64)\n"
        "a(1, 9223372036854775807).\n"
        ".decl r(g: int64, s: int64)\n"
        "r(g, sum(x + 1)) :- a(g, x).\n";

    struct result_ctx out;
    int rc = run_snapshot(src, &out);
    if (rc != ERANGE) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected ERANGE, got %d", rc);
        FAIL(msg);
        return;
    }
    PASS();
}

int
main(void)
{
    printf("=== Arithmetic Overflow Hardening Tests (Issue #822) ===\n\n");

    test_filter_addition_overflow_rejects_row();
    test_filter_division_by_zero_rejects_row();
    test_filter_modulo_by_zero_rejects_row();
    test_filter_invalid_shift_rejects_row();
    test_map_head_overflow_fails_hard();
    test_reduce_sum_accumulation_overflow_fails_hard();
    test_slow_filter_to_number_expression_rejects_row();
    test_filter_to_number_range_error_rejects_row();
    test_map_to_number_range_error_fails_hard();
    test_reduce_to_number_range_error_fails_hard();
    test_reduce_expression_overflow_fails_hard();

    printf("\n=== Results: %d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf(" ===\n");

    return tests_failed > 0 ? 1 : 0;
}
