/*
 * test_tdd_multiworker.c - TDD multi-worker tuple count regression tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Regression tests for Issue #404: multi-worker execution (W=4) must produce
 * the same tuple count as single-worker (W=1) for recursive strata.
 *
 * Root cause was a combination of two budget regressions in commit 6929689:
 *   1. join_output_limit divided by num_workers (per-worker divisor)
 *   2. pre-join backpressure firing too early on worker sessions
 *
 * These tests guard against silent data loss from budget/limit regressions.
 */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Harness                                                             */
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

/* ======================================================================== */
/* Helpers                                                                  */
/* ======================================================================== */

struct count_ctx {
    int64_t count;
};

static void
count_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct count_ctx *ctx = (struct count_ctx *)user_data;
    ctx->count++;
    (void)relation;
    (void)row;
    (void)ncols;
}

/*
 * Run transitive closure on a 20-node chain graph with the given number of
 * workers.  Returns the tuple count for the tc relation, or -1 on error.
 *
 * Graph: 1->2, 2->3, ..., 19->20  (19 edges)
 * TC should produce n*(n-1)/2 = 190 tuples for a 20-node chain.
 */
static int64_t
run_tc_chain20(uint32_t num_workers)
{
    /* Build the Datalog program */
    const char *src =
        ".decl edge(x: int32, y: int32)\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    if (rc != 0 || !plan)
        return -1;

    wl_session_t *session = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        return -1;
    }

    /* Insert 19 edges forming a 20-node chain */
    for (int i = 1; i <= 19; i++) {
        int64_t row[2] = { i, i + 1 };
        rc = wl_session_insert(session, "edge", row, 1, 2);
        if (rc != 0) {
            wl_session_destroy(session);
            wl_plan_free(plan);
            return -1;
        }
    }

    struct count_ctx ctx = { 0 };
    rc = wl_session_snapshot(session, count_cb, &ctx);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        return -1;
    }

    int64_t count = ctx.count;

    wl_session_destroy(session);
    wl_plan_free(plan);
    return count;
}

/* ======================================================================== */
/* Test 1: W=1 vs W=4 produce same tuple count (Issue #404 regression)     */
/* ======================================================================== */

static int
test_w1_vs_w4_tuple_count(void)
{
    TEST("TC 20-node chain: W=1 and W=4 produce same tuple count");

    int64_t count1 = run_tc_chain20(1);
    if (count1 < 0) {
        FAIL("session with W=1 failed");
        return 1;
    }

    int64_t count4 = run_tc_chain20(4);
    if (count4 < 0) {
        FAIL("session with W=4 failed");
        return 1;
    }

    if (count1 != count4) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "W=1 produced %" PRId64 " tuples, W=4 produced %" PRId64,
            count1, count4);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 2: W=1 produces the expected 190 tuples for a 20-node chain        */
/* ======================================================================== */

static int
test_w1_expected_count(void)
{
    TEST("TC 20-node chain: W=1 produces 190 tuples");

    int64_t count = run_tc_chain20(1);
    if (count < 0) {
        FAIL("session failed");
        return 1;
    }

    /* 20-node chain: n*(n-1)/2 = 20*19/2 = 190 */
    if (count != 190) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 190, got %" PRId64, count);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 3: W=8 also matches W=1                                            */
/* ======================================================================== */

static int
test_w1_vs_w8_tuple_count(void)
{
    TEST("TC 20-node chain: W=1 and W=8 produce same tuple count");

    int64_t count1 = run_tc_chain20(1);
    if (count1 < 0) {
        FAIL("session with W=1 failed");
        return 1;
    }

    int64_t count8 = run_tc_chain20(8);
    if (count8 < 0) {
        FAIL("session with W=8 failed");
        return 1;
    }

    if (count1 != count8) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "W=1 produced %" PRId64 " tuples, W=8 produced %" PRId64,
            count1, count8);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== test_tdd_multiworker (Issue #404) ===\n");

    test_w1_expected_count();
    test_w1_vs_w4_tuple_count();
    test_w1_vs_w8_tuple_count();

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
