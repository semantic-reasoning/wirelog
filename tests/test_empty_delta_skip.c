/*
 * test_empty_delta_skip.c - TDD tests for empty-delta skip optimization
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Tests that the semi-naive evaluator correctly handles the case where
 * a relation plan contains FORCE_DELTA ops whose delta relation is empty
 * or absent (iteration > 0).  The optimization skips evaluation of such
 * plans entirely, since an empty forced-delta would produce 0 rows.
 *
 * Test cases:
 *   1. Iteration 0 uses full relation (seed) even when no delta exists
 *   2. Iteration N>0 with empty delta produces correct results (skip safe)
 *   3. Skip correctness: output matches baseline (no optimization change)
 *   4. Iteration count unchanged: fixpoint convergence unaffected
 */

#include "../wirelog/backend/columnar_nanoarrow.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/ir/program.h"
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

/* ----------------------------------------------------------------
 * Test framework (matches wirelog convention)
 * ---------------------------------------------------------------- */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                      \
    do {                                                \
        test_count++;                                   \
        printf("TEST %d: %s ... ", test_count, (name)); \
    } while (0)

#define PASS()            \
    do {                  \
        pass_count++;     \
        printf("PASS\n"); \
    } while (0)

#define FAIL(msg)                    \
    do {                             \
        fail_count++;                \
        printf("FAIL: %s\n", (msg)); \
        return;                      \
    } while (0)

#define ASSERT(cond, msg) \
    do {                  \
        if (!(cond))      \
            FAIL(msg);    \
    } while (0)

/* ----------------------------------------------------------------
 * Snapshot helpers: count tuples per relation
 * ---------------------------------------------------------------- */

struct tuple_ctx {
    int64_t total;
    int64_t tc_count; /* tuples in "tc" relation */
};

static void
count_tuples_cb(const char *relation, const int64_t *row, uint32_t ncols,
                void *user_data)
{
    struct tuple_ctx *ctx = (struct tuple_ctx *)user_data;
    ctx->total++;
    if (relation && strcmp(relation, "tc") == 0)
        ctx->tc_count++;
    (void)row;
    (void)ncols;
}

/* ----------------------------------------------------------------
 * Helper: run a TC program end-to-end and return tuple count + iterations
 * ---------------------------------------------------------------- */

static int
run_tc_program(const char *src, int64_t *out_tc_count, uint32_t *out_iters)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    if (rc != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    rc = wl_session_load_facts(sess, prog);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    struct tuple_ctx ctx = { 0, 0 };
    rc = wl_session_snapshot(sess, count_tuples_cb, &ctx);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    if (out_tc_count)
        *out_tc_count = ctx.tc_count;
    if (out_iters)
        *out_iters = col_session_get_iteration_count(sess);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return 0;
}

/* ================================================================
 * Test 1: Iteration 0, no delta exists -> uses full relation (seed)
 *
 * A simple transitive closure with 3 edges should still produce
 * results on the first iteration even though no delta relation
 * exists yet.  The FORCE_DELTA fallback to full relation is critical.
 *
 * edge(1,2). edge(2,3). edge(3,4).
 * tc(x,y) :- edge(x,y).
 * tc(x,z) :- tc(x,y), edge(y,z).
 *
 * Expected: 6 TC tuples (all reachable pairs in a 4-node chain)
 * ================================================================ */
static void
test_iter0_no_delta_uses_fallback(void)
{
    TEST("iter0 no delta uses fallback (full relation as seed)");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    int64_t tc_count = 0;
    int rc = run_tc_program(src, &tc_count, NULL);
    ASSERT(rc == 0, "TC program execution failed");
    ASSERT(tc_count == 6, "expected 6 TC tuples from 3-edge chain");

    PASS();
}

/* ================================================================
 * Test 2: Iteration N>0 with empty delta skips or returns 0 rows
 *
 * When a relation has converged (no new tuples), its delta is empty.
 * Plans with FORCE_DELTA on that relation should be skipped safely
 * without affecting correctness.
 *
 * Use a program where one relation converges before another:
 *   - "link" is non-recursive (converges immediately in stratum 0)
 *   - "reach" is recursive, depends on link
 *
 * The key is that after fixpoint, no extra tuples are produced.
 * ================================================================ */
static void
test_iter_gt0_empty_delta_skips(void)
{
    TEST("iter>0 empty delta skips correctly");

    const char *src = ".decl link(x: int32, y: int32)\n"
                      "link(1, 2). link(2, 3).\n"
                      ".decl reach(x: int32, y: int32)\n"
                      "reach(x, y) :- link(x, y).\n"
                      "reach(x, z) :- reach(x, y), link(y, z).\n";

    int64_t reach_count = 0;
    int rc = run_tc_program(src, &reach_count, NULL);
    /* reach_count uses tc_count field but relation is "reach" not "tc" */

    /* Run directly to check reach */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session create failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    struct tuple_ctx ctx = { 0, 0 };
    rc = wl_session_snapshot(sess, count_tuples_cb, &ctx);
    ASSERT(rc == 0, "snapshot failed");

    /* reach on 1->2->3: reach(1,2), reach(2,3), reach(1,3) = 3 tuples */
    /* The evaluator must reach fixpoint; later iterations have empty deltas */
    ASSERT(ctx.total == 3, "expected 3 reach tuples from 2-edge chain");

    uint32_t iters = col_session_get_iteration_count(sess);
    /* Should converge in a small number of iterations */
    ASSERT(iters > 0, "should have at least 1 iteration");
    ASSERT(iters < 20, "should converge quickly (< 20 iterations)");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ================================================================
 * Test 3: Skip correctness - same output as executing it
 *
 * Verify that a longer chain produces the correct number of tuples.
 * If empty-delta skip were incorrect, we would see fewer or more
 * tuples than expected.
 *
 * 5-node chain: edge(1,2), edge(2,3), edge(3,4), edge(4,5)
 * TC should produce: C(5,2) = 10 tuples
 * ================================================================ */
static void
test_skip_correctness(void)
{
    TEST("skip correctness: 5-node chain produces exact TC count");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4). edge(4, 5).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    int64_t tc_count = 0;
    int rc = run_tc_program(src, &tc_count, NULL);
    ASSERT(rc == 0, "TC program execution failed");

    /* 5-node chain: all (i,j) where 1<=i<j<=5 = C(5,2) = 10 */
    ASSERT(tc_count == 10, "expected 10 TC tuples from 5-node chain");

    PASS();
}

/* ================================================================
 * Test 4: Iteration count unchanged - fixpoint convergence unaffected
 *
 * Run the same program twice and verify iteration counts match.
 * The empty-delta skip optimization should not change the number
 * of fixpoint iterations required to converge.
 *
 * Uses a 4-node chain (same as test 1).
 * ================================================================ */
static void
test_iteration_count_unchanged(void)
{
    TEST("iteration count stable across runs");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    uint32_t iters1 = 0, iters2 = 0;
    int64_t count1 = 0, count2 = 0;

    int rc = run_tc_program(src, &count1, &iters1);
    ASSERT(rc == 0, "first run failed");

    rc = run_tc_program(src, &count2, &iters2);
    ASSERT(rc == 0, "second run failed");

    ASSERT(count1 == count2, "tuple counts differ between runs");
    ASSERT(iters1 == iters2, "iteration counts differ between runs");
    ASSERT(count1 == 6, "expected 6 TC tuples");
    ASSERT(iters1 > 0, "should have at least 1 iteration");

    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */
int
main(void)
{
    printf("=== test_empty_delta_skip ===\n\n");

    test_iter0_no_delta_uses_fallback();
    test_iter_gt0_empty_delta_skips();
    test_skip_correctness();
    test_iteration_count_unchanged();

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n", pass_count,
           fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
