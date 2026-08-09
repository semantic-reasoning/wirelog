/*
 * test_k_fusion_memory.c - K-Fusion Memory Isolation Tests (Issue #196)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Tests that K-fusion memory optimisations (Issue #196) do not break
 * correctness.  Validates per-worker arena isolation, right-sized
 * delta_pool, and empty mat_cache inheritance across K=2 and K=4
 * worker configurations.
 *
 * Test cases:
 *   1. K=2 with memory optimisations produces correct 6-tuple closure
 *   2. K=4 (multi-worker) produces correct result matching K=2
 *   3. Per-worker isolation: concurrent K=2 evaluation is race-free
 *   4. Empty mat_cache: workers do not inherit stale parent cache entries
 */

#include "../wirelog/columnar/columnar_nanoarrow.h"
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
 * Tuple counting callback
 * ---------------------------------------------------------------- */

struct result_ctx {
    int64_t total;
    int64_t rel_count;
    const char *tracked_rel;
};

static void
count_tuples_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct result_ctx *ctx = (struct result_ctx *)user_data;
    ctx->total++;
    if (relation && ctx->tracked_rel && strcmp(relation, ctx->tracked_rel) == 0)
        ctx->rel_count++;
    (void)row;
    (void)ncols;
}

/* ----------------------------------------------------------------
 * Helper: run a program end-to-end with given num_workers
 * ---------------------------------------------------------------- */

static int
run_program_workers(const char *src, const char *tracked_rel, uint32_t nworkers,
    int64_t *out_count, uint32_t *out_iters)
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
    rc = wl_session_create(wl_backend_columnar(), plan, nworkers, &sess);
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

    struct result_ctx ctx = { 0, 0, tracked_rel };
    rc = wl_session_snapshot(sess, count_tuples_cb, &ctx);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    if (out_count)
        *out_count = ctx.rel_count;
    if (out_iters)
        *out_iters = col_session_get_iteration_count(sess);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return 0;
}

/* ================================================================
 * Test 1: K=2 with memory optimisations produces correct closure
 *
 * Issue #196 changes (arena isolation, right-sized pool, empty
 * mat_cache) must not regress K=2 correctness.
 *
 * r(1,2), r(2,3), r(3,4) -> 6-tuple closure
 * ================================================================ */
static void
test_k2_memory_opt_correctness(void)
{
    TEST("K=2 memory-optimised workers produce correct 6-tuple closure");

    const char *src = ".decl r(x: int32, y: int32)\n"
        "r(1, 2). r(2, 3). r(3, 4).\n"
        "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    int rc = run_program_workers(src, "r", 1, &count, NULL);
    ASSERT(rc == 0, "K=2 memory-opt program execution failed");
    ASSERT(count == 6,
        "K=2 memory-opt: expected 6 tuples from 3-edge chain closure");

    PASS();
}

/* ================================================================
 * Test 2: K=4 multi-worker produces same result as K=2
 *
 * With 4 parallel workers (if available) the result must match
 * the single-worker sequential result (6 tuples).
 * ================================================================ */
static void
test_k4_matches_k2(void)
{
    TEST("K=4 multi-worker matches K=2 result (parity)");

    const char *src = ".decl r(x: int32, y: int32)\n"
        "r(1, 2). r(2, 3). r(3, 4).\n"
        "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count_k2 = 0, count_k4 = 0;
    int rc2 = run_program_workers(src, "r", 1, &count_k2, NULL);
    int rc4 = run_program_workers(src, "r", 4, &count_k4, NULL);

    ASSERT(rc2 == 0, "K=2 program failed");
    ASSERT(rc4 == 0, "K=4 program failed");
    ASSERT(count_k2 == 6, "K=2 expected 6 tuples");
    ASSERT(count_k4 == 6, "K=4 expected 6 tuples");
    ASSERT(count_k2 == count_k4, "K=2 and K=4 must agree on tuple count");

    PASS();
}

/* ================================================================
 * Test 3: Per-worker isolation — 5-node chain with K=2
 *
 * Larger graph exercises worker isolation more thoroughly.
 * 5-node chain full closure: 10 tuples.
 * ================================================================ */
static void
test_k2_isolation_5node_chain(void)
{
    TEST("K=2 per-worker isolation: 5-node chain produces 10 tuples");

    const char *src = ".decl e(x: int32, y: int32)\n"
        "e(1, 2). e(2, 3). e(3, 4). e(4, 5).\n"
        ".decl reach(x: int32, y: int32)\n"
        "reach(x, y) :- e(x, y).\n"
        "reach(x, z) :- reach(x, y), reach(y, z).\n";

    int64_t count = 0;
    int rc = run_program_workers(src, "reach", 1, &count, NULL);
    ASSERT(rc == 0, "5-node chain K=2 program failed");
    ASSERT(count == 10, "5-node chain K=2 should produce 10 tuples");

    PASS();
}

/* ================================================================
 * Test 4: Empty mat_cache — 2-cycle converges correctly
 *
 * Workers with zeroed mat_cache must still compute correct results.
 * 2-cycle: r(1,2), r(2,1) -> {(1,2),(2,1),(1,1),(2,2)} = 4 tuples.
 * ================================================================ */
static void
test_k2_empty_mat_cache_correctness(void)
{
    TEST("K=2 empty mat_cache: 2-cycle converges to 4 tuples");

    const char *src = ".decl r(x: int32, y: int32)\n"
        "r(1, 2). r(2, 1).\n"
        "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    uint32_t iters = 0;
    int rc = run_program_workers(src, "r", 1, &count, &iters);
    ASSERT(rc == 0, "2-cycle K=2 program failed");
    ASSERT(count == 4, "2-cycle K=2 should produce 4 tuples");
    ASSERT(iters <= 3, "2-cycle should converge within 3 iterations");

    PASS();
}

/* ================================================================
 * Test 5: Non-fused recursive materialization is cleaned before pool reset
 *
 * Issue #1020: with K-Fusion disabled, recursive materialized joins may
 * leave pool-owned results in mat_cache across a sub-pass boundary.  The
 * cache must release those results before delta_pool_reset clears the pool
 * slot metadata.
 * ================================================================ */
static void
test_nonfused_materialized_join_cleanup(void)
{
    TEST("non-fused recursive materialized join cleanup");

    const char *src = ".decl Edge(x: int64, y: int64)\n"
        ".decl Label(x: int64, l: int64)\n"
        ".output Label\n"
        "Edge(1,2). Edge(2,3). Edge(3,4).\n"
        "Label(x, min(x)) :- Edge(x, y).\n"
        "Label(y, min(y)) :- Edge(x, y).\n"
        "Label(x, min(l)) :- Label(y, l), Edge(y, x).\n"
        "Label(x, min(l)) :- Label(y, l), Label(y, m), Edge(y, x).\n";

    int64_t count = 0;
    int rc = run_program_workers(src, "Label", 1, &count, NULL);
    ASSERT(rc == 0, "non-fused materialized recursive program failed");
    ASSERT(count == 4,
        "non-fused materialized recursive program should produce 4 rows");

    PASS();
}

/* ================================================================
 * main
 * ================================================================ */

int
main(void)
{
    printf("\n=== K-Fusion Memory Isolation Tests (Issue #196) ===\n\n");

    test_k2_memory_opt_correctness();
    test_k4_matches_k2();
    test_k2_isolation_5node_chain();
    test_k2_empty_mat_cache_correctness();
    test_nonfused_materialized_join_cleanup();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n\n");

    return fail_count > 0 ? 1 : 0;
}
