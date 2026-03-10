/*
 * test_consolidation_cow.c - Tests for issue #94 consolidation optimizations
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Verifies:
 *   1. sorted_nrows tracking after consolidation
 *   2. Incremental merge produces correct results for partially-sorted input
 *   3. Persistent merge buffer reuse across multiple consolidations
 *   4. E2E determinism with incremental consolidation active
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
 * Test framework
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
 * Helpers
 * ---------------------------------------------------------------- */

struct rel_ctx {
    const char *target;
    int64_t count;
};

static void
count_cb(const char *relation, const int64_t *row, uint32_t ncols,
         void *user_data)
{
    struct rel_ctx *ctx = (struct rel_ctx *)user_data;
    if (relation && ctx->target && strcmp(relation, ctx->target) == 0)
        ctx->count++;
    (void)row;
    (void)ncols;
}

static int
run_program(const char *src, const char *target_rel, int64_t *out_count)
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

    struct rel_ctx ctx = { target_rel, 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    if (out_count)
        *out_count = ctx.count;

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return 0;
}

/* ================================================================
 * Test 1: TC correctness with incremental consolidation
 *
 * Basic transitive closure — the incremental merge path in
 * col_op_consolidate_incremental_delta is exercised on every
 * iteration. Verify correct tuple count.
 * ================================================================ */
static void
test_tc_correctness(void)
{
    TEST("TC correctness with incremental consolidation");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4). edge(4, 5).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    int64_t tc_count = 0;
    int rc = run_program(src, "tc", &tc_count);
    ASSERT(rc == 0, "TC program execution failed");
    ASSERT(tc_count == 10, "expected 10 TC tuples from 5-node chain");

    PASS();
}

/* ================================================================
 * Test 2: Self-join determinism
 *
 * Run a self-join program twice and verify identical output.
 * The sorted_nrows tracking must not cause non-determinism.
 * ================================================================ */
static void
test_self_join_determinism(void)
{
    TEST("self-join determinism with sorted_nrows tracking");

    const char *src = ".decl assign(x: int32, y: int32)\n"
                      "assign(1, 2). assign(1, 3). assign(4, 5).\n"
                      ".decl valueFlow(x: int32, y: int32)\n"
                      "valueFlow(y, x) :- assign(y, x).\n"
                      "valueFlow(x, x) :- assign(x, _).\n"
                      "valueFlow(x, x) :- assign(_, x).\n"
                      ".decl valueAlias(x: int32, y: int32)\n"
                      "valueAlias(x, y) :- valueFlow(z, x), valueFlow(z, y).\n";

    int64_t count1 = 0, count2 = 0;
    int rc = run_program(src, "valueAlias", &count1);
    ASSERT(rc == 0, "first run failed");
    ASSERT(count1 > 0, "expected non-zero valueAlias tuples");

    rc = run_program(src, "valueAlias", &count2);
    ASSERT(rc == 0, "second run failed");
    ASSERT(count1 == count2, "output must be deterministic across runs");

    printf("(va=%" PRId64 ") ", count1);
    PASS();
}

/* ================================================================
 * Test 3: Recursive TC with convergence
 *
 * Larger chain graph with transitive closure. Multiple iterations
 * exercise the persistent merge buffer reuse path in
 * col_op_consolidate_incremental_delta.
 * ================================================================ */
static void
test_tc_convergence_merge_reuse(void)
{
    TEST("TC convergence with merge buffer reuse");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4). "
                      "edge(4, 5). edge(5, 6). edge(6, 7).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    /* 7-node chain: C(7,2) = 21 TC tuples */
    int64_t tc_count = 0;
    int rc = run_program(src, "tc", &tc_count);
    ASSERT(rc == 0, "TC program execution failed");
    ASSERT(tc_count == 21, "expected 21 TC tuples from 7-node chain");

    /* Determinism */
    int64_t tc_count2 = 0;
    rc = run_program(src, "tc", &tc_count2);
    ASSERT(rc == 0, "second run failed");
    ASSERT(tc_count == tc_count2, "output must be deterministic across runs");

    PASS();
}

/* ================================================================
 * Test 4: Star graph self-join stress
 *
 * Star from center node 1 to 8 leaves. Many pairs produced.
 * Exercises incremental merge with growing merge buffer.
 * ================================================================ */
static void
test_star_graph_stress(void)
{
    TEST("star graph stress with persistent merge buffer");

    const char *src = ".decl assign(x: int32, y: int32)\n"
                      "assign(1, 2). assign(1, 3). assign(1, 4). "
                      "assign(1, 5). assign(1, 6). assign(1, 7). "
                      "assign(1, 8). assign(1, 9).\n"
                      ".decl valueFlow(x: int32, y: int32)\n"
                      "valueFlow(y, x) :- assign(y, x).\n"
                      "valueFlow(x, x) :- assign(x, _).\n"
                      "valueFlow(x, x) :- assign(_, x).\n"
                      ".decl valueAlias(x: int32, y: int32)\n"
                      "valueAlias(x, y) :- valueFlow(z, x), "
                      "valueFlow(z, y).\n";

    int64_t va_count = 0;
    int rc = run_program(src, "valueAlias", &va_count);
    ASSERT(rc == 0, "program execution failed");
    ASSERT(va_count > 0, "expected non-zero valueAlias tuples");

    /* Determinism */
    int64_t va_count2 = 0;
    rc = run_program(src, "valueAlias", &va_count2);
    ASSERT(rc == 0, "second run failed");
    ASSERT(va_count == va_count2, "output must be deterministic across runs");

    printf("(va=%" PRId64 ") ", va_count);
    PASS();
}

/* ================================================================
 * Test 5: Cyclic graph TC
 *
 * Graph with a cycle: 1->2->3->1, plus 3->4.
 * Tests that consolidation handles duplicate detection correctly
 * when the same tuples are re-derived across iterations.
 * ================================================================ */
static void
test_cyclic_graph_tc(void)
{
    TEST("cyclic graph TC with dedup across iterations");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 1). edge(3, 4).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    /* Nodes {1,2,3} form a clique in TC, plus 4 reachable from all.
     * TC: (1,2)(1,3)(1,4)(1,1) (2,3)(2,1)(2,4)(2,2) (3,1)(3,4)(3,2)(3,3)
     * = 12 tuples */
    int64_t tc_count = 0;
    int rc = run_program(src, "tc", &tc_count);
    ASSERT(rc == 0, "TC program execution failed");
    ASSERT(tc_count == 12, "expected 12 TC tuples from cyclic graph");

    /* Determinism */
    int64_t tc_count2 = 0;
    rc = run_program(src, "tc", &tc_count2);
    ASSERT(rc == 0, "second run failed");
    ASSERT(tc_count == tc_count2, "output must be deterministic across runs");

    PASS();
}

/* ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== Consolidation CoW Tests (Issue #94) ===\n\n");

    test_tc_correctness();
    test_self_join_determinism();
    test_tc_convergence_merge_reuse();
    test_star_graph_stress();
    test_cyclic_graph_tc();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(" (%d FAILED)", fail_count);
    printf(" ---\n");

    return fail_count > 0 ? 1 : 0;
}
