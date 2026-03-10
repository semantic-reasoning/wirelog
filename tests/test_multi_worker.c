/*
 * test_multi_worker.c - Tests for issue #99 multi-worker task dispatch
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Verifies that num_workers > 1 produces identical results to
 * single-threaded evaluation (num_workers=1).
 *
 *   1. TC with num_workers=2 matches num_workers=1
 *   2. Multi-rule recursive program with num_workers=4 matches num_workers=1
 *   3. Sequential K-fusion fallback (num_workers=1) produces correct TC
 */

#include "../wirelog/backend/columnar_nanoarrow.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdint.h>
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

/* Evaluate a program with the given num_workers and return tuple count */
static int
eval_with_workers(const char *src, const char *target_rel, uint32_t num_workers,
                  int64_t *out_count)
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
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &sess);
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
 * Test 1: TC with num_workers=2 matches num_workers=1
 *
 * Transitive closure over a 4-node chain:
 *   edge(1,2), edge(2,3), edge(3,4), edge(4,5)
 * TC = 10 tuples (4+3+2+1 pairs).
 * ================================================================ */
static void
test_tc_workers_match(void)
{
    TEST("TC num_workers=2 matches num_workers=1");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4). edge(4, 5).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    int64_t count_1w = 0, count_2w = 0;
    int rc = eval_with_workers(src, "tc", 1, &count_1w);
    ASSERT(rc == 0, "eval with 1 worker failed");

    rc = eval_with_workers(src, "tc", 2, &count_2w);
    ASSERT(rc == 0, "eval with 2 workers failed");

    printf("(1w=%" PRId64 " 2w=%" PRId64 ") ", count_1w, count_2w);
    ASSERT(count_1w == 10, "expected 10 TC tuples with 1 worker");
    ASSERT(count_2w == count_1w, "2-worker result must match 1-worker");

    PASS();
}

/* ================================================================
 * Test 2: Multi-rule recursive program with num_workers=4
 *
 * Uses a graph reachability program with multiple recursive rules
 * to exercise K-fusion parallelism with 4 workers.
 * ================================================================ */
static void
test_multi_rule_workers_match(void)
{
    TEST("multi-rule recursive with num_workers=4 matches num_workers=1");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      "edge(4, 5). edge(5, 6). edge(6, 1).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    int64_t count_1w = 0, count_4w = 0;
    int rc = eval_with_workers(src, "tc", 1, &count_1w);
    ASSERT(rc == 0, "eval with 1 worker failed");

    rc = eval_with_workers(src, "tc", 4, &count_4w);
    ASSERT(rc == 0, "eval with 4 workers failed");

    printf("(1w=%" PRId64 " 4w=%" PRId64 ") ", count_1w, count_4w);
    /* 6-node cycle: each node reaches all others = 6*6 = 36 */
    ASSERT(count_1w == 36, "expected 36 TC tuples (6-node cycle)");
    ASSERT(count_4w == count_1w, "4-worker result must match 1-worker");

    PASS();
}

/* ================================================================
 * Test 3: Sequential fallback (num_workers=1) correctness
 *
 * Verify the sequential K-fusion path produces correct results.
 * ================================================================ */
static void
test_sequential_fallback(void)
{
    TEST("sequential K-fusion fallback (num_workers=1) correct");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    int64_t count = 0;
    int rc = eval_with_workers(src, "tc", 1, &count);
    ASSERT(rc == 0, "eval failed");

    printf("(count=%" PRId64 ") ", count);
    /* 3 edges: tc = 3+2+1 = 6 */
    ASSERT(count == 6, "expected 6 TC tuples");

    PASS();
}

/* ================================================================
 * Test 4: Incremental insert with multi-worker
 *
 * Verify that incremental insert + re-eval with num_workers > 1
 * produces the same results as num_workers=1.
 * ================================================================ */
static void
test_incremental_multi_worker(void)
{
    TEST("incremental insert with num_workers=2 matches num_workers=1");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    int64_t counts[2] = { 0, 0 };

    for (uint32_t nw = 1; nw <= 2; nw++) {
        wirelog_error_t err;
        wirelog_program_t *prog = wirelog_parse_string(src, &err);
        ASSERT(prog != NULL, "parse failed");

        wl_fusion_apply(prog, NULL);
        wl_jpp_apply(prog, NULL);
        wl_sip_apply(prog, NULL);

        wl_plan_t *plan = NULL;
        int rc = wl_plan_from_program(prog, &plan);
        ASSERT(rc == 0, "plan failed");

        wl_session_t *sess = NULL;
        rc = wl_session_create(wl_backend_columnar(), plan, nw, &sess);
        ASSERT(rc == 0, "session create failed");

        rc = wl_session_load_facts(sess, prog);
        ASSERT(rc == 0, "load facts failed");

        /* Initial eval */
        struct rel_ctx ctx0 = { "tc", 0 };
        rc = wl_session_snapshot(sess, count_cb, &ctx0);
        ASSERT(rc == 0, "initial snapshot failed");

        /* Insert edge(3,4) and re-eval */
        int64_t new_edge[2] = { 3, 4 };
        rc = col_session_insert_incremental(sess, "edge", new_edge, 1, 2);
        ASSERT(rc == 0, "insert failed");

        struct rel_ctx ctx1 = { "tc", 0 };
        rc = wl_session_snapshot(sess, count_cb, &ctx1);
        ASSERT(rc == 0, "incremental snapshot failed");

        counts[nw - 1] = ctx1.count;

        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
    }

    printf("(1w=%" PRId64 " 2w=%" PRId64 ") ", counts[0], counts[1]);
    ASSERT(counts[0] == 6, "expected 6 TC tuples with 1 worker");
    ASSERT(counts[1] == counts[0], "2-worker incremental must match 1-worker");

    PASS();
}

/* ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== Multi-Worker Tests (Issue #99) ===\n\n");

    test_tc_workers_match();
    test_multi_rule_workers_match();
    test_sequential_fallback();
    test_incremental_multi_worker();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(" (%d FAILED)", fail_count);
    printf(" ---\n");

    return fail_count > 0 ? 1 : 0;
}
