/*
 * test_2d_frontier_skip.c - Verify 2D frontier skip condition behavior (Issue #104)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Tests:
 *   1. Skip fires within the same epoch when iter > convergence_iter
 *   2. Skip does NOT fire across epoch boundaries (different epoch forces re-eval)
 *   3. Convergence is correctly recorded with (outer_epoch, iteration) pairs
 */

#include "../wirelog/columnar/columnar_nanoarrow.h"
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

static void
noop_cb(const char *r, const int64_t *row, uint32_t nc, void *u)
{
    (void)r;
    (void)row;
    (void)nc;
    (void)u;
}

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

/* Build a TC session from src, apply passes, return session + plan + prog.
 * Caller is responsible for wl_session_destroy / wl_plan_free /
 * wirelog_program_free. */
static int
make_tc_session(const char *src, wl_session_t **out_sess, wl_plan_t **out_plan,
                wirelog_program_t **out_prog)
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

    *out_sess = sess;
    *out_plan = plan;
    *out_prog = prog;
    return 0;
}

/* ================================================================
 * Test 1: Skip fires within the same epoch
 *
 * After first snapshot (epoch 0), frontiers record (epoch=0, iter=I).
 * A second snapshot WITHOUT any new insertion is still epoch 0.
 * Iterations beyond the recorded convergence point should be skipped,
 * producing the same result tuple count as the first snapshot.
 * ================================================================ */
static void
test_skip_within_same_epoch(void)
{
    TEST("skip fires within same insertion epoch");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    int rc = make_tc_session(src, &sess, &plan, &prog);
    ASSERT(rc == 0, "session creation failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    /* First snapshot: establishes frontier at (epoch=0, iter=I) */
    struct rel_ctx ctx1 = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx1);
    ASSERT(rc == 0, "first snapshot failed");
    ASSERT(ctx1.count > 0, "expected non-zero TC tuples after first snapshot");

    /* Read frontier for stratum 0 - should record outer_epoch=0 and a
     * finite iteration, meaning the skip condition can fire next time. */
    col_frontier_2d_t f0;
    rc = col_session_get_frontier(sess, 0, &f0);
    ASSERT(rc == 0, "get_frontier stratum 0 failed");
    ASSERT(f0.outer_epoch == 0,
           "frontier outer_epoch should be 0 after first snap");
    ASSERT(f0.iteration != UINT32_MAX,
           "frontier iteration should be finite after convergence");

    printf("(epoch=%u iter=%u tc=%" PRId64 ") ", f0.outer_epoch, f0.iteration,
           ctx1.count);

    /* Second snapshot with NO insertion — same epoch, skip should fire
     * for iterations beyond f0.iteration.  Result must be identical. */
    struct rel_ctx ctx2 = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx2);
    ASSERT(rc == 0, "second snapshot failed");
    ASSERT(ctx2.count == ctx1.count,
           "second snapshot (same epoch) must return same tuple count");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ================================================================
 * Test 2: Skip does NOT fire across epoch boundaries
 *
 * First snapshot converges at (epoch=0, iter=I).
 * Insert new facts via col_session_insert_incremental — this bumps
 * outer_epoch to 1 for affected strata, so their frontier.outer_epoch
 * no longer matches the current epoch.  The skip condition (which
 * requires outer_epoch match) must NOT fire; iterations are re-evaluated
 * from iter=0.  The result after the second snapshot must reflect the
 * newly inserted data.
 * ================================================================ */
static void
test_skip_does_not_fire_across_epochs(void)
{
    TEST("skip does not fire across epoch boundaries");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    int rc = make_tc_session(src, &sess, &plan, &prog);
    ASSERT(rc == 0, "session creation failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    /* Epoch 0: edge(1,2), edge(2,3) -> tc = {(1,2),(2,3),(1,3)} = 3 */
    struct rel_ctx ctx1 = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx1);
    ASSERT(rc == 0, "first snapshot failed");
    ASSERT(ctx1.count == 3, "expected 3 TC tuples for chain 1->2->3");

    col_frontier_2d_t f0_before;
    rc = col_session_get_frontier(sess, 0, &f0_before);
    ASSERT(rc == 0, "get_frontier before insert failed");
    ASSERT(f0_before.outer_epoch == 0,
           "frontier epoch should be 0 before insert");

    /* Insert edge(3,4) — bumps outer_epoch on affected strata */
    int64_t e34[2] = { 3, 4 };
    rc = col_session_insert_incremental(sess, "edge", e34, 1, 2);
    ASSERT(rc == 0, "insert_incremental failed");

    /* Epoch 1: skip must NOT fire; full re-eval from iter=0 required.
     * TC should now contain 6 tuples: the original 3 plus
     * (1,4), (2,4), (3,4). */
    struct rel_ctx ctx2 = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx2);
    ASSERT(rc == 0, "second snapshot failed");

    printf("(epoch0_tc=%" PRId64 " epoch1_tc=%" PRId64 ") ", ctx1.count,
           ctx2.count);

    ASSERT(ctx2.count == 6, "after crossing epoch boundary tc must include new "
                            "tuples (expected 6)");
    ASSERT(ctx2.count > ctx1.count,
           "new epoch must produce more tuples than previous epoch");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ================================================================
 * Test 3: Convergence recorded with correct (outer_epoch, iteration) pairs
 *
 * After two sequential inserts (two epoch increments), verify that the
 * frontier stores the epoch value matching the most recent snapshot and
 * a finite iteration number, confirming 2D tracking is working end-to-end.
 * ================================================================ */
static void
test_convergence_recorded_with_2d_pairs(void)
{
    TEST("convergence recorded with (outer_epoch, iteration) pairs");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    int rc = make_tc_session(src, &sess, &plan, &prog);
    ASSERT(rc == 0, "session creation failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    /* Snapshot 1: epoch 0 */
    rc = wl_session_snapshot(sess, noop_cb, NULL);
    ASSERT(rc == 0, "first snapshot failed");

    col_frontier_2d_t f_epoch0;
    rc = col_session_get_frontier(sess, 0, &f_epoch0);
    ASSERT(rc == 0, "get_frontier epoch0 failed");
    ASSERT(f_epoch0.outer_epoch == 0,
           "frontier outer_epoch must be 0 after first snapshot");
    ASSERT(f_epoch0.iteration != UINT32_MAX,
           "frontier iteration must be finite after convergence (epoch 0)");

    /* Insert edge(2,3) → new epoch */
    int64_t e23[2] = { 2, 3 };
    rc = col_session_insert_incremental(sess, "edge", e23, 1, 2);
    ASSERT(rc == 0, "first insert failed");

    /* Snapshot 2: epoch 1 */
    rc = wl_session_snapshot(sess, noop_cb, NULL);
    ASSERT(rc == 0, "second snapshot failed");

    col_frontier_2d_t f_epoch1;
    rc = col_session_get_frontier(sess, 0, &f_epoch1);
    ASSERT(rc == 0, "get_frontier epoch1 failed");
    ASSERT(f_epoch1.outer_epoch == 1,
           "frontier outer_epoch must be 1 after second snapshot");
    ASSERT(f_epoch1.iteration != UINT32_MAX,
           "frontier iteration must be finite after convergence (epoch 1)");
    ASSERT(f_epoch1.outer_epoch > f_epoch0.outer_epoch,
           "frontier epoch must increment across insertions");

    /* Insert edge(3,4) → epoch 2 */
    int64_t e34[2] = { 3, 4 };
    rc = col_session_insert_incremental(sess, "edge", e34, 1, 2);
    ASSERT(rc == 0, "second insert failed");

    /* Snapshot 3: epoch 2 */
    struct rel_ctx ctx3 = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx3);
    ASSERT(rc == 0, "third snapshot failed");

    col_frontier_2d_t f_epoch2;
    rc = col_session_get_frontier(sess, 0, &f_epoch2);
    ASSERT(rc == 0, "get_frontier epoch2 failed");
    ASSERT(f_epoch2.outer_epoch == 2,
           "frontier outer_epoch must be 2 after third snapshot");
    ASSERT(f_epoch2.iteration != UINT32_MAX,
           "frontier iteration must be finite after convergence (epoch 2)");

    printf("(e0=(%u,%u) e1=(%u,%u) e2=(%u,%u) tc=%" PRId64 ") ",
           f_epoch0.outer_epoch, f_epoch0.iteration, f_epoch1.outer_epoch,
           f_epoch1.iteration, f_epoch2.outer_epoch, f_epoch2.iteration,
           ctx3.count);

    ASSERT(ctx3.count == 6, "expected 6 TC tuples for 1->2->3->4 chain");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== 2D Frontier Skip Condition Tests (Issue #104) ===\n\n");

    test_skip_within_same_epoch();
    test_skip_does_not_fire_across_epochs();
    test_convergence_recorded_with_2d_pairs();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(" (%d FAILED)\n", fail_count);
    else
        printf(" ---\n");

    return fail_count > 0 ? 1 : 0;
}
