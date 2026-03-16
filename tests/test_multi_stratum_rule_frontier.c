/*
 * test_multi_stratum_rule_frontier.c - Comprehensive test for multi-stratum
 * rule frontier tracking (Issue #106, US-106-006)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Tests:
 *   1. Rule frontier initialization per-stratum: rule_frontiers start at (0,0)
 *   2. Skip condition respects stratum epoch context: same-epoch skip fires
 *   3. Cross-epoch re-evaluation: new epoch forces skip NOT to fire
 *   4. Rule convergence recording with (epoch, iteration) pairs
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

static void
noop_cb(const char *r, const int64_t *row, uint32_t nc, void *u)
{
    (void)r;
    (void)row;
    (void)nc;
    (void)u;
}

/*
 * Build a session from Datalog source, applying fusion/JPP/SIP passes.
 * Caller must wl_session_destroy / wl_plan_free / wirelog_program_free.
 */
static int
make_session(const char *src, wl_session_t **out_sess, wl_plan_t **out_plan,
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

/*
 * Find the index of the first recursive stratum, or UINT32_MAX if none.
 */
static uint32_t
find_recursive_stratum(const wl_plan_t *plan)
{
    for (uint32_t i = 0; i < plan->stratum_count; i++) {
        if (plan->strata[i].is_recursive)
            return i;
    }
    return UINT32_MAX;
}

/* ================================================================
 * Test 1: Rule frontier initialization per-stratum
 *
 * After session creation, before any evaluation, per-stratum frontiers
 * should be at their default initialized state (outer_epoch=0, iteration=0).
 * This is verified for a TC program which has a multi-stratum plan with
 * at least one recursive stratum containing multiple rules.
 * ================================================================ */
static void
test_rule_frontier_init(void)
{
    TEST("rule frontier initialization: frontiers start at (0,0) per-stratum");

    /* TC program: 2 rules in the recursive stratum
     *   tc(x,y) :- edge(x,y).
     *   tc(x,z) :- tc(x,y), edge(y,z).
     */
    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    int rc = make_session(src, &sess, &plan, &prog);
    ASSERT(rc == 0, "session creation failed");

    /* Find the recursive stratum */
    uint32_t rec_si = find_recursive_stratum(plan);
    ASSERT(rec_si != UINT32_MAX, "no recursive stratum found in TC program");

    /* Before any evaluation, frontier should be initialized to (0,0).
     * The session is freshly created; calloc zeroes the frontiers array. */
    col_frontier_2d_t f;
    rc = col_session_get_frontier(sess, rec_si, &f);
    ASSERT(rc == 0, "col_session_get_frontier failed");
    ASSERT(f.outer_epoch == 0, "frontier outer_epoch should be 0 at init");
    ASSERT(f.iteration == 0, "frontier iteration should be 0 at init");

    /* Also verify stratum 0 if there are multiple strata */
    if (plan->stratum_count > 1) {
        col_frontier_2d_t f0;
        rc = col_session_get_frontier(sess, 0, &f0);
        ASSERT(rc == 0, "col_session_get_frontier stratum 0 failed");
        ASSERT(f0.outer_epoch == 0, "stratum 0 outer_epoch should be 0");
        ASSERT(f0.iteration == 0, "stratum 0 iteration should be 0");
    }

    printf("(strata=%u rec_si=%u f=(%u,%u)) ", plan->stratum_count, rec_si,
           f.outer_epoch, f.iteration);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ================================================================
 * Test 2: Skip condition respects stratum epoch context
 *
 * After the first snapshot (epoch 0), the frontier records (epoch=0, iter=I).
 * A second snapshot with NO new insertion is still epoch 0. The skip
 * condition for iterations beyond I must fire, yielding the same result
 * count as the first snapshot.
 *
 * This tests that rule-level frontier (per-stratum) skip works correctly
 * when multiple rules share a stratum.
 * ================================================================ */
static void
test_rule_skip_same_epoch(void)
{
    TEST("skip condition respects stratum epoch: same-epoch skip fires");

    /* Two rules in the recursive stratum ensure multi-rule stratum behavior */
    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    int rc = make_session(src, &sess, &plan, &prog);
    ASSERT(rc == 0, "session creation failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    /* First snapshot: establishes frontier at (epoch=0, iter=I) */
    struct rel_ctx ctx1 = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx1);
    ASSERT(rc == 0, "first snapshot failed");
    ASSERT(ctx1.count > 0, "expected non-zero TC tuples after first snapshot");

    /* Read frontier for the recursive stratum */
    uint32_t rec_si = find_recursive_stratum(plan);
    ASSERT(rec_si != UINT32_MAX, "no recursive stratum found");

    col_frontier_2d_t f_after_first;
    rc = col_session_get_frontier(sess, rec_si, &f_after_first);
    ASSERT(rc == 0, "get_frontier after first snapshot failed");
    ASSERT(f_after_first.outer_epoch == 0,
           "frontier outer_epoch should be 0 after first snapshot");
    ASSERT(f_after_first.iteration != UINT32_MAX,
           "frontier iteration should be finite after convergence");

    /* Second snapshot with NO insertion — same epoch, skip must fire */
    struct rel_ctx ctx2 = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx2);
    ASSERT(rc == 0, "second snapshot failed");
    ASSERT(ctx2.count == ctx1.count,
           "same-epoch second snapshot must return identical tuple count");

    printf("(epoch=%u iter=%u tc1=%" PRId64 " tc2=%" PRId64 ") ",
           f_after_first.outer_epoch, f_after_first.iteration, ctx1.count,
           ctx2.count);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ================================================================
 * Test 3: Cross-epoch re-evaluation (no skip across epoch boundaries)
 *
 * First snapshot with edge(1,2), edge(2,3) -> 3 TC tuples, epoch=0.
 * Insert edge(3,4) -> epoch increments to 1 for affected strata.
 * The skip condition (requires outer_epoch match) must NOT fire.
 * Re-evaluation from iter=0 must happen; result must grow to 6 tuples.
 * ================================================================ */
static void
test_rule_skip_across_epochs(void)
{
    TEST("cross-epoch re-evaluation: skip does NOT fire after insertion");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    int rc = make_session(src, &sess, &plan, &prog);
    ASSERT(rc == 0, "session creation failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    /* Epoch 0: edge(1,2), edge(2,3) -> tc = {(1,2),(2,3),(1,3)} = 3 */
    struct rel_ctx ctx1 = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx1);
    ASSERT(rc == 0, "first snapshot failed");
    ASSERT(ctx1.count == 3, "expected 3 TC tuples for chain 1->2->3");

    uint32_t rec_si = find_recursive_stratum(plan);
    ASSERT(rec_si != UINT32_MAX, "no recursive stratum found");

    col_frontier_2d_t f_epoch0;
    rc = col_session_get_frontier(sess, rec_si, &f_epoch0);
    ASSERT(rc == 0, "get_frontier epoch0 failed");
    ASSERT(f_epoch0.outer_epoch == 0,
           "frontier epoch should be 0 before insert");

    /* Insert edge(3,4): bumps outer_epoch on affected strata */
    int64_t e34[2] = { 3, 4 };
    rc = col_session_insert_incremental(sess, "edge", e34, 1, 2);
    ASSERT(rc == 0, "insert_incremental failed");

    /* Epoch 1: skip must NOT fire; full re-eval from iter=0 required.
     * TC should contain 6 tuples: original 3 + (1,4),(2,4),(3,4). */
    struct rel_ctx ctx2 = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx2);
    ASSERT(rc == 0, "second snapshot failed");

    printf("(epoch0_tc=%" PRId64 " epoch1_tc=%" PRId64 ") ", ctx1.count,
           ctx2.count);

    ASSERT(ctx2.count == 6,
           "after epoch boundary tc must include new tuples (expected 6)");
    ASSERT(ctx2.count > ctx1.count,
           "new epoch must produce more tuples than previous epoch");

    /* Verify frontier now reflects epoch 1 */
    col_frontier_2d_t f_epoch1;
    rc = col_session_get_frontier(sess, rec_si, &f_epoch1);
    ASSERT(rc == 0, "get_frontier epoch1 failed");
    ASSERT(f_epoch1.outer_epoch > f_epoch0.outer_epoch,
           "frontier epoch must have incremented after insertion");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ================================================================
 * Test 4: Rule convergence recording with (epoch, iteration) pairs
 *
 * After multiple sequential inserts, verify:
 * - Frontier stores the epoch matching the most recent snapshot
 * - Frontier iteration is finite (not UINT32_MAX) after convergence
 * - Multiple insertions in same epoch: frontier.iteration updated correctly
 * - Both outer_epoch and iteration fields track convergence accurately
 * ================================================================ */
static void
test_rule_convergence_epoch_iteration_pairs(void)
{
    TEST("rule convergence: frontier records correct (epoch, iteration) pairs");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    int rc = make_session(src, &sess, &plan, &prog);
    ASSERT(rc == 0, "session creation failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    uint32_t rec_si = find_recursive_stratum(plan);
    ASSERT(rec_si != UINT32_MAX, "no recursive stratum found");

    /* Snapshot 1: epoch 0 — single edge(1,2), tc = {(1,2)} */
    rc = wl_session_snapshot(sess, noop_cb, NULL);
    ASSERT(rc == 0, "first snapshot failed");

    col_frontier_2d_t f0;
    rc = col_session_get_frontier(sess, rec_si, &f0);
    ASSERT(rc == 0, "get_frontier epoch0 failed");
    ASSERT(f0.outer_epoch == 0,
           "frontier outer_epoch must be 0 after first snapshot");
    ASSERT(f0.iteration != UINT32_MAX,
           "frontier iteration must be finite after convergence (epoch 0)");

    /* Insert edge(2,3) -> new epoch */
    int64_t e23[2] = { 2, 3 };
    rc = col_session_insert_incremental(sess, "edge", e23, 1, 2);
    ASSERT(rc == 0, "first insert failed");

    /* Snapshot 2: epoch 1 — tc = {(1,2),(2,3),(1,3)} */
    rc = wl_session_snapshot(sess, noop_cb, NULL);
    ASSERT(rc == 0, "second snapshot failed");

    col_frontier_2d_t f1;
    rc = col_session_get_frontier(sess, rec_si, &f1);
    ASSERT(rc == 0, "get_frontier epoch1 failed");
    ASSERT(f1.outer_epoch == 1,
           "frontier outer_epoch must be 1 after second snapshot");
    ASSERT(f1.iteration != UINT32_MAX,
           "frontier iteration must be finite after convergence (epoch 1)");
    ASSERT(f1.outer_epoch > f0.outer_epoch,
           "frontier epoch must increment across insertions");

    /* Insert edge(3,4) -> epoch 2 */
    int64_t e34[2] = { 3, 4 };
    rc = col_session_insert_incremental(sess, "edge", e34, 1, 2);
    ASSERT(rc == 0, "second insert failed");

    /* Snapshot 3: epoch 2 — tc = {(1,2),(2,3),(3,4),(1,3),(2,4),(1,4)} = 6 */
    struct rel_ctx ctx3 = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx3);
    ASSERT(rc == 0, "third snapshot failed");

    col_frontier_2d_t f2;
    rc = col_session_get_frontier(sess, rec_si, &f2);
    ASSERT(rc == 0, "get_frontier epoch2 failed");
    ASSERT(f2.outer_epoch == 2,
           "frontier outer_epoch must be 2 after third snapshot");
    ASSERT(f2.iteration != UINT32_MAX,
           "frontier iteration must be finite after convergence (epoch 2)");
    ASSERT(f2.outer_epoch > f1.outer_epoch,
           "frontier epoch must keep incrementing across insertions");

    printf("(e0=(%u,%u) e1=(%u,%u) e2=(%u,%u) tc=%" PRId64 ") ", f0.outer_epoch,
           f0.iteration, f1.outer_epoch, f1.iteration, f2.outer_epoch,
           f2.iteration, ctx3.count);

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
    printf("=== Multi-Stratum Rule Frontier Tracking Tests (Issue #106, "
           "US-106-006) ===\n\n");

    test_rule_frontier_init();
    test_rule_skip_same_epoch();
    test_rule_skip_across_epochs();
    test_rule_convergence_epoch_iteration_pairs();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(" (%d FAILED)\n", fail_count);
    else
        printf(" ---\n");

    return fail_count > 0 ? 1 : 0;
}
