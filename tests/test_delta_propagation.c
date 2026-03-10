/*
 * test_delta_propagation.c - Tests for issue #83 delta-seeded evaluation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Verifies that incremental re-evaluation via delta propagation produces
 * correct results matching fresh evaluation from scratch.
 *
 *   1. TC incremental result matches fresh eval after edge insert
 *   2. Multi-insert accumulation produces correct cumulative results
 *   3. Delta propagation with unaffected relations preserves correctness
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

/* Run a fresh evaluation and return tuple count for target_rel */
static int
run_fresh(const char *src, const char *target_rel, int64_t *out_count)
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
 * Test 1: TC delta propagation matches fresh eval
 *
 * Initial: edge(1,2), edge(2,3) -> TC = 3 tuples
 * Insert: edge(3,4)
 * Incremental re-eval should produce TC = 6 (same as fresh eval
 * with all 3 edges).
 * ================================================================ */
static void
test_tc_delta_matches_fresh(void)
{
    TEST("TC delta propagation matches fresh evaluation");

    /* Fresh eval with all edges */
    const char *full_src = ".decl edge(x: int32, y: int32)\n"
                           "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                           ".decl tc(x: int32, y: int32)\n"
                           "tc(x, y) :- edge(x, y).\n"
                           "tc(x, z) :- tc(x, y), edge(y, z).\n";

    int64_t fresh_count = 0;
    int rc = run_fresh(full_src, "tc", &fresh_count);
    ASSERT(rc == 0, "fresh eval failed");

    /* Incremental: start with 2 edges, insert third */
    const char *incr_src = ".decl edge(x: int32, y: int32)\n"
                           "edge(1, 2). edge(2, 3).\n"
                           ".decl tc(x: int32, y: int32)\n"
                           "tc(x, y) :- edge(x, y).\n"
                           "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(incr_src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan failed");

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session create failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    /* Initial eval */
    rc = wl_session_snapshot(sess, noop_cb, NULL);
    ASSERT(rc == 0, "initial snapshot failed");

    /* Insert edge(3, 4) */
    int64_t new_edge[2] = { 3, 4 };
    rc = col_session_insert_incremental(sess, "edge", new_edge, 1, 2);
    ASSERT(rc == 0, "insert_incremental failed");

    /* Incremental re-eval */
    struct rel_ctx ctx = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    ASSERT(rc == 0, "incremental snapshot failed");

    printf("(fresh=%" PRId64 " incr=%" PRId64 ") ", fresh_count, ctx.count);
    ASSERT(ctx.count == fresh_count,
           "incremental result must match fresh evaluation");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ================================================================
 * Test 2: Sequential incremental inserts accumulate correctly
 *
 * Start with edge(1,2). Insert edge(2,3), re-eval. Insert edge(3,4),
 * re-eval again. Final result should match fresh eval with all 3 edges.
 * ================================================================ */
static void
test_sequential_inserts(void)
{
    TEST("sequential incremental inserts accumulate correctly");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

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
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session create failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    /* Initial eval: edge(1,2) -> tc = 1 tuple */
    struct rel_ctx ctx1 = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx1);
    ASSERT(rc == 0, "initial snapshot failed");
    ASSERT(ctx1.count == 1, "expected 1 TC tuple initially");

    /* Insert edge(2,3), re-eval: tc = 3 tuples */
    int64_t e23[2] = { 2, 3 };
    rc = col_session_insert_incremental(sess, "edge", e23, 1, 2);
    ASSERT(rc == 0, "insert edge(2,3) failed");

    struct rel_ctx ctx2 = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx2);
    ASSERT(rc == 0, "second snapshot failed");
    ASSERT(ctx2.count == 3, "expected 3 TC tuples after edge(2,3)");

    /* Insert edge(3,4), re-eval: tc = 6 tuples */
    int64_t e34[2] = { 3, 4 };
    rc = col_session_insert_incremental(sess, "edge", e34, 1, 2);
    ASSERT(rc == 0, "insert edge(3,4) failed");

    struct rel_ctx ctx3 = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx3);
    ASSERT(rc == 0, "third snapshot failed");
    ASSERT(ctx3.count == 6, "expected 6 TC tuples after edge(3,4)");

    /* Verify against fresh eval */
    const char *full_src = ".decl edge(x: int32, y: int32)\n"
                           "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                           ".decl tc(x: int32, y: int32)\n"
                           "tc(x, y) :- edge(x, y).\n"
                           "tc(x, z) :- tc(x, y), edge(y, z).\n";

    int64_t fresh_count = 0;
    rc = run_fresh(full_src, "tc", &fresh_count);
    ASSERT(rc == 0, "fresh eval failed");
    ASSERT(ctx3.count == fresh_count,
           "cumulative incremental must match fresh eval");

    printf("(1->%" PRId64 " 2->%" PRId64 " 3->%" PRId64 ") ", ctx1.count,
           ctx2.count, ctx3.count);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ================================================================
 * Test 3: Cyclic graph delta propagation
 *
 * Start with edge(1,2), edge(2,3). Insert edge(3,1) creating a cycle.
 * TC should have all 9 pairs (3 nodes, each reaches all others + self).
 * ================================================================ */
static void
test_cyclic_delta(void)
{
    TEST("cyclic graph delta propagation correctness");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

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
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session create failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    /* Initial eval: 1->2, 2->3, 1->3 = 3 tuples */
    rc = wl_session_snapshot(sess, noop_cb, NULL);
    ASSERT(rc == 0, "initial snapshot failed");

    /* Insert edge(3,1) creating cycle */
    int64_t e31[2] = { 3, 1 };
    rc = col_session_insert_incremental(sess, "edge", e31, 1, 2);
    ASSERT(rc == 0, "insert edge(3,1) failed");

    struct rel_ctx ctx = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    ASSERT(rc == 0, "incremental snapshot failed");

    /* Verify against fresh eval */
    const char *full_src = ".decl edge(x: int32, y: int32)\n"
                           "edge(1, 2). edge(2, 3). edge(3, 1).\n"
                           ".decl tc(x: int32, y: int32)\n"
                           "tc(x, y) :- edge(x, y).\n"
                           "tc(x, z) :- tc(x, y), edge(y, z).\n";

    int64_t fresh_count = 0;
    rc = run_fresh(full_src, "tc", &fresh_count);
    ASSERT(rc == 0, "fresh eval failed");

    printf("(incr=%" PRId64 " fresh=%" PRId64 ") ", ctx.count, fresh_count);
    ASSERT(ctx.count == fresh_count,
           "cyclic incremental must match fresh eval");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== Delta Propagation Tests (Issue #83) ===\n\n");

    test_tc_delta_matches_fresh();
    test_sequential_inserts();
    test_cyclic_delta();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(" (%d FAILED)", fail_count);
    printf(" ---\n");

    return fail_count > 0 ? 1 : 0;
}
