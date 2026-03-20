/*
 * test_diff_recursive_arrangement.c - Tests for Issue #282:
 *   Incremental arrangement building for recursive strata.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Verifies that differential operators are activated during recursive
 * fixed-point iterations (eff_iter > 0) when diff_enabled is true,
 * enabling O(D) incremental hash build instead of O(N) full rebuild.
 *
 * Tests:
 *   1. Correctness: TC with diff recursive path == epoch path (3-node)
 *   2. Correctness: TC with diff recursive path == epoch path (4-node)
 *   3. Correctness: cycle TC with diff recursive path
 *   4. Correctness: 5-node chain TC
 *   5. Guard: diff_operators_active is restored after recursive stratum
 *   6. Disabled: when diff_enabled=false, epoch path used throughout
 *   7. Incremental insert + recursive diff correctness
 */

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/internal.h"
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

#define TEST(name)                                          \
        do {                                                    \
            test_count++;                                       \
            printf("TEST %d: %s ... ", test_count, (name));    \
        } while (0)

#define PASS()              \
        do {                    \
            pass_count++;       \
            printf("PASS\n");   \
        } while (0)

#define FAIL(msg)                       \
        do {                                \
            fail_count++;                   \
            printf("FAIL: %s\n", (msg));    \
            return;                         \
        } while (0)

#define ASSERT(cond, msg)   \
        do {                    \
            if (!(cond))        \
            FAIL(msg);      \
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

/*
 * run_tc: Evaluate a TC program and return the tuple count for "tc".
 * diff_on controls whether diff_enabled is set on the session.
 * Returns -1 on error.
 */
static int64_t
run_tc(const char *src, bool diff_on)
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

    COL_SESSION(sess)->diff_enabled = diff_on;

    rc = wl_session_load_facts(sess, prog);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    struct rel_ctx ctx = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    int64_t result = ctx.count;
    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return result;
}

/* ----------------------------------------------------------------
 * TC programs (wirelog syntax with .decl / .output)
 * ---------------------------------------------------------------- */

/* 3-node chain 1->2->3: 3 TC pairs */
static const char *TC_3NODE =
    ".decl edge(x: int32, y: int32)\n"
    "edge(1, 2). edge(2, 3).\n"
    ".decl tc(x: int32, y: int32)\n"
    "tc(x, y) :- edge(x, y).\n"
    "tc(x, z) :- tc(x, y), edge(y, z).\n"
    ".output tc\n";

/* 4-node chain 1->2->3->4: 6 TC pairs */
static const char *TC_4NODE =
    ".decl edge(x: int32, y: int32)\n"
    "edge(1, 2). edge(2, 3). edge(3, 4).\n"
    ".decl tc(x: int32, y: int32)\n"
    "tc(x, y) :- edge(x, y).\n"
    "tc(x, z) :- tc(x, y), edge(y, z).\n"
    ".output tc\n";

/* 3-node cycle 1->2->3->1: 9 TC pairs */
static const char *TC_CYCLE_3 =
    ".decl edge(x: int32, y: int32)\n"
    "edge(1, 2). edge(2, 3). edge(3, 1).\n"
    ".decl tc(x: int32, y: int32)\n"
    "tc(x, y) :- edge(x, y).\n"
    "tc(x, z) :- tc(x, y), edge(y, z).\n"
    ".output tc\n";

/* 5-node chain 1->2->3->4->5: 10 TC pairs */
static const char *TC_5NODE =
    ".decl edge(x: int32, y: int32)\n"
    "edge(1, 2). edge(2, 3). edge(3, 4). edge(4, 5).\n"
    ".decl tc(x: int32, y: int32)\n"
    "tc(x, y) :- edge(x, y).\n"
    "tc(x, z) :- tc(x, y), edge(y, z).\n"
    ".output tc\n";

/* ----------------------------------------------------------------
 * Tests
 * ---------------------------------------------------------------- */

static void
test_recursive_diff_3node_chain(void)
{
    TEST("recursive diff: 3-node chain TC diff==epoch");
    int64_t epoch = run_tc(TC_3NODE, false);
    int64_t diff = run_tc(TC_3NODE, true);
    ASSERT(epoch == 3, "epoch path: expected 3 tc tuples");
    ASSERT(diff == 3, "diff path: expected 3 tc tuples");
    ASSERT(diff == epoch, "diff path != epoch path");
    PASS();
}

static void
test_recursive_diff_4node_chain(void)
{
    TEST("recursive diff: 4-node chain TC diff==epoch");
    int64_t epoch = run_tc(TC_4NODE, false);
    int64_t diff = run_tc(TC_4NODE, true);
    ASSERT(epoch == 6, "epoch path: expected 6 tc tuples");
    ASSERT(diff == 6, "diff path: expected 6 tc tuples");
    ASSERT(diff == epoch, "diff path != epoch path");
    PASS();
}

static void
test_recursive_diff_cycle(void)
{
    TEST("recursive diff: 3-node cycle TC diff==epoch");
    int64_t epoch = run_tc(TC_CYCLE_3, false);
    int64_t diff = run_tc(TC_CYCLE_3, true);
    ASSERT(epoch == 9, "epoch path: expected 9 tc tuples");
    ASSERT(diff == 9, "diff path: expected 9 tc tuples");
    ASSERT(diff == epoch, "diff path != epoch path");
    PASS();
}

static void
test_recursive_diff_5node_chain(void)
{
    TEST("recursive diff: 5-node chain TC diff==epoch");
    int64_t epoch = run_tc(TC_5NODE, false);
    int64_t diff = run_tc(TC_5NODE, true);
    ASSERT(epoch == 10, "epoch path: expected 10 tc tuples");
    ASSERT(diff == 10, "diff path: expected 10 tc tuples");
    ASSERT(diff == epoch, "diff path != epoch path");
    PASS();
}

static void
test_diff_operators_restored_after_recursive_stratum(void)
{
    TEST("recursive diff: diff_operators_active restored after recursive eval");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(TC_3NODE, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan_from_program failed");

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session_create failed");

    wl_col_session_t *cs = COL_SESSION(sess);
    cs->diff_enabled = true;

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load_facts failed");

    struct rel_ctx ctx = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    ASSERT(rc == 0, "snapshot failed");
    ASSERT(ctx.count == 3, "expected 3 tc tuples");

    /* After a bulk (non-incremental) snapshot, affected_mask==UINT64_MAX,
     * so col_should_activate_diff returns false.  The per-iteration override
     * inside the recursive loop must not leak out after the stratum completes. */
    ASSERT(cs->diff_operators_active == false,
        "diff_operators_active should be false after full bulk eval");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_recursive_diff_disabled_stays_epoch(void)
{
    TEST("recursive diff: diff_enabled=false gives correct results");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(TC_3NODE, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan_from_program failed");

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session_create failed");

    wl_col_session_t *cs = COL_SESSION(sess);
    cs->diff_enabled = false;

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load_facts failed");

    struct rel_ctx ctx = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    ASSERT(rc == 0, "snapshot failed");
    ASSERT(ctx.count == 3, "expected 3 tc tuples with diff disabled");
    ASSERT(cs->diff_operators_active == false,
        "diff_operators_active must be false when diff_enabled=false");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_recursive_diff_incremental_insert(void)
{
    TEST("recursive diff: incremental insert TC correctness");

    /* Program without inline facts; edges inserted via wl_session_insert */
    const char *prog_src =
        ".decl edge(x: int32, y: int32)\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n"
        ".output tc\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(prog_src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan_from_program failed");

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session_create failed");

    COL_SESSION(sess)->diff_enabled = true;

    /* Initial snapshot: tc empty */
    struct rel_ctx ctx = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    ASSERT(rc == 0, "initial snapshot failed");
    ASSERT(ctx.count == 0, "initial tc should be empty");

    /* Insert edge(1,2): tc should have 1 pair */
    int64_t row1[2] = { 1, 2 };
    rc = col_session_insert_incremental(sess, "edge", row1, 1, 2);
    ASSERT(rc == 0, "insert edge(1,2) failed");
    ctx.count = 0;
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    ASSERT(rc == 0, "snapshot after edge(1,2) failed");
    ASSERT(ctx.count == 1, "expected 1 tc tuple after edge(1,2)");

    /* Insert edge(2,3): tc should have 3 pairs */
    int64_t row2[2] = { 2, 3 };
    rc = col_session_insert_incremental(sess, "edge", row2, 1, 2);
    ASSERT(rc == 0, "insert edge(2,3) failed");
    ctx.count = 0;
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    ASSERT(rc == 0, "snapshot after edge(2,3) failed");
    ASSERT(ctx.count == 3, "expected 3 tc tuples after edges (1,2) (2,3)");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */
int
main(void)
{
    printf("=== test_diff_recursive_arrangement (Issue #282) ===\n\n");

    printf("-- Correctness: diff path == epoch path --\n");
    test_recursive_diff_3node_chain();
    test_recursive_diff_4node_chain();
    test_recursive_diff_cycle();
    test_recursive_diff_5node_chain();

    printf("\n-- Guard state --\n");
    test_diff_operators_restored_after_recursive_stratum();
    test_recursive_diff_disabled_stays_epoch();

    printf("\n-- Incremental correctness --\n");
    test_recursive_diff_incremental_insert();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(" (%d FAILED)", fail_count);
    printf(" ---\n");

    return fail_count > 0 ? 1 : 0;
}
