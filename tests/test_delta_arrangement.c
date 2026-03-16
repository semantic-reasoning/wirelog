/*
 * test_delta_arrangement.c - Delta arrangement cache tests (Phase 3C-001-Ext)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates per-worker delta arrangement caching:
 *   1. TC 3-edge correctness — delta probe path (regression)
 *   2. TC 2-cycle correctness — self-referential delta join
 *   3. 5-node chain (10 tuples) — multiple iterations, stale detection
 *   4. Complete 3-node graph (9 tuples) — KI-1 dedup regression
 *   5. Main session delta cache is empty after evaluation (per-worker isolation)
 *
 * Tests 1-4 are GREEN even without 3C-001-Ext (ephemeral hash produces correct
 * results). They are included as regression guards. Test 5 is the structural
 * validation specific to this feature: it verifies the delta arrangement cache
 * is per-worker (not accumulated on the main session).
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
 * Private session mirror (partial)
 *
 * Mirrors the first fields of wl_col_session_t up to darr_cap, to
 * allow verification that darr_count == 0 on the main session after
 * evaluation (per-worker isolation invariant).
 *
 * KEEP IN SYNC with wl_col_session_t in columnar_nanoarrow.c.
 *
 * Layout (x86-64, LP64):
 *   wl_session_t   base          (sizeof(wl_session_t) bytes)
 *   const void    *plan          (8)
 *   col_rel_t    **rels          (8)
 *   uint32_t       nrels         (4)
 *   uint32_t       rel_cap       (4)
 *   wl_on_delta_fn delta_cb      (8)  -- function pointer
 *   void          *delta_data    (8)
 *   wl_arena_t    *eval_arena    (8)
 *   col_mat_cache_t mat_cache    (sizeof(col_mat_cache_t))
 *   uint32_t       total_iterations (4)
 *   uint32_t       _pad_iters    (4 implicit pad before pointer)
 *   wl_work_queue_t *wq          (8)
 *   uint64_t       consolidation_ns (8)
 *   uint64_t       kfusion_ns    (8)
 *   col_arr_entry_t *arr_entries (8)
 *   uint32_t       arr_count     (4)
 *   uint32_t       arr_cap       (4)
 *   col_arr_entry_t *darr_entries (8)
 *   uint32_t       darr_count    (4)
 *   uint32_t       darr_cap      (4)
 *
 * Rather than mirror the full struct (fragile), we use offsetof-based
 * access for darr_count only.
 * ---------------------------------------------------------------- */

/*
 * col_mat_cache_t is also private. Its size determines where darr_* fields
 * start. We use a static assert approach: compute the expected session size
 * by summing field sizes, then use a test to verify darr_count == 0.
 *
 * To avoid brittle offset computation, we instead verify the invariant
 * indirectly: after evaluation, call col_session_get_delta_arrangement
 * is NOT expected to persist beyond the session scope.  We verify
 * this by checking there are no darr_entries cached in the session
 * using the accessor defined in 3C-001-Ext.
 */

/* ----------------------------------------------------------------
 * Helpers
 * ---------------------------------------------------------------- */

struct count_ctx {
    int64_t count;
    const char *tracked;
};

static void
count_cb(const char *rel, const int64_t *row, uint32_t nc, void *u)
{
    (void)row;
    (void)nc;
    struct count_ctx *ctx = (struct count_ctx *)u;
    if (rel && ctx->tracked && strcmp(rel, ctx->tracked) == 0)
        ctx->count++;
}

static int
run_program(const char *src, const char *rel, int64_t *out_count,
            uint32_t *out_iters, wl_session_t **out_sess_keep)
{
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

    struct count_ctx ctx = { 0, rel };
    int rc = wl_session_snapshot(sess, count_cb, &ctx);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    if (out_count)
        *out_count = ctx.count;
    if (out_iters)
        *out_iters = col_session_get_iteration_count(sess);

    if (out_sess_keep) {
        *out_sess_keep = sess;
        wl_plan_free(plan);
        wirelog_program_free(prog);
    } else {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
    }
    return 0;
}

/* ================================================================
 * Test 1: TC 3-edge — delta path correctness (6 tuples)
 *
 * K=2 semi-naive: worker 1 probes Δr on right side.
 * With 3C-001-Ext, delta arrangement is used instead of ephemeral hash.
 * ================================================================ */
static void
test_delta_arr_tc_3edge(void)
{
    TEST("TC 3-edge: delta arrangement probe produces 6 tuples");

    const char *src = ".decl r(x: int32, y: int32)\n"
                      "r(1, 2). r(2, 3). r(3, 4).\n"
                      "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    ASSERT(run_program(src, "r", &count, NULL, NULL) == 0,
           "TC 3-edge program failed");
    ASSERT(count == 6, "expected 6 tuples from 3-edge TC");

    PASS();
}

/* ================================================================
 * Test 2: TC 2-cycle — self-referential delta join
 *
 * r(1,2), r(2,1) -> closure {(1,1),(1,2),(2,1),(2,2)} = 4 tuples.
 * Verifies delta arrangement doesn't duplicate or drop self-joins.
 * ================================================================ */
static void
test_delta_arr_tc_2cycle(void)
{
    TEST("TC 2-cycle: delta arrangement self-join = 4 tuples");

    const char *src = ".decl r(x: int32, y: int32)\n"
                      "r(1, 2). r(2, 1).\n"
                      "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    ASSERT(run_program(src, "r", &count, NULL, NULL) == 0,
           "2-cycle program failed");
    ASSERT(count == 4, "expected 4 tuples from 2-cycle TC");

    PASS();
}

/* ================================================================
 * Test 3: 5-node chain — stale delta detection across iterations
 *
 * 5-node chain requires multiple iterations.  Each iteration the delta
 * grows; the delta arrangement must detect stale indexed_rows and
 * rebuild (not silently return the old arrangement).
 * Expected: 10 tuples.
 * ================================================================ */
static void
test_delta_arr_5node_chain(void)
{
    TEST("5-node chain: stale delta detected and rebuilt, 10 tuples");

    const char *src = ".decl e(x: int32, y: int32)\n"
                      "e(1, 2). e(2, 3). e(3, 4). e(4, 5).\n"
                      ".decl reach(x: int32, y: int32)\n"
                      "reach(x, y) :- e(x, y).\n"
                      "reach(x, z) :- reach(x, y), reach(y, z).\n";

    int64_t count = 0;
    uint32_t iters = 0;
    ASSERT(run_program(src, "reach", &count, &iters, NULL) == 0,
           "5-node chain program failed");
    ASSERT(count == 10, "expected 10 tuples from 5-node chain");
    ASSERT(iters >= 2, "5-node chain needs at least 2 iterations");

    PASS();
}

/* ================================================================
 * Test 4: Complete 3-node graph — KI-1 dedup with delta arrangement
 *
 * Bidirectional 3-node: 6 base edges -> closure = 9 tuples.
 * Delta arrangement must not reintroduce the KI-1 over-count bug.
 * ================================================================ */
static void
test_delta_arr_complete_graph_dedup(void)
{
    TEST("Complete 3-node graph: delta arrangement dedup = 9 tuples (KI-1)");

    const char *src = ".decl r(x: int32, y: int32)\n"
                      "r(1, 2). r(2, 1). r(1, 3). r(3, 1). r(2, 3). r(3, 2).\n"
                      "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    ASSERT(run_program(src, "r", &count, NULL, NULL) == 0,
           "complete-graph program failed");
    ASSERT(count == 9,
           "expected 9 tuples (KI-1 regression check with delta arrangement)");

    PASS();
}

/* ================================================================
 * Test 5: Per-worker isolation — main session darr_count stays 0
 *
 * After evaluation, the main session must have darr_count == 0.
 * Delta arrangements are built only in per-worker session copies
 * (which are freed after K-fusion dispatch).
 *
 * Uses col_session_get_delta_arrangement_count() accessor introduced
 * in 3C-001-Ext (returns cs->darr_count for the given session).
 * ================================================================ */
static void
test_delta_arr_worker_isolation(void)
{
    TEST("Worker isolation: main session darr_count == 0 after evaluation");

    const char *src = ".decl r(x: int32, y: int32)\n"
                      "r(1, 2). r(2, 3). r(3, 4).\n"
                      "r(x, z) :- r(x, y), r(y, z).\n";

    wl_session_t *sess = NULL;
    int64_t count = 0;
    ASSERT(run_program(src, "r", &count, NULL, &sess) == 0,
           "TC 3-edge program failed");
    ASSERT(count == 6, "expected 6 tuples");

    uint32_t darr_count = col_session_get_darr_count(sess);
    ASSERT(darr_count == 0,
           "darr_count must be 0 on main session: delta caches are per-worker");

    wl_session_destroy(sess);
    PASS();
}

/* ================================================================
 * main
 * ================================================================ */

int
main(void)
{
    printf("\n=== Delta Arrangement Cache Tests (Phase 3C-001-Ext) ===\n\n");

    test_delta_arr_tc_3edge();
    test_delta_arr_tc_2cycle();
    test_delta_arr_5node_chain();
    test_delta_arr_complete_graph_dedup();
    test_delta_arr_worker_isolation();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n\n");

    return fail_count > 0 ? 1 : 0;
}
