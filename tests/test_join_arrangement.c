/*
 * test_join_arrangement.c - col_op_join arrangement integration tests (3C-002)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates that col_op_join() correctly uses the arrangement cache:
 *   1. TC 3-edge correctness (6 tuples) — arrangement probe path
 *   2. TC 2-cycle correctness (4 tuples)
 *   3. 5-node chain (10 tuples) — larger relation, incremental update path
 *   4. Complete 3-node graph (9 tuples) — KI-1 dedup regression with arrangement
 *   5. K=1 vs K=2 parity — arrangement shared read-only across K-fusion workers
 *   6. Multi-stratum: edge EDB cached, reach IDB join uses cached edge arrangement
 *   7. After join, arrangement is populated for queried relation+key
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
#include <errno.h>
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
 * Helper: tuple counting callback
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

/* ----------------------------------------------------------------
 * Helper: run program end-to-end, return tuple count
 * ---------------------------------------------------------------- */

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
        /* Caller takes ownership of sess (plan/prog freed here) */
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

static col_rel_t *
make_join_left(void)
{
    col_rel_t *left = NULL;
    const char *cols[] = { "x", "payload" };
    wirelog_column_type_t types[] = {
        WIRELOG_TYPE_INT32,
        WIRELOG_TYPE_INT32,
    };
    int64_t row1[] = { 1, 10 };
    int64_t row2[] = { 2, 20 };

    if (col_rel_alloc(&left, "left") != 0)
        return NULL;
    if (col_rel_set_schema(left, 2, cols) != 0
        || col_rel_set_column_types(left, types, 2) != 0
        || col_rel_append_row(left, row1) != 0
        || col_rel_append_row(left, row2) != 0) {
        col_rel_destroy(left);
        return NULL;
    }
    return left;
}

static int
run_direct_join(wl_col_session_t *col_session, col_rel_t *left,
    eval_entry_t *out_entry)
{
    wl_plan_op_t op = { 0 };
    const char *left_keys[] = { "x" };
    const char *right_keys[] = { "x" };
    eval_stack_t stack;

    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "edge";
    op.key_count = 1;
    op.left_keys = left_keys;
    op.right_keys = right_keys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_init(&stack);
    if (eval_stack_push(&stack, left, false) != 0)
        return ENOMEM;
    int rc = col_op_join(&op, &stack, col_session);
    if (rc != 0) {
        eval_stack_drain(&stack);
        return rc;
    }
    if (out_entry)
        *out_entry = eval_stack_pop(&stack);
    else
        eval_stack_drain(&stack);
    return 0;
}

/* An enforcing governor that admits no arrangement hash table.  Existing
 * relations stay on their original governor; swapping this into a session
 * only affects subsequently attached arrangement entries. */
static wl_columnar_memory_governor_ref_t *
tight_arrangement_governor(void)
{
    wl_columnar_memory_resolution_t resolution = { 0 };
    resolution.budget_bytes = 1u;
    resolution.usable_bytes = 1u;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    return wl_columnar_memory_governor_ref_create(&resolution);
}

/* ================================================================
 * Test 1: TC 3-edge correctness — arrangement probe path
 *
 * r(x,z) :- r(x,y), r(y,z). — K=2, arrangement used for full r.
 * Base: r(1,2), r(2,3), r(3,4). Expected: 6 tuples.
 * ================================================================ */
static void
test_join_arr_tc_3edge(void)
{
    TEST("TC 3-edge: arrangement probe produces 6 tuples");

    const char *src = ".decl r(x: int32, y: int32)\n"
                      "r(1, 2). r(2, 3). r(3, 4).\n"
                      "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    int rc = run_program(src, "r", &count, NULL, NULL);
    ASSERT(rc == 0, "TC 3-edge program failed");
    ASSERT(count == 6, "expected 6 tuples from 3-edge TC");

    PASS();
}

/* ================================================================
 * Test 2: TC 2-cycle — arrangement with self-referential join
 *
 * r(1,2), r(2,1) -> closure {(1,2),(2,1),(1,1),(2,2)} = 4 tuples.
 * ================================================================ */
static void
test_join_arr_tc_2cycle(void)
{
    TEST("TC 2-cycle: arrangement probe produces 4 tuples");

    const char *src = ".decl r(x: int32, y: int32)\n"
                      "r(1, 2). r(2, 1).\n"
                      "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    int rc = run_program(src, "r", &count, NULL, NULL);
    ASSERT(rc == 0, "2-cycle program failed");
    ASSERT(count == 4, "expected 4 tuples from 2-cycle TC");

    PASS();
}

/* ================================================================
 * Test 3: 5-node chain — incremental arrangement update
 *
 * 5-node chain: 4 base edges, full closure = 10 tuples.
 * Tests that incremental arrangement update works across iterations.
 * ================================================================ */
static void
test_join_arr_5node_chain(void)
{
    TEST("5-node chain: arrangement incremental update, 10 tuples");

    const char *src = ".decl e(x: int32, y: int32)\n"
                      "e(1, 2). e(2, 3). e(3, 4). e(4, 5).\n"
                      ".decl reach(x: int32, y: int32)\n"
                      "reach(x, y) :- e(x, y).\n"
                      "reach(x, z) :- reach(x, y), reach(y, z).\n";

    int64_t count = 0;
    uint32_t iters = 0;
    int rc = run_program(src, "reach", &count, &iters, NULL);
    ASSERT(rc == 0, "5-node chain program failed");
    ASSERT(count == 10, "expected 10 tuples from 5-node chain");
    ASSERT(iters >= 2, "5-node chain needs at least 2 iterations");

    PASS();
}

/* ================================================================
 * Test 4: Complete 3-node graph — KI-1 dedup with arrangement
 *
 * Bidirectional 3-node: 6 base edges -> closure = 9 tuples.
 * Verifies arrangement-based probe does not re-introduce KI-1 over-count.
 * ================================================================ */
static void
test_join_arr_complete_graph_dedup(void)
{
    TEST("Complete 3-node graph: arrangement dedup = 9 tuples (KI-1)");

    const char *src = ".decl r(x: int32, y: int32)\n"
                      "r(1, 2). r(2, 1). r(1, 3). r(3, 1). r(2, 3). r(3, 2).\n"
                      "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    int rc = run_program(src, "r", &count, NULL, NULL);
    ASSERT(rc == 0, "complete-graph program failed");
    ASSERT(count == 9, "expected 9 tuples (KI-1 regression check)");

    PASS();
}

/* ================================================================
 * Test 5: K=1 vs K=2 parity — arrangement shared read-only
 *
 * K=1 (tc JOIN edge) vs K=2 (tc JOIN tc) must produce same 6-tuple TC.
 * K-fusion workers must share the arrangement pointer safely.
 * ================================================================ */
static void
test_join_arr_k1_k2_parity(void)
{
    TEST("K=1 vs K=2 parity: arrangement read-only across workers");

    const char *src_k1 = ".decl edge(x: int32, y: int32)\n"
                         "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                         ".decl tc(x: int32, y: int32)\n"
                         "tc(x, y) :- edge(x, y).\n"
                         "tc(x, z) :- tc(x, y), edge(y, z).\n";

    const char *src_k2 = ".decl e(x: int32, y: int32)\n"
                         "e(1, 2). e(2, 3). e(3, 4).\n"
                         ".decl tc(x: int32, y: int32)\n"
                         "tc(x, y) :- e(x, y).\n"
                         "tc(x, z) :- tc(x, y), tc(y, z).\n";

    int64_t count_k1 = 0, count_k2 = 0;
    int rc1 = run_program(src_k1, "tc", &count_k1, NULL, NULL);
    int rc2 = run_program(src_k2, "tc", &count_k2, NULL, NULL);

    ASSERT(rc1 == 0, "K=1 program failed");
    ASSERT(rc2 == 0, "K=2 program failed");
    ASSERT(count_k1 == 6, "K=1 expected 6 tuples");
    ASSERT(count_k2 == 6, "K=2 expected 6 tuples");
    ASSERT(count_k1 == count_k2, "K=1 and K=2 must agree");

    PASS();
}

/* ================================================================
 * Test 6: Multi-stratum — EDB cached, IDB join uses EDB arrangement
 *
 * Two strata: edge (EDB, stratum 0) + reach (recursive, stratum 1).
 * The EDB `edge` relation is never modified, so its arrangement built
 * in stratum 1 iter 0 should remain valid for all subsequent iterations.
 * Expected: 6 tuples (3-edge TC).
 * ================================================================ */
static void
test_join_arr_multi_stratum_edb_cached(void)
{
    TEST("Multi-stratum: EDB arrangement cached across iterations, 6 tuples");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl reach(x: int32, y: int32)\n"
                      "reach(x, y) :- edge(x, y).\n"
                      "reach(x, z) :- reach(x, y), edge(y, z).\n";

    int64_t count = 0;
    uint32_t iters = 0;
    int rc = run_program(src, "reach", &count, &iters, NULL);
    ASSERT(rc == 0, "multi-stratum program failed");
    ASSERT(count == 6, "expected 6 tuples from 3-edge TC");
    ASSERT(iters >= 1, "must require at least 1 iteration");

    PASS();
}

/* ================================================================
 * Test 7: Arrangement populated after join
 *
 * Run a K=1 multi-stratum program where edge is joined.  After
 * evaluation, col_session_get_arrangement on the EDB relation with
 * the known key column (col 0 = x) must return a non-NULL arrangement
 * with indexed_rows > 0.
 *
 * This is the primary arrangement-cache integration test: verifies
 * col_op_join() actually registers the arrangement in the session.
 * ================================================================ */
static void
test_join_arr_populated_after_join(void)
{
    TEST("Arrangement populated in session after join evaluation");

    /* K=1 program: reach JOIN edge ON reach.y = edge.x.
     * Right relation = edge; right key column = x = col 0. */
    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl reach(x: int32, y: int32)\n"
                      "reach(x, y) :- edge(x, y).\n"
                      "reach(x, z) :- reach(x, y), edge(y, z).\n";

    wl_session_t *sess = NULL;
    int64_t count = 0;
    int rc = run_program(src, "reach", &count, NULL, &sess);
    ASSERT(rc == 0, "program failed");
    ASSERT(count == 6, "expected 6 tuples");

    /* Query arrangement for 'edge' relation keyed on column 0 (x).
     * This matches the right key used in reach JOIN edge. */
    uint32_t key_cols[1] = { 0 };
    col_arrangement_t *arr
        = col_session_get_arrangement(sess, "edge", key_cols, 1);
    ASSERT(arr != NULL, "arrangement must be populated after join");
    ASSERT(arr->indexed_rows > 0,
           "arrangement must have indexed rows after join");

    wl_session_destroy(sess);

    PASS();
}

/* ================================================================
 * Test 8: Delta arrangement freed after K-fusion dispatch (3C-002-Ext)
 *
 * After K=2 evaluation completes (wl_session_snapshot returns), the main
 * session's delta arrangement cache must be empty.  K-fusion workers own
 * their own delta caches and free them on completion; the main session
 * should never accumulate orphaned delta arrangements.
 * ================================================================ */
static void
test_join_arr_darr_cleared_after_kfusion(void)
{
    TEST("Delta arr cache empty on main session after K=2 snapshot");

    const char *src = ".decl r(x: int32, y: int32)\n"
                      "r(1, 2). r(2, 3). r(3, 4).\n"
                      "r(x, z) :- r(x, y), r(y, z).\n";

    wl_session_t *sess = NULL;
    int64_t count = 0;
    int rc = run_program(src, "r", &count, NULL, &sess);
    ASSERT(rc == 0, "K=2 program failed");
    ASSERT(count == 6, "expected 6 tuples");

    /* Main session must have zero delta arrangement cache entries after
     * K-fusion dispatch: worker caches are per-worker and freed on join. */
    uint32_t darr_count = col_session_get_darr_count(sess);
    ASSERT(darr_count == 0,
           "main session must have 0 delta arr entries after K-fusion");

    wl_session_destroy(sess);

    PASS();
}

/* ================================================================
 * Test 9: Large K=2 correctness with delta arrangement (10-node chain)
 *
 * 10-node chain exercises many delta iterations, verifying that the
 * delta arrangement probe path (3C-002-Ext) produces correct results
 * across many incremental updates.
 *
 * Expected: 45 tuples (all pairs i<j in 1..10).
 * ================================================================ */
static void
test_join_arr_large_chain_delta(void)
{
    TEST("Large K=2 10-node chain with delta arrangement: 45 tuples");

    const char *src = ".decl r(x: int32, y: int32)\n"
                      "r(1,2). r(2,3). r(3,4). r(4,5). r(5,6).\n"
                      "r(6,7). r(7,8). r(8,9). r(9,10).\n"
                      "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    uint32_t iters = 0;
    int rc = run_program(src, "r", &count, &iters, NULL);
    ASSERT(rc == 0, "10-node chain program failed");
    ASSERT(count == 45, "10-node chain must produce 45 tuples");
    ASSERT(iters >= 3, "10-node chain needs at least 3 iterations");

    PASS();
}

/* ================================================================
 * Test 10: Primary full-right JOIN honors exact-source probe leases
 *
 * The unfiltered, non-delta primary arrangement path must acquire the exact
 * right source before any cold build or stale rebuild.  A right writer must
 * therefore return EBUSY instead of falling back to an ephemeral hash table.
 * ================================================================ */
static void
test_join_arr_primary_probe_writer_retry(void)
{
    TEST("Primary arrangement JOIN admits source before build/rebuild");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 100). edge(2, 200).\n";
    wl_session_t *sess = NULL;
    int64_t count = 0;
    int rc = run_program(src, "edge", &count, NULL, &sess);
    ASSERT(rc == 0, "edge fixture setup failed");
    ASSERT(sess != NULL, "session kept for direct join");

    wl_col_session_t *col_session = COL_SESSION(sess);
    col_rel_t *right = session_find_rel(col_session, "edge");
    ASSERT(right != NULL, "right relation lookup");

    uint32_t key_cols[] = { 0 };
    ASSERT(col_session->arr_count == 0, "right arrangement starts cold");

    wl_columnar_source_access_writer_t writer = { 0 };
    ASSERT(wl_columnar_source_access_writer_acquire(&right->source_access,
        &writer) == 0, "cold right writer acquire");

    col_rel_t *cold_busy_left = make_join_left();
    ASSERT(cold_busy_left != NULL, "cold busy left fixture");
    rc = run_direct_join(col_session, cold_busy_left, NULL);
    col_rel_destroy(cold_busy_left);
    ASSERT(rc == EBUSY, "cold active right writer must propagate EBUSY");
    ASSERT(col_session->arr_count == 0,
        "cold writer must not build arrangement before admission");
    ASSERT(wl_columnar_source_access_writer_release(&writer) == 0,
        "cold right writer release");

    col_rel_t *cold_retry_left = make_join_left();
    eval_entry_t cold_out = { 0 };
    ASSERT(cold_retry_left != NULL, "cold retry left fixture");
    rc = run_direct_join(col_session, cold_retry_left, &cold_out);
    col_rel_destroy(cold_retry_left);
    ASSERT(rc == 0, "cold join retry after writer release");
    ASSERT(cold_out.rel != NULL, "cold retry join produces a relation");
    col_rel_destroy(cold_out.rel);

    col_arrangement_t *arr = col_session_get_arrangement(sess, "edge",
            key_cols, 1);
    ASSERT(arr != NULL && arr->indexed_rows == right->nrows,
           "right arrangement built after admitted retry");

    ASSERT(wl_columnar_source_access_writer_acquire(&right->source_access,
        &writer) == 0, "prebuilt right writer acquire");

    col_rel_t *busy_left = make_join_left();
    ASSERT(busy_left != NULL, "busy left fixture");
    rc = run_direct_join(col_session, busy_left, NULL);
    col_rel_destroy(busy_left);
    ASSERT(rc == EBUSY, "prebuilt active right writer must propagate EBUSY");
    ASSERT(wl_columnar_source_access_writer_release(&writer) == 0,
        "prebuilt right writer release");

    col_rel_t *retry_left = make_join_left();
    eval_entry_t out = { 0 };
    ASSERT(retry_left != NULL, "retry left fixture");
    rc = run_direct_join(col_session, retry_left, &out);
    col_rel_destroy(retry_left);
    ASSERT(rc == 0, "join retry after writer release");
    ASSERT(out.rel != NULL, "retry join produces a relation");
    col_rel_destroy(out.rel);

    ASSERT(wl_columnar_source_access_writer_acquire(&right->source_access,
        &writer) == 0, "right writer reacquire after retry");
    ASSERT(wl_columnar_source_access_writer_release(&writer) == 0,
        "right writer release after retry");

    int64_t stale_row[] = { 3, 300 };
    ASSERT(col_rel_append_row(right, stale_row) == 0,
        "append makes arrangement stale");
    ASSERT(wl_columnar_source_access_writer_acquire(&right->source_access,
        &writer) == 0, "stale right writer acquire");

    col_rel_t *stale_busy_left = make_join_left();
    ASSERT(stale_busy_left != NULL, "stale busy left fixture");
    rc = run_direct_join(col_session, stale_busy_left, NULL);
    col_rel_destroy(stale_busy_left);
    ASSERT(rc == EBUSY, "stale active right writer must propagate EBUSY");
    ASSERT(wl_columnar_source_access_writer_release(&writer) == 0,
        "stale right writer release");

    col_rel_t *stale_retry_left = make_join_left();
    eval_entry_t stale_out = { 0 };
    ASSERT(stale_retry_left != NULL, "stale retry left fixture");
    rc = run_direct_join(col_session, stale_retry_left, &stale_out);
    col_rel_destroy(stale_retry_left);
    ASSERT(rc == 0, "stale join retry after writer release");
    ASSERT(stale_out.rel != NULL, "stale retry join produces a relation");
    col_rel_destroy(stale_out.rel);

    wl_session_destroy(sess);
    PASS();
}

/* ================================================================
 * Test 11: Primary full-right JOIN propagates arrangement build ENOMEM
 *
 * ENOENT is the only primary probe miss that may use the old ephemeral
 * fallback.  Once the probe has admitted the exact source, allocation failure
 * during cold build or stale rebuild must remain visible to the caller.
 * ================================================================ */
static void
test_join_arr_primary_probe_enomem_propagates(void)
{
    TEST("Primary arrangement JOIN propagates build ENOMEM");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 100). edge(2, 200).\n";
    wl_session_t *sess = NULL;
    int64_t count = 0;
    int rc = run_program(src, "edge", &count, NULL, &sess);
    ASSERT(rc == 0, "edge fixture setup failed");
    ASSERT(sess != NULL, "session kept for direct join");

    wl_col_session_t *col_session = COL_SESSION(sess);
    ASSERT(col_session->arr_count == 0, "right arrangement starts cold");

    wl_columnar_memory_governor_ref_t *tight
        = tight_arrangement_governor();
    ASSERT(tight != NULL, "tight arrangement governor allocation failed");
    wl_columnar_memory_governor_ref_t *orig_governor
        = col_session->memory_governor;
    col_session->memory_governor = tight;

    col_rel_t *left = make_join_left();
    ASSERT(left != NULL, "ENOMEM left fixture");
    rc = run_direct_join(col_session, left, NULL);
    col_rel_destroy(left);

    col_session->memory_governor = orig_governor;
    wl_columnar_memory_governor_ref_release(tight);

    ASSERT(rc == ENOMEM, "primary build ENOMEM must not fall back");
    ASSERT(col_session->arr_count == 0,
        "failed primary build must not publish an arrangement");

    wl_session_destroy(sess);
    PASS();
}

/* ================================================================
 * main
 * ================================================================ */

int
main(void)
{
    printf("\n=== Join Arrangement Integration Tests (3C-002 / 3C-002-Ext) "
           "===\n\n");

    test_join_arr_tc_3edge();
    test_join_arr_tc_2cycle();
    test_join_arr_5node_chain();
    test_join_arr_complete_graph_dedup();
    test_join_arr_k1_k2_parity();
    test_join_arr_multi_stratum_edb_cached();
    test_join_arr_populated_after_join();
    test_join_arr_darr_cleared_after_kfusion();
    test_join_arr_large_chain_delta();
    test_join_arr_primary_probe_writer_retry();
    test_join_arr_primary_probe_enomem_propagates();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n\n");

    return fail_count > 0 ? 1 : 0;
}
