/*
 * test_arrangement_incremental_invalidation.c - Tests for issue #92
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Verifies that col_session_insert_incremental invalidates arrangement
 * caches for the modified relation so subsequent re-evaluation sees
 * newly inserted rows.
 *
 *   1. After insert_incremental, arrangement indexed_rows == 0 for modified rel
 *   2. After insert_incremental, arrangement for unmodified rel is NOT reset
 *   3. Re-evaluation after insert_incremental produces correct results
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

    if (wl_session_snapshot(sess, noop_cb, NULL) != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    *out_sess = sess;
    *out_plan = plan;
    *out_prog = prog;
    return 0;
}

static void
free_session(wl_session_t *sess, wl_plan_t *plan, wirelog_program_t *prog)
{
    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
}

/* ================================================================
 * Test 1: insert_incremental invalidates arrangement for modified relation
 *
 * Build an arrangement on an EDB relation, then call
 * col_session_insert_incremental. The arrangement's indexed_rows
 * must be reset to 0.
 * ================================================================ */
static void
test_insert_invalidates_modified_rel(void)
{
    TEST("insert_incremental invalidates arrangement for modified relation");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    /* Build arrangement on edge(col 0) */
    uint32_t key_cols[1] = { 0 };
    col_arrangement_t *arr
        = col_session_get_arrangement(sess, "edge", key_cols, 1);
    ASSERT(arr != NULL, "arrangement must be non-NULL");
    ASSERT(arr->indexed_rows == 3, "indexed_rows must be 3 before insert");

    /* Insert a new edge via incremental path */
    int64_t new_edge[2] = { 4, 5 };
    int rc = col_session_insert_incremental(sess, "edge", new_edge, 1, 2);
    ASSERT(rc == 0, "insert_incremental must succeed");

    /* Arrangement must be invalidated */
    ASSERT(arr->indexed_rows == 0,
           "indexed_rows must be 0 after insert_incremental");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 2: insert_incremental does NOT invalidate unmodified relations
 *
 * Two EDB relations: edge and weight. Insert into edge only.
 * Arrangement on weight must remain valid (indexed_rows unchanged).
 * ================================================================ */
static void
test_insert_preserves_unmodified_rel(void)
{
    TEST("insert_incremental preserves arrangement for unmodified relation");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3).\n"
                      ".decl weight(x: int32, w: int32)\n"
                      "weight(1, 10). weight(2, 20). weight(3, 30).\n"
                      ".decl sink(x: int32, y: int32)\n"
                      "sink(x, y) :- edge(x, y).\n"
                      ".decl wsink(x: int32, w: int32)\n"
                      "wsink(x, w) :- weight(x, w).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    /* Build arrangements on both relations */
    uint32_t key_cols[1] = { 0 };
    col_arrangement_t *arr_edge
        = col_session_get_arrangement(sess, "edge", key_cols, 1);
    col_arrangement_t *arr_weight
        = col_session_get_arrangement(sess, "weight", key_cols, 1);
    ASSERT(arr_edge != NULL, "edge arrangement must be non-NULL");
    ASSERT(arr_weight != NULL, "weight arrangement must be non-NULL");
    ASSERT(arr_edge->indexed_rows == 2, "edge indexed_rows must be 2");
    ASSERT(arr_weight->indexed_rows == 3, "weight indexed_rows must be 3");

    /* Insert into edge only */
    int64_t new_edge[2] = { 3, 4 };
    int rc = col_session_insert_incremental(sess, "edge", new_edge, 1, 2);
    ASSERT(rc == 0, "insert_incremental must succeed");

    /* edge arrangement invalidated, weight arrangement preserved */
    ASSERT(arr_edge->indexed_rows == 0,
           "edge indexed_rows must be 0 after insert");
    ASSERT(arr_weight->indexed_rows == 3,
           "weight indexed_rows must still be 3 (not invalidated)");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 3: Re-evaluation after insert_incremental includes new facts
 *
 * Run TC to fixpoint, insert a new edge, re-evaluate, and verify
 * the new edge is included in join results.
 * ================================================================ */
static void
test_reeval_includes_new_facts(void)
{
    TEST("re-evaluation after insert_incremental includes new facts");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    /* Count TC tuples before insert: 1->2, 2->3, 1->3 = 3 */
    struct rel_ctx ctx1 = { "tc", 0 };
    int rc = wl_session_snapshot(sess, count_cb, &ctx1);
    ASSERT(rc == 0, "snapshot failed");
    ASSERT(ctx1.count == 3, "expected 3 TC tuples before insert");

    /* Insert edge(3, 4) */
    int64_t new_edge[2] = { 3, 4 };
    rc = col_session_insert_incremental(sess, "edge", new_edge, 1, 2);
    ASSERT(rc == 0, "insert_incremental must succeed");

    /* Re-evaluate: step triggers re-computation */
    rc = wl_session_step(sess);
    ASSERT(rc == 0, "session_step must succeed");

    /* Count TC tuples after: should include paths through new edge
     * edges: 1->2, 2->3, 3->4
     * tc: 1->2, 1->3, 1->4, 2->3, 2->4, 3->4 = 6 */
    struct rel_ctx ctx2 = { "tc", 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx2);
    ASSERT(rc == 0, "snapshot after step failed");
    ASSERT(ctx2.count == 6, "expected 6 TC tuples after inserting edge(3,4)");

    printf("(before=%" PRId64 " after=%" PRId64 ") ", ctx1.count, ctx2.count);

    free_session(sess, plan, prog);
    PASS();
}

/* ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== Arrangement Incremental Invalidation Tests (Issue #92) "
           "===\n\n");

    test_insert_invalidates_modified_rel();
    test_insert_preserves_unmodified_rel();
    test_reeval_includes_new_facts();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(" (%d FAILED)", fail_count);
    printf(" ---\n");

    return fail_count > 0 ? 1 : 0;
}
