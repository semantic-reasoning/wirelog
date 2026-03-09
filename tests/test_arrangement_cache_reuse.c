/*
 * test_arrangement_cache_reuse.c - Arrangement cache reuse across rules within
 * an iteration (Phase 3C)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates that arrangement cache is reused across multiple rule accesses
 * within the same iteration, and only invalidated at iteration boundaries:
 *   1. First get_arrangement call builds the index (indexed_rows goes 0 -> N)
 *   2. Second get_arrangement call (same rel+key) returns identical pointer
 *   3. indexed_rows unchanged after second call (no rebuild between rules)
 *   4. Different key_cols produce a separate arrangement entry
 *   5. col_session_invalidate_arrangements resets indexed_rows to 0
 *   6. Next get_arrangement after invalidation rebuilds (indexed_rows = N)
 *   7. Multi-key: two rules joining on different columns each get own cache
 *   8. valueFlow-like: col 0 (z key) arrangement reused across R6/R9/R10-style
 *      rule accesses
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

/* build and evaluate a simple program, return open session.
 * Caller owns *out_sess, *out_plan, *out_prog and must free them. */
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
 * Test 1: First get_arrangement call builds the index
 *
 * Create a session with a 2-column relation (simulating valueFlow z,x).
 * First call to col_session_get_arrangement must return non-NULL with
 * indexed_rows > 0 (index was built).
 * ================================================================ */
static void
test_first_call_builds_index(void)
{
    TEST("First get_arrangement call: index built (indexed_rows > 0)");

    /* valueFlow-like: 2 columns (z, x); 4 base facts */
    const char *src = ".decl valueFlow(z: int32, x: int32)\n"
                      "valueFlow(10, 1). valueFlow(20, 2).\n"
                      "valueFlow(30, 3). valueFlow(40, 4).\n"
                      ".decl sink(z: int32, x: int32)\n"
                      "sink(z, x) :- valueFlow(z, x).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    /* Request arrangement on col 0 (z key) */
    uint32_t key_cols[1] = { 0 };
    col_arrangement_t *arr
        = col_session_get_arrangement(sess, "valueFlow", key_cols, 1);

    ASSERT(arr != NULL, "arrangement must be non-NULL for existing relation");
    ASSERT(arr->indexed_rows > 0,
           "first call must build index (indexed_rows > 0)");
    ASSERT(arr->indexed_rows == 4, "indexed_rows must equal fact count (4)");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 2: Second call returns same pointer (cache hit)
 *
 * Two consecutive calls to col_session_get_arrangement with the same
 * rel_name and key_cols must return the identical pointer.
 * This simulates R6 and R9 both accessing valueFlow on col 0.
 * ================================================================ */
static void
test_second_call_same_pointer(void)
{
    TEST("Second get_arrangement call: same pointer (cache hit)");

    const char *src = ".decl valueFlow(z: int32, x: int32)\n"
                      "valueFlow(10, 1). valueFlow(20, 2).\n"
                      "valueFlow(30, 3). valueFlow(40, 4).\n"
                      ".decl sink(z: int32, x: int32)\n"
                      "sink(z, x) :- valueFlow(z, x).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    uint32_t key_cols[1] = { 0 };

    /* First access: simulates R6 requesting arrangement */
    col_arrangement_t *arr_r6
        = col_session_get_arrangement(sess, "valueFlow", key_cols, 1);
    ASSERT(arr_r6 != NULL, "R6 arrangement must be non-NULL");

    /* Second access: simulates R9 requesting same arrangement */
    col_arrangement_t *arr_r9
        = col_session_get_arrangement(sess, "valueFlow", key_cols, 1);
    ASSERT(arr_r9 != NULL, "R9 arrangement must be non-NULL");

    ASSERT(arr_r6 == arr_r9,
           "R6 and R9 must receive same arrangement pointer (cache hit)");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 3: No rebuild between rule accesses (indexed_rows unchanged)
 *
 * After the first call builds the index, the second and third calls
 * must not increment indexed_rows further.  indexed_rows stays at N.
 * This simulates R6, R9, R10 all accessing valueFlow without rebuild.
 * ================================================================ */
static void
test_no_rebuild_between_rules(void)
{
    TEST("No rebuild between rules: indexed_rows unchanged on 2nd/3rd call");

    const char *src = ".decl valueFlow(z: int32, x: int32)\n"
                      "valueFlow(10, 1). valueFlow(20, 2).\n"
                      "valueFlow(30, 3). valueFlow(40, 4).\n"
                      "valueFlow(50, 5).\n"
                      ".decl sink(z: int32, x: int32)\n"
                      "sink(z, x) :- valueFlow(z, x).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    uint32_t key_cols[1] = { 0 };

    /* R6: first access, builds index */
    col_arrangement_t *arr
        = col_session_get_arrangement(sess, "valueFlow", key_cols, 1);
    ASSERT(arr != NULL, "arrangement must be non-NULL");
    uint32_t rows_after_r6 = arr->indexed_rows;
    ASSERT(rows_after_r6 == 5, "indexed_rows must be 5 after first build");

    /* R9: second access, must return same pointer, no rebuild */
    col_arrangement_t *arr2
        = col_session_get_arrangement(sess, "valueFlow", key_cols, 1);
    ASSERT(arr2 == arr, "must return same pointer");
    ASSERT(arr->indexed_rows == rows_after_r6,
           "indexed_rows must be unchanged after second access (no rebuild)");

    /* R10: third access */
    col_arrangement_t *arr3
        = col_session_get_arrangement(sess, "valueFlow", key_cols, 1);
    ASSERT(arr3 == arr, "third access must return same pointer");
    ASSERT(arr->indexed_rows == rows_after_r6,
           "indexed_rows must be unchanged after third access");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 4: Different key_cols produce separate cache entries
 *
 * An arrangement on col 0 and an arrangement on col 1 must be distinct
 * objects.  This ensures the cache key includes key_cols, not just rel_name.
 * ================================================================ */
static void
test_different_keycols_separate_entries(void)
{
    TEST("Different key_cols: separate cache entries (distinct pointers)");

    const char *src = ".decl valueFlow(z: int32, x: int32)\n"
                      "valueFlow(10, 1). valueFlow(20, 2).\n"
                      "valueFlow(30, 3).\n"
                      ".decl sink(z: int32, x: int32)\n"
                      "sink(z, x) :- valueFlow(z, x).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    /* Arrangement keyed on col 0 (z) */
    uint32_t key_col0[1] = { 0 };
    col_arrangement_t *arr_z
        = col_session_get_arrangement(sess, "valueFlow", key_col0, 1);
    ASSERT(arr_z != NULL, "arrangement on col 0 must be non-NULL");

    /* Arrangement keyed on col 1 (x) */
    uint32_t key_col1[1] = { 1 };
    col_arrangement_t *arr_x
        = col_session_get_arrangement(sess, "valueFlow", key_col1, 1);
    ASSERT(arr_x != NULL, "arrangement on col 1 must be non-NULL");

    ASSERT(arr_z != arr_x,
           "different key_cols must produce distinct cache entries");

    /* Both must be fully indexed */
    ASSERT(arr_z->indexed_rows == 3, "col-0 arrangement must index all 3 rows");
    ASSERT(arr_x->indexed_rows == 3, "col-1 arrangement must index all 3 rows");

    /* Repeated access to each must return same pointer (both cached) */
    col_arrangement_t *arr_z2
        = col_session_get_arrangement(sess, "valueFlow", key_col0, 1);
    col_arrangement_t *arr_x2
        = col_session_get_arrangement(sess, "valueFlow", key_col1, 1);
    ASSERT(arr_z2 == arr_z, "col-0 repeated access must be cached");
    ASSERT(arr_x2 == arr_x, "col-1 repeated access must be cached");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 5: Invalidation resets indexed_rows to 0
 *
 * col_session_invalidate_arrangements must set indexed_rows = 0 on all
 * cached arrangements for the named relation.  Simulates an iteration
 * boundary where consolidated data changes.
 * ================================================================ */
static void
test_invalidation_resets_indexed_rows(void)
{
    TEST("Invalidation: indexed_rows reset to 0 immediately");

    const char *src = ".decl valueFlow(z: int32, x: int32)\n"
                      "valueFlow(10, 1). valueFlow(20, 2).\n"
                      "valueFlow(30, 3). valueFlow(40, 4).\n"
                      ".decl sink(z: int32, x: int32)\n"
                      "sink(z, x) :- valueFlow(z, x).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    uint32_t key_cols[1] = { 0 };
    col_arrangement_t *arr
        = col_session_get_arrangement(sess, "valueFlow", key_cols, 1);
    ASSERT(arr != NULL, "arrangement must be non-NULL");
    ASSERT(arr->indexed_rows == 4,
           "indexed_rows must be 4 before invalidation");

    /* Simulate iteration boundary: invalidate */
    col_session_invalidate_arrangements(sess, "valueFlow");

    ASSERT(arr->indexed_rows == 0,
           "indexed_rows must be 0 immediately after invalidation");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 6: get_arrangement after invalidation rebuilds the index
 *
 * After col_session_invalidate_arrangements, the next call to
 * col_session_get_arrangement must trigger a full rebuild and return
 * indexed_rows == nrows again.
 * ================================================================ */
static void
test_rebuild_after_invalidation(void)
{
    TEST("Rebuild after invalidation: indexed_rows restored to N");

    const char *src = ".decl valueFlow(z: int32, x: int32)\n"
                      "valueFlow(10, 1). valueFlow(20, 2).\n"
                      "valueFlow(30, 3). valueFlow(40, 4).\n"
                      ".decl sink(z: int32, x: int32)\n"
                      "sink(z, x) :- valueFlow(z, x).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    uint32_t key_cols[1] = { 0 };

    /* Build arrangement */
    col_arrangement_t *arr
        = col_session_get_arrangement(sess, "valueFlow", key_cols, 1);
    ASSERT(arr != NULL, "arrangement must be non-NULL");
    ASSERT(arr->indexed_rows == 4,
           "indexed_rows must be 4 before invalidation");

    /* Invalidate (iteration boundary) */
    col_session_invalidate_arrangements(sess, "valueFlow");
    ASSERT(arr->indexed_rows == 0, "indexed_rows must be 0 after invalidation");

    /* Re-request: must trigger rebuild */
    col_arrangement_t *arr2
        = col_session_get_arrangement(sess, "valueFlow", key_cols, 1);
    ASSERT(arr2 != NULL, "arrangement must be non-NULL after rebuild");
    ASSERT(arr2->indexed_rows == 4,
           "indexed_rows must be restored to 4 after rebuild");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 7: Invalidation only affects named relation
 *
 * Invalidating valueFlow must not reset arrangements on other relations.
 * ================================================================ */
static void
test_invalidation_scope(void)
{
    TEST("Invalidation scope: only named relation is reset");

    const char *src = ".decl valueFlow(z: int32, x: int32)\n"
                      "valueFlow(10, 1). valueFlow(20, 2).\n"
                      ".decl edge(a: int32, b: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl sink(z: int32, x: int32)\n"
                      "sink(z, x) :- valueFlow(z, x).\n"
                      ".decl reach(a: int32, b: int32)\n"
                      "reach(a, b) :- edge(a, b).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    uint32_t key_cols[1] = { 0 };

    col_arrangement_t *arr_vf
        = col_session_get_arrangement(sess, "valueFlow", key_cols, 1);
    col_arrangement_t *arr_edge
        = col_session_get_arrangement(sess, "edge", key_cols, 1);

    ASSERT(arr_vf != NULL, "valueFlow arrangement must be non-NULL");
    ASSERT(arr_edge != NULL, "edge arrangement must be non-NULL");
    ASSERT(arr_vf->indexed_rows == 2, "valueFlow indexed_rows must be 2");
    ASSERT(arr_edge->indexed_rows == 3, "edge indexed_rows must be 3");

    /* Invalidate only valueFlow */
    col_session_invalidate_arrangements(sess, "valueFlow");

    ASSERT(arr_vf->indexed_rows == 0,
           "valueFlow indexed_rows must be 0 after invalidation");
    ASSERT(arr_edge->indexed_rows == 3,
           "edge indexed_rows must be unchanged (not invalidated)");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 8: valueFlow z-key cache reuse across R6/R9/R10-style accesses
 *
 * Simulate three rules in the same iteration all accessing valueFlow
 * keyed on col 0 (z).  All three must receive the same arrangement
 * with indexed_rows unchanged between accesses.
 *
 * This is the primary regression test for the reported issue:
 * arrangements must NOT be rebuilt between rules R6, R9, R10 that
 * all join on the same valueFlow(z, x) key column.
 * ================================================================ */
static void
test_valueflow_z_key_cache_reuse(void)
{
    TEST("valueFlow z-key: arrangement reused across R6/R9/R10-style accesses");

    /* valueFlow with 6 facts, 2 columns (z, x).
     * Three rules (R6, R9, R10) all join on col 0 = z. */
    const char *src = ".decl valueFlow(z: int32, x: int32)\n"
                      "valueFlow(10, 1). valueFlow(20, 2).\n"
                      "valueFlow(30, 3). valueFlow(40, 4).\n"
                      "valueFlow(50, 5). valueFlow(60, 6).\n"
                      ".decl result(z: int32, x: int32)\n"
                      "result(z, x) :- valueFlow(z, x).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    uint32_t key_col_z[1] = { 0 }; /* col 0 = z */

    /* R6 accesses valueFlow on z key */
    col_arrangement_t *arr_r6
        = col_session_get_arrangement(sess, "valueFlow", key_col_z, 1);
    ASSERT(arr_r6 != NULL, "R6: arrangement must be non-NULL");
    ASSERT(arr_r6->indexed_rows == 6, "R6: indexed_rows must be 6");
    uint32_t rows_at_r6 = arr_r6->indexed_rows;

    /* R9 accesses same arrangement */
    col_arrangement_t *arr_r9
        = col_session_get_arrangement(sess, "valueFlow", key_col_z, 1);
    ASSERT(arr_r9 != NULL, "R9: arrangement must be non-NULL");
    ASSERT(arr_r9 == arr_r6, "R9: must receive same pointer as R6 (cache hit)");
    ASSERT(arr_r9->indexed_rows == rows_at_r6,
           "R9: indexed_rows must be unchanged (no rebuild between R6 and R9)");

    /* R10 accesses same arrangement */
    col_arrangement_t *arr_r10
        = col_session_get_arrangement(sess, "valueFlow", key_col_z, 1);
    ASSERT(arr_r10 != NULL, "R10: arrangement must be non-NULL");
    ASSERT(arr_r10 == arr_r6,
           "R10: must receive same pointer as R6 (cache hit)");
    ASSERT(
        arr_r10->indexed_rows == rows_at_r6,
        "R10: indexed_rows must be unchanged (no rebuild between R9 and R10)");

    /* Verify that invalidation (iteration boundary) is what resets it */
    col_session_invalidate_arrangements(sess, "valueFlow");
    ASSERT(arr_r6->indexed_rows == 0,
           "indexed_rows must be 0 after iteration boundary invalidation");

    /* After iteration boundary: next rule access rebuilds */
    col_arrangement_t *arr_next_iter
        = col_session_get_arrangement(sess, "valueFlow", key_col_z, 1);
    ASSERT(arr_next_iter != NULL,
           "post-invalidation get_arrangement must return non-NULL");
    ASSERT(arr_next_iter->indexed_rows == 6,
           "post-invalidation: index must be fully rebuilt (indexed_rows = 6)");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * main
 * ================================================================ */

int
main(void)
{
    printf("\n=== Arrangement Cache Reuse Tests (Phase 3C) ===\n\n");

    test_first_call_builds_index();
    test_second_call_same_pointer();
    test_no_rebuild_between_rules();
    test_different_keycols_separate_entries();
    test_invalidation_resets_indexed_rows();
    test_rebuild_after_invalidation();
    test_invalidation_scope();
    test_valueflow_z_key_cache_reuse();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n\n");

    return fail_count > 0 ? 1 : 0;
}
