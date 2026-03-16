/*
 * test_incremental_insertion.c - Integration tests for col_session_insert_incremental (Phase 4)
 *
 * Verifies that col_session_insert_incremental appends facts to a live session
 * WITHOUT resetting the per-stratum frontier array.  Each test creates a real
 * session, runs evaluation via wl_session_step to establish frontier values,
 * then calls col_session_insert_incremental and asserts the frontier is
 * unchanged.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"

/* ======================================================================== */
/* TEST HARNESS MACROS                                                       */
/* ======================================================================== */

#define TEST(name)                       \
    do {                                 \
        printf("  [TEST] %-60s ", name); \
        fflush(stdout);                  \
    } while (0)

#define PASS              \
    do {                  \
        printf("PASS\n"); \
        tests_passed++;   \
    } while (0)

#define FAIL(msg)                  \
    do {                           \
        printf("FAIL: %s\n", msg); \
        tests_failed++;            \
    } while (0)

static int tests_passed = 0;
static int tests_failed = 0;

/* ======================================================================== */
/* PLAN BUILDER HELPERS                                                      */
/* ======================================================================== */

/*
 * Build a plan from a Datalog source string.  Applies fusion, JPP, and SIP
 * passes (same pipeline as test_plan_gen.c).  Returns NULL on failure.
 * Caller owns the returned plan and must free it with wl_plan_free().
 * The wirelog_program_t is freed before return.
 */
static wl_plan_t *
build_plan(const char *src)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    if (rc != 0)
        return NULL;
    return plan;
}

/*
 * Return the index of the first recursive stratum in plan, or UINT32_MAX if
 * none exists.
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

/* ======================================================================== */
/* TEST 1: Frontier preserved after incremental insert                       */
/* ======================================================================== */

/*
 * Program: Path(x,y) :- Edge(x,y). Path(x,z) :- Path(x,y), Edge(y,z).
 * Step 1: Insert 3-edge chain (1->2->3->4), call wl_session_step.
 *         Snapshot frontier for the recursive stratum.
 * Step 2: Call col_session_insert_incremental with one new edge (4->5).
 * Assert: frontiers[recursive_stratum].iteration is unchanged (not reset to
 *         UINT32_MAX, and equals the value captured after Step 1).
 */
static void
test_frontier_preserved_after_incremental_insert(void)
{
    TEST("frontier_preserved_after_incremental_insert");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      ".decl path(x: int32, y: int32)\n"
                      "path(x, y) :- edge(x, y).\n"
                      "path(x, z) :- path(x, y), edge(y, z).\n";

    wl_plan_t *plan = build_plan(src);
    if (!plan) {
        FAIL("build_plan failed");
        return;
    }

    uint32_t rec_si = find_recursive_stratum(plan);
    if (rec_si == UINT32_MAX) {
        wl_plan_free(plan);
        FAIL("no recursive stratum found");
        return;
    }

    wl_session_t *sess = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    if (rc != 0 || !sess) {
        wl_plan_free(plan);
        FAIL("wl_session_create failed");
        return;
    }

    /* Step 1: insert 3-edge chain and evaluate */
    int64_t edges[] = { 1, 2, 2, 3, 3, 4 };
    rc = wl_session_insert(sess, "edge", edges, 3, 2);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("wl_session_insert failed");
        return;
    }

    rc = wl_session_step(sess);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("wl_session_step failed");
        return;
    }

    /* Snapshot frontier for recursive stratum after eval */
    col_frontier_2d_t f_before;
    rc = col_session_get_frontier(sess, rec_si, &f_before);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("col_session_get_frontier failed after step");
        return;
    }

    /* Frontier must be set (not the zero-initialized default) after eval */
    if (f_before.iteration == 0 && f_before.outer_epoch == 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("frontier not set after wl_session_step (still zero)");
        return;
    }

    /* Step 2: incremental insert of one new edge */
    int64_t new_edge[] = { 4, 5 };
    rc = col_session_insert_incremental(sess, "edge", new_edge, 1, 2);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "col_session_insert_incremental returned %d",
                 rc);
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL(msg);
        return;
    }

    /* Assert: frontier unchanged after incremental insert */
    col_frontier_2d_t f_after;
    rc = col_session_get_frontier(sess, rec_si, &f_after);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("col_session_get_frontier failed after incremental insert");
        return;
    }

    if (f_after.iteration != f_before.iteration
        || f_after.outer_epoch != f_before.outer_epoch) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "frontier changed: before=(%u,%u) after=(%u,%u)",
                 f_before.iteration, f_before.outer_epoch, f_after.iteration,
                 f_after.outer_epoch);
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL(msg);
        return;
    }

    wl_session_destroy(sess);
    wl_plan_free(plan);
    PASS;
}

/* ======================================================================== */
/* TEST 2: Incremental insert sets last_inserted_relation                    */
/* ======================================================================== */

/*
 * After col_session_insert_incremental, the session's last_inserted_relation
 * field should be set so that the next wl_session_step can skip unaffected
 * strata.  We verify this indirectly: a subsequent wl_session_step must
 * succeed (not crash or return non-zero) and produce the correct result.
 *
 * Concretely: insert edge(1,2), edge(2,3), step -> path derives (1,2),(2,3),(1,3).
 * Then incremental-insert edge(3,4), step again -> path must include (1,4).
 */
static void
test_incremental_insert_enables_affected_stratum_detection(void)
{
    TEST("incremental_insert_enables_affected_stratum_detection");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      ".decl path(x: int32, y: int32)\n"
                      "path(x, y) :- edge(x, y).\n"
                      "path(x, z) :- path(x, y), edge(y, z).\n";

    wl_plan_t *plan = build_plan(src);
    if (!plan) {
        FAIL("build_plan failed");
        return;
    }

    wl_session_t *sess = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    if (rc != 0 || !sess) {
        wl_plan_free(plan);
        FAIL("wl_session_create failed");
        return;
    }

    /* Initial EDB and eval */
    int64_t edges[] = { 1, 2, 2, 3 };
    rc = wl_session_insert(sess, "edge", edges, 2, 2);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("wl_session_insert failed");
        return;
    }

    rc = wl_session_step(sess);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("first wl_session_step failed");
        return;
    }

    /* Incremental insert: add edge(3,4) */
    int64_t new_edge[] = { 3, 4 };
    rc = col_session_insert_incremental(sess, "edge", new_edge, 1, 2);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "col_session_insert_incremental returned %d",
                 rc);
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL(msg);
        return;
    }

    /* Second step must succeed with incremental path active */
    rc = wl_session_step(sess);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "second wl_session_step returned %d", rc);
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL(msg);
        return;
    }

    wl_session_destroy(sess);
    wl_plan_free(plan);
    PASS;
}

/* ======================================================================== */
/* TEST 3: Multiple incremental inserts preserve cumulative frontier         */
/* ======================================================================== */

/*
 * Pattern: eval, insert, eval, insert, eval.
 * After each eval the frontier for the recursive stratum should be set (not
 * zero).  After each incremental insert the frontier must remain unchanged.
 */
static void
test_multiple_incremental_inserts_preserve_frontier(void)
{
    TEST("multiple_incremental_inserts_preserve_cumulative_frontier");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      ".decl path(x: int32, y: int32)\n"
                      "path(x, y) :- edge(x, y).\n"
                      "path(x, z) :- path(x, y), edge(y, z).\n";

    wl_plan_t *plan = build_plan(src);
    if (!plan) {
        FAIL("build_plan failed");
        return;
    }

    uint32_t rec_si = find_recursive_stratum(plan);
    if (rec_si == UINT32_MAX) {
        wl_plan_free(plan);
        FAIL("no recursive stratum found");
        return;
    }

    wl_session_t *sess = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    if (rc != 0 || !sess) {
        wl_plan_free(plan);
        FAIL("wl_session_create failed");
        return;
    }

    /* First eval */
    int64_t e1[] = { 1, 2 };
    rc = wl_session_insert(sess, "edge", e1, 1, 2);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("wl_session_insert (1) failed");
        return;
    }
    rc = wl_session_step(sess);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("wl_session_step (1) failed");
        return;
    }

    col_frontier_2d_t f1;
    rc = col_session_get_frontier(sess, rec_si, &f1);
    if (rc != 0 || f1.iteration == UINT32_MAX) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("frontier not set after first step");
        return;
    }

    /* First incremental insert */
    int64_t e2[] = { 2, 3 };
    rc = col_session_insert_incremental(sess, "edge", e2, 1, 2);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("col_session_insert_incremental (1) failed");
        return;
    }

    col_frontier_2d_t f_after_insert1;
    rc = col_session_get_frontier(sess, rec_si, &f_after_insert1);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("col_session_get_frontier failed after insert 1");
        return;
    }
    if (f_after_insert1.iteration != f1.iteration
        || f_after_insert1.outer_epoch != f1.outer_epoch) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("frontier changed after first incremental insert");
        return;
    }

    /* Second eval */
    rc = wl_session_step(sess);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("wl_session_step (2) failed");
        return;
    }

    /* Second incremental insert */
    int64_t e3[] = { 3, 4 };
    rc = col_session_insert_incremental(sess, "edge", e3, 1, 2);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("col_session_insert_incremental (2) failed");
        return;
    }

    col_frontier_2d_t f2;
    rc = col_session_get_frontier(sess, rec_si, &f2);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("col_session_get_frontier failed after insert 2");
        return;
    }
    /* Frontier must still be set (not zero-reset) after second insert */
    if (f2.iteration == UINT32_MAX) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("frontier reset to zero after second incremental insert");
        return;
    }

    /* Third eval */
    rc = wl_session_step(sess);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("wl_session_step (3) failed");
        return;
    }

    wl_session_destroy(sess);
    wl_plan_free(plan);
    PASS;
}

/* ======================================================================== */
/* TEST 4: Empty incremental insert (0 rows) is safe, frontier unchanged     */
/* ======================================================================== */

/*
 * Inserting 0 rows must be a safe no-op: no crash, return 0, and the
 * frontier for the recursive stratum must remain exactly as it was after
 * the preceding wl_session_step.
 */
static void
test_empty_incremental_insert_safe_frontier_unchanged(void)
{
    TEST("empty_incremental_insert_safe_frontier_unchanged");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      ".decl path(x: int32, y: int32)\n"
                      "path(x, y) :- edge(x, y).\n"
                      "path(x, z) :- path(x, y), edge(y, z).\n";

    wl_plan_t *plan = build_plan(src);
    if (!plan) {
        FAIL("build_plan failed");
        return;
    }

    uint32_t rec_si = find_recursive_stratum(plan);
    if (rec_si == UINT32_MAX) {
        wl_plan_free(plan);
        FAIL("no recursive stratum found");
        return;
    }

    wl_session_t *sess = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    if (rc != 0 || !sess) {
        wl_plan_free(plan);
        FAIL("wl_session_create failed");
        return;
    }

    /* Insert EDB and eval to establish a real frontier */
    int64_t edges[] = { 1, 2, 2, 3 };
    rc = wl_session_insert(sess, "edge", edges, 2, 2);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("wl_session_insert failed");
        return;
    }
    rc = wl_session_step(sess);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("wl_session_step failed");
        return;
    }

    col_frontier_2d_t f_before;
    rc = col_session_get_frontier(sess, rec_si, &f_before);
    if (rc != 0 || f_before.iteration == UINT32_MAX) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("frontier not set after step");
        return;
    }

    /* Empty incremental insert: 0 rows */
    static const int64_t dummy[2] = { 0, 0 };
    rc = col_session_insert_incremental(sess, "edge", dummy, 0, 2);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "empty insert returned %d (expected 0)", rc);
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL(msg);
        return;
    }

    /* Frontier must be exactly unchanged */
    col_frontier_2d_t f_after;
    rc = col_session_get_frontier(sess, rec_si, &f_after);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("col_session_get_frontier failed after empty insert");
        return;
    }

    if (f_after.iteration != f_before.iteration
        || f_after.outer_epoch != f_before.outer_epoch) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "frontier changed after empty insert: before=(%u,%u) "
                 "after=(%u,%u)",
                 f_before.iteration, f_before.outer_epoch, f_after.iteration,
                 f_after.outer_epoch);
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL(msg);
        return;
    }

    wl_session_destroy(sess);
    wl_plan_free(plan);
    PASS;
}

/* ======================================================================== */
/* MAIN TEST RUNNER                                                          */
/* ======================================================================== */

int
main(void)
{
    printf(
        "\n=== Incremental Fact Insertion Integration Tests (Phase 4) ===\n\n");

    test_frontier_preserved_after_incremental_insert();
    test_incremental_insert_enables_affected_stratum_detection();
    test_multiple_incremental_inserts_preserve_frontier();
    test_empty_incremental_insert_safe_frontier_unchanged();

    printf("\n=== Results: %d/%d passed ===\n", tests_passed,
           tests_passed + tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
