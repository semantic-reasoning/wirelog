/*
 * test_delta_retraction.c - Delta retraction validation for columnar backend
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Validates that the columnar backend correctly fires delta callbacks
 * for retracted tuples using semi-naive delta propagation.
 *
 * Issue #158: Semi-naive Delta Propagation for Efficient Incremental Retractions
 * Non-recursive strata optimize retractions by propagating only delta tuples
 * using $r$<name> delta relations, avoiding full re-evaluation.
 */

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Harness                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                            \
    do {                                      \
        tests_run++;                          \
        printf("  [%d] %s", tests_run, name); \
    } while (0)
#define PASS()                 \
    do {                       \
        tests_passed++;        \
        printf(" ... PASS\n"); \
    } while (0)
#define FAIL(msg)                         \
    do {                                  \
        tests_failed++;                   \
        printf(" ... FAIL: %s\n", (msg)); \
    } while (0)

/* ======================================================================== */
/* Plan Helper                                                             */
/* ======================================================================== */

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

/* ======================================================================== */
/* Delta Callback Collection                                               */
/* ======================================================================== */

#define MAX_DELTAS 256
#define MAX_COLS 16

typedef struct {
    int count;
    char relations[MAX_DELTAS][64];
    int64_t rows[MAX_DELTAS][MAX_COLS];
    uint32_t ncols[MAX_DELTAS];
    int32_t diffs[MAX_DELTAS];
} delta_collector_t;

static void
collect_delta(const char *relation, const int64_t *row, uint32_t ncols,
              int32_t diff, void *user_data)
{
    delta_collector_t *c = (delta_collector_t *)user_data;
    if (c->count >= MAX_DELTAS)
        return;
    int idx = c->count++;
    strncpy(c->relations[idx], relation, 63);
    c->relations[idx][63] = '\0';
    c->ncols[idx] = ncols;
    c->diffs[idx] = diff;
    for (uint32_t i = 0; i < ncols && i < MAX_COLS; i++)
        c->rows[idx][i] = row[i];
}

/* ======================================================================== */
/* Test 1: Simple Non-recursive Retraction                                  */
/* ======================================================================== */

/*
 * Rule: r(x) :- a(x)
 * Insert a(1), a(2)
 * Step 1: deltas r(1) diff=+1, r(2) diff=+1
 * Remove a(1)
 * Step 2: delta r(1) diff=-1 only (semi-naive)
 */
static int
test_simple_nonrecursive(void)
{
    TEST("Simple non-recursive retraction (semi-naive)");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
                                ".decl r(x: int32)\n"
                                "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return 1;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert a(1), a(2) */
    int64_t a_data[] = { 1, 2 };
    wl_session_insert(session, "a", a_data, 2, 1);
    rc = wl_session_step(session);
    if (rc != 0) {
        FAIL("step 1 failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }
    int count_after_step1 = deltas.count;

    /* Remove a(1) */
    int64_t remove_data[] = { 1 };
    rc = wl_session_remove(session, "a", remove_data, 1, 1);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "remove failed with rc=%d", rc);
        FAIL(msg);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }

    rc = wl_session_step(session);
    if (rc != 0) {
        FAIL("step 2 failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);

    /* Verify: Step 2 should have exactly one delta: r(1) diff=-1 */
    int r1_minus1_count = 0;
    for (int i = count_after_step1; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "r") == 0 && deltas.ncols[i] == 1
            && deltas.rows[i][0] == 1 && deltas.diffs[i] == -1) {
            r1_minus1_count++;
        }
    }
    if (r1_minus1_count != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "expected exactly 1 r(1) diff=-1 in step 2, got %d",
                 r1_minus1_count);
        FAIL(msg);
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 2: No Phantom Deltas                                                */
/* ======================================================================== */

/*
 * Rule: r(x) :- a(x)
 * Insert a(1), a(2)
 * Step 1: deltas r(1), r(2)
 * Remove a(1)
 * Step 2: delta r(1) diff=-1 only, NOT r(2)
 */
static int
test_no_phantom_delta(void)
{
    TEST("No phantom deltas on retraction");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
                                ".decl r(x: int32)\n"
                                "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return 1;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    int64_t a_data[] = { 1, 2 };
    wl_session_insert(session, "a", a_data, 2, 1);
    rc = wl_session_step(session);
    if (rc != 0) {
        FAIL("step 1 failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }
    int count_after_step1 = deltas.count;

    /* Remove a(1) only */
    int64_t remove_data[] = { 1 };
    rc = wl_session_remove(session, "a", remove_data, 1, 1);
    if (rc != 0) {
        FAIL("remove failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }

    rc = wl_session_step(session);
    if (rc != 0) {
        FAIL("step 2 failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);

    /* Check: r(1) diff=-1 exists, r(2) should NOT appear in step 2 */
    bool found_r1_minus1 = false;
    bool found_r2 = false;
    for (int i = count_after_step1; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "r") == 0 && deltas.ncols[i] == 1) {
            if (deltas.rows[i][0] == 1 && deltas.diffs[i] == -1)
                found_r1_minus1 = true;
            if (deltas.rows[i][0] == 2)
                found_r2 = true;
        }
    }
    if (!found_r1_minus1) {
        FAIL("missing r(1) diff=-1");
        return 1;
    }
    if (found_r2) {
        FAIL("phantom delta r(2) appeared");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 3: Join Retraction                                                  */
/* ======================================================================== */

/*
 * Rule: r(x,y) :- a(x), b(y)
 * Insert a(1), a(2) + b(10)
 * Step 1: deltas r(1,10), r(2,10)
 * Remove a(1)
 * Step 2: delta r(1,10) diff=-1, but r(2,10) NOT retracted
 */
static int
test_join_retraction(void)
{
    TEST("Join retraction (correct row selection)");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
                                ".decl b(y: int32)\n"
                                ".decl r(x: int32, y: int32)\n"
                                "r(x, y) :- a(x), b(y).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return 1;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert a(1), a(2) */
    int64_t a_data[] = { 1, 2 };
    wl_session_insert(session, "a", a_data, 2, 1);

    /* Insert b(10) */
    int64_t b_data[] = { 10 };
    wl_session_insert(session, "b", b_data, 1, 1);

    rc = wl_session_step(session);
    if (rc != 0) {
        FAIL("step 1 failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }
    int count_after_step1 = deltas.count;

    /* Remove a(1) */
    int64_t remove_data[] = { 1 };
    rc = wl_session_remove(session, "a", remove_data, 1, 1);
    if (rc != 0) {
        FAIL("remove failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }

    rc = wl_session_step(session);
    if (rc != 0) {
        FAIL("step 2 failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);

    /* Verify: r(1,10) diff=-1, but r(2,10) NOT touched */
    bool found_r1_10 = false;
    bool found_r2_10 = false;
    for (int i = count_after_step1; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "r") == 0 && deltas.ncols[i] == 2) {
            if (deltas.rows[i][0] == 1 && deltas.rows[i][1] == 10
                && deltas.diffs[i] == -1)
                found_r1_10 = true;
            if (deltas.rows[i][0] == 2 && deltas.rows[i][1] == 10)
                found_r2_10 = true;
        }
    }
    if (!found_r1_10) {
        FAIL("missing r(1,10) diff=-1");
        return 1;
    }
    if (found_r2_10) {
        FAIL("phantom delta r(2,10)");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 4: Recursive Fallback                                               */
/* ======================================================================== */

/*
 * TC program: edge(x,y), tc(x,y) :- edge(x,y) | tc(x,z), edge(z,y)
 * Insert edge(1,2), edge(2,3)
 * Step 1: compute tc(1,2), tc(2,3), tc(1,3)
 * Remove edge(1,2)
 * Step 2: compute tc via full re-eval, correct result should be tc(2,3) only
 *         delta callbacks should show tc(1,2) diff=-1, tc(1,3) diff=-1
 */
static int
test_recursive_fallback(void)
{
    TEST("Recursive fallback (full re-eval)");

    wl_plan_t *ffi = build_plan(".decl edge(x: int32, y: int32)\n"
                                ".decl tc(x: int32, y: int32)\n"
                                "tc(x, y) :- edge(x, y).\n"
                                "tc(x, y) :- tc(x, z), edge(z, y).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return 1;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert edge(1,2), edge(2,3) */
    int64_t edge_data[] = { 1, 2, 2, 3 };
    wl_session_insert(session, "edge", edge_data, 2, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        FAIL("step 1 failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }
    int count_after_step1 = deltas.count;

    /* Remove edge(1,2) */
    int64_t remove_data[] = { 1, 2 };
    rc = wl_session_remove(session, "edge", remove_data, 1, 2);
    if (rc != 0) {
        FAIL("remove failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }

    rc = wl_session_step(session);
    if (rc != 0) {
        FAIL("step 2 failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);

    /* Step 2 should retract tc(1,2) and tc(1,3) */
    bool found_tc_1_2_minus1 = false;
    bool found_tc_1_3_minus1 = false;
    for (int i = count_after_step1; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "tc") == 0 && deltas.ncols[i] == 2
            && deltas.diffs[i] == -1) {
            if (deltas.rows[i][0] == 1 && deltas.rows[i][1] == 2)
                found_tc_1_2_minus1 = true;
            if (deltas.rows[i][0] == 1 && deltas.rows[i][1] == 3)
                found_tc_1_3_minus1 = true;
        }
    }
    if (!found_tc_1_2_minus1 || !found_tc_1_3_minus1) {
        FAIL("missing expected retractions");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 5: Alternate Path                                                   */
/* ======================================================================== */

/*
 * TC: edge(1,2), edge(1,3) → tc includes 1→2, 1→3
 * Remove edge(1,2)
 * Step: tc(1,3) should NOT be retracted (alternate path via 1→3)
 */
static int
test_alternate_path(void)
{
    TEST("Alternate path (no spurious retraction)");

    wl_plan_t *ffi = build_plan(".decl edge(x: int32, y: int32)\n"
                                ".decl tc(x: int32, y: int32)\n"
                                "tc(x, y) :- edge(x, y).\n"
                                "tc(x, y) :- tc(x, z), edge(z, y).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return 1;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert edge(1,2), edge(1,3) */
    int64_t edge_data[] = { 1, 2, 1, 3 };
    wl_session_insert(session, "edge", edge_data, 2, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        FAIL("step 1 failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }
    int count_after_step1 = deltas.count;

    /* Remove edge(1,2) only */
    int64_t remove_data[] = { 1, 2 };
    rc = wl_session_remove(session, "edge", remove_data, 1, 2);
    if (rc != 0) {
        FAIL("remove failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }

    rc = wl_session_step(session);
    if (rc != 0) {
        FAIL("step 2 failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);

    /* tc(1,2) should be retracted, but tc(1,3) should NOT */
    bool found_tc_1_2_minus1 = false;
    bool found_tc_1_3 = false;
    for (int i = count_after_step1; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "tc") == 0 && deltas.ncols[i] == 2) {
            if (deltas.rows[i][0] == 1 && deltas.rows[i][1] == 2
                && deltas.diffs[i] == -1)
                found_tc_1_2_minus1 = true;
            if (deltas.rows[i][0] == 1 && deltas.rows[i][1] == 3)
                found_tc_1_3 = true;
        }
    }
    if (!found_tc_1_2_minus1) {
        FAIL("missing tc(1,2) diff=-1");
        return 1;
    }
    if (found_tc_1_3) {
        FAIL("spurious delta for tc(1,3)");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 6: Remove Non-existent                                              */
/* ======================================================================== */

/*
 * Rule: r(x) :- a(x)
 * Insert a(1)
 * Step 1: delta r(1) diff=+1
 * Remove a(99) (never inserted)
 * Step 2: should return 0 (no error), no callbacks fired
 */
static int
test_remove_nonexistent(void)
{
    TEST("Remove non-existent row (no error/crash)");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
                                ".decl r(x: int32)\n"
                                "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return 1;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert a(1) */
    int64_t a_data[] = { 1 };
    wl_session_insert(session, "a", a_data, 1, 1);
    rc = wl_session_step(session);
    if (rc != 0) {
        FAIL("step 1 failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }

    /* Try to remove a(99) which was never inserted */
    int64_t remove_data[] = { 99 };
    rc = wl_session_remove(session, "a", remove_data, 1, 1);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "remove returned rc=%d, expected 0", rc);
        FAIL(msg);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }

    /* Step should succeed with no additional deltas */
    int count_before_step2 = deltas.count;
    rc = wl_session_step(session);
    if (rc != 0) {
        FAIL("step 2 failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }
    int count_after_step2 = deltas.count;

    wl_session_destroy(session);
    wl_plan_free(ffi);

    if (count_after_step2 != count_before_step2) {
        FAIL("step 2 fired unexpected deltas");
        return 1;
    }
    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 7: Remove and Reinsert                                              */
/* ======================================================================== */

/*
 * Rule: r(x) :- a(x)
 * Insert a(1) → delta r(1) diff=+1
 * Remove a(1) → delta r(1) diff=-1
 * Insert a(1) → delta r(1) diff=+1
 * Verify sequence
 */
static int
test_remove_reinject(void)
{
    TEST("Remove and reinsert (delta sequence)");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
                                ".decl r(x: int32)\n"
                                "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return 1;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Step 1: Insert a(1) */
    int64_t a_data[] = { 1 };
    wl_session_insert(session, "a", a_data, 1, 1);
    rc = wl_session_step(session);
    if (rc != 0) {
        FAIL("step 1 failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }
    int count_after_step1 = deltas.count;

    /* Step 2: Remove a(1) */
    int64_t remove_data[] = { 1 };
    rc = wl_session_remove(session, "a", remove_data, 1, 1);
    if (rc != 0) {
        FAIL("remove failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }
    rc = wl_session_step(session);
    if (rc != 0) {
        FAIL("step 2 failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }
    int count_after_step2 = deltas.count;

    /* Step 3: Reinsert a(1) */
    wl_session_insert(session, "a", a_data, 1, 1);
    rc = wl_session_step(session);
    if (rc != 0) {
        FAIL("step 3 failed");
        wl_session_destroy(session);
        wl_plan_free(ffi);
        return 1;
    }
    int count_after_step3 = deltas.count;

    wl_session_destroy(session);
    wl_plan_free(ffi);

    /* Verify sequence: +1, -1, +1 for r(1) */
    /* Step 1: r(1) diff=+1 */
    bool found_plus1 = false;
    for (int i = 0; i < count_after_step1; i++) {
        if (strcmp(deltas.relations[i], "r") == 0 && deltas.ncols[i] == 1
            && deltas.rows[i][0] == 1 && deltas.diffs[i] == 1) {
            found_plus1 = true;
            break;
        }
    }
    if (!found_plus1) {
        FAIL("step 1: missing r(1) diff=+1");
        return 1;
    }

    /* Step 2: r(1) diff=-1 */
    bool found_minus1 = false;
    for (int i = count_after_step1; i < count_after_step2; i++) {
        if (strcmp(deltas.relations[i], "r") == 0 && deltas.ncols[i] == 1
            && deltas.rows[i][0] == 1 && deltas.diffs[i] == -1) {
            found_minus1 = true;
            break;
        }
    }
    if (!found_minus1) {
        FAIL("step 2: missing r(1) diff=-1");
        return 1;
    }

    /* Step 3: r(1) diff=+1 */
    bool found_plus1_again = false;
    for (int i = count_after_step2; i < count_after_step3; i++) {
        if (strcmp(deltas.relations[i], "r") == 0 && deltas.ncols[i] == 1
            && deltas.rows[i][0] == 1 && deltas.diffs[i] == 1) {
            found_plus1_again = true;
            break;
        }
    }
    if (!found_plus1_again) {
        FAIL("step 3: missing r(1) diff=+1");
        return 1;
    }

    PASS();
    return 0;
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("Delta Retraction Tests (Issue #158)\n");
    printf("====================================\n\n");

    test_simple_nonrecursive();
    test_no_phantom_delta();
    test_join_retraction();
    test_recursive_fallback();
    test_alternate_path();
    test_remove_nonexistent();
    test_remove_reinject();

    printf("\n");
    printf("Passed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
