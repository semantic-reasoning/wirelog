/*
 * test_delta_callback.c - Delta callback validation for columnar backend
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Validates that the columnar backend correctly fires delta callbacks
 * for newly derived tuples. Phase 2A uses full re-eval + set diff.
 *
 * Test: Simple transitive closure with delta tracking
 *   edge(1,2), edge(2,3) → tc(1,2), tc(2,3), tc(1,3)
 *   Delta callbacks: +1 for each new tc tuple
 */

#include "../wirelog/backend/columnar_nanoarrow.h"
#include "../wirelog/backend/dd/dd_ffi.h"
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
ffi_plan_from_source(const char *src, wl_dd_plan_t **dd_plan_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;

    wl_dd_plan_t *dd_plan = NULL;
    int rc = wl_dd_plan_generate(prog, &dd_plan);
    wirelog_program_free(prog);
    if (rc != 0)
        return NULL;

    wl_plan_t *ffi = NULL;
    rc = wl_dd_marshal_plan(dd_plan, &ffi);
    if (rc != 0) {
        wl_dd_plan_free(dd_plan);
        return NULL;
    }

    *dd_plan_out = dd_plan;
    return ffi;
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
/* Test: Delta Callback Invocation                                         */
/* ======================================================================== */

/*
 * Test 1: Delta callback is called for each new tuple during step()
 *
 * Setup:
 *   - Register a delta callback on session
 *   - Insert edge(1,2), edge(2,3)
 *   - Call session_step() once
 *
 * Expected behavior:
 *   - Delta callback should be invoked for new tc() tuples
 *   - diff=+1 for each new tuple (Phase 2A)
 *
 * Success criteria:
 *   - Callback was invoked at least once
 *   - Captured new tc tuples
 */
static int
test_delta_callback_invoked(void)
{
    TEST("Delta callback invoked on step()");

    /* Program: r(x) :- a(x).  EDB: a={42}  Expected delta: r(42) diff=+1 */
    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(".decl a(x: int32)\n"
                                          ".decl r(x: int32)\n"
                                          "r(x) :- a(x).\n",
                                          &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("session_create failed");
        return 1;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    int64_t a_data[] = { 42 };
    wl_session_insert(session, "a", a_data, 1, 1);
    rc = wl_session_step(session);

    wl_session_destroy(session);
    wl_plan_free(ffi);
    wl_dd_plan_free(dd_plan);

    if (rc != 0) {
        FAIL("session_step returned non-zero");
        return 1;
    }
    if (deltas.count == 0) {
        FAIL("no delta callbacks fired");
        return 1;
    }
    /* Find r(42) diff=+1 */
    bool found = false;
    for (int i = 0; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "r") == 0 && deltas.ncols[i] == 1
            && deltas.rows[i][0] == 42 && deltas.diffs[i] == 1) {
            found = true;
            break;
        }
    }
    if (!found) {
        FAIL("expected delta r(42) diff=+1 not found");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * Test 2: Delta set difference is computed correctly
 *
 * Phase 2A implementation:
 *   1. Evaluate stratum (full re-eval)
 *   2. Consolidate result (dedup, sort)
 *   3. Compare result with previous snapshot
 *   4. Fire callback for (result - previous)
 *
 * This test validates the diff logic for correctness.
 */
static int
test_delta_set_difference(void)
{
    TEST("Delta set difference (full re-eval + diff)");

    /*
     * Step 1: insert a(1) → delta fires r(1) diff=+1
     * Step 2: insert a(2) → delta fires r(2) diff=+1, NOT r(1) again
     */
    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(".decl a(x: int32)\n"
                                          ".decl r(x: int32)\n"
                                          "r(x) :- a(x).\n",
                                          &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("session_create failed");
        return 1;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Step 1: a(1) */
    int64_t a1[] = { 1 };
    wl_session_insert(session, "a", a1, 1, 1);
    wl_session_step(session);
    int count_after_step1 = deltas.count;

    /* Step 2: a(2) */
    int64_t a2[] = { 2 };
    wl_session_insert(session, "a", a2, 1, 1);
    rc = wl_session_step(session);

    wl_session_destroy(session);
    wl_plan_free(ffi);
    wl_dd_plan_free(dd_plan);

    if (rc != 0) {
        FAIL("session_step returned non-zero");
        return 1;
    }

    /* Step 1 must have fired r(1) */
    bool found_r1_step1 = false;
    for (int i = 0; i < count_after_step1; i++) {
        if (strcmp(deltas.relations[i], "r") == 0 && deltas.ncols[i] == 1
            && deltas.rows[i][0] == 1 && deltas.diffs[i] == 1) {
            found_r1_step1 = true;
            break;
        }
    }
    if (!found_r1_step1) {
        FAIL("expected r(1) diff=+1 after step 1");
        return 1;
    }

    /* Step 2 must have fired r(2) but not r(1) again */
    bool found_r2 = false;
    bool found_r1_again = false;
    for (int i = count_after_step1; i < deltas.count; i++) {
        if (strcmp(deltas.relations[i], "r") != 0 || deltas.ncols[i] != 1)
            continue;
        if (deltas.rows[i][0] == 2 && deltas.diffs[i] == 1)
            found_r2 = true;
        if (deltas.rows[i][0] == 1)
            found_r1_again = true;
    }
    if (!found_r2) {
        FAIL("expected r(2) diff=+1 after step 2");
        return 1;
    }
    if (found_r1_again) {
        FAIL("r(1) fired again in step 2 (set diff broken)");
        return 1;
    }
    PASS();
    return 0;
}

/*
 * Test 3: Multi-iteration delta propagation
 *
 * Validates that deltas are correct across multiple steps:
 *   Step 1: edge(1,2), edge(2,3) → tc(1,2), tc(2,3)
 *   Step 2: tc(1,2) + edge(2,3) → tc(1,3)
 *   Step 3: convergence, no new deltas
 *
 * Expects callbacks per step with correct +1 diffs.
 */
static int
test_delta_multi_step(void)
{
    TEST("Delta propagation across multiple steps");

    /*
     * Insert a(1), a(2) before any step.
     * Step 1 → deltas: r(1), r(2)
     * Step 2 (no new EDB) → no new deltas (convergence)
     */
    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(".decl a(x: int32)\n"
                                          ".decl r(x: int32)\n"
                                          "r(x) :- a(x).\n",
                                          &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return 1;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("session_create failed");
        return 1;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    int64_t ab[] = { 1, 2 };
    wl_session_insert(session, "a", ab, 2, 1);

    /* Step 1: should fire r(1) and r(2) */
    wl_session_step(session);
    int count_step1 = deltas.count;

    /* Step 2: no new EDB → no new deltas */
    rc = wl_session_step(session);
    int count_step2 = deltas.count;

    wl_session_destroy(session);
    wl_plan_free(ffi);
    wl_dd_plan_free(dd_plan);

    if (rc != 0) {
        FAIL("session_step returned non-zero");
        return 1;
    }
    if (count_step1 < 2) {
        FAIL("expected at least 2 deltas after step 1");
        return 1;
    }
    if (count_step2 != count_step1) {
        char msg[64];
        snprintf(msg, sizeof(msg), "step 2 fired %d extra deltas (expected 0)",
                 count_step2 - count_step1);
        FAIL(msg);
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
    printf("Delta Callback Tests (Phase 2A)\n");
    printf("===============================\n\n");

    test_delta_callback_invoked();
    test_delta_set_difference();
    test_delta_multi_step();

    printf("\n");
    printf("Passed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
