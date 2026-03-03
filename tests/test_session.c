/*
 * test_session.c - Persistent session delta tests via wl_session_* API
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests the persistent DD session: create, insert, step, delta callback.
 * Uses the wl_session_* wrapper API (not raw FFI), exercising the full
 * backend vtable dispatch.
 */

#include "../wirelog/backend.h"
#include "../wirelog/ffi/dd_ffi.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Helpers                                                             */
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
/* Delta Collector                                                          */
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
/* Delta Query Helpers                                                      */
/* ======================================================================== */

static int
count_deltas(const delta_collector_t *c, const char *relation, int32_t diff)
{
    int n = 0;
    for (int i = 0; i < c->count; i++) {
        if (strcmp(c->relations[i], relation) == 0 && c->diffs[i] == diff)
            n++;
    }
    return n;
}

static bool
has_delta(const delta_collector_t *c, const char *relation,
          const int64_t *expected, uint32_t ncols, int32_t diff)
{
    for (int i = 0; i < c->count; i++) {
        if (strcmp(c->relations[i], relation) != 0)
            continue;
        if (c->ncols[i] != ncols || c->diffs[i] != diff)
            continue;
        bool match = true;
        for (uint32_t j = 0; j < ncols; j++) {
            if (c->rows[i][j] != expected[j]) {
                match = false;
                break;
            }
        }
        if (match)
            return true;
    }
    return false;
}

/* ======================================================================== */
/* Helper: FFI plan from Datalog source                                     */
/* ======================================================================== */

static wl_ffi_plan_t *
ffi_plan_from_source(const char *src, wl_ffi_dd_plan_t **dd_plan_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;

    wl_ffi_dd_plan_t *dd_plan = NULL;
    int rc = wl_ffi_dd_plan_generate(prog, &dd_plan);
    wirelog_program_free(prog);
    if (rc != 0)
        return NULL;

    wl_ffi_plan_t *ffi = NULL;
    rc = wl_dd_marshal_plan(dd_plan, &ffi);
    if (rc != 0) {
        wl_ffi_dd_plan_free(dd_plan);
        return NULL;
    }

    *dd_plan_out = dd_plan;
    return ffi;
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

/*
 * Test: create a session and destroy it without crash.
 */
static void
test_session_create_destroy(void)
{
    TEST("session: create and destroy");

    wl_ffi_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(".decl a(x: int32)\n"
                                              ".decl r(x: int32)\n"
                                              "r(x) :- a(x).\n",
                                              &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_dd(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL("session_create failed");
        return;
    }

    wl_session_destroy(session);
    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(dd_plan);
    PASS();
}

/*
 * Test: num_workers > 1 is rejected.
 */
static void
test_session_create_multi_worker_rejected(void)
{
    TEST("session: multi-worker rejected");

    wl_ffi_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(".decl a(x: int32)\n"
                                              ".decl r(x: int32)\n"
                                              "r(x) :- a(x).\n",
                                              &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_dd(), ffi, 2, &session);
    if (rc == 0 && session)
        wl_session_destroy(session);
    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(dd_plan);

    if (rc != 0) {
        PASS();
    } else {
        FAIL("expected error for num_workers=2");
    }
}

/*
 * Test: initial step produces all results as diff=+1.
 * Datalog: r(X,Y,Z) :- a(X,Y), b(Y,Z).
 * EDB: a={(1,2)}, b={(2,3)}
 * Expected delta: r(1,2,3) with diff=+1
 */
static void
test_session_step_initial_delta(void)
{
    TEST("session: initial step produces diff=+1 deltas");

    wl_ffi_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi
        = ffi_plan_from_source(".decl a(x: int32, y: int32)\n"
                               ".decl b(y: int32, z: int32)\n"
                               ".decl r(x: int32, y: int32, z: int32)\n"
                               "r(x, y, z) :- a(x, y), b(y, z).\n",
                               &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_dd(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL("session_create failed");
        return;
    }

    /* Set delta callback */
    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert EDB facts */
    int64_t a_data[] = { 1, 2 };
    rc = wl_session_insert(session, "a", a_data, 1, 2);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL("insert a failed");
        return;
    }

    int64_t b_data[] = { 2, 3 };
    rc = wl_session_insert(session, "b", b_data, 1, 2);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL("insert b failed");
        return;
    }

    /* Step: should produce delta r(1,2,3) with diff=+1 */
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL("session_step failed");
        return;
    }

    int64_t expected[] = { 1, 2, 3 };
    if (!has_delta(&deltas, "r", expected, 3, +1)) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "expected r(1,2,3) diff=+1, got %d deltas total",
                 deltas.count);
        wl_session_destroy(session);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    if (count_deltas(&deltas, "r", +1) != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected exactly 1 positive delta, got %d",
                 count_deltas(&deltas, "r", +1));
        wl_session_destroy(session);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(dd_plan);
    PASS();
}

/*
 * Test: incremental insert + step produces only new derivations.
 * After initial step (r(1,2,3)), insert a=(1,4), b=(4,5).
 * Second step should produce exactly r(1,4,5) with diff=+1.
 */
static void
test_session_step_incremental_delta(void)
{
    TEST("session: incremental step produces only new deltas");

    wl_ffi_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi
        = ffi_plan_from_source(".decl a(x: int32, y: int32)\n"
                               ".decl b(y: int32, z: int32)\n"
                               ".decl r(x: int32, y: int32, z: int32)\n"
                               "r(x, y, z) :- a(x, y), b(y, z).\n",
                               &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_dd(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL("session_create failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Initial data */
    int64_t a1[] = { 1, 2 };
    int64_t b1[] = { 2, 3 };
    wl_session_insert(session, "a", a1, 1, 2);
    wl_session_insert(session, "b", b1, 1, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL("first step failed");
        return;
    }

    /* Reset collector for second step */
    memset(&deltas, 0, sizeof(deltas));

    /* Insert new facts */
    int64_t a2[] = { 1, 4 };
    int64_t b2[] = { 4, 5 };
    wl_session_insert(session, "a", a2, 1, 2);
    wl_session_insert(session, "b", b2, 1, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL("second step failed");
        return;
    }

    /* Should have exactly r(1,4,5) with diff=+1 */
    int64_t expected[] = { 1, 4, 5 };
    if (!has_delta(&deltas, "r", expected, 3, +1)) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "expected r(1,4,5) diff=+1, got %d deltas total",
                 deltas.count);
        wl_session_destroy(session);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    if (count_deltas(&deltas, "r", +1) != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected exactly 1 positive delta, got %d",
                 count_deltas(&deltas, "r", +1));
        wl_session_destroy(session);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(dd_plan);
    PASS();
}

/*
 * Test: step with no changes produces zero delta callbacks.
 */
static void
test_session_step_no_change(void)
{
    TEST("session: step with no changes produces no deltas");

    wl_ffi_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(".decl a(x: int32)\n"
                                              ".decl r(x: int32)\n"
                                              "r(x) :- a(x).\n",
                                              &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_dd(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL("session_create failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert and step once */
    int64_t a_data[] = { 10 };
    wl_session_insert(session, "a", a_data, 1, 1);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL("first step failed");
        return;
    }

    /* Reset collector, step again with no changes */
    memset(&deltas, 0, sizeof(deltas));
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL("second step failed");
        return;
    }

    if (deltas.count != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 0 deltas, got %d", deltas.count);
        wl_session_destroy(session);
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(dd_plan);
    PASS();
}

/*
 * Test: session_remove returns -1 (not supported in insert-only MVP).
 */
static void
test_session_remove_returns_error(void)
{
    TEST("session: remove returns error");

    wl_ffi_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(".decl a(x: int32)\n"
                                              ".decl r(x: int32)\n"
                                              "r(x) :- a(x).\n",
                                              &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_dd(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_ffi_plan_free(ffi);
        wl_ffi_dd_plan_free(dd_plan);
        FAIL("session_create failed");
        return;
    }

    int64_t data[] = { 1 };
    rc = wl_session_remove(session, "a", data, 1, 1);
    wl_session_destroy(session);
    wl_ffi_plan_free(ffi);
    wl_ffi_dd_plan_free(dd_plan);

    if (rc != 0) {
        PASS();
    } else {
        FAIL("expected remove to return error");
    }
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_session: persistent DD session delta tests\n");

    test_session_create_destroy();
    test_session_create_multi_worker_rejected();
    test_session_step_initial_delta();
    test_session_step_incremental_delta();
    test_session_step_no_change();
    test_session_remove_returns_error();

    printf("\n  %d tests: %d passed, %d failed\n", tests_run, tests_passed,
           tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
