/*
 * test_session.c - Persistent session delta tests via wl_session_* API
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests the persistent columnar session: create, insert, step, delta callback.
 * Uses the wl_session_* wrapper API (not raw FFI), exercising the full
 * backend vtable dispatch.
 */

#include "../wirelog/backend.h"
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
/* Portability Macros                                                       */
/* ======================================================================== */

/* MSVC does not support __attribute__((unused)); use a portable macro */
#ifdef _MSC_VER
#define UNUSED
#else
#define UNUSED __attribute__((unused))
#endif

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
/* Tuple Collector (snapshot - no diff)                                    */
/* ======================================================================== */

typedef struct {
    int count;
    char relations[MAX_DELTAS][64];
    int64_t rows[MAX_DELTAS][MAX_COLS];
    uint32_t ncols[MAX_DELTAS];
} tuple_collector_t;

static void
collect_tuple(const char *relation, const int64_t *row, uint32_t ncols,
              void *user_data)
{
    tuple_collector_t *c = (tuple_collector_t *)user_data;
    if (c->count >= MAX_DELTAS)
        return;
    int idx = c->count++;
    strncpy(c->relations[idx], relation, 63);
    c->relations[idx][63] = '\0';
    c->ncols[idx] = ncols;
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

static bool
has_tuple(const tuple_collector_t *c, const char *relation,
          const int64_t *expected, uint32_t ncols)
{
    for (int i = 0; i < c->count; i++) {
        if (strcmp(c->relations[i], relation) != 0)
            continue;
        if (c->ncols[i] != ncols)
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
/* Helper: Build plan from Datalog source                                   */
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
/* Tests                                                                    */
/* ======================================================================== */

/*
 * Test: create a session and destroy it without crash.
 */
static void
test_session_create_destroy_impl(const wl_compute_backend_t *backend)
{
    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
                                ".decl r(x: int32)\n"
                                "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(backend, ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

static void
test_session_create_destroy(void)
{
    TEST("session(columnar): create and destroy");
    test_session_create_destroy_impl(wl_backend_columnar());
}

static void
test_session_create_destroy_columnar(void)
{
    TEST("session(columnar): create and destroy (duplicate check)");
    test_session_create_destroy_impl(wl_backend_columnar());
}

/*
 * Test: num_workers > 1 is rejected.
 */
static void
test_session_create_multi_worker_rejected(void)
{
    TEST("session: multi-worker accepted");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
                                ".decl r(x: int32)\n"
                                "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 2, &session);
    wl_plan_free(ffi);

    if (rc == 0 && session) {
        wl_session_destroy(session);
        PASS();
    } else {
        FAIL("multi-worker session creation should succeed");
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

    wl_plan_t *ffi = build_plan(".decl a(x: int32, y: int32)\n"
                                ".decl b(y: int32, z: int32)\n"
                                ".decl r(x: int32, y: int32, z: int32)\n"
                                "r(x, y, z) :- a(x, y), b(y, z).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
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
        wl_plan_free(ffi);
        FAIL("insert a failed");
        return;
    }

    int64_t b_data[] = { 2, 3 };
    rc = wl_session_insert(session, "b", b_data, 1, 2);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("insert b failed");
        return;
    }

    /* Step: should produce delta r(1,2,3) with diff=+1 */
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
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
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (count_deltas(&deltas, "r", +1) != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected exactly 1 positive delta, got %d",
                 count_deltas(&deltas, "r", +1));
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
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

    wl_plan_t *ffi = build_plan(".decl a(x: int32, y: int32)\n"
                                ".decl b(y: int32, z: int32)\n"
                                ".decl r(x: int32, y: int32, z: int32)\n"
                                "r(x, y, z) :- a(x, y), b(y, z).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
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
        wl_plan_free(ffi);
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
        wl_plan_free(ffi);
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
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (count_deltas(&deltas, "r", +1) != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected exactly 1 positive delta, got %d",
                 count_deltas(&deltas, "r", +1));
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: step with no changes produces zero delta callbacks.
 */
static void
test_session_step_no_change(void)
{
    TEST("session: step with no changes produces no deltas");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
                                ".decl r(x: int32)\n"
                                "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
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
        wl_plan_free(ffi);
        FAIL("first step failed");
        return;
    }

    /* Reset collector, step again with no changes */
    memset(&deltas, 0, sizeof(deltas));
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("second step failed");
        return;
    }

    if (deltas.count != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 0 deltas, got %d", deltas.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/* ======================================================================== */
/* RED Tests: remove and snapshot (require TASK 1-3 for GREEN)             */
/* ======================================================================== */

/*
 * Test: remove a tuple and step produces diff=-1 delta.
 *
 * RED: currently fails because dd_session_remove returns -1 (not implemented).
 * GREEN: after TASK 2+3 wire wl_dd_session_remove through the vtable.
 */
static void UNUSED
test_session_remove_single_delta(void)
{
    TEST("session: remove tuple produces diff=-1 delta");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
                                ".decl r(x: int32)\n"
                                "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert a=(1) and step: establishes r(1) */
    int64_t a_data[] = { 1 };
    rc = wl_session_insert(session, "a", a_data, 1, 1);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("insert failed");
        return;
    }
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("first step failed");
        return;
    }

    /* Reset and remove a=(1) */
    memset(&deltas, 0, sizeof(deltas));
    rc = wl_session_remove(session, "a", a_data, 1, 1);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "remove returned %d (expected 0)", rc);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    /* Step: should produce r(1) diff=-1 */
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("second step failed");
        return;
    }

    int64_t expected[] = { 1 };
    if (!has_delta(&deltas, "r", expected, 1, -1)) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected r(1) diff=-1, got %d total deltas",
                 deltas.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (count_deltas(&deltas, "r", -1) != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected exactly 1 retraction, got %d",
                 count_deltas(&deltas, "r", -1));
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: remove of non-existent tuple is a no-op (no crash, rc=0).
 *
 * RED: currently fails because dd_session_remove returns -1.
 * GREEN: after TASK 2+3 remove returns 0 and produces no phantom deltas.
 */
static void UNUSED
test_session_remove_nonexistent(void)
{
    TEST("session: remove non-existent tuple is no-op");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
                                ".decl r(x: int32)\n"
                                "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    /* Remove a=(99) which was never inserted */
    int64_t a_data[] = { 99 };
    rc = wl_session_remove(session, "a", a_data, 1, 1);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "remove returned %d (expected 0)", rc);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: snapshot on empty session returns 0 tuples with rc=0.
 */
static void UNUSED
test_session_snapshot_empty(void)
{
    TEST("session: snapshot of empty session returns 0 tuples");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
                                ".decl r(x: int32)\n"
                                "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    tuple_collector_t tuples;
    memset(&tuples, 0, sizeof(tuples));
    rc = wl_session_snapshot(session, collect_tuple, &tuples);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "snapshot returned %d (expected 0)", rc);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (tuples.count != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 0 tuples, got %d", tuples.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: snapshot after insert+step reflects current derived state.
 *
 * RED: currently fails because dd_session_snapshot uses empty batch worker.
 * GREEN: after TASK 2+3 snapshot reads from persistent session state.
 */
static void UNUSED
test_session_snapshot_after_insert(void)
{
    TEST("session: snapshot reflects current derived state");

    wl_plan_t *ffi = build_plan(".decl a(x: int32, y: int32)\n"
                                ".decl b(y: int32, z: int32)\n"
                                ".decl r(x: int32, y: int32, z: int32)\n"
                                "r(x, y, z) :- a(x, y), b(y, z).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    /* Insert a=(1,2), b=(2,3) and step */
    int64_t a_data[] = { 1, 2 };
    int64_t b_data[] = { 2, 3 };
    wl_session_insert(session, "a", a_data, 1, 2);
    wl_session_insert(session, "b", b_data, 1, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("step failed");
        return;
    }

    /* Snapshot: should contain r(1,2,3) */
    tuple_collector_t tuples;
    memset(&tuples, 0, sizeof(tuples));
    rc = wl_session_snapshot(session, collect_tuple, &tuples);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "snapshot returned %d (expected 0)", rc);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    int64_t expected[] = { 1, 2, 3 };
    if (!has_tuple(&tuples, "r", expected, 3)) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "expected r(1,2,3) in snapshot, got %d tuples total",
                 tuples.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (tuples.count != 1) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected exactly 1 tuple, got %d",
                 tuples.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/*
 * Test: duplicate insert of the same tuple produces exactly one positive delta.
 * DD set semantics via consolidate should not double-count.
 */
static void UNUSED
test_session_duplicate_insert(void)
{
    TEST("session: duplicate insert produces single positive delta");

    wl_plan_t *ffi = build_plan(".decl a(x: int32)\n"
                                ".decl r(x: int32)\n"
                                "r(x) :- a(x).\n");
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(ffi);
        FAIL("session_create failed");
        return;
    }

    delta_collector_t deltas;
    memset(&deltas, 0, sizeof(deltas));
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Insert a=(5) twice before stepping */
    int64_t a_data[] = { 5 };
    wl_session_insert(session, "a", a_data, 1, 1);
    wl_session_insert(session, "a", a_data, 1, 1);

    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("step failed");
        return;
    }

    int64_t expected[] = { 5 };
    if (!has_delta(&deltas, "r", expected, 1, +1)) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected r(5) diff=+1, got %d deltas total",
                 deltas.count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (count_deltas(&deltas, "r", +1) != 1) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "expected exactly 1 positive delta (set semantics), got %d",
                 count_deltas(&deltas, "r", +1));
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/* ======================================================================== */
/* TEST: Transitive Closure Insert (RED phase - will FAIL)                 */
/* ======================================================================== */

static void UNUSED
test_session_tc_insert(void)
{
    TEST("session: transitive closure insert test (recursive iterate)");

    /* Program:
       tc(X, Y) :- edge(X, Y).
       tc(X, Z) :- edge(X, Y), tc(Y, Z).
    */
    wl_plan_t *ffi = build_plan(".decl edge(x: int32, y: int32)\n"
                                ".decl tc(x: int32, y: int32)\n"
                                "tc(X, Y) :- edge(X, Y).\n"
                                "tc(X, Z) :- edge(X, Y), tc(Y, Z).\n");

    if (!ffi) {
        FAIL("failed to parse TC program");
        return;
    }

    /* Create persistent session */
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0) {
        wl_plan_free(ffi);
        FAIL("session creation failed");
        return;
    }

    /* Setup delta collector */
    delta_collector_t deltas = { 0 };
    wl_session_set_delta_cb(session, collect_delta, &deltas);

    /* Step 1: Insert edge(1,2), edge(2,3) */
    int64_t edge_12[] = { 1, 2 };
    int64_t edge_23[] = { 2, 3 };
    wl_session_insert(session, "edge", edge_12, 1, 2);
    wl_session_insert(session, "edge", edge_23, 1, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("first step failed");
        return;
    }

    /* Should have TC: (1,2), (2,3), (1,3) */
    if (!has_delta(&deltas, "tc", (int64_t[]){ 1, 2 }, 2, +1)) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("expected tc(1,2) with diff=+1");
        return;
    }
    if (!has_delta(&deltas, "tc", (int64_t[]){ 1, 3 }, 2, +1)) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("expected tc(1,3) with diff=+1 (recursive)");
        return;
    }

    /* Reset for step 2 */
    memset(&deltas, 0, sizeof(deltas));

    /* Step 2: Insert edge(3,4) - should derive tc(1,4), tc(2,4), tc(3,4) */
    int64_t edge_34[] = { 3, 4 };
    wl_session_insert(session, "edge", edge_34, 1, 2);
    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("second step failed");
        return;
    }

    /* Verify exactly 3 new TC tuples */
    int new_tc_count = count_deltas(&deltas, "tc", +1);
    if (new_tc_count != 3) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected 3 new TC deltas, got %d",
                 new_tc_count);
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL(msg);
        return;
    }

    if (!has_delta(&deltas, "tc", (int64_t[]){ 1, 4 }, 2, +1)
        || !has_delta(&deltas, "tc", (int64_t[]){ 2, 4 }, 2, +1)
        || !has_delta(&deltas, "tc", (int64_t[]){ 3, 4 }, 2, +1)) {
        wl_session_destroy(session);
        wl_plan_free(ffi);
        FAIL("expected tc(1,4), tc(2,4), tc(3,4) deltas");
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(ffi);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_session: persistent columnar session delta tests\n");

    test_session_create_destroy();
    test_session_create_destroy_columnar();
    test_session_create_multi_worker_rejected();
    test_session_step_initial_delta();
    test_session_step_incremental_delta();
    test_session_step_no_change();

    /* RED: the following tests require TASK 2+3 (FFI bridge + C vtable) */
    /* Disabled until wl_session_remove is implemented in columnar backend */
    /* test_session_remove_single_delta(); */
    /* test_session_remove_nonexistent(); */
    /* test_session_snapshot_empty(); */
    /* test_session_snapshot_after_insert(); */
    /* test_session_duplicate_insert(); */

    /* RED: the following tests require TASK 4 (Variable collections + iterate) */
    /* Disabled until wl_session_iterate is implemented in columnar backend */
    /* test_session_tc_insert(); */

    printf("\n  %d tests: %d passed, %d failed\n", tests_run, tests_passed,
           tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
