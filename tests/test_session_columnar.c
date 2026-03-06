/*
 * test_session_columnar.c - Columnar backend session tests via wl_session_* API
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests the columnar backend session: create, insert, snapshot.
 * Uses the wl_session_* wrapper API, exercising the columnar vtable.
 * Plan generation uses the DD marshal path (pure C, no Rust required).
 */

#include "../wirelog/backend/columnar_nanoarrow.h"
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
/* Tuple Collector                                                          */
/* ======================================================================== */

#define MAX_TUPLES 256
#define MAX_COLS 16

typedef struct {
    int count;
    char relations[MAX_TUPLES][64];
    int64_t rows[MAX_TUPLES][MAX_COLS];
    uint32_t ncols[MAX_TUPLES];
} tuple_collector_t;

static void
collect_tuple(const char *relation, const int64_t *row, uint32_t ncols,
              void *user_data)
{
    tuple_collector_t *c = (tuple_collector_t *)user_data;
    if (c->count >= MAX_TUPLES)
        return;
    int idx = c->count++;
    strncpy(c->relations[idx], relation, 63);
    c->relations[idx][63] = '\0';
    c->ncols[idx] = ncols;
    for (uint32_t i = 0; i < ncols && i < MAX_COLS; i++)
        c->rows[idx][i] = row[i];
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
/* Helper: FFI plan from Datalog source (DD marshal, pure C)               */
/* ======================================================================== */

static wl_ffi_plan_t *
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

    wl_ffi_plan_t *ffi = NULL;
    rc = wl_dd_marshal_plan(dd_plan, &ffi);
    if (rc != 0) {
        wl_dd_plan_free(dd_plan);
        return NULL;
    }

    *dd_plan_out = dd_plan;
    return ffi;
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

/*
 * Test: create a columnar session and destroy it without crash.
 */
static void
test_col_session_create_destroy(void)
{
    TEST("columnar session: create and destroy");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(".decl a(x: int32)\n"
                                              ".decl r(x: int32)\n"
                                              "r(x) :- a(x).\n",
                                              &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("session_create failed");
        return;
    }

    wl_session_destroy(session);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/*
 * Test: snapshot on empty session returns 0 tuples.
 */
static void
test_col_session_snapshot_empty(void)
{
    TEST("columnar session: snapshot of empty session returns 0 tuples");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(".decl a(x: int32)\n"
                                              ".decl r(x: int32)\n"
                                              "r(x) :- a(x).\n",
                                              &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
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
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    if (tuples.count != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 0 tuples, got %d", tuples.count);
        wl_session_destroy(session);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/*
 * Test: insert facts and snapshot returns derived tuples.
 * Datalog: r(x) :- a(x).  (single-column, no JOIN key resolution needed)
 * EDB: a={(42)}
 * Expected snapshot: r={(42)}
 */
static void
test_col_session_snapshot_after_insert(void)
{
    TEST("columnar session: snapshot reflects inserted EDB");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(".decl a(x: int32)\n"
                                              ".decl r(x: int32)\n"
                                              "r(x) :- a(x).\n",
                                              &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_columnar(), ffi, 1, &session);
    if (rc != 0 || !session) {
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("session_create failed");
        return;
    }

    int64_t a_data[] = { 42 };
    wl_session_insert(session, "a", a_data, 1, 1);

    tuple_collector_t tuples;
    memset(&tuples, 0, sizeof(tuples));
    rc = wl_session_snapshot(session, collect_tuple, &tuples);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "snapshot returned %d (expected 0)", rc);
        wl_session_destroy(session);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int64_t expected[] = { 42 };
    if (!has_tuple(&tuples, "r", expected, 1)) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "expected r(42) in snapshot, got %d tuples total",
                 tuples.count);
        wl_session_destroy(session);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_session_columnar: columnar backend session tests\n");

    test_col_session_create_destroy();
    test_col_session_snapshot_empty();
    test_col_session_snapshot_after_insert();

    printf("\n  %d tests: %d passed, %d failed\n", tests_run, tests_passed,
           tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
