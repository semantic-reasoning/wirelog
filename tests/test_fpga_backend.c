/*
 * test_fpga_backend.c - Tests for the naive row-store FPGA backend
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Validates that the wl_compute_backend_t vtable is truly pluggable
 * by exercising a second backend implementation beyond columnar.
 */

#include "fpga_backend.h"

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
/* Tuple Collector (snapshot)                                               */
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
/* Plan Builder Helper                                                      */
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
/* Commit 2: Lifecycle Tests                                                */
/* ======================================================================== */

static void
test_backend_name(void)
{
    TEST("fpga: backend name is \"fpga\"");
    const wl_compute_backend_t *b = wl_backend_fpga();
    if (!b || !b->name || strcmp(b->name, "fpga") != 0) {
        FAIL("name mismatch");
        return;
    }
    PASS();
}

static void
test_create_destroy(void)
{
    TEST("fpga: create and destroy session");
    wl_plan_t *plan = build_plan(
        ".decl a(x: int32)\n"
        ".decl r(x: int32)\n"
        "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not generate plan");
        return;
    }
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_fpga(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("session_create failed");
        return;
    }
    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
}

static void
test_insert_rows(void)
{
    TEST("fpga: insert rows into EDB");
    wl_plan_t *plan = build_plan(
        ".decl a(x: int32)\n"
        ".decl r(x: int32)\n"
        "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not generate plan");
        return;
    }
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_fpga(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("session_create failed");
        return;
    }

    int64_t data[] = {1, 2, 3};
    rc = wl_session_insert(session, "a", data, 3, 1);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("insert failed");
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
}

static void
test_remove_unsupported(void)
{
    TEST("fpga: remove returns -1");
    wl_plan_t *plan = build_plan(
        ".decl a(x: int32)\n"
        ".decl r(x: int32)\n"
        "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not generate plan");
        return;
    }
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_fpga(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("session_create failed");
        return;
    }

    int64_t data[] = {1};
    rc = wl_session_remove(session, "a", data, 1, 1);
    if (rc != -1) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("remove should return -1");
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
}

static void
test_snapshot_empty(void)
{
    TEST("fpga: snapshot empty session returns 0");
    wl_plan_t *plan = build_plan(
        ".decl a(x: int32)\n"
        ".decl r(x: int32)\n"
        "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not generate plan");
        return;
    }
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_fpga(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("session_create failed");
        return;
    }

    tuple_collector_t tc;
    memset(&tc, 0, sizeof(tc));
    rc = wl_session_snapshot(session, collect_tuple, &tc);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("snapshot failed");
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Commit 4: Plan Interpretation Tests                                      */
/* ======================================================================== */

static void
test_passthrough(void)
{
    TEST("fpga: simple passthrough r(X) :- a(X).");
    wl_plan_t *plan = build_plan(
        ".decl a(x: int32)\n"
        ".decl r(x: int32)\n"
        "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not generate plan");
        return;
    }
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_fpga(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("session_create failed");
        return;
    }

    int64_t data[] = {10, 20, 30};
    rc = wl_session_insert(session, "a", data, 3, 1);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("insert failed");
        return;
    }

    tuple_collector_t tc;
    memset(&tc, 0, sizeof(tc));
    rc = wl_session_snapshot(session, collect_tuple, &tc);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("snapshot failed");
        return;
    }

    /* Should have 3 tuples in r */
    if (tc.count != 3) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected 3 tuples, got %d", tc.count);
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL(msg);
        return;
    }

    int64_t e1[] = {10};
    int64_t e2[] = {20};
    int64_t e3[] = {30};
    if (!has_tuple(&tc, "r", e1, 1)
        || !has_tuple(&tc, "r", e2, 1)
        || !has_tuple(&tc, "r", e3, 1)) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("missing expected tuple");
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
}

static void
test_join(void)
{
    TEST("fpga: join query r(X,Z) :- a(X,Y), b(Y,Z).");
    wl_plan_t *plan = build_plan(
        ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, z: int32)\n"
        ".decl r(x: int32, z: int32)\n"
        "r(x, z) :- a(x, y), b(y, z).\n");
    if (!plan) {
        FAIL("could not generate plan");
        return;
    }
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_fpga(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("session_create failed");
        return;
    }

    /* a: (1,2), (3,4) */
    int64_t a_data[] = {1, 2, 3, 4};
    rc = wl_session_insert(session, "a", a_data, 2, 2);
    /* b: (2,5), (4,6) */
    int64_t b_data[] = {2, 5, 4, 6};
    rc |= wl_session_insert(session, "b", b_data, 2, 2);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("insert failed");
        return;
    }

    tuple_collector_t tc;
    memset(&tc, 0, sizeof(tc));
    rc = wl_session_snapshot(session, collect_tuple, &tc);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("snapshot failed");
        return;
    }

    /* Expect r(1,5) and r(3,6) */
    int64_t e1[] = {1, 5};
    int64_t e2[] = {3, 6};
    if (tc.count < 2
        || !has_tuple(&tc, "r", e1, 2)
        || !has_tuple(&tc, "r", e2, 2)) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "expected r(1,5) and r(3,6), got %d tuples", tc.count);
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
}

static void
test_filter(void)
{
    TEST("fpga: filter query r(X) :- a(X), X > 5.");
    wl_plan_t *plan = build_plan(
        ".decl a(x: int32)\n"
        ".decl r(x: int32)\n"
        "r(x) :- a(x), x > 5.\n");
    if (!plan) {
        FAIL("could not generate plan");
        return;
    }
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_fpga(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("session_create failed");
        return;
    }

    int64_t data[] = {1, 3, 7, 10, 5};
    rc = wl_session_insert(session, "a", data, 5, 1);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("insert failed");
        return;
    }

    tuple_collector_t tc;
    memset(&tc, 0, sizeof(tc));
    rc = wl_session_snapshot(session, collect_tuple, &tc);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("snapshot failed");
        return;
    }

    /* Expect r(7) and r(10) */
    int64_t e1[] = {7};
    int64_t e2[] = {10};
    if (tc.count < 2
        || !has_tuple(&tc, "r", e1, 1)
        || !has_tuple(&tc, "r", e2, 1)) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "expected r(7) and r(10), got %d tuples", tc.count);
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL(msg);
        return;
    }

    /* Should NOT have r(1), r(3), r(5) */
    int64_t ne1[] = {1};
    int64_t ne2[] = {3};
    int64_t ne3[] = {5};
    if (has_tuple(&tc, "r", ne1, 1)
        || has_tuple(&tc, "r", ne2, 1)
        || has_tuple(&tc, "r", ne3, 1)) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("found tuple that should have been filtered");
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
}

static void
test_transitive_closure(void)
{
    TEST("fpga: transitive closure reaches fixed point");
    wl_plan_t *plan = build_plan(
        ".decl edge(x: int32, y: int32)\n"
        ".decl path(x: int32, y: int32)\n"
        "path(x, y) :- edge(x, y).\n"
        "path(x, z) :- path(x, y), edge(y, z).\n");
    if (!plan) {
        FAIL("could not generate plan");
        return;
    }
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_fpga(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("session_create failed");
        return;
    }

    /* edge: 1->2, 2->3, 3->4 */
    int64_t edges[] = {1, 2, 2, 3, 3, 4};
    rc = wl_session_insert(session, "edge", edges, 3, 2);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("insert failed");
        return;
    }

    tuple_collector_t tc;
    memset(&tc, 0, sizeof(tc));
    rc = wl_session_snapshot(session, collect_tuple, &tc);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("snapshot failed");
        return;
    }

    /* Expect path: (1,2),(1,3),(1,4),(2,3),(2,4),(3,4) = 6 tuples */
    int64_t p12[] = {1, 2};
    int64_t p13[] = {1, 3};
    int64_t p14[] = {1, 4};
    int64_t p23[] = {2, 3};
    int64_t p24[] = {2, 4};
    int64_t p34[] = {3, 4};
    if (!has_tuple(&tc, "path", p12, 2)
        || !has_tuple(&tc, "path", p13, 2)
        || !has_tuple(&tc, "path", p14, 2)
        || !has_tuple(&tc, "path", p23, 2)
        || !has_tuple(&tc, "path", p24, 2)
        || !has_tuple(&tc, "path", p34, 2)) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "expected 6 path tuples, got %d total", tc.count);
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL(msg);
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
}

static void
test_delta_callback(void)
{
    TEST("fpga: delta callback fires with diff=+1");
    wl_plan_t *plan = build_plan(
        ".decl a(x: int32)\n"
        ".decl r(x: int32)\n"
        "r(x) :- a(x).\n");
    if (!plan) {
        FAIL("could not generate plan");
        return;
    }
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_fpga(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("session_create failed");
        return;
    }

    delta_collector_t dc;
    memset(&dc, 0, sizeof(dc));
    wl_session_set_delta_cb(session, collect_delta, &dc);

    int64_t data[] = {42, 99};
    rc = wl_session_insert(session, "a", data, 2, 1);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("insert failed");
        return;
    }

    rc = wl_session_step(session);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("step failed");
        return;
    }

    /* Should have received delta callbacks with diff=+1 */
    if (dc.count < 2) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "expected >= 2 delta callbacks, got %d", dc.count);
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL(msg);
        return;
    }

    /* Verify diffs are all +1 */
    bool all_positive = true;
    for (int i = 0; i < dc.count; i++) {
        if (dc.diffs[i] != +1) {
            all_positive = false;
            break;
        }
    }
    if (!all_positive) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("expected all diffs to be +1");
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
}

static void
test_backend_specific_ops(void)
{
    TEST("fpga: backend-specific ops in plan don't crash");
    /* Use a multi-atom recursive rule that will trigger K_FUSION
     * and other columnar-specific operators in the plan. */
    wl_plan_t *plan = build_plan(
        ".decl edge(x: int32, y: int32)\n"
        ".decl path(x: int32, y: int32)\n"
        "path(x, y) :- edge(x, y).\n"
        "path(x, z) :- path(x, y), edge(y, z).\n");
    if (!plan) {
        FAIL("could not generate plan");
        return;
    }
    wl_session_t *session = NULL;
    int rc = wl_session_create(wl_backend_fpga(), plan, 1, &session);
    if (rc != 0 || !session) {
        wl_plan_free(plan);
        FAIL("session_create failed");
        return;
    }

    int64_t edges[] = {1, 2};
    rc = wl_session_insert(session, "edge", edges, 1, 2);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("insert failed");
        return;
    }

    /* The plan may contain K_FUSION/LFTJ/EXCHANGE ops; the FPGA backend
     * must skip them without crashing. */
    tuple_collector_t tc;
    memset(&tc, 0, sizeof(tc));
    rc = wl_session_snapshot(session, collect_tuple, &tc);
    if (rc != 0) {
        wl_session_destroy(session);
        wl_plan_free(plan);
        FAIL("snapshot failed with backend-specific ops");
        return;
    }

    wl_session_destroy(session);
    wl_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_fpga_backend\n");

    /* Lifecycle tests */
    test_backend_name();
    test_create_destroy();
    test_insert_rows();
    test_remove_unsupported();
    test_snapshot_empty();

    /* Plan interpretation tests */
    test_passthrough();
    test_join();
    test_filter();
    test_transitive_closure();
    test_delta_callback();
    test_backend_specific_ops();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return tests_failed > 0 ? 1 : 0;
}
