/*
 * test_recursive_agg_sssp_max.c - Integration tests for single-source
 *                                  shortest path using monotone MAX
 *                                  aggregation in recursive iterate() scope
 *                                  (issue #69, Phase 5 REFACTOR)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * These tests exercise the full pipeline for SSSP with MAX aggregation:
 *   Datalog source -> parse -> IR -> stratify -> DD plan -> marshal ->
 *   FFI -> Rust DD execute -> callback results
 *
 * Algorithm: monotone MAX distance propagation (longest reachable path
 * from a fixed source node 0).  This is the dual of SSSP-MIN: MAX grows
 * distances monotonically so it is safe inside iterate().
 *
 *   Dist(y, max(w))       :- Edge(0, y, w).
 *   Dist(y, max(dz + w))  :- Dist(z, dz), Edge(z, y, w).
 *
 * At fixpoint Dist(y, d) holds where d is the longest path distance from
 * source node 0 to node y.
 *
 * Tests:
 *   1. Triangle graph          -- basic MAX distances, 3 nodes
 *   2. Fixpoint convergence    -- longer path must update initial value
 *   3. Disconnected graph      -- unreachable nodes must not appear
 *   4. Single edge             -- trivial one-hop case
 *   5. Dense 5-node graph      -- multiple competing paths, verify MAX
 */

#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
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

#define MAX_TUPLES 512
#define MAX_COLS 8

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

/* ======================================================================== */
/* Result Query Helpers                                                     */
/* ======================================================================== */

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

static int
count_tuples(const tuple_collector_t *c, const char *relation)
{
    int n = 0;
    for (int i = 0; i < c->count; i++) {
        if (strcmp(c->relations[i], relation) == 0)
            n++;
    }
    return n;
}

/*
 * Return the distance (column 1) for destination node @node_id (column 0)
 * in @relation.  Returns INT64_MIN if not found.
 */
static int64_t
dist_of(const tuple_collector_t *c, const char *relation, int64_t node_id)
{
    for (int i = 0; i < c->count; i++) {
        if (strcmp(c->relations[i], relation) != 0)
            continue;
        if (c->ncols[i] < 2)
            continue;
        if (c->rows[i][0] == node_id)
            return c->rows[i][1];
    }
    return INT64_MIN;
}

/* ======================================================================== */
/* Pipeline Helper                                                          */
/* ======================================================================== */

/*
 * SSSP-MAX Datalog program: monotone MAX distance from source node 0.
 *
 * The base case seeds direct neighbors of node 0.  The recursive rule
 * propagates distances via MAX, so values only ever increase -- a
 * monotone operation safe inside iterate().
 */
#define SSSP_MAX_PROGRAM                         \
    ".decl Edge(x: int32, y: int32, w: int32)\n" \
    ".decl Dist(y: int32, d: int32)\n"           \
    "Dist(y, max(w))       :- Edge(0, y, w).\n"  \
    "Dist(y, max(dz + w))  :- Dist(z, dz), Edge(z, y, w).\n"

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
/* Test 1: MAX SSSP on triangle graph                                      */
/* ======================================================================== */

/*
 * Graph (directed, source = 0):
 *   0 --(4)--> 1
 *   0 --(1)--> 2
 *   2 --(2)--> 1
 *   1 --(1)--> 3
 *
 * MAX distances from node 0:
 *   Dist(1) = 4     (direct edge 0->1: 4, longer than 0->2->1: 1+2=3)
 *   Dist(2) = 1     (only path: 0->2: 1)
 *   Dist(3) = 5     (0->1->3: 4+1=5, longer than 0->2->1->3: 1+2+1=4)
 */
static void
test_max_sssp_triangle(void)
{
    TEST("MAX SSSP: triangle graph - correct maximum distances");

    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(SSSP_MAX_PROGRAM, &dd_plan);

    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);
    if (!w) {
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("could not create worker");
        return;
    }

    /*        src dst wgt */
    int64_t edges[] = {
        0, 1, 4, 0, 2, 1, 2, 1, 2, 1, 3, 1,
    };
    wl_dd_load_edb(w, "Edge", edges, 4, 3);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[80];
        snprintf(msg, sizeof(msg), "execute_cb returned %d", rc);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* nodes 1, 2, 3 reachable from 0 -> exactly 3 Dist tuples */
    int n = count_tuples(&results, "Dist");
    if (n != 3) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 3 Dist tuples, got %d", n);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* Dist(1) = 4 (direct edge beats 0->2->1=3) */
    int64_t d1[] = { 1, 4 };
    if (!has_tuple(&results, "Dist", d1, 2)) {
        char msg[64];
        int64_t got = dist_of(&results, "Dist", 1);
        snprintf(msg, sizeof(msg), "Dist(1) should be 4, got %" PRId64, got);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* Dist(2) = 1 */
    int64_t d2[] = { 2, 1 };
    if (!has_tuple(&results, "Dist", d2, 2)) {
        char msg[64];
        int64_t got = dist_of(&results, "Dist", 2);
        snprintf(msg, sizeof(msg), "Dist(2) should be 1, got %" PRId64, got);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* Dist(3) = 5 via 0->1->3 */
    int64_t d3[] = { 3, 5 };
    if (!has_tuple(&results, "Dist", d3, 2)) {
        char msg[64];
        int64_t got = dist_of(&results, "Dist", 3);
        snprintf(msg, sizeof(msg), "Dist(3) should be 5, got %" PRId64, got);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    wl_dd_worker_destroy(w);
    wl_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test 2: MAX SSSP converges to maximum (not initial direct-edge value)   */
/* ======================================================================== */

/*
 * Graph (source = 0):
 *   0 --(1)--> 1
 *   0 --(2)--> 2
 *   2 --(5)--> 1
 *
 * Initial base-case: Dist(1) = 1, Dist(2) = 2.
 * After one recursive step: Dist(1) via 0->2->1 = 2+5 = 7.
 * MAX must update Dist(1) from 1 to 7 at fixpoint.
 *
 * This tests that iterate() loops until convergence, not just one pass.
 */
static void
test_max_sssp_fixpoint_convergence(void)
{
    TEST("MAX SSSP: converges to global maximum (multi-iteration needed)");

    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(SSSP_MAX_PROGRAM, &dd_plan);

    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);
    if (!w) {
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("could not create worker");
        return;
    }

    int64_t edges[] = { 0, 1, 1, 0, 2, 2, 2, 1, 5 };
    wl_dd_load_edb(w, "Edge", edges, 3, 3);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[80];
        snprintf(msg, sizeof(msg), "execute_cb returned %d", rc);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* Dist(1) must be 7 (via 0->2->1=2+5), not 1 (direct) */
    int64_t val = dist_of(&results, "Dist", 1);
    if (val != 7) {
        char msg[80];
        snprintf(msg, sizeof(msg),
                 "Dist(1) should be 7 (max path via node 2), got %" PRId64,
                 val);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* Dist(2) = 2 */
    int64_t val2 = dist_of(&results, "Dist", 2);
    if (val2 != 2) {
        char msg[64];
        snprintf(msg, sizeof(msg), "Dist(2) should be 2, got %" PRId64, val2);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    wl_dd_worker_destroy(w);
    wl_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test 3: MAX SSSP - disconnected graph, unreachable nodes absent         */
/* ======================================================================== */

/*
 * Graph:  0->1 (w=5), 2->3 (w=1)
 * Nodes 2 and 3 are not reachable from source 0.
 *
 * Expected: only Dist(1, 5).  Dist(2) and Dist(3) must NOT appear.
 */
static void
test_max_sssp_disconnected(void)
{
    TEST("MAX SSSP: disconnected graph - unreachable nodes absent from Dist");

    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(SSSP_MAX_PROGRAM, &dd_plan);

    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);
    if (!w) {
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("could not create worker");
        return;
    }

    int64_t edges[] = { 0, 1, 5, 2, 3, 1 };
    wl_dd_load_edb(w, "Edge", edges, 2, 3);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[80];
        snprintf(msg, sizeof(msg), "execute_cb returned %d", rc);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* only Dist(1, 5) */
    int n = count_tuples(&results, "Dist");
    if (n != 1) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 1 Dist tuple, got %d", n);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int64_t d1[] = { 1, 5 };
    if (!has_tuple(&results, "Dist", d1, 2)) {
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("Dist(1, 5) missing");
        return;
    }

    /* unreachable nodes must not appear */
    int64_t d2_any = dist_of(&results, "Dist", 2);
    if (d2_any != INT64_MIN) {
        char msg[64];
        snprintf(msg, sizeof(msg), "Dist(2) should be absent, got %" PRId64,
                 d2_any);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    wl_dd_worker_destroy(w);
    wl_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test 4: MAX SSSP - single edge                                          */
/* ======================================================================== */

/*
 * Graph: 0 --(7)--> 1
 * Only one reachable node; Dist(1) = 7.
 */
static void
test_max_sssp_single_edge(void)
{
    TEST("MAX SSSP: single edge - Dist(1) = 7");

    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(SSSP_MAX_PROGRAM, &dd_plan);

    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);
    if (!w) {
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("could not create worker");
        return;
    }

    int64_t edges[] = { 0, 1, 7 };
    wl_dd_load_edb(w, "Edge", edges, 1, 3);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[80];
        snprintf(msg, sizeof(msg), "execute_cb returned %d", rc);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int n = count_tuples(&results, "Dist");
    if (n != 1) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 1 Dist tuple, got %d", n);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int64_t d1[] = { 1, 7 };
    if (!has_tuple(&results, "Dist", d1, 2)) {
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("Dist(1, 7) missing");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test 5: MAX SSSP - dense 5-node graph with multiple competing paths     */
/* ======================================================================== */

/*
 * Graph (source = 0):
 *   0 --(1)--> 1
 *   0 --(3)--> 2
 *   0 --(2)--> 3
 *   1 --(4)--> 4
 *   2 --(2)--> 4
 *   3 --(6)--> 4
 *
 * MAX distances from node 0:
 *   Dist(1) = 1     (only path: 0->1)
 *   Dist(2) = 3     (only path: 0->2)
 *   Dist(3) = 2     (only path: 0->3)
 *   Dist(4) = 8     (0->3->4: 2+6=8, beats 0->1->4: 1+4=5 and 0->2->4: 3+2=5)
 *
 * This tests that MAX selects the correct winner among several path sums
 * reaching the same destination.
 */
static void
test_max_sssp_dense(void)
{
    TEST(
        "MAX SSSP: dense 5-node graph - MAX selects longest path to each node");

    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(SSSP_MAX_PROGRAM, &dd_plan);

    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);
    if (!w) {
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("could not create worker");
        return;
    }

    /*
     * src dst wgt
     * 0->1 (1), 0->2 (3), 0->3 (2)
     * 1->4 (4), 2->4 (2), 3->4 (6)
     */
    int64_t edges[] = {
        0, 1, 1, 0, 2, 3, 0, 3, 2, 1, 4, 4, 2, 4, 2, 3, 4, 6,
    };
    wl_dd_load_edb(w, "Edge", edges, 6, 3);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[80];
        snprintf(msg, sizeof(msg), "execute_cb returned %d", rc);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* 4 reachable nodes: 1, 2, 3, 4 */
    int n = count_tuples(&results, "Dist");
    if (n != 4) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 4 Dist tuples, got %d", n);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* Dist(1) = 1 */
    int64_t val1 = dist_of(&results, "Dist", 1);
    if (val1 != 1) {
        char msg[64];
        snprintf(msg, sizeof(msg), "Dist(1) should be 1, got %" PRId64, val1);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* Dist(2) = 3 */
    int64_t val2 = dist_of(&results, "Dist", 2);
    if (val2 != 3) {
        char msg[64];
        snprintf(msg, sizeof(msg), "Dist(2) should be 3, got %" PRId64, val2);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* Dist(3) = 2 */
    int64_t val3 = dist_of(&results, "Dist", 3);
    if (val3 != 2) {
        char msg[64];
        snprintf(msg, sizeof(msg), "Dist(3) should be 2, got %" PRId64, val3);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* Dist(4) = 8 (via 0->3->4: 2+6=8, the longest of the three paths) */
    int64_t val4 = dist_of(&results, "Dist", 4);
    if (val4 != 8) {
        char msg[80];
        snprintf(msg, sizeof(msg),
                 "Dist(4) should be 8 (max via 0->3->4), got %" PRId64, val4);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    wl_dd_worker_destroy(w);
    wl_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf(
        "\n=== wirelog SSSP with MAX Aggregation - Integration Tests ===\n\n");

    test_max_sssp_triangle();
    test_max_sssp_fixpoint_convergence();
    test_max_sssp_disconnected();
    test_max_sssp_single_edge();
    test_max_sssp_dense();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
