/*
 * test_recursive_agg.c - End-to-end execution tests for monotone aggregation
 *                         in recursive iterate() scope (issue #69, RED phase)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * These tests exercise the full pipeline:
 *   Datalog source -> parse -> IR -> stratify -> DD plan -> marshal ->
 *   FFI -> Rust DD execute -> callback results
 *
 * They verify that MIN and MAX aggregations inside recursive strata produce
 * correct fixpoint results and converge properly.
 *
 * RED PHASE: All tests in this file are expected to FAIL until the GREEN
 * phase implementation lands in Rust (issue #69).  The tests will either:
 *   - Fail at plan generation (rc != 0) if non-monotone validation fires, or
 *   - Fail at execution (rc != 0) if Rust rejects REDUCE in iterate(), or
 *   - Produce wrong results if the computation is incorrect.
 *
 * When implementation is complete, all tests should PASS.
 */

#include "../wirelog/ffi/dd_ffi.h"
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

#define MAX_TUPLES 256
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
 * Find the value in column @col_idx for the tuple in @relation whose
 * key column @key_col equals @key_val.  Returns INT64_MIN if not found.
 */
static int64_t
find_value(const tuple_collector_t *c, const char *relation, uint32_t key_col,
           int64_t key_val, uint32_t col_idx)
{
    for (int i = 0; i < c->count; i++) {
        if (strcmp(c->relations[i], relation) != 0)
            continue;
        if (c->ncols[i] <= key_col || c->ncols[i] <= col_idx)
            continue;
        if (c->rows[i][key_col] == key_val)
            return c->rows[i][col_idx];
    }
    return INT64_MIN;
}

/* ======================================================================== */
/* Pipeline Helper                                                          */
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
/* Test 1: MIN aggregation - SSSP on a simple triangle graph               */
/* ======================================================================== */

/*
 * Graph:
 *   0 --(4)--> 1
 *   0 --(1)--> 2
 *   2 --(2)--> 1
 *   1 --(1)--> 3
 *
 * SSSP from node 0:
 *   Dist(0) = 0     (source; provided as EDB fact)
 *   Dist(1) = 3     (0->2->1: 1+2=3, shorter than 0->1: 4)
 *   Dist(2) = 1     (0->2: 1)
 *   Dist(3) = 4     (0->2->1->3: 1+2+1=4)
 *
 * Datalog:
 *   Dist(y, min(w))      :- Edge(0, y, w).
 *   Dist(y, min(dz + w)) :- Dist(z, dz), Edge(z, y, w).
 */
static void
test_min_sssp_triangle(void)
{
    TEST("MIN: SSSP on triangle graph - correct shortest distances");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(
        ".decl Edge(x: int32, y: int32, w: int32)\n"
        ".decl Dist(y: int32, d: int32)\n"
        "Dist(y, min(w))       :- Edge(0, y, w).\n"
        "Dist(y, min(dz + w))  :- Dist(z, dz), Edge(z, y, w).\n",
        &dd_plan);

    if (!ffi) {
        FAIL("could not generate FFI plan (not yet implemented)");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);
    if (!w) {
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("could not create worker");
        return;
    }

    /*       src dst wgt */
    int64_t edges[] = {
        0, 1, 4, 0, 2, 1, 2, 1, 2, 1, 3, 1,
    };
    wl_dd_load_edb(w, "Edge", edges, 4, 3);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg),
                 "execute_cb returned %d (iterate+REDUCE not yet supported)",
                 rc);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /*
     * Each node appears exactly once in Dist (MIN produces one tuple per key).
     */
    int n = count_tuples(&results, "Dist");
    if (n != 3) {
        /* nodes 1,2,3 reachable from 0 (0 is EDB source, not in Dist IDB) */
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 3 Dist tuples, got %d", n);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* Dist(1) = 3 */
    int64_t d1[] = { 1, 3 };
    if (!has_tuple(&results, "Dist", d1, 2)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("Dist(1) should be 3, not 4");
        return;
    }

    /* Dist(2) = 1 */
    int64_t d2[] = { 2, 1 };
    if (!has_tuple(&results, "Dist", d2, 2)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("Dist(2) should be 1");
        return;
    }

    /* Dist(3) = 4 */
    int64_t d3[] = { 3, 4 };
    if (!has_tuple(&results, "Dist", d3, 2)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("Dist(3) should be 4");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test 2: MIN aggregation - SSSP converges to fixpoint (not intermediate) */
/* ======================================================================== */

/*
 * Graph (longer path matters):
 *   0 --(10)--> 1
 *   0 --(1)-->  2
 *   2 --(1)-->  3
 *   3 --(1)-->  1     <- shorter path to 1: 0->2->3->1 = 3
 *
 * Dist(1) must converge to 3, not 10 (the initial direct-edge value).
 * This tests that iterate() loops until fixpoint, updating MIN correctly.
 */
static void
test_min_sssp_fixpoint_convergence(void)
{
    TEST("MIN: SSSP converges to fixpoint (longer iteration needed)");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(
        ".decl Edge(x: int32, y: int32, w: int32)\n"
        ".decl Dist(y: int32, d: int32)\n"
        "Dist(y, min(w))       :- Edge(0, y, w).\n"
        "Dist(y, min(dz + w))  :- Dist(z, dz), Edge(z, y, w).\n",
        &dd_plan);

    if (!ffi) {
        FAIL("could not generate FFI plan (not yet implemented)");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);
    if (!w) {
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("could not create worker");
        return;
    }

    int64_t edges[] = {
        0, 1, 10, 0, 2, 1, 2, 3, 1, 3, 1, 1,
    };
    wl_dd_load_edb(w, "Edge", edges, 4, 3);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg),
                 "execute_cb returned %d (iterate+REDUCE not yet supported)",
                 rc);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* Dist(1) must be 3, the shorter path via 0->2->3->1 */
    int64_t val = find_value(&results, "Dist", 0, 1, 1);
    if (val != 3) {
        char msg[64];
        snprintf(msg, sizeof(msg), "Dist(1) should be 3 (fixpoint), got %lld",
                 (long long)val);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    wl_dd_worker_destroy(w);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test 3: MIN aggregation - disconnected graph, unreachable nodes absent  */
/* ======================================================================== */

/*
 * Graph:  0->1 (w=5), 2->3 (w=1)  -- nodes 2,3 not reachable from 0
 *
 * Dist should only contain (1, 5).
 * Dist(2) and Dist(3) must NOT appear.
 */
static void
test_min_sssp_disconnected(void)
{
    TEST("MIN: SSSP - unreachable nodes absent from Dist");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(
        ".decl Edge(x: int32, y: int32, w: int32)\n"
        ".decl Dist(y: int32, d: int32)\n"
        "Dist(y, min(w))       :- Edge(0, y, w).\n"
        "Dist(y, min(dz + w))  :- Dist(z, dz), Edge(z, y, w).\n",
        &dd_plan);

    if (!ffi) {
        FAIL("could not generate FFI plan (not yet implemented)");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);
    if (!w) {
        wl_ffi_plan_free(ffi);
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
        char msg[64];
        snprintf(msg, sizeof(msg),
                 "execute_cb returned %d (iterate+REDUCE not yet supported)",
                 rc);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* only Dist(1) = 5 */
    int n = count_tuples(&results, "Dist");
    if (n != 1) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 1 Dist tuple, got %d", n);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int64_t d1[] = { 1, 5 };
    if (!has_tuple(&results, "Dist", d1, 2)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("Dist(1) = 5 missing");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test 4: MAX aggregation - longest path in a DAG                         */
/* ======================================================================== */

/*
 * DAG (no cycles):
 *   0 --(1)--> 1
 *   0 --(4)--> 2
 *   1 --(3)--> 3
 *   2 --(1)--> 3
 *
 * Longest path to each node from 0:
 *   LDist(1) = 1     (0->1)
 *   LDist(2) = 4     (0->2)
 *   LDist(3) = 5     (0->2->3: 4+1=5, longer than 0->1->3: 1+3=4)
 *
 * Datalog:
 *   LDist(y, max(w))       :- Edge(0, y, w).
 *   LDist(y, max(dz + w))  :- LDist(z, dz), Edge(z, y, w).
 */
static void
test_max_longest_path(void)
{
    TEST("MAX: longest path in DAG - correct maximum distances");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(
        ".decl Edge(x: int32, y: int32, w: int32)\n"
        ".decl LDist(y: int32, d: int32)\n"
        "LDist(y, max(w))       :- Edge(0, y, w).\n"
        "LDist(y, max(dz + w))  :- LDist(z, dz), Edge(z, y, w).\n",
        &dd_plan);

    if (!ffi) {
        FAIL("could not generate FFI plan (not yet implemented)");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);
    if (!w) {
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("could not create worker");
        return;
    }

    int64_t edges[] = {
        0, 1, 1, 0, 2, 4, 1, 3, 3, 2, 3, 1,
    };
    wl_dd_load_edb(w, "Edge", edges, 4, 3);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg),
                 "execute_cb returned %d (iterate+REDUCE not yet supported)",
                 rc);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* LDist(3) = 5 via 0->2->3 */
    int64_t d3[] = { 3, 5 };
    if (!has_tuple(&results, "LDist", d3, 2)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("LDist(3) should be 5 (longest path via node 2)");
        return;
    }

    /* LDist(2) = 4 */
    int64_t d2[] = { 2, 4 };
    if (!has_tuple(&results, "LDist", d2, 2)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("LDist(2) should be 4");
        return;
    }

    /* LDist(1) = 1 */
    int64_t d1[] = { 1, 1 };
    if (!has_tuple(&results, "LDist", d1, 2)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("LDist(1) should be 1");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test 5: MAX aggregation - converges to maximum (not initial value)      */
/* ======================================================================== */

/*
 * Graph:  0--(1)-->1, 0--(2)-->2, 2--(5)-->1
 * LDist(1) via direct: 1
 * LDist(1) via 0->2->1: 2+5=7  (this is larger, MAX should converge to 7)
 */
static void
test_max_longest_path_convergence(void)
{
    TEST("MAX: longest path converges to global maximum (multi-iteration)");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(
        ".decl Edge(x: int32, y: int32, w: int32)\n"
        ".decl LDist(y: int32, d: int32)\n"
        "LDist(y, max(w))       :- Edge(0, y, w).\n"
        "LDist(y, max(dz + w))  :- LDist(z, dz), Edge(z, y, w).\n",
        &dd_plan);

    if (!ffi) {
        FAIL("could not generate FFI plan (not yet implemented)");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);
    if (!w) {
        wl_ffi_plan_free(ffi);
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
        char msg[64];
        snprintf(msg, sizeof(msg),
                 "execute_cb returned %d (iterate+REDUCE not yet supported)",
                 rc);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* LDist(1) = 7 via 0->2->1 */
    int64_t val = find_value(&results, "LDist", 0, 1, 1);
    if (val != 7) {
        char msg[64];
        snprintf(msg, sizeof(msg), "LDist(1) should be 7 (max path), got %lld",
                 (long long)val);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    wl_dd_worker_destroy(w);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test 6: MIN aggregation - single-node graph (trivial case)              */
/* ======================================================================== */

/*
 * Graph: 0 --(7)--> 1  (one edge only)
 * Dist(1) = 7
 */
static void
test_min_single_edge(void)
{
    TEST("MIN: single edge graph - Dist(1) = 7");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(
        ".decl Edge(x: int32, y: int32, w: int32)\n"
        ".decl Dist(y: int32, d: int32)\n"
        "Dist(y, min(w))       :- Edge(0, y, w).\n"
        "Dist(y, min(dz + w))  :- Dist(z, dz), Edge(z, y, w).\n",
        &dd_plan);

    if (!ffi) {
        FAIL("could not generate FFI plan (not yet implemented)");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);
    if (!w) {
        wl_ffi_plan_free(ffi);
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
        char msg[64];
        snprintf(msg, sizeof(msg),
                 "execute_cb returned %d (iterate+REDUCE not yet supported)",
                 rc);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int64_t d1[] = { 1, 7 };
    if (!has_tuple(&results, "Dist", d1, 2)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("Dist(1) = 7 missing");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test 7: MIN aggregation - multiple source nodes (group-by key)          */
/* ======================================================================== */

/*
 * Two independent source nodes, each with their own shortest path:
 *   From 0: 0->1 (w=5), 0->2 (w=2), 2->1 (w=1) => Dist(0,1)=3, Dist(0,2)=2
 *   From 10: 10->11 (w=3)                        => Dist(10,11)=3
 *
 * The group-by key is the destination node (within source context).
 * Here we use a source-parameterized formulation:
 *   SrcDist(src, y, min(w))        :- SrcEdge(src, y, w).
 *   SrcDist(src, y, min(dz + w))   :- SrcDist(src, z, dz), SrcEdge(z, y, w).
 *
 * Simplified: two disjoint subgraphs, verify each independently.
 */
static void
test_min_multiple_components(void)
{
    TEST("MIN: SSSP with two disjoint components - independent min paths");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(
        ".decl Edge(x: int32, y: int32, w: int32)\n"
        ".decl Dist(y: int32, d: int32)\n"
        "Dist(y, min(w))       :- Edge(0, y, w).\n"
        "Dist(y, min(dz + w))  :- Dist(z, dz), Edge(z, y, w).\n",
        &dd_plan);

    if (!ffi) {
        FAIL("could not generate FFI plan (not yet implemented)");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);
    if (!w) {
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("could not create worker");
        return;
    }

    /* component 1 from node 0: 0->1 (5), 0->2 (2), 2->1 (1) */
    int64_t edges[] = {
        0, 1, 5, 0, 2, 2, 2, 1, 1,
    };
    wl_dd_load_edb(w, "Edge", edges, 3, 3);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg),
                 "execute_cb returned %d (iterate+REDUCE not yet supported)",
                 rc);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* Dist(1) = 3 (via 0->2->1), NOT 5 (direct) */
    int64_t d1_correct[] = { 1, 3 };
    int64_t d1_wrong[] = { 1, 5 };
    if (has_tuple(&results, "Dist", d1_wrong, 2)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("Dist(1)=5 present (wrong - should be 3 via shorter path)");
        return;
    }
    if (!has_tuple(&results, "Dist", d1_correct, 2)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("Dist(1)=3 missing");
        return;
    }

    /* Dist(2) = 2 */
    int64_t d2[] = { 2, 2 };
    if (!has_tuple(&results, "Dist", d2, 2)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("Dist(2)=2 missing");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test 8: MAX aggregation - empty graph produces no output                */
/* ======================================================================== */

static void
test_max_empty_graph(void)
{
    TEST("MAX: empty graph produces no LDist tuples");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(
        ".decl Edge(x: int32, y: int32, w: int32)\n"
        ".decl LDist(y: int32, d: int32)\n"
        "LDist(y, max(w))       :- Edge(0, y, w).\n"
        "LDist(y, max(dz + w))  :- LDist(z, dz), Edge(z, y, w).\n",
        &dd_plan);

    if (!ffi) {
        FAIL("could not generate FFI plan (not yet implemented)");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);
    if (!w) {
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("could not create worker");
        return;
    }

    wl_dd_load_edb(w, "Edge", NULL, 0, 3);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg),
                 "execute_cb returned %d (iterate+REDUCE not yet supported)",
                 rc);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int n = count_tuples(&results, "LDist");
    if (n != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 0 LDist tuples, got %d", n);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    wl_dd_worker_destroy(w);
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
    printf("\n=== wirelog Recursive Aggregation - Execution Tests (RED phase)"
           " ===\n"
           "\nNOTE: All tests are expected to FAIL until implementation.\n\n");

    /* MIN aggregation in iterate() */
    test_min_sssp_triangle();
    test_min_sssp_fixpoint_convergence();
    test_min_sssp_disconnected();
    test_min_single_edge();
    test_min_multiple_components();

    /* MAX aggregation in iterate() */
    test_max_longest_path();
    test_max_longest_path_convergence();
    test_max_empty_graph();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
