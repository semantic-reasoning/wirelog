/*
 * test_recursive_agg_cc_min.c - Integration tests for connected component
 *                                labeling using monotone MIN aggregation in
 *                                recursive iterate() scope (issue #69,
 *                                Phase 4 REFACTOR)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * These tests exercise the full pipeline for CC labeling:
 *   Datalog source -> parse -> IR -> stratify -> DD plan -> marshal ->
 *   FFI -> Rust DD execute -> callback results
 *
 * Algorithm: MIN-label propagation on undirected graphs.
 *
 * The executor's JOIN operator keys the right relation by its first column.
 * Therefore the recursive rule uses the form where Edge's first column is
 * the join variable:
 *
 *   Label(x, min(x))  :- Edge(x, y).
 *   Label(y, min(y))  :- Edge(x, y).
 *   Label(x, min(l))  :- Label(y, l), Edge(y, x).
 *
 * To model undirected edges each test loads both (a, b) and (b, a) for
 * every undirected edge {a, b}.  The single recursive rule then propagates
 * the minimum label in both directions at fixpoint.
 *
 * At fixpoint, Label(x, c) holds where c is the minimum node ID in the
 * connected component containing x.  Each node appears exactly once
 * (MIN produces one tuple per key).
 *
 * Tests:
 *   1. Two-component graph  -- minimal label correctness per component
 *   2. Dense triangle       -- all nodes in one component, one label
 *   3. Single edge          -- trivial two-node component
 *   4. Linear chain         -- label propagates across multiple hops
 *   5. Three components     -- each component gets correct minimal label
 */

#include "../wirelog/backend/dd/dd_ffi.h"
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
 * Return the label (column 1) assigned to node @node_id (column 0) in
 * @relation.  Returns INT64_MIN if no tuple found.
 */
static int64_t
label_of(const tuple_collector_t *c, const char *relation, int64_t node_id)
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
 * CC Datalog program: MIN-label propagation on undirected graph.
 *
 * Each node seeds itself as its own label.  Labels propagate along both
 * directions of each edge, taking the MIN at each step.  At fixpoint every
 * node carries the minimum node-ID of its component.
 */
/*
 * CC Datalog program: MIN-label propagation using forward-edge joins.
 *
 * The executor joins the right relation by its first column.  To propagate
 * labels in both directions of an undirected edge {a,b}, each test loads
 * both (a,b) and (b,a).  The single recursive rule then propagates the
 * minimum label via the edge source (first column of Edge).
 */
#define CC_MIN_PROGRAM                   \
    ".decl Edge(x: int32, y: int32)\n"   \
    ".decl Label(x: int32, l: int32)\n"  \
    "Label(x, min(x))  :- Edge(x, y).\n" \
    "Label(y, min(y))  :- Edge(x, y).\n" \
    "Label(x, min(l))  :- Label(y, l), Edge(y, x).\n"

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
/* Test 1: Two-component graph                                             */
/* ======================================================================== */

/*
 * Graph (undirected):
 *   Component A: 1-2, 2-3  -> nodes {1, 2, 3}, min label = 1
 *   Component B: 4-5        -> nodes {4, 5},    min label = 4
 *
 * Each undirected edge is stored as both (a,b) and (b,a) so that the
 * single forward-propagation rule covers both directions.
 *
 * Expected Label tuples: (1,1), (2,1), (3,1), (4,4), (5,4)
 * No cross-component labeling: Label(1,4), Label(4,1) must not appear.
 */
static void
test_cc_two_components(void)
{
    TEST("CC MIN: two-component graph - correct min label per component");

    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(CC_MIN_PROGRAM, &dd_plan);

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

    /* undirected edges: 1-2, 2-3, 4-5 -- each stored both ways */
    int64_t edges[] = { 1, 2, 2, 1, 2, 3, 3, 2, 4, 5, 5, 4 };
    wl_dd_load_edb(w, "Edge", edges, 6, 2);

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

    /* exactly 5 Label tuples (one per node) */
    int n = count_tuples(&results, "Label");
    if (n != 5) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 5 Label tuples, got %d", n);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* component A: every node labelled 1 */
    for (int64_t node = 1; node <= 3; node++) {
        int64_t lbl = label_of(&results, "Label", node);
        if (lbl != 1) {
            char msg[80];
            snprintf(msg, sizeof(msg),
                     "Label(%" PRId64 ") = %" PRId64 ", expected 1", node, lbl);
            wl_dd_worker_destroy(w);
            wl_plan_free(ffi);
            wl_dd_plan_free(dd_plan);
            FAIL(msg);
            return;
        }
    }

    /* component B: every node labelled 4 */
    for (int64_t node = 4; node <= 5; node++) {
        int64_t lbl = label_of(&results, "Label", node);
        if (lbl != 4) {
            char msg[80];
            snprintf(msg, sizeof(msg),
                     "Label(%" PRId64 ") = %" PRId64 ", expected 4", node, lbl);
            wl_dd_worker_destroy(w);
            wl_plan_free(ffi);
            wl_dd_plan_free(dd_plan);
            FAIL(msg);
            return;
        }
    }

    /* no cross-contamination: Label(1, 4) must not exist */
    int64_t cross[] = { 1, 4 };
    if (has_tuple(&results, "Label", cross, 2)) {
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("Label(1,4) present: component A contaminated by component B");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test 2: Dense triangle - all nodes in one component                     */
/* ======================================================================== */

/*
 * Graph (undirected triangle): 2-3, 3-4, 4-2
 * All three nodes are in the same component -> min label = 2.
 * Each edge loaded both ways for symmetric propagation.
 *
 * Expected: Label(2,2), Label(3,2), Label(4,2) -- exactly 3 tuples.
 */
static void
test_cc_triangle(void)
{
    TEST("CC MIN: dense triangle - all three nodes labelled with minimum");

    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(CC_MIN_PROGRAM, &dd_plan);

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

    /* triangle: 2-3, 3-4, 4-2 -- each edge stored both ways */
    int64_t edges[] = { 2, 3, 3, 2, 3, 4, 4, 3, 4, 2, 2, 4 };
    wl_dd_load_edb(w, "Edge", edges, 6, 2);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "execute_cb returned %d", rc);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int n = count_tuples(&results, "Label");
    if (n != 3) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 3 Label tuples, got %d", n);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* all three nodes must carry label 2 (the minimum) */
    for (int64_t node = 2; node <= 4; node++) {
        int64_t lbl = label_of(&results, "Label", node);
        if (lbl != 2) {
            char msg[80];
            snprintf(msg, sizeof(msg),
                     "Label(%" PRId64 ") = %" PRId64 ", expected 2", node, lbl);
            wl_dd_worker_destroy(w);
            wl_plan_free(ffi);
            wl_dd_plan_free(dd_plan);
            FAIL(msg);
            return;
        }
    }

    wl_dd_worker_destroy(w);
    wl_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test 3: Single edge - trivial two-node component                        */
/* ======================================================================== */

/*
 * Graph: single edge 3-7.
 * Both nodes are in one component; min label = 3.
 * Edge stored both ways for symmetric propagation.
 *
 * Expected: Label(3,3), Label(7,3) -- exactly 2 tuples.
 */
static void
test_cc_single_edge(void)
{
    TEST("CC MIN: single edge - both endpoints get min label");

    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(CC_MIN_PROGRAM, &dd_plan);

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

    /* edge 3-7 stored both ways */
    int64_t edges[] = { 3, 7, 7, 3 };
    wl_dd_load_edb(w, "Edge", edges, 2, 2);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "execute_cb returned %d", rc);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int n = count_tuples(&results, "Label");
    if (n != 2) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 2 Label tuples, got %d", n);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* Label(3) = 3 */
    int64_t t33[] = { 3, 3 };
    if (!has_tuple(&results, "Label", t33, 2)) {
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("Label(3,3) missing");
        return;
    }

    /* Label(7) = 3 */
    int64_t t73[] = { 7, 3 };
    if (!has_tuple(&results, "Label", t73, 2)) {
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("Label(7,3) missing: label did not propagate from 3 to 7");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test 4: Linear chain - label propagates across multiple hops            */
/* ======================================================================== */

/*
 * Chain: 5 - 3 - 1 - 2 - 4
 * All five nodes form one component; min label = 1.
 *
 * This tests that MIN propagation converges through multiple hops (not just
 * direct neighbors).  The minimum node (1) is in the middle; its label
 * must reach both endpoints (5 and 4).
 * Each edge stored both ways for symmetric propagation.
 *
 * Expected: Label(x, 1) for x in {1, 2, 3, 4, 5} -- exactly 5 tuples.
 */
static void
test_cc_linear_chain(void)
{
    TEST("CC MIN: linear chain - min label propagates to all endpoints");

    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(CC_MIN_PROGRAM, &dd_plan);

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

    /* chain: 5-3, 3-1, 1-2, 2-4 -- each edge stored both ways */
    int64_t edges[] = { 5, 3, 3, 5, 3, 1, 1, 3, 1, 2, 2, 1, 2, 4, 4, 2 };
    wl_dd_load_edb(w, "Edge", edges, 8, 2);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "execute_cb returned %d", rc);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int n = count_tuples(&results, "Label");
    if (n != 5) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 5 Label tuples, got %d", n);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* every node in the chain must carry label 1 */
    int64_t nodes[] = { 1, 2, 3, 4, 5 };
    for (int i = 0; i < 5; i++) {
        int64_t lbl = label_of(&results, "Label", nodes[i]);
        if (lbl != 1) {
            char msg[80];
            snprintf(msg, sizeof(msg),
                     "Label(%" PRId64 ") = %" PRId64
                     ", expected 1 (min not propagated far enough)",
                     nodes[i], lbl);
            wl_dd_worker_destroy(w);
            wl_plan_free(ffi);
            wl_dd_plan_free(dd_plan);
            FAIL(msg);
            return;
        }
    }

    wl_dd_worker_destroy(w);
    wl_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test 5: Three components including a self-connected singleton via edge  */
/* ======================================================================== */

/*
 * Graph (undirected):
 *   Component A: 10-11, 11-12  -> nodes {10, 11, 12}, min label = 10
 *   Component B: 20-21          -> nodes {20, 21},    min label = 20
 *   Component C: 30-31, 30-32  -> nodes {30, 31, 32}, min label = 30
 *
 * Each edge stored both ways for symmetric propagation.
 *
 * Total nodes: 3 + 2 + 3 = 8.  Expected: 8 Label tuples (one per node).
 * Correctness:
 *   - All nodes in A carry label 10
 *   - All nodes in B carry label 20
 *   - All nodes in C carry label 30
 *   - No cross-component labels
 */
static void
test_cc_three_components(void)
{
    TEST("CC MIN: three components - each gets correct minimal label");

    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(CC_MIN_PROGRAM, &dd_plan);

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

    /* A: 10-11, 11-12 | B: 20-21 | C: 30-31, 30-32 -- each edge both ways */
    int64_t edges[] = { 10, 11, 11, 10, 11, 12, 12, 11, 20, 21,
                        21, 20, 30, 31, 31, 30, 30, 32, 32, 30 };
    wl_dd_load_edb(w, "Edge", edges, 10, 2);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "execute_cb returned %d", rc);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int n = count_tuples(&results, "Label");
    if (n != 8) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 8 Label tuples, got %d", n);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* component A: nodes 10,11,12 -> label 10 */
    int64_t a_nodes[] = { 10, 11, 12 };
    int64_t a_label = 10;
    for (int i = 0; i < 3; i++) {
        int64_t lbl = label_of(&results, "Label", a_nodes[i]);
        if (lbl != a_label) {
            char msg[80];
            snprintf(msg, sizeof(msg),
                     "Label(%" PRId64 ") = %" PRId64 ", expected %" PRId64,
                     a_nodes[i], lbl, a_label);
            wl_dd_worker_destroy(w);
            wl_plan_free(ffi);
            wl_dd_plan_free(dd_plan);
            FAIL(msg);
            return;
        }
    }

    /* component B: nodes 20,21 -> label 20 */
    int64_t b_nodes[] = { 20, 21 };
    int64_t b_label = 20;
    for (int i = 0; i < 2; i++) {
        int64_t lbl = label_of(&results, "Label", b_nodes[i]);
        if (lbl != b_label) {
            char msg[80];
            snprintf(msg, sizeof(msg),
                     "Label(%" PRId64 ") = %" PRId64 ", expected %" PRId64,
                     b_nodes[i], lbl, b_label);
            wl_dd_worker_destroy(w);
            wl_plan_free(ffi);
            wl_dd_plan_free(dd_plan);
            FAIL(msg);
            return;
        }
    }

    /* component C: nodes 30,31,32 -> label 30 */
    int64_t c_nodes[] = { 30, 31, 32 };
    int64_t c_label = 30;
    for (int i = 0; i < 3; i++) {
        int64_t lbl = label_of(&results, "Label", c_nodes[i]);
        if (lbl != c_label) {
            char msg[80];
            snprintf(msg, sizeof(msg),
                     "Label(%" PRId64 ") = %" PRId64 ", expected %" PRId64,
                     c_nodes[i], lbl, c_label);
            wl_dd_worker_destroy(w);
            wl_plan_free(ffi);
            wl_dd_plan_free(dd_plan);
            FAIL(msg);
            return;
        }
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
    printf("\n=== wirelog CC with MIN Aggregation - Integration Tests ===\n\n");

    test_cc_two_components();
    test_cc_triangle();
    test_cc_single_edge();
    test_cc_linear_chain();
    test_cc_three_components();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
