/*
 * test_recursive_agg_baseline.c - Baseline tests for recursive programs
 *                                  WITHOUT aggregation (must pass before
 *                                  implementing monotone aggregation)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * These tests exercise the existing recursive iterate() execution path
 * to establish a working baseline.  They MUST pass with the current
 * codebase.  Any failure here indicates a regression unrelated to
 * issue #69.
 *
 * Baseline programs:
 *   1. Transitive closure (TC) - pure recursion, no aggregation
 *   2. Connected components (CC) - symmetric closure, no aggregation
 *   3. SSSP skeleton - graph reachability without distance aggregation
 */

#include "../wirelog/backend/dd/dd_ffi.h"
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

/* ======================================================================== */
/* Pipeline Helper                                                          */
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
/* Baseline Test 1: Transitive Closure                                     */
/* ======================================================================== */

/*
 * Graph:  1->2, 2->3, 3->4
 * TC(1,2), TC(2,3), TC(3,4)  -- from base arcs
 * TC(1,3), TC(2,4)            -- 1-hop transitive
 * TC(1,4)                     -- 2-hop transitive
 * Total: 6 TC tuples
 */
static void
test_baseline_transitive_closure(void)
{
    TEST("baseline: TC recursive - all transitive pairs computed");

    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(".decl Arc(x: int32, y: int32)\n"
                                          ".decl Tc(x: int32, y: int32)\n"
                                          "Tc(x, y) :- Arc(x, y).\n"
                                          "Tc(x, y) :- Tc(x, z), Arc(z, y).\n",
                                          &dd_plan);

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

    /* chain: 1->2, 2->3, 3->4 */
    int64_t arcs[] = { 1, 2, 2, 3, 3, 4 };
    wl_dd_load_edb(w, "Arc", arcs, 3, 2);

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

    int n = count_tuples(&results, "Tc");
    if (n != 6) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 6 TC tuples, got %d", n);
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* spot-check transitive pair (1,4) */
    int64_t t14[] = { 1, 4 };
    if (!has_tuple(&results, "Tc", t14, 2)) {
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("missing transitive pair (1,4)");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Baseline Test 2: Connected Components (symmetric closure)               */
/* ======================================================================== */

/*
 * Graph edges (undirected): {1,2}, {2,3}, {4,5}
 * Component 1: nodes 1,2,3 all reachable from each other
 * Component 2: nodes 4,5 reachable from each other
 *
 * Reach(x, y) :- Edge(x, y).
 * Reach(x, y) :- Edge(y, x).           -- symmetry
 * Reach(x, y) :- Reach(x, z), Reach(z, y).  -- transitivity
 *
 * Expected: 6 pairs within comp-1 + 2 pairs within comp-2 = 8 Reach tuples
 * (excluding self-loops: we verify only cross-pairs)
 */
static void
test_baseline_connected_components(void)
{
    TEST("baseline: connected components - two separate components");

    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi
        = ffi_plan_from_source(".decl Edge(x: int32, y: int32)\n"
                               ".decl Reach(x: int32, y: int32)\n"
                               "Reach(x, y) :- Edge(x, y).\n"
                               "Reach(x, y) :- Edge(y, x).\n"
                               "Reach(x, y) :- Reach(x, z), Reach(z, y).\n",
                               &dd_plan);

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

    /* undirected: 1-2, 2-3, 4-5 */
    int64_t edges[] = { 1, 2, 2, 3, 4, 5 };
    wl_dd_load_edb(w, "Edge", edges, 3, 2);

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

    /* nodes 1,2,3 should be mutually reachable -> at least pairs (1,3) and
     * (3,1) */
    int64_t t13[] = { 1, 3 };
    int64_t t31[] = { 3, 1 };
    if (!has_tuple(&results, "Reach", t13, 2)
        || !has_tuple(&results, "Reach", t31, 2)) {
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("nodes 1 and 3 should be mutually reachable");
        return;
    }

    /* nodes 1 and 4 should NOT be reachable from each other */
    int64_t t14[] = { 1, 4 };
    if (has_tuple(&results, "Reach", t14, 2)) {
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("nodes 1 and 4 should NOT be reachable");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Baseline Test 3: Graph reachability (source-specific)                   */
/* ======================================================================== */

/*
 * Graph:  0->1 (weight 4), 0->2 (weight 1), 2->1 (weight 2), 1->3 (weight 1)
 * Reachable(x, y) :- Arc(x, y, _).
 * Reachable(x, y) :- Reachable(x, z), Arc(z, y, _).
 * From node 0: reaches 1, 2, 3
 */
static void
test_baseline_graph_reachability(void)
{
    TEST("baseline: graph reachability without distances");

    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(
        ".decl Arc(x: int32, y: int32)\n"
        ".decl Reachable(x: int32, y: int32)\n"
        "Reachable(x, y) :- Arc(x, y).\n"
        "Reachable(x, y) :- Reachable(x, z), Arc(z, y).\n",
        &dd_plan);

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

    /* 0->1, 0->2, 2->1, 1->3 */
    int64_t arcs[] = { 0, 1, 0, 2, 2, 1, 1, 3 };
    wl_dd_load_edb(w, "Arc", arcs, 4, 2);

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

    /* 0 should reach 1, 2, and 3 */
    int64_t r01[] = { 0, 1 };
    int64_t r02[] = { 0, 2 };
    int64_t r03[] = { 0, 3 };
    if (!has_tuple(&results, "Reachable", r01, 2)
        || !has_tuple(&results, "Reachable", r02, 2)
        || !has_tuple(&results, "Reachable", r03, 2)) {
        wl_dd_worker_destroy(w);
        wl_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("node 0 should reach nodes 1, 2, and 3");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Baseline Test 4: Empty recursive program (no EDB facts)                 */
/* ======================================================================== */

static void
test_baseline_empty_edb(void)
{
    TEST("baseline: recursive program with no EDB facts produces no IDB");

    wl_dd_plan_t *dd_plan = NULL;
    wl_plan_t *ffi = ffi_plan_from_source(".decl Arc(x: int32, y: int32)\n"
                                          ".decl Tc(x: int32, y: int32)\n"
                                          "Tc(x, y) :- Arc(x, y).\n"
                                          "Tc(x, y) :- Tc(x, z), Arc(z, y).\n",
                                          &dd_plan);

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

    /* load zero rows */
    wl_dd_load_edb(w, "Arc", NULL, 0, 2);

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

    int n = count_tuples(&results, "Tc");
    if (n != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 0 Tc tuples, got %d", n);
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
/* Baseline Test 5: Stratum is_recursive flag set for recursive programs   */
/* ======================================================================== */

static void
test_baseline_recursive_stratum_flag(void)
{
    TEST("baseline: TC program stratum has is_recursive=true");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl Arc(x: int32, y: int32)\n"
                               ".decl Tc(x: int32, y: int32)\n"
                               "Tc(x, y) :- Arc(x, y).\n"
                               "Tc(x, y) :- Tc(x, z), Arc(z, y).\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);
    wirelog_program_free(prog);

    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "plan_generate returned %d", rc);
        FAIL(msg);
        return;
    }

    /* find at least one recursive stratum */
    bool found_recursive = false;
    for (uint32_t i = 0; i < plan->stratum_count; i++) {
        if (plan->strata[i].is_recursive) {
            found_recursive = true;
            break;
        }
    }

    if (!found_recursive) {
        wl_dd_plan_free(plan);
        FAIL("TC program should have at least one recursive stratum");
        return;
    }

    wl_dd_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Baseline Test 6: Non-recursive program stratum flag is false            */
/* ======================================================================== */

static void
test_baseline_nonrecursive_stratum_flag(void)
{
    TEST("baseline: simple non-recursive program has is_recursive=false");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n"
                                                   ".decl r(x: int32)\n"
                                                   "r(x) :- a(x).\n",
                                                   &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_dd_plan_t *plan = NULL;
    int rc = wl_dd_plan_generate(prog, &plan);
    wirelog_program_free(prog);

    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "plan_generate returned %d", rc);
        FAIL(msg);
        return;
    }

    /* all strata should be non-recursive */
    for (uint32_t i = 0; i < plan->stratum_count; i++) {
        if (plan->strata[i].is_recursive) {
            wl_dd_plan_free(plan);
            FAIL(
                "simple non-recursive program should have no recursive strata");
            return;
        }
    }

    wl_dd_plan_free(plan);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("\n=== wirelog Recursive Aggregation - Baseline Tests ===\n\n");

    test_baseline_transitive_closure();
    test_baseline_connected_components();
    test_baseline_graph_reachability();
    test_baseline_empty_edb();
    test_baseline_recursive_stratum_flag();
    test_baseline_nonrecursive_stratum_flag();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
