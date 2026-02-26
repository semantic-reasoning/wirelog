/*
 * test_dd_execute.c - End-to-end Datalog execution tests via C -> Rust FFI
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests the full pipeline: Datalog source -> parse -> IR -> stratify ->
 * DD plan -> marshal -> FFI -> Rust execute -> callback results.
 */

#include "../wirelog/ffi/dd_ffi.h"
#include "../wirelog/wirelog-parser.h"

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

#define MAX_TUPLES 128
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
/* Helper: full pipeline from Datalog source to FFI plan                    */
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
/* Test: Passthrough                                                        */
/* ======================================================================== */

/*
 * Datalog:  r(x) :- a(x).
 * EDB:      a(10). a(20). a(30).
 * Expected: r(10), r(20), r(30)
 */
static void
test_execute_passthrough(void)
{
    TEST("execute: passthrough r(x) :- a(x)");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(".decl a(x: int32)\n"
                                              ".decl r(x: int32)\n"
                                              "r(x) :- a(x).\n",
                                              &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);

    int64_t edb[] = { 10, 20, 30 };
    wl_dd_load_edb(w, "a", edb, 3, 1);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "execute_cb returned %d", rc);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int n = count_tuples(&results, "r");
    if (n != 3) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 3 tuples, got %d", n);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int64_t t1[] = { 10 }, t2[] = { 20 }, t3[] = { 30 };
    if (!has_tuple(&results, "r", t1, 1) || !has_tuple(&results, "r", t2, 1)
        || !has_tuple(&results, "r", t3, 1)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("missing expected tuple");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test: Transitive Closure (recursive)                                     */
/* ======================================================================== */

/*
 * Datalog:
 *   edge(1,2). edge(2,3). edge(3,4).
 *   tc(x, y) :- edge(x, y).
 *   tc(x, z) :- tc(x, y), edge(y, z).
 *
 * Expected: 6 tuples: (1,2),(2,3),(3,4),(1,3),(2,4),(1,4)
 */
static void
test_execute_transitive_closure(void)
{
    TEST("execute: transitive closure (recursive)");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi
        = ffi_plan_from_source(".decl edge(x: int32, y: int32)\n"
                               ".decl tc(x: int32, y: int32)\n"
                               "tc(x, y) :- edge(x, y).\n"
                               "tc(x, z) :- tc(x, y), edge(y, z).\n",
                               &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);

    int64_t edb[] = { 1, 2, 2, 3, 3, 4 };
    wl_dd_load_edb(w, "edge", edb, 3, 2);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "execute_cb returned %d", rc);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int n = count_tuples(&results, "tc");
    if (n != 6) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 6 tc tuples, got %d", n);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    /* Direct edges */
    int64_t e12[] = { 1, 2 }, e23[] = { 2, 3 }, e34[] = { 3, 4 };
    /* Transitive */
    int64_t e13[] = { 1, 3 }, e24[] = { 2, 4 }, e14[] = { 1, 4 };

    if (!has_tuple(&results, "tc", e12, 2) || !has_tuple(&results, "tc", e23, 2)
        || !has_tuple(&results, "tc", e34, 2)
        || !has_tuple(&results, "tc", e13, 2)
        || !has_tuple(&results, "tc", e24, 2)
        || !has_tuple(&results, "tc", e14, 2)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("missing expected tc tuple");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test: Join                                                               */
/* ======================================================================== */

/*
 * Datalog:
 *   a(1,2). a(3,2).
 *   b(2,5). b(2,6).
 *   r(x, z) :- a(x, y), b(y, z).
 *
 * Expected: r(1,5), r(1,6), r(3,5), r(3,6)
 */
static void
test_execute_join(void)
{
    TEST("execute: join r(x,z) :- a(x,y), b(y,z)");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(".decl a(x: int32, y: int32)\n"
                                              ".decl b(y: int32, z: int32)\n"
                                              ".decl r(x: int32, z: int32)\n"
                                              "r(x, z) :- a(x, y), b(y, z).\n",
                                              &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);

    int64_t a_data[] = { 1, 2, 3, 2 };
    int64_t b_data[] = { 2, 5, 2, 6 };
    wl_dd_load_edb(w, "a", a_data, 2, 2);
    wl_dd_load_edb(w, "b", b_data, 2, 2);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "execute_cb returned %d", rc);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int n = count_tuples(&results, "r");
    if (n != 4) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 4 result tuples, got %d", n);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int64_t r1[] = { 1, 5 }, r2[] = { 1, 6 };
    int64_t r3[] = { 3, 5 }, r4[] = { 3, 6 };
    if (!has_tuple(&results, "r", r1, 2) || !has_tuple(&results, "r", r2, 2)
        || !has_tuple(&results, "r", r3, 2)
        || !has_tuple(&results, "r", r4, 2)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("missing expected result tuple");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test: Filter                                                             */
/* ======================================================================== */

/*
 * Datalog:
 *   edge(1,2). edge(2,3). edge(3,4).
 *   big(x, y) :- edge(x, y), x > 1.
 *
 * Expected: big(2,3), big(3,4)
 */
static void
test_execute_filter(void)
{
    TEST("execute: filter big(x,y) :- edge(x,y), x > 1");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi
        = ffi_plan_from_source(".decl edge(x: int32, y: int32)\n"
                               ".decl big(x: int32, y: int32)\n"
                               "big(x, y) :- edge(x, y), x > 1.\n",
                               &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);

    int64_t edb[] = { 1, 2, 2, 3, 3, 4 };
    wl_dd_load_edb(w, "edge", edb, 3, 2);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "execute_cb returned %d", rc);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int n = count_tuples(&results, "big");
    if (n != 2) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 2 big tuples, got %d", n);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int64_t t1[] = { 2, 3 }, t2[] = { 3, 4 };
    if (!has_tuple(&results, "big", t1, 2)
        || !has_tuple(&results, "big", t2, 2)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("missing expected big tuple");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test: Aggregation                                                        */
/* ======================================================================== */

/*
 * Datalog:
 *   data(1,10). data(1,20). data(2,30).
 *   cnt(x, count(y)) :- data(x, y).
 *
 * Expected: cnt(1, 2), cnt(2, 1)
 */
static void
test_execute_aggregation(void)
{
    TEST("execute: aggregation cnt(x, count(y)) :- data(x, y)");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi
        = ffi_plan_from_source(".decl data(x: int32, y: int32)\n"
                               ".decl cnt(x: int32, c: int32)\n"
                               "cnt(x, count(y)) :- data(x, y).\n",
                               &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);

    int64_t edb[] = { 1, 10, 1, 20, 2, 30 };
    wl_dd_load_edb(w, "data", edb, 3, 2);

    tuple_collector_t results;
    memset(&results, 0, sizeof(results));

    int rc = wl_dd_execute_cb(ffi, w, collect_tuple, &results);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "execute_cb returned %d", rc);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int n = count_tuples(&results, "cnt");
    if (n != 2) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 2 cnt tuples, got %d", n);
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL(msg);
        return;
    }

    int64_t c1[] = { 1, 2 }, c2[] = { 2, 1 };
    if (!has_tuple(&results, "cnt", c1, 2)
        || !has_tuple(&results, "cnt", c2, 2)) {
        wl_dd_worker_destroy(w);
        wl_ffi_plan_free(ffi);
        wl_dd_plan_free(dd_plan);
        FAIL("missing expected cnt tuple");
        return;
    }

    wl_dd_worker_destroy(w);
    wl_ffi_plan_free(ffi);
    wl_dd_plan_free(dd_plan);
    PASS();
}

/* ======================================================================== */
/* Test: Callback Variants                                                  */
/* ======================================================================== */

static void
test_execute_null_callback(void)
{
    TEST("execute: NULL callback discards results");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(".decl a(x: int32)\n"
                                              ".decl r(x: int32)\n"
                                              "r(x) :- a(x).\n",
                                              &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);

    int64_t edb[] = { 1, 2, 3 };
    wl_dd_load_edb(w, "a", edb, 3, 1);

    int rc = wl_dd_execute_cb(ffi, w, NULL, NULL);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "execute_cb returned %d", rc);
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

static void
test_execute_no_callback(void)
{
    TEST("execute: wl_dd_execute (no callback variant)");

    wl_dd_plan_t *dd_plan = NULL;
    wl_ffi_plan_t *ffi = ffi_plan_from_source(".decl a(x: int32)\n"
                                              ".decl r(x: int32)\n"
                                              "r(x) :- a(x).\n",
                                              &dd_plan);
    if (!ffi) {
        FAIL("could not generate FFI plan");
        return;
    }

    wl_dd_worker_t *w = wl_dd_worker_create(1);

    int64_t edb[] = { 1, 2 };
    wl_dd_load_edb(w, "a", edb, 2, 1);

    int rc = wl_dd_execute(ffi, w);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "execute returned %d", rc);
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
    printf("\n=== wirelog DD Execution Tests (C -> Rust FFI) ===\n\n");

    printf("--- End-to-end Datalog Execution ---\n");
    test_execute_passthrough();
    test_execute_transitive_closure();
    test_execute_join();
    test_execute_filter();
    test_execute_aggregation();

    printf("\n--- Callback Variants ---\n");
    test_execute_null_callback();
    test_execute_no_callback();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
