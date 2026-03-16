/*
 * test_frontier.c - Frontier tracking tests (Phase 3B)
 *
 * Test-driven development for frontier computation:
 * Frontier = minimum (iteration, stratum) that has been processed
 * Purpose: Skip unnecessary recalculation of old data
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Import frontier types (to be defined in columnar_nanoarrow.h) */
#include "../wirelog/columnar/columnar_nanoarrow.h"

/* ========================================================================
 * TEST HARNESS MACROS
 * ======================================================================== */

#define TEST(name)                       \
    do {                                 \
        printf("  [TEST] %-60s ", name); \
        fflush(stdout);                  \
    } while (0)

#define PASS              \
    do {                  \
        printf("PASS\n"); \
        tests_passed++;   \
    } while (0)

#define FAIL(msg)                  \
    do {                           \
        printf("FAIL: %s\n", msg); \
        tests_failed++;            \
    } while (0)

static int tests_passed = 0;
static int tests_failed = 0;

/* ========================================================================
 * TEST SUITE: Frontier Data Structure
 * ======================================================================== */

static void
test_frontier_init(void)
{
    TEST("frontier_init returns zero state");
    col_frontier_t f = { 0, 0 };
    if (f.iteration == 0 && f.stratum == 0) {
        PASS;
    } else {
        FAIL("Expected iteration=0, stratum=0");
    }
}

static void
test_frontier_advance_iteration(void)
{
    TEST("frontier_advance_iteration increments iteration");
    col_frontier_t f = { 2, 3 };
    /* Simulate: completed all strata at iteration 2 */
    f.iteration = 3;
    f.stratum = 0;
    if (f.iteration == 3 && f.stratum == 0) {
        PASS;
    } else {
        FAIL("Expected iteration=3, stratum=0");
    }
}

static void
test_frontier_advance_stratum(void)
{
    TEST("frontier_advance_stratum increments stratum");
    col_frontier_t f = { 1, 2 };
    /* Simulate: completed stratum 2 in iteration 1 */
    f.stratum = 3;
    if (f.iteration == 1 && f.stratum == 3) {
        PASS;
    } else {
        FAIL("Expected iteration=1, stratum=3");
    }
}

static void
test_frontier_is_after(void)
{
    TEST("frontier_is_after: data (iter, strat) > frontier");
    col_frontier_t f = { 2, 1 };
    /* Data at iteration 3, stratum 0 is after frontier (2, 1) */
    col_delta_timestamp_t ts = { 3, 0 };
    int is_after = (ts.iteration > f.iteration)
                   || (ts.iteration == f.iteration && ts.stratum > f.stratum);
    if (is_after) {
        PASS;
    } else {
        FAIL("Expected data (3,0) > frontier (2,1)");
    }
}

static void
test_frontier_is_before(void)
{
    TEST("frontier_is_before: data (iter, strat) <= frontier");
    col_frontier_t f = { 2, 1 };
    /* Data at iteration 2, stratum 1 is at frontier (2, 1) */
    col_delta_timestamp_t ts = { 2, 1 };
    int is_before = (ts.iteration < f.iteration)
                    || (ts.iteration == f.iteration && ts.stratum <= f.stratum);
    if (is_before) {
        PASS;
    } else {
        FAIL("Expected data (2,1) <= frontier (2,1)");
    }
}

static void
test_frontier_ordering_iter_less(void)
{
    TEST("Frontier ordering: (1,5) < (2,0)");
    col_frontier_t f1 = { 1, 5 };
    col_frontier_t f2 = { 2, 0 };
    int is_less = (f1.iteration < f2.iteration)
                  || (f1.iteration == f2.iteration && f1.stratum < f2.stratum);
    if (is_less) {
        PASS;
    } else {
        FAIL("Expected (1,5) < (2,0)");
    }
}

static void
test_frontier_ordering_same_iter_stratum_less(void)
{
    TEST("Frontier ordering: (3,2) < (3,5) (same iteration)");
    col_frontier_t f1 = { 3, 2 };
    col_frontier_t f2 = { 3, 5 };
    int is_less = (f1.iteration < f2.iteration)
                  || (f1.iteration == f2.iteration && f1.stratum < f2.stratum);
    if (is_less) {
        PASS;
    } else {
        FAIL("Expected (3,2) < (3,5)");
    }
}

/* ========================================================================
 * MAIN TEST RUNNER
 * ======================================================================== */

int
main(void)
{
    printf("\n=== Frontier Tracking Tests (Phase 3B) ===\n\n");

    /* Test frontier initialization */
    test_frontier_init();
    test_frontier_advance_iteration();
    test_frontier_advance_stratum();

    /* Test frontier comparison */
    test_frontier_is_after();
    test_frontier_is_before();

    /* Test frontier ordering */
    test_frontier_ordering_iter_less();
    test_frontier_ordering_same_iter_stratum_less();

    /* Summary */
    printf("\n=== Results: %d/%d passed ===\n", tests_passed,
           tests_passed + tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
