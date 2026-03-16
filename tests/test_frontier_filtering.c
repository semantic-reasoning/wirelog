/*
 * test_frontier_filtering.c - Frontier-based iteration filtering tests (Phase 3D-001)
 *
 * Tests frontier boundary check: skip stratum evaluation when all relations
 * have been processed (iteration > frontier.iteration).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

/* Import frontier types */
#include "../wirelog/columnar/columnar_nanoarrow.h"

/* ======================================================================== */
/* TEST HARNESS MACROS */
/* ======================================================================== */

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

/* ======================================================================== */
/* FRONTIER FILTERING HELPER FUNCTION */
/* ======================================================================== */

/**
 * col_eval_stratum_should_skip_iteration - Determine if iteration should be skipped
 *
 * Returns true if current_iteration > frontier.iteration, meaning all relations
 * have been processed up to frontier, so this iteration adds no new data.
 */
static inline bool
col_eval_stratum_should_skip_iteration(col_frontier_t frontier,
                                       uint32_t current_iteration)
{
    return current_iteration > frontier.iteration;
}

/* ======================================================================== */
/* TEST SUITE: Frontier Filtering Logic */
/* ======================================================================== */

static void
test_skip_iteration_false_when_iteration_equals_frontier_iteration(void)
{
    TEST("skip_iteration_false_when_iteration_equals_frontier_iteration");

    col_frontier_t frontier = { 2, 0 }; /* Frontier at iteration 2 */
    uint32_t current_iteration = 2;     /* Current = frontier iteration */

    bool should_skip
        = col_eval_stratum_should_skip_iteration(frontier, current_iteration);

    if (!should_skip) {
        PASS; /* Should NOT skip when equal */
    } else {
        FAIL("Expected should_skip=false when iteration == frontier.iteration");
    }
}

static void
test_skip_iteration_true_when_iteration_greater_than_frontier_iteration(void)
{
    TEST("skip_iteration_true_when_iteration_greater_than_frontier_iteration");

    col_frontier_t frontier = { 2, 0 }; /* Frontier at iteration 2 */
    uint32_t current_iteration = 3;     /* Current > frontier iteration */

    bool should_skip
        = col_eval_stratum_should_skip_iteration(frontier, current_iteration);

    if (should_skip) {
        PASS; /* Should skip when current > frontier */
    } else {
        FAIL("Expected should_skip=true when iteration > frontier.iteration");
    }
}

static void
test_skip_iteration_false_when_iteration_before_frontier(void)
{
    TEST("skip_iteration_false_when_iteration_before_frontier");

    col_frontier_t frontier = { 2, 0 }; /* Frontier at iteration 2 */
    uint32_t current_iteration = 1;     /* Current < frontier iteration */

    bool should_skip
        = col_eval_stratum_should_skip_iteration(frontier, current_iteration);

    if (!should_skip) {
        PASS; /* Should NOT skip when current < frontier */
    } else {
        FAIL("Expected should_skip=false when iteration < frontier.iteration");
    }
}

static void
test_multi_stratum_skip_only_if_all_relations_before_frontier(void)
{
    TEST("multi_stratum_skip_only_if_all_relations_before_frontier");

    /* Frontier at (iteration=3, stratum=1) means:
     * - All of iteration 0, 1, 2 fully processed
     * - Iteration 3 stratum 0 processed
     * - Iteration 3 stratum 1+ not yet processed
     */
    col_frontier_t frontier = { 3, 1 };

    /* At iteration 4: should skip (all previous iterations processed) */
    bool skip_iter4 = col_eval_stratum_should_skip_iteration(frontier, 4);

    /* At iteration 3: should NOT skip (in progress) */
    bool skip_iter3 = col_eval_stratum_should_skip_iteration(frontier, 3);

    if (skip_iter4 && !skip_iter3) {
        PASS; /* Correct: skip iteration 4, but NOT iteration 3 */
    } else {
        FAIL("Expected skip_iter4=true and skip_iter3=false");
    }
}

/* ======================================================================== */
/* MAIN TEST RUNNER */
/* ======================================================================== */

int
main(void)
{
    printf("\n=== Frontier Filtering Tests (Phase 3D-001) ===\n\n");

    /* Test frontier filtering logic */
    test_skip_iteration_false_when_iteration_equals_frontier_iteration();
    test_skip_iteration_true_when_iteration_greater_than_frontier_iteration();
    test_skip_iteration_false_when_iteration_before_frontier();
    test_multi_stratum_skip_only_if_all_relations_before_frontier();

    /* Summary */
    printf("\n=== Results: %d/%d passed ===\n", tests_passed,
           tests_passed + tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
