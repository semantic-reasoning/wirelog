/*
 * test_frontier_skip.c - Fine-grained frontier iteration skip tests (Phase 3D-Ext-001)
 *
 * Tests frontier skip semantics: skip stratum evaluation only when
 * current_iteration > frontier.iteration AND frontier.stratum < current_stratum.
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
/* FRONTIER SKIP HELPER FUNCTION */
/* ======================================================================== */

/**
 * col_eval_stratum_should_skip_stratum - Determine if stratum should be skipped
 *
 * Returns true if current_iteration > frontier.iteration AND frontier.stratum < current_stratum.
 * Both conditions must be met to skip: iteration must advance beyond frontier AND
 * current stratum must be beyond frontier's stratum level.
 */
static inline bool
col_eval_stratum_should_skip_stratum(col_frontier_t frontier,
                                     uint32_t current_iteration,
                                     uint32_t current_stratum)
{
    return (current_iteration > frontier.iteration)
           && (frontier.stratum < current_stratum);
}

/* ======================================================================== */
/* TEST SUITE: Fine-Grained Frontier Skip Logic */
/* ======================================================================== */

static void
test_skip_false_when_iteration_equals_frontier(void)
{
    TEST("skip_false_when_iteration_equals_frontier");

    col_frontier_t frontier = { 2, 1 }; /* Frontier at (2, 1) */
    uint32_t current_iteration = 2;     /* Current = frontier iteration */
    uint32_t current_stratum = 2;       /* Current stratum > frontier stratum */

    bool should_skip = col_eval_stratum_should_skip_stratum(
        frontier, current_iteration, current_stratum);

    if (!should_skip) {
        PASS; /* Should NOT skip when iteration == frontier.iteration */
    } else {
        FAIL("Expected should_skip=false when iteration == frontier.iteration");
    }
}

static void
test_skip_false_when_iteration_before_frontier(void)
{
    TEST("skip_false_when_iteration_before_frontier");

    col_frontier_t frontier = { 2, 1 }; /* Frontier at (2, 1) */
    uint32_t current_iteration = 1;     /* Current < frontier iteration */
    uint32_t current_stratum = 2;

    bool should_skip = col_eval_stratum_should_skip_stratum(
        frontier, current_iteration, current_stratum);

    if (!should_skip) {
        PASS; /* Should NOT skip when iteration < frontier.iteration */
    } else {
        FAIL("Expected should_skip=false when iteration < frontier.iteration");
    }
}

static void
test_skip_false_when_stratum_lte_frontier_stratum(void)
{
    TEST("skip_false_when_stratum_lte_frontier_stratum");

    col_frontier_t frontier = { 3, 1 }; /* Frontier at (3, 1) */
    uint32_t current_iteration = 4;     /* Current > frontier iteration */
    uint32_t current_stratum = 1; /* Current stratum == frontier stratum */

    bool should_skip = col_eval_stratum_should_skip_stratum(
        frontier, current_iteration, current_stratum);

    if (!should_skip) {
        PASS; /* Should NOT skip when stratum <= frontier.stratum */
    } else {
        FAIL("Expected should_skip=false when stratum <= frontier.stratum");
    }
}

static void
test_skip_true_when_iteration_greater_and_stratum_gt_frontier(void)
{
    TEST("skip_true_when_iteration_greater_and_stratum_gt_frontier");

    col_frontier_t frontier = { 3, 1 }; /* Frontier at (3, 1) */
    uint32_t current_iteration = 4;     /* Current > frontier iteration */
    uint32_t current_stratum = 2;       /* Current > frontier stratum */

    bool should_skip = col_eval_stratum_should_skip_stratum(
        frontier, current_iteration, current_stratum);

    if (should_skip) {
        PASS; /* Should skip when both conditions met */
    } else {
        FAIL("Expected should_skip=true when iteration > and stratum > "
             "frontier");
    }
}

static void
test_skip_false_for_first_stratum(void)
{
    TEST("skip_false_for_first_stratum");

    col_frontier_t frontier = { 3, 1 }; /* Frontier at (3, 1) */
    uint32_t current_iteration = 4;     /* Current > frontier iteration */
    uint32_t current_stratum = 0;       /* Stratum 0 (first/entry stratum) */

    bool should_skip = col_eval_stratum_should_skip_stratum(
        frontier, current_iteration, current_stratum);

    if (!should_skip) {
        PASS; /* Should NOT skip first stratum (recursion entry) */
    } else {
        FAIL("Expected should_skip=false for first stratum (recursion entry)");
    }
}

static void
test_multi_stratum_progression(void)
{
    TEST("multi_stratum_progression");

    col_frontier_t frontier = { 3, 1 }; /* Frontier at (3, 1) */

    /* At iteration 4:
     * - Stratum 0: should NOT skip (first stratum, recursion entry)
     * - Stratum 1: should NOT skip (stratum == frontier.stratum)
     * - Stratum 2: should skip (stratum > frontier.stratum)
     * - Stratum 3: should skip (stratum > frontier.stratum)
     */

    bool skip_stratum0 = col_eval_stratum_should_skip_stratum(frontier, 4, 0);
    bool skip_stratum1 = col_eval_stratum_should_skip_stratum(frontier, 4, 1);
    bool skip_stratum2 = col_eval_stratum_should_skip_stratum(frontier, 4, 2);
    bool skip_stratum3 = col_eval_stratum_should_skip_stratum(frontier, 4, 3);

    if (!skip_stratum0 && !skip_stratum1 && skip_stratum2 && skip_stratum3) {
        PASS; /* Correct progression: s0/s1 eval, s2+ skip */
    } else {
        FAIL("Expected progression: !s0, !s1, s2, s3");
    }
}

/* ======================================================================== */
/* MAIN TEST RUNNER */
/* ======================================================================== */

int
main(void)
{
    printf("\n=== Fine-Grained Frontier Skip Tests (Phase 3D-Ext-001) ===\n\n");

    /* Test fine-grained frontier skip logic */
    test_skip_false_when_iteration_equals_frontier();
    test_skip_false_when_iteration_before_frontier();
    test_skip_false_when_stratum_lte_frontier_stratum();
    test_skip_true_when_iteration_greater_and_stratum_gt_frontier();
    test_skip_false_for_first_stratum();
    test_multi_stratum_progression();

    /* Summary */
    printf("\n=== Results: %d/%d passed ===\n", tests_passed,
           tests_passed + tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
