/*
 * tests/test_diff_trace.c - Unit tests for differential trace structures
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Unit tests for the col_diff_trace_t lattice timestamp implementation.
 */

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Include the headers we need */
#include "wirelog/columnar/diff_trace.h"

/* Test framework */
static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                      \
        do {                                                \
            printf("TEST %d: %s ... ", ++test_count, name); \
            fflush(stdout);                                 \
        } while (0)

#define PASS                               \
        do {                                   \
            printf("PASS\n");                  \
            pass_count++;                      \
        } while (0)

#define FAIL(msg)                          \
        do {                                   \
            printf("FAIL: %s\n", msg);         \
            fail_count++;                      \
        } while (0)

/* Test 1: col_diff_trace_init */
static void
test_diff_trace_init(void)
{
    TEST("col_diff_trace_init");

    col_diff_trace_t trace = col_diff_trace_init(1, 5, 0);

    if (trace.outer_epoch != 1) {
        FAIL("epoch not 1");
        return;
    }
    if (trace.iteration != 5) {
        FAIL("iteration not 5");
        return;
    }
    if (trace.worker != 0) {
        FAIL("worker not 0");
        return;
    }

    PASS;
}

/* Test 2: col_diff_trace_compare */
static void
test_diff_trace_compare(void)
{
    TEST("col_diff_trace_compare");

    col_diff_trace_t t1 = col_diff_trace_init(1, 3, 0);
    col_diff_trace_t t2 = col_diff_trace_init(1, 5, 0);
    col_diff_trace_t t3 = col_diff_trace_init(2, 0, 0);

    if (col_diff_trace_compare(&t1, &t2) >= 0) {
        FAIL("t1 should be < t2");
        return;
    }
    if (col_diff_trace_compare(&t2, &t1) <= 0) {
        FAIL("t2 should be > t1");
        return;
    }
    if (col_diff_trace_compare(&t2, &t3) >= 0) {
        FAIL("t2 (epoch 1) should be < t3 (epoch 2)");
        return;
    }

    PASS;
}

/* Test 3: col_diff_trace_join */
static void
test_diff_trace_join(void)
{
    TEST("col_diff_trace_join");

    col_diff_trace_t t1 = col_diff_trace_init(1, 3, 0);
    col_diff_trace_t t2 = col_diff_trace_init(1, 5, 0);

    col_diff_trace_t result = col_diff_trace_join(&t1, &t2);

    if (result.outer_epoch != 1 || result.iteration != 5) {
        FAIL("join should return max");
        return;
    }

    PASS;
}

/* Test 4: col_diff_trace_has_converged */
static void
test_diff_trace_has_converged(void)
{
    TEST("col_diff_trace_has_converged");

    col_diff_trace_t converged = col_diff_trace_init(1, 5, 0);
    col_diff_trace_t not_converged = col_diff_trace_init(1, UINT32_MAX, 0);

    if (!col_diff_trace_has_converged(&converged)) {
        FAIL("converged trace should return true");
        return;
    }
    if (col_diff_trace_has_converged(&not_converged)) {
        FAIL("not_converged trace should return false");
        return;
    }

    PASS;
}

/* Test 5: col_diff_trace_reset_for_epoch */
static void
test_diff_trace_reset_for_epoch(void)
{
    TEST("col_diff_trace_reset_for_epoch");

    col_diff_trace_t trace = col_diff_trace_init(1, 5, 0);
    col_diff_trace_reset_for_epoch(&trace, 2);

    if (trace.outer_epoch != 2) {
        FAIL("epoch should be 2");
        return;
    }
    if (trace.iteration != UINT32_MAX) {
        FAIL("iteration should be UINT32_MAX");
        return;
    }

    PASS;
}

/* Test 6: col_diff_trace_t size */
static void
test_diff_trace_size(void)
{
    TEST("col_diff_trace_t size");

    if (sizeof(col_diff_trace_t) != 16) {
        printf("FAIL: size is %zu, expected 16\n",
            sizeof(col_diff_trace_t));
        fail_count++;
        return;
    }

    PASS;
}

int
main(void)
{
    printf("\nTesting Differential Trace Structures\n");
    printf("======================================\n\n");

    test_diff_trace_init();
    test_diff_trace_compare();
    test_diff_trace_join();
    test_diff_trace_has_converged();
    test_diff_trace_reset_for_epoch();
    test_diff_trace_size();

    printf("\n======================================\n");
    printf("Results: %d/%d passed\n", pass_count, test_count);

    if (fail_count == 0) {
        printf("All tests passed!\n");
        printf("======================================\n\n");
        return 0;
    }

    printf("FAILED: %d test(s)\n", fail_count);
    printf("======================================\n\n");
    return 1;
}
