/*
 * tests/test_diff_arrangement.c - Unit tests for differential arrangement structures
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Unit tests for the col_diff_arrangement_t delta-aware hash table implementation.
 */

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Include the headers we need */
#include "wirelog/columnar/diff_arrangement.h"

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

/* Test 1: col_diff_arrangement_create */
static void
test_diff_arrangement_create(void)
{
    TEST("col_diff_arrangement_create");

    uint32_t key_cols[] = {0, 1};
    col_diff_arrangement_t *darr =
        col_diff_arrangement_create(key_cols, 2, 0);

    if (!darr) {
        FAIL("create returned NULL");
        return;
    }

    col_diff_arrangement_destroy(darr);
    PASS;
}

/* Test 2: col_diff_arrangement_has_delta */
static void
test_diff_arrangement_has_delta(void)
{
    TEST("col_diff_arrangement_has_delta");

    uint32_t key_cols[] = {0};
    col_diff_arrangement_t *darr =
        col_diff_arrangement_create(key_cols, 1, 0);

    if (!darr) {
        FAIL("create failed");
        return;
    }

    if (col_diff_arrangement_has_delta(darr)) {
        FAIL("new arrangement should have no delta");
        col_diff_arrangement_destroy(darr);
        return;
    }

    col_diff_arrangement_destroy(darr);
    PASS;
}

/* Test 3: col_diff_arrangement_get_delta_range */
static void
test_diff_arrangement_get_delta_range(void)
{
    TEST("col_diff_arrangement_get_delta_range");

    uint32_t key_cols[] = {0};
    col_diff_arrangement_t *darr =
        col_diff_arrangement_create(key_cols, 1, 0);

    if (!darr) {
        FAIL("create failed");
        return;
    }

    uint32_t base, current;
    col_diff_arrangement_get_delta_range(darr, &base, &current);

    if (base != 0 || current != 0) {
        FAIL("new arrangement should have range [0,0)");
        col_diff_arrangement_destroy(darr);
        return;
    }

    col_diff_arrangement_destroy(darr);
    PASS;
}

/* Test 4: col_diff_arrangement_deep_copy */
static void
test_diff_arrangement_deep_copy(void)
{
    TEST("col_diff_arrangement_deep_copy");

    uint32_t key_cols[] = {0, 1};
    col_diff_arrangement_t *darr =
        col_diff_arrangement_create(key_cols, 2, 0);
    col_diff_arrangement_t *copy = col_diff_arrangement_deep_copy(darr);

    if (!copy) {
        FAIL("deep_copy returned NULL");
        col_diff_arrangement_destroy(darr);
        return;
    }

    if (col_diff_arrangement_has_delta(copy)) {
        FAIL("copy should match original");
        col_diff_arrangement_destroy(darr);
        col_diff_arrangement_destroy(copy);
        return;
    }

    col_diff_arrangement_destroy(darr);
    col_diff_arrangement_destroy(copy);
    PASS;
}

/* Test 5: col_diff_arrangement_reset_delta */
static void
test_diff_arrangement_reset_delta(void)
{
    TEST("col_diff_arrangement_reset_delta");

    uint32_t key_cols[] = {0};
    col_diff_arrangement_t *darr =
        col_diff_arrangement_create(key_cols, 1, 0);

    if (!darr) {
        FAIL("create failed");
        return;
    }

    col_diff_arrangement_reset_delta(darr);

    uint32_t base, current;
    col_diff_arrangement_get_delta_range(darr, &base, &current);

    if (base != current) {
        FAIL("reset_delta should align base and current");
        col_diff_arrangement_destroy(darr);
        return;
    }

    col_diff_arrangement_destroy(darr);
    PASS;
}

int
main(void)
{
    printf("\nTesting Differential Arrangement Structures\n");
    printf("==========================================\n\n");

    test_diff_arrangement_create();
    test_diff_arrangement_has_delta();
    test_diff_arrangement_get_delta_range();
    test_diff_arrangement_deep_copy();
    test_diff_arrangement_reset_delta();

    printf("\n==========================================\n");
    printf("Results: %d/%d passed\n", pass_count, test_count);

    if (fail_count == 0) {
        printf("All tests passed!\n");
        printf("==========================================\n\n");
        return 0;
    }

    printf("FAILED: %d test(s)\n", fail_count);
    printf("==========================================\n\n");
    return 1;
}
