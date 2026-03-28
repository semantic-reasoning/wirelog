/*
 * test_radix_sort.c - Unit tests for col_rel_radix_sort_int64()
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests: empty relation, single row, already sorted, reverse sorted,
 * duplicates, negatives, multi-column lexicographic order, random data.
 *
 * Issue #308: Radix sort for int64_t relation data
 */

#include "../wirelog/columnar/internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Harness                                                             */
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
/* Helpers                                                                  */
/* ======================================================================== */

/* Build a relation with ncols columns from a flat row-major array. */
static col_rel_t *
make_rel(uint32_t ncols, const int64_t *rows, uint32_t nrows)
{
    col_rel_t *r = col_rel_new_auto("test", ncols);
    if (!r)
        return NULL;
    for (uint32_t i = 0; i < nrows; i++) {
        if (col_rel_append_row(r, rows + (size_t)i * ncols) != 0) {
            col_rel_destroy(r);
            return NULL;
        }
    }
    return r;
}

/* Compare two row-major arrays lexicographically: returns 1 if equal. */
static int
rows_equal(const int64_t *a, const int64_t *b, uint32_t nrows, uint32_t ncols)
{
    return memcmp(a, b, (size_t)nrows * ncols * sizeof(int64_t)) == 0;
}

/* Verify r->columns matches expected rows. */
static int
check_sorted(col_rel_t *r, const int64_t *expected, uint32_t nrows,
    uint32_t ncols, const char *label)
{
    if (r->nrows != nrows) {
        char msg[128];
        snprintf(msg, sizeof(msg), "%s: nrows %u != expected %u", label,
            r->nrows, nrows);
        FAIL(msg);
        return 0;
    }
    if (r->sorted_nrows != nrows) {
        char msg[128];
        snprintf(msg, sizeof(msg), "%s: sorted_nrows %u != nrows %u", label,
            r->sorted_nrows, nrows);
        FAIL(msg);
        return 0;
    }
    /* Gather into flat buffer for comparison */
    int64_t *flat = (int64_t *)malloc((size_t)nrows * ncols * sizeof(int64_t));
    if (flat) {
        for (uint32_t row = 0; row < nrows; row++)
            col_rel_row_copy_out(r, row, flat + (size_t)row * ncols);
    }
    if (!flat || !rows_equal(flat, expected, nrows, ncols)) {
        free(flat);
        FAIL(label);
        /* Print first mismatch for debugging */
        for (uint32_t row = 0; row < nrows; row++) {
            for (uint32_t c = 0; c < ncols; c++) {
                int64_t got = col_rel_get(r, row, c);
                int64_t exp = expected[(size_t)row * ncols + c];
                if (got != exp) {
                    printf("      row %u col %u: got %lld expected %lld\n",
                        row, c, (long long)got, (long long)exp);
                    return 0;
                }
            }
        }
        return 0;
    }
    free(flat);
    return 1;
}

/* ======================================================================== */
/* Test 1: Empty relation                                                   */
/* ======================================================================== */

static int
test_empty(void)
{
    TEST("Empty relation (nrows=0)");

    col_rel_t *r = col_rel_new_auto("t", 2);
    if (!r) {
        FAIL("alloc"); return 1;
    }

    col_rel_radix_sort_int64(r);

    if (r->sorted_nrows != 0) {
        col_rel_destroy(r);
        FAIL("sorted_nrows != 0");
        return 1;
    }

    col_rel_destroy(r);
    PASS();
    return 0;
}

/* ======================================================================== */
/* Test 2: Single row                                                       */
/* ======================================================================== */

static int
test_single_row(void)
{
    TEST("Single row (nrows=1)");

    int64_t row[] = { 42, -7 };
    col_rel_t *r = make_rel(2, row, 1);
    if (!r) {
        FAIL("alloc"); return 1;
    }

    col_rel_radix_sort_int64(r);

    int ok = check_sorted(r, row, 1, 2, "single row unchanged");
    col_rel_destroy(r);
    if (ok) PASS();
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 3: Already sorted (1 column)                                       */
/* ======================================================================== */

static int
test_already_sorted(void)
{
    TEST("Already sorted 1-col relation");

    int64_t rows[] = { 1, 2, 3, 4, 5 };
    col_rel_t *r = make_rel(1, rows, 5);
    if (!r) {
        FAIL("alloc"); return 1;
    }

    col_rel_radix_sort_int64(r);

    int ok = check_sorted(r, rows, 5, 1, "already sorted");
    col_rel_destroy(r);
    if (ok) PASS();
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 4: Reverse sorted (1 column)                                       */
/* ======================================================================== */

static int
test_reverse_sorted(void)
{
    TEST("Reverse sorted 1-col relation");

    int64_t rows[] = { 5, 4, 3, 2, 1 };
    int64_t expected[] = { 1, 2, 3, 4, 5 };
    col_rel_t *r = make_rel(1, rows, 5);
    if (!r) {
        FAIL("alloc"); return 1;
    }

    col_rel_radix_sort_int64(r);

    int ok = check_sorted(r, expected, 5, 1, "reverse sorted");
    col_rel_destroy(r);
    if (ok) PASS();
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 5: Duplicates (stable relative order preserved)                    */
/* ======================================================================== */

static int
test_duplicates(void)
{
    TEST("Duplicates sorted correctly");

    int64_t rows[] = { 3, 1, 2, 1, 3, 2 };
    int64_t expected[] = { 1, 1, 2, 2, 3, 3 };
    col_rel_t *r = make_rel(1, rows, 6);
    if (!r) {
        FAIL("alloc"); return 1;
    }

    col_rel_radix_sort_int64(r);

    int ok = check_sorted(r, expected, 6, 1, "duplicates");
    col_rel_destroy(r);
    if (ok) PASS();
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 6: Negative numbers                                                */
/* ======================================================================== */

static int
test_negatives(void)
{
    TEST("Negative numbers sort correctly");

    int64_t rows[] = { 0, -3, 5, -1, 2, -100 };
    int64_t expected[] = { -100, -3, -1, 0, 2, 5 };
    col_rel_t *r = make_rel(1, rows, 6);
    if (!r) {
        FAIL("alloc"); return 1;
    }

    col_rel_radix_sort_int64(r);

    int ok = check_sorted(r, expected, 6, 1, "negatives");
    col_rel_destroy(r);
    if (ok) PASS();
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 7: INT64_MIN / INT64_MAX boundary values                           */
/* ======================================================================== */

static int
test_boundary_values(void)
{
    TEST("INT64_MIN/MAX boundary values");

    int64_t rows[] = {
        INT64_MAX, 0, INT64_MIN, -1, 1
    };
    int64_t expected[] = {
        INT64_MIN, -1, 0, 1, INT64_MAX
    };
    col_rel_t *r = make_rel(1, rows, 5);
    if (!r) {
        FAIL("alloc"); return 1;
    }

    col_rel_radix_sort_int64(r);

    int ok = check_sorted(r, expected, 5, 1, "boundary values");
    col_rel_destroy(r);
    if (ok) PASS();
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 8: Multi-column lexicographic order (2 cols)                       */
/* ======================================================================== */

static int
test_multi_col(void)
{
    TEST("Multi-column (2 cols) lexicographic order");

    /* Rows: (col0, col1) */
    int64_t rows[] = {
        2, 1,
        1, 3,
        1, 1,
        2, 2,
        1, 2,
    };
    int64_t expected[] = {
        1, 1,
        1, 2,
        1, 3,
        2, 1,
        2, 2,
    };
    col_rel_t *r = make_rel(2, rows, 5);
    if (!r) {
        FAIL("alloc"); return 1;
    }

    col_rel_radix_sort_int64(r);

    int ok = check_sorted(r, expected, 5, 2, "multi-col lex");
    col_rel_destroy(r);
    if (ok) PASS();
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 9: Multi-column with negatives (2 cols)                            */
/* ======================================================================== */

static int
test_multi_col_neg(void)
{
    TEST("Multi-column (2 cols) with negatives");

    int64_t rows[] = {
        -1,  5,
        0, -3,
        -1, -2,
        0,  1,
    };
    int64_t expected[] = {
        -1, -2,
        -1,  5,
        0, -3,
        0,  1,
    };
    col_rel_t *r = make_rel(2, rows, 4);
    if (!r) {
        FAIL("alloc"); return 1;
    }

    col_rel_radix_sort_int64(r);

    int ok = check_sorted(r, expected, 4, 2, "multi-col neg");
    col_rel_destroy(r);
    if (ok) PASS();
    return ok ? 0 : 1;
}

/* ======================================================================== */
/* Test 10: Random 1-col — verify matches qsort result                     */
/* ======================================================================== */

static int
cmp_int64(const void *a, const void *b)
{
    int64_t x = *(const int64_t *)a;
    int64_t y = *(const int64_t *)b;
    return (x > y) - (x < y);
}

static int
test_random_1col(void)
{
    TEST("Random 1-col (256 rows) matches qsort");

#define NRAND 256
    int64_t rows[NRAND];
    /* Deterministic pseudo-random sequence */
    uint64_t state = 0xdeadbeefcafe1234ULL;
    for (int i = 0; i < NRAND; i++) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        rows[i] = (int64_t)state;
    }

    int64_t expected[NRAND];
    memcpy(expected, rows, sizeof(rows));
    qsort(expected, NRAND, sizeof(int64_t), cmp_int64);

    col_rel_t *r = make_rel(1, rows, NRAND);
    if (!r) {
        FAIL("alloc"); return 1;
    }

    col_rel_radix_sort_int64(r);

    int ok = check_sorted(r, expected, NRAND, 1, "random 1-col");
    col_rel_destroy(r);
    if (ok) PASS();
    return ok ? 0 : 1;
#undef NRAND
}

/* ======================================================================== */
/* Test 11: sorted_nrows set correctly                                      */
/* ======================================================================== */

static int
test_sorted_nrows_set(void)
{
    TEST("sorted_nrows set to nrows after sort");

    int64_t rows[] = { 5, 3, 1, 4, 2 };
    col_rel_t *r = make_rel(1, rows, 5);
    if (!r) {
        FAIL("alloc"); return 1;
    }

    if (r->sorted_nrows != 0) {
        col_rel_destroy(r);
        FAIL("sorted_nrows should be 0 before sort");
        return 1;
    }

    col_rel_radix_sort_int64(r);

    if (r->sorted_nrows != r->nrows) {
        char msg[64];
        snprintf(msg, sizeof(msg), "sorted_nrows=%u nrows=%u",
            r->sorted_nrows, r->nrows);
        col_rel_destroy(r);
        FAIL(msg);
        return 1;
    }

    col_rel_destroy(r);
    PASS();
    return 0;
}

/* ======================================================================== */
/* Tests 12-14: Adaptive k=8/k=16 dispatch boundary (Issue #363 Phase 5c)  */
/* ======================================================================== */

/* Generic large-nrows helper: fill with deterministic random int64_t,
 * sort via col_rel_radix_sort_int64, compare against qsort reference. */
static int
test_boundary_nrows(uint32_t nrows, const char *label)
{
    TEST(label);

    int64_t *rows = (int64_t *)malloc(nrows * sizeof(int64_t));
    int64_t *expected = (int64_t *)malloc(nrows * sizeof(int64_t));
    if (!rows || !expected) {
        free(rows);
        free(expected);
        FAIL("alloc");
        return 1;
    }

    /* Deterministic LCG covering the full int64 range (exercises sign pass). */
    uint64_t state = 0xc0ffee12deadULL ^ (uint64_t)nrows;
    for (uint32_t i = 0; i < nrows; i++) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        rows[i] = (int64_t)state;
    }
    memcpy(expected, rows, nrows * sizeof(int64_t));
    qsort(expected, nrows, sizeof(int64_t), cmp_int64);

    col_rel_t *r = make_rel(1, rows, nrows);
    free(rows);
    if (!r) {
        free(expected);
        FAIL("alloc");
        return 1;
    }

    col_rel_radix_sort_int64(r);

    int ok = check_sorted(r, expected, nrows, 1, label);
    col_rel_destroy(r);
    free(expected);
    if (ok)
        PASS();
    return ok ? 0 : 1;
}

/* nrows=49999: just below threshold → k=8 SIMD path */
static int
test_k8_path(void)
{
    return test_boundary_nrows(49999,
               "k=8 path: nrows=49999 (below threshold) matches qsort");
}

/* nrows=50000: exactly at threshold → k=16 path */
static int
test_k16_boundary(void)
{
    return test_boundary_nrows(50000,
               "k=16 path: nrows=50000 (at threshold) matches qsort");
}

/* nrows=50001: just above threshold → k=16 path */
static int
test_k16_path(void)
{
    return test_boundary_nrows(50001,
               "k=16 path: nrows=50001 (above threshold) matches qsort");
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("Radix Sort Tests (Issue #308)\n");
    printf("==============================\n\n");

    test_empty();
    test_single_row();
    test_already_sorted();
    test_reverse_sorted();
    test_duplicates();
    test_negatives();
    test_boundary_values();
    test_multi_col();
    test_multi_col_neg();
    test_random_1col();
    test_sorted_nrows_set();
    test_k8_path();
    test_k16_boundary();
    test_k16_path();

    printf("\n");
    printf("Passed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
