/*
 * test_mobius_join_weighted.c - TDD RED PHASE
 * Tests for JOIN with multiplicity multiplication (Z-set / Mobius semantics)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * These tests define expected behaviour BEFORE the function is implemented
 * (US-3B-004 RED phase).  Expected failure mode:
 *
 *   undefined reference to `col_op_join_weighted`
 *
 * Semantics under test:
 *   JOIN in Z-set semantics multiplies the multiplicities of matching rows.
 *   (r1 x mult=2) JOIN (r2 x mult=3) on key -> output mult=2*3=6
 *
 * Test cases:
 *   1. r1(k=1, mult=2) JOIN r2(k=1, mult=3) on k -> output mult=6
 *   2. Multiple join keys with different multiplicities
 *   3. Output records have multiplicity = mult_left * mult_right
 */

#define _GNU_SOURCE

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * ArrowSchema stub: replicates the layout of struct ArrowSchema from
 * nanoarrow.h so that col_rel_t has the correct field offsets without
 * pulling in the nanoarrow dependency.
 */
#include "../wirelog/columnar/internal.h"

/*
 * col_op_join_weighted:
 *   Join `lhs` and `rhs` on the columns indicated by `key_col` (a zero-based
 *   column index present in both relations).  For each pair of matching rows
 *   the output row is appended to `dst`, and its timestamp.multiplicity is
 *   set to lhs.timestamps[i].multiplicity * rhs.timestamps[j].multiplicity.
 *
 *   lhs        - left input relation with timestamps carrying multiplicities
 *   rhs        - right input relation with timestamps carrying multiplicities
 *   key_col    - column index used as the equi-join key (must exist in both)
 *   dst        - output relation (caller-allocated, empty on entry); ncols
 *                will be set to lhs->ncols + rhs->ncols (key is duplicated)
 *
 * Returns 0 on success, non-zero on error.
 *
 * RED phase: function does not exist yet -> link error (expected).
 */
int
col_op_join_weighted(const col_rel_t *lhs, const col_rel_t *rhs,
    uint32_t key_col, col_rel_t *dst);

/* ----------------------------------------------------------------
 * Test framework  (matches wirelog convention: test_workqueue.c)
 * ---------------------------------------------------------------- */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                      \
        do {                                                \
            test_count++;                                   \
            printf("TEST %d: %s ... ", test_count, (name)); \
        } while (0)

#define PASS()            \
        do {                  \
            pass_count++;     \
            printf("PASS\n"); \
        } while (0)

#define FAIL(msg)                    \
        do {                             \
            fail_count++;                \
            printf("FAIL: %s\n", (msg)); \
            return;                      \
        } while (0)

#define ASSERT(cond, msg) \
        do {                  \
            if (!(cond))      \
            FAIL(msg);    \
        } while (0)

/* ----------------------------------------------------------------
 * Helper: allocate col_rel_t with ncols columns, no rows, no timestamps.
 * ---------------------------------------------------------------- */
static col_rel_t *
test_rel_alloc(uint32_t ncols)
{
    col_rel_t *r = (col_rel_t *)calloc(1, sizeof(col_rel_t));
    if (!r)
        return NULL;
    r->ncols = ncols;
    if (ncols > 0) {
        r->col_names = (char **)calloc(ncols, sizeof(char *));
        if (!r->col_names) {
            free(r);
            return NULL;
        }
        for (uint32_t i = 0; i < ncols; i++) {
            char buf[16];
            snprintf(buf, sizeof(buf), "col%u", i);
            r->col_names[i] = strdup(buf);
            if (!r->col_names[i]) {
                for (uint32_t j = 0; j < i; j++)
                    free(r->col_names[j]);
                free((void *)r->col_names);
                free(r);
                return NULL;
            }
        }
    }
    return r;
}

static int test_row_match(const col_rel_t *r, uint32_t row,
    const int64_t *target)
{
    for (uint32_t c = 0; c < r->ncols;
        c++) if (col_rel_get(r, row, c) != target[c]) return 0; return 1;
}
static int test_flat_cmp(const col_rel_t *a, const col_rel_t *b)
{
    if (a->nrows != b->nrows || a->ncols != b->ncols) return 1;
    for (uint32_t i = 0; i < a->nrows; i++) for (uint32_t c = 0; c < a->ncols;
            c++)
            if (col_rel_get(a, i, c) != col_rel_get(b, i, c)) return 1;
    return 0;
}

/* ----------------------------------------------------------------
 * Helper: free col_rel_t (data, timestamps, col_names, struct).
 * ---------------------------------------------------------------- */
static void
test_rel_free(col_rel_t *r)
{
    if (!r)
        return;
    free(r->name);
    col_columns_free(r->columns, r->ncols);
    free(r->row_scratch);
    free(r->timestamps);
    if (r->col_names) {
        for (uint32_t i = 0; i < r->ncols; i++)
            free(r->col_names[i]);
        free((void *)r->col_names);
    }
    free(r);
}

/* ----------------------------------------------------------------
 * Helper: append one row + multiplicity, growing buffers as needed.
 * Returns 0 on success, -1 on ENOMEM.
 * ---------------------------------------------------------------- */
static int
test_rel_append_row_mult(col_rel_t *r, const int64_t *row, int64_t multiplicity)
{
    if (r->nrows >= r->capacity) {
        uint32_t cap = r->capacity == 0 ? 16 : r->capacity * 2;
        if (r->columns) {
            if (col_columns_realloc(r->columns, r->ncols, cap) != 0)
                return -1;
        } else {
            r->columns = col_columns_alloc(r->ncols, cap);
            if (!r->columns) return -1;
        }

        col_delta_timestamp_t *nt = (col_delta_timestamp_t *)realloc(
            r->timestamps, (size_t)cap * sizeof(col_delta_timestamp_t));
        if (!nt)
            return -1;
        r->timestamps = nt;
        r->capacity = cap;
    }
    if (r->ncols > 0)
        col_rel_row_copy_in(r, r->nrows, row);
    col_delta_timestamp_t ts;
    memset(&ts, 0, sizeof(ts));
    ts.multiplicity = multiplicity;
    r->timestamps[r->nrows] = ts;
    r->nrows++;
    return 0;
}

/* ================================================================
 * Test 1: Simple equi-join: r1(k=1, mult=2) JOIN r2(k=1, mult=3)
 *         -> one output row with mult = 2 * 3 = 6
 *
 * lhs: 1 row, data=(1), multiplicity=2   [key col 0 = 1]
 * rhs: 1 row, data=(1), multiplicity=3   [key col 0 = 1]
 *
 * Expected:
 *   dst->nrows == 1                   (exactly one matching pair)
 *   dst->timestamps[0].multiplicity == 6  (2 * 3)
 * ================================================================ */
static void
test_simple_join_mult_multiply(void)
{
    TEST("r1(k=1,mult=2) JOIN r2(k=1,mult=3) on k -> output mult=6");

    col_rel_t *lhs = test_rel_alloc(1);
    col_rel_t *rhs = test_rel_alloc(1);
    col_rel_t *dst = test_rel_alloc(0); /* ncols set by join */
    ASSERT(lhs && rhs && dst, "test_rel_alloc failed");

    int64_t lrow[] = { 1 };
    int64_t rrow[] = { 1 };
    ASSERT(test_rel_append_row_mult(lhs, lrow, 2) == 0,
        "append lhs row mult=2");
    ASSERT(test_rel_append_row_mult(rhs, rrow, 3) == 0,
        "append rhs row mult=3");

    int rc = col_op_join_weighted(lhs, rhs, 0, dst);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(dst->nrows == 1, "dst->nrows == 1 (one matching pair)");
    ASSERT(dst->timestamps != NULL, "dst->timestamps is non-NULL");
    ASSERT(dst->timestamps[0].multiplicity == 6, "output mult == 6 (2 * 3)");

    test_rel_free(lhs);
    test_rel_free(rhs);
    test_rel_free(dst);
    PASS();
}

/* ================================================================
 * Test 2: Multiple join keys with different multiplicities
 *
 * lhs rows: (k=1, v=10, mult=2), (k=2, v=20, mult=5)
 * rhs rows: (k=1, w=100, mult=3), (k=2, w=200, mult=4)
 *
 * Join on col 0 (k):
 *   k=1 pair -> mult = 2 * 3 = 6
 *   k=2 pair -> mult = 5 * 4 = 20
 *
 * Expected:
 *   dst->nrows == 2
 *   multiplicities are {6, 20} in some order (sort by key for determinism)
 * ================================================================ */
static void
test_multiple_keys_different_mults(void)
{
    TEST("multiple join keys: k=1 (2*3=6), k=2 (5*4=20)");

    col_rel_t *lhs = test_rel_alloc(2); /* cols: k, v */
    col_rel_t *rhs = test_rel_alloc(2); /* cols: k, w */
    col_rel_t *dst = test_rel_alloc(0);
    ASSERT(lhs && rhs && dst, "test_rel_alloc failed");

    int64_t l0[] = { 1, 10 };
    int64_t l1[] = { 2, 20 };
    int64_t r0[] = { 1, 100 };
    int64_t r1[] = { 2, 200 };
    ASSERT(test_rel_append_row_mult(lhs, l0, 2) == 0, "append lhs k=1 mult=2");
    ASSERT(test_rel_append_row_mult(lhs, l1, 5) == 0, "append lhs k=2 mult=5");
    ASSERT(test_rel_append_row_mult(rhs, r0, 3) == 0, "append rhs k=1 mult=3");
    ASSERT(test_rel_append_row_mult(rhs, r1, 4) == 0, "append rhs k=2 mult=4");

    int rc = col_op_join_weighted(lhs, rhs, 0, dst);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(dst->nrows == 2, "dst->nrows == 2 (two matching pairs)");
    ASSERT(dst->timestamps != NULL, "dst->timestamps is non-NULL");

    /* Verify both expected multiplicities appear (order may vary). */
    int64_t m0 = dst->timestamps[0].multiplicity;
    int64_t m1 = dst->timestamps[1].multiplicity;
    bool found6 = (m0 == 6 || m1 == 6);
    bool found20 = (m0 == 20 || m1 == 20);
    ASSERT(found6, "output contains a row with mult=6  (2*3)");
    ASSERT(found20, "output contains a row with mult=20 (5*4)");

    test_rel_free(lhs);
    test_rel_free(rhs);
    test_rel_free(dst);
    PASS();
}

/* ================================================================
 * Test 3: Output record multiplicity = mult_left * mult_right
 *
 * Explicitly verify the multiplication rule for three combinations:
 *   lhs(k=7, mult=3) JOIN rhs(k=7, mult=-2) -> mult = 3 * -2 = -6
 *   lhs(k=7, mult=3) JOIN rhs(k=7, mult= 0) -> mult = 3 *  0 =  0
 *   lhs(k=7, mult=1) JOIN rhs(k=7, mult= 1) -> mult = 1 *  1 =  1
 *
 * Each sub-relation has a single row with k=7 so joins are 1:1.
 * ================================================================ */
static void
test_output_multiplicity_is_product(void)
{
    TEST(
        "output multiplicity = mult_left * mult_right (3*-2=-6, 3*0=0, 1*1=1)");

    /* Sub-test A: mult=3 * mult=-2 -> -6 */
    {
        col_rel_t *lhs = test_rel_alloc(1);
        col_rel_t *rhs = test_rel_alloc(1);
        col_rel_t *dst = test_rel_alloc(0);
        ASSERT(lhs && rhs && dst, "test_rel_alloc failed (A)");

        int64_t lrow[] = { 7 };
        int64_t rrow[] = { 7 };
        ASSERT(test_rel_append_row_mult(lhs, lrow, 3) == 0,
            "append lhs mult= 3");
        ASSERT(test_rel_append_row_mult(rhs, rrow, -2) == 0,
            "append rhs mult=-2");

        int rc = col_op_join_weighted(lhs, rhs, 0, dst);
        ASSERT(rc == 0, "returns 0 (A)");
        ASSERT(dst->nrows == 1, "dst->nrows == 1 (A)");
        ASSERT(dst->timestamps != NULL, "dst->timestamps non-NULL (A)");
        ASSERT(dst->timestamps[0].multiplicity == -6,
            "output mult == -6 (3 * -2)");

        test_rel_free(lhs);
        test_rel_free(rhs);
        test_rel_free(dst);
    }

    /* Sub-test B: mult=3 * mult=0 -> 0 */
    {
        col_rel_t *lhs = test_rel_alloc(1);
        col_rel_t *rhs = test_rel_alloc(1);
        col_rel_t *dst = test_rel_alloc(0);
        ASSERT(lhs && rhs && dst, "test_rel_alloc failed (B)");

        int64_t lrow[] = { 7 };
        int64_t rrow[] = { 7 };
        ASSERT(test_rel_append_row_mult(lhs, lrow, 3) == 0,
            "append lhs mult=3");
        ASSERT(test_rel_append_row_mult(rhs, rrow, 0) == 0,
            "append rhs mult=0");

        int rc = col_op_join_weighted(lhs, rhs, 0, dst);
        ASSERT(rc == 0, "returns 0 (B)");
        ASSERT(dst->nrows == 1, "dst->nrows == 1 (B)");
        ASSERT(dst->timestamps != NULL, "dst->timestamps non-NULL (B)");
        ASSERT(dst->timestamps[0].multiplicity == 0,
            "output mult == 0 (3 * 0)");

        test_rel_free(lhs);
        test_rel_free(rhs);
        test_rel_free(dst);
    }

    /* Sub-test C: mult=1 * mult=1 -> 1 */
    {
        col_rel_t *lhs = test_rel_alloc(1);
        col_rel_t *rhs = test_rel_alloc(1);
        col_rel_t *dst = test_rel_alloc(0);
        ASSERT(lhs && rhs && dst, "test_rel_alloc failed (C)");

        int64_t lrow[] = { 7 };
        int64_t rrow[] = { 7 };
        ASSERT(test_rel_append_row_mult(lhs, lrow, 1) == 0,
            "append lhs mult=1");
        ASSERT(test_rel_append_row_mult(rhs, rrow, 1) == 0,
            "append rhs mult=1");

        int rc = col_op_join_weighted(lhs, rhs, 0, dst);
        ASSERT(rc == 0, "returns 0 (C)");
        ASSERT(dst->nrows == 1, "dst->nrows == 1 (C)");
        ASSERT(dst->timestamps != NULL, "dst->timestamps non-NULL (C)");
        ASSERT(dst->timestamps[0].multiplicity == 1,
            "output mult == 1 (1 * 1)");

        test_rel_free(lhs);
        test_rel_free(rhs);
        test_rel_free(dst);
    }

    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */
int
main(void)
{
    printf("=== test_mobius_join_weighted (TDD RED PHASE) ===\n\n");
    printf("NOTE: Expected to FAIL at link time until col_op_join_weighted\n");
    printf("      is implemented with extern linkage.\n\n");

    test_simple_join_mult_multiply();
    test_multiple_keys_different_mults();
    test_output_multiplicity_is_product();

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n", pass_count,
        fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
