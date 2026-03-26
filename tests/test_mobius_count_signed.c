/*
 * test_mobius_count_signed.c - TDD RED PHASE
 * Tests for COUNT with signed multiplicities (Z-set / Mobius semantics)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * These tests define expected behaviour BEFORE the function is implemented
 * (US-3B-002 RED phase).  Expected failure mode:
 *
 *   undefined reference to `col_op_reduce_weighted`
 *
 * Semantics under test:
 *   COUNT aggregation in Z-set semantics = sum of input multiplicities,
 *   NOT the number of physical rows.
 *
 * Test cases:
 *   1. Single row mult=2  -> COUNT result value = 2
 *   2. Two rows mult=2, mult=-1 -> COUNT result value = 1
 *   3. Output row multiplicity = sum of input multiplicities = 1
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
 * col_op_reduce_weighted:
 *   Aggregate `src` using COUNT semantics over Z-set multiplicities.
 *   The result relation `dst` receives one output row per group (empty
 *   group key for a global COUNT) whose data column holds the sum of
 *   all input multiplicities, and whose timestamp.multiplicity equals
 *   the same sum.
 *
 *   src        - input relation; timestamps[i].multiplicity carries the
 *                signed weight of each row
 *   dst        - output relation (caller-allocated, empty on entry)
 *
 * Returns 0 on success, non-zero on error.
 *
 * RED phase: function does not exist yet -> link error (expected).
 */
int
col_op_reduce_weighted(const col_rel_t *src, col_rel_t *dst);

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
 * Test 1: Single row with mult=2 -> COUNT result value = 2
 *
 * Input:  1 row, data=(42), multiplicity=2
 * Expected:
 *   dst->nrows == 1
 *   dst->columns[(0) % dst->ncols][(0) / dst->ncols]  == 2   (COUNT = sum of multiplicities)
 * ================================================================ */
static void
test_single_row_mult2(void)
{
    TEST("single row mult=2 -> COUNT output value = 2");

    col_rel_t *src = test_rel_alloc(1);
    col_rel_t *dst = test_rel_alloc(1);
    ASSERT(src && dst, "test_rel_alloc failed");

    int64_t row[] = { 42 };
    ASSERT(test_rel_append_row_mult(src, row, 2) == 0, "append row mult=2");

    int rc = col_op_reduce_weighted(src, dst);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(dst->nrows == 1, "dst->nrows == 1 (one output row)");
    ASSERT(dst->columns != NULL, "dst->columns is non-NULL");
    ASSERT(dst->columns[(0) % dst->ncols][(0) / dst->ncols] == 2,
        "COUNT value == 2 (sum of multiplicities)");

    test_rel_free(src);
    test_rel_free(dst);
    PASS();
}

/* ================================================================
 * Test 2: Two rows mult=2 and mult=-1 -> COUNT result value = 1
 *
 * Input:  row0=(10), multiplicity=2
 *         row1=(20), multiplicity=-1
 * Expected:
 *   dst->nrows == 1
 *   dst->columns[(0) % dst->ncols][(0) / dst->ncols] == 1   (COUNT = 2 + (-1) = 1)
 * ================================================================ */
static void
test_two_rows_mixed_mult(void)
{
    TEST("two rows mult=2, mult=-1 -> COUNT output value = 1");

    col_rel_t *src = test_rel_alloc(1);
    col_rel_t *dst = test_rel_alloc(1);
    ASSERT(src && dst, "test_rel_alloc failed");

    int64_t r0[] = { 10 };
    int64_t r1[] = { 20 };
    ASSERT(test_rel_append_row_mult(src, r0, 2) == 0, "append row0 mult=2");
    ASSERT(test_rel_append_row_mult(src, r1, -1) == 0, "append row1 mult=-1");

    int rc = col_op_reduce_weighted(src, dst);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(dst->nrows == 1, "dst->nrows == 1 (one aggregate output)");
    ASSERT(dst->columns != NULL, "dst->columns is non-NULL");
    ASSERT(dst->columns[(0) % dst->ncols][(0) / dst->ncols] == 1,
        "COUNT value == 1 (2 + (-1) = 1)");

    test_rel_free(src);
    test_rel_free(dst);
    PASS();
}

/* ================================================================
 * Test 3: Output row multiplicity equals sum of input multiplicities
 *
 * Input:  row0=(10), multiplicity=2
 *         row1=(20), multiplicity=-1
 * Expected:
 *   dst->timestamps[0].multiplicity == 1   (2 + (-1))
 *
 * Rationale: in Z-set semantics the output row itself carries a
 * multiplicity representing "how many times this aggregate fact holds".
 * For a global COUNT the output multiplicity = sum of input weights.
 * ================================================================ */
static void
test_output_row_multiplicity(void)
{
    TEST("output row multiplicity = sum of input multiplicities (2 + -1 = 1)");

    col_rel_t *src = test_rel_alloc(1);
    col_rel_t *dst = test_rel_alloc(1);
    ASSERT(src && dst, "test_rel_alloc failed");

    int64_t r0[] = { 10 };
    int64_t r1[] = { 20 };
    ASSERT(test_rel_append_row_mult(src, r0, 2) == 0, "append row0 mult=2");
    ASSERT(test_rel_append_row_mult(src, r1, -1) == 0, "append row1 mult=-1");

    int rc = col_op_reduce_weighted(src, dst);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(dst->nrows == 1, "dst->nrows == 1");
    ASSERT(dst->timestamps != NULL, "dst->timestamps is non-NULL");
    ASSERT(dst->timestamps[0].multiplicity == 1,
        "output row multiplicity == 1 (sum of inputs)");

    test_rel_free(src);
    test_rel_free(dst);
    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */
int
main(void)
{
    printf("=== test_mobius_count_signed (TDD RED PHASE) ===\n\n");
    printf(
        "NOTE: Expected to FAIL at link time until col_op_reduce_weighted\n");
    printf("      is implemented with extern linkage.\n\n");

    test_single_row_mult2();
    test_two_rows_mixed_mult();
    test_output_row_multiplicity();

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n", pass_count,
        fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
