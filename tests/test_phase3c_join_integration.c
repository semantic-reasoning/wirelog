/*
 * test_phase3c_join_integration.c - Phase 3C JOIN integration test (US-3C-001)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates that JOIN via the evaluation pipeline uses Z-set multiplicities:
 *   output multiplicity = mult_left * mult_right
 *
 * This is a TDD RED test.  col_eval_relation_plan currently dispatches JOIN
 * to col_op_join which does NOT multiply multiplicities.  The test exercises
 * col_op_join_weighted (the correct implementation) directly to establish the
 * expected contract.  Once col_eval_relation_plan is updated to call
 * col_op_join_weighted, the pipeline path will satisfy the same invariants.
 *
 * Test cases:
 *   1. Simple JOIN: mult_left=2, mult_right=3 -> output mult=6
 *   2. Negative multiplicities: mult_left=-1, mult_right=2 -> output mult=-2
 *   3. Zero multiplicity: if either input is 0, output mult=0
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
struct ArrowSchema {
    const char *format;
    const char *name;
    const char *metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema **children;
    struct ArrowSchema *dictionary;
    void (*release)(struct ArrowSchema *);
    void *private_data;
};

/*
 * col_delta_timestamp_t - mirrors the public definition in columnar_nanoarrow.h.
 * Must match exactly (4 x uint32_t + int64_t = 24 bytes).
 */
typedef struct {
    uint32_t iteration;
    uint32_t stratum;
    uint32_t worker;
    uint32_t _reserved;
    int64_t multiplicity;
} col_delta_timestamp_t;

/*
 * col_rel_t - mirrors the private definition in columnar_nanoarrow.c.
 * Field order and layout must match the implementation exactly.
 */
typedef struct {
    char *name;                /* owned, null-terminated               */
    uint32_t ncols;            /* columns per tuple (0 = unset)        */
    int64_t *data;             /* owned, row-major int64 buffer        */
    uint32_t nrows;            /* current row count                    */
    uint32_t capacity;         /* allocated row capacity               */
    char **col_names;          /* owned array of ncols owned strings   */
    struct ArrowSchema schema; /* owned Arrow schema (lazy-inited)     */
    bool schema_ok;            /* true after schema is initialised     */
    uint32_t sorted_nrows;     /* sorted prefix row count (issue #94)   */
    int64_t *merge_buf;        /* persistent merge buffer (issue #94)   */
    uint32_t merge_buf_cap;    /* merge buffer capacity in rows         */
    uint32_t base_nrows;       /* base row count for delta prop (#83)   */
    col_delta_timestamp_t
        *timestamps; /* NULL when not tracking               */
} col_rel_t;

/* col_op_join_weighted declaration (defined in columnar_nanoarrow.c) */
int
col_op_join_weighted(const col_rel_t *lhs, const col_rel_t *rhs,
                     uint32_t key_col, col_rel_t *dst);

/* ----------------------------------------------------------------
 * Test framework  (matches wirelog convention)
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
 * Returns NULL on allocation failure.
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

/* ----------------------------------------------------------------
 * Helper: free col_rel_t (data, timestamps, col_names, struct).
 * ---------------------------------------------------------------- */
static void
test_rel_free(col_rel_t *r)
{
    if (!r)
        return;
    free(r->name);
    free(r->data);
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
        int64_t *nd = (int64_t *)realloc(r->data, (size_t)cap * r->ncols
                                                      * sizeof(int64_t));
        if (!nd)
            return -1;
        r->data = nd;

        col_delta_timestamp_t *nt = (col_delta_timestamp_t *)realloc(
            r->timestamps, (size_t)cap * sizeof(col_delta_timestamp_t));
        if (!nt)
            return -1;
        r->timestamps = nt;
        r->capacity = cap;
    }
    if (r->ncols > 0)
        memcpy(r->data + (size_t)r->nrows * r->ncols, row,
               r->ncols * sizeof(int64_t));
    col_delta_timestamp_t ts;
    memset(&ts, 0, sizeof(ts));
    ts.multiplicity = multiplicity;
    r->timestamps[r->nrows] = ts;
    r->nrows++;
    return 0;
}

/* ================================================================
 * Test 1: Simple JOIN - mult_left=2, mult_right=3 -> output mult=6
 *
 * Construct two relations joined on col0 (key=10):
 *   lhs: (key=10, mult=2)
 *   rhs: (key=10, mult=3)
 *
 * col_op_join_weighted must produce one output row with mult = 2*3 = 6.
 *
 * TDD context: col_eval_relation_plan dispatches to col_op_join which does
 * NOT multiply multiplicities (produces mult=0).  This test documents the
 * required contract for the pipeline JOIN operation.
 * ================================================================ */
static void
test_join_simple_multiplicity_product(void)
{
    TEST("JOIN plan: mult_left=2 * mult_right=3 -> output mult=6");

    col_rel_t *lhs = test_rel_alloc(1); /* col0 = key */
    col_rel_t *rhs = test_rel_alloc(1); /* col0 = key */
    col_rel_t *dst = test_rel_alloc(0); /* ncols set by join */
    ASSERT(lhs && rhs && dst, "test_rel_alloc failed");

    int64_t lrow[] = { 10 };
    int64_t rrow[] = { 10 };
    ASSERT(test_rel_append_row_mult(lhs, lrow, 2) == 0,
           "append lhs key=10 mult=2");
    ASSERT(test_rel_append_row_mult(rhs, rrow, 3) == 0,
           "append rhs key=10 mult=3");

    int rc = col_op_join_weighted(lhs, rhs, 0, dst);

    ASSERT(rc == 0, "col_op_join_weighted returns 0 on success");
    ASSERT(dst->nrows == 1, "join produces exactly 1 output row");
    ASSERT(dst->timestamps != NULL, "output timestamps non-NULL");
    ASSERT(dst->timestamps[0].multiplicity == 6, "output mult = 6 (2 * 3)");

    test_rel_free(lhs);
    test_rel_free(rhs);
    test_rel_free(dst);
    PASS();
}

/* ================================================================
 * Test 2: Negative multiplicity - mult_left=-1, mult_right=2 -> output mult=-2
 *
 * lhs: (key=5, mult=-1)   [retraction]
 * rhs: (key=5, mult=2)    [positive weight]
 *
 * Expected: output mult = -1 * 2 = -2 (retraction propagates through join).
 * ================================================================ */
static void
test_join_negative_multiplicity(void)
{
    TEST("JOIN plan: mult_left=-1 * mult_right=2 -> output mult=-2");

    col_rel_t *lhs = test_rel_alloc(1);
    col_rel_t *rhs = test_rel_alloc(1);
    col_rel_t *dst = test_rel_alloc(0);
    ASSERT(lhs && rhs && dst, "test_rel_alloc failed");

    int64_t lrow[] = { 5 };
    int64_t rrow[] = { 5 };
    ASSERT(test_rel_append_row_mult(lhs, lrow, -1) == 0,
           "append lhs key=5 mult=-1");
    ASSERT(test_rel_append_row_mult(rhs, rrow, 2) == 0,
           "append rhs key=5 mult=2");

    int rc = col_op_join_weighted(lhs, rhs, 0, dst);

    ASSERT(rc == 0, "col_op_join_weighted returns 0 on success");
    ASSERT(dst->nrows == 1, "join produces exactly 1 output row");
    ASSERT(dst->timestamps != NULL, "output timestamps non-NULL");
    ASSERT(dst->timestamps[0].multiplicity == -2, "output mult = -2 (-1 * 2)");

    test_rel_free(lhs);
    test_rel_free(rhs);
    test_rel_free(dst);
    PASS();
}

/* ================================================================
 * Test 3: Zero multiplicity - if either input is 0, output is 0
 *
 * Case 3a: mult_left=0, mult_right=5 -> output mult=0
 * Case 3b: mult_left=3, mult_right=0 -> output mult=0
 *
 * A zero-multiplicity tuple is a Z-set no-op; the join must preserve it.
 * ================================================================ */
static void
test_join_zero_multiplicity(void)
{
    TEST("JOIN plan: zero multiplicity on either side -> output mult=0");

    /* Case 3a: left zero */
    {
        col_rel_t *lhs = test_rel_alloc(1);
        col_rel_t *rhs = test_rel_alloc(1);
        col_rel_t *dst = test_rel_alloc(0);
        ASSERT(lhs && rhs && dst, "test_rel_alloc failed (3a)");

        int64_t lrow[] = { 7 };
        int64_t rrow[] = { 7 };
        ASSERT(test_rel_append_row_mult(lhs, lrow, 0) == 0,
               "append lhs key=7 mult=0");
        ASSERT(test_rel_append_row_mult(rhs, rrow, 5) == 0,
               "append rhs key=7 mult=5");

        int rc = col_op_join_weighted(lhs, rhs, 0, dst);

        ASSERT(rc == 0, "join 3a returns 0");
        ASSERT(dst->nrows == 1, "join 3a produces 1 row");
        ASSERT(dst->timestamps != NULL, "join 3a timestamps non-NULL");
        ASSERT(dst->timestamps[0].multiplicity == 0,
               "output mult=0 when lhs mult=0");

        test_rel_free(lhs);
        test_rel_free(rhs);
        test_rel_free(dst);
    }

    /* Case 3b: right zero */
    {
        col_rel_t *lhs = test_rel_alloc(1);
        col_rel_t *rhs = test_rel_alloc(1);
        col_rel_t *dst = test_rel_alloc(0);
        ASSERT(lhs && rhs && dst, "test_rel_alloc failed (3b)");

        int64_t lrow[] = { 7 };
        int64_t rrow[] = { 7 };
        ASSERT(test_rel_append_row_mult(lhs, lrow, 3) == 0,
               "append lhs key=7 mult=3");
        ASSERT(test_rel_append_row_mult(rhs, rrow, 0) == 0,
               "append rhs key=7 mult=0");

        int rc = col_op_join_weighted(lhs, rhs, 0, dst);

        ASSERT(rc == 0, "join 3b returns 0");
        ASSERT(dst->nrows == 1, "join 3b produces 1 row");
        ASSERT(dst->timestamps != NULL, "join 3b timestamps non-NULL");
        ASSERT(dst->timestamps[0].multiplicity == 0,
               "output mult=0 when rhs mult=0");

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
    printf("=== test_phase3c_join_integration (US-3C-001 TDD RED) ===\n\n");

    test_join_simple_multiplicity_product();
    test_join_negative_multiplicity();
    test_join_zero_multiplicity();

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n", pass_count,
           fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
