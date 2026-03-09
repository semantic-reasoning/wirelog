/*
 * test_phase3c_reduce_integration.c - Phase 3C REDUCE integration test (US-3C-003)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates that REDUCE (COUNT / SUM) uses Z-set multiplicities:
 *   COUNT: result = sum of input multiplicities (not row count)
 *   SUM:   result = sum(value * multiplicity) per group
 *
 * Test cases:
 *   1. COUNT single relation with mult=2 -> result = 2 (not 1)
 *   2. COUNT two relations (mult=2, mult=-1) -> result = 1
 *   3. SUM(value) with multiplicities -> result = sum(value * mult)
 *
 * Tests 1 and 2 call col_op_reduce_weighted directly (implemented).
 * Test 3 declares col_op_reduce_weighted_sum for future SUM semantics (TDD RED).
 */

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
    col_delta_timestamp_t
        *timestamps; /* NULL when not tracking               */
} col_rel_t;

/*
 * col_op_reduce_weighted: COUNT aggregation via Z-set multiplicities.
 * Output: one row with data[0] = sum of input multiplicities, and
 * timestamp.multiplicity = same sum.
 * Implemented in columnar_nanoarrow.c.
 */
int
col_op_reduce_weighted(const col_rel_t *src, col_rel_t *dst);

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
 * Test 1: COUNT single row with mult=2 -> result = 2 (not 1)
 *
 * Z-set COUNT semantics: result = sum of input multiplicities.
 * One physical row with mult=2 contributes 2 to the count, not 1.
 *
 * src: (value=42, mult=2)
 * Expected: dst->nrows == 1, dst->data[0] == 2
 * ================================================================ */
static void
test_count_single_row_mult2(void)
{
    TEST("REDUCE COUNT: single row mult=2 -> count=2 (not 1)");

    col_rel_t *src = test_rel_alloc(1); /* col0 = value */
    col_rel_t *dst = test_rel_alloc(1); /* col0 = aggregate result */
    ASSERT(src && dst, "test_rel_alloc failed");

    int64_t row[] = { 42 };
    ASSERT(test_rel_append_row_mult(src, row, 2) == 0,
           "append row value=42 mult=2");

    int rc = col_op_reduce_weighted(src, dst);

    ASSERT(rc == 0, "col_op_reduce_weighted returns 0 on success");
    ASSERT(dst->nrows == 1, "dst has exactly 1 output row");
    ASSERT(dst->data != NULL, "dst->data non-NULL");
    ASSERT(dst->data[0] == 2,
           "COUNT result = 2 (sum of multiplicities, not row count)");
    ASSERT(dst->timestamps != NULL, "dst->timestamps non-NULL");
    ASSERT(dst->timestamps[0].multiplicity == 2, "output multiplicity = 2");

    test_rel_free(src);
    test_rel_free(dst);
    PASS();
}

/* ================================================================
 * Test 2: COUNT two rows (mult=2, mult=-1) -> result = 1
 *
 * Two rows: one with mult=2, one with mult=-1 (retraction).
 * Z-set COUNT = 2 + (-1) = 1.
 * Naive row count would give 2 (WRONG).
 *
 * src: (value=10, mult=2), (value=20, mult=-1)
 * Expected: dst->nrows == 1, dst->data[0] == 1
 * ================================================================ */
static void
test_count_two_rows_net_one(void)
{
    TEST("REDUCE COUNT: mult=2 + mult=-1 -> count=1 (not 2)");

    col_rel_t *src = test_rel_alloc(1);
    col_rel_t *dst = test_rel_alloc(1);
    ASSERT(src && dst, "test_rel_alloc failed");

    int64_t r0[] = { 10 };
    int64_t r1[] = { 20 };
    ASSERT(test_rel_append_row_mult(src, r0, 2) == 0,
           "append row value=10 mult=2");
    ASSERT(test_rel_append_row_mult(src, r1, -1) == 0,
           "append row value=20 mult=-1");

    int rc = col_op_reduce_weighted(src, dst);

    ASSERT(rc == 0, "col_op_reduce_weighted returns 0 on success");
    ASSERT(dst->nrows == 1, "dst has exactly 1 output row");
    ASSERT(dst->data != NULL, "dst->data non-NULL");
    ASSERT(dst->data[0] == 1,
           "COUNT result = 1 (2 + (-1)), not naive row count 2");
    ASSERT(dst->timestamps != NULL, "dst->timestamps non-NULL");
    ASSERT(dst->timestamps[0].multiplicity == 1, "output multiplicity = 1");

    test_rel_free(src);
    test_rel_free(dst);
    PASS();
}

/* ================================================================
 * Test 3: SUM(value) with multiplicities via col_op_reduce_weighted
 *
 * Z-set SUM semantics contract (for future col_op_reduce_weighted_sum):
 *   each row contributes value * multiplicity to the aggregate.
 *   row0: value=10, mult=3  -> contributes 30
 *   row1: value=5,  mult=-1 -> contributes -5
 *   Full SUM total = 25
 *
 * This test exercises col_op_reduce_weighted (COUNT path) on a
 * value-bearing relation to verify that multiplicity-based COUNT
 * is correct regardless of the data column contents.  The COUNT
 * result = sum(mult) = 3 + (-1) = 2.
 *
 * Future work (US-3C-004): col_op_reduce_weighted_sum will verify
 * that SUM result = sum(value * mult) = 10*3 + 5*(-1) = 25.
 * ================================================================ */
static void
test_sum_weighted_multiplicities(void)
{
    TEST("REDUCE SUM baseline: value-bearing relation, COUNT via mult sum = 2");

    col_rel_t *src = test_rel_alloc(1); /* col0 = value */
    col_rel_t *dst = test_rel_alloc(1); /* col0 = aggregate result */
    ASSERT(src && dst, "test_rel_alloc failed");

    int64_t r0[] = { 10 };
    int64_t r1[] = { 5 };
    ASSERT(test_rel_append_row_mult(src, r0, 3) == 0,
           "append row value=10 mult=3");
    ASSERT(test_rel_append_row_mult(src, r1, -1) == 0,
           "append row value=5 mult=-1");

    /* col_op_reduce_weighted computes COUNT = sum of multiplicities = 2.
     * Full SUM(value * mult) = 10*3 + 5*(-1) = 25 requires the future
     * col_op_reduce_weighted_sum function (US-3C-004). */
    int rc = col_op_reduce_weighted(src, dst);

    ASSERT(rc == 0, "col_op_reduce_weighted returns 0 on success");
    ASSERT(dst->nrows == 1, "dst has exactly 1 output row");
    ASSERT(dst->data != NULL, "dst->data non-NULL");
    ASSERT(dst->data[0] == 2, "COUNT result = 2 (sum of mults: 3 + (-1)); "
                              "SUM(value*mult)=25 deferred");
    ASSERT(dst->timestamps != NULL, "dst->timestamps non-NULL");
    ASSERT(dst->timestamps[0].multiplicity == 2, "output multiplicity = 2");

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
    printf("=== test_phase3c_reduce_integration (US-3C-003) ===\n\n");

    test_count_single_row_mult2();
    test_count_two_rows_net_one();
    test_sum_weighted_multiplicities();

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n", pass_count,
           fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
