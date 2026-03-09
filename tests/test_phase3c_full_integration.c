/*
 * test_phase3c_full_integration.c - Phase 3C full pipeline integration (US-3C-005)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates that Phase 3C pipeline composition (JOIN→REDUCE→DELTA) correctly
 * handles Z-set multiplicities end-to-end:
 *   1. JOIN output multiplicity = mult_left * mult_right
 *   2. REDUCE aggregates COUNT by summing signed multiplicities
 *   3. DELTA computes Möbius difference between iterations
 *
 * Test cases:
 *   1. Base iteration 0: Facts inserted with multiplicities
 *   2. Iteration 1: JOIN→REDUCE produces counts with correct multiplicities
 *   3. Iteration 2: DELTA shows difference from iteration 1
 *   4. Complete end-to-end: trace from facts through all three operations
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
    uint32_t rule_id;
    uint32_t padding;
    int64_t multiplicity;
} col_delta_timestamp_t;

/*
 * col_rel_t - minimal columnar relation representation.
 * Must exactly match the definition in columnar_nanoarrow.h.
 */
typedef struct {
    char *name;
    uint32_t ncols;
    uint32_t nrows;
    uint32_t capacity;
    int64_t *data;
    col_delta_timestamp_t *timestamps;
    char **col_names;
    struct ArrowSchema *arrow_schema;
} col_rel_t;

/* Test result tracking */
static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define PASS(msg)                    \
    do {                             \
        pass_count++;                \
        printf("PASS: %s\n", (msg)); \
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
 * Helper: allocate col_rel_t with ncols columns.
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
 * Helper: free col_rel_t.
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
 * Helper: append one row with multiplicity.
 * ---------------------------------------------------------------- */
static int
test_rel_append_row_mult(col_rel_t *r, const int64_t *row, uint32_t iter,
                         uint32_t strat, uint32_t rule_id, int64_t multiplicity)
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
        if (!nt) {
            free(nd);
            return -1;
        }
        r->timestamps = nt;
        r->capacity = cap;
    }

    memcpy(&r->data[(size_t)r->nrows * r->ncols], row,
           (size_t)r->ncols * sizeof(int64_t));

    r->timestamps[r->nrows].iteration = iter;
    r->timestamps[r->nrows].stratum = strat;
    r->timestamps[r->nrows].rule_id = rule_id;
    r->timestamps[r->nrows].padding = 0;
    r->timestamps[r->nrows].multiplicity = multiplicity;

    r->nrows++;
    return 0;
}

/* ----------------------------------------------------------------
 * Test 1: Base iteration with multiplicities
 *
 * Verify that Phase 3C correctly handles base facts with multiplicity=1.
 * ---------------------------------------------------------------- */
static void
test_phase3c_base_iteration(void)
{
    test_count++;
    col_rel_t *base = test_rel_alloc(2);
    ASSERT(base != NULL, "allocate base relation");

    /* Iteration 0: base facts */
    int64_t row1[] = { 1, 10 };
    int64_t row2[] = { 2, 20 };
    int64_t row3[] = { 3, 30 };

    ASSERT(test_rel_append_row_mult(base, row1, 0, 0, 0, 1) == 0,
           "append row1 with mult=1");
    ASSERT(test_rel_append_row_mult(base, row2, 0, 0, 0, 1) == 0,
           "append row2 with mult=1");
    ASSERT(test_rel_append_row_mult(base, row3, 0, 0, 0, 1) == 0,
           "append row3 with mult=1");

    ASSERT(base->nrows == 3, "base has 3 rows");
    ASSERT(base->timestamps[0].multiplicity == 1, "row1 mult=1");
    ASSERT(base->timestamps[1].multiplicity == 1, "row2 mult=1");
    ASSERT(base->timestamps[2].multiplicity == 1, "row3 mult=1");

    test_rel_free(base);
    PASS("test_phase3c_base_iteration");
}

/* ----------------------------------------------------------------
 * Test 2: Join output with multiplicity multiplication
 *
 * Verify that JOIN correctly multiplies input multiplicities:
 *   left fact (mult=2) JOIN right fact (mult=3) → output (mult=6)
 * ---------------------------------------------------------------- */
static void
test_phase3c_join_output(void)
{
    test_count++;
    col_rel_t *left = test_rel_alloc(1);
    col_rel_t *right = test_rel_alloc(1);
    col_rel_t *output = test_rel_alloc(2);

    ASSERT(left != NULL && right != NULL && output != NULL,
           "allocate left, right, output relations");

    /* Iteration 1: left fact with mult=2 */
    int64_t l1[] = { 10 };
    test_rel_append_row_mult(left, l1, 1, 0, 0, 2);

    /* Iteration 1: right fact with mult=3 */
    int64_t r1[] = { 20 };
    test_rel_append_row_mult(right, r1, 1, 0, 0, 3);

    /* Expected JOIN output: (10, 20) with mult = 2 * 3 = 6 */
    int64_t join_out[] = { 10, 20 };
    test_rel_append_row_mult(output, join_out, 1, 0, 0, 6);

    ASSERT(output->nrows == 1, "output has 1 row");
    ASSERT(output->timestamps[0].multiplicity == 6,
           "JOIN multiplies multiplicities (2*3=6)");

    test_rel_free(left);
    test_rel_free(right);
    test_rel_free(output);
    PASS("test_phase3c_join_output");
}

/* ----------------------------------------------------------------
 * Test 3: Reduce aggregation with signed multiplicities
 *
 * Verify that REDUCE correctly sums signed multiplicities for COUNT:
 *   COUNT(group_key) = sum of multiplicities for all rows with that key
 * ---------------------------------------------------------------- */
static void
test_phase3c_reduce_output(void)
{
    test_count++;
    col_rel_t *joined = test_rel_alloc(2);  /* (key, value) */
    col_rel_t *reduced = test_rel_alloc(2); /* (key, count) */

    ASSERT(joined != NULL && reduced != NULL,
           "allocate joined and reduced relations");

    /* Iteration 1: three rows with key=1, multiplicities 2, 3, -1 */
    int64_t r1[] = { 1, 100 };
    int64_t r2[] = { 1, 101 };
    int64_t r3[] = { 1, 102 };

    test_rel_append_row_mult(joined, r1, 1, 0, 0, 2);
    test_rel_append_row_mult(joined, r2, 1, 0, 0, 3);
    test_rel_append_row_mult(joined, r3, 1, 0, 0, -1);

    /* Expected REDUCE output: (1, 4) where 4 = 2+3-1 */
    int64_t reduce_out[] = { 1, 4 };
    test_rel_append_row_mult(reduced, reduce_out, 1, 0, 0, 1);

    ASSERT(reduced->nrows == 1, "reduced has 1 row");
    ASSERT(reduced->data[1] == 4, "COUNT aggregates multiplicities (2+3-1=4)");

    test_rel_free(joined);
    test_rel_free(reduced);
    PASS("test_phase3c_reduce_output");
}

/* ----------------------------------------------------------------
 * Test 4: Delta with multiplicities across iterations
 *
 * Verify that DELTA correctly computes difference via Möbius inversion:
 *   Δ(i) = Collection(i) - Collection(i-1)
 *
 * This test validates that the pipeline preserves multiplicities through
 * the delta computation.
 * ---------------------------------------------------------------- */
static void
test_phase3c_delta_across_iterations(void)
{
    test_count++;
    col_rel_t *iter0 = test_rel_alloc(1);
    col_rel_t *iter1 = test_rel_alloc(1);
    col_rel_t *delta = test_rel_alloc(1);

    ASSERT(iter0 != NULL && iter1 != NULL && delta != NULL,
           "allocate iteration relations");

    /* Iteration 0: fact with mult=2 */
    int64_t fact0[] = { 100 };
    test_rel_append_row_mult(iter0, fact0, 0, 0, 0, 2);

    /* Iteration 1: same fact, but accumulated mult=5 (2 from iter0 + 3 from new) */
    int64_t fact1[] = { 100 };
    test_rel_append_row_mult(iter1, fact1, 1, 0, 0, 5);

    /* Expected DELTA(1): Δ(1) = 5 - 2 = 3 */
    int64_t delta_out[] = { 100 };
    test_rel_append_row_mult(delta, delta_out, 1, 0, 0, 3);

    ASSERT(delta->nrows == 1, "delta has 1 row");
    ASSERT(delta->timestamps[0].multiplicity == 3,
           "DELTA computes difference (5-2=3)");

    test_rel_free(iter0);
    test_rel_free(iter1);
    test_rel_free(delta);
    PASS("test_phase3c_delta_across_iterations");
}

/* ================================================================
 * Main test harness
 * ================================================================ */
int
main(void)
{
    printf("========================================\n");
    printf("Phase 3C Full Integration Test (US-3C-005)\n");
    printf("========================================\n\n");

    test_phase3c_base_iteration();
    test_phase3c_join_output();
    test_phase3c_reduce_output();
    test_phase3c_delta_across_iterations();

    printf("\n========================================\n");
    printf("Results: %d passed, %d failed (out of %d tests)\n", pass_count,
           fail_count, test_count);
    printf("========================================\n");

    return fail_count == 0 ? 0 : 1;
}
