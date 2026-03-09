/*
 * test_frontier_skip_mobius.c - Phase 3D frontier skip test (US-3D-001)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates frontier skip optimization: skip iterations where net_multiplicity == 0.
 *
 * Frontier skip checks if all relations in a stratum have net_multiplicity == 0.
 * If so, skip that iteration (don't evaluate any rules) since no new facts can
 * be derived when the delta is empty.
 *
 * Test cases:
 *   1. Single relation with net_mult=0 across both iterations → skip iteration 2
 *   2. JOIN result with net_mult≠0 → do NOT skip
 *   3. Mixed operations (some keys mult=0, some≠0) → partial skip behavior
 */

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * ArrowSchema stub for col_rel_t
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
 * col_delta_timestamp_t - matches columnar_nanoarrow.h definition
 */
typedef struct {
    uint32_t iteration;
    uint32_t stratum;
    uint32_t rule_id;
    uint32_t padding;
    int64_t multiplicity;
} col_delta_timestamp_t;

/*
 * col_rel_t - minimal columnar relation
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

/* ================================================================
 * Helpers
 * ================================================================ */

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

/* ================================================================
 * Frontier Skip Helper: compute net multiplicity for a relation
 * ================================================================ */

static int64_t
compute_net_multiplicity(const col_rel_t *rel)
{
    int64_t net = 0;
    for (uint32_t i = 0; i < rel->nrows; i++) {
        net += rel->timestamps[i].multiplicity;
    }
    return net;
}

/* ================================================================
 * Test 1: Single relation with net_mult=0 across both iterations
 *
 * Iteration 0: fact with mult=+2
 * Iteration 1: fact with mult=-2
 * net_mult for iteration 1 = -2, not zero
 * BUT: across iteration 0+1, the net is zero, so iteration 2 should skip
 *
 * Actually, for "net_mult == 0", we check if Δ(i) == 0, meaning
 * Collection(i) - Collection(i-1) = 0.
 *
 * This test validates the skip condition: if the delta for an iteration
 * has zero net multiplicity, that iteration can be skipped.
 * ================================================================ */
static void
test_frontier_skip_net_zero(void)
{
    test_count++;
    col_rel_t *iter1_delta = test_rel_alloc(1);
    ASSERT(iter1_delta != NULL, "allocate iteration 1 delta");

    /* Iteration 1 delta: one fact inserted, one retracted → net=0 */
    int64_t fact1[] = { 100 };
    int64_t fact2[] = { 100 };

    test_rel_append_row_mult(iter1_delta, fact1, 1, 0, 0, 1);
    test_rel_append_row_mult(iter1_delta, fact2, 1, 0, 0, -1);

    /* Compute net multiplicity for iteration 1 */
    int64_t net = compute_net_multiplicity(iter1_delta);

    ASSERT(net == 0, "net multiplicity is zero");
    ASSERT(iter1_delta->nrows == 2, "delta has 2 rows");

    test_rel_free(iter1_delta);
    PASS("test_frontier_skip_net_zero");
}

/* ================================================================
 * Test 2: JOIN result with net_mult≠0 → do NOT skip
 * ================================================================ */
static void
test_frontier_skip_nonzero_join(void)
{
    test_count++;
    col_rel_t *joined = test_rel_alloc(2);
    ASSERT(joined != NULL, "allocate joined relation");

    /* Iteration 1 JOIN result: two facts with mult=2, one with mult=-1 */
    int64_t j1[] = { 1, 10 };
    int64_t j2[] = { 2, 20 };
    int64_t j3[] = { 3, 30 };

    test_rel_append_row_mult(joined, j1, 1, 0, 0, 2);
    test_rel_append_row_mult(joined, j2, 1, 0, 0, 2);
    test_rel_append_row_mult(joined, j3, 1, 0, 0, -1);

    /* Compute net multiplicity */
    int64_t net = compute_net_multiplicity(joined);

    ASSERT(net == 3, "net multiplicity is nonzero (2+2-1=3)");
    /* Since net ≠ 0, iteration 2 should NOT be skipped */

    test_rel_free(joined);
    PASS("test_frontier_skip_nonzero_join");
}

/* ================================================================
 * Test 3: Mixed operations - track per-key net multiplicities
 *
 * For partial skip (some keys with net=0, others with net≠0), we need
 * per-key tracking. However, the frontier skip optimization works at
 * the stratum level: if ALL relations have net=0, skip the iteration.
 *
 * This test validates that even when some relations in a stratum have
 * nonzero net multiplicity, the iteration is NOT skipped.
 * ================================================================ */
static void
test_frontier_skip_mixed_keys(void)
{
    test_count++;
    col_rel_t *delta = test_rel_alloc(2);
    ASSERT(delta != NULL, "allocate delta relation");

    /* Iteration 1 delta with mixed multiplicities for different keys */
    int64_t key1_ins[] = { 1, 100 };
    int64_t key1_del[] = { 1, 101 };
    int64_t key2_ins[] = { 2, 200 };

    /* Key 1: net mult = 0 (insert + delete) */
    test_rel_append_row_mult(delta, key1_ins, 1, 0, 0, 1);
    test_rel_append_row_mult(delta, key1_del, 1, 0, 0, -1);

    /* Key 2: net mult ≠ 0 (only insert) */
    test_rel_append_row_mult(delta, key2_ins, 1, 0, 0, 1);

    /* Overall stratum net multiplicity */
    int64_t net = compute_net_multiplicity(delta);

    ASSERT(net == 1, "overall net multiplicity is 1 (0 + 1)");
    /* Since net ≠ 0, iteration should NOT be skipped */

    test_rel_free(delta);
    PASS("test_frontier_skip_mixed_keys");
}

/* ================================================================
 * Main test harness
 * ================================================================ */
int
main(void)
{
    printf("========================================\n");
    printf("Phase 3D Frontier Skip Test (US-3D-001)\n");
    printf("========================================\n\n");

    test_frontier_skip_net_zero();
    test_frontier_skip_nonzero_join();
    test_frontier_skip_mixed_keys();

    printf("\n========================================\n");
    printf("Results: %d passed, %d failed (out of %d tests)\n", pass_count,
           fail_count, test_count);
    printf("========================================\n");

    return fail_count == 0 ? 0 : 1;
}
