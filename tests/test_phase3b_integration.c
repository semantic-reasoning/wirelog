/*
 * test_phase3b_integration.c - Phase 3B end-to-end integration test
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates the full Phase 3B pipeline:
 *   1. Build two relations with multiplicities (parent->child)
 *   2. JOIN them using col_op_join_weighted (multiplies multiplicities)
 *   3. Aggregate result using col_op_reduce_weighted (sums multiplicities)
 *   4. Compute delta using col_compute_delta_mobius (shows differences)
 *
 * Scenario: parent relation with mult=2, child relation with mult=3.
 * Join on shared key produces mult=6. Aggregate sums to total count.
 * Delta reveals changes between two collection snapshots.
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
    col_delta_timestamp_t
        *timestamps; /* NULL when not tracking               */
} col_rel_t;

/* Phase 3B operation declarations */
int
col_op_join_weighted(const col_rel_t *lhs, const col_rel_t *rhs,
                     uint32_t key_col, col_rel_t *dst);
int
col_op_reduce_weighted(const col_rel_t *src, col_rel_t *dst);
int
col_compute_delta_mobius(const col_rel_t *prev_collection,
                         const col_rel_t *curr_collection,
                         col_rel_t *out_delta);

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
 * Test 1: Build two relations with multiplicities.
 *
 * parent: (person_id=1, mult=2), (person_id=2, mult=3)
 * child:  (person_id=1, mult=4), (person_id=2, mult=1)
 *
 * Verify both relations are built correctly before any join.
 * ================================================================ */
static void
test_build_relations_with_multiplicities(void)
{
    TEST("build two relations with multiplicities");

    col_rel_t *parent = test_rel_alloc(1); /* cols: person_id */
    col_rel_t *child = test_rel_alloc(1);  /* cols: person_id */
    ASSERT(parent && child, "test_rel_alloc failed");

    int64_t p0[] = { 1 };
    int64_t p1[] = { 2 };
    ASSERT(test_rel_append_row_mult(parent, p0, 2) == 0,
           "append parent person_id=1 mult=2");
    ASSERT(test_rel_append_row_mult(parent, p1, 3) == 0,
           "append parent person_id=2 mult=3");

    int64_t c0[] = { 1 };
    int64_t c1[] = { 2 };
    ASSERT(test_rel_append_row_mult(child, c0, 4) == 0,
           "append child person_id=1 mult=4");
    ASSERT(test_rel_append_row_mult(child, c1, 1) == 0,
           "append child person_id=2 mult=1");

    /* Verify parent relation */
    ASSERT(parent->nrows == 2, "parent has 2 rows");
    ASSERT(parent->timestamps != NULL, "parent timestamps non-NULL");
    ASSERT(parent->timestamps[0].multiplicity == 2, "parent[0] mult=2");
    ASSERT(parent->timestamps[1].multiplicity == 3, "parent[1] mult=3");
    ASSERT(parent->data[0] == 1, "parent[0] person_id=1");
    ASSERT(parent->data[1] == 2, "parent[1] person_id=2");

    /* Verify child relation */
    ASSERT(child->nrows == 2, "child has 2 rows");
    ASSERT(child->timestamps != NULL, "child timestamps non-NULL");
    ASSERT(child->timestamps[0].multiplicity == 4, "child[0] mult=4");
    ASSERT(child->timestamps[1].multiplicity == 1, "child[1] mult=1");

    test_rel_free(parent);
    test_rel_free(child);
    PASS();
}

/* ================================================================
 * Test 2: JOIN two relations - multiplicities must be multiplied.
 *
 * parent: (person_id=1, mult=2), (person_id=2, mult=3)
 * child:  (person_id=1, mult=4), (person_id=2, mult=1)
 *
 * JOIN on person_id (col 0):
 *   person_id=1: mult = 2 * 4 = 8
 *   person_id=2: mult = 3 * 1 = 3
 *
 * Expected: dst->nrows == 2, multiplicities {8, 3} in some order.
 * ================================================================ */
static void
test_join_multiplies_multiplicities(void)
{
    TEST("JOIN two relations - multiplicities are multiplied");

    col_rel_t *parent = test_rel_alloc(1);
    col_rel_t *child = test_rel_alloc(1);
    col_rel_t *joined = test_rel_alloc(0); /* ncols set by join */
    ASSERT(parent && child && joined, "test_rel_alloc failed");

    int64_t p0[] = { 1 };
    int64_t p1[] = { 2 };
    int64_t c0[] = { 1 };
    int64_t c1[] = { 2 };
    ASSERT(test_rel_append_row_mult(parent, p0, 2) == 0,
           "parent person_id=1 mult=2");
    ASSERT(test_rel_append_row_mult(parent, p1, 3) == 0,
           "parent person_id=2 mult=3");
    ASSERT(test_rel_append_row_mult(child, c0, 4) == 0,
           "child  person_id=1 mult=4");
    ASSERT(test_rel_append_row_mult(child, c1, 1) == 0,
           "child  person_id=2 mult=1");

    int rc = col_op_join_weighted(parent, child, 0, joined);

    ASSERT(rc == 0, "join returns 0 on success");
    ASSERT(joined->nrows == 2, "joined has 2 rows");
    ASSERT(joined->timestamps != NULL, "joined timestamps non-NULL");

    /* Check both expected products appear (order may vary). */
    int64_t m0 = joined->timestamps[0].multiplicity;
    int64_t m1 = joined->timestamps[1].multiplicity;
    bool found8 = (m0 == 8 || m1 == 8);
    bool found3 = (m0 == 3 || m1 == 3);
    ASSERT(found8, "output contains mult=8 (person_id=1: 2*4)");
    ASSERT(found3, "output contains mult=3 (person_id=2: 3*1)");

    test_rel_free(parent);
    test_rel_free(child);
    test_rel_free(joined);
    PASS();
}

/* ================================================================
 * Test 3: Aggregate JOIN result - sum of multiplicities.
 *
 * After joining parent x child on person_id:
 *   person_id=1: mult=8
 *   person_id=2: mult=3
 *
 * col_op_reduce_weighted sums all multiplicities:
 *   total = 8 + 3 = 11
 *
 * Expected: dst->nrows == 1, dst->data[0] == 11.
 * ================================================================ */
static void
test_aggregate_join_result(void)
{
    TEST("aggregate JOIN result - sum of multiplicities = 11");

    /* Build the joined relation directly (bypassing re-running the join). */
    col_rel_t *joined = test_rel_alloc(1);
    col_rel_t *agg = test_rel_alloc(1);
    ASSERT(joined && agg, "test_rel_alloc failed");

    /* Represent the join output: two rows with mult=8 and mult=3. */
    int64_t row0[] = { 1 };
    int64_t row1[] = { 2 };
    ASSERT(test_rel_append_row_mult(joined, row0, 8) == 0,
           "append joined row person_id=1 mult=8");
    ASSERT(test_rel_append_row_mult(joined, row1, 3) == 0,
           "append joined row person_id=2 mult=3");

    int rc = col_op_reduce_weighted(joined, agg);

    ASSERT(rc == 0, "reduce returns 0 on success");
    ASSERT(agg->nrows == 1, "agg has 1 output row");
    ASSERT(agg->data != NULL, "agg data non-NULL");
    ASSERT(agg->data[0] == 11, "COUNT = 11 (8 + 3)");
    ASSERT(agg->timestamps != NULL, "agg timestamps non-NULL");
    ASSERT(agg->timestamps[0].multiplicity == 11,
           "agg output multiplicity = 11");

    test_rel_free(joined);
    test_rel_free(agg);
    PASS();
}

/* ================================================================
 * Test 4: Compute delta between two collection snapshots.
 *
 * Scenario: collection of joined parent-child pairs changes between
 * two steps:
 *   prev: (person_id=1, mult=6), (person_id=2, mult=3)
 *   curr: (person_id=1, mult=8), (person_id=3, mult=5)
 *
 * Expected delta:
 *   person_id=1: mult = 8 - 6 = +2  (grew)
 *   person_id=2: mult = -3           (vanished)
 *   person_id=3: mult = +5           (new)
 *
 * delta->nrows == 3, all three changes present.
 * ================================================================ */
static void
test_compute_delta_between_snapshots(void)
{
    TEST("delta between snapshots: grows, vanishes, appears");

    col_rel_t *prev = test_rel_alloc(1);
    col_rel_t *curr = test_rel_alloc(1);
    col_rel_t *delta = test_rel_alloc(1);
    ASSERT(prev && curr && delta, "test_rel_alloc failed");

    int64_t key1[] = { 1 };
    int64_t key2[] = { 2 };
    int64_t key3[] = { 3 };

    ASSERT(test_rel_append_row_mult(prev, key1, 6) == 0,
           "prev person_id=1 mult=6");
    ASSERT(test_rel_append_row_mult(prev, key2, 3) == 0,
           "prev person_id=2 mult=3");
    ASSERT(test_rel_append_row_mult(curr, key1, 8) == 0,
           "curr person_id=1 mult=8");
    ASSERT(test_rel_append_row_mult(curr, key3, 5) == 0,
           "curr person_id=3 mult=5");

    int rc = col_compute_delta_mobius(prev, curr, delta);

    ASSERT(rc == 0, "delta returns 0 on success");
    ASSERT(delta->nrows == 3, "delta has 3 rows (changed, vanished, appeared)");
    ASSERT(delta->timestamps != NULL, "delta timestamps non-NULL");
    ASSERT(delta->data != NULL, "delta data non-NULL");

    /* Verify all three expected changes appear (order may vary). */
    bool found_plus2 = false;
    bool found_minus3 = false;
    bool found_plus5 = false;
    for (uint32_t i = 0; i < delta->nrows; i++) {
        int64_t key = delta->data[i];
        int64_t mult = delta->timestamps[i].multiplicity;
        if (key == 1 && mult == 2)
            found_plus2 = true;
        if (key == 2 && mult == -3)
            found_minus3 = true;
        if (key == 3 && mult == 5)
            found_plus5 = true;
    }
    ASSERT(found_plus2, "delta contains person_id=1 mult=+2 (grew: 8-6)");
    ASSERT(found_minus3, "delta contains person_id=2 mult=-3 (vanished)");
    ASSERT(found_plus5, "delta contains person_id=3 mult=+5 (appeared)");

    test_rel_free(prev);
    test_rel_free(curr);
    test_rel_free(delta);
    PASS();
}

/* ================================================================
 * Test 5: Full pipeline - JOIN -> aggregate -> delta in sequence.
 *
 * Step A: JOIN parent x child
 *   parent: (k=10, mult=2)
 *   child:  (k=10, mult=3)
 *   joined: (k=10, mult=6)
 *
 * Step B: Aggregate joined result
 *   agg: COUNT = 6
 *
 * Step C: Delta from prev_agg (COUNT=4) to curr_agg (COUNT=6)
 *   delta: mult = 6 - 4 = +2
 *
 * Validates all three operations compose correctly end-to-end.
 * ================================================================ */
static void
test_full_pipeline_join_agg_delta(void)
{
    TEST("full pipeline: JOIN -> aggregate -> delta");

    /* Step A: JOIN */
    col_rel_t *parent = test_rel_alloc(1);
    col_rel_t *child = test_rel_alloc(1);
    col_rel_t *joined = test_rel_alloc(0);
    ASSERT(parent && child && joined, "test_rel_alloc failed (join)");

    int64_t pk[] = { 10 };
    int64_t ck[] = { 10 };
    ASSERT(test_rel_append_row_mult(parent, pk, 2) == 0, "parent k=10 mult=2");
    ASSERT(test_rel_append_row_mult(child, ck, 3) == 0, "child  k=10 mult=3");

    int rc = col_op_join_weighted(parent, child, 0, joined);
    ASSERT(rc == 0, "join step returns 0");
    ASSERT(joined->nrows == 1, "join produces 1 row");
    ASSERT(joined->timestamps[0].multiplicity == 6, "join mult = 6 (2*3)");

    test_rel_free(parent);
    test_rel_free(child);

    /* Step B: Aggregate */
    col_rel_t *agg = test_rel_alloc(1);
    ASSERT(agg, "test_rel_alloc failed (agg)");

    rc = col_op_reduce_weighted(joined, agg);
    ASSERT(rc == 0, "reduce step returns 0");
    ASSERT(agg->nrows == 1, "reduce produces 1 row");
    ASSERT(agg->data[0] == 6, "COUNT = 6");
    ASSERT(agg->timestamps[0].multiplicity == 6, "agg output multiplicity = 6");

    test_rel_free(joined);

    /* Step C: Delta from prev COUNT (mult=4) to curr COUNT (mult=6).
     * col_op_reduce_weighted writes data[0] = sum of multiplicities.
     * curr_agg has data[0]=6, mult=6.  For delta to see these as the
     * same key (col 0 == 6) evolving from mult=4 to mult=6, prev_agg
     * must also carry key=6. */
    col_rel_t *prev_agg = test_rel_alloc(1);
    col_rel_t *delta = test_rel_alloc(1);
    ASSERT(prev_agg && delta, "test_rel_alloc failed (delta)");

    int64_t prev_row[] = { 6 }; /* same key as curr_agg->data[0] */
    ASSERT(test_rel_append_row_mult(prev_agg, prev_row, 4) == 0,
           "prev_agg key=6 mult=4");

    rc = col_compute_delta_mobius(prev_agg, agg, delta);
    ASSERT(rc == 0, "delta step returns 0");
    ASSERT(delta->nrows == 1, "delta has 1 row");
    ASSERT(delta->timestamps[0].multiplicity == 2, "delta mult = +2 (6 - 4)");

    test_rel_free(prev_agg);
    test_rel_free(agg);
    test_rel_free(delta);
    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */
int
main(void)
{
    printf("=== test_phase3b_integration (Phase 3B end-to-end) ===\n\n");

    test_build_relations_with_multiplicities();
    test_join_multiplies_multiplicities();
    test_aggregate_join_result();
    test_compute_delta_between_snapshots();
    test_full_pipeline_join_agg_delta();

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n", pass_count,
           fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
