/*
 * test_mobius_delta_formula.c - TDD RED PHASE
 * Tests for Mobius delta computation (Z-set collection difference).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * These tests define expected behaviour BEFORE the function is implemented
 * (US-3B-006 RED phase).  Expected failure mode:
 *
 *   undefined reference to `col_compute_delta_mobius`
 *
 * Semantics under test:
 *   delta(i) = Collection(i) - Collection(i-1) computed via multiplicities:
 *   - For each key in Collection(i):
 *       If in Collection(i-1): delta_mult = mult(i) - mult(i-1)
 *       If NOT in Collection(i-1): delta_mult = mult(i)
 *   - For each key in Collection(i-1) but not Collection(i):
 *       delta_mult = -mult(i-1)
 *   - Rows with delta_mult == 0 are omitted from output.
 *
 * Test cases:
 *   1. Collection grows {k1:2} -> {k1:3} -> delta={k1:mult=+1}
 *   2. Collection shrinks {k1:3} -> {k1:2} -> delta={k1:mult=-1}
 *   3. New key appears {} -> {k1:5} -> delta={k1:mult=+5}
 *   4. Key vanishes {k1:2} -> {} -> delta={k1:mult=-2}
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

/*
 * col_compute_delta_mobius:
 *   Compute the Mobius delta between prev_collection and curr_collection.
 *
 *   For each unique key (first column) in the union of both collections:
 *     - If key present in curr but not prev: output row with mult = curr_mult
 *     - If key present in prev but not curr: output row with mult = -prev_mult
 *     - If key present in both:             output row with mult = curr_mult - prev_mult
 *                                           (omitted if mult == 0)
 *
 *   Relations are assumed to have ncols >= 1 where column 0 is the key.
 *   Both collections must have timestamps != NULL.
 *   out_delta must be caller-allocated, empty (nrows==0) on entry, with
 *   ncols matching prev_collection->ncols and curr_collection->ncols.
 *
 *   Returns 0 on success, EINVAL on bad arguments, ENOMEM on allocation failure.
 *
 * RED phase: function does not exist yet -> link error (expected).
 */
int
col_compute_delta_mobius(const col_rel_t *prev_collection,
                         const col_rel_t *curr_collection,
                         col_rel_t *out_delta);

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
 * Test 1: Collection grows {k1:2} -> {k1:3} -> delta={k1:mult=+1}
 *
 * prev: row=(key=1), multiplicity=2
 * curr: row=(key=1), multiplicity=3
 * Expected delta:
 *   nrows == 1
 *   data[0] == 1   (key=1)
 *   timestamps[0].multiplicity == 1   (3 - 2 = +1)
 * ================================================================ */
static void
test_collection_grows(void)
{
    TEST("collection grows {k1:2} -> {k1:3} -> delta={k1:mult=+1}");

    col_rel_t *prev = test_rel_alloc(1);
    col_rel_t *curr = test_rel_alloc(1);
    col_rel_t *delta = test_rel_alloc(1);
    ASSERT(prev && curr && delta, "test_rel_alloc failed");

    int64_t row1[] = { 1 };
    ASSERT(test_rel_append_row_mult(prev, row1, 2) == 0,
           "append prev row mult=2");
    ASSERT(test_rel_append_row_mult(curr, row1, 3) == 0,
           "append curr row mult=3");

    int rc = col_compute_delta_mobius(prev, curr, delta);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(delta->nrows == 1, "delta->nrows == 1");
    ASSERT(delta->data != NULL, "delta->data is non-NULL");
    ASSERT(delta->data[0] == 1, "delta key == 1");
    ASSERT(delta->timestamps != NULL, "delta->timestamps is non-NULL");
    ASSERT(delta->timestamps[0].multiplicity == 1,
           "delta multiplicity == +1 (3 - 2)");

    test_rel_free(prev);
    test_rel_free(curr);
    test_rel_free(delta);
    PASS();
}

/* ================================================================
 * Test 2: Collection shrinks {k1:3} -> {k1:2} -> delta={k1:mult=-1}
 *
 * prev: row=(key=1), multiplicity=3
 * curr: row=(key=1), multiplicity=2
 * Expected delta:
 *   nrows == 1
 *   data[0] == 1   (key=1)
 *   timestamps[0].multiplicity == -1   (2 - 3 = -1)
 * ================================================================ */
static void
test_collection_shrinks(void)
{
    TEST("collection shrinks {k1:3} -> {k1:2} -> delta={k1:mult=-1}");

    col_rel_t *prev = test_rel_alloc(1);
    col_rel_t *curr = test_rel_alloc(1);
    col_rel_t *delta = test_rel_alloc(1);
    ASSERT(prev && curr && delta, "test_rel_alloc failed");

    int64_t row1[] = { 1 };
    ASSERT(test_rel_append_row_mult(prev, row1, 3) == 0,
           "append prev row mult=3");
    ASSERT(test_rel_append_row_mult(curr, row1, 2) == 0,
           "append curr row mult=2");

    int rc = col_compute_delta_mobius(prev, curr, delta);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(delta->nrows == 1, "delta->nrows == 1");
    ASSERT(delta->data != NULL, "delta->data is non-NULL");
    ASSERT(delta->data[0] == 1, "delta key == 1");
    ASSERT(delta->timestamps != NULL, "delta->timestamps is non-NULL");
    ASSERT(delta->timestamps[0].multiplicity == -1,
           "delta multiplicity == -1 (2 - 3)");

    test_rel_free(prev);
    test_rel_free(curr);
    test_rel_free(delta);
    PASS();
}

/* ================================================================
 * Test 3: New key appears {} -> {k1:5} -> delta={k1:mult=+5}
 *
 * prev: empty
 * curr: row=(key=7), multiplicity=5
 * Expected delta:
 *   nrows == 1
 *   data[0] == 7   (key=7)
 *   timestamps[0].multiplicity == 5
 * ================================================================ */
static void
test_new_key_appears(void)
{
    TEST("new key appears {} -> {k1:5} -> delta={k1:mult=+5}");

    col_rel_t *prev = test_rel_alloc(1);
    col_rel_t *curr = test_rel_alloc(1);
    col_rel_t *delta = test_rel_alloc(1);
    ASSERT(prev && curr && delta, "test_rel_alloc failed");

    /* prev is empty; allocate empty timestamps so timestamps != NULL check passes */
    prev->timestamps
        = (col_delta_timestamp_t *)calloc(1, sizeof(col_delta_timestamp_t));
    ASSERT(prev->timestamps != NULL, "alloc prev timestamps");
    prev->capacity = 1;

    int64_t row7[] = { 7 };
    ASSERT(test_rel_append_row_mult(curr, row7, 5) == 0,
           "append curr row key=7 mult=5");

    int rc = col_compute_delta_mobius(prev, curr, delta);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(delta->nrows == 1, "delta->nrows == 1");
    ASSERT(delta->data != NULL, "delta->data is non-NULL");
    ASSERT(delta->data[0] == 7, "delta key == 7");
    ASSERT(delta->timestamps != NULL, "delta->timestamps is non-NULL");
    ASSERT(delta->timestamps[0].multiplicity == 5, "delta multiplicity == +5");

    test_rel_free(prev);
    test_rel_free(curr);
    test_rel_free(delta);
    PASS();
}

/* ================================================================
 * Test 4: Key vanishes {k1:2} -> {} -> delta={k1:mult=-2}
 *
 * prev: row=(key=3), multiplicity=2
 * curr: empty
 * Expected delta:
 *   nrows == 1
 *   data[0] == 3   (key=3)
 *   timestamps[0].multiplicity == -2
 * ================================================================ */
static void
test_key_vanishes(void)
{
    TEST("key vanishes {k1:2} -> {} -> delta={k1:mult=-2}");

    col_rel_t *prev = test_rel_alloc(1);
    col_rel_t *curr = test_rel_alloc(1);
    col_rel_t *delta = test_rel_alloc(1);
    ASSERT(prev && curr && delta, "test_rel_alloc failed");

    int64_t row3[] = { 3 };
    ASSERT(test_rel_append_row_mult(prev, row3, 2) == 0,
           "append prev row key=3 mult=2");

    /* curr is empty; allocate empty timestamps so timestamps != NULL check passes */
    curr->timestamps
        = (col_delta_timestamp_t *)calloc(1, sizeof(col_delta_timestamp_t));
    ASSERT(curr->timestamps != NULL, "alloc curr timestamps");
    curr->capacity = 1;

    int rc = col_compute_delta_mobius(prev, curr, delta);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(delta->nrows == 1, "delta->nrows == 1");
    ASSERT(delta->data != NULL, "delta->data is non-NULL");
    ASSERT(delta->data[0] == 3, "delta key == 3");
    ASSERT(delta->timestamps != NULL, "delta->timestamps is non-NULL");
    ASSERT(delta->timestamps[0].multiplicity == -2, "delta multiplicity == -2");

    test_rel_free(prev);
    test_rel_free(curr);
    test_rel_free(delta);
    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */
int
main(void)
{
    printf("=== test_mobius_delta_formula (TDD RED PHASE) ===\n\n");
    printf(
        "NOTE: Expected to FAIL at link time until col_compute_delta_mobius\n");
    printf("      is implemented with extern linkage.\n\n");

    test_collection_grows();
    test_collection_shrinks();
    test_new_key_appears();
    test_key_vanishes();

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n", pass_count,
           fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
