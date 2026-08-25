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
#include "../wirelog/columnar/internal.h"

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
    ASSERT(delta->columns != NULL, "delta->columns is non-NULL");
    ASSERT(delta->columns[(0) % delta->ncols][(0) / delta->ncols] == 1,
        "delta key == 1");
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
    ASSERT(delta->columns != NULL, "delta->columns is non-NULL");
    ASSERT(delta->columns[(0) % delta->ncols][(0) / delta->ncols] == 1,
        "delta key == 1");
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
    ASSERT(delta->columns != NULL, "delta->columns is non-NULL");
    ASSERT(delta->columns[(0) % delta->ncols][(0) / delta->ncols] == 7,
        "delta key == 7");
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
    ASSERT(delta->columns != NULL, "delta->columns is non-NULL");
    ASSERT(delta->columns[(0) % delta->ncols][(0) / delta->ncols] == 3,
        "delta key == 3");
    ASSERT(delta->timestamps != NULL, "delta->timestamps is non-NULL");
    ASSERT(delta->timestamps[0].multiplicity == -2, "delta multiplicity == -2");

    test_rel_free(prev);
    test_rel_free(curr);
    test_rel_free(delta);
    PASS();
}

/* ================================================================
 * Test 5: relations wider than COL_STACK_MAX (Issue #1000)
 *
 * Both passes of col_compute_delta_mobius copied rows into an
 * int64_t[COL_STACK_MAX] (32) and filled them at the relation's width, so
 * a 33-column collection overflowed all four buffers.  The function has
 * no caller outside the test suite, so no .dl program reaches it and the
 * width guard can only be exercised here.
 *
 * prev: key 1 (mult 2), key 2 (mult 4)     [33 columns]
 * curr: key 1 (mult 5), key 3 (mult 7)     [33 columns]
 *
 * Expected delta, in emission order (pass 1 over curr, then pass 2 over
 * prev): key 1 -> +3 (5 - 2), key 3 -> +7 (new), key 2 -> -4 (vanished).
 * ================================================================ */
#define WIDE_NCOLS 33u

static void
test_wide_relation_delta(void)
{
    TEST("33-column delta does not overflow the row buffers (#1000)");

    col_rel_t *prev = test_rel_alloc(WIDE_NCOLS);
    col_rel_t *curr = test_rel_alloc(WIDE_NCOLS);
    col_rel_t *delta = test_rel_alloc(WIDE_NCOLS);
    ASSERT(prev && curr && delta, "test_rel_alloc failed");

    int64_t rows[3][WIDE_NCOLS];
    for (uint32_t k = 0; k < 3; k++) {
        for (uint32_t c = 0; c < WIDE_NCOLS; c++)
            rows[k][c] = (c == 0) ? (int64_t)(k + 1u)
                                  : (int64_t)(1000u * (k + 1u) + c);
    }

    ASSERT(test_rel_append_row_mult(prev, rows[0], 2) == 0, "prev key1 mult=2");
    ASSERT(test_rel_append_row_mult(prev, rows[1], 4) == 0, "prev key2 mult=4");
    ASSERT(test_rel_append_row_mult(curr, rows[0], 5) == 0, "curr key1 mult=5");
    ASSERT(test_rel_append_row_mult(curr, rows[2], 7) == 0, "curr key3 mult=7");

    int rc = col_compute_delta_mobius(prev, curr, delta);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(delta->nrows == 3, "delta->nrows == 3");
    ASSERT(delta->timestamps != NULL, "delta->timestamps is non-NULL");

    static const int64_t expect_mult[3] = { 3, 7, -4 };
    static const uint32_t expect_src[3] = { 0u, 2u, 1u };
    for (uint32_t i = 0; i < 3; i++) {
        ASSERT(delta->timestamps[i].multiplicity == expect_mult[i],
            "delta multiplicity matches");
        for (uint32_t c = 0; c < WIDE_NCOLS; c++)
            ASSERT(col_rel_get(delta, i, c) == rows[expect_src[i]][c],
                "every column of the delta row is intact");
    }

    test_rel_free(prev);
    test_rel_free(curr);
    test_rel_free(delta);
    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */
/* ================================================================
 * Test 6 (Issue #1076): an out_delta carrying rows is rejected
 *
 * The append path grows out_delta only when nrows >= capacity, to
 * capacity ? capacity * 2 : 16 -- which can still be <= nrows.  An
 * out_delta handed over with nrows already past its buffer therefore
 * writes off the end: ASan reports a heap-buffer-overflow WRITE at
 * internal.h:414 reached from mobius.c.
 *
 * capacity and columns are left at 0/NULL so the pre-fix grow path
 * allocates 16 rows and the write at index 8 lands in bounds; before the
 * fix this case returns 0 and fails the assertion cleanly instead of
 * corrupting the heap.
 * ================================================================ */
static void
test_non_empty_out_delta_rejected(void)
{
    TEST("col_compute_delta_mobius rejects an out_delta with rows (#1076)");

    col_rel_t *prev = test_rel_alloc(1);
    col_rel_t *curr = test_rel_alloc(1);
    col_rel_t *out = test_rel_alloc(1);
    ASSERT(prev && curr && out, "test_rel_alloc failed");

    int64_t row[] = { 42 };
    ASSERT(test_rel_append_row_mult(curr, row, 1) == 0, "append curr row");

    out->nrows = 8;

    int rc = col_compute_delta_mobius(prev, curr, out);

    ASSERT(rc == EINVAL, "returns EINVAL for an out_delta with nrows != 0");
    ASSERT(out->nrows == 8, "out_delta is left untouched on rejection");

    test_rel_free(prev);
    test_rel_free(curr);
    test_rel_free(out);
    PASS();
}

/* ================================================================
 * Test 7 (Issue #1076): an out_delta whose arity disagrees is rejected
 *
 * out_delta->ncols is overwritten by this function, but ncols is also the
 * length bound for col_names, merge_columns and retract_backup_columns,
 * which it does not touch.  Widening it silently desyncs those from what
 * the caller allocated: col_rel_free_contents() then walks col_names to
 * the new, larger ncols and reads past the allocation (ASan:
 * heap-buffer-overflow READ, followed by free() on garbage pointers).
 *
 * The relation here is pristine in nrows/capacity/columns -- the check
 * added for shapes A and C does not catch it -- but carries one column
 * against three-column inputs.
 * ================================================================ */
static void
test_arity_mismatch_out_delta_rejected(void)
{
    TEST("col_compute_delta_mobius rejects a wrong-arity out_delta (#1076)");

    col_rel_t *prev = test_rel_alloc(3);
    col_rel_t *curr = test_rel_alloc(3);
    col_rel_t *out = test_rel_alloc(1); /* pristine, but one column */
    ASSERT(prev && curr && out, "test_rel_alloc failed");

    int64_t row[] = { 1, 2, 3 };
    ASSERT(test_rel_append_row_mult(curr, row, 1) == 0, "append curr row");

    int rc = col_compute_delta_mobius(prev, curr, out);

    ASSERT(rc == EINVAL, "returns EINVAL when out_delta->ncols != input ncols");
    ASSERT(out->ncols == 1, "out_delta->ncols is not overwritten on rejection");

    test_rel_free(prev);
    test_rel_free(curr);
    test_rel_free(out);
    PASS();
}

/* ================================================================
 * Test 8 (Issue #1076): an out_delta with a lying capacity is rejected
 *
 * This is the case the documented contract did not cover: nrows == 0 is
 * satisfied, so the old precondition was met.  The arity matches too, so
 * the check added for Test 7 does not see it either -- what is wrong is
 * that capacity claims four rows while the buffers hold one, and
 * timestamps is NULL.  Because nrows < capacity the append path skips
 * the grow that would have allocated timestamps, so the first append
 * writes out_delta->timestamps[0] through a NULL pointer.
 *
 * This case is what pins the capacity/columns half of the precondition:
 * remove only those clauses and this is the sole test that fails.
 * ================================================================ */
static void
test_lying_capacity_out_delta_rejected(void)
{
    TEST("col_compute_delta_mobius rejects a lying capacity (#1076)");

    col_rel_t *prev = test_rel_alloc(1);
    col_rel_t *curr = test_rel_alloc(1);
    col_rel_t *out = test_rel_alloc(1);
    ASSERT(prev && curr && out, "test_rel_alloc failed");

    int64_t row[] = { 7 };
    ASSERT(test_rel_append_row_mult(curr, row, 1) == 0, "append curr row");

    /* nrows stays 0 and ncols matches: only capacity/columns are wrong. */
    out->capacity = 4;
    out->columns = col_columns_alloc(1, 1);
    ASSERT(out->columns != NULL, "col_columns_alloc failed");

    int rc = col_compute_delta_mobius(prev, curr, out);

    ASSERT(rc == EINVAL, "returns EINVAL for an out_delta with capacity != 0");

    test_rel_free(prev);
    test_rel_free(curr);
    test_rel_free(out);
    PASS();
}

/* ================================================================
 * Test 9 (Issue #1182): an out_delta with timestamps is rejected
 *
 * timestamps is an owned output buffer.  Allowing it with capacity==0 would
 * pass it to realloc() during the first append, even though the caller has
 * not given the delta computation ownership of that allocation.
 * ================================================================ */
static void
test_timestamps_out_delta_rejected(void)
{
    TEST(
        "col_compute_delta_mobius rejects an out_delta with timestamps (#1182)");

    col_rel_t *prev = test_rel_alloc(1);
    col_rel_t *curr = test_rel_alloc(1);
    col_rel_t *out = test_rel_alloc(1);
    ASSERT(prev && curr && out, "test_rel_alloc failed");

    int64_t row[] = { 42 };
    ASSERT(test_rel_append_row_mult(curr, row, 1) == 0, "append curr row");

    out->timestamps = (col_delta_timestamp_t *)malloc(
        sizeof(*out->timestamps));
    ASSERT(out->timestamps != NULL, "timestamp allocation failed");

    int rc = col_compute_delta_mobius(prev, curr, out);

    ASSERT(rc == EINVAL, "returns EINVAL for an out_delta with timestamps");
    ASSERT(out->ncols == 1 && out->nrows == 0 && out->capacity == 0,
        "out_delta shape is left untouched");
    ASSERT(out->columns == NULL, "out_delta columns is left untouched");
    ASSERT(out->timestamps != NULL, "out_delta timestamps is left untouched");

    test_rel_free(prev);
    test_rel_free(curr);
    test_rel_free(out);
    PASS();
}

int
main(void)
{
    /* Unbuffered: a row-buffer overflow aborts the process, and a buffered
     * stdout would discard the name of the case that crashed.  _IONBF is the
     * only portable spelling: MSVC's UCRT rejects a size of 0 for _IOLBF and
     * _IOFBF through the invalid-parameter handler, which terminates the
     * process with 0xC0000409 -- an exit code that reads as a stack-cookie
     * failure but is not one.  Win32 also maps _IOLBF onto full buffering,
     * so it would not have flushed per line even where it was accepted. */
    setvbuf(stdout, NULL, _IONBF, 0);

    printf("=== test_mobius_delta_formula (TDD RED PHASE) ===\n\n");
    printf(
        "NOTE: Expected to FAIL at link time until col_compute_delta_mobius\n");
    printf("      is implemented with extern linkage.\n\n");

    test_collection_grows();
    test_collection_shrinks();
    test_new_key_appears();
    test_key_vanishes();
    test_wide_relation_delta();
    test_non_empty_out_delta_rejected();
    test_arity_mismatch_out_delta_rejected();
    test_lying_capacity_out_delta_rejected();
    test_timestamps_out_delta_rejected();

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n", pass_count,
        fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
