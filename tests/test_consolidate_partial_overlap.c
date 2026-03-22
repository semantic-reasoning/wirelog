/*
 * test_consolidate_partial_overlap.c - TDD tests for partial-overlap fast-paths
 *
 * Tests for Cases D, E, F, G fast-paths in col_op_consolidate_incremental_delta.
 * Issue #280: Partial-overlap consolidation fast-paths.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Test cases:
 *   1.  Case D: delta all-before-old (2-col, 3 delta + 4 old)
 *   2.  Case D: boundary (D_hi just below O_lo)
 *   3.  Case E: suffix overlap (old=[1..10], delta=[8..15])
 *   4.  Case E: below threshold (split_o < N/4, falls to slow path)
 *   5.  Case F: delta fully contained in old
 *   6.  Case F: delta contained, all duplicates (delta_out empty)
 *   7.  Case F: delta contained, some new rows
 *   8.  Case G: delta contains old (prefix+suffix all new)
 *   9.  Exact boundaries overlap (slow path, D_lo==O_lo, D_hi==O_hi)
 *  10.  Correctness oracle: random 1000 old + 200 delta
 *  11.  Path code output verification
 *  12.  Regression: cases A and B still work
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
    char *name;
    uint32_t ncols;
    int64_t *data;
    uint32_t nrows;
    uint32_t capacity;
    char **col_names;
    struct ArrowSchema schema;
    bool schema_ok;
    uint32_t sorted_nrows;
    int64_t *merge_buf;
    uint32_t merge_buf_cap;
    uint32_t base_nrows;
    col_delta_timestamp_t *timestamps;
    bool pool_owned;
    void *ledger;
} col_rel_t;

/*
 * Forward declaration of the function under test.
 */
int
col_op_consolidate_incremental_delta(col_rel_t *rel, uint32_t old_nrows,
    col_rel_t *delta_out, int *out_fast_path);

/* Expected path codes (must match cons_path_t enum once implemented) */
#define PATH_SLOW      0
#define PATH_EMPTY_OLD 1
#define PATH_APPEND    2
#define PATH_PREPEND   3
#define PATH_SUFFIX_OL 4
#define PATH_CONTAINED 5
#define PATH_CONTAINS  6

/* ----------------------------------------------------------------
 * Test framework (matches wirelog convention)
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
 * Helpers
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

static void
test_rel_free(col_rel_t *r)
{
    if (!r)
        return;
    free(r->name);
    free(r->data);
    free(r->merge_buf);
    if (r->col_names) {
        for (uint32_t i = 0; i < r->ncols; i++)
            free(r->col_names[i]);
        free((void *)r->col_names);
    }
    free(r);
}

static int
test_rel_append_row(col_rel_t *r, const int64_t *row)
{
    if (r->nrows >= r->capacity) {
        uint32_t cap = r->capacity == 0 ? 16 : r->capacity * 2;
        int64_t *nd = (int64_t *)realloc(
            r->data, (size_t)cap * r->ncols * sizeof(int64_t));
        if (!nd)
            return -1;
        r->data = nd;
        r->capacity = cap;
    }
    memcpy(r->data + (size_t)r->nrows * r->ncols, row,
        r->ncols * sizeof(int64_t));
    r->nrows++;
    return 0;
}

static int
test_row_cmp(const int64_t *a, const int64_t *b, uint32_t ncols)
{
    for (uint32_t c = 0; c < ncols; c++) {
        if (a[c] < b[c])
            return -1;
        if (a[c] > b[c])
            return 1;
    }
    return 0;
}

static int
test_rel_is_sorted(const col_rel_t *r)
{
    if (r->nrows <= 1)
        return 1;
    for (uint32_t i = 1; i < r->nrows; i++) {
        if (test_row_cmp(r->data + (size_t)(i - 1) * r->ncols,
            r->data + (size_t)i * r->ncols, r->ncols)
            >= 0)
            return 0;
    }
    return 1;
}

static int
test_rel_is_unique(const col_rel_t *r)
{
    if (r->nrows <= 1)
        return 1;
    for (uint32_t i = 1; i < r->nrows; i++) {
        if (test_row_cmp(r->data + (size_t)(i - 1) * r->ncols,
            r->data + (size_t)i * r->ncols, r->ncols)
            == 0)
            return 0;
    }
    return 1;
}

static int
test_rel_contains_row(const col_rel_t *r, const int64_t *row)
{
    for (uint32_t i = 0; i < r->nrows; i++) {
        if (test_row_cmp(r->data + (size_t)i * r->ncols, row, r->ncols)
            == 0)
            return 1;
    }
    return 0;
}

/* Helper: append a single-column row by value */
static int
test_rel_append_1col(col_rel_t *r, int64_t val)
{
    return test_rel_append_row(r, &val);
}

/* ================================================================
 * Test 1: Case D - delta all-before-old (2-col, 3 delta + 4 old)
 *
 * old  = [(10,1), (20,2), (30,3), (40,4)]  sorted
 * delta = [(1,0), (5,0), (3,0)]            unsorted, all < old[0]
 * Expected:
 *   merged = 7 rows sorted, all delta rows are new
 * ================================================================ */
static void
test_case_d_basic(void)
{
    TEST("Case D: delta all-before-old (2-col, 3+4)");

    col_rel_t *rel = test_rel_alloc(2);
    col_rel_t *delta_out = test_rel_alloc(2);
    ASSERT(rel && delta_out, "alloc failed");

    /* Old rows (sorted) */
    int64_t old0[] = {10, 1};
    int64_t old1[] = {20, 2};
    int64_t old2[] = {30, 3};
    int64_t old3[] = {40, 4};
    test_rel_append_row(rel, old0);
    test_rel_append_row(rel, old1);
    test_rel_append_row(rel, old2);
    test_rel_append_row(rel, old3);
    uint32_t old_nrows = rel->nrows;

    /* Delta rows (unsorted, all before old) */
    int64_t d0[] = {1, 0};
    int64_t d1[] = {5, 0};
    int64_t d2[] = {3, 0};
    test_rel_append_row(rel, d0);
    test_rel_append_row(rel, d1);
    test_rel_append_row(rel, d2);

    int path = -1;
    int rc = col_op_consolidate_incremental_delta(
        rel, old_nrows, delta_out, &path);

    ASSERT(rc == 0, "returns 0");
    ASSERT(rel->nrows == 7, "merged has 7 rows");
    ASSERT(test_rel_is_sorted(rel), "merged is sorted");
    ASSERT(test_rel_is_unique(rel), "merged is unique");
    ASSERT(delta_out->nrows == 3, "delta_out has 3 new rows");
    ASSERT(test_rel_contains_row(delta_out, d0), "delta_out has (1,0)");
    ASSERT(test_rel_contains_row(delta_out, d1), "delta_out has (5,0)");
    ASSERT(test_rel_contains_row(delta_out, d2), "delta_out has (3,0)");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 2: Case D - boundary (D_hi just below O_lo)
 *
 * old  = [(5,0), (10,0), (15,0)]
 * delta = [(1,0), (2,0), (4,0)]   D_hi=(4,0) < O_lo=(5,0)
 * Verifies no off-by-one at boundary.
 * ================================================================ */
static void
test_case_d_boundary(void)
{
    TEST("Case D: boundary (D_hi just below O_lo)");

    col_rel_t *rel = test_rel_alloc(2);
    col_rel_t *delta_out = test_rel_alloc(2);
    ASSERT(rel && delta_out, "alloc failed");

    int64_t old0[] = {5, 0};
    int64_t old1[] = {10, 0};
    int64_t old2[] = {15, 0};
    test_rel_append_row(rel, old0);
    test_rel_append_row(rel, old1);
    test_rel_append_row(rel, old2);
    uint32_t old_nrows = rel->nrows;

    int64_t d0[] = {1, 0};
    int64_t d1[] = {2, 0};
    int64_t d2[] = {4, 0};
    test_rel_append_row(rel, d0);
    test_rel_append_row(rel, d1);
    test_rel_append_row(rel, d2);

    int path = -1;
    int rc = col_op_consolidate_incremental_delta(
        rel, old_nrows, delta_out, &path);

    ASSERT(rc == 0, "returns 0");
    ASSERT(rel->nrows == 6, "merged has 6 rows");
    ASSERT(test_rel_is_sorted(rel), "merged is sorted");
    ASSERT(test_rel_is_unique(rel), "merged is unique");
    ASSERT(delta_out->nrows == 3, "all 3 delta rows are new");

    /* Verify first row is (1,0) and last is (15,0) */
    ASSERT(rel->data[0] == 1 && rel->data[1] == 0,
        "first merged row is (1,0)");
    ASSERT(rel->data[10] == 15 && rel->data[11] == 0,
        "last merged row is (15,0)");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 3: Case E - suffix overlap
 *
 * old  = [1,2,3,4,5,6,7,8,9,10]  (1-col, 10 rows)
 * delta = [8,9,10,11,12,13,14,15] (8 rows, overlap at 8-10)
 * Prefix [1..7] should be copied without comparison.
 * Overlap [8..10] deduped. New: [11..15].
 * ================================================================ */
static void
test_case_e_suffix_overlap(void)
{
    TEST("Case E: suffix overlap old=[1..10] delta=[8..15]");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "alloc failed");

    for (int64_t v = 1; v <= 10; v++)
        test_rel_append_1col(rel, v);
    uint32_t old_nrows = rel->nrows;

    /* Delta (unsorted to exercise sort) */
    int64_t delta_vals[] = {15, 11, 8, 13, 9, 10, 12, 14};
    for (int i = 0; i < 8; i++)
        test_rel_append_1col(rel, delta_vals[i]);

    int path = -1;
    int rc = col_op_consolidate_incremental_delta(
        rel, old_nrows, delta_out, &path);

    ASSERT(rc == 0, "returns 0");
    ASSERT(rel->nrows == 15, "merged has 15 rows");
    ASSERT(test_rel_is_sorted(rel), "merged is sorted");
    ASSERT(test_rel_is_unique(rel), "merged is unique");
    ASSERT(delta_out->nrows == 5, "delta_out has 5 new rows (11-15)");

    /* Verify new rows are 11..15 */
    for (int64_t v = 11; v <= 15; v++)
        ASSERT(test_rel_contains_row(delta_out, &v),
            "delta_out contains expected new row");

    /* Verify old rows 8,9,10 are NOT in delta_out */
    for (int64_t v = 8; v <= 10; v++)
        ASSERT(!test_rel_contains_row(delta_out, &v),
            "delta_out does not contain old dup row");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 4: Case E - below threshold (split_o < N/4)
 *
 * old  = [1..20]  (20 rows)
 * delta = [3..25]  (overlap starts at 3, so split_o=2 < 20/4=5)
 * Should fall through to slow path. Correctness still verified.
 * ================================================================ */
static void
test_case_e_below_threshold(void)
{
    TEST("Case E: below threshold (split_o < N/4, slow path)");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "alloc failed");

    for (int64_t v = 1; v <= 20; v++)
        test_rel_append_1col(rel, v);
    uint32_t old_nrows = rel->nrows;

    for (int64_t v = 3; v <= 25; v++)
        test_rel_append_1col(rel, v);

    int path = -1;
    int rc = col_op_consolidate_incremental_delta(
        rel, old_nrows, delta_out, &path);

    ASSERT(rc == 0, "returns 0");
    ASSERT(rel->nrows == 25, "merged has 25 rows");
    ASSERT(test_rel_is_sorted(rel), "merged is sorted");
    ASSERT(test_rel_is_unique(rel), "merged is unique");
    ASSERT(delta_out->nrows == 5, "5 new rows (21-25)");

    /* This scenario should NOT take the E fast-path due to threshold.
     * Once profiling is implemented, path should be SLOW (0). */

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 5: Case F - delta fully contained in old
 *
 * old  = [1..50]  (50 rows)
 * delta = [20..30] (11 rows, mix of old dupes and new)
 *   but here all delta rows exist in old
 * Prefix [1..19] and suffix [31..50] should be memcpy'd.
 * ================================================================ */
static void
test_case_f_contained(void)
{
    TEST("Case F: delta fully contained in old (with new rows)");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "alloc failed");

    /* Old: even numbers 2,4,6,...,100 (50 rows) */
    for (int64_t v = 2; v <= 100; v += 2)
        test_rel_append_1col(rel, v);
    uint32_t old_nrows = rel->nrows; /* 50 */

    /* Delta: 39,40,41,42,...,61 (mix: odd=new, even=dup) */
    for (int64_t v = 39; v <= 61; v++)
        test_rel_append_1col(rel, v);

    int path = -1;
    int rc = col_op_consolidate_incremental_delta(
        rel, old_nrows, delta_out, &path);

    ASSERT(rc == 0, "returns 0");
    ASSERT(test_rel_is_sorted(rel), "merged is sorted");
    ASSERT(test_rel_is_unique(rel), "merged is unique");

    /* Delta range [39..61] is within old range [2..100].
     * New rows: 39,41,43,45,47,49,51,53,55,57,59,61 = 12 odd numbers */
    ASSERT(delta_out->nrows == 12, "12 new rows (odd numbers 39-61)");

    /* Verify total: 50 old + 12 new = 62 */
    ASSERT(rel->nrows == 62, "merged has 62 rows");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 6: Case F - delta contained, all duplicates
 *
 * old  = [10,20,30,40,50]
 * delta = [20,30,40]  all exist in old
 * delta_out should be empty.
 * ================================================================ */
static void
test_case_f_all_duplicates(void)
{
    TEST("Case F: delta contained, all duplicates -> delta_out empty");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "alloc failed");

    int64_t old_vals[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++)
        test_rel_append_1col(rel, old_vals[i]);
    uint32_t old_nrows = rel->nrows;

    int64_t delta_vals[] = {30, 20, 40}; /* unsorted, all dups */
    for (int i = 0; i < 3; i++)
        test_rel_append_1col(rel, delta_vals[i]);

    int path = -1;
    int rc = col_op_consolidate_incremental_delta(
        rel, old_nrows, delta_out, &path);

    ASSERT(rc == 0, "returns 0");
    ASSERT(rel->nrows == 5, "nrows unchanged (all dups)");
    ASSERT(test_rel_is_sorted(rel), "merged is sorted");
    ASSERT(test_rel_is_unique(rel), "merged is unique");
    ASSERT(delta_out->nrows == 0, "delta_out empty");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 7: Case F - delta contained, some new rows
 *
 * old  = [10,20,30,40,50]
 * delta = [15,20,25,30,35]  (new: 15,25,35; dup: 20,30)
 * ================================================================ */
static void
test_case_f_some_new(void)
{
    TEST("Case F: delta contained, some new rows");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "alloc failed");

    int64_t old_vals[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++)
        test_rel_append_1col(rel, old_vals[i]);
    uint32_t old_nrows = rel->nrows;

    int64_t delta_vals[] = {25, 15, 30, 35, 20}; /* unsorted */
    for (int i = 0; i < 5; i++)
        test_rel_append_1col(rel, delta_vals[i]);

    int path = -1;
    int rc = col_op_consolidate_incremental_delta(
        rel, old_nrows, delta_out, &path);

    ASSERT(rc == 0, "returns 0");
    ASSERT(rel->nrows == 8, "merged has 8 rows (5 old + 3 new)");
    ASSERT(test_rel_is_sorted(rel), "merged is sorted");
    ASSERT(test_rel_is_unique(rel), "merged is unique");
    ASSERT(delta_out->nrows == 3, "3 new rows");

    int64_t expected_new[] = {15, 25, 35};
    for (int i = 0; i < 3; i++)
        ASSERT(test_rel_contains_row(delta_out, &expected_new[i]),
            "delta_out has expected new row");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 8: Case G - delta contains old (prefix+suffix all new)
 *
 * old  = [50,60,70]  (3 rows)
 * delta = [10,20,30,40,50,60,70,80,90,100]  (10 rows)
 * Prefix [10,20,30,40] and suffix [80,90,100] are all new.
 * Overlap [50,60,70] deduped.
 * ================================================================ */
static void
test_case_g_contains_old(void)
{
    TEST("Case G: delta contains old (prefix+suffix new)");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "alloc failed");

    int64_t old_vals[] = {50, 60, 70};
    for (int i = 0; i < 3; i++)
        test_rel_append_1col(rel, old_vals[i]);
    uint32_t old_nrows = rel->nrows;

    int64_t delta_vals[] = {100, 40, 70, 20, 80, 50, 30, 90, 10, 60};
    for (int i = 0; i < 10; i++)
        test_rel_append_1col(rel, delta_vals[i]);

    int path = -1;
    int rc = col_op_consolidate_incremental_delta(
        rel, old_nrows, delta_out, &path);

    ASSERT(rc == 0, "returns 0");
    ASSERT(rel->nrows == 10, "merged has 10 rows");
    ASSERT(test_rel_is_sorted(rel), "merged is sorted");
    ASSERT(test_rel_is_unique(rel), "merged is unique");
    ASSERT(delta_out->nrows == 7, "7 new rows");

    /* Verify new rows */
    int64_t expected_new[] = {10, 20, 30, 40, 80, 90, 100};
    for (int i = 0; i < 7; i++)
        ASSERT(test_rel_contains_row(delta_out, &expected_new[i]),
            "delta_out has expected new row");

    /* Verify old rows are NOT in delta_out */
    for (int i = 0; i < 3; i++)
        ASSERT(!test_rel_contains_row(delta_out, &old_vals[i]),
            "delta_out does not contain old dup");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 9: Exact boundaries overlap (D_lo==O_lo, D_hi==O_hi)
 *
 * old  = [10,20,30]
 * delta = [10,15,20,25,30]  (same boundaries, interleaved new)
 * Note: exact boundary match does not qualify for any fast-path case;
 * ================================================================ */
static void
test_case_g_exact_boundaries(void)
{
    TEST("Exact boundaries overlap (slow path, D_lo==O_lo, D_hi==O_hi)");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "alloc failed");

    int64_t old_vals[] = {10, 20, 30};
    for (int i = 0; i < 3; i++)
        test_rel_append_1col(rel, old_vals[i]);
    uint32_t old_nrows = rel->nrows;

    int64_t delta_vals[] = {25, 10, 30, 15, 20}; /* unsorted */
    for (int i = 0; i < 5; i++)
        test_rel_append_1col(rel, delta_vals[i]);

    int path = -1;
    int rc = col_op_consolidate_incremental_delta(
        rel, old_nrows, delta_out, &path);

    ASSERT(rc == 0, "returns 0");
    ASSERT(rel->nrows == 5, "merged has 5 rows");
    ASSERT(test_rel_is_sorted(rel), "merged is sorted");
    ASSERT(test_rel_is_unique(rel), "merged is unique");
    ASSERT(delta_out->nrows == 2, "2 new rows (15, 25)");

    int64_t v15 = 15, v25 = 25;
    ASSERT(test_rel_contains_row(delta_out, &v15), "has 15");
    ASSERT(test_rel_contains_row(delta_out, &v25), "has 25");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 10: Correctness oracle - random 1000 old + 200 delta
 *
 * Generate random data, compute expected result naively, compare.
 * ================================================================ */

/* qsort comparator for int64_t */
static int
qsort_i64_cmp(const void *a, const void *b)
{
    int64_t va = *(const int64_t *)a;
    int64_t vb = *(const int64_t *)b;
    if (va < vb)
        return -1;
    if (va > vb)
        return 1;
    return 0;
}

static void
test_correctness_oracle(void)
{
    TEST("Correctness oracle: random 1000 old + 200 delta");

    /* Use fixed seed for reproducibility */
    srand(42);

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "alloc failed");

    /* Generate 1000 unique sorted old values */
    uint32_t old_count = 1000;
    int64_t *old_vals = (int64_t *)malloc(old_count * sizeof(int64_t));
    ASSERT(old_vals, "malloc old_vals");
    {
        int64_t v = 0;
        for (uint32_t i = 0; i < old_count; i++) {
            v += 1 + (rand() % 5); /* gaps of 1-5 */
            old_vals[i] = v;
        }
    }
    for (uint32_t i = 0; i < old_count; i++)
        test_rel_append_1col(rel, old_vals[i]);
    uint32_t old_nrows = rel->nrows;

    /* Generate 200 delta values: mix of dups and new */
    uint32_t delta_count = 200;
    int64_t *delta_vals = (int64_t *)malloc(delta_count * sizeof(int64_t));
    ASSERT(delta_vals, "malloc delta_vals");
    for (uint32_t i = 0; i < delta_count; i++) {
        if (rand() % 3 == 0) {
            /* Pick a random existing old value (will be dup) */
            delta_vals[i] = old_vals[rand() % old_count];
        } else {
            /* Random value in range (may or may not collide) */
            delta_vals[i] = 1 + (rand() % (old_vals[old_count - 1] + 100));
        }
    }
    for (uint32_t i = 0; i < delta_count; i++)
        test_rel_append_1col(rel, delta_vals[i]);

    /* Compute expected: sorted unique union of old + delta */
    uint32_t all_count = old_count + delta_count;
    int64_t *all_vals = (int64_t *)malloc(all_count * sizeof(int64_t));
    ASSERT(all_vals, "malloc all_vals");
    memcpy(all_vals, old_vals, old_count * sizeof(int64_t));
    memcpy(all_vals + old_count, delta_vals, delta_count * sizeof(int64_t));
    qsort(all_vals, all_count, sizeof(int64_t), qsort_i64_cmp);

    /* Dedup */
    uint32_t expected_count = 1;
    for (uint32_t i = 1; i < all_count; i++) {
        if (all_vals[i] != all_vals[expected_count - 1])
            all_vals[expected_count++] = all_vals[i];
    }

    /* Count expected new rows (in delta but not in old) */
    uint32_t expected_new = 0;
    int64_t *sorted_delta =
        (int64_t *)malloc(delta_count * sizeof(int64_t));
    ASSERT(sorted_delta, "malloc sorted_delta");
    memcpy(sorted_delta, delta_vals, delta_count * sizeof(int64_t));
    qsort(sorted_delta, delta_count, sizeof(int64_t), qsort_i64_cmp);
    /* dedup sorted_delta */
    uint32_t sd_unique = 1;
    for (uint32_t i = 1; i < delta_count; i++) {
        if (sorted_delta[i] != sorted_delta[sd_unique - 1])
            sorted_delta[sd_unique++] = sorted_delta[i];
    }
    /* count those not in old (binary search) */
    for (uint32_t i = 0; i < sd_unique; i++) {
        /* binary search old_vals for sorted_delta[i] */
        int found = 0;
        uint32_t lo = 0, hi = old_count;
        while (lo < hi) {
            uint32_t mid = lo + (hi - lo) / 2;
            if (old_vals[mid] < sorted_delta[i])
                lo = mid + 1;
            else if (old_vals[mid] > sorted_delta[i])
                hi = mid;
            else {
                found = 1;
                break;
            }
        }
        if (!found)
            expected_new++;
    }

    /* Run consolidation */
    int path = -1;
    int rc = col_op_consolidate_incremental_delta(
        rel, old_nrows, delta_out, &path);

    ASSERT(rc == 0, "returns 0");
    ASSERT(rel->nrows == expected_count, "merged row count matches oracle");
    ASSERT(test_rel_is_sorted(rel), "merged is sorted");
    ASSERT(test_rel_is_unique(rel), "merged is unique");
    ASSERT(delta_out->nrows == expected_new,
        "delta_out count matches oracle");

    /* Verify merged content matches oracle */
    for (uint32_t i = 0; i < expected_count; i++)
        ASSERT(rel->data[i] == all_vals[i], "merged data matches oracle");

    free(old_vals);
    free(delta_vals);
    free(all_vals);
    free(sorted_delta);
    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 11: Path code output verification
 *
 * Verify that out_fast_path returns the correct path code for
 * each case (A=1, B=2, D=3, E=4, F=5, G=6, slow=0).
 * ================================================================ */
static void
test_path_code_output(void)
{
    TEST("Path code: out_fast_path returns correct case codes");

    int path;
    int rc;

    /* Case A: empty old -> PATH_EMPTY_OLD (1) */
    {
        col_rel_t *rel = test_rel_alloc(1);
        col_rel_t *dout = test_rel_alloc(1);
        test_rel_append_1col(rel, 10);
        path = -1;
        rc = col_op_consolidate_incremental_delta(rel, 0, dout, &path);
        ASSERT(rc == 0, "case A returns 0");
        ASSERT(path == PATH_EMPTY_OLD, "case A path == 1");
        test_rel_free(rel);
        test_rel_free(dout);
    }

    /* Case B: delta after old -> PATH_APPEND (2) */
    {
        col_rel_t *rel = test_rel_alloc(1);
        col_rel_t *dout = test_rel_alloc(1);
        test_rel_append_1col(rel, 10);
        test_rel_append_1col(rel, 20);
        path = -1;
        rc = col_op_consolidate_incremental_delta(rel, 1, dout, &path);
        ASSERT(rc == 0, "case B returns 0");
        ASSERT(path == PATH_APPEND, "case B path == 2");
        test_rel_free(rel);
        test_rel_free(dout);
    }

    /* Case D: delta before old -> PATH_PREPEND (3) */
    {
        col_rel_t *rel = test_rel_alloc(1);
        col_rel_t *dout = test_rel_alloc(1);
        test_rel_append_1col(rel, 100);
        test_rel_append_1col(rel, 10);
        path = -1;
        rc = col_op_consolidate_incremental_delta(rel, 1, dout, &path);
        ASSERT(rc == 0, "case D returns 0");
        ASSERT(path == PATH_PREPEND, "case D path == 3");
        test_rel_free(rel);
        test_rel_free(dout);
    }

    /* Case E: suffix overlap -> PATH_SUFFIX_OL (4) */
    {
        col_rel_t *rel = test_rel_alloc(1);
        col_rel_t *dout = test_rel_alloc(1);
        for (int64_t v = 1; v <= 10; v++)
            test_rel_append_1col(rel, v);
        /* delta overlaps suffix: 8,9,10,11,12 */
        int64_t evals[] = {8, 9, 10, 11, 12};
        for (int i = 0; i < 5; i++)
            test_rel_append_1col(rel, evals[i]);
        path = -1;
        rc = col_op_consolidate_incremental_delta(rel, 10, dout, &path);
        ASSERT(rc == 0, "case E returns 0");
        ASSERT(path == PATH_SUFFIX_OL, "case E path == 4");
        test_rel_free(rel);
        test_rel_free(dout);
    }

    /* Case F: delta contained in old -> PATH_CONTAINED (5) */
    {
        col_rel_t *rel = test_rel_alloc(1);
        col_rel_t *dout = test_rel_alloc(1);
        for (int64_t v = 1; v <= 10; v++)
            test_rel_append_1col(rel, v);
        test_rel_append_1col(rel, 5); /* dup, contained */
        path = -1;
        rc = col_op_consolidate_incremental_delta(rel, 10, dout, &path);
        ASSERT(rc == 0, "case F returns 0");
        ASSERT(path == PATH_CONTAINED, "case F path == 5");
        test_rel_free(rel);
        test_rel_free(dout);
    }

    /* Case G: delta contains old -> PATH_CONTAINS (6) */
    {
        col_rel_t *rel = test_rel_alloc(1);
        col_rel_t *dout = test_rel_alloc(1);
        test_rel_append_1col(rel, 50);
        test_rel_append_1col(rel, 10);
        test_rel_append_1col(rel, 90);
        path = -1;
        rc = col_op_consolidate_incremental_delta(rel, 1, dout, &path);
        ASSERT(rc == 0, "case G returns 0");
        ASSERT(path == PATH_CONTAINS, "case G path == 6");
        test_rel_free(rel);
        test_rel_free(dout);
    }

    PASS();
}

/* ================================================================
 * Test 12: Regression - cases A and B still work
 * ================================================================ */
static void
test_regression_cases_a_b(void)
{
    TEST("Regression: cases A and B still produce correct results");

    /* Case A: empty old */
    {
        col_rel_t *rel = test_rel_alloc(2);
        col_rel_t *dout = test_rel_alloc(2);
        int64_t r0[] = {3, 4};
        int64_t r1[] = {1, 2};
        test_rel_append_row(rel, r0);
        test_rel_append_row(rel, r1);
        int rc = col_op_consolidate_incremental_delta(rel, 0, dout, NULL);
        ASSERT(rc == 0, "case A returns 0");
        ASSERT(rel->nrows == 2, "case A: 2 rows");
        ASSERT(test_rel_is_sorted(rel), "case A: sorted");
        ASSERT(dout->nrows == 2, "case A: all 2 delta rows new");
        test_rel_free(rel);
        test_rel_free(dout);
    }

    /* Case B: delta after old */
    {
        col_rel_t *rel = test_rel_alloc(2);
        col_rel_t *dout = test_rel_alloc(2);
        int64_t old0[] = {1, 0};
        int64_t old1[] = {2, 0};
        int64_t d0[] = {3, 0};
        int64_t d1[] = {4, 0};
        test_rel_append_row(rel, old0);
        test_rel_append_row(rel, old1);
        test_rel_append_row(rel, d0);
        test_rel_append_row(rel, d1);
        int rc = col_op_consolidate_incremental_delta(rel, 2, dout, NULL);
        ASSERT(rc == 0, "case B returns 0");
        ASSERT(rel->nrows == 4, "case B: 4 rows");
        ASSERT(test_rel_is_sorted(rel), "case B: sorted");
        ASSERT(dout->nrows == 2, "case B: 2 new rows");
        test_rel_free(rel);
        test_rel_free(dout);
    }

    /* Case B with interleaved duplicates shouldn't take B */
    {
        col_rel_t *rel = test_rel_alloc(2);
        col_rel_t *dout = test_rel_alloc(2);
        int64_t old0[] = {1, 0};
        int64_t old1[] = {3, 0};
        int64_t d0[] = {2, 0};
        int64_t d1[] = {4, 0};
        test_rel_append_row(rel, old0);
        test_rel_append_row(rel, old1);
        test_rel_append_row(rel, d0);
        test_rel_append_row(rel, d1);
        int rc = col_op_consolidate_incremental_delta(rel, 2, dout, NULL);
        ASSERT(rc == 0, "interleaved returns 0");
        ASSERT(rel->nrows == 4, "interleaved: 4 rows");
        ASSERT(test_rel_is_sorted(rel), "interleaved: sorted");
        ASSERT(test_rel_is_unique(rel), "interleaved: unique");
        ASSERT(dout->nrows == 2, "interleaved: 2 new rows");
        test_rel_free(rel);
        test_rel_free(dout);
    }

    PASS();
}

/* ================================================================
 * main
 * ================================================================ */
int
main(void)
{
    printf("=== test_consolidate_partial_overlap ===\n\n");

    /* Case D tests */
    test_case_d_basic();
    test_case_d_boundary();

    /* Case E tests */
    test_case_e_suffix_overlap();
    test_case_e_below_threshold();

    /* Case F tests */
    test_case_f_contained();
    test_case_f_all_duplicates();
    test_case_f_some_new();

    /* Case G tests */
    test_case_g_contains_old();
    test_case_g_exact_boundaries();

    /* Cross-cutting */
    test_correctness_oracle();
    test_path_code_output();
    test_regression_cases_a_b();

    printf("\n=== Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf(" ===\n");

    return fail_count > 0 ? 1 : 0;
}
