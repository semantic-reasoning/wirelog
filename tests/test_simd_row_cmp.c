/*
 * test_simd_row_cmp.c - SIMD Row Comparison Unit Tests (Issue #197)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Tests row comparison correctness via col_op_consolidate_kway_merge which
 * internally calls kway_row_cmp (and after Commit 2, row_cmp_optimized).
 * This validates the SIMD path for various ncols values.
 *
 * Test cases:
 *   1.  ncols=1: equal rows (K=2 merge, dedup collapses to 1 row)
 *   2.  ncols=1: a < b ordering (merge preserves order)
 *   3.  ncols=1: a > b ordering (merge preserves order)
 *   4.  ncols=4: equal rows (AVX2 sweet spot - full vector dedup)
 *   5.  ncols=4: differ at first column
 *   6.  ncols=4: differ at last column
 *   7.  ncols=5: SIMD + scalar tail (4 + 1)
 *   8.  ncols=8: full 2x AVX2 width
 *   9.  ncols=2: negative values including INT64_MIN vs INT64_MAX ordering
 *   10. ncols=16: large row comparison
 */

#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200112L

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * ArrowSchema stub: replicates the layout used in test_consolidate_kway_merge.c
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
 * col_delta_timestamp_t stub - must match the real definition exactly.
 */
typedef struct {
    uint32_t iteration;
    uint32_t stratum;
    uint32_t worker;
    uint32_t _reserved;
    int64_t multiplicity;
} col_delta_timestamp_t;

/*
 * col_rel_t stub - must match the private implementation layout.
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
} col_rel_t;

/*
 * Forward declaration of the function under test.
 */
int
col_op_consolidate_kway_merge(col_rel_t *rel, const uint32_t *seg_boundaries,
                              uint32_t seg_count);

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
                free(r->col_names);
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
        free(r->col_names);
    }
    free(r);
}

/*
 * Helper: set rows in a relation from a flat int64_t array.
 * data_vals has nrows * ncols values in row-major order.
 */
static int
test_rel_set_rows(col_rel_t *r, const int64_t *data_vals, uint32_t nrows)
{
    size_t total = (size_t)nrows * r->ncols;
    int64_t *buf = (int64_t *)malloc(total * sizeof(int64_t));
    if (!buf)
        return -1;
    memcpy(buf, data_vals, total * sizeof(int64_t));
    free(r->data);
    r->data = buf;
    r->nrows = nrows;
    r->capacity = nrows;
    return 0;
}

/* ----------------------------------------------------------------
 * Test cases
 * ---------------------------------------------------------------- */

/*
 * Test 1: ncols=1, equal rows.
 * Two segments each with row {42}. K=2 merge should dedup to 1 row.
 */
static void
test_ncols1_equal_rows(void)
{
    TEST("ncols=1: equal rows dedup via K=2 merge");

    col_rel_t *rel = test_rel_alloc(1);
    ASSERT(rel != NULL, "alloc failed");

    int64_t data[] = { 42, 42 }; /* 2 rows, 1 col */
    ASSERT(test_rel_set_rows(rel, data, 2) == 0, "set_rows failed");

    uint32_t segs[] = { 0, 1, 2 };
    int rc = col_op_consolidate_kway_merge(rel, segs, 2);
    ASSERT(rc == 0, "kway_merge returned error");
    ASSERT(rel->nrows == 1, "expected 1 deduplicated row");
    ASSERT(rel->data[0] == 42, "wrong value after dedup");

    test_rel_free(rel);
    PASS();
}

/*
 * Test 2: ncols=1, a < b ordering.
 * Two segments: {10} and {20}. Merge should produce sorted {10, 20}.
 */
static void
test_ncols1_a_lt_b(void)
{
    TEST("ncols=1: a < b ordering preserved");

    col_rel_t *rel = test_rel_alloc(1);
    ASSERT(rel != NULL, "alloc failed");

    int64_t data[] = { 10, 20 };
    ASSERT(test_rel_set_rows(rel, data, 2) == 0, "set_rows failed");

    uint32_t segs[] = { 0, 1, 2 };
    int rc = col_op_consolidate_kway_merge(rel, segs, 2);
    ASSERT(rc == 0, "kway_merge returned error");
    ASSERT(rel->nrows == 2, "expected 2 rows");
    ASSERT(rel->data[0] == 10, "first row wrong");
    ASSERT(rel->data[1] == 20, "second row wrong");

    test_rel_free(rel);
    PASS();
}

/*
 * Test 3: ncols=1, a > b ordering.
 * Two segments: {20} and {10}. Merge should produce sorted {10, 20}.
 */
static void
test_ncols1_a_gt_b(void)
{
    TEST("ncols=1: a > b ordering corrected by merge");

    col_rel_t *rel = test_rel_alloc(1);
    ASSERT(rel != NULL, "alloc failed");

    int64_t data[] = { 20, 10 };
    ASSERT(test_rel_set_rows(rel, data, 2) == 0, "set_rows failed");

    uint32_t segs[] = { 0, 1, 2 };
    int rc = col_op_consolidate_kway_merge(rel, segs, 2);
    ASSERT(rc == 0, "kway_merge returned error");
    ASSERT(rel->nrows == 2, "expected 2 rows");
    ASSERT(rel->data[0] == 10, "first row wrong");
    ASSERT(rel->data[1] == 20, "second row wrong");

    test_rel_free(rel);
    PASS();
}

/*
 * Test 4: ncols=4, equal rows (AVX2 sweet spot).
 * Two segments each with row {1,2,3,4}. Should dedup to 1 row.
 */
static void
test_ncols4_equal_rows(void)
{
    TEST("ncols=4: equal rows dedup (AVX2 sweet spot)");

    col_rel_t *rel = test_rel_alloc(4);
    ASSERT(rel != NULL, "alloc failed");

    int64_t data[] = { 1, 2, 3, 4, 1, 2, 3, 4 }; /* 2 rows, 4 cols */
    ASSERT(test_rel_set_rows(rel, data, 2) == 0, "set_rows failed");

    uint32_t segs[] = { 0, 1, 2 };
    int rc = col_op_consolidate_kway_merge(rel, segs, 2);
    ASSERT(rc == 0, "kway_merge returned error");
    ASSERT(rel->nrows == 1, "expected 1 deduplicated row");
    ASSERT(rel->data[0] == 1, "col0 wrong");
    ASSERT(rel->data[1] == 2, "col1 wrong");
    ASSERT(rel->data[2] == 3, "col2 wrong");
    ASSERT(rel->data[3] == 4, "col3 wrong");

    test_rel_free(rel);
    PASS();
}

/*
 * Test 5: ncols=4, differ at first column.
 * Rows {1,2,3,4} and {2,2,3,4}. Order by col0: {1,...} < {2,...}.
 */
static void
test_ncols4_differ_first_col(void)
{
    TEST("ncols=4: differ at first column");

    col_rel_t *rel = test_rel_alloc(4);
    ASSERT(rel != NULL, "alloc failed");

    int64_t data[] = { 1, 2, 3, 4, 2, 2, 3, 4 };
    ASSERT(test_rel_set_rows(rel, data, 2) == 0, "set_rows failed");

    uint32_t segs[] = { 0, 1, 2 };
    int rc = col_op_consolidate_kway_merge(rel, segs, 2);
    ASSERT(rc == 0, "kway_merge returned error");
    ASSERT(rel->nrows == 2, "expected 2 rows");
    ASSERT(rel->data[0] == 1, "row0 col0 wrong");
    ASSERT(rel->data[4] == 2, "row1 col0 wrong");

    test_rel_free(rel);
    PASS();
}

/*
 * Test 6: ncols=4, differ at last column.
 * Rows {1,2,3,4} and {1,2,3,5}. Order by last col.
 */
static void
test_ncols4_differ_last_col(void)
{
    TEST("ncols=4: differ at last column");

    col_rel_t *rel = test_rel_alloc(4);
    ASSERT(rel != NULL, "alloc failed");

    int64_t data[] = { 1, 2, 3, 4, 1, 2, 3, 5 };
    ASSERT(test_rel_set_rows(rel, data, 2) == 0, "set_rows failed");

    uint32_t segs[] = { 0, 1, 2 };
    int rc = col_op_consolidate_kway_merge(rel, segs, 2);
    ASSERT(rc == 0, "kway_merge returned error");
    ASSERT(rel->nrows == 2, "expected 2 rows");
    ASSERT(rel->data[3] == 4, "row0 col3 wrong");
    ASSERT(rel->data[7] == 5, "row1 col3 wrong");

    test_rel_free(rel);
    PASS();
}

/*
 * Test 7: ncols=5, SIMD + scalar tail (4 + 1 elements).
 * Rows {1,2,3,4,5} and {1,2,3,4,6}. Differ at col4 (scalar tail).
 */
static void
test_ncols5_simd_scalar_tail(void)
{
    TEST("ncols=5: SIMD + scalar tail differ at col4");

    col_rel_t *rel = test_rel_alloc(5);
    ASSERT(rel != NULL, "alloc failed");

    int64_t data[] = { 1, 2, 3, 4, 5, 1, 2, 3, 4, 6 };
    ASSERT(test_rel_set_rows(rel, data, 2) == 0, "set_rows failed");

    uint32_t segs[] = { 0, 1, 2 };
    int rc = col_op_consolidate_kway_merge(rel, segs, 2);
    ASSERT(rc == 0, "kway_merge returned error");
    ASSERT(rel->nrows == 2, "expected 2 rows");
    ASSERT(rel->data[4] == 5, "row0 col4 wrong");
    ASSERT(rel->data[9] == 6, "row1 col4 wrong");

    test_rel_free(rel);
    PASS();
}

/*
 * Test 8: ncols=8, full 2x AVX2 width.
 * Equal rows with 8 columns - tests full two-vector dedup path.
 */
static void
test_ncols8_full_avx2_width(void)
{
    TEST("ncols=8: full 2x AVX2 width dedup");

    col_rel_t *rel = test_rel_alloc(8);
    ASSERT(rel != NULL, "alloc failed");

    int64_t data[] = {
        10, 20, 30, 40, 50, 60, 70, 80, /* row 0 */
        10, 20, 30, 40, 50, 60, 70, 80  /* row 1 (duplicate) */
    };
    ASSERT(test_rel_set_rows(rel, data, 2) == 0, "set_rows failed");

    uint32_t segs[] = { 0, 1, 2 };
    int rc = col_op_consolidate_kway_merge(rel, segs, 2);
    ASSERT(rc == 0, "kway_merge returned error");
    ASSERT(rel->nrows == 1, "expected 1 deduplicated row");
    ASSERT(rel->data[0] == 10, "col0 wrong");
    ASSERT(rel->data[7] == 80, "col7 wrong");

    test_rel_free(rel);
    PASS();
}

/*
 * Test 9: ncols=2, negative values including INT64_MIN vs INT64_MAX.
 * Rows {INT64_MIN, 0} and {INT64_MAX, 0}. INT64_MIN < INT64_MAX.
 */
static void
test_negative_values_int64_extremes(void)
{
    TEST("ncols=2: negative values INT64_MIN < INT64_MAX ordering");

    col_rel_t *rel = test_rel_alloc(2);
    ASSERT(rel != NULL, "alloc failed");

    int64_t data[] = {
        INT64_MIN, 0, /* row 0 */
        INT64_MAX, 0  /* row 1 */
    };
    ASSERT(test_rel_set_rows(rel, data, 2) == 0, "set_rows failed");

    uint32_t segs[] = { 0, 1, 2 };
    int rc = col_op_consolidate_kway_merge(rel, segs, 2);
    ASSERT(rc == 0, "kway_merge returned error");
    ASSERT(rel->nrows == 2, "expected 2 rows");
    ASSERT(rel->data[0] == INT64_MIN, "first row should be INT64_MIN");
    ASSERT(rel->data[2] == INT64_MAX, "second row should be INT64_MAX");

    test_rel_free(rel);
    PASS();
}

/*
 * Test 10: ncols=16, large row comparison.
 * Rows differ at col15 (last) to exercise full SIMD + tail path.
 */
static void
test_ncols16_large_row(void)
{
    TEST("ncols=16: large row differ at last column");

    col_rel_t *rel = test_rel_alloc(16);
    ASSERT(rel != NULL, "alloc failed");

    int64_t data[32];
    /* row 0: all 1s except col15 = 100 */
    for (int i = 0; i < 16; i++)
        data[i] = 1;
    data[15] = 100;
    /* row 1: all 1s except col15 = 200 */
    for (int i = 16; i < 32; i++)
        data[i] = 1;
    data[31] = 200;

    ASSERT(test_rel_set_rows(rel, data, 2) == 0, "set_rows failed");

    uint32_t segs[] = { 0, 1, 2 };
    int rc = col_op_consolidate_kway_merge(rel, segs, 2);
    ASSERT(rc == 0, "kway_merge returned error");
    ASSERT(rel->nrows == 2, "expected 2 distinct rows");
    ASSERT(rel->data[15] == 100, "row0 col15 wrong");
    ASSERT(rel->data[31] == 200, "row1 col15 wrong");

    test_rel_free(rel);
    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */
int
main(void)
{
    printf("=== SIMD Row Comparison Tests (Issue #197) ===\n");

    test_ncols1_equal_rows();
    test_ncols1_a_lt_b();
    test_ncols1_a_gt_b();
    test_ncols4_equal_rows();
    test_ncols4_differ_first_col();
    test_ncols4_differ_last_col();
    test_ncols5_simd_scalar_tail();
    test_ncols8_full_avx2_width();
    test_negative_values_int64_extremes();
    test_ncols16_large_row();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf(" ---\n");

    return (fail_count > 0) ? 1 : 0;
}
