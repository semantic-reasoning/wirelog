/*
 * test_consolidate_kway_merge.c - TDD RED PHASE
 * Tests for col_op_consolidate_kway_merge
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * These tests define expected behaviour BEFORE the function is implemented
 * (US-002 RED phase).  Expected failure mode:
 *
 *   undefined reference to `col_op_consolidate_kway_merge`
 *
 * Test cases:
 *   1. Single copy (K=1) passes through unchanged
 *   2. Two copies (K=2) direct merge produces correct row order
 *   3. Three copies (K=3) heap merge correctly identified and merged
 *   4. Merged output is lexicographically sorted (per-segment qsort)
 *   5. Duplicate row dedup works across merge boundaries
 *   6. Large dataset (1000+ rows, 3 copies) merge performance
 *   7. Empty middle segment (edge case)
 */

#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200112L

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _MSC_VER
#include <time.h>
#else
#include <windows.h>
#endif

/*
 * ArrowSchema stub: replicates the layout of struct ArrowSchema from
 * nanoarrow.h so that col_rel_t has the correct field offsets without
 * pulling in the nanoarrow dependency.
 */
#include "../wirelog/columnar/internal.h"

/*
 * Forward declaration of the function under test.
 * RED phase: does not exist -> link error (expected).
 *
 * col_op_consolidate_kway_merge:
 *   rel           - relation containing K concatenated sorted segments
 *   seg_boundaries - array of (seg_count+1) offsets [s0, s1, ..., sK]
 *                    where segment i spans rows [seg_boundaries[i], seg_boundaries[i+1])
 *   seg_count     - number of segments K
 *
 * Returns 0 on success.  On return, rel contains the merged, sorted,
 * deduplicated result.
 */
int
col_op_consolidate_kway_merge(col_rel_t *rel, const uint32_t *seg_boundaries,
    uint32_t seg_count);

/* ----------------------------------------------------------------
 * Test framework (matches wirelog convention: test_consolidate_incremental_delta.c)
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
 * Helper: allocate col_rel_t with ncols columns and no rows.
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
static int test_row_cmp(const int64_t *a, const int64_t *b, uint32_t ncols);

static int test_row_cmp_rel(const col_rel_t *r, uint32_t row, const int64_t *b,
    uint32_t ncols)
{
    int64_t buf[64]; col_rel_row_copy_out(r, row, buf);
    return test_row_cmp(buf, b, ncols);
}

static int test_flat_cmp(const col_rel_t *a, const col_rel_t *b)
{
    if (a->nrows != b->nrows || a->ncols != b->ncols) return 1;
    for (uint32_t i = 0; i < a->nrows; i++) for (uint32_t c = 0; c < a->ncols;
            c++)
            if (col_rel_get(a, i, c) != col_rel_get(b, i, c)) return 1;
    return 0;
}

static int test_row_match(const col_rel_t *r, uint32_t row,
    const int64_t *target)
{
    for (uint32_t c = 0; c < r->ncols;
        c++) if (col_rel_get(r, row, c) != target[c]) return 0; return 1;
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
    col_columns_free(r->columns, r->ncols);
    free(r->row_scratch);
    if (r->col_names) {
        for (uint32_t i = 0; i < r->ncols; i++)
            free(r->col_names[i]);
        free((void *)r->col_names);
    }
    free(r);
}

/* ----------------------------------------------------------------
 * Helper: append one row, growing buffer as needed.
 * Returns 0 on success, -1 on ENOMEM.
 * ---------------------------------------------------------------- */
static int
test_rel_append_row(col_rel_t *r, const int64_t *row)
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
        r->capacity = cap;
    }
    col_rel_row_copy_in(r, r->nrows, row);
    r->nrows++;
    return 0;
}

/* ----------------------------------------------------------------
 * Helper: lexicographic int64_t row comparison.
 * ---------------------------------------------------------------- */
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

/* ----------------------------------------------------------------
 * Helper: 1 if relation is strictly sorted (lex, ascending, no dups).
 * ---------------------------------------------------------------- */
static int
test_rel_is_sorted_unique(const col_rel_t *r)
{
    if (r->nrows <= 1)
        return 1;
    for (uint32_t i = 1; i < r->nrows; i++) {
        int cmp;
        { int64_t _a[32], _b[32]; col_rel_row_copy_out(r, i-1, _a);
          col_rel_row_copy_out(r, i, _b); cmp = test_row_cmp(_a, _b, r->ncols);
        };
        if (cmp >= 0)
            return 0;
    }
    return 1;
}

/* ----------------------------------------------------------------
 * Helper: 1 if row is present in rel (linear scan).
 * ---------------------------------------------------------------- */
static int
test_rel_contains_row(const col_rel_t *r, const int64_t *row)
{
    for (uint32_t i = 0; i < r->nrows; i++) {
        if ((test_row_match(r, i, row)))
            return 1;
    }
    return 0;
}

/* ----------------------------------------------------------------
 * Helper: 1 if two relations have identical row sets (order-sensitive).
 * ---------------------------------------------------------------- */
static int
test_rel_equals(const col_rel_t *a, const col_rel_t *b)
{
    if (a->nrows != b->nrows || a->ncols != b->ncols)
        return 0;
    return (test_flat_cmp(a, b) == 0);
}

/* ================================================================
 * Test 1: Single copy (K=1) passes through unchanged
 *
 * Input:  10 sorted unique rows in one segment
 *         seg_boundaries = [0, 10], seg_count = 1
 * Expected:
 *   rel->nrows == 10
 *   output is sorted and deduplicated
 *   all original rows present
 * ================================================================ */
static void
test_single_copy_passthrough(void)
{
    TEST("single copy (K=1) passes through unchanged");

    col_rel_t *rel = test_rel_alloc(2);
    ASSERT(rel != NULL, "test_rel_alloc failed");

    /* Insert 10 sorted unique rows: (0,1),(2,3),...,(18,19) */
    for (int i = 0; i < 10; i++) {
        int64_t row[2] = { (int64_t)(i * 2), (int64_t)(i * 2 + 1) };
        ASSERT(test_rel_append_row(rel, row) == 0, "append row");
    }

    uint32_t seg_boundaries[2] = { 0, 10 };
    int rc = col_op_consolidate_kway_merge(rel, seg_boundaries, 1);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(rel->nrows == 10, "rel->nrows == 10 (unchanged)");
    ASSERT(test_rel_is_sorted_unique(rel), "output is sorted and unique");

    /* Spot-check a few rows */
    int64_t r0[2] = { 0, 1 };
    int64_t r9[2] = { 18, 19 };
    ASSERT(test_rel_contains_row(rel, r0), "row(0,1) present");
    ASSERT(test_rel_contains_row(rel, r9), "row(18,19) present");

    test_rel_free(rel);
    PASS();
}

/* ================================================================
 * Test 2: Two copies (K=2) detected via CONCAT markers, merge produces
 * correct row order
 *
 * Input:
 *   Copy 0: [(1,'a'), (3,'c'), (5,'e')] - already sorted (3 rows)
 *   Copy 1: [(2,'b'), (4,'d')]           - already sorted (2 rows)
 *   Total: 5 rows, seg_boundaries = [0, 3, 5]
 *
 * Using int64 encoding: 'a'=1, 'b'=2, 'c'=3, 'd'=4, 'e'=5
 *   Copy 0: [(1,1), (3,3), (5,5)]
 *   Copy 1: [(2,2), (4,4)]
 *
 * Expected: merged in lex order:
 *   [(1,1), (2,2), (3,3), (4,4), (5,5)]
 * ================================================================ */
static void
test_two_copies_direct_merge(void)
{
    TEST("two copies (K=2) merge produces correct row order");

    col_rel_t *rel = test_rel_alloc(2);
    ASSERT(rel != NULL, "test_rel_alloc failed");

    /* Copy 0: (1,1),(3,3),(5,5) */
    int64_t c0[3][2] = { { 1, 1 }, { 3, 3 }, { 5, 5 } };
    for (int i = 0; i < 3; i++)
        ASSERT(test_rel_append_row(rel, c0[i]) == 0, "append copy0 row");

    /* Copy 1: (2,2),(4,4) */
    int64_t c1[2][2] = { { 2, 2 }, { 4, 4 } };
    for (int i = 0; i < 2; i++)
        ASSERT(test_rel_append_row(rel, c1[i]) == 0, "append copy1 row");

    uint32_t seg_boundaries[3] = { 0, 3, 5 };
    int rc = col_op_consolidate_kway_merge(rel, seg_boundaries, 2);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(rel->nrows == 5, "rel->nrows == 5 after 2-way merge");
    ASSERT(test_rel_is_sorted_unique(rel),
        "merged output is sorted and unique");

    /* Verify exact merged order */
    int64_t expected[5][2]
        = { { 1, 1 }, { 2, 2 }, { 3, 3 }, { 4, 4 }, { 5, 5 } };
    for (int i = 0; i < 5; i++) {
        int64_t _rp[32]; col_rel_row_copy_out(rel, i, _rp);
        int64_t *row_ptr = _rp;
        ASSERT(test_row_cmp(row_ptr, expected[i], rel->ncols) == 0,
            "row at position i has wrong value");
        (void)test_rel_equals; /* suppress unused warning */
    }

    test_rel_free(rel);
    PASS();
}

/* ================================================================
 * Test 3: Three copies (K=3) correctly identified and merged
 *
 * Input:
 *   Copy 0: [(1,1), (6,6)]  (2 rows)
 *   Copy 1: [(2,2), (5,5)]  (2 rows)
 *   Copy 2: [(3,3), (4,4)]  (2 rows)
 *   seg_boundaries = [0, 2, 4, 6]
 *
 * Expected merged: [(1,1),(2,2),(3,3),(4,4),(5,5),(6,6)]
 * ================================================================ */
static void
test_three_copies_heap_merge(void)
{
    TEST("three copies (K=3) correctly identified and merged");

    col_rel_t *rel = test_rel_alloc(2);
    ASSERT(rel != NULL, "test_rel_alloc failed");

    int64_t c0[2][2] = { { 1, 1 }, { 6, 6 } };
    int64_t c1[2][2] = { { 2, 2 }, { 5, 5 } };
    int64_t c2[2][2] = { { 3, 3 }, { 4, 4 } };

    for (int i = 0; i < 2; i++)
        ASSERT(test_rel_append_row(rel, c0[i]) == 0, "append copy0 row");
    for (int i = 0; i < 2; i++)
        ASSERT(test_rel_append_row(rel, c1[i]) == 0, "append copy1 row");
    for (int i = 0; i < 2; i++)
        ASSERT(test_rel_append_row(rel, c2[i]) == 0, "append copy2 row");

    uint32_t seg_boundaries[4] = { 0, 2, 4, 6 };
    int rc = col_op_consolidate_kway_merge(rel, seg_boundaries, 3);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(rel->nrows == 6, "rel->nrows == 6 after 3-way merge");
    ASSERT(test_rel_is_sorted_unique(rel),
        "merged output is sorted and unique");

    int64_t expected[6][2]
        = { { 1, 1 }, { 2, 2 }, { 3, 3 }, { 4, 4 }, { 5, 5 }, { 6, 6 } };
    for (int i = 0; i < 6; i++) {
        int64_t _rp[32]; col_rel_row_copy_out(rel, i, _rp);
        int64_t *row_ptr = _rp;
        ASSERT(test_row_cmp(row_ptr, expected[i], rel->ncols) == 0,
            "row at position i has wrong value");
    }

    test_rel_free(rel);
    PASS();
}

/* ================================================================
 * Test 4: Merged output is lexicographically sorted (per-segment sort)
 *
 * Input (segments deliberately unsorted internally):
 *   Copy 0: [(5,5), (1,1)]   (unsorted)
 *   Copy 1: [(4,4), (2,2)]   (unsorted)
 *   seg_boundaries = [0, 2, 4]
 *
 * Expected: per-segment qsort first, then merge:
 *   [(1,1), (2,2), (4,4), (5,5)]
 * ================================================================ */
static void
test_per_segment_sort_before_merge(void)
{
    TEST("per-segment qsort before merge produces lexicographically sorted "
        "output");

    col_rel_t *rel = test_rel_alloc(2);
    ASSERT(rel != NULL, "test_rel_alloc failed");

    /* Copy 0: unsorted (5,5),(1,1) */
    int64_t c0[2][2] = { { 5, 5 }, { 1, 1 } };
    /* Copy 1: unsorted (4,4),(2,2) */
    int64_t c1[2][2] = { { 4, 4 }, { 2, 2 } };

    for (int i = 0; i < 2; i++)
        ASSERT(test_rel_append_row(rel, c0[i]) == 0, "append copy0 row");
    for (int i = 0; i < 2; i++)
        ASSERT(test_rel_append_row(rel, c1[i]) == 0, "append copy1 row");

    uint32_t seg_boundaries[3] = { 0, 2, 4 };
    int rc = col_op_consolidate_kway_merge(rel, seg_boundaries, 2);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(rel->nrows == 4, "rel->nrows == 4 after merge");
    ASSERT(test_rel_is_sorted_unique(rel), "output is sorted and unique");

    int64_t expected[4][2] = { { 1, 1 }, { 2, 2 }, { 4, 4 }, { 5, 5 } };
    for (int i = 0; i < 4; i++) {
        int64_t _rp[32]; col_rel_row_copy_out(rel, i, _rp);
        int64_t *row_ptr = _rp;
        ASSERT(test_row_cmp(row_ptr, expected[i], rel->ncols) == 0,
            "row at position i has wrong value after sort+merge");
    }

    test_rel_free(rel);
    PASS();
}

/* ================================================================
 * Test 5: Duplicate row dedup works across merge boundaries
 *
 * Input:
 *   Copy 0: [(1,1), (3,3)]
 *   Copy 1: [(3,3), (5,5)]   <- (3,3) is a cross-segment duplicate
 *   seg_boundaries = [0, 2, 4]
 *
 * Expected after dedup: [(1,1), (3,3), (5,5)] - only one (3,3) kept
 * ================================================================ */
static void
test_cross_segment_dedup(void)
{
    TEST("duplicate row dedup works across merge boundaries");

    col_rel_t *rel = test_rel_alloc(2);
    ASSERT(rel != NULL, "test_rel_alloc failed");

    /* Copy 0: (1,1),(3,3) */
    int64_t c0[2][2] = { { 1, 1 }, { 3, 3 } };
    /* Copy 1: (3,3),(5,5) -- (3,3) is duplicated */
    int64_t c1[2][2] = { { 3, 3 }, { 5, 5 } };

    for (int i = 0; i < 2; i++)
        ASSERT(test_rel_append_row(rel, c0[i]) == 0, "append copy0 row");
    for (int i = 0; i < 2; i++)
        ASSERT(test_rel_append_row(rel, c1[i]) == 0, "append copy1 row");

    uint32_t seg_boundaries[3] = { 0, 2, 4 };
    int rc = col_op_consolidate_kway_merge(rel, seg_boundaries, 2);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(rel->nrows == 3, "rel->nrows == 3 (one dup removed)");
    ASSERT(test_rel_is_sorted_unique(rel),
        "output is sorted and unique (no dups)");

    int64_t expected[3][2] = { { 1, 1 }, { 3, 3 }, { 5, 5 } };
    for (int i = 0; i < 3; i++) {
        int64_t _rp[32]; col_rel_row_copy_out(rel, i, _rp);
        int64_t *row_ptr = _rp;
        ASSERT(test_row_cmp(row_ptr, expected[i], rel->ncols) == 0,
            "row at position i has wrong value after dedup");
    }

    test_rel_free(rel);
    PASS();
}

/* ================================================================
 * Test 6: Large dataset (1000+ rows, 3 copies) merge performance
 *
 * Each copy: 334 pre-sorted rows with some overlap between copies
 *   Copy 0: rows with col0 = 0, 3, 6, ..., 999   (333 rows, stride 3)
 *   Copy 1: rows with col0 = 1, 4, 7, ..., 1000  (334 rows, stride 3)
 *   Copy 2: rows with col0 = 2, 5, 8, ..., 1001  (334 rows, stride 3)
 *   Total input: 1001 rows, output should be 1002 unique sorted rows
 *
 * Timing constraint: < 100ms (O(M log K) merge, K=3)
 * ================================================================ */
static void
test_large_dataset_performance(void)
{
    TEST("large dataset (1000+ rows, K=3) merge is O(M log K) fast");

    const uint32_t ROWS_PER_COPY = 334;
    col_rel_t *rel = test_rel_alloc(2);
    ASSERT(rel != NULL, "test_rel_alloc failed");

    /* Copy 0: col0 = 0, 3, 6, ..., (ROWS_PER_COPY-1)*3 */
    for (uint32_t i = 0; i < ROWS_PER_COPY; i++) {
        int64_t row[2] = { (int64_t)(i * 3), (int64_t)(i * 3) };
        ASSERT(test_rel_append_row(rel, row) == 0, "append copy0 row");
    }
    uint32_t boundary0 = rel->nrows; /* = ROWS_PER_COPY */

    /* Copy 1: col0 = 1, 4, 7, ..., (ROWS_PER_COPY-1)*3+1 */
    for (uint32_t i = 0; i < ROWS_PER_COPY; i++) {
        int64_t row[2] = { (int64_t)(i * 3 + 1), (int64_t)(i * 3 + 1) };
        ASSERT(test_rel_append_row(rel, row) == 0, "append copy1 row");
    }
    uint32_t boundary1 = rel->nrows; /* = 2 * ROWS_PER_COPY */

    /* Copy 2: col0 = 2, 5, 8, ..., (ROWS_PER_COPY-1)*3+2 */
    for (uint32_t i = 0; i < ROWS_PER_COPY; i++) {
        int64_t row[2] = { (int64_t)(i * 3 + 2), (int64_t)(i * 3 + 2) };
        ASSERT(test_rel_append_row(rel, row) == 0, "append copy2 row");
    }
    uint32_t total_rows = rel->nrows; /* = 3 * ROWS_PER_COPY */

    uint32_t seg_boundaries[4] = { 0, boundary0, boundary1, total_rows };

#ifndef _MSC_VER
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    int rc = col_op_consolidate_kway_merge(rel, seg_boundaries, 3);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    uint64_t elapsed_ns = (uint64_t)(t1.tv_sec - t0.tv_sec) * 1000000000ULL
        + (uint64_t)t1.tv_nsec - (uint64_t)t0.tv_nsec;
    uint64_t elapsed_ms = elapsed_ns / 1000000;
#else
    DWORD t0 = GetTickCount();
    int rc = col_op_consolidate_kway_merge(rel, seg_boundaries, 3);
    DWORD t1 = GetTickCount();
    uint64_t elapsed_ms = (uint64_t)(t1 - t0);
#endif

    ASSERT(rc == 0, "returns 0 on success");

    /* All 3*ROWS_PER_COPY rows are unique (stride-3 interleaved), expect all present */
    ASSERT(rel->nrows == 3 * ROWS_PER_COPY,
        "rel->nrows == 3*ROWS_PER_COPY (all unique)");
    ASSERT(test_rel_is_sorted_unique(rel), "output is sorted and unique");

    /* Spot-check first, middle, and last rows */
    int64_t first_row[2] = { 0, 0 };
    int64_t last_row[2] = { (int64_t)((ROWS_PER_COPY - 1) * 3 + 2),
                            (int64_t)((ROWS_PER_COPY - 1) * 3 + 2) };
    ASSERT(test_rel_contains_row(rel, first_row), "first row present");
    ASSERT(test_rel_contains_row(rel, last_row), "last row present");

    /* Timing check: < 100ms */
    if (elapsed_ms >= 100) {
        printf("WARN: merge took %" PRIu64 " ms (expected < 100ms)\n",
            elapsed_ms);
    }
    ASSERT(elapsed_ms < 100, "merge completed in < 100ms (O(M log K))");

    test_rel_free(rel);
    PASS();
}

/* ================================================================
 * Test 7: Empty middle segment (edge case)
 *
 * Input:
 *   Copy 0: [(1,1), (2,2)]  (2 rows)
 *   Copy 1: (empty)         (0 rows)
 *   Copy 2: [(3,3)]         (1 row)
 *   seg_boundaries = [0, 2, 2, 3]  <- [2,2] is empty segment
 *
 * Expected: merge skips empty segment -> [(1,1), (2,2), (3,3)]
 * ================================================================ */
static void
test_empty_middle_segment(void)
{
    TEST("empty middle segment (K=3 with one empty) is handled gracefully");

    col_rel_t *rel = test_rel_alloc(2);
    ASSERT(rel != NULL, "test_rel_alloc failed");

    /* Copy 0: (1,1),(2,2) */
    int64_t c0[2][2] = { { 1, 1 }, { 2, 2 } };
    for (int i = 0; i < 2; i++)
        ASSERT(test_rel_append_row(rel, c0[i]) == 0, "append copy0 row");

    /* Copy 1: empty (no rows appended) */

    /* Copy 2: (3,3) */
    int64_t c2[1][2] = { { 3, 3 } };
    ASSERT(test_rel_append_row(rel, c2[0]) == 0, "append copy2 row");

    /* seg_boundaries: [0, 2, 2, 3] - middle segment [2,2) is empty */
    uint32_t seg_boundaries[4] = { 0, 2, 2, 3 };
    int rc = col_op_consolidate_kway_merge(rel, seg_boundaries, 3);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(rel->nrows == 3, "rel->nrows == 3 (empty segment skipped)");
    ASSERT(test_rel_is_sorted_unique(rel), "output is sorted and unique");

    int64_t expected[3][2] = { { 1, 1 }, { 2, 2 }, { 3, 3 } };
    for (int i = 0; i < 3; i++) {
        int64_t _rp[32]; col_rel_row_copy_out(rel, i, _rp);
        int64_t *row_ptr = _rp;
        ASSERT(test_row_cmp(row_ptr, expected[i], rel->ncols) == 0,
            "row at position i has wrong value (empty segment case)");
    }

    test_rel_free(rel);
    PASS();
}

/* ================================================================
 * Test 8: Large unsorted K=2 - verifies sort phase correctness
 *
 * Input: two segments of 3000 unique rows each, intentionally shuffled
 *        (not pre-sorted) to explicitly exercise the per-segment qsort.
 *        ncols=4 to match typical CRDT tuple width and SIMD lane width.
 *
 * Expected: all 6000 unique rows present, output sorted and unique.
 *
 * TDD note: this test is written BEFORE the SIMD sort optimisation
 * (issue #300) to guard against correctness regressions.
 * ================================================================ */
#define T8_NCOLS 4
#define T8_ROWS_PER_SEG 3000
static void
test_large_unsorted_k2_sort_correctness(void)
{
    TEST("large unsorted K=2 sort correctness (nc=4, 3000 rows/seg)");

    col_rel_t *rel = test_rel_alloc(T8_NCOLS);
    ASSERT(rel != NULL, "test_rel_alloc failed");

    /* Build two segments with non-overlapping rows, each shuffled.
     * seg0: rows with col[0] in [0, T8_ROWS_PER_SEG)
     * seg1: rows with col[0] in [T8_ROWS_PER_SEG, 2*T8_ROWS_PER_SEG)
     * Shuffle each using simple LCG so qsort is forced to reorder them.
     */
    const uint32_t N = T8_ROWS_PER_SEG;

    /* Allocate scratch arrays for shuffled indices */
    uint32_t *idx0 = (uint32_t *)malloc(N * sizeof(uint32_t));
    uint32_t *idx1 = (uint32_t *)malloc(N * sizeof(uint32_t));
    ASSERT(idx0 != NULL && idx1 != NULL, "scratch alloc failed");

    for (uint32_t i = 0; i < N; i++) {
        idx0[i] = i;
        idx1[i] = i;
    }

    /* Fisher-Yates with fixed seed for reproducibility */
    uint64_t rng = 0xDEADBEEFCAFEBABEULL;
#define LCG_NEXT(r) ((r) = (r) * 6364136223846793005ULL + \
        1442695040888963407ULL)
    for (uint32_t i = N - 1; i > 0; i--) {
        LCG_NEXT(rng);
        uint32_t j = (uint32_t)(rng >> 33) % (i + 1);
        uint32_t tmp = idx0[i]; idx0[i] = idx0[j]; idx0[j] = tmp;
    }
    for (uint32_t i = N - 1; i > 0; i--) {
        LCG_NEXT(rng);
        uint32_t j = (uint32_t)(rng >> 33) % (i + 1);
        uint32_t tmp = idx1[i]; idx1[i] = idx1[j]; idx1[j] = tmp;
    }
#undef LCG_NEXT

    /* Append seg0 rows in shuffled order */
    for (uint32_t i = 0; i < N; i++) {
        int64_t row[T8_NCOLS];
        int64_t v = (int64_t)idx0[i];
        for (uint32_t c = 0; c < T8_NCOLS; c++)
            row[c] = v + (int64_t)c;
        ASSERT(test_rel_append_row(rel, row) == 0, "append seg0 row");
    }

    /* Append seg1 rows in shuffled order */
    for (uint32_t i = 0; i < N; i++) {
        int64_t row[T8_NCOLS];
        int64_t v = (int64_t)N + (int64_t)idx1[i];
        for (uint32_t c = 0; c < T8_NCOLS; c++)
            row[c] = v + (int64_t)c;
        ASSERT(test_rel_append_row(rel, row) == 0, "append seg1 row");
    }

    free(idx0);
    free(idx1);

    uint32_t seg_boundaries[3] = { 0, N, 2 * N };
    int rc = col_op_consolidate_kway_merge(rel, seg_boundaries, 2);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(rel->nrows == 2 * N, "all 6000 unique rows preserved");
    ASSERT(test_rel_is_sorted_unique(rel), "output is sorted and unique");

    /* Spot-check: first row should be (0,1,2,3) */
    int64_t expected_first[T8_NCOLS] = { 0, 1, 2, 3 };
    ASSERT(test_row_cmp_rel(rel, 0, expected_first, T8_NCOLS) == 0,
        "first row is (0,1,2,3)");

    /* Spot-check: last row should be (2*N-1, 2*N, 2*N+1, 2*N+2) */
    int64_t expected_last[T8_NCOLS];
    for (uint32_t c = 0; c < T8_NCOLS; c++)
        expected_last[c] = (int64_t)(2 * N - 1) + (int64_t)c;
    ASSERT(test_row_cmp_rel(rel, rel->nrows - 1,
        expected_last, T8_NCOLS) == 0,
        "last row is (2N-1, 2N, 2N+1, 2N+2)");

    test_rel_free(rel);
    PASS();
}

/* ================================================================
 * Test 9: Wide rows K=4 unsorted - exercises SIMD with nc=8
 *
 * Input: four segments of 500 unique rows each, unsorted, ncols=8.
 *        Wide rows (8 int64_t = 64 bytes) stress the SIMD lane
 *        utilisation in both qsort and merge compare paths.
 *
 * Expected: all 2000 unique rows present, output sorted and unique.
 * ================================================================ */
#define T9_NCOLS 8
#define T9_ROWS_PER_SEG 500
static void
test_wide_rows_k4_sort_correctness(void)
{
    TEST("wide rows K=4 sort correctness (nc=8, 500 rows/seg)");

    col_rel_t *rel = test_rel_alloc(T9_NCOLS);
    ASSERT(rel != NULL, "test_rel_alloc failed");

    const uint32_t N = T9_ROWS_PER_SEG;
    const uint32_t K = 4;

    /* Each segment has rows with col[0] in [k*N, (k+1)*N), shuffled */
    uint64_t rng = 0xFEEDFACEDEADC0DEULL;
#define LCG_NEXT(r) ((r) = (r) * 6364136223846793005ULL + \
        1442695040888963407ULL)
    for (uint32_t k = 0; k < K; k++) {
        uint32_t *idx = (uint32_t *)malloc(N * sizeof(uint32_t));
        ASSERT(idx != NULL, "scratch alloc failed");
        for (uint32_t i = 0; i < N; i++)
            idx[i] = i;
        for (uint32_t i = N - 1; i > 0; i--) {
            LCG_NEXT(rng);
            uint32_t j = (uint32_t)(rng >> 33) % (i + 1);
            uint32_t tmp = idx[i]; idx[i] = idx[j]; idx[j] = tmp;
        }
        for (uint32_t i = 0; i < N; i++) {
            int64_t row[T9_NCOLS];
            int64_t base = (int64_t)k * (int64_t)N + (int64_t)idx[i];
            for (uint32_t c = 0; c < T9_NCOLS; c++)
                row[c] = base + (int64_t)c;
            ASSERT(test_rel_append_row(rel, row) == 0, "append row");
        }
        free(idx);
    }
#undef LCG_NEXT

    uint32_t seg_boundaries[5] = { 0, N, 2*N, 3*N, 4*N };
    int rc = col_op_consolidate_kway_merge(rel, seg_boundaries, K);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(rel->nrows == K * N, "all 2000 unique rows preserved");
    ASSERT(test_rel_is_sorted_unique(rel), "output is sorted and unique");

    test_rel_free(rel);
    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */
int
main(void)
{
    printf("=== test_consolidate_kway_merge (TDD RED PHASE) ===\n\n");
    printf("NOTE: Expected to FAIL at link time until US-002 GREEN phase\n");
    printf("      implements col_op_consolidate_kway_merge.\n\n");

    test_single_copy_passthrough();
    test_two_copies_direct_merge();
    test_three_copies_heap_merge();
    test_per_segment_sort_before_merge();
    test_cross_segment_dedup();
    test_large_dataset_performance();
    test_empty_middle_segment();
    test_large_unsorted_k2_sort_correctness();
    test_wide_rows_k4_sort_correctness();

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n", pass_count,
        fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
