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

#include <errno.h>
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
static const char *consolidate_fail_site = NULL;
static bool consolidate_fail_used = false;
static uint32_t consolidate_fail_match = 1;
static uint32_t consolidate_seen_matches = 0;

static bool
test_consolidate_alloc_should_fail(const char *site)
{
    if (consolidate_fail_site && !consolidate_fail_used
        && strcmp(site, consolidate_fail_site) == 0) {
        consolidate_seen_matches++;
        if (consolidate_seen_matches == consolidate_fail_match) {
            consolidate_fail_used = true;
            return true;
        }
    }
    return false;
}

wl_columnar_consolidate_alloc_hook_t wl_columnar_consolidate_alloc_hook
    = test_consolidate_alloc_should_fail;

static void
fail_consolidate_allocation_at(const char *site)
{
    consolidate_fail_site = site;
    consolidate_fail_used = false;
    consolidate_fail_match = 1;
    consolidate_seen_matches = 0;
}

static void
fail_consolidate_allocation_on_match(const char *site, uint32_t match)
{
    consolidate_fail_site = site;
    consolidate_fail_used = false;
    consolidate_fail_match = match;
    consolidate_seen_matches = 0;
}

static void
clear_consolidate_allocation_failure(void)
{
    consolidate_fail_site = NULL;
}

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
    col_rel_t *r = NULL;
    if (col_rel_alloc(&r, "consolidate_test") != 0)
        return NULL;
    r->ncols = ncols;
    if (ncols > 0) {
        r->col_names = (char **)calloc(ncols, sizeof(char *));
        if (!r->col_names) {
            free(r->name);
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
                free(r->name);
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
    col_rel_destroy(r);
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

static void
test_source_reader_blocks_consolidation_sort(void)
{
    col_rel_t *rel = test_rel_alloc(1);
    wl_columnar_source_access_reader_t reader = { 0 };
    uint32_t boundaries[] = { 0, 4 };
    int64_t rows[] = { 4, 1, 3, 2 };
    int64_t before[4];
    int rc;

    TEST("source reader blocks consolidation sort transactionally");
    ASSERT(rel != NULL, "relation allocation failed");
    for (uint32_t i = 0; i < 4; i++) {
        if (test_rel_append_row(rel, &rows[i]) != 0) {
            test_rel_free(rel);
            FAIL("failed to append fixture row");
        }
    }
    memcpy(before, rel->columns[0], sizeof(before));
    if (col_rel_source_reader_acquire(rel, &reader) != 0) {
        test_rel_free(rel);
        FAIL("source reader acquisition failed");
    }
    rc = col_op_consolidate_kway_merge(rel, boundaries, 1);
    if (col_rel_source_reader_release(&reader) != 0) {
        test_rel_free(rel);
        FAIL("source reader release failed");
    }
    if (rc != EBUSY) {
        test_rel_free(rel);
        FAIL("consolidation should propagate sort EBUSY");
    }
    if (rel->nrows != 4
        || memcmp(before, rel->columns[0], sizeof(before)) != 0) {
        test_rel_free(rel);
        FAIL("blocked consolidation must leave rows unchanged");
    }
    test_rel_free(rel);
    PASS();
}

static void
test_source_reader_blocks_sorted_dedup(void)
{
    col_rel_t *rel = test_rel_alloc(1);
    wl_columnar_source_access_reader_t reader = { 0 };
    uint32_t boundaries[] = { 0, 4 };
    int64_t rows[] = { 1, 1, 2, 3 };
    int64_t before[4];
    int rc;

    TEST("source reader blocks sorted-segment dedup transactionally");
    ASSERT(rel != NULL, "relation allocation failed");
    for (uint32_t i = 0; i < 4; i++) {
        if (test_rel_append_row(rel, &rows[i]) != 0) {
            test_rel_free(rel);
            FAIL("failed to append fixture row");
        }
    }
    memcpy(before, rel->columns[0], sizeof(before));
    if (col_rel_source_reader_acquire(rel, &reader) != 0) {
        test_rel_free(rel);
        FAIL("source reader acquisition failed");
    }
    rc = col_op_consolidate_kway_merge(rel, boundaries, 1);
    if (col_rel_source_reader_release(&reader) != 0) {
        test_rel_free(rel);
        FAIL("source reader release failed");
    }
    if (rc != EBUSY) {
        test_rel_free(rel);
        FAIL("consolidation should reject an active source reader");
    }
    if (rel->nrows != 4
        || memcmp(before, rel->columns[0], sizeof(before)) != 0) {
        test_rel_free(rel);
        FAIL("reader-blocked sorted dedup must leave rows unchanged");
    }
    test_rel_free(rel);
    PASS();
}

static void
test_source_reader_blocks_large_hash_dedup(void)
{
    const uint32_t row_count = 10001;
    col_rel_t *rel = test_rel_alloc(1);
    wl_columnar_source_access_reader_t reader = { 0 };
    uint32_t boundaries[] = { 0, row_count };
    int64_t *before = (int64_t *)malloc((size_t)row_count * sizeof(*before));
    int rc;

    TEST("source reader blocks large hash dedup transactionally");
    if (!rel || !before) {
        free(before);
        test_rel_free(rel);
        FAIL("relation or row snapshot allocation failed");
    }
    for (uint32_t i = 0; i < row_count; i++) {
        int64_t value = (int64_t)(i % 100u);
        if (test_rel_append_row(rel, &value) != 0) {
            free(before);
            test_rel_free(rel);
            FAIL("failed to append fixture row");
        }
    }
    memcpy(before, rel->columns[0], (size_t)row_count * sizeof(*before));
    if (col_rel_source_reader_acquire(rel, &reader) != 0) {
        free(before);
        test_rel_free(rel);
        FAIL("source reader acquisition failed");
    }
    rc = col_op_consolidate_kway_merge(rel, boundaries, 1);
    if (col_rel_source_reader_release(&reader) != 0) {
        free(before);
        test_rel_free(rel);
        FAIL("source reader release failed");
    }
    if (rc != EBUSY) {
        free(before);
        test_rel_free(rel);
        FAIL("large consolidation should reject an active source reader");
    }
    if (rel->nrows != row_count
        || memcmp(before, rel->columns[0],
        (size_t)row_count * sizeof(*before)) != 0) {
        free(before);
        test_rel_free(rel);
        FAIL("reader-blocked hash dedup must leave rows unchanged");
    }
    free(before);
    if (col_op_consolidate_kway_merge(rel, boundaries, 1) != 0) {
        test_rel_free(rel);
        FAIL("large hash dedup should succeed after reader release");
    }
    if (rel->nrows != 100 || !test_rel_is_sorted_unique(rel)) {
        test_rel_free(rel);
        FAIL("large hash dedup should produce sorted unique rows");
    }
    test_rel_free(rel);
    PASS();
}

static void
test_shared_view_sorted_dedup_is_copy_on_write(void)
{
    col_rel_t *source = test_rel_alloc(1);
    col_rel_t *view = test_rel_alloc(1);
    uint32_t boundaries[] = { 0, 4 };
    int64_t rows[] = { 1, 1, 2, 3 };
    int64_t source_before[4];
    int64_t *source_column;

    TEST("shared-view sorted dedup preserves source storage");
    if (!source || !view) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("relation allocation failed");
    }
    for (uint32_t i = 0; i < 4; i++) {
        if (test_rel_append_row(source, &rows[i]) != 0) {
            test_rel_free(view);
            test_rel_free(source);
            FAIL("failed to append source row");
        }
    }
    if (col_rel_install_shared_view(view, source) != 0) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("failed to install shared view");
    }
    memcpy(source_before, source->columns[0], sizeof(source_before));
    source_column = source->columns[0];
    if (col_op_consolidate_kway_merge(view, boundaries, 1) != 0) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("shared-view consolidation failed");
    }
    if (source->columns[0] != source_column || source->nrows != 4
        || memcmp(source_before, source->columns[0], sizeof(source_before)) != 0
        || view->nrows != 3 || !test_rel_is_sorted_unique(view)) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("shared-view dedup must detach and preserve source rows");
    }
    test_rel_free(view);
    test_rel_free(source);
    PASS();
}

static void
test_shared_view_merge_scatter_is_copy_on_write(void)
{
    col_rel_t *source = test_rel_alloc(1);
    col_rel_t *view = test_rel_alloc(1);
    uint32_t boundaries[] = { 0, 2, 4 };
    int64_t rows[] = { 1, 4, 2, 3 };
    int64_t source_before[4];
    int64_t *source_column;

    TEST("shared-view merge scatter preserves source storage");
    if (!source || !view) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("relation allocation failed");
    }
    for (uint32_t i = 0; i < 4; i++) {
        if (test_rel_append_row(source, &rows[i]) != 0) {
            test_rel_free(view);
            test_rel_free(source);
            FAIL("failed to append source row");
        }
    }
    if (col_rel_install_shared_view(view, source) != 0) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("failed to install shared view");
    }
    memcpy(source_before, source->columns[0], sizeof(source_before));
    source_column = source->columns[0];
    if (col_op_consolidate_kway_merge(view, boundaries, 2) != 0) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("shared-view merge failed");
    }
    if (source->columns[0] != source_column || source->nrows != 4
        || memcmp(source_before, source->columns[0], sizeof(source_before)) != 0
        || view->nrows != 4 || !test_rel_is_sorted_unique(view)) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("shared-view scatter must detach and preserve source rows");
    }
    test_rel_free(view);
    test_rel_free(source);
    PASS();
}

static void
test_shared_view_merge_oom_preserves_view_state(void)
{
    col_rel_t *source = test_rel_alloc(1);
    col_rel_t *view = test_rel_alloc(1);
    uint32_t boundaries[] = { 0, 2, 4 };
    int64_t rows[] = { 4, 1, 3, 2 };
    int64_t before[4];
    int64_t *view_column;
    int64_t *source_column;
    bool *shared_flags;
    uint64_t view_generation;
    uint64_t storage_generation;
    col_rel_t *storage_owner;
    uint64_t owner_identity;
    uint64_t owner_generation;
    uint32_t alias_borrows;
    int rc;

    TEST("shared-view merge OOM preserves view and owner metadata");
    if (!source || !view) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("relation allocation failed");
    }
    for (uint32_t i = 0; i < 4; i++) {
        if (test_rel_append_row(source, &rows[i]) != 0) {
            test_rel_free(view);
            test_rel_free(source);
            FAIL("failed to append fixture row");
        }
    }
    if (col_rel_install_shared_view(view, source) != 0) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("failed to install shared view");
    }
    memcpy(before, view->columns[0], sizeof(before));
    view_column = view->columns[0];
    source_column = source->columns[0];
    shared_flags = view->col_shared;
    view_generation = view->view_generation;
    storage_generation = view->storage_generation;
    storage_owner = view->storage_owner;
    owner_identity = view->storage_owner_identity;
    owner_generation = view->storage_owner_generation;
    alias_borrows = source->storage_alias_borrows;

    fail_consolidate_allocation_at("merge_output");
    rc = col_op_consolidate_kway_merge(view, boundaries, 2);
    clear_consolidate_allocation_failure();
    if (rc != ENOMEM || !consolidate_fail_used) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("shared-view merge output OOM should be injected");
    }
    if (view->nrows != 4 || view->columns[0] != view_column
        || view->col_shared != shared_flags || !view->col_shared[0]
        || view->view_generation != view_generation
        || view->storage_generation != storage_generation
        || view->storage_owner != storage_owner
        || view->storage_owner_identity != owner_identity
        || view->storage_owner_generation != owner_generation
        || source->storage_alias_borrows != alias_borrows
        || source->columns[0] != source_column
        || memcmp(before, view->columns[0], sizeof(before)) != 0
        || memcmp(before, source->columns[0], sizeof(before)) != 0) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("shared-view merge OOM must preserve rows and ownership state");
    }
    test_rel_free(view);
    test_rel_free(source);
    PASS();
}

static void
test_merge_output_oom_is_transactional(void)
{
    col_rel_t *rel = test_rel_alloc(1);
    uint32_t boundaries[] = { 0, 2, 4 };
    int64_t rows[] = { 4, 1, 3, 2 };
    int64_t before[4];
    uint64_t view_generation;
    uint64_t storage_generation;
    int rc;

    TEST("merge output allocation failure leaves relation unchanged");
    ASSERT(rel != NULL, "relation allocation failed");
    for (uint32_t i = 0; i < 4; i++) {
        if (test_rel_append_row(rel, &rows[i]) != 0) {
            test_rel_free(rel);
            FAIL("failed to append fixture row");
        }
    }
    memcpy(before, rel->columns[0], sizeof(before));
    view_generation = rel->view_generation;
    storage_generation = rel->storage_generation;
    fail_consolidate_allocation_at("merge_output");
    rc = col_op_consolidate_kway_merge(rel, boundaries, 2);
    clear_consolidate_allocation_failure();
    if (rc != ENOMEM || !consolidate_fail_used) {
        test_rel_free(rel);
        FAIL("merge output OOM should be injected and propagated");
    }
    if (rel->nrows != 4 || rel->view_generation != view_generation
        || rel->storage_generation != storage_generation
        || memcmp(before, rel->columns[0], sizeof(before)) != 0) {
        test_rel_free(rel);
        FAIL("merge output OOM must leave relation unchanged");
    }
    test_rel_free(rel);
    PASS();
}

static void
test_merge_heap_oom_is_transactional(void)
{
    col_rel_t *rel = test_rel_alloc(1);
    uint32_t boundaries[] = { 0, 2, 4, 6 };
    int64_t rows[] = { 6, 1, 4, 2, 5, 3 };
    int64_t before[6];
    uint64_t view_generation;
    uint64_t storage_generation;
    int rc;

    TEST("merge heap allocation failure leaves relation unchanged");
    ASSERT(rel != NULL, "relation allocation failed");
    for (uint32_t i = 0; i < 6; i++) {
        if (test_rel_append_row(rel, &rows[i]) != 0) {
            test_rel_free(rel);
            FAIL("failed to append fixture row");
        }
    }
    memcpy(before, rel->columns[0], sizeof(before));
    view_generation = rel->view_generation;
    storage_generation = rel->storage_generation;
    fail_consolidate_allocation_at("merge_heap");
    rc = col_op_consolidate_kway_merge(rel, boundaries, 3);
    clear_consolidate_allocation_failure();
    if (rc != ENOMEM || !consolidate_fail_used) {
        test_rel_free(rel);
        FAIL("merge heap OOM should be injected and propagated");
    }
    if (rel->nrows != 6 || rel->view_generation != view_generation
        || rel->storage_generation != storage_generation
        || memcmp(before, rel->columns[0], sizeof(before)) != 0) {
        test_rel_free(rel);
        FAIL("merge heap OOM must leave relation unchanged");
    }
    test_rel_free(rel);
    PASS();
}

static wl_columnar_memory_governor_ref_t *
test_consolidate_governor_create(uint64_t usable_bytes)
{
    wl_columnar_memory_resolution_t resolution = { 0 };
    resolution.budget_bytes = usable_bytes;
    resolution.usable_bytes = usable_bytes;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    return wl_columnar_memory_governor_ref_create(&resolution);
}

static void
test_consolidate_scratch_admission(void)
{
    uint32_t boundaries[] = { 0, 2, 4 };
    int64_t rows[] = { 4, 1, 3, 2 };
    int64_t before[4];
    wl_columnar_memory_governor_ref_t *ref;
    col_rel_t *source;
    int64_t *borrowed_column;
    bool *shared_flags;
    uint32_t alias_borrows;
    col_rel_t *rel;
    uint64_t view_generation;
    uint64_t storage_generation;
    col_rel_t *storage_owner;
    int rc;

    TEST("consolidation scratch is governor-admitted and released");
    source = test_rel_alloc(1);
    rel = test_rel_alloc(1);
    if (!source || !rel) {
        test_rel_free(rel);
        test_rel_free(source);
        FAIL("relation allocation failed");
    }
    for (uint32_t i = 0; i < 4; i++) {
        if (test_rel_append_row(source, &rows[i]) != 0) {
            test_rel_free(rel);
            test_rel_free(source);
            FAIL("failed to append source fixture row");
        }
    }
    ref = test_consolidate_governor_create(1);
    if (!ref || col_rel_attach_memory_governor(rel, ref) != 0
        || col_rel_install_shared_view(rel, source) != 0) {
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        test_rel_free(rel);
        test_rel_free(source);
        FAIL("failed to attach constrained governor/shared view");
    }
    memcpy(before, rel->columns[0], sizeof(before));
    view_generation = rel->view_generation;
    storage_generation = rel->storage_generation;
    storage_owner = rel->storage_owner;
    borrowed_column = rel->columns[0];
    shared_flags = rel->col_shared;
    alias_borrows = source->storage_alias_borrows;
    rc = col_op_consolidate_kway_merge(rel, boundaries, 2);
    if (rc != ENOMEM || rel->nrows != 4
        || rel->view_generation != view_generation
        || rel->storage_generation != storage_generation
        || rel->storage_owner != storage_owner
        || rel->columns[0] != borrowed_column
        || rel->col_shared != shared_flags || !rel->col_shared[0]
        || source->storage_alias_borrows != alias_borrows
        || memcmp(before, source->columns[0], sizeof(before)) != 0
        || memcmp(before, rel->columns[0], sizeof(before)) != 0
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != 0) {
        test_rel_free(rel);
        test_rel_free(source);
        wl_columnar_memory_governor_ref_release(ref);
        FAIL(
            "denied shared-view scratch admission must preserve ownership/data");
    }
    test_rel_free(rel);
    test_rel_free(source);
    wl_columnar_memory_governor_ref_release(ref);

    rel = test_rel_alloc(1);
    if (!rel) {
        FAIL("relation allocation failed");
    }
    for (uint32_t i = 0; i < 4; i++) {
        if (test_rel_append_row(rel, &rows[i]) != 0) {
            test_rel_free(rel);
            FAIL("failed to append success fixture row");
        }
    }
    ref = test_consolidate_governor_create(1024u * 1024u);
    if (!ref || col_rel_attach_memory_governor(rel, ref) != 0) {
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        test_rel_free(rel);
        FAIL("failed to attach sufficient memory governor");
    }
    rc = col_op_consolidate_kway_merge(rel, boundaries, 2);
    if (rc != 0 || !test_rel_is_sorted_unique(rel)
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != 0) {
        test_rel_free(rel);
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("admitted scratch must succeed and release reservation");
    }
    test_rel_free(rel);
    wl_columnar_memory_governor_ref_release(ref);
    PASS();
}

static void
test_later_segment_sort_oom_is_transactional(void)
{
    const uint32_t segment_rows = 40;
    const uint32_t row_count = segment_rows * 2;
    col_rel_t *rel = test_rel_alloc(1);
    uint32_t boundaries[] = { 0, segment_rows, row_count };
    int64_t *before = (int64_t *)malloc((size_t)row_count * sizeof(*before));
    uint64_t view_generation;
    uint64_t storage_generation;
    int rc;

    TEST("radix workspace OOM precedes mutation and is reused");
    if (!rel || !before) {
        free(before);
        test_rel_free(rel);
        FAIL("relation or snapshot allocation failed");
    }
    for (uint32_t i = 0; i < segment_rows; i++) {
        int64_t value = (int64_t)(segment_rows - i);
        if (test_rel_append_row(rel, &value) != 0) {
            free(before);
            test_rel_free(rel);
            FAIL("failed to append first segment fixture row");
        }
    }
    for (uint32_t i = 0; i < segment_rows; i++) {
        int64_t value = (int64_t)(row_count - i);
        if (test_rel_append_row(rel, &value) != 0) {
            free(before);
            test_rel_free(rel);
            FAIL("failed to append second segment fixture row");
        }
    }
    memcpy(before, rel->columns[0], (size_t)row_count * sizeof(*before));
    view_generation = rel->view_generation;
    storage_generation = rel->storage_generation;
    fail_consolidate_allocation_at("radix_workspace_perm_a");
    rc = col_op_consolidate_kway_merge(rel, boundaries, 2);
    clear_consolidate_allocation_failure();
    if (rc != ENOMEM || !consolidate_fail_used) {
        free(before);
        test_rel_free(rel);
        FAIL("radix workspace allocation failure should propagate");
    }
    if (rel->nrows != row_count || rel->view_generation != view_generation
        || rel->storage_generation != storage_generation
        || memcmp(before, rel->columns[0],
        (size_t)row_count * sizeof(*before)) != 0) {
        free(before);
        test_rel_free(rel);
        FAIL("workspace OOM must leave the original relation unchanged");
    }
    fail_consolidate_allocation_on_match("radix_workspace_perm_a", 2);
    rc = col_op_consolidate_kway_merge(rel, boundaries, 2);
    clear_consolidate_allocation_failure();
    if (rc != 0 || consolidate_fail_used || !test_rel_is_sorted_unique(rel)) {
        free(before);
        test_rel_free(rel);
        FAIL("later segment sorting must reuse preallocated workspace");
    }
    free(before);
    test_rel_free(rel);
    PASS();
}

static void
test_hash_allocation_oom_is_not_fallback(void)
{
    const uint32_t row_count = 10001;
    col_rel_t *source = test_rel_alloc(1);
    col_rel_t *rel = test_rel_alloc(1);
    uint32_t boundaries[] = { 0, row_count };
    int64_t *before = (int64_t *)malloc((size_t)row_count * sizeof(*before));
    int64_t *column;
    bool *shared;
    uint64_t view_generation;
    uint64_t storage_generation;
    uint32_t alias_borrows;
    int rc;

    TEST("shared-view hash OOM preserves rows and ownership metadata");
    if (!source || !rel || !before) {
        free(before);
        test_rel_free(rel);
        test_rel_free(source);
        FAIL("relation or snapshot allocation failed");
    }
    for (uint32_t i = 0; i < row_count; i++) {
        int64_t value = (int64_t)(i % 100u);
        if (test_rel_append_row(source, &value) != 0) {
            free(before);
            test_rel_free(rel);
            test_rel_free(source);
            FAIL("failed to append fixture row");
        }
    }
    if (col_rel_install_shared_view(rel, source) != 0) {
        free(before);
        test_rel_free(rel);
        test_rel_free(source);
        FAIL("failed to install shared view");
    }
    memcpy(before, rel->columns[0], (size_t)row_count * sizeof(*before));
    column = rel->columns[0];
    shared = rel->col_shared;
    view_generation = rel->view_generation;
    storage_generation = rel->storage_generation;
    alias_borrows = source->storage_alias_borrows;
    fail_consolidate_allocation_at("hash_table");
    rc = col_op_consolidate_kway_merge(rel, boundaries, 1);
    clear_consolidate_allocation_failure();
    if (rc != ENOMEM || !consolidate_fail_used) {
        free(before);
        test_rel_free(rel);
        test_rel_free(source);
        FAIL("shared-view hash OOM should be injected");
    }
    if (rel->nrows != row_count || rel->view_generation != view_generation
        || rel->storage_generation != storage_generation
        || rel->columns[0] != column || rel->col_shared != shared
        || !rel->col_shared[0] || source->storage_alias_borrows != alias_borrows
        || source->columns[0] != column
        || memcmp(before, rel->columns[0],
        (size_t)row_count * sizeof(*before)) != 0
        || memcmp(before, source->columns[0],
        (size_t)row_count * sizeof(*before)) != 0) {
        free(before);
        test_rel_free(rel);
        test_rel_free(source);
        FAIL("hash table OOM must preserve shared-view metadata");
    }
    fail_consolidate_allocation_at("hash_sort_scratch");
    rc = col_op_consolidate_kway_merge(rel, boundaries, 1);
    clear_consolidate_allocation_failure();
    if (rc != ENOMEM || !consolidate_fail_used) {
        free(before);
        test_rel_free(rel);
        test_rel_free(source);
        FAIL("shared-view hash scratch OOM should be injected");
    }
    if (rel->nrows != row_count || rel->view_generation != view_generation
        || rel->storage_generation != storage_generation
        || rel->columns[0] != column || rel->col_shared != shared
        || !rel->col_shared[0] || source->storage_alias_borrows != alias_borrows
        || source->columns[0] != column
        || memcmp(before, rel->columns[0],
        (size_t)row_count * sizeof(*before)) != 0
        || memcmp(before, source->columns[0],
        (size_t)row_count * sizeof(*before)) != 0) {
        free(before);
        test_rel_free(rel);
        test_rel_free(source);
        FAIL("hash scratch OOM must preserve shared-view metadata");
    }
    free(before);
    test_rel_free(rel);
    test_rel_free(source);
    PASS();
}

static void
test_hash_float_signed_zero_lexicographic_order(void)
{
    const uint32_t row_count = 10001;
    col_rel_t *rel = test_rel_alloc(2);
    wirelog_column_type_t types[] = {
        WIRELOG_TYPE_FLOAT,
        WIRELOG_TYPE_FLOAT,
    };
    uint32_t boundaries[] = { 0, row_count };
    int64_t row[] = { 0, 0 };
    int64_t expected_second[] = {
        wl_columnar_float_to_bits(1.0),
        wl_columnar_float_to_bits(2.0),
    };

    TEST("hash sort preserves float signed-zero lexicographic equivalence");
    ASSERT(rel != NULL, "relation allocation failed");
    if (col_rel_set_column_types(rel, types, 2) != 0) {
        test_rel_free(rel);
        FAIL("failed to set float column types");
    }
    for (uint32_t i = 0; i < row_count; i++) {
        row[1] = wl_columnar_float_to_bits(i % 2 == 0 ? 2.0 : 1.0);
        if (test_rel_append_row(rel, row) != 0) {
            test_rel_free(rel);
            FAIL("failed to append signed-zero fixture row");
        }
        /* Exercise non-canonical input bits that can enter through raw views. */
        if (i % 2 == 0)
            rel->columns[0][i] = INT64_MIN;
    }

    if (col_op_consolidate_kway_merge(rel, boundaries, 1) != 0
        || rel->nrows != 2 || rel->columns[0][0] != 0
        || rel->columns[0][1] != 0
        || rel->columns[1][0] != expected_second[0]
        || rel->columns[1][1] != expected_second[1]) {
        test_rel_free(rel);
        FAIL("signed-zero keys must tie and sort by the next float column");
    }
    test_rel_free(rel);
    PASS();
}

static void
test_hash_heuristic_fallback_succeeds(void)
{
    const uint32_t row_count = 10001;
    col_rel_t *rel = test_rel_alloc(1);
    uint32_t boundaries[] = { 0, row_count };
    int rc;

    TEST("hash unique-count heuristic falls back successfully");
    ASSERT(rel != NULL, "relation allocation failed");
    for (uint32_t i = 0; i < row_count; i++) {
        int64_t value = (int64_t)(row_count - i);
        if (test_rel_append_row(rel, &value) != 0) {
            test_rel_free(rel);
            FAIL("failed to append fixture row");
        }
    }
    rc = col_op_consolidate_kway_merge(rel, boundaries, 1);
    if (rc != 0 || rel->nrows != row_count
        || !test_rel_is_sorted_unique(rel)) {
        test_rel_free(rel);
        FAIL("heuristic fallback should preserve the sorted unique relation");
    }
    test_rel_free(rel);
    PASS();
}

static void
test_k16_workspace_is_preallocated(void)
{
    const uint32_t row_count = 50001;
    col_rel_t *rel = test_rel_alloc(1);
    uint32_t boundaries[] = { 0, row_count };
    int64_t *before = (int64_t *)malloc((size_t)row_count * sizeof(*before));
    int rc;

    TEST("k16 workspace OOM is transactional and successful sort reuses it");
    if (!rel || !before) {
        free(before);
        test_rel_free(rel);
        FAIL("relation or snapshot allocation failed");
    }
    for (uint32_t i = 0; i < row_count; i++) {
        int64_t value = (int64_t)(row_count - i);
        if (test_rel_append_row(rel, &value) != 0) {
            free(before);
            test_rel_free(rel);
            FAIL("failed to append k16 fixture row");
        }
    }
    memcpy(before, rel->columns[0], (size_t)row_count * sizeof(*before));
    fail_consolidate_allocation_at("radix_workspace_count16");
    rc = col_op_consolidate_kway_merge(rel, boundaries, 1);
    clear_consolidate_allocation_failure();
    if (rc != ENOMEM || !consolidate_fail_used
        || rel->nrows != row_count
        || memcmp(before, rel->columns[0],
        (size_t)row_count * sizeof(*before)) != 0) {
        free(before);
        test_rel_free(rel);
        FAIL("k16 workspace OOM must happen before mutation");
    }
    rc = col_op_consolidate_kway_merge(rel, boundaries, 1);
    if (rc != 0 || rel->nrows != row_count || !test_rel_is_sorted_unique(rel)) {
        free(before);
        test_rel_free(rel);
        FAIL("k16 workspace sort should complete successfully");
    }
    free(before);
    test_rel_free(rel);
    PASS();
}

static void
test_float_insertion_workspace(void)
{
    col_rel_t *rel = test_rel_alloc(1);
    wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
    uint32_t boundaries[] = { 0, 4 };
    int64_t values[] = {
        wl_columnar_float_to_bits(3.0),
        wl_columnar_float_to_bits(1.0),
        wl_columnar_float_to_bits(2.0),
        wl_columnar_float_to_bits(1.0),
    };
    int64_t expected[] = {
        wl_columnar_float_to_bits(1.0),
        wl_columnar_float_to_bits(2.0),
        wl_columnar_float_to_bits(3.0),
    };

    TEST("typed-float insertion workspace is preallocated and reused");
    ASSERT(rel != NULL, "relation allocation failed");
    if (col_rel_set_column_types(rel, &type, 1) != 0) {
        test_rel_free(rel);
        FAIL("failed to set float type");
    }
    for (uint32_t i = 0; i < 4; i++) {
        if (test_rel_append_row(rel, &values[i]) != 0) {
            test_rel_free(rel);
            FAIL("failed to append float fixture row");
        }
    }
    fail_consolidate_allocation_at("radix_workspace_insertion_rows");
    int rc = col_op_consolidate_kway_merge(rel, boundaries, 1);
    clear_consolidate_allocation_failure();
    if (rc != ENOMEM || !consolidate_fail_used || rel->nrows != 4
        || memcmp(rel->columns[0], values, sizeof(values)) != 0) {
        test_rel_free(rel);
        FAIL("float workspace OOM must precede mutation");
    }
    if (col_op_consolidate_kway_merge(rel, boundaries, 1) != 0
        || rel->nrows != 3
        || memcmp(rel->columns[0], expected, sizeof(expected)) != 0) {
        test_rel_free(rel);
        FAIL("typed-float sort should deduplicate in numeric order");
    }
    test_rel_free(rel);
    PASS();
}

/* ----------------------------------------------------------------
 * Zero-arity relations (.decl p()) hold rows without column storage:
 * col_rel_prepare_resize() skips the allocation when ncols == 0 and
 * col_rel_append_row() still counts the empty tuple.  Such a relation
 * therefore reaches the k-way merge with nrows > 1 and ncols == 0, and
 * its merge-output buffer is sized from nc * sizeof(int64_t) * nr == 0.
 *
 * col_op_consolidate_malloc() is a thin malloc() wrapper whose NULL is
 * read as exhaustion, and malloc(0) may return NULL on a conforming
 * libc, so a zero-sized merge output would be a spurious ENOMEM.  The
 * sizing code keeps the request unconditionally non-zero; this test
 * pins that, plus the surrounding behaviour:
 *
 *   - the zero-width merge output is a real allocation the OOM hook can
 *     intercept, and that injection is transactional;
 *   - the merge dedups the identical empty tuples to one row;
 *   - the request is strictly larger than the segment scratch alone.
 *
 * The last one is the size assertion, and the memory governor is the
 * seam that makes it observable: merged_alloc_bytes is admitted as part
 * of the consolidation scratch, so a budget covering only the two
 * segment arrays must be refused while one extra int64_t admits.  A
 * mutation that drops the non-zero guarantee is caught there -- it is
 * not caught by the allocation hook, which receives only a site name,
 * nor by malloc(0) itself, which returns non-NULL on glibc.
 * ---------------------------------------------------------------- */

/* Three appended empty tuples across two segments; the caller owns it. */
static col_rel_t *
zero_arity_fixture(void)
{
    col_rel_t *rel = test_rel_alloc(0);
    if (!rel)
        return NULL;
    for (int i = 0; i < 3; i++) {
        /* col_rel_append_row is the production path; the file-local
         * helper cannot be used because col_columns_alloc() refuses
         * ncols == 0.  No column is read, but row must be non-NULL. */
        int64_t empty_tuple = 0;
        if (col_rel_append_row(rel, &empty_tuple) != 0) {
            test_rel_free(rel);
            return NULL;
        }
    }
    if (rel->nrows != 3 || rel->ncols != 0 || rel->columns != NULL) {
        test_rel_free(rel);
        return NULL;
    }
    return rel;
}

static void
test_zero_arity_merge_output_is_never_zero_sized(void)
{
    const uint32_t boundaries[] = { 0, 1, 3 };
    /* segment_starts + segment_ends for seg_count == 2.  A zero-arity
     * relation adds no radix workspace, so this is the whole scratch
     * requirement apart from the merge output itself. */
    const uint64_t segment_scratch = 2u * 2u * sizeof(uint32_t);
    wl_columnar_memory_governor_ref_t *ref = NULL;
    col_rel_t *rel = NULL;
    int rc;

    TEST("zero-arity merge keeps a non-zero merge output allocation");

    /* (1) The merge output is a real, interceptable allocation, and the
     * injected failure leaves the rows untouched. */
    rel = zero_arity_fixture();
    if (!rel)
        FAIL("failed to build the zero-arity fixture");
    fail_consolidate_allocation_at("merge_output");
    rc = col_op_consolidate_kway_merge(rel, boundaries, 2);
    clear_consolidate_allocation_failure();
    if (rc != ENOMEM || !consolidate_fail_used || rel->nrows != 3) {
        test_rel_free(rel);
        FAIL("zero-arity merge output allocation was not injectable");
    }
    test_rel_free(rel);

    /* (2) A budget covering only the segment scratch must be refused:
     * the merge output still asks for more than zero bytes. */
    rel = zero_arity_fixture();
    ref = test_consolidate_governor_create(segment_scratch);
    if (!rel || !ref || col_rel_attach_memory_governor(rel, ref) != 0) {
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        test_rel_free(rel);
        FAIL("failed to attach the segment-sized governor");
    }
    rc = col_op_consolidate_kway_merge(rel, boundaries, 2);
    if (rc == ENOMEM && rel->nrows != 3)
        rc = -1; /* a denied admission must leave the rows alone */
    test_rel_free(rel);
    wl_columnar_memory_governor_ref_release(ref);
    if (rc == -1)
        FAIL("denied scratch admission must not disturb the rows");
    if (rc != ENOMEM)
        FAIL("a zero-sized merge output would have been admitted here");

    /* (3) One extra int64_t of budget admits it, and the merge dedups
     * the identical empty tuples to a single row. */
    rel = zero_arity_fixture();
    ref = test_consolidate_governor_create(segment_scratch
            + sizeof(int64_t));
    if (!rel || !ref || col_rel_attach_memory_governor(rel, ref) != 0) {
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        test_rel_free(rel);
        FAIL("failed to attach the admitting governor");
    }
    rc = col_op_consolidate_kway_merge(rel, boundaries, 2);
    if (rc != 0 || rel->nrows != 1) {
        test_rel_free(rel);
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("zero-arity merge did not dedup under an exact budget");
    }
    /* The scratch is transient: it must be given back, or a second
     * consolidation under the same exact budget could not be admitted. */
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(ref)) != 0) {
        test_rel_free(rel);
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("consolidation scratch was not released back to the governor");
    }
    test_rel_free(rel);
    wl_columnar_memory_governor_ref_release(ref);
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
    test_source_reader_blocks_consolidation_sort();
    test_source_reader_blocks_sorted_dedup();
    test_source_reader_blocks_large_hash_dedup();
    test_shared_view_sorted_dedup_is_copy_on_write();
    test_shared_view_merge_scatter_is_copy_on_write();
    test_shared_view_merge_oom_preserves_view_state();
    test_merge_output_oom_is_transactional();
    test_zero_arity_merge_output_is_never_zero_sized();
    test_merge_heap_oom_is_transactional();
    test_consolidate_scratch_admission();
    test_later_segment_sort_oom_is_transactional();
    test_hash_allocation_oom_is_not_fallback();
    test_hash_float_signed_zero_lexicographic_order();
    test_hash_heuristic_fallback_succeeds();
    test_k16_workspace_is_preallocated();
    test_float_insertion_workspace();

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n", pass_count,
        fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
