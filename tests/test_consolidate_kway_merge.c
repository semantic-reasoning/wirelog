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

static void
test_two_copies_compare_full_raw_row_width(void)
{
    TEST("two-way merge compares every column of the raw output row");

    col_rel_t *rel = test_rel_alloc(2);
    ASSERT(rel != NULL, "test_rel_alloc failed");

    int64_t rows[][2] = { { 1, 1 }, { 2, 5 }, { 1, 2 }, { 2, 5 } };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++)
        ASSERT(test_rel_append_row(rel, rows[i]) == 0, "append row");

    const uint32_t boundaries[] = { 0, 2, 4 };
    ASSERT(col_op_consolidate_kway_merge(rel, boundaries, 2) == 0,
        "two-way merge failed");
    ASSERT(rel->nrows == 3,
        "full-row comparison did not deduplicate correctly");

    const int64_t expected[][2] = { { 1, 1 }, { 1, 2 }, { 2, 5 } };
    for (uint32_t i = 0; i < 3; i++) {
        int64_t actual[2];
        col_rel_row_copy_out(rel, i, actual);
        ASSERT(test_row_cmp(actual, expected[i], 2) == 0,
            "full-row merge order is incorrect");
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

static void
test_two_way_merge_empty_segments_uses_no_heap_allocation(void)
{
    int64_t rows[][2] = { { 3, 3 }, { 1, 1 }, { 1, 1 } };
    int64_t expected[][2] = { { 1, 1 }, { 3, 3 } };
    uint32_t empty_left[] = { 0, 0, 3 };
    uint32_t empty_right[] = { 0, 3, 3 };

    TEST("two-way heap merge handles empty segments without heap allocation");
    for (uint32_t empty_side = 0; empty_side < 2; empty_side++) {
        col_rel_t *rel = test_rel_alloc(2);
        uint32_t *boundaries
            = empty_side == 0 ? empty_left : empty_right;
        ASSERT(rel != NULL, "relation allocation failed");
        for (uint32_t row = 0; row < 3; row++) {
            if (test_rel_append_row(rel, rows[row]) != 0) {
                test_rel_free(rel);
                FAIL("failed to append empty-segment fixture row");
            }
        }
        fail_consolidate_allocation_at("merge_heap");
        int rc = col_op_consolidate_kway_merge(rel, boundaries, 2);
        clear_consolidate_allocation_failure();
        if (rc != 0 || consolidate_fail_used || rel->nrows != 2
            || !test_rel_is_sorted_unique(rel)) {
            test_rel_free(rel);
            FAIL("two-way merge should handle empty segments on stack");
        }
        for (uint32_t row = 0; row < 2; row++) {
            int64_t actual[2];
            col_rel_row_copy_out(rel, row, actual);
            if (test_row_cmp(actual, expected[row], 2) != 0) {
                test_rel_free(rel);
                FAIL("two-way merge returned an unexpected row");
            }
        }
        test_rel_free(rel);
    }
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
    uint64_t descriptor_bytes = sizeof(col_rel_t)
        + sizeof("consolidate_test");
    uint64_t metadata_bytes = sizeof(*rel->col_names)
        + strlen(rel->col_names[0]) + 1u;
    uint64_t view_metadata_bytes = 3u + sizeof(*rel->col_names)
        + strlen(source->col_names[0]) + 1u
        + sizeof(struct ArrowSchema *) + sizeof(struct ArrowSchema)
        + 2u + strlen(source->col_names[0]) + 1u
        + sizeof(int64_t *) + sizeof(bool);
    /* Two segment bounds, one four-row merge buffer plus its sentinel,
     * and a three-slot insertion workspace for the unsorted pairs. */
    uint64_t scratch_bytes = 2u * 2u * sizeof(uint32_t)
        + (4u + 1u + 3u) * sizeof(int64_t);
    ref = test_consolidate_governor_create(descriptor_bytes
            + metadata_bytes + view_metadata_bytes
            + 2u * sizeof(int64_t));
    if (!ref || col_rel_attach_memory_governor(rel, ref) != 0
        || col_rel_install_shared_view(rel, source) != 0) {
        if (ref)
            wl_columnar_memory_governor_ref_release(ref);
        test_rel_free(rel);
        test_rel_free(source);
        FAIL("failed to attach constrained governor/shared view");
    }
    if (rel->metadata_reserved_bytes != view_metadata_bytes) {
        test_rel_free(rel);
        test_rel_free(source);
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("shared-view metadata footprint differs from exact fixture");
    }
    uint64_t live_bytes = rel->descriptor_reserved_bytes
        + rel->metadata_reserved_bytes + rel->retained_reserved_bytes;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes, live_bytes + scratch_bytes - 1u,
        memory_order_release);
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
            wl_columnar_memory_governor_ref_get(ref))
        != rel->descriptor_reserved_bytes + rel->metadata_reserved_bytes
        + rel->retained_reserved_bytes) {
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
            wl_columnar_memory_governor_ref_get(ref))
        != rel->descriptor_reserved_bytes + rel->metadata_reserved_bytes
        + rel->retained_reserved_bytes) {
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
    fail_consolidate_allocation_at("hash_unique_rows");
    rc = col_op_consolidate_kway_merge(rel, boundaries, 1);
    clear_consolidate_allocation_failure();
    if (rc != ENOMEM || !consolidate_fail_used) {
        free(before);
        test_rel_free(rel);
        test_rel_free(source);
        FAIL("shared-view hash result OOM should be injected");
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
        FAIL("hash result OOM must preserve shared-view metadata");
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
test_hash_fallback_releases_credit_before_radix_path(void)
{
    const uint32_t row_count = 10001;
    const uint32_t boundaries[] = { 0, row_count };
    col_rel_t *rel = test_rel_alloc(1);
    wl_columnar_memory_governor_ref_t *ref = NULL;
    TEST("hash heuristic releases maximum credit before radix fallback");
    if (!rel)
        FAIL("relation allocation failed");
    for (uint32_t i = 0; i < row_count; i++) {
        int64_t value = (int64_t)(row_count - i);
        if (test_rel_append_row(rel, &value) != 0) {
            test_rel_free(rel);
            FAIL("failed to append unique hash fixture");
        }
    }
    ref = test_consolidate_governor_create(UINT64_C(1) << 30);
    if (!ref || col_rel_attach_memory_governor(rel, ref) != 0) {
        if (ref) wl_columnar_memory_governor_ref_release(ref);
        test_rel_free(rel);
        FAIL("failed to attach hash fallback governor");
    }
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(ref);
    uint64_t baseline = wl_columnar_memory_reserved(governor);
    /* At this row count the hash reservation's maximum rehash transient is
     * about 625 KiB. The ordinary one-segment radix path needs substantially
     * less; this cap admits either phase but cannot admit both concurrently. */
    atomic_store_explicit(&governor->usable_bytes, baseline + 700u * 1024u,
        memory_order_seq_cst);
    int rc = col_op_consolidate_kway_merge(rel, boundaries, 1);
    if (rc != 0 || rel->nrows != row_count
        || !test_rel_is_sorted_unique(rel)
        || wl_columnar_memory_reserved(governor) != baseline) {
        test_rel_free(rel);
        wl_columnar_memory_governor_ref_release(ref);
        FAIL("fallback failed or retained hash/radix admission credit");
    }
    test_rel_free(rel);
    wl_columnar_memory_governor_ref_release(ref);
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
 * segment arrays must be refused while one extra int64_t admits.  The
 * prepared sequence also keeps its copied 3-boundary array and 2-byte
 * sort mask admitted at the same time, so the exact admission includes 14
 * bytes for that metadata. A
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
test_leased_merge_alias_epoch_paths(void)
{
    const uint32_t boundaries[] = { 0, 2, 4 };
    const int64_t fixtures[][4] = {
        { 1, 2, 3, 4 }, /* no segment needs sorting */
        { 1, 3, 4, 2 }, /* first segment sorted, second unsorted */
    };
    TEST("leased merge privatizes sorted aliases and advances exact epochs");
    for (size_t mode = 0; mode < sizeof(fixtures) / sizeof(fixtures[0]);
        mode++) {
        col_rel_t *source = test_rel_alloc(1);
        col_rel_t *view = test_rel_alloc(1);
        if (!source || !view) {
            test_rel_free(view);
            test_rel_free(source);
            FAIL("alias fixture allocation failed");
        }
        for (uint32_t i = 0; i < 4; i++) {
            if (test_rel_append_row(source, &fixtures[mode][i]) != 0) {
                test_rel_free(view);
                test_rel_free(source);
                FAIL("alias fixture append failed");
            }
        }
        if (col_rel_install_shared_view(view, source) != 0) {
            test_rel_free(view);
            test_rel_free(source);
            FAIL("alias installation failed");
        }
        uint64_t view_generation = view->view_generation;
        uint64_t storage_generation = view->storage_generation;
        if (col_op_consolidate_kway_merge(view, boundaries, 2) != 0
            || view->nrows != 4 || view->col_shared
            || view->storage_owner != view
            || view->storage_generation != storage_generation + 1u
            || view->view_generation != view_generation
            + (mode == 0 ? 1u : 2u)
            || col_rel_storage_alias_borrow_count(source) != 0) {
            test_rel_free(view);
            test_rel_free(source);
            FAIL("alias detach/sort/publish epochs differ from the path");
        }
        for (uint32_t i = 0; i < 4; i++) {
            if (view->columns[0][i] != (int64_t)i + 1
                || source->columns[0][i] != fixtures[mode][i]) {
                test_rel_free(view);
                test_rel_free(source);
                FAIL("alias result or sibling storage changed");
            }
        }
        test_rel_free(view);
        test_rel_free(source);
    }
    PASS();
}

static void
test_merge_epoch_headroom_preflight(void)
{
    const uint32_t boundaries[] = { 0, 2, 4 };
    const int64_t values[] = { 2, 1, 4, 3 };
    TEST("merge preflights all view events and alias storage epoch");
    col_rel_t *rel = test_rel_alloc(1);
    ASSERT(rel != NULL, "relation allocation failed");
    for (uint32_t i = 0; i < 4; i++) {
        if (test_rel_append_row(rel, &values[i]) != 0) {
            test_rel_free(rel);
            FAIL("failed to append epoch fixture");
        }
    }
    uint64_t view_generation = rel->view_generation;
    /* Two segment sorts fit, but the final publication is the third event. */
    rel->view_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 3u;
    int rc = col_op_consolidate_kway_merge(rel, boundaries, 2);
    if (rc != EOVERFLOW || rel->nrows != 4
        || rel->view_generation != WL_COLUMNAR_REL_GENERATION_INVALID - 3u
        || rel->columns[0][0] != values[0]
        || rel->columns[0][1] != values[1]) {
        rel->view_generation = view_generation;
        test_rel_free(rel);
        FAIL("sort plus publication headroom must be checked before sorting");
    }
    rel->view_generation = view_generation;
    test_rel_free(rel);

    col_rel_t *source = test_rel_alloc(1);
    col_rel_t *view = test_rel_alloc(1);
    if (!source || !view) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("shared epoch fixture allocation failed");
    }
    for (uint32_t i = 0; i < 4; i++) {
        if (test_rel_append_row(source, &values[i]) != 0) {
            test_rel_free(view);
            test_rel_free(source);
            FAIL("failed to append shared epoch fixture");
        }
    }
    if (col_rel_install_shared_view(view, source) != 0) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("failed to install epoch alias");
    }
    int64_t *old_column = view->columns[0];
    uint64_t old_storage_generation = view->storage_generation;
    view->storage_generation = WL_COLUMNAR_REL_GENERATION_INVALID - 1u;
    rc = col_op_consolidate_kway_merge(view, boundaries, 2);
    bool unchanged = rc == EOVERFLOW && view->nrows == 4
        && view->columns[0] == old_column && view->col_shared
        && view->storage_generation
        == WL_COLUMNAR_REL_GENERATION_INVALID - 1u
        && col_rel_storage_alias_borrow_count(source) == 1
        && source->columns[0] == old_column;
    view->storage_generation = old_storage_generation;
    test_rel_free(view);
    test_rel_free(source);
    if (!unchanged)
        FAIL("alias privatization headroom must precede COW or publication");
    PASS();
}

static void
test_nullary_alias_large_hash_merge(void)
{
    const uint32_t row_count = 10001;
    const uint32_t boundaries[] = { 0, row_count };
    col_rel_t *source = test_rel_alloc(0);
    col_rel_t *view = test_rel_alloc(0);
    TEST("large nullary hash merge releases borrowed owner binding");
    if (!source || !view) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("nullary fixture allocation failed");
    }
    for (uint32_t i = 0; i < row_count; i++) {
        int64_t empty_tuple = 0;
        if (col_rel_append_row(source, &empty_tuple) != 0) {
            test_rel_free(view);
            test_rel_free(source);
            FAIL("nullary fixture append failed");
        }
    }
    if (col_rel_install_shared_view(view, source) != 0) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("nullary alias installation failed");
    }
    uint64_t view_generation = view->view_generation;
    uint64_t storage_generation = view->storage_generation;
    if (view->col_shared != NULL
        || col_rel_storage_alias_borrow_count(source) != 1
        || col_op_consolidate_kway_merge(view, boundaries, 1) != 0
        || view->nrows != 1 || view->storage_owner != view
        || view->storage_generation != storage_generation + 1u
        || view->view_generation != view_generation + 1u
        || col_rel_storage_alias_borrow_count(source) != 0) {
        test_rel_free(view);
        test_rel_free(source);
        FAIL("nullary hash path did not privatize and publish exactly once");
    }
    test_rel_free(view);
    test_rel_free(source);
    PASS();
}

static void
test_zero_arity_merge_output_is_never_zero_sized(void)
{
    const uint32_t boundaries[] = { 0, 1, 3 };
    /* segment_starts + segment_ends for seg_count == 2.  A zero-arity
     * relation adds no radix workspace, so this is the whole scratch
     * requirement apart from the merge output itself. */
    const uint64_t segment_scratch = 2u * 2u * sizeof(uint32_t);
    const uint64_t sequence_metadata = 3u * sizeof(uint32_t)
        + 2u * sizeof(uint8_t);
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
    const uint64_t descriptor_bytes = sizeof(col_rel_t)
        + sizeof("consolidate_test");
    ref = test_consolidate_governor_create(descriptor_bytes
            + segment_scratch);
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
    ref = test_consolidate_governor_create(descriptor_bytes
            + segment_scratch + sizeof(int64_t) + sequence_metadata);
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
            wl_columnar_memory_governor_ref_get(ref))
        != rel->descriptor_reserved_bytes) {
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
static col_delta_timestamp_t
kway_timestamp(uint32_t original)
{
    return (col_delta_timestamp_t){ .iteration = original + 1,
                                    .stratum = original + 11,
                                    .worker = original + 21,
                                    .multiplicity = original % 3 ==
                                        0 ? -5 : original % 3 == 1 ? 0 : 7 };
}
static bool
kway_timestamp_equal(col_delta_timestamp_t a, col_delta_timestamp_t b)
{
    return a.iteration == b.iteration && a.stratum == b.stratum
           && a.worker == b.worker && a.multiplicity == b.multiplicity;
}

static void
test_timestamp_set_merge(uint32_t segments, uint32_t rows_per_segment,
    unsigned failure_mode, bool alias, bool empty_segment)
{
    TEST("timestamped k-way set merge keeps first full input record");
    uint32_t boundaries[6] = { 0 };
    uint32_t first[17];
    for (uint32_t k = 0; k < 17; k++) first[k] = UINT32_MAX;
    col_rel_t *source = col_rel_new_auto("timestamp_source", 1);
    col_rel_t *view = NULL;
    col_rel_t *rel = source;
    wl_columnar_memory_governor_ref_t *ref = NULL;
    wl_columnar_source_access_reader_t reader = { 0 };
    const char *failure = NULL;
    uint32_t total = 0;
#define KTS_CHECK(c, m) do { if (!(c)) { failure = m; goto cleanup; \
                             } } while (0)
    KTS_CHECK(source && col_rel_enable_timestamps(source) == 0, "setup");
    for (uint32_t s = 0; s < segments; s++) {
        uint32_t count = empty_segment && s == 1 ? 0 : rows_per_segment;
        for (uint32_t r = 0; r < count; r++) {
            int64_t key = (total * 13u + 7u) % 17;
            KTS_CHECK(col_rel_append_row(source, &key) == 0, "append");
            source->timestamps[total] = kway_timestamp(total);
            if (first[key] == UINT32_MAX) first[key] = total;
            total++;
        }
        boundaries[s + 1] = total;
    }
    if (alias) {
        view = col_rel_new_like("timestamp_view", source);
        KTS_CHECK(view && col_rel_install_shared_view(view, source) == 0,
            "shared view");
        rel = view;
    }
    ref = test_consolidate_governor_create(UINT64_C(1) << 30);
    KTS_CHECK(ref && col_rel_attach_memory_governor(rel, ref) == 0,
        "governor");
    wl_columnar_memory_governor_t *g = wl_columnar_memory_governor_ref_get(ref);
    uint64_t baseline = wl_columnar_memory_reserved(g);
    uint64_t generation = rel->view_generation;
    uint64_t storage = rel->storage_generation;
    int64_t *column = rel->columns[0];
    if (failure_mode == 1)
        KTS_CHECK(col_rel_source_reader_acquire(rel, &reader) == 0, "reader");
    else if (failure_mode == 2)
        fail_consolidate_allocation_at("merge_timestamps");
    else if (failure_mode == 3)
        fail_consolidate_allocation_at("radix_workspace_timestamps");
    else if (failure_mode == 4)
        atomic_store_explicit(&g->usable_bytes, baseline, memory_order_relaxed);
    if (failure_mode) {
        int rc = col_op_consolidate_kway_merge(rel, boundaries, segments);
        atomic_store_explicit(&g->usable_bytes, UINT64_C(1) << 30,
            memory_order_relaxed);
        KTS_CHECK(rc == (failure_mode == 1 ? EBUSY : ENOMEM), "refusal status");
        if (failure_mode == 2 || failure_mode == 3)
            KTS_CHECK(consolidate_fail_used, "allocation fault witness");
        KTS_CHECK(rel->nrows == total && rel->columns[0] == column
            && rel->view_generation == generation
            && rel->storage_generation == storage
            && wl_columnar_memory_reserved(g) == baseline,
            "refusal changed ownership, generations or reservation");
        for (uint32_t r = 0; r < total; r++)
            KTS_CHECK(col_rel_get(rel, r, 0) == (int64_t)((r * 13u + 7u) % 17)
                && kway_timestamp_equal(rel->timestamps[r], kway_timestamp(r)),
                "refusal changed row or timestamp");
        clear_consolidate_allocation_failure();
        if (reader.owner)
            KTS_CHECK(col_rel_source_reader_release(&reader) == 0, "release");
    }
    KTS_CHECK(col_op_consolidate_kway_merge(rel, boundaries, segments) == 0,
        "merge/retry");
    KTS_CHECK(rel->nrows == 17 && rel->timestamps, "unique rows and mode");
    for (uint32_t key = 0; key < 17; key++)
        KTS_CHECK(col_rel_get(rel, key, 0) == key
            && kway_timestamp_equal(rel->timestamps[key],
            kway_timestamp(first[key])),
            "set representative must be first entire record, not weight sum");
    if (alias)
        for (uint32_t r = 0; r < total; r++)
            KTS_CHECK(col_rel_get(source, r,
                0) == (int64_t)((r * 13u + 7u) % 17)
                && kway_timestamp_equal(source->timestamps[r],
                kway_timestamp(r)),
                "COW changed source provenance");
cleanup:
    clear_consolidate_allocation_failure();
    if (reader.owner) (void)col_rel_source_reader_release(&reader);
    col_rel_destroy(view);
    col_rel_destroy(source);
    if (ref) {
        if (wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                ref))
            != 0 && !failure) failure = "reservation leak";
        wl_columnar_memory_governor_ref_release(ref);
    }
    if (failure) {
        FAIL(failure);
    }
    PASS();
#undef KTS_CHECK
}

static void
test_timestamp_cons_entry(unsigned ownership, unsigned route, unsigned fault)
{
    TEST(
        "timestamp CONS owned/borrowed/COW entry preserves provenance and retry");
    const int64_t values[] = { 1, 3, 2, 1, 3, 2 };
    const uint32_t representatives[] = { 0, 2, 1 };
    uint32_t count = route == 3 ? 0 : route == 4 ? 1 : 6;
    col_rel_t *source = col_rel_new_auto("cons_source", 1);
    col_rel_t *unowned = NULL;
    col_rel_t *input = source;
    delta_pool_t *pool = NULL;
    wl_col_session_t sess = { 0 };
    eval_stack_t stack;
    eval_stack_init(&stack);
    wl_columnar_source_access_reader_t reader = { 0 };
    wl_columnar_source_access_writer_t writer = { 0 };
    wl_columnar_memory_governor_ref_t *source_governor = NULL;
    bool source_owned = true;
    const char *failure = NULL;
#define CONS_TS_CHECK(c, m) do { if (!(c)) { failure = m; goto cleanup; \
                                 } } while (0)
    CONS_TS_CHECK(source && col_rel_enable_timestamps(source) == 0, "source");
    for (uint32_t i = 0; i < count; i++) {
        CONS_TS_CHECK(col_rel_append_row(source, &values[i]) == 0, "append");
        source->timestamps[i] = kway_timestamp(i);
    }
    if (ownership == 1 && route == 0 && fault == 0) {
        source_governor = test_consolidate_governor_create(UINT64_C(1) << 30);
        CONS_TS_CHECK(source_governor
            && col_rel_attach_memory_governor(source, source_governor) == 0,
            "source governor");
    }
    if (route == 1) source->sorted_nrows = 2;
    uint64_t source_view = source->view_generation;
    uint32_t source_sorted = source->sorted_nrows;
    if (ownership == 2) {
        unowned = col_rel_new_like("cons_view", source);
        CONS_TS_CHECK(unowned && col_rel_install_shared_view(unowned,
            source) == 0,
            "view");
        input = unowned;
    }
    if (ownership == 1) {
        pool = delta_pool_create(4, sizeof(col_rel_t), 4096);
        CONS_TS_CHECK(pool, "pool");
        sess.delta_pool = pool;
    }
    /* A full stack proves failed CONS can restore its one original entry. */
    for (uint32_t i = 0; i < COL_STACK_MAX - 1; i++)
        CONS_TS_CHECK(eval_stack_push(&stack, source, false) == 0,
            "lower push");
    CONS_TS_CHECK(eval_stack_push(&stack, input, ownership != 1) == 0,
        "input push");
    if (ownership == 0) source_owned = false;
    if (ownership == 2) unowned = NULL;
    eval_entry_t *entry = &stack.items[stack.top - 1];
    if (route == 2) {
        entry->seg_boundaries = malloc(4 * sizeof(uint32_t));
        CONS_TS_CHECK(entry->seg_boundaries, "segments");
        const uint32_t boundaries[] = { 0, 2, 4, 6 };
        memcpy(entry->seg_boundaries, boundaries, sizeof(boundaries));
        entry->seg_count = 3;
    }
    uint32_t *segments = entry->seg_boundaries;
    if (fault == 1) {
        if (ownership == 1)
            CONS_TS_CHECK(col_rel_source_writer_acquire(input, &writer) == 0,
                "block borrowed capture");
        else
            CONS_TS_CHECK(col_rel_source_reader_acquire(input, &reader) == 0,
                "block owned normalize");
    } else if (fault == 2)
        fail_consolidate_allocation_at(route == 0
            ? "radix_workspace_timestamps" : "merge_timestamps");
    if (fault) {
        int rc = col_op_consolidate(&stack, &sess);
        CONS_TS_CHECK(rc == (fault == 1 ? EBUSY : ENOMEM)
            && stack.top == COL_STACK_MAX && entry->rel == input
            && entry->owned == (ownership != 1)
            && entry->seg_boundaries == segments,
            "retry entry or segments lost");
        if (fault == 2) CONS_TS_CHECK(consolidate_fail_used, "fault witness");
        for (uint32_t i = 0; i < count; i++)
            CONS_TS_CHECK(col_rel_get(input, i, 0) == values[i]
                && kway_timestamp_equal(input->timestamps[i],
                kway_timestamp(i)),
                "refusal changed input");
        clear_consolidate_allocation_failure();
        if (reader.owner)
            CONS_TS_CHECK(col_rel_source_reader_release(&reader) == 0,
                "release reader");
        if (writer.owner)
            CONS_TS_CHECK(wl_columnar_source_access_writer_release(&writer) ==
                0,
                "release writer");
    }
    CONS_TS_CHECK(col_op_consolidate(&stack, &sess) == 0
        && stack.top == COL_STACK_MAX, "CONS retry");
    entry = &stack.items[stack.top - 1];
    col_rel_t *out = entry->rel;
    uint32_t expected = count > 1 ? 3 : count;
    CONS_TS_CHECK(out && entry->owned && out->timestamps
        && out->nrows == expected && out->sorted_nrows == expected
        && out->run_count == 1 && out->run_ends[0] == expected
        && !entry->seg_boundaries && entry->seg_count == 0, "output metadata");
    if (source_governor)
        CONS_TS_CHECK(out->memory_governor != NULL,
            "borrowed clone lost governor");
    for (uint32_t i = 0; i < expected; i++) {
        uint32_t original = count > 1 ? representatives[i] : 0;
        CONS_TS_CHECK(col_rel_get(out, i, 0) == (int64_t)i + 1
            && kway_timestamp_equal(out->timestamps[i],
            kway_timestamp(original)),
            "first whole representative record");
    }
    if (ownership != 0) {
        CONS_TS_CHECK(source->view_generation == source_view
            && source->sorted_nrows == source_sorted && source->nrows == count,
            "source metadata changed");
        for (uint32_t i = 0; i < count; i++)
            CONS_TS_CHECK(col_rel_get(source, i, 0) == values[i]
                && kway_timestamp_equal(source->timestamps[i],
                kway_timestamp(i)),
                "source provenance changed");
    }
cleanup:
    clear_consolidate_allocation_failure();
    if (reader.owner) (void)col_rel_source_reader_release(&reader);
    if (writer.owner) (void)wl_columnar_source_access_writer_release(&writer);
    while (stack.top) {
        eval_entry_t e = eval_stack_pop(&stack);
        free(e.seg_boundaries);
        if (e.owned) col_rel_destroy(e.rel);
    }
    col_rel_destroy(unowned);
    if (source_owned) col_rel_destroy(source);
    if (source_governor) wl_columnar_memory_governor_ref_release(
            source_governor);
    if (pool) delta_pool_destroy(pool);
    if (failure) {
        FAIL(failure);
    }
    PASS();
#undef CONS_TS_CHECK
}

static void
test_timestamp_cons_governor_denial(void)
{
    TEST("timestamp CONS borrowed clone admission denial retains entry");
    col_rel_t *source = col_rel_new_auto("governed_source", 1);
    wl_columnar_memory_governor_ref_t *denied = NULL, *allowed = NULL;
    wl_col_session_t sess = { 0 };
    eval_stack_t stack;
    eval_stack_init(&stack);
    const char *failure = NULL;
#define GOV_CHECK(c, m) do { if (!(c)) { failure = m; goto cleanup; \
                             } } while (0)
    GOV_CHECK(source && col_rel_enable_timestamps(source) == 0, "source");
    int64_t value = 42;
    GOV_CHECK(col_rel_append_row(source, &value) == 0, "append");
    source->timestamps[0] = kway_timestamp(7);
    GOV_CHECK(eval_stack_push(&stack, source, false) == 0, "stack");
    denied = test_consolidate_governor_create(1);
    GOV_CHECK(denied, "denied governor");
    sess.memory_governor = denied;
    eval_entry_t *entry = &stack.items[stack.top - 1];
    GOV_CHECK(col_op_consolidate(&stack, &sess) == ENOSPC
        && stack.top == 1 && entry->rel == source && !entry->owned
        && source->nrows == 1 && source->timestamps[0].iteration == 8,
        "denial lost borrowed entry");
    GOV_CHECK(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(denied)) == 0,
        "denial reservation leak");
    wl_columnar_memory_governor_ref_release(denied);
    denied = NULL;
    allowed = test_consolidate_governor_create(UINT64_C(1) << 30);
    GOV_CHECK(allowed, "allowed governor");
    sess.memory_governor = allowed;
    GOV_CHECK(col_op_consolidate(&stack, &sess) == 0
        && stack.top == 1 && stack.items[0].owned
        && stack.items[0].rel->memory_governor == allowed,
        "governed retry");
cleanup:
    while (stack.top) {
        eval_entry_t e = eval_stack_pop(&stack);
        free(e.seg_boundaries);
        if (e.owned) col_rel_destroy(e.rel);
    }
    col_rel_destroy(source);
    if (denied) wl_columnar_memory_governor_ref_release(denied);
    if (allowed) wl_columnar_memory_governor_ref_release(allowed);
    if (failure) FAIL(failure); else PASS();
#undef GOV_CHECK
}

static void
test_governed_borrowed_cons_late_allocator(unsigned route)
{
    TEST("governed borrowed CONS restores entry after late allocator failure");
    wl_col_session_t sess = { 0 };
    col_rel_t *source = col_rel_new_auto("late-cons", 1);
    eval_stack_t stack;
    eval_stack_init(&stack);
    const char *failure = NULL;
    uint32_t nrows = route == 2 ? 10001u : 42u;
#define LATE_CHECK(c, m) do { if (!(c)) { failure = m; goto cleanup; \
                              } } while (0)
    sess.memory_governor = test_consolidate_governor_create(UINT64_C(1) << 26);
    LATE_CHECK(source && sess.memory_governor, "late CONS fixture");
    if (route == 4) {
        wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
        LATE_CHECK(col_rel_set_column_types(source, &type, 1) == 0,
            "late float metadata");
    }
    for (uint32_t i = 0; i < nrows; i++) {
        int64_t value = (nrows - i) % 7u;
        if (route == 4)
            value = wl_columnar_float_to_bits((double)value - 3.0);
        LATE_CHECK(col_rel_append_row(source, &value) == 0, "late source row");
    }
    if (route == 3)
        LATE_CHECK(col_rel_enable_timestamps(source) == 0, "late timestamps");
    LATE_CHECK(eval_stack_push(&stack, source, false) == 0, "late input push");
    uint32_t *bounds = malloc(3u * sizeof(*bounds));
    LATE_CHECK(bounds, "late input metadata");
    bounds[0] = 0; bounds[1] = nrows / 2u; bounds[2] = nrows;
    stack.items[0].seg_boundaries = bounds;
    stack.items[0].seg_count = route == 0 || route == 2 || route == 4 ? 1u : 2u;
    stack.items[0].is_delta = true;
    if (stack.items[0].seg_count == 1) bounds[1] = nrows;
    uint64_t generation = source->view_generation;
    const char *site = route == 0 ? "radix_workspace_perm_a"
        : route == 4 ? "radix_workspace_insertion_rows"
        : route == 2 ? "hash_table"
        : route == 3 ? "merge_timestamps" : "merge_output";
    fail_consolidate_allocation_at(site);
    LATE_CHECK(col_op_consolidate(&stack, &sess) == ENOMEM
        && consolidate_fail_used && !sess.memory_budget_denied
        && stack.top == 1 && stack.items[0].rel == source
        && !stack.items[0].owned && stack.items[0].is_delta
        && stack.items[0].seg_boundaries == bounds
        && source->nrows == nrows && source->view_generation == generation
        && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
            sess.memory_governor)) == 0,
        "late allocator refusal must restore complete input and credit");
    for (uint32_t i = 0; i < nrows; i++)
        LATE_CHECK(source->columns[0][i] == (route == 4
            ? wl_columnar_float_to_bits((double)((nrows - i) % 7u) - 3.0)
            : (int64_t)((nrows - i) % 7u)),
            "late allocator refusal changed source rows");
    clear_consolidate_allocation_failure();
    LATE_CHECK(col_op_consolidate(&stack, &sess) == 0 && stack.top == 1
        && stack.items[0].owned && stack.items[0].rel->nrows == 7,
        "late allocator refusal successful retry");
    for (uint32_t i = 0; i < 7; i++)
        LATE_CHECK(stack.items[0].rel->columns[0][i] == (route == 4
            ? wl_columnar_float_to_bits((double)i - 3.0) : (int64_t)i),
            "late allocator retry exact result");
cleanup:
    clear_consolidate_allocation_failure();
    (void)eval_stack_drain(&stack);
    col_rel_destroy(source);
    if (sess.memory_governor) {
        if (wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                sess.memory_governor)) != 0 && !failure)
            failure = "late CONS leaked credit";
        wl_columnar_memory_governor_ref_release(sess.memory_governor);
    }
    if (failure) FAIL(failure); else PASS();
#undef LATE_CHECK
}

/* Exercise the public preparation primitive with real governor outcomes and
 * intercepted allocations, without mutating or forging a relation shape. */
static void
test_radix_workspace_preflight(uint32_t count, bool timestamped)
{
    TEST("prepared radix workspace retains ownership and typed failures");
    col_rel_t *rel = col_rel_new_auto("workspace", 1);
    wl_columnar_memory_governor_ref_t *ref =
        test_consolidate_governor_create(UINT64_C(1) << 30);
    wl_columnar_radix_workspace_t workspace = { 0 };
    wl_columnar_source_access_writer_t writer = { 0 };
    int64_t *before = malloc((size_t)count * sizeof(*before));
    col_delta_timestamp_t *before_ts = timestamped
        ? calloc(count, sizeof(*before_ts)) : NULL;
    const char *failure = NULL;
#define WP_CHECK(c, m) do { if (!(c)) { failure = m; goto cleanup; } } while (0)
    WP_CHECK(rel && ref && before && (!timestamped || before_ts), "fixture");
    if (timestamped)
        WP_CHECK(col_rel_enable_timestamps(rel) == 0, "enable timestamps");
    for (uint32_t i = 0; i < count; i++) {
        before[i] = count - i;
        WP_CHECK(col_rel_append_row(rel, &before[i]) == 0, "append");
        if (timestamped) {
            rel->timestamps[i] = (col_delta_timestamp_t){
                .iteration = i, .stratum = 7, .worker = 3, .multiplicity = -2,
            };
        }
    }
    if (timestamped)
        memcpy(before_ts, rel->timestamps, (size_t)count * sizeof(*before_ts));
    WP_CHECK(col_rel_attach_memory_governor(rel, ref) == 0, "attach governor");
    wl_columnar_memory_governor_t *g = wl_columnar_memory_governor_ref_get(ref);
    uint64_t baseline = wl_columnar_memory_reserved(g);
    uint64_t view = rel->view_generation, storage = rel->storage_generation;
    uint32_t bounds[] = { 0, count };
    uint64_t scratch = count <= 32 ? ((uint64_t)count + 1) * sizeof(int64_t)
        : (uint64_t)count * (2 * sizeof(uint32_t) + sizeof(int64_t)
        + (count >= 50000 ? sizeof(uint16_t) : sizeof(uint8_t)))
        + (count >= 50000 ? 65536u * sizeof(uint32_t) : 0);
    if (timestamped)
        scratch += (uint64_t)count * sizeof(col_delta_timestamp_t);
    const uint64_t extra = 19;
    atomic_store_explicit(&g->usable_bytes, baseline + scratch + extra - 1,
        memory_order_seq_cst);
    WP_CHECK(wl_columnar_radix_workspace_prepare(rel, bounds, 1, extra,
        &workspace) == ENOMEM && rel->memory_budget_denial_pending
        && wl_columnar_memory_reserved(g) == baseline, "real budget denial");
    atomic_store_explicit(&g->usable_bytes, UINT64_C(1) << 30,
        memory_order_seq_cst);
    rel->memory_budget_denial_pending = false; /* New owning operation. */
    WP_CHECK(wl_columnar_radix_workspace_prepare(rel, bounds, 1, UINT64_MAX,
        &workspace) == EOVERFLOW && !rel->memory_budget_denial_pending
        && wl_columnar_memory_reserved(g) == baseline, "arithmetic overflow");
    wl_columnar_memory_reservation_t padding;
    wl_columnar_memory_reservation_init(&padding);
    atomic_store_explicit(&g->usable_bytes, UINT64_MAX, memory_order_seq_cst);
    WP_CHECK(wl_columnar_memory_reserve_checked(g, UINT64_MAX - baseline - 1,
        &padding) == WL_COLUMNAR_MEMORY_ADMISSION_OK,
        "reserve overflow padding");
    int rc = wl_columnar_radix_workspace_prepare(rel, bounds, 1, 0,
            &workspace);
    uint64_t after = wl_columnar_memory_reserved(g);
    bool released = wl_columnar_memory_release(&padding);
    atomic_store_explicit(&g->usable_bytes, UINT64_C(1) << 30,
        memory_order_seq_cst);
    WP_CHECK(rc == EOVERFLOW && after == UINT64_MAX - 1 && released
        && wl_columnar_memory_reserved(g) == baseline
        && !rel->memory_budget_denial_pending, "accounting overflow");

    const uint32_t invalid[][3] = {
        { 0, count + 1, count }, { 1, 0, count }, { 0, count, count + 1 },
    };
    for (uint32_t i = 0; i < 3; i++)
        WP_CHECK(wl_columnar_radix_workspace_prepare(rel, invalid[i], 2, 0,
            &workspace) == EINVAL && !rel->memory_budget_denial_pending
            && wl_columnar_memory_reserved(g) == baseline, "invalid range");

    const char *sites[] = {
        "radix_workspace_timestamps", "radix_workspace_insertion_rows",
        "radix_workspace_perm_a", "radix_workspace_perm_b",
        "radix_workspace_temp_column", "radix_workspace_bucket_values",
        "radix_workspace_count16",
    };
    for (uint32_t i = 0; i < 7; i++) {
        if ((i == 0 && !timestamped) || (i == 1 && count > 32)
            || (i >= 2 && count <= 32) || (i == 6 && count < 50000))
            continue;
        fail_consolidate_allocation_at(sites[i]);
        rc = wl_columnar_radix_workspace_prepare(rel, bounds, 1, extra,
                &workspace);
        clear_consolidate_allocation_failure();
        WP_CHECK(consolidate_fail_used && rc == ENOMEM
            && !rel->memory_budget_denial_pending
            && wl_columnar_memory_reserved(g) == baseline
            && !workspace.admission_active && !workspace.perm_a
            && !workspace.timestamps && !workspace.insertion_rows,
            "allocator failure must release prior allocations and credit");
    }
    rel->memory_budget_denial_pending = true; /* Evidence from an outer scope. */
    WP_CHECK(wl_columnar_radix_workspace_prepare(rel, bounds, 1, UINT64_MAX,
        &workspace) == EOVERFLOW && rel->memory_budget_denial_pending,
        "nested failure must preserve earlier denial");
    atomic_store_explicit(&g->usable_bytes, baseline + scratch + extra,
        memory_order_seq_cst);
    WP_CHECK(wl_columnar_radix_workspace_prepare(rel, bounds, 1, extra,
        &workspace) == 0 && rel->memory_budget_denial_pending
        && wl_columnar_memory_reserved(g) == baseline + scratch + extra,
        "exact admission and nested success");
    void *allocation = count <= 32 ? (void *)workspace.insertion_rows
        : (void *)workspace.perm_a;
    WP_CHECK(wl_columnar_radix_workspace_prepare(rel, bounds, 1, 0,
        &workspace) == EINVAL
        && allocation == (count <= 32 ? (void *)workspace.insertion_rows
            : (void *)workspace.perm_a)
        && wl_columnar_memory_reserved(g) == baseline + scratch + extra,
        "live workspace must remain owned after refusal");
    WP_CHECK(memcmp(before, rel->columns[0],
        (size_t)count * sizeof(*before)) == 0
        && (!timestamped || memcmp(before_ts, rel->timestamps,
        (size_t)count * sizeof(*before_ts)) == 0)
        && rel->view_generation == view && rel->storage_generation == storage,
        "preparation must preserve source bytes and generations");
    WP_CHECK(col_rel_source_writer_acquire(rel, &writer) == 0, "writer");
    rc = wl_columnar_relation_radix_sort_with_workspace(rel, 0, count,
            &writer, &workspace);
    WP_CHECK(wl_columnar_source_access_writer_release(&writer) == 0,
        "release writer");
    WP_CHECK(rc == 0 &&
        wl_columnar_memory_reserved(g) == baseline + scratch + extra,
        "prepared sort must not charge twice");
    for (uint32_t i = 0; i < count; i++)
        WP_CHECK(rel->columns[0][i] == (int64_t)i + 1
            && (!timestamped || memcmp(&rel->timestamps[i],
            &before_ts[count - i - 1],
            sizeof(*before_ts)) == 0), "retry rows and timestamp permutation");
    wl_columnar_radix_workspace_destroy(&workspace);
    WP_CHECK(wl_columnar_memory_reserved(g) == baseline, "scratch retired");
    uint32_t partial[] = { 1, 1, count - 1 }; /* empty then sorted partial range */
    WP_CHECK(wl_columnar_radix_workspace_prepare(rel, partial, 2, 0,
        &workspace) == 0 && !workspace.admission_active && !workspace.perm_a
        && !workspace.insertion_rows && !workspace.timestamps
        && wl_columnar_memory_reserved(g) == baseline,
        "sorted and empty ranges");
cleanup:
    clear_consolidate_allocation_failure();
    if (writer.owner)
        (void)wl_columnar_source_access_writer_release(&writer);
    wl_columnar_radix_workspace_destroy(&workspace);
    col_rel_destroy(rel);
    free(before);
    free(before_ts);
    if (ref) {
        if (wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                ref)) != 0)
            failure = "reservation leak after teardown";
        wl_columnar_memory_governor_ref_release(ref);
    }
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef WP_CHECK
}

static int
test_radix_admitted_call(col_rel_t *rel, unsigned route)
{
    if (route == 2)
        return col_rel_radix_sort_int64(rel);
    if (route == 0)
        return col_rel_radix_sort(rel, 0, rel->nrows);
    wl_columnar_source_access_writer_t writer = { 0 };
    int rc = col_rel_source_writer_acquire(rel, &writer);
    if (rc != 0)
        return rc;
    bool pending = false;
    rc = col_rel_radix_sort_locked(rel, 0, rel->nrows, &writer, false,
            &pending);
    int release_rc = wl_columnar_source_access_writer_release(&writer);
    return rc != 0 ? rc : release_rc;
}

static void
test_radix_direct_admission(uint32_t count, unsigned route, unsigned ownership,
    bool timestamped, bool sorted, bool floating)
{
    TEST("direct radix scratch admission precedes mutation and COW");
    col_rel_t *source = col_rel_new_auto("direct-radix", 1), *view = NULL;
    col_rel_t *rel = source;
    wl_arena_t *arena = NULL;
    wl_columnar_memory_governor_ref_t *ref =
        test_consolidate_governor_create(UINT64_C(1) << 30);
    int64_t *before = malloc((size_t)count * sizeof(*before));
    col_delta_timestamp_t *before_ts = timestamped
        ? calloc(count, sizeof(*before_ts)) : NULL;
    const char *failure = NULL;
#define DA_CHECK(c, m) do { if (!(c)) { failure = m; goto cleanup; } } while (0)
    DA_CHECK(source && ref && before && (!timestamped || before_ts), "fixture");
    if (floating) {
        wirelog_column_type_t type = WIRELOG_TYPE_FLOAT;
        DA_CHECK(col_rel_set_column_types(source, &type, 1) == 0,
            "float schema");
    }
    if (timestamped)
        DA_CHECK(col_rel_enable_timestamps(source) == 0, "timestamps");
    for (uint32_t i = 0; i < count; i++) {
        int64_t value = sorted ? i + 1 : count - i;
        before[i] = floating ? wl_columnar_float_to_bits((double)value) : value;
        DA_CHECK(col_rel_append_row(source, &before[i]) == 0, "append");
        if (timestamped)
            source->timestamps[i] = (col_delta_timestamp_t){
                .iteration = i, .stratum = 4, .worker = 2, .multiplicity = -3,
            };
    }
    if (timestamped)
        memcpy(before_ts, source->timestamps,
            (size_t)count * sizeof(*before_ts));
    if (ownership == 1) {
        view = col_rel_new_like("radix-view", source);
        DA_CHECK(view && col_rel_install_shared_view(view, source) == 0,
            "alias");
        rel = view;
    } else if (ownership == 2) {
        arena = wl_arena_create(4096);
        int64_t *column = arena ? wl_arena_alloc(arena,
                (size_t)source->capacity * sizeof(*column)) : NULL;
        DA_CHECK(column, "arena payload");
        memcpy(column, source->columns[0], (size_t)count * sizeof(*column));
        free(source->columns[0]);
        source->columns[0] = column;
        source->arena_owned = true;
    }
    DA_CHECK(col_rel_attach_memory_governor(rel, ref) == 0, "governor");
    wl_columnar_memory_governor_t *g = wl_columnar_memory_governor_ref_get(ref);
    uint64_t baseline = wl_columnar_memory_reserved(g);
    uint64_t generation = rel->view_generation,
        storage = rel->storage_generation;
    int64_t *column_before = rel->columns[0];
    bool *shared_before = rel->col_shared;
    uint64_t scratch = count <= 32 || floating
        ? ((uint64_t)count + 1) * sizeof(int64_t)
        : (uint64_t)count * (2 * sizeof(uint32_t) + sizeof(int64_t)
        + (count >= 50000 ? sizeof(uint16_t) : sizeof(uint8_t)))
        + (count >= 50000 ? 65536u * sizeof(uint32_t) : 0);
    if (timestamped)
        scratch += (uint64_t)count * sizeof(col_delta_timestamp_t);
    atomic_store_explicit(&g->usable_bytes, baseline + scratch - 1,
        memory_order_seq_cst);
    int rc = test_radix_admitted_call(rel, route);
    atomic_store_explicit(&g->usable_bytes, UINT64_C(1) << 30,
        memory_order_seq_cst);
    DA_CHECK(rc == ENOMEM && rel->memory_budget_denial_pending,
        "scratch one-byte-short must deny");
    const char *sites[] = {
        "radix_workspace_timestamps", "radix_workspace_insertion_rows",
        "radix_workspace_perm_a", "radix_workspace_perm_b",
        "radix_workspace_temp_column", "radix_workspace_bucket_values",
        "radix_workspace_count16",
    };
    for (uint32_t i = 0; i < 7; i++) {
        bool insertion = count <= 32 || floating;
        if ((i == 0 && !timestamped) || (i == 1 && !insertion)
            || (i >= 2 && insertion) || (i == 6 && count < 50000))
            continue;
        rel->memory_budget_denial_pending = route != 1; /* Stale outer cause. */
        fail_consolidate_allocation_at(sites[i]);
        rc = test_radix_admitted_call(rel, route);
        clear_consolidate_allocation_failure();
        DA_CHECK(consolidate_fail_used && rc == ENOMEM
            && !rel->memory_budget_denial_pending, "actual allocator failure");
        if (route == 1) {
            rel->memory_budget_denial_pending = true;
            fail_consolidate_allocation_at(sites[i]);
            rc = test_radix_admitted_call(rel, route);
            clear_consolidate_allocation_failure();
            DA_CHECK(consolidate_fail_used && rc == ENOMEM
                && rel->memory_budget_denial_pending,
                "nested allocator failure preserves earlier denial");
        }
    }
    wl_columnar_memory_reservation_t padding;
    wl_columnar_memory_reservation_init(&padding);
    atomic_store_explicit(&g->usable_bytes, UINT64_MAX, memory_order_seq_cst);
    DA_CHECK(wl_columnar_memory_reserve_checked(g, UINT64_MAX - baseline,
        &padding) == WL_COLUMNAR_MEMORY_ADMISSION_OK, "scratch padding");
    rel->memory_budget_denial_pending = false;
    rc = test_radix_admitted_call(rel, route);
    bool released = wl_columnar_memory_release(&padding);
    atomic_store_explicit(&g->usable_bytes, UINT64_C(1) << 30,
        memory_order_seq_cst);
    DA_CHECK(rc == EOVERFLOW && released && !rel->memory_budget_denial_pending,
        "direct scratch overflow keeps typed cause");
    wl_columnar_source_access_reader_t reader = { 0 };
    DA_CHECK(col_rel_source_reader_acquire(rel, &reader) == 0, "held reader");
    rel->memory_budget_denial_pending = true;
    rc = test_radix_admitted_call(rel, route);
    int release_rc = col_rel_source_reader_release(&reader);
    DA_CHECK(rc == EBUSY && release_rc == 0 &&
        rel->memory_budget_denial_pending,
        "writer refusal preserves earlier denial");
    DA_CHECK(wl_columnar_memory_reserved(g) == baseline
        && rel->columns[0] == column_before && rel->col_shared == shared_before
        && rel->view_generation == generation &&
        rel->storage_generation == storage
        && memcmp(before, rel->columns[0], (size_t)count * sizeof(*before)) == 0
        && (!timestamped || memcmp(before_ts, rel->timestamps,
        (size_t)count * sizeof(*before_ts)) == 0), "refusal changed source");
    if (ownership == 1)
        DA_CHECK(col_rel_storage_alias_borrow_count(source) == 1,
            "refusal detached alias");
    /* Exact scratch budget proves no duplicate admission for owned storage.
     * Shared storage additionally requires the independently admitted COW. */
    if (ownership != 1)
        atomic_store_explicit(&g->usable_bytes, baseline + scratch,
            memory_order_seq_cst);
    rel->memory_budget_denial_pending = true;
    DA_CHECK(test_radix_admitted_call(rel, route) == 0
        && rel->memory_budget_denial_pending == (route == 1),
        "retry/provenance");
    for (uint32_t i = 0; i < count; i++) {
        int64_t expected = floating ? wl_columnar_float_to_bits((double)i + 1)
            : (int64_t)i + 1;
        DA_CHECK(rel->columns[0][i] == expected
            && (!timestamped || memcmp(&rel->timestamps[i],
            &before_ts[sorted ? i : count - i - 1], sizeof(*before_ts)) == 0),
            "exact sorted output");
    }
    if (ownership == 1)
        DA_CHECK(col_rel_storage_alias_borrow_count(source) == 0
            && memcmp(before, source->columns[0],
            (size_t)count * sizeof(*before)) == 0,
            "COW source or alias ownership");
    else
        DA_CHECK(wl_columnar_memory_reserved(g) == baseline, "scratch leak");
cleanup:
    clear_consolidate_allocation_failure();
    col_rel_destroy(view);
    col_rel_destroy(source);
    wl_arena_free(arena);
    free(before);
    free(before_ts);
    if (ref) {
        if (wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                ref)) != 0)
            failure = "credit leak after direct sort teardown";
        wl_columnar_memory_governor_ref_release(ref);
    }
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef DA_CHECK
}

static void
test_radix_workspace_width(uint32_t count, bool floating)
{
    TEST("insertion workspace checks byte capacity before COW");
    col_rel_t *narrow = col_rel_new_auto("narrow", 1);
    col_rel_t *wide = col_rel_new_auto("wide", 2), *view = NULL;
    wl_columnar_memory_governor_ref_t *ref =
        test_consolidate_governor_create(UINT64_C(1) << 30);
    wl_columnar_radix_workspace_t workspace = { 0 };
    wl_columnar_source_access_writer_t writer = { 0 };
    const char *failure = NULL;
#define WW_CHECK(c, m) do { if (!(c)) { failure = m; goto cleanup; } } while (0)
    WW_CHECK(narrow && wide && ref, "fixture");
    if (floating) {
        wirelog_column_type_t types[] = { WIRELOG_TYPE_FLOAT,
                                          WIRELOG_TYPE_FLOAT };
        WW_CHECK(col_rel_set_column_types(narrow, types, 1) == 0
            && col_rel_set_column_types(wide, types, 2) == 0, "float schemas");
    }
    for (uint32_t i = 0; i < count; i++) {
        int64_t row[] = { count - i, (count - i) * 10 };
        if (floating) {
            row[0] = wl_columnar_float_to_bits((double)row[0]);
            row[1] = wl_columnar_float_to_bits((double)row[1]);
        }
        WW_CHECK(col_rel_append_row(narrow, row) == 0
            && col_rel_append_row(wide, row) == 0, "append");
    }
    view = col_rel_new_like("width-view", wide);
    WW_CHECK(view && col_rel_install_shared_view(view, wide) == 0
        && col_rel_attach_memory_governor(narrow, ref) == 0
        && col_rel_attach_memory_governor(wide, ref) == 0
        && col_rel_attach_memory_governor(view, ref) == 0, "governed fixtures");
    wl_columnar_memory_governor_t *g = wl_columnar_memory_governor_ref_get(ref);
    uint32_t bounds[] = { 0, count };
    uint64_t baseline = wl_columnar_memory_reserved(g);
    WW_CHECK(wl_columnar_radix_workspace_prepare(narrow, bounds, 1, 0,
        &workspace) == 0, "narrow preparation");
    uint64_t admitted = wl_columnar_memory_reserved(g);
    uint64_t generation = view->view_generation,
        storage = view->storage_generation;
    int64_t *old_column = view->columns[0];
    WW_CHECK(col_rel_source_writer_acquire(view, &writer) == 0, "view writer");
    /* If COW is tried first, this limit yields ENOMEM, not required EINVAL. */
    atomic_store_explicit(&g->usable_bytes, admitted, memory_order_seq_cst);
    WW_CHECK(wl_columnar_relation_radix_sort_with_workspace(view, 0, count,
        &writer, &workspace) == EINVAL && view->col_shared
        && view->columns[0] == old_column && view->view_generation == generation
        && view->storage_generation == storage
        && col_rel_storage_alias_borrow_count(wide) == 1
        && wl_columnar_memory_reserved(g) == admitted,
        "width refusal before COW");
    WW_CHECK(wl_columnar_source_access_writer_release(&writer) == 0, "release");
    for (uint32_t i = 0; i < count; i++) {
        int64_t a = count - i, b = (count - i) * 10;
        if (floating) {
            a = wl_columnar_float_to_bits((double)a);
            b = wl_columnar_float_to_bits((double)b);
        }
        WW_CHECK(view->columns[0][i] == a && view->columns[1][i] == b,
            "wide bytes preserved");
    }
    WW_CHECK(col_rel_source_writer_acquire(narrow, &writer) == 0,
        "narrow writer");
    WW_CHECK(wl_columnar_relation_radix_sort_with_workspace(narrow, 0, count,
        &writer, &workspace) == 0, "original workspace still usable");
    WW_CHECK(wl_columnar_source_access_writer_release(&writer) == 0, "release");
    wl_columnar_radix_workspace_destroy(&workspace);
    WW_CHECK(workspace.insertion_bytes == 0
        && wl_columnar_memory_reserved(g) == baseline,
        "destroy clears capacity");
    atomic_store_explicit(&g->usable_bytes, UINT64_C(1) << 30,
        memory_order_seq_cst);
    WW_CHECK(wl_columnar_radix_workspace_prepare(wide, bounds, 1, 0,
        &workspace) == 0, "wide reprepare");
    admitted = wl_columnar_memory_reserved(g);
    atomic_store_explicit(&g->usable_bytes, admitted, memory_order_seq_cst);
    WW_CHECK(col_rel_source_writer_acquire(narrow, &writer) == 0,
        "narrow writer");
    WW_CHECK(wl_columnar_relation_radix_sort_with_workspace(narrow, 0, count,
        &writer, &workspace) == 0 && wl_columnar_memory_reserved(g) == admitted,
        "wider capacity may serve narrower input without another charge");
    WW_CHECK(wl_columnar_source_access_writer_release(&writer) == 0, "release");
    for (uint32_t i = 0; i < count; i++)
        WW_CHECK(narrow->columns[0][i] == (floating
            ? wl_columnar_float_to_bits((double)i + 1) : (int64_t)i + 1),
            "narrow sorted output");

    /* The supplied workspace is admitted; force the later COW reservation
     * to overflow with a real padding token, and preserve its typed cause. */
    wl_columnar_memory_reservation_t padding;
    wl_columnar_memory_reservation_init(&padding);
    atomic_store_explicit(&g->usable_bytes, UINT64_MAX, memory_order_seq_cst);
    WW_CHECK(wl_columnar_memory_reserve_checked(g, UINT64_MAX - admitted,
        &padding) == WL_COLUMNAR_MEMORY_ADMISSION_OK, "COW overflow padding");
    int rc = col_rel_source_writer_acquire(view, &writer);
    if (rc == 0)
        rc = wl_columnar_relation_radix_sort_with_workspace(view, 0, count,
                &writer, &workspace);
    bool released = wl_columnar_memory_release(&padding);
    atomic_store_explicit(&g->usable_bytes, UINT64_C(1) << 30,
        memory_order_seq_cst);
    WW_CHECK(rc == EOVERFLOW && released && !view->memory_budget_denial_pending
        && wl_columnar_memory_reserved(g) == admitted && view->col_shared
        && view->view_generation == generation &&
        view->storage_generation == storage,
        "COW overflow must keep local cause and ownership");
    WW_CHECK(wl_columnar_relation_radix_sort_with_workspace(view, 0, count,
        &writer, &workspace) == 0 && !view->col_shared
        && col_rel_storage_alias_borrow_count(wide) == 0, "COW exact retry");
    WW_CHECK(wl_columnar_source_access_writer_release(&writer) == 0, "release");
    for (uint32_t i = 0; i < count; i++) {
        int64_t a = i + 1, b = (i + 1) * 10;
        if (floating) {
            a = wl_columnar_float_to_bits((double)a);
            b = wl_columnar_float_to_bits((double)b);
        }
        WW_CHECK(view->columns[0][i] == a && view->columns[1][i] == b,
            "COW sorted result");
    }
cleanup:
    if (writer.owner)
        (void)wl_columnar_source_access_writer_release(&writer);
    wl_columnar_radix_workspace_destroy(&workspace);
    col_rel_destroy(view);
    col_rel_destroy(narrow);
    col_rel_destroy(wide);
    if (ref) {
        if (wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                ref)) != 0)
            failure = "capacity test credit leak";
        wl_columnar_memory_governor_ref_release(ref);
    }
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef WW_CHECK
}

/* Exercise the operator's ownership boundary, not just the merge kernel. */
static void
test_governed_owned_cons(uint32_t count, uint32_t unique, unsigned route,
    unsigned storage, bool timestamped)
{
    TEST("governed owned CONS retains its complete entry on scratch refusal");
    col_rel_t *source = col_rel_new_auto("owned-cons", 1), *rel = source;
    col_rel_t *view = NULL;
    wl_arena_t *arena = NULL;
    wl_columnar_memory_governor_ref_t *ref =
        test_consolidate_governor_create(UINT64_C(1) << 30);
    int64_t *before = malloc((size_t)count * sizeof(*before));
    col_delta_timestamp_t *before_ts = timestamped
        ? malloc((size_t)count * sizeof(*before_ts)) : NULL;
    eval_stack_t stack;
    eval_stack_init(&stack);
    wl_col_session_t sess = { 0 };
    wl_columnar_source_access_reader_t reader = { 0 };
    bool source_owned = true;
    const char *failure = NULL;
#define OC_CHECK(c, m) do { if (!(c)) { failure = m; goto cleanup; } } while (0)
    OC_CHECK(source && ref && before && (!timestamped || before_ts), "fixture");
    if (timestamped)
        OC_CHECK(col_rel_enable_timestamps(source) == 0, "timestamps");
    for (uint32_t i = 0; i < count; i++) {
        before[i] = route == 1 && i < 2 ? (int64_t)i + 1
            : (int64_t)((count - i - 1) % unique) + 1;
        OC_CHECK(col_rel_append_row(source, &before[i]) == 0, "append");
        if (timestamped) source->timestamps[i] = kway_timestamp(i);
    }
    if (timestamped)
        memcpy(before_ts, source->timestamps,
            (size_t)count * sizeof(*before_ts));
    if (route == 1) source->sorted_nrows = 2;
    if (storage == 1) {
        view = col_rel_new_like("owned-cons-view", source);
        OC_CHECK(view && col_rel_install_shared_view(view, source) == 0,
            "shared view");
        rel = view;
    } else if (storage == 2) {
        arena = wl_arena_create(4096);
        int64_t *column = arena ? wl_arena_alloc(arena,
                (size_t)source->capacity * sizeof(*column)) : NULL;
        OC_CHECK(column, "arena payload");
        memcpy(column, before, (size_t)count * sizeof(*column));
        free(source->columns[0]);
        source->columns[0] = column;
        source->arena_owned = true;
    }
    OC_CHECK(col_rel_attach_memory_governor(rel, ref) == 0, "governor");
    for (uint32_t i = 0; i < COL_STACK_MAX - 1; i++)
        OC_CHECK(eval_stack_push(&stack, source, false) == 0, "lower stack");
    OC_CHECK(eval_stack_push_delta(&stack, rel, true, true) == 0,
        "owned entry");
    if (storage == 1) view = NULL; else source_owned = false;
    eval_entry_t *entry = &stack.items[stack.top - 1];
    if (route >= 2) {
        entry->seg_count = route;
        entry->seg_boundaries = malloc((route + 1) * sizeof(uint32_t));
        OC_CHECK(entry->seg_boundaries, "boundaries");
        for (unsigned i = 0; i <= route; i++)
            entry->seg_boundaries[i] = (uint32_t)((uint64_t)count * i / route);
    }
    eval_entry_t original = *entry;
    uint64_t generation = rel->view_generation, epoch = rel->storage_generation;
    uint32_t capacity = rel->capacity, sorted = rel->sorted_nrows;
    uint32_t runs = rel->run_count, run_ends[COL_MAX_RUNS];
    memcpy(run_ends, rel->run_ends, sizeof(run_ends));
    int64_t **columns = rel->columns;
    int64_t *column = rel->columns[0];
    bool *shared = rel->col_shared;
    wl_columnar_memory_governor_t *g = wl_columnar_memory_governor_ref_get(ref);
    uint64_t baseline = wl_columnar_memory_reserved(g);
    bool hash = count > 10000 && !timestamped;
    uint32_t max_segment = route >= 2 ? (count + route - 1) / route
        : route == 1 ? count - 2 : count;
    /* First deny real admission; then exercise allocation and accounting
     * failures with deliberately stale denial evidence at each new call. */
    const char *sites[] = {
        "radix_workspace_insertion_rows", "radix_workspace_perm_a",
        "radix_workspace_timestamps", "segment_starts", "segment_ends",
        "merge_output", "merge_heap", "merge_timestamps",
        "hash_table", "hash_table_used", "hash_unique_rows",
        "hash_rehash_rows", "hash_rehash_used", "hash_unique_grow",
    };
    for (unsigned fault = 0; fault < 18; fault++) {
        const char *site = fault >= 4 ? sites[fault - 4] : NULL;
        if (site) {
            unsigned i = fault - 4;
            if ((hash && i < 8) || (!hash && i >= 8)
                || (i == 0 && max_segment > 32) || (i == 1 && max_segment <= 32)
                || (i == 2 && !timestamped)
                || (i == 5 && route < 1) || (i == 6 && route < 3)
                || (i == 7 && (!timestamped || route < 1))
                || (i >= 11 && (count < 20000 || unique <= 4096)))
                continue;
        }
        if (fault == 3 && route < 2) continue;
        rel->memory_budget_denial_pending = true;
        sess.memory_budget_denied = false;
        wl_columnar_memory_reservation_t padding;
        wl_columnar_memory_reservation_init(&padding);
        uint32_t saved_boundary = 0;
        if (fault == 0)
            atomic_store_explicit(&g->usable_bytes, baseline,
                memory_order_seq_cst);
        else if (fault == 1) {
            atomic_store_explicit(&g->usable_bytes, UINT64_MAX,
                memory_order_seq_cst);
            OC_CHECK(wl_columnar_memory_reserve_checked(g,
                UINT64_MAX - baseline,
                &padding) == WL_COLUMNAR_MEMORY_ADMISSION_OK,
                "overflow padding");
        } else if (fault == 2)
            OC_CHECK(col_rel_source_reader_acquire(rel, &reader) == 0,
                "held reader");
        else if (fault == 3) {
            saved_boundary = entry->seg_boundaries[1];
            entry->seg_boundaries[1] = count + 1;
        } else
            fail_consolidate_allocation_at(site);
        int rc = col_op_consolidate(&stack, &sess);
        bool hook_used = consolidate_fail_used;
        clear_consolidate_allocation_failure();
        if (fault == 1)
            OC_CHECK(wl_columnar_memory_release(&padding), "padding release");
        atomic_store_explicit(&g->usable_bytes, UINT64_C(1) << 30,
            memory_order_seq_cst);
        if (reader.owner)
            OC_CHECK(col_rel_source_reader_release(&reader) == 0,
                "reader release");
        /* Test entry reachability before dereferencing an owned relation:
         * the legacy implementation may destroy it when scratch fails. */
        OC_CHECK(stack.top == COL_STACK_MAX && entry->rel == original.rel
            && entry->owned == original.owned &&
            entry->is_delta == original.is_delta
            && entry->seg_boundaries == original.seg_boundaries
            && entry->seg_count == original.seg_count &&
            entry->kind == original.kind
            && entry->continuation == original.continuation,
            "exact retry entry");
        if (fault == 3) entry->seg_boundaries[1] = saved_boundary;
        int expected = fault == 0 ? ENOSPC : fault == 1 ? EOVERFLOW
            : fault == 2 ? EBUSY : fault == 3 ? EINVAL : ENOMEM;
        OC_CHECK(rc == expected && (!site || hook_used),
            "typed failure/hook witness");
        OC_CHECK(rel->memory_budget_denial_pending == (fault == 0 || fault == 2)
            && sess.memory_budget_denied == (fault == 0),
            "fresh denial provenance");
        OC_CHECK(rel->nrows == count && rel->capacity == capacity
            && rel->sorted_nrows == sorted && rel->run_count == runs
            && memcmp(rel->run_ends, run_ends, sizeof(run_ends)) == 0
            && rel->columns == columns && rel->columns[0] == column
            && rel->col_shared == shared && rel->view_generation == generation
            && rel->storage_generation == epoch
            && memcmp(rel->columns[0], before,
            (size_t)count * sizeof(*before)) == 0
            && (!timestamped || memcmp(rel->timestamps, before_ts,
            (size_t)count * sizeof(*before_ts)) == 0)
            && wl_columnar_memory_reserved(g) == baseline,
            "refusal changed source/credit");
        if (storage == 1)
            OC_CHECK(col_rel_storage_alias_borrow_count(source) == 1,
                "refusal detached view");
    }
    rel->memory_budget_denial_pending = true;
    sess.memory_budget_denied = false;
    OC_CHECK(col_op_consolidate(&stack, &sess) == 0
        && stack.top == COL_STACK_MAX && entry->rel == rel && entry->owned
        && entry->is_delta == timestamped && !entry->seg_boundaries
        && entry->seg_count == 0 && rel->nrows == unique
        && rel->sorted_nrows == unique && rel->run_count == 1
        && rel->run_ends[0] == unique && !rel->memory_budget_denial_pending
        && !sess.memory_budget_denied, "exact retry metadata");
    for (uint32_t r = 0; r < unique; r++) {
        OC_CHECK(col_rel_get(rel, r, 0) == (int64_t)r + 1,
            "retry exact values");
        if (timestamped) {
            uint32_t first = 0;
            while (before[first] != (int64_t)r + 1) first++;
            OC_CHECK(kway_timestamp_equal(rel->timestamps[r], before_ts[first]),
                "first timestamp representative");
        }
    }
    if (storage == 1)
        OC_CHECK(source->nrows == count
            && memcmp(source->columns[0], before,
            (size_t)count * sizeof(*before)) == 0
            && col_rel_storage_alias_borrow_count(source) == 0,
            "source survived COW");
    else
        OC_CHECK(wl_columnar_memory_reserved(g) == baseline,
            "scratch credit retired");
cleanup:
    clear_consolidate_allocation_failure();
    if (reader.owner) (void)col_rel_source_reader_release(&reader);
    while (stack.top) {
        eval_entry_t e = eval_stack_pop(&stack);
        free(e.seg_boundaries);
        if (e.owned) col_rel_destroy(e.rel);
    }
    col_rel_destroy(view);
    if (source_owned) col_rel_destroy(source);
    wl_arena_free(arena);
    free(before);
    free(before_ts);
    if (ref) {
        if (wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                ref)))
            failure = "teardown credit leak";
        wl_columnar_memory_governor_ref_release(ref);
    }
    if (failure) {
        printf("[rows=%u unique=%u route=%u storage=%u ts=%d] ", count, unique,
            route, storage, timestamped);
        FAIL(failure);
    }
    PASS();
#undef OC_CHECK
}

int
main(void)
{
    for (unsigned route = 0; route < 4; route++) {
        test_governed_owned_cons(6, 3, route, 0, false);
        test_governed_owned_cons(64, 8, route, 0, true);
    }
    test_governed_owned_cons(64, 8, 3, 1, false);
    test_governed_owned_cons(64, 8, 2, 2, false);
    test_governed_owned_cons(20000, 5000, 3, 0, false);
    test_governed_owned_cons(12000, 12000, 2, 0, false);

    printf("=== test_consolidate_kway_merge (TDD RED PHASE) ===\n\n");
    printf("NOTE: Expected to FAIL at link time until US-002 GREEN phase\n");
    printf("      implements col_op_consolidate_kway_merge.\n\n");

    for (unsigned route = 0; route < 3; route++) {
        const uint32_t counts[] = { 3, 64, 50000 };
        for (unsigned i = 0; i < 3; i++) {
            test_radix_direct_admission(counts[i], route, 0, false, false,
                false);
            test_radix_direct_admission(counts[i], route, 0, true, true, false);
        }
        test_radix_direct_admission(64, route, 1, true, false, false);
        test_radix_direct_admission(64, route, 2, false, false, false);
        test_radix_direct_admission(64, route, 0, true, false, true);
    }
    test_radix_workspace_width(3, false);
    test_radix_workspace_width(64, true);
    test_radix_workspace_preflight(3, false);
    test_radix_workspace_preflight(64, true);
    test_radix_workspace_preflight(50000, true);
    test_single_copy_passthrough();
    test_two_copies_direct_merge();
    test_two_copies_compare_full_raw_row_width();
    test_three_copies_heap_merge();
    test_two_way_merge_empty_segments_uses_no_heap_allocation();
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
    test_leased_merge_alias_epoch_paths();
    test_merge_epoch_headroom_preflight();
    test_nullary_alias_large_hash_merge();
    test_merge_output_oom_is_transactional();
    test_zero_arity_merge_output_is_never_zero_sized();
    test_merge_heap_oom_is_transactional();
    test_consolidate_scratch_admission();
    test_later_segment_sort_oom_is_transactional();
    test_hash_allocation_oom_is_not_fallback();
    test_hash_float_signed_zero_lexicographic_order();
    test_hash_heuristic_fallback_succeeds();
    test_hash_fallback_releases_credit_before_radix_path();
    test_k16_workspace_is_preallocated();
    test_float_insertion_workspace();
    for (unsigned ownership = 0; ownership < 3; ownership++) {
        for (unsigned route = 0; route < 5; route++)
            test_timestamp_cons_entry(ownership, route, 0);
        test_timestamp_cons_entry(ownership, 2, 1);
        test_timestamp_cons_entry(ownership, 2, 2);
        test_timestamp_cons_entry(ownership, 0, 2);
    }
    for (unsigned route = 0; route < 5; route++)
        test_governed_borrowed_cons_late_allocator(route);
    test_timestamp_cons_governor_denial();
    test_timestamp_set_merge(1, 34, 0, false, false);
    test_timestamp_set_merge(2, 34, 0, false, false);
    test_timestamp_set_merge(3, 34, 0, false, true);
    test_timestamp_set_merge(3, 4001, 0, false, false);
    test_timestamp_set_merge(3, 34, 1, true, false);
    test_timestamp_set_merge(3, 34, 2, true, false);
    test_timestamp_set_merge(3, 34, 3, false, false);
    test_timestamp_set_merge(3, 34, 4, false, false);

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n", pass_count,
        fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
