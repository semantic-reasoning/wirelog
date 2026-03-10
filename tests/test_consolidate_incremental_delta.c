/*
 * test_consolidate_incremental_delta.c - TDD RED PHASE
 * Tests for col_op_consolidate_incremental_delta
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * These tests define expected behaviour BEFORE the function is implemented
 * (US-003 RED phase).  Expected failure mode:
 *
 *   undefined reference to `col_op_consolidate_incremental_delta`
 *
 * US-004 (GREEN phase): adds the function to columnar_nanoarrow.c with
 * extern linkage; backend_src is then added to this test's meson entry.
 *
 * Test cases:
 *   1. empty old (old_nrows=0) + sorted delta -> all rows in delta_out
 *   2. old + all-duplicate delta -> no change, delta_out empty
 *   3. old + unique delta (appended unsorted) -> sorted merged + new in delta_out
 *   4. first iteration (old_nrows=0) with intra-delta duplicates
 *   5. large dataset correctness oracle (1000 old, 200 dups + 500 new)
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
 * pulling in the nanoarrow dependency.  The function under test does not
 * access schema or schema_ok; fields before those (name, ncols, data,
 * nrows, capacity, col_names) are identical in both layouts.
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
 * Forward declaration of the function under test.
 * RED phase: does not exist -> link error (expected).
 * GREEN phase (US-004): implemented in columnar_nanoarrow.c with extern
 * linkage; backend_src added to meson entry.
 */
int
col_op_consolidate_incremental_delta(col_rel_t *rel, uint32_t old_nrows,
                                     col_rel_t *delta_out);

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
 * Helper: allocate col_rel_t with ncols columns and no rows.
 * col_names is populated; data starts NULL.
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
 * Helper: free col_rel_t (handles data replaced by the function).
 * ---------------------------------------------------------------- */
static void
test_rel_free(col_rel_t *r)
{
    if (!r)
        return;
    free(r->name);
    free(r->data);
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
        int64_t *nd = (int64_t *)realloc(r->data, (size_t)cap * r->ncols
                                                      * sizeof(int64_t));
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

/* ----------------------------------------------------------------
 * Helper: 1 if relation is strictly sorted (lex, int64 rows).
 * ---------------------------------------------------------------- */
/* Helper: lexicographic int64_t row comparison. */
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
        int cmp = test_row_cmp(r->data + (size_t)(i - 1) * r->ncols,
                               r->data + (size_t)i * r->ncols, r->ncols);
        if (cmp >= 0) {
            /* Debug: report first sort failure */
            if (r->ncols == 2) {
                fprintf(stderr,
                        "SORT FAIL @ row %u: [%lld,%lld] >= [%lld,%lld]\n", i,
                        r->data[(size_t)(i - 1) * 2],
                        r->data[(size_t)(i - 1) * 2 + 1],
                        r->data[(size_t)i * 2], r->data[(size_t)i * 2 + 1]);
            }
            return 0;
        }
    }
    return 1;
}

/* ----------------------------------------------------------------
 * Helper: 1 if relation has no duplicate rows (assumes sorted).
 * ---------------------------------------------------------------- */
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

/* ----------------------------------------------------------------
 * Helper: 1 if row is present in rel (linear scan).
 * ---------------------------------------------------------------- */
static int
test_rel_contains_row(const col_rel_t *r, const int64_t *row)
{
    for (uint32_t i = 0; i < r->nrows; i++) {
        if (test_row_cmp(r->data + (size_t)i * r->ncols, row, r->ncols) == 0)
            return 1;
    }
    return 0;
}

/* ================================================================
 * Test 1: empty old + delta -> all rows new
 *
 * Input:  old_nrows=0, rel=[row(1,2), row(3,4)]
 * Expected:
 *   rel->nrows       == 2  sorted+unique
 *   delta_out->nrows == 2  all rows are new
 * ================================================================ */
static void
test_empty_old_all_new(void)
{
    TEST("empty old (old_nrows=0) + delta -> all rows in delta_out");

    col_rel_t *rel = test_rel_alloc(2);
    col_rel_t *delta_out = test_rel_alloc(2);
    ASSERT(rel && delta_out, "test_rel_alloc failed");

    int64_t r0[] = { 1, 2 };
    int64_t r1[] = { 3, 4 };
    ASSERT(test_rel_append_row(rel, r0) == 0, "append row(1,2)");
    ASSERT(test_rel_append_row(rel, r1) == 0, "append row(3,4)");

    int rc = col_op_consolidate_incremental_delta(rel, 0, delta_out);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(rel->nrows == 2, "rel->nrows == 2");
    ASSERT(test_rel_is_sorted(rel), "rel is sorted");
    ASSERT(test_rel_is_unique(rel), "rel has no duplicates");
    ASSERT(delta_out->nrows == 2, "delta_out->nrows == 2 (all new)");
    ASSERT(test_rel_contains_row(delta_out, r0), "delta_out has row(1,2)");
    ASSERT(test_rel_contains_row(delta_out, r1), "delta_out has row(3,4)");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 2: old + all-duplicate delta -> no change, delta_out empty
 *
 * Input:  old=[row(1,2), row(3,4)] sorted, old_nrows=2
 *         delta (appended) = [row(1,2), row(3,4)]  same rows
 * Expected:
 *   rel->nrows       == 2  unchanged
 *   delta_out->nrows == 0  nothing new
 * ================================================================ */
static void
test_all_duplicate_delta_no_change(void)
{
    TEST("old + all-duplicate delta -> no change, delta_out empty");

    col_rel_t *rel = test_rel_alloc(2);
    col_rel_t *delta_out = test_rel_alloc(2);
    ASSERT(rel && delta_out, "test_rel_alloc failed");

    int64_t r0[] = { 1, 2 };
    int64_t r1[] = { 3, 4 };
    ASSERT(test_rel_append_row(rel, r0) == 0, "append old row(1,2)");
    ASSERT(test_rel_append_row(rel, r1) == 0, "append old row(3,4)");
    ASSERT(test_rel_append_row(rel, r0) == 0, "append dup row(1,2)");
    ASSERT(test_rel_append_row(rel, r1) == 0, "append dup row(3,4)");

    int rc = col_op_consolidate_incremental_delta(rel, 2, delta_out);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(rel->nrows == 2, "rel->nrows == 2 (no new rows)");
    ASSERT(test_rel_is_sorted(rel), "rel is sorted");
    ASSERT(test_rel_is_unique(rel), "rel has no duplicates");
    ASSERT(delta_out->nrows == 0, "delta_out->nrows == 0");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 3: old + unique delta (appended unsorted) -> merged + new rows
 *
 * Input:  old=[row(1,2), row(3,4)] sorted, old_nrows=2
 *         delta appended in reverse order: [row(5,6), row(2,3)]
 *         (intentionally unsorted to exercise the sort step)
 * Expected:
 *   rel       = [(1,2),(2,3),(3,4),(5,6)]  4 rows sorted
 *   delta_out = rows (2,3) and (5,6)       2 new rows
 * ================================================================ */
static void
test_partial_delta_merged_and_new(void)
{
    TEST("old + unique delta (unsorted) -> sorted merged result + new in "
         "delta_out");

    col_rel_t *rel = test_rel_alloc(2);
    col_rel_t *delta_out = test_rel_alloc(2);
    ASSERT(rel && delta_out, "test_rel_alloc failed");

    int64_t r_old0[] = { 1, 2 };
    int64_t r_old1[] = { 3, 4 };
    int64_t r_d0[] = { 2, 3 };
    int64_t r_d1[] = { 5, 6 };

    ASSERT(test_rel_append_row(rel, r_old0) == 0, "append old row(1,2)");
    ASSERT(test_rel_append_row(rel, r_old1) == 0, "append old row(3,4)");
    /* Append delta in reverse order to exercise the sort step */
    ASSERT(test_rel_append_row(rel, r_d1) == 0, "append delta row(5,6) first");
    ASSERT(test_rel_append_row(rel, r_d0) == 0, "append delta row(2,3) second");

    int rc = col_op_consolidate_incremental_delta(rel, 2, delta_out);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(rel->nrows == 4, "rel->nrows == 4 after merge");
    ASSERT(test_rel_is_sorted(rel), "merged rel is sorted");
    ASSERT(test_rel_is_unique(rel), "merged rel has no duplicates");

    /* Verify exact merged order: (1,2),(2,3),(3,4),(5,6) */
    int64_t expected[][2] = { { 1, 2 }, { 2, 3 }, { 3, 4 }, { 5, 6 } };
    for (int i = 0; i < 4; i++)
        ASSERT(test_rel_contains_row(rel, expected[i]),
               "merged rel missing expected row");

    ASSERT(delta_out->nrows == 2, "delta_out->nrows == 2");
    ASSERT(test_rel_contains_row(delta_out, r_d0), "delta_out has row(2,3)");
    ASSERT(test_rel_contains_row(delta_out, r_d1), "delta_out has row(5,6)");
    ASSERT(!test_rel_contains_row(delta_out, r_old0),
           "delta_out must not contain old row(1,2)");
    ASSERT(!test_rel_contains_row(delta_out, r_old1),
           "delta_out must not contain old row(3,4)");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 4: first iteration (old_nrows=0) with intra-delta duplicates
 *
 * Input:  old_nrows=0
 *         rel = [(5,6),(1,2),(3,4),(1,2),(7,8)]  unsorted, one dup
 * Expected:
 *   rel       = [(1,2),(3,4),(5,6),(7,8)]  4 unique sorted rows
 *   delta_out = [(1,2),(3,4),(5,6),(7,8)]  all 4 are new
 * ================================================================ */
static void
test_first_iteration_dedup_all_new(void)
{
    TEST("first iter (old_nrows=0): unsorted+dup delta -> sorted unique, all "
         "in delta_out");

    col_rel_t *rel = test_rel_alloc(2);
    col_rel_t *delta_out = test_rel_alloc(2);
    ASSERT(rel && delta_out, "test_rel_alloc failed");

    int64_t rows[][2] = { { 5, 6 }, { 1, 2 }, { 3, 4 }, { 1, 2 }, { 7, 8 } };
    for (int i = 0; i < 5; i++)
        ASSERT(test_rel_append_row(rel, rows[i]) == 0, "append row");

    int rc = col_op_consolidate_incremental_delta(rel, 0, delta_out);

    ASSERT(rc == 0, "returns 0 on success");
    ASSERT(rel->nrows == 4, "rel->nrows == 4 (dup removed)");
    ASSERT(test_rel_is_sorted(rel), "rel is sorted");
    ASSERT(test_rel_is_unique(rel), "rel has no duplicates");
    ASSERT(delta_out->nrows == 4, "delta_out->nrows == 4");
    ASSERT(test_rel_is_sorted(delta_out), "delta_out is sorted");
    ASSERT(test_rel_is_unique(delta_out), "delta_out has no duplicates");

    int64_t expected[][2] = { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 7, 8 } };
    for (int i = 0; i < 4; i++) {
        ASSERT(test_rel_contains_row(rel, expected[i]),
               "rel missing expected row");
        ASSERT(test_rel_contains_row(delta_out, expected[i]),
               "delta_out missing expected row");
    }

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 5: large dataset correctness oracle
 *
 * old:    1000 unique sorted rows: (0,1),(2,3),...,(1998,1999)
 * delta:  200 duplicates from old (rows 0..199)
 *       + 500 new rows: (2000,2001),(2002,2003),...
 * Total appended: 700 rows after old_nrows=1000.
 *
 * Expected:
 *   rel->nrows       == 1500  sorted+unique
 *   delta_out->nrows == 500   only the 500 new rows
 *
 * Oracle checks:
 *   A. every delta_out row is in merged rel
 *   B. every delta_out row is outside the old range (col0 >= 2000)
 *   C. every new row appears in delta_out
 *   D. no old/duplicate row appears in delta_out
 * ================================================================ */
static void
test_large_dataset_correctness(void)
{
    TEST("large dataset: merged sorted+unique, delta_out == R_new - R_old");

    const uint32_t OLD = 1000;
    const uint32_t NEW = 500;
    const uint32_t DUPS = 200;

    col_rel_t *rel = test_rel_alloc(2);
    col_rel_t *delta_out = test_rel_alloc(2);
    ASSERT(rel && delta_out, "test_rel_alloc failed");

    /* old rows: (0,1),(2,3),...,(1998,1999) */
    for (uint32_t i = 0; i < OLD; i++) {
        int64_t row[2] = { (int64_t)(i * 2), (int64_t)(i * 2 + 1) };
        ASSERT(test_rel_append_row(rel, row) == 0, "append old row");
    }

    /* 200 duplicates: rows 0..199 from old range */
    for (uint32_t i = 0; i < DUPS; i++) {
        int64_t row[2] = { (int64_t)(i * 2), (int64_t)(i * 2 + 1) };
        ASSERT(test_rel_append_row(rel, row) == 0, "append dup delta row");
    }

    /* 500 new rows: (2000,2001),(2002,2003),... */
    for (uint32_t i = 0; i < NEW; i++) {
        int64_t row[2]
            = { (int64_t)(OLD * 2 + i * 2), (int64_t)(OLD * 2 + i * 2 + 1) };
        ASSERT(test_rel_append_row(rel, row) == 0, "append new delta row");
    }

    ASSERT(rel->nrows == OLD + DUPS + NEW, "pre-consolidate count correct");

    int rc = col_op_consolidate_incremental_delta(rel, OLD, delta_out);

    ASSERT(rc == 0, "returns 0 on success");

    char msg[128];
    snprintf(msg, sizeof(msg), "rel->nrows: expected %u, got %u", OLD + NEW,
             rel->nrows);
    ASSERT(rel->nrows == OLD + NEW, msg);
    ASSERT(test_rel_is_sorted(rel), "merged rel is sorted");
    ASSERT(test_rel_is_unique(rel), "merged rel has no duplicates");

    snprintf(msg, sizeof(msg), "delta_out->nrows: expected %u, got %u", NEW,
             delta_out->nrows);
    ASSERT(delta_out->nrows == NEW, msg);

    /* Oracle A+B */
    for (uint32_t i = 0; i < delta_out->nrows; i++) {
        const int64_t *dr = delta_out->data + (size_t)i * 2;
        ASSERT(test_rel_contains_row(rel, dr),
               "oracle A: delta_out row missing from merged rel");
        ASSERT(dr[0] >= (int64_t)(OLD * 2),
               "oracle B: delta_out row col0 in old range");
    }

    /* Oracle C: every new row appears in delta_out */
    for (uint32_t i = 0; i < NEW; i++) {
        int64_t nr[2]
            = { (int64_t)(OLD * 2 + i * 2), (int64_t)(OLD * 2 + i * 2 + 1) };
        ASSERT(test_rel_contains_row(delta_out, nr),
               "oracle C: new row missing from delta_out");
    }

    /* Oracle D: no old/duplicate row in delta_out */
    for (uint32_t i = 0; i < DUPS; i++) {
        int64_t dr[2] = { (int64_t)(i * 2), (int64_t)(i * 2 + 1) };
        ASSERT(!test_rel_contains_row(delta_out, dr),
               "oracle D: old row incorrectly in delta_out");
    }

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */
int
main(void)
{
    printf("=== test_consolidate_incremental_delta (TDD RED PHASE) ===\n\n");
    printf("NOTE: Expected to FAIL at link time until US-004 implements\n");
    printf(
        "      col_op_consolidate_incremental_delta with extern linkage.\n\n");

    test_empty_old_all_new();
    test_all_duplicate_delta_no_change();
    test_partial_delta_merged_and_new();
    test_first_iteration_dedup_all_new();
    test_large_dataset_correctness();

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n", pass_count,
           fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
