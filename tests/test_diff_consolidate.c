/*
 * test_diff_consolidate.c - Differential consolidate operator tests (Issue #263)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Tests col_op_consolidate_diff: trace-based incremental compaction.
 * Part of #244 Timely-Differential Dataflow Migration, Stage 2.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

#include "../wirelog/columnar/internal.h"

/* ========================================================================
 * TEST HARNESS MACROS
 * ======================================================================== */

#define TEST(name)                       \
        do {                                 \
            printf("  [TEST] %-60s ", name); \
            fflush(stdout);                  \
        } while (0)

#define PASS              \
        do {                  \
            printf("PASS\n"); \
            tests_passed++;   \
        } while (0)

#define FAIL(msg)                  \
        do {                           \
            printf("FAIL: %s\n", msg); \
            tests_failed++;            \
        } while (0)

#define ASSERT_TRUE(cond, msg) \
        do {                       \
            if (!(cond)) {         \
                FAIL(msg);         \
                return;            \
            }                      \
        } while (0)

static int tests_passed = 0;
static int tests_failed = 0;

/* ========================================================================
 * Helpers
 * ======================================================================== */

static wl_col_session_t *
make_mock_session(void)
{
    wl_col_session_t *s = calloc(1, sizeof(*s));
    s->frontier_ops = &col_frontier_epoch_ops;
    s->delta_pool = delta_pool_create(256, sizeof(col_rel_t), 1024 * 1024);
    wl_mem_ledger_init(&s->mem_ledger, 0);
    return s;
}

static void
destroy_mock_session(wl_col_session_t *s)
{
    delta_pool_destroy(s->delta_pool);
    free(s);
}

static bool
is_sorted(const col_rel_t *r)
{
    for (uint32_t i = 1; i < r->nrows; i++) {
        for (uint32_t c = 0; c < r->ncols; c++) {
            int64_t prev = col_rel_get(r, i - 1, c);
            int64_t curr = col_rel_get(r, i, c);
            if (prev < curr)
                break;
            if (prev > curr)
                return false;
        }
    }
    return true;
}

static bool
is_unique(const col_rel_t *r)
{
    for (uint32_t i = 1; i < r->nrows; i++) {
        bool same = true;
        for (uint32_t c = 0; c < r->ncols; c++) {
            if (col_rel_get(r, i - 1, c) != col_rel_get(r, i, c)) {
                same = false;
                break;
            }
        }
        if (same)
            return false;
    }
    return true;
}

static bool
contains_row(const col_rel_t *r, const int64_t *row)
{
    for (uint32_t i = 0; i < r->nrows; i++) {
        bool match = true;
        for (uint32_t c = 0; c < r->ncols; c++) {
            if (col_rel_get(r, i, c) != row[c]) {
                match = false;
                break;
            }
        }
        if (match)
            return true;
    }
    return false;
}

/* ========================================================================
 * TEST CASES
 * ======================================================================== */

static void
test_empty_relation(void)
{
    TEST("empty relation => no change");
    wl_col_session_t *sess = make_mock_session();

    col_rel_t *rel = col_rel_new_auto("test", 2);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_consolidate_diff(&stack, sess);
    ASSERT_TRUE(rc == 0, "should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->nrows == 0, "still 0 rows");
    ASSERT_TRUE(result.rel->sorted_nrows == 0, "sorted_nrows == 0");

    if (result.owned)
        col_rel_destroy(result.rel);
    destroy_mock_session(sess);
    PASS;
}

static void
test_single_row(void)
{
    TEST("single row => unchanged, sorted_nrows = 1");
    wl_col_session_t *sess = make_mock_session();

    col_rel_t *rel = col_rel_new_auto("test", 2);
    int64_t row[] = {5, 10};
    col_rel_append_row(rel, row);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_consolidate_diff(&stack, sess);
    ASSERT_TRUE(rc == 0, "should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->nrows == 1, "1 row");
    ASSERT_TRUE(result.rel->sorted_nrows == 1, "sorted_nrows == 1");
    ASSERT_TRUE(col_rel_get(result.rel, 0, 0) == 5, "data preserved");
    ASSERT_TRUE(col_rel_get(result.rel, 0, 1) == 10, "data preserved");

    if (result.owned)
        col_rel_destroy(result.rel);
    destroy_mock_session(sess);
    PASS;
}

static void
test_already_sorted_unique(void)
{
    TEST("already sorted + unique => sorted_nrows set");
    wl_col_session_t *sess = make_mock_session();

    col_rel_t *rel = col_rel_new_auto("test", 2);
    int64_t r1[] = {1, 10};
    int64_t r2[] = {2, 20};
    int64_t r3[] = {3, 30};
    col_rel_append_row(rel, r1);
    col_rel_append_row(rel, r2);
    col_rel_append_row(rel, r3);
    /* Already sorted but sorted_nrows not set */

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_consolidate_diff(&stack, sess);
    ASSERT_TRUE(rc == 0, "should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->nrows == 3, "3 unique rows");
    ASSERT_TRUE(result.rel->sorted_nrows == 3, "sorted_nrows == 3");
    ASSERT_TRUE(is_sorted(result.rel), "should be sorted");
    ASSERT_TRUE(is_unique(result.rel), "should be unique");

    if (result.owned)
        col_rel_destroy(result.rel);
    destroy_mock_session(sess);
    PASS;
}

static void
test_unsorted_full_sort(void)
{
    TEST("unsorted (sorted_nrows=0) => full sort + dedup");
    wl_col_session_t *sess = make_mock_session();

    col_rel_t *rel = col_rel_new_auto("test", 2);
    int64_t r1[] = {3, 30};
    int64_t r2[] = {1, 10};
    int64_t r3[] = {2, 20};
    int64_t r4[] = {1, 10}; /* duplicate */
    col_rel_append_row(rel, r1);
    col_rel_append_row(rel, r2);
    col_rel_append_row(rel, r3);
    col_rel_append_row(rel, r4);
    rel->sorted_nrows = 0; /* no sorted prefix */

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_consolidate_diff(&stack, sess);
    ASSERT_TRUE(rc == 0, "should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->nrows == 3, "3 unique rows after dedup");
    ASSERT_TRUE(result.rel->sorted_nrows == 3, "sorted_nrows set");
    ASSERT_TRUE(is_sorted(result.rel), "should be sorted");
    ASSERT_TRUE(is_unique(result.rel), "should be unique");

    if (result.owned)
        col_rel_destroy(result.rel);
    destroy_mock_session(sess);
    PASS;
}

static void
test_all_duplicates(void)
{
    TEST("all duplicate rows => single row output");
    wl_col_session_t *sess = make_mock_session();

    col_rel_t *rel = col_rel_new_auto("test", 2);
    int64_t row[] = {5, 50};
    for (int i = 0; i < 10; i++)
        col_rel_append_row(rel, row);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_consolidate_diff(&stack, sess);
    ASSERT_TRUE(rc == 0, "should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->nrows == 1, "all dups => 1 row");
    ASSERT_TRUE(result.rel->sorted_nrows == 1, "sorted_nrows == 1");

    if (result.owned)
        col_rel_destroy(result.rel);
    destroy_mock_session(sess);
    PASS;
}

static void
test_partial_duplicates(void)
{
    TEST("mix of unique and duplicate rows");
    wl_col_session_t *sess = make_mock_session();

    col_rel_t *rel = col_rel_new_auto("test", 1);
    int64_t vals[] = {3, 1, 2, 1, 3, 4, 2};
    for (int i = 0; i < 7; i++)
        col_rel_append_row(rel, &vals[i]);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_consolidate_diff(&stack, sess);
    ASSERT_TRUE(rc == 0, "should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->nrows == 4, "unique values: 1,2,3,4");
    ASSERT_TRUE(is_sorted(result.rel), "should be sorted");
    ASSERT_TRUE(is_unique(result.rel), "should be unique");

    if (result.owned)
        col_rel_destroy(result.rel);
    destroy_mock_session(sess);
    PASS;
}

static void
test_incremental_merge_basic(void)
{
    TEST("sorted prefix + unsorted suffix => incremental merge");
    wl_col_session_t *sess = make_mock_session();

    col_rel_t *rel = col_rel_new_auto("test", 2);
    /* Sorted prefix */
    int64_t r1[] = {1, 10};
    int64_t r2[] = {3, 30};
    int64_t r3[] = {5, 50};
    col_rel_append_row(rel, r1);
    col_rel_append_row(rel, r2);
    col_rel_append_row(rel, r3);
    rel->sorted_nrows = 3;

    /* Unsorted suffix (delta) */
    int64_t r4[] = {4, 40};
    int64_t r5[] = {2, 20};
    col_rel_append_row(rel, r4);
    col_rel_append_row(rel, r5);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_consolidate_diff(&stack, sess);
    ASSERT_TRUE(rc == 0, "should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->nrows == 5, "5 unique rows");
    ASSERT_TRUE(result.rel->sorted_nrows == 5, "sorted_nrows updated");
    ASSERT_TRUE(is_sorted(result.rel), "should be sorted");
    ASSERT_TRUE(is_unique(result.rel), "should be unique");

    /* Verify order: 1,2,3,4,5 */
    ASSERT_TRUE(col_rel_get(result.rel, 0, 0) == 1, "first row key=1");
    ASSERT_TRUE(col_rel_get(result.rel, 1, 0) == 2, "second row key=2");
    ASSERT_TRUE(col_rel_get(result.rel, 2, 0) == 3, "third row key=3");
    ASSERT_TRUE(col_rel_get(result.rel, 3, 0) == 4, "fourth row key=4");
    ASSERT_TRUE(col_rel_get(result.rel, 4, 0) == 5, "fifth row key=5");

    if (result.owned)
        col_rel_destroy(result.rel);
    destroy_mock_session(sess);
    PASS;
}

static void
test_incremental_merge_with_duplicates(void)
{
    TEST("prefix + suffix with cross-duplicates");
    wl_col_session_t *sess = make_mock_session();

    col_rel_t *rel = col_rel_new_auto("test", 2);
    /* Sorted prefix: {1,10}, {3,30}, {5,50} */
    int64_t r1[] = {1, 10};
    int64_t r2[] = {3, 30};
    int64_t r3[] = {5, 50};
    col_rel_append_row(rel, r1);
    col_rel_append_row(rel, r2);
    col_rel_append_row(rel, r3);
    rel->sorted_nrows = 3;

    /* Suffix: {5,50} (dup), {1,10} (dup), {4,40} (new) */
    int64_t r4[] = {5, 50};
    int64_t r5[] = {1, 10};
    int64_t r6[] = {4, 40};
    col_rel_append_row(rel, r4);
    col_rel_append_row(rel, r5);
    col_rel_append_row(rel, r6);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_consolidate_diff(&stack, sess);
    ASSERT_TRUE(rc == 0, "should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->nrows == 4, "4 unique: 1,3,4,5");
    ASSERT_TRUE(is_sorted(result.rel), "should be sorted");
    ASSERT_TRUE(is_unique(result.rel), "should be unique");

    int64_t exp4[] = {4, 40};
    ASSERT_TRUE(contains_row(result.rel, exp4), "should contain {4,40}");

    if (result.owned)
        col_rel_destroy(result.rel);
    destroy_mock_session(sess);
    PASS;
}

static void
test_incremental_merge_suffix_only_new(void)
{
    TEST("all suffix rows are new (no overlap with prefix)");
    wl_col_session_t *sess = make_mock_session();

    col_rel_t *rel = col_rel_new_auto("test", 1);
    int64_t v1 = 1, v2 = 2, v3 = 3;
    col_rel_append_row(rel, &v1);
    col_rel_append_row(rel, &v2);
    col_rel_append_row(rel, &v3);
    rel->sorted_nrows = 3;

    int64_t v4 = 4, v5 = 5;
    col_rel_append_row(rel, &v5); /* unsorted */
    col_rel_append_row(rel, &v4);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_consolidate_diff(&stack, sess);
    ASSERT_TRUE(rc == 0, "should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->nrows == 5, "5 unique rows");
    ASSERT_TRUE(is_sorted(result.rel), "should be sorted");

    /* Values should be 1,2,3,4,5 in order */
    for (uint32_t i = 0; i < 5; i++)
        ASSERT_TRUE(col_rel_get(result.rel, i, 0) == (int64_t)(i + 1),
            "sequential");

    if (result.owned)
        col_rel_destroy(result.rel);
    destroy_mock_session(sess);
    PASS;
}

static void
test_large_dataset_correctness(void)
{
    TEST("large dataset: 500 sorted prefix + 200 delta");
    wl_col_session_t *sess = make_mock_session();

    col_rel_t *rel = col_rel_new_auto("test", 2);

    /* Sorted prefix: even numbers 0,2,4,...,998 */
    for (int i = 0; i < 500; i++) {
        int64_t row[] = {i * 2, i * 20};
        col_rel_append_row(rel, row);
    }
    rel->sorted_nrows = 500;

    /* Delta: mix of new odds and existing evens */
    for (int i = 0; i < 100; i++) {
        /* New odd numbers */
        int64_t row[] = {i * 2 + 1, (i * 2 + 1) * 10};
        col_rel_append_row(rel, row);
    }
    for (int i = 0; i < 100; i++) {
        /* Duplicate even numbers */
        int64_t row[] = {i * 2, i * 20};
        col_rel_append_row(rel, row);
    }

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_consolidate_diff(&stack, sess);
    ASSERT_TRUE(rc == 0, "should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    /* 500 evens + 100 odds = 600 unique */
    ASSERT_TRUE(result.rel->nrows == 600, "600 unique rows");
    ASSERT_TRUE(result.rel->sorted_nrows == 600, "sorted_nrows updated");
    ASSERT_TRUE(is_sorted(result.rel), "should be sorted");
    ASSERT_TRUE(is_unique(result.rel), "should be unique");

    if (result.owned)
        col_rel_destroy(result.rel);
    destroy_mock_session(sess);
    PASS;
}

static void
test_merge_buffer_reuse(void)
{
    TEST("merge buffer reuse across calls");
    wl_col_session_t *sess = make_mock_session();

    /* First consolidation */
    col_rel_t *rel = col_rel_new_auto("test", 1);
    int64_t v1 = 1, v2 = 3;
    col_rel_append_row(rel, &v1);
    col_rel_append_row(rel, &v2);
    rel->sorted_nrows = 2;
    int64_t v3 = 2;
    col_rel_append_row(rel, &v3);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);
    int rc = col_op_consolidate_diff(&stack, sess);
    ASSERT_TRUE(rc == 0, "first consolidate ok");

    eval_entry_t r1 = eval_stack_pop(&stack);
    ASSERT_TRUE(r1.rel->nrows == 3, "3 rows after first");
    ASSERT_TRUE(r1.rel->merge_columns != NULL, "merge_columns allocated");

    /* Second consolidation on same relation: add more data */
    int64_t v4 = 4;
    col_rel_append_row(r1.rel, &v4);

    eval_stack_init(&stack);
    eval_stack_push(&stack, r1.rel, true);
    rc = col_op_consolidate_diff(&stack, sess);
    ASSERT_TRUE(rc == 0, "second consolidate ok");

    eval_entry_t r2 = eval_stack_pop(&stack);
    ASSERT_TRUE(r2.rel->nrows == 4, "4 rows after second");

    if (r2.owned)
        col_rel_destroy(r2.rel);
    destroy_mock_session(sess);
    PASS;
}

static void
test_sorted_nrows_set_correctly(void)
{
    TEST("sorted_nrows == nrows after consolidation");
    wl_col_session_t *sess = make_mock_session();

    col_rel_t *rel = col_rel_new_auto("test", 2);
    int64_t rows[][2] = {{5, 50}, {3, 30}, {1, 10}, {4, 40}, {2, 20}};
    for (int i = 0; i < 5; i++)
        col_rel_append_row(rel, rows[i]);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);

    int rc = col_op_consolidate_diff(&stack, sess);
    ASSERT_TRUE(rc == 0, "should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->sorted_nrows == result.rel->nrows,
        "sorted_nrows must equal nrows");

    if (result.owned)
        col_rel_destroy(result.rel);
    destroy_mock_session(sess);
    PASS;
}

/* ========================================================================
 * MAIN
 * ======================================================================== */

int
main(void)
{
    printf("=== Differential Consolidate Tests (Issue #263) ===\n\n");

    test_empty_relation();
    test_single_row();
    test_already_sorted_unique();
    test_unsorted_full_sort();
    test_all_duplicates();
    test_partial_duplicates();
    test_incremental_merge_basic();
    test_incremental_merge_with_duplicates();
    test_incremental_merge_suffix_only_new();
    test_large_dataset_correctness();
    test_merge_buffer_reuse();
    test_sorted_nrows_set_correctly();

    printf("\n=== Results: %d/%d passed ===\n",
        tests_passed, tests_passed + tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
