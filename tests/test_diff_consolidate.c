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
    if (s->deferred_relations)
        (void)wl_columnar_session_retry_deferred(s);
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

/* A refused sort must fail the operation rather than run the dedup loop.
 *
 * The fallback path sorts with col_rel_radix_sort_int64() and then keeps
 * only rows that differ from their predecessor -- a correct compaction
 * only over a sorted relation -- before publishing sorted_nrows
 * unconditionally.  col_rel_radix_sort_int64() became fallible when the
 * sort started taking the canonical-owner lease: a live source reader now
 * refuses it with EBUSY.  Swallowing that would drop every row that is not
 * adjacent-equal and then mark the unsorted remainder as sorted, which is
 * silent data loss.  This pins the propagation instead. */
static void
test_blocked_sort_does_not_dedup_unsorted(void)
{
    TEST("reader-blocked fallback sort fails instead of deduping");
    wl_col_session_t *sess = make_mock_session();
    col_rel_t *rel = col_rel_new_auto("test", 1);
    wl_columnar_source_access_reader_t reader = { 0 };
    /* Sorted this dedups 4 -> 2; left unsorted the trailing pair is the
     * only adjacent match, so it dedups 4 -> 3.  A swallowed failure
     * therefore both loses a row and marks the result sorted, and each is
     * an independent kill of the mutant. */
    const int64_t rows[] = { 4, 1, 4, 4 };
    eval_stack_t stack;
    int rc;

    for (uint32_t i = 0; i < 4; i++)
        col_rel_append_row(rel, &rows[i]);
    /* sorted_nrows == 0 routes the operator to the sort+dedup fallback. */
    ASSERT_TRUE(rel->sorted_nrows == 0, "fixture must reach the fallback");
    if (col_rel_source_reader_acquire(rel, &reader) != 0) {
        col_rel_destroy(rel);
        destroy_mock_session(sess);
        FAIL("source reader acquisition failed");
        return;
    }

    eval_stack_init(&stack);
    eval_stack_push(&stack, rel, true);
    rc = col_op_consolidate_diff(&stack, sess);

    if (eval_stack_drain_to_session(&stack, sess) != 0) {
        (void)col_rel_source_reader_release(&reader);
        destroy_mock_session(sess);
        FAIL("evaluator cleanup must transfer the busy entry");
        return;
    }
    if (stack.top != 0 || sess->deferred_relation_count != 1) {
        (void)col_rel_source_reader_release(&reader);
        destroy_mock_session(sess);
        FAIL("busy entry must survive evaluator return in session registry");
        return;
    }
    if (rc == 0) {
        (void)col_rel_source_reader_release(&reader);
        (void)wl_columnar_session_retry_deferred(sess);
        destroy_mock_session(sess);
        FAIL("blocked sort must not report success");
        return;
    }
    /* rel is still live to inspect because the operator hands it back to
     * the stack rather than destroying it on this path.  The evaluator
     * cleanup above transferred that exact entry to the session registry. */
    /* Nothing may have moved: not the row count, not the order, and not
     * the sorted marking. */
    if (rel->nrows != 4 || rel->sorted_nrows != 0) {
        (void)col_rel_source_reader_release(&reader);
        (void)wl_columnar_session_retry_deferred(sess);
        destroy_mock_session(sess);
        FAIL("blocked sort must leave rows and sorted_nrows untouched");
        return;
    }
    for (uint32_t i = 0; i < 4; i++) {
        if (col_rel_get(rel, i, 0) != rows[i]) {
            (void)col_rel_source_reader_release(&reader);
            (void)wl_columnar_session_retry_deferred(sess);
            destroy_mock_session(sess);
            FAIL("blocked sort must not reorder or drop rows");
            return;
        }
    }
    if (col_rel_source_reader_release(&reader) != 0) {
        (void)wl_columnar_session_retry_deferred(sess);
        destroy_mock_session(sess);
        FAIL("source reader release failed");
        return;
    }
    if (wl_columnar_session_retry_deferred(sess) != 0
        || sess->deferred_relation_count != 0) {
        destroy_mock_session(sess);
        FAIL("session retry must destroy the transferred relation");
        return;
    }
    destroy_mock_session(sess);
    PASS;
}

static void
test_blocked_kway_merge_retains_boundaries(void)
{
    TEST("reader-blocked k-way merge retains boundaries for retry");
    wl_col_session_t *sess = make_mock_session();
    wl_arena_t *arena = wl_arena_create(64 * 1024);
    const int64_t rows[] = { 1, 3, 0, 2 };

    ASSERT_TRUE(sess != NULL && arena != NULL,
        "k-way fixture allocation");
    for (unsigned mode = 0; mode < 3; mode++) {
        col_rel_t *rel = mode == 0
            ? col_rel_new_auto("kway-heap", 1)
            : col_rel_pool_new_auto(sess->delta_pool,
                mode == 2 ? arena : NULL, "kway-owned", 1);
        wl_columnar_source_access_reader_t reader = { 0 };
        eval_stack_t stack;
        uint32_t *boundaries;

        ASSERT_TRUE(rel != NULL, "k-way relation allocation");
        for (uint32_t i = 0; i < 4; i++)
            ASSERT_TRUE(col_rel_append_row(rel, &rows[i]) == 0,
                "k-way rows");
        eval_stack_init(&stack);
        for (unsigned lower = 0; lower < COL_STACK_MAX - 1; lower++) {
            col_rel_t *lower_rel = col_rel_new_auto("kway-lower", 1);
            int64_t value = (int64_t)lower;
            ASSERT_TRUE(lower_rel != NULL
                && col_rel_append_row(lower_rel, &value) == 0
                && eval_stack_push(&stack, lower_rel, true) == 0,
                "k-way lower stack");
            if (lower == 0) {
                stack.items[0].seg_boundaries =
                    malloc(2 * sizeof(uint32_t));
                ASSERT_TRUE(stack.items[0].seg_boundaries != NULL,
                    "k-way lower metadata");
                stack.items[0].seg_count = 1;
                stack.items[0].seg_boundaries[0] = 0;
                stack.items[0].seg_boundaries[1] = 1;
            }
        }
        ASSERT_TRUE(eval_stack_push(&stack, rel, true) == 0,
            "k-way stack push");
        boundaries = malloc(3 * sizeof(*boundaries));
        ASSERT_TRUE(boundaries != NULL, "k-way boundary allocation");
        boundaries[0] = 0;
        boundaries[1] = 2;
        boundaries[2] = 4;
        stack.items[COL_STACK_MAX - 1].seg_boundaries = boundaries;
        stack.items[COL_STACK_MAX - 1].seg_count = 2;
        ASSERT_TRUE(col_rel_source_reader_acquire(rel, &reader) == 0,
            "k-way reader acquisition");

        ASSERT_TRUE(col_op_consolidate_diff(&stack, sess) == EBUSY
            && stack.top == COL_STACK_MAX
            && stack.items[COL_STACK_MAX - 1].rel == rel
            && stack.items[COL_STACK_MAX - 1].seg_boundaries == boundaries
            && stack.items[COL_STACK_MAX - 1].seg_count == 2,
            "k-way refusal retains relation and boundaries");
        ASSERT_TRUE(col_op_consolidate_diff(&stack, sess) == EBUSY
            && stack.top == COL_STACK_MAX
            && stack.items[COL_STACK_MAX - 1].seg_boundaries == boundaries,
            "k-way repeated refusal retains metadata");
        ASSERT_TRUE(col_rel_source_reader_release(&reader) == 0,
            "k-way reader release");
        ASSERT_TRUE(col_op_consolidate_diff(&stack, sess) == 0
            && stack.top == COL_STACK_MAX
            && stack.items[COL_STACK_MAX - 1].rel->nrows == 4
            && stack.items[COL_STACK_MAX - 1].seg_boundaries == NULL
            && stack.items[COL_STACK_MAX - 1].seg_count == 0,
            "k-way retry uses retained boundaries");
        for (uint32_t i = 0; i < 4; i++)
            ASSERT_TRUE(col_rel_get(stack.items[COL_STACK_MAX - 1].rel,
                i, 0) == (int64_t)i, "k-way retry sorted output");
        ASSERT_TRUE(eval_stack_drain(&stack) == 0, "k-way stack drain");
    }
    {
        col_rel_t *borrowed = col_rel_new_auto("kway-borrowed", 1);
        eval_stack_t stack;
        uint32_t *boundaries;
        int64_t values[] = { 7, 8 };

        ASSERT_TRUE(borrowed != NULL
            && col_rel_append_row(borrowed, &values[0]) == 0
            && col_rel_append_row(borrowed, &values[1]) == 0,
            "borrowed k-way relation");
        eval_stack_init(&stack);
        ASSERT_TRUE(eval_stack_push(&stack, borrowed, false) == 0,
            "borrowed k-way stack");
        boundaries = malloc(3 * sizeof(*boundaries));
        ASSERT_TRUE(boundaries != NULL, "borrowed k-way metadata");
        boundaries[0] = 0;
        boundaries[1] = 1;
        boundaries[2] = 2;
        stack.items[0].seg_boundaries = boundaries;
        stack.items[0].seg_count = 2;
        ASSERT_TRUE(col_op_consolidate_diff(&stack, sess) == 0
            && stack.top == 1 && stack.items[0].rel != borrowed
            && stack.items[0].owned == true
            && stack.items[0].seg_boundaries == NULL
            && stack.items[0].seg_count == 0,
            "borrowed copy consumes metadata exactly once");
        ASSERT_TRUE(eval_stack_drain(&stack) == 0, "borrowed k-way drain");
        col_rel_destroy(borrowed);
    }
#ifdef WL_SESSION_TEST_HOOKS
    {
        col_rel_t *borrowed = col_rel_new_auto("kway-copy-fail", 1);
        eval_stack_t stack;
        uint32_t *boundaries;
        int64_t values[] = { 7, 8 };

        ASSERT_TRUE(borrowed != NULL
            && col_rel_append_row(borrowed, &values[0]) == 0
            && col_rel_append_row(borrowed, &values[1]) == 0,
            "copy failure relation");
        eval_stack_init(&stack);
        ASSERT_TRUE(eval_stack_push(&stack, borrowed, false) == 0,
            "copy failure stack");
        boundaries = malloc(3 * sizeof(*boundaries));
        ASSERT_TRUE(boundaries != NULL, "copy failure metadata");
        boundaries[0] = 0;
        boundaries[1] = 1;
        boundaries[2] = 2;
        stack.items[0].seg_boundaries = boundaries;
        stack.items[0].seg_count = 2;
        wl_columnar_diff_test_fail_copy_alloc = true;
        ASSERT_TRUE(col_op_consolidate_diff(&stack, sess) == ENOMEM
            && stack.top == 1 && stack.items[0].rel == borrowed
            && !stack.items[0].owned
            && stack.items[0].seg_boundaries == boundaries
            && stack.items[0].seg_count == 2,
            "copy allocation failure retains borrowed metadata");
        wl_columnar_diff_test_fail_copy_alloc = false;
        ASSERT_TRUE(col_op_consolidate_diff(&stack, sess) == 0
            && stack.top == 1 && stack.items[0].rel != borrowed
            && stack.items[0].owned && stack.items[0].seg_boundaries == NULL,
            "copy allocation retry");
        ASSERT_TRUE(eval_stack_drain(&stack) == 0, "copy failure drain");
        col_rel_destroy(borrowed);
    }
    {
        col_rel_t *borrowed = col_rel_new_auto("append-fail", 1);
        eval_stack_t stack;
        uint32_t *boundaries;
        int64_t values[] = { 9, 10 };

        ASSERT_TRUE(borrowed != NULL
            && col_rel_append_row(borrowed, &values[0]) == 0
            && col_rel_append_row(borrowed, &values[1]) == 0,
            "append failure relation");
        eval_stack_init(&stack);
        ASSERT_TRUE(eval_stack_push(&stack, borrowed, false) == 0,
            "append failure stack");
        boundaries = malloc(3 * sizeof(*boundaries));
        ASSERT_TRUE(boundaries != NULL, "append failure metadata");
        boundaries[0] = 0;
        boundaries[1] = 1;
        boundaries[2] = 2;
        stack.items[0].seg_boundaries = boundaries;
        stack.items[0].seg_count = 2;
        wl_columnar_diff_test_fail_copy_append = true;
        ASSERT_TRUE(col_op_consolidate_diff(&stack, sess) == ENOMEM
            && stack.top == 1 && stack.items[0].rel == borrowed
            && !stack.items[0].owned
            && stack.items[0].seg_boundaries == boundaries,
            "copy append failure retains borrowed metadata");
        wl_columnar_diff_test_fail_copy_append = false;
        ASSERT_TRUE(col_op_consolidate_diff(&stack, sess) == 0
            && stack.top == 1 && stack.items[0].rel != borrowed
            && stack.items[0].owned && stack.items[0].seg_boundaries == NULL,
            "copy append retry");
        ASSERT_TRUE(eval_stack_drain(&stack) == 0, "append failure drain");
        col_rel_destroy(borrowed);
    }
#endif
    {
        col_rel_t *invalid = col_rel_new_auto("kway-invalid", 1);
        eval_stack_t stack;
        uint32_t *boundaries;
        int64_t values[] = { 1, 2, 3, 4 };

        ASSERT_TRUE(invalid != NULL
            && col_rel_append_row(invalid, &values[0]) == 0
            && col_rel_append_row(invalid, &values[1]) == 0
            && col_rel_append_row(invalid, &values[2]) == 0
            && col_rel_append_row(invalid, &values[3]) == 0,
            "invalid k-way relation");
        eval_stack_init(&stack);
        ASSERT_TRUE(eval_stack_push(&stack, invalid, true) == 0,
            "invalid k-way stack");
        boundaries = malloc(3 * sizeof(*boundaries));
        ASSERT_TRUE(boundaries != NULL, "invalid k-way metadata");
        boundaries[0] = 0;
        boundaries[1] = 3;
        boundaries[2] = 2;
        stack.items[0].seg_boundaries = boundaries;
        stack.items[0].seg_count = 2;
        ASSERT_TRUE(col_op_consolidate_diff(&stack, sess) == EINVAL
            && stack.top == 0,
            "invalid k-way cleanup releases metadata");
    }
    wl_arena_free(arena);
    destroy_mock_session(sess);
    PASS;
}

static void
test_eval_entry_dispose_retains_busy_relation(void)
{
    TEST("eval entry disposal retains a relation while its reader is held");
    col_rel_t *rel = col_rel_new_auto("busy-dispose", 1);
    wl_columnar_source_access_reader_t reader = { 0 };
    eval_entry_t entry;
    eval_stack_t stack;

    ASSERT_TRUE(rel != NULL, "busy disposal relation allocation");
    ASSERT_TRUE(col_rel_source_reader_acquire(rel, &reader) == 0,
        "busy disposal reader acquisition");
    entry = (eval_entry_t){
        .rel = rel,
        .owned = true,
        .kind = WL_COLUMNAR_EVAL_ENTRY_RELATION,
    };
    ASSERT_TRUE(eval_entry_dispose(&entry) == EBUSY,
        "busy disposal reports EBUSY");
    ASSERT_TRUE(entry.rel == rel && entry.owned,
        "busy disposal preserves relation ownership");
    ASSERT_TRUE(col_rel_source_reader_release(&reader) == 0,
        "busy disposal reader release");
    ASSERT_TRUE(eval_entry_dispose(&entry) == 0,
        "disposal retries after reader release");
    ASSERT_TRUE(entry.rel == NULL && !entry.owned,
        "successful disposal clears the entry");

    rel = col_rel_new_auto("busy-drain", 1);
    ASSERT_TRUE(rel != NULL, "busy drain relation allocation");
    ASSERT_TRUE(col_rel_source_reader_acquire(rel, &reader) == 0,
        "busy drain reader acquisition");
    eval_stack_init(&stack);
    ASSERT_TRUE(eval_stack_push(&stack, rel, true) == 0,
        "busy drain relation push");
    ASSERT_TRUE(eval_stack_drain(&stack) == EBUSY,
        "busy drain reports EBUSY");
    ASSERT_TRUE(stack.top == 1 && stack.items[0].rel == rel
        && stack.items[0].owned,
        "busy drain retains the exact entry");
    ASSERT_TRUE(col_rel_source_reader_release(&reader) == 0,
        "busy drain reader release");
    ASSERT_TRUE(eval_stack_drain(&stack) == 0 && stack.top == 0,
        "busy drain retries successfully");
    PASS;
}

static void
test_plain_kway_cleanup_retains_metadata(void)
{
    TEST("plain k-way consolidate retains metadata across refusal");
    wl_col_session_t *sess = make_mock_session();
    wl_arena_t *arena = wl_arena_create(64 * 1024);
    ASSERT_TRUE(sess != NULL && arena != NULL, "plain k-way session");

    for (unsigned mode = 0; mode < 3; mode++) {
        col_rel_t *rel = mode == 0
            ? col_rel_new_auto("plain-kway", 1)
            : col_rel_pool_new_auto(sess->delta_pool, NULL,
                "plain-kway", 1);
        eval_stack_t stack;
        wl_columnar_source_access_reader_t reader = { 0 };
        int64_t rows[] = { 3, 1, 2, 1 };
        ASSERT_TRUE(rel != NULL, "plain k-way relation");
        if (mode == 2) {
            /* Exercise the arena-backed allocator as well. */
            col_rel_destroy(rel);
            rel = col_rel_pool_new_auto(sess->delta_pool, arena,
                    "plain-kway-arena", 1);
            ASSERT_TRUE(rel != NULL, "plain k-way arena relation");
            /* The arena is intentionally kept alive by the session fixture. */
        }
        for (unsigned i = 0; i < 4; i++)
            ASSERT_TRUE(col_rel_append_row(rel, &rows[i]) == 0,
                "plain k-way rows");
        eval_stack_init(&stack);
        for (unsigned i = 0; i < COL_STACK_MAX - 1; i++) {
            col_rel_t *lower = col_rel_new_auto("plain-kway-lower", 1);
            int64_t value = (int64_t)i;
            ASSERT_TRUE(lower != NULL
                && col_rel_append_row(lower, &value) == 0
                && eval_stack_push(&stack, lower, true) == 0,
                "plain k-way lower stack");
        }
        ASSERT_TRUE(eval_stack_push(&stack, rel, true) == 0,
            "plain k-way push");
        uint32_t *bounds = malloc(3 * sizeof(*bounds));
        ASSERT_TRUE(bounds != NULL, "plain k-way bounds");
        bounds[0] = 0; bounds[1] = 2; bounds[2] = 4;
        stack.items[COL_STACK_MAX - 1].seg_boundaries = bounds;
        stack.items[COL_STACK_MAX - 1].seg_count = 2;
        ASSERT_TRUE(col_rel_source_reader_acquire(rel, &reader) == 0,
            "plain k-way reader");
        ASSERT_TRUE(col_op_consolidate(&stack, sess) == EBUSY
            && stack.top == COL_STACK_MAX
            && stack.items[COL_STACK_MAX - 1].seg_boundaries == bounds,
            "plain k-way refusal retains bounds");
        ASSERT_TRUE(col_op_consolidate(&stack, sess) == EBUSY
            && stack.items[COL_STACK_MAX - 1].seg_boundaries == bounds,
            "plain k-way repeated refusal retains bounds");
        ASSERT_TRUE(col_rel_source_reader_release(&reader) == 0,
            "plain k-way reader release");
        ASSERT_TRUE(col_op_consolidate(&stack, sess) == 0
            && stack.top == COL_STACK_MAX
            && stack.items[COL_STACK_MAX - 1].seg_boundaries == NULL
            && stack.items[COL_STACK_MAX - 1].rel->nrows == 3,
            "plain k-way retry consumes bounds");
        ASSERT_TRUE(col_rel_get(stack.items[COL_STACK_MAX - 1].rel, 0, 0) == 1
            && col_rel_get(stack.items[COL_STACK_MAX - 1].rel, 1, 0) == 2
            && col_rel_get(stack.items[COL_STACK_MAX - 1].rel, 2, 0) == 3,
            "plain k-way retry sorted output");
        ASSERT_TRUE(eval_stack_drain(&stack) == 0, "plain k-way drain");
    }

#ifdef WL_SESSION_TEST_HOOKS
    {
        col_rel_t *borrowed = col_rel_new_auto("plain-copy-fail", 1);
        eval_stack_t stack;
        uint32_t *bounds = malloc(3 * sizeof(*bounds));
        int64_t values[] = { 7, 8 };
        ASSERT_TRUE(borrowed != NULL && bounds != NULL
            && col_rel_append_row(borrowed, &values[0]) == 0
            && col_rel_append_row(borrowed, &values[1]) == 0,
            "plain borrowed copy fixture");
        eval_stack_init(&stack);
        ASSERT_TRUE(eval_stack_push(&stack, borrowed, false) == 0,
            "plain borrowed push");
        bounds[0] = 0; bounds[1] = 1; bounds[2] = 2;
        stack.items[0].seg_boundaries = bounds;
        stack.items[0].seg_count = 2;
        wl_columnar_merge_test_fail_copy_alloc = true;
        ASSERT_TRUE(col_op_consolidate(&stack, sess) == ENOMEM
            && stack.top == 1 && stack.items[0].rel == borrowed
            && !stack.items[0].owned
            && stack.items[0].seg_boundaries == bounds,
            "plain copy allocation failure retains metadata");
        wl_columnar_merge_test_fail_copy_alloc = false;
        ASSERT_TRUE(col_op_consolidate(&stack, sess) == 0
            && stack.top == 1 && stack.items[0].rel != borrowed
            && stack.items[0].seg_boundaries == NULL,
            "plain copy allocation retry");
        ASSERT_TRUE(eval_stack_drain(&stack) == 0, "plain copy drain");
        col_rel_destroy(borrowed);
    }
#endif
    wl_arena_free(arena);
    destroy_mock_session(sess);
    PASS;
}

/* Reader admission is a short-lived, operator-scoped gate, not the place
 * that decides deferred ownership.  Pool- and arena-backed relations are
 * read by ordinary operators -- the join builds its owned right-hand filter
 * in sess->delta_pool -- so admission must accept them.  What must refuse
 * them is the session's deferred-destruction registry, because only that
 * makes a pointer outlive the allocator it came from (Issue #1593). */
static void
test_deferred_registry_refuses_unsafe_storage(void)
{
    TEST("pool and arena relations read fine but never defer");
    wl_col_session_t *sess = make_mock_session();
    delta_pool_t *pool = delta_pool_create(4, sizeof(col_rel_t), 4096);
    wl_arena_t *arena = wl_arena_create(64 * 1024);
    wl_columnar_source_access_reader_t reader = { 0 };
    col_rel_t *pool_rel;
    col_rel_t *arena_rel;

    ASSERT_TRUE(sess != NULL && pool != NULL && arena != NULL,
        "unsafe-storage fixture allocation");

    pool_rel = col_rel_pool_new_auto(pool, NULL, "pool-reader", 1);
    ASSERT_TRUE(pool_rel != NULL && pool_rel->pool_owned,
        "pool relation allocation");
    ASSERT_TRUE(col_rel_source_reader_acquire(pool_rel, &reader) == 0,
        "pool-backed reader admission must be accepted");
    ASSERT_TRUE(col_rel_source_reader_release(&reader) == 0,
        "pool-backed reader release");
    ASSERT_TRUE(!wl_columnar_deferred_relation_eligible(pool_rel),
        "pool relation is not deferral-eligible");
    ASSERT_TRUE(wl_columnar_session_defer_relation(sess, pool_rel) == EINVAL,
        "pool relation is refused by the deferred registry");

    arena_rel = col_rel_pool_new_auto(pool, arena, "arena-reader", 1);
    ASSERT_TRUE(arena_rel != NULL && arena_rel->pool_owned
        && arena_rel->arena_owned, "arena-backed relation allocation");
    ASSERT_TRUE(col_rel_source_reader_acquire(arena_rel, &reader) == 0,
        "arena-backed reader admission must be accepted");
    ASSERT_TRUE(col_rel_source_reader_release(&reader) == 0,
        "arena-backed reader release");
    ASSERT_TRUE(!wl_columnar_deferred_relation_eligible(arena_rel),
        "arena relation is not deferral-eligible");
    ASSERT_TRUE(wl_columnar_session_defer_relation(sess, arena_rel) == EINVAL,
        "arena relation is refused by the deferred registry");

    /* A refused admission must leave no intrusive link behind. */
    ASSERT_TRUE(sess->deferred_relations == NULL
        && sess->deferred_relation_count == 0,
        "refused admissions leave the registry empty");
    ASSERT_TRUE(pool_rel->deferred_relation_session == NULL
        && arena_rel->deferred_relation_session == NULL,
        "refused admissions record no owning session");

    ASSERT_TRUE(col_rel_destroy_checked(pool_rel) == 0,
        "pool-backed relation cleanup");
    ASSERT_TRUE(col_rel_destroy_checked(arena_rel) == 0,
        "arena-backed relation cleanup");
    delta_pool_destroy(pool);
    wl_arena_free(arena);
    destroy_mock_session(sess);
    PASS;
}

/* A busy relation that cannot be deferred is a cleanup refusal, not a
 * reason to kill the host process.  The entry stays on the stack and owned
 * by it, so the caller can release the reader and retry. */
static void
test_drain_to_session_reports_unsafe_refusal(void)
{
    TEST("drain reports an undeferrable busy relation without aborting");
    wl_col_session_t *sess = make_mock_session();
    wl_columnar_source_access_reader_t reader = { 0 };
    eval_stack_t stack;
    col_rel_t *pool_rel;

    ASSERT_TRUE(sess != NULL, "drain refusal fixture allocation");
    pool_rel = col_rel_pool_new_auto(sess->delta_pool, NULL, "drain-pool", 1);
    ASSERT_TRUE(pool_rel != NULL && pool_rel->pool_owned,
        "drain pool relation allocation");
    ASSERT_TRUE(col_rel_source_reader_acquire(pool_rel, &reader) == 0,
        "drain reader acquisition");

    eval_stack_init(&stack);
    ASSERT_TRUE(eval_stack_push(&stack, pool_rel, true) == 0,
        "drain relation push");
    ASSERT_TRUE(eval_stack_drain_to_session(&stack, sess) == EBUSY,
        "undeferrable busy relation reports EBUSY");
    ASSERT_TRUE(stack.top == 1 && stack.items[0].rel == pool_rel
        && stack.items[0].owned,
        "refused drain retains the exact entry");
    ASSERT_TRUE(sess->deferred_relations == NULL
        && sess->deferred_relation_count == 0,
        "refused drain admits nothing to the registry");

    ASSERT_TRUE(col_rel_source_reader_release(&reader) == 0,
        "drain reader release");
    ASSERT_TRUE(eval_stack_drain_to_session(&stack, sess) == 0
        && stack.top == 0, "drain retries successfully");
    destroy_mock_session(sess);
    PASS;
}

static void
test_intrusive_deferred_registry(void)
{
    TEST("deferred registry is intrusive, idempotent, and single-owner");
    wl_col_session_t *sess = make_mock_session();
    wl_col_session_t *other = make_mock_session();
    col_rel_t *first = col_rel_new_auto("deferred-first", 1);
    col_rel_t *second = col_rel_new_auto("deferred-second", 1);
    wl_columnar_source_access_reader_t first_reader = { 0 };
    wl_columnar_source_access_reader_t second_reader = { 0 };

    ASSERT_TRUE(sess != NULL && other != NULL && first != NULL
        && second != NULL, "intrusive registry fixture allocation");
    ASSERT_TRUE(col_rel_source_reader_acquire(first, &first_reader) == 0,
        "first deferred reader acquisition");
    ASSERT_TRUE(col_rel_source_reader_acquire(second, &second_reader) == 0,
        "second deferred reader acquisition");
    ASSERT_TRUE(wl_columnar_session_defer_relation(sess, first) == 0,
        "first relation deferred without an allocation");
    ASSERT_TRUE(first->deferred_relation_session == sess
        && sess->deferred_relations == first,
        "first relation records its intrusive owner");
    ASSERT_TRUE(wl_columnar_session_defer_relation(sess, first) == 0
        && sess->deferred_relation_count == 1,
        "duplicate deferral is idempotent");
    ASSERT_TRUE(wl_columnar_session_defer_relation(other, first) == EBUSY,
        "relation cannot be deferred into two sessions");
    ASSERT_TRUE(wl_columnar_session_defer_relation(sess, second) == 0
        && sess->deferred_relation_count == 2,
        "second relation joins the intrusive registry");
    ASSERT_TRUE(sess->deferred_relations == first
        && first->deferred_relation_next == second,
        "multiple relations form one intrusive chain");

    {
        col_rel_t *transferred = col_rel_new_auto("deferred-transfer", 1);
        wl_columnar_source_access_reader_t transferred_reader = { 0 };
        ASSERT_TRUE(transferred != NULL
            && col_rel_source_reader_acquire(transferred,
            &transferred_reader) == 0,
            "transfer relation fixture");
        ASSERT_TRUE(wl_columnar_session_defer_relation(sess, transferred)
            == 0, "transfer relation deferred by source session");
        ASSERT_TRUE(wl_columnar_session_transfer_deferred(sess, other) == 0
            && transferred->deferred_relation_session == other
            && sess->deferred_relation_count == 0
            && other->deferred_relation_count == 3,
            "deferred relations transfer ownership between sessions");
        ASSERT_TRUE(col_rel_source_reader_release(&transferred_reader) == 0
            && wl_columnar_session_retry_deferred(other) == EBUSY
            && other->deferred_relation_count == 2,
            "transferred relation is retried by destination session");
    }

    ASSERT_TRUE(col_rel_source_reader_release(&first_reader) == 0,
        "first deferred reader release");
    ASSERT_TRUE(wl_columnar_session_retry_deferred(other) == EBUSY
        && other->deferred_relation_count == 1,
        "retry removes only the relation whose reader is released");
    ASSERT_TRUE(col_rel_source_reader_release(&second_reader) == 0,
        "second deferred reader release");
    ASSERT_TRUE(wl_columnar_session_retry_deferred(other) == 0
        && other->deferred_relation_count == 0
        && other->deferred_relations == NULL,
        "retry drains the remaining relation");
    {
        delta_pool_t *pool = delta_pool_create(4, sizeof(col_rel_t), 4096);
        wl_arena_t *arena = wl_arena_create(64 * 1024);
        col_rel_t *pool_rel;
        col_rel_t *arena_rel;

        ASSERT_TRUE(pool != NULL && arena != NULL,
            "unsafe deferred registry fixture allocation");
        pool_rel = col_rel_pool_new_auto(pool, NULL, "pool-deferred", 1);
        arena_rel = col_rel_pool_new_auto(pool, arena, "arena-deferred", 1);
        ASSERT_TRUE(pool_rel != NULL && arena_rel != NULL,
            "unsafe deferred relation allocation");
        ASSERT_TRUE(wl_columnar_session_defer_relation(sess, pool_rel)
            == EINVAL, "pool relation is rejected by deferred registry");
        ASSERT_TRUE(wl_columnar_session_defer_relation(sess, arena_rel)
            == EINVAL, "arena relation is rejected by deferred registry");
        ASSERT_TRUE(col_rel_destroy_checked(pool_rel) == 0
            && col_rel_destroy_checked(arena_rel) == 0,
            "unsafe deferred relations clean up normally");
        delta_pool_destroy(pool);
        wl_arena_free(arena);
    }
    destroy_mock_session(other);
    destroy_mock_session(sess);
    PASS;
}

int
main(void)
{
    printf("=== Differential Consolidate Tests (Issue #263) ===\n\n");

    test_empty_relation();
    test_blocked_sort_does_not_dedup_unsorted();
    test_blocked_kway_merge_retains_boundaries();
    test_plain_kway_cleanup_retains_metadata();
    test_eval_entry_dispose_retains_busy_relation();
    test_deferred_registry_refuses_unsafe_storage();
    test_drain_to_session_reports_unsafe_refusal();
    test_intrusive_deferred_registry();
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
