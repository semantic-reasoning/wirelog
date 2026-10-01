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

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/thread.h"

/*
 * ArrowSchema stub: replicates the layout of struct ArrowSchema from
 * nanoarrow.h so that col_rel_t has the correct field offsets without
 * pulling in the nanoarrow dependency.  The function under test does not
 * access schema or schema_ok; fields before those (name, ncols, data,
 * nrows, capacity, col_names) are identical in both layouts.
 */
#include "../wirelog/columnar/internal.h"

/*
 * Forward declaration of the function under test.
 * RED phase: does not exist -> link error (expected).
 * GREEN phase (US-004): implemented in columnar_nanoarrow.c with extern
 * linkage; backend_src added to meson entry.
 */
int
col_op_consolidate_incremental_delta(col_rel_t *rel, uint32_t old_nrows,
    col_rel_t *delta_out,
    int *out_fast_path);

#ifdef WL_TEST_CONSOLIDATE_HOOK
typedef struct consolidation_pause_state {
    wl_mutex_t mutex;
    wl_cond_t condition;
    col_rel_t *expected_relation;
    col_rel_t *expected_owner;
    int64_t *old_columns;
    uint32_t expected_nrows;
    int64_t expected_values[4];
    wl_columnar_consolidation_test_stage_t expected_stage;
    bool hook_reached;
    bool resume;
    bool operation_done;
    bool hook_state_ok;
    int operation_rc;
    int reader_rc;
} consolidation_pause_state_t;

static consolidation_pause_state_t *consolidation_pause_state;

wl_columnar_consolidation_transition_hook_t
    wl_columnar_consolidation_transition_hook;

static void
consolidation_pause_hook(col_rel_t *relation,
    wl_columnar_consolidation_test_stage_t stage)
{
    consolidation_pause_state_t *state = consolidation_pause_state;
    if (!state || wl_mutex_lock(&state->mutex) != 0)
        return;
    state->hook_state_ok = stage == state->expected_stage
        && relation == state->expected_relation
        && relation->storage_owner == state->expected_owner
        && relation->col_shared == NULL
        && relation->columns[0] != state->old_columns
        && relation->nrows == state->expected_nrows
        && state->expected_owner->storage_alias_borrows > 0;
    for (uint32_t i = 0; state->hook_state_ok && i < relation->nrows; i++)
        state->hook_state_ok = relation->columns[0][i]
            == state->expected_values[i];
    state->hook_reached = true;
    (void)wl_cond_broadcast(&state->condition);
    while (!state->resume)
        if (wl_cond_wait(&state->condition, &state->mutex) != 0)
            break;
    (void)wl_mutex_unlock(&state->mutex);
}

typedef struct consolidation_operation {
    consolidation_pause_state_t *state;
    col_rel_t *rel;
    uint32_t old_nrows;
    col_rel_t *delta_out;
} consolidation_operation_t;

static void *
consolidation_operation_thread(void *opaque)
{
    consolidation_operation_t *operation = opaque;
    consolidation_pause_state_t *state = operation->state;
    state->operation_rc = col_op_consolidate_incremental_delta(
        operation->rel, operation->old_nrows, operation->delta_out, NULL);
    if (wl_mutex_lock(&state->mutex) == 0) {
        state->operation_done = true;
        (void)wl_cond_broadcast(&state->condition);
        (void)wl_mutex_unlock(&state->mutex);
    }
    return NULL;
}

static bool
run_consolidation_during_pause(col_rel_t *rel, uint32_t old_nrows,
    col_rel_t *delta_out, col_rel_t *expected_relation,
    col_rel_t *expected_owner, wl_columnar_consolidation_test_stage_t stage,
    const int64_t *expected_values, uint32_t expected_nrows,
    int64_t *old_columns, int *out_reader_rc, int *out_operation_rc,
    bool *out_hook_state_ok)
{
    consolidation_pause_state_t state = { 0 };
    consolidation_operation_t operation = { &state, rel, old_nrows,
                                            delta_out };
    wl_thread_t operation_thread;
    bool mutex_ready = false;
    bool cond_ready = false;
    bool thread_ready = false;
    bool reached;

    if (wl_mutex_init(&state.mutex) != 0)
        goto cleanup;
    mutex_ready = true;
    if (wl_cond_init(&state.condition) != 0)
        goto cleanup;
    cond_ready = true;
    state.expected_relation = expected_relation;
    state.expected_owner = expected_owner;
    state.old_columns = old_columns;
    state.expected_nrows = expected_nrows;
    state.expected_stage = stage;
    for (uint32_t i = 0; i < expected_nrows; i++)
        state.expected_values[i] = expected_values[i];
    consolidation_pause_state = &state;
    wl_columnar_consolidation_transition_hook = consolidation_pause_hook;
    if (wl_thread_create(&operation_thread, consolidation_operation_thread,
        &operation) != 0)
        goto cleanup;
    thread_ready = true;

    if (wl_mutex_lock(&state.mutex) != 0)
        goto cleanup;
    while (!state.hook_reached && !state.operation_done)
        if (wl_cond_wait(&state.condition, &state.mutex) != 0)
            break;
    reached = state.hook_reached;
    (void)wl_mutex_unlock(&state.mutex);

    if (reached) {
        wl_columnar_source_access_reader_t reader = { 0 };
        state.reader_rc = col_rel_source_reader_acquire(expected_relation,
                &reader);
        if (state.reader_rc == 0)
            (void)col_rel_source_reader_release(&reader);
        if (wl_mutex_lock(&state.mutex) == 0) {
            state.resume = true;
            (void)wl_cond_broadcast(&state.condition);
            (void)wl_mutex_unlock(&state.mutex);
        }
    }
    if (thread_ready) {
        (void)wl_thread_join(&operation_thread);
        thread_ready = false;
    }

    if (out_reader_rc)
        *out_reader_rc = reached ? state.reader_rc : EINVAL;
    if (out_operation_rc)
        *out_operation_rc = state.operation_rc;
    if (out_hook_state_ok)
        *out_hook_state_ok = reached && state.hook_state_ok;

cleanup:
    if (thread_ready) {
        if (wl_mutex_lock(&state.mutex) == 0) {
            state.resume = true;
            (void)wl_cond_broadcast(&state.condition);
            (void)wl_mutex_unlock(&state.mutex);
        }
        (void)wl_thread_join(&operation_thread);
    }
    wl_columnar_consolidation_transition_hook = NULL;
    consolidation_pause_state = NULL;
    if (cond_ready)
        wl_cond_destroy(&state.condition);
    if (mutex_ready)
        wl_mutex_destroy(&state.mutex);
    return out_reader_rc && out_operation_rc && out_hook_state_ok;
}
#endif

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
 * Helpers: construct, append to, and destroy relations through production
 * lifecycle APIs so fixtures carry normal ownership/generation metadata.
 * ---------------------------------------------------------------- */
static col_rel_t *
test_rel_alloc(uint32_t ncols)
{
    return col_rel_new_auto("test_rel", ncols);
}

static int test_row_match(const col_rel_t *r, uint32_t row,
    const int64_t *target)
{
    for (uint32_t c = 0; c < r->ncols;
        c++) if (col_rel_get(r, row, c) != target[c]) return 0; return 1;
}

/* ----------------------------------------------------------------
 * Helper: free col_rel_t (handles data replaced by the function).
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
    return col_rel_append_row(r, row);
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

static int test_row_cmp_idx(const col_rel_t *r, uint32_t a, uint32_t b)
{
    int64_t ba[64], bb[64]; col_rel_row_copy_out(r, a, ba);
    col_rel_row_copy_out(r, b, bb); return test_row_cmp(ba, bb, r->ncols);
}

static int test_row_cmp_to(const col_rel_t *r, uint32_t row,
    const int64_t *target)
{
    int64_t buf[64]; col_rel_row_copy_out(r, row, buf);
    return test_row_cmp(buf, target, r->ncols);
}

static int
test_rel_is_sorted(const col_rel_t *r)
{
    if (r->nrows <= 1)
        return 1;
    for (uint32_t i = 1; i < r->nrows; i++) {
        int cmp;
        { int64_t _a[32], _b[32]; col_rel_row_copy_out(r, i-1, _a);
          col_rel_row_copy_out(r, i, _b); cmp = test_row_cmp(_a, _b, r->ncols);
        };
        if (cmp >= 0) {
            /* Debug: report first sort failure */
            if (r->ncols == 2) {
                fprintf(stderr,
                    "SORT FAIL @ row %u: [%lld,%lld] >= [%lld,%lld]\n", i,
                    r->columns[((size_t)(i - 1) * 2) %
                    r->ncols][((size_t)(i - 1) * 2) / r->ncols],
                    r->columns[((size_t)(i - 1) * 2 + 1) %
                    r->ncols][((size_t)(i - 1) * 2 + 1) / r->ncols],
                    r->columns[((size_t)i * 2) % r->ncols][((size_t)i * 2) /
                    r->ncols],
                    r->columns[((size_t)i * 2 + 1) %
                    r->ncols][((size_t)i * 2 + 1) / r->ncols]);
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
        if (test_row_cmp_idx(r, i-1, i)
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
        if ((test_row_match(r, i, row)))
            return 1;
    }
    return 0;
}

static void
test_initialized_zero_column_relation(void)
{
    TEST("initialized zero-column relation supports the empty tuple");

    col_rel_t *empty = test_rel_alloc(0);
    ASSERT(empty, "test_rel_alloc failed");
    ASSERT(wl_columnar_relation_float_values_valid(empty),
        "empty relation should be valid without column storage");

    ASSERT(empty->schema_ok, "zero-column relation has initialized schema");
    ASSERT(empty->relation_identity != 0,
        "zero-column relation has initialized identity");
    int64_t empty_tuple = 0;
    ASSERT(test_rel_append_row(empty, &empty_tuple) == 0,
        "append zero-column tuple");
    ASSERT(empty->nrows == 1, "zero-column relation has one empty tuple");
    ASSERT(wl_columnar_relation_float_values_valid(empty),
        "zero-column tuple remains valid without column storage");
    ASSERT(test_rel_append_row(empty, &empty_tuple) == 0,
        "append duplicate zero-column tuple");
    col_rel_t *delta_out = test_rel_alloc(0);
    ASSERT(delta_out, "test_rel_alloc delta_out failed");
    ASSERT(col_op_consolidate_incremental_delta(empty, 1, delta_out, NULL) == 0,
        "consolidate duplicate zero-column tuple");
    ASSERT(empty->nrows == 1, "duplicate empty tuple is consolidated");
    ASSERT(delta_out->nrows == 0, "duplicate empty tuple is not emitted");
    test_rel_free(delta_out);

    test_rel_free(empty);
    PASS();
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

    int rc = col_op_consolidate_incremental_delta(rel, 0, delta_out, NULL);

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

    int rc = col_op_consolidate_incremental_delta(rel, 2, delta_out, NULL);

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

    int rc = col_op_consolidate_incremental_delta(rel, 2, delta_out, NULL);

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

    int rc = col_op_consolidate_incremental_delta(rel, 0, delta_out, NULL);

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

static void
test_fallback_retains_merge_capacity_after_heavy_dedup(void)
{
    TEST(
        "fallback keeps all column allocations and truthful capacity after dedup");

    col_rel_t *rel = test_rel_alloc(3);
    col_rel_t *delta_out = test_rel_alloc(3);
    ASSERT(rel && delta_out, "test_rel_alloc failed");

    const int64_t old_row[3] = { 1000, 10, 20 };
    const int64_t duplicate[3] = { 5, 30, 40 };
    for (uint32_t i = 0; i < 256; i++) {
        const int64_t *row = i == 0 ? old_row : duplicate;
        ASSERT(test_rel_append_row(rel, row) == 0,
            "append old and duplicate rows");
    }
    ASSERT(col_rel_reserve_merge_grid(rel, 256) == 0,
        "reserve fallback merge grid");
    uint32_t retained_capacity = rel->merge_buf_cap;
    int64_t *retained_columns[3] = {
        rel->merge_columns[0], rel->merge_columns[1], rel->merge_columns[2]
    };
    ASSERT(retained_capacity >= 256, "merge grid has spare capacity");

    int fast_path = -1;
    ASSERT(col_op_consolidate_incremental_delta(rel, 1, delta_out,
        &fast_path) == 0, "fallback consolidation succeeds");
    ASSERT(fast_path == 0, "heavy duplicate delta takes fallback path");
    ASSERT(rel->nrows == 2 && rel->sorted_nrows == 2,
        "fallback leaves two sorted rows");
    ASSERT(rel->run_count == 1 && rel->run_ends[0] == 2,
        "fallback records one complete run");
    ASSERT(test_row_match(rel, 0, duplicate)
        && test_row_match(rel, 1, old_row),
        "fallback keeps lexicographic row order");
    ASSERT(delta_out->nrows == 1 && test_row_match(delta_out, 0, duplicate),
        "delta output contains only the new row");

    /* The removed shrink predicate was true here, with a tight bound of
     * COL_REL_INIT_CAP. Every primary column must still own the merge grid. */
    uint32_t old_tight = rel->nrows + rel->nrows / 4;
    if (old_tight < COL_REL_INIT_CAP)
        old_tight = COL_REL_INIT_CAP;
    ASSERT(retained_capacity > old_tight,
        "fixture would have entered the removed shrink block");
    ASSERT(rel->capacity == retained_capacity,
        "primary capacity matches retained allocations");
    for (uint32_t c = 0; c < 3; c++) {
        ASSERT(rel->columns[c] == retained_columns[c],
            "primary column retains its merge allocation");
        rel->columns[c][old_tight + 1] = (int64_t)(100 + c);
        ASSERT(rel->columns[c][old_tight + 1] == (int64_t)(100 + c),
            "column remains writable above the former tight bound");
    }

    const int64_t later_row[3] = { 2000, 50, 60 };
    ASSERT(test_rel_append_row(rel, later_row) == 0,
        "relation can append using retained capacity");
    ASSERT(rel->nrows == 3 && test_row_match(rel, 2, later_row),
        "appended row can be read back");

    test_rel_free(delta_out);
    test_rel_free(rel);
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

    int rc = col_op_consolidate_incremental_delta(rel, OLD, delta_out, NULL);

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
        int64_t _dr[2]; col_rel_row_copy_out(delta_out, i, _dr);
        const int64_t *dr = _dr;
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

/* ================================================================
 * Test 6: fast-path hit -- all delta rows > all old rows (Issue #239)
 *
 * Input:  old=[10,20,30] sorted, old_nrows=3
 *         delta=[40,50,60] appended (all > last old row 30)
 * Expected:
 *   rel       = [10,20,30,40,50,60]  direct append, no merge walk
 *   delta_out = [40,50,60]           all three are new
 * ================================================================ */
static void
test_fast_path_hit_append(void)
{
    TEST("fast-path hit: all delta > all old -> direct append, no merge walk");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "test_rel_alloc failed");

    int64_t old0[] = { 10 }, old1[] = { 20 }, old2[] = { 30 };
    int64_t d0[] = { 40 }, d1[] = { 50 }, d2[] = { 60 };
    ASSERT(test_rel_append_row(rel, old0) == 0, "append old 10");
    ASSERT(test_rel_append_row(rel, old1) == 0, "append old 20");
    ASSERT(test_rel_append_row(rel, old2) == 0, "append old 30");
    ASSERT(test_rel_append_row(rel, d0) == 0, "append delta 40");
    ASSERT(test_rel_append_row(rel, d1) == 0, "append delta 50");
    ASSERT(test_rel_append_row(rel, d2) == 0, "append delta 60");

    int rc = col_op_consolidate_incremental_delta(rel, 3, delta_out, NULL);

    ASSERT(rc == 0, "returns 0");
    ASSERT(rel->nrows == 6, "rel->nrows == 6");
    ASSERT(test_rel_is_sorted(rel), "merged rel is sorted");
    ASSERT(test_rel_is_unique(rel), "merged rel has no duplicates");
    ASSERT(delta_out->nrows == 3, "delta_out->nrows == 3 (all new)");
    ASSERT(test_rel_contains_row(delta_out, d0), "delta_out has 40");
    ASSERT(test_rel_contains_row(delta_out, d1), "delta_out has 50");
    ASSERT(test_rel_contains_row(delta_out, d2), "delta_out has 60");
    ASSERT(!test_rel_contains_row(delta_out, old0),
        "delta_out must not have 10");

    /* Verify exact order */
    int64_t expected[] = { 10, 20, 30, 40, 50, 60 };
    for (int i = 0; i < 6; i++)
        ASSERT(rel->columns[(i) % rel->ncols][(i) / rel->ncols] == expected[i],
            "merged row order mismatch");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 7: fast-path miss -- delta interleaves with old (Issue #239)
 *
 * Input:  old=[10,30,50] sorted, old_nrows=3
 *         delta=[20,40] (interleaved with old)
 * Expected:
 *   rel       = [10,20,30,40,50]  O(N) merge walk used
 *   delta_out = [20,40]           both are new
 * ================================================================ */
static void
test_fast_path_miss_interleaved(void)
{
    TEST("fast-path miss: delta interleaves old -> O(N) merge walk used");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "test_rel_alloc failed");

    int64_t old0[] = { 10 }, old1[] = { 30 }, old2[] = { 50 };
    int64_t d0[] = { 20 }, d1[] = { 40 };
    ASSERT(test_rel_append_row(rel, old0) == 0, "append old 10");
    ASSERT(test_rel_append_row(rel, old1) == 0, "append old 30");
    ASSERT(test_rel_append_row(rel, old2) == 0, "append old 50");
    ASSERT(test_rel_append_row(rel, d0) == 0, "append delta 20");
    ASSERT(test_rel_append_row(rel, d1) == 0, "append delta 40");

    int rc = col_op_consolidate_incremental_delta(rel, 3, delta_out, NULL);

    ASSERT(rc == 0, "returns 0");
    ASSERT(rel->nrows == 5, "rel->nrows == 5");
    ASSERT(test_rel_is_sorted(rel), "merged rel is sorted");
    ASSERT(test_rel_is_unique(rel), "merged rel has no duplicates");
    ASSERT(delta_out->nrows == 2, "delta_out->nrows == 2");
    ASSERT(test_rel_contains_row(delta_out, d0), "delta_out has 20");
    ASSERT(test_rel_contains_row(delta_out, d1), "delta_out has 40");

    int64_t expected[] = { 10, 20, 30, 40, 50 };
    for (int i = 0; i < 5; i++)
        ASSERT(rel->columns[(i) % rel->ncols][(i) / rel->ncols] == expected[i],
            "merged row order mismatch");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 8: fast-path with no old rows (old_nrows=0)
 *
 * Input:  old_nrows=0, rel=[1,2,3] (all delta)
 * Expected:
 *   rel       = [1,2,3]  sorted
 *   delta_out = [1,2,3]  all new (fast-path sub-case a)
 * ================================================================ */
static void
test_fast_path_no_old_rows(void)
{
    TEST("fast-path no-old: old_nrows=0, all delta rows are new");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "test_rel_alloc failed");

    int64_t r0[] = { 1 }, r1[] = { 2 }, r2[] = { 3 };
    ASSERT(test_rel_append_row(rel, r0) == 0, "append 1");
    ASSERT(test_rel_append_row(rel, r1) == 0, "append 2");
    ASSERT(test_rel_append_row(rel, r2) == 0, "append 3");

    int rc = col_op_consolidate_incremental_delta(rel, 0, delta_out, NULL);

    ASSERT(rc == 0, "returns 0");
    ASSERT(rel->nrows == 3, "rel->nrows == 3");
    ASSERT(test_rel_is_sorted(rel), "rel is sorted");
    ASSERT(delta_out->nrows == 3, "delta_out->nrows == 3");
    ASSERT(test_rel_contains_row(delta_out, r0), "delta_out has 1");
    ASSERT(test_rel_contains_row(delta_out, r1), "delta_out has 2");
    ASSERT(test_rel_contains_row(delta_out, r2), "delta_out has 3");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 9: early return when no delta rows (old_nrows == nr)
 *
 * Input:  old=[1,2,3], old_nrows=3, nr=3 (nothing appended)
 * Expected:  returns 0, rel unchanged, delta_out empty
 * ================================================================ */
static void
test_no_delta_early_return(void)
{
    TEST("no delta rows: old_nrows==nr -> early return, rel unchanged");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "test_rel_alloc failed");

    int64_t r0[] = { 1 }, r1[] = { 2 }, r2[] = { 3 };
    ASSERT(test_rel_append_row(rel, r0) == 0, "append 1");
    ASSERT(test_rel_append_row(rel, r1) == 0, "append 2");
    ASSERT(test_rel_append_row(rel, r2) == 0, "append 3");

    int rc = col_op_consolidate_incremental_delta(rel, 3, delta_out, NULL);

    ASSERT(rc == 0, "returns 0");
    ASSERT(rel->nrows == 3, "rel->nrows unchanged == 3");
    ASSERT(delta_out->nrows == 0, "delta_out->nrows == 0 (nothing new)");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 10: fast-path single row each
 *
 * Input:  old=[10], old_nrows=1, delta=[20]
 * Expected:
 *   rel       = [10,20]
 *   delta_out = [20]  (fast-path: 20 > 10)
 * ================================================================ */
static void
test_fast_path_single_row_each(void)
{
    TEST("fast-path single row: old=[10], delta=[20] -> fast append");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "test_rel_alloc failed");

    int64_t old0[] = { 10 }, d0[] = { 20 };
    ASSERT(test_rel_append_row(rel, old0) == 0, "append old 10");
    ASSERT(test_rel_append_row(rel, d0) == 0, "append delta 20");

    int rc = col_op_consolidate_incremental_delta(rel, 1, delta_out, NULL);

    ASSERT(rc == 0, "returns 0");
    ASSERT(rel->nrows == 2, "rel->nrows == 2");
    ASSERT(test_rel_is_sorted(rel), "rel is sorted");
    ASSERT(delta_out->nrows == 1, "delta_out->nrows == 1");
    ASSERT(test_rel_contains_row(delta_out, d0), "delta_out has 20");
    ASSERT(rel->columns[(0) % rel->ncols][(0) / rel->ncols] == 10,
        "first row is 10");
    ASSERT(rel->columns[(1) % rel->ncols][(1) / rel->ncols] == 20,
        "second row is 20");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 11: fast-path hit-rate over CRDT-like chain (Issue #239)
 *
 * Simulates 10 rounds of chain-pattern inserts where each new batch
 * of rows sorts strictly after all existing rows (typical CRDT pattern).
 * Verifies correctness across all rounds; all consolidations should
 * hit the fast-path (last_old < first_delta).
 *
 * Expected:
 *   After round R: rel->nrows == R*10, all rows sorted+unique
 *   delta_out for each round: exactly 10 new rows
 * ================================================================ */
static void
test_fast_path_chain_hit_rate(void)
{
    TEST("fast-path chain: 10 rounds of appending higher rows, all hit");

    const int ROUNDS = 10;
    const int BATCH = 10;

    col_rel_t *rel = test_rel_alloc(1);
    ASSERT(rel != NULL, "test_rel_alloc failed");

    for (int round = 0; round < ROUNDS; round++) {
        col_rel_t *delta_out = test_rel_alloc(1);
        ASSERT(delta_out != NULL, "test_rel_alloc delta_out failed");

        uint32_t old_nrows = rel->nrows;
        /* Append BATCH new rows strictly greater than all existing */
        for (int j = 0; j < BATCH; j++) {
            int64_t val = (int64_t)(round * BATCH + j + 1);
            int64_t row[] = { val };
            ASSERT(test_rel_append_row(rel, row) == 0, "append row");
        }

        int rc
            = col_op_consolidate_incremental_delta(rel, old_nrows, delta_out,
                NULL);
        ASSERT(rc == 0, "consolidate returns 0");

        /* Correctness: all old+new rows present, sorted, unique */
        uint32_t expected_total = (uint32_t)((round + 1) * BATCH);
        ASSERT(rel->nrows == expected_total, "rel->nrows matches expected");
        ASSERT(test_rel_is_sorted(rel), "rel is sorted after round");
        ASSERT(test_rel_is_unique(rel), "rel has no duplicates after round");

        /* All BATCH rows are new */
        ASSERT(delta_out->nrows == (uint32_t)BATCH,
            "delta_out has exactly BATCH new rows");

        test_rel_free(delta_out);
    }

    test_rel_free(rel);
    PASS();
}

/* ================================================================
 * Test 12: out_fast_path reports 1 for empty-old fast path (Issue #278)
 *
 * When old_nrows == 0, fast path sub-case (a) is taken.
 * out_fast_path must be set to 1.
 * ================================================================ */
static void
test_fastpath_counter_empty_old(void)
{
    TEST("out_fast_path==1 when old_nrows==0 (sub-case a)");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "test_rel_alloc failed");

    int64_t r0[] = { 10 }, r1[] = { 20 };
    ASSERT(test_rel_append_row(rel, r0) == 0, "append 10");
    ASSERT(test_rel_append_row(rel, r1) == 0, "append 20");

    int fast_flag = -1;
    int rc = col_op_consolidate_incremental_delta(rel, 0, delta_out,
            &fast_flag);
    ASSERT(rc == 0, "returns 0");
    ASSERT(fast_flag == 1, "out_fast_path == 1 for empty-old case");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 13: out_fast_path reports 1 when delta sorts after all old (Issue #278)
 *
 * Sub-case (b): last old row < first delta row.
 * out_fast_path must be set to 1.
 * ================================================================ */
static void
test_fastpath_counter_sorted_after(void)
{
    TEST("out_fast_path==1 when delta > all old rows (sub-case b)");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "test_rel_alloc failed");

    int64_t old0[] = { 10 }, old1[] = { 20 }, d0[] = { 30 };
    ASSERT(test_rel_append_row(rel, old0) == 0, "append 10");
    ASSERT(test_rel_append_row(rel, old1) == 0, "append 20");
    ASSERT(test_rel_append_row(rel, d0) == 0, "append 30");

    int fast_flag = -1;
    int rc = col_op_consolidate_incremental_delta(rel, 2, delta_out,
            &fast_flag);
    ASSERT(rc == 0, "returns 0");
    ASSERT(fast_flag == 1, "out_fast_path == 1 for sorted-after case");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

/* ================================================================
 * Test 14: out_fast_path reports 0 for interleaved (slow path) (Issue #278)
 *
 * When delta interleaves with old rows, the O(N) merge walk is used.
 * out_fast_path must be set to 0.
 * ================================================================ */
static void
test_fastpath_counter_interleaved(void)
{
    TEST("out_fast_path==0 when delta interleaves old (slow path)");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "test_rel_alloc failed");

    int64_t old0[] = { 10 }, old1[] = { 30 }, d0[] = { 20 };
    ASSERT(test_rel_append_row(rel, old0) == 0, "append 10");
    ASSERT(test_rel_append_row(rel, old1) == 0, "append 30");
    ASSERT(test_rel_append_row(rel, d0) == 0, "append 20");

    int fast_flag = -1;
    int rc = col_op_consolidate_incremental_delta(rel, 2, delta_out,
            &fast_flag);
    ASSERT(rc == 0, "returns 0");
    ASSERT(fast_flag == 0, "out_fast_path == 0 for interleaved case");

    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

static void
test_shared_single_delta_fallback_detaches_view(void)
{
    TEST("shared one-row fallback detaches before mutating storage");

    col_rel_t *owner = test_rel_alloc(1);
    col_rel_t *view = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(owner && view && delta_out, "shared fallback relations");

    int64_t values[] = { 10, 30, 20 };
    for (uint32_t i = 0; i < 3; i++)
        ASSERT(test_rel_append_row(owner, &values[i]) == 0,
            "shared fallback source row");
    ASSERT(col_rel_install_shared_view(view, owner) == 0,
        "shared fallback view installation");
    int64_t *owner_columns = owner->columns[0];
    int fast_path = -1;

    ASSERT(col_op_consolidate_incremental_delta(view, 2, delta_out,
        &fast_path) == 0, "shared fallback consolidation succeeds");
    ASSERT(fast_path == 0, "interleaved one-row delta uses fallback");
    ASSERT(view->col_shared == NULL && view->storage_owner == view
        && owner->storage_alias_borrows == 0,
        "shared fallback retires the view alias");
    ASSERT(owner->columns[0] == owner_columns
        && owner->columns[0][0] == 10
        && owner->columns[0][1] == 30
        && owner->columns[0][2] == 20,
        "shared fallback preserves the canonical owner");
    ASSERT(view->nrows == 3 && view->sorted_nrows == 3
        && view->run_count == 1 && view->run_ends[0] == 3
        && test_rel_is_sorted(view) && test_rel_is_unique(view)
        && view->columns[0][0] == 10
        && view->columns[0][1] == 20
        && view->columns[0][2] == 30,
        "shared fallback publishes sorted private storage");
    ASSERT(delta_out->nrows == 1 && delta_out->columns[0][0] == 20,
        "shared fallback emits the novel row");

    test_rel_free(delta_out);
    test_rel_free(view);
    test_rel_free(owner);
    PASS();
}

/* ================================================================
 * Issue #2048: fast-path run compaction rewrites rel->columns in place, so
 * a shared view must detach before compacting or it rewrites the canonical
 * owner's buffers.
 *
 * Fixture: 32 sorted rows 0, 10, ..., 310 form one run; seven binary-path
 * consolidations then append the novel rows 15, 25, ..., 75 as singleton
 * runs that interleave with the first, reaching COL_MAX_RUNS.  The trailing
 * row 1000 is appended unconsolidated, so a one-row fast-path consolidation
 * of a view over this owner must compact.
 * ================================================================ */
#define EIGHT_RUN_OWNER_ROWS 40u

static bool
build_eight_run_owner(col_rel_t *owner)
{
    int fast_path = -1;
    for (uint32_t i = 0; i < 32u; i++) {
        int64_t value = (int64_t)i * 10;
        if (test_rel_append_row(owner, &value) != 0)
            return false;
    }
    if (col_op_consolidate_incremental_delta(owner, 0, NULL, &fast_path) != 0
        || fast_path != 1 || owner->run_count != 1)
        return false;
    for (int64_t k = 0; k < 7; k++) {
        int64_t value = 15 + 10 * k;
        uint32_t old_nrows = owner->nrows;
        if (test_rel_append_row(owner, &value) != 0
            || col_op_consolidate_incremental_delta(owner, old_nrows, NULL,
            &fast_path) != 0 || fast_path != 0)
            return false;
    }
    if (owner->run_count != COL_MAX_RUNS || owner->nrows != 39u)
        return false;
    for (uint32_t r = 0; r < COL_MAX_RUNS; r++) {
        if (owner->run_ends[r] != 32u + r)
            return false;
    }
    int64_t trailing = 1000;
    return test_rel_append_row(owner, &trailing) == 0
           && owner->nrows == EIGHT_RUN_OWNER_ROWS;
}

/* The owner keeps its interleaved physical layout and its eight runs. */
static bool
eight_run_owner_intact(const col_rel_t *owner, const int64_t *before)
{
    if (owner->nrows != EIGHT_RUN_OWNER_ROWS
        || owner->run_count != COL_MAX_RUNS
        || memcmp(before, owner->columns[0],
        EIGHT_RUN_OWNER_ROWS * sizeof(int64_t)) != 0)
        return false;
    for (uint32_t r = 0; r < COL_MAX_RUNS; r++) {
        if (owner->run_ends[r] != 32u + r)
            return false;
    }
    for (uint32_t k = 0; k < 7u; k++) {
        if (owner->columns[0][32u + k] != 15 + 10 * (int64_t)k)
            return false;
    }
    return true;
}

/* The view publishes one private, compacted run of all 40 values. */
static bool
eight_run_view_compacted(const col_rel_t *view, const int64_t *owner_columns)
{
    int64_t expected[EIGHT_RUN_OWNER_ROWS];
    uint32_t n = 0;
    for (uint32_t i = 0; i < 32u; i++) {
        int64_t value = (int64_t)i * 10;
        expected[n++] = value;
        if (value >= 10 && value <= 70)
            expected[n++] = value + 5;
    }
    expected[n++] = 1000;
    if (n != EIGHT_RUN_OWNER_ROWS || view->col_shared != NULL
        || view->storage_owner != view || view->columns[0] == owner_columns
        || view->nrows != n || view->sorted_nrows != n
        || view->run_count != 1 || view->run_ends[0] != n
        || !test_rel_is_sorted(view) || !test_rel_is_unique(view))
        return false;
    return memcmp(expected, view->columns[0], sizeof(expected)) == 0;
}

static void
test_shared_fast_path_compaction_detaches_view(void)
{
    TEST("shared fast-path compaction detaches before rewriting (#2048)");

    col_rel_t *owner = test_rel_alloc(1);
    col_rel_t *view = test_rel_alloc(1);
    col_rel_t *sibling = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(owner && view && sibling && delta_out,
        "shared compaction relations");
    ASSERT(build_eight_run_owner(owner), "eight-run owner fixture");

    int64_t before[EIGHT_RUN_OWNER_ROWS];
    memcpy(before, owner->columns[0], sizeof(before));
    int64_t *owner_columns = owner->columns[0];
    ASSERT(col_rel_install_shared_view(view, owner) == 0
        && col_rel_install_shared_view(sibling, owner) == 0,
        "shared compaction views");
    ASSERT(col_rel_storage_alias_borrow_count(owner) == 2
        && view->run_count == COL_MAX_RUNS && view->run_ends[7] == 39u,
        "views borrow the eight-run owner");
    uint64_t storage_generation = view->storage_generation;

    int fast_path = -1;
    ASSERT(col_op_consolidate_incremental_delta(view, 39u, delta_out,
        &fast_path) == 0 && fast_path == 1,
        "trailing one-row delta takes the compacting fast path");
    ASSERT(owner->columns[0] == owner_columns
        && eight_run_owner_intact(owner, before),
        "compaction leaves the canonical owner's rows and runs intact");
    ASSERT(col_rel_storage_alias_borrow_count(owner) == 1
        && sibling->col_shared && sibling->col_shared[0]
        && sibling->columns[0] == owner_columns
        && memcmp(before, sibling->columns[0], sizeof(before)) == 0,
        "only the detached view releases its borrow");
    ASSERT(eight_run_view_compacted(view, owner_columns)
        && view->storage_generation > storage_generation,
        "view publishes private compacted storage");
    ASSERT(delta_out->nrows == 1 && delta_out->columns[0][0] == 1000,
        "fast path emits the trailing row");

    wl_columnar_source_access_reader_t reader = { 0 };
    ASSERT(col_rel_source_reader_acquire(view, &reader) == 0
        && col_rel_source_reader_release(&reader) == 0,
        "detached view accepts readers after cleanup");

    test_rel_free(delta_out);
    test_rel_free(view);
    test_rel_free(sibling);
    test_rel_free(owner);
    PASS();
}

/* Without compaction the fast path writes no borrowed column buffer, so the
 * view keeps borrowing; timestamp retirement frees only the view's copy. */
static void
test_shared_fast_path_without_compaction_keeps_borrow(void)
{
    TEST("shared fast path without compaction keeps its borrow (#2048)");

    for (uint32_t with_timestamps = 0; with_timestamps < 2; with_timestamps++) {
        col_rel_t *owner = test_rel_alloc(1);
        col_rel_t *view = test_rel_alloc(1);
        int fast_path = -1;
        int64_t rows[] = { 10, 20, 30 };
        ASSERT(owner && view, "borrowing fast-path relations");
        ASSERT(test_rel_append_row(owner, &rows[0]) == 0
            && test_rel_append_row(owner, &rows[1]) == 0
            && col_op_consolidate_incremental_delta(owner, 0, NULL,
            &fast_path) == 0
            && test_rel_append_row(owner, &rows[2]) == 0,
            "borrowing fast-path owner");
        if (with_timestamps)
            ASSERT(col_rel_enable_timestamps(owner) == 0
                && owner->timestamps != NULL, "owner timestamps");
        ASSERT(col_rel_install_shared_view(view, owner) == 0,
            "borrowing fast-path view");
        int64_t *owner_columns = owner->columns[0];
        const col_delta_timestamp_t *owner_timestamps = owner->timestamps;
        ASSERT(!with_timestamps
            || (view->timestamps && view->timestamps != owner_timestamps),
            "view holds a private timestamp copy");
        uint64_t storage_generation = view->storage_generation;

        fast_path = -1;
        ASSERT(col_op_consolidate_incremental_delta(view, 2, NULL,
            &fast_path) == 0 && fast_path == 1,
            "trailing row takes the fast path");
        ASSERT(view->col_shared && view->col_shared[0]
            && view->columns[0] == owner_columns
            && col_rel_storage_alias_borrow_count(owner) == 1,
            "non-compacting fast path keeps the borrow");
        ASSERT(view->nrows == 3 && view->run_count == 2
            && view->run_ends[0] == 2 && view->run_ends[1] == 3,
            "view registers the trailing run");
        ASSERT(owner->nrows == 3 && owner->run_count == 1
            && owner->run_ends[0] == 2
            && owner->columns[0][0] == 10 && owner->columns[0][1] == 20
            && owner->columns[0][2] == 30,
            "owner rows and runs unchanged");
        if (with_timestamps) {
            ASSERT(view->timestamps == NULL && view->timestamp_capacity == 0
                && owner->timestamps == owner_timestamps,
                "retirement frees only the view's timestamp copy");
        } else {
            ASSERT(view->storage_generation == storage_generation,
                "borrowing fast path does not replace storage");
        }
        test_rel_free(view);
        test_rel_free(owner);
    }
    PASS();
}

static wl_columnar_memory_governor_ref_t *
test_shared_compaction_governor_create(uint64_t usable_bytes)
{
    wl_columnar_memory_resolution_t resolution = { 0 };
    resolution.budget_bytes = usable_bytes;
    resolution.usable_bytes = usable_bytes;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    return wl_columnar_memory_governor_ref_create(&resolution);
}

/* A denied detach refuses before the fast path emits a row or publishes
 * nrows, runs, or a generation, so a retry takes the same path. */
static void
test_shared_fast_path_compaction_denied_is_transactional(void)
{
    TEST("denied shared fast-path detach publishes nothing (#2048)");

    col_rel_t *owner = test_rel_alloc(1);
    col_rel_t *view = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    wl_columnar_memory_governor_ref_t *ref
        = test_shared_compaction_governor_create(1u << 20);
    ASSERT(owner && view && delta_out && ref, "denied detach relations");
    ASSERT(build_eight_run_owner(owner), "eight-run owner fixture");
    ASSERT(col_rel_attach_memory_governor(view, ref) == 0
        && col_rel_install_shared_view(view, owner) == 0,
        "governed shared view");

    int64_t before[EIGHT_RUN_OWNER_ROWS];
    memcpy(before, owner->columns[0], sizeof(before));
    int64_t *owner_columns = owner->columns[0];
    const bool *shared = view->col_shared;
    uint32_t run_ends[COL_MAX_RUNS];
    memcpy(run_ends, view->run_ends, sizeof(run_ends));
    uint32_t nrows = view->nrows;
    uint32_t sorted_nrows = view->sorted_nrows;
    uint32_t run_count = view->run_count;
    uint64_t view_generation = view->view_generation;
    uint64_t storage_generation = view->storage_generation;
    wl_columnar_memory_governor_t *governor
        = wl_columnar_memory_governor_ref_get(ref);
    atomic_store_explicit(&governor->usable_bytes,
        wl_columnar_memory_reserved(governor), memory_order_release);

    int fast_path = -1;
    ASSERT(col_op_consolidate_incremental_delta(view, 39u, delta_out,
        &fast_path) == ENOMEM && fast_path == -1,
        "denied detach is refused");
    /* Before #2048 the call also failed with ENOMEM, from the compaction
     * scratch; the run, row-count and generation checks tell them apart. */
    ASSERT(view->nrows == nrows && view->sorted_nrows == sorted_nrows
        && view->run_count == run_count
        && memcmp(run_ends, view->run_ends, sizeof(run_ends)) == 0
        && view->view_generation == view_generation
        && view->storage_generation == storage_generation,
        "denied detach publishes no rows, runs or generations");
    ASSERT(view->col_shared == shared && view->columns[0] == owner_columns
        && col_rel_storage_alias_borrow_count(owner) == 1
        && delta_out->nrows == 0
        && eight_run_owner_intact(owner, before),
        "denied detach keeps the borrow, the owner and delta_out");
    ASSERT(wl_columnar_memory_reserved(governor)
        == view->descriptor_reserved_bytes + view->metadata_reserved_bytes
        + view->retained_reserved_bytes,
        "denied detach leaks no governor credit");

    atomic_store_explicit(&governor->usable_bytes, 1u << 20,
        memory_order_release);
    ASSERT(col_op_consolidate_incremental_delta(view, 39u, delta_out,
        &fast_path) == 0 && fast_path == 1,
        "retry after denial succeeds");
    ASSERT(eight_run_view_compacted(view, owner_columns)
        && eight_run_owner_intact(owner, before)
        && col_rel_storage_alias_borrow_count(owner) == 0
        && delta_out->nrows == 1 && delta_out->columns[0][0] == 1000,
        "retry detaches and compacts privately");

    test_rel_free(delta_out);
    test_rel_free(view);
    test_rel_free(owner);
    wl_columnar_memory_governor_ref_release(ref);
    PASS();
}

/* ================================================================
 * Test 15: NULL out_fast_path does not crash (Issue #278)
 *
 * Both fast and slow paths must be NULL-safe.
 * ================================================================ */
static void
test_fastpath_counter_null_safe(void)
{
    TEST("NULL out_fast_path: no crash on fast path");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "test_rel_alloc failed");

    int64_t old0[] = { 10 }, d0[] = { 20 };
    ASSERT(test_rel_append_row(rel, old0) == 0, "append 10");
    ASSERT(test_rel_append_row(rel, d0) == 0, "append 20");

    /* Should not crash with NULL out_fast_path (fast path taken) */
    int rc = col_op_consolidate_incremental_delta(rel, 1, delta_out, NULL);
    ASSERT(rc == 0, "returns 0 with NULL out_fast_path");

    test_rel_free(rel);
    test_rel_free(delta_out);

    /* Also test slow path with NULL */
    col_rel_t *rel2 = test_rel_alloc(1);
    col_rel_t *delta_out2 = test_rel_alloc(1);
    ASSERT(rel2 && delta_out2, "test_rel_alloc failed (2)");

    int64_t a[] = { 10 }, b[] = { 30 }, c[] = { 20 };
    ASSERT(test_rel_append_row(rel2, a) == 0, "append 10");
    ASSERT(test_rel_append_row(rel2, b) == 0, "append 30");
    ASSERT(test_rel_append_row(rel2, c) == 0, "append 20");

    rc = col_op_consolidate_incremental_delta(rel2, 2, delta_out2, NULL);
    ASSERT(rc == 0, "returns 0 with NULL out_fast_path (slow path)");

    test_rel_free(rel2);
    test_rel_free(delta_out2);
    PASS();
}

/* Issue #1495: consolidation must admit both the source and delta output
 * owners before sorting or appending any rows. */
static void
test_source_exclusion_is_transactional(void)
{
    TEST("consolidation excludes readers before mutation");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "test_rel_alloc failed");
    int64_t old_row[] = { 10 };
    int64_t new_row[] = { 20 };
    ASSERT(test_rel_append_row(rel, old_row) == 0, "append old row");
    ASSERT(test_rel_append_row(rel, new_row) == 0, "append new row");

    int64_t *columns_before = rel->columns[0];
    uint32_t rows_before = rel->nrows;
    uint32_t delta_rows_before = delta_out->nrows;
    wl_columnar_source_access_reader_t reader = { 0 };
    ASSERT(col_rel_source_reader_acquire(rel, &reader) == 0,
        "acquire source reader");
    int rc = col_op_consolidate_incremental_delta(rel, 1, delta_out, NULL);
    ASSERT(rc == EBUSY, "source reader blocks consolidation");
    ASSERT(rel->columns[0] == columns_before
        && rel->nrows == rows_before
        && delta_out->nrows == delta_rows_before,
        "source-blocked consolidation changed state");
    ASSERT(col_rel_source_reader_release(&reader) == 0,
        "release source reader");
    ASSERT(col_op_consolidate_incremental_delta(rel, 1, delta_out, NULL) == 0,
        "retry after source reader release");
    ASSERT(delta_out->nrows == 1, "retry produced delta output");
    test_rel_free(rel);
    test_rel_free(delta_out);

    rel = test_rel_alloc(1);
    delta_out = test_rel_alloc(1);
    ASSERT(rel && delta_out, "test_rel_alloc failed (delta reader)");
    ASSERT(test_rel_append_row(rel, old_row) == 0, "append old row (delta)");
    ASSERT(test_rel_append_row(rel, new_row) == 0, "append new row (delta)");
    columns_before = rel->columns[0];
    rows_before = rel->nrows;
    ASSERT(col_rel_source_reader_acquire(delta_out, &reader) == 0,
        "acquire delta reader");
    rc = col_op_consolidate_incremental_delta(rel, 1, delta_out, NULL);
    ASSERT(rc == EBUSY, "delta reader blocks consolidation");
    ASSERT(rel->columns[0] == columns_before && rel->nrows == rows_before
        && delta_out->nrows == 0,
        "delta-blocked consolidation changed state");
    ASSERT(col_rel_source_reader_release(&reader) == 0,
        "release delta reader");
    ASSERT(col_op_consolidate_incremental_delta(rel, 1, rel, NULL) == EINVAL,
        "reject source/output alias");
    test_rel_free(rel);
    test_rel_free(delta_out);
    PASS();
}

#ifdef WL_TEST_CONSOLIDATE_HOOK
/* Issue #1594: the sort hook fires on the deferred-alias shape, not on which
 * wrapper called in.  This pins the scope so the re-key can neither widen it
 * (every plain col_rel_radix_sort would fire, and the paused hook used by the
 * tests above blocks on a condvar, so a widened scope hangs the suite rather
 * than failing an assertion) nor drop it.  A counting hook is used here for
 * exactly that reason: it can observe an unexpected fire without deadlocking. */
static uint32_t sort_hook_fire_count;
static wl_columnar_consolidation_test_stage_t sort_hook_last_stage;

static void
counting_sort_hook(col_rel_t *rel,
    wl_columnar_consolidation_test_stage_t stage)
{
    (void)rel;
    if (stage == WL_COLUMNAR_CONSOLIDATION_TEST_SORT_AFTER_DETACH) {
        sort_hook_fire_count++;
        sort_hook_last_stage = stage;
    }
}

static void
test_sort_hook_scope_follows_the_deferred_alias(void)
{
    TEST("sort hook fires for the deferred alias shape only");

    col_rel_t *owner = test_rel_alloc(1);
    col_rel_t *view = test_rel_alloc(1);
    int64_t values[] = { 30, 10, 20 };
    bool standalone_ok;
    bool consolidation_ok;

    ASSERT(owner && view, "allocate hook scope relations");
    for (uint32_t i = 0; i < 3; i++)
        ASSERT(test_rel_append_row(owner, &values[i]) == 0,
            "append hook scope row");
    ASSERT(col_rel_install_shared_view(view, owner) == 0,
        "install hook scope view");

    sort_hook_fire_count = 0;
    sort_hook_last_stage = 0;
    wl_columnar_consolidation_transition_hook = counting_sort_hook;

    /* Self-releasing shape: col_rel_radix_sort takes and retires the borrow
     * itself, so the hook must stay silent. */
    standalone_ok = col_rel_radix_sort(view, 0, view->nrows) == 0
        && sort_hook_fire_count == 0;

    /* Deferred shape: consolidation hands the borrow back, so the hook fires
     * exactly once. */
    col_rel_t *owner2 = test_rel_alloc(1);
    col_rel_t *rel2 = test_rel_alloc(1);
    consolidation_ok = owner2 && rel2;
    if (consolidation_ok) {
        for (uint32_t i = 0; i < 3; i++)
            consolidation_ok = consolidation_ok
                && test_rel_append_row(owner2, &values[i]) == 0;
        consolidation_ok = consolidation_ok
            && col_rel_install_shared_view(rel2, owner2) == 0;
        sort_hook_fire_count = 0;
        consolidation_ok = consolidation_ok
            && col_op_consolidate_incremental_delta(rel2, 1, NULL, NULL) == 0
            && sort_hook_fire_count == 1
            && sort_hook_last_stage
            == WL_COLUMNAR_CONSOLIDATION_TEST_SORT_AFTER_DETACH;
    }

    wl_columnar_consolidation_transition_hook = NULL;
    ASSERT(standalone_ok, "self-releasing sort does not fire the hook");
    ASSERT(consolidation_ok, "deferred consolidation sort fires it once");
    test_rel_free(owner);
    test_rel_free(view);
    test_rel_free(owner2);
    test_rel_free(rel2);
    PASS();
}

static void
test_shared_source_reader_excluded_through_sort(void)
{
    TEST("shared source keeps owner writer through detach and radix sort");

    col_rel_t *owner = test_rel_alloc(1);
    col_rel_t *rel = test_rel_alloc(1);
    ASSERT(owner && rel, "allocate shared source relations");
    int64_t values[] = { 10, 30, 20 };
    for (uint32_t i = 0; i < 3; i++)
        ASSERT(test_rel_append_row(owner, &values[i]) == 0,
            "append shared source row");
    ASSERT(col_rel_install_shared_view(rel, owner) == 0,
        "install shared source view");
    int64_t *old_columns = rel->columns[0];
    int reader_rc = EINVAL;
    int operation_rc = EINVAL;
    bool hook_state_ok = false;
    ASSERT(run_consolidation_during_pause(rel, 1, NULL, rel, owner,
        WL_COLUMNAR_CONSOLIDATION_TEST_SORT_AFTER_DETACH, values, 3,
        old_columns, &reader_rc, &operation_rc, &hook_state_ok),
        "run source consolidation with deterministic pause");
    ASSERT(hook_state_ok && reader_rc == EBUSY,
        "source reader is excluded after detach and before sort");
    ASSERT(operation_rc == 0 && rel->nrows == 3
        && rel->storage_owner == rel && owner->storage_alias_borrows == 0,
        "source consolidation commits and releases deferred alias");
    ASSERT(test_rel_is_sorted(rel) && test_rel_is_unique(rel)
        && rel->columns[0][0] == 10
        && rel->columns[0][1] == 20
        && rel->columns[0][2] == 30,
        "detached source rows sort and consolidate correctly");
    ASSERT(owner->columns[0] == old_columns
        && owner->columns[0][0] == 10
        && owner->columns[0][1] == 30
        && owner->columns[0][2] == 20,
        "canonical source owner remains unchanged");
    wl_columnar_source_access_reader_t reader = { 0 };
    ASSERT(col_rel_source_reader_acquire(rel, &reader) == 0
        && col_rel_source_reader_release(&reader) == 0,
        "source reader succeeds after consolidation cleanup");
    test_rel_free(rel);
    test_rel_free(owner);
    PASS();
}

static void
test_shared_delta_reader_excluded_through_append(void)
{
    TEST("shared delta keeps owner writer through post-detach append");

    col_rel_t *rel = test_rel_alloc(1);
    col_rel_t *owner = test_rel_alloc(1);
    col_rel_t *delta_out = test_rel_alloc(1);
    ASSERT(rel && owner && delta_out, "allocate shared delta relations");
    int64_t values[] = { 2, 1 };
    ASSERT(test_rel_append_row(rel, &values[0]) == 0
        && test_rel_append_row(rel, &values[1]) == 0,
        "append source delta rows");
    ASSERT(col_rel_install_shared_view(delta_out, owner) == 0,
        "install shared delta output view");
    ASSERT(delta_out->capacity > delta_out->nrows,
        "shared delta view has spare capacity");
    int64_t *old_columns = delta_out->columns[0];
    int reader_rc = EINVAL;
    int operation_rc = EINVAL;
    bool hook_state_ok = false;
    ASSERT(run_consolidation_during_pause(rel, 0, delta_out, delta_out,
        owner, WL_COLUMNAR_CONSOLIDATION_TEST_APPEND_AFTER_DETACH,
        NULL, 0, old_columns, &reader_rc, &operation_rc, &hook_state_ok),
        "run delta consolidation with deterministic pause");
    ASSERT(hook_state_ok && reader_rc == EBUSY,
        "delta reader is excluded at the first post-detach append");
    ASSERT(operation_rc == 0 && delta_out->nrows == 2
        && delta_out->storage_owner == delta_out
        && owner->storage_alias_borrows == 0,
        "delta consolidation commits and releases deferred alias");
    ASSERT(test_rel_is_sorted(delta_out) && test_rel_is_unique(delta_out)
        && delta_out->columns[0][0] == 1
        && delta_out->columns[0][1] == 2,
        "delta output rows are correct and ordered");
    ASSERT(owner->nrows == 0 && owner->columns[0] == old_columns,
        "canonical delta owner remains unchanged");
    wl_columnar_source_access_reader_t reader = { 0 };
    ASSERT(col_rel_source_reader_acquire(delta_out, &reader) == 0
        && col_rel_source_reader_release(&reader) == 0,
        "delta reader succeeds after consolidation cleanup");
    test_rel_free(delta_out);
    test_rel_free(owner);
    test_rel_free(rel);
    PASS();
}
#endif

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */
int
main(void)
{
    printf("=== test_consolidate_incremental_delta ===\n\n");

    test_empty_old_all_new();
    test_all_duplicate_delta_no_change();
    test_partial_delta_merged_and_new();
    test_first_iteration_dedup_all_new();
    test_fallback_retains_merge_capacity_after_heavy_dedup();
    test_large_dataset_correctness();

    /* Fast-path tests (Issue #239) */
    test_fast_path_hit_append();
    test_fast_path_miss_interleaved();
    test_fast_path_no_old_rows();
    test_no_delta_early_return();
    test_fast_path_single_row_each();
    test_fast_path_chain_hit_rate();

    /* Fast-path counter tests (Issue #278) */
    test_fastpath_counter_empty_old();
    test_fastpath_counter_sorted_after();
    test_fastpath_counter_interleaved();
    test_shared_single_delta_fallback_detaches_view();
    test_shared_fast_path_compaction_detaches_view();
    test_shared_fast_path_without_compaction_keeps_borrow();
    test_shared_fast_path_compaction_denied_is_transactional();
    test_fastpath_counter_null_safe();
    test_initialized_zero_column_relation();
    test_source_exclusion_is_transactional();
#ifdef WL_TEST_CONSOLIDATE_HOOK
    test_sort_hook_scope_follows_the_deferred_alias();
    test_shared_source_reader_excluded_through_sort();
    test_shared_delta_reader_excluded_through_append();
#endif

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n", pass_count,
        fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
