/*
 * test_diff_join.c - Differential join operator tests (Issue #263)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Tests col_op_join_diff: arrangement reuse via col_diff_arrangement_t.
 * Part of #244 Timely-Differential Dataflow Migration, Stage 2.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

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
    for (uint32_t i = 0; i < s->nrels; i++) {
        col_rel_free_contents(s->rels[i]);
        free(s->rels[i]);
    }
    free(s->rels);
    /* Free arrangement registry (matches real session destroy) */
    for (uint32_t i = 0; i < s->arr_count; i++) {
        free(s->arr_entries[i].rel_name);
        free(s->arr_entries[i].key_cols);
        arr_free_contents(&s->arr_entries[i].arr);
    }
    free(s->arr_entries);
    col_session_free_diff_arrangements(s);
    delta_pool_destroy(s->delta_pool);
    free(s);
}

static col_rel_t *
make_rel(const char *name, uint32_t ncols, const char *const *col_names)
{
    col_rel_t *r = col_rel_new_auto(name, ncols);
    if (r && col_names)
        col_rel_set_schema(r, ncols, col_names);
    return r;
}

static bool
output_contains_row(const col_rel_t *r, const int64_t *row)
{
    for (uint32_t i = 0; i < r->nrows; i++) {
        bool match = true;
        for (uint32_t c = 0; c < r->ncols; c++) {
            if (r->data[(size_t)i * r->ncols + c] != row[c]) {
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
test_basic_join_correctness(void)
{
    TEST("basic join correctness - 2-col join");
    wl_col_session_t *sess = make_mock_session();

    const char *cn[] = {"x", "y"};
    col_rel_t *left = make_rel("left", 2, cn);
    int64_t lr1[] = {1, 10};
    int64_t lr2[] = {2, 20};
    col_rel_append_row(left, lr1);
    col_rel_append_row(left, lr2);

    col_rel_t *right = make_rel("right", 2, cn);
    int64_t rr1[] = {1, 100};
    int64_t rr2[] = {3, 300};
    col_rel_append_row(right, rr1);
    col_rel_append_row(right, rr2);
    session_add_rel(sess, right);

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *lkeys[] = {"x"};
    char *rkeys[] = {"x"};
    op.left_keys = lkeys;
    op.right_keys = rkeys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "join should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel != NULL, "result should not be NULL");
    ASSERT_TRUE(result.rel->nrows == 1, "should have 1 matching row");
    ASSERT_TRUE(result.rel->ncols == 4, "output should have 4 cols");

    /* Output: [1, 10, 1, 100] */
    ASSERT_TRUE(result.rel->data[0] == 1, "left x=1");
    ASSERT_TRUE(result.rel->data[1] == 10, "left y=10");
    ASSERT_TRUE(result.rel->data[2] == 1, "right x=1");
    ASSERT_TRUE(result.rel->data[3] == 100, "right y=100");

    free(result.seg_boundaries);
    if (result.owned)
        col_rel_destroy(result.rel);
    col_rel_destroy(left);
    destroy_mock_session(sess);
    PASS;
}

static void
test_empty_left(void)
{
    TEST("empty left relation => empty output");
    wl_col_session_t *sess = make_mock_session();

    const char *cn[] = {"x"};
    col_rel_t *left = make_rel("left", 1, cn);

    col_rel_t *right = make_rel("right", 1, cn);
    int64_t rr[] = {1};
    col_rel_append_row(right, rr);
    session_add_rel(sess, right);

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *keys[] = {"x"};
    op.left_keys = keys;
    op.right_keys = keys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "join should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->nrows == 0, "empty left => empty output");

    free(result.seg_boundaries);
    if (result.owned)
        col_rel_destroy(result.rel);
    col_rel_destroy(left);
    destroy_mock_session(sess);
    PASS;
}

static void
test_empty_right(void)
{
    TEST("missing right relation => empty output");
    wl_col_session_t *sess = make_mock_session();

    const char *cn[] = {"x"};
    col_rel_t *left = make_rel("left", 1, cn);
    int64_t lr[] = {1};
    col_rel_append_row(left, lr);

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "nonexistent";
    op.key_count = 1;
    char *keys[] = {"x"};
    op.left_keys = keys;
    op.right_keys = keys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "join should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->nrows == 0, "missing right => empty output");

    free(result.seg_boundaries);
    if (result.owned)
        col_rel_destroy(result.rel);
    col_rel_destroy(left);
    destroy_mock_session(sess);
    PASS;
}

static void
test_no_matching_keys(void)
{
    TEST("disjoint key values => empty output");
    wl_col_session_t *sess = make_mock_session();

    const char *cn[] = {"x", "y"};
    col_rel_t *left = make_rel("left", 2, cn);
    int64_t lr[] = {1, 10};
    col_rel_append_row(left, lr);

    col_rel_t *right = make_rel("right", 2, cn);
    int64_t rr[] = {2, 20};
    col_rel_append_row(right, rr);
    session_add_rel(sess, right);

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *lkeys[] = {"x"};
    char *rkeys[] = {"x"};
    op.left_keys = lkeys;
    op.right_keys = rkeys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "join should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->nrows == 0, "disjoint keys => empty output");

    free(result.seg_boundaries);
    if (result.owned)
        col_rel_destroy(result.rel);
    col_rel_destroy(left);
    destroy_mock_session(sess);
    PASS;
}

static void
test_all_rows_match(void)
{
    TEST("all rows match => cross-product on matching keys");
    wl_col_session_t *sess = make_mock_session();

    const char *cn[] = {"x", "y"};
    col_rel_t *left = make_rel("left", 2, cn);
    int64_t lr1[] = {1, 10};
    int64_t lr2[] = {1, 20};
    col_rel_append_row(left, lr1);
    col_rel_append_row(left, lr2);

    col_rel_t *right = make_rel("right", 2, cn);
    int64_t rr1[] = {1, 100};
    int64_t rr2[] = {1, 200};
    col_rel_append_row(right, rr1);
    col_rel_append_row(right, rr2);
    session_add_rel(sess, right);

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *lkeys[] = {"x"};
    char *rkeys[] = {"x"};
    op.left_keys = lkeys;
    op.right_keys = rkeys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "join should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->nrows == 4, "2x2 matching => 4 output rows");

    free(result.seg_boundaries);
    if (result.owned)
        col_rel_destroy(result.rel);
    col_rel_destroy(left);
    destroy_mock_session(sess);
    PASS;
}

static void
test_multi_key_columns(void)
{
    TEST("join on two key columns");
    wl_col_session_t *sess = make_mock_session();

    const char *cn[] = {"a", "b", "v"};
    col_rel_t *left = make_rel("left", 3, cn);
    int64_t lr1[] = {10, 20, 1};
    int64_t lr2[] = {30, 40, 2};
    col_rel_append_row(left, lr1);
    col_rel_append_row(left, lr2);

    col_rel_t *right = make_rel("right", 3, cn);
    int64_t rr1[] = {10, 20, 100};
    int64_t rr2[] = {50, 60, 200};
    col_rel_append_row(right, rr1);
    col_rel_append_row(right, rr2);
    session_add_rel(sess, right);

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 2;
    char *lkeys[] = {"a", "b"};
    char *rkeys[] = {"a", "b"};
    op.left_keys = lkeys;
    op.right_keys = rkeys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    /* Verify differential join matches standard join output */
    eval_stack_t stack;
    eval_stack_init(&stack);
    col_rel_t *left_copy = make_rel("left_copy", 3, cn);
    col_rel_append_row(left_copy, lr1);
    col_rel_append_row(left_copy, lr2);
    eval_stack_push(&stack, left_copy, false);
    int rc_std = col_op_join(&op, &stack, sess);
    eval_entry_t std_result = eval_stack_pop(&stack);

    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);
    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "diff join should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(rc_std == 0, "std join should succeed");
    ASSERT_TRUE(result.rel->nrows == std_result.rel->nrows,
        "diff join matches std join nrows");
    ASSERT_TRUE(result.rel->ncols == 6, "output should have 6 cols");

    free(result.seg_boundaries);
    if (result.owned)
        col_rel_destroy(result.rel);
    free(std_result.seg_boundaries);
    if (std_result.owned)
        col_rel_destroy(std_result.rel);
    col_rel_destroy(left);
    col_rel_destroy(left_copy);
    destroy_mock_session(sess);
    PASS;
}

static void
test_duplicate_keys_in_right(void)
{
    TEST("multiple matches per left row");
    wl_col_session_t *sess = make_mock_session();

    const char *cn[] = {"x", "y"};
    col_rel_t *left = make_rel("left", 2, cn);
    int64_t lr[] = {1, 10};
    col_rel_append_row(left, lr);

    col_rel_t *right = make_rel("right", 2, cn);
    int64_t rr1[] = {1, 100};
    int64_t rr2[] = {1, 200};
    int64_t rr3[] = {1, 300};
    col_rel_append_row(right, rr1);
    col_rel_append_row(right, rr2);
    col_rel_append_row(right, rr3);
    session_add_rel(sess, right);

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *lkeys[] = {"x"};
    char *rkeys[] = {"x"};
    op.left_keys = lkeys;
    op.right_keys = rkeys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "join should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->nrows == 3, "1 left x 3 right => 3 rows");

    free(result.seg_boundaries);
    if (result.owned)
        col_rel_destroy(result.rel);
    col_rel_destroy(left);
    destroy_mock_session(sess);
    PASS;
}

static void
test_arrangement_reuse(void)
{
    TEST("arrangement reuse: second call reuses existing");
    wl_col_session_t *sess = make_mock_session();

    const char *cn[] = {"x", "y"};
    col_rel_t *right = make_rel("right", 2, cn);
    int64_t rr1[] = {1, 100};
    int64_t rr2[] = {2, 200};
    col_rel_append_row(right, rr1);
    col_rel_append_row(right, rr2);
    session_add_rel(sess, right);

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *lkeys[] = {"x"};
    char *rkeys[] = {"x"};
    op.left_keys = lkeys;
    op.right_keys = rkeys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    /* First join */
    col_rel_t *left1 = make_rel("left1", 2, cn);
    int64_t lr1[] = {1, 10};
    col_rel_append_row(left1, lr1);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left1, false);
    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "first join ok");
    eval_entry_t r1 = eval_stack_pop(&stack);
    ASSERT_TRUE(r1.rel->nrows == 1, "first join: 1 row");

    ASSERT_TRUE(sess->diff_arr_count == 1, "one diff arrangement created");

    /* Second join - should reuse */
    col_rel_t *left2 = make_rel("left2", 2, cn);
    int64_t lr2[] = {2, 20};
    col_rel_append_row(left2, lr2);

    eval_stack_init(&stack);
    eval_stack_push(&stack, left2, false);
    rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "second join ok");
    eval_entry_t r2 = eval_stack_pop(&stack);
    ASSERT_TRUE(r2.rel->nrows == 1, "second join: 1 row");

    ASSERT_TRUE(sess->diff_arr_count == 1,
        "still one diff arrangement (reused)");

    free(r1.seg_boundaries);
    if (r1.owned)
        col_rel_destroy(r1.rel);
    free(r2.seg_boundaries);
    if (r2.owned)
        col_rel_destroy(r2.rel);
    col_rel_destroy(left1);
    col_rel_destroy(left2);
    destroy_mock_session(sess);
    PASS;
}

static void
test_arrangement_incremental(void)
{
    TEST("arrangement incremental: add rows between joins");
    wl_col_session_t *sess = make_mock_session();

    const char *cn[] = {"x", "y"};
    col_rel_t *right = make_rel("right", 2, cn);
    int64_t rr1[] = {1, 100};
    col_rel_append_row(right, rr1);
    session_add_rel(sess, right);

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *lkeys[] = {"x"};
    char *rkeys[] = {"x"};
    op.left_keys = lkeys;
    op.right_keys = rkeys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    /* First join: right has 1 row */
    col_rel_t *left1 = make_rel("left1", 2, cn);
    int64_t lr1[] = {1, 10};
    int64_t lr2[] = {2, 20};
    col_rel_append_row(left1, lr1);
    col_rel_append_row(left1, lr2);

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left1, false);
    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "first join ok");
    eval_entry_t r1 = eval_stack_pop(&stack);
    ASSERT_TRUE(r1.rel->nrows == 1, "first join: only x=1 matches");

    /* Add row to right: x=2 */
    col_rel_t *right_ref = session_find_rel(sess, "right");
    int64_t rr2[] = {2, 200};
    col_rel_append_row(right_ref, rr2);

    /* Second join: right now has 2 rows, arrangement incrementally updated */
    col_rel_t *left2 = make_rel("left2", 2, cn);
    col_rel_append_row(left2, lr1);
    col_rel_append_row(left2, lr2);

    eval_stack_init(&stack);
    eval_stack_push(&stack, left2, false);
    rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "second join ok");
    eval_entry_t r2 = eval_stack_pop(&stack);
    ASSERT_TRUE(r2.rel->nrows == 2, "second join: x=1 and x=2 match");

    free(r1.seg_boundaries);
    if (r1.owned)
        col_rel_destroy(r1.rel);
    free(r2.seg_boundaries);
    if (r2.owned)
        col_rel_destroy(r2.rel);
    col_rel_destroy(left1);
    col_rel_destroy(left2);
    destroy_mock_session(sess);
    PASS;
}

static void
test_result_is_delta_flag(void)
{
    TEST("result is_delta flag propagation");
    wl_col_session_t *sess = make_mock_session();

    const char *cn[] = {"x"};
    col_rel_t *left = make_rel("left", 1, cn);
    int64_t lr[] = {1};
    col_rel_append_row(left, lr);

    col_rel_t *right = make_rel("right", 1, cn);
    int64_t rr[] = {1};
    col_rel_append_row(right, rr);
    session_add_rel(sess, right);

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *keys[] = {"x"};
    op.left_keys = keys;
    op.right_keys = keys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    /* Push left as delta */
    eval_stack_push_delta(&stack, left, false, true);

    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "join should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.is_delta == true, "result should inherit delta flag");

    free(result.seg_boundaries);
    if (result.owned)
        col_rel_destroy(result.rel);
    col_rel_destroy(left);
    destroy_mock_session(sess);
    PASS;
}

static void
test_large_dataset(void)
{
    TEST("large dataset: 100 left x 100 right correctness");
    wl_col_session_t *sess = make_mock_session();

    const char *cn[] = {"x", "y"};
    col_rel_t *left = make_rel("left", 2, cn);
    for (int i = 0; i < 100; i++) {
        int64_t row[] = {i, i * 10};
        col_rel_append_row(left, row);
    }

    col_rel_t *right = make_rel("right", 2, cn);
    for (int i = 50; i < 150; i++) {
        int64_t row[] = {i, i * 100};
        col_rel_append_row(right, row);
    }
    session_add_rel(sess, right);

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *lkeys[] = {"x"};
    char *rkeys[] = {"x"};
    op.left_keys = lkeys;
    op.right_keys = rkeys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "join should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    /* Overlap: keys 50..99 => 50 matches */
    ASSERT_TRUE(result.rel->nrows == 50, "50 matching rows");

    /* Verify a sample row: x=75 */
    int64_t expected[] = {75, 750, 75, 7500};
    ASSERT_TRUE(output_contains_row(result.rel, expected),
        "should contain [75,750,75,7500]");

    free(result.seg_boundaries);
    if (result.owned)
        col_rel_destroy(result.rel);
    col_rel_destroy(left);
    destroy_mock_session(sess);
    PASS;
}

static void
test_diff_operators_active_flag(void)
{
    TEST("diff_operators_active flag in session");
    wl_col_session_t *sess = make_mock_session();

    /* Default should be false */
    ASSERT_TRUE(sess->diff_operators_active == false,
        "default should be false");

    /* Set it and verify */
    sess->diff_operators_active = true;
    ASSERT_TRUE(sess->diff_operators_active == true,
        "should be settable to true");

    destroy_mock_session(sess);
    PASS;
}

static void
test_force_full_mode(void)
{
    TEST("FORCE_FULL mode never substitutes delta");
    wl_col_session_t *sess = make_mock_session();

    const char *cn[] = {"x"};
    col_rel_t *right = make_rel("right", 1, cn);
    int64_t rr[] = {1};
    col_rel_append_row(right, rr);
    session_add_rel(sess, right);

    /* Create a delta relation that should NOT be used */
    col_rel_t *delta = make_rel("$d$right", 1, cn);
    int64_t dr[] = {99};
    col_rel_append_row(delta, dr);
    session_add_rel(sess, delta);

    col_rel_t *left = make_rel("left", 1, cn);
    int64_t lr[] = {1};
    col_rel_append_row(left, lr);

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *keys[] = {"x"};
    op.left_keys = keys;
    op.right_keys = keys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "join should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    /* Should match on x=1 from full right, not x=99 from delta */
    ASSERT_TRUE(result.rel->nrows == 1, "one match from full right");
    ASSERT_TRUE(result.rel->data[0] == 1, "matched full right x=1");

    free(result.seg_boundaries);
    if (result.owned)
        col_rel_destroy(result.rel);
    col_rel_destroy(left);
    destroy_mock_session(sess);
    PASS;
}

static void
test_delta_substitution(void)
{
    TEST("FORCE_DELTA with delta relation");
    wl_col_session_t *sess = make_mock_session();
    sess->current_iteration = 0;
    sess->delta_seeded = false;

    const char *cn[] = {"x"};
    col_rel_t *right = make_rel("right", 1, cn);
    int64_t rr[] = {1};
    col_rel_append_row(right, rr);
    session_add_rel(sess, right);

    col_rel_t *delta = make_rel("$d$right", 1, cn);
    int64_t dr[] = {2};
    col_rel_append_row(delta, dr);
    session_add_rel(sess, delta);

    col_rel_t *left = make_rel("left", 1, cn);
    int64_t lr1[] = {1};
    int64_t lr2[] = {2};
    col_rel_append_row(left, lr1);
    col_rel_append_row(left, lr2);

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *keys[] = {"x"};
    op.left_keys = keys;
    op.right_keys = keys;
    op.delta_mode = WL_DELTA_FORCE_DELTA;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "join should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    /* Delta has x=2, so should match only left x=2 */
    ASSERT_TRUE(result.rel->nrows == 1, "one match from delta");
    ASSERT_TRUE(result.rel->data[0] == 2, "matched delta x=2");
    ASSERT_TRUE(result.is_delta == true, "result is delta");

    free(result.seg_boundaries);
    if (result.owned)
        col_rel_destroy(result.rel);
    col_rel_destroy(left);
    destroy_mock_session(sess);
    PASS;
}

static void
test_single_key_column(void)
{
    TEST("single key column correctness");
    wl_col_session_t *sess = make_mock_session();

    const char *lcn[] = {"k", "a"};
    const char *rcn[] = {"k", "b"};
    col_rel_t *left = make_rel("left", 2, lcn);
    int64_t lr1[] = {10, 1};
    int64_t lr2[] = {20, 2};
    int64_t lr3[] = {30, 3};
    col_rel_append_row(left, lr1);
    col_rel_append_row(left, lr2);
    col_rel_append_row(left, lr3);

    col_rel_t *right = make_rel("right", 2, rcn);
    int64_t rr1[] = {20, 200};
    int64_t rr2[] = {30, 300};
    int64_t rr3[] = {40, 400};
    col_rel_append_row(right, rr1);
    col_rel_append_row(right, rr2);
    col_rel_append_row(right, rr3);
    session_add_rel(sess, right);

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *lkeys[] = {"k"};
    char *rkeys[] = {"k"};
    op.left_keys = lkeys;
    op.right_keys = rkeys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "join should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT_TRUE(result.rel->nrows == 2, "keys 20 and 30 match");

    int64_t exp1[] = {20, 2, 20, 200};
    int64_t exp2[] = {30, 3, 30, 300};
    ASSERT_TRUE(output_contains_row(result.rel, exp1), "row [20,2,20,200]");
    ASSERT_TRUE(output_contains_row(result.rel, exp2), "row [30,3,30,300]");

    free(result.seg_boundaries);
    if (result.owned)
        col_rel_destroy(result.rel);
    col_rel_destroy(left);
    destroy_mock_session(sess);
    PASS;
}

static void
test_diff_arr_entry_cleanup(void)
{
    TEST("diff arrangement entries cleaned up on session destroy");
    wl_col_session_t *sess = make_mock_session();

    const char *cn[] = {"x"};
    col_rel_t *right = make_rel("right", 1, cn);
    int64_t rr[] = {1};
    col_rel_append_row(right, rr);
    session_add_rel(sess, right);

    col_rel_t *left = make_rel("left", 1, cn);
    int64_t lr[] = {1};
    col_rel_append_row(left, lr);

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *keys[] = {"x"};
    op.left_keys = keys;
    op.right_keys = keys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);
    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "join ok");
    eval_entry_t r = eval_stack_pop(&stack);

    ASSERT_TRUE(sess->diff_arr_count == 1, "1 diff arrangement");

    free(r.seg_boundaries);
    if (r.owned)
        col_rel_destroy(r.rel);
    col_rel_destroy(left);
    /* destroy_mock_session should free diff arrangements without leak */
    destroy_mock_session(sess);
    PASS;
}

/* ========================================================================
 * MAIN
 * ======================================================================== */

int
main(void)
{
    printf("=== Differential Join Tests (Issue #263) ===\n\n");

    test_basic_join_correctness();
    test_empty_left();
    test_empty_right();
    test_no_matching_keys();
    test_all_rows_match();
    test_multi_key_columns();
    test_duplicate_keys_in_right();
    test_single_key_column();
    test_arrangement_reuse();
    test_arrangement_incremental();
    test_result_is_delta_flag();
    test_large_dataset();
    test_diff_operators_active_flag();
    test_force_full_mode();
    test_delta_substitution();
    test_diff_arr_entry_cleanup();

    printf("\n=== Results: %d/%d passed ===\n",
        tests_passed, tests_passed + tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
