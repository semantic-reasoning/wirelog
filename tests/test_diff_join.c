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
#include <errno.h>

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

static col_rel_t *
make_rel(const char *name, uint32_t ncols, const char *const *col_names);

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
    wl_workqueue_destroy(s->wq);
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
    col_mat_cache_clear(&s->mat_cache);
    session_rel_free_hash(s);
    delta_pool_destroy(s->delta_pool);
    free(s);
}

static col_rel_t *
make_large_left_rel(void)
{
    const char *cn[] = {"k", "v"};
    col_rel_t *left = make_rel("left", 2, cn);
    if (!left)
        return NULL;
    for (int64_t i = 0; i < 20000; i++) {
        int64_t row[] = {i % 8, i};
        if (col_rel_append_row(left, row) != 0) {
            col_rel_destroy(left);
            return NULL;
        }
    }
    return left;
}

static col_rel_t *
make_large_right_rel(void)
{
    const char *cn[] = {"k", "r"};
    col_rel_t *right = make_rel("right", 2, cn);
    if (!right)
        return NULL;
    for (int64_t k = 0; k < 8; k++) {
        int64_t row1[] = {k, 100 + k};
        int64_t row2[] = {k, 200 + k};
        if (col_rel_append_row(right, row1) != 0
            || col_rel_append_row(right, row2) != 0) {
            col_rel_destroy(right);
            return NULL;
        }
    }
    return right;
}

static col_rel_t *
run_standard_join(wl_col_session_t *sess, col_rel_t *left)
{
    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *lkeys[] = {"k"};
    char *rkeys[] = {"k"};
    op.left_keys = (const char *const *)lkeys;
    op.right_keys = (const char *const *)rkeys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join(&op, &stack, sess);
    if (rc != 0)
        return NULL;
    eval_entry_t result = eval_stack_pop(&stack);
    if (!result.owned)
        return NULL;
    return result.rel;
}

static col_rel_t *
run_projected_join(wl_col_session_t *sess, col_rel_t *left, bool diff)
{
    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *lkeys[] = {"k"};
    char *rkeys[] = {"k"};
    uint32_t project_indices[] = {1, 3};
    op.left_keys = (const char *const *)lkeys;
    op.right_keys = (const char *const *)rkeys;
    op.project_indices = project_indices;
    op.project_count = 2;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = diff ? col_op_join_diff(&op, &stack, sess)
                  : col_op_join(&op, &stack, sess);
    if (rc != 0)
        return NULL;
    eval_entry_t result = eval_stack_pop(&stack);
    if (!result.owned)
        return NULL;
    return result.rel;
}

static col_rel_t *
run_projected_join_with_indices(wl_col_session_t *sess, col_rel_t *left,
    const uint32_t *project_indices, uint32_t project_count, bool materialized)
{
    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *lkeys[] = {"k"};
    char *rkeys[] = {"k"};
    op.left_keys = (const char *const *)lkeys;
    op.right_keys = (const char *const *)rkeys;
    op.project_indices = project_indices;
    op.project_count = project_count;
    op.delta_mode = WL_DELTA_FORCE_FULL;
    op.materialized = materialized;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join(&op, &stack, sess);
    if (rc != 0)
        return NULL;
    eval_entry_t result = eval_stack_pop(&stack);
    if (!result.owned)
        return NULL;
    return result.rel;
}

static col_rel_t *
run_projected_unary_join(wl_col_session_t *sess, col_rel_t *left)
{
    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *lkeys[] = {"k"};
    char *rkeys[] = {"k"};
    uint32_t project_indices[] = {1, 2};
    op.left_keys = (const char *const *)lkeys;
    op.right_keys = (const char *const *)rkeys;
    op.project_indices = project_indices;
    op.project_count = 2;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join(&op, &stack, sess);
    if (rc != 0)
        return NULL;
    eval_entry_t result = eval_stack_pop(&stack);
    if (!result.owned)
        return NULL;
    return result.rel;
}

static col_rel_t *
run_cross_join(wl_col_session_t *sess, col_rel_t *left)
{
    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 0;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join(&op, &stack, sess);
    if (rc != 0)
        return NULL;
    eval_entry_t result = eval_stack_pop(&stack);
    if (!result.owned)
        return NULL;
    return result.rel;
}

static col_rel_t *
make_project_left_rel(void)
{
    const char *cn[] = {"k", "v"};
    col_rel_t *left = make_rel("left", 2, cn);
    if (!left)
        return NULL;
    int64_t row1[] = {1, 10};
    int64_t row2[] = {2, 20};
    if (col_rel_append_row(left, row1) != 0
        || col_rel_append_row(left, row2) != 0) {
        col_rel_destroy(left);
        return NULL;
    }
    return left;
}

static col_rel_t *
make_project_right_rel(void)
{
    const char *cn[] = {"k", "r"};
    col_rel_t *right = make_rel("right", 2, cn);
    if (!right)
        return NULL;
    int64_t row1[] = {1, 100};
    int64_t row2[] = {2, 200};
    if (col_rel_append_row(right, row1) != 0
        || col_rel_append_row(right, row2) != 0) {
        col_rel_destroy(right);
        return NULL;
    }
    return right;
}

static col_rel_t *
make_project_unary_right_rel(void)
{
    const char *cn[] = {"k"};
    col_rel_t *right = make_rel("right", 1, cn);
    if (!right)
        return NULL;
    int64_t row1[] = {1};
    int64_t row2[] = {2};
    if (col_rel_append_row(right, row1) != 0
        || col_rel_append_row(right, row2) != 0) {
        col_rel_destroy(right);
        return NULL;
    }
    return right;
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

static bool
relation_contents_equal(const col_rel_t *left, const col_rel_t *right)
{
    if (!left || !right || left->nrows != right->nrows
        || left->ncols != right->ncols)
        return false;
    for (uint32_t row = 0; row < left->nrows; row++) {
        for (uint32_t col = 0; col < left->ncols; col++) {
            if (col_rel_get(left, row, col) != col_rel_get(right, row, col))
                return false;
        }
    }
    return true;
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
    op.left_keys = (const char *const *)lkeys;
    op.right_keys = (const char *const *)rkeys;
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
    ASSERT_TRUE(col_rel_get(result.rel, 0, 0) == 1, "left x=1");
    ASSERT_TRUE(col_rel_get(result.rel, 0, 1) == 10, "left y=10");
    ASSERT_TRUE(col_rel_get(result.rel, 0, 2) == 1, "right x=1");
    ASSERT_TRUE(col_rel_get(result.rel, 0, 3) == 100, "right y=100");

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
    op.left_keys = (const char *const *)keys;
    op.right_keys = (const char *const *)keys;
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
    op.left_keys = (const char *const *)keys;
    op.right_keys = (const char *const *)keys;
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
    op.left_keys = (const char *const *)lkeys;
    op.right_keys = (const char *const *)rkeys;
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
    op.left_keys = (const char *const *)lkeys;
    op.right_keys = (const char *const *)rkeys;
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
    op.left_keys = (const char *const *)lkeys;
    op.right_keys = (const char *const *)rkeys;
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
    op.left_keys = (const char *const *)lkeys;
    op.right_keys = (const char *const *)rkeys;
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
    op.left_keys = (const char *const *)lkeys;
    op.right_keys = (const char *const *)rkeys;
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
    op.left_keys = (const char *const *)lkeys;
    op.right_keys = (const char *const *)rkeys;
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

static bool
diff_arrangement_links_in_bounds(const col_diff_arrangement_t *arr)
{
    for (uint32_t bucket = 0; bucket < arr->nbuckets; bucket++) {
        uint32_t traversed = 0;
        for (uint32_t entry = arr->ht_head[bucket]; entry != 0;
            entry = arr->ht_next[entry - 1]) {
            if (entry > arr->indexed_rows || traversed++ >= arr->indexed_rows)
                return false;
        }
    }
    return true;
}

static uint64_t
diff_arrangement_ledger_bytes(const wl_col_session_t *sess)
{
    wl_mem_ledger_snapshot_t snapshot;
    wl_mem_ledger_snapshot(&sess->mem_ledger, &snapshot);
    return snapshot.subsys_bytes[WL_MEM_SUBSYS_ARRANGEMENT];
}

static void
test_diff_arrangement_txn_ledger_accounting(void)
{
    TEST("diff arrangement transaction ledger accounting");
    wl_col_session_t *sess = make_mock_session();
    const char *cn[] = {"x"};
    col_rel_t *right = make_rel("right", 1, cn);
    int64_t row[] = {1};
    uint32_t key_col = 0;
    ASSERT_TRUE(right && col_rel_append_row(right, row) == 0,
        "right relation allocated");
    ASSERT_TRUE(session_add_rel(sess, right) == 0, "right registered");

    wl_columnar_arrangement_diff_txn_t txn = {0};
    ASSERT_TRUE(wl_columnar_arrangement_diff_txn_begin(sess, "right", right,
        &key_col, 1, &txn) == 0, "pending transaction begins");
    uint64_t working_bytes = col_diff_arrangement_bytes(txn.working);
    ASSERT_TRUE(txn.working->ledger == &sess->mem_ledger,
        "pending working copy is ledger-attached");
    ASSERT_TRUE(sess->diff_arr_count == 0,
        "pending arrangement remains unpublished");
    ASSERT_TRUE(diff_arrangement_ledger_bytes(sess) == working_bytes,
        "pending working copy is charged");
    wl_columnar_arrangement_diff_txn_abort(&txn);
    ASSERT_TRUE(sess->diff_arr_count == 0,
        "pending abort leaves no cache entry");
    ASSERT_TRUE(diff_arrangement_ledger_bytes(sess) == 0,
        "pending abort credits working copy");

    ASSERT_TRUE(wl_columnar_arrangement_diff_txn_begin(sess, "right", right,
        &key_col, 1, &txn) == 0, "pending transaction restarts");
    working_bytes = col_diff_arrangement_bytes(txn.working);
    wl_columnar_arrangement_diff_txn_commit(&txn);
    ASSERT_TRUE(sess->diff_arr_count == 1, "pending commit publishes entry");
    ASSERT_TRUE(diff_arrangement_ledger_bytes(sess) == working_bytes,
        "pending commit does not double-charge");

    uint64_t persistent_bytes = diff_arrangement_ledger_bytes(sess);
    ASSERT_TRUE(wl_columnar_arrangement_diff_txn_begin(sess, "right", right,
        &key_col, 1, &txn) == 0, "existing transaction begins");
    working_bytes = col_diff_arrangement_bytes(txn.working);
    ASSERT_TRUE(diff_arrangement_ledger_bytes(sess)
        == persistent_bytes + working_bytes,
        "existing working copy is additionally charged");
    wl_columnar_arrangement_diff_txn_abort(&txn);
    ASSERT_TRUE(diff_arrangement_ledger_bytes(sess) == persistent_bytes,
        "existing abort credits working copy");

    destroy_mock_session(sess);
    PASS;
}

static void
test_late_abort_does_not_advance_arrangement(void)
{
    TEST("late abort preserves persistent diff arrangement");
    wl_col_session_t *sess = make_mock_session();
    const char *cn[] = {"x", "y"};
    col_rel_t *right = make_rel("right", 2, cn);
    int64_t right1[] = {1, 100};
    ASSERT_TRUE(right && col_rel_append_row(right, right1) == 0,
        "initial right row");
    ASSERT_TRUE(session_add_rel(sess, right) == 0, "right registered");

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *lkeys[] = {"x"};
    char *rkeys[] = {"x"};
    op.left_keys = (const char *const *)lkeys;
    op.right_keys = (const char *const *)rkeys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    col_rel_t *left1 = make_rel("left1", 2, cn);
    int64_t left_row1[] = {1, 10};
    ASSERT_TRUE(left1 && col_rel_append_row(left1, left_row1) == 0,
        "initial left row");
    eval_stack_t stack;
    eval_stack_init(&stack);
    ASSERT_TRUE(eval_stack_push(&stack, left1, false) == 0,
        "initial input pushed");
    ASSERT_TRUE(col_op_join_diff(&op, &stack, sess) == 0,
        "initial join committed");
    eval_entry_t first = eval_stack_pop(&stack);
    ASSERT_TRUE(sess->diff_arr_count == 1, "arrangement created");

    col_diff_arrangement_t *before = sess->diff_arr_entries[0].diff_arr;
    col_relation_snapshot_t snapshot_before = before->source_snapshot;
    uint32_t indexed_before = before->indexed_rows;
    uint32_t current_before = before->current_nrows;
    uint32_t base_before = before->base_nrows;
    uint32_t *heads_before = malloc(
        (size_t)before->nbuckets * sizeof(*heads_before));
    uint32_t *next_before = malloc(
        (size_t)before->ht_cap * sizeof(*next_before));
    ASSERT_TRUE(heads_before && next_before, "arrangement snapshot allocated");
    memcpy(heads_before, before->ht_head,
        (size_t)before->nbuckets * sizeof(*heads_before));
    memcpy(next_before, before->ht_next,
        (size_t)before->ht_cap * sizeof(*next_before));

    int64_t right2[] = {2, 200};
    ASSERT_TRUE(col_rel_append_row(right, right2) == 0, "right row appended");
    col_rel_t *left2 = make_rel("left2", 2, cn);
    int64_t left_row2[] = {2, 20};
    ASSERT_TRUE(left2 && col_rel_append_row(left2, left_row2) == 0,
        "retry left row");
    sess->join_output_limit = 1;
    eval_stack_init(&stack);
    ASSERT_TRUE(eval_stack_push(&stack, left2, false) == 0,
        "failing input pushed");
    ASSERT_TRUE(col_op_join_diff(&op, &stack, sess) == EOVERFLOW,
        "late output-limit abort reported");

    col_diff_arrangement_t *after_abort
        = sess->diff_arr_entries[0].diff_arr;
    ASSERT_TRUE(after_abort == before, "persistent arrangement not replaced");
    ASSERT_TRUE(after_abort->indexed_rows == indexed_before
        && after_abort->current_nrows == current_before
        && after_abort->base_nrows == base_before,
        "persistent index bounds restored");
    ASSERT_TRUE(wl_columnar_relation_snapshot_equal(
            after_abort->source_snapshot, snapshot_before),
        "persistent source snapshot restored");
    ASSERT_TRUE(memcmp(after_abort->ht_head, heads_before,
        (size_t)after_abort->nbuckets * sizeof(*heads_before)) == 0
        && memcmp(after_abort->ht_next, next_before,
        (size_t)after_abort->ht_cap * sizeof(*next_before)) == 0,
        "persistent hash chains restored");
    ASSERT_TRUE(diff_arrangement_links_in_bounds(after_abort),
        "aborted arrangement links remain in bounds");

    sess->join_output_limit = 0;
    eval_stack_init(&stack);
    ASSERT_TRUE(eval_stack_push(&stack, left2, false) == 0,
        "retry input pushed");
    ASSERT_TRUE(col_op_join_diff(&op, &stack, sess) == 0, "retry succeeds");
    eval_entry_t retry = eval_stack_pop(&stack);
    ASSERT_TRUE(retry.rel->nrows == 1, "retry returns appended match");
    col_diff_arrangement_t *committed = sess->diff_arr_entries[0].diff_arr;
    ASSERT_TRUE(committed->indexed_rows == right->nrows
        && committed->current_nrows == right->nrows,
        "retry commits complete index bounds");
    ASSERT_TRUE(wl_columnar_relation_snapshot_equal(
            committed->source_snapshot, wl_columnar_relation_snapshot(right)),
        "retry commits current source snapshot");
    ASSERT_TRUE(diff_arrangement_links_in_bounds(committed),
        "committed arrangement links remain in bounds");

    free(heads_before);
    free(next_before);
    eval_entry_dispose(&first);
    eval_entry_dispose(&retry);
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
    op.left_keys = (const char *const *)keys;
    op.right_keys = (const char *const *)keys;
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
    op.left_keys = (const char *const *)lkeys;
    op.right_keys = (const char *const *)rkeys;
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
    op.left_keys = (const char *const *)keys;
    op.right_keys = (const char *const *)keys;
    op.delta_mode = WL_DELTA_FORCE_FULL;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "join should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    /* Should match on x=1 from full right, not x=99 from delta */
    ASSERT_TRUE(result.rel->nrows == 1, "one match from full right");
    ASSERT_TRUE(col_rel_get(result.rel, 0, 0) == 1, "matched full right x=1");

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
    op.left_keys = (const char *const *)keys;
    op.right_keys = (const char *const *)keys;
    op.delta_mode = WL_DELTA_FORCE_DELTA;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);

    int rc = col_op_join_diff(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "join should succeed");

    eval_entry_t result = eval_stack_pop(&stack);
    /* Delta has x=2, so should match only left x=2 */
    ASSERT_TRUE(result.rel->nrows == 1, "one match from delta");
    ASSERT_TRUE(col_rel_get(result.rel, 0, 0) == 2, "matched delta x=2");
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
    op.left_keys = (const char *const *)lkeys;
    op.right_keys = (const char *const *)rkeys;
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
    op.left_keys = (const char *const *)keys;
    op.right_keys = (const char *const *)keys;
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

static void
test_projected_join_emits_projected_columns(void)
{
    TEST("projected join emits projected columns");

    wl_col_session_t *sess = make_mock_session();
    col_rel_t *left = make_project_left_rel();
    col_rel_t *right = make_project_right_rel();
    ASSERT_TRUE(left && right, "relations allocated");
    ASSERT_TRUE(session_add_rel(sess, right) == 0, "right registered");

    col_rel_t *out = run_projected_join(sess, left, false);
    ASSERT_TRUE(out != NULL, "projected join output");
    ASSERT_TRUE(out->ncols == 2, "projected column count");
    ASSERT_TRUE(out->nrows == 2, "projected row count");
    int64_t expected1[] = {10, 100};
    int64_t expected2[] = {20, 200};
    ASSERT_TRUE(output_contains_row(out, expected1), "contains first row");
    ASSERT_TRUE(output_contains_row(out, expected2), "contains second row");

    col_rel_destroy(out);
    col_rel_destroy(left);
    destroy_mock_session(sess);
    PASS;
}

static void
test_projected_diff_join_emits_projected_columns(void)
{
    TEST("projected diff join emits projected columns");

    wl_col_session_t *sess = make_mock_session();
    col_rel_t *left = make_project_left_rel();
    col_rel_t *right = make_project_right_rel();
    ASSERT_TRUE(left && right, "relations allocated");
    ASSERT_TRUE(session_add_rel(sess, right) == 0, "right registered");

    col_rel_t *out = run_projected_join(sess, left, true);
    ASSERT_TRUE(out != NULL, "projected diff join output");
    ASSERT_TRUE(out->ncols == 2, "projected column count");
    ASSERT_TRUE(out->nrows == 2, "projected row count");
    int64_t expected1[] = {10, 100};
    int64_t expected2[] = {20, 200};
    ASSERT_TRUE(output_contains_row(out, expected1), "contains first row");
    ASSERT_TRUE(output_contains_row(out, expected2), "contains second row");

    col_rel_destroy(out);
    col_rel_destroy(left);
    destroy_mock_session(sess);
    PASS;
}

static void
test_projected_unary_join_emits_projected_columns(void)
{
    TEST("projected unary join emits projected columns");

    wl_col_session_t *sess = make_mock_session();
    col_rel_t *left = make_project_left_rel();
    col_rel_t *right = make_project_unary_right_rel();
    ASSERT_TRUE(left && right, "relations allocated");
    ASSERT_TRUE(session_add_rel(sess, right) == 0, "right registered");

    col_rel_t *out = run_projected_unary_join(sess, left);
    ASSERT_TRUE(out != NULL, "projected unary join output");
    ASSERT_TRUE(out->ncols == 2, "projected column count");
    ASSERT_TRUE(out->nrows == 2, "projected row count");
    int64_t expected1[] = {10, 1};
    int64_t expected2[] = {20, 2};
    ASSERT_TRUE(output_contains_row(out, expected1), "contains first row");
    ASSERT_TRUE(output_contains_row(out, expected2), "contains second row");

    col_rel_destroy(out);
    col_rel_destroy(left);
    destroy_mock_session(sess);
    PASS;
}

static void
test_projected_materialized_join_does_not_reuse_wrong_shape(void)
{
    TEST("projected materialized join does not reuse wrong shape");

    wl_col_session_t *sess = make_mock_session();
    col_rel_t *left1 = make_project_left_rel();
    col_rel_t *left2 = make_project_left_rel();
    col_rel_t *right = make_project_right_rel();
    ASSERT_TRUE(left1 && left2 && right, "relations allocated");
    ASSERT_TRUE(session_add_rel(sess, right) == 0, "right registered");

    uint32_t first_projection[] = {1, 3};
    col_rel_t *out1 = run_projected_join_with_indices(sess, left1,
            first_projection, 2, true);
    ASSERT_TRUE(out1 != NULL, "first projected materialized join output");
    ASSERT_TRUE(out1->ncols == 2, "first projected column count");

    uint32_t second_projection[] = {3};
    col_rel_t *out2 = run_projected_join_with_indices(sess, left2,
            second_projection, 1, true);
    ASSERT_TRUE(out2 != NULL, "second projected materialized join output");
    ASSERT_TRUE(out2->ncols == 1, "second projected column count");
    ASSERT_TRUE(out2->nrows == 2, "second projected row count");
    int64_t expected1[] = {100};
    int64_t expected2[] = {200};
    ASSERT_TRUE(output_contains_row(out2, expected1), "contains first row");
    ASSERT_TRUE(output_contains_row(out2, expected2), "contains second row");

    col_rel_destroy(out1);
    col_rel_destroy(out2);
    col_rel_destroy(left1);
    col_rel_destroy(left2);
    destroy_mock_session(sess);
    PASS;
}

static void
test_materialized_join_cleans_owned_left(void)
{
    TEST("materialized join cleans owned left");

    wl_col_session_t *sess = make_mock_session();
    col_rel_t *left = make_project_left_rel();
    col_rel_t *right = make_project_right_rel();
    ASSERT_TRUE(left && right, "relations allocated");
    ASSERT_TRUE(session_add_rel(sess, right) == 0, "right registered");

    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 1;
    char *lkeys[] = {"k"};
    char *rkeys[] = {"k"};
    op.left_keys = (const char *const *)lkeys;
    op.right_keys = (const char *const *)rkeys;
    op.delta_mode = WL_DELTA_FORCE_FULL;
    op.materialized = true;

    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, true);
    int rc = col_op_join(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "materialized join output");
    eval_entry_t out = eval_stack_pop(&stack);
    ASSERT_TRUE(out.rel != NULL, "result relation exists");
    ASSERT_TRUE(out.owned, "materialized result is an owned deep copy");
    ASSERT_TRUE(out.rel->nrows == 2, "materialized row count");

    col_rel_t *left_hit = make_project_left_rel();
    ASSERT_TRUE(left_hit != NULL, "cache-hit left allocated");
    eval_stack_init(&stack);
    eval_stack_push(&stack, left_hit, true);
    rc = col_op_join(&op, &stack, sess);
    ASSERT_TRUE(rc == 0, "materialized cache hit output");
    eval_entry_t hit = eval_stack_pop(&stack);
    ASSERT_TRUE(hit.owned, "cache-hit result is an owned deep copy");
    ASSERT_TRUE(hit.rel != out.rel,
        "cache hit does not alias the first returned copy");
    ASSERT_TRUE(relation_contents_equal(out.rel, hit.rel),
        "cache-hit contents match the first returned copy");
    ASSERT_TRUE(hit.rel->nrows == 2, "cache-hit row count");

    /* Both returned copies are caller-owned; the cached original remains
     * owned by the session materialization cache until session teardown. */
    col_rel_destroy(out.rel);
    col_rel_destroy(hit.rel);
    destroy_mock_session(sess);
    PASS;
}

static void
test_direct_standard_join_matches_serial_with_workers(void)
{
    TEST("direct standard join matches serial output with workers");

    wl_col_session_t *serial = make_mock_session();
    wl_col_session_t *parallel = make_mock_session();
    parallel->num_workers = 4;
    parallel->wq = wl_workqueue_create(parallel->num_workers);
    ASSERT_TRUE(parallel->wq != NULL, "workqueue create");

    col_rel_t *serial_left = make_large_left_rel();
    col_rel_t *parallel_left = make_large_left_rel();
    col_rel_t *serial_right = make_large_right_rel();
    col_rel_t *parallel_right = make_large_right_rel();
    ASSERT_TRUE(serial_left && parallel_left && serial_right && parallel_right,
        "relations allocated");
    ASSERT_TRUE(session_add_rel(serial, serial_right) == 0,
        "serial right registered");
    ASSERT_TRUE(session_add_rel(parallel, parallel_right) == 0,
        "parallel right registered");

    col_rel_t *serial_out = run_standard_join(serial, serial_left);
    col_rel_t *parallel_out = run_standard_join(parallel, parallel_left);
    ASSERT_TRUE(serial_out && parallel_out, "join outputs");
    ASSERT_TRUE(serial_out->nrows == parallel_out->nrows, "row count");
    ASSERT_TRUE(serial_out->ncols == parallel_out->ncols, "column count");
    for (uint32_t r = 0; r < serial_out->nrows; r++) {
        for (uint32_t c = 0; c < serial_out->ncols; c++) {
            ASSERT_TRUE(col_rel_get(serial_out, r, c)
                == col_rel_get(parallel_out, r, c),
                "worker-enabled output preserves deterministic row order");
        }
    }

    col_rel_destroy(serial_out);
    col_rel_destroy(parallel_out);
    col_rel_destroy(serial_left);
    col_rel_destroy(parallel_left);
    destroy_mock_session(serial);
    destroy_mock_session(parallel);
    PASS;
}

static void
test_parallel_cross_join_matches_serial(void)
{
    TEST("parallel cross join matches serial output");

    wl_col_session_t *serial = make_mock_session();
    wl_col_session_t *parallel = make_mock_session();
    parallel->num_workers = 4;
    parallel->wq = wl_workqueue_create(parallel->num_workers);
    ASSERT_TRUE(parallel->wq != NULL, "workqueue create");

    col_rel_t *serial_left = make_large_left_rel();
    col_rel_t *parallel_left = make_large_left_rel();
    col_rel_t *serial_right = make_large_right_rel();
    col_rel_t *parallel_right = make_large_right_rel();
    ASSERT_TRUE(serial_left && parallel_left && serial_right && parallel_right,
        "relations allocated");
    ASSERT_TRUE(session_add_rel(serial, serial_right) == 0,
        "serial right registered");
    ASSERT_TRUE(session_add_rel(parallel, parallel_right) == 0,
        "parallel right registered");

    col_rel_t *serial_out = run_cross_join(serial, serial_left);
    col_rel_t *parallel_out = run_cross_join(parallel, parallel_left);
    ASSERT_TRUE(serial_out && parallel_out, "join outputs");
    ASSERT_TRUE(serial_out->nrows == parallel_out->nrows, "row count");
    ASSERT_TRUE(serial_out->ncols == parallel_out->ncols, "column count");
    for (uint32_t r = 0; r < serial_out->nrows; r++) {
        for (uint32_t c = 0; c < serial_out->ncols; c++) {
            ASSERT_TRUE(col_rel_get(serial_out, r, c)
                == col_rel_get(parallel_out, r, c),
                "parallel cross output preserves deterministic row order");
        }
    }

    col_rel_destroy(serial_out);
    col_rel_destroy(parallel_out);
    col_rel_destroy(serial_left);
    col_rel_destroy(parallel_left);
    destroy_mock_session(serial);
    destroy_mock_session(parallel);
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
    test_diff_arrangement_txn_ledger_accounting();
    test_late_abort_does_not_advance_arrangement();
    test_result_is_delta_flag();
    test_large_dataset();
    test_diff_operators_active_flag();
    test_force_full_mode();
    test_delta_substitution();
    test_diff_arr_entry_cleanup();
    test_projected_join_emits_projected_columns();
    test_projected_diff_join_emits_projected_columns();
    test_projected_unary_join_emits_projected_columns();
    test_projected_materialized_join_does_not_reuse_wrong_shape();
    test_materialized_join_cleans_owned_left();
    test_direct_standard_join_matches_serial_with_workers();
    test_parallel_cross_join_matches_serial();

    printf("\n=== Results: %d/%d passed ===\n",
        tests_passed, tests_passed + tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
