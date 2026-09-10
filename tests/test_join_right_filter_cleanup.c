/*
 * test_join_right_filter_cleanup.c - Owned right-filter cleanup on early
 * exits of the join operators (Issue #1505)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * When a join applies its right-side constant filter outside the session
 * cache (delta substitution, no relation name, or an unavailable cache),
 * the filtered relation is pool-allocated and owned by the operator in
 * right_filtered.  Every exit after that point must destroy it: the pool
 * only rewinds its slot counter on reset, so a slot whose column buffers
 * were never freed is a heap leak.
 *
 * The tests drive the operators directly on a mock session and force the
 * owned path with WL_DELTA_FORCE_DELTA plus a populated $d$right.  Two
 * early exits are deterministic without allocation injection:
 *   - the ledger backpressure return (a non-error exit that pushes the
 *     empty output and returns 0), reached by pre-setting the RELATION
 *     gauge above the 80% threshold;
 *   - the key-type EINVAL return, reached with a FLOAT left key against an
 *     INT64 right key.
 * The leak check is independent of the sanitizer: the filtered relation
 * occupies the first pool slot taken by the call, and col_rel_destroy on
 * a pool relation zeroes its slot, so a leaked slot still carries its
 * name and column pointers after the call.  Under ASan/LSan the leak is
 * additionally reported at exit.
 */

#include "../wirelog/columnar/internal.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                                          \
        do {                                                    \
            tests_run++;                                        \
            printf("  [%d] %s ... ", tests_run, (name));         \
        } while (0)
#define PASS()            \
        do {                  \
            tests_passed++;   \
            printf("PASS\n"); \
        } while (0)
#define FAIL(msg)                    \
        do {                             \
            tests_failed++;              \
            printf("FAIL: %s\n", (msg)); \
        } while (0)

/* ------------------------------------------------------------------------
 * Fixture (mirrors tests/test_diff_join.c)
 * ------------------------------------------------------------------------ */

static wl_col_session_t *
make_mock_session(void)
{
    wl_col_session_t *s = calloc(1, sizeof(*s));
    if (!s)
        return NULL;
    s->frontier_ops = &col_frontier_epoch_ops;
    s->delta_pool = delta_pool_create(256, sizeof(col_rel_t),
            (size_t)1024 * 1024);
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
    free((void *)s->rels);
    for (uint32_t i = 0; i < s->arr_count; i++) {
        free(s->arr_entries[i].rel_name);
        free(s->arr_entries[i].key_cols);
        arr_free_contents(&s->arr_entries[i].arr);
    }
    free(s->arr_entries);
    col_session_free_diff_arrangements(s);
    /* The owned path builds a delta arrangement over the filtered delta
     * (col_session_get_delta_arrangement); free its registry as the real
     * session teardown does. */
    col_session_free_delta_arrangements(s);
    col_mat_cache_clear(&s->mat_cache);
    session_rel_free_hash(s);
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

/* right(k, r) with rows (k, 100 + k) and (k, 200 + k) for k < 4, registered
 * as both "right" and "$d$right" so FORCE_DELTA substitutes the delta and
 * the filter takes the owned (pool) path. */
static int
register_right(wl_col_session_t *sess)
{
    const char *cn[] = { "k", "r" };
    const char *names[] = { "right", "$d$right" };
    for (int n = 0; n < 2; n++) {
        col_rel_t *right = make_rel(names[n], 2, cn);
        if (!right)
            return -1;
        for (int64_t k = 0; k < 4; k++) {
            int64_t row1[] = { k, 100 + k };
            int64_t row2[] = { k, 200 + k };
            if (col_rel_append_row(right, row1) != 0
                || col_rel_append_row(right, row2) != 0) {
                col_rel_destroy(right);
                return -1;
            }
        }
        if (session_add_rel(sess, right) != 0) {
            col_rel_destroy(right);
            return -1;
        }
    }
    return 0;
}

/* left(k, v); when float_key is set the key column is typed FLOAT before
 * any row is appended (col_rel_set_column_types refuses a FLOAT type on a
 * populated untyped relation), which makes the join's key-type check fail
 * against the INT64 right key. */
static col_rel_t *
make_left(bool float_key)
{
    const char *cn[] = { "k", "v" };
    col_rel_t *left = make_rel("left", 2, cn);
    if (!left)
        return NULL;
    if (float_key) {
        wirelog_column_type_t types[] = { WIRELOG_TYPE_FLOAT,
                                          WIRELOG_TYPE_INT64 };
        if (col_rel_set_column_types(left, types, 2) != 0) {
            col_rel_destroy(left);
            return NULL;
        }
    }
    for (int64_t k = 0; k < 4; k++) {
        int64_t row[] = { k, 10 + k };
        if (col_rel_append_row(left, row) != 0) {
            col_rel_destroy(left);
            return NULL;
        }
    }
    return left;
}

/* VAR("col1") CONST_INT(150) CMP_GT: col1 is positional (column 1 = r), so
 * the filter keeps the (k, 200 + k) rows.  A simple compare needs no
 * intern table. */
static uint8_t filter_bytes[] = {
    WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
    WL_PLAN_EXPR_CONST_INT, 150, 0, 0, 0, 0, 0, 0, 0,
    WL_PLAN_EXPR_CMP_GT
};

static void
init_op(wl_plan_op_t *op, wl_plan_op_type_t type)
{
    static const char *const lkeys[] = { "k" };
    static const char *const rkeys[] = { "k" };
    memset(op, 0, sizeof(*op));
    op->op = type;
    op->right_relation = "right";
    op->key_count = 1;
    op->left_keys = lkeys;
    op->right_keys = rkeys;
    op->delta_mode = WL_DELTA_FORCE_DELTA;
    op->right_filter_expr.data = filter_bytes;
    op->right_filter_expr.size = sizeof(filter_bytes);
}

typedef int (*op_fn_t)(const wl_plan_op_t *, eval_stack_t *,
    wl_col_session_t *);

/* Pool slot `index` as the relation struct it holds. */
static const col_rel_t *
pool_slot(const wl_col_session_t *sess, uint32_t index)
{
    return (const col_rel_t *)(sess->delta_pool->slab
           + (size_t)index * sess->delta_pool->slot_size);
}

/* The filtered relation is the first pool slot the call takes.  After the
 * call it must be destroyed: a destroyed pool relation is zeroed in place,
 * a leaked one still carries its buffers.  `expected_slots` pins the number
 * of slots the call may take, so a heap fallback (which does not take a
 * slot) cannot make the walk vacuous. */
static int
filter_slot_released(const wl_col_session_t *sess, uint32_t before,
    uint32_t expected_slots, const char *label)
{
    const delta_pool_t *pool = sess->delta_pool;
    if (pool->slot_used != before + expected_slots) {
        printf("(%s: expected %u pool slots taken, got %u) ", label,
            expected_slots, pool->slot_used - before);
        return 0;
    }
    const col_rel_t *slot = pool_slot(sess, before);
    if (slot->columns || slot->name || slot->col_names) {
        printf("(%s: filtered relation slot %u still holds %s) ", label,
            before, slot->name ? slot->name : "buffers");
        return 0;
    }
    return 1;
}

/* Drive `fn` once with the owned filter path and `float_key`, returning the
 * operator's rc; *out_rel receives a pushed owned relation (or NULL). */
static int
run_op(wl_col_session_t *sess, op_fn_t fn, wl_plan_op_type_t type,
    bool float_key, col_rel_t **out_rel)
{
    *out_rel = NULL;
    col_rel_t *left = make_left(float_key);
    if (!left)
        return -1;
    wl_plan_op_t op;
    init_op(&op, type);
    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);
    int rc = fn(&op, &stack, sess);
    if (stack.top > 0) {
        eval_entry_t e = eval_stack_pop(&stack);
        if (e.owned)
            *out_rel = e.rel;
    }
    col_rel_destroy(left);
    return rc;
}

/* ------------------------------------------------------------------------
 * Cases
 * ------------------------------------------------------------------------ */

/* Ledger backpressure: the operator pushes its (empty) output and returns 0
 * before joining; the owned filtered relation must not survive.  Budget 1000
 * gives the RELATION subsystem a 500-byte cap; a 400-byte gauge is at the 80%
 * threshold before the output's own bytes are attached. */
static int
test_backpressure_exit(const char *name, op_fn_t fn, wl_plan_op_type_t type)
{
    TEST(name);
    wl_col_session_t *sess = make_mock_session();
    if (!sess || register_right(sess) != 0) {
        FAIL("fixture");
        if (sess)
            destroy_mock_session(sess);
        return 1;
    }
    wl_mem_ledger_init(&sess->mem_ledger, 1000);
    wl_mem_ledger_set_gauge(&sess->mem_ledger, WL_MEM_SUBSYS_RELATION, 400);
    uint32_t before = sess->delta_pool->slot_used;

    col_rel_t *out = NULL;
    int rc = run_op(sess, fn, type, false, &out);
    int ok = rc == 0 && out != NULL && out->nrows == 0;
    if (!ok)
        printf("(rc=%d out=%p) ", rc, (void *)out);
    if (out)
        col_rel_destroy(out);
    /* Two slots: the filtered relation, then the pool output. */
    if (ok)
        ok = filter_slot_released(sess, before, 2, name);

    destroy_mock_session(sess);
    if (!ok) {
        FAIL("owned right filter leaked on the backpressure exit");
        return 1;
    }
    PASS();
    return 0;
}

/* Key-type mismatch: the operator returns EINVAL before allocating its
 * output; the owned filtered relation must not survive. */
static int
test_key_type_exit(const char *name, op_fn_t fn, wl_plan_op_type_t type)
{
    TEST(name);
    wl_col_session_t *sess = make_mock_session();
    if (!sess || register_right(sess) != 0) {
        FAIL("fixture");
        if (sess)
            destroy_mock_session(sess);
        return 1;
    }
    uint32_t before = sess->delta_pool->slot_used;

    col_rel_t *out = NULL;
    int rc = run_op(sess, fn, type, true, &out);
    int ok = rc == EINVAL && out == NULL;
    if (!ok)
        printf("(rc=%d out=%p) ", rc, (void *)out);
    if (out)
        col_rel_destroy(out);
    /* One slot: only the filtered relation was taken. */
    if (ok)
        ok = filter_slot_released(sess, before, 1, name);

    destroy_mock_session(sess);
    if (!ok) {
        FAIL("owned right filter leaked on the key-type exit");
        return 1;
    }
    PASS();
    return 0;
}

/* The fixture itself must reach the owned path: with no forced exit the
 * join succeeds, its output reflects the filter, and the filtered relation
 * was destroyed on the success path (which already cleaned up before this
 * issue).  Guards against a fixture that silently skips the filter. */
static int
test_success_path_uses_owned_filter(void)
{
    TEST("fixture reaches the owned filter path and joins");
    wl_col_session_t *sess = make_mock_session();
    if (!sess || register_right(sess) != 0) {
        FAIL("fixture");
        if (sess)
            destroy_mock_session(sess);
        return 1;
    }
    uint32_t before = sess->delta_pool->slot_used;
    col_rel_t *out = NULL;
    int rc = run_op(sess, wl_columnar_join_op, WL_PLAN_OP_JOIN, false, &out);
    /* 4 left rows x 1 surviving right row each (r = 200 + k > 150). */
    int ok = rc == 0 && out != NULL && out->nrows == 4;
    if (!ok)
        printf("(rc=%d rows=%u) ", rc, out ? out->nrows : 0);
    if (out)
        col_rel_destroy(out);
    if (ok)
        ok = filter_slot_released(sess, before, 2, "success");
    destroy_mock_session(sess);
    if (!ok) {
        FAIL("fixture did not take the owned filter path");
        return 1;
    }
    PASS();
    return 0;
}

int
main(void)
{
    printf("Join right-filter cleanup tests (Issue #1505)\n");

    test_success_path_uses_owned_filter();
    test_backpressure_exit("join: backpressure exit releases the filter",
        wl_columnar_join_op, WL_PLAN_OP_JOIN);
    test_backpressure_exit("join(diff): backpressure exit releases the filter",
        wl_columnar_join_diff_op, WL_PLAN_OP_JOIN);
    test_key_type_exit("join: key-type exit releases the filter (control)",
        wl_columnar_join_op, WL_PLAN_OP_JOIN);
    test_key_type_exit("join(diff): key-type exit releases the filter",
        wl_columnar_join_diff_op, WL_PLAN_OP_JOIN);
    test_key_type_exit("semijoin: key-type exit releases the filter",
        wl_columnar_semijoin_op, WL_PLAN_OP_SEMIJOIN);

    printf("\nPassed: %d/%d\n", tests_passed, tests_run);
    printf("Failed: %d/%d\n", tests_failed, tests_run);
    return tests_failed > 0 ? 1 : 0;
}
