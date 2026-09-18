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

#include <assert.h>
#include <stdint.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef WL_TEST_ALLOC_WRAP
void *__real_calloc(size_t count, size_t size);
static size_t fail_calloc_bytes;
static unsigned fail_calloc_hits;
void *__wrap_calloc(size_t count, size_t size)
{
    if (fail_calloc_bytes != 0 && count > 0
        && count * size == fail_calloc_bytes) {
        fail_calloc_bytes = 0;
        fail_calloc_hits++;
        return NULL;
    }
    return __real_calloc(count, size);
}
#endif

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
    col_session_free_filt_arrangements(s);
    col_mat_cache_clear(&s->mat_cache);
    /* The cached right-filter path owns its filtered relations and the entry
     * array itself; free them exactly as the real session teardown does in
     * wirelog/columnar/session.c.  Every lease must already be released, as
     * it asserts there -- this is a final teardown, not a per-relation
     * eviction, so it does not unwind pins. */
    assert(s->filt_cache_active_pins == 0);
    for (uint32_t i = 0; i < s->filt_cache_count; i++) {
        free(s->filt_cache[i].rel_name);
        free(s->filt_cache[i].filter_data);
        if (s->filt_cache[i].filtered)
            col_rel_destroy(s->filt_cache[i].filtered);
    for (uint32_t i = 0; i < s->filt_cache_count; i++) {
        col_rel_destroy(s->filt_cache[i].filtered);
        free(s->filt_cache[i].rel_name);
        free(s->filt_cache[i].filter_data);
    }
    free(s->filt_cache);
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

static int
test_probe_error_releases_filter(void)
{
    TEST("join: probe overflow releases filter and permits retry");
    wl_col_session_t *sess = make_mock_session();
    if (!sess || register_right(sess) != 0) {
        FAIL("fixture");
        if (sess)
            destroy_mock_session(sess);
        return 1;
    }
    uint32_t before = sess->delta_pool->slot_used;
    sess->join_output_limit = 1;
    col_rel_t *out = NULL;
    int rc = run_op(sess, wl_columnar_join_op, WL_PLAN_OP_JOIN, false, &out);
    int ok = rc == EOVERFLOW && out == NULL;
    if (out)
        col_rel_destroy(out);
    if (!filter_slot_released(sess, before, 2, "probe overflow"))
        ok = 0;

    sess->join_output_limit = 0;
    before = sess->delta_pool->slot_used;
    rc = run_op(sess, wl_columnar_join_op, WL_PLAN_OP_JOIN, false, &out);
    if (rc != 0 || !out || out->nrows != 4 || out->ncols != 4) {
        ok = 0;
    } else {
        for (uint32_t k = 0; k < 4; k++) {
            if (out->columns[0][k] != (int64_t)k
                || out->columns[1][k] != 10 + (int64_t)k
                || out->columns[2][k] != (int64_t)k
                || out->columns[3][k] != 200 + (int64_t)k)
                ok = 0;
        }
    }
    if (out)
        col_rel_destroy(out);
    if (!filter_slot_released(sess, before, 2, "probe retry"))
        ok = 0;
    destroy_mock_session(sess);
    if (!ok) {
        FAIL("probe error retained filter or retry differed from oracle");
        return 1;
    }
    PASS();
    return 0;
}

static int
test_right_filter_preserves_timestamps(void)
{
    TEST("right FILTER preserves timestamps in cached and uncached paths");
    const char *names[] = { "k", "r" };
    col_rel_t *rel = make_rel("timestamp-right", 2, names);
    delta_pool_t *pool = delta_pool_create(32, sizeof(col_rel_t), 4096);
    wl_col_session_t *sess = make_mock_session();
    wl_plan_expr_buffer_t expr = { filter_bytes, sizeof(filter_bytes) };
    uint8_t empty_bytes[sizeof(filter_bytes)];
    memcpy(empty_bytes, filter_bytes, sizeof(empty_bytes));
    /* CONST_INT is little-endian at offset 8; make the predicate false. */
    empty_bytes[8] = (uint8_t)232;
    wl_plan_expr_buffer_t empty_expr = { empty_bytes, sizeof(empty_bytes) };
    static uint8_t fallback_bytes[] = {
        WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
        WL_PLAN_EXPR_CONST_INT, 1, 0, 0, 0, 0, 0, 0, 0,
        WL_PLAN_EXPR_ARITH_ADD,
        WL_PLAN_EXPR_CONST_INT, 0, 0, 0, 0, 0, 0, 0, 0,
        WL_PLAN_EXPR_CMP_GT
    };
    wl_plan_expr_buffer_t fallback_expr = {
        fallback_bytes, sizeof(fallback_bytes)
    };
    col_filt_cache_pin_t pin = { 0 };
    col_rel_t *uncached = NULL;
    col_rel_t *cached = NULL;
    int ok = rel && pool && sess && col_rel_enable_timestamps(rel) == 0;

    for (int64_t i = 0; ok && i < 6; i++) {
        int64_t row[] = { i, i < 3 ? 100 + i : 200 + i };
        ok = col_rel_append_row(rel, row) == 0;
        if (ok)
            rel->timestamps[i] = (col_delta_timestamp_t){
                .iteration = (uint32_t)(40 + i),
                .stratum = (uint32_t)(50 + i),
                .worker = (uint32_t)(60 + i),
                .multiplicity = i == 4 ? -7 : i + 1
            };
    }
    if (ok) {
        uncached = wl_columnar_filter_apply_right_filter(&expr, rel, pool,
                NULL);
        cached = wl_columnar_filter_apply_right_filter_cached_pin(sess, &expr,
                "timestamp-right", rel, &pin);
        ok = uncached && cached && uncached->timestamps && cached->timestamps
            && uncached->nrows == 3 && cached->nrows == 3;
    }
    for (col_rel_t *out = uncached; ok && out;
        out = out == uncached ? cached : NULL) {
        for (uint32_t i = 0; i < 3; i++) {
            uint32_t src = i + 3;
            ok = ok && out->columns[0][i] == (int64_t)src
                && out->columns[1][i] == (int64_t)(200 + src)
                && out->timestamps[i].iteration == 40 + src
                && out->timestamps[i].stratum == 50 + src
                && out->timestamps[i].worker == 60 + src
                && out->timestamps[i].multiplicity
                == (src == 4 ? -7 : (int64_t)src + 1);
        }
    }
#ifdef WL_TEST_ALLOC_WRAP
    if (ok) {
        /* Exercise timestamp allocation failure on cache creation and retry. */
        wl_col_session_t *fault_sess = make_mock_session();
        col_filt_cache_pin_t fault_pin = { 0 };
        size_t ts_bytes = (size_t)rel->capacity * sizeof(*rel->timestamps);
        fail_calloc_bytes = ts_bytes;
        fail_calloc_hits = 0;
        col_rel_t *failed = wl_columnar_filter_apply_right_filter_cached_pin(
            fault_sess, &expr, "fault-right", rel, &fault_pin);
        ok = failed == NULL && !fault_pin.active && fail_calloc_hits == 1;
        if (ok) {
            col_rel_t *retried = wl_columnar_filter_apply_right_filter_cached(
                fault_sess, &expr, "fault-right", rel);
            ok = retried && retried->timestamps && retried->nrows == 3
                && retried->timestamps[1].multiplicity == -7;
        }
        /* The pin is inactive on this path only because the call failed.
         * Release it unconditionally so a wrap that stops firing reports a
         * readable test failure instead of aborting in teardown. */
        col_filt_cache_pin_release(&fault_pin);
        if (fault_sess)
            destroy_mock_session(fault_sess);
    }
#endif
    col_filt_cache_pin_release(&pin);
    if (ok) {
        /* A fresh lookup is a cache hit and must retain the same records. */
        col_rel_t *hit = wl_columnar_filter_apply_right_filter_cached(sess,
                &expr, "timestamp-right", rel);
        ok = hit && hit->timestamps && hit->nrows == 3
            && hit->timestamps[1].multiplicity == -7;
    }
    if (ok) {
        /* Source growth invalidates the entry and rebuilds it in place. */
        int64_t row[] = { 9, 209 };
        ok = col_rel_append_row(rel, row) == 0;
        if (ok) {
            rel->timestamps[6] = (col_delta_timestamp_t){
                .iteration = 99, .stratum = 98, .worker = 97,
                .multiplicity = -11
            };
            col_rel_t *rebuilt =
                wl_columnar_filter_apply_right_filter_cached(sess, &expr,
                    "timestamp-right", rel);
            ok = rebuilt && rebuilt->timestamps && rebuilt->nrows == 4
                && rebuilt->timestamps[3].iteration == 99
                && rebuilt->timestamps[3].multiplicity == -11;
        }
    }
#ifdef WL_TEST_ALLOC_WRAP
    if (ok) {
        /* The same fault during stale rebuild leaves a retryable cache entry. */
        int64_t row[] = { 11, 211 };
        ok = col_rel_append_row(rel, row) == 0;
        if (ok) {
            rel->timestamps[7] = (col_delta_timestamp_t){
                .iteration = 117, .stratum = 118, .worker = 119,
                .multiplicity = -13
            };
            size_t ts_bytes = (size_t)rel->capacity
                * sizeof(*rel->timestamps);
            fail_calloc_bytes = ts_bytes;
            fail_calloc_hits = 0;
            col_filt_cache_pin_t rebuild_pin = { 0 };
            col_rel_t *failed =
                wl_columnar_filter_apply_right_filter_cached_pin(
                sess, &expr, "timestamp-right", rel, &rebuild_pin);
            ok = failed == NULL && !rebuild_pin.active
                && fail_calloc_hits == 1;
            if (ok) {
                col_rel_t *retried =
                    wl_columnar_filter_apply_right_filter_cached(
                    sess, &expr, "timestamp-right", rel);
                ok = retried && retried->timestamps && retried->nrows == 5
                    && retried->timestamps[4].iteration == 117
                    && retried->timestamps[4].multiplicity == -13;
            }
            /* Same reason as fault_pin above. */
            col_filt_cache_pin_release(&rebuild_pin);
        }
    }
#endif
    if (ok) {
        /* A non-simple compiled predicate must copy timestamps as well. */
        col_rel_t *fallback = wl_columnar_filter_apply_right_filter(
            &fallback_expr, rel, pool, NULL);
        ok = fallback && fallback->timestamps && fallback->nrows == rel->nrows
            && fallback->timestamps[5].iteration == 45;
        if (fallback)
            col_rel_destroy(fallback);
    }
    if (ok) {
        /* An empty timestamp-enabled result remains timestamp-enabled. */
        col_rel_t *empty = wl_columnar_filter_apply_right_filter(
            &empty_expr, rel, pool, NULL);
        ok = empty && empty->timestamps && empty->nrows == 0;
        if (empty)
            col_rel_destroy(empty);
    }
    if (ok) {
        /* A held lease defers stale eviction until its final release. */
        col_filt_cache_pin_t held = { 0 };
        col_filt_cache_pin_t blocked = { 0 };
        col_rel_t *leased = wl_columnar_filter_apply_right_filter_cached_pin(
            sess, &expr, "timestamp-right", rel, &held);
        uint32_t held_src = rel->nrows;
        uint32_t held_expected = 1;
        for (uint32_t i = 0; i < rel->nrows; i++)
            if (rel->columns[1][i] > 150)
                held_expected++;
        int64_t row[] = { 10, 210 };
        ok = leased && col_rel_append_row(rel, row) == 0;
        if (ok) {
            rel->timestamps[held_src] = (col_delta_timestamp_t){
                .iteration = 107, .stratum = 108, .worker = 109,
                .multiplicity = -12
            };
            ok = wl_columnar_filter_apply_right_filter_cached_pin(
                sess, &expr, "timestamp-right", rel, &blocked) == NULL;
        }
        col_filt_cache_pin_release(&blocked);
        col_filt_cache_pin_release(&held);
        if (ok) {
            col_rel_t *retry = wl_columnar_filter_apply_right_filter_cached(
                sess, &expr, "timestamp-right", rel);
            ok = retry && retry->timestamps
                && retry->nrows == held_expected
                && retry->timestamps[held_expected - 1].iteration == 107
                && retry->timestamps[held_expected - 1].multiplicity == -12;
        }
    }
    if (uncached)
        col_rel_destroy(uncached);
    if (sess) {
        /* The cached result is owned by the session cache. */
        destroy_mock_session(sess);
    }
    if (rel)
        col_rel_destroy(rel);
    if (pool)
        delta_pool_destroy(pool);
    if (!ok) {
        FAIL("right FILTER timestamp correspondence mismatch");
        return 1;
    }
    PASS();
    return 0;
static wl_columnar_memory_governor_ref_t *
right_filter_governor(uint64_t bytes)
{
    wl_columnar_memory_resolution_t resolution = { 0 };
    resolution.budget_bytes = resolution.usable_bytes = bytes;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    return wl_columnar_memory_governor_ref_create(&resolution);
}

static bool
exact_join_rows(const col_rel_t *out, bool filtered, bool semi)
{
    if (!out || out->nrows != (filtered || semi ? 4u : 8u)
        || out->ncols != (semi ? 2u : 4u)) return false;
    unsigned seen[4] = { 0 };
    for (uint32_t r = 0; r < out->nrows; r++) {
        int64_t k = col_rel_get(out, r, 0);
        if (k < 0 || k >= 4 || col_rel_get(out, r, 1) != 10 + k) return false;
        if (!semi) {
            int64_t v = col_rel_get(out, r, 3);
            if (col_rel_get(out, r, 2) != k
                || (v != 200 + k && (filtered || v != 100 + k))) return false;
            unsigned bit = v == 200 + k ? 2u : 1u;
            if (seen[k] & bit) return false;
            seen[k] |= bit;
        } else {
            if (seen[k]) return false;
            seen[k] = 1;
        }
    }
    for (unsigned k = 0; k < 4; k++)
        if (seen[k] != (semi ? 1u : filtered ? 2u : 3u)) return false;
    return true;
}

static void
test_terminal_cache_retry(op_fn_t fn, bool hit, bool differential_txn)
{
    TEST("JOIN terminal refusal preserves cache/transaction and retries");
    wl_col_session_t *sess = make_mock_session();
    col_rel_t *left = make_left(false);
    eval_stack_t stack;
    eval_stack_init(&stack);
    wl_columnar_source_access_reader_t reader = { 0 };
    bool held = false, local = true;
    int ok = sess && left && register_right(sess) == 0;
    if (!ok) goto cleanup;
    wl_plan_op_t op;
    init_op(&op, WL_PLAN_OP_JOIN);
    op.delta_mode = WL_DELTA_FORCE_FULL;
    op.materialized = !differential_txn;
    if (differential_txn) op.right_filter_expr.size = 0;
    if (hit) {
        ok = eval_stack_push(&stack, left, false) == 0
            && fn(&op, &stack, sess) == 0;
        if (!ok) goto cleanup;
        (void)eval_stack_drain(&stack);
        ok = sess->mat_cache.count > 0;
        if (!ok) goto cleanup;
    }
    uint32_t old_cache_count = sess->mat_cache.count;
    const col_rel_t *old_cached =
        hit ? sess->mat_cache.entries[0].result : NULL;
    ok = col_rel_source_reader_acquire(left, &reader) == 0;
    if (!ok) goto cleanup;
    held = true;
    ok = eval_stack_push(&stack, left, true) == 0;
    if (!ok) goto cleanup;
    local = false;
    stack.items[0].seg_boundaries = malloc(2 * sizeof(uint32_t));
    ok = stack.items[0].seg_boundaries != NULL;
    if (!ok) goto cleanup;
    uint32_t *segments = stack.items[0].seg_boundaries;
    segments[0] = 0; segments[1] = left->nrows;
    stack.items[0].seg_count = 1;
    uint64_t generation = left->view_generation;
    for (unsigned attempt = 0; attempt < 2 && ok; attempt++) {
        ok = fn(&op, &stack, sess) == EBUSY && stack.top == 1
            && stack.items[0].rel == left && stack.items[0].owned
            && stack.items[0].seg_boundaries == segments
            && stack.items[0].seg_count == 1 &&
            left->view_generation == generation
            && sess->mat_cache.count == old_cache_count;
        if (hit) ok = ok && sess->mat_cache.entries[0].result == old_cached;
        if (differential_txn)
            ok = ok && sess->diff_txn_count == 0 && sess->diff_arr_count == 0;
    }
    if (!ok) goto cleanup;
    ok = col_rel_source_reader_release(&reader) == 0;
    held = false;
    ok = ok && fn(&op, &stack, sess) == 0 && stack.top == 1
        && exact_join_rows(stack.items[0].rel, !differential_txn, false);
    if (differential_txn)
        ok = ok && sess->diff_txn_count == 0 && sess->diff_arr_count == 1;
cleanup:
    if (held) (void)col_rel_source_reader_release(&reader);
    (void)eval_stack_drain(&stack);
    if (local) col_rel_destroy(left);
    if (sess) destroy_mock_session(sess);
    if (ok) PASS(); else FAIL("cache/transaction terminal ownership");
}

static void
test_missing_right_entry(op_fn_t fn, wl_plan_op_type_t type, bool owned)
{
    TEST("missing-right passthrough preserves complete evaluator entry");
    wl_col_session_t *sess = make_mock_session();
    col_rel_t *left = make_left(false);
    eval_stack_t stack;
    eval_stack_init(&stack);
    int ok = sess && left;
    bool transferred = false;
    if (!ok) goto cleanup;
    wl_plan_op_t op;
    init_op(&op, type);
    ok = eval_stack_push_delta(&stack, left, owned, true) == 0;
    if (!ok) goto cleanup;
    transferred = owned;
    stack.items[0].seg_boundaries = malloc(2 * sizeof(uint32_t));
    ok = stack.items[0].seg_boundaries != NULL;
    if (!ok) goto cleanup;
    uint32_t *segments = stack.items[0].seg_boundaries;
    segments[0] = 0; segments[1] = left->nrows;
    stack.items[0].seg_count = 1;
    ok = fn(&op, &stack, sess) == 0 && stack.top == 1
        && stack.items[0].rel == left && stack.items[0].owned == owned
        && stack.items[0].is_delta && stack.items[0].seg_boundaries == segments
        && stack.items[0].seg_count == 1 && segments[1] == left->nrows;
cleanup:
    (void)eval_stack_drain(&stack);
    if (!transferred) col_rel_destroy(left);
    if (sess) destroy_mock_session(sess);
    if (ok) PASS(); else FAIL("passthrough lost metadata");
}

static void
test_managed_right_filter(op_fn_t fn, wl_plan_op_type_t type, bool grow,
    bool exhaust)
{
    TEST("right-filter denial preserves held JOIN input and retries");
    wl_col_session_t *sess = make_mock_session();
    wl_columnar_memory_governor_ref_t *ref = right_filter_governor(
        grow ? (uint64_t)COL_REL_INIT_CAP * 2 * sizeof(int64_t) : 0);
    col_rel_t *left = make_left(false);
    eval_stack_t stack;
    eval_stack_init(&stack);
    wl_columnar_source_access_reader_t reader = { 0 };
    bool held = false, local = true;
    uint32_t *segments = NULL;
    int ok = sess && ref && left && register_right(sess) == 0;
    if (!ok) goto cleanup;
    sess->memory_governor = ref;
    if (grow) {
        const char *names[] = { "right", "$d$right" };
        for (unsigned n = 0; n < 2 && ok; n++) {
            col_rel_t *r = session_find_rel(sess, names[n]);
            for (uint32_t i = 0; i < COL_REL_INIT_CAP + 1 && ok; i++) {
                int64_t row[] = { 100 + i, 200 + i };
                ok = col_rel_append_row(r, row) == 0;
            }
        }
        if (!ok) goto cleanup;
    }
    if (exhaust) {
        while (sess->delta_pool->slot_used < sess->delta_pool->slot_cap) {
            col_rel_t *filler = col_rel_pool_new_like(sess->delta_pool,
                    "fill", left);
            ok = filler && filler->pool_owned;
            col_rel_destroy(filler);
            if (!ok) goto cleanup;
        }
    }
    wl_plan_op_t op;

    init_op(&op, type);
    ok = eval_stack_push(&stack, left, false) == 0;
    if (!ok) goto cleanup;
    ok = fn(&op, &stack, sess) == ENOMEM && stack.top == 0;
    if (!ok) goto cleanup;
    ok = col_rel_source_reader_acquire(left, &reader) == 0;
    if (!ok) goto cleanup;
    held = true;
    for (uint32_t i = 0; i < COL_STACK_MAX - 1; i++) {
        ok = eval_stack_push(&stack, session_find_rel(sess, "right"),
                false) == 0;
        if (!ok) goto cleanup;
    }
    ok = eval_stack_push(&stack, left, true) == 0;
    if (!ok) goto cleanup;
    local = false;
    stack.items[COL_STACK_MAX - 1].seg_boundaries = malloc(2 *
            sizeof(uint32_t));
    ok = stack.items[COL_STACK_MAX - 1].seg_boundaries != NULL;
    if (!ok) goto cleanup;
    segments = stack.items[COL_STACK_MAX - 1].seg_boundaries;
    segments[0] = 0; segments[1] = left->nrows;
    stack.items[COL_STACK_MAX - 1].seg_count = 1;
    for (unsigned i = 0; i < 2 && ok; i++)
        ok = fn(&op, &stack, sess) == EBUSY && stack.top == COL_STACK_MAX
            && stack.items[COL_STACK_MAX - 1].rel == left &&
            stack.items[COL_STACK_MAX - 1].owned
            && stack.items[COL_STACK_MAX - 1].seg_boundaries == segments
            && wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                    ref)) == 0;
    if (!ok) goto cleanup;
    /* With allocation allowed, refuse terminal cleanup after computing the
    * output. No result may replace the held entry or lose its metadata. */
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        1u << 20, memory_order_release);
    for (unsigned i = 0; i < 2 && ok; i++)
        ok = fn(&op, &stack, sess) == EBUSY && stack.top == COL_STACK_MAX
            && stack.items[COL_STACK_MAX - 1].rel == left
            && stack.items[COL_STACK_MAX - 1].owned
            && stack.items[COL_STACK_MAX - 1].seg_boundaries == segments
            && stack.items[COL_STACK_MAX - 1].seg_count == 1;
    if (!ok) goto cleanup;
    ok = col_rel_source_reader_release(&reader) == 0;

    held = false;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            ref)->usable_bytes,
        1u << 20, memory_order_release);
    ok = ok && fn(&op, &stack, sess) == 0 && stack.top == COL_STACK_MAX
        && (type == WL_PLAN_OP_ANTIJOIN
            ? stack.items[COL_STACK_MAX - 1].rel->nrows == 0
            : exact_join_rows(stack.items[COL_STACK_MAX - 1].rel, true,
        type == WL_PLAN_OP_SEMIJOIN));
cleanup:
    if (held) (void)col_rel_source_reader_release(&reader);
    (void)eval_stack_drain(&stack);
    if (local) col_rel_destroy(left);
    if (sess) {
        sess->memory_governor = NULL; destroy_mock_session(sess);
    }
    if (ref) {
        ok = ok &&
            wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                    ref)) == 0;
        wl_columnar_memory_governor_ref_release(ref);
    }
    if (ok) PASS(); else FAIL("governed JOIN refusal/retry");
}

static void
test_managed_filter_cache(void)
{
    TEST("right-filter cache governor identity and pinned fallback");
    wl_col_session_t *sess = make_mock_session();
    wl_columnar_memory_governor_ref_t *a = right_filter_governor(1u << 20);
    wl_columnar_memory_governor_ref_t *b = right_filter_governor(1u << 20);
    col_filt_cache_pin_t pin = { 0 }, second = { 0 };
    col_rel_t *fallback = NULL;
    int ok = sess && a && b && register_right(sess) == 0;
    if (!ok) goto cleanup;
    col_rel_t *right = session_find_rel(sess, "right");
    wl_plan_expr_buffer_t expr = { filter_bytes, sizeof(filter_bytes) };
    ok = col_rel_attach_memory_governor(right, b) == 0;
    sess->memory_governor = a;
    col_rel_t *cached = wl_columnar_filter_apply_right_filter_cached_pin(
        sess, &expr, "right", right, &pin);
    ok = ok && cached && cached->memory_governor == a && cached->nrows == 4;
    if (!ok) goto cleanup;
    ok = wl_columnar_filter_apply_right_filter_cached_pin(sess, &expr,
            "right", right, &second) == cached;
    col_filt_cache_pin_release(&second);
    sess->memory_governor = NULL; /* source fallback changes effective governor */
    ok = ok && !wl_columnar_filter_apply_right_filter_cached_pin(sess, &expr,
            "right", right, &second) && pin.active && cached->nrows == 4;
    fallback = wl_columnar_filter_apply_right_filter_governed(&expr, right,
            sess->delta_pool, sess->intern, b);
    ok = ok && fallback && fallback->memory_governor == b &&
        fallback->nrows == 4;
    col_rel_destroy(fallback); fallback = NULL;
    col_filt_cache_pin_release(&pin);
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(b)->usable_bytes,
        0, memory_order_release);
    ok = ok && !wl_columnar_filter_apply_right_filter_cached_pin(sess, &expr,
            "right", right, &second) && !second.active;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(b)->usable_bytes,
        1u << 20, memory_order_release);
    cached = wl_columnar_filter_apply_right_filter_cached_pin(sess, &expr,
            "right", right, &second);
    ok = ok && cached && cached->memory_governor == b && cached->nrows == 4;
cleanup:
    col_filt_cache_pin_release(&second);
    col_filt_cache_pin_release(&pin);
    col_rel_destroy(fallback);
    if (sess) {
        for (uint32_t i = 0; i < sess->filt_cache_count; i++) {
            col_rel_destroy(sess->filt_cache[i].filtered);
            free(sess->filt_cache[i].rel_name);
            free(sess->filt_cache[i].filter_data);
        }
        free(sess->filt_cache);
        sess->filt_cache = NULL; sess->filt_cache_count = 0;
        sess->memory_governor = NULL;
        destroy_mock_session(sess);
    }
    if (a) {
        ok = ok &&
            wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                    a)) == 0;
        wl_columnar_memory_governor_ref_release(a);
    }
    if (b) {
        ok = ok &&
            wl_columnar_memory_reserved(wl_columnar_memory_governor_ref_get(
                    b)) == 0;
        wl_columnar_memory_governor_ref_release(b);
    }
    if (ok) PASS(); else FAIL("cache governor admission contract");
}

int
main(void)
{
    test_terminal_cache_retry(wl_columnar_join_op, false, false);
    test_terminal_cache_retry(wl_columnar_join_op, true, false);
    test_terminal_cache_retry(wl_columnar_join_diff_op, false, false);
    test_terminal_cache_retry(wl_columnar_join_diff_op, true, false);
    test_terminal_cache_retry(wl_columnar_join_diff_op, false, true);
    for (unsigned owned = 0; owned < 2; owned++) {
        test_missing_right_entry(wl_columnar_antijoin_op, WL_PLAN_OP_ANTIJOIN,
            owned != 0);
        test_missing_right_entry(wl_columnar_semijoin_op, WL_PLAN_OP_SEMIJOIN,
            owned != 0);
    }
    test_managed_filter_cache();
    for (unsigned grow = 0; grow < 2; grow++)
        for (unsigned exhaust = 0; exhaust < 2; exhaust++)
            test_managed_right_filter(wl_columnar_join_op, WL_PLAN_OP_JOIN,
                grow != 0, exhaust != 0);
    for (unsigned grow = 0; grow < 2; grow++)
        for (unsigned exhaust = 0; exhaust < 2; exhaust++)
            test_managed_right_filter(wl_columnar_join_diff_op, WL_PLAN_OP_JOIN,
                grow != 0, exhaust != 0);
    for (unsigned grow = 0; grow < 2; grow++)
        for (unsigned exhaust = 0; exhaust < 2; exhaust++)
            test_managed_right_filter(wl_columnar_antijoin_op,
                WL_PLAN_OP_ANTIJOIN, grow != 0, exhaust != 0);
    for (unsigned grow = 0; grow < 2; grow++)
        for (unsigned exhaust = 0; exhaust < 2; exhaust++)
            test_managed_right_filter(wl_columnar_semijoin_op,
                WL_PLAN_OP_SEMIJOIN, grow != 0, exhaust != 0);
    printf("Join right-filter cleanup tests (Issue #1505)\n");

    test_success_path_uses_owned_filter();
    test_probe_error_releases_filter();
    test_right_filter_preserves_timestamps();
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
