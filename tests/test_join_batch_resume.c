/*
 * test_join_batch_resume.c - bounded keyed-join sub-batches (Issue #1446)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Drives the resumable producer in wirelog/columnar/join_batch.c directly
 * and through wl_columnar_join_op with WIRELOG_JOIN_BATCH_BYTES set.  The
 * oracle for every content check is the one-shot join with the knob unset,
 * compared as sorted row multisets.
 */
#ifndef _WIN32
#define _DEFAULT_SOURCE 1
#endif

#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/join_batch.h"
#include "../wirelog/util/log.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
static int
wl_test_setenv_(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, (value && *value) ? value : "1");
}

static int
wl_test_unsetenv_(const char *name)
{
    return _putenv_s(name, "");
}

#define setenv wl_test_setenv_
#define unsetenv wl_test_unsetenv_
#include <process.h>
#define wl_test_getpid _getpid
#else
#include <unistd.h>
#define wl_test_getpid getpid
#endif

/* Defined under WL_TEST_JOIN_BATCH_DESCRIPTOR_HOOKS in testlib_prod. */
extern void wl_columnar_diff_join_batch_test_fail_allocation_at(uint32_t n);

static int tests_run;
static int tests_passed;
static int tests_failed;

#define TEST(name)                            \
        do {                                      \
            tests_run++;                          \
            printf("  [%2d] %-64s ", tests_run, name); \
            fflush(stdout);                       \
        } while (0)
#define PASS()               \
        do {                     \
            printf("PASS\n");    \
            tests_passed++;      \
        } while (0)
#define FAIL(msg)                    \
        do {                             \
            printf("FAIL: %s\n", msg);   \
            tests_failed++;              \
        } while (0)

/* ---- session fixture --------------------------------------------------- */

static wl_columnar_memory_governor_ref_t *
make_governor(uint64_t budget)
{
    wl_columnar_memory_resolution_t resolution = { 0 };
    resolution.budget_bytes = budget;
    resolution.usable_bytes = budget;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    return wl_columnar_memory_governor_ref_create(&resolution);
}

static wl_col_session_t *
make_session(uint64_t budget)
{
    wl_col_session_t *s = (wl_col_session_t *)calloc(1, sizeof(*s));
    if (!s)
        return NULL;
    s->frontier_ops = &col_frontier_epoch_ops;
    s->delta_pool = delta_pool_create(256, sizeof(col_rel_t), 1024 * 1024);
    wl_mem_ledger_init(&s->mem_ledger, 0);
    s->memory_governor = make_governor(budget);
    if (!s->delta_pool || !s->memory_governor) {
        if (s->delta_pool)
            delta_pool_destroy(s->delta_pool);
        if (s->memory_governor)
            wl_columnar_memory_governor_ref_release(s->memory_governor);
        free(s);
        return NULL;
    }
    return s;
}

static void
destroy_session(wl_col_session_t *s)
{
    if (!s)
        return;
    wl_workqueue_destroy(s->wq);
    for (uint32_t i = 0; i < s->nrels; i++) {
        col_rel_destroy(s->rels[i]);
    }
    free(s->rels);
    if (s->rels_reservation.identity == &s->rels_reservation
        && atomic_load_explicit(&s->rels_reservation.state,
        memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED)
        (void)wl_columnar_memory_rollback(&s->rels_reservation);
    for (uint32_t i = 0; i < s->arr_count; i++) {
        free(s->arr_entries[i].rel_name);
        free(s->arr_entries[i].key_cols);
        arr_free_contents(&s->arr_entries[i].arr);
        col_arr_detach_memory_governor(&s->arr_entries[i].arr);
    }
    free(s->arr_entries);
    col_session_free_diff_arrangements(s);
    col_session_free_delta_arrangements(s);
    col_session_free_sorted_arrangements(s);
    col_session_free_filt_arrangements(s);
    col_mat_cache_clear(&s->mat_cache);
    session_rel_free_hash(s);
    for (uint32_t i = 0; i < s->filt_cache_count; i++) {
        free(s->filt_cache[i].rel_name);
        free(s->filt_cache[i].filter_data);
        if (s->filt_cache[i].metadata_reservation) {
            (void)wl_columnar_memory_rollback(
                s->filt_cache[i].metadata_reservation);
            free(s->filt_cache[i].metadata_reservation);
        }
        if (s->filt_cache[i].filtered)
            col_rel_destroy(s->filt_cache[i].filtered);
    }
    free(s->filt_cache);
    if (s->filt_cache_array_reservation) {
        (void)wl_columnar_memory_rollback(s->filt_cache_array_reservation);
        free(s->filt_cache_array_reservation);
    }
    delta_pool_destroy(s->delta_pool);
    wl_columnar_memory_governor_ref_release(s->memory_governor);
    free(s);
}

static uint64_t
reserved_of(const wl_col_session_t *s)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(s->memory_governor));
}

static col_rel_t *
make_rel(const char *name, uint32_t ncols, const char *const *col_names)
{
    col_rel_t *r = col_rel_new_auto(name, ncols);
    if (r && col_names)
        col_rel_set_schema(r, ncols, col_names);
    return r;
}

/* right(k, r): @fanout rows per key in [0, @keys). */
static col_rel_t *
make_right_named(const char *name, uint32_t keys, uint32_t fanout)
{
    const char *cn[] = { "k", "r" };
    col_rel_t *right = make_rel(name, 2, cn);
    if (!right)
        return NULL;
    for (uint32_t k = 0; k < keys; k++) {
        for (uint32_t f = 0; f < fanout; f++) {
            int64_t row[] = { (int64_t)k, (int64_t)(1000 * k + f) };
            if (col_rel_append_row(right, row) != 0) {
                col_rel_destroy(right);
                return NULL;
            }
        }
    }
    return right;
}

static col_rel_t *
make_right(uint32_t keys, uint32_t fanout)
{
    return make_right_named("right", keys, fanout);
}

/* The ordinary sequential fixture is intentionally compact and can happen
 * to be injective in the low bucket bits.  This deterministic permutation
 * supplies a broad key distribution while the test still discovers the
 * actual collision by inspecting the built arrangement. */
static col_rel_t *
make_hashed_right(uint32_t keys, uint32_t fanout)
{
    const char *cn[] = { "k", "r" };
    col_rel_t *right = make_rel("right", 2, cn);
    if (!right)
        return NULL;
    for (uint32_t k = 0; k < keys; k++) {
        int64_t key = (int64_t)((uint64_t)k * UINT64_C(2654435761));
        for (uint32_t f = 0; f < fanout; f++) {
            int64_t row[] = { key, (int64_t)(1000 * k + f) };
            if (col_rel_append_row(right, row) != 0) {
                col_rel_destroy(right);
                return NULL;
            }
        }
    }
    return right;
}

/* left(k, v) from an explicit key list. */
static col_rel_t *
make_left(const int64_t *keys, uint32_t n)
{
    const char *cn[] = { "k", "v" };
    col_rel_t *left = make_rel("left", 2, cn);
    if (!left)
        return NULL;
    for (uint32_t i = 0; i < n; i++) {
        int64_t row[] = { keys[i], (int64_t)i };
        if (col_rel_append_row(left, row) != 0) {
            col_rel_destroy(left);
            return NULL;
        }
    }
    return left;
}

static void
init_join_op(wl_plan_op_t *op, const char *const *lkeys,
    const char *const *rkeys)
{
    memset(op, 0, sizeof(*op));
    op->op = WL_PLAN_OP_JOIN;
    op->right_relation = "right";
    op->key_count = 1;
    op->left_keys = lkeys;
    op->right_keys = rkeys;
    op->delta_mode = WL_DELTA_FORCE_FULL;
}

/* VAR("col1") CONST_INT(50) CMP_GT: keep the right rows whose payload is
 * above 50.  This fixture retains some rows and rejects others, so filtered
 * fallback comparisons cannot pass vacuously. */
static uint8_t right_filter_bytes[] = {
    WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1',
    WL_PLAN_EXPR_CONST_INT, 50, 0, 0, 0, 0, 0, 0, 0,
    WL_PLAN_EXPR_CMP_GT
};

/* One-shot oracle through the operator with the knob off. */
static col_rel_t *
run_oracle(wl_col_session_t *sess, col_rel_t *left, const wl_plan_op_t *op)
{
    uint64_t saved = sess->join_batch_bytes;
    eval_stack_t stack;
    eval_entry_t result;
    int rc;

    sess->join_batch_bytes = 0;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);
    rc = col_op_join(op, &stack, sess);
    sess->join_batch_bytes = saved;
    if (rc != 0) {
        eval_stack_drain(&stack);
        return NULL;
    }
    result = eval_stack_pop(&stack);
    return result.owned ? result.rel : NULL;
}

static int
row_cmp(const void *a, const void *b)
{
    const int64_t *x = (const int64_t *)a;
    const int64_t *y = (const int64_t *)b;
    for (uint32_t c = 0; c < 4; c++) {
        if (x[c] < y[c])
            return -1;
        if (x[c] > y[c])
            return 1;
    }
    return 0;
}

/* Sorted-multiset equality of two relations with up to four columns. */
static bool
same_rows(const col_rel_t *a, const col_rel_t *b)
{
    int64_t *ra;
    int64_t *rb;
    bool equal = true;

    if (!a || !b || a->nrows != b->nrows || a->ncols != b->ncols
        || a->ncols > 4)
        return false;
    ra = (int64_t *)calloc((size_t)a->nrows * 4u + 4u, sizeof(int64_t));
    rb = (int64_t *)calloc((size_t)b->nrows * 4u + 4u, sizeof(int64_t));
    if (!ra || !rb) {
        free(ra);
        free(rb);
        return false;
    }
    for (uint32_t i = 0; i < a->nrows; i++)
        for (uint32_t c = 0; c < a->ncols; c++) {
            ra[i * 4u + c] = col_rel_get(a, i, c);
            rb[i * 4u + c] = col_rel_get(b, i, c);
        }
    qsort(ra, a->nrows, 4u * sizeof(int64_t), row_cmp);
    qsort(rb, b->nrows, 4u * sizeof(int64_t), row_cmp);
    if (memcmp(ra, rb, (size_t)a->nrows * 4u * sizeof(int64_t)) != 0)
        equal = false;
    free(ra);
    free(rb);
    return equal;
}

static bool
same_join_batch_cursor(const col_join_batch_cursor_t *a,
    const col_join_batch_cursor_t *b)
{
    return a && b && a->next.lr == b->next.lr && a->next.rr == b->next.rr
           && a->sequence == b->sequence
           && a->left_identity == b->left_identity
           && a->left_view_gen == b->left_view_gen
           && a->left_storage_gen == b->left_storage_gen
           && a->right_identity == b->right_identity
           && a->right_view_gen == b->right_view_gen
           && a->right_storage_gen == b->right_storage_gen
           && a->arr_generation == b->arr_generation
           && a->arr_indexed_rows == b->arr_indexed_rows
           && a->left_is_delta == b->left_is_delta
           && a->timestamps == b->timestamps;
}

static bool
same_continuation_cursor(const wl_columnar_continuation_cursor_t *a,
    const wl_columnar_continuation_cursor_t *b)
{
    return a && b && a->input_identity == b->input_identity
           && a->arrangement_identity == b->arrangement_identity
           && a->generation == b->generation && a->position == b->position
           && a->sequence == b->sequence;
}

/* Resolved key positions: column 0 on both sides in every fixture. */
static const uint32_t KEY0[1] = { 0 };

typedef struct {
    wl_col_session_t *sess;
    col_rel_t *left;
    col_rel_t *right;   /* owned by the session after registration */
    bool right_registered;
    col_rel_t *out;
    wl_plan_op_t op;
    const char *lkeys[1];
    const char *rkeys[1];
} fixture_t;

static bool
fixture_init(fixture_t *f, uint64_t budget, const int64_t *left_keys,
    uint32_t nleft, uint32_t right_keys, uint32_t fanout)
{
    memset(f, 0, sizeof(*f));
    f->lkeys[0] = "k";
    f->rkeys[0] = "k";
    f->sess = make_session(budget);
    f->right = make_right(right_keys, fanout);
    f->left = make_left(left_keys, nleft);
    if (!f->sess || !f->right || !f->left)
        return false;
    if (session_add_rel(f->sess, f->right) != 0)
        return false;
    f->right_registered = true;
    init_join_op(&f->op, f->lkeys, f->rkeys);
    f->out = col_rel_new_auto("$join", 4);
    if (!f->out || col_join_set_output_types(f->out, f->left, f->right,
        &f->op) != 0)
        return false;
    return true;
}

static void
fixture_fini(fixture_t *f)
{
    if (f->out)
        col_rel_destroy(f->out);
    if (f->left)
        col_rel_destroy(f->left);
    if (f->right && !f->right_registered)
        col_rel_destroy(f->right);
    destroy_session(f->sess);
    memset(f, 0, sizeof(*f));
}

static const col_arr_entry_t *
find_entry(const wl_col_session_t *sess, const char *name)
{
    for (uint32_t i = 0; i < sess->arr_count; i++)
        if (strcmp(sess->arr_entries[i].rel_name, name) == 0)
            return &sess->arr_entries[i];
    return NULL;
}

static int
register_relation(wl_col_session_t *sess, col_rel_t **owned,
    const char *name)
{
    int rc;

    if (!sess || !owned || !*owned || !name)
        return EINVAL;
    rc = wl_columnar_relation_rename_checked(*owned, name);
    if (rc != 0)
        return rc;
    rc = session_add_rel(sess, *owned);
    if (rc == 0)
        *owned = NULL;
    return rc;
}

/* Arrangement heads carry the generation in their upper 32 bits.  Only a
 * head from the currently active generation, with a real row index, may be
 * traversed as a chain; zero/stale heads are not valid row indices. */
static bool
arrangement_bucket_head(const col_arrangement_t *arr, uint32_t bucket,
    uint32_t *row_out)
{
    uint64_t encoded;
    uint32_t row;

    if (!arr || !row_out || !arr->ht_head || bucket >= arr->nbuckets)
        return false;
    encoded = arr->ht_head[bucket];
    if ((uint32_t)(encoded >> 32) != arr->generation)
        return false;
    row = (uint32_t)encoded;
    if (row == UINT32_MAX || row >= arr->indexed_rows)
        return false;
    *row_out = row;
    return true;
}

/* Select a real collision from the arrangement that the test fixture built.
 * Do not encode assumptions about the hash function or bucket count here: the
 * chain is the source of truth, and the selected key must also have at least
 * two matching rows so a one-row batch can park inside that chain. */
static bool
find_true_collision(const col_arrangement_t *arr, const col_rel_t *rel,
    int64_t *match_key, int64_t *other_key, uint32_t *bucket_out)
{
    if (!arr || !rel || !match_key || !other_key || !bucket_out
        || !arr->ht_head || !arr->ht_next)
        return false;
    for (uint32_t bucket = 0; bucket < arr->nbuckets; bucket++) {
        uint32_t head;
        if (!arrangement_bucket_head(arr, bucket, &head))
            continue;
        for (uint32_t first = head; first != UINT32_MAX;
            first = arr->ht_next[first]) {
            int64_t candidate = col_rel_get(rel, first, 0);
            uint32_t matches = 0;
            int64_t distinct = 0;
            bool has_distinct = false;
            for (uint32_t row = head; row != UINT32_MAX;
                row = arr->ht_next[row]) {
                int64_t key = col_rel_get(rel, row, 0);
                if (key == candidate)
                    matches++;
                else if (!has_distinct) {
                    distinct = key;
                    has_distinct = true;
                }
            }
            if (matches >= 2u && has_distinct) {
                *match_key = candidate;
                *other_key = distinct;
                *bucket_out = bucket;
                return true;
            }
        }
    }
    return false;
}

/* Fault-injecting wrapper around the real relation sink. */
typedef struct {
    wl_columnar_continuation_sink_t inner;
    int fail_append_at;      /* 1-based append call to fail, 0 = never */
    int fail_commit_at;      /* 1-based commit call to report failure */
    int appends;
    int commits;
} faulty_sink_t;

static wl_columnar_continuation_status_t
faulty_begin(void *ctx, const wl_columnar_continuation_batch_t *b)
{
    faulty_sink_t *f = (faulty_sink_t *)ctx;
    return f->inner.begin(f->inner.context, b);
}

static wl_columnar_continuation_status_t
faulty_reserve(void *ctx, uint64_t bytes, uint32_t rows)
{
    faulty_sink_t *f = (faulty_sink_t *)ctx;
    return f->inner.reserve(f->inner.context, bytes, rows);
}

static wl_columnar_continuation_status_t
faulty_append(void *ctx, const wl_columnar_continuation_batch_t *b)
{
    faulty_sink_t *f = (faulty_sink_t *)ctx;
    f->appends++;
    if (f->fail_append_at == f->appends)
        return WL_COLUMNAR_CONTINUATION_SINK_FAILURE;
    return f->inner.append(f->inner.context, b);
}

static wl_columnar_continuation_status_t
faulty_commit(void *ctx, bool *committed)
{
    faulty_sink_t *f = (faulty_sink_t *)ctx;
    wl_columnar_continuation_status_t st;
    f->commits++;
    st = f->inner.commit(f->inner.context, committed);
    if (f->fail_commit_at == f->commits)
        return WL_COLUMNAR_CONTINUATION_COMMIT_FAILURE; /* committed=true */
    return st;
}

static void
faulty_abort(void *ctx)
{
    faulty_sink_t *f = (faulty_sink_t *)ctx;
    f->inner.abort(f->inner.context);
}

static void
faulty_sink_wrap(faulty_sink_t *f, wl_columnar_continuation_sink_t *sink)
{
    sink->context = f;
    sink->begin = faulty_begin;
    sink->reserve = faulty_reserve;
    sink->append = faulty_append;
    sink->commit = faulty_commit;
    sink->abort = faulty_abort;
}

/* ---- tests ------------------------------------------------------------- */

/* (1) A single high-fanout left row spans several batches with no loss or
 * duplication, and the batch count matches. */
static void
test_high_fanout_spans_batches(void)
{
    fixture_t f;
    const int64_t keys[] = { 3 };
    wl_columnar_continuation_t *cont = NULL;
    col_rel_t *oracle;
    col_join_batch_cursor_t cur;
    int rc;

    TEST("high-fanout left row crosses batches without loss or duplication");
    if (!fixture_init(&f, 1u << 24, keys, 1, 4, 1000)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    /* 4 columns x 8 B = 32 B per row; 7 rows per batch -> 143 batches. */
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 7u * 32u, &cont);
    if (rc != 0 || col_join_batch_rows_per_batch(cont) != 7u) {
        FAIL("producer create or rows_per_batch");
        fixture_fini(&f);
        return;
    }
    rc = col_join_batch_run_to_relation(cont, f.sess, f.out);
    oracle = run_oracle(f.sess, f.left, &f.op);
    if (rc != 0 || !oracle || !same_rows(oracle, f.out)
        || f.out->nrows != 1000u) {
        FAIL("result differs from the one-shot oracle");
    } else if (!col_join_batch_cursor_get(cont, &cur)
        || cur.sequence != 143u || cur.next.lr != 1u
        || cur.next.rr != UINT32_MAX) {
        FAIL("cursor did not end at (1, head) after 143 committed batches");
    } else {
        PASS();
    }
    if (oracle)
        col_rel_destroy(oracle);
    wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* (2)+(3) Boundaries: on a collision row inside a bucket chain, on the
 * last match of a left row followed by no-match rows, then more matches. */
static void
test_boundaries_collision_and_last_match(void)
{
    fixture_t f;
    /* keys 5 and 21 share a bucket in a 16-bucket table only if the hash
     * does; instead force chain sharing by repeating keys: the chain for
     * key 2 holds 5 rows, so a 3-row batch parks mid-chain.  Left rows 1-3
     * have no matches (key 9 is absent), then key 1 matches again. */
    const int64_t keys[] = { 2, 9, 9, 9, 1, 2 };
    wl_columnar_continuation_t *cont = NULL;
    wl_columnar_continuation_sink_t sink;
    col_join_batch_relation_sink_t sctx;
    col_join_batch_cursor_t cur;
    col_rel_t *oracle;
    wl_columnar_continuation_status_t st;

    TEST("batch boundaries on a chain row and on a last match are exact");
    if (!fixture_init(&f, 1u << 24, keys, 6, 3, 5)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    if (col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
        KEY0, 1, 5u * 32u, &cont) != 0
        || col_join_batch_relation_sink_init(&sctx, &sink, f.sess, f.out)
        != 0) {
        FAIL("create or sink init");
        fixture_fini(&f);
        return;
    }
    /* Batch 1: exactly the five matches of left row 0 (key 2): the batch
     * fills on the last match, so the cursor must park on (1, head), and
     * the next batch must skip rows 1-3 (no matches) without returning an
     * empty batch. */
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_OK || f.out->nrows != 5u
        || !col_join_batch_cursor_get(cont, &cur) || cur.next.lr != 1u
        || cur.next.rr != UINT32_MAX || cur.sequence != 1u) {
        FAIL("boundary on the last match did not park on the next left row");
        goto out;
    }
    /* Batch 2: five matches of left row 4 (key 1). */
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_OK || f.out->nrows != 10u
        || !col_join_batch_cursor_get(cont, &cur) || cur.next.lr != 5u) {
        FAIL("no-match rows were not skipped inside one batch");
        goto out;
    }
    /* Batch 3: left row 5 (key 2), five matches; then DONE. */
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_OK || f.out->nrows != 15u) {
        FAIL("third batch");
        goto out;
    }
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_DONE || f.out->nrows != 15u
        || !wl_columnar_continuation_is_done(cont)) {
        FAIL("terminal publish must report DONE without appending");
        goto out;
    }
    oracle = run_oracle(f.sess, f.left, &f.op);
    if (!oracle || !same_rows(oracle, f.out))
        FAIL("result differs from the one-shot oracle");
    else
        PASS();
    if (oracle)
        col_rel_destroy(oracle);
out:
    wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* Mid-chain boundary: a 3-row batch over a 5-row chain parks on a chain
 * row and resumes from it without repeating or skipping. */
static void
test_boundary_mid_chain(void)
{
    fixture_t f;
    const int64_t keys[] = { 1, 1 };
    wl_columnar_continuation_t *cont = NULL;
    col_rel_t *oracle;
    col_join_batch_cursor_t cur;
    int rc;

    TEST("batch boundary inside a bucket chain resumes at the next row");
    if (!fixture_init(&f, 1u << 24, keys, 2, 2, 5)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 3u * 32u, &cont);
    if (rc == 0)
        rc = col_join_batch_run_to_relation(cont, f.sess, f.out);
    oracle = run_oracle(f.sess, f.left, &f.op);
    if (rc != 0 || !oracle || f.out->nrows != 10u
        || !same_rows(oracle, f.out)
        || !col_join_batch_cursor_get(cont, &cur) || cur.sequence != 4u)
        FAIL("10 matches over 3-row batches must commit 4 batches exactly");
    else
        PASS();
    if (oracle)
        col_rel_destroy(oracle);
    if (cont)
        wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* (4) Sink failure before commit leaves rows, cursor, sequence and the
 * reservation unchanged; the retry publishes once with no new reservation.
 * A failure reported after commit is ambiguous and never retried. */
static void
test_sink_failure_before_and_after_commit(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1 };
    wl_columnar_continuation_t *cont = NULL;
    wl_columnar_continuation_sink_t real;
    wl_columnar_continuation_sink_t sink;
    col_join_batch_relation_sink_t sctx;
    faulty_sink_t faulty;
    col_join_batch_cursor_t cur;
    uint64_t reserved_after_reserve;
    wl_columnar_continuation_status_t st;

    TEST("pre-commit sink failure retries once; post-commit is ambiguous");
    if (!fixture_init(&f, 1u << 24, keys, 2, 2, 100)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    if (col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
        KEY0, 1, 80u * 32u, &cont) != 0
        || col_join_batch_relation_sink_init(&sctx, &real, f.sess, f.out)
        != 0) {
        FAIL("create or sink init");
        fixture_fini(&f);
        return;
    }
    memset(&faulty, 0, sizeof(faulty));
    faulty.inner = real;
    faulty.fail_append_at = 1;
    faulty_sink_wrap(&faulty, &sink);
    st = wl_columnar_continuation_publish(cont, &sink);
    reserved_after_reserve = reserved_of(f.sess);
    if (st != WL_COLUMNAR_CONTINUATION_SINK_FAILURE || f.out->nrows != 0u
        || !col_join_batch_cursor_get(cont, &cur) || cur.sequence != 0u
        || cur.next.lr != 0u || cur.next.rr != UINT32_MAX
        || f.out->capacity < 80u) {
        FAIL("failed append changed visible state");
        goto out;
    }
    /* Retry: same batch, exactly once, and no further reservation because
     * the capacity admitted for it is retained. */
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_OK || f.out->nrows != 80u
        || faulty.appends != 2 || faulty.commits != 1
        || reserved_of(f.sess) != reserved_after_reserve
        || !col_join_batch_cursor_get(cont, &cur) || cur.sequence != 1u) {
        FAIL("retry did not publish exactly once without a new reservation");
        goto out;
    }
    /* Post-commit failure: the sink committed, so the cursor advances and
     * the status is ambiguous; a further publish continues, not replays. */
    faulty.fail_commit_at = 2;
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_COMMIT_AMBIGUOUS
        || f.out->nrows != 160u
        || !col_join_batch_cursor_get(cont, &cur) || cur.sequence != 2u) {
        FAIL("ambiguous commit did not advance the cursor");
        goto out;
    }
    faulty.fail_commit_at = 0;
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_OK || f.out->nrows != 200u) {
        FAIL("publish after an ambiguous commit replayed or stalled");
        goto out;
    }
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_DONE || f.out->nrows != 200u)
        FAIL("completion");
    else
        PASS();
out:
    wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* Representation overflow is not a memory failure: a batch that would take
 * the output past UINT32_MAX rows fails in the sink with EOVERFLOW recorded,
 * before any output capacity is requested, and leaves no visible change.  The
 * output's row count is faked; no row is appended while it is. */
static void
test_sink_reserve_overflow_is_typed(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1 };
    wl_columnar_continuation_t *cont = NULL;
    wl_columnar_continuation_sink_t sink;
    col_join_batch_relation_sink_t sctx;
    col_join_batch_cursor_t cur;
    uint32_t cap;
    wl_columnar_continuation_status_t st;

    TEST("sink reserve past UINT32_MAX rows is EOVERFLOW, not a denial");
    if (!fixture_init(&f, 1u << 24, keys, 2, 2, 100)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    if (col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
        KEY0, 1, 80u * 32u, &cont) != 0
        || col_join_batch_relation_sink_init(&sctx, &sink, f.sess, f.out)
        != 0) {
        FAIL("create or sink init");
        fixture_fini(&f);
        return;
    }
    cap = f.out->capacity;
    f.out->nrows = UINT32_MAX - 2u;
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_SINK_FAILURE
        || sctx.error != EOVERFLOW || f.out->nrows != UINT32_MAX - 2u
        || f.out->capacity != cap || f.out->memory_budget_denial_pending
        || !col_join_batch_cursor_get(cont, &cur) || cur.sequence != 0u) {
        f.out->nrows = 0;
        FAIL("overflow was not a typed, side-effect-free sink failure");
        goto out;
    }
    /* The recorded error belongs to the failed batch only. */
    f.out->nrows = 0;
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_OK || sctx.error != 0
        || f.out->nrows != 80u)
        FAIL("the next batch did not publish with the error cleared");
    else
        PASS();
out:
    wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* The overflow reaches the caller of col_join_batch_run_to_relation as
 * EOVERFLOW (it was ENOMEM) and is not reported as a budget denial. */
static void
test_run_to_relation_overflow_returns_eoverflow(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1 };
    wl_columnar_continuation_t *cont = NULL;
    uint32_t cap;
    int rc;

    TEST("run_to_relation returns EOVERFLOW for row-count overflow");
    if (!fixture_init(&f, 1u << 24, keys, 2, 2, 100)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    if (col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
        KEY0, 1, 80u * 32u, &cont) != 0) {
        FAIL("create");
        fixture_fini(&f);
        return;
    }
    cap = f.out->capacity;
    f.out->nrows = UINT32_MAX - 2u;
    rc = col_join_batch_run_to_relation(cont, f.sess, f.out);
    f.out->nrows = 0;
    if (rc != EOVERFLOW) {
        char msg[64];
        snprintf(msg, sizeof(msg), "rc %d, want EOVERFLOW", rc);
        FAIL(msg);
    } else if (f.sess->memory_budget_denied
        || f.out->memory_budget_denial_pending || f.out->capacity != cap) {
        FAIL("overflow was reported as a budget denial or grew the output");
    } else {
        PASS();
    }
    wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* The pipeline sink records the same EOVERFLOW in its error, the field the
 * pipeline returns as its operator errno on a sink failure. */
static void
test_pipeline_reserve_overflow_is_typed(void)
{
    fixture_t f;
    const int64_t keys[] = { 0 };
    int err = 0;
    uint32_t cap;
    wl_columnar_continuation_status_t st;

    TEST("pipeline sink reserve past UINT32_MAX rows records EOVERFLOW");
    if (!fixture_init(&f, 1u << 24, keys, 1, 1, 1)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    cap = f.out->capacity;
    f.out->nrows = UINT32_MAX - 2u;
    st = wl_columnar_join_pipeline_test_reserve(f.out, 8u, &err);
    f.out->nrows = 0;
    if (st != WL_COLUMNAR_CONTINUATION_SINK_FAILURE || err != EOVERFLOW
        || f.out->capacity != cap)
        FAIL("pipeline overflow was not a typed sink failure");
    else
        PASS();
    fixture_fini(&f);
}

/* ---- independent oracles and fault/reuse evidence (#1382) ------------- */

/* Nested-loop oracle over the inputs' own cells: every (l, r) pair whose
 * column-0 keys are equal yields (l0, l1, r0, r1).  It reads the inputs
 * through col_rel_get only, so it shares no code with any join path, and
 * compares the expected rows with @out as sorted multisets. */
static bool
nested_loop_equal(const col_rel_t *left, const col_rel_t *right,
    const col_rel_t *out)
{
    uint64_t n = 0;
    int64_t *want;
    int64_t *got;
    bool equal;

    if (!left || !right || !out || out->ncols != 4)
        return false;
    for (uint32_t l = 0; l < left->nrows; l++)
        for (uint32_t r = 0; r < right->nrows; r++)
            if (col_rel_get(left, l, 0) == col_rel_get(right, r, 0))
                n++;
    if (n != out->nrows)
        return false;
    want = (int64_t *)calloc((size_t)n * 4u + 4u, sizeof(int64_t));
    got = (int64_t *)calloc((size_t)n * 4u + 4u, sizeof(int64_t));
    if (!want || !got) {
        free(want);
        free(got);
        return false;
    }
    n = 0;
    for (uint32_t l = 0; l < left->nrows; l++)
        for (uint32_t r = 0; r < right->nrows; r++) {
            if (col_rel_get(left, l, 0) != col_rel_get(right, r, 0))
                continue;
            want[n * 4u + 0u] = col_rel_get(left, l, 0);
            want[n * 4u + 1u] = col_rel_get(left, l, 1);
            want[n * 4u + 2u] = col_rel_get(right, r, 0);
            want[n * 4u + 3u] = col_rel_get(right, r, 1);
            n++;
        }
    for (uint32_t i = 0; i < out->nrows; i++)
        for (uint32_t c = 0; c < 4u; c++)
            got[(size_t)i * 4u + c] = col_rel_get(out, i, c);
    qsort(want, (size_t)n, 4u * sizeof(int64_t), row_cmp);
    qsort(got, (size_t)n, 4u * sizeof(int64_t), row_cmp);
    equal = memcmp(want, got, (size_t)n * 4u * sizeof(int64_t)) == 0;
    free(want);
    free(got);
    return equal;
}

/* Projected counterpart of nested_loop_equal for a projection of
 * (left column 1, right column 1), the shape test_projected_output uses. */
static bool
nested_loop_projected_equal(const col_rel_t *left, const col_rel_t *right,
    const col_rel_t *out)
{
    uint64_t n = 0;
    int64_t *want;
    int64_t *got;
    bool equal;

    if (!left || !right || !out || out->ncols != 2)
        return false;
    for (uint32_t l = 0; l < left->nrows; l++)
        for (uint32_t r = 0; r < right->nrows; r++)
            if (col_rel_get(left, l, 0) == col_rel_get(right, r, 0))
                n++;
    if (n != out->nrows)
        return false;
    want = (int64_t *)calloc((size_t)n * 4u + 4u, sizeof(int64_t));
    got = (int64_t *)calloc((size_t)n * 4u + 4u, sizeof(int64_t));
    if (!want || !got) {
        free(want);
        free(got);
        return false;
    }
    n = 0;
    for (uint32_t l = 0; l < left->nrows; l++)
        for (uint32_t r = 0; r < right->nrows; r++) {
            if (col_rel_get(left, l, 0) != col_rel_get(right, r, 0))
                continue;
            want[n * 4u + 0u] = col_rel_get(left, l, 1);
            want[n * 4u + 1u] = col_rel_get(right, r, 1);
            n++;
        }
    for (uint32_t i = 0; i < out->nrows; i++) {
        got[(size_t)i * 4u + 0u] = col_rel_get(out, i, 0);
        got[(size_t)i * 4u + 1u] = col_rel_get(out, i, 1);
    }
    qsort(want, (size_t)n, 4u * sizeof(int64_t), row_cmp);
    qsort(got, (size_t)n, 4u * sizeof(int64_t), row_cmp);
    equal = memcmp(want, got, (size_t)n * 4u * sizeof(int64_t)) == 0;
    free(want);
    free(got);
    return equal;
}

/* Run the differential operator once over @f's left input.  Returns its rc;
 * on success *@out receives the owned result (NULL if not owned). */
static int
run_diff_operator(fixture_t *f, col_rel_t *left, bool owned,
    col_rel_t **out)
{
    eval_stack_t stack;
    eval_entry_t result;
    int rc;

    *out = NULL;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, owned);
    rc = col_op_join_diff(&f->op, &stack, f->sess);
    if (rc == 0 && stack.top > 0) {
        result = eval_stack_pop(&stack);
        free(result.seg_boundaries);
        if (result.owned)
            *out = result.rel;
    }
    eval_stack_drain(&stack);
    return rc;
}

/* A writer on @rel is granted only when no reader holds it.  The check is
 * validated first: with a reader deliberately held it must be refused. */
static bool
no_reader_holds(const col_rel_t *rel)
{
    wl_columnar_source_access_writer_t writer = { 0 };
    int rc = col_rel_source_writer_acquire(rel, &writer);

    if (rc != 0)
        return false;
    return wl_columnar_source_access_writer_release(&writer) == 0;
}

static bool
held_reader_is_observed(const col_rel_t *rel)
{
    wl_columnar_source_access_reader_t reader = { 0 };
    bool observed;

    if (col_rel_source_reader_acquire(rel, &reader) != 0)
        return false;
    observed = !no_reader_holds(rel);
    return col_rel_source_reader_release(&reader) == 0 && observed;
}

/* High-fanout keys checked against the nested-loop oracle rather than the
 * one-shot join: two left rows on key 1 and one on key 2, 300 right rows
 * per key, so 900 output rows in 7-row batches. */
static void
test_high_fanout_independent_oracle(void)
{
    fixture_t f;
    const int64_t keys[] = { 1, 1, 2 };
    wl_columnar_continuation_t *cont = NULL;
    col_join_batch_cursor_t cur;
    int rc;

    TEST("high-fanout bounded join equals a nested-loop oracle");
    if (!fixture_init(&f, 1u << 24, keys, 3, 3, 300)
        || col_join_batch_producer_create(f.sess, &f.op, f.left, false,
        KEY0, KEY0, 1, 7u * 32u, &cont) != 0) {
        FAIL("fixture or producer");
        if (cont)
            wl_columnar_continuation_destroy(cont);
        fixture_fini(&f);
        return;
    }
    rc = col_join_batch_run_to_relation(cont, f.sess, f.out);
    if (rc != 0 || f.out->nrows != 900u
        || !nested_loop_equal(f.left, f.right, f.out))
        FAIL("result differs from the nested-loop oracle");
    else if (!col_join_batch_cursor_get(cont, &cur) || cur.sequence < 100u)
        FAIL("the result was not produced in many batches");
    else
        PASS();
    wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* The differential operator under the knob takes its bounded producer
 * ($join_diff_batch) and equals the same nested-loop oracle. */
static void
test_diff_operator_high_fanout_independent_oracle(void)
{
    fixture_t f;
    const int64_t keys[] = { 1, 1, 2 };
    col_rel_t *out = NULL;
    int rc;

    TEST("bounded differential join equals a nested-loop oracle");
    if (!fixture_init(&f, 1u << 24, keys, 3, 3, 300)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    f.sess->join_batch_bytes = 7u * 32u;
    rc = run_diff_operator(&f, f.left, false, &out);
    if (rc != 0 || !out || !out->name
        || strcmp(out->name, "$join_diff_batch") != 0)
        FAIL("the bounded differential producer did not run");
    else if (f.sess->join_batch_fallback_count != 0
        || !nested_loop_equal(f.left, f.right, out))
        FAIL("result differs from the nested-loop oracle");
    else
        PASS();
    if (out)
        col_rel_destroy(out);
    fixture_fini(&f);
}

/* An empty left input and an owned left input on the bounded differential
 * path: exact results, no fallback, and (under ASan) the owned input is
 * released exactly once. */
static void
test_diff_operator_empty_and_owned_left(void)
{
    fixture_t f;
    const int64_t keys[] = { 1, 1, 2 };
    col_rel_t *empty = NULL;
    col_rel_t *owned = NULL;
    col_rel_t *out = NULL;
    bool ok = true;

    TEST("bounded differential join over empty and owned left inputs");
    if (!fixture_init(&f, 1u << 24, keys, 3, 3, 4)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    f.sess->join_batch_bytes = 7u * 32u;
    empty = make_left(keys, 0);
    if (!empty || run_diff_operator(&f, empty, false, &out) != 0
        || (out && out->nrows != 0)) {
        printf("(empty left) ");
        ok = false;
    }
    if (out)
        col_rel_destroy(out);
    out = NULL;
    if (ok && (col_rel_deep_copy(f.left, &owned, NULL) != 0
        || run_diff_operator(&f, owned, true, &out) != 0
        || !out || !nested_loop_equal(f.left, f.right, out))) {
        printf("(owned left) ");
        ok = false;
    }
    if (ok && f.sess->join_batch_fallback_count != 0) {
        printf("(fallback recorded) ");
        ok = false;
    }
    if (out)
        col_rel_destroy(out);
    if (empty)
        col_rel_destroy(empty);
    if (ok)
        PASS();
    else
        FAIL("empty or owned left");
    fixture_fini(&f);
}

/* #1382: a projected differential join under the knob takes the bounded
 * producer and equals a nested-loop oracle over the projected columns. */
static void
test_diff_operator_projected_independent_oracle(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1, 1 };
    uint32_t project[] = { 1, 3 };
    col_rel_t *out = NULL;
    int rc;

    TEST("projected bounded differential join equals a nested-loop oracle");
    if (!fixture_init(&f, 1u << 24, keys, 3, 2, 30)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    f.op.project_indices = project;
    f.op.project_count = 2;
    f.sess->join_batch_bytes = 7u * 16u;
    rc = run_diff_operator(&f, f.left, false, &out);
    if (rc != 0 || !out || !out->name
        || strcmp(out->name, "$join_diff_batch") != 0)
        FAIL("the bounded differential producer did not run");
    else if (f.sess->join_batch_fallback_count != 0 || out->nrows != 90u
        || !nested_loop_projected_equal(f.left, f.right, out))
        FAIL("projected result differs from the nested-loop oracle");
    else
        PASS();
    if (out)
        col_rel_destroy(out);
    fixture_fini(&f);
}

/* #1382: an empty left delta under the knob, for the ordinary and the
 * differential operator: an empty result, no recorded fallback, and the
 * session's reserved bytes unchanged once the result is gone. */
static void
test_operator_empty_left_delta(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1 };
    col_rel_t *delta = NULL;
    bool ok = true;

    TEST("bounded operators over an empty left delta");
    if (!fixture_init(&f, 1u << 24, keys, 2, 2, 30)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    f.sess->join_batch_bytes = 7u * 32u;
    delta = make_left(keys, 0);
    /* Warm-up: build the session's persistent and differential
     * arrangements, which the session keeps, before measuring. */
    for (int diff = 0; diff < 2; diff++) {
        eval_stack_t stack;
        eval_entry_t result;

        eval_stack_init(&stack);
        eval_stack_push(&stack, f.left, false);
        if ((diff ? col_op_join_diff(&f.op, &stack, f.sess)
            : col_op_join(&f.op, &stack, f.sess)) != 0 || stack.top == 0) {
            ok = false;
        } else {
            result = eval_stack_pop(&stack);
            free(result.seg_boundaries);
            if (result.owned)
                col_rel_destroy(result.rel);
        }
        eval_stack_drain(&stack);
    }
    if (!ok)
        printf("(warm-up) ");
    for (int diff = 0; diff < 2 && ok && delta; diff++) {
        eval_stack_t stack;
        eval_entry_t result;
        uint64_t reserved = reserved_of(f.sess);
        col_rel_t *out = NULL;
        int rc;

        eval_stack_init(&stack);
        eval_stack_push_delta(&stack, delta, false, true);
        rc = diff ? col_op_join_diff(&f.op, &stack, f.sess)
                  : col_op_join(&f.op, &stack, f.sess);
        if (rc == 0 && stack.top > 0) {
            result = eval_stack_pop(&stack);
            free(result.seg_boundaries);
            if (result.owned)
                out = result.rel;
            else if (result.rel && result.rel->nrows != 0)
                rc = EINVAL;
        }
        eval_stack_drain(&stack);
        if (rc != 0 || (out && out->nrows != 0)
            || f.sess->join_batch_fallback_count != 0) {
            printf("(%s rc=%d) ", diff ? "differential" : "ordinary", rc);
            ok = false;
        }
        if (out)
            col_rel_destroy(out);
        if (ok && reserved_of(f.sess) != reserved) {
            printf("(%s reserved) ", diff ? "differential" : "ordinary");
            ok = false;
        }
    }
    if (!delta)
        ok = false;
    if (delta)
        col_rel_destroy(delta);
    if (ok)
        PASS();
    else
        FAIL("empty left delta");
    fixture_fini(&f);
}

/* Three runs of the bounded differential operator on one governed session:
 * a fault after committed batches (output growth fails mid-stream), a
 * fault at create on the retry, and a clean run.  After each failure the
 * session's reserved bytes are back at the warmed baseline and neither input
 * is still held by a reader; the clean run equals the nested-loop oracle. */
static void
test_diff_operator_fault_retry_restores_baseline(void)
{
    fixture_t f;
    const int64_t keys[] = { 1, 1, 2 };
    col_rel_t *out = NULL;
    uint64_t baseline;
    int rc;
    bool ok = true;

    TEST("differential faults mid-stream and on retry restore the baseline");
    if (!fixture_init(&f, 1u << 24, keys, 3, 3, 300)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    f.sess->join_batch_bytes = 7u * 32u;
    /* Warm-up: build the differential arrangement once. */
    if (run_diff_operator(&f, f.left, false, &out) != 0 || !out) {
        FAIL("warm-up run");
        fixture_fini(&f);
        return;
    }
    col_rel_destroy(out);
    out = NULL;
    baseline = reserved_of(f.sess);
    if (!held_reader_is_observed(f.right)
        || !held_reader_is_observed(f.left)) {
        FAIL("a held reader is not observable through writer acquisition");
        fixture_fini(&f);
        return;
    }

    /* Run 1: the output fills its 64-row capacity, then its growth fails.
     * The session's output high-water mark shows the nine 7-row batches
     * (63 rows) committed before the fault. */
    f.sess->join_output_peak = 0;
    wl_columnar_relation_test_fail_next_prepare_resize();
    rc = run_diff_operator(&f, f.left, false, &out);
    wl_columnar_relation_test_fail_prepare_resize = false;
    if (rc == 0 || out || f.sess->join_output_peak != 63u
        || reserved_of(f.sess) != baseline
        || !no_reader_holds(f.right) || !no_reader_holds(f.left)) {
        printf("(mid-stream fault rc=%d) ", rc);
        ok = false;
    }
    if (out)
        col_rel_destroy(out);
    out = NULL;

    /* Run 2: the retry fails again, at the producer's first allocation. */
    wl_columnar_diff_join_batch_test_fail_allocation_at(1u);
    rc = run_diff_operator(&f, f.left, false, &out);
    wl_columnar_diff_join_batch_test_fail_allocation_at(0u);
    if (ok && (rc == 0 || out || reserved_of(f.sess) != baseline
        || !no_reader_holds(f.right) || !no_reader_holds(f.left))) {
        printf("(retry fault rc=%d) ", rc);
        ok = false;
    }
    if (out)
        col_rel_destroy(out);
    out = NULL;

    /* Run 3: clean. */
    rc = run_diff_operator(&f, f.left, false, &out);
    if (ok && (rc != 0 || !out || !nested_loop_equal(f.left, f.right, out))) {
        printf("(clean run rc=%d) ", rc);
        ok = false;
    }
    if (out)
        col_rel_destroy(out);
    if (ok && reserved_of(f.sess) != baseline) {
        printf("(reserved after clean run) ");
        ok = false;
    }
    if (ok)
        PASS();
    else
        FAIL("fault, retry or reuse");
    fixture_fini(&f);
}

/* Operator-level counterpart for the ordinary join: after a warm-up run, a
 * mid-stream output growth fault (after the nine 7-row batches that fill the
 * 64-row output), a second fault on the retry, then a clean run, all on one
 * session.  Reserved bytes return to the warmed baseline after each
 * failure. */
static void
test_operator_fault_during_retry(void)
{
    fixture_t f;
    const int64_t keys[] = { 1, 1, 2 };
    eval_stack_t stack;
    eval_entry_t result;
    col_rel_t *out = NULL;
    uint64_t baseline = 0;
    int rc;
    bool ok = true;

    TEST("ordinary join faults twice, then reuses the session exactly");
    if (!fixture_init(&f, 1u << 24, keys, 3, 3, 300)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    f.sess->join_batch_bytes = 7u * 32u;
    /* run 0 warms up, runs 1 and 2 fault, run 3 is clean */
    for (int run = 0; run < 4 && ok; run++) {
        bool fault = run == 1 || run == 2;

        f.sess->join_output_peak = 0;
        if (fault)
            wl_columnar_relation_test_fail_next_prepare_resize();
        eval_stack_init(&stack);
        eval_stack_push(&stack, f.left, false);
        rc = col_op_join(&f.op, &stack, f.sess);
        wl_columnar_relation_test_fail_prepare_resize = false;
        out = NULL;
        if (rc == 0 && stack.top > 0) {
            result = eval_stack_pop(&stack);
            free(result.seg_boundaries);
            if (result.owned)
                out = result.rel;
        }
        eval_stack_drain(&stack);
        if (fault) {
            if (rc == 0 || out || f.sess->join_output_peak != 63u) {
                printf("(run %d rc=%d peak=%" PRIu64 ") ", run, rc,
                    (uint64_t)f.sess->join_output_peak);
                ok = false;
            }
        } else if (rc != 0 || !out
            || !nested_loop_equal(f.left, f.right, out)) {
            printf("(run %d rc=%d) ", run, rc);
            ok = false;
        }
        /* The warm-up ran the bounded producer, not a recorded fallback. */
        if (run == 0 && (f.sess->join_batch_fallback_count != 0 || !out
            || f.sess->join_output_peak != 900u)) {
            printf("(warm-up was not bounded) ");
            ok = false;
        }
        if (out)
            col_rel_destroy(out);
        if (run == 0)
            baseline = reserved_of(f.sess);
        else if (ok && reserved_of(f.sess) != baseline) {
            printf("(run %d reserved) ", run);
            ok = false;
        }
    }
    if (ok)
        PASS();
    else
        FAIL("fault, retry or reuse");
    fixture_fini(&f);
}

/* (5) Mutating either input between batches makes the cursor stale and
 * leaves the output untouched. */
static void
test_stale_inputs_are_rejected(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1 };
    wl_columnar_continuation_t *cont = NULL;
    wl_columnar_continuation_sink_t sink;
    col_join_batch_relation_sink_t sctx;
    int64_t extra[] = { 1, 999 };
    const col_arr_entry_t *entry;
    const col_arrangement_t *arr;
    col_join_batch_cursor_t before;
    col_join_batch_cursor_t after;
    wl_columnar_continuation_cursor_t raw_before = { 0 };
    wl_columnar_continuation_cursor_t raw_after = { 0 };
    int64_t output_before[16];
    uint64_t output_view_generation;
    uint32_t generation;
    wl_columnar_continuation_status_t st;

    TEST("appending to the right relation between batches reports STALE");
    if (!fixture_init(&f, 1u << 24, keys, 2, 2, 10)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    if (col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
        KEY0, 1, 4u * 32u, &cont) != 0
        || col_join_batch_relation_sink_init(&sctx, &sink, f.sess, f.out)
        != 0) {
        FAIL("create or sink init");
        fixture_fini(&f);
        return;
    }
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_OK || f.out->nrows != 4u
        || !col_join_batch_cursor_get(cont, &before)
        || !wl_columnar_continuation_cursor(cont)) {
        FAIL("first batch");
        goto out;
    }
    raw_before = *wl_columnar_continuation_cursor(cont);
    for (uint32_t row = 0; row < f.out->nrows; row++)
        for (uint32_t col = 0; col < f.out->ncols; col++)
            output_before[row * 4u + col] = col_rel_get(f.out, row, col);
    output_view_generation = f.out->view_generation;
    entry = find_entry(f.sess, "right");
    arr = entry ? &entry->arr : NULL;
    generation = arr ? arr->generation : 0u;
    if (col_rel_append_row(f.right, extra) != 0) {
        FAIL("append");
        goto out;
    }
    col_session_invalidate_arrangements(&f.sess->base, "right");
    if (!entry || entry->pin_count != 1u || !entry->rebuild_deferred) {
        FAIL("pinned arrangement invalidation was not deferred");
        goto out;
    }
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_STALE || f.out->nrows != 4u
        || !col_join_batch_cursor_get(cont, &after)
        || !same_join_batch_cursor(&after, &before)
        || !wl_columnar_continuation_cursor(cont)) {
        FAIL("stale right relation was not rejected");
        goto out;
    }
    raw_after = *wl_columnar_continuation_cursor(cont);
    if (!same_continuation_cursor(&raw_after, &raw_before)
        || f.out->view_generation != output_view_generation) {
        FAIL("stale right relation was not rejected");
        goto out;
    }
    for (uint32_t row = 0; row < f.out->nrows; row++)
        for (uint32_t col = 0; col < f.out->ncols; col++)
            if (output_before[row * 4u + col] != col_rel_get(f.out, row,
                col)) {
                FAIL("stale publish changed visible output rows");
                goto out;
            }
    wl_columnar_continuation_destroy(cont);
    cont = NULL;
    if (entry->pin_count != 0u || entry->rebuild_deferred
        || entry->arr.indexed_rows != 0u) {
        FAIL("destroy did not transition deferred invalidation to unbuilt");
        goto out;
    }
    arr = col_session_get_arrangement(&f.sess->base, "right", KEY0, 1);
    if (!arr || arr != &entry->arr || entry->pin_count != 0u
        || entry->rebuild_deferred || arr->indexed_rows != f.right->nrows
        || arr->generation == 0u || arr->generation == generation) {
        FAIL("lazy arrangement lookup did not rebuild after release");
        goto out;
    }
    PASS();
out:
    wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* Eviction between healthy batches must preserve the producer's leases. */
static void
test_suspended_producer_survives_eviction(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1 };
    wl_columnar_continuation_t *cont = NULL;
    wl_columnar_continuation_sink_t sink;
    col_join_batch_relation_sink_t sctx;
    col_rel_t *cold = NULL;
    col_rel_t *pressure = NULL;

    TEST("eviction preserves a suspended producer until its final release");
    if (!fixture_init(&f, 1u << 24, keys, 2, 2, 20)) {
        FAIL("fixture");
        goto out;
    }
    f.sess->arr_cache_limit_bytes = SIZE_MAX;
    cold = make_right_named("cold", 1, 2);
    pressure = make_right_named("pressure", 1, 2);
    if (!cold || !pressure || session_add_rel(f.sess, cold) != 0) {
        FAIL("eviction control relations");
        goto out;
    }
    cold = NULL; /* session owns it */
    if (session_add_rel(f.sess, pressure) != 0) {
        FAIL("pressure registration");
        goto out;
    }
    pressure = NULL;
    if (!col_session_get_arrangement(&f.sess->base, "cold", KEY0, 1)
        || col_join_batch_producer_create(f.sess, &f.op, f.left, false,
        KEY0, KEY0, 1, 4u * 32u, &cont) != 0
        || col_join_batch_relation_sink_init(&sctx, &sink, f.sess, f.out) != 0
        || wl_columnar_continuation_publish(cont, &sink)
        != WL_COLUMNAR_CONTINUATION_OK) {
        FAIL("first batch and unpinned control");
        goto out;
    }
    const col_arr_entry_t *entry = find_entry(f.sess, "right");
    const col_arr_entry_t *control = find_entry(f.sess, "cold");
    if (!entry || entry->pin_count != 1 || !entry->mem_bytes
        || !control || control->pin_count != 0 || !control->mem_bytes
        || f.out->nrows != 4) {
        FAIL("eviction prerequisites");
        goto out;
    }
    wl_columnar_continuation_cursor_t cursor =
        *wl_columnar_continuation_cursor(cont);
    uint64_t source_identity = f.right->relation_identity;
    uint64_t source_generation = f.right->storage_generation;
    const void *buckets = entry->arr.ht_head;
    size_t bytes = entry->mem_bytes;
    f.sess->arr_cache_limit_bytes = 1;
    /* Building another index runs normal LRU eviction. The cold index is
     * evicted/reused, while the producer's leased index must survive. */
    if (!col_session_get_arrangement(&f.sess->base, "pressure", KEY0, 1)) {
        FAIL("eviction trigger");
        goto out;
    }
    control = find_entry(f.sess, "cold");
    if ((control && control->mem_bytes != 0)
        || find_entry(f.sess, "right") != entry
        || entry->pin_count != 1 || !entry->evict_deferred
        || entry->arr.ht_head != buckets || entry->mem_bytes != bytes
        || f.right->relation_identity != source_identity
        || f.right->storage_generation != source_generation
        || !same_continuation_cursor(&cursor,
        wl_columnar_continuation_cursor(cont))) {
        FAIL("eviction did not distinguish pinned and unpinned indexes");
        goto out;
    }
    if (col_join_batch_run_to_relation(cont, f.sess, f.out) != 0
        || f.out->nrows != 40) {
        FAIL("resume did not complete");
        goto out;
    }
    bool seen[2][20] = { { false } };
    for (uint32_t row = 0; row < f.out->nrows; row++) {
        int64_t key = col_rel_get(f.out, row, 0);
        int64_t payload = col_rel_get(f.out, row, 3);
        int64_t offset = payload - key * 1000;
        if (key < 0 || key >= 2 || offset < 0 || offset >= 20
            || col_rel_get(f.out, row, 1) != key
            || col_rel_get(f.out, row, 2) != key || seen[key][offset]) {
            FAIL("resumed output differs from the independent 40-row oracle");
            goto out;
        }
        seen[key][offset] = true;
    }
    wl_columnar_continuation_destroy(cont);
    cont = NULL;
    if (entry->pin_count != 0 || entry->evict_deferred
        || entry->mem_bytes != 0 || entry->arr.ht_head != NULL) {
        FAIL("final release did not allow deferred eviction");
        goto out;
    }
    PASS();
out:
    wl_columnar_continuation_destroy(cont);
    col_rel_destroy(cold);
    col_rel_destroy(pressure);
    fixture_fini(&f);
}

/* A collision control is separate from the eviction/lifetime control. */
static void
test_true_cross_key_collision_resumes(void)
{
    fixture_t f;
    const col_arr_entry_t *entry;
    const col_arrangement_t *arr;
    col_rel_t *oracle = NULL;
    wl_columnar_continuation_t *cont = NULL;
    wl_columnar_continuation_sink_t sink;
    col_join_batch_relation_sink_t sctx;
    col_join_batch_cursor_t cur;
    wl_columnar_continuation_status_t st;
    int64_t left_keys[2];
    int64_t match_key;
    int64_t other_key;
    uint32_t bucket;
    bool parked_in_bucket = false;
    bool done = false;

    TEST("true cross-key bucket collision parks and resumes exactly");
    memset(&f, 0, sizeof(f));
    f.sess = make_session(1u << 24);
    f.right = make_hashed_right(512, 2);
    if (!f.sess || !f.right) {
        FAIL("fixture");
        if (!f.sess && f.right) {
            col_rel_destroy(f.right);
            f.right = NULL;
        }
        fixture_fini(&f);
        return;
    }
    if (session_add_rel(f.sess, f.right) != 0) {
        FAIL("register right relation");
        col_rel_destroy(f.right);
        f.right = NULL;
        fixture_fini(&f);
        return;
    }
    f.right_registered = true;
    f.right = session_find_rel(f.sess, "right");
    f.lkeys[0] = "k";
    f.rkeys[0] = "k";
    init_join_op(&f.op, f.lkeys, f.rkeys);
    arr = col_session_get_arrangement(&f.sess->base, "right", KEY0, 1);
    entry = find_entry(f.sess, "right");
    if (!arr || !entry || !find_true_collision(arr, f.right, &match_key,
        &other_key, &bucket)) {
        FAIL("fixture did not contain an inspectable true collision");
        fixture_fini(&f);
        return;
    }
    left_keys[0] = match_key;
    left_keys[1] = other_key;
    f.left = make_left(left_keys, 2);
    f.out = col_rel_new_auto("$join", 4);
    if (!f.left || !f.out || col_join_set_output_types(f.out, f.left,
        f.right, &f.op) != 0) {
        FAIL("collision output fixture");
        fixture_fini(&f);
        return;
    }
    oracle = run_oracle(f.sess, f.left, &f.op);
    if (!oracle || col_join_batch_producer_create(f.sess, &f.op, f.left,
        false, KEY0, KEY0, 1, 32u, &cont) != 0
        || col_join_batch_relation_sink_init(&sctx, &sink, f.sess, f.out)
        != 0) {
        FAIL("collision producer or sink");
        goto out;
    }
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_OK || f.out->nrows != 1u
        || !col_join_batch_cursor_get(cont, &cur) || cur.next.lr != 0u
        || cur.next.rr == UINT32_MAX) {
        FAIL("first batch did not park inside the collision chain");
        goto out;
    }
    {
        uint32_t row;
        if (arrangement_bucket_head(arr, bucket, &row))
            for (; row != UINT32_MAX; row = arr->ht_next[row])
                if (row == cur.next.rr)
                    parked_in_bucket = true;
    }
    if (!parked_in_bucket) {
        FAIL("cursor did not remain in the selected collision bucket");
        goto out;
    }
    while (st == WL_COLUMNAR_CONTINUATION_OK)
        st = wl_columnar_continuation_publish(cont, &sink);
    done = st == WL_COLUMNAR_CONTINUATION_DONE && f.out->nrows == 4u
        && same_rows(oracle, f.out);
    if (!done)
        FAIL("collision chain did not resume to the one-shot result");
    else
        PASS();
out:
    (void)bucket;
    if (oracle)
        col_rel_destroy(oracle);
    if (cont)
        wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* (#1481) Lazy scratch sizing.  Two things need proving and they are
 * separate: that a small join under a huge knob no longer reserves the knob,
 * and that a join which DOES cross the 64-row floor still produces the right
 * rows after the scratch is reallocated underneath it.
 *
 * The second is the one with no prior coverage.  Every other oracle
 * comparison in this file runs at 1..16 rows per batch, all below the floor,
 * so none of them ever grows.  A grow that forgets to publish nrows first
 * copies zero rows into the new columns and leaves the written rows pointing
 * at uninitialised malloc memory -- a silently wrong join that ASan and UBSan
 * both pass.  Only comparing content across a grow catches it. */
static void
test_lazy_scratch_growth(void)
{
    static const uint32_t knobs[] = { 200u, 512u, 1000u, 4096u };
    fixture_t f;
    const int64_t keys[] = { 1, 2, 3 };
    const int64_t k0[] = { 0 };
    wl_columnar_continuation_t *cont = NULL;
    col_rel_t *oracle;
    uint32_t cap0;
    uint32_t capN;
    int rc;
    size_t i;

    for (i = 0; i < sizeof(knobs) / sizeof(knobs[0]); i++) {
        TEST("a join crossing the 64-row floor matches the one-shot oracle");
        /* 3 left rows x 5 keys x 47 fanout: enough matches to cross the
         * floor several times at every knob below. */
        if (!fixture_init(&f, 1u << 24, keys, 3, 5, 47)) {
            FAIL("fixture");
            fixture_fini(&f);
            continue;
        }
        rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false,
                KEY0, KEY0, 1, (uint64_t)knobs[i] * 32u, &cont);
        if (rc != 0 || col_join_batch_rows_per_batch(cont) != knobs[i]) {
            FAIL("producer create or rows_per_batch");
            if (cont)
                wl_columnar_continuation_destroy(cont);
            fixture_fini(&f);
            continue;
        }
        cap0 = col_join_batch_scratch_capacity(cont);
        rc = col_join_batch_run_to_relation(cont, f.sess, f.out);
        capN = col_join_batch_scratch_capacity(cont);
        oracle = run_oracle(f.sess, f.left, &f.op);
        if (rc != 0 || !oracle || !same_rows(oracle, f.out)) {
            FAIL("result differs from the one-shot oracle across a grow");
        } else if (cap0 != COL_REL_INIT_CAP || capN <= cap0) {
            /* If the scratch never grew, the oracle comparison above proves
             * nothing about growth and this case is worthless. */
            FAIL("the scratch did not start at the floor and grow past it");
        } else {
            PASS();
        }
        if (oracle)
            col_rel_destroy(oracle);
        wl_columnar_continuation_destroy(cont);
        fixture_fini(&f);
    }

    /* A grow that FAILS must still produce every row.  Without this the
     * park choice is dead code: growth succeeds in every case above, so
     * nothing exercises the short-batch path at all -- a mutant that parks
     * on the NEXT candidate instead of the unwritten one survives the whole
     * file.  The one-shot resize hook fails exactly one grow, so the
     * producer emits one short batch, parks, and resumes; if it parked past
     * the candidate it was about to write, that row is silently lost and
     * the oracle comparison is what notices.
     *
     * This is also the case the resize hook's other user (see the comment at
     * test_sink_prepare_resize_failure) must not be confused with: there the
     * hook is meant for the SINK, and it only reaches the sink because that
     * batch is 16 rows and the scratch never resizes. */
    TEST("a denied scratch grow emits a short batch without losing a row");
    if (!fixture_init(&f, 1u << 24, keys, 3, 5, 47)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 512u * 32u, &cont);
    if (rc != 0) {
        FAIL("producer create");
        fixture_fini(&f);
        return;
    }
    wl_columnar_relation_test_fail_next_prepare_resize();
    rc = col_join_batch_run_to_relation(cont, f.sess, f.out);
    /* The hook is one-shot and global.  This run consumes it -- the capN
     * assertion below proves the producer's grow took it -- but clear it
     * unconditionally so an edit that stops consuming it cannot leak a
     * refusal into the exact-fit case that follows. */
    wl_columnar_relation_test_clear_prepare_resize();
    capN = col_join_batch_scratch_capacity(cont);
    oracle = run_oracle(f.sess, f.left, &f.op);
    if (rc != 0 || !oracle || !same_rows(oracle, f.out)) {
        FAIL("a failed grow lost or duplicated a row");
    } else if (f.out->nrows != 3u * 47u) {
        FAIL("wrong row count after a failed grow");
    } else if (capN != COL_REL_INIT_CAP * 2u) {
        /* The first grow was refused, so the scratch stayed at the floor
         * for that batch; the next batch retried and succeeded once. */
        FAIL("the scratch did not retry the grow on the following batch");
    } else {
        PASS();
    }
    if (oracle)
        col_rel_destroy(oracle);
    wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);

    /* The issue's own acceptance criterion, in its own numbers.  k0 rather
     * than keys: make_right generates keys 0..right_keys-1, so a left key of
     * 1 against one right key joins nothing -- and same_rows(empty, empty) is
     * true, so without the nrows check below this case would pass while
     * testing nothing at all.
     *
     * The criterion is measured DIFFERENTIALLY: the same fixture is created
     * twice, once under a knob that fits in the 64-row floor and once under
     * the issue's 1 GiB knob, and the two governor deltas must be equal.
     * Everything else create reserves -- the arrangement lease, the producer
     * descriptor -- is identical across the pair, so it cancels without the
     * test having to know what those things are or what they cost.  An
     * earlier form subtracted the arrangement's bytes by name and broke the
     * first time create grew a third reservation; this cannot.
     *
     * Restoring the eager reservation is caught here, but by the create
     * guard rather than by the delta: 1 GiB exceeds the fixture's budget, so
     * create is denied and returns non-zero first.  The delta comparison is
     * what catches a knob-dependent reservation small enough to be admitted,
     * which is the class it exists for. */
    TEST("a 10-row join under a 1 GiB knob reserves the floor, not the knob");
    {
        uint64_t small_delta = 0;
        uint64_t huge_delta = 0;
        int k;

        for (k = 0; k < 2; k++) {
            uint64_t knob = k ? 1073741824ull : 32ull * 8ull;
            uint64_t before;
            uint64_t after;

            if (!fixture_init(&f, 1u << 24, k0, 1, 1, 10)) {
                FAIL("fixture");
                fixture_fini(&f);
                return;
            }
            before = reserved_of(f.sess);
            rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false,
                    KEY0, KEY0, 1, knob, &cont);
            after = reserved_of(f.sess);
            if (rc != 0) {
                FAIL("producer create");
                fixture_fini(&f);
                return;
            }
            if (k && col_join_batch_rows_per_batch(cont) != 1073741824u / 32u) {
                /* Without this the pair would agree just as well under two
                 * small knobs, and would not be testing the criterion. */
                FAIL("create did not settle on the knob's rows_per_batch");
                wl_columnar_continuation_destroy(cont);
                fixture_fini(&f);
                return;
            }
            if (k)
                huge_delta = after - before;
            else
                small_delta = after - before;
            if (k) {
                rc = col_join_batch_run_to_relation(cont, f.sess, f.out);
                capN = col_join_batch_scratch_capacity(cont);
                oracle = run_oracle(f.sess, f.left, &f.op);
            }
            wl_columnar_continuation_destroy(cont);
            cont = NULL;
            if (!k)
                fixture_fini(&f);
        }
        if (rc != 0 || !oracle || !same_rows(oracle, f.out)
            || f.out->nrows != 10u) {
            FAIL("result differs from the one-shot oracle");
        } else if (huge_delta != small_delta) {
            FAIL("the 1 GiB knob reserved more at create than a tiny knob");
        } else if (capN > 2u * f.out->nrows + COL_REL_INIT_CAP) {
            /* One reading after the run is the PEAK, not just the final
             * value: col_rel_reserve_capacity_admitted never shrinks
             * capacity.  Sampling mid-run would be no stronger and would
             * couple the case to the batch schedule. */
            FAIL("scratch exceeded twice the output plus the floor");
        } else {
            PASS();
        }
        if (oracle)
            col_rel_destroy(oracle);
        fixture_fini(&f);
    }

}

/* (6) Exact-fit admission: the budget that exactly covers the first batch
 * growth admits it; one byte less is denied by the governor with every
 * observable value unchanged. */
static void
test_exact_fit_and_one_byte_over(void)
{
    for (int deny = 0; deny < 2; deny++) {
        fixture_t f;
        const int64_t keys[] = { 0 };
        wl_columnar_continuation_t *cont = NULL;
        wl_columnar_continuation_sink_t sink;
        col_join_batch_relation_sink_t sctx;
        col_join_batch_cursor_t cur;
        uint64_t r0;
        uint64_t before;
        uint64_t after;
        uint64_t live_before;
        uint64_t budget;
        uint32_t pins_before;
        const col_arr_entry_t *entry;
        wl_columnar_continuation_status_t st;
        bool ok;

        TEST(deny ? "one byte below exact fit is denied, nothing changes"
                  : "exact-fit budget admits the first batch growth");
        /* 100 rows per batch > the 64 rows a fresh relation pre-allocates,
         * so the first batch really grows the output. */
        if (!fixture_init(&f, 1u << 24, keys, 1, 1, 300)) {
            FAIL("fixture");
            fixture_fini(&f);
            continue;
        }
        if (col_join_batch_producer_create(f.sess, &f.op, f.left, false,
            KEY0, KEY0, 1, 100u * 32u, &cont) != 0
            || col_join_batch_relation_sink_init(&sctx, &sink, f.sess,
            f.out) != 0) {
            FAIL("create or sink init");
            fixture_fini(&f);
            continue;
        }
        r0 = reserved_of(f.sess);
        if (!col_rel_retained_bytes_for(f.out, f.out->capacity, &before)
            || !col_rel_retained_live_bytes(f.out, &live_before)
            || live_before != f.out->retained_reserved_bytes
            || !col_rel_retained_bytes_for(f.out, 100u, &after)) {
            FAIL("sink init did not admit the existing capacity");
            wl_columnar_continuation_destroy(cont);
            fixture_fini(&f);
            continue;
        }
        /* Growth is reserved with the old buffers still live, so the
         * budget must hold r0 plus the whole new footprint.  A governor's
         * budget is fixed at creation, so rebuild the fixture under the
         * computed budget; every value above is deterministic. */
        /* TWO admissions since #1481, not one, and they serialise.  The
         * producer's scratch is no longer sized at create: it starts at the
         * 64-row floor and must itself grow 64 -> 100 inside this publish,
         * before the sink reserves.  The scratch peaks at r0 + after and
         * settles at r0 + after - before; the sink then peaks on top of
         * that.  So the exact fit is one `after` higher than it was, and one
         * byte below it denies the SINK -- the scratch grow has already been
         * admitted by then. */
        budget = r0 + 2u * after - before
            + (uint64_t)f.out->ncols * sizeof(int64_t *)
            - (uint64_t)deny;
        wl_columnar_continuation_destroy(cont);
        fixture_fini(&f);
        if (!fixture_init(&f, budget, keys, 1, 1, 300)
            || col_join_batch_producer_create(f.sess, &f.op, f.left, false,
            KEY0, KEY0, 1, 100u * 32u, &cont) != 0
            || col_join_batch_relation_sink_init(&sctx, &sink, f.sess,
            f.out) != 0) {
            FAIL("fixture under the exact budget");
            if (cont)
                wl_columnar_continuation_destroy(cont);
            fixture_fini(&f);
            continue;
        }
        entry = find_entry(f.sess, "right");
        pins_before = entry ? entry->pin_count : 0u;
        r0 = reserved_of(f.sess);
        st = wl_columnar_continuation_publish(cont, &sink);
        ok = col_join_batch_cursor_get(cont, &cur);
        if (deny) {
            ok = ok && st == WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED
                && f.out->nrows == 0u && cur.sequence == 0u
                && cur.next.lr == 0u && cur.next.rr == UINT32_MAX
                /* The scratch grow landed before the sink was refused, so
                 * a denied publish no longer leaves the session counter
                 * exactly as it found it.  Everything the CALLER can see --
                 * rows, cursor, output reservation, pin -- is unchanged; the
                 * narrowing is to the governor total alone. */
                && reserved_of(f.sess) == r0 + after - before
                && f.out->retained_reserved_bytes == live_before
                && entry && entry->pin_count == pins_before;
        } else {
            uint64_t now;
            ok = ok && st == WL_COLUMNAR_CONTINUATION_OK
                && f.out->nrows == 100u && cur.sequence == 1u
                && col_rel_retained_live_bytes(f.out, &now)
                && now == f.out->retained_reserved_bytes
                /* Both grows settled: the scratch's and the sink's. */
                && reserved_of(f.sess) == r0 + 2u * (after - before);
        }
        if (!ok)
            FAIL(deny ?
                "denial changed visible state or was not a governor verdict"
                      : "exact fit was not admitted or the token drifted");
        else
            PASS();
        wl_columnar_continuation_destroy(cont);
        fixture_fini(&f);
    }
}

/* (7) Leases: pin_count returns to its pre-create value after destroy on
 * success, after cancel following a committed batch (which keeps only the
 * committed rows and makes further publishes INVALID), and after error. */
static void
test_lease_released_on_every_path(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1 };
    wl_columnar_continuation_t *cont = NULL;
    wl_columnar_continuation_sink_t sink;
    col_join_batch_relation_sink_t sctx;
    const col_arr_entry_t *entry;
    uint32_t base_pins;
    uint64_t reservation_base;
    uint64_t producer_live_reservation;
    uint64_t min_batch_bytes;
    wl_columnar_continuation_status_t st;
    int rc;

    TEST("arrangement lease is released once on success, cancel and error");
    if (!fixture_init(&f, 1u << 24, keys, 2, 2, 20)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    if (!col_session_get_arrangement(&f.sess->base, "right", KEY0, 1u)) {
        FAIL("arrangement warmup");
        fixture_fini(&f);
        return;
    }
    reservation_base = reserved_of(f.sess);
    /* Success path. */
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 8u * 32u, &cont);
    entry = find_entry(f.sess, "right");
    if (rc != 0 || !entry || entry->pin_count != 1u) {
        FAIL("create did not take exactly one lease");
        goto out;
    }
    if (reserved_of(f.sess) <= reservation_base) {
        FAIL("producer descriptor and batch were not admitted");
        goto out;
    }
    producer_live_reservation = reserved_of(f.sess);
    if (!col_rel_retained_bytes_for(f.out, 64u, &min_batch_bytes)
        || producer_live_reservation - reservation_base <= min_batch_bytes) {
        FAIL("producer descriptor admission was missing beside batch storage");
        goto out;
    }
    base_pins = entry->pin_count - 1u;
    rc = col_join_batch_run_to_relation(cont, f.sess, f.out);
    uint64_t output_reservation = f.out->retained_reserved_bytes
        + f.out->descriptor_reserved_bytes
        + f.out->metadata_reserved_bytes;
    wl_columnar_continuation_destroy(cont);
    cont = NULL;
    if (rc != 0 || entry->pin_count != base_pins || f.out->nrows != 40u
        || reserved_of(f.sess) != reservation_base + output_reservation) {
        FAIL("success path leaked a lease");
        goto out;
    }
    /* Cancel after one committed batch. */
    f.out->nrows = 0;
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 8u * 32u, &cont);
    if (rc != 0 || col_join_batch_relation_sink_init(&sctx, &sink, f.sess,
        f.out) != 0) {
        FAIL("second create");
        goto out;
    }
    st = wl_columnar_continuation_publish(cont, &sink);
    uint64_t cancel_live_reservation = reserved_of(f.sess);
    wl_columnar_continuation_cancel(cont);
    if (st != WL_COLUMNAR_CONTINUATION_OK || f.out->nrows != 8u
        || entry->pin_count != base_pins
        || reserved_of(f.sess) != cancel_live_reservation
        || wl_columnar_continuation_publish(cont, &sink)
        != WL_COLUMNAR_CONTINUATION_INVALID || f.out->nrows != 8u) {
        FAIL("cancel after a committed batch");
        goto out;
    }
    wl_columnar_continuation_destroy(cont);
    cont = NULL;
    if (entry->pin_count != base_pins
        || reserved_of(f.sess) != reservation_base
        + f.out->retained_reserved_bytes + f.out->descriptor_reserved_bytes
        + f.out->metadata_reserved_bytes) {
        FAIL("destroy after cancel leaked or double-released producer state");
        goto out;
    }
    /* Cancellation before the first batch also releases the pin immediately,
    * while the producer-owned descriptor reservation lives until destroy. */
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 8u * 32u, &cont);
    if (rc != 0 || entry->pin_count != base_pins + 1u) {
        FAIL("create before cancel");
        goto out;
    }
    cancel_live_reservation = reserved_of(f.sess);
    wl_columnar_continuation_cancel(cont);
    if (entry->pin_count != base_pins
        || reserved_of(f.sess) != cancel_live_reservation) {
        FAIL("cancel before first batch changed producer reservation lifetime");
        goto out;
    }
    wl_columnar_continuation_destroy(cont);
    cont = NULL;
    if (reserved_of(f.sess) != reservation_base
        + f.out->retained_reserved_bytes + f.out->descriptor_reserved_bytes
        + f.out->metadata_reserved_bytes) {
        FAIL("destroy after pre-batch cancel leaked producer reservation");
        goto out;
    }
    ((col_arr_entry_t *)entry)->pin_count = UINT32_MAX;
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 8u * 32u, &cont);
    ((col_arr_entry_t *)entry)->pin_count = base_pins;
    if (rc != EOVERFLOW || cont != NULL
        || reserved_of(f.sess) != reservation_base
        + f.out->retained_reserved_bytes + f.out->descriptor_reserved_bytes
        + f.out->metadata_reserved_bytes) {
        FAIL("pin overflow leaked producer descriptor admission");
        goto out;
    }
    /* Error path: a budget that admits the arrangement but not the scratch
     * relation denies create; the lease and the descriptor reservation must
     * be gone, leaving only the arrangement's own reservation. */
    {
        /* Leave one byte beyond the already-admitted arrangement so the
         * producer descriptor reservation is denied before its allocations. */
        wl_col_session_t *tight = make_session(64ull * 1024 * 1024);
        col_rel_t *right2 = make_right(2, 20);
        const col_arr_entry_t *e2;
        if (!tight || !right2) {
            FAIL("tight session");
            if (right2)
                col_rel_destroy(right2);
            destroy_session(tight);
            goto out;
        }
        if (session_add_rel(tight, right2) != 0) {
            FAIL("tight relation registration");
            col_rel_destroy(right2);
            destroy_session(tight);
            goto out;
        }
        if (!col_session_get_arrangement(&tight->base, "right", KEY0, 1u)) {
            FAIL("tight arrangement warmup");
            destroy_session(tight);
            goto out;
        }
        uint64_t tight_base = reserved_of(tight);
        atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
                tight->memory_governor)->usable_bytes, tight_base + 1u,
            memory_order_release);
        rc = col_join_batch_producer_create(tight, &f.op, f.left, false,
                KEY0, KEY0, 1, 8u * 32u, &cont);
        e2 = find_entry(tight, "right");
        if (rc != ENOSPC || !tight->memory_budget_denied || cont != NULL
            || (e2 && e2->pin_count != 0u)
            || reserved_of(tight) != tight_base) {
            FAIL(
                "descriptor denial lacked a typed result or left state behind");
            destroy_session(tight);
            goto out;
        }
        destroy_session(tight);
    }
    /* A denied create in the tight session above did not poison an equivalent
     * arrangement: a later adequately budgeted create succeeds and cleans up. */
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 8u * 32u, &cont);
    if (rc != 0 || !cont || entry->pin_count != base_pins + 1u) {
        FAIL("create after denial against an unchanged arrangement");
        goto out;
    }
    wl_columnar_continuation_destroy(cont);
    cont = NULL;
    if (entry->pin_count != base_pins
        || reserved_of(f.sess) != reservation_base
        + f.out->retained_reserved_bytes + f.out->descriptor_reserved_bytes
        + f.out->metadata_reserved_bytes) {
        FAIL("post-denial create leaked its pin or producer reservation");
        goto out;
    }
    PASS();
out:
    if (cont)
        wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* The producer's descriptor reservation grows by exactly the additional
 * key-index bytes and right-side probe-row bytes.  Subtract the independently
 * computed batch relation storage, descriptor, and metadata reservations so
 * this check isolates the producer descriptor for narrow and wide schemas. */
static uint64_t
auto_metadata_bytes(uint32_t ncols)
{
    uint64_t bytes = 3u + (uint64_t)ncols
        * (sizeof(char *) + sizeof(struct ArrowSchema *)
        + sizeof(struct ArrowSchema) + 2u);
    for (uint32_t i = 0; i < ncols; i++) {
        char name[32];
        snprintf(name, sizeof(name), "col%u", i);
        bytes += 2u * (strlen(name) + 1u);
    }
    return bytes;
}

static bool
descriptor_footprint(uint32_t key_count, uint32_t right_ncols,
    uint64_t *bytes_out)
{
    wl_col_session_t *sess = NULL;
    col_rel_t *right = NULL;
    col_rel_t *left = NULL;
    col_rel_t *out = NULL;
    wl_plan_op_t op;
    const char *left_names[3] = { "k0", "k1", "lv" };
    const char *right_names[8];
    char right_name_storage[8][16];
    const char *left_keys[2] = { "k0", "k1" };
    const char *right_keys[2] = { "k0", "k1" };
    uint32_t lk[2] = { 0u, 1u };
    uint32_t rk[2] = { 0u, 1u };
    int64_t left_row[3] = { 1, 2, 3 };
    int64_t right_row[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    uint64_t batch_bytes;
    uint64_t batch_payload;
    uint64_t batch_descriptor;
    uint64_t batch_metadata;
    uint64_t baseline;
    uint64_t batch_budget;
    wl_columnar_continuation_t *cont = NULL;
    bool ok = false;

    if (key_count == 0 || key_count > 2 || right_ncols < key_count
        || right_ncols > 8 || !bytes_out)
        return false;
    for (uint32_t i = 0; i < right_ncols; i++) {
        const char *prefix = i < key_count ? "k" : "r";
        (void)snprintf(right_name_storage[i], sizeof(right_name_storage[i]),
            "%s%u", prefix, i);
        right_names[i] = right_name_storage[i];
    }
    sess = make_session(1u << 24);
    right = make_rel("right", right_ncols, right_names);
    left = make_rel("left", 3, left_names);
    if (!sess || !right || !left
        || col_rel_append_row(right, right_row) != 0
        || col_rel_append_row(left, left_row) != 0
        || session_add_rel(sess, right) != 0)
        goto done;
    right = NULL;
    memset(&op, 0, sizeof(op));
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = key_count;
    op.left_keys = left_keys;
    op.right_keys = right_keys;
    op.delta_mode = WL_DELTA_FORCE_FULL;
    if (!col_session_get_arrangement(&sess->base, "right", rk, key_count))
        goto done;
    out = col_rel_new_auto("$shape", col_join_output_width(left,
            sess->rels[0], &op));
    if (!out || col_join_set_output_types(out, left, sess->rels[0], &op) != 0
        || !col_rel_retained_bytes_for(out, 64u, &batch_bytes))
        goto done;
    batch_payload = batch_bytes + (uint64_t)out->ncols
        * (sizeof(int64_t *) + sizeof(int64_t));
    batch_descriptor = sizeof(col_rel_t) + sizeof("$join_batch");
    batch_metadata = 3u + (uint64_t)out->ncols
        * (sizeof(char *) + sizeof(wirelog_column_type_t)
        + sizeof(struct ArrowSchema *) + sizeof(struct ArrowSchema)
        + 2u);
    for (uint32_t i = 0; i < out->ncols; i++)
        batch_metadata += 2u * (strlen(out->col_names[i]) + 1u);
    batch_budget = batch_bytes;
    baseline = reserved_of(sess);
    int create_rc = col_join_batch_producer_create(sess, &op, left, false,
            lk, rk, key_count, batch_budget, &cont);
    if (create_rc != 0 || !cont || reserved_of(sess) <= baseline
        + batch_payload + batch_descriptor + batch_metadata)
        goto done;
    *bytes_out = reserved_of(sess) - baseline - batch_payload
        - batch_descriptor - batch_metadata;
    ok = *bytes_out >= (uint64_t)sizeof(uint32_t) * key_count * 2u
        + (uint64_t)sizeof(int64_t) * right_ncols;

done:
    if (cont)
        wl_columnar_continuation_destroy(cont);
    if (out)
        col_rel_destroy(out);
    if (left)
        col_rel_destroy(left);
    if (right)
        col_rel_destroy(right);
    if (sess)
        destroy_session(sess);
    return ok;
}

static void
test_descriptor_reservation_shape(void)
{
    uint64_t narrow_bytes;
    uint64_t wide_bytes;

    TEST("producer admission includes multi-key and wide probe scratch");
    if (!descriptor_footprint(1u, 2u, &narrow_bytes)
        || !descriptor_footprint(2u, 8u, &wide_bytes)
        || wide_bytes - narrow_bytes != 6u * sizeof(int64_t)
        + 2u * sizeof(uint32_t)) {
        FAIL("producer reservation did not scale with key and probe arrays");
        return;
    }
    PASS();
}

static void
test_descriptor_uses_source_governor(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1 };
    wl_columnar_memory_governor_ref_t *source_governor = NULL;
    wl_columnar_memory_governor_ref_t *governor_ref;
    wl_columnar_memory_governor_ref_t *session_governor;
    wl_columnar_continuation_t *cont = NULL;
    uint64_t baseline;
    uint64_t admitted;
    uint64_t descriptor_bytes;
    uint64_t payload_bytes;
    uint64_t session_baseline;

    TEST(
        "producer descriptor uses left-source governor without session governor");
    if (!fixture_init(&f, 1u << 24, keys, 2, 2, 20)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    source_governor = make_governor(1u << 20);
    session_governor = f.sess->memory_governor;
    if (!descriptor_footprint(1u, 2u, &descriptor_bytes)
        || col_rel_enable_timestamps(f.left) != 0
        || col_rel_enable_timestamps(f.out) != 0
        || !col_rel_retained_bytes_for(f.out, COL_REL_INIT_CAP,
        &payload_bytes)
        || !source_governor
        || col_rel_attach_memory_governor(f.left, source_governor) != 0) {
        FAIL("source governor setup");
        goto out;
    }
    payload_bytes += (uint64_t)f.out->ncols
        * (sizeof(int64_t *) + sizeof(int64_t));
    baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(source_governor));
    session_baseline = reserved_of(f.sess);
    if (col_join_batch_producer_create(f.sess, &f.op, f.left, false,
        KEY0, KEY0, 1u, 8u * 32u, &cont) != 0 || !cont
        || reserved_of(f.sess) <= session_baseline
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(source_governor)) != baseline) {
        FAIL("session governor did not take precedence over left source");
        goto out;
    }
    wl_columnar_continuation_destroy(cont);
    cont = NULL;
    f.sess->memory_governor = NULL;
    if (col_join_batch_producer_create(f.sess, &f.op, f.left, false,
        KEY0, KEY0, 1u, 8u * 32u, &cont) != 0 || !cont) {
        FAIL("producer creation with source-only governor");
        goto out;
    }
    admitted = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(source_governor));
    if (admitted - baseline != descriptor_bytes + payload_bytes
        + sizeof(col_rel_t) + sizeof("$join_batch")
        + auto_metadata_bytes(f.out->ncols)
        + (uint64_t)f.out->ncols * sizeof(wirelog_column_type_t)) {
        FAIL("source governor did not charge the exact initial payload");
        goto out;
    }
    wl_columnar_continuation_cancel(cont);
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(source_governor)) != admitted) {
        FAIL("cancellation released producer storage credit early");
        goto out;
    }
    uint64_t producer_credit = admitted - baseline;
    governor_ref = source_governor;
    col_rel_destroy(f.left);
    f.left = NULL;
    wl_columnar_memory_governor_ref_release(source_governor);
    source_governor = NULL;
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(governor_ref))
        != producer_credit) {
        FAIL("producer did not retain the governor after source detach");
        goto out;
    }
    wl_columnar_continuation_destroy(cont);
    cont = NULL;
    PASS();
out:
    if (cont)
        wl_columnar_continuation_destroy(cont);
    f.sess->memory_governor = session_governor;
    if (source_governor)
        wl_columnar_memory_governor_ref_release(source_governor);
    fixture_fini(&f);
}

static void
test_batch_payload_uses_right_governor_fallback(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1 };
    wl_columnar_memory_governor_ref_t *source_governor = NULL;
    wl_columnar_memory_governor_ref_t *session_governor;
    wl_columnar_continuation_t *cont = NULL;
    col_rel_t *oracle = NULL;
    uint64_t baseline;
    uint64_t admitted;
    uint64_t descriptor_bytes;
    uint64_t payload_bytes;

    TEST("batch payload uses right-source governor fallback");
    if (!fixture_init(&f, 1u << 24, keys, 2, 2, 20)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    source_governor = make_governor(1u << 20);
    session_governor = f.sess->memory_governor;
    if (!descriptor_footprint(1u, 2u, &descriptor_bytes)
        || !col_rel_retained_bytes_for(f.out, COL_REL_INIT_CAP,
        &payload_bytes)
        || !source_governor
        || col_rel_attach_memory_governor(f.right, source_governor) != 0) {
        FAIL("right source governor setup");
        goto out;
    }
    payload_bytes += (uint64_t)f.out->ncols
        * (sizeof(int64_t *) + sizeof(int64_t));
    oracle = run_oracle(f.sess, f.left, &f.op);
    if (!oracle) {
        FAIL("oracle");
        goto out;
    }
    baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(source_governor));
    f.sess->memory_governor = NULL;
    if (col_join_batch_producer_create(f.sess, &f.op, f.left, false,
        KEY0, KEY0, 1u, 8u * 32u, &cont) != 0 || !cont) {
        FAIL("producer creation with right-only governor");
        goto out;
    }
    admitted = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(source_governor));
    if (admitted - baseline != descriptor_bytes + payload_bytes
        + sizeof(col_rel_t) + sizeof("$join_batch")
        + auto_metadata_bytes(f.out->ncols)
        + (uint64_t)f.out->ncols * sizeof(wirelog_column_type_t)) {
        FAIL("right governor did not charge the exact initial payload");
        goto out;
    }
    if (col_join_batch_run_to_relation(cont, f.sess, f.out) != 0
        || !same_rows(oracle, f.out)) {
        FAIL("right-governed producer output differed from the oracle");
        goto out;
    }
    wl_columnar_continuation_destroy(cont);
    cont = NULL;
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(source_governor)) != baseline) {
        FAIL("right-source producer destruction leaked admission");
        goto out;
    }
    PASS();
out:
    if (cont)
        wl_columnar_continuation_destroy(cont);
    if (oracle)
        col_rel_destroy(oracle);
    f.sess->memory_governor = session_governor;
    if (source_governor)
        wl_columnar_memory_governor_ref_release(source_governor);
    fixture_fini(&f);
}

static void
test_batch_payload_denial_rolls_back_and_retries(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1 };
    wl_columnar_memory_governor_ref_t *source_governor = NULL;
    wl_columnar_memory_governor_ref_t *session_governor;
    wl_columnar_continuation_t *cont = NULL;
    const col_arr_entry_t *entry;
    uint64_t descriptor_bytes;
    uint64_t payload_bytes;
    uint64_t budget;
    uint64_t baseline;
    int rc;

    TEST("initial batch payload denial rolls back and permits retry");
    if (!fixture_init(&f, 1u << 24, keys, 2, 2, 20)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    session_governor = f.sess->memory_governor;
    if (!descriptor_footprint(1u, 2u, &descriptor_bytes)
        || !col_rel_retained_bytes_for(f.out, COL_REL_INIT_CAP,
        &payload_bytes)
        || !col_session_get_arrangement(&f.sess->base, "right", KEY0, 1u)) {
        FAIL("admission footprint setup");
        goto out;
    }
    payload_bytes += (uint64_t)f.out->ncols
        * (sizeof(int64_t *) + sizeof(int64_t));
    budget = descriptor_bytes + payload_bytes
        + sizeof(col_rel_t) + sizeof("$join_batch")
        + 2u * auto_metadata_bytes(f.out->ncols)
        + (uint64_t)f.out->ncols * sizeof(wirelog_column_type_t);
    source_governor = make_governor(1u << 20);
    if (!source_governor
        || col_rel_attach_memory_governor(f.left, source_governor) != 0) {
        FAIL("tight source governor setup");
        goto out;
    }
    baseline = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(source_governor));
    entry = find_entry(f.sess, "right");
    f.sess->memory_governor = NULL;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            source_governor)->usable_bytes, baseline + budget - 1u,
        memory_order_release);
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false,
            KEY0, KEY0, 1u, 8u * 32u, &cont);
    if (rc != ENOSPC || cont != NULL || !entry || entry->pin_count != 0u
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(source_governor)) != baseline) {
        FAIL("initial payload denial was not typed or leaked a reservation");
        goto out;
    }
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            source_governor)->usable_bytes, baseline + budget,
        memory_order_release);
    wl_columnar_relation_test_fail_next_metadata_alloc();
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false,
            KEY0, KEY0, 1u, 8u * 32u, &cont);
    if (rc != ENOMEM || cont != NULL || entry->pin_count != 0u
        || wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(source_governor)) != baseline) {
        FAIL("metadata allocation failure was not typed or balanced");
        goto out;
    }
    if (col_join_batch_producer_create(f.sess, &f.op, f.left, false,
        KEY0, KEY0, 1u, 8u * 32u, &cont) != 0 || !cont) {
        FAIL("producer retry after denied source admission");
        goto out;
    }
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(source_governor))
        != baseline + budget - auto_metadata_bytes(f.out->ncols)) {
        FAIL("metadata overlap credit did not settle after creation");
        goto out;
    }
    wl_columnar_continuation_destroy(cont);
    cont = NULL;
    if (wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(source_governor)) != baseline) {
        FAIL("exact-fit producer destruction leaked admission");
        goto out;
    }
    PASS();
out:
    if (cont)
        wl_columnar_continuation_destroy(cont);
    f.sess->memory_governor = session_governor;
    if (source_governor)
        wl_columnar_memory_governor_ref_release(source_governor);
    fixture_fini(&f);
}

/* (8) Projection: a projected output width matches the oracle. */
static void
test_projected_output(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1, 1 };
    uint32_t project[] = { 1, 3 };
    wl_columnar_continuation_t *cont = NULL;
    col_rel_t *oracle = NULL;
    col_rel_t *out2 = NULL;
    int rc;

    TEST("projected output matches the oracle");
    if (!fixture_init(&f, 1u << 24, keys, 3, 2, 30)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    f.op.project_indices = project;
    f.op.project_count = 2;
    out2 = col_rel_new_auto("$join", 2);
    if (!out2 || col_join_set_output_types(out2, f.left, f.right, &f.op)
        != 0) {
        FAIL("projected output");
        goto out;
    }
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 7u * 16u, &cont);
    if (rc == 0)
        rc = col_join_batch_run_to_relation(cont, f.sess, out2);
    oracle = run_oracle(f.sess, f.left, &f.op);
    if (rc != 0 || !oracle || !same_rows(oracle, out2) || out2->nrows != 90u)
        FAIL("projected result differs from the one-shot oracle");
    else if (!nested_loop_projected_equal(f.left, f.right, out2))
        FAIL("projected result differs from the nested-loop oracle");
    else
        PASS();
out:
    if (oracle)
        col_rel_destroy(oracle);
    if (out2)
        col_rel_destroy(out2);
    if (cont)
        wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* #1909: the projected test above runs at seven rows per batch, below the
 * 64-row floor, so projection never meets a scratch reallocation.  Here the
 * 90 projected matches cross the floor inside one batch. */
static void
test_projected_output_across_grow(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1, 1 };
    uint32_t project[] = { 1, 3 };
    wl_columnar_continuation_t *cont = NULL;
    col_rel_t *oracle = NULL;
    col_rel_t *out2 = NULL;
    uint32_t cap0, capN;
    int rc;

    TEST("projected output across a scratch grow matches the oracle");
    if (!fixture_init(&f, 1u << 24, keys, 3, 2, 30)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    f.op.project_indices = project;
    f.op.project_count = 2;
    out2 = col_rel_new_auto("$join", 2);
    if (!out2 || col_join_set_output_types(out2, f.left, f.right, &f.op)
        != 0) {
        FAIL("projected output");
        goto out;
    }
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 512u * 16u, &cont);
    if (rc != 0 || col_join_batch_rows_per_batch(cont) <= 90u) {
        FAIL("producer create or rows_per_batch");
        goto out;
    }
    cap0 = col_join_batch_scratch_capacity(cont);
    rc = col_join_batch_run_to_relation(cont, f.sess, out2);
    capN = col_join_batch_scratch_capacity(cont);
    oracle = run_oracle(f.sess, f.left, &f.op);
    if (rc != 0 || !oracle || !same_rows(oracle, out2) || out2->nrows != 90u)
        FAIL("projected result differs from the oracle across a grow");
    else if (cap0 != COL_REL_INIT_CAP || capN <= cap0)
        FAIL("the projected scratch did not start at the floor and grow");
    else
        PASS();
out:
    if (oracle)
        col_rel_destroy(oracle);
    if (out2)
        col_rel_destroy(out2);
    if (cont)
        wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* #1909: a timestamped left gives the scratch a timestamp array, which the
 * resize path copies by nrows just as it copies the columns.  The producer
 * writes zero timestamps, so the columns are what can be lost; the sink's
 * timestamps must stay zero. */
static void
test_timestamped_scratch_across_grow(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1, 2 };
    wl_columnar_continuation_t *cont = NULL;
    col_rel_t *oracle = NULL;
    uint32_t cap0, capN;
    bool zero = true;
    int rc;

    TEST("timestamped scratch across a grow matches the oracle");
    if (!fixture_init(&f, 1u << 24, keys, 3, 5, 47)
        || col_rel_enable_timestamps(f.left) != 0
        || col_rel_enable_timestamps(f.out) != 0) {
        FAIL("timestamped fixture");
        fixture_fini(&f);
        return;
    }
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 1u << 16, &cont);
    if (rc != 0 || col_join_batch_rows_per_batch(cont) <= 3u * 47u) {
        FAIL("producer create or rows_per_batch");
        goto out;
    }
    cap0 = col_join_batch_scratch_capacity(cont);
    rc = col_join_batch_run_to_relation(cont, f.sess, f.out);
    capN = col_join_batch_scratch_capacity(cont);
    oracle = run_oracle(f.sess, f.left, &f.op);
    for (uint32_t i = 0; f.out->timestamps && i < f.out->nrows; i++) {
        const col_delta_timestamp_t *ts = &f.out->timestamps[i];
        if (ts->multiplicity != 0 || ts->iteration != 0 || ts->stratum != 0
            || ts->worker != 0)
            zero = false;
    }
    if (rc != 0 || !oracle || !same_rows(oracle, f.out)
        || f.out->nrows != 3u * 47u)
        FAIL("timestamped result differs from the oracle across a grow");
    else if (!f.out->timestamps || !zero)
        FAIL("the sink did not publish zero timestamps");
    else if (cap0 != COL_REL_INIT_CAP || capN <= cap0)
        FAIL("the timestamped scratch did not start at the floor and grow");
    else
        PASS();
out:
    if (oracle)
        col_rel_destroy(oracle);
    if (cont)
        wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* #1909: the grow-refusal WARN is operator-facing, and no producer test
 * could see it: the logger was not initialised until this binary's last
 * test, and removing the warning, its once-per-producer gate, or rotating
 * its reason strings survived the suite.  Capture the JOIN section to a
 * file and check the reason for a refused allocation and for an admission
 * denial, and that two refused grows in one producer warn once.  The
 * caller's WL_LOG and WL_LOG_FILE are saved and restored. */
static char warn_log_path[512];
static char *warn_saved_log;
static char *warn_saved_file;

static char *
warn_env_dup(const char *name)
{
    const char *value = getenv(name);
    char *copy;
    size_t len;

    if (!value)
        return NULL;
    len = strlen(value) + 1u;
    copy = (char *)malloc(len);
    if (copy)
        memcpy(copy, value, len);
    return copy;
}

static void
warn_env_restore(const char *name, char **saved)
{
    if (*saved)
        (void)setenv(name, *saved, 1);
    else
        (void)unsetenv(name);
    free(*saved);
    *saved = NULL;
}

static bool
warn_log_begin(void)
{
    const char *dir = getenv("TMPDIR");
#ifdef _WIN32
    if (!dir || !*dir)
        dir = getenv("TEMP");
#endif
    if (!dir || !*dir)
        dir = ".";
    /* The process id keeps concurrent runs sharing a TMPDIR apart. */
    snprintf(warn_log_path, sizeof(warn_log_path),
        "%s/wl_join_batch_warn_%ld_%lu.log", dir, (long)wl_test_getpid(),
        (unsigned long)time(NULL));
    (void)remove(warn_log_path);
    warn_saved_log = warn_env_dup("WL_LOG");
    warn_saved_file = warn_env_dup("WL_LOG_FILE");
    if (setenv("WL_LOG_FILE", warn_log_path, 1) != 0
        || setenv("WL_LOG", "JOIN:2", 1) != 0)
        return false;
    return wl_log_init() == 0;
}

/* Stop capturing, read the file into buf and restore the caller's logging
 * configuration. */
static void
warn_log_end(char *buf, size_t size)
{
    FILE *fp;
    size_t n = 0;

    wl_log_shutdown();
    fp = fopen(warn_log_path, "r");
    if (fp) {
        n = fread(buf, 1, size - 1u, fp);
        fclose(fp);
    }
    buf[n] = '\0';
    (void)remove(warn_log_path);
    warn_env_restore("WL_LOG", &warn_saved_log);
    warn_env_restore("WL_LOG_FILE", &warn_saved_file);
    (void)wl_log_init();
}

static unsigned
count_substr(const char *haystack, const char *needle)
{
    unsigned count = 0;
    for (const char *p = strstr(haystack, needle); p;
        p = strstr(p + 1, needle))
        count++;
    return count;
}

static void
test_grow_refusal_warn_reason(void)
{
    for (int denied = 0; denied < 2; denied++) {
        fixture_t f;
        const int64_t keys[] = { 0, 1, 2 };
        wl_columnar_continuation_t *cont = NULL;
        wl_columnar_continuation_sink_t sink;
        col_join_batch_relation_sink_t sctx;
        wl_columnar_continuation_status_t st = WL_COLUMNAR_CONTINUATION_OK;
        wl_columnar_memory_governor_t *governor;
        col_rel_t *oracle = NULL;
        bool refused_ok = true;
        char log[4096];

        TEST(denied ? "a denied scratch grow warns: admission denied"
                    : "failed scratch grows warn once: allocation failed");
        /* The ceiling is a project-wide define, so it matches the library;
         * 2 is WL_LOG_WARN, which #if cannot name. */
#if WL_LOG_COMPILE_MAX_LEVEL < 2
        printf("SKIP (WARN compiled out) ");
        PASS();
        continue;
#endif
        if (!fixture_init(&f, 1u << 24, keys, 3, 5, 47)
            || col_join_batch_producer_create(f.sess, &f.op, f.left, false,
            KEY0, KEY0, 1, 512u * 32u, &cont) != 0
            || col_join_batch_relation_sink_init(&sctx, &sink, f.sess,
            f.out) != 0 || !warn_log_begin()) {
            FAIL("fixture, producer, sink or log capture");
            if (cont)
                wl_columnar_continuation_destroy(cont);
            fixture_fini(&f);
            continue;
        }
        governor = wl_columnar_memory_governor_ref_get(f.sess->memory_governor);
        if (denied) {
            /* Room for a 64-row payload (2 KiB at four columns) but not
             * for growing the scratch to 128 rows, which holds the old and
             * the new buffers at once: the first batch is emitted short. */
            atomic_store_explicit(&governor->usable_bytes,
                reserved_of(f.sess) + 64u * 32u * 2u, memory_order_release);
            st = wl_columnar_continuation_publish(cont, &sink);
            refused_ok = st == WL_COLUMNAR_CONTINUATION_OK
                && f.out->nrows == COL_REL_INIT_CAP;
            atomic_store_explicit(&governor->usable_bytes, 1u << 24,
                memory_order_release);
        } else {
            /* Refuse the grow of two consecutive batches: both are emitted
             * short, and the producer warns only for the first. */
            for (int b = 0; b < 2 && refused_ok; b++) {
                wl_columnar_relation_test_fail_next_prepare_resize();
                st = wl_columnar_continuation_publish(cont, &sink);
                refused_ok = st == WL_COLUMNAR_CONTINUATION_OK
                    && f.out->nrows == COL_REL_INIT_CAP * (uint32_t)(b + 1)
                    && col_join_batch_scratch_capacity(cont)
                    == COL_REL_INIT_CAP;
            }
            wl_columnar_relation_test_clear_prepare_resize();
        }
        while (refused_ok && st == WL_COLUMNAR_CONTINUATION_OK)
            st = wl_columnar_continuation_publish(cont, &sink);
        warn_log_end(log, sizeof(log));
        oracle = run_oracle(f.sess, f.left, &f.op);
        if (!refused_ok)
            FAIL("a refused grow did not emit a short batch");
        else if (st != WL_COLUMNAR_CONTINUATION_DONE || !oracle
            || !same_rows(oracle, f.out) || f.out->nrows != 3u * 47u)
            FAIL("a refused grow lost or duplicated a row");
        else if (count_substr(log, "join batch scratch stuck") != 1u)
            FAIL("the refusals did not warn exactly once");
        else if (count_substr(log, denied ? "admission denied"
            : "allocation failed") != 1u
            || count_substr(log, denied ? "allocation failed"
            : "admission denied") != 0u)
            FAIL("the warning names the wrong reason");
        else
            PASS();
        if (oracle)
            col_rel_destroy(oracle);
        wl_columnar_continuation_destroy(cont);
        fixture_fini(&f);
    }
}

static int64_t
double_bits(double d)
{
    int64_t bits;
    memcpy(&bits, &d, sizeof(bits));
    return bits;
}

/* FLOAT-typed relation with the type set before any row is appended. */
static col_rel_t *
make_float_rel(const char *name, const double *keys, uint32_t n,
    int64_t payload_base)
{
    const char *cn[] = { "k", "v" };
    wirelog_column_type_t types[2] = { WIRELOG_TYPE_FLOAT, WIRELOG_TYPE_INT64 };
    col_rel_t *r = make_rel(name, 2, cn);
    if (!r)
        return NULL;
    if (col_rel_set_column_types(r, types, 2) != 0) {
        col_rel_destroy(r);
        return NULL;
    }
    for (uint32_t i = 0; i < n; i++) {
        int64_t row[] = { double_bits(keys[i]), payload_base + (int64_t)i };
        if (col_rel_append_row(r, row) != 0) {
            col_rel_destroy(r);
            return NULL;
        }
    }
    return r;
}

/* (8b) A FLOAT key takes the typed equality path: 0.0 and -0.0 match. */
static void
test_float_key(void)
{
    static const double lkeys[] = { 1.5, -0.0, 2.25 };
    static const double rkeys[] = { 1.5, 1.5, 0.0, 0.0, 0.0, 3.0 };
    wl_col_session_t *sess = make_session(1u << 24);
    col_rel_t *left = make_float_rel("left", lkeys, 3, 0);
    col_rel_t *right = make_float_rel("right", rkeys, 6, 100);
    col_rel_t *out = NULL;
    col_rel_t *oracle = NULL;
    wl_columnar_continuation_t *cont = NULL;
    wl_plan_op_t op;
    const char *lk[1] = { "k" };
    const char *rk[1] = { "k" };
    int rc = -1;

    TEST("FLOAT key uses typed equality and matches the oracle");
    if (!sess || !left || !right) {
        FAIL("fixture");
        goto out;
    }
    session_add_rel(sess, right);
    right = NULL;
    init_join_op(&op, lk, rk);
    out = col_rel_new_auto("$join", 4);
    if (!out || col_join_set_output_types(out, left,
        session_find_rel(sess, "right"), &op) != 0) {
        FAIL("output");
        goto out;
    }
    rc = col_join_batch_producer_create(sess, &op, left, false, KEY0, KEY0,
            1, 2u * 32u, &cont);
    if (rc == 0)
        rc = col_join_batch_run_to_relation(cont, sess, out);
    oracle = run_oracle(sess, left, &op);
    /* 1.5 x2, -0.0 vs 0.0 x3, 2.25 x0 -> 5 rows. */
    if (rc != 0 || !oracle || out->nrows != 5u || !same_rows(oracle, out))
        FAIL("float-key result differs from the one-shot oracle");
    else
        PASS();
out:
    if (oracle)
        col_rel_destroy(oracle);
    if (cont)
        wl_columnar_continuation_destroy(cont);
    if (out)
        col_rel_destroy(out);
    if (left)
        col_rel_destroy(left);
    if (right)
        col_rel_destroy(right);
    destroy_session(sess);
}

/* (9) A budget below one row is unsupported at create; the sink refuses a
 * pool-owned output. */
static void
test_unsupported_budget_and_pooled_output(void)
{
    fixture_t f;
    const int64_t keys[] = { 0 };
    wl_columnar_continuation_t *cont = NULL;
    wl_columnar_continuation_sink_t sink;
    col_join_batch_relation_sink_t sctx;
    col_rel_t *pooled;
    const col_arr_entry_t *entry;
    uint64_t reservation_base;
    int rc;

    TEST("sub-row budget is ENOTSUP; pool-owned output is refused");
    if (!fixture_init(&f, 1u << 24, keys, 1, 1, 3)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    if (!col_session_get_arrangement(&f.sess->base, "right", KEY0, 1u)) {
        FAIL("arrangement warmup");
        fixture_fini(&f);
        return;
    }
    reservation_base = reserved_of(f.sess);
    entry = find_entry(f.sess, "right");
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 31u, &cont);
    pooled = col_rel_pool_new_auto(f.sess->delta_pool, NULL, "$pooled", 4);
    if (rc != ENOTSUP || cont != NULL || !pooled
        || !entry || entry->pin_count != 0u
        || reserved_of(f.sess) != reservation_base
        || col_join_batch_relation_sink_init(&sctx, &sink, f.sess, pooled)
        != EINVAL)
        FAIL("unsupported budget or pooled output was accepted");
    else
        PASS();
    if (pooled)
        col_rel_destroy(pooled);
    fixture_fini(&f);
}

/* (10) The legacy row cap still trips in bounded mode, after the first
 * committed batch that reaches it, overshooting by less than a batch. */
static void
test_row_cap_trips_after_a_committed_batch(void)
{
    fixture_t f;
    const int64_t keys[] = { 0 };
    wl_columnar_continuation_t *cont = NULL;
    int rc;

    TEST("WIRELOG_JOIN_OUTPUT_LIMIT trips after a committed batch");
    if (!fixture_init(&f, 1u << 24, keys, 1, 1, 100)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    f.sess->join_output_limit = 10;
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 7u * 32u, &cont);
    if (rc == 0)
        rc = col_join_batch_run_to_relation(cont, f.sess, f.out);
    if (rc != EOVERFLOW || f.out->nrows < 10u || f.out->nrows > 16u)
        FAIL("row cap not enforced within one batch of the limit");
    else
        PASS();
    if (cont)
        wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

static void
test_shared_cap_counts_only_committed_rows(void)
{
    const int64_t keys[] = { 0 };
    const int64_t existing[] = { 0, 0, 0, 0 };

    for (unsigned capped = 0; capped < 2; capped++) {
        fixture_t f;
        wl_columnar_continuation_t *cont = NULL;
        atomic_uint_fast64_t count;
        atomic_init(&count, 5);
        TEST("shared cap charges batch rows once, excluding existing output");
        if (!fixture_init(&f, 1u << 24, keys, 1, 1, 100)) {
            FAIL("fixture");
            fixture_fini(&f);
            continue;
        }
        f.sess->join_output_shared_count = &count;
        f.sess->join_output_shared_limit = capped ? 20 : UINT64_MAX;
        int rc = col_rel_append_row(f.out, existing);
        if (rc == 0)
            rc = col_rel_append_row(f.out, existing);
        if (rc == 0)
            rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false,
                    KEY0, KEY0, 1, 7u * 32u, &cont);
        if (rc == 0)
            rc = col_join_batch_run_to_relation(cont, f.sess, f.out);
        if (rc == (capped ? EOVERFLOW : 0)
            && f.out->nrows == (capped ? 23u : 102u)
            && atomic_load_explicit(&count,
            memory_order_relaxed) == (capped ? 26u : 105u))
            PASS();
        else
            FAIL("shared cap lost rows or counted preexisting/cumulative rows");
        if (cont)
            wl_columnar_continuation_destroy(cont);
        fixture_fini(&f);
    }
}

/* (11) Operator dispatch: an eligible shape uses the producer under the
 * knob and equals the oracle; excluded shapes fall back with the reason
 * recorded, or fail with ENOTSUP in strict mode. */
/* #2177: the bounded sink translates a growth refusal to ENOSPC before
 * the operator sees it. Calibrate from an actual successful output admission,
 * then withhold the extra space needed while old/new growth buffers overlap. */
static void
test_operator_output_growth_denial(void)
{
    fixture_t f;
    const int64_t keys[] = { 0 };
    const uint64_t ample_budget = 1u << 24;
    eval_stack_t stack;
    col_rel_t *oracle = NULL;
    wl_columnar_memory_governor_t *governor;
    uint64_t baseline, output_bytes;
    const col_arr_entry_t *entry;
    uint32_t pins;
    int rc;
    bool ok = true;

    TEST("operator marks output growth denial, preserving other errors");
    if (!fixture_init(&f, ample_budget, keys, 1, 1, 300)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    /* Warm the persistent arrangement and retain a content oracle. Neither
     * its ownership nor its reservation may change during failed attempts. */
    oracle = run_oracle(f.sess, f.left, &f.op);
    entry = find_entry(f.sess, "right");
    if (!oracle || oracle->nrows != 300 || !entry) {
        FAIL("oracle or arrangement");
        goto out;
    }
    pins = entry->pin_count;
    governor = wl_columnar_memory_governor_ref_get(f.sess->memory_governor);
    baseline = reserved_of(f.sess);
    f.sess->join_batch_bytes = 64u * f.out->ncols * sizeof(int64_t);
    f.sess->memory_budget_denied = false;
    eval_stack_init(&stack);
    eval_stack_push(&stack, f.left, false);
    rc = col_op_join(&f.op, &stack, f.sess);
    if (rc != 0 || stack.top != 1 || !same_rows(stack.items[0].rel, oracle)
        || f.sess->memory_budget_denied || reserved_of(f.sess) <= baseline) {
        FAIL("ample-budget bounded output calibration");
        eval_stack_drain(&stack);
        goto out;
    }
    output_bytes = reserved_of(f.sess) - baseline;
    eval_stack_drain(&stack);
    if (reserved_of(f.sess) != baseline) {
        FAIL("calibration output did not release its admission");
        goto out;
    }

    for (unsigned which = 0; which < 4; which++) {
        uint64_t budget = which == 0 ? baseline
            : which == 1 ? baseline + output_bytes : ample_budget;
        int expected = which < 2 ? ENOSPC : which == 2 ? ENOMEM : EOVERFLOW;
        atomic_store_explicit(&governor->usable_bytes, budget,
            memory_order_relaxed);
        f.sess->memory_budget_denied = false;
        f.sess->join_output_peak = 0;
        f.sess->join_output_limit = which == 3 ? 1 : 0;
        if (which == 2)
            wl_columnar_relation_test_fail_next_prepare_resize();
        eval_stack_init(&stack);
        eval_stack_push(&stack, f.left, false);
        rc = col_op_join(&f.op, &stack, f.sess);
        wl_columnar_relation_test_clear_prepare_resize();
        /* 64-row batches fit the producer's initial scratch. A committed
         * batch followed by ENOMEM therefore targets the output resize,
         * not an earlier descriptor, scratch, or initial-capacity failure. */
        if (rc != expected || f.sess->memory_budget_denied != (which < 2)
            || (which == 0 ? f.sess->join_output_peak != 0
                : f.sess->join_output_peak < 64)
            || stack.top != 0 || f.sess->join_batch_fallback_count != 0
            || entry->pin_count != pins || reserved_of(f.sess) != baseline) {
            printf(
                "(case=%u rc=%d denied=%d peak=%llu reserved=%llu baseline=%llu) ",
                which, rc, (int)f.sess->memory_budget_denied,
                (unsigned long long)f.sess->join_output_peak,
                (unsigned long long)reserved_of(f.sess),
                (unsigned long long)baseline);
            ok = false;
        }
        eval_stack_drain(&stack);
    }
    /* A fresh attempt after all refusals still produces the exact result. */
    atomic_store_explicit(&governor->usable_bytes, ample_budget,
        memory_order_relaxed);
    f.sess->join_output_limit = 0;
    f.sess->memory_budget_denied = false;
    eval_stack_init(&stack);
    eval_stack_push(&stack, f.left, false);
    rc = col_op_join(&f.op, &stack, f.sess);
    ok = ok && rc == 0 && stack.top == 1
        && same_rows(stack.items[0].rel, oracle)
        && !f.sess->memory_budget_denied;
    eval_stack_drain(&stack);
    ok = ok && reserved_of(f.sess) == baseline && entry->pin_count == pins;
    if (ok)
        PASS();
    else
        FAIL("operator denial classification, progress, or cleanup");
out:
    wl_columnar_relation_test_clear_prepare_resize();
    col_rel_destroy(oracle);
    fixture_fini(&f);
}

static void
test_operator_dispatch_and_eligibility(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1, 1, 0 };
    eval_stack_t stack;
    eval_entry_t result;
    col_rel_t *oracle;
    wl_plan_op_t cross;
    int rc;

    TEST("operator uses the producer when eligible and records fallbacks");
    if (!fixture_init(&f, 1u << 24, keys, 4, 2, 50)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    oracle = run_oracle(f.sess, f.left, &f.op);
    f.sess->join_batch_bytes = 9u * 32u;
    eval_stack_init(&stack);
    eval_stack_push(&stack, f.left, false);
    rc = col_op_join(&f.op, &stack, f.sess);
    if (rc != 0) {
        FAIL("bounded join through the operator failed");
        eval_stack_drain(&stack);
        goto out;
    }
    result = eval_stack_pop(&stack);
    if (!result.owned || !oracle || !same_rows(oracle, result.rel)
        || result.rel->pool_owned || result.rel->memory_governor == NULL
        || f.sess->join_batch_fallback_count != 0u) {
        FAIL(
            "bounded operator result is not a governed heap copy of the oracle");
        if (result.owned)
            col_rel_destroy(result.rel);
        eval_stack_drain(&stack);
        goto out;
    }
    col_rel_destroy(result.rel);
    /* Cross join (key_count 0) falls back with the reason recorded. */
    cross = f.op;
    cross.key_count = 0;
    eval_stack_init(&stack);
    eval_stack_push(&stack, f.left, false);
    rc = col_op_join(&cross, &stack, f.sess);
    if (rc != 0 || f.sess->join_batch_fallback_count != 1u
        || f.sess->join_batch_last_reason != COL_JOIN_BATCH_EXCLUDED_CROSS) {
        FAIL("cross join did not fall back with its reason recorded");
        if (rc == 0 && stack.top > 0) {
            result = eval_stack_pop(&stack);
            if (result.owned)
                col_rel_destroy(result.rel);
        }
        eval_stack_drain(&stack);
        goto out;
    }
    result = eval_stack_pop(&stack);
    if (result.owned)
        col_rel_destroy(result.rel);
    /* Strict mode turns the fallback into ENOTSUP. */
    f.sess->join_batch_strict = true;
    eval_stack_init(&stack);
    eval_stack_push(&stack, f.left, false);
    rc = col_op_join(&cross, &stack, f.sess);
    if (rc != ENOTSUP) {
        FAIL("strict mode did not refuse an excluded shape");
        if (rc == 0 && stack.top > 0) {
            result = eval_stack_pop(&stack);
            if (result.owned)
                col_rel_destroy(result.rel);
        }
        eval_stack_drain(&stack);
        goto out;
    }
    eval_stack_drain(&stack);
    PASS();
out:
    if (oracle)
        col_rel_destroy(oracle);
    fixture_fini(&f);
}

/* Run one excluded operator shape in both modes.  The non-strict invocation
 * must preserve the one-shot result and record exactly one reason; strict
 * mode must reject before recording another fallback. */
static bool
run_excluded_operator_case(fixture_t *f, wl_plan_op_t *op,
    col_join_batch_eligibility_t reason, uint32_t expected_rows,
    const char *label)
{
    eval_stack_t stack;
    eval_entry_t result;
    col_rel_t *oracle = NULL;
    int rc;
    bool ok = true;

    /* Exercise the excluded operator before running the oracle.  In
     * particular, the filtered-right case must not be turned into a cache-hit
     * test by pre-populating the same filtered relation first. */
    f->sess->join_batch_bytes = 9u * 32u;
    eval_stack_init(&stack);
    eval_stack_push(&stack, f->left, false);
    rc = col_op_join(op, &stack, f->sess);
    if (rc != 0 || stack.top == 0) {
        printf("(%s: non-strict dispatch) ", label);
        ok = false;
        eval_stack_drain(&stack);
    } else {
        result = eval_stack_pop(&stack);
        oracle = run_oracle(f->sess, f->left, op);
        if (!result.owned || !oracle || oracle->nrows != expected_rows
            || !same_rows(oracle, result.rel)
            || f->sess->join_batch_fallback_count != 1u
            || f->sess->join_batch_last_reason != reason) {
            printf("(%s: result/count/reason) ", label);
            ok = false;
        }
        if (oracle)
            col_rel_destroy(oracle);
        if (result.owned)
            col_rel_destroy(result.rel);
        eval_stack_drain(&stack);
    }

    f->sess->join_batch_strict = true;
    eval_stack_init(&stack);
    eval_stack_push(&stack, f->left, false);
    rc = col_op_join(op, &stack, f->sess);
    if (rc != ENOTSUP || f->sess->join_batch_fallback_count != 1u) {
        printf("(%s: strict ENOTSUP/count) ", label);
        ok = false;
        if (stack.top > 0) {
            result = eval_stack_pop(&stack);
            if (result.owned)
                col_rel_destroy(result.rel);
        }
    }
    eval_stack_drain(&stack);
    f->sess->join_batch_strict = false;
    return ok;
}

/* #1656: delta-right and filtered-right are excluded after the operator has
 * selected the effective right relation.  Both paths must still equal their
 * own one-shot oracle and must not enter bounded mode in strict mode. */
static void
test_operator_delta_and_filtered_fallbacks(void)
{
    const int64_t keys[] = { 0, 1, 1, 0 };
    bool ok = true;

    TEST("delta-right and filtered-right fall back with stable reasons");
    {
        fixture_t f;
        wl_plan_op_t op;
        col_rel_t *delta = NULL;

        if (!fixture_init(&f, 1u << 24, keys, 4, 2, 2)) {
            FAIL("delta fixture");
            fixture_fini(&f);
            return;
        }
        delta = make_right_named("$d$right", 2, 1);
        if (!delta || session_add_rel(f.sess, delta) != 0) {
            FAIL("delta registration");
            if (delta)
                col_rel_destroy(delta);
            fixture_fini(&f);
            return;
        }
        init_join_op(&op, f.lkeys, f.rkeys);
        op.delta_mode = WL_DELTA_FORCE_DELTA;
        ok = ok && run_excluded_operator_case(&f, &op,
                COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT, 4u, "delta-right");
        fixture_fini(&f);
    }
    {
        fixture_t f;
        wl_plan_op_t op;

        if (!fixture_init(&f, 1u << 24, keys, 4, 2, 2)) {
            FAIL("filtered fixture");
            fixture_fini(&f);
            return;
        }
        init_join_op(&op, f.lkeys, f.rkeys);
        op.right_filter_expr.data = right_filter_bytes;
        op.right_filter_expr.size = sizeof(right_filter_bytes);
        ok = ok && run_excluded_operator_case(&f, &op,
                COL_JOIN_BATCH_EXCLUDED_FILTERED_RIGHT, 4u,
                "filtered-right");
        fixture_fini(&f);
    }
    if (ok)
        PASS();
    else
        FAIL("operator fallback matrix");
}

/* Differential counterpart of run_excluded_operator_case (#1382).  The
 * oracle is the ordinary one-shot join of the same shape with the knob off
 * and no materialization, so a cached result cannot stand in for it.  The
 * strict run must refuse with no net change to the session's reserved
 * bytes. */
static bool
run_excluded_diff_case(fixture_t *f, wl_plan_op_t *op, uint64_t knob,
    col_join_batch_eligibility_t reason, uint32_t expected_rows,
    const char *label)
{
    eval_stack_t stack;
    eval_entry_t result;
    wl_plan_op_t oracle_op = *op;
    col_rel_t *oracle = NULL;
    uint64_t reserved;
    int rc;
    bool ok = true;

    f->sess->join_batch_bytes = knob;
    eval_stack_init(&stack);
    eval_stack_push(&stack, f->left, false);
    rc = col_op_join_diff(op, &stack, f->sess);
    if (rc != 0 || stack.top == 0) {
        printf("(%s: non-strict rc %d) ", label, rc);
        ok = false;
        eval_stack_drain(&stack);
    } else {
        result = eval_stack_pop(&stack);
        oracle_op.materialized = false;
        oracle = run_oracle(f->sess, f->left, &oracle_op);
        if (!oracle || oracle->nrows != expected_rows
            || !same_rows(oracle, result.rel)
            || f->sess->join_batch_fallback_count != 1u
            || f->sess->join_batch_last_reason != reason) {
            printf("(%s: result/count/reason %u/%u) ", label,
                f->sess->join_batch_fallback_count,
                (unsigned)f->sess->join_batch_last_reason);
            ok = false;
        }
        if (oracle)
            col_rel_destroy(oracle);
        if (result.owned)
            col_rel_destroy(result.rel);
        eval_stack_drain(&stack);
    }

    /* A cached materialized result would be copied before the bounded-mode
     * decision is reached. */
    col_mat_cache_clear(&f->sess->mat_cache);
    f->sess->join_batch_strict = true;
    reserved = reserved_of(f->sess);
    eval_stack_init(&stack);
    eval_stack_push(&stack, f->left, false);
    rc = col_op_join_diff(op, &stack, f->sess);
    if (rc != ENOTSUP || stack.top != 0
        || f->sess->join_batch_fallback_count != 1u
        || reserved_of(f->sess) != reserved) {
        printf("(%s: strict rc %d) ", label, rc);
        ok = false;
        if (stack.top > 0) {
            result = eval_stack_pop(&stack);
            if (result.owned)
                col_rel_destroy(result.rel);
        }
    }
    eval_stack_drain(&stack);
    f->sess->join_batch_strict = false;
    f->sess->join_batch_bytes = 0;
    return ok;
}

/* #1382: the differential join records the reason for every shape it keeps
 * on the one-shot path, as the ordinary join does, and refuses it under
 * WIRELOG_JOIN_BATCH_STRICT.  Before, cross, delta-right, filtered-right and
 * materialized fell through silently and ran one-shot even when strict. */
static void
test_diff_operator_exclusions_recorded_and_strict(void)
{
    const int64_t keys[] = { 0, 1, 1, 0 };
    bool ok = true;

    TEST("differential join records and strict-refuses excluded shapes");
    for (int c = 0; c < 5 && ok; c++) {
        fixture_t f;
        wl_plan_op_t op;
        col_rel_t *delta = NULL;

        if (!fixture_init(&f, 1u << 24, keys, 4, 2, 2)) {
            FAIL("fixture");
            fixture_fini(&f);
            return;
        }
        init_join_op(&op, f.lkeys, f.rkeys);
        switch (c) {
        case 0:
            op.key_count = 0;
            op.left_keys = NULL;
            op.right_keys = NULL;
            ok = run_excluded_diff_case(&f, &op, 9u * 32u,
                    COL_JOIN_BATCH_EXCLUDED_CROSS, 16u, "cross");
            break;
        case 1:
            delta = make_right_named("$d$right", 2, 1);
            if (!delta || session_add_rel(f.sess, delta) != 0) {
                if (delta)
                    col_rel_destroy(delta);
                ok = false;
                break;
            }
            op.delta_mode = WL_DELTA_FORCE_DELTA;
            ok = run_excluded_diff_case(&f, &op, 9u * 32u,
                    COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT, 4u, "delta-right");
            break;
        case 2:
            op.right_filter_expr.data = right_filter_bytes;
            op.right_filter_expr.size = sizeof(right_filter_bytes);
            ok = run_excluded_diff_case(&f, &op, 9u * 32u,
                    COL_JOIN_BATCH_EXCLUDED_FILTERED_RIGHT, 4u,
                    "filtered-right");
            break;
        case 3:
            op.materialized = true;
            ok = run_excluded_diff_case(&f, &op, 9u * 32u,
                    COL_JOIN_BATCH_EXCLUDED_MATERIALIZED, 8u, "materialized");
            break;
        default:
            /* An eligible shape whose batch budget is below one 32-byte
             * output row. */
            ok = run_excluded_diff_case(&f, &op, 8u,
                    COL_JOIN_BATCH_EXCLUDED_ROW_TOO_LARGE, 8u,
                    "row-too-large");
            break;
        }
        fixture_fini(&f);
    }
    if (ok)
        PASS();
    else
        FAIL("differential exclusion matrix");
}

/* Only a create ENOTSUP is a recorded fallback: any other create failure is
 * returned as the operator's error and records nothing. */
static void
test_diff_operator_create_failure_records_nothing(void)
{
    const int64_t keys[] = { 0, 1 };
    fixture_t f;
    wl_plan_op_t op;
    eval_stack_t stack;
    int rc;

    TEST("differential create failure is returned, not recorded");
    if (!fixture_init(&f, 1u << 24, keys, 2, 2, 2)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    init_join_op(&op, f.lkeys, f.rkeys);
    f.sess->join_batch_bytes = 9u * 32u;
    eval_stack_init(&stack);
    eval_stack_push(&stack, f.left, false);
    wl_columnar_diff_join_batch_test_fail_allocation_at(1u);
    rc = col_op_join_diff(&op, &stack, f.sess);
    wl_columnar_diff_join_batch_test_fail_allocation_at(0u);
    if (stack.top > 0) {
        eval_entry_t result = eval_stack_pop(&stack);
        if (result.owned)
            col_rel_destroy(result.rel);
    }
    eval_stack_drain(&stack);
    if (rc != ENOMEM || f.sess->join_batch_fallback_count != 0u)
        FAIL("create failure was recorded or masked");
    else
        PASS();
    fixture_fini(&f);
}

/* Every recordable reason warns once, on its own bit of the session's
 * warned-reasons mask (#1382).  Under the former 8-bit "& 7" mask, LFTJ
 * (10) shared CROSS's bit (2), so a session that recorded a cross join
 * never warned about LFTJ. */
static void
test_join_batch_reason_mask_distinct(void)
{
    wl_col_session_t *sess = make_session(1u << 24);
    bool ok = sess != NULL;

    TEST("each fallback reason warns on its own mask bit");
    if (!ok) {
        FAIL("session");
        return;
    }
    sess->join_batch_bytes = 224;
    for (unsigned r = COL_JOIN_BATCH_EXCLUDED_CROSS;
        ok && r < COL_JOIN_BATCH_REASON_COUNT; r++) {
        unsigned before = sess->join_batch_warned_reasons;
        unsigned added;

        col_join_batch_record_fallback(sess,
            (col_join_batch_eligibility_t)r);
        added = sess->join_batch_warned_reasons & ~before;
        /* exactly one new bit per reason */
        if (added == 0 || (added & (added - 1u)) != 0) {
            printf("(reason %u added mask 0x%x) ", r, added);
            ok = false;
        }
    }
    /* Recording a reason a second time adds no bit. */
    if (ok) {
        unsigned before = sess->join_batch_warned_reasons;
        col_join_batch_record_fallback(sess, COL_JOIN_BATCH_EXCLUDED_LFTJ);
        ok = sess->join_batch_warned_reasons == before
            && sess->join_batch_last_reason == COL_JOIN_BATCH_EXCLUDED_LFTJ;
    }
    if (ok)
        PASS();
    else
        FAIL("reasons share a warned-reasons bit");
    destroy_session(sess);
}

static void
test_eligibility_ordering(void)
{
    wl_col_session_t *sess = make_session(1u << 24);
    bool ok = sess != NULL;

    TEST("bounded join eligibility ordering");
    if (!ok) {
        FAIL("session");
        return;
    }
    ok = ok && col_join_batch_eligibility(NULL, 1, false, false)
        == COL_JOIN_BATCH_EXCLUDED_OFF;
    sess->join_batch_bytes = 1u;
    ok = ok && col_join_batch_eligibility(sess, 0, false, false)
        == COL_JOIN_BATCH_EXCLUDED_CROSS;
    ok = ok && col_join_batch_eligibility(sess, 1, true, true)
        == COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT;
    ok = ok && col_join_batch_eligibility(sess, 1, false, true)
        == COL_JOIN_BATCH_EXCLUDED_FILTERED_RIGHT;
    sess->coordinator = sess;
    /* Worker sessions take precedence over every keyed/cross and
     * right-shape exclusion. */
    ok = ok && col_join_batch_eligibility(sess, 0, false, false)
        == COL_JOIN_BATCH_EXCLUDED_WORKER;
    ok = ok && col_join_batch_eligibility(sess, 0, true, true)
        == COL_JOIN_BATCH_EXCLUDED_WORKER;
    ok = ok && col_join_batch_eligibility(sess, 1, true, true)
        == COL_JOIN_BATCH_EXCLUDED_WORKER;
    sess->coordinator = NULL;
    /* Cross joins are checked before either right-side exclusion. */
    ok = ok && col_join_batch_eligibility(sess, 0, true, true)
        == COL_JOIN_BATCH_EXCLUDED_CROSS;
    ok = ok && col_join_batch_eligibility(sess, 0, false, true)
        == COL_JOIN_BATCH_EXCLUDED_CROSS;
    /* With a keyed join, delta-right precedes filtered-right. */
    ok = ok && col_join_batch_eligibility(sess, 1, true, true)
        == COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT;
    ok = ok && col_join_batch_eligibility(sess, 1, false, true)
        == COL_JOIN_BATCH_EXCLUDED_FILTERED_RIGHT;
    ok = ok && col_join_batch_eligibility(sess, 1, false, false)
        == COL_JOIN_BATCH_ELIGIBLE;

    if (ok)
        PASS();
    else
        FAIL("eligibility ordering");
    destroy_session(sess);
}

/* (11a) A probe that reports no persistent arrangement must take the
 * operator's ordinary fallback path, while strict mode turns the same
 * excluded shape into ENOTSUP.  The test-only seam is one-shot so the final
 * retry also proves that a later normal probe still succeeds. */
static void
test_operator_no_arrangement_probe_failure(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1, 1, 0 };
    const col_arr_entry_t *entry;
    eval_stack_t stack;
    eval_entry_t result = { 0 };
    col_rel_t *oracle = NULL;
    uint32_t fallback_count;
    int rc;

    TEST("operator handles no-arrangement probe failure in both modes");
    if (!fixture_init(&f, 1u << 24, keys, 4, 2, 50)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    oracle = run_oracle(f.sess, f.left, &f.op);
    entry = find_entry(f.sess, "right");
    f.sess->join_batch_bytes = 9u * 32u;
    if (!oracle || !entry || entry->pin_count != 0u
        || entry->rebuild_deferred) {
        FAIL("oracle did not establish a clean arrangement");
        goto out;
    }

    /* Non-strict mode maps ENOENT from the probe to the explicit fallback. */
    wl_columnar_arrangement_probe_test_fail_next_acquire(ENOENT);
    eval_stack_init(&stack);
    if (eval_stack_push(&stack, f.left, false) != 0) {
        FAIL("push non-strict input");
        goto out;
    }
    rc = col_op_join(&f.op, &stack, f.sess);
    if (rc != 0 || stack.top != 1u) {
        FAIL("non-strict probe failure did not return a result");
        goto out;
    }
    result = eval_stack_pop(&stack);
    fallback_count = f.sess->join_batch_fallback_count;
    if (!result.owned || !same_rows(oracle, result.rel)
        || fallback_count != 1u
        || f.sess->join_batch_last_reason
        != COL_JOIN_BATCH_EXCLUDED_NO_ARRANGEMENT
        || entry->pin_count != 0u || entry->rebuild_deferred) {
        FAIL("non-strict fallback result, reason, or lease cleanup");
        if (result.owned)
            col_rel_destroy(result.rel);
        goto out;
    }
    col_rel_destroy(result.rel);

    /* Strict mode must consume the next injected failure without leaving an
     * output or the input on the evaluation stack. */
    f.sess->join_batch_fallback_count = 0;
    f.sess->join_batch_strict = true;
    wl_columnar_arrangement_probe_test_fail_next_acquire(ENOENT);
    eval_stack_init(&stack);
    if (eval_stack_push(&stack, f.left, false) != 0) {
        FAIL("push strict input");
        goto out;
    }
    rc = col_op_join(&f.op, &stack, f.sess);
    if (rc != ENOTSUP || stack.top != 0u
        || f.sess->join_batch_fallback_count != 0u
        || entry->pin_count != 0u || entry->rebuild_deferred) {
        FAIL("strict probe failure leaked a result, fallback, or lease");
        goto out;
    }

    /* The one-shot hook is consumed by the strict attempt; normal bounded
     * dispatch must work again and must not record a fallback. */
    f.sess->join_batch_strict = false;
    eval_stack_init(&stack);
    if (eval_stack_push(&stack, f.left, false) != 0) {
        FAIL("push retry input");
        goto out;
    }
    rc = col_op_join(&f.op, &stack, f.sess);
    if (rc != 0 || stack.top != 1u) {
        FAIL("normal retry after injected failure failed");
        goto out;
    }
    result = eval_stack_pop(&stack);
    if (!result.owned || !same_rows(oracle, result.rel)
        || f.sess->join_batch_fallback_count != 0u
        || entry->pin_count != 0u || entry->rebuild_deferred) {
        FAIL("normal retry differed or left arrangement state behind");
        if (result.owned)
            col_rel_destroy(result.rel);
        goto out;
    }
    col_rel_destroy(result.rel);
    PASS();
out:
    wl_columnar_arrangement_probe_test_clear();
    if (oracle)
        col_rel_destroy(oracle);
    fixture_fini(&f);
}

/* (11b) A direct producer over an empty right relation holds the single
 * lease itself (no operator pin ahead of it), runs to completion with zero
 * rows, and tears down cleanly: the producer's empty-relation support stays
 * exercised even though the operator no longer dispatches it for that case. */
static void
test_producer_empty_right_single_lease(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1 };
    wl_columnar_continuation_t *cont = NULL;
    col_join_batch_cursor_t cur;
    int rc;

    TEST(
        "direct producer over an empty right relation completes with zero rows");
    if (!fixture_init(&f, 1u << 24, keys, 2, 2, 0)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    if (f.right->nrows != 0u) {
        FAIL("fixture right relation is not empty");
        fixture_fini(&f);
        return;
    }
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 4u * 32u, &cont);
    if (rc != 0) {
        FAIL("producer create over an empty right relation");
        fixture_fini(&f);
        return;
    }
    rc = col_join_batch_run_to_relation(cont, f.sess, f.out);
    /* No batch is ever committed, so the cursor stays at its origin with
     * sequence 0 while the continuation reports completion. */
    if (rc != 0 || f.out->nrows != 0u) {
        FAIL("empty right relation did not run to an empty result");
    } else if (!col_join_batch_cursor_get(cont, &cur)
        || cur.sequence != 0u || cur.next.lr != 0u
        || cur.next.rr != UINT32_MAX) {
        FAIL("cursor moved although no batch was committed");
    } else {
        PASS();
    }
    wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* (11c) An empty right relation yields zero rows without dispatching the
 * producer: the operator's ordinary probe handles it in one pass, so
 * join_batch_fallback_count stays 0 because nothing fell back, and the
 * window lease is released with the empty index neither pinned nor marked
 * for a deferred rebuild (a second lease on a pinned empty index is refused
 * by the registry, #1500).  A sub-row budget through the operator falls
 * back with the row-too-large reason and still equals the oracle. */
static void
test_operator_empty_right_and_row_too_large(void)
{
    wl_col_session_t *sess = make_session(1u << 24);
    col_rel_t *right = make_right(2, 0);
    const int64_t keys[] = { 0, 1 };
    col_rel_t *left = make_left(keys, 2);
    col_rel_t *big_right = NULL;
    col_rel_t *oracle = NULL;
    wl_plan_op_t op;
    const char *lk[1] = { "k" };
    const char *rk[1] = { "k" };
    eval_stack_t stack;
    eval_entry_t result;
    int rc;

    TEST("empty right relation yields zero rows without bounded dispatch; "
        "sub-row budget falls back");
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (register_relation(sess, &right, "right") != 0) {
        FAIL("register empty right");
        goto out;
    }
    init_join_op(&op, lk, rk);
    sess->join_batch_bytes = 4u * 32u;
    /* Strict mode must not fail an empty right side: the shape is eligible
     * and nothing falls back, the producer is simply not needed. */
    sess->join_batch_strict = true;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);
    rc = col_op_join(&op, &stack, sess);
    sess->join_batch_strict = false;
    if (rc != 0) {
        FAIL("bounded join over an empty right relation failed");
        goto out;
    }
    result = eval_stack_pop(&stack);
    if (!result.owned || result.rel->nrows != 0u
        || sess->join_batch_fallback_count != 0u) {
        FAIL("empty right relation did not produce an empty result");
        if (result.owned)
            col_rel_destroy(result.rel);
        goto out;
    }
    col_rel_destroy(result.rel);
    {
        /* The window lease was released and the empty index is intact. */
        const col_arr_entry_t *entry = NULL;
        for (uint32_t i = 0; i < sess->arr_count; i++) {
            if (sess->arr_entries[i].rel_name
                && strcmp(sess->arr_entries[i].rel_name, "right") == 0)
                entry = &sess->arr_entries[i];
        }
        if (!entry || entry->pin_count != 0 || entry->rebuild_deferred) {
            FAIL("empty right index left pinned or deferred");
            goto out;
        }
    }
    /* Populate a second right relation and shrink the budget below a row. */
    big_right = make_right(2, 5);
    if (!big_right) {
        FAIL("second right");
        goto out;
    }
    if (register_relation(sess, &big_right, "right") != 0) {
        FAIL("replace right relation");
        goto out;
    }
    oracle = run_oracle(sess, left, &op);
    sess->join_batch_bytes = 1u;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);
    rc = col_op_join(&op, &stack, sess);
    if (rc != 0) {
        FAIL("sub-row budget did not fall back");
        goto out;
    }
    result = eval_stack_pop(&stack);
    if (!result.owned || !oracle || !same_rows(oracle, result.rel)
        || sess->join_batch_fallback_count != 1u
        || sess->join_batch_last_reason
        != COL_JOIN_BATCH_EXCLUDED_ROW_TOO_LARGE)
        FAIL("row-too-large fallback was not recorded or differs from oracle");
    else
        PASS();
    if (result.owned)
        col_rel_destroy(result.rel);
out:
    if (oracle)
        col_rel_destroy(oracle);
    if (big_right)
        col_rel_destroy(big_right);
    if (right)
        col_rel_destroy(right);
    if (left)
        col_rel_destroy(left);
    destroy_session(sess);
}

/* Run one keyed JOIN over @left against the registered "right" and report
 * the rc, the output width and row count, and whether a fallback was
 * recorded.  The stack does not own @left. */
static int
empty_left_join(wl_col_session_t *sess, col_rel_t *left, uint32_t batch_bytes,
    bool strict, uint32_t *ncols, uint32_t *nrows)
{
    const char *lk[1] = { "k" };
    const char *rk[1] = { "k" };
    wl_plan_op_t op;
    eval_stack_t stack;
    eval_entry_t result;
    int rc;

    init_join_op(&op, lk, rk);
    sess->join_batch_bytes = batch_bytes;
    sess->join_batch_strict = strict;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);
    rc = col_op_join(&op, &stack, sess);
    sess->join_batch_strict = false;
    *ncols = 0;
    *nrows = 0;
    if (rc != 0) {
        (void)eval_stack_drain(&stack);
        return rc;
    }
    result = eval_stack_pop(&stack);
    *ncols = result.rel ? result.rel->ncols : 0;
    *nrows = result.rel ? result.rel->nrows : UINT32_MAX;
    if (result.owned)
        col_rel_destroy(result.rel);
    return 0;
}

static void
test_operator_empty_left_nothing_to_bound(void)
{
    wl_col_session_t *sess = make_session(1u << 24);
    col_rel_t *right = make_right(2, 3);
    col_rel_t *zero_col = col_rel_new_auto("left", 0);
    col_rel_t *two_col = make_left(NULL, 0);
    col_rel_t *zero_col_rows = col_rel_new_auto("left", 0);
    const int64_t no_columns[1] = { 0 };
    const char *lk[1] = { "k" };
    const char *rk[1] = { "k" };
    wl_plan_op_t op;
    uint32_t want_zero_col;
    uint32_t want_two_col;
    bool ok = true;

    TEST("empty left input yields zero rows without bounded dispatch");
    if (!sess || !right || !zero_col || !two_col || !zero_col_rows
        || col_rel_append_row(zero_col_rows, no_columns) != 0
        || register_relation(sess, &right, "right") != 0) {
        FAIL("fixture");
        goto out;
    }
    init_join_op(&op, lk, rk);
    {
        const col_rel_t *r = session_find_rel(sess, "right");
        want_zero_col = col_join_output_width(zero_col, r, &op);
        want_two_col = col_join_output_width(two_col, r, &op);
    }
    /* A zero-column placeholder has no column types to read. */
    if (zero_col->column_types != NULL || zero_col_rows->nrows != 1u) {
        FAIL("zero-column fixture shape");
        goto out;
    }
    for (int run = 0; run < 3; run++) {
        uint32_t bytes = run == 0 ? 0u : 4u * 32u;
        bool strict = run == 2;
        uint32_t ncols;
        uint32_t nrows;
        uint32_t fallbacks = sess->join_batch_fallback_count;
        int rc = empty_left_join(sess, zero_col, bytes, strict, &ncols,
                &nrows);
        if (rc != 0 || ncols != want_zero_col || nrows != 0u
            || sess->join_batch_fallback_count != fallbacks) {
            printf("(zero-column run %d: rc=%d ncols=%u nrows=%u) ", run, rc,
                ncols, nrows);
            ok = false;
        }
        rc = empty_left_join(sess, two_col, bytes, strict, &ncols, &nrows);
        if (rc != 0 || ncols != want_two_col || nrows != 0u
            || sess->join_batch_fallback_count != fallbacks) {
            printf("(two-column run %d: rc=%d ncols=%u nrows=%u) ", run, rc,
                ncols, nrows);
            ok = false;
        }
    }
    for (int run = 0; run < 3; run++) {
        /* A zero-column left that holds a row names no key column;
         * reject it in one-shot, bounded and strict bounded modes. */
        uint32_t ncols;
        uint32_t nrows;
        int rc = empty_left_join(sess, zero_col_rows,
                run == 0 ? 0u : 4u * 32u, run == 2,
                &ncols, &nrows);
        if (rc != EINVAL) {
            printf("(zero-column left with a row: rc=%d) ", rc);
            ok = false;
        }
    }
    {
        const col_arr_entry_t *entry = NULL;
        for (uint32_t i = 0; i < sess->arr_count; i++) {
            if (sess->arr_entries[i].rel_name
                && strcmp(sess->arr_entries[i].rel_name, "right") == 0)
                entry = &sess->arr_entries[i];
        }
        if (entry && (entry->pin_count != 0 || entry->rebuild_deferred)) {
            printf("(right index left pinned or deferred) ");
            ok = false;
        }
    }
    if (ok)
        PASS();
    else
        FAIL("an empty left did not join to the empty result");
out:
    col_rel_destroy(zero_col_rows);
    col_rel_destroy(two_col);
    col_rel_destroy(zero_col);
    if (right)
        col_rel_destroy(right);
    destroy_session(sess);
}

static void
test_nonempty_nullary_left_cleanup(void)
{
    const char *names[] = { "k", "r" };
    const char *keys[] = { "k" };
    const int64_t row[] = { 0, 1 };
    static uint8_t keep[] = { WL_PLAN_EXPR_BOOL, 1 };
    static uint8_t reject[] = { WL_PLAN_EXPR_BOOL, 0 };

    /* Twelve plain cases cover both probe shapes, ownership modes and all
     * batching modes. Three delta-filter cases exercise the owned filtered
     * fallback (including an empty effective right), without a cache.
     * Then cover existing-empty, missing and forced-empty right operands. */
    for (unsigned c = 0; c < 19; c++) {
        bool filtered = c >= 12 && c < 15;
        bool empty_right = c == 14;
        bool early_empty = c >= 17;
        bool owned = c >= 12 ? c == 13 : (c % 2) != 0;
        unsigned mode = c >= 12 ? 0 : (c / 2) % 3;
        unsigned width = c >= 12 ? (c == 15 ? 1 : 2) : 1 + c / 6;
        wl_col_session_t *sess = make_session(1u << 24);
        col_rel_t *right = make_rel("right", width, names);
        col_rel_t *delta = NULL;
        col_rel_t *left = col_rel_new_auto("left", 0);
        col_rel_t *valid = NULL;
        col_rel_t *result = NULL;
        eval_stack_t stack;
        wl_plan_op_t op;
        bool ok = false;

        TEST("nullary keyed refusal preserves ownership and session reuse");
        eval_stack_init(&stack);
        if (!sess || !right || !left
            || col_rel_append_row(right, row) != 0
            || col_rel_append_row(left, row) != 0
            || register_relation(sess, &right, "right") != 0)
            goto cleanup;
        init_join_op(&op, keys, keys);
        if (filtered) {
            delta = make_right_named("$d$right", 1, 1);
            if (!delta || register_relation(sess, &delta, "$d$right") != 0)
                goto cleanup;
            op.delta_mode = WL_DELTA_FORCE_DELTA;
            op.right_filter_expr = (wl_plan_expr_buffer_t){
                empty_right ? reject : keep, sizeof(keep)
            };
        }
        if (c == 15 || c == 16) {
            right = make_rel("right", width, names);
            if (!right || register_relation(sess, &right, "right") != 0)
                goto cleanup;
        }
        if (c == 17)
            op.right_relation = "missing";
        if (c == 18) {
            op.delta_mode = WL_DELTA_FORCE_DELTA;
            sess->delta_seeded = true;
        }
        if (!col_session_get_arrangement(&sess->base, "right", KEY0, 1))
            goto cleanup;
        const col_arr_entry_t *entry = find_entry(sess, "right");
        uint32_t pins = entry->pin_count;
        uint64_t reserved = reserved_of(sess);
        sess->join_batch_bytes = mode == 0 ? 0 : 128;
        sess->join_batch_strict = mode == 2;
        if (eval_stack_push(&stack, left, owned) != 0)
            goto cleanup;
        if (owned)
            left = NULL; /* The stack/operator owns the original. */
        int rc = col_op_join(&op, &stack, sess);
        if (early_empty) {
            ok = rc == 0 && stack.top == 1
                && stack.items[0].rel->nrows == 0;
            (void)eval_stack_drain(&stack);
        } else {
            ok = rc == EINVAL && stack.top == 0;
        }
        ok = ok && reserved_of(sess) == reserved
            && entry->pin_count == pins && !entry->rebuild_deferred;
        if (left)
            ok = ok && left->ncols == 0 && left->nrows == 1
                && left->columns == NULL;

        /* The same session still handles a valid keyed join. */
        init_join_op(&op, keys, keys);
        sess->delta_seeded = false;
        valid = make_left(row, 1);
        if (!valid) {
            ok = false;
            goto cleanup;
        }
        result = run_oracle(sess, valid, &op);
        uint32_t want_rows = c == 15 || c == 16 ? 0 : 1;
        ok = ok && result && result->nrows == want_rows;
        col_rel_destroy(result);
        result = NULL;
        /* A nonempty nullary relation is valid for a cross join. */
        if (!left) {
            left = col_rel_new_auto("left", 0);
            if (!left || col_rel_append_row(left, row) != 0) {
                ok = false;
                goto cleanup;
            }
        }
        op.key_count = 0;
        sess->join_batch_strict = false;
        result = run_oracle(sess, left, &op);
        ok = ok && result && result->nrows == want_rows
            && result->ncols == width;
cleanup:
        (void)eval_stack_drain(&stack);
        col_rel_destroy(result);
        col_rel_destroy(valid);
        col_rel_destroy(left);
        col_rel_destroy(delta);
        col_rel_destroy(right);
        destroy_session(sess);
        if (ok)
            PASS();
        else
            FAIL("refusal, cleanup, empty-right or valid reuse mismatch");
    }
}

static void
test_join_batch_eligibility_order(void)
{
    wl_col_session_t session;
    wl_col_session_t worker;

    TEST("bounded join eligibility has deterministic exclusion ordering");
    memset(&session, 0, sizeof(session));
    memset(&worker, 0, sizeof(worker));
    session.join_batch_bytes = 4096;
    worker.join_batch_bytes = 4096;
    worker.coordinator = &session;
    if (col_join_batch_eligibility(NULL, 1, true, true)
        != COL_JOIN_BATCH_EXCLUDED_OFF
        || col_join_batch_eligibility(&session, 0, true, true)
        != COL_JOIN_BATCH_EXCLUDED_CROSS
        || col_join_batch_eligibility(&worker, 1, true, true)
        != COL_JOIN_BATCH_EXCLUDED_WORKER
        || col_join_batch_eligibility(&session, 1, true, true)
        != COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT
        || col_join_batch_eligibility(&session, 1, false, true)
        != COL_JOIN_BATCH_EXCLUDED_FILTERED_RIGHT
        || col_join_batch_eligibility(&session, 1, false, false)
        != COL_JOIN_BATCH_ELIGIBLE) {
        FAIL("eligibility precedence changed");
        return;
    }
    PASS();
}

static bool
run_operator_fallback_case(const char *label, wl_plan_op_t *op,
    const wl_plan_op_t *oracle_op, fixture_t *f,
    col_join_batch_eligibility_t reason)
{
    eval_stack_t stack;
    eval_entry_t result = { 0 };
    col_rel_t *oracle = NULL;
    uint32_t before;
    int rc;

    oracle = run_oracle(f->sess, f->left, oracle_op);
    f->sess->join_batch_bytes = 9u * 32u;
    before = f->sess->join_batch_fallback_count;
    eval_stack_init(&stack);
    if (eval_stack_push(&stack, f->left, false) != 0)
        goto fail;
    rc = col_op_join(op, &stack, f->sess);
    if (rc != 0 || stack.top != 1u)
        goto fail;
    result = eval_stack_pop(&stack);
    if (!result.owned || !oracle || !same_rows(oracle, result.rel)
        || f->sess->join_batch_fallback_count != before + 1u
        || f->sess->join_batch_last_reason != reason)
        goto fail;
    col_rel_destroy(result.rel);
    result.rel = NULL;
    f->sess->join_batch_strict = true;
    f->sess->join_batch_fallback_count = before;
    eval_stack_init(&stack);
    if (eval_stack_push(&stack, f->left, false) != 0)
        goto fail;
    rc = col_op_join(op, &stack, f->sess);
    f->sess->join_batch_strict = false;
    if (rc != ENOTSUP || stack.top != 0u
        || f->sess->join_batch_fallback_count != before)
        goto fail;
    col_rel_destroy(oracle);
    return true;
fail:
    f->sess->join_batch_strict = false;
    if (result.owned)
        col_rel_destroy(result.rel);
    if (oracle)
        col_rel_destroy(oracle);
    printf("%s ", label);
    return false;
}

static void
test_operator_delta_and_filter_fallbacks(void)
{
    fixture_t f;
    const int64_t keys[] = { 0, 1, 1, 0 };
    col_rel_t *delta = NULL;
    wl_plan_op_t delta_op;
    wl_plan_op_t filter_op;
    wl_plan_op_t oracle_op;
    static const uint8_t true_expr[] = { WL_PLAN_EXPR_BOOL, 1 };
    bool ok = true;

    TEST(
        "delta-right and filtered-right fallbacks preserve oracle and strict mode");
    if (!fixture_init(&f, 1u << 24, keys, 4, 2, 4)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    delta = make_right(2, 4);
    if (!delta || register_relation(f.sess, &delta, "$d$right") != 0) {
        FAIL("delta relation");
        col_rel_destroy(delta);
        fixture_fini(&f);
        return;
    }
    delta_op = f.op;
    delta_op.delta_mode = WL_DELTA_FORCE_DELTA;
    oracle_op = f.op;
    ok = run_operator_fallback_case("delta-right", &delta_op, &oracle_op, &f,
            COL_JOIN_BATCH_EXCLUDED_DELTA_RIGHT);
    filter_op = f.op;
    filter_op.right_filter_expr.data = (uint8_t *)(uintptr_t)true_expr;
    filter_op.right_filter_expr.size = sizeof(true_expr);
    ok = run_operator_fallback_case("filtered-right", &filter_op, &oracle_op,
            &f,
            COL_JOIN_BATCH_EXCLUDED_FILTERED_RIGHT) && ok;
    if (ok)
        PASS();
    else
        FAIL("operator fallback matrix");
    fixture_fini(&f);
}

static void
test_stale_by_arrangement_rebuild(void)
{
    fixture_t f;
    const int64_t keys[] = { 0 };
    wl_columnar_continuation_t *cont = NULL;
    wl_columnar_continuation_sink_t sink;
    col_join_batch_relation_sink_t sctx;
    const col_arr_entry_t *entry;
    col_join_batch_cursor_t cur;
    uint32_t rows;
    wl_columnar_continuation_status_t st;

    TEST("arrangement rebuild stales a continuation and releases its pin");
    if (!fixture_init(&f, 1u << 24, keys, 1, 1, 20)
        || col_join_batch_producer_create(f.sess, &f.op, f.left, false,
        KEY0, KEY0, 1, 3u * 32u, &cont) != 0
        || col_join_batch_relation_sink_init(&sctx, &sink, f.sess, f.out)
        != 0) {
        FAIL("fixture or producer");
        if (cont)
            wl_columnar_continuation_destroy(cont);
        fixture_fini(&f);
        return;
    }
    entry = find_entry(f.sess, "right");
    st = wl_columnar_continuation_publish(cont, &sink);
    rows = f.out->nrows;
    if (st != WL_COLUMNAR_CONTINUATION_OK || rows == 0 || !entry
        || entry->pin_count != 1) {
        FAIL("initial publish did not establish a pinned arrangement");
        goto out;
    }
    wl_columnar_arrangement_test_force_stale(f.sess, "right");
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_STALE || f.out->nrows != rows
        || !col_join_batch_cursor_get(cont, &cur)
        || cur.sequence != 1u || entry->pin_count != 1) {
        FAIL("stale rebuild changed output or cursor state");
        goto out;
    }
    wl_columnar_continuation_destroy(cont);
    cont = NULL;
    if (entry->pin_count != 0) {
        FAIL("stale continuation did not release its arrangement pin");
        goto out;
    }
    PASS();
out:
    if (cont)
        wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

static void
test_sink_prepare_resize_failure(void)
{
    fixture_t f;
    const int64_t keys[] = { 0 };
    wl_columnar_continuation_t *cont = NULL;
    wl_columnar_continuation_sink_t sink;
    col_join_batch_relation_sink_t sctx;
    col_join_batch_cursor_t before;
    col_join_batch_cursor_t cursor_before;
    col_rel_t *before_output = NULL;
    col_rel_t *oracle = NULL;
    uint64_t reserved;
    uint32_t capacity;
    uint32_t timestamp_capacity;
    uint64_t view_generation;
    uint64_t storage_generation;
    uint64_t retained_reserved;
    uint32_t rows;
    wl_columnar_continuation_status_t st;

    TEST("relation sink classifies resize preparation failure and retries");
    if (!fixture_init(&f, 1u << 24, keys, 1, 1, 100)
        || col_join_batch_producer_create(f.sess, &f.op, f.left, false,
        KEY0, KEY0, 1, 16u * 32u, &cont) != 0
        || col_join_batch_relation_sink_init(&sctx, &sink, f.sess, f.out)
        != 0
        || !col_join_batch_cursor_get(cont, &before)) {
        FAIL("fixture or producer");
        if (cont)
            wl_columnar_continuation_destroy(cont);
        fixture_fini(&f);
        return;
    }
    for (uint32_t i = 0; i < 4u; i++) {
        st = wl_columnar_continuation_publish(cont, &sink);
        if (st != WL_COLUMNAR_CONTINUATION_OK) {
            FAIL("setup publish did not fill the output capacity");
            goto out;
        }
    }
    rows = f.out->nrows;
    if (rows != 64u) {
        FAIL("setup publishes did not fill the output capacity");
        goto out;
    }
    if (col_rel_deep_copy(f.out, &before_output, NULL) != 0) {
        FAIL("could not snapshot output before resize failure");
        goto out;
    }
    capacity = f.out->capacity;
    timestamp_capacity = f.out->timestamp_capacity;
    view_generation = f.out->view_generation;
    storage_generation = f.out->storage_generation;
    retained_reserved = f.out->retained_reserved_bytes;
    if (!col_join_batch_cursor_get(cont, &cursor_before)) {
        FAIL("could not snapshot cursor before resize failure");
        goto out;
    }
    reserved = reserved_of(f.sess);
    wl_columnar_relation_test_fail_next_prepare_resize();
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_SINK_FAILURE
        || st == WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED
        || f.out->nrows != rows || f.out->capacity != capacity
        || f.out->timestamp_capacity != timestamp_capacity
        || f.out->view_generation != view_generation
        || f.out->storage_generation != storage_generation
        || f.out->retained_reserved_bytes != retained_reserved
        || reserved_of(f.sess) != reserved
        || !same_rows(before_output, f.out)
        || !col_join_batch_cursor_get(cont, &before)
        || !same_join_batch_cursor(&before, &cursor_before)
        /* #2185: a non-budget failure is not reported as a denial. */
        || wl_columnar_session_budget_denied(f.sess)) {
        FAIL("resize preparation failure was misclassified or not rolled back");
        goto out;
    }
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_OK || f.out->nrows != 80u) {
        FAIL("retry after resize preparation failure did not commit");
        goto out;
    }
    if (col_join_batch_run_to_relation(cont, f.sess, f.out) != 0
        || (oracle = run_oracle(f.sess, f.left, &f.op)) == NULL
        || !same_rows(oracle, f.out)) {
        FAIL("retry result differs from one-shot oracle");
        goto out;
    }
    PASS();
out:
    wl_columnar_relation_test_clear_prepare_resize();
    if (oracle)
        col_rel_destroy(oracle);
    if (before_output)
        col_rel_destroy(before_output);
    if (cont)
        wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* Issue #2173: 4096 output rows in 7-row batches (224 bytes over four int64
 * columns).  Growing the output exactly to each batch reallocated it once
 * per batch (578 storage generations measured); geometric growth needs a
 * logarithmic number, so 24 = 2 * log2(4096) is a ceiling, not parity with
 * the one-shot join. */
static void
test_sink_growth_is_geometric(void)
{
    fixture_t f;
    const int64_t keys[] = { 0 };
    wl_columnar_continuation_t *cont = NULL;
    col_rel_t *oracle = NULL;
    char msg[128];

    TEST("relation sink grows its output geometrically across batches");
    if (!fixture_init(&f, 1u << 24, keys, 1, 1, 4096)
        || col_join_batch_producer_create(f.sess, &f.op, f.left, false,
        KEY0, KEY0, 1, 7u * 32u, &cont) != 0
        || col_join_batch_rows_per_batch(cont) != 7u) {
        FAIL("fixture or producer");
        goto out;
    }
    if (col_join_batch_run_to_relation(cont, f.sess, f.out) != 0
        || f.out->nrows != 4096u) {
        FAIL("bounded run did not complete");
        goto out;
    }
    if (f.out->storage_generation > 24u) {
        snprintf(msg, sizeof(msg),
            "output reached storage generation %" PRIu64 " (ceiling 24)",
            f.out->storage_generation);
        FAIL(msg);
        goto out;
    }
    oracle = run_oracle(f.sess, f.left, &f.op);
    if (!oracle || !same_rows(oracle, f.out)) {
        FAIL("bounded result differs from one-shot oracle");
        goto out;
    }
    PASS();
out:
    if (oracle)
        col_rel_destroy(oracle);
    if (cont)
        wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* Issue #2173 guard for the sink's exact fallback.  The budget is the
 * exact-fit figure of test_exact_fit_and_one_byte_over: it admits growing
 * the output from 64 to the 100 rows of the first batch, but not doubling
 * it to 128.  The denied doubling is retried exactly and is not a failure:
 * the publish succeeds at capacity 100 with no denial left pending.  One
 * byte less denies the exact retry too, and that denial stays pending. */
static void
test_sink_denied_doubling_falls_back_to_exact(void)
{
    for (int deny = 0; deny < 2; deny++) {
        fixture_t f;
        fixture_t g;
        const int64_t keys[] = { 0 };
        wl_columnar_continuation_t *cont = NULL;
        wl_columnar_continuation_sink_t sink;
        col_join_batch_relation_sink_t sctx;
        col_rel_t *oracle = NULL;
        uint64_t r0;
        uint64_t before;
        uint64_t after;
        uint64_t budget;
        uint64_t live;
        wl_columnar_continuation_status_t st;
        bool ok;

        memset(&g, 0, sizeof(g));
        TEST(deny ? "sink exact retry denied too keeps the denial pending"
                  : "sink retries a denied doubling at the exact need");
        if (!fixture_init(&f, 1u << 24, keys, 1, 1, 100)
            || col_join_batch_producer_create(f.sess, &f.op, f.left, false,
            KEY0, KEY0, 1, 100u * 32u, &cont) != 0
            || col_join_batch_relation_sink_init(&sctx, &sink, f.sess,
            f.out) != 0
            || !col_rel_retained_bytes_for(f.out, f.out->capacity, &before)
            || !col_rel_retained_bytes_for(f.out, 100u, &after)) {
            FAIL("fixture or producer");
            if (cont)
                wl_columnar_continuation_destroy(cont);
            fixture_fini(&f);
            continue;
        }
        r0 = reserved_of(f.sess);
        budget = r0 + 2u * after - before
            + (uint64_t)f.out->ncols * sizeof(int64_t *)
            - (uint64_t)deny;
        wl_columnar_continuation_destroy(cont);
        cont = NULL;
        fixture_fini(&f);
        if (!fixture_init(&f, budget, keys, 1, 1, 100)
            || col_join_batch_producer_create(f.sess, &f.op, f.left, false,
            KEY0, KEY0, 1, 100u * 32u, &cont) != 0
            || col_join_batch_relation_sink_init(&sctx, &sink, f.sess,
            f.out) != 0) {
            FAIL("fixture under the exact budget");
            goto next;
        }
        r0 = reserved_of(f.sess);
        st = wl_columnar_continuation_publish(cont, &sink);
        if (deny) {
            ok = st == WL_COLUMNAR_CONTINUATION_RESERVATION_DENIED
                && f.out->nrows == 0u && f.out->capacity == COL_REL_INIT_CAP
                && f.out->memory_budget_denial_pending;
            if (!ok)
                FAIL("denied exact retry was not reported as a denial");
            else
                PASS();
            goto next;
        }
        ok = st == WL_COLUMNAR_CONTINUATION_OK && f.out->nrows == 100u
            && f.out->capacity == 100u
            && !f.out->memory_budget_denial_pending
            && !f.sess->memory_budget_denied
            && col_rel_retained_live_bytes(f.out, &live)
            && live == f.out->retained_reserved_bytes
            && reserved_of(f.sess) == r0 + 2u * (after - before);
        if (!ok) {
            FAIL("denied doubling was not retried at the exact need");
            goto next;
        }
        if (col_join_batch_run_to_relation(cont, f.sess, f.out) != 0
            || f.out->capacity != 100u
            || f.out->memory_budget_denial_pending) {
            FAIL("bounded run after the exact retry did not complete");
            goto next;
        }
        /* The tight budget has no room for a one-shot result; the oracle
         * runs on an identical fixture under a large budget. */
        if (!fixture_init(&g, 1u << 24, keys, 1, 1, 100)
            || (oracle = run_oracle(g.sess, g.left, &g.op)) == NULL
            || !same_rows(oracle, f.out)) {
            FAIL("exact-retry result differs from one-shot oracle");
            goto next;
        }
        PASS();
next:
        if (oracle)
            col_rel_destroy(oracle);
        if (cont)
            wl_columnar_continuation_destroy(cont);
        fixture_fini(&g);
        fixture_fini(&f);
    }
}

static void
test_cross_key_collision_boundary(void)
{
    fixture_t f;
    const int64_t keys[] = { 0 };
    col_arrangement_t *arr;
    const col_arr_entry_t *entry;
    uint32_t head = UINT32_MAX;
    uint32_t next = UINT32_MAX;
    uint32_t bucket;
    wl_columnar_continuation_t *cont = NULL;
    wl_columnar_continuation_sink_t sink;
    col_join_batch_relation_sink_t sctx;
    col_join_batch_cursor_t cur;
    wl_columnar_continuation_status_t st;
    bool found = false;

    TEST("cross-key hash collision is preserved at a batch boundary");
    if (!fixture_init(&f, 1u << 24, keys, 1, 1024, 1)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    arr = col_session_get_arrangement((wl_session_t *)f.sess, "right", KEY0,
            1);
    entry = find_entry(f.sess, "right");
    if (!arr || !entry || arr->nbuckets == 0) {
        FAIL("arrangement");
        fixture_fini(&f);
        return;
    }
    for (bucket = 0; bucket < arr->nbuckets && !found; bucket++) {
        uint64_t tagged = arr->ht_head[bucket];
        uint32_t row = tagged == UINT64_MAX
            ? UINT32_MAX : (uint32_t)(tagged & UINT64_C(0xffffffff));
        while (row != UINT32_MAX) {
            uint32_t candidate = arr->ht_next[row];
            if (candidate != UINT32_MAX
                && f.right->columns[0][row]
                != f.right->columns[0][candidate]) {
                head = row;
                next = candidate;
                found = true;
                break;
            }
            row = candidate;
        }
    }
    if (!found || col_rel_set_raw(f.left, 0, 0, f.right->columns[0][head])
        != 0
        || col_join_batch_producer_create(f.sess, &f.op, f.left, false,
        KEY0, KEY0, 1, 1u * 32u, &cont) != 0
        || col_join_batch_relation_sink_init(&sctx, &sink, f.sess, f.out)
        != 0) {
        FAIL("could not construct a cross-key collision boundary");
        if (cont)
            wl_columnar_continuation_destroy(cont);
        fixture_fini(&f);
        return;
    }
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_OK || f.out->nrows != 1u
        || !col_join_batch_cursor_get(cont, &cur) || cur.next.lr != 0u
        || cur.next.rr != next) {
        FAIL("collision row was not retained as the resume boundary");
        goto out;
    }
    if (col_join_batch_run_to_relation(cont, f.sess, f.out) != 0) {
        FAIL("collision retry failed");
        goto out;
    }
    {
        col_rel_t *oracle = run_oracle(f.sess, f.left, &f.op);
        if (!oracle || !same_rows(oracle, f.out))
            FAIL("collision result differs from oracle");
        else
            PASS();
        if (oracle)
            col_rel_destroy(oracle);
    }
out:
    if (cont)
        wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
}

/* (11b) Session creation parses the knobs strictly. */
static void
test_env_parse(void)
{
    wl_session_t *s = NULL;
    wl_plan_t *plan = NULL;
    wirelog_error_t err;
    wirelog_program_t *prog;
    bool ok = true;

    TEST("WIRELOG_JOIN_BATCH_BYTES / _STRICT parse as strict decimals");
    prog = wirelog_parse_string("edge(1, 2).\npath(x, y) :- edge(x, y).\n",
            &err);
    if (!prog || wl_plan_from_program(prog, &plan) != 0 || !plan) {
        FAIL("plan");
        if (prog)
            wirelog_program_free(prog);
        return;
    }
    setenv("WIRELOG_JOIN_BATCH_BYTES", "4096", 1);
    setenv("WIRELOG_JOIN_BATCH_STRICT", "1", 1);
    ok = ok && wl_session_create(wl_backend_columnar(), plan, 1, &s) == 0
        && COL_SESSION(s)->join_batch_bytes == 4096u
        && COL_SESSION(s)->join_batch_strict;
    if (s)
        wl_session_destroy(s);
    s = NULL;
    setenv("WIRELOG_JOIN_BATCH_BYTES", "12x", 1);
    ok = ok && wl_session_create(wl_backend_columnar(), plan, 1, &s) == 0
        && COL_SESSION(s)->join_batch_bytes == 0u
        && !COL_SESSION(s)->join_batch_strict; /* strict ignored when off */
    if (s)
        wl_session_destroy(s);
    s = NULL;
    unsetenv("WIRELOG_JOIN_BATCH_BYTES");
    unsetenv("WIRELOG_JOIN_BATCH_STRICT");
    ok = ok && wl_session_create(wl_backend_columnar(), plan, 1, &s) == 0
        && COL_SESSION(s)->join_batch_bytes == 0u;
    if (s)
        wl_session_destroy(s);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    if (ok)
        PASS();
    else
        FAIL("knob parsing");
}

int
main(void)
{
    printf("test_join_batch_resume: bounded keyed-join sub-batches (#1446)\n");
    test_high_fanout_spans_batches();
    test_boundaries_collision_and_last_match();
    test_boundary_mid_chain();
    test_sink_failure_before_and_after_commit();
    test_sink_reserve_overflow_is_typed();
    test_run_to_relation_overflow_returns_eoverflow();
    test_pipeline_reserve_overflow_is_typed();
    test_stale_inputs_are_rejected();
    test_true_cross_key_collision_resumes();
    test_suspended_producer_survives_eviction();
    test_lazy_scratch_growth();
    test_exact_fit_and_one_byte_over();
    test_lease_released_on_every_path();
    test_descriptor_reservation_shape();
    test_descriptor_uses_source_governor();
    test_batch_payload_uses_right_governor_fallback();
    test_batch_payload_denial_rolls_back_and_retries();
    test_projected_output();
    test_projected_output_across_grow();
    test_timestamped_scratch_across_grow();
    test_grow_refusal_warn_reason();
    test_float_key();
    test_unsupported_budget_and_pooled_output();
    test_row_cap_trips_after_a_committed_batch();
    test_shared_cap_counts_only_committed_rows();
    test_operator_output_growth_denial();
    test_operator_dispatch_and_eligibility();
    test_operator_no_arrangement_probe_failure();
    test_operator_delta_and_filtered_fallbacks();
    test_diff_operator_exclusions_recorded_and_strict();
    test_diff_operator_create_failure_records_nothing();
    test_join_batch_reason_mask_distinct();
    test_high_fanout_independent_oracle();
    test_diff_operator_high_fanout_independent_oracle();
    test_diff_operator_empty_and_owned_left();
    test_diff_operator_projected_independent_oracle();
    test_operator_empty_left_delta();
    test_diff_operator_fault_retry_restores_baseline();
    test_operator_fault_during_retry();
    test_eligibility_ordering();
    test_producer_empty_right_single_lease();
    test_operator_empty_right_and_row_too_large();
    test_operator_empty_left_nothing_to_bound();
    test_nonempty_nullary_left_cleanup();
    test_join_batch_eligibility_order();
    test_operator_delta_and_filter_fallbacks();
    test_stale_by_arrangement_rebuild();
    test_sink_prepare_resize_failure();
    test_sink_growth_is_geometric();
    test_sink_denied_doubling_falls_back_to_exact();
    test_cross_key_collision_boundary();
    test_env_parse();
    printf("\n  %d tests: %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
