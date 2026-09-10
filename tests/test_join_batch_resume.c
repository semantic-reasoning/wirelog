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
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
#endif

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
        col_rel_free_contents(s->rels[i]);
        free(s->rels[i]);
    }
    free(s->rels);
    for (uint32_t i = 0; i < s->arr_count; i++) {
        free(s->arr_entries[i].rel_name);
        free(s->arr_entries[i].key_cols);
        arr_free_contents(&s->arr_entries[i].arr);
        col_arr_detach_memory_governor(&s->arr_entries[i].arr);
    }
    free(s->arr_entries);
    col_session_free_diff_arrangements(s);
    col_mat_cache_clear(&s->mat_cache);
    session_rel_free_hash(s);
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
make_right(uint32_t keys, uint32_t fanout)
{
    const char *cn[] = { "k", "r" };
    col_rel_t *right = make_rel("right", 2, cn);
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
    if (rc != 0)
        return NULL;
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

/* Resolved key positions: column 0 on both sides in every fixture. */
static const uint32_t KEY0[1] = { 0 };

typedef struct {
    wl_col_session_t *sess;
    col_rel_t *left;
    col_rel_t *right;   /* owned by the session after registration */
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
    session_add_rel(f->sess, f->right);
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
    if (st != WL_COLUMNAR_CONTINUATION_OK || f.out->nrows != 4u) {
        FAIL("first batch");
        goto out;
    }
    if (col_rel_append_row(f.right, extra) != 0) {
        FAIL("append");
        goto out;
    }
    st = wl_columnar_continuation_publish(cont, &sink);
    if (st != WL_COLUMNAR_CONTINUATION_STALE || f.out->nrows != 4u) {
        FAIL("stale right relation was not rejected");
        goto out;
    }
    PASS();
out:
    wl_columnar_continuation_destroy(cont);
    fixture_fini(&f);
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
            || before != f.out->retained_reserved_bytes
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
        budget = r0 + after - (uint64_t)deny;
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
                && reserved_of(f.sess) == r0
                && f.out->retained_reserved_bytes == before
                && entry && entry->pin_count == pins_before;
        } else {
            uint64_t now;
            ok = ok && st == WL_COLUMNAR_CONTINUATION_OK
                && f.out->nrows == 100u && cur.sequence == 1u
                && col_rel_retained_bytes_for(f.out, f.out->capacity, &now)
                && now == f.out->retained_reserved_bytes
                && reserved_of(f.sess) == r0 + after - before;
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
    wl_columnar_continuation_status_t st;
    int rc;

    TEST("arrangement lease is released once on success, cancel and error");
    if (!fixture_init(&f, 1u << 24, keys, 2, 2, 20)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    /* Success path. */
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 8u * 32u, &cont);
    entry = find_entry(f.sess, "right");
    if (rc != 0 || !entry || entry->pin_count != 1u) {
        FAIL("create did not take exactly one lease");
        goto out;
    }
    base_pins = entry->pin_count - 1u;
    rc = col_join_batch_run_to_relation(cont, f.sess, f.out);
    wl_columnar_continuation_destroy(cont);
    cont = NULL;
    if (rc != 0 || entry->pin_count != base_pins || f.out->nrows != 40u) {
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
    wl_columnar_continuation_cancel(cont);
    if (st != WL_COLUMNAR_CONTINUATION_OK || f.out->nrows != 8u
        || entry->pin_count != base_pins
        || wl_columnar_continuation_publish(cont, &sink)
        != WL_COLUMNAR_CONTINUATION_INVALID || f.out->nrows != 8u) {
        FAIL("cancel after a committed batch");
        goto out;
    }
    wl_columnar_continuation_destroy(cont);
    cont = NULL;
    if (entry->pin_count != base_pins) {
        FAIL("destroy after cancel released the lease twice");
        goto out;
    }
    /* Error path: a budget that admits the arrangement but not the scratch
     * relation denies create; the lease and the scratch reservation must
     * be gone, leaving only the arrangement's own reservation. */
    {
        /* The arrangement's own reservation is known from the generous
         * session above; a budget just above it admits the arrangement and
         * denies the 64-row scratch relation (2 KiB). */
        uint64_t arr_bytes = entry->arr.reserved_bytes;
        wl_col_session_t *tight = make_session(arr_bytes + 256u);
        col_rel_t *right2 = make_right(2, 20);
        const col_arr_entry_t *e2;
        if (!tight || !right2) {
            FAIL("tight session");
            if (right2)
                col_rel_destroy(right2);
            destroy_session(tight);
            goto out;
        }
        session_add_rel(tight, right2);
        rc = col_join_batch_producer_create(tight, &f.op, f.left, false,
                KEY0, KEY0, 1, 8u * 32u, &cont);
        e2 = find_entry(tight, "right");
        if (rc != ENOMEM || cont != NULL || (e2 && e2->pin_count != 0u)
            || reserved_of(tight) != (e2 ? e2->arr.reserved_bytes : 0u)) {
            FAIL("denied create left a lease or a reservation behind");
            destroy_session(tight);
            goto out;
        }
        destroy_session(tight);
    }
    PASS();
out:
    if (cont)
        wl_columnar_continuation_destroy(cont);
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
    int rc;

    TEST("sub-row budget is ENOTSUP; pool-owned output is refused");
    if (!fixture_init(&f, 1u << 24, keys, 1, 1, 3)) {
        FAIL("fixture");
        fixture_fini(&f);
        return;
    }
    rc = col_join_batch_producer_create(f.sess, &f.op, f.left, false, KEY0,
            KEY0, 1, 31u, &cont);
    pooled = col_rel_pool_new_auto(f.sess->delta_pool, NULL, "$pooled", 4);
    if (rc != ENOTSUP || cont != NULL || !pooled
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

/* (11) Operator dispatch: an eligible shape uses the producer under the
 * knob and equals the oracle; excluded shapes fall back with the reason
 * recorded, or fail with ENOTSUP in strict mode. */
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
        if (rc == 0) {
            result = eval_stack_pop(&stack);
            if (result.owned)
                col_rel_destroy(result.rel);
        }
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
        if (rc == 0) {
            result = eval_stack_pop(&stack);
            if (result.owned)
                col_rel_destroy(result.rel);
        }
        goto out;
    }
    PASS();
out:
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
    session_add_rel(sess, right);
    right = NULL;
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
    {
        col_rel_t *slot = session_find_rel(sess, "right");
        col_rel_free_contents(slot);
        /* Keep the session's slot: move the populated contents into it. */
        memcpy(slot, big_right, sizeof(*slot));
        free(big_right);
        big_right = NULL;
        col_session_invalidate_arrangements(&sess->base, "right");
        session_rel_free_hash(sess);
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
    test_stale_inputs_are_rejected();
    test_exact_fit_and_one_byte_over();
    test_lease_released_on_every_path();
    test_projected_output();
    test_float_key();
    test_unsupported_budget_and_pooled_output();
    test_row_cap_trips_after_a_committed_batch();
    test_operator_dispatch_and_eligibility();
    test_producer_empty_right_single_lease();
    test_operator_empty_right_and_row_too_large();
    test_env_parse();
    printf("\n  %d tests: %d passed, %d failed\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
