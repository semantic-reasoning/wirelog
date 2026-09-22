/*
 * test_memory_admission_join.c - governed JOIN output capacity (Issue #1477)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Covers the admission contract for heap-owned join outputs: no col_rel_t
 * reachable from join.c whose memory_governor is non-NULL ever has its
 * capacity increased without a covering reservation committed first.
 *
 * Every budget in this file is DERIVED with col_rel_retained_bytes_for()
 * rather than written as a literal.  A literal would silently stop being
 * the boundary as soon as the output width changes or timestamps are
 * enabled (col_rel_retained_bytes() adds capacity * sizeof(col_delta_
 * timestamp_t) in that case), and the test would keep passing for the
 * wrong reason.
 *
 * Cases 8 and 9 are the regression gate for the two parallel paths that
 * grow an output through col_join_reserve_exact() rather than row append.
 * They MUST fail against a join.c without the admission branch; if they
 * pass there, the fixture is not reaching the parallel path and the
 * fixture -- not the production code -- is what needs fixing.
 */
#ifndef _WIN32
#define _DEFAULT_SOURCE 1
#endif

#include "../wirelog/backend.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
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
static bool test_cache_reclaim_attempt;
static bool test_cache_pin_protected;
static wl_col_session_t *test_reclaim_sess;
static const col_rel_t *test_reclaim_source;
static bool test_fail_diff_commit;
static bool test_diff_commit_failure_injected;
static eval_stack_t *test_diff_commit_stack;
static wl_columnar_source_access_reader_t test_diff_commit_reader;
static col_rel_t *test_diff_commit_retained_rel;
static const col_rel_t *test_diff_commit_cache_original;
static const col_rel_t *test_diff_commit_control_cache;
static uint32_t *test_diff_commit_segments;
static bool test_diff_commit_refusal_witnessed;

void wl_columnar_relation_test_fail_next_governed_copy_payload_alloc(void);

static void
try_reclaim_during_governed_copy(const col_rel_t *source)
{
    if (!test_cache_reclaim_attempt)
        return;
    test_cache_reclaim_attempt = false;
    wl_col_session_t *sess = test_reclaim_sess;
    if (!sess || source != test_reclaim_source)
        return;
    uint64_t copy_bytes = 0;
    uint64_t already_charged = source->memory_governor
        == sess->memory_governor
        ? source->retained_reserved_bytes : 0;
    uint64_t governor_reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(sess->memory_governor));
    bool admission_committed = col_rel_retained_bytes_for(source,
            source->capacity, &copy_bytes)
        && already_charged <= UINT64_MAX - copy_bytes
        && governor_reserved >= already_charged + copy_bytes;
    bool source_reader_held = wl_columnar_source_access_gate_busy(
        &source->source_access);
    for (uint32_t i = 0; i < sess->mat_cache.count; i++) {
        col_mat_entry_t *entry = &sess->mat_cache.entries[i];
        if (entry->result != source)
            continue;
        const col_rel_t *retained = entry->result;
        uint32_t count = sess->mat_cache.count;
        uint64_t identity = entry->identity;
        uint64_t generation = entry->generation;
        wl_mem_reclaim_result_t reclaimed = col_mat_cache_reclaim_entry(
            &sess->mat_cache, i, generation);
        test_cache_pin_protected = reclaimed.candidates == 0
            && sess->mat_cache.count == count
            && sess->mat_cache.entries[i].identity == identity
            && sess->mat_cache.entries[i].result == retained
            && sess->mat_cache.entries[i].pin_count > 0
            && admission_committed && source_reader_held;
        return;
    }
}

static void
fail_next_diff_commit_after_mutation(
    wl_columnar_arrangement_diff_txn_t *txn)
{
    if (!test_fail_diff_commit || !txn || !txn->entry)
        return;
    if (test_diff_commit_stack) {
        if (test_diff_commit_stack->top == 0)
            return;
        eval_entry_t *entry = &test_diff_commit_stack->items[
            test_diff_commit_stack->top - 1];
        if (entry->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION || !entry->owned
            || !entry->rel || !entry->rel->memory_governor
            || !entry->rel->retained_reserved_bytes
            || col_rel_source_reader_acquire(entry->rel,
            &test_diff_commit_reader) != 0)
            return;
        for (uint32_t i = 0; i < txn->session->mat_cache.count; i++) {
            const col_rel_t *cached = txn->session->mat_cache.entries[i].result;
            if (cached && cached != entry->rel
                && cached != test_diff_commit_control_cache) {
                test_diff_commit_cache_original = cached;
                break;
            }
        }
        if (!test_diff_commit_cache_original) {
            int release_rc = col_rel_source_reader_release(
                &test_diff_commit_reader);
            (void)release_rc;
            return;
        }
        test_diff_commit_segments = calloc(3, sizeof(uint32_t));
        if (!test_diff_commit_segments) {
            int release_rc = col_rel_source_reader_release(
                &test_diff_commit_reader);
            (void)release_rc;
            return;
        }
        test_diff_commit_segments[0] = 0;
        test_diff_commit_segments[1] = 1;
        test_diff_commit_segments[2] = entry->rel->nrows;
        entry->seg_boundaries = test_diff_commit_segments;
        entry->seg_count = 2;
        entry->is_delta = true;
        test_diff_commit_retained_rel = entry->rel;
        test_diff_commit_refusal_witnessed = true;
    }
    test_fail_diff_commit = false;
    txn->entry->invalidation_deferred = true;
    test_diff_commit_failure_injected = true;
}

#define TEST(name)                                     \
        do {                                               \
            tests_run++;                                   \
            printf("  [%2d] %-64s ", tests_run, name);     \
            fflush(stdout);                                \
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

/*
 * A bare managed session.  mem_ledger budget stays 0 so
 * wl_mem_ledger_should_backpressure() is false: the Issue #224 early return
 * in col_op_join pushes an EMPTY output with rc 0, which would let several
 * cases below pass without ever generating a row.
 *
 * @workers > 1 (with coordinator NULL) is what lets
 * col_join_should_parallelize_rows() admit the parallel paths.
 */
static wl_col_session_t *
make_session_workers(uint64_t budget, uint32_t workers)
{
    wl_col_session_t *s = (wl_col_session_t *)calloc(1, sizeof(*s));
    if (!s)
        return NULL;
    s->frontier_ops = &col_frontier_epoch_ops;
    s->delta_pool = delta_pool_create(256, sizeof(col_rel_t), 1024 * 1024);
    wl_mem_ledger_init(&s->mem_ledger, 0);
    s->memory_governor = make_governor(budget);
    s->num_workers = workers;
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

static wl_col_session_t *
make_session(uint64_t budget)
{
    return make_session_workers(budget, 0);
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

static uint64_t
reserved_for(wl_columnar_memory_governor_ref_t *ref)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
}

/*
 * The invariant, as an assertion: a governed relation's committed token
 * covers exactly the footprint of its current capacity.  Checked at every
 * observation point, so a capacity raised behind the governor's back --
 * which is what col_join_reserve_exact() did before this unit -- fails
 * here rather than silently under-reporting forever.
 */
static bool
admission_invariant(const col_rel_t *r)
{
    uint64_t want = 0;

    if (!r || !r->memory_governor)
        return true;
    if (!col_rel_retained_bytes_for(r, r->capacity, &want))
        return false;
    return want == r->retained_reserved_bytes;
}

/*
 * The relation a materialized join leaves GOVERNED.
 *
 * col_op_join deep-copies its output for the evaluation stack and hands the
 * original to the materialization cache. Both retained owners have committed
 * reservations while they are live.
 */
static col_rel_t *
cached_output(wl_col_session_t *sess, const col_rel_t *left)
{
    (void)left;
    if (!sess)
        return NULL;
    /* Read the entry directly rather than through col_mat_cache_lookup():
     * the legacy helper takes one implicit epoch pin, and a pinned entry
     * makes col_mat_cache_clear() defer its release (including governed
     * bytes), which would hide the accounting this file checks. */
    for (uint32_t i = 0; i < sess->mat_cache.count; i++) {
        if (sess->mat_cache.entries[i].result
            && sess->mat_cache.entries[i].owns_result)
            return sess->mat_cache.entries[i].result;
    }
    return NULL;
}

/* ---- relation helpers -------------------------------------------------- */

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

/* left(k, v) with @n rows cycling over @keys distinct key values. */
static col_rel_t *
make_left(uint32_t n, uint32_t keys)
{
    const char *cn[] = { "k", "v" };
    col_rel_t *left = make_rel("left", 2, cn);

    if (!left)
        return NULL;
    for (uint32_t i = 0; i < n; i++) {
        int64_t row[] = { (int64_t)(keys ? i % keys : 0), (int64_t)i };
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
    op->materialized = true;
}

/* Run one materialized join, returning rc and the popped entry. */
static int
run_join(wl_col_session_t *sess, col_rel_t *left, const wl_plan_op_t *op,
    eval_entry_t *out)
{
    eval_stack_t stack;
    int rc;

    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);
    rc = col_op_join(op, &stack, sess);
    if (rc != 0) {
        if (out) {
            out->rel = NULL;
            out->owned = false;
        }
        /* The operator consumed the pushed input on failure paths; an
         * entry left behind would be a leak this test must not mask. */
        while (stack.top > 0) {
            eval_entry_t e = eval_stack_pop(&stack);
            if (e.owned)
                col_rel_destroy(e.rel);
        }
        return rc;
    }
    if (out)
        *out = eval_stack_pop(&stack);
    else {
        eval_entry_t e = eval_stack_pop(&stack);
        if (e.owned)
            col_rel_destroy(e.rel);
    }
    return 0;
}

/* Run the differential operator directly so its persistent arrangement and
 * parallel keyed reserve path are exercised rather than the ordinary join. */
static int
run_diff_join(wl_col_session_t *sess, col_rel_t *left,
    const wl_plan_op_t *op, eval_entry_t *out)
{
    eval_stack_t stack;
    int rc;

    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);
    rc = wl_columnar_join_diff_op(op, &stack, sess);
    if (rc != 0) {
        if (out) {
            out->rel = NULL;
            out->owned = false;
        }
        while (stack.top > 0) {
            eval_entry_t e = eval_stack_pop(&stack);
            if (e.owned)
                col_rel_destroy(e.rel);
        }
        return rc;
    }
    if (out)
        *out = eval_stack_pop(&stack);
    else {
        eval_entry_t e = eval_stack_pop(&stack);
        if (e.owned)
            col_rel_destroy(e.rel);
    }
    return 0;
}

/* ---- case 1: attach + baseline ----------------------------------------- */

static void
test_attach_baseline(void)
{
    const char *lk[] = { "k" };
    const char *rk[] = { "k" };
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(4, 2);
    col_rel_t *left = make_left(8, 4);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    const col_rel_t *governed;
    uint64_t before;

    TEST("materialized join output is attached to the session governor");
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    session_add_rel(sess, right);
    right = NULL;
    init_join_op(&op, lk, rk);
    before = reserved_of(sess);
    if (run_join(sess, left, &op, &result) != 0) {
        FAIL("join failed under a generous budget");
        goto out;
    }
    governed = cached_output(sess, left);
    if (!governed || governed->pool_owned) {
        FAIL("materialized output is not a heap relation in the cache");
        goto out_entry;
    }
    if (governed->memory_governor == NULL) {
        FAIL("heap join output carries no governor reservation");
        goto out_entry;
    }
    if (!admission_invariant(governed)) {
        FAIL("committed token does not cover the output capacity");
        goto out_entry;
    }
    if (reserved_of(sess) <= before) {
        FAIL("governor reserved bytes did not grow");
        goto out_entry;
    }
    PASS();
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

/* ---- cases 2 and 3: exact fit and one byte over ------------------------ */

/*
 * Cases 2 and 3 need the join OUTPUT to be the binding constraint, so they
 * use a SERIAL CROSS join: with key_count == 0 no keyed arrangement is
 * built, and the output relation is the only governed allocation in the
 * session.  A keyed join would charge the arrangement too (Issue #1425),
 * and a budget of "session total minus one" would then be denied at the
 * arrangement while the output was admitted successfully -- a boundary
 * about the wrong allocation.
 *
 * The fixture is sized so the result fits inside COL_REL_INIT_CAP: with no
 * doubling, the output's committed token IS its peak, so "exact fit" is
 * exact and "one byte over" is a real one-byte boundary.
 */
static void
init_cross_op(wl_plan_op_t *op)
{
    memset(op, 0, sizeof(*op));
    op->op = WL_PLAN_OP_JOIN;
    op->right_relation = "right";
    op->key_count = 0;
    op->delta_mode = WL_DELTA_FORCE_FULL;
    op->materialized = true;
}

static void
init_diff_keyed_op(wl_plan_op_t *op)
{
    static const char *const left_keys[] = { "k" };
    static const char *const right_keys[] = { "k" };

    memset(op, 0, sizeof(*op));
    op->op = WL_PLAN_OP_JOIN;
    op->right_relation = "right";
    op->key_count = 1;
    op->left_keys = left_keys;
    op->right_keys = right_keys;
    op->delta_mode = WL_DELTA_FORCE_FULL;
    op->materialized = true;
}

static bool
measure_output_bytes(uint64_t *out_bytes)
{
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(2, 1);
    col_rel_t *left = make_left(4, 2);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    bool ok = false;

    if (!sess || !right || !left)
        goto out;
    session_add_rel(sess, right);
    right = NULL;
    init_cross_op(&op);
    if (run_join(sess, left, &op, &result) != 0)
        goto out;
    {
        const col_rel_t *governed = cached_output(sess, left);
        if (!governed || governed->nrows > COL_REL_INIT_CAP
            || governed->retained_reserved_bytes == 0u)
            goto out_entry;
        /* A governed stack twin now lives alongside the cached output; this
         * boundary measures the output owner itself. */
        *out_bytes = governed->retained_reserved_bytes;
    }
    ok = true;
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
    return ok;
}

static void
run_at_budget(const char *name, uint64_t budget, bool expect_ok)
{
    wl_col_session_t *sess = make_session(budget);
    col_rel_t *right = make_right(2, 1);
    col_rel_t *left = make_left(4, 2);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    int rc;

    TEST(name);
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    session_add_rel(sess, right);
    right = NULL;
    init_cross_op(&op);
    rc = run_join(sess, left, &op, &result);
    if (expect_ok) {
        const col_rel_t *governed;
        if (rc != 0) {
            FAIL("join was denied at its exact-fit budget");
            goto out;
        }
        governed = cached_output(sess, left);
        if (governed && !admission_invariant(governed)) {
            FAIL("committed token does not cover the output capacity");
            goto out_entry;
        }
        if (reserved_of(sess) != budget) {
            FAIL("exact-fit single-owner fallback did not consume the budget");
            goto out_entry;
        }
        PASS();
        goto out_entry;
    }
    if (rc == 0) {
        FAIL("join succeeded one byte below the output footprint");
        goto out_entry;
    }
    if (rc != ENOMEM) {
        FAIL("denial did not surface as ENOMEM");
        goto out;
    }
    /* No arrangement is built on this path, so a denied cross join must
     * leave the governor exactly where it started. */
    if (reserved_of(sess) != 0u) {
        FAIL("denied join left reserved bytes behind");
        goto out;
    }
    PASS();
    goto out;
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

/* ---- case 4: growth denial leaves the relation intact ------------------ */

static void
test_growth_rollback(void)
{
    const char *cn[] = { "a", "b" };
    wl_col_session_t *sess;
    col_rel_t *r = NULL;
    uint64_t at_init = 0;
    uint64_t at_double = 0;
    uint64_t budget;
    uint32_t i;

    TEST("denied growth preserves capacity, rows and the committed token");
    /* Measure the two footprints under a generous governor first. */
    sess = make_session(64ull * 1024 * 1024);
    if (!sess) {
        FAIL("fixture");
        return;
    }
    r = make_rel("$join", 2, cn);
    if (!r || col_rel_attach_memory_governor(r, sess->memory_governor) != 0
        || !col_rel_retained_bytes_for(r, COL_REL_INIT_CAP, &at_init)
        || !col_rel_retained_bytes_for(r, COL_REL_INIT_CAP * 2, &at_double)) {
        FAIL("could not derive the growth footprints");
        col_rel_destroy(r);
        destroy_session(sess);
        return;
    }
    col_rel_destroy(r);
    destroy_session(sess);

    /* col_rel_reserve_capacity_admitted() commits the full new footprint
     * while the old token is still held, so the doubling peak is the sum. */
    budget = at_init + at_double - 1u;
    sess = make_session(budget);
    r = make_rel("$join", 2, cn);
    if (!sess || !r
        || col_rel_attach_memory_governor(r, sess->memory_governor) != 0
        || col_rel_reserve_capacity_admitted(r, r->capacity, NULL) != 0) {
        FAIL("fixture");
        col_rel_destroy(r);
        destroy_session(sess);
        return;
    }
    for (i = 0; i < COL_REL_INIT_CAP; i++) {
        int64_t row[] = { (int64_t)i, (int64_t)(i * 3) };
        if (col_rel_append_row(r, row) != 0) {
            FAIL("append failed inside the admitted capacity");
            goto out;
        }
    }
    {
        int64_t row[] = { -1, -1 };
        uint64_t reserved_before = reserved_of(sess);
        if (col_rel_append_row(r, row) != ENOMEM) {
            FAIL("growth past the budget was not denied");
            goto out;
        }
        if (r->capacity != COL_REL_INIT_CAP || r->nrows != COL_REL_INIT_CAP) {
            FAIL("denied growth mutated capacity or row count");
            goto out;
        }
        if (reserved_of(sess) != reserved_before) {
            FAIL("denied growth left a pending reservation committed");
            goto out;
        }
        if (!admission_invariant(r)) {
            FAIL("token no longer covers capacity after a denial");
            goto out;
        }
    }
    for (i = 0; i < COL_REL_INIT_CAP; i++) {
        if (r->columns[0][i] != (int64_t)i
            || r->columns[1][i] != (int64_t)(i * 3)) {
            FAIL("rows written before the denial were corrupted");
            goto out;
        }
    }
    PASS();
out:
    col_rel_destroy(r);
    destroy_session(sess);
}

/* ---- case 5: reuse after a denial --------------------------------------- */

/*
 * The denied join must need MORE than the recovery join, and a join output
 * always starts at COL_REL_INIT_CAP -- so a join with fewer result rows is
 * not smaller at all.  The difference has to come from a doubling: 64 left
 * rows at fanout 2 produce 128 output rows and force a growth whose peak
 * holds the old and new footprints at once, while the 4-row recovery join
 * fits inside the initial capacity.
 */
static bool
measure_big_join_peak(uint64_t *peak_out)
{
    const char *lk[] = { "k" };
    const char *rk[] = { "k" };
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(4, 2);
    col_rel_t *left = make_left(64, 4);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    bool ok = false;

    if (!sess || !right || !left)
        goto out;
    session_add_rel(sess, right);
    right = NULL;
    init_join_op(&op, lk, rk);
    if (run_join(sess, left, &op, &result) != 0)
        goto out;
    {
        const col_rel_t *governed = cached_output(sess, left);
        if (!governed || governed->capacity <= COL_REL_INIT_CAP)
            goto out_entry;  /* no doubling: the case would prove nothing */
        *peak_out = reserved_of(sess);
    }
    ok = true;
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
    return ok;
}

static void
test_reuse_after_denial(void)
{
    const char *lk[] = { "k" };
    const char *rk[] = { "k" };
    wl_col_session_t *sess = NULL;
    col_rel_t *right = make_right(4, 2);
    col_rel_t *big = make_left(64, 4);
    col_rel_t *small_left = make_left(4, 4);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    uint64_t peak = 0;
    uint64_t before;

    TEST(
        "a session falls back to one governed owner and serves a smaller join");
    if (!measure_big_join_peak(&peak)) {
        FAIL("could not measure the growing join budget");
        goto out;
    }
    sess = make_session(peak - 1u);
    if (!sess || !right || !big || !small_left) {
        FAIL("fixture");
        goto out;
    }
    session_add_rel(sess, right);
    right = NULL;
    init_join_op(&op, lk, rk);
    before = reserved_of(sess);
    if (run_join(sess, big, &op, &result) != 0 || !result.rel
        || result.rel->memory_governor != sess->memory_governor
        || !admission_invariant(result.rel)) {
        FAIL(
            "copy admission failure did not safely publish one accounted owner");
        goto out;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    if (reserved_of(sess) < before) {
        FAIL("the denial released session state it did not own");
        goto out;
    }
    if (run_join(sess, small_left, &op, &result) != 0) {
        FAIL("the session did not serve a smaller join after a denial");
        goto out;
    }
    if (!admission_invariant(cached_output(sess, small_left))) {
        FAIL("token does not cover capacity after recovery");
        goto out_entry;
    }
    PASS();
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    col_rel_destroy(big);
    col_rel_destroy(small_left);
    col_rel_destroy(right);
    destroy_session(sess);
}

/* ---- case 6: a pool-owned output stays ungoverned ----------------------- */

/*
 * The discriminator is pool_owned, not materialized.  join.c allocates the
 * output with (bounded || (materialized && !projected_join)), and
 * projected_join is (project_count > 0 && project_indices), so a
 * MATERIALIZED join with a projection is pool allocated.  Testing
 * materialized == false here would exercise the wrong branch.
 */
static void
test_projected_output_stays_pooled(void)
{
    const char *lk[] = { "k" };
    const char *rk[] = { "k" };
    const uint32_t proj[] = { 0u };
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(4, 2);
    col_rel_t *left = make_left(8, 4);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };

    TEST("materialized+projected output stays pooled and ungoverned");
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    session_add_rel(sess, right);
    right = NULL;
    init_join_op(&op, lk, rk);
    op.project_indices = proj;
    op.project_count = 1u;
    if (run_join(sess, left, &op, &result) != 0) {
        FAIL("projected join failed");
        goto out;
    }
    if (!result.rel || !result.rel->pool_owned) {
        FAIL("projected materialized output was not pool allocated");
        goto out_entry;
    }
    /* Assert on the relation, not on the session total: the keyed
     * arrangement built during the join is legitimately charged. */
    if (result.rel->memory_governor != NULL) {
        FAIL("a pooled output must not carry a governor reference");
        goto out_entry;
    }
    if (result.rel->retained_reserved_bytes != 0u) {
        FAIL("a pooled output holds a committed reservation");
        goto out_entry;
    }
    PASS();
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

/* ---- case 7: cache adoption charges once -------------------------------- */

static void
test_cache_adoption_charges_once(void)
{
    const char *lk[] = { "k" };
    const char *rk[] = { "k" };
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(4, 2);
    col_rel_t *left = make_left(8, 4);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    const col_rel_t *governed;
    uint64_t out_bytes;
    uint64_t before_clear;

    TEST("cache adoption re-parents the ledger without a second charge");
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    sess->mat_cache.ledger = &sess->mem_ledger;
    session_add_rel(sess, right);
    right = NULL;
    init_join_op(&op, lk, rk);
    if (run_join(sess, left, &op, &result) != 0) {
        FAIL("join failed");
        goto out;
    }
    /* The operator inserted its governed output into the cache itself. */
    governed = cached_output(sess, left);
    if (!governed) {
        FAIL("the materialized output did not reach the cache");
        goto out_entry;
    }
    out_bytes = governed->retained_reserved_bytes;
    if (out_bytes == 0u) {
        FAIL("the cached join output holds no reservation to account");
        goto out_entry;
    }
    /* Re-parenting moves the LEDGER link only; the governor token rides
     * with the relation, so there is no second charge. */
    if (governed->mem_ledger != NULL) {
        FAIL("cache adoption did not release the RELATION ledger link");
        goto out_entry;
    }
    /* Clearing must return exactly the output's bytes.  Other session
     * state (the keyed arrangement) is not the cache's to release. */
    before_clear = reserved_of(sess);
    col_mat_cache_clear(&sess->mat_cache);
    if (before_clear - reserved_of(sess) != out_bytes) {
        FAIL("clearing the cache did not release exactly the output bytes");
        goto out_entry;
    }
    PASS();
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

/* ---- case 8: materialized parallel cross join --------------------------- */

/*
 * Regression gate for col_join_parallel_cross(): it allocates its OWN
 * output, sizes it with col_join_reserve_exact(left->nrows * right->nrows)
 * and substitutes it for the caller's relation.  Without admission that
 * substitute reaches the materialization cache ungoverned, holding the
 * largest single allocation in join.c.
 *
 * col_join_should_parallelize_rows() requires coordinator == NULL,
 * num_workers > 1, left->nrows >= min_left AND left->nrows >= num_workers
 * * min_left -- the second conjunct is the binding one, so with
 * WIRELOG_JOIN_PAR_MIN_LEFT_ROWS=2 and 2 workers the left side needs 4+
 * rows.  join_output_limit must stay 0 and join_batch_bytes must stay 0.
 */
static void
test_parallel_cross_output_is_governed(void)
{
    wl_col_session_t *sess = make_session_workers(64ull * 1024 * 1024, 2);
    col_rel_t *right = make_right(13, 1);
    col_rel_t *left = make_left(7, 2);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    const col_rel_t *governed;

    /* 7 x 13 = 91 rows.  Past COL_REL_INIT_CAP so the bulk reserve really
     * grows, and deliberately NOT a power of two: an exact bulk reserve
     * leaves capacity == 91, whereas the serial path's row-append doubling
     * would leave 128.  Asserting capacity == 91 therefore proves the
     * PARALLEL path ran; asserting nrows alone would not, because the
     * serial cross join produces the same 91 rows. */
    TEST("parallel cross-join output is governed after substitution");
    setenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS", "2", 1);
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    session_add_rel(sess, right);
    right = NULL;
    memset(&op, 0, sizeof(op));
    op.op = WL_PLAN_OP_JOIN;
    op.right_relation = "right";
    op.key_count = 0; /* cross */
    op.delta_mode = WL_DELTA_FORCE_FULL;
    op.materialized = true;
    if (run_join(sess, left, &op, &result) != 0) {
        FAIL("parallel cross join failed under a generous budget");
        goto out;
    }
    governed = cached_output(sess, left);
    if (!governed || governed->pool_owned) {
        FAIL("cross-join output is not a heap relation in the cache");
        goto out_entry;
    }
    if (governed->nrows != 7u * 13u) {
        FAIL("the cross join did not produce the expected rows");
        goto out_entry;
    }
    if (governed->capacity != 7u * 13u) {
        FAIL("capacity is not the exact bulk size: parallel path not taken");
        goto out_entry;
    }
    if (sess->wq == NULL) {
        FAIL("no workqueue was created: parallel path not taken");
        goto out_entry;
    }
    if (governed->memory_governor == NULL) {
        FAIL("substituted cross-join output carries no governor");
        goto out_entry;
    }
    if (!admission_invariant(governed)) {
        FAIL("cross-join capacity grew without a covering reservation");
        goto out_entry;
    }
    PASS();
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    unsetenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS");
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

/* ---- case 9: bulk reserve on a governed keyed output -------------------- */

/*
 * The denial half of case 8, and the regression case for the early-return
 * window.
 *
 * Case 9 does NOT claim a one-byte boundary.  The parallel cross path holds
 * TWO charged footprints transiently -- the caller's placeholder plus the
 * substitute's own token -- and briefly THREE physical ones, because the
 * substitute's initial COL_REL_INIT_CAP buffers are resident and uncharged
 * between col_rel_new_auto() and the bulk reserve's publish.  reserved_of()
 * sampled after the join sees only the residual, not that peak.  Deriving
 * "peak - 1" from the residual would assert a boundary that
 * is nowhere near the real one.  What this case proves is the weaker but
 * true property: a budget that cannot hold the substitute is a
 * deterministic ENOMEM rather than an oversized allocation.
 *
 * Case 10 is the regression gate for the early return in
 * col_join_reserve_exact(): a cross product that FITS inside
 * COL_REL_INIT_CAP takes no growth path at all, so if the governed branch
 * sat below that early return the substitute would be attached to the
 * governor and never admitted.
 */
static bool
measure_cross_residual(uint64_t *residual_out)
{
    wl_col_session_t *sess = make_session_workers(64ull * 1024 * 1024, 2);
    col_rel_t *right = make_right(13, 1);
    col_rel_t *left = make_left(7, 2);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    bool ok = false;

    if (!sess || !right || !left)
        goto out;
    session_add_rel(sess, right);
    right = NULL;
    init_cross_op(&op);
    if (run_join(sess, left, &op, &result) != 0)
        goto out;
    *residual_out = cached_output(sess, left)->retained_reserved_bytes;
    ok = *residual_out != 0u;
out:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
    return ok;
}

static void
test_parallel_cross_denial(void)
{
    wl_col_session_t *sess = NULL;
    col_rel_t *right = make_right(13, 1);
    col_rel_t *left = make_left(7, 2);
    wl_plan_op_t op;
    uint64_t residual = 0;

    TEST("parallel cross-join bulk reserve denies deterministically");
    setenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS", "2", 1);
    if (!measure_cross_residual(&residual)) {
        FAIL("could not measure the cross-join footprint");
        goto out;
    }
    /* Strictly below the substitute's own footprint, so the bulk reserve
     * cannot fit however the transient peak is counted. */
    sess = make_session_workers(residual - 1u, 2);
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    session_add_rel(sess, right);
    right = NULL;
    init_cross_op(&op);
    if (run_join(sess, left, &op, NULL) != ENOMEM) {
        FAIL("an unaffordable cross join was not denied with ENOMEM");
        goto out;
    }
    PASS();
out:
    unsetenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS");
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

/* ---- case 10: cross product inside the initial capacity ----------------- */

static void
test_small_parallel_cross_is_admitted(void)
{
    wl_col_session_t *sess = make_session_workers(64ull * 1024 * 1024, 2);
    col_rel_t *right = make_right(2, 1);
    col_rel_t *left = make_left(4, 2);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    const col_rel_t *governed;

    TEST("cross join inside COL_REL_INIT_CAP is still admitted");
    setenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS", "2", 1);
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    session_add_rel(sess, right);
    right = NULL;
    init_cross_op(&op);
    if (run_join(sess, left, &op, &result) != 0) {
        FAIL("small parallel cross join failed under a generous budget");
        goto out;
    }
    governed = cached_output(sess, left);
    if (!governed || governed->pool_owned) {
        FAIL("cross-join output is not a heap relation in the cache");
        goto out_entry;
    }
    if (governed->nrows != 4u * 2u
        || governed->capacity != COL_REL_INIT_CAP) {
        FAIL("the fixture did not stay inside the initial capacity");
        goto out_entry;
    }
    if (sess->wq == NULL) {
        FAIL("no workqueue was created: parallel path not taken");
        goto out_entry;
    }
    if (governed->memory_governor == NULL) {
        FAIL("small cross-join output carries no governor");
        goto out_entry;
    }
    /* The whole point: no growth happened, so only an admission that runs
     * BEFORE the nrows <= capacity early return can cover these buffers. */
    if (governed->retained_reserved_bytes == 0u) {
        FAIL("governed output holds a zero token over live capacity");
        goto out_entry;
    }
    if (!admission_invariant(governed)) {
        FAIL("token does not cover the un-grown capacity");
        goto out_entry;
    }
    PASS();
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    unsetenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS");
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

/* ---- cases 11 and 12: parallel keyed differential join ---------------- */

static bool
measure_parallel_diff_peak(uint64_t *peak_out)
{
    const uint32_t expected_rows = 130u;
    wl_col_session_t *sess = make_session_workers(64ull * 1024 * 1024, 2);
    col_rel_t *right = make_right(1, 2);
    col_rel_t *left = make_left(65, 1);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    const col_rel_t *governed;
    uint64_t final_bytes;
    uint64_t initial_bytes;
    uint64_t other_bytes;
    bool ok = false;

    setenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS", "2", 1);
    if (!sess || !right || !left)
        goto out;
    if (session_add_rel(sess, right) != 0)
        goto out;
    right = NULL;
    init_diff_keyed_op(&op);
    if (run_diff_join(sess, left, &op, &result) != 0)
        goto out;
    governed = cached_output(sess, left);
    if (!governed || governed->nrows != expected_rows
        || governed->capacity != expected_rows
        || governed->pool_owned || governed->memory_governor == NULL
        || sess->wq == NULL || sess->diff_arr_count != 1
        || !admission_invariant(governed))
        goto out_entry;
    if (!col_rel_retained_bytes_for(governed, governed->capacity,
        &final_bytes)
        || !col_rel_retained_bytes_for(governed, COL_REL_INIT_CAP,
        &initial_bytes)
        || governed->retained_reserved_bytes != final_bytes)
        goto out_entry;
    other_bytes = reserved_of(sess) - governed->retained_reserved_bytes;
    /* A diff transaction deep-copies the persistent arrangement while the
     * output grows, so include that second arrangement footprint and the
     * output's initial token in the one-byte-short budget. */
    if (other_bytes > UINT64_MAX - initial_bytes
        || reserved_of(sess) > UINT64_MAX - other_bytes - initial_bytes)
        goto out_entry;
    *peak_out = reserved_of(sess) + other_bytes + initial_bytes;
    ok = *peak_out > 0;
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    unsetenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS");
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
    return ok;
}

static void
test_parallel_diff_output_is_governed(void)
{
    const uint32_t expected_rows = 130u;
    wl_col_session_t *sess = make_session_workers(64ull * 1024 * 1024, 2);
    col_rel_t *right = make_right(1, 2);
    col_rel_t *left = make_left(65, 1);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    const col_rel_t *governed;

    TEST("parallel keyed diff output is governed after bulk reserve");
    setenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS", "2", 1);
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
    right = NULL;
    init_diff_keyed_op(&op);
    if (op.key_count == 0 || op.right_filter_expr.size != 0
        || !op.materialized) {
        FAIL("diff fixture does not select the governed keyed path");
        goto out;
    }
    if (run_diff_join(sess, left, &op, &result) != 0) {
        FAIL("parallel keyed diff join failed under a generous budget");
        goto out;
    }
    governed = cached_output(sess, left);
    if (!governed || governed->pool_owned || governed->memory_governor == NULL
        || governed->nrows != expected_rows
        || governed->capacity != expected_rows
        || governed->memory_governor != sess->memory_governor
        || col_rel_get(governed, 0, 0) != 0
        || col_rel_get(governed, expected_rows - 1u, 0) != 0) {
        FAIL("parallel keyed diff output did not reach governed exact reserve");
        goto out_entry;
    }
    if (sess->wq == NULL || sess->diff_arr_count != 1) {
        FAIL("persistent parallel keyed diff path was not reached");
        goto out_entry;
    }
    if (!admission_invariant(governed)
        || governed->retained_reserved_bytes == 0u) {
        FAIL("governed diff output token does not cover capacity");
        goto out_entry;
    }
    {
        uint64_t cached_bytes = governed->retained_reserved_bytes;
        uint64_t before = reserved_of(sess);
        if (!result.rel || result.rel->memory_governor != sess->memory_governor
            || result.rel->retained_reserved_bytes == 0
            || reserved_of(sess) != before) {
            FAIL("diff miss did not charge its governed stack twin");
            goto out_entry;
        }
        uint64_t result_bytes = result.rel->retained_reserved_bytes;
        eval_stack_t cons_stack;
        eval_stack_init(&cons_stack);
        if (eval_stack_push(&cons_stack, result.rel, true) != 0) {
            col_rel_destroy(result.rel);
            result.rel = NULL;
            FAIL("could not push differential result for CONS");
            goto out_entry;
        }
        result.rel = NULL;
        if (col_op_consolidate_diff(&cons_stack, sess) != 0
            || cons_stack.top != 1 || cons_stack.items[0].rel->nrows
            != expected_rows
            || cons_stack.items[0].rel->memory_governor
            != sess->memory_governor
            || cons_stack.items[0].rel->retained_reserved_bytes != result_bytes
            || reserved_of(sess) != before) {
            (void)eval_stack_drain(&cons_stack);
            FAIL("differential CONS changed governed output accounting");
            goto out_entry;
        }
        (void)eval_stack_drain(&cons_stack);
        if (reserved_of(sess) != before - result_bytes) {
            FAIL("differential CONS teardown did not release its copy token");
            goto out_entry;
        }
        if (cached_output(sess, left) != governed
            || governed->retained_reserved_bytes != cached_bytes) {
            FAIL("differential CONS mutated its cached materialized output");
            goto out_entry;
        }
    }
    PASS();
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    unsetenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS");
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

static void
test_parallel_diff_denial(void)
{
    wl_col_session_t *sess = NULL;
    col_rel_t *right = make_right(1, 2);
    col_rel_t *left = make_left(65, 1);
    wl_plan_op_t op;
    uint64_t peak = 0;
    eval_entry_t denied_result = { 0 };

    TEST("parallel keyed diff copy OOM publishes one accounted result");
    setenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS", "2", 1);
    if (!measure_parallel_diff_peak(&peak) || peak <= 1u) {
        FAIL("could not measure the parallel diff overlap peak");
        goto out;
    }
    sess = make_session_workers(peak - 1u, 2);
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
    right = NULL;
    init_diff_keyed_op(&op);
    int rc = run_diff_join(sess, left, &op, &denied_result);
    if (rc != 0 || !denied_result.rel || !denied_result.owned
        || denied_result.rel->memory_governor != sess->memory_governor
        || !admission_invariant(denied_result.rel)
        || cached_output(sess, left) != NULL || sess->diff_arr_count != 1) {
        FAIL(
            "twin-copy OOM did not commit the differential result as one owner");
        goto out;
    }
    if (reserved_of(sess) < denied_result.rel->retained_reserved_bytes) {
        FAIL("single-owner fallback reservation is missing");
        goto out;
    }
    col_rel_destroy(denied_result.rel);
    denied_result.rel = NULL;
    PASS();
out:
    unsetenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS");
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

static void
test_differential_cache_hit_reclaim_pin(void)
{
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(1, 2);
    col_rel_t *left = make_left(65, 1);
    col_rel_t *cached_result = make_left(1, 1);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    bool cache_owns_result = false;

    TEST("differential cache pin protects cached result during governed copy");
    if (!sess || !right || !left || !cached_result) {
        FAIL("fixture");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
    right = NULL;
    if (col_mat_cache_insert(&sess->mat_cache, left,
        session_find_rel(sess, "right"), cached_result) != 0) {
        FAIL("could not seed differential cache hit");
        goto out;
    }
    cache_owns_result = true;
    init_diff_keyed_op(&op);
    test_cache_pin_protected = false;
    test_cache_reclaim_attempt = true;
    test_reclaim_sess = sess;
    test_reclaim_source = cached_result;
    if (run_diff_join(sess, left, &op, &result) != 0 || !result.rel
        || !result.owned || !test_cache_pin_protected
        || cached_output(sess, left) != cached_result
        || result.rel == cached_result
        || result.rel->memory_governor != sess->memory_governor
        || !admission_invariant(result.rel)
        || reserved_of(sess) != result.rel->retained_reserved_bytes) {
        FAIL("differential cache hit did not retain pin and charge its copy");
        goto out_result;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    if (reserved_of(sess) != 0u) {
        FAIL("differential cache-hit copy did not release its reservation");
        goto out;
    }
    PASS();
    goto out;
out_result:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    if (!cache_owns_result)
        col_rel_destroy(cached_result);
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

static void
test_parallel_diff_true_admission_denial_rolls_back(void)
{
    wl_col_session_t *sess = make_session_workers(1u, 2);
    col_rel_t *right = make_right(1, 2);
    col_rel_t *left = make_left(65, 1);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };

    TEST("parallel keyed diff admission denial rolls back transaction state");
    setenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS", "2", 1);
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
    right = NULL;
    init_diff_keyed_op(&op);
    if (run_diff_join(sess, left, &op, &result) != ENOMEM || result.rel
        || reserved_of(sess) != 0u || sess->mat_cache.count != 0
        || sess->diff_arr_count != 0) {
        FAIL("true admission denial published output or committed diff state");
        goto out;
    }
    PASS();
out:
    unsetenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS");
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

static void
test_diff_commit_failure_unwinds_pushed_materialization(void)
{
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(2, 1);
    col_rel_t *left = make_left(4, 2);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };

    TEST(
        "post-push diff commit refusal rolls back cache, twin and reservation");
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
    right = NULL;
    init_diff_keyed_op(&op);
    test_diff_commit_failure_injected = false;
    test_fail_diff_commit = true;
    int rc = run_diff_join(sess, left, &op, &result);
    if (rc != EBUSY || !test_diff_commit_failure_injected || result.rel
        || sess->mat_cache.count != 0 || sess->diff_arr_count != 0
        || sess->diff_txn_count != 0 || reserved_of(sess) != 0u) {
        FAIL("commit refusal published or retained differential output state");
        goto out_entry;
    }
    PASS();
    goto out;
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    test_fail_diff_commit = false;
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

static bool
run_refused_diff_result_cleanup(uint32_t stack_depth)
{
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(2, 1);
    col_rel_t *left = make_left(4, 2);
    col_rel_t *control_left = make_left(1, 2);
    wl_plan_op_t op;
    wl_plan_op_t control_op;
    eval_stack_t stack = { 0 };
    eval_entry_t retry = { 0 };
    eval_entry_t control_result = { 0 };
    bool ok = false;
    uint64_t baseline = 0;
    uint64_t charged = 0;
    uint64_t owner_bits = 0;
    uint64_t reservation_state = 0;
    wl_columnar_memory_reservation_t *reservation = NULL;
    wl_columnar_memory_governor_t *reservation_governor = NULL;
    const void *reservation_identity = NULL;
    uint32_t *segments = NULL;

    if (!sess || !right || !left || !control_left || stack_depth == 0
        || stack_depth > COL_STACK_MAX)
        goto out;
    if (session_add_rel(sess, right) != 0)
        goto out;
    right = NULL;
    const char *left_keys[] = { "k" };
    const char *right_keys[] = { "k" };
    init_join_op(&control_op, left_keys, right_keys);
    if (run_join(sess, control_left, &control_op, &control_result) != 0
        || !control_result.rel || !control_result.owned
        || sess->mat_cache.count != 1)
        goto out;
    test_diff_commit_control_cache = sess->mat_cache.entries[0].result;
    if (!test_diff_commit_control_cache || control_result.rel
        == test_diff_commit_control_cache)
        goto out;
    col_rel_destroy(control_result.rel);
    control_result.rel = NULL;
    baseline = reserved_of(sess);
    init_diff_keyed_op(&op);
    eval_stack_init(&stack);
    for (uint32_t i = 0; i < stack_depth; i++) {
        if (eval_stack_push(&stack, left, false) != 0)
            goto out;
    }
    test_diff_commit_stack = &stack;
    test_diff_commit_cache_original = NULL;
    test_diff_commit_retained_rel = NULL;
    test_diff_commit_segments = NULL;
    test_diff_commit_refusal_witnessed = false;
    memset(&test_diff_commit_reader, 0, sizeof(test_diff_commit_reader));
    test_diff_commit_failure_injected = false;
    test_fail_diff_commit = true;
    int rc = wl_columnar_join_diff_op(&op, &stack, sess);
    test_diff_commit_stack = NULL;
    if (rc != EBUSY || !test_diff_commit_failure_injected
        || !test_diff_commit_refusal_witnessed || stack.top != stack_depth
        || !test_diff_commit_retained_rel || !test_diff_commit_segments
        || sess->mat_cache.count != 1
        || sess->mat_cache.entries[0].result
        != test_diff_commit_control_cache
        || sess->diff_arr_count != 0
        || sess->diff_txn_count != 0)
        goto out;
    eval_entry_t *retained = &stack.items[stack.top - 1];
    segments = retained->seg_boundaries;
    charged = retained->rel->retained_reserved_bytes;
    reservation = &retained->rel->retained_reservation;
    reservation_governor = reservation->governor;
    reservation_identity = reservation->identity;
    owner_bits = atomic_load_explicit(&reservation->owner_bits,
            memory_order_acquire);
    reservation_state = atomic_load_explicit(&reservation->state,
            memory_order_acquire);
    if (retained->rel != test_diff_commit_retained_rel
        || retained->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
        || !retained->owned || !retained->is_delta
        || retained->seg_count != 2 || segments != test_diff_commit_segments
        || segments[0] != 0 || segments[1] != 1
        || segments[2] != retained->rel->nrows
        || retained->rel->memory_governor != sess->memory_governor
        || reservation_governor
        != wl_columnar_memory_governor_ref_get(sess->memory_governor)
        || !reservation_identity || owner_bits == 0
        || reservation_state != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        || reservation->bytes != charged
        || !charged || reserved_of(sess) != baseline + charged)
        goto out;
    for (uint32_t i = 0; i + 1 < stack_depth; i++) {
        if (stack.items[i].rel != left || stack.items[i].owned)
            goto out;
    }
    if (eval_stack_drain(&stack) != EBUSY || stack.top != stack_depth
        || stack.items[stack.top - 1].rel != test_diff_commit_retained_rel
        || stack.items[stack.top - 1].seg_boundaries != segments
        || stack.items[stack.top - 1].seg_count != 2
        || stack.items[stack.top - 1].rel->retained_reserved_bytes != charged
        || &stack.items[stack.top - 1].rel->retained_reservation
        != reservation
        || reservation->identity != reservation_identity
        || reservation->governor != reservation_governor
        || atomic_load_explicit(&reservation->owner_bits,
        memory_order_acquire) != owner_bits
        || atomic_load_explicit(&reservation->state,
        memory_order_acquire) != reservation_state
        || reservation->bytes != charged
        || reserved_of(sess) != baseline + charged)
        goto out;
    if (col_rel_source_reader_release(&test_diff_commit_reader) != 0)
        goto out;
    if (eval_stack_drain(&stack) != 0 || stack.top != 0
        || reserved_of(sess) != baseline)
        goto out;
    test_diff_commit_retained_rel = NULL;
    test_diff_commit_segments = NULL;

    static const int64_t expected[][4] = {
        { 0, 0, 0, 0 }, { 1, 1, 1, 1000 },
        { 0, 2, 0, 0 }, { 1, 3, 1, 1000 },
    };
    if (run_diff_join(sess, left, &op, &retry) != 0 || !retry.rel
        || retry.rel->nrows != 4 || retry.rel->ncols != 4)
        goto out;
    for (uint32_t row = 0; row < 4; row++) {
        for (uint32_t col = 0; col < 4; col++) {
            if (col_rel_get(retry.rel, row, col) != expected[row][col])
                goto out;
        }
    }
    if (retry.owned) {
        col_rel_destroy(retry.rel);
        retry.rel = NULL;
    }
    ok = true;
out:
    test_diff_commit_stack = NULL;
    test_fail_diff_commit = false;
    if (test_diff_commit_reader.owner) {
        int release_rc = col_rel_source_reader_release(
            &test_diff_commit_reader);
        (void)release_rc;
    }
    if (retry.owned && retry.rel)
        col_rel_destroy(retry.rel);
    if (control_result.owned && control_result.rel)
        col_rel_destroy(control_result.rel);
    if (stack.top > 0)
        (void)eval_stack_drain(&stack);
    test_diff_commit_retained_rel = NULL;
    test_diff_commit_cache_original = NULL;
    test_diff_commit_control_cache = NULL;
    test_diff_commit_segments = NULL;
    col_rel_destroy(left);
    col_rel_destroy(control_left);
    col_rel_destroy(right);
    destroy_session(sess);
    return ok;
}

static void
test_diff_commit_refusal_retains_complete_stack_entry(void)
{
    TEST(
        "post-push disposal refusal retains complete JOIN result at stack limit");
    if (!run_refused_diff_result_cleanup(1)
        || !run_refused_diff_result_cleanup(COL_STACK_MAX)) {
        FAIL("refused destruction lost entry ownership or accounting");
        return;
    }
    PASS();
}

/* ---- case 13: materialized cache/eval-stack twin contract ------------- */

static void
test_materialized_stack_copy_contract(void)
{
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(2, 1);
    col_rel_t *left = make_left(4, 2);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    const col_rel_t *cached;
    uint64_t reserved, cache_bytes, copy_bytes;

    TEST("materialized join stack copy keeps the transient twin contract");
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
    right = NULL;
    init_cross_op(&op);
    if (run_join(sess, left, &op, &result) != 0 || !result.rel
        || !result.owned) {
        FAIL("materialized join did not return an owned stack copy");
        goto out;
    }
    cached = cached_output(sess, left);
    if (!cached || cached == result.rel || cached->pool_owned
        || cached->memory_governor != sess->memory_governor
        || !admission_invariant(cached)
        || result.rel->memory_governor != sess->memory_governor
        || !admission_invariant(result.rel)
        || result.rel->pool_owned
        || result.rel->nrows != cached->nrows
        || result.rel->ncols != cached->ncols
        || col_rel_get(result.rel, 0, 0) != col_rel_get(cached, 0, 0)) {
        FAIL("cache and stack twin contract mismatch");
        goto out_entry;
    }
    reserved = reserved_of(sess);
    cache_bytes = cached->retained_reserved_bytes;
    copy_bytes = result.rel->retained_reserved_bytes;
    if (cache_bytes == 0 || copy_bytes == 0
        || reserved != cache_bytes + copy_bytes) {
        FAIL("cache and stack twin reservations are not both charged");
        goto out_entry;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    if (reserved_of(sess) != cache_bytes) {
        FAIL("destroying stack twin did not release exactly its reservation");
        goto out;
    }
    test_cache_pin_protected = false;
    test_cache_reclaim_attempt = true;
    test_reclaim_sess = sess;
    test_reclaim_source = cached;
    if (run_join(sess, left, &op, &result) != 0 || !result.rel
        || !test_cache_pin_protected
        || result.rel == cached || result.rel->memory_governor
        != sess->memory_governor
        || result.rel->retained_reserved_bytes != copy_bytes
        || col_rel_get(result.rel, 0, 0) != col_rel_get(cached, 0, 0)
        || reserved_of(sess) != cache_bytes + copy_bytes) {
        FAIL("cache hit did not return a governed, value-identical copy");
        goto out_entry;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    if (reserved_of(sess) != cache_bytes) {
        FAIL("cache-hit copy release changed cache reservation");
        goto out;
    }
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            sess->memory_governor)->usable_bytes, cache_bytes - 1u,
        memory_order_release);
    if (run_join(sess, left, &op, &result) != ENOMEM || result.rel
        || cached_output(sess, left) != cached
        || reserved_of(sess) != cache_bytes) {
        FAIL(
            "reclaim pressure evicted or changed the cache entry during its pin");
        goto out_entry;
    }
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            sess->memory_governor)->usable_bytes, 64ull * 1024 * 1024,
        memory_order_release);
    if (run_join(sess, left, &op, &result) != 0 || !result.rel) {
        FAIL("cache-hit copy did not recover after admission was restored");
        goto out_entry;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    col_mat_cache_clear(&sess->mat_cache);
    if (reserved_of(sess) != 0u) {
        FAIL("cache clear did not release the governed original once");
        goto out;
    }
    PASS();
    goto out;
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

static void
test_cache_copy_governor_precedence(void)
{
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    wl_columnar_memory_governor_ref_t *source_governor
        = sess ? sess->memory_governor : NULL;
    wl_columnar_memory_governor_ref_t *other = make_governor(
        64ull * 1024 * 1024);
    col_rel_t *right = make_right(2, 1);
    col_rel_t *left = make_left(4, 2);
    col_rel_t *cached = NULL;
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    bool cache_owns_result = false;
    uint64_t source_bytes = 0;
    uint64_t copy_bytes = 0;

    TEST("materialized cache copy prefers session then source governor");
    if (!sess || !other || !right || !left
        || col_rel_deep_copy(left, &cached, NULL) != 0
        || col_rel_attach_memory_governor(cached, sess->memory_governor) != 0) {
        FAIL("fixture");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
    right = NULL;
    source_bytes = cached->retained_reserved_bytes;
    if (col_mat_cache_insert(&sess->mat_cache, left,
        session_find_rel(sess, "right"), cached) != 0) {
        FAIL("could not seed governed cache result");
        goto out;
    }
    cache_owns_result = true;
    init_cross_op(&op);

    sess->memory_governor = other;
    if (run_join(sess, left, &op, &result) != 0 || !result.rel
        || result.rel->memory_governor != other
        || cached->memory_governor != source_governor
        || !admission_invariant(result.rel)) {
        FAIL("cache hit did not select the session governor");
        goto out_result;
    }
    copy_bytes = result.rel->retained_reserved_bytes;
    if (reserved_for(source_governor) != source_bytes
        || reserved_for(other) != copy_bytes || copy_bytes == 0) {
        FAIL("session-governor copy changed source accounting");
        goto out_result;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    if (reserved_for(other) != 0u) {
        FAIL("session-governor copy did not release its own token");
        goto out;
    }

    sess->memory_governor = NULL;
    if (run_join(sess, left, &op, &result) != 0 || !result.rel
        || result.rel->memory_governor != source_governor
        || !admission_invariant(result.rel)
        || reserved_for(source_governor)
        != source_bytes + result.rel->retained_reserved_bytes
        || reserved_for(other) != 0u) {
        FAIL("cache hit did not fall back to the source governor");
        goto out_result;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    sess->memory_governor = source_governor;
    if (reserved_for(source_governor) != source_bytes) {
        FAIL("source-governor copy did not release its own token");
        goto out;
    }
    col_mat_cache_clear(&sess->mat_cache);
    if (reserved_for(source_governor) != 0u) {
        FAIL("cache teardown did not release source-governed result");
        goto out;
    }
    PASS();
    goto out;
out_result:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    if (sess)
        sess->memory_governor = source_governor;
    if (cache_owns_result && sess)
        col_mat_cache_clear(&sess->mat_cache);
    if (!cache_owns_result)
        col_rel_destroy(cached);
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
    if (other) {
        if (reserved_for(other) != 0u) {
            fprintf(stderr,
                "session governor precedence test leaked B reservation\n");
            tests_failed++;
        }
        wl_columnar_memory_governor_ref_release(other);
    }
}

static void
test_governed_copy_payload_failure_falls_back_to_accounted_original(void)
{
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(2, 1);
    col_rel_t *left = make_left(4, 2);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };

    TEST(
        "post-admission twin allocation failure publishes one accounted owner");
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
    right = NULL;
    init_cross_op(&op);
    wl_columnar_relation_test_fail_next_governed_copy_payload_alloc();
    if (run_join(sess, left, &op, &result) != 0 || !result.rel
        || !result.owned || result.rel->memory_governor != sess->memory_governor
        || !admission_invariant(result.rel)
        || cached_output(sess, left) != NULL
        || reserved_of(sess) != result.rel->retained_reserved_bytes) {
        FAIL(
            "failed twin admission leaked a token or published an unaccounted result");
        goto out_entry;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    if (reserved_of(sess) != 0u) {
        FAIL("single-owner cleanup left its reservation behind");
        goto out;
    }
    PASS();
    goto out;
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

static void
test_materialized_cache_insert_failure_unwinds_both_results(void)
{
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(2, 1);
    col_rel_t *left = make_left(4, 2);
    col_mat_cache_pin_t pins[COL_MAT_CACHE_MAX] = { 0 };
    wl_plan_op_t op;
    eval_entry_t ordinary = { 0 }, differential = { 0 };
    bool fixture_ok = sess && right && left;

    TEST("full pinned cache falls back to one accounted JOIN owner");
    if (!fixture_ok) {
        FAIL("fixture");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
    right = NULL;
    for (uint32_t i = 0; i < COL_MAT_CACHE_MAX; i++) {
        col_rel_t *fill_left = make_left(1, 1);
        col_rel_t *fill_right = make_right(1, 1);
        col_rel_t *fill_result = make_left(1, 1);
        if (!fill_left || !fill_right || !fill_result) {
            col_rel_destroy(fill_left);
            col_rel_destroy(fill_right);
            col_rel_destroy(fill_result);
            fixture_ok = false;
            break;
        }
        fill_right->columns[0][0] = (int64_t)i + 100;
        if (col_mat_cache_insert_pin(&sess->mat_cache, fill_left,
            fill_right, fill_result, &pins[i]) != 0) {
            col_rel_destroy(fill_left);
            col_rel_destroy(fill_right);
            col_rel_destroy(fill_result);
            fixture_ok = false;
            break;
        }
        col_rel_destroy(fill_left);
        col_rel_destroy(fill_right);
    }
    if (!fixture_ok || sess->mat_cache.count != COL_MAT_CACHE_MAX) {
        FAIL("could not fill and pin every cache slot");
        goto out;
    }
    init_cross_op(&op);
    if (run_join(sess, left, &op, &ordinary) != 0 || !ordinary.rel
        || !ordinary.owned
        || ordinary.rel->memory_governor != sess->memory_governor
        || !admission_invariant(ordinary.rel)
        || sess->mat_cache.count != COL_MAT_CACHE_MAX
        || reserved_of(sess) != ordinary.rel->retained_reserved_bytes) {
        FAIL("ENOSPC did not publish one already-accounted JOIN result");
        goto out;
    }
    col_rel_destroy(ordinary.rel);
    ordinary.rel = NULL;
    if (reserved_of(sess) != 0u) {
        FAIL("ordinary single-owner fallback failed to release its token");
        goto out;
    }

    init_diff_keyed_op(&op);
    if (run_diff_join(sess, left, &op, &differential) != 0
        || !differential.rel || !differential.owned
        || differential.rel->memory_governor != sess->memory_governor
        || !admission_invariant(differential.rel)
        || sess->mat_cache.count != COL_MAT_CACHE_MAX
        || sess->diff_arr_count != 1) {
        FAIL(
            "ENOSPC did not commit differential fallback as one accounted owner");
        goto out;
    }
    uint64_t diff_reserved = reserved_of(sess);
    uint64_t differential_bytes = differential.rel->retained_reserved_bytes;
    if (diff_reserved < differential_bytes || differential_bytes == 0) {
        FAIL("differential fallback has no retained reservation");
        col_rel_destroy(differential.rel);
        differential.rel = NULL;
        goto out;
    }
    col_rel_destroy(differential.rel);
    differential.rel = NULL;
    differential.owned = false;
    if (reserved_of(sess) != diff_reserved - differential_bytes) {
        FAIL("differential fallback did not release its single output token");
        goto out;
    }
    PASS();
out:
    if (ordinary.owned && ordinary.rel)
        col_rel_destroy(ordinary.rel);
    if (differential.owned && differential.rel)
        col_rel_destroy(differential.rel);
    for (uint32_t i = 0; i < COL_MAT_CACHE_MAX; i++)
        col_mat_cache_pin_release(&pins[i]);
    if (sess)
        col_mat_cache_clear(&sess->mat_cache);
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

static void
test_governed_join_consumers_release_exactly_once(void)
{
    static const uint32_t map_col[] = { 0u };
    static uint8_t filter_bytes[] = {
        WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '3',
        WL_PLAN_EXPR_CONST_INT, 0, 0, 0, 0, 0, 0, 0, 0,
        WL_PLAN_EXPR_CMP_GT
    };
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(2, 1);
    col_rel_t *left = make_left(4, 2);
    eval_stack_t stack;
    wl_plan_op_t join_op, filter_op, map_op;
    col_rel_t *cache_entry = NULL;
    uint64_t cache_bytes = 0;
    bool stack_ready = false;

    TEST(
        "governed materialized JOIN survives FILTER/MAP and CONCAT/CONSOLIDATE");
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
    right = NULL;
    init_cross_op(&join_op);
    memset(&filter_op, 0, sizeof(filter_op));
    filter_op.op = WL_PLAN_OP_FILTER;
    filter_op.filter_expr.data = filter_bytes;
    filter_op.filter_expr.size = sizeof(filter_bytes);
    memset(&map_op, 0, sizeof(map_op));
    map_op.op = WL_PLAN_OP_MAP;
    map_op.project_indices = map_col;
    map_op.project_count = 1;

    eval_stack_init(&stack);
    stack_ready = true;
    if (eval_stack_push(&stack, left, false) != 0
        || wl_columnar_join_op(&join_op, &stack, sess) != 0
        || stack.top != 1 || !stack.items[0].rel
        || !stack.items[0].owned
        || stack.items[0].rel->memory_governor != sess->memory_governor) {
        FAIL("materialized JOIN did not leave its governed stack copy");
        goto out;
    }
    cache_entry = cached_output(sess, left);
    if (!cache_entry || !admission_invariant(cache_entry)
        || stack.items[0].rel->retained_reserved_bytes == 0
        || reserved_of(sess) != cache_entry->retained_reserved_bytes
        + stack.items[0].rel->retained_reserved_bytes) {
        FAIL("JOIN did not charge cache and stack outputs exactly");
        goto out;
    }
    cache_bytes = cache_entry->retained_reserved_bytes;
    int filter_rc = wl_columnar_filter_op(&filter_op, &stack, sess);
    if (filter_rc != 0 || stack.top != 1 || stack.items[0].rel->nrows != 4
        || stack.items[0].rel->memory_governor != sess->memory_governor
        || reserved_of(sess) != cache_bytes
        + stack.items[0].rel->retained_reserved_bytes) {
        FAIL(
            "FILTER did not consume the governed copy and preserve cache charge");
        goto out;
    }
    if (col_op_map(&map_op, &stack, sess) != 0
        || stack.top != 1 || stack.items[0].rel->ncols != 1
        || stack.items[0].rel->nrows != 4
        || reserved_of(sess) != cache_bytes) {
        FAIL("MAP changed retained JOIN cache accounting");
        goto out;
    }
    if (eval_stack_drain(&stack) != 0 || reserved_of(sess) != cache_bytes) {
        FAIL("downstream output teardown changed the cache reservation");
        goto out;
    }
    stack_ready = false;

    eval_entry_t joined = { 0 };
    col_rel_t *duplicate = NULL;
    if (run_join(sess, left, &join_op, &joined) != 0 || !joined.rel
        || col_rel_deep_copy(joined.rel, &duplicate, NULL) != 0) {
        FAIL("could not prepare CONCAT/CONSOLIDATE inputs");
        if (joined.owned && joined.rel)
            col_rel_destroy(joined.rel);
        goto out;
    }
    eval_stack_init(&stack);
    stack_ready = true;
    if (eval_stack_push(&stack, joined.rel, joined.owned) != 0) {
        col_rel_destroy(joined.rel);
        col_rel_destroy(duplicate);
        FAIL("could not push governed CONCAT input");
        goto out;
    }
    joined.rel = NULL;
    if (eval_stack_push(&stack, duplicate, true) != 0) {
        col_rel_destroy(duplicate);
        FAIL("could not push duplicate CONCAT input");
        goto out;
    }
    duplicate = NULL;
    if (col_op_concat(&stack, sess) != 0 || stack.top != 1
        || reserved_of(sess) != cache_bytes) {
        FAIL("CONCAT did not release the governed input twin once");
        goto out;
    }
    if (col_op_consolidate(&stack, sess) != 0 || stack.top != 1
        || stack.items[0].rel->nrows != 8u
        || reserved_of(sess) != cache_bytes) {
        FAIL("CONSOLIDATE changed result or retained-cache accounting");
        goto out;
    }
    if (eval_stack_drain(&stack) != 0) {
        FAIL("could not release consolidated output");
        goto out;
    }
    stack_ready = false;
    col_mat_cache_clear(&sess->mat_cache);
    if (reserved_of(sess) != 0u) {
        FAIL("cache teardown did not release the final governed owner");
        goto out;
    }
    PASS();
out:
    if (stack_ready)
        (void)eval_stack_drain(&stack);
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

/* ---- main --------------------------------------------------------------- */

int
main(void)
{
    uint64_t out_bytes = 0;

    wl_columnar_relation_test_after_governed_copy_admission =
        try_reclaim_during_governed_copy;
    wl_columnar_join_test_before_diff_commit =
        fail_next_diff_commit_after_mutation;

    printf("Memory admission: JOIN output capacity (Issue #1477)\n");

    test_attach_baseline();
    if (measure_output_bytes(&out_bytes)) {
        run_at_budget("cross join succeeds at exactly its output footprint",
            out_bytes, true);
        run_at_budget("cross join is denied one byte below that footprint",
            out_bytes - 1u, false);
    } else {
        TEST("cross join succeeds at exactly its output footprint");
        FAIL("could not measure the output footprint");
        TEST("cross join is denied one byte below that footprint");
        FAIL("could not measure the output footprint");
    }
    test_growth_rollback();
    test_reuse_after_denial();
    test_projected_output_stays_pooled();
    test_cache_adoption_charges_once();
    test_parallel_cross_output_is_governed();
    test_parallel_cross_denial();
    test_small_parallel_cross_is_admitted();
    test_parallel_diff_output_is_governed();
    test_parallel_diff_denial();
    test_parallel_diff_true_admission_denial_rolls_back();
    test_diff_commit_failure_unwinds_pushed_materialization();
    test_diff_commit_refusal_retains_complete_stack_entry();
    test_differential_cache_hit_reclaim_pin();
    test_materialized_stack_copy_contract();
    test_cache_copy_governor_precedence();
    test_governed_copy_payload_failure_falls_back_to_accounted_original();
    test_materialized_cache_insert_failure_unwinds_both_results();
    test_governed_join_consumers_release_exactly_once();

    wl_columnar_relation_test_after_governed_copy_admission = NULL;
    wl_columnar_join_test_before_diff_commit = NULL;

    printf("\n  %d run, %d passed, %d failed\n",
        tests_run, tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
