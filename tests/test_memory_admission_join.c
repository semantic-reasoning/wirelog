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
 * original to the materialization cache (the copy is taken with a NULL
 * governor, which is residual R1: the governor charges once while two
 * physically identical buffers are live).  So the popped stack entry is the
 * ungoverned copy, and the admission contract has to be observed on the
 * cached original.
 */
static col_rel_t *
cached_output(wl_col_session_t *sess, const col_rel_t *left)
{
    (void)left;
    if (!sess)
        return NULL;
    /* Read the entry directly rather than through col_mat_cache_lookup():
     * that helper takes a pin (cache.c, one per pin_epoch) despite its
     * header describing the result as unpinned, and a pinned entry makes
     * col_mat_cache_clear() DEFER its release -- which would hide the very
     * accounting this file checks. */
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
        /* The output must be the ONLY governed allocation, or the boundary
         * below would be about something else. */
        if (reserved_of(sess) != governed->retained_reserved_bytes)
            goto out_entry;
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
        if (!governed) {
            FAIL("the materialized output did not reach the cache");
            goto out_entry;
        }
        if (!admission_invariant(governed)) {
            FAIL("committed token does not cover the output capacity");
            goto out_entry;
        }
        if (reserved_of(sess) != budget) {
            FAIL("exact fit did not consume exactly the budget");
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

    TEST("a session denied one join still serves a smaller one");
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
    if (run_join(sess, big, &op, NULL) != ENOMEM) {
        FAIL("the growing join was not denied");
        goto out;
    }
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
    *residual_out = reserved_of(sess);
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

/* ---- main --------------------------------------------------------------- */

int
main(void)
{
    uint64_t out_bytes = 0;

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

    printf("\n  %d run, %d passed, %d failed\n",
        tests_run, tests_passed, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
