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
#include "../wirelog/wirelog-extension.h"

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
extern bool wl_columnar_join_test_fail_next_key_scratch_alloc;
extern int (*wl_columnar_join_test_submit_override)(wl_work_queue_t *,
    void (*)(void *), void *);
extern uint32_t wl_columnar_join_test_last_keyed_workers;
extern void (*wl_columnar_join_test_before_pair_growth)(
    wl_columnar_memory_governor_t *, uint64_t, uint64_t);
extern bool wl_columnar_join_test_fail_pair_realloc;
extern atomic_bool wl_columnar_join_test_fail_pair_commit;
extern uint32_t wl_columnar_join_test_pair_cap_limit_override;
extern bool wl_columnar_join_test_last_cross_ctx_admitted;
extern bool wl_columnar_join_test_last_keyed_parallel_admitted;
extern bool wl_columnar_join_test_last_semijoin_parallel_admitted;
extern bool wl_columnar_merge_test_fail_concat_boundaries_alloc;

static uint32_t test_join_submit_calls;
static uint32_t test_join_submit_fail_at;
static bool test_pair_growth_denial_injected;

static void
deny_pair_growth_with_governor_limit(wl_columnar_memory_governor_t *governor,
    uint64_t old_bytes, uint64_t new_bytes)
{
    if (!governor || old_bytes == 0 || new_bytes == 0)
        return;
    uint64_t reserved = wl_columnar_memory_reserved(governor);
    if (reserved > UINT64_MAX - new_bytes)
        return;
    atomic_store_explicit(&governor->usable_bytes,
        reserved + new_bytes - 1u, memory_order_release);
    test_pair_growth_denial_injected = true;
}

static int
fail_join_submit_at(wl_work_queue_t *wq, void (*work_fn)(void *), void *ctx)
{
    test_join_submit_calls++;
    if (test_join_submit_calls == test_join_submit_fail_at)
        return ENOMEM;
    return wl_workqueue_submit(wq, work_fn, ctx);
}

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
    bool admission_committed = col_rel_retained_live_bytes(source,
            &copy_bytes)
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
 * A bare managed session.  The ledger is observational; result allocation
 * admission is enforced independently by the governor.
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
registry_charge(const wl_col_session_t *s)
{
    /* Session registration retains the pointer array and, when admission
     * permits it, the optional relation-name hash. */
    return s->rels_reservation.bytes + s->rel_hash_reservation.bytes;
}

static uint64_t
reserved_for(wl_columnar_memory_governor_ref_t *ref)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(ref));
}

static uint64_t
relation_charge(const col_rel_t *relation)
{
    return relation->retained_reserved_bytes
           + relation->descriptor_reserved_bytes
           + relation->metadata_reserved_bytes
           + relation->pool_name_reserved_bytes;
}

static bool
concat_entry_unchanged(const eval_entry_t *entry, const eval_entry_t *before)
{
    return entry->rel == before->rel && entry->owned == before->owned
           && entry->is_delta == before->is_delta
           && entry->seg_boundaries == before->seg_boundaries
           && entry->seg_count == before->seg_count
           && entry->kind == before->kind
           && entry->continuation == before->continuation;
}

static unsigned map_admission_callback_calls;

static int
map_admission_callback(const wirelog_extension_value_t *args, uint32_t nargs,
    wirelog_extension_value_t *result, void *user_data)
{
    (void)args;
    (void)nargs;
    (void)user_data;
    map_admission_callback_calls++;
    result->type = WIRELOG_EXTENSION_VALUE_INT64;
    result->size = sizeof(int64_t);
    result->as.int64_value = 42;
    return 0;
}

static void
test_map_admission(unsigned governor_mode, bool owned, uint32_t nrows,
    bool nullary, bool callback)
{
    TEST("MAP preadmits full output before callbacks and retries exact input");
    wl_col_session_t *sess = make_session(UINT64_C(1) << 24);
    wl_columnar_memory_governor_ref_t *saved_ref = NULL, *source_ref = NULL;
    wl_columnar_memory_governor_ref_t *expected = NULL;
    col_rel_t *input = col_rel_new_auto("map_input", 1), *probe = NULL;
    col_rel_t *lower = col_rel_new_auto("lower", 1);
    wirelog_extension_registry_t *registry = NULL;
    wirelog_extension_snapshot_t *snapshot = NULL;
    eval_stack_t stack;
    eval_stack_init(&stack);
    const char *failure = NULL;
    bool input_stacked_owned = false;
#define MAP_CHECK(c, message) do { if (!(c)) { failure = message; \
                                               goto cleanup; } } while (0)
    MAP_CHECK(sess && input && lower, "fixture allocation");
    saved_ref = sess->memory_governor;
    if (governor_mode)
        source_ref = make_governor(UINT64_C(1) << 24);
    MAP_CHECK(!governor_mode || source_ref, "source governor");
    expected = governor_mode == 1 ? source_ref : saved_ref;
    wl_columnar_memory_governor_ref_retain(expected);
    MAP_CHECK(col_rel_attach_memory_governor(input,
        source_ref ? source_ref : saved_ref) == 0, "input admission");
    for (uint32_t r = 0; r < nrows; r++) {
        int64_t value = (int64_t)r + 7;
        MAP_CHECK(col_rel_append_row(input, &value) == 0, "input rows");
    }
    MAP_CHECK(col_rel_enable_timestamps(input) == 0, "input timestamps");
    if (governor_mode == 1)
        sess->memory_governor = NULL;
    delta_pool_destroy(sess->delta_pool);
    sess->delta_pool = delta_pool_create_managed(8, sizeof(col_rel_t), 4096,
            wl_columnar_memory_governor_ref_get(expected));
    sess->eval_arena = wl_arena_create_managed(4096,
            wl_columnar_memory_governor_ref_get(expected));
    MAP_CHECK(sess->delta_pool && sess->eval_arena, "admitted pool and arena");
    if (governor_mode == 2)
        atomic_store_explicit(&wl_columnar_memory_governor_ref_get(source_ref)
            ->usable_bytes, reserved_for(source_ref), memory_order_release);
    uint32_t pc = nullary ? 0 : 1;
    uint8_t call[] = {WL_PLAN_EXPR_EXTENSION_CALL, 8, 0,
                      't', 'e', 's', 't', '.', 'm', 'a', 'p', 0, 0, 0, 0};
    wl_plan_expr_buffer_t expression = {call, sizeof(call)};
    wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = pc};
    if (callback) {
        wirelog_extension_descriptor_t descriptor = {
            .abi_version = WIRELOG_EXTENSION_ABI_VERSION,
            .size = sizeof(descriptor), .name = "test.map",
            .result_type = WIRELOG_EXTENSION_VALUE_INT64,
            .invoke = map_admission_callback
        };
        registry = wirelog_extension_registry_create();
        MAP_CHECK(registry && wirelog_extension_register(registry,
            &descriptor) == 0, "callback registration");
        snapshot = wirelog_extension_snapshot_acquire(registry);
        MAP_CHECK(snapshot, "callback snapshot");
        sess->base.extension_snapshot = snapshot;
        op.map_exprs = &expression;
        op.map_expr_count = 1;
    }
    MAP_CHECK(eval_stack_push(&stack, lower, false) == 0
        && eval_stack_push_delta(&stack, input, owned, true) == 0,
        "input stack");
    input_stacked_owned = owned;
    stack.items[1].seg_boundaries = malloc(2 * sizeof(uint32_t));
    MAP_CHECK(stack.items[1].seg_boundaries, "input segment metadata");
    stack.items[1].seg_count = 1;
    stack.items[1].seg_boundaries[0] = 0;
    stack.items[1].seg_boundaries[1] = nrows;
    eval_entry_t before = stack.items[1];
    uint64_t view = input->view_generation, storage = input->storage_generation;
    col_delta_timestamp_t *timestamps = input->timestamps;
    uint64_t baseline = reserved_for(expected);
    uint64_t input_charge = input->memory_governor == expected
        ? relation_charge(input) : 0;
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(expected);
    /* Measure the checked constructor independently of MAP, including its
     * transient metadata peak; MAP scratch overlaps that whole operation. */
    uint64_t low = baseline, high = baseline + (UINT64_C(1) << 20);
    while (low < high) {
        uint64_t mid = low + (high - low) / 2;
        atomic_store_explicit(&governor->usable_bytes, mid,
            memory_order_release);
        int rc = wl_columnar_relation_new_auto_governed("$map", pc, nrows,
                false, expected, &probe);
        MAP_CHECK(rc == 0 || rc == ENOSPC, "constructor peak calibration");
        col_rel_destroy(probe);
        probe = NULL;
        MAP_CHECK(reserved_for(expected) == baseline, "probe charge released");
        if (rc == 0)
            high = mid;
        else
            low = mid + 1;
    }
    uint64_t scratch = sizeof(int64_t)
        + (callback ? pc * sizeof(wl_columnar_expr_compiled_t *) : 0);
    uint64_t limit = high + scratch;
    map_admission_callback_calls = 0;
    for (unsigned phase = 0; phase < 2; phase++) {
        atomic_store_explicit(&governor->usable_bytes,
            phase == 0 ? baseline : limit - 1, memory_order_release);
        sess->memory_budget_denied = false;
        MAP_CHECK(col_op_map(&op, &stack, sess) == ENOSPC
            && sess->memory_budget_denied, "typed scratch/output denial");
        MAP_CHECK(stack.top == 2 && stack.items[0].rel == lower
            && concat_entry_unchanged(&stack.items[1], &before)
            && before.seg_boundaries[0] == 0
            && before.seg_boundaries[1] == nrows,
            "denial retains complete input entry");
        MAP_CHECK(reserved_for(expected) == baseline
            && sess->delta_pool->slot_used == 0 && sess->eval_arena->used == 0,
            "denial preserves pool/arena credit and usage");
        MAP_CHECK(input->view_generation == view
            && input->storage_generation == storage
            && input->timestamps == timestamps && input->nrows == nrows
            && map_admission_callback_calls == 0,
            "denial leaves source and callback effects unchanged");
    }
    if (callback) {
        /* The output fits, but a large compiled expression does not. This
         * refusal is still before row evaluation and must retain the entry. */
        uint8_t arithmetic[647] = {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0'};
        for (unsigned i = 0; i < 64; i++) {
            arithmetic[7 + i * 10] = WL_PLAN_EXPR_CONST_INT;
            arithmetic[16 + i * 10] = WL_PLAN_EXPR_ARITH_ADD;
        }
        wl_plan_expr_buffer_t compiled = {arithmetic, sizeof(arithmetic)};
        op.map_exprs = &compiled;
        atomic_store_explicit(&governor->usable_bytes, limit,
            memory_order_release);
        sess->memory_budget_denied = false;
        MAP_CHECK(col_op_map(&op, &stack, sess) == ENOSPC
            && sess->memory_budget_denied && stack.top == 2
            && concat_entry_unchanged(&stack.items[1], &before)
            && reserved_for(expected) == baseline
            && map_admission_callback_calls == 0,
            "compiled-expression admission retains original input");
        op.map_exprs = &expression;
    }
    atomic_store_explicit(&governor->usable_bytes, limit, memory_order_release);
    sess->memory_budget_denied = false;
    int rc = col_op_map(&op, &stack, sess);
    if (owned && rc == 0) {
        input = NULL;
        input_stacked_owned = false;
    }
    MAP_CHECK(rc == 0 && stack.top == 2, "exact-budget retry");
    const col_rel_t *out = stack.items[1].rel;
    MAP_CHECK(out && out->memory_governor == expected && !out->pool_owned
        && !out->arena_owned && out->capacity == nrows && out->nrows == nrows
        && out->ncols == pc && !out->timestamps && stack.items[1].owned,
        "governed complete output shape");
    MAP_CHECK(reserved_for(expected) == baseline - (owned ? input_charge : 0)
        + relation_charge(out) && sess->delta_pool->slot_used == 0
        && sess->eval_arena->used == 0,
        "output exact charge, prepaid floors intact");
    for (uint32_t r = 0; r < nrows && pc > 0; r++)
        MAP_CHECK(col_rel_get(out, r, 0) == (callback ? 42 : (int64_t)r + 7),
            "all projected values preserved");
    MAP_CHECK(map_admission_callback_calls == (callback ? nrows : 0),
        "callbacks executed exactly once after admission");
    MAP_CHECK(eval_stack_drain(&stack) == 0
        && reserved_for(expected) == baseline - (owned ? input_charge : 0),
        "output drain releases only output charge");
cleanup:
    col_rel_destroy(probe);
    if (input_stacked_owned)
        input = NULL; /* stack owns it until checked drain */
    (void)eval_stack_drain(&stack);
    col_rel_destroy(input);
    col_rel_destroy(lower);
    if (sess) {
        sess->base.extension_snapshot = NULL;
        wl_arena_free(sess->eval_arena);
        sess->eval_arena = NULL;
        if (saved_ref)
            sess->memory_governor = saved_ref;
    }
    wirelog_extension_snapshot_release(snapshot);
    if (registry) {
        (void)wirelog_extension_unregister(registry, "test.map");
        if (wirelog_extension_registry_destroy(registry) != 0 && !failure)
            failure = "callback registry cleanup refused";
    }
    destroy_session(sess);
    if (source_ref) {
        if (reserved_for(source_ref) != 0 && !failure)
            failure = "source governor leaked credit";
        wl_columnar_memory_governor_ref_release(source_ref);
    }
    if (expected) {
        if (reserved_for(expected) != 0 && !failure)
            failure = "resolved governor leaked credit";
        wl_columnar_memory_governor_ref_release(expected);
    }
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef MAP_CHECK
}

static void
test_operator_unmanaged_storage(unsigned mode, bool reduce)
{
    TEST("unmanaged MAP/REDUCE retains heap/pool/arena allocation policy");
    wl_col_session_t sess = {0};
    col_rel_t *input = col_rel_new_auto("input", 1);
    eval_stack_t stack;
    eval_stack_init(&stack);
    if (mode)
        sess.delta_pool = delta_pool_create(8, sizeof(col_rel_t), 4096);
    if (mode == 2)
        sess.eval_arena = wl_arena_create(4096);
    const int64_t row = 7;
    wl_plan_op_t op = {.op = reduce ? WL_PLAN_OP_REDUCE : WL_PLAN_OP_MAP,
                       .project_count = 1, .agg_fn = WIRELOG_AGG_COUNT};
    bool ok = input && (!mode || sess.delta_pool)
        && (mode != 2 || sess.eval_arena)
        && col_rel_append_row(input, &row) == 0
        && eval_stack_push(&stack, input, false) == 0
        && (reduce ? col_op_reduce(&op, &stack, &sess)
                   : col_op_map(&op, &stack, &sess)) == 0 && stack.top == 1;
    if (ok) {
        const col_rel_t *out = stack.items[0].rel;
        ok = out && !out->memory_governor && out->pool_owned == (mode != 0)
            && out->arena_owned == (mode == 2) && out->nrows == 1
            && col_rel_get(out, 0, 0) == (reduce ? 1 : 7);
    }
    (void)eval_stack_drain(&stack);
    col_rel_destroy(input);
    delta_pool_destroy(sess.delta_pool);
    wl_arena_free(sess.eval_arena);
    if (!ok) {
        FAIL("legacy storage route"); return;
    }
    PASS();
}

static int
reduce_probe(wl_col_session_t *sess, col_rel_t *input, wl_plan_op_t *op,
    uint32_t *capacity)
{
    eval_stack_t stack;
    eval_stack_init(&stack);
    int rc = eval_stack_push(&stack, input, false);
    if (rc == 0)
        rc = col_op_reduce(op, &stack, sess);
    if (rc == 0 && capacity)
        *capacity = stack.items[stack.top - 1].rel->capacity;
    (void)eval_stack_drain(&stack);
    return rc;
}

static bool
reduce_measure_limit(wl_col_session_t *sess, col_rel_t *input,
    wl_plan_op_t *op, wl_columnar_memory_governor_ref_t *ref,
    uint64_t baseline, uint64_t *limit)
{
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(ref);
    uint64_t low = baseline, high = baseline + (UINT64_C(1) << 24);
    while (low < high) {
        uint64_t mid = low + (high - low) / 2;
        atomic_store_explicit(&governor->usable_bytes, mid,
            memory_order_release);
        int rc = reduce_probe(sess, input, op, NULL);
        if ((rc != 0 && rc != ENOSPC) || reserved_for(ref) != baseline)
            return false;
        if (rc == 0)
            high = mid;
        else
            low = mid + 1;
    }
    *limit = high;
    return true;
}

/* kind: COUNT, direct SUM, compiled SUM, interpreted SUM. */
static void
test_reduce_admission(unsigned governor_mode, bool owned, unsigned kind,
    uint32_t nrows)
{
    TEST(
        "REDUCE governs groups and preserves the defined denial retry boundary");
    wl_col_session_t *sess = make_session(UINT64_C(1) << 26);
    wl_columnar_memory_governor_ref_t *saved = NULL, *source_ref = NULL;
    wl_columnar_memory_governor_ref_t *expected = NULL;
    col_rel_t *input = col_rel_new_auto("reduce_input", 2);
    col_rel_t *lower = col_rel_new_auto("lower", 1);
    bool stacked_owned = false;
    eval_stack_t stack;
    eval_stack_init(&stack);
    const char *failure = NULL;
#define REDUCE_CHECK(c, message) do { if (!(c)) { failure = message; \
                                                  goto cleanup; } } while (0)
    REDUCE_CHECK(sess && input && lower, "fixture");
    saved = sess->memory_governor;
    if (governor_mode)
        source_ref = make_governor(UINT64_C(1) << 26);
    REDUCE_CHECK(!governor_mode || source_ref, "source governor");
    expected = governor_mode == 1 ? source_ref : saved;
    wl_columnar_memory_governor_ref_retain(expected);
    REDUCE_CHECK(col_rel_attach_memory_governor(input,
        source_ref ? source_ref : saved) == 0, "source admission");
    for (uint32_t r = 0; r < nrows; r++) {
        const int64_t row[] = {0, 3};
        REDUCE_CHECK(col_rel_append_row(input, row) == 0, "source rows");
    }
    REDUCE_CHECK(col_rel_enable_timestamps(input) == 0, "source timestamps");
    if (governor_mode == 1)
        sess->memory_governor = NULL;
    delta_pool_destroy(sess->delta_pool);
    sess->delta_pool = delta_pool_create_managed(8, sizeof(col_rel_t), 4096,
            wl_columnar_memory_governor_ref_get(expected));
    sess->eval_arena = wl_arena_create_managed(4096,
            wl_columnar_memory_governor_ref_get(expected));
    REDUCE_CHECK(sess->delta_pool && sess->eval_arena, "prepaid allocators");
    if (governor_mode == 2)
        atomic_store_explicit(&wl_columnar_memory_governor_ref_get(source_ref)
            ->usable_bytes, reserved_for(source_ref), memory_order_release);
    uint8_t compiled[] = {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1'};
    /* String length is an interpreter-only expression returning an integer.
     * This exercises the conservative interpreter policy without a registry. */
    uint8_t interpreted[] = {WL_PLAN_EXPR_CONST_STR, 3, 0, 'a', 'b', 'c',
                             WL_PLAN_EXPR_STR_FN_STRLEN};
    wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .group_by_count = 1,
                       .aggregate_index = 1,
                       .agg_fn = kind == 0 ? WIRELOG_AGG_COUNT
                                                  : WIRELOG_AGG_SUM};
    if (kind == 2)
        op.agg_expr = (wl_plan_expr_buffer_t){compiled, sizeof(compiled)};
    if (kind == 3) {
        sess->intern = wl_intern_create();
        REDUCE_CHECK(sess->intern, "interpreter intern table");
        op.agg_expr = (wl_plan_expr_buffer_t){interpreted, sizeof(interpreted)};
    }
    uint64_t baseline = reserved_for(expected);
    uint64_t few_limit = 0, full_limit = 0;
    REDUCE_CHECK(reduce_measure_limit(sess, input, &op, expected, baseline,
        &few_limit), "few-group threshold");
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(expected);
    atomic_store_explicit(&governor->usable_bytes, few_limit,
        memory_order_release);
    uint32_t capacity = UINT32_MAX;
    REDUCE_CHECK(reduce_probe(sess, input, &op, &capacity) == 0
        && capacity == (nrows < COL_REL_INIT_CAP ? nrows : COL_REL_INIT_CAP),
        "few groups do not preallocate worst-case input cardinality");
    for (uint32_t r = 0; r < nrows; r++)
        REDUCE_CHECK(col_rel_set(input, r, 0, r) == 0,
            "distinct group fixture");
    REDUCE_CHECK(reduce_measure_limit(sess, input, &op, expected, baseline,
        &full_limit), "distinct-group threshold");
    REDUCE_CHECK(nrows <= COL_REL_INIT_CAP || full_limit > few_limit,
        "growth requires additional overlap credit");
    atomic_store_explicit(&governor->usable_bytes,
        baseline + (UINT64_C(1) << 24),
        memory_order_release);
    sess->memory_budget_denied = false;
    wl_columnar_relation_test_fail_next_metadata_alloc();
    REDUCE_CHECK(reduce_probe(sess, input, &op, NULL) == ENOMEM
        && !sess->memory_budget_denied && reserved_for(expected) == baseline,
        "allocator failure remains ENOMEM");
    if (nrows > COL_REL_INIT_CAP) {
        wl_columnar_relation_test_fail_next_prepare_resize();
        REDUCE_CHECK(reduce_probe(sess, input, &op, NULL) == ENOMEM
            && !wl_columnar_relation_test_fail_prepare_resize
            && !sess->memory_budget_denied &&
            reserved_for(expected) == baseline,
            "late resize allocator failure is not budget denial");
    }
    wl_plan_op_t invalid = op;
    invalid.group_by_count = UINT32_MAX;
    REDUCE_CHECK(reduce_probe(sess, input, &invalid, NULL) == EOVERFLOW
        && !sess->memory_budget_denied && reserved_for(expected) == baseline,
        "output width overflow remains distinct");
    uint64_t view = input->view_generation, storage = input->storage_generation;
    col_delta_timestamp_t *timestamps = input->timestamps;
    uint64_t input_charge = input->memory_governor == expected
        ? relation_charge(input) : 0;
    REDUCE_CHECK(eval_stack_push(&stack, lower, false) == 0
        && eval_stack_push_delta(&stack, input, owned, true) == 0,
        "input stack");
    stacked_owned = owned;
    stack.items[1].seg_boundaries = malloc(2 * sizeof(uint32_t));
    REDUCE_CHECK(stack.items[1].seg_boundaries, "segments");
    stack.items[1].seg_count = 1;
    stack.items[1].seg_boundaries[0] = 0;
    stack.items[1].seg_boundaries[1] = nrows;
    eval_entry_t before = stack.items[1];
    for (unsigned phase = 0; phase < (kind == 3 ? 1u : 3u); phase++) {
        uint64_t limit = phase == 0 ? baseline : phase == 1 ? few_limit - 1
                                                                           :
            full_limit - 1;
        atomic_store_explicit(&governor->usable_bytes, limit,
            memory_order_release);
        sess->memory_budget_denied = false;
        REDUCE_CHECK(col_op_reduce(&op, &stack, sess) == ENOSPC
            && sess->memory_budget_denied, "typed admission denial");
        REDUCE_CHECK(stack.top == 2
            && concat_entry_unchanged(&stack.items[1], &before)
            && stack.items[0].rel == lower
            && before.seg_boundaries[0] == 0 &&
            before.seg_boundaries[1] == nrows,
            "denial retains exact input entry");
        REDUCE_CHECK(reserved_for(expected) == baseline
            && input->view_generation == view &&
            input->storage_generation == storage
            && input->timestamps == timestamps && input->nrows == nrows
            && sess->delta_pool->slot_used == 0 && sess->eval_arena->used == 0,
            "private work unwinds without source or allocator-floor mutation");
    }
    if (kind == 2) {
        uint8_t arithmetic[647] = {WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '1'};
        for (unsigned i = 0; i < 64; i++) {
            arithmetic[7 + i * 10] = WL_PLAN_EXPR_CONST_INT;
            arithmetic[16 + i * 10] = WL_PLAN_EXPR_ARITH_ADD;
        }
        op.agg_expr = (wl_plan_expr_buffer_t){arithmetic, sizeof(arithmetic)};
        atomic_store_explicit(&governor->usable_bytes, few_limit,
            memory_order_release);
        sess->memory_budget_denied = false;
        REDUCE_CHECK(col_op_reduce(&op, &stack, sess) == ENOSPC
            && sess->memory_budget_denied && stack.top == 2
            && concat_entry_unchanged(&stack.items[1], &before)
            && reserved_for(expected) == baseline,
            "compiled expression denial restores input");
        op.agg_expr = (wl_plan_expr_buffer_t){compiled, sizeof(compiled)};
    }
    if (kind == 3 && nrows > COL_REL_INIT_CAP) {
        REDUCE_CHECK(!owned, "interpreter fixture retains borrowed source");
        atomic_store_explicit(&governor->usable_bytes, full_limit - 1,
            memory_order_release);
        sess->memory_budget_denied = false;
        REDUCE_CHECK(col_op_reduce(&op, &stack, sess) == ENOSPC
            && sess->memory_budget_denied && stack.top == 1
            && reserved_for(expected) == baseline && input->nrows == nrows
            && input->view_generation == view,
            "interpreter late refusal consumes entry");
        REDUCE_CHECK(eval_stack_push(&stack, input, false) == 0,
            "explicit new interpreter invocation");
    }
    atomic_store_explicit(&governor->usable_bytes, full_limit,
        memory_order_release);
    sess->memory_budget_denied = false;
    int rc = col_op_reduce(&op, &stack, sess);
    if (owned && rc == 0) {
        input = NULL; stacked_owned = false;
    }
    REDUCE_CHECK(rc == 0 && stack.top == 2 && !sess->memory_budget_denied,
        "exact-limit successful reduction");
    const col_rel_t *out = stack.items[1].rel;
    REDUCE_CHECK(out && out->memory_governor == expected && !out->pool_owned
        && !out->arena_owned && out->nrows == nrows && out->ncols == 2
        && out->column_types && out->column_types[1] == WIRELOG_TYPE_INT64,
        "governed grouped output schema");
    for (uint32_t r = 0; r < nrows; r++)
        REDUCE_CHECK(col_rel_get(out, r, 0) == r
            && col_rel_get(out, r, 1) == (kind == 0 ? 1 : 3),
            "exact groups and aggregates");
    REDUCE_CHECK(reserved_for(expected) == baseline - (owned ? input_charge : 0)
        + relation_charge(out), "complete output accounting");
    REDUCE_CHECK(eval_stack_drain(&stack) == 0
        && reserved_for(expected) == baseline - (owned ? input_charge : 0),
        "output reservation release");
cleanup:
    wl_columnar_relation_test_clear_prepare_resize();
    if (stacked_owned)
        input = NULL;
    (void)eval_stack_drain(&stack);
    col_rel_destroy(input);
    col_rel_destroy(lower);
    if (sess) {
        wl_arena_free(sess->eval_arena);
        sess->eval_arena = NULL;
        wl_intern_free(sess->intern);
        sess->intern = NULL;
        if (saved)
            sess->memory_governor = saved;
    }
    destroy_session(sess);
    if (source_ref) {
        if (reserved_for(source_ref) != 0 && !failure)
            failure = "source credit leaked";
        wl_columnar_memory_governor_ref_release(source_ref);
    }
    if (expected) {
        if (reserved_for(expected) != 0 && !failure)
            failure = "resolved credit leaked";
        wl_columnar_memory_governor_ref_release(expected);
    }
    if (failure) {
        FAIL(failure); return;
    }
    PASS();
#undef REDUCE_CHECK
}

static void
test_variable_empty_admission(bool pooled, unsigned governor_mode,
    unsigned condition)
{
    char label[112];
    snprintf(label, sizeof(label),
        "VARIABLE empty admission pool=%d governor=%u condition=%u",
        pooled, governor_mode, condition);
    TEST(label);
    wl_col_session_t *sess = make_session(UINT64_C(1) << 24);
    wl_columnar_memory_governor_ref_t *source_ref = NULL, *saved_ref = NULL;
    wl_columnar_memory_governor_ref_t *expected = NULL;
    col_rel_t *source = col_rel_new_auto("input", 2), *probe = NULL;
    eval_stack_t stack;
    eval_entry_t lower = {0};
    const char *failure = NULL;
    eval_stack_init(&stack);
#define EMPTY_CHECK(c, message) do { if (!(c)) { failure = message; \
                                                 goto cleanup; } } while (0)
    EMPTY_CHECK(sess && source, "fixture allocation");
    saved_ref = sess->memory_governor;
    if (governor_mode) {
        source_ref = make_governor(UINT64_C(1) << 24);
        EMPTY_CHECK(source_ref, "source governor");
    }
    expected = governor_mode == 1 ? source_ref : saved_ref;
    wl_columnar_memory_governor_ref_retain(expected);
    EMPTY_CHECK(col_rel_attach_memory_governor(source,
        source_ref ? source_ref : saved_ref) == 0,
        "source admission");
    const wirelog_column_type_t types[2] = {WIRELOG_TYPE_INT64,
                                            WIRELOG_TYPE_INT64};
    const col_rel_logical_col_t logical[2] = {
        {WIRELOG_COMPOUND_KIND_SIDE, 2, 1}, {WIRELOG_COMPOUND_KIND_NONE, 0, 0}
    };
    const int64_t row[2] = {13, 29};
    EMPTY_CHECK(col_rel_set_column_types(source, types, 2) == 0
        && col_rel_apply_compound_schema(source, logical, 2) == 0
        && col_rel_append_row(source, row) == 0
        && col_rel_enable_timestamps(source) == 0, "source schema and row");
    source->has_graph_column = true;
    source->graph_col_idx = 1;
    source->declared_ncols = 1;
    uint64_t source_view = source->view_generation;
    uint64_t source_storage = source->storage_generation;
    col_delta_timestamp_t *source_timestamps = source->timestamps;
    if (governor_mode == 1)
        sess->memory_governor = NULL;
    /* A unit fixture registry isolates output admission from registry growth. */
    sess->rels = calloc(2, sizeof(*sess->rels));
    EMPTY_CHECK(sess->rels, "fixture registry");
    sess->rels[0] = source;
    sess->nrels = 1;
    source = NULL; /* session now owns it */
    col_rel_t *full = sess->rels[0];
    if (condition == 5) {
        sess->rels[1] = col_rel_new_auto("$d$input", 2);
        EMPTY_CHECK(sess->rels[1], "empty registered delta");
        sess->nrels = 2;
    }
    delta_pool_destroy(sess->delta_pool);
    sess->delta_pool = pooled ? delta_pool_create_managed(128,
            sizeof(col_rel_t), 4096,
            wl_columnar_memory_governor_ref_get(expected)) : NULL;
    EMPTY_CHECK(!pooled || sess->delta_pool, "pre-admitted pool");
    if (governor_mode == 2) {
        /* Exhaust only the source governor: session precedence must win. */
        atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
                source_ref)->usable_bytes,
            reserved_for(source_ref), memory_order_release);
    }
    EMPTY_CHECK(eval_stack_push(&stack, full, false) == 0, "lower stack input");
    stack.items[0].seg_boundaries = malloc(2 * sizeof(uint32_t));
    EMPTY_CHECK(stack.items[0].seg_boundaries, "lower segments");
    stack.items[0].seg_boundaries[0] = 0;
    stack.items[0].seg_boundaries[1] = 1;
    stack.items[0].seg_count = 1;
    lower = stack.items[0];
    /* Lookup may build the session's governed name index. Include that
     * independent registry charge before measuring operator-only admission. */
    EMPTY_CHECK(session_find_rel(sess, "input") == full,
        "warm registry lookup");
    const uint64_t baseline = reserved_for(expected);
    const uint64_t other_baseline = source_ref ? reserved_for(source_ref) : 0;
    wl_columnar_memory_governor_t *governor =
        wl_columnar_memory_governor_ref_get(expected);
    const char *name = condition < 2 ? "$empty_skip" : "$empty_delta";
    wl_plan_op_t op = {.op = WL_PLAN_OP_VARIABLE, .relation_name = "input",
                       .delta_mode = condition == 0 ? WL_DELTA_FORCE_EMPTY
            : condition ==
                           1 ? WL_DELTA_FORCE_EMPTY_AFTER_SEED :
                           WL_DELTA_FORCE_DELTA};
    sess->current_iteration = condition == 1 || condition >= 4 ? 1 : 0;
    sess->tdd_outbound_only_active = condition == 1;
    sess->delta_seeded = condition == 2;
    sess->retraction_seeded = condition == 3;

    /* Locate the exact constructor peak, including metadata replacement
     * overlap. Every successful probe releases its live charge; pool slots
     * are deliberately plentiful because they are reclaimed at pool reset. */
    uint64_t low = baseline, high = baseline + (UINT64_C(1) << 20);
    while (low + 1 < high) {
        uint64_t mid = low + (high - low) / 2;
        atomic_store_explicit(&governor->usable_bytes, mid,
            memory_order_release);
        int rc = wl_columnar_relation_pool_new_like_governed_checked(&probe,
                sess->delta_pool, name, full, expected);
        EMPTY_CHECK(rc == 0 || rc == ENOSPC, "constructor peak calibration");
        if (rc == 0) high = mid;
        else low = mid;
        col_rel_destroy(probe);
        probe = NULL;
        EMPTY_CHECK(reserved_for(expected) == baseline,
            "calibration charge release");
    }
    atomic_store_explicit(&governor->usable_bytes, high, memory_order_release);
    EMPTY_CHECK(wl_columnar_relation_pool_new_like_governed_checked(&probe,
        sess->delta_pool, name, full, expected) == 0,
        "measured constructor limit fits");
    col_rel_destroy(probe);
    probe = NULL;

    for (unsigned phase = 0; phase < 3; phase++) {
        uint32_t slots = pooled ? sess->delta_pool->slot_used : 0;
        atomic_store_explicit(&governor->usable_bytes, phase == 0 ? high - 1
            : baseline + (UINT64_C(1) << 20), memory_order_release);
        sess->memory_budget_denied = false;
        if (phase == 1)
            wl_columnar_relation_test_fail_next_metadata_alloc();
        if (phase == 2)
            full->graph_col_idx = full->ncols;
        int rc = col_op_variable(&op, &stack, sess);
        full->graph_col_idx = 1;
        EMPTY_CHECK(rc == (phase == 0 ? ENOSPC : phase == 1 ? ENOMEM : EINVAL),
            "empty constructor typed failure");
        EMPTY_CHECK(sess->memory_budget_denied == (phase == 0),
            "denial provenance");
        EMPTY_CHECK(stack.top == 1 && concat_entry_unchanged(&stack.items[0],
            &lower)
            && lower.seg_boundaries[0] == 0 && lower.seg_boundaries[1] == 1,
            "refusal preserves complete lower entry");
        EMPTY_CHECK(reserved_for(expected) == baseline
            && (!pooled || sess->delta_pool->slot_used == slots),
            "failure reservation/slot rollback");
    }
    atomic_store_explicit(&governor->usable_bytes, high, memory_order_release);
    sess->memory_budget_denied = false;
    EMPTY_CHECK(col_op_variable(&op, &stack, sess) == 0 && stack.top == 2,
        "retry at exact constructor limit");
    const eval_entry_t *result = &stack.items[1];
    const col_rel_t *out = result->rel;
    EMPTY_CHECK(out && out->memory_governor == expected &&
        out->pool_owned == pooled
        && strcmp(out->name, name) == 0 && out->nrows == 0
        && result->owned && result->is_delta == (condition >= 2)
        && !sess->memory_budget_denied, "empty result ownership/governor/tag");
    EMPTY_CHECK(out->ncols == full->ncols &&
        out->declared_ncols == full->declared_ncols
        && out->has_graph_column && out->graph_col_idx == 1
        && out->compound_arity_len == full->compound_arity_len
        && out->compound_arity_map != full->compound_arity_map
        && out->compound_kind == full->compound_kind && out->timestamps == NULL,
        "empty result metadata and legacy timestamp policy");
    for (uint32_t c = 0; c < full->ncols; c++) {
        EMPTY_CHECK(strcmp(out->col_names[c], full->col_names[c]) == 0
            && out->column_types[c] == full->column_types[c]
            && out->compound_arity_map[c] == full->compound_arity_map[c],
            "private schema metadata");
    }
    EMPTY_CHECK(reserved_for(expected) == baseline + relation_charge(out),
        "output charged once");
    eval_entry_t entry = eval_stack_pop(&stack);
    EMPTY_CHECK(eval_entry_dispose(&entry) == 0 &&
        reserved_for(expected) == baseline,
        "output disposal restores baseline");
    /* Success after ENOMEM also proves the one-shot fault was consumed. */
    while (stack.top < COL_STACK_MAX)
        EMPTY_CHECK(eval_stack_push(&stack, full, false) == 0, "fill stack");
    EMPTY_CHECK(col_op_variable(&op, &stack, sess) == ENOBUFS
        && stack.top == COL_STACK_MAX && !sess->memory_budget_denied
        && reserved_for(expected) == baseline,
        "push failure discards private result");
    for (uint32_t i = 1; i < stack.top; i++)
        EMPTY_CHECK(stack.items[i].rel == full && !stack.items[i].owned,
            "full stack unchanged");
    stack.top = 1; /* discard borrowed, metadata-free fixture entries */

    /* Unseeded iteration zero and FORCE_FULL remain borrowed reads even at
     * an exhausted budget. No empty output may be allocated here. */
    sess->current_iteration = 0;
    sess->delta_seeded = sess->retraction_seeded = false;
    atomic_store_explicit(&governor->usable_bytes, baseline,
        memory_order_release);
    op.delta_mode = WL_DELTA_FORCE_DELTA;
    for (unsigned control = 0; control < 2; control++) {
        if (control) op.delta_mode = WL_DELTA_FORCE_FULL;
        EMPTY_CHECK(col_op_variable(&op, &stack, sess) == 0 && stack.top == 2
            && stack.items[1].rel == full && !stack.items[1].owned
            && !stack.items[1].is_delta && reserved_for(expected) == baseline,
            "borrowed full-relation control");
        entry = eval_stack_pop(&stack);
        EMPTY_CHECK(eval_entry_dispose(&entry) == 0,
            "borrowed control release");
    }
    EMPTY_CHECK(full->nrows == 1 && full->columns[0][0] == row[0]
        && full->columns[1][0] == row[1] &&
        full->timestamps == source_timestamps
        && full->view_generation == source_view &&
        full->storage_generation == source_storage,
        "source contents and generations unchanged");
    EMPTY_CHECK(governor_mode != 2 ||
        reserved_for(source_ref) == other_baseline,
        "session precedence leaves source charge alone");
cleanup:
    col_rel_destroy(probe);
    (void)eval_stack_drain(&stack);
    col_rel_destroy(source);
    if (sess && saved_ref)
        sess->memory_governor = saved_ref;
    destroy_session(sess);
    if (expected && reserved_for(expected) != 0 && !failure)
        failure = "teardown leaked governor bytes";
    wl_columnar_memory_governor_ref_release(expected);
    wl_columnar_memory_governor_ref_release(source_ref);
    if (failure) FAIL(failure);
    else PASS();
#undef EMPTY_CHECK
}

static void
test_concat_admission_rollback(bool pooled, bool second_append)
{
    TEST(pooled ? "pooled CONCAT admission preserves staged inputs"
         : "heap CONCAT admission preserves staged inputs");
    wl_col_session_t *sess = make_session(UINT64_C(1) << 24);
    col_rel_t *a = col_rel_new_auto("a", 1);
    col_rel_t *b = col_rel_new_auto("b", 1);
    col_rel_t *lower = col_rel_new_auto("lower", 1);
    col_rel_t *probe = NULL;
    eval_stack_t stack;
    eval_entry_t original_a, original_b, original_lower;
    const char *failure = NULL;
    uint32_t a_rows = COL_REL_INIT_CAP + (second_append ? 0u : 1u);
    uint64_t baseline = 0, clone_limit = 0, a_charge = 0;
    wl_columnar_memory_governor_t *governor = NULL;
    eval_stack_init(&stack);
#define CONCAT_CHECK(condition, message) \
        do { \
            if (!(condition)) { \
                failure = message; \
                goto cleanup; \
            } \
        } while (0)
    CONCAT_CHECK(sess && a && b && lower, "fixture");
    if (!pooled) {
        delta_pool_destroy(sess->delta_pool);
        sess->delta_pool = NULL;
    }
    governor = wl_columnar_memory_governor_ref_get(sess->memory_governor);
    CONCAT_CHECK(col_rel_attach_memory_governor(a, sess->memory_governor) == 0
        && col_rel_attach_memory_governor(b, sess->memory_governor) == 0,
        "source admission");
    for (uint32_t row = 0; row < a_rows; row++) {
        int64_t value = row;
        CONCAT_CHECK(col_rel_append_row(a, &value) == 0, "source rows");
    }
    int64_t last = a_rows, sentinel = 777;
    CONCAT_CHECK(col_rel_append_row(b, &last) == 0
        && col_rel_append_row(lower, &sentinel) == 0, "other rows");
    a_charge = relation_charge(a);
    CONCAT_CHECK(eval_stack_push(&stack, lower, false) == 0, "lower entry");
    original_lower = stack.items[0];
    CONCAT_CHECK(eval_stack_push(&stack, a, pooled) == 0, "left entry");
    if (pooled)
        a = NULL; /* stack owns the left input in this variant */
    stack.items[1].is_delta = true;
    stack.items[1].seg_boundaries = malloc(2 * sizeof(uint32_t));
    CONCAT_CHECK(stack.items[1].seg_boundaries != NULL, "left segments");
    stack.items[1].seg_boundaries[0] = 0;
    stack.items[1].seg_boundaries[1] = a_rows;
    stack.items[1].seg_count = 1;
    original_a = stack.items[1];
    CONCAT_CHECK(eval_stack_push(&stack, b, false) == 0, "right entry");
    stack.items[2].seg_boundaries = malloc(2 * sizeof(uint32_t));
    CONCAT_CHECK(stack.items[2].seg_boundaries != NULL, "right segments");
    stack.items[2].seg_boundaries[0] = 0;
    stack.items[2].seg_boundaries[1] = 1;
    stack.items[2].seg_count = 1;
    original_b = stack.items[2];
    baseline = reserved_of(sess);

    /* Measure the same pool/heap constructor route used by CONCAT. Probes
     * consume pool slots; ensure they cannot accidentally force fallback. */
    CONCAT_CHECK(!pooled || sess->delta_pool->slot_cap >= 16,
        "pool capacity");
    CONCAT_CHECK(wl_columnar_relation_pool_new_like_governed_checked(&probe,
        sess->delta_pool, "$concat", original_a.rel,
        sess->memory_governor) == 0, "measure constructor");
    clone_limit = reserved_of(sess);
    col_rel_destroy(probe);
    probe = NULL;
    CONCAT_CHECK(reserved_of(sess) == baseline, "probe release");

    for (unsigned phase = 0; phase < 3; phase++) {
        sess->memory_budget_denied = false;
        uint64_t limit = phase == 0 ? baseline
            : phase == 1 ? clone_limit : UINT64_C(1) << 24;
        atomic_store_explicit(&governor->usable_bytes, limit,
            memory_order_release);
        if (phase == 1) {
            CONCAT_CHECK(wl_columnar_relation_pool_new_like_governed_checked(
                    &probe, sess->delta_pool, "$concat", original_a.rel,
                    sess->memory_governor) == 0
                && probe->capacity == COL_REL_INIT_CAP,
                "constructor must fit constrained budget");
            int probe_rc = col_rel_append_all(probe, original_a.rel, NULL);
            if (second_append) {
                CONCAT_CHECK(probe_rc == 0, "first append must fit");
                probe_rc = col_rel_append_all(probe, b, NULL);
            }
            CONCAT_CHECK(probe_rc == ENOMEM
                && probe->memory_budget_denial_pending,
                "append must reach budget refusal");
            col_rel_destroy(probe);
            probe = NULL;
            CONCAT_CHECK(reserved_of(sess) == baseline, "growth probe release");
        }
        if (phase == 2)
            wl_columnar_merge_test_fail_concat_boundaries_alloc = true;
        int rc = col_op_concat(&stack, sess);
        CONCAT_CHECK(rc == (phase == 2 ? ENOMEM : ENOSPC)
            && sess->memory_budget_denied == (phase != 2),
            "CONCAT failure status");
        CONCAT_CHECK(!wl_columnar_merge_test_fail_concat_boundaries_alloc,
            "ordinary allocation failure hook not consumed");
        CONCAT_CHECK(stack.top == 3
            && concat_entry_unchanged(&stack.items[0], &original_lower)
            && concat_entry_unchanged(&stack.items[1], &original_a)
            && concat_entry_unchanged(&stack.items[2], &original_b)
            && reserved_of(sess) == baseline,
            "CONCAT failure lost inputs or reservations");
        CONCAT_CHECK(original_a.rel->nrows == a_rows && b->nrows == 1
            && b->columns[0][0] == last && lower->columns[0][0] == sentinel
            && original_a.seg_boundaries[0] == 0
            && original_a.seg_boundaries[1] == a_rows
            && original_b.seg_boundaries[0] == 0
            && original_b.seg_boundaries[1] == 1, "source metadata changed");
        for (uint32_t row = 0; row < a_rows; row++)
            CONCAT_CHECK(original_a.rel->columns[0][row] == row,
                "source contents changed");
    }
    CONCAT_CHECK(col_op_concat(&stack, sess) == 0 && stack.top == 2,
        "retry same inputs");
    col_rel_t *out = stack.items[1].rel;
    CONCAT_CHECK(out->memory_governor == sess->memory_governor
        && out->nrows == a_rows + 1 && stack.items[1].owned
        && stack.items[1].seg_count == 2
        && stack.items[1].seg_boundaries[0] == 0
        && stack.items[1].seg_boundaries[1] == a_rows
        && stack.items[1].seg_boundaries[2] == a_rows + 1
        && !sess->memory_budget_denied
        && reserved_of(sess) == baseline - (pooled ? a_charge : 0)
        + relation_charge(out), "retry output accounting/segments");
    for (uint32_t row = 0; row <= a_rows; row++)
        CONCAT_CHECK(out->columns[0][row] == row, "retry output rows");
cleanup:
    wl_columnar_merge_test_fail_concat_boundaries_alloc = false;
    col_rel_destroy(probe);
    (void)eval_stack_drain(&stack);
    col_rel_destroy(a);
    col_rel_destroy(b);
    col_rel_destroy(lower);
    if (sess && reserved_of(sess) != 0 && !failure)
        failure = "reservation leak";
    destroy_session(sess);
    if (failure) {
        FAIL(failure);
        return;
    }
    PASS();
#undef CONCAT_CHECK
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
    if (!col_rel_retained_live_bytes(r, &want))
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

static col_rel_t *
make_split_left(uint32_t n, uint32_t first_key_rows)
{
    const char *cn[] = { "k", "v" };
    col_rel_t *left = make_rel("left", 2, cn);
    if (!left)
        return NULL;
    for (uint32_t i = 0; i < n; i++) {
        int64_t row[] = { i < first_key_rows ? 0 : 1, (int64_t)i };
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

static int
run_filter_join(int (*op_fn)(const wl_plan_op_t *, eval_stack_t *,
    wl_col_session_t *), wl_col_session_t *sess, col_rel_t *left,
    const wl_plan_op_t *op, eval_entry_t *out)
{
    eval_stack_t stack;
    eval_stack_init(&stack);
    eval_stack_push(&stack, left, false);
    int rc = op_fn(op, &stack, sess);
    if (rc != 0) {
        while (stack.top > 0) {
            eval_entry_t entry = eval_stack_pop(&stack);
            if (entry.owned)
                col_rel_destroy(entry.rel);
        }
        return rc;
    }
    *out = eval_stack_pop(&stack);
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
    if (session_add_rel(sess, right) != 0)
        goto out;
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
    uint64_t scratch_bytes = 0;
    uint64_t row_bytes = 0;
    uint64_t hash_bytes = 0;
    uint32_t buckets = 0;

    if (!sess || !right || !left)
        goto out;
    if (session_add_rel(sess, right) != 0)
        goto out;
    right = NULL;
    uint64_t source_baseline = reserved_of(sess);
    init_cross_op(&op);
    if (run_join(sess, left, &op, &result) != 0)
        goto out;
    {
        const col_rel_t *governed = cached_output(sess, left);
        if (!governed || governed->nrows > COL_REL_INIT_CAP
            || governed->retained_reserved_bytes == 0u)
            goto out_entry;
        /* Include the standard cross-join fallback row in the exact-fit
         * boundary; it remains live throughout row generation. */
        buckets = wl_columnar_filter_next_pow2(4u);
        if (buckets == 0
            || !wl_columnar_memory_size_mul(governed->ncols,
            sizeof(int64_t), &row_bytes)
            || !wl_columnar_memory_size_mul((uint64_t)buckets + 2u,
            sizeof(uint32_t), &hash_bytes)
            || !wl_columnar_memory_size_add(row_bytes, hash_bytes,
            &scratch_bytes)
            || !wl_columnar_memory_size_add(
                relation_charge(governed), scratch_bytes, out_bytes))
            goto out_entry;
    }
    /* Type replacement holds the old Arrow metadata until commit. */
    uint64_t metadata_peak = relation_charge(cached_output(sess, left))
        - cached_output(sess, left)->descriptor_reserved_bytes
        + sizeof(col_rel_t) + sizeof("$join")
        + cached_output(sess, left)->metadata_reserved_bytes
        - (uint64_t)cached_output(sess, left)->ncols
        * sizeof(wirelog_column_type_t);
    if (metadata_peak > *out_bytes)
        *out_bytes = metadata_peak;
    *out_bytes += source_baseline;
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
test_missing_right_metadata_boundary(void)
{
    TEST("missing-right JOIN preserves metadata denial and exact overlap");
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *left = make_left(1, 2);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    col_rel_t *shape = col_rel_new_auto("$join_empty", 2);
    uint64_t payload = 0;
    uint64_t metadata = 3u + 2u * (sizeof(char *)
        + sizeof(struct ArrowSchema *) + sizeof(struct ArrowSchema) + 2u)
        + 4u * sizeof("col0");
    if (!sess || !left || !shape
        || !col_rel_retained_bytes_for(shape, COL_REL_INIT_CAP, &payload)) {
        FAIL("missing-right boundary fixture");
        goto out;
    }
    payload += (uint64_t)shape->ncols
        * (sizeof(int64_t *) + sizeof(int64_t));
    init_cross_op(&op);
    op.right_relation = "missing";
    uint64_t peak = sizeof(col_rel_t) + sizeof("$join_empty")
        + payload + 2u * metadata + 2u * sizeof(wirelog_column_type_t);
    wl_columnar_memory_governor_t *governor
        = wl_columnar_memory_governor_ref_get(sess->memory_governor);
    atomic_store_explicit(&governor->usable_bytes, peak - 1u,
        memory_order_release);
    if (run_join(sess, left, &op, &result) != ENOSPC
        || result.rel || reserved_of(sess) != 0) {
        FAIL("missing-right metadata denial was not typed or balanced");
        goto out;
    }
    atomic_store_explicit(&governor->usable_bytes, peak, memory_order_release);
    wl_columnar_relation_test_fail_next_metadata_alloc();
    if (run_join(sess, left, &op, &result) != ENOMEM
        || result.rel || reserved_of(sess) != 0) {
        FAIL("missing-right metadata allocator failure lost its type");
        goto out;
    }
    if (run_join(sess, left, &op, &result) != 0 || !result.rel
        || result.rel->nrows != 0 || reserved_of(sess) != peak - metadata) {
        FAIL("missing-right exact metadata overlap did not settle");
        goto out;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    if (reserved_of(sess) != 0) {
        FAIL("missing-right result destruction leaked credit");
        goto out;
    }
    PASS();
out:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
    col_rel_destroy(shape);
    col_rel_destroy(left);
    destroy_session(sess);
}

static void
run_at_budget(const char *name, uint64_t budget, bool expect_ok)
{
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
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
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
    right = NULL;
    uint64_t source_baseline = reserved_of(sess);
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            sess->memory_governor)->usable_bytes, budget,
        memory_order_release);
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
        const col_rel_t *retained_output = governed ? governed : result.rel;
        if (!retained_output || !admission_invariant(retained_output)
            || reserved_of(sess) >= budget) {
            FAIL("scratch credit was retained after the join completed");
            goto out_entry;
        }
        PASS();
        goto out_entry;
    }
    if (rc == 0) {
        FAIL("join succeeded one byte below the output footprint");
        goto out_entry;
    }
    if (rc != ENOSPC) {
        FAIL("denial did not surface as ENOSPC");
        goto out;
    }
    /* No arrangement is built on this path, so a denied cross join must
     * leave the governor exactly where it started. */
    if (reserved_of(sess) != source_baseline) {
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

static void
test_worker_join_above_ledger_threshold_keeps_all_rows(void)
{
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(1, 1);
    col_rel_t *left = make_left(10002u, 1u);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };

    TEST("worker JOIN above ledger threshold keeps all governed rows");
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
    right = NULL;
    /* The legacy in-loop signal fired at exactly 10,000 rows on worker
     * sessions and converted the otherwise complete output into EOVERFLOW. */
    sess->coordinator = sess;
    wl_mem_ledger_init(&sess->mem_ledger, 1000u);
    wl_mem_ledger_set_gauge(&sess->mem_ledger, WL_MEM_SUBSYS_RELATION, 400u);
    init_join_op(&op, (const char *const[]){ "k" },
        (const char *const[]){ "k" });
    op.materialized = false;
    if (run_join(sess, left, &op, &result) != 0 || !result.rel
        || result.rel->nrows != 10002u
        || col_rel_get(result.rel, 0, 1) != 0
        || col_rel_get(result.rel, 10001u, 1) != 10001) {
        FAIL("ledger pressure truncated or failed the exact JOIN result");
        goto out;
    }
    for (uint32_t row = 0; row < result.rel->nrows; row++) {
        if (col_rel_get(result.rel, row, 0) != 0
            || col_rel_get(result.rel, row, 1) != row
            || col_rel_get(result.rel, row, 2) != 0
            || col_rel_get(result.rel, row, 3) != 0) {
            FAIL("ledger pressure changed a JOIN result row");
            goto out;
        }
    }
    PASS();
out:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
    col_rel_destroy(left);
    col_rel_destroy(right);
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
    if (session_add_rel(sess, right) != 0)
        goto out;
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
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
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

/* ---- case 6: projected materialized outputs are governed ---------------- */

/* A projected materialized result is still an output payload and must carry
 * the effective governor even though it bypasses the materialization cache. */
static void
test_projected_output_is_governed(void)
{
    const char *lk[] = { "k" };
    const char *rk[] = { "k" };
    const uint32_t proj[] = { 0u };
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(4, 2);
    col_rel_t *left = make_left(8, 4);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };

    TEST("materialized+projected output follows output admission");
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
    right = NULL;
    init_join_op(&op, lk, rk);
    op.project_indices = proj;
    op.project_count = 1u;
    if (run_join(sess, left, &op, &result) != 0) {
        FAIL("projected join failed");
        goto out;
    }
    if (!result.rel || result.rel->pool_owned
        || result.rel->memory_governor != sess->memory_governor) {
        FAIL("projected materialized output is not governor-owned");
        goto out_entry;
    }
    if (!admission_invariant(result.rel)) {
        FAIL("projected output token does not cover its capacity");
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

static void
test_join_output_growth_denial_is_typed(void)
{
    wl_col_session_t *measure = make_session(64ull * 1024 * 1024);
    col_rel_t *probe = make_rel("$join", 4, NULL);
    uint64_t initial_bytes = 0;
    uint64_t grown_bytes = 0;
    wl_col_session_t *sess = NULL;
    col_rel_t *right = make_right(13, 1);
    col_rel_t *left = make_left(7, 2);
    wl_plan_op_t op;

    TEST("JOIN output growth denial is ENOSPC and releases its output");
    if (!measure || !probe || !right || !left
        || !col_rel_retained_bytes_for(probe, COL_REL_INIT_CAP,
        &initial_bytes)
        || !col_rel_retained_bytes_for(probe, COL_REL_INIT_CAP * 2u,
        &grown_bytes)) {
        FAIL("could not derive output growth footprints");
        goto out;
    }
    destroy_session(measure);
    measure = NULL;
    col_rel_destroy(probe);
    probe = NULL;
    sess = make_session(64ull * 1024 * 1024);
    if (!sess) {
        FAIL("could not create governor");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
    right = NULL;
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            sess->memory_governor)->usable_bytes,
        registry_charge(sess) + initial_bytes + grown_bytes - 1u,
        memory_order_release);
    init_cross_op(&op);
    op.materialized = false;
    if (run_join(sess, left, &op, NULL) != ENOSPC
        || !sess->memory_budget_denied
        || reserved_of(sess) != registry_charge(sess)) {
        FAIL("growth denial was not typed or left output bytes reserved");
        goto out;
    }
    PASS();
out:
    if (measure)
        destroy_session(measure);
    col_rel_destroy(probe);
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

static void
test_source_governed_join_key_scratch_denial(void)
{
    static const char *const left_keys[] = { "k" };
    static const char *const right_keys[] = { "k" };
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(2, 1);
    col_rel_t *left = make_left(4, 2);
    wl_columnar_memory_governor_ref_t *source_ref = NULL;
    wl_columnar_memory_governor_ref_t *session_ref = NULL;
    wl_plan_op_t op;

    TEST("source-only JOIN key scratch denial is typed and balanced");
    if (!sess || !right || !left
        || !(source_ref = make_governor(64ull * 1024 * 1024))
        || col_rel_attach_memory_governor(left, source_ref) != 0) {
        FAIL("fixture");
        goto out;
    }
    uint64_t source_baseline = reserved_for(source_ref);
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            source_ref)->usable_bytes,
        source_baseline + 2u * sizeof(uint32_t) - 1u,
        memory_order_release);
    session_ref = sess->memory_governor;
    sess->memory_governor = NULL;
    if (session_add_rel(sess, right) != 0) {
        sess->memory_governor = session_ref;
        session_ref = NULL;
        FAIL("right relation setup");
        goto out;
    }
    right = NULL;
    init_join_op(&op, left_keys, right_keys);
    if (run_join(sess, left, &op, NULL) != ENOSPC
        || !sess->memory_budget_denied
        || reserved_for(source_ref) != source_baseline) {
        FAIL("key scratch denial was not typed or leaked its reservation");
    } else {
        PASS();
    }
out:
    if (sess && session_ref)
        sess->memory_governor = session_ref;
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
    wl_columnar_memory_governor_ref_release(source_ref);
}

static void
test_source_governed_join_key_scratch_alloc_failure(void)
{
    static const char *const left_keys[] = { "k" };
    static const char *const right_keys[] = { "k" };
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *right = make_right(2, 1);
    col_rel_t *left = make_left(4, 2);
    wl_columnar_memory_governor_ref_t *source_ref = NULL;
    wl_columnar_memory_governor_ref_t *session_ref = NULL;
    wl_plan_op_t op;

    TEST("key scratch allocation failure releases its admitted source bytes");
    if (!sess || !right || !left
        || !(source_ref = make_governor(64ull * 1024 * 1024))
        || col_rel_attach_memory_governor(left, source_ref) != 0) {
        FAIL("fixture");
        goto out;
    }
    uint64_t source_baseline = reserved_for(source_ref);
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            source_ref)->usable_bytes, source_baseline + 2u * sizeof(uint32_t),
        memory_order_release);
    session_ref = sess->memory_governor;
    sess->memory_governor = NULL;
    if (session_add_rel(sess, right) != 0) {
        sess->memory_governor = session_ref;
        session_ref = NULL;
        FAIL("right relation setup");
        goto out;
    }
    right = NULL;
    init_join_op(&op, left_keys, right_keys);
    wl_columnar_join_test_fail_next_key_scratch_alloc = true;
    int rc = run_join(sess, left, &op, NULL);
    bool injected = !wl_columnar_join_test_fail_next_key_scratch_alloc;
    if (rc != ENOMEM || !injected || sess->memory_budget_denied
        || reserved_for(source_ref) != source_baseline) {
        FAIL("allocator failure was confused with denial or leaked bytes");
    } else {
        PASS();
    }
out:
    wl_columnar_join_test_fail_next_key_scratch_alloc = false;
    if (sess && session_ref)
        sess->memory_governor = session_ref;
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
    wl_columnar_memory_governor_ref_release(source_ref);
}

static void
test_source_governor_output_precedence(void)
{
    const char *unused_keys[] = { "k" };

    for (uint32_t mode = 0; mode < 3; mode++) {
        wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
        wl_columnar_memory_governor_ref_t *session_ref;
        wl_columnar_memory_governor_ref_t *left_ref = make_governor(
            64ull * 1024 * 1024);
        wl_columnar_memory_governor_ref_t *right_ref = make_governor(
            64ull * 1024 * 1024);
        col_rel_t *right = make_right(2, 1);
        col_rel_t *left = make_left(4, 2);
        wl_plan_op_t op;
        eval_entry_t result = { 0 };
        wl_columnar_memory_governor_ref_t *expected = mode == 0
            ? (sess ? sess->memory_governor : NULL)
            : mode == 1 ? left_ref : right_ref;

        TEST(mode == 0
            ? "output admission prefers session over source governors"
            : mode == 1
            ? "source output admission prefers left over right governor"
            : "source output admission falls back to right governor");
        if (!sess || !left_ref || !right_ref || !right || !left) {
            FAIL("fixture");
            col_rel_destroy(left);
            col_rel_destroy(right);
            destroy_session(sess);
            wl_columnar_memory_governor_ref_release(left_ref);
            wl_columnar_memory_governor_ref_release(right_ref);
            break;
        }
        session_ref = sess->memory_governor;
        if (mode != 0)
            sess->memory_governor = NULL;
        if ((mode != 2 && col_rel_attach_memory_governor(left,
            left_ref) != 0)
            || col_rel_attach_memory_governor(right, right_ref) != 0
            || session_add_rel(sess, right) != 0) {
            FAIL("source governor setup failed");
            sess->memory_governor = session_ref;
            col_rel_destroy(left);
            col_rel_destroy(right);
            destroy_session(sess);
            wl_columnar_memory_governor_ref_release(left_ref);
            wl_columnar_memory_governor_ref_release(right_ref);
            break;
        }
        right = NULL;
        init_cross_op(&op);
        op.materialized = false;
        op.left_keys = unused_keys;
        op.right_keys = unused_keys;
        if (run_join(sess, left, &op, &result) != 0 || !result.rel
            || result.rel->memory_governor != expected
            || !admission_invariant(result.rel)) {
            FAIL("output did not follow the effective source governor");
        } else {
            PASS();
        }
        if (result.owned && result.rel)
            col_rel_destroy(result.rel);
        col_rel_destroy(left);
        sess->memory_governor = session_ref;
        destroy_session(sess);
        wl_columnar_memory_governor_ref_release(left_ref);
        wl_columnar_memory_governor_ref_release(right_ref);
    }
}

static void
test_filter_join_hash_denial_unwinds_scratch(void)
{
    const char *left_keys[] = { "k" };
    const char *right_keys[] = { "k" };
    col_rel_t *shape = col_rel_new_auto("$filter_join_budget", 2);
    uint64_t output_bytes = 0;
    if (!shape || !col_rel_retained_bytes_for(shape, COL_REL_INIT_CAP,
        &output_bytes)) {
        col_rel_destroy(shape);
        TEST("SEMI/ANTI hash denial restores admitted scratch");
        FAIL("could not measure filter-join output footprint");
        return;
    }
    col_rel_destroy(shape);

    for (uint32_t which = 0; which < 2; which++) {
        /* Admit output, key-index slots, probe row, and all but one byte
         * of the 16-bucket hash: two chains for SEMI, three for ANTI. */
        uint64_t operation_budget = output_bytes + 2u * sizeof(uint32_t)
            + 2u * sizeof(int64_t);
        wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
        col_rel_t *right = make_right(2, 1);
        col_rel_t *left = make_left(4, 2);
        wl_plan_op_t op = { 0 };
        eval_entry_t result = { 0 };
        int (*fn)(const wl_plan_op_t *, eval_stack_t *, wl_col_session_t *)
            = which == 0 ? wl_columnar_semijoin_op : wl_columnar_antijoin_op;

        TEST(which == 0
            ? "SEMI hash denial restores admitted scratch"
            : "ANTI hash denial destroys output and restores scratch");
        if (!sess || !right || !left) {
            tests_failed++;
            printf("FAIL: fixture\n");
            col_rel_destroy(left);
            col_rel_destroy(right);
            destroy_session(sess);
            continue;
        }
        uint32_t buckets = wl_columnar_filter_next_pow2(
            2u * right->nrows);
        uint64_t head_bytes = 0, next_bytes = 0, hash_bytes = 0;
        if (right->nrows != 2u || buckets != 16u
            || !wl_columnar_memory_size_mul(buckets, sizeof(uint32_t),
            &head_bytes)
            || !wl_columnar_memory_size_mul(
                (uint64_t)right->nrows + (which == 1u),
                sizeof(uint32_t), &next_bytes)
            || !wl_columnar_memory_size_add(head_bytes, next_bytes,
            &hash_bytes)
            || !wl_columnar_memory_size_add(operation_budget,
            hash_bytes - 1u, &operation_budget)) {
            tests_failed++;
            printf("FAIL: hash footprint fixture\n");
            col_rel_destroy(left);
            col_rel_destroy(right);
            destroy_session(sess);
            continue;
        }
        if (session_add_rel(sess, right) != 0) {
            tests_failed++;
            printf("FAIL: right relation setup\n");
            col_rel_destroy(left);
            col_rel_destroy(right);
            destroy_session(sess);
            continue;
        }
        right = NULL;
        atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
                sess->memory_governor)->usable_bytes,
            registry_charge(sess) + operation_budget, memory_order_release);
        memset(&op, 0, sizeof(op));
        op.right_relation = "right";
        op.key_count = 1;
        op.left_keys = left_keys;
        op.right_keys = right_keys;
        int rc = run_filter_join(fn, sess, left, &op, &result);
        if (rc != ENOSPC || result.rel || !sess->memory_budget_denied
            || reserved_of(sess) != registry_charge(sess) || left->nrows != 4
            || col_rel_get(left, 0, 0) != 0) {
            tests_failed++;
            printf(
                "FAIL: hash refusal leaked bytes or changed the held input\n");
        } else {
            PASS();
        }
        col_rel_destroy(left);
        destroy_session(sess);
    }
}

static void
test_source_governed_anti_semi_outputs(void)
{
    const char *left_keys[] = { "k" };
    const char *right_keys[] = { "k" };
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    wl_columnar_memory_governor_ref_t *session_ref;
    wl_columnar_memory_governor_ref_t *source_ref = make_governor(
        64ull * 1024 * 1024);
    col_rel_t *right = make_right(2, 1);
    col_rel_t *left = make_left(4, 2);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };

    TEST("source-only governor admits ANTI and SEMI JOIN outputs");
    if (!sess || !source_ref || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (col_rel_enable_timestamps(left) != 0) {
        FAIL("timestamped ANTI fixture");
        goto out;
    }
    session_ref = sess->memory_governor;
    sess->memory_governor = NULL;
    if (col_rel_attach_memory_governor(left, source_ref) != 0
        || session_add_rel(sess, right) != 0) {
        sess->memory_governor = session_ref;
        FAIL("source governor setup failed");
        goto out;
    }
    right = NULL;
    memset(&op, 0, sizeof(op));
    op.right_relation = "right";
    op.key_count = 1;
    op.left_keys = left_keys;
    op.right_keys = right_keys;
    if (run_filter_join(wl_columnar_antijoin_op, sess, left, &op, &result)
        != 0 || !result.rel || result.rel->memory_governor != source_ref
        || !admission_invariant(result.rel) || result.rel->timestamps) {
        FAIL("ANTI output admission or timestamp policy changed");
        goto out_result;
    }
    if (result.owned)
        col_rel_destroy(result.rel);
    memset(&result, 0, sizeof(result));
    if (run_filter_join(wl_columnar_semijoin_op, sess, left, &op, &result)
        != 0 || !result.rel || result.rel->memory_governor != source_ref
        || !admission_invariant(result.rel)) {
        FAIL("SEMI output was not charged to its source governor");
        goto out_result;
    }
    PASS();
out_result:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
    result.rel = NULL;
    result.owned = false;
    col_rel_destroy(left);
    left = NULL;
    sess->memory_governor = session_ref;
out:
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
    wl_columnar_memory_governor_ref_release(source_ref);
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
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
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
    out_bytes = relation_charge(governed);
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
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
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

static void
test_parallel_cross_source_governor_accounts_contexts(void)
{
    wl_col_session_t *sess = make_session_workers(64ull * 1024 * 1024, 2);
    wl_columnar_memory_governor_ref_t *source_ref
        = make_governor(64ull * 1024 * 1024);
    wl_columnar_memory_governor_ref_t *session_ref
        = sess ? sess->memory_governor : NULL;
    col_rel_t *right = make_right(13, 1);
    col_rel_t *left = make_left(7, 2);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    uint64_t left_bytes = 0;

    TEST("parallel cross JOIN admits source-governed worker contexts");
    setenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS", "2", 1);
    if (!sess || !source_ref || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (col_rel_attach_memory_governor(left, source_ref) != 0) {
        FAIL("left source governor setup");
        goto out;
    }
    left_bytes = relation_charge(left);
    sess->memory_governor = NULL;
    if (session_add_rel(sess, right) != 0) {
        sess->memory_governor = session_ref;
        FAIL("right relation setup");
        goto out;
    }
    right = NULL;
    init_cross_op(&op);
    wl_columnar_join_test_last_cross_ctx_admitted = false;
    if (run_join(sess, left, &op, &result) != 0 || !result.rel
        || result.rel->nrows != 91u
        || result.rel->memory_governor != source_ref
        || !admission_invariant(result.rel)
        || !wl_columnar_join_test_last_cross_ctx_admitted
        || sess->wq == NULL) {
        FAIL("parallel cross scratch did not use the source governor");
        goto out_entry;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    col_mat_cache_clear(&sess->mat_cache);
    if (reserved_for(source_ref) != left_bytes) {
        FAIL("parallel cross cleanup retained context or output credit");
        goto out;
    }
    PASS();
    goto out_entry;
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    if (sess)
        sess->memory_governor = session_ref;
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
    wl_columnar_memory_governor_ref_release(source_ref);
}

static void
test_parallel_semijoin_source_governor_accounts_scratch(void)
{
    wl_col_session_t *sess = make_session_workers(64ull * 1024 * 1024, 2);
    wl_columnar_memory_governor_ref_t *source_ref
        = make_governor(64ull * 1024 * 1024);
    wl_columnar_memory_governor_ref_t *session_ref
        = sess ? sess->memory_governor : NULL;
    col_rel_t *right = make_right(1, 2);
    col_rel_t *left = make_left(65, 1);
    const char *left_keys[] = { "k" };
    const char *right_keys[] = { "k" };
    wl_plan_op_t op = { 0 };
    eval_entry_t result = { 0 };
    uint64_t left_bytes = 0;

    TEST("parallel SEMI JOIN admits source-governed worker scratch");
    setenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS", "2", 1);
    if (!sess || !source_ref || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (col_rel_attach_memory_governor(left, source_ref) != 0) {
        FAIL("left source governor setup");
        goto out;
    }
    left_bytes = relation_charge(left);
    sess->memory_governor = NULL;
    if (session_add_rel(sess, right) != 0) {
        sess->memory_governor = session_ref;
        FAIL("right relation setup");
        goto out;
    }
    right = NULL;
    op.right_relation = "right";
    op.key_count = 1;
    op.left_keys = left_keys;
    op.right_keys = right_keys;
    wl_columnar_join_test_last_semijoin_parallel_admitted = false;
    if (run_filter_join(wl_columnar_semijoin_op, sess, left, &op, &result)
        != 0 || !result.rel || result.rel->nrows != 65u
        || result.rel->memory_governor != source_ref
        || !admission_invariant(result.rel)
        || !wl_columnar_join_test_last_semijoin_parallel_admitted
        || sess->wq == NULL) {
        FAIL("parallel SEMI scratch did not use the source governor");
        goto out_entry;
    }
    for (uint32_t row = 0; row < result.rel->nrows; row++) {
        if (col_rel_get(result.rel, row, 0) != 0
            || col_rel_get(result.rel, row, 1) != row) {
            FAIL("parallel SEMI output differs from matching input rows");
            goto out_entry;
        }
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    if (reserved_for(source_ref) != left_bytes) {
        FAIL("parallel SEMI cleanup retained scratch or output credit");
        goto out;
    }
    PASS();
    goto out_entry;
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    if (sess)
        sess->memory_governor = session_ref;
    unsetenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS");
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
    wl_columnar_memory_governor_ref_release(source_ref);
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
    if (session_add_rel(sess, right) != 0)
        goto out;
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
    sess = make_session_workers(residual + 16u * sizeof(col_rel_t *)
            + 16u * sizeof(uint32_t) + sizeof(uint32_t) - 1u, 2);
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
    if (run_join(sess, left, &op, NULL) != ENOSPC) {
        FAIL("an unaffordable cross join was not denied with ENOSPC");
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
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation registration failed");
        goto out;
    }
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
    if (!col_rel_retained_live_bytes(governed, &final_bytes)
        || !col_rel_retained_bytes_for(governed, COL_REL_INIT_CAP,
        &initial_bytes)
        || governed->retained_reserved_bytes != final_bytes)
        goto out_entry;
    other_bytes = reserved_of(sess) - relation_charge(governed);
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
    wl_mem_ledger_init(&sess->mem_ledger, 1000u);
    wl_mem_ledger_set_gauge(&sess->mem_ledger, WL_MEM_SUBSYS_RELATION, 400u);
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
        uint64_t result_bytes = relation_charge(result.rel);
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
            || relation_charge(cons_stack.items[0].rel) != result_bytes
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
test_diff_consolidate_copy_admission(void)
{
    wl_col_session_t *sess = make_session(64ull * 1024 * 1024);
    col_rel_t *borrowed = make_left(4, 2);
    eval_stack_t stack;
    uint64_t reserved;
    int rc;

    TEST("differential CONS copy obeys admission and can retry");
    if (!sess || !borrowed
        || col_rel_attach_memory_governor(borrowed,
        sess->memory_governor) != 0) {
        FAIL("fixture");
        goto out;
    }
    eval_stack_init(&stack);
    if (eval_stack_push(&stack, borrowed, false) != 0) {
        FAIL("could not push borrowed CONS input");
        goto out;
    }
    reserved = reserved_of(sess);
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            sess->memory_governor)->usable_bytes, reserved,
        memory_order_release);
    rc = col_op_consolidate_diff(&stack, sess);
    if (rc != ENOSPC || stack.top != 1 || stack.items[0].rel != borrowed
        || stack.items[0].owned || !sess->memory_budget_denied
        || reserved_of(sess) != reserved) {
        (void)eval_stack_drain(&stack);
        FAIL("denied CONS copy did not preserve borrowed retry input");
        goto out;
    }
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            sess->memory_governor)->usable_bytes, 64ull * 1024 * 1024,
        memory_order_release);
    sess->memory_budget_denied = false;
    rc = col_op_consolidate_diff(&stack, sess);
    if (rc != 0 || stack.top != 1 || stack.items[0].rel == borrowed
        || !stack.items[0].owned || stack.items[0].rel->nrows != 4) {
        (void)eval_stack_drain(&stack);
        FAIL("CONS copy did not retry after admission was restored");
        goto out;
    }
    if (eval_stack_drain(&stack) != 0) {
        FAIL("CONS retry cleanup");
        goto out;
    }
    PASS();
out:
    col_rel_destroy(borrowed);
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
        || reserved_of(sess) != registry_charge(sess)
        + relation_charge(result.rel)) {
        FAIL("differential cache hit did not retain pin and charge its copy");
        goto out_result;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    if (reserved_of(sess) != registry_charge(sess)) {
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
    wl_col_session_t *sess = make_session_workers(
        16u * sizeof(col_rel_t *), 2);
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
    if (run_diff_join(sess, left, &op, &result) != ENOSPC || result.rel
        || reserved_of(sess) != registry_charge(sess)
        || sess->mat_cache.count != 0
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
run_parallel_diff_partial_submit_failure(uint32_t fail_at,
    const char *test_name)
{
    wl_col_session_t *sess = make_session_workers(64ull * 1024 * 1024, 2);
    col_rel_t *right = make_right(1, 2);
    col_rel_t *left = make_left(65, 1);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };

    TEST(test_name);
    setenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS", "2", 1);
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation setup");
        goto out;
    }
    right = NULL;
    init_diff_keyed_op(&op);
    test_join_submit_calls = 0;
    test_join_submit_fail_at = fail_at;
    wl_columnar_join_test_submit_override = fail_join_submit_at;
    int rc = run_diff_join(sess, left, &op, &result);
    wl_columnar_join_test_submit_override = NULL;
    if (rc != ENOMEM || test_join_submit_calls != fail_at || result.rel
        || left->nrows != 65
        || reserved_of(sess) != registry_charge(sess)
        || sess->mat_cache.count != 0 || sess->diff_arr_count != 0
        || sess->diff_txn_count != 0) {
        FAIL("partial worker submission published or retained worker state");
        goto out_entry;
    }
    if (run_diff_join(sess, left, &op, &result) != 0 || !result.rel
        || result.rel->nrows != 130 || sess->wq == NULL
        || sess->diff_arr_count != 1) {
        FAIL("parallel retry did not reproduce the full differential output");
        goto out_entry;
    }
    for (uint32_t row = 0; row < result.rel->nrows; row++) {
        if (col_rel_get(result.rel, row, 0) != 0
            || col_rel_get(result.rel, row, 1) != row / 2u
            || col_rel_get(result.rel, row, 2) != 0
            || col_rel_get(result.rel, row, 3) != 1u - (row % 2u)) {
            FAIL(
                "parallel retry rows differ from expected differential output");
            goto out_entry;
        }
    }
    PASS();
    goto out_entry;
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    wl_columnar_join_test_submit_override = NULL;
    unsetenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS");
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

static void
test_parallel_diff_partial_count_submit_drains_and_retries(void)
{
    run_parallel_diff_partial_submit_failure(2u,
        "parallel diff partial count submission drains and retries");
}

static void
test_parallel_diff_partial_fill_submit_drains_and_retries(void)
{
    run_parallel_diff_partial_submit_failure(4u,
        "parallel diff partial fill submission drains and retries");
}

static void
test_parallel_diff_w8_dispatch_and_output(void)
{
    wl_col_session_t *sess = make_session_workers(64ull * 1024 * 1024, 8);
    col_rel_t *right = make_right(1, 2);
    col_rel_t *left = make_left(65, 1);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };

    TEST("parallel keyed diff dispatches W8 and preserves exact output");
    setenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS", "2", 1);
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation setup");
        goto out;
    }
    right = NULL;
    init_diff_keyed_op(&op);
    if (run_diff_join(sess, left, &op, &result) != 0 || !result.rel
        || result.rel->nrows != 130u || sess->wq == NULL
        || wl_columnar_join_test_last_keyed_workers != 8u
        || sess->diff_arr_count != 1) {
        FAIL("eight-worker differential join did not reach expected output");
        goto out_entry;
    }
    for (uint32_t row = 0; row < result.rel->nrows; row++) {
        if (col_rel_get(result.rel, row, 0) != 0
            || col_rel_get(result.rel, row, 1) != row / 2u
            || col_rel_get(result.rel, row, 2) != 0
            || col_rel_get(result.rel, row, 3) != 1u - (row % 2u)) {
            FAIL("W8 output differs from the exact keyed-join rows");
            goto out_entry;
        }
    }
    PASS();
    goto out_entry;
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
test_parallel_diff_source_governor_accounting(void)
{
    wl_col_session_t *sess = make_session_workers(64ull * 1024 * 1024, 2);
    wl_columnar_memory_governor_ref_t *source_ref
        = make_governor(64ull * 1024 * 1024);
    wl_columnar_memory_governor_ref_t *session_ref
        = sess ? sess->memory_governor : NULL;
    col_rel_t *right = make_right(1, 2);
    col_rel_t *left = make_left(65, 1);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };

    TEST("parallel keyed diff uses and releases the source-only governor");
    setenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS", "2", 1);
    if (!sess || !source_ref || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (col_rel_attach_memory_governor(left, source_ref) != 0) {
        FAIL("could not govern the borrowed left input");
        goto out;
    }
    sess->memory_governor = NULL;
    if (session_add_rel(sess, right) != 0) {
        sess->memory_governor = session_ref;
        FAIL("right relation setup");
        goto out;
    }
    right = NULL;
    init_diff_keyed_op(&op);
    if (run_diff_join(sess, left, &op, &result) != 0 || !result.rel
        || result.rel->nrows != 130u || result.rel->memory_governor
        != source_ref || !admission_invariant(result.rel)
        || sess->wq == NULL
        || wl_columnar_join_test_last_keyed_workers != 2u
        || !wl_columnar_join_test_last_keyed_parallel_admitted) {
        FAIL("parallel diff scratch ignored the source governor");
        goto out_entry;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    col_mat_cache_clear(&sess->mat_cache);
    if (reserved_for(source_ref) != relation_charge(left)) {
        FAIL("parallel diff cleanup retained scratch or output charges");
        goto out;
    }
    PASS();
    goto out;
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    if (sess)
        sess->memory_governor = session_ref;
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
    wl_columnar_memory_governor_ref_release(source_ref);
}

static void
run_parallel_pair_growth_failure(bool deny_growth, bool fail_commit)
{
    wl_col_session_t *sess = make_session_workers(64ull * 1024 * 1024, 2);
    col_rel_t *right = make_right(1, 1);
    col_rel_t *left = make_split_left(100001u, 50001u);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };
    bool injected;

    TEST(deny_growth
        ? "parallel pair-vector growth denial restores old reservation"
        : fail_commit
        ? "parallel pair-vector commit failure restores reservation"
        : "parallel pair-vector realloc failure restores old reservation");
    setenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS", "2", 1);
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    atomic_store_explicit(&sess->mem_ledger.total_budget,
        64ull * 1024 * 1024, memory_order_release);
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation setup");
        goto out;
    }
    right = NULL;
    init_diff_keyed_op(&op);
    test_pair_growth_denial_injected = false;
    if (deny_growth)
        wl_columnar_join_test_before_pair_growth
            = deny_pair_growth_with_governor_limit;
    else if (fail_commit)
        atomic_store_explicit(&wl_columnar_join_test_fail_pair_commit, true,
            memory_order_release);
    else
        wl_columnar_join_test_fail_pair_realloc = true;
    int rc = run_diff_join(sess, left, &op, &result);
    wl_columnar_join_test_before_pair_growth = NULL;
    injected = deny_growth ? test_pair_growth_denial_injected
        : fail_commit ? !atomic_load_explicit(
        &wl_columnar_join_test_fail_pair_commit, memory_order_acquire)
        : !wl_columnar_join_test_fail_pair_realloc;
    if (rc != (deny_growth ? ENOSPC : ENOMEM) || !injected || result.rel
        || (deny_growth && !sess->memory_budget_denied)
        || reserved_of(sess) != registry_charge(sess)
        || sess->mat_cache.count != 0
        || sess->diff_arr_count != 0 || sess->diff_txn_count != 0) {
        FAIL("pair-vector failure published output or retained scratch");
        goto out_entry;
    }

    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            sess->memory_governor)->usable_bytes, 64ull * 1024 * 1024,
        memory_order_release);
    sess->memory_budget_denied = false;
    if (run_diff_join(sess, left, &op, &result) != 0 || !result.rel
        || result.rel->nrows != 50001u || sess->diff_arr_count != 1) {
        FAIL("pair-vector failure retry did not produce all matches");
        goto out_entry;
    }
    for (uint32_t row = 0; row < result.rel->nrows; row++) {
        if (col_rel_get(result.rel, row, 0) != 0
            || col_rel_get(result.rel, row, 1) != row
            || col_rel_get(result.rel, row, 2) != 0
            || col_rel_get(result.rel, row, 3) != 0) {
            FAIL("pair-vector failure retry changed output rows");
            goto out_entry;
        }
    }
    PASS();
    goto out_entry;
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    wl_columnar_join_test_before_pair_growth = NULL;
    wl_columnar_join_test_fail_pair_realloc = false;
    atomic_store_explicit(&wl_columnar_join_test_fail_pair_commit, false,
        memory_order_release);
    unsetenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS");
    col_rel_destroy(left);
    col_rel_destroy(right);
    destroy_session(sess);
}

static void
test_parallel_pair_growth_denial_unwinds_and_retries(void)
{
    run_parallel_pair_growth_failure(true, false);
}

static void
test_parallel_pair_growth_alloc_failure_unwinds_and_retries(void)
{
    run_parallel_pair_growth_failure(false, false);
}

static void
test_parallel_pair_growth_commit_failure_unwinds_and_retries(void)
{
    run_parallel_pair_growth_failure(false, true);
}

static void
test_parallel_pair_cache_limit_falls_back_to_reprobe(void)
{
    wl_col_session_t *sess = make_session_workers(64ull * 1024 * 1024, 2);
    col_rel_t *right = make_right(1, 2);
    col_rel_t *left = make_left(3000, 1);
    wl_plan_op_t op;
    eval_entry_t result = { 0 };

    TEST("parallel pair-cache cap falls back without losing JOIN rows");
    setenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS", "2", 1);
    wl_columnar_join_test_pair_cap_limit_override = 1024u;
    if (!sess || !right || !left) {
        FAIL("fixture");
        goto out;
    }
    if (session_add_rel(sess, right) != 0) {
        FAIL("right relation setup");
        goto out;
    }
    right = NULL;
    init_diff_keyed_op(&op);
    if (run_diff_join(sess, left, &op, &result) != 0 || !result.rel
        || result.rel->nrows != 6000u) {
        FAIL("pair-cache cap failed instead of reprobeing all matches");
        goto out_entry;
    }
    for (uint32_t row = 0; row < result.rel->nrows; row++) {
        if (col_rel_get(result.rel, row, 0) != 0
            || col_rel_get(result.rel, row, 1) != row / 2u
            || col_rel_get(result.rel, row, 2) != 0
            || col_rel_get(result.rel, row, 3) != 1u - (row % 2u)) {
            FAIL("pair-cache cap fallback changed output rows");
            goto out_entry;
        }
    }
    PASS();
out_entry:
    if (result.owned && result.rel)
        col_rel_destroy(result.rel);
out:
    wl_columnar_join_test_pair_cap_limit_override = 0;
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
        || sess->diff_txn_count != 0
        || reserved_of(sess) != registry_charge(sess)) {
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
    charged = relation_charge(retained->rel);
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
        || reservation->bytes != retained->rel->retained_reserved_bytes
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
        || relation_charge(stack.items[stack.top - 1].rel) != charged
        || &stack.items[stack.top - 1].rel->retained_reservation
        != reservation
        || reservation->identity != reservation_identity
        || reservation->governor != reservation_governor
        || atomic_load_explicit(&reservation->owner_bits,
        memory_order_acquire) != owner_bits
        || atomic_load_explicit(&reservation->state,
        memory_order_acquire) != reservation_state
        || reservation->bytes != retained->rel->retained_reserved_bytes
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
    cache_bytes = relation_charge(cached);
    copy_bytes = relation_charge(result.rel);
    if (cache_bytes == 0 || copy_bytes == 0
        || reserved != registry_charge(sess) + cache_bytes + copy_bytes) {
        FAIL("cache and stack twin reservations are not both charged");
        goto out_entry;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    if (reserved_of(sess) != registry_charge(sess) + cache_bytes) {
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
        || relation_charge(result.rel) != copy_bytes
        || col_rel_get(result.rel, 0, 0) != col_rel_get(cached, 0, 0)
        || reserved_of(sess) != registry_charge(sess)
        + cache_bytes + copy_bytes) {
        FAIL("cache hit did not return a governed, value-identical copy");
        goto out_entry;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    if (reserved_of(sess) != registry_charge(sess) + cache_bytes) {
        FAIL("cache-hit copy release changed cache reservation");
        goto out;
    }
    atomic_store_explicit(&wl_columnar_memory_governor_ref_get(
            sess->memory_governor)->usable_bytes,
        registry_charge(sess) + cache_bytes - 1u,
        memory_order_release);
    if (run_join(sess, left, &op, &result) != ENOSPC || result.rel
        || cached_output(sess, left) != cached
        || reserved_of(sess) != registry_charge(sess) + cache_bytes) {
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
    if (reserved_of(sess) != registry_charge(sess)) {
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
    source_bytes = relation_charge(cached);
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
    copy_bytes = relation_charge(result.rel);
    if (reserved_for(source_governor)
        != registry_charge(sess) + source_bytes
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
        != registry_charge(sess) + source_bytes
        + relation_charge(result.rel)
        || reserved_for(other) != 0u) {
        FAIL("cache hit did not fall back to the source governor");
        goto out_result;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    sess->memory_governor = source_governor;
    if (reserved_for(source_governor)
        != registry_charge(sess) + source_bytes) {
        FAIL("source-governor copy did not release its own token");
        goto out;
    }
    col_mat_cache_clear(&sess->mat_cache);
    if (reserved_for(source_governor) != registry_charge(sess)) {
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
        || reserved_of(sess) != registry_charge(sess)
        + relation_charge(result.rel)) {
        FAIL(
            "failed twin admission leaked a token or published an unaccounted result");
        goto out_entry;
    }
    col_rel_destroy(result.rel);
    result.rel = NULL;
    if (reserved_of(sess) != registry_charge(sess)) {
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
        || sess->memory_budget_denied
        || reserved_of(sess) != registry_charge(sess)
        + relation_charge(ordinary.rel)) {
        FAIL("ENOSPC did not publish one already-accounted JOIN result");
        goto out;
    }
    col_rel_destroy(ordinary.rel);
    ordinary.rel = NULL;
    if (reserved_of(sess) != registry_charge(sess)) {
        FAIL("ordinary single-owner fallback failed to release its token");
        goto out;
    }

    init_diff_keyed_op(&op);
    if (run_diff_join(sess, left, &op, &differential) != 0
        || !differential.rel || !differential.owned
        || differential.rel->memory_governor != sess->memory_governor
        || !admission_invariant(differential.rel)
        || sess->mat_cache.count != COL_MAT_CACHE_MAX
        || sess->memory_budget_denied
        || sess->diff_arr_count != 1) {
        FAIL(
            "ENOSPC did not commit differential fallback as one accounted owner");
        goto out;
    }
    uint64_t diff_reserved = reserved_of(sess);
    uint64_t differential_bytes = relation_charge(differential.rel);
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
        || reserved_of(sess) != registry_charge(sess)
        + relation_charge(cache_entry)
        + relation_charge(stack.items[0].rel)) {
        FAIL("JOIN did not charge cache and stack outputs exactly");
        goto out;
    }
    cache_bytes = relation_charge(cache_entry);
    int filter_rc = wl_columnar_filter_op(&filter_op, &stack, sess);
    if (filter_rc != 0 || stack.top != 1 || stack.items[0].rel->nrows != 4
        || stack.items[0].rel->memory_governor != sess->memory_governor
        || reserved_of(sess) != registry_charge(sess) + cache_bytes
        + relation_charge(stack.items[0].rel)) {
        FAIL(
            "FILTER did not consume the governed copy and preserve cache charge");
        goto out;
    }
    if (col_op_map(&map_op, &stack, sess) != 0
        || stack.top != 1 || stack.items[0].rel->ncols != 1
        || stack.items[0].rel->nrows != 4
        || stack.items[0].rel->memory_governor != sess->memory_governor
        || reserved_of(sess) != registry_charge(sess) + cache_bytes
        + relation_charge(stack.items[0].rel)) {
        FAIL("MAP changed retained JOIN cache accounting");
        goto out;
    }
    if (eval_stack_drain(&stack) != 0
        || reserved_of(sess) != registry_charge(sess) + cache_bytes) {
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
        || stack.items[0].rel->memory_governor != sess->memory_governor
        || reserved_of(sess) != registry_charge(sess) + cache_bytes
        + relation_charge(stack.items[0].rel)) {
        FAIL(
            "CONCAT did not replace the governed input charge with its output");
        goto out;
    }
    if (col_op_consolidate(&stack, sess) != 0 || stack.top != 1
        || stack.items[0].rel->nrows != 8u
        || reserved_of(sess) != registry_charge(sess) + cache_bytes
        + relation_charge(stack.items[0].rel)) {
        FAIL("CONSOLIDATE changed result or retained-cache accounting");
        goto out;
    }
    if (eval_stack_drain(&stack) != 0) {
        FAIL("could not release consolidated output");
        goto out;
    }
    stack_ready = false;
    col_mat_cache_clear(&sess->mat_cache);
    if (reserved_of(sess) != registry_charge(sess)) {
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

static void
test_timestamp_batch_projection_and_live_charge(void)
{
    col_rel_t *batch = col_rel_new_auto("$join_batch", 1);
    wl_col_session_t *sess = make_session(1u << 20);
    uint64_t row_bytes = 0, live_bytes = 0;

    TEST("timestamp JOIN batch projects one row but admits physical capacity");
    if (!batch || !sess || col_rel_enable_timestamps(batch) != 0
        || col_rel_attach_memory_governor(batch, sess->memory_governor) != 0
        || col_rel_reserve_capacity_admitted(batch, batch->capacity,
        NULL) != 0
        || !col_rel_retained_bytes_for(batch, 1u, &row_bytes)
        || !col_rel_retained_live_bytes(batch, &live_bytes)
        || batch->capacity <= 1u
        || row_bytes != sizeof(int64_t) + sizeof(col_delta_timestamp_t)
        || live_bytes != (uint64_t)batch->capacity * row_bytes
        + sizeof(int64_t *) + sizeof(int64_t)
        || batch->retained_reserved_bytes != live_bytes) {
        FAIL("projected row or live retained footprint changed");
    } else {
        PASS();
    }
    col_rel_destroy(batch);
    destroy_session(sess);
}

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
    for (unsigned governor = 0; governor < 3; governor++)
        for (unsigned owned = 0; owned < 2; owned++)
            test_reduce_admission(governor, owned != 0, 0,
                COL_REL_INIT_CAP * 3 + 1);
    test_reduce_admission(0, false, 1, COL_REL_INIT_CAP * 3 + 1);
    test_reduce_admission(1, true, 2, COL_REL_INIT_CAP * 3 + 1);
    test_reduce_admission(1, false, 3, COL_REL_INIT_CAP * 3 + 1);
    test_reduce_admission(0, true, 0, 0);
    for (unsigned mode = 0; mode < 3; mode++)
        test_operator_unmanaged_storage(mode, true);
    for (unsigned governor = 0; governor < 3; governor++) {
        for (unsigned owned = 0; owned < 2; owned++)
            test_map_admission(governor, owned != 0, COL_REL_INIT_CAP + 1,
                false, false);
    }
    for (unsigned mode = 0; mode < 3; mode++)
        test_operator_unmanaged_storage(mode, false);
    test_map_admission(0, false, 0, false, false);
    test_map_admission(1, true, 3, true, false);
    test_map_admission(0, false, 3, false, true);
    test_map_admission(1, true, 3, false, true);
    for (unsigned pooled = 0; pooled < 2; pooled++) {
        for (unsigned governor = 0; governor < 3; governor++) {
            for (unsigned condition = 0; condition < 6; condition++)
                test_variable_empty_admission(pooled != 0, governor, condition);
        }
    }
    test_concat_admission_rollback(false, false);
    test_concat_admission_rollback(false, true);
    test_concat_admission_rollback(true, false);
    test_concat_admission_rollback(true, true);
    test_timestamp_batch_projection_and_live_charge();
    if (measure_output_bytes(&out_bytes)) {
        run_at_budget("cross join succeeds at the exact output+scratch peak",
            out_bytes, true);
        run_at_budget("cross join denied one byte below output+scratch peak",
            out_bytes - 1u, false);
    } else {
        TEST("cross join succeeds at the exact output+scratch peak");
        FAIL("could not measure the output footprint");
        TEST("cross join is denied one byte below its output+scratch peak");
        FAIL("could not measure the output footprint");
    }
    test_missing_right_metadata_boundary();
    test_growth_rollback();
    test_worker_join_above_ledger_threshold_keeps_all_rows();
    test_reuse_after_denial();
    test_projected_output_is_governed();
    test_join_output_growth_denial_is_typed();
    test_filter_join_hash_denial_unwinds_scratch();
    test_source_governed_join_key_scratch_denial();
    test_source_governed_join_key_scratch_alloc_failure();
    test_source_governor_output_precedence();
    test_source_governed_anti_semi_outputs();
    test_cache_adoption_charges_once();
    test_parallel_cross_output_is_governed();
    test_parallel_cross_source_governor_accounts_contexts();
    test_parallel_semijoin_source_governor_accounts_scratch();
    test_parallel_cross_denial();
    test_small_parallel_cross_is_admitted();
    test_parallel_diff_output_is_governed();
    test_diff_consolidate_copy_admission();
    test_parallel_diff_denial();
    test_parallel_diff_true_admission_denial_rolls_back();
    test_parallel_diff_partial_count_submit_drains_and_retries();
    test_parallel_diff_partial_fill_submit_drains_and_retries();
    test_parallel_diff_w8_dispatch_and_output();
    test_parallel_diff_source_governor_accounting();
    test_parallel_pair_growth_denial_unwinds_and_retries();
    test_parallel_pair_growth_alloc_failure_unwinds_and_retries();
    test_parallel_pair_growth_commit_failure_unwinds_and_retries();
    test_parallel_pair_cache_limit_falls_back_to_reprobe();
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
