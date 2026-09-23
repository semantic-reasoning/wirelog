/*
 * columnar/kfusion.c - wirelog Columnar K-Fusion Operator
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#define _GNU_SOURCE

#define WL_KFUSION_MIN_PARALLEL_K 4

#if defined(_MSC_VER)
#define WL_COLUMNAR_KFUSION_ALWAYS_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define WL_COLUMNAR_KFUSION_ALWAYS_INLINE inline __attribute__((always_inline))
#else
#define WL_COLUMNAR_KFUSION_ALWAYS_INLINE inline
#endif

#include "columnar/internal.h"
#include "columnar/lftj.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"

#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int
col_kfusion_drain(eval_stack_t *stack, wl_col_session_t *sess,
    int primary_rc)
{
    int drain_rc = eval_stack_drain_to_session(stack, sess);
    if (drain_rc != 0) {
        fprintf(stderr, "wirelog: K-Fusion stack cleanup failed: %d\n",
            drain_rc);
        /* A block-local stack cannot be allowed to disappear while it still
         * owns a pool/arena relation.  Transfer every remaining entry to the
         * parent session; its allocator outlives this evaluator and the
         * registry retries the exact entry after readers release. */
        int retain_rc = wl_columnar_session_retain_eval_stack(sess, stack);
        if (primary_rc == 0)
            primary_rc = retain_rc != 0 ? retain_rc : drain_rc;
    }
    return primary_rc;
}

static int
col_kfusion_dispose_entry(eval_stack_t *stack, eval_entry_t *entry,
    int primary_rc)
{
    int dispose_rc = eval_stack_dispose_entry(stack, entry);
    return primary_rc != 0 ? primary_rc : dispose_rc;
}

#ifdef __AVX2__
#include <immintrin.h>
#endif

#ifdef __SSE2__
#include <emmintrin.h>
#endif

#ifdef __ARM_NEON__
#include <arm_neon.h>
#endif

/* --- K-FUSION ------------------------------------------------------------ */

/**
 * col_rel_merge_k:
 * Merge K sorted relations into a single deduplicated relation.
 * Uses the same min-heap merging strategy as col_op_consolidate_kway_merge.
 *
 * @relations: Array of K col_rel_t pointers (caller-owned, each sorted)
 * @k:         Number of relations to merge
 *
 * Returns: Newly allocated merged relation (caller must free).
 *          Returns NULL on allocation failure.
 *
 * The output relation name is "<merged-k>" and contains all rows from
 * the K input relations with duplicates removed.
 */
/*
 * col_rel_merge_k - Deterministic K-way sorted merge with deduplication.
 *
 * Determinism guarantee (Issue #260):
 *   - k=1: sequential copy, input order preserved
 *   - k=2: two-pointer merge on sorted inputs, left-before-right tie-break
 *   - k>=3: left-fold over pairs: merge(merge(r[0],r[1]),r[2]),...
 *     Fixed input order + sorted inputs => identical output across runs.
 *
 * Precondition: each input relation is already sorted+deduped
 *   (WL_PLAN_OP_CONSOLIDATE is the last K-fusion worker op).
 */
static col_rel_t *
col_rel_merge_k(col_rel_t **relations, uint32_t k)
{
    if (k == 0)
        return NULL;

    /* All K relations must have the same schema */
    uint32_t nc = relations[0]->ncols;
    uint32_t total_rows = 0;
    for (uint32_t i = 0; i < k; i++) {
        if (relations[i]->ncols != nc)
            return NULL; /* Schema mismatch */
        if (!wl_columnar_relation_float_values_valid(relations[i]))
            return NULL;
        for (uint32_t c = 0; c < nc; c++) {
            wirelog_column_type_t expected = relations[0]->column_types
                ? relations[0]->column_types[c] : WIRELOG_TYPE_INT64;
            wirelog_column_type_t actual = relations[i]->column_types
                ? relations[i]->column_types[c] : WIRELOG_TYPE_INT64;
            if (expected != actual)
                return NULL;
        }
        total_rows += relations[i]->nrows;
    }

    if (total_rows == 0) {
        /* Create empty result with correct schema */
        return col_rel_new_like("<merged-k>", relations[0]);
    }

    /* Create output relation with capacity for all rows */
    col_rel_t *out = col_rel_new_like("<merged-k>", relations[0]);
    if (!out)
        return NULL;

    /* Per-block scratch (#1000): both the staging row and the dedup key must
     * be nc wide, not COL_STACK_MAX wide.  MERGE_K_SETUP declares and
     * allocates them once per merge block -- allocating inside
     * MERGE_K_APPEND would malloc once per row for wide relations. */
#define MERGE_K_SETUP()                                                      \
        col_row_buf_t _rowbuf, _lastbuf;                                         \
        int64_t *_rb, *last_row_buf;                                             \
        const int64_t *last_row = NULL;                                          \
        _rb = col_row_buf_init(&_rowbuf, nc);                                    \
        last_row_buf = col_row_buf_init(&_lastbuf, nc);                          \
        if (!_rb || !last_row_buf) {                                             \
            col_row_buf_release(&_rowbuf);                                       \
            col_row_buf_release(&_lastbuf);                                      \
            col_rel_destroy(out);                                                \
            return NULL;                                                         \
        }

#define MERGE_K_RELEASE()                                                    \
        do {                                                                     \
            col_row_buf_release(&_rowbuf);                                       \
            col_row_buf_release(&_lastbuf);                                      \
        } while (0)

    /* Helper: copy row from relation into temp buf, append to out, dedup
     * against last_row in out.  Bails out of the enclosing function on
     * failure (after releasing the block scratch). */
#define MERGE_K_APPEND(rel_ptr, row_idx)                                     \
        do {                                                                     \
            col_rel_row_copy_out((rel_ptr), (row_idx), _rb);                     \
            if (last_row == NULL                                                 \
                || col_rel_row_values_cmp(relations[0], last_row, _rb) != 0) {  \
                if (col_rel_append_row(out, _rb) != 0) {                         \
                    MERGE_K_RELEASE();                                           \
                    col_rel_destroy(out);                                        \
                    return NULL;                                                 \
                }                                                                \
                col_rel_row_copy_out(out, out->nrows - 1, last_row_buf);         \
                last_row = last_row_buf;                                         \
            }                                                                    \
        } while (0)

    /* K=1: Copy with dedup using append (handles dynamic growth) */
    if (k == 1) {
        col_rel_t *src = relations[0];
        MERGE_K_SETUP();
        for (uint32_t r = 0; r < src->nrows; r++) {
            MERGE_K_APPEND(src, r);
        }
        MERGE_K_RELEASE();
        return out;
    }

    /* K=2: Optimized 2-pointer merge using append */
    if (k == 2) {
        col_rel_t *left = relations[0];
        col_rel_t *right = relations[1];
        uint32_t li = 0, ri = 0;
        MERGE_K_SETUP();

        while (li < left->nrows && ri < right->nrows) {
            int cmp = col_rel_row_cmp2(left, li, right, ri);
            if (cmp == WL_COLUMNAR_CMP_INCOMPATIBLE) {
                MERGE_K_RELEASE();
                col_rel_destroy(out);
                return NULL;
            }

            if (cmp < 0) {
                MERGE_K_APPEND(left, li);
                li++;
            } else if (cmp > 0) {
                MERGE_K_APPEND(right, ri);
                ri++;
            } else {
                /* Equal rows: add once, skip both */
                MERGE_K_APPEND(left, li);
                li++;
                ri++;
            }
        }

        /* Drain remaining rows from left */
        while (li < left->nrows) {
            MERGE_K_APPEND(left, li);
            li++;
        }

        /* Drain remaining rows from right */
        while (ri < right->nrows) {
            MERGE_K_APPEND(right, ri);
            ri++;
        }

        MERGE_K_RELEASE();
        return out;
    }

    /* K >= 3: Pairwise merge fallback */
    col_rel_t *temp = relations[0];
    for (uint32_t i = 1; i < k; i++) {
        col_rel_t *pair[2] = { temp, relations[i] };
        col_rel_t *merged = col_rel_merge_k(pair, 2);
        if (!merged) {
            col_rel_destroy(out);
            if (i > 1)
                col_rel_destroy(temp);
            return NULL;
        }
        if (i > 1)
            col_rel_destroy(temp);
        temp = merged;
    }

    /* Move final result into output using append */
    {
        MERGE_K_SETUP();
        for (uint32_t r = 0; r < temp->nrows; r++) {
            MERGE_K_APPEND(temp, r);
        }
        MERGE_K_RELEASE();
        col_rel_destroy(temp);
    }

#undef MERGE_K_APPEND
#undef MERGE_K_RELEASE
#undef MERGE_K_SETUP
    return out;
}
/**
 * Worker task context for K-fusion evaluation.
 * plan_data is embedded (not a pointer) so its lifetime matches the worker array.
 * sess points to an isolated session wrapper with a per-worker mat_cache so
 * concurrent col_op_join calls do not share the non-thread-safe cache.
 */
typedef struct {
    wl_plan_relation_t plan_data; /* Embedded plan (stable lifetime) */
    eval_stack_t stack;           /* Output stack (initialized by worker) */
    wl_col_session_t
    *sess;        /* Per-worker session wrapper (isolated mat_cache) */
    int rc;       /* Return code from evaluation */
    bool initialized;
    bool submitted;
} col_op_k_fusion_worker_t;

struct wl_columnar_kfusion_cohort {
    wl_col_session_t *parent;
    col_rel_t **results;
    col_op_k_fusion_worker_t *workers;
    wl_col_session_t *worker_sess;
    uint32_t *live_indices;
    uint32_t live_count;
    uint32_t initialized_count;
    uint32_t submitted_count;
    bool dispatch_active;
    bool barrier_complete;
    bool resources_cleaned;
    wl_columnar_memory_reservation_t scratch_reservation;
    col_rel_t *merged;
    int operation_rc;
    atomic_uint_fast64_t shared_join_count;
};

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_kfusion_test_before_cleanup)(wl_col_session_t *sess,
    eval_stack_t *stack, col_rel_t **results, uint32_t result_count);
int (*wl_columnar_kfusion_test_submit)(wl_work_queue_t *wq,
    void (*work_fn)(void *ctx), void *ctx);
#endif

static int
col_kfusion_prepare_cohort_cleanup(struct wl_columnar_kfusion_cohort *cohort)
{
    wl_col_session_t *parent = cohort->parent;
    if (!cohort->barrier_complete && cohort->submitted_count > 0) {
        int rc = wl_workqueue_drain(parent->wq);
        if (rc != 0)
            return rc;
        cohort->barrier_complete = true;
    }
    for (;;) {
        size_t before = 0;
        size_t after = 0;
        int first_rc = 0;
        for (uint32_t d = 0; d < cohort->initialized_count; d++) {
            wl_col_session_t *worker_sess = &cohort->worker_sess[d];
            before += cohort->workers[d].stack.top;
            before += worker_sess->deferred_relation_count;
            before += worker_sess->retained_eval_entry_count;
        }
        for (uint32_t d = 0; d < cohort->live_count; d++)
            before += cohort->results[d] != NULL;

        /* Visit every owner before retrying. A result alias can be the reason
         * a lower worker-stack owner refuses destruction; freeing that alias
         * makes the next pass able to drain the stack. */
        for (uint32_t d = 0; d < cohort->initialized_count; d++) {
            int rc = eval_stack_drain_to_session(&cohort->workers[d].stack,
                    &cohort->worker_sess[d]);
            if (rc != 0 && first_rc == 0)
                first_rc = rc;
        }
        for (uint32_t d = 0; d < cohort->initialized_count; d++) {
            int rc = wl_columnar_session_retry_deferred(
                &cohort->worker_sess[d]);
            if (rc != 0 && first_rc == 0)
                first_rc = rc;
            rc = wl_columnar_session_retry_retained_eval_entries(
                &cohort->worker_sess[d]);
            if (rc != 0 && first_rc == 0)
                first_rc = rc;
        }
        for (uint32_t d = 0; d < cohort->live_count; d++) {
            if (!cohort->results[d])
                continue;
            int rc = col_rel_destroy_checked(cohort->results[d]);
            if (rc == 0)
                cohort->results[d] = NULL;
            else if (first_rc == 0)
                first_rc = rc;
        }

        for (uint32_t d = 0; d < cohort->initialized_count; d++) {
            wl_col_session_t *worker_sess = &cohort->worker_sess[d];
            after += cohort->workers[d].stack.top;
            after += worker_sess->deferred_relation_count;
            after += worker_sess->retained_eval_entry_count;
        }
        for (uint32_t d = 0; d < cohort->live_count; d++)
            after += cohort->results[d] != NULL;
        if (after == 0)
            return 0;
        if (after >= before)
            return first_rc != 0 ? first_rc : EBUSY;
    }
}

static int
col_kfusion_cleanup_cohort(struct wl_columnar_kfusion_cohort *cohort)
{
    wl_col_session_t *parent = cohort->parent;
    int rc = col_kfusion_prepare_cohort_cleanup(cohort);
    if (rc != 0)
        return rc;

    for (uint32_t d = 0; d < cohort->initialized_count; d++) {
        wl_col_session_t *worker = &cohort->worker_sess[d];
        rc = col_session_free_diff_arrangements(worker);
        if (rc != 0)
            return rc;
        if (worker->filt_cache_active_pins != 0)
            return EBUSY;
    }

    for (uint32_t d = 0; d < cohort->initialized_count; d++) {
        wl_col_session_t *worker = &cohort->worker_sess[d];
        if (worker->compound_borrow.arena
            && !wl_compound_arena_borrow_release(
                &worker->compound_borrow))
            return EBUSY;
        col_mat_cache_release_pins(&worker->mat_cache);
        col_mat_cache_clear(&worker->mat_cache);
        if (worker->mat_cache.active_pins != 0)
            return EBUSY;
        {
            wl_col_session_t *co = COL_SESSION(parent);
            uint32_t shared = worker->arr_count < co->arr_count
                ? worker->arr_count : co->arr_count;
            for (uint32_t i = 0; i < shared; i++) {
                if (worker->arr_entries[i].lru_clock
                    > co->arr_entries[i].lru_clock)
                    co->arr_entries[i].lru_clock
                        = worker->arr_entries[i].lru_clock;
            }
            if (worker->arr_clock > co->arr_clock)
                co->arr_clock = worker->arr_clock;
        }
        for (uint32_t i = 0; i < worker->arr_count; i++) {
            col_arr_entry_t *entry = &worker->arr_entries[i];
            free(entry->rel_name);
            free(entry->key_cols);
            arr_free_contents(&entry->arr);
            col_arr_detach_memory_governor(&entry->arr);
        }
        free(worker->arr_entries);
        worker->arr_entries = NULL;
        worker->arr_count = worker->arr_cap = 0;
        col_session_free_delta_arrangements(worker);
        col_session_free_filt_arrangements(worker);
        for (uint32_t i = 0; i < worker->filt_cache_count; i++) {
            col_filt_cache_entry_t *entry = &worker->filt_cache[i];
            if (entry->filtered) {
                rc = col_rel_destroy_checked(entry->filtered);
                if (rc != 0)
                    return rc;
                entry->filtered = NULL;
            }
            free(entry->rel_name);
            entry->rel_name = NULL;
            free(entry->filter_data);
            entry->filter_data = NULL;
        }
        free(worker->filt_cache);
        worker->filt_cache = NULL;
        worker->filt_cache_count = worker->filt_cache_cap = 0;
    }

    for (uint32_t d = 0; d < cohort->initialized_count; d++) {
        wl_col_session_t *worker = &cohort->worker_sess[d];
        delta_pool_t *pool = worker->delta_pool;
        if (pool) {
            rc = col_rel_pool_destroy_aliases_checked(pool, 0,
                    pool->slot_used);
            if (rc != 0)
                return rc;
            rc = col_rel_pool_destroy_roots_checked(pool, 0,
                    pool->slot_used);
            if (rc != 0)
                return rc;
        }
    }

    for (uint32_t d = 0; d < cohort->initialized_count; d++) {
        wl_col_session_t *worker = &cohort->worker_sess[d];
        uint64_t bytes = 0;
        if (worker->delta_pool)
            bytes += (uint64_t)worker->delta_pool->slot_cap
                * worker->delta_pool->slot_size
                + worker->delta_pool->arena_cap;
        if (worker->eval_arena)
            bytes += worker->eval_arena->capacity;
        if (bytes > 0)
            wl_mem_ledger_free(&parent->mem_ledger, WL_MEM_SUBSYS_ARENA,
                bytes);
        delta_pool_destroy(worker->delta_pool);
        worker->delta_pool = NULL;
        wl_arena_free(worker->eval_arena);
        worker->eval_arena = NULL;
    }
    cohort->resources_cleaned = true;
    return 0;
}

static void
col_kfusion_free_cohort(struct wl_columnar_kfusion_cohort *cohort)
{
    if (!cohort)
        return;
    if (cohort->merged)
        col_rel_destroy(cohort->merged);
    free(cohort->worker_sess);
    free(cohort->workers);
    free(cohort->results);
    free(cohort->live_indices);
    wl_columnar_memory_release(&cohort->scratch_reservation);
    free(cohort);
}

int
wl_columnar_kfusion_retry_pending(wl_col_session_t *sess)
{
    struct wl_columnar_kfusion_cohort *cohort;
    int rc;
    if (!sess)
        return EINVAL;
    cohort = sess->kfusion_pending_cohort;
    if (!cohort)
        return 0;
    if (cohort->dispatch_active)
        return EBUSY;
    rc = col_kfusion_cleanup_cohort(cohort);
    if (rc != 0)
        return rc;
    sess->kfusion_pending_cohort = NULL;
    col_kfusion_free_cohort(cohort);
    return 0;
}

/**
 * Worker thread function for K-fusion parallel evaluation.
 * Evaluates a single relation plan and collects result in context.
 */
static void
col_op_k_fusion_worker(void *ctx)
{
    col_op_k_fusion_worker_t *wc = (col_op_k_fusion_worker_t *)ctx;
    eval_stack_init(&wc->stack);
    wc->rc = col_eval_relation_plan(&wc->plan_data, &wc->stack, wc->sess);
}

/**
 * K-Fusion W=1 serial fast-path (Issue #549).
 *
 * Why this is safe:
 *   At num_workers <= 1 there is only one thread, so the per-worker session
 *   clone / arena / delta_pool machinery exists purely to isolate concurrent
 *   workers that no longer exist. Arrangements and mat_cache are stateful
 *   caches that the engine rebuilds on demand, so executing K branches
 *   sequentially against the parent sess cannot race and cannot change the
 *   computed result. Skipping the 375-per-iter arrangement clones is pure
 *   profit for workloads like DOOP at W=1.
 *
 * The parallel path deliberately drops any mat_cache entries its workers
 * produced during a dispatch (workers start with count=0, cleanup frees
 * from index 0). Mirror that invariant here by snapshotting sess->mat_cache
 * count on entry and trimming any branch-added entries on return, so outside
 * code sees identical K-Fusion side-effects regardless of worker count.
 */
static int
col_op_k_fusion_serial(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    wl_plan_op_k_fusion_t *meta = (wl_plan_op_k_fusion_t *)op->opaque_data;
    uint32_t k = meta->k;

    int retained_rc = wl_columnar_session_retry_retained_eval_entries(sess);
    if (retained_rc != 0)
        return retained_rc;

    uint64_t results_bytes = 0;
    if (!wl_columnar_memory_size_mul(k, sizeof(col_rel_t *),
        &results_bytes))
        return EOVERFLOW;
    wl_columnar_memory_reservation_t results_reservation;
    wl_columnar_memory_reservation_init(&results_reservation);
    if (sess->memory_governor) {
        wl_columnar_memory_admission_status_t admission
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(sess->memory_governor),
                results_bytes, &results_reservation);
        if (admission != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && admission != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
            return admission == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : ENOMEM;
    }

    uint64_t _phase_t0 = now_ns();
    col_rel_t **results = (col_rel_t **)calloc(k, sizeof(col_rel_t *));
    COL_SESSION(sess)->kfusion_alloc_ns += now_ns() - _phase_t0;
    if (!results) {
        wl_columnar_memory_release(&results_reservation);
        return ENOMEM;
    }

    /* Snapshot mat_cache so branch-added entries don't leak past K-Fusion
     * (parity with parallel path which discards all worker additions). */
    uint32_t mat_base = sess->mat_cache.count;

    /* Snapshot delta_pool->slot_used so any intermediate pool-allocated
     * relations produced by branch evaluation (VARIABLE FORCE_EMPTY,
     * JOIN/SEMIJOIN outputs, etc.) can have their heap-allocated fields
     * (name, columns, col_names) freed in cleanup.  The parallel path gets
     * this for free by owning a per-worker delta_pool that is fully
     * destroyed at teardown; the serial path shares the parent pool and
     * must sweep the range itself (#549 ASAN fix). */
    uint32_t pool_slot_base
        = sess->delta_pool ? sess->delta_pool->slot_used : 0;

    int rc = 0;
    uint32_t n_results = 0;

    /* Evaluate each K branch sequentially against the parent session. */
    _phase_t0 = now_ns();
    for (uint32_t d = 0; d < k; d++) {
        wl_plan_relation_t plan_data;
        memset(&plan_data, 0, sizeof(plan_data));
        plan_data.name = "<k_fusion_copy>";
        plan_data.delta_name = NULL;
        plan_data.ops = meta->k_ops[d];
        plan_data.op_count = meta->k_op_counts[d];

        /* Per-copy empty-delta skip (issue #85): if this copy references an
         * empty/absent delta on iteration > 0, skip — produces 0 rows. */
        if (has_empty_forced_delta(&plan_data, sess, sess->current_iteration))
            continue;

        eval_stack_t s;
        eval_stack_init(&s);
        int branch_rc = col_eval_relation_plan(&plan_data, &s, sess);
        if (branch_rc != 0) {
            rc = branch_rc;
            rc = col_kfusion_drain(&s, sess, rc);
            goto cleanup;
        }

        eval_entry_t e;
        rc = eval_stack_pop_relation(&s, &e);
        if (rc != 0) {
            rc = col_kfusion_drain(&s, sess, rc);
            goto cleanup;
        }

        /* If not owned, use the common shared-view publication helper.  This
         * preserves the worker-local identity and advances its destination
         * epoch instead of copying the coordinator's generation numbers. */
        if (!e.owned) {
            col_rel_t *copy = col_rel_pool_new_like(sess->delta_pool,
                    "<k_fusion_copy>", e.rel);
            if (!copy) {
                rc = ENOMEM;
                rc = col_kfusion_dispose_entry(&s, &e, rc);
                rc = col_kfusion_drain(&s, sess, rc);
                goto cleanup;
            }
            rc = col_rel_install_shared_view(copy, e.rel);
            if (rc != 0) {
                rc = col_rel_append_all(copy, e.rel, NULL);
                if (rc != 0) {
                    col_rel_destroy(copy);
                    rc = col_kfusion_drain(&s, sess, rc);
                    goto cleanup;
                }
            }
            results[n_results++] = copy;
        } else {
            results[n_results++] = e.rel;
        }
        rc = col_kfusion_drain(&s, sess, rc);
        if (rc != 0)
            goto cleanup;
    }
    COL_SESSION(sess)->kfusion_dispatch_ns += now_ns() - _phase_t0;

    /* Merge: inputs are sorted+deduped (CONSOLIDATE is each branch's last op). */
    _phase_t0 = now_ns();
    {
        col_rel_t *merged;
        if (n_results == 0) {
            /* All copies skipped: empty output with target relation schema. */
            uint32_t ncols = 0;
            if (op->relation_name) {
                col_rel_t *target = session_find_rel(sess, op->relation_name);
                if (target)
                    ncols = target->ncols;
            }
            merged = col_rel_new_auto("$kfusion_empty", ncols);
        } else {
            merged = col_rel_merge_k(results, n_results);
        }
        if (!merged) {
            rc = ENOMEM;
            goto cleanup;
        }
        rc = eval_stack_push(stack, merged, true);
        if (rc != 0)
            col_rel_destroy(merged);
    }
    COL_SESSION(sess)->kfusion_merge_ns += now_ns() - _phase_t0;

cleanup:
    _phase_t0 = now_ns();
    for (uint32_t d = 0; d < n_results; d++) {
        if (results[d]) {
            bool eligible = wl_columnar_deferred_relation_eligible(
                results[d]);
            int destroy_rc = col_rel_destroy_checked(results[d]);
            if (destroy_rc == EBUSY && eligible) {
                int defer_rc = wl_columnar_session_defer_relation(sess,
                        results[d]);
                if (defer_rc != 0) {
                    fprintf(stderr,
                        "wirelog: K-Fusion serial deferred result admission failed: %d\n",
                        defer_rc);
                    eval_entry_t retained = {
                        .rel = results[d],
                        .owned = true,
                        .kind = WL_COLUMNAR_EVAL_ENTRY_RELATION,
                    };
                    int retain_rc = wl_columnar_session_retain_eval_entry(
                        sess, &retained);
                    if (retain_rc == 0)
                        results[d] = NULL;
                    if (rc == 0)
                        rc = retain_rc != 0 ? retain_rc : defer_rc;
                }
            } else if (destroy_rc != 0) {
                eval_entry_t retained = {
                    .rel = results[d],
                    .owned = true,
                    .kind = WL_COLUMNAR_EVAL_ENTRY_RELATION,
                };
                int retain_rc = wl_columnar_session_retain_eval_entry(sess,
                        &retained);
                if (retain_rc == 0)
                    results[d] = NULL;
                if (rc == 0)
                    rc = retain_rc != 0 ? retain_rc : destroy_rc;
            }
        }
    }
    /* Trim mat_cache back to pre-dispatch baseline. Entries added by branches
     * are owned by the cache and must be freed the same way the parallel path
     * frees its worker caches. */
    col_mat_cache_release_pins(&sess->mat_cache);
    col_mat_cache_truncate(&sess->mat_cache, mat_base);
    /* Sweep any pool slots allocated during branch eval (#549 ASAN fix).
     * Slots whose relations were already col_rel_destroy'd upstream are
     * zeroed and this walk is a safe no-op for them (free(NULL) chains).
     * Slots still holding heap pointers (e.g. intermediate $empty_skip
     * names, JOIN $join_out names that the engine left dangling in the
     * parent pool) get their name/columns/col_names/etc. freed here.
     * After the sweep we reclaim slot_used so the pool can reuse the
     * range for the next K-Fusion dispatch. */
    if (sess->delta_pool) {
        delta_pool_t *dp = sess->delta_pool;
        uint32_t slot_limit = dp->slot_used;
        int alias_rc = col_rel_pool_destroy_aliases_checked(dp,
                pool_slot_base, slot_limit);
        int root_rc = col_rel_pool_destroy_roots_checked(dp,
                pool_slot_base, slot_limit);
        int cleanup_rc = alias_rc != 0 ? alias_rc : root_rc;
        if (cleanup_rc == 0)
            dp->slot_used = pool_slot_base;
        else if (rc == 0)
            rc = cleanup_rc;
    }
    free((void *)results);
    wl_columnar_memory_release(&results_reservation);
    COL_SESSION(sess)->kfusion_cleanup_ns += now_ns() - _phase_t0;
    return rc;
}
/**
 * K-Fusion operator: evaluate K copies of a relation plan via workqueue,
 * merge results with deduplication, and push result onto stack.
 *
 * Each of the K operator sequences in opaque_data is submitted as a
 * separate worker task to the workqueue. The K workers evaluate in
 * parallel (or sequentially on single-threaded systems).
 * Results are merged via col_rel_merge_k() after all workers complete.
 */
static int
col_op_k_fusion_dispatch(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess, bool adaptive_parallel, bool *parallel_executed)
{
    if (parallel_executed)
        *parallel_executed = false;
    if (!op->opaque_data)
        return EINVAL;

    wl_plan_op_k_fusion_t *meta = (wl_plan_op_k_fusion_t *)op->opaque_data;
    uint32_t k = meta->k;
    if (k == 0)
        return EINVAL;

    /* Issue #549: W=1 fast-path. Skip per-worker clone/arena/delta_pool
     * machinery when there's only one thread — pure overhead otherwise.
     * TDD workers already run under the distributed stratum workqueue; running
     * nested K-fusion workers inside them oversubscribes execution and divides
     * join_output_limit a second time. */
    if (sess->tdd_subpass_active || sess->coordinator
        || (sess->wq == NULL && sess->num_workers <= 1))
        return col_op_k_fusion_serial(op, stack, sess);

    /* Issue #560: Advance the compound-arena epoch frontier before
     * evaluating K-Fusion branch liveness so the parallel path preserves the
     * same coordinator epoch boundary ordering as worker dispatch. The
     * compound_arena is borrowed from the coordinator (Issue #579 / R-5);
     * only the coordinator may mutate it. */
    if (sess->coordinator == NULL
        && sess->compound_arena && sess->rotation_ops
        && sess->rotation_ops->gc_epoch_boundary) {
        sess->rotation_ops->gc_epoch_boundary(sess);
    }

    uint64_t _phase_t0 = now_ns();
    uint32_t *live_indices = (uint32_t *)malloc(k * sizeof(uint32_t));
    if (!live_indices)
        return ENOMEM;
    uint32_t live_count = 0;
    for (uint32_t d = 0; d < k; d++) {
        wl_plan_relation_t plan_data;
        memset(&plan_data, 0, sizeof(plan_data));
        plan_data.name = "<k_fusion_copy>";
        plan_data.delta_name = NULL;
        plan_data.ops = meta->k_ops[d];
        plan_data.op_count = meta->k_op_counts[d];
        if (!has_empty_forced_delta(&plan_data, sess, sess->current_iteration))
            live_indices[live_count++] = d;
    }
    if (live_count == 0) {
        COL_SESSION(sess)->kfusion_alloc_ns += now_ns() - _phase_t0;
        uint32_t ncols = 0;
        if (op->relation_name) {
            col_rel_t *target = session_find_rel(sess, op->relation_name);
            if (target)
                ncols = target->ncols;
        }
        col_rel_t *empty = col_rel_new_auto("$kfusion_empty", ncols);
        free(live_indices);
        if (!empty)
            return ENOMEM;
        int push_rc = eval_stack_push(stack, empty, true);
        if (push_rc != 0)
            col_rel_destroy(empty);
        return push_rc;
    }

    /* Use session-level workqueue created at col_session_create (issue #99).
     * If this invocation cannot or should not dispatch parallel branch work,
     * use the existing serial K-fusion evaluator before allocating worker
     * sessions. */
    uint32_t active_workers = live_count < sess->num_workers
        ? live_count : sess->num_workers;
    wl_work_queue_t *wq = NULL; /* NULL when serial or in workers */
    if (active_workers > 1
        && (live_count >= WL_KFUSION_MIN_PARALLEL_K || adaptive_parallel)) {
        int ensure_rc = wl_columnar_session_ensure_workqueue(sess,
                active_workers);
        if (ensure_rc != 0) {
            free(live_indices);
            return ensure_rc;
        }
        wq = sess->wq;
    }
    if (!wq || (live_count < WL_KFUSION_MIN_PARALLEL_K
        && !adaptive_parallel)) {
        free(live_indices);
        return col_op_k_fusion_serial(op, stack, sess);
    }
    if (parallel_executed)
        *parallel_executed = true;

    uint64_t scratch_bytes = 0;
    uint64_t bytes = 0;
    if (!wl_columnar_memory_size_mul(k, sizeof(uint32_t), &scratch_bytes)
        || !wl_columnar_memory_size_mul(live_count, sizeof(col_rel_t *),
        &bytes)
        || !wl_columnar_memory_size_add(scratch_bytes, bytes,
        &scratch_bytes)
        || !wl_columnar_memory_size_mul(live_count,
        sizeof(col_op_k_fusion_worker_t), &bytes)
        || !wl_columnar_memory_size_add(scratch_bytes, bytes,
        &scratch_bytes)
        || !wl_columnar_memory_size_mul(live_count,
        sizeof(wl_col_session_t), &bytes)
        || !wl_columnar_memory_size_add(scratch_bytes, bytes,
        &scratch_bytes)
        || !wl_columnar_memory_size_mul(1,
        sizeof(struct wl_columnar_kfusion_cohort), &bytes)
        || !wl_columnar_memory_size_add(scratch_bytes, bytes,
        &scratch_bytes)
        || !wl_columnar_memory_size_mul(live_count, sizeof(col_rel_t *),
        &bytes)
        || !wl_columnar_memory_size_add(scratch_bytes, bytes,
        &scratch_bytes)) {
        free(live_indices);
        return EOVERFLOW;
    }
    wl_columnar_memory_reservation_t scratch_reservation;
    wl_columnar_memory_reservation_init(&scratch_reservation);
    if (sess->memory_governor) {
        wl_columnar_memory_admission_status_t admission
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(sess->memory_governor),
                scratch_bytes, &scratch_reservation);
        if (admission != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && admission != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            free(live_indices);
            return admission == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : ENOMEM;
        }
    }

    col_rel_t **results = (col_rel_t **)calloc(live_count, sizeof(col_rel_t *));
    col_op_k_fusion_worker_t *workers = (col_op_k_fusion_worker_t *)calloc(
        live_count, sizeof(col_op_k_fusion_worker_t));
    /* Per-worker session wrappers: shallow copy of sess with isolated mutable
     * caches so concurrent branch evaluation does not race on cache state. */
    wl_col_session_t *worker_sess
        = (wl_col_session_t *)calloc(live_count, sizeof(wl_col_session_t));
    struct wl_columnar_kfusion_cohort *cohort
        = (struct wl_columnar_kfusion_cohort *)calloc(1, sizeof(*cohort));
    COL_SESSION(sess)->kfusion_alloc_ns += now_ns() - _phase_t0;
    if (!results || !workers || !worker_sess || !cohort) {
        free(live_indices);
        free((void *)results);
        free(workers);
        free(worker_sess);
        free(cohort);
        wl_columnar_memory_release(&scratch_reservation);
        return ENOMEM;
    }

    cohort->parent = COL_SESSION(sess);
    cohort->results = results;
    cohort->workers = workers;
    cohort->worker_sess = worker_sess;
    cohort->live_indices = live_indices;
    cohort->live_count = live_count;
    cohort->scratch_reservation = scratch_reservation;
    wl_columnar_memory_reservation_init(&scratch_reservation);
    cohort->dispatch_active = true;
    sess->kfusion_pending_cohort = cohort;

    int rc = 0;

    /* Issue #959: one shared counter for the whole fused branch set, rather
     * than giving each branch join_output_limit / live_count.
     *
     * A branch's join output is not 1/live_count of what the relation would
     * produce -- each branch materialises its own -- so dividing made the
     * effective capacity of a run *shrink* as the fan grew.  Measured on
     * DOOP: a 19-branch fusion cut the cap from 1,406,500,309 to 74,026,332,
     * against a single-threaded peak of 641,550,746 for the same workload.
     * Even a perfectly even split needs 80,193,843 per branch, so the run
     * could not fit its own ideal case; it failed at W=8 and completed at
     * W=1, which is exactly what #959 reported.
     *
     * col_join_output_limit_reached() already implements this shape: when
     * join_output_shared_count is set it accumulates atomically against
     * join_output_shared_limit instead of testing one relation's row count.
     * The TDD worker path (eval.c) has used it since #426; this is the same
     * treatment on the K-fusion branch path, so the cap bounds the aggregate
     * across branches -- which is what a global row budget means. */
    atomic_store_explicit(&cohort->shared_join_count, 0,
        memory_order_relaxed);

    /* Issue #196: Workers start with zeroed mat_cache (no shared entries).
     * All worker cache entries are worker-owned; cleanup frees all of them
     * starting from index 0, so no base_count snapshot is needed. */

    /* Initialise per-worker session wrappers and submit only live tasks in one
     * batch so W acts as a cap instead of forcing allocation for skipped
     * delta-copy branches. */
    _phase_t0 = now_ns();
    for (uint32_t d = 0; d < live_count; d++) {
        uint32_t branch_idx = live_indices[d];
        /* Shallow copy shares rels[], plan, etc. (read-only during K-fusion).
         * mat_cache is zeroed below (Issue #196): workers start fresh.
         * arrangement caches are zeroed below, so workers rebuild private
         * entries on demand instead of cloning coordinator state. */
        worker_sess[d] = *sess;
        worker_sess[d].base.owns_evaluation_control = false;
        worker_sess[d].wq = NULL; /* prevent nested K-fusion from workers */
        worker_sess[d].wq_workers = 0;
        worker_sess[d].kfusion_adaptive = NULL;
        worker_sess[d].num_workers = 1;
        worker_sess[d].callback_active_workers = active_workers;
        worker_sess[d].callback_parallel_execution = active_workers > 1;
        worker_sess[d].tdd_workers = NULL;
        worker_sess[d].tdd_workers_cap = 0;
        worker_sess[d].tdd_workers_count = 0;
        /* The shallow branch wrapper does not own the parent worker's
         * persistent source-reader registry. */
        worker_sess[d].source_leases = NULL;
        worker_sess[d].deferred_relations = NULL;
        worker_sess[d].deferred_relation_count = 0;
        /* #1765: the wrapper is a shallow copy, so without this the branch
         * shares the parent's retained-entry head pointer.  A branch that
         * retains then prepends in front of the parent's own list, and any
         * later splice of the two links a shared node to itself -- a cyclic
         * list whose next walk never terminates.  Starting empty keeps the
         * branch's retentions its own.
         *
         * Entries the branch does retain are still lost when the wrapper is
         * released with free() below; that leak is deliberately left alone.
         * The relations behind them come from this branch's delta_pool and
         * eval_arena, which are destroyed a few lines after any hand-off
         * could happen, so exporting them to the parent would replace a leak
         * with a use-after-free.  K-Fusion is slated for removal, so the
         * orphan is left to go with it rather than carrying a transfer path
         * for a doomed component. */
        worker_sess[d].retained_eval_entries = NULL;
        worker_sess[d].retained_eval_entry_count = 0;
        worker_sess[d].kfusion_pending_cohort = NULL;
        /* The shallow session copy must not retain coordinator reclaimer
         * callbacks after the worker cache is replaced below. */
        memset(worker_sess[d].mem_ledger.reclaimers, 0,
            sizeof(worker_sess[d].mem_ledger.reclaimers));
        worker_sess[d].mem_ledger.next_reclaimer_handle = 0;
        if (worker_sess[d].join_output_limit > 0 && live_count > 1) {
            /* Issue #959: share one budget instead of splitting it. */
            worker_sess[d].join_output_shared_count
                = &cohort->shared_join_count;
            worker_sess[d].join_output_shared_limit =
                worker_sess[d].join_output_limit;
        }
        /* NULL out owned resources before allocation so cleanup_wq is safe
         * even if we abort early (e.g. clone failure).  Each owned pointer
         * is replaced below; the parent session retains its own copies. */
        worker_sess[d].eval_arena = NULL;
        worker_sess[d].delta_pool = NULL;
        worker_sess[d].arr_entries = NULL;
        worker_sess[d].arr_count = 0;
        worker_sess[d].arr_cap = 0;
        worker_sess[d].arr_clock = sess->arr_clock;
        worker_sess[d].arr_total_bytes = 0;
        worker_sess[d].arr_cache_limit_bytes = sess->arr_cache_limit_bytes;
        worker_sess[d].diff_arr_entries = NULL;
        worker_sess[d].diff_arr_count = 0;
        worker_sess[d].diff_arr_cap = 0;
        worker_sess[d].darr_entries = NULL;
        worker_sess[d].darr_count = 0;
        worker_sess[d].darr_cap = 0;
        /* Issue #433: workers start with empty filt_arr (isolation safety).
         * Workers rebuild filt_arr from filt_cache if needed per dispatch. */
        worker_sess[d].filt_arr_entries = NULL;
        worker_sess[d].filt_arr_count = 0;
        worker_sess[d].filt_arr_cap = 0;
        worker_sess[d].filt_cache = NULL;
        worker_sess[d].filt_cache_count = 0;
        worker_sess[d].filt_cache_cap = 0;
        worker_sess[d].filt_cache_active_pins = 0; /* Issue #1435 */
        /* Issue #196: Workers start with empty mat_cache.  Divergent rule
         * copies have ~0% cache hit rate, so inheriting parent entries
         * wastes memory without benefit. */
        memset(&worker_sess[d].mat_cache, 0, sizeof(col_mat_cache_t));
        /* Issue #196: Per-worker arena isolation (arena.h contract: NOT
         * thread-safe, each worker must own its arena). */
        {
            size_t parent_cap
                = sess->eval_arena ? sess->eval_arena->capacity : 0;
            size_t worker_cap = parent_cap / live_count;
            if (worker_cap < 8 * 1024 * 1024)
                worker_cap = 8 * 1024 * 1024; /* 8MB minimum */
            worker_sess[d].eval_arena = wl_arena_create_managed(worker_cap,
                    wl_columnar_memory_governor_ref_get(sess->memory_governor));
            /* NULL arena is handled gracefully: operators check before use */
            /* Issue #1380: branch sessions are struct copies whose embedded
             * ledger is discarded at teardown, so charge the parent. */
            if (worker_sess[d].eval_arena)
                wl_mem_ledger_alloc(&sess->mem_ledger, WL_MEM_SUBSYS_ARENA,
                    worker_sess[d].eval_arena->capacity);
        }
        /* Issue #196: Scale per-worker delta_pool inversely with active
         * branch count to keep aggregate memory ~constant. */
        {
            size_t pool_arena = 32 * 1024 * 1024 / live_count;
            if (pool_arena < 4 * 1024 * 1024)
                pool_arena = 4 * 1024 * 1024; /* 4MB minimum */
            uint32_t pool_slots = 128 / live_count;
            if (pool_slots < 16)
                pool_slots = 16;
            wl_columnar_memory_admission_status_t pool_status =
                WL_COLUMNAR_MEMORY_ADMISSION_OK;
            worker_sess[d].delta_pool
                = delta_pool_create_managed_status(pool_slots,
                    sizeof(col_rel_t), pool_arena,
                    wl_columnar_memory_governor_ref_get(
                        sess->memory_governor), &pool_status);
            if (!worker_sess[d].delta_pool
                && pool_status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                worker_sess[d].memory_budget_denied = true;
            if (worker_sess[d].delta_pool) {
                const delta_pool_t *dp = worker_sess[d].delta_pool;
                wl_mem_ledger_alloc(&sess->mem_ledger, WL_MEM_SUBSYS_ARENA,
                    (uint64_t)dp->slot_cap * dp->slot_size + dp->arena_cap);
            }
        }
        /* Acquire the borrowed arena only after coordinator-owned fields have
        * been nulled, so failed acquisition can use common cleanup safely. */
        memset(&worker_sess[d].compound_borrow, 0,
            sizeof(worker_sess[d].compound_borrow));
        eval_stack_init(&workers[d].stack);
        workers[d].initialized = true;
        cohort->initialized_count = d + 1;
        if (worker_sess[d].compound_arena
            && !wl_compound_arena_borrow(worker_sess[d].compound_arena,
            &worker_sess[d].compound_borrow)) {
            rc = EBUSY;
            goto cleanup_wq;
        }

        workers[d].plan_data.name = "<k_fusion_copy>";
        workers[d].plan_data.ops = meta->k_ops[branch_idx];
        workers[d].plan_data.op_count = meta->k_op_counts[branch_idx];
        workers[d].sess = &worker_sess[d];
        workers[d].rc = 0;

        if (wq) {
            /* Parallel path: submit to session workqueue (issue #99) */
            int submit_rc;
#ifdef WL_SESSION_TEST_HOOKS
            submit_rc = wl_columnar_kfusion_test_submit
                ? wl_columnar_kfusion_test_submit(wq,
                    col_op_k_fusion_worker, &workers[d])
                : wl_workqueue_submit(wq, col_op_k_fusion_worker,
                    &workers[d]);
#else
            submit_rc = wl_workqueue_submit(wq, col_op_k_fusion_worker,
                    &workers[d]);
#endif
            if (submit_rc != 0) {
                rc = ENOMEM;
                int barrier_rc = wl_workqueue_drain(wq);
                if (barrier_rc == 0)
                    cohort->barrier_complete = true;
                else
                    rc = barrier_rc;
                goto cleanup_wq;
            }
            workers[d].submitted = true;
            cohort->submitted_count++;
        } else {
            /* Sequential fallback: execute directly (num_workers=1) */
            col_op_k_fusion_worker(&workers[d]);
        }
    }

    /* Barrier: wait for all parallel workers to complete.
     * Skipped when wq is NULL (sequential path already finished). */
    if (wq && wl_workqueue_wait_all(wq) != 0) {
        rc = wl_workqueue_drain(wq);
        if (rc == 0)
            cohort->barrier_complete = true;
        else
            goto cleanup_wq;
        rc = EIO;
        goto cleanup_wq;
    }
    cohort->barrier_complete = true;
    COL_SESSION(sess)->kfusion_dispatch_ns += now_ns() - _phase_t0;

    /* Issue #177: Merge worker profile counters back to session.
     * K-fusion workers accumulate profiling stats (join_calls, join_unary,
     * etc.) during parallel evaluation. Aggregate these counters to the
     * session profile for comprehensive profiling. */
#ifdef WL_PROFILE
    {
        wl_profile_t base_profile = sess->profile;
        for (uint32_t d = 0; d < live_count; d++) {
            /* Merge counters: sum increments from baseline */
            sess->profile.join_calls
                += worker_sess[d].profile.join_calls - base_profile.join_calls;
            sess->profile.join_unary
                += worker_sess[d].profile.join_unary - base_profile.join_unary;
            sess->profile.join_binary += worker_sess[d].profile.join_binary
                - base_profile.join_binary;
            sess->profile.seminaive_ops += worker_sess[d].profile.seminaive_ops
                - base_profile.seminaive_ops;
        }
    }
#endif

    /* Collect results from each worker's eval_stack */
    _phase_t0 = now_ns();
    for (uint32_t d = 0; d < live_count; d++) {
        if (workers[d].rc != 0) {
            if (worker_sess[d].memory_budget_denied)
                sess->memory_budget_denied = true;
            if (worker_sess[d].extension_expr_status != 0)
                sess->extension_expr_status
                    = worker_sess[d].extension_expr_status;
            rc = workers[d].rc;
            goto cleanup_results;
        }

        eval_entry_t e;
        rc = eval_stack_pop_relation(&workers[d].stack, &e);
        if (rc != 0) {
            rc = col_kfusion_drain(&workers[d].stack, &worker_sess[d], rc);
            goto cleanup_results;
        }

        /* If not owned, publish a worker-local shared view through the common
         * helper so identity and generation epochs cannot be aliased. */
        if (!e.owned) {
            col_rel_t *copy = col_rel_pool_new_like(worker_sess[d].delta_pool,
                    "<k_fusion_copy>", e.rel);
            if (!copy) {
                rc = ENOMEM;
                rc = col_kfusion_dispose_entry(&workers[d].stack, &e, rc);
                rc = col_kfusion_drain(&workers[d].stack, &worker_sess[d], rc);
                goto cleanup_results;
            }
            rc = col_rel_install_shared_view(copy, e.rel);
            if (rc != 0) {
                rc = col_rel_append_all(copy, e.rel, NULL);
                if (rc != 0) {
                    col_rel_destroy(copy);
                    rc = col_kfusion_drain(&workers[d].stack, &worker_sess[d],
                            rc);
                    goto cleanup_results;
                }
            }
            results[d] = copy;
        } else {
            results[d] = e.rel;
        }
        rc = col_kfusion_drain(&workers[d].stack, &worker_sess[d], rc);
        if (rc != 0)
            goto cleanup_results;
    }

    /* Merge live branch results with deduplication.
     * Workers ran WL_PLAN_OP_CONSOLIDATE as the last plan op, so each
     * result is already sorted+deduped — no qsort needed here. */
    {
        /* Compact non-NULL results. Use the
         * existing results array as backing — we build compact in-place. */
        col_rel_t **compact
            = (col_rel_t **)malloc(live_count * sizeof(col_rel_t *));
        if (!compact) {
            rc = ENOMEM;
            goto cleanup_results;
        }
        uint32_t n_results = 0;
        for (uint32_t d = 0; d < live_count; d++) {
            if (results[d])
                compact[n_results++] = results[d];
        }

        col_rel_t *merged;
        if (n_results == 0) {
            /* Defensive fallback: produce empty output with the target
             * relation schema if no worker produced a relation. */
            uint32_t ncols = 0;
            if (op->relation_name) {
                col_rel_t *target = session_find_rel(sess, op->relation_name);
                if (target)
                    ncols = target->ncols;
            }
            merged = col_rel_new_auto("$kfusion_empty", ncols);
        } else {
            merged = col_rel_merge_k(compact, n_results);
        }
        free((void *)compact);
        if (!merged) {
            rc = ENOMEM;
            goto cleanup_results;
        }
        cohort->merged = merged;
        rc = 0;
    }
    COL_SESSION(sess)->kfusion_merge_ns += now_ns() - _phase_t0;

cleanup_results:
cleanup_wq:
    _phase_t0 = now_ns();
    cohort->dispatch_active = false;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_kfusion_test_before_cleanup
        && cohort->initialized_count > 0)
        wl_columnar_kfusion_test_before_cleanup(&cohort->worker_sess[0],
            &cohort->workers[0].stack, cohort->results, cohort->live_count);
#endif
    cohort->operation_rc = rc;
    {
        int cleanup_rc = col_kfusion_cleanup_cohort(cohort);
        if (cleanup_rc != 0) {
            /* The cohort itself is the durable ownership node.  No allocation
             * or list-node construction is needed after refusal. */
            sess->kfusion_pending_cohort = cohort;
            COL_SESSION(sess)->kfusion_cleanup_ns += now_ns() - _phase_t0;
            return cleanup_rc;
        }
    }
    if (rc == 0 && cohort->merged) {
        rc = eval_stack_push(stack, cohort->merged, true);
        if (rc == 0)
            cohort->merged = NULL;
    }
    sess->kfusion_pending_cohort = NULL;
    col_kfusion_free_cohort(cohort);
    COL_SESSION(sess)->kfusion_cleanup_ns += now_ns() - _phase_t0;
    return rc;
}

/*
 * Adaptive low-K entry point.  The existing fixed K>=4 path is deliberately
 * untouched.  K=2/K=3 starts serial, then periodically samples the complete
 * parallel invocation; the policy only changes dispatch and never participates
 * in result construction or worker cleanup.
 */
int
col_op_k_fusion(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    if (!op || !op->opaque_data || !stack || !sess)
        return EINVAL;

    int pending_rc = wl_columnar_kfusion_retry_pending(COL_SESSION(sess));
    if (pending_rc != 0)
        return pending_rc;

    wl_plan_op_k_fusion_t *meta = (wl_plan_op_k_fusion_t *)op->opaque_data;
    uint32_t k = meta->k;
    if (k == 0)
        return EINVAL;
    if (k >= WL_KFUSION_MIN_PARALLEL_K)
        return col_op_k_fusion_dispatch(op, stack, sess, false, NULL);

    /* Preserve the established serial safety cases and avoid allocating
     * policy state in worker sessions. */
    if (sess->tdd_subpass_active || sess->coordinator
        || sess->num_workers <= 1)
        return col_op_k_fusion_serial(op, stack, sess);

    if (!sess->kfusion_adaptive) {
        sess->kfusion_adaptive = wl_kfusion_adaptive_create();
        if (!sess->kfusion_adaptive)
            return col_op_k_fusion_serial(op, stack, sess);
    }

    uint64_t size_class = 0;
    if (op->relation_name) {
        col_rel_t *target = session_find_rel(sess, op->relation_name);
        if (target)
            size_class = target->nrows;
    }
    wl_kfusion_adaptive_decision_t decision = wl_kfusion_adaptive_begin(
        sess->kfusion_adaptive, meta, k, sess->num_workers, size_class);
    uint64_t started = now_ns();
    bool parallel_executed = false;
    int rc = decision == WL_KFUSION_ADAPTIVE_DECISION_PARALLEL
        ? col_op_k_fusion_dispatch(op, stack, sess, true,
            &parallel_executed)
        : col_op_k_fusion_serial(op, stack, sess);
    uint64_t elapsed = now_ns() - started;
    if (decision == WL_KFUSION_ADAPTIVE_DECISION_PARALLEL
        && !parallel_executed)
        decision = WL_KFUSION_ADAPTIVE_DECISION_SERIAL;
    wl_kfusion_adaptive_observe(sess->kfusion_adaptive, meta, k,
        sess->num_workers, size_class, decision, elapsed, rc == 0);
    return rc;
}
