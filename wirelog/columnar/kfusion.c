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

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
 * col_arr_entry_clone - Deep-copy one arrangement registry entry (#260).
 *
 * All owned memory (rel_name, key_cols, ht_head, ht_next) is freshly
 * allocated.  arr.key_cols is set to the new entry's key_cols (shared alias,
 * not a separate allocation — matches arrangement.c creation convention).
 * Returns 0 on success; dst is memset-zeroed before returning on failure.
 */
static int
col_arr_entry_clone(const col_arr_entry_t *src, col_arr_entry_t *dst)
{
    size_t head_bytes = 0;
    size_t next_bytes = 0;

    memset(dst, 0, sizeof(*dst));

    dst->rel_name = wl_strdup(src->rel_name);
    if (!dst->rel_name)
        return ENOMEM;

    if (src->key_count > 0) {
        dst->key_cols = (uint32_t *)malloc(src->key_count * sizeof(uint32_t));
        if (!dst->key_cols) {
            free(dst->rel_name);
            memset(dst, 0, sizeof(*dst));
            return ENOMEM;
        }
        memcpy(dst->key_cols, src->key_cols, src->key_count * sizeof(uint32_t));
    }
    dst->key_count = src->key_count;

    /* arr.key_cols is a shared alias of entry.key_cols (not separately owned).
    * Mirrors the convention in col_session_get_arrangement (arrangement.c). */
    dst->arr.key_cols = dst->key_cols;
    dst->arr.key_count = src->arr.key_count;
    dst->arr.indexed_rows = src->arr.indexed_rows;
    dst->arr.content_hash = src->arr.content_hash;
    dst->arr.nbuckets = src->arr.nbuckets;
    dst->arr.ht_cap = src->arr.ht_cap;
    dst->arr.generation = src->arr.generation;
    /* Issue #216: copy LRU metadata so worker clones inherit access state. */
    dst->lru_clock = src->lru_clock;
    dst->mem_bytes = src->mem_bytes;

    if (src->arr.nbuckets > 0 && src->arr.ht_head) {
        head_bytes = (size_t)src->arr.nbuckets * sizeof(uint64_t);
        if (head_bytes / sizeof(uint64_t) != src->arr.nbuckets) {
            free(dst->key_cols);
            free(dst->rel_name);
            memset(dst, 0, sizeof(*dst));
            return ENOMEM;
        }
        dst->arr.ht_head
            = (uint64_t *)malloc(head_bytes);
        if (!dst->arr.ht_head) {
            free(dst->key_cols);
            free(dst->rel_name);
            memset(dst, 0, sizeof(*dst));
            return ENOMEM;
        }
        memcpy(dst->arr.ht_head, src->arr.ht_head,
            head_bytes);
    }

    if (src->arr.ht_cap > 0 && src->arr.ht_next) {
        next_bytes = (size_t)src->arr.ht_cap * sizeof(uint32_t);
        if (next_bytes / sizeof(uint32_t) != src->arr.ht_cap) {
            free(dst->arr.ht_head);
            free(dst->key_cols);
            free(dst->rel_name);
            memset(dst, 0, sizeof(*dst));
            return ENOMEM;
        }
        dst->arr.ht_next
            = (uint32_t *)malloc(next_bytes);
        if (!dst->arr.ht_next) {
            free(dst->arr.ht_head);
            free(dst->key_cols);
            free(dst->rel_name);
            memset(dst, 0, sizeof(*dst));
            return ENOMEM;
        }
        memcpy(dst->arr.ht_next, src->arr.ht_next,
            next_bytes);
    }

    return 0;
}

/**
 * col_arr_entries_clone - Deep-copy an arrangement registry array (#260).
 *
 * Creates an independent copy of `count` entries for a K-fusion worker.
 * On success, *out_entries owns all allocations and *out_cap equals count.
 * On failure, *out_entries is NULL.
 */
int
col_arr_entries_clone(const col_arr_entry_t *src, uint32_t count,
    col_arr_entry_t **out_entries, uint32_t *out_cap)
{
    *out_entries = NULL;
    *out_cap = 0;

    if (count == 0)
        return 0;

    col_arr_entry_t *cloned
        = (col_arr_entry_t *)calloc(count, sizeof(col_arr_entry_t));
    if (!cloned)
        return ENOMEM;

    for (uint32_t i = 0; i < count; i++) {
        int clone_rc = col_arr_entry_clone(&src[i], &cloned[i]);
        if (clone_rc != 0) {
            for (uint32_t j = 0; j < i; j++) {
                free(cloned[j].rel_name);
                free(cloned[j].key_cols);
                arr_free_contents(&cloned[j].arr);
            }
            free(cloned);
            return clone_rc;
        }
    }

    *out_entries = cloned;
    *out_cap = count;
    return 0;
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
} col_op_k_fusion_worker_t;

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

    uint64_t _phase_t0 = now_ns();
    col_rel_t **results = (col_rel_t **)calloc(k, sizeof(col_rel_t *));
    COL_SESSION(sess)->kfusion_alloc_ns += now_ns() - _phase_t0;
    if (!results)
        return ENOMEM;

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
            eval_stack_drain(&s);
            goto cleanup;
        }

        eval_entry_t e = eval_stack_pop(&s);
        if (!e.rel) {
            rc = EINVAL;
            eval_stack_drain(&s);
            goto cleanup;
        }

        /* If not owned, share columns zero-copy (parity with parallel path). */
        if (!e.owned) {
            col_rel_t *copy = col_rel_pool_new_like(sess->delta_pool,
                    "<k_fusion_copy>", e.rel);
            if (!copy) {
                rc = ENOMEM;
                eval_stack_drain(&s);
                goto cleanup;
            }
            copy->col_shared = (bool *)calloc(e.rel->ncols, sizeof(bool));
            if (copy->col_shared) {
                for (uint32_t c = 0; c < e.rel->ncols; c++) {
                    free(copy->columns[c]); /* free pool-allocated column */
                    copy->columns[c] = e.rel->columns[c];
                    copy->col_shared[c] = true;
                }
            } else {
                /* Fallback: deep copy on alloc failure */
                for (uint32_t c = 0; c < e.rel->ncols; c++)
                    memcpy(copy->columns[c], e.rel->columns[c],
                        (size_t)e.rel->nrows * sizeof(int64_t));
            }
            copy->nrows = e.rel->nrows;
            results[n_results++] = copy;
        } else {
            results[n_results++] = e.rel;
        }
        eval_stack_drain(&s);
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
        if (results[d])
            col_rel_destroy(results[d]);
    }
    /* Trim mat_cache back to pre-dispatch baseline. Entries added by branches
     * are owned by the cache and must be freed the same way the parallel path
     * frees its worker caches. */
    {
        col_mat_cache_t *mc = &sess->mat_cache;
        for (uint32_t i = mat_base; i < mc->count; i++) {
            col_rel_destroy(mc->entries[i].result);
            mc->total_bytes -= mc->entries[i].mem_bytes;
        }
        mc->count = mat_base;
    }
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
        for (uint32_t s = pool_slot_base; s < dp->slot_used; s++) {
            col_rel_t *pr = (col_rel_t *)(dp->slab
                + (size_t)s * dp->slot_size);
            col_rel_free_contents(pr);
        }
        dp->slot_used = pool_slot_base;
    }
    free((void *)results);
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

    col_rel_t **results = (col_rel_t **)calloc(live_count, sizeof(col_rel_t *));
    col_op_k_fusion_worker_t *workers = (col_op_k_fusion_worker_t *)calloc(
        live_count, sizeof(col_op_k_fusion_worker_t));
    /* Per-worker session wrappers: shallow copy of sess with isolated mutable
     * caches so concurrent branch evaluation does not race on cache state. */
    wl_col_session_t *worker_sess
        = (wl_col_session_t *)calloc(live_count, sizeof(wl_col_session_t));
    COL_SESSION(sess)->kfusion_alloc_ns += now_ns() - _phase_t0;
    if (!results || !workers || !worker_sess) {
        free(live_indices);
        free((void *)results);
        free(workers);
        free(worker_sess);
        return ENOMEM;
    }

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
    atomic_uint_fast64_t shared_join_count;
    atomic_store_explicit(&shared_join_count, 0, memory_order_relaxed);

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
        worker_sess[d].wq = NULL; /* prevent nested K-fusion from workers */
        worker_sess[d].wq_workers = 0;
        worker_sess[d].kfusion_adaptive = NULL;
        worker_sess[d].num_workers = 1;
        worker_sess[d].tdd_workers = NULL;
        worker_sess[d].tdd_workers_cap = 0;
        worker_sess[d].tdd_workers_count = 0;
        if (worker_sess[d].join_output_limit > 0 && live_count > 1) {
            /* Issue #959: share one budget instead of splitting it. */
            worker_sess[d].join_output_shared_count = &shared_join_count;
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
            worker_sess[d].eval_arena = wl_arena_create(worker_cap);
            /* NULL arena is handled gracefully: operators check before use */
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
            worker_sess[d].delta_pool
                = delta_pool_create(pool_slots, sizeof(col_rel_t), pool_arena);
        }

        workers[d].plan_data.name = "<k_fusion_copy>";
        workers[d].plan_data.ops = meta->k_ops[branch_idx];
        workers[d].plan_data.op_count = meta->k_op_counts[branch_idx];
        workers[d].sess = &worker_sess[d];
        workers[d].rc = 0;

        if (wq) {
            /* Parallel path: submit to session workqueue (issue #99) */
            if (wl_workqueue_submit(wq, col_op_k_fusion_worker, &workers[d])
                != 0) {
                rc = ENOMEM;
                wl_workqueue_drain(wq);
                goto cleanup_wq;
            }
        } else {
            /* Sequential fallback: execute directly (num_workers=1) */
            col_op_k_fusion_worker(&workers[d]);
        }
    }

    /* Barrier: wait for all parallel workers to complete.
     * Skipped when wq is NULL (sequential path already finished). */
    if (wq && wl_workqueue_wait_all(wq) != 0) {
        rc = -1;
        goto cleanup_wq;
    }
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
            rc = workers[d].rc;
            goto cleanup_results;
        }

        eval_entry_t e = eval_stack_pop(&workers[d].stack);
        if (!e.rel) {
            rc = EINVAL;
            eval_stack_drain(&workers[d].stack);
            goto cleanup_results;
        }

        /* If not owned, share columns zero-copy (6B optimization).
         * The source relation outlives the merge, so borrowing is safe. */
        if (!e.owned) {
            col_rel_t *copy = col_rel_pool_new_like(worker_sess[d].delta_pool,
                    "<k_fusion_copy>", e.rel);
            if (!copy) {
                rc = ENOMEM;
                eval_stack_drain(&workers[d].stack);
                goto cleanup_results;
            }
            copy->col_shared = (bool *)calloc(e.rel->ncols, sizeof(bool));
            if (copy->col_shared) {
                for (uint32_t c = 0; c < e.rel->ncols; c++) {
                    free(copy->columns[c]); /* free pool-allocated column */
                    copy->columns[c] = e.rel->columns[c];
                    copy->col_shared[c] = true;
                }
            } else {
                /* Fallback: deep copy on alloc failure */
                for (uint32_t c = 0; c < e.rel->ncols; c++)
                    memcpy(copy->columns[c], e.rel->columns[c],
                        (size_t)e.rel->nrows * sizeof(int64_t));
            }
            copy->nrows = e.rel->nrows;
            results[d] = copy;
        } else {
            results[d] = e.rel;
        }
        eval_stack_drain(&workers[d].stack);
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
        rc = eval_stack_push(stack, merged, true);
        if (rc != 0)
            col_rel_destroy(merged);
    }
    COL_SESSION(sess)->kfusion_merge_ns += now_ns() - _phase_t0;

cleanup_results:
    _phase_t0 = now_ns();
    for (uint32_t d = 0; d < live_count; d++) {
        if (results[d])
            col_rel_destroy(results[d]);
        eval_stack_drain(&workers[d].stack);
    }

cleanup_wq:
    /* On early-exit paths (submit failure, wait failure) _phase_t0 may hold
    * a stale dispatch value; reset it here so cleanup timing is correct. */
    _phase_t0 = now_ns();
    /* wq is session-owned and reused across iterations — do not destroy here.
     * Workers start with empty mat_cache (Issue #196), so all entries are
     * worker-owned and freed from index 0.
     * Free each worker's private arrangement caches (arr_* and darr_*).
     * Lock-free design: no synchronization needed because each worker owns
     * its isolated cache — no races at cleanup time. */
    for (uint32_t d = 0; d < live_count; d++) {
        eval_stack_drain(&workers[d].stack);
        col_mat_cache_t *wc = &worker_sess[d].mat_cache;
        /* Issue #196: worker mat_cache starts empty (zeroed above), so ALL
         * entries were created by this worker — free from index 0. */
        for (uint32_t i = 0; i < wc->count; i++)
            col_rel_destroy(wc->entries[i].result);
        /* Issue #216: merge worker lru_clocks back into coordinator so
         * arrangements accessed by any worker are counted as recently used.
         * Worker entries were cloned in the same order as coordinator entries,
         * so index-matched comparison is valid. */
        {
            wl_col_session_t *cs = COL_SESSION(sess);
            uint32_t shared = worker_sess[d].arr_count < cs->arr_count
                ? worker_sess[d].arr_count
                : cs->arr_count;
            for (uint32_t i = 0; i < shared; i++) {
                col_arr_entry_t *wk = &worker_sess[d].arr_entries[i];
                col_arr_entry_t *co = &cs->arr_entries[i];
                if (wk->lru_clock > co->lru_clock)
                    co->lru_clock = wk->lru_clock;
            }
            /* Advance coordinator clock once outside the loop. */
            if (worker_sess[d].arr_clock > cs->arr_clock)
                cs->arr_clock = worker_sess[d].arr_clock;
        }
        /* Free worker's private full-arrangement cache (arr_*). */
        for (uint32_t i = 0; i < worker_sess[d].arr_count; i++) {
            col_arr_entry_t *e = &worker_sess[d].arr_entries[i];
            free(e->rel_name);
            free(e->key_cols);
            arr_free_contents(&e->arr);
        }
        free(worker_sess[d].arr_entries);
        /* Free worker's private delta-arrangement cache (darr_*). */
        col_session_free_delta_arrangements(&worker_sess[d]);
        /* Free worker's private diff-arrangement cache (diff_arr_*). */
        col_session_free_diff_arrangements(&worker_sess[d]);
        /* Free worker's private filtered arrangement cache (filt_arr_*). */
        col_session_free_filt_arrangements(&worker_sess[d]);
        for (uint32_t i = 0; i < worker_sess[d].filt_cache_count; i++) {
            free(worker_sess[d].filt_cache[i].rel_name);
            free(worker_sess[d].filt_cache[i].filter_data);
            if (worker_sess[d].filt_cache[i].filtered)
                col_rel_destroy(worker_sess[d].filt_cache[i].filtered);
        }
        free(worker_sess[d].filt_cache);
        /* Free contents of pool-allocated relations before bulk destroy.
         * delta_pool_destroy frees the slab/arena but skips individually
         * malloc'd members (name, columns, col_names) -- leaks under ASAN.
         * col_rel_free_contents zeroes each slot, so already-destroyed
         * relations (via mat_cache or results cleanup) are safe no-ops. */
        {
            delta_pool_t *dp = worker_sess[d].delta_pool;
            if (dp) {
                for (uint32_t s = 0; s < dp->slot_used; s++) {
                    col_rel_t *pr = (col_rel_t *)(dp->slab
                        + (size_t)s * dp->slot_size);
                    col_rel_free_contents(pr);
                }
            }
        }
        delta_pool_destroy(worker_sess[d].delta_pool);
        wl_arena_free(worker_sess[d].eval_arena);
    }
    free(worker_sess);
    free((void *)results);
    free(workers);
    free(live_indices);
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
