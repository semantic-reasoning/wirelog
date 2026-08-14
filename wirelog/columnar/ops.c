/*
 * columnar/ops.c - wirelog Columnar Backend Operator Implementations
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * All col_op_* operator functions and supporting helpers extracted from
 * backend/columnar_nanoarrow.c for modular compilation.
 */

#define _GNU_SOURCE

/* Minimum K to use parallel K-fusion dispatch.  For K below this threshold,
 * thread-dispatch + per-worker setup overhead (arena alloc, delta pool,
 * synchronization) exceeds the parallelisation benefit.
 * Measured: DDISASM K=3 is 14% slower with 8-worker parallel than sequential.
 * K < WL_KFUSION_MIN_PARALLEL_K falls back to sequential execution. */
#define WL_KFUSION_MIN_PARALLEL_K 4

/* Best-effort match-pair cache for parallel keyed diff joins.  The cache is
 * scratch memory outside the final output relation, so keep it bounded and
 * fall back to the old fill traversal when a worker reaches the cap. */
#define WL_JOIN_PAIR_CACHE_MAX_BYTES (256ULL * 1024ULL * 1024ULL)
#define WL_JOIN_PAIR_CACHE_MIN_LEFT_ROWS 100000u

#if defined(_MSC_VER)
#define WL_OPS_ALWAYS_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define WL_OPS_ALWAYS_INLINE inline __attribute__((always_inline))
#else
#define WL_OPS_ALWAYS_INLINE inline
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

/* ======================================================================== */
/* Postfix Filter Expression Evaluator                                       */
/* ======================================================================== */

/* ======================================================================== */
/* Operator Implementations                                                  */
/* ======================================================================== */

/* Cross-module function declarations are in columnar/internal.h */

/* --- VARIABLE ------------------------------------------------------------ */

int
col_op_variable(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    if (!op->relation_name)
        return ENOENT;
    col_rel_t *full_rel = session_find_rel(sess, op->relation_name);
    if (!full_rel)
        return ENOENT;

    /* Delta mode controls whether we use delta or full relation.
     * FORCE_FULL:  always use the full relation (no delta substitution).
     * FORCE_DELTA: always use the delta relation; if no delta exists or
     *              it is empty, push an empty relation so that the rule
     *              copy produces no output (correct semi-naive behavior).
     * AUTO:        heuristic -- prefer delta only when it is a genuine
     *              strict subset of the full relation (nrows < full).
     *
     * Issue #158 extension: When retraction_seeded and iteration == 0,
     * look for $r$<name> (retraction delta) instead of $d$<name> */
    char dname[256];
    col_rel_t *delta = NULL;

    if (sess->retraction_seeded && sess->current_iteration == 0
        && !sess->retraction_right_pass) {
        /* Retraction mode (left pass): look for $r$<name> retraction delta.
         * Issue #472: Skip during right pass — VARIABLE loads full relation
         * so JOIN/SEMIJOIN can use $r$ on the right side instead. */
        if (retraction_rel_name(op->relation_name, dname, sizeof(dname)) == 0)
            delta = session_find_rel(sess, dname);
    } else {
        /* Normal mode: look for $d$<name> insertion delta */
        snprintf(dname, sizeof(dname), "$d$%s", op->relation_name);
        delta = session_find_rel(sess, dname);
    }

    if (op->delta_mode == WL_DELTA_FORCE_EMPTY
        || (op->delta_mode == WL_DELTA_FORCE_EMPTY_AFTER_SEED
        && sess->tdd_outbound_only_active
        && sess->current_iteration > 0)) {
        /* Issue #370: segment has no FORCE_DELTA — push empty to skip. */
        col_rel_t *empty = col_rel_pool_new_like(
            sess->delta_pool, "$empty_skip", full_rel);
        if (!empty)
            return ENOMEM;
        int push_rc = eval_stack_push_delta(stack, empty, true, false);
        if (push_rc != 0)
            col_rel_destroy(empty);
        return push_rc;
    }
    if (op->delta_mode == WL_DELTA_FORCE_FULL) {
        return eval_stack_push_delta(stack, full_rel, false, false);
    }
    if (op->delta_mode == WL_DELTA_FORCE_DELTA) {
        if (delta && delta->nrows > 0) {
            return eval_stack_push_delta(stack, delta, false, true);
        }
        if (sess->current_iteration == 0) {
            if (sess->delta_seeded || sess->retraction_seeded) {
                /* Issue #83 (delta-seeded) or #158 (retraction-seeded):
                 * No pre-seeded delta means this relation has no new/removed facts.
                 * Push empty so only rules with actual deltas produce output. */
                col_rel_t *empty = col_rel_pool_new_like(
                    sess->delta_pool, "$empty_delta", full_rel);
                if (!empty)
                    return ENOMEM;
                int push_rc = eval_stack_push_delta(stack, empty, true, true);
                if (push_rc != 0)
                    col_rel_destroy(empty);
                return push_rc;
            }
            /* Base-case iteration: no deltas exist yet, fall back to full
             * relation so EDB-grounded rules can still fire on iter 0. */
            return eval_stack_push_delta(stack, full_rel, false, false);
        }
        /* Iteration > 0: delta absent or empty means the relation has
         * converged.  Push an empty relation so this rule copy produces
         * no output (correct semi-naive semantics, issue #85). */
        col_rel_t *empty
            = col_rel_pool_new_like(sess->delta_pool, "$empty_delta", full_rel);
        if (!empty)
            return ENOMEM;
        int push_rc = eval_stack_push_delta(stack, empty, true, true);
        if (push_rc != 0)
            col_rel_destroy(empty);
        return push_rc;
    }

    /* WL_DELTA_AUTO: use delta if strictly smaller than full relation.
     * Exception: inside a TDD worker sub-pass the broadcast $d$<rel> may be
     * >= the local partition, so we must use it whenever it is non-empty. */
    bool use_delta = delta && (((delta->nrows > 0
        && delta->nrows < full_rel->nrows) || (delta->nrows > 0
        && sess->tdd_subpass_active)) || (sess->tdd_outbound_only_active
        && sess->current_iteration > 0));
    col_rel_t *rel = use_delta ? delta : full_rel;
    /* push borrowed reference - session owns the relation */
    return eval_stack_push_delta(stack, rel, false, use_delta);
}

/* --- MAP ----------------------------------------------------------------- */

int
col_op_map(const wl_plan_op_t *op, eval_stack_t *stack, wl_col_session_t *sess)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    uint32_t pc = op->project_count;
    col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
            "$map", pc);
    if (!out) {
        if (e.owned)
            col_rel_destroy(e.rel);
        return ENOMEM;
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * pc);
    if (!tmp) {
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(e.rel);
        return ENOMEM;
    }

    /* Pre-compile map expressions once to avoid per-row strtol. */
    wl_columnar_expr_compiled_t **ce_map = NULL;
    uint32_t ce_map_count = 0;
    if (op->map_exprs && op->map_expr_count > 0) {
        ce_map = (wl_columnar_expr_compiled_t **)calloc(pc,
                sizeof(wl_columnar_expr_compiled_t *));
        if (ce_map) {
            ce_map_count = (op->map_expr_count < pc) ? op->map_expr_count : pc;
            for (uint32_t c = 0; c < ce_map_count; c++) {
                if (op->map_exprs[c].data && op->map_exprs[c].size > 0)
                    ce_map[c] = wl_columnar_expr_compile(op->map_exprs[c].data,
                            op->map_exprs[c].size,
                            sess ? sess->intern : NULL);
            }
        }
    }

    /* Row scratch, hoisted out of the loop: initialising it per row would
     * malloc once per row for relations wider than COL_STACK_MAX (#1000). */
    col_row_buf_t row_rb;
    if (!col_row_buf_init(&row_rb, e.rel->ncols)) {
        if (ce_map) {
            for (uint32_t c = 0; c < ce_map_count; c++)
                wl_columnar_expr_compiled_free(ce_map[c]);
            free(ce_map);
        }
        free(tmp);
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(e.rel);
        return ENOMEM;
    }

    int64_t *const row = row_rb.ptr;
    for (uint32_t r = 0; r < e.rel->nrows; r++) {
        col_rel_row_copy_out(e.rel, r, row);
        for (uint32_t c = 0; c < pc; c++) {
            if (op->map_exprs && c < op->map_expr_count && op->map_exprs[c].data
                && op->map_exprs[c].size > 0) {
                if (ce_map && c < ce_map_count && ce_map[c]) {
                    int64_t val = 0;
                    if (wl_columnar_expr_eval_compiled(ce_map[c], row,
                        e.rel->ncols,
                        &val) != 0) {
                        if (ce_map) {
                            for (uint32_t i = 0; i < ce_map_count; i++)
                                wl_columnar_expr_compiled_free(ce_map[i]);
                            free(ce_map);
                        }
                        col_row_buf_release(&row_rb);
                        free(tmp);
                        col_rel_destroy(out);
                        if (e.owned)
                            col_rel_destroy(e.rel);
                        return ERANGE;
                    }
                    tmp[c] = val;
                } else {
                    int64_t val = 0;
                    if (wl_columnar_expr_eval_i64(op->map_exprs[c].data,
                        op->map_exprs[c].size, row, e.rel->ncols,
                        &val, sess->intern) != 0) {
                        if (ce_map) {
                            for (uint32_t i = 0; i < ce_map_count; i++)
                                wl_columnar_expr_compiled_free(ce_map[i]);
                            free(ce_map);
                        }
                        col_row_buf_release(&row_rb);
                        free(tmp);
                        col_rel_destroy(out);
                        if (e.owned)
                            col_rel_destroy(e.rel);
                        return ERANGE;
                    }
                    tmp[c] = val;
                }
            } else {
                uint32_t src = op->project_indices ? op->project_indices[c] : c;
                tmp[c] = (src < e.rel->ncols) ? row[src] : 0;
            }
        }
        int rc = col_rel_append_row(out, tmp);
        if (rc != 0) {
            if (ce_map) {
                for (uint32_t c = 0; c < ce_map_count; c++)
                    wl_columnar_expr_compiled_free(ce_map[c]);
                free(ce_map);
            }
            col_row_buf_release(&row_rb);
            free(tmp);
            col_rel_destroy(out);
            if (e.owned)
                col_rel_destroy(e.rel);
            return rc;
        }
    }

    if (ce_map) {
        for (uint32_t c = 0; c < ce_map_count; c++)
            wl_columnar_expr_compiled_free(ce_map[c]);
        free(ce_map);
    }
    col_row_buf_release(&row_rb);
    free(tmp);

    if (e.owned)
        col_rel_destroy(e.rel);
    return eval_stack_push(stack, out, true);
}

/* --- CONCAT and CONSOLIDATE are implemented in columnar/merge.c. */
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
                || row_cmp_dispatch(last_row, _rb, nc) != 0) {                   \
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
    /* Issue #216: copy LRU metadata so worker clones inherit access state. */
    dst->lru_clock = src->lru_clock;
    dst->mem_bytes = src->mem_bytes;

    if (src->arr.nbuckets > 0 && src->arr.ht_head) {
        dst->arr.ht_head
            = (uint32_t *)malloc(src->arr.nbuckets * sizeof(uint32_t));
        if (!dst->arr.ht_head) {
            free(dst->key_cols);
            free(dst->rel_name);
            memset(dst, 0, sizeof(*dst));
            return ENOMEM;
        }
        memcpy(dst->arr.ht_head, src->arr.ht_head,
            src->arr.nbuckets * sizeof(uint32_t));
    }

    if (src->arr.ht_cap > 0 && src->arr.ht_next) {
        dst->arr.ht_next
            = (uint32_t *)malloc(src->arr.ht_cap * sizeof(uint32_t));
        if (!dst->arr.ht_next) {
            free(dst->arr.ht_head);
            free(dst->key_cols);
            free(dst->rel_name);
            memset(dst, 0, sizeof(*dst));
            return ENOMEM;
        }
        memcpy(dst->arr.ht_next, src->arr.ht_next,
            src->arr.ht_cap * sizeof(uint32_t));
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
    free(results);
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
int
col_op_k_fusion(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
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
    if (active_workers > 1 && live_count >= WL_KFUSION_MIN_PARALLEL_K) {
        int ensure_rc = wl_columnar_session_ensure_workqueue(sess,
                active_workers);
        if (ensure_rc != 0) {
            free(live_indices);
            return ensure_rc;
        }
        wq = sess->wq;
    }
    if (!wq || live_count < WL_KFUSION_MIN_PARALLEL_K) {
        free(live_indices);
        return col_op_k_fusion_serial(op, stack, sess);
    }

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
        free(results);
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
        free(compact);
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
    free(results);
    free(workers);
    free(live_indices);
    COL_SESSION(sess)->kfusion_cleanup_ns += now_ns() - _phase_t0;
    return rc;
}

/* --- REDUCE (aggregate) -------------------------------------------------- */

int
col_op_reduce(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    col_rel_t *in = e.rel;
    uint32_t gc = op->group_by_count;

    /* Output: the group columns and aggregate retain the rule-head order. */
    uint32_t ocols = gc + 1;
    uint32_t agg_index = op->aggregate_index < ocols
        ? op->aggregate_index : gc;
    col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
            "$reduce", ocols);
    if (!out) {
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }

    wl_columnar_expr_compiled_t *agg_ce = NULL;
    if (op->agg_expr.data && op->agg_expr.size > 0)
        agg_ce = wl_columnar_expr_compile(op->agg_expr.data, op->agg_expr.size,
                sess ? sess->intern : NULL);

    /* Row scratch, hoisted out of the loop (#1000). */
    col_row_buf_t row_rb;
    if (!col_row_buf_init(&row_rb, in->ncols)) {
        wl_columnar_expr_compiled_free(agg_ce);
        free(tmp);
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }

    typedef struct {
        uint64_t hash;
        uint32_t row;
    } reduce_group_slot_t;
    uint32_t map_cap = 1;
    uint64_t desired = (uint64_t)(in->nrows ? in->nrows : 1) * 2U;
    while ((uint64_t)map_cap < desired && map_cap <= UINT32_MAX / 2U)
        map_cap <<= 1;
    if ((uint64_t)map_cap < desired) {
        col_row_buf_release(&row_rb);
        wl_columnar_expr_compiled_free(agg_ce);
        free(tmp);
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }
    reduce_group_slot_t *groups = (reduce_group_slot_t *)calloc(map_cap,
            sizeof(*groups));
    if (!groups) {
        col_row_buf_release(&row_rb);
        wl_columnar_expr_compiled_free(agg_ce);
        free(tmp);
        col_rel_destroy(out);
        if (e.owned)
            col_rel_destroy(in);
        return ENOMEM;
    }
    uint32_t map_mask = map_cap - 1;

    /* Index groups by their key so reduction remains linear in the number of
     * input rows rather than scanning every output group. */
    int64_t *const row = row_rb.ptr;
    for (uint32_t r = 0; r < in->nrows; r++) {
        col_rel_row_copy_out(in, r, row);
        int64_t agg_val = (in->ncols > gc) ? row[gc] : 1;
        if (op->agg_fn != WIRELOG_AGG_COUNT
            && op->agg_expr.data && op->agg_expr.size > 0) {
            if (agg_ce) {
                int64_t val = 0;
                if (wl_columnar_expr_eval_compiled(agg_ce, row, in->ncols,
                    &val) == 0)
                    agg_val = val;
                else {
                    col_row_buf_release(&row_rb);
                    wl_columnar_expr_compiled_free(agg_ce);
                    free(groups);
                    free(tmp);
                    col_rel_destroy(out);
                    if (e.owned)
                        col_rel_destroy(in);
                    return ERANGE;
                }
            } else {
                int64_t val = 0;
                if (wl_columnar_expr_eval_i64(op->agg_expr.data,
                    op->agg_expr.size,
                    row, in->ncols, &val, sess->intern) == 0) {
                    agg_val = val;
                } else {
                    col_row_buf_release(&row_rb);
                    wl_columnar_expr_compiled_free(agg_ce);
                    free(groups);
                    free(tmp);
                    col_rel_destroy(out);
                    if (e.owned)
                        col_rel_destroy(in);
                    return ERANGE;
                }
            }
        }

        /* Use an open-addressed key index instead of scanning all output
         * groups for every input row. */
        uint64_t hash = UINT64_C(1469598103934665603);
        for (uint32_t k = 0; k < gc; k++) {
            uint32_t gi = op->group_by_indices ? op->group_by_indices[k] : k;
            hash ^= (uint64_t)row[gi < in->ncols ? gi : 0];
            hash *= UINT64_C(1099511628211);
        }
        if (!hash)
            hash = 1;
        uint32_t slot = (uint32_t)hash & map_mask;
        bool found = false;
        uint32_t group_row = UINT32_MAX;
        while (groups[slot].hash != 0) {
            bool match = groups[slot].hash == hash;
            for (uint32_t k = 0; k < gc && match; k++) {
                uint32_t gi
                    = op->group_by_indices ? op->group_by_indices[k] : k;
                uint32_t out_col = k >= agg_index ? k + 1 : k;
                match = row[gi < in->ncols ? gi : 0]
                    == col_rel_get(out, groups[slot].row, out_col);
            }
            if (match) {
                found = true;
                group_row = groups[slot].row;
                break;
            }
            slot = (slot + 1) & map_mask;
        }
        if (found) {
            /* Update aggregate */
            int64_t cur = col_rel_get(out, group_row, agg_index);
            switch (op->agg_fn) {
            case WIRELOG_AGG_COUNT:
                col_rel_set(out, group_row, agg_index, cur + 1);
                break;
            case WIRELOG_AGG_SUM:
            {
                int64_t next;
                if (wl_columnar_arithmetic_checked_add_int64(cur, agg_val,
                    &next) != 0) {
                    col_row_buf_release(&row_rb);
                    wl_columnar_expr_compiled_free(agg_ce);
                    free(groups);
                    free(tmp);
                    col_rel_destroy(out);
                    if (e.owned)
                        col_rel_destroy(in);
                    return ERANGE;
                }
                col_rel_set(out, group_row, agg_index, next);
            }
            break;
            case WIRELOG_AGG_MIN:
            case WIRELOG_AGG_MAX:
                /* Ordered by the operand's declared domain, not by the
                 * raw int64 -- for a symbol column that int64 is an
                 * intern id (Issue #965). */
                if (col_agg_better(op->agg_fn, op->agg_operand_type,
                    sess->intern, agg_val, cur))
                    col_rel_set(out, group_row, agg_index, agg_val);
                break;
            default:
                break;
            }
        }
        if (!found) {
            for (uint32_t k = 0; k < gc; k++) {
                uint32_t gi
                    = op->group_by_indices ? op->group_by_indices[k] : k;
                uint32_t out_col = k >= agg_index ? k + 1 : k;
                tmp[out_col] = row[gi < in->ncols ? gi : 0];
            }
            tmp[agg_index] = (op->agg_fn == WIRELOG_AGG_COUNT) ? 1 : agg_val;
            int rc = col_rel_append_row(out, tmp);
            if (rc != 0) {
                col_row_buf_release(&row_rb);
                wl_columnar_expr_compiled_free(agg_ce);
                free(groups);
                free(tmp);
                col_rel_destroy(out);
                if (e.owned)
                    col_rel_destroy(in);
                return rc;
            }
            groups[slot].hash = hash;
            groups[slot].row = out->nrows - 1;
        }
    }

    col_row_buf_release(&row_rb);
    wl_columnar_expr_compiled_free(agg_ce);
    free(groups);
    free(tmp);
    if (e.owned)
        col_rel_destroy(in);
    return eval_stack_push(stack, out, true);
}

/* --- REDUCE WEIGHTED (Z-set / Mobius COUNT) ------------------------------ */

/*
 * col_op_reduce_weighted:
 * Global COUNT aggregation using Z-set (signed multiplicity) semantics.
 * Output: one row whose data value = sum of input multiplicities, and whose
 * timestamp.multiplicity = the same sum.
 *
 * src - input relation; src->timestamps[i].multiplicity carries each row's
 *       signed weight.
 * dst - output relation (caller-allocated, empty on entry, ncols >= 1).
 *
 * Returns 0 on success, EINVAL / ENOMEM on error.
 */
int
col_op_reduce_weighted(const col_rel_t *src, col_rel_t *dst)
{
    if (!src || !dst)
        return EINVAL;

    /* Sum all input multiplicities. */
    int64_t total = 0;
    if (src->timestamps) {
        for (uint32_t i = 0; i < src->nrows; i++)
            total += src->timestamps[i].multiplicity;
    } else {
        /* No timestamp tracking: treat each row as multiplicity 1. */
        total = (int64_t)src->nrows;
    }

    /* Allocate timestamp tracking on dst if not already present. */
    if (!dst->timestamps) {
        dst->timestamps
            = (col_delta_timestamp_t *)calloc(1, sizeof(col_delta_timestamp_t));
        if (!dst->timestamps)
            return ENOMEM;
        dst->capacity = (dst->capacity == 0) ? 1 : dst->capacity;
    }

    /* Allocate column buffers for one output row if not already present. */
    if (!dst->columns) {
        uint32_t ncols = dst->ncols ? dst->ncols : 1;
        dst->columns = col_columns_alloc(ncols, 1);
        if (!dst->columns)
            return ENOMEM;
        /* Zero-initialize the single row */
        for (uint32_t c = 0; c < ncols; c++)
            dst->columns[c][0] = 0;
        dst->capacity = 1;
    }

    /* Write the single aggregate row. */
    col_rel_set(dst, 0, 0, total);
    dst->nrows = 1;

    /* Set output row multiplicity. */
    memset(&dst->timestamps[0], 0, sizeof(col_delta_timestamp_t));
    dst->timestamps[0].multiplicity = total;

    return 0;
}

/* ======================================================================== */
/* LFTJ Operator (Issue #195)                                               */
/* ======================================================================== */

/*
 * lftj_binary_ctx_t: callback context for col_op_lftj.
 *
 * wl_lftj_join delivers rows in compact format:
 *   [key, non_key_rel0..., non_key_rel1..., ...]
 *
 * This context reconstructs binary-join-compatible rows:
 *   [all_rel0_cols, all_rel1_cols, ...]  (key duplicated per relation)
 *
 * The downstream WL_PLAN_OP_MAP project_indices are unchanged because the
 * output column layout matches what a cascade of WL_PLAN_OP_JOIN produces.
 */
typedef struct {
    uint32_t k;
    uint32_t *ncols;          /* per-relation column count (k entries)    */
    uint32_t *key_cols;       /* per-relation join key column (k entries) */
    uint32_t *lftj_offsets;   /* start of Ri's non-key cols in LFTJ row  */
    uint32_t *binary_offsets; /* start of Ri's cols in binary output     */
    uint32_t total_binary_ncols;
    int64_t *tmp;   /* scratch row buffer                       */
    col_rel_t *out; /* destination relation                     */
    int rc;         /* first error code encountered; 0 = ok    */
} lftj_binary_ctx_t;

/*
 * lftj_binary_cb: output callback for col_op_lftj.
 *
 * Converts compact LFTJ output to binary-join-compatible format and appends
 * the result to ctx->out.  Sets ctx->rc on allocation failure (subsequent
 * calls are no-ops).
 */
static void
lftj_binary_cb(const int64_t *row, uint32_t lftj_ncols, void *user)
{
    (void)lftj_ncols;
    lftj_binary_ctx_t *ctx = (lftj_binary_ctx_t *)user;
    if (ctx->rc)
        return; /* already OOM; skip remaining rows */

    const int64_t key = row[0];
    for (uint32_t i = 0; i < ctx->k; i++) {
        uint32_t nc = ctx->ncols[i];
        uint32_t kc = ctx->key_cols[i];
        uint32_t lo = ctx->lftj_offsets[i];
        uint32_t bo = ctx->binary_offsets[i];
        for (uint32_t c = 0; c < nc; c++) {
            int64_t val;
            if (c == kc)
                val = key;
            else if (c < kc)
                val = row[lo + c];
            else
                val = row[lo + c - 1u];
            ctx->tmp[bo + c] = val;
        }
    }
    int rc = col_rel_append_row(ctx->out, ctx->tmp);
    if (rc)
        ctx->rc = rc;
}

/*
 * col_op_lftj: execute a WL_PLAN_OP_LFTJ operator.
 *
 * Performs a multi-way leapfrog triejoin over the k EDB relations named in
 * op->opaque_data.  Uses the sorted arrangement cache to avoid re-sorting
 * on repeated calls (the sort inside wl_lftj_join degrades to O(N) when the
 * input is already sorted).  Pushes binary-join-compatible result onto stack.
 */
int
col_op_lftj(const wl_plan_op_t *op, eval_stack_t *stack, wl_col_session_t *sess)
{
    if (!op->opaque_data)
        return EINVAL;
    const wl_plan_op_lftj_t *meta = (const wl_plan_op_lftj_t *)op->opaque_data;
    uint32_t k = meta->k;
    if (k < 2u || !meta->rel_names || !meta->key_cols)
        return EINVAL;

    /* Allocate per-relation working arrays. */
    wl_lftj_input_t *inputs
        = (wl_lftj_input_t *)calloc(k, sizeof(wl_lftj_input_t));
    uint32_t *ncols = (uint32_t *)malloc(k * sizeof(uint32_t));
    uint32_t *lftj_offsets = (uint32_t *)malloc(k * sizeof(uint32_t));
    uint32_t *binary_offsets = (uint32_t *)malloc(k * sizeof(uint32_t));
    if (!inputs || !ncols || !lftj_offsets || !binary_offsets) {
        free(inputs);
        free(ncols);
        free(lftj_offsets);
        free(binary_offsets);
        return ENOMEM;
    }

    /* Resolve each relation and populate LFTJ input descriptors. */
    uint32_t total_binary_ncols = 0u;
    uint32_t lftj_nk_total = 0u;
    int rc = 0;
    for (uint32_t i = 0; i < k; i++) {
        col_rel_t *rel = session_find_rel(sess, meta->rel_names[i]);
        if (!rel) {
            rc = ENOENT;
            goto cleanup_arrays;
        }
        uint32_t kc = meta->key_cols[i];
        if (kc >= rel->ncols) {
            rc = EINVAL;
            goto cleanup_arrays;
        }

        /* Use the pre-sorted arrangement when available: wl_lftj_join still
         * copies and sorts internally, but starting from a sorted copy
         * reduces its qsort from O(N log N) to O(N). */
        col_sorted_arr_t *sarr
            = col_session_get_sorted_arrangement(sess, meta->rel_names[i], kc);
        if (sarr && sarr->indexed_rows == rel->nrows && sarr->nrows > 0) {
            inputs[i].data = sarr->sorted;
            inputs[i].nrows = sarr->nrows;
        } else {
            /* Gather column-major into flat buffer for LFTJ */
            int64_t *flat = (int64_t *)malloc(
                (size_t)rel->nrows * rel->ncols * sizeof(int64_t));
            if (!flat) {
                /* Free previously allocated flat buffers */
                for (uint32_t j = 0; j < i; j++) {
                    if (inputs[j].data != NULL) {
                        col_sorted_arr_t *prev_sarr
                            = col_session_get_sorted_arrangement(sess,
                                meta->rel_names[j], meta->key_cols[j]);
                        if (!(prev_sarr
                            && prev_sarr->indexed_rows
                            == inputs[j].nrows
                            && prev_sarr->nrows > 0))
                            free((void *)inputs[j].data);
                    }
                }
                rc = ENOMEM;
                goto cleanup_arrays;
            }
            for (uint32_t r = 0; r < rel->nrows; r++)
                col_rel_row_copy_out(rel, r,
                    flat + (size_t)r * rel->ncols);
            inputs[i].data = flat;
            inputs[i].nrows = rel->nrows;
        }
        inputs[i].ncols = rel->ncols;
        inputs[i].key_col = kc;

        ncols[i] = rel->ncols;
        binary_offsets[i] = total_binary_ncols;
        lftj_offsets[i] = 1u + lftj_nk_total; /* 1: shared key lives at [0] */
        total_binary_ncols += rel->ncols;
        lftj_nk_total += rel->ncols - 1u;
    }

    {
        col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool,
                sess->eval_arena, "$lftj",
                total_binary_ncols);
        int64_t *tmp = (int64_t *)malloc(
            (total_binary_ncols ? total_binary_ncols : 1u) * sizeof(int64_t));
        if (!out || !tmp) {
            free(tmp);
            if (out)
                col_rel_destroy(out);
            rc = ENOMEM;
            goto cleanup_arrays;
        }

        lftj_binary_ctx_t ctx = { k,
                                  ncols,
                                  meta->key_cols,
                                  lftj_offsets,
                                  binary_offsets,
                                  total_binary_ncols,
                                  tmp,
                                  out,
                                  0 };

        rc = wl_lftj_join(inputs, k, lftj_binary_cb, &ctx);
        if (rc == 0)
            rc = ctx.rc;

        free(tmp);
        if (rc != 0) {
            col_rel_destroy(out);
            goto cleanup_arrays;
        }
        rc = eval_stack_push(stack, out, true);
    }

cleanup_arrays:
    /* Free flat buffers allocated for non-sarr LFTJ inputs */
    if (inputs) {
        for (uint32_t i = 0; i < k; i++) {
            if (inputs[i].data) {
                col_sorted_arr_t *sarr2
                    = col_session_get_sorted_arrangement(sess,
                        meta->rel_names[i], meta->key_cols[i]);
                if (!(sarr2 && sarr2->sorted == inputs[i].data))
                    free((void *)inputs[i].data);
            }
        }
    }
    free(inputs);
    free(ncols);
    free(lftj_offsets);
    free(binary_offsets);
    return rc;
}

/* --- DIFFERENTIAL CONSOLIDATE -------------------------------------------- */

/*
 * col_op_consolidate_diff - Differential consolidate with trace-based
 * incremental compaction (Issue #263).
 *
 * Key optimization over col_op_consolidate:
 *   - Uses sorted prefix tracking for incremental merge: O(D log D + N)
 *   - Creates trace checkpoint for frontier persistence across iterations
 *   - Preserves arrangement validity by using incremental merge path
 *
 * Algorithm:
 *   1. If sorted prefix exists [0..sorted_nrows): sort only suffix (delta)
 *   2. Dedup within delta
 *   3. Merge sorted prefix + sorted delta, emitting unique rows
 *   4. Record trace timestamp for convergence tracking
 *
 * Guard: activated when sess->diff_operators_active is true
 */
int
col_op_consolidate_diff(eval_stack_t *stack, wl_col_session_t *sess)
{
    eval_entry_t e = eval_stack_pop(stack);
    if (!e.rel)
        return EINVAL;

    col_rel_t *in = e.rel;
    uint32_t nc = in->ncols;
    uint32_t nr = in->nrows;

    if (nr <= 1) {
        if (e.seg_boundaries)
            free(e.seg_boundaries);
        in->sorted_nrows = nr;
        in->run_count = 1;
        in->run_ends[0] = nr;
        return eval_stack_push(stack, in, e.owned);
    }

    /* Own the relation for in-place sort */
    col_rel_t *work = in;
    bool work_owned = e.owned;
    if (!work_owned) {
        work = col_rel_pool_new_like(sess->delta_pool, "$consol_diff", in);
        if (!work) {
            if (e.seg_boundaries)
                free(e.seg_boundaries);
            return ENOMEM;
        }
        if (col_rel_append_all(work, in, NULL) != 0) {
            col_rel_destroy(work);
            if (e.seg_boundaries)
                free(e.seg_boundaries);
            return ENOMEM;
        }
        work_owned = true;
    }

    /* K-way merge dispatch (same as col_op_consolidate) */
    uint32_t k = e.seg_count > 0 ? e.seg_count : 1;
    if (k >= 2 && e.seg_boundaries != NULL) {
        int rc = col_op_consolidate_kway_merge(work, e.seg_boundaries, k);
        free(e.seg_boundaries);
        if (rc != 0) {
            if (work_owned)
                col_rel_destroy(work);
            return rc;
        }
        work->sorted_nrows = work->nrows;
        work->run_count = 1;
        work->run_ends[0] = work->nrows;
        return eval_stack_push(stack, work, work_owned);
    }

    if (e.seg_boundaries)
        free(e.seg_boundaries);

    /* Trace-based incremental compaction:
     * When a sorted prefix exists, use incremental merge (O(D log D + N))
     * instead of full sort (O(N log N)). Record trace for frontier tracking. */
    uint32_t sn = work->sorted_nrows;
    if (sn > 0 && sn < nr) {
        uint32_t delta_count = nr - sn;

        /* Phase 1: sort only the unsorted suffix using radix sort */
        col_rel_radix_sort(work, sn, delta_count);

        /* Phase 1b: dedup within suffix */
        uint32_t d_unique = 1;
        for (uint32_t i = 1; i < delta_count; i++) {
            if (col_rel_row_cmp(work, sn + i - 1, sn + i) != 0) {
                col_rel_row_move(work, sn + d_unique, sn + i);
                d_unique++;
            }
        }

        /* Phase 2: merge sorted prefix with sorted suffix */
        uint32_t max_rows = sn + d_unique;

        /* Reuse persistent merge buffer when possible (column-major) */
        int64_t **merged_cols;
        bool used_merge_buf = false;
        if (work->merge_columns && work->merge_buf_cap >= max_rows) {
            merged_cols = work->merge_columns;
            used_merge_buf = true;
        } else {
            uint32_t new_cap = max_rows > work->merge_buf_cap * 2
                                   ? max_rows
                                   : work->merge_buf_cap * 2;
            if (new_cap < max_rows)
                new_cap = max_rows;
            if (work->merge_columns) {
                if (col_columns_realloc(work->merge_columns, nc,
                    new_cap) != 0) {
                    if (work_owned && work != in)
                        col_rel_destroy(work);
                    return ENOMEM;
                }
            } else {
                work->merge_columns = col_columns_alloc(nc, new_cap);
                if (!work->merge_columns) {
                    if (work_owned && work != in)
                        col_rel_destroy(work);
                    return ENOMEM;
                }
            }
            work->merge_buf_cap = new_cap;
            merged_cols = work->merge_columns;
            used_merge_buf = true;
        }

        uint32_t oi = 0, di = 0, out_idx = 0;
        while (oi < sn && di < d_unique) {
            int cmp = col_rel_row_cmp(work, oi, sn + di);
            if (cmp < 0) {
                col_columns_copy_row(merged_cols, out_idx,
                    work->columns, oi, nc);
                oi++;
            } else if (cmp == 0) {
                col_columns_copy_row(merged_cols, out_idx,
                    work->columns, oi, nc);
                oi++;
                di++;
            } else {
                col_columns_copy_row(merged_cols, out_idx,
                    work->columns, sn + di, nc);
                di++;
            }
            out_idx++;
        }
        while (oi < sn) {
            col_columns_copy_row(merged_cols, out_idx,
                work->columns, oi, nc);
            oi++;
            out_idx++;
        }
        while (di < d_unique) {
            col_columns_copy_row(merged_cols, out_idx,
                work->columns, sn + di, nc);
            di++;
            out_idx++;
        }

        /* Swap merge_columns and columns (issue #218) */
        if (used_merge_buf) {
            int64_t **old_cols = work->columns;
            uint32_t old_cap = work->capacity;
            work->columns = work->merge_columns;
            work->capacity = work->merge_buf_cap;
            work->merge_columns = old_cols;
            work->merge_buf_cap = old_cap;
        }
        work->nrows = out_idx;
        work->sorted_nrows = out_idx;
        work->run_count = 1;
        work->run_ends[0] = out_idx;

        /* Right-size columns after dedup (issue #218) */
        if (out_idx > 0 && work->capacity > out_idx + out_idx / 4) {
            uint32_t tight = out_idx + out_idx / 4;
            if (tight < COL_REL_INIT_CAP)
                tight = COL_REL_INIT_CAP;
            if (col_columns_realloc(work->columns, nc, tight) == 0)
                work->capacity = tight;
        }

        return eval_stack_push(stack, work, work_owned);
    }

    /* Fallback: radix sort + dedup */
    col_rel_radix_sort_int64(work);

    uint32_t out_r = 1;
    for (uint32_t r = 1; r < nr; r++) {
        if (col_rel_row_cmp(work, r - 1, r) != 0) {
            col_rel_row_move(work, out_r, r);
            out_r++;
        }
    }
    work->nrows = out_r;
    work->sorted_nrows = out_r;
    work->run_count = 1;
    work->run_ends[0] = out_r;

    return eval_stack_push(stack, work, work_owned);
}

/* ======================================================================== */
/* Exchange Operator (Issue #316)                                           */
/* ======================================================================== */

/*
 * col_op_exchange:
 * Redistribute tuples by hash(key_columns) % W across workers.
 *
 * Single-worker (W=1): no-op, leave stack unchanged.
 *
 * Multi-worker: pops input from eval stack, partitions it into W
 * sub-relations stored in coord->exchange_bufs[my_worker_id][0..W-1].
 * Does NOT push a result -- the coordinator gathers exchange_bufs[*][w]
 * for each worker w after the barrier.
 *
 * Precondition: coord->exchange_bufs must be allocated by the caller
 * (coordinator) before submitting workers to the workqueue.
 */
int
col_op_exchange(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    if (!op->opaque_data)
        return EINVAL;

    const wl_plan_op_exchange_t *meta
        = (const wl_plan_op_exchange_t *)op->opaque_data;

    /* Single-worker no-op: leave stack unchanged */
    if (meta->num_workers <= 1)
        return 0;

    /* Pop input from eval stack */
    if (stack->top == 0)
        return EINVAL;
    eval_entry_t input_entry = eval_stack_pop(stack);
    col_rel_t *input = input_entry.rel;

    /* NULL or empty input is a no-op for exchange */
    if (!input || input->ncols == 0) {
        if (input_entry.owned && input)
            col_rel_destroy(input);
        return 0;
    }

    /* Validate key column indices against input schema */
    if (input->ncols > 0) {
        for (uint32_t i = 0; i < meta->key_col_count; i++) {
            if (meta->key_col_idxs[i] >= input->ncols) {
                if (input_entry.owned)
                    col_rel_destroy(input);
                return EINVAL;
            }
        }
    }

    /* Locate coordinator and determine this worker's id */
    wl_col_session_t *coord = sess->coordinator ? sess->coordinator : sess;
    uint32_t my_id = sess->coordinator ? sess->worker_id : 0;

    if (!coord->exchange_bufs || my_id >= coord->exchange_num_workers) {
        if (input_entry.owned)
            col_rel_destroy(input);
        return EINVAL;
    }

    /* Scatter: partition input into exchange_bufs[my_id][0..W-1] */
    int rc = col_rel_exchange_partition(input, meta->key_col_idxs,
            meta->key_col_count, meta->num_workers,
            coord->exchange_bufs[my_id]);

    if (input_entry.owned)
        col_rel_destroy(input);

    return rc;
}
/* --- JOIN and ANTIJOIN are implemented in columnar/join.c. */
