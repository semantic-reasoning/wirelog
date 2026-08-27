/*
 * columnar/join.c - wirelog Columnar Backend Join Operators
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#define _GNU_SOURCE

#define WL_JOIN_PAR_MIN_LEFT_ROWS_DEFAULT 4096u
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
/* Join and antijoin operators                                               */
/* ======================================================================== */

static uint32_t
col_op_resolve_key(const col_rel_t *rel, const char *name, const char *side,
    const char *op_name, const char *right_relation)
{
    int idx = col_rel_col_idx(rel, name);
    if (idx >= 0)
        return (uint32_t)idx;
    WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_ERROR,
        "%s: unresolved %s key '%s' (right=%s, ncols=%u) -- falling back to "
        "column 0",
        op_name, side, name ? name : "(null)",
        right_relation ? right_relation : "-", rel->ncols);
    return 0;
}

/* Return a valid hash bucket count, rejecting representational overflow. */
static int
col_join_bucket_count(uint32_t nrows, uint32_t *out)
{
    if (!out || nrows > UINT32_MAX / 2u)
        return ENOMEM;
    uint32_t count = wl_columnar_filter_next_pow2(
        nrows > 0 ? nrows * 2u : 1u);
    if (count == 0)
        return ENOMEM;
    *out = count;
    return 0;
}

#define WL_JOIN_PAR_MIN_LEFT_ROWS_DEFAULT 4096u

static uint32_t
col_join_parallel_min_left_rows(void)
{
    const char *env = getenv("WIRELOG_JOIN_PAR_MIN_LEFT_ROWS");
    if (!env || env[0] == '\0')
        return WL_JOIN_PAR_MIN_LEFT_ROWS_DEFAULT;

    char *endp = NULL;
    errno = 0;
    unsigned long v = strtoul(env, &endp, 10);
    if (endp == env || *endp != '\0' || errno == ERANGE || v > UINT32_MAX)
        return WL_JOIN_PAR_MIN_LEFT_ROWS_DEFAULT;
    return (uint32_t)v;
}

static bool
col_join_should_parallelize_rows(const wl_col_session_t *sess,
    const col_rel_t *left, const col_rel_t *right)
{
    if (!sess || !left || !right)
        return false;
    if (sess->coordinator || sess->num_workers <= 1)
        return false;
    uint32_t min_left = col_join_parallel_min_left_rows();
    if (min_left == 0)
        min_left = 1;
    return left->nrows >= min_left
           && left->nrows >= sess->num_workers * min_left;
}

static bool
col_join_should_parallelize_cross(const wl_col_session_t *sess,
    const col_rel_t *left, const col_rel_t *right)
{
    if (!col_join_should_parallelize_rows(sess, left, right))
        return false;
    if (right->nrows != 0 && left->nrows > UINT64_MAX / right->nrows)
        return false;
    uint64_t total = (uint64_t)left->nrows * right->nrows;
    return sess->join_output_limit == 0 || total < sess->join_output_limit;
}

static void
col_join_attach_ledger(wl_col_session_t *sess, col_rel_t *rel)
{
    if (!sess || !rel || rel->mem_ledger)
        return;
    rel->mem_ledger = &sess->mem_ledger;
    col_rel_ledger_reconcile(rel, 0);
}

static bool
col_join_output_limit_reached(wl_col_session_t *sess, const col_rel_t *out)
{
    if (!sess)
        return false;
    uint64_t limit = sess->join_output_shared_count
        ? sess->join_output_shared_limit : sess->join_output_limit;
    /* Issue #959: record the high-water mark even when no cap is set, so a
     * run that completes still reports what it needed.  Cheap: this helper
     * is already called once per output-size check, and it is a compare and
     * a store on a session-local field. */
    if (out && out->nrows > sess->join_output_peak)
        sess->join_output_peak = out->nrows;
    if (limit == 0)
        return false;
    if (sess->join_output_shared_count) {
        uint64_t n = atomic_fetch_add_explicit(
            sess->join_output_shared_count, 1, memory_order_relaxed) + 1;
        return n >= limit;
    }
    return out && out->nrows >= limit;
}

/*
 * col_join_inloop_backpressure: in-loop soft backpressure check.
 *
 * The RELATION-subsystem 80% threshold is a SOFT signal intended for the
 * multi-worker TDD path, where col_eval_stratum_tdd recovers via EAGAIN
 * (retry with fewer workers).  Coordinator and single-session evaluators
 * have no fallback — propagating EOVERFLOW from this signal turns a soft
 * advisory into an unrecoverable, silent abort (Issue #791: DOOP fails at
 * stratum 54 SubtypeOf eff_iter=1 when RELATION usage crosses 4.7 GB on a
 * 16 GB host even though absolute memory is fine and the cardinality cap
 * is far away).  Restrict this check to worker sessions; the hard
 * cardinality cap (col_join_output_limit_reached) remains the universal
 * safety net.  The pre-join coordinator-side BP check still applies and
 * degrades gracefully to an empty result (Issue #404 design).
 */
static inline bool
col_join_inloop_backpressure(wl_col_session_t *sess, const col_rel_t *out)
{
    if (!sess || !sess->coordinator)
        return false;
    return out && out->nrows > 0 && out->nrows % 10000 == 0
           && wl_mem_ledger_should_backpressure(
        &sess->mem_ledger, WL_MEM_SUBSYS_RELATION, 80);
}

static int
col_join_reserve_exact(col_rel_t *rel, uint32_t nrows)
{
    if (!rel)
        return EINVAL;
    if (nrows <= rel->capacity)
        return 0;
    uint64_t ledger_before = col_rel_owned_ledger_bytes(rel);
    uint32_t old_cap = rel->capacity;
    if (rel->arena_owned) {
        int64_t **new_cols = col_columns_alloc(rel->ncols, nrows);
        if (!new_cols)
            return ENOMEM;
        for (uint32_t c = 0; c < rel->ncols; c++)
            memcpy(new_cols[c], rel->columns[c],
                sizeof(int64_t) * rel->nrows);
        free((void *)rel->columns);
        rel->columns = new_cols;
        rel->arena_owned = false;
    } else if (rel->columns) {
        if (col_columns_realloc_atomic(rel->columns, rel->ncols, old_cap,
            nrows) != 0)
            return ENOMEM;
    } else {
        rel->columns = col_columns_alloc(rel->ncols, nrows);
        if (!rel->columns)
            return ENOMEM;
    }
    rel->capacity = nrows;
    col_rel_ledger_reconcile(rel, ledger_before);
    return 0;
}

typedef struct {
    const col_rel_t *left;
    const col_rel_t *right;
    col_rel_t *out;
    const uint32_t *project_indices;
    uint32_t project_count;
    uint64_t begin;
    uint64_t end;
    atomic_int *write_error;
} col_join_cross_ctx_t;

typedef struct {
    uint32_t lr;
    uint32_t rr;
} col_join_pair_ref_t;

typedef struct {
    const col_rel_t *left;
    const col_rel_t *right;
    const col_diff_arrangement_t *darr;
    const uint32_t *lk;
    const uint32_t *rk;
    uint32_t kc;
    const wl_plan_op_t *op;
    col_rel_t *out;
    uint32_t begin;
    uint32_t end;
    uint64_t out_begin;
    uint64_t count;
    uint64_t limit;
    uint32_t *left_hashes;
    col_join_pair_ref_t *pairs;
    uint32_t pair_count;
    uint32_t pair_cap;
    uint32_t pair_cap_limit;
    bool pairs_complete;
    atomic_bool *stop;
    atomic_uint_fast64_t *shared_count;
    int rc;
} col_join_keyed_ctx_t;

typedef struct {
    const col_rel_t *left;
    const col_rel_t *right;
    const uint32_t *lk;
    const uint32_t *rk;
    uint32_t kc;
    const wl_plan_op_t *op;
    const uint32_t *ht_head;
    const uint32_t *ht_next;
    uint32_t nbuckets;
    col_rel_t *out;
    uint32_t begin;
    uint32_t end;
    uint64_t out_begin;
    uint64_t count;
    uint32_t *left_hashes;
    atomic_int *write_error;
} col_semijoin_ctx_t;

static int64_t
col_join_pair_value(const col_rel_t *left, uint32_t lr, const col_rel_t *right,
    uint32_t rr, uint32_t idx);

static WL_OPS_ALWAYS_INLINE uint32_t
col_join_hash_rel_keys(const col_rel_t *rel, uint32_t row,
    const uint32_t *key_cols, uint32_t kc);

static WL_OPS_ALWAYS_INLINE bool
col_join_keys_match_rel(const col_rel_t *left, uint32_t lr,
    const uint32_t *lk, const col_rel_t *right, uint32_t rr,
    const uint32_t *rk, uint32_t kc);

static uint32_t
col_join_output_width(const col_rel_t *left, const col_rel_t *right,
    const wl_plan_op_t *op)
{
    return (op && op->project_count > 0 && op->project_indices)
        ? op->project_count : left->ncols + right->ncols;
}

static int
col_join_set_output_types(col_rel_t *out, const col_rel_t *left,
    const col_rel_t *right, const wl_plan_op_t *op)
{
    uint32_t width = col_join_output_width(left, right, op);
    wirelog_column_type_t *types = (wirelog_column_type_t *)malloc(
        (size_t)width * sizeof(*types));
    if (width > 0 && !types)
        return ENOMEM;
    for (uint32_t c = 0; c < width; c++) {
        uint32_t source = (op && op->project_count > 0
            && op->project_indices) ? op->project_indices[c] : c;
        const col_rel_t *source_rel = source < left->ncols ? left : right;
        uint32_t source_col = source < left->ncols ? source
            : source - left->ncols;
        types[c] = source_rel->column_types
            ? source_rel->column_types[source_col] : WIRELOG_TYPE_INT64;
    }
    int rc = col_rel_set_column_types(out, types, width);
    free(types);
    return rc;
}

static int
col_join_set_left_output_types(col_rel_t *out, const col_rel_t *left,
    const wl_plan_op_t *op)
{
    uint32_t width = op && op->project_count > 0 ? op->project_count
        : left->ncols;
    wirelog_column_type_t *types = (wirelog_column_type_t *)malloc(
        (size_t)width * sizeof(*types));
    if (width > 0 && !types)
        return ENOMEM;
    for (uint32_t c = 0; c < width; c++) {
        uint32_t source = op && op->project_count > 0
            ? op->project_indices[c] : c;
        types[c] = left->column_types
            ? left->column_types[source] : WIRELOG_TYPE_INT64;
    }
    int rc = col_rel_set_column_types(out, types, width);
    free(types);
    return rc;
}

static bool
col_join_key_types_compatible(const col_rel_t *left, const uint32_t *lk,
    const col_rel_t *right, const uint32_t *rk, uint32_t key_count)
{
    for (uint32_t k = 0; k < key_count; k++) {
        wirelog_column_type_t left_type = left->column_types
            ? left->column_types[lk[k]] : WIRELOG_TYPE_INT64;
        wirelog_column_type_t right_type = right->column_types
            ? right->column_types[rk[k]] : WIRELOG_TYPE_INT64;
        if ((left_type == WIRELOG_TYPE_FLOAT)
            != (right_type == WIRELOG_TYPE_FLOAT))
            return false;
    }
    return true;
}

static int
col_join_write_pair_at(col_rel_t *out, uint64_t out_row,
    const col_rel_t *left, uint32_t lr, const col_rel_t *right, uint32_t rr,
    const uint32_t *project_indices, uint32_t project_count)
{
    if (project_count > 0 && project_indices) {
        for (uint32_t c = 0; c < project_count; c++) {
            int rc = col_rel_set(out, (uint32_t)out_row, c,
                    col_join_pair_value(left, lr, right, rr,
                    project_indices[c]));
            if (rc != 0)
                return rc;
        }
    } else {
        for (uint32_t c = 0; c < left->ncols; c++) {
            int rc = col_rel_set(out, (uint32_t)out_row, c,
                    left->columns[c][lr]);
            if (rc != 0)
                return rc;
        }
        for (uint32_t c = 0; c < right->ncols; c++) {
            int rc = col_rel_set(out, (uint32_t)out_row, left->ncols + c,
                    right->columns[c][rr]);
            if (rc != 0)
                return rc;
        }
    }
    return 0;
}

static bool
col_join_pair_cache_append(col_join_keyed_ctx_t *ctx, uint32_t lr,
    uint32_t rr)
{
    if (!ctx->pairs_complete)
        return false;
    if (ctx->pair_cap_limit == 0) {
        ctx->pairs_complete = false;
        return false;
    }
    if (ctx->pair_count == ctx->pair_cap) {
        uint32_t new_cap = ctx->pair_cap ? ctx->pair_cap * 2u : 1024u;
        if (new_cap <= ctx->pair_cap) {
            ctx->pairs_complete = false;
            return false;
        }
        if (ctx->pair_cap_limit > 0 && new_cap > ctx->pair_cap_limit)
            new_cap = ctx->pair_cap_limit;
        size_t max_pairs = SIZE_MAX / sizeof(col_join_pair_ref_t);
        if (new_cap <= ctx->pair_cap || (size_t)new_cap > max_pairs) {
            free(ctx->pairs);
            ctx->pairs = NULL;
            ctx->pair_count = 0;
            ctx->pair_cap = 0;
            ctx->pairs_complete = false;
            return false;
        }
        col_join_pair_ref_t *new_pairs = (col_join_pair_ref_t *)realloc(
            ctx->pairs, (size_t)new_cap * sizeof(col_join_pair_ref_t));
        if (!new_pairs) {
            free(ctx->pairs);
            ctx->pairs = NULL;
            ctx->pair_count = 0;
            ctx->pair_cap = 0;
            ctx->pairs_complete = false;
            return false;
        }
        ctx->pairs = new_pairs;
        ctx->pair_cap = new_cap;
    }
    ctx->pairs[ctx->pair_count].lr = lr;
    ctx->pairs[ctx->pair_count].rr = rr;
    ctx->pair_count++;
    return true;
}

static void
col_join_keyed_count_worker_fn(void *arg)
{
    col_join_keyed_ctx_t *ctx = (col_join_keyed_ctx_t *)arg;
    const col_rel_t *left = ctx->left;
    const col_rel_t *right = ctx->right;
    const col_diff_arrangement_t *darr = ctx->darr;
    uint64_t count = 0;
    uint64_t reported = 0;

    for (uint32_t lr = ctx->begin; lr < ctx->end
        && (!ctx->stop || !atomic_load_explicit(ctx->stop,
        memory_order_relaxed)); lr++) {
        uint32_t h = col_join_hash_rel_keys(left, lr, ctx->lk, ctx->kc)
            & (darr->nbuckets - 1);
        if (ctx->left_hashes)
            ctx->left_hashes[lr] = h;
        for (uint32_t e = darr->ht_head[h]; e != 0;
            e = darr->ht_next[e - 1]) {
            if (ctx->stop && atomic_load_explicit(ctx->stop,
                memory_order_relaxed))
                break;
            uint32_t rr = e - 1;
            if (col_join_keys_match_rel(left, lr, ctx->lk, right, rr, ctx->rk,
                ctx->kc)) {
                (void)col_join_pair_cache_append(ctx, lr, rr);
                count++;
                if (ctx->limit > 0 && ctx->shared_count
                    && (count - reported) >= 1024u) {
                    uint64_t seen = atomic_fetch_add_explicit(
                        ctx->shared_count, 1024u, memory_order_relaxed)
                        + 1024u;
                    reported += 1024u;
                    if (seen >= ctx->limit && ctx->stop) {
                        atomic_store_explicit(ctx->stop, true,
                            memory_order_relaxed);
                        break;
                    }
                }
            }
        }
    }
    if (count > reported && ctx->shared_count) {
        uint64_t delta = count - reported;
        uint64_t seen = atomic_fetch_add_explicit(ctx->shared_count, delta,
                memory_order_relaxed) + delta;
        if (ctx->limit > 0 && seen >= ctx->limit && ctx->stop)
            atomic_store_explicit(ctx->stop, true, memory_order_relaxed);
    }
    ctx->count = count;
}

static void
col_join_keyed_fill_worker_fn(void *arg)
{
    col_join_keyed_ctx_t *ctx = (col_join_keyed_ctx_t *)arg;
    const col_rel_t *left = ctx->left;
    const col_rel_t *right = ctx->right;
    const col_diff_arrangement_t *darr = ctx->darr;
    uint64_t out_row = ctx->out_begin;

    if (ctx->pairs_complete && (uint64_t)ctx->pair_count == ctx->count) {
        for (uint32_t i = 0; i < ctx->pair_count; i++) {
            ctx->rc = col_join_write_pair_at(ctx->out, out_row, left,
                    ctx->pairs[i].lr, right, ctx->pairs[i].rr,
                    ctx->op->project_indices, ctx->op->project_count);
            if (ctx->rc != 0)
                return;
            out_row++;
        }
        return;
    }

    for (uint32_t lr = ctx->begin; lr < ctx->end; lr++) {
        uint32_t h = ctx->left_hashes ? ctx->left_hashes[lr]
            : (col_join_hash_rel_keys(left, lr, ctx->lk, ctx->kc)
            & (darr->nbuckets - 1));
        for (uint32_t e = darr->ht_head[h]; e != 0;
            e = darr->ht_next[e - 1]) {
            uint32_t rr = e - 1;
            if (col_join_keys_match_rel(left, lr, ctx->lk, right, rr, ctx->rk,
                ctx->kc)) {
                ctx->rc = col_join_write_pair_at(ctx->out, out_row, left, lr,
                        right, rr, ctx->op->project_indices,
                        ctx->op->project_count);
                if (ctx->rc != 0)
                    return;
                out_row++;
            }
        }
    }
}

static bool
col_semijoin_row_found(const col_semijoin_ctx_t *ctx, uint32_t lr)
{
    const col_rel_t *left = ctx->left;
    const col_rel_t *right = ctx->right;
    uint32_t h = ctx->left_hashes ? ctx->left_hashes[lr]
        : (col_join_hash_rel_keys(left, lr, ctx->lk, ctx->kc)
        & (ctx->nbuckets - 1));

    for (uint32_t e = ctx->ht_head[h]; e != 0; e = ctx->ht_next[e - 1]) {
        uint32_t rr = e - 1;
        if (col_join_keys_match_rel(left, lr, ctx->lk, right, rr, ctx->rk,
            ctx->kc))
            return true;
    }
    return false;
}

static void
col_semijoin_count_worker_fn(void *arg)
{
    col_semijoin_ctx_t *ctx = (col_semijoin_ctx_t *)arg;
    uint64_t count = 0;

    for (uint32_t lr = ctx->begin; lr < ctx->end; lr++) {
        if (ctx->left_hashes)
            ctx->left_hashes[lr] = col_join_hash_rel_keys(ctx->left, lr,
                    ctx->lk, ctx->kc) & (ctx->nbuckets - 1);
        if (col_semijoin_row_found(ctx, lr))
            count++;
    }
    ctx->count = count;
}

static void
col_semijoin_fill_worker_fn(void *arg)
{
    col_semijoin_ctx_t *ctx = (col_semijoin_ctx_t *)arg;
    const col_rel_t *left = ctx->left;
    col_rel_t *out = ctx->out;
    uint64_t out_row = ctx->out_begin;
    uint32_t ocols = ctx->op->project_count ? ctx->op->project_count
        : left->ncols;

    for (uint32_t lr = ctx->begin; lr < ctx->end; lr++) {
        if (atomic_load_explicit(ctx->write_error, memory_order_relaxed) != 0)
            return;
        if (!col_semijoin_row_found(ctx, lr))
            continue;
        if (ctx->op->project_count > 0 && ctx->op->project_indices) {
            for (uint32_t c = 0; c < ocols; c++) {
                uint32_t si = ctx->op->project_indices[c];
                int rc = col_rel_set(out, (uint32_t)out_row, c,
                        (si < left->ncols) ? left->columns[si][lr] : 0);
                if (rc != 0) {
                    atomic_store_explicit(ctx->write_error, rc,
                        memory_order_relaxed);
                    return;
                }
            }
        } else {
            for (uint32_t c = 0; c < left->ncols; c++) {
                int rc = col_rel_set(out, (uint32_t)out_row, c,
                        left->columns[c][lr]);
                if (rc != 0) {
                    atomic_store_explicit(ctx->write_error, rc,
                        memory_order_relaxed);
                    return;
                }
            }
        }
        out_row++;
    }
}

static int64_t
col_join_pair_value(const col_rel_t *left, uint32_t lr, const col_rel_t *right,
    uint32_t rr, uint32_t idx)
{
    if (idx < left->ncols)
        return left->columns[idx][lr];
    idx -= left->ncols;
    return idx < right->ncols ? right->columns[idx][rr] : 0;
}

static void
col_join_cross_fill_worker_fn(void *arg)
{
    col_join_cross_ctx_t *ctx = (col_join_cross_ctx_t *)arg;
    const col_rel_t *left = ctx->left;
    const col_rel_t *right = ctx->right;
    col_rel_t *out = ctx->out;
    uint64_t right_rows = right->nrows;

    uint64_t oi = ctx->begin;
    uint32_t lr = (uint32_t)(ctx->begin / right_rows);
    uint32_t rpos = (uint32_t)(ctx->begin % right_rows);
    while (oi < ctx->end) {
        if (atomic_load_explicit(ctx->write_error, memory_order_relaxed) != 0)
            return;
        uint32_t rr = right->nrows - 1u - rpos;
        if (ctx->project_count > 0 && ctx->project_indices) {
            for (uint32_t c = 0; c < ctx->project_count; c++) {
                int rc = col_rel_set(out, (uint32_t)oi, c,
                        col_join_pair_value(left, lr, right, rr,
                        ctx->project_indices[c]));
                if (rc != 0) {
                    atomic_store_explicit(ctx->write_error, rc,
                        memory_order_relaxed);
                    return;
                }
            }
        } else {
            for (uint32_t c = 0; c < left->ncols; c++) {
                int rc = col_rel_set(out, (uint32_t)oi, c,
                        left->columns[c][lr]);
                if (rc != 0) {
                    atomic_store_explicit(ctx->write_error, rc,
                        memory_order_relaxed);
                    return;
                }
            }
            for (uint32_t c = 0; c < right->ncols; c++) {
                int rc = col_rel_set(out, (uint32_t)oi, left->ncols + c,
                        right->columns[c][rr]);
                if (rc != 0) {
                    atomic_store_explicit(ctx->write_error, rc,
                        memory_order_relaxed);
                    return;
                }
            }
        }
        oi++;
        rpos++;
        if (rpos == right->nrows) {
            rpos = 0;
            lr++;
        }
    }
}

static int
col_join_parallel_cross(wl_col_session_t *sess, const col_rel_t *left,
    const col_rel_t *right, const wl_plan_op_t *op, col_rel_t **outp,
    int *out_overflow)
{
    if (!sess || !left || !right || !outp || !*outp)
        return EINVAL;
    if (sess->num_workers <= 1 || right->nrows == 0)
        return EINVAL;

    uint64_t total = (uint64_t)left->nrows * (uint64_t)right->nrows;
    uint64_t emit_total = total;
    if (sess->join_output_limit > 0 && emit_total >= sess->join_output_limit) {
        emit_total = sess->join_output_limit;
        *out_overflow = 1;
    }
    if (emit_total > UINT32_MAX)
        return ENOMEM;

    uint32_t nrows = (uint32_t)emit_total;
    uint32_t active_workers = sess->num_workers > emit_total
        ? (uint32_t)emit_total
        : sess->num_workers;
    if (active_workers <= 1)
        return EINVAL;
    int ensure_rc = wl_columnar_session_ensure_workqueue(sess, active_workers);
    if (ensure_rc != 0)
        return ensure_rc;

    uint32_t W = active_workers;
    col_rel_t *out = col_rel_new_auto("$join",
            col_join_output_width(left, right, op));
    if (!out)
        return ENOMEM;
    if (col_join_set_output_types(out, left, right, op) != 0) {
        col_rel_destroy(out);
        return ENOMEM;
    }
    col_join_attach_ledger(sess, out);
    if (col_join_reserve_exact(out, nrows) != 0) {
        col_rel_destroy(out);
        return ENOMEM;
    }
    out->nrows = nrows;

    col_join_cross_ctx_t *ctxs = (col_join_cross_ctx_t *)calloc(
        W, sizeof(col_join_cross_ctx_t));
    if (!ctxs) {
        col_rel_destroy(out);
        return ENOMEM;
    }

    uint64_t chunk = (emit_total + W - 1u) / W;
    atomic_int write_error = ATOMIC_VAR_INIT(0);
    int rc = 0;
    for (uint32_t w = 0; w < W; w++) {
        uint64_t begin = (uint64_t)w * chunk;
        uint64_t end = begin + chunk;
        if (begin > emit_total)
            begin = emit_total;
        if (end > emit_total)
            end = emit_total;
        ctxs[w].left = left;
        ctxs[w].right = right;
        ctxs[w].out = out;
        ctxs[w].project_indices = op ? op->project_indices : NULL;
        ctxs[w].project_count = op ? op->project_count : 0;
        ctxs[w].begin = begin;
        ctxs[w].end = end;
        ctxs[w].write_error = &write_error;
        if (wl_workqueue_submit(sess->wq, col_join_cross_fill_worker_fn,
            &ctxs[w]) != 0) {
            rc = ENOMEM;
            break;
        }
    }
    wl_workqueue_wait_all(sess->wq);
    if (rc == 0)
        rc = atomic_load_explicit(&write_error, memory_order_relaxed);
    free(ctxs);
    if (rc != 0) {
        col_rel_destroy(out);
        return rc;
    }

    /* The placeholder was ledger-accounted before selecting this fast path;
     * retain its ledger while destroying it so the initial allocation is
     * released. */
    col_rel_destroy(*outp);
    *outp = out;
    return 0;
}

static WL_OPS_ALWAYS_INLINE uint32_t
col_join_hash_rel_keys(const col_rel_t *rel, uint32_t row,
    const uint32_t *key_cols, uint32_t kc)
{
    uint32_t h = 2166136261u;
    for (uint32_t i = 0; i < kc; i++)
        h = wl_columnar_hash_value(h, rel, key_cols[i],
                rel->columns[key_cols[i]][row]);
    return h;
}

static WL_OPS_ALWAYS_INLINE bool
col_join_keys_match_rel(const col_rel_t *left, uint32_t lr,
    const uint32_t *lk, const col_rel_t *right, uint32_t rr,
    const uint32_t *rk, uint32_t kc)
{
    for (uint32_t k = 0; k < kc; k++) {
        if (!wl_columnar_value_equal(left, lk[k], left->columns[lk[k]][lr],
            right, rk[k], right->columns[rk[k]][rr]))
            return false;
    }
    return true;
}

static int
col_join_append_pair(col_rel_t *out, const col_rel_t *left, uint32_t lr,
    const col_rel_t *right, uint32_t rr, const uint32_t *project_indices,
    uint32_t project_count, int64_t *fallback_row)
{
    if (out->nrows < out->capacity) {
        uint32_t out_row = out->nrows;
        if (out->timestamps)
            memset(&out->timestamps[out_row], 0,
                sizeof(col_delta_timestamp_t));
        if (project_count > 0 && project_indices) {
            for (uint32_t c = 0; c < project_count; c++) {
                int rc = col_rel_set(out, out_row, c,
                        col_join_pair_value(left, lr, right, rr,
                        project_indices[c]));
                if (rc != 0)
                    return rc;
            }
        } else {
            for (uint32_t c = 0; c < left->ncols; c++) {
                int rc = col_rel_set(out, out_row, c, left->columns[c][lr]);
                if (rc != 0)
                    return rc;
            }
            for (uint32_t c = 0; c < right->ncols; c++) {
                int rc = col_rel_set(out, out_row, left->ncols + c,
                        right->columns[c][rr]);
                if (rc != 0)
                    return rc;
            }
        }
        out->nrows++;
        return 0;
    }

    if (project_count > 0 && project_indices) {
        for (uint32_t c = 0; c < project_count; c++)
            fallback_row[c] = col_join_pair_value(left, lr, right, rr,
                    project_indices[c]);
    } else {
        for (uint32_t c = 0; c < left->ncols; c++)
            fallback_row[c] = left->columns[c][lr];
        for (uint32_t c = 0; c < right->ncols; c++)
            fallback_row[left->ncols + c] = right->columns[c][rr];
    }
    return col_rel_append_row(out, fallback_row);
}

int
wl_columnar_join_op(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    eval_entry_t left_e = eval_stack_pop(stack);
    if (!left_e.rel)
        return EINVAL;

    /* right_filtered: non-NULL only when right was pool-allocated by
     * wl_columnar_filter_apply_right_filter (non-cached path: antijoin/semijoin callers).
     * For col_op_join we use wl_columnar_filter_apply_right_filter_cached; the cache owns
     * the filtered relation and we must NOT destroy it here. */
    col_rel_t *right_filtered = NULL;
    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right) {
        /* If right relation doesn't exist, join produces empty result (cross-product with nothing).
         * Similar to ANTIJOIN logic (which keeps all left rows on missing right).
         * This can occur in generated plans where optional relations may not exist. */
        uint32_t empty_cols = (op->project_count > 0 && op->project_indices)
            ? op->project_count : left_e.rel->ncols;
        col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool,
                sess->eval_arena, "$join_empty", empty_cols);
        if (!out) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        if (col_join_set_left_output_types(out, left_e.rel, op) != 0) {
            col_rel_destroy(out);
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        if (left_e.owned)
            col_rel_destroy(left_e.rel);
        return eval_stack_push_delta(stack, out, true, false);
    }

#ifdef WL_PROFILE
    uint64_t _t0_join = now_ns();
    sess->profile.join_calls++;
#endif

    /* Right-side delta substitution controlled by delta_mode:
     * FORCE_DELTA: always substitute delta of right if available; if no
     *              delta exists, short-circuit with an empty result (this
     *              rule copy produces no tuples from this permutation).
     * FORCE_FULL:  never substitute delta; always use full right.
     * AUTO:        heuristic -- substitute delta when left is not already
     *              a delta and right-delta is strictly smaller than full. */
    bool used_right_delta = false;
    if (op->delta_mode == WL_DELTA_FORCE_DELTA && op->right_relation) {
        char rdname[256];
        snprintf(rdname, sizeof(rdname), "$d$%s", op->right_relation);
        col_rel_t *rdelta = session_find_rel(sess, rdname);
        /* Issue #472: mirror VARIABLE op retraction-aware pattern —
         * fall back to $r$<name> when retraction_seeded at iteration 0. */
        if (!rdelta && sess->retraction_seeded
            && sess->current_iteration == 0) {
            if (retraction_rel_name(op->right_relation, rdname,
                sizeof(rdname)) == 0)
                rdelta = session_find_rel(sess, rdname);
        }
        if (rdelta && rdelta->nrows > 0) {
            right = rdelta;
            used_right_delta = true;
        } else if (sess->current_iteration > 0 || sess->delta_seeded
            || sess->retraction_seeded) {
            /* Iteration > 0, delta-seeded iter 0 (issue #83), or
             * retraction-seeded iter 0 (issue #472):
             * FORCE_DELTA required but delta absent/empty. Short-circuit to
             * empty result — this rule copy produces no tuples from this
             * permutation (correct semi-naive, issue #85). */
            uint32_t ocols = col_join_output_width(left_e.rel, right, op);
            col_rel_t *empty = col_rel_new_auto("$join_empty", ocols);
            if (!empty) {
                if (left_e.owned)
                    col_rel_destroy(left_e.rel);
                return ENOMEM;
            }
            if (col_join_set_output_types(empty, left_e.rel, right, op) != 0) {
                col_rel_destroy(empty);
                if (left_e.owned)
                    col_rel_destroy(left_e.rel);
                return ENOMEM;
            }
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            int push_rc = eval_stack_push(stack, empty, true);
            if (push_rc != 0)
                col_rel_destroy(empty);
            return push_rc;
        }
        /* else: iteration 0 — no deltas yet, fall through to full right */
    } else if (op->delta_mode != WL_DELTA_FORCE_FULL && op->right_relation
        && (!left_e.is_delta || (sess->tdd_outbound_only_active
        && sess->current_iteration > 0))) {
        /* AUTO: original heuristic */
        char rdname[256];
        snprintf(rdname, sizeof(rdname), "$d$%s", op->right_relation);
        col_rel_t *rdelta = session_find_rel(sess, rdname);
        if (rdelta && (((rdelta->nrows > 0
            && rdelta->nrows < right->nrows) || (rdelta->nrows > 0
            && sess->tdd_subpass_active))
            || (sess->tdd_outbound_only_active
            && sess->current_iteration > 0))) {
            right = rdelta;
            used_right_delta = true;
        }
    }
    /* Issue #472: Retraction right-pass — when retraction_right_pass is set,
     * use the $r$ retraction delta on the right side so that self-join rules
     * derive retractions from full(left) x $r$(right). */
    if (!used_right_delta && sess->retraction_right_pass
        && sess->current_iteration == 0 && op->right_relation) {
        char rdname[256];
        if (retraction_rel_name(op->right_relation, rdname,
            sizeof(rdname)) == 0) {
            col_rel_t *rdelta = session_find_rel(sess, rdname);
            if (rdelta && rdelta->nrows > 0) {
                right = rdelta;
                used_right_delta = true;
            }
        }
    }

    /* Apply constant filter on right child (from FILTER wrappers collected
     * during plan generation).  Use session-level cache (Issue #386): the
     * filtered relation is owned by sess->filt_cache and must NOT be
     * destroyed here.  right_filtered remains NULL for the cached path. */
    if (op->right_filter_expr.size > 0 && op->right_relation
        && !used_right_delta) {
        col_rel_t *filtered = wl_columnar_filter_apply_right_filter_cached(sess,
                &op->right_filter_expr, op->right_relation, right);
        if (!filtered) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        right = filtered;
        /* right_filtered stays NULL: cache owns the relation */
    } else if (op->right_filter_expr.size > 0) {
        /* Delta path or no relation name: fall back to pool-allocated filter */
        col_rel_t *filtered =
            wl_columnar_filter_apply_right_filter(&op->right_filter_expr, right,
                sess->delta_pool, sess->intern);
        if (!filtered) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        right = filtered;
        right_filtered = filtered;
    }

    /* Materialization cache: reuse previous join result when available.
     * Works with both stable (borrowed) and worker-owned relations since
     * the cache key is based on content hash, not ownership. This enables
     * cache reuse in K-fusion worker sessions, eliminating redundant joins. */
    bool projected_join = op->project_count > 0 && op->project_indices;
    if (op->materialized && !projected_join) {
        col_rel_t *cached
            = col_mat_cache_lookup(&sess->mat_cache, left_e.rel, right);
        if (cached) {
#ifdef WL_PROFILE
            sess->profile.join_cache_hit_ns += now_ns() - _t0_join;
#endif
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return eval_stack_push_delta(stack, cached, false,
                       left_e.is_delta || used_right_delta);
        }
    }

    uint32_t kc = op->key_count;
    col_rel_t *left = left_e.rel;

    /* Resolve key column positions */
    uint32_t *lk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    uint32_t *rk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    if (!lk || !rk) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    for (uint32_t k = 0; k < kc; k++) {
        lk[k] = col_op_resolve_key(left,
                op->left_keys ? op->left_keys[k] : NULL, "left", "JOIN",
                op->right_relation);
        rk[k] = col_op_resolve_key(right,
                op->right_keys ? op->right_keys[k] : NULL, "right", "JOIN",
                op->right_relation);
    }
    if (!col_join_key_types_compatible(left, lk, right, rk, kc)) {
        free(lk);
        free(rk);
        if (right_filtered)
            col_rel_destroy(right_filtered);
        if (left_e.owned)
            col_rel_destroy(left);
        return EINVAL;
    }

    uint32_t ocols = col_join_output_width(left, right, op);
    /* Materialized results outlive the current delta-pool reset while they
     * remain in mat_cache, so cache-owned joins must be heap allocated. */
    col_rel_t *out = (op->materialized && !projected_join)
        ? col_rel_new_auto("$join", ocols)
        : col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
            "$join", ocols);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    if (col_join_set_output_types(out, left, right, op) != 0) {
        free(lk);
        free(rk);
        col_rel_destroy(out);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    /* Attach ledger so row growth is tracked under RELATION subsystem */
    col_join_attach_ledger(sess, out);

    /* Backpressure check (Issue #224): when RELATION subsystem reaches >= 80%
     * of its budget, skip row generation and push an empty result instead of
     * risking EOVERFLOW (rc=84).  Evaluation continues with gracefully
     * degraded (incomplete) results rather than failing entirely.
     *
     * TDD workers (coordinator != NULL) skip this pre-join check (Issue #404):
     * returning empty results before row generation causes silent correctness
     * bugs — zero join output leads to premature fixed-point convergence.
     * Workers still have in-loop backpressure + join_output_limit as hard
     * safety nets.  Coordinator sessions retain full pre-join protection. */
    if (wl_mem_ledger_should_backpressure(&sess->mem_ledger,
        WL_MEM_SUBSYS_RELATION, 80)
        && !sess->coordinator) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return eval_stack_push(stack, out, true);
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }

    /* BOOLEAN SPECIALIZATION (Issue #62): Fast-path for unary relations.
     * When right relation is unary (ncols == 1) and single join key,
     * use O(1) hash-set membership test instead of merge-join.
     * Profiling shows 37.7% of DOOP-class joins are unary; this path
     * provides ~30-40% speedup for such workloads. */
    bool right_is_unary = (right->ncols == 1);
    bool left_is_unary = (left->ncols == 1);
#ifdef WL_PROFILE
    if ((right_is_unary || left_is_unary) && kc == 1)
        sess->profile.join_unary++;
#endif
    if ((right_is_unary || left_is_unary) && kc == 1) {
        /* build: unary side as hash set; probe: non-unary side iterated.
         * When both are unary, right is preferred as build side. */
        col_rel_t *build = right_is_unary ? right : left;
        col_rel_t *probe = right_is_unary ? left : right;
        uint32_t build_kcol = right_is_unary ? rk[0] : lk[0];
        uint32_t probe_kcol = right_is_unary ? lk[0] : rk[0];

        /* Build hash set from the unary relation's single column. */
        uint32_t nbuckets;
        if (col_join_bucket_count(build->nrows, &nbuckets) != 0) {
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (left_e.owned)
                col_rel_destroy(left);
            return ENOMEM;
        }
        uint32_t *ht_head = (uint32_t *)calloc(nbuckets, sizeof(uint32_t));
        uint32_t *ht_next = (uint32_t *)malloc(
            (build->nrows > 0 ? build->nrows : 1) * sizeof(uint32_t));
        if (!ht_head || !ht_next) {
            free(ht_head);
            free(ht_next);
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (left_e.owned)
                col_rel_destroy(left);
            return ENOMEM;
        }
        for (uint32_t bi = 0; bi < build->nrows; bi++) {
            uint32_t h = col_join_hash_rel_keys(build, bi, &build_kcol, 1);
            h &= (nbuckets - 1);
            ht_next[bi] = ht_head[h];
            ht_head[h] = bi + 1; /* 1-based; 0 = end of chain */
        }

        /* Probe: iterate non-unary side, test membership in hash set. */
        int join_rc = 0;
        for (uint32_t pr = 0; pr < probe->nrows && join_rc == 0; pr++) {
            int64_t pkey = probe->columns[probe_kcol][pr];
            uint32_t h = col_join_hash_rel_keys(probe, pr, &probe_kcol, 1);
            h &= (nbuckets - 1);

            for (uint32_t e = ht_head[h]; e != 0; e = ht_next[e - 1]) {
                uint32_t bi = e - 1;
                int64_t bkey
                    = col_rel_get(build, bi, build_kcol);
                if (!wl_columnar_value_equal(build, build_kcol, bkey,
                    probe, probe_kcol, pkey))
                    continue;
                uint32_t lr = right_is_unary ? pr : bi;
                uint32_t rr = right_is_unary ? bi : pr;
                join_rc = col_join_append_pair(out, left, lr, right, rr,
                        op->project_indices, op->project_count, tmp);
                if (join_rc != 0) {
                    fprintf(stderr,
                        "ERROR: col_rel_append_row failed with rc=%d at "
                        "unary join\n",
                        join_rc);
                    break;
                }
                if (col_join_output_limit_reached(sess, out)
                    || col_join_inloop_backpressure(sess, out)) {
                    fprintf(stderr,
                        "join output limit reached: %u rows "
                        "(limit=%llu)\n",
                        out->nrows,
                        (unsigned long long)sess->join_output_limit);
                    join_rc = EOVERFLOW;
                    break;
                }
            }
        }

        free(ht_head);
        free(ht_next);
        if (join_rc != 0) {
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (left_e.owned)
                col_rel_destroy(left);
            return join_rc;
        }
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
            "Unary join completed, out->nrows=%u",
            out->nrows);
    } else {
        /* Standard merge-join for non-unary relations. */
        /* Hash join: use persistent arrangement for the full right relation;
         * fall back to an ephemeral hash table for delta substitution or when
         * the arrangement cannot be allocated. */
        col_arrangement_t *arr = NULL;
        uint32_t nbuckets_ep = 0;
        uint32_t *ht_head_ep = NULL;
        uint32_t *ht_next_ep = NULL;

        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
            "Standard merge-join starting - left=%u rows, right=%u rows, kc=%u",
            left->nrows, right->nrows, kc);

        if (!used_right_delta && op->right_relation && kc > 0) {
            if (op->right_filter_expr.size == 0) {
                arr = col_session_get_arrangement(&sess->base,
                        op->right_relation, rk, kc);
            } else {
                /* Issue #433: filtered right arrangement cache.
                 * `right` is the cached filtered relation from filt_cache;
                 * filt_arr persists across sub-passes to avoid ephemeral
                 * hash table rebuild on every semi-naive iteration. */
                if (!sess->coordinator) {
                    uint64_t fhash =
                        wl_columnar_filter_fnv1a_hash(
                        op->right_filter_expr.data,
                        op->right_filter_expr.size);
                    arr = col_session_get_filt_arrangement(sess,
                            op->right_relation, fhash, right, rk, kc);
                }
            }
        } else if (used_right_delta && op->right_relation && kc > 0)
            arr = col_session_get_delta_arrangement(sess, op->right_relation,
                    right, rk, kc);

        if (arr)
            WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                "Using persistent arrangement");
        else
            WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                "No arrangement available, will use ephemeral hash table");

        if (!arr) {
            /* Ephemeral hash table (delta path or arrangement unavailable). */
            if (col_join_bucket_count(right->nrows, &nbuckets_ep) != 0) {
                free(tmp);
                col_rel_destroy(out);
                free(lk);
                free(rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                if (left_e.owned)
                    col_rel_destroy(left);
                return ENOMEM;
            }
            ht_head_ep = (uint32_t *)calloc(nbuckets_ep, sizeof(uint32_t));
            ht_next_ep = (uint32_t *)malloc(
                (right->nrows > 0 ? right->nrows : 1) * sizeof(uint32_t));
            if (!ht_head_ep || !ht_next_ep) {
                fprintf(stderr,
                    "ERROR: Ephemeral hash table allocation failed "
                    "(nbuckets=%u)\n",
                    nbuckets_ep);
                free(ht_head_ep);
                free(ht_next_ep);
                free(tmp);
                col_rel_destroy(out);
                free(lk);
                free(rk);
                if (left_e.owned)
                    col_rel_destroy(left);
                return ENOMEM;
            }
            WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                "Ephemeral hash table created - nbuckets=%u",
                nbuckets_ep);
            for (uint32_t rr = 0; rr < right->nrows; rr++) {
                uint32_t h = col_join_hash_rel_keys(right, rr, rk, kc)
                    & (nbuckets_ep - 1);
                ht_next_ep[rr] = ht_head_ep[h];
                ht_head_ep[h] = rr + 1; /* 1-based; 0 = end of chain */
            }
            WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                "Ephemeral hash table built successfully");
        }

        /* key_row scratch buffer for arrangement probe: values placed at rk[]
         * positions so col_arrangement_find_first() matches correctly. */
        int64_t *key_row = NULL;
        if (arr) {
            key_row = (int64_t *)malloc(
                sizeof(int64_t) * (right->ncols > 0 ? right->ncols : 1));
            if (!key_row) {
                free(ht_head_ep);
                free(ht_next_ep);
                free(tmp);
                col_rel_destroy(out);
                free(lk);
                free(rk);
                if (left_e.owned)
                    col_rel_destroy(left);
                return ENOMEM;
            }
        }

        int join_rc = 0;
        int join_overflow = 0;
        if (kc == 0 && col_join_should_parallelize_cross(sess, left, right)) {
            join_rc = col_join_parallel_cross(sess, left, right, op, &out,
                    &join_overflow);
            if (join_rc == EINVAL)
                join_rc = 0;
            else if (join_rc == 0 && join_overflow)
                join_rc = EOVERFLOW;
        } else for (uint32_t lr = 0; lr < left->nrows && join_rc == 0; lr++) {
                if (arr) {
                    /* Arrangement probe: fill key_row at right-side positions. */
                    for (uint32_t k = 0; k < kc; k++)
                        key_row[rk[k]] = left->columns[lk[k]][lr];
                    uint32_t rr = col_arrangement_find_first_typed(arr,
                            right, key_row);
                    while (rr != UINT32_MAX && join_rc == 0) {
                        /* Verify key match: find_next may return collision rows. */
                        if (col_join_keys_match_rel(left, lr, lk, right, rr, rk,
                            kc)) {
                            join_rc = col_join_append_pair(out, left, lr, right,
                                    rr, op->project_indices, op->project_count,
                                    tmp);
                            if (join_rc != 0) {
                                fprintf(stderr,
                                    "ERROR: col_rel_append_row failed in "
                                    "arrangement probe with rc=%d\n",
                                    join_rc);
                                break;
                            }
                            if (col_join_output_limit_reached(sess, out)
                                || col_join_inloop_backpressure(sess, out)) {
                                fprintf(
                                    stderr,
                                    "join output limit reached: %u rows "
                                    "(limit=%llu)\n",
                                    out->nrows,
                                    (unsigned long long)sess->join_output_limit);
                                join_rc = EOVERFLOW;
                            }
                        }
                        rr = col_arrangement_find_next(arr, rr);
                    }
                } else {
                    /* Ephemeral hash probe. */
                    uint32_t h = col_join_hash_rel_keys(left, lr, lk, kc)
                        & (nbuckets_ep - 1);
                    for (uint32_t e = ht_head_ep[h]; e != 0;
                        e = ht_next_ep[e - 1]) {
                        uint32_t rr = e - 1;
                        if (!col_join_keys_match_rel(left, lr, lk, right, rr,
                            rk,
                            kc))
                            continue;
                        join_rc = col_join_append_pair(out, left, lr, right, rr,
                                op->project_indices, op->project_count, tmp);
                        if (join_rc != 0) {
                            fprintf(stderr,
                                "ERROR: col_rel_append_row failed in ephemeral "
                                "hash probe with rc=%d\n",
                                join_rc);
                            break;
                        }
                        if (col_join_output_limit_reached(sess, out)
                            || col_join_inloop_backpressure(sess, out)) {
                            fprintf(stderr,
                                "join output limit reached: %u rows "
                                "(limit=%llu)\n",
                                out->nrows,
                                (unsigned long long)sess->join_output_limit);
                            join_rc = EOVERFLOW;
                            break;
                        }
                    }
                }
            }

        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
            "Merge-join loop completed, out->nrows=%u, rc=%d",
            out->nrows, join_rc);

        free(key_row);
        free(ht_head_ep);
        free(ht_next_ep);
        if (join_rc != 0) {
            WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                "Merge-join failed with rc=%d, out->nrows=%u",
                join_rc, out->nrows);
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (left_e.owned)
                col_rel_destroy(left);
            return join_rc;
        }
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG, "Merge-join succeeded");
    }

    free(tmp);
    free(lk);
    free(rk);
    /* Propagate delta flag: result is a delta if left was delta OR we used
     * right-delta. This ensures subsequent JOINs in the same rule plan know
     * whether to apply right-delta (they should NOT if we already used one). */
    bool result_is_delta = projected_join ? false
        : (left_e.is_delta || used_right_delta);

    /* Populate materialization cache when hint is set.
     * Works with both stable and worker-owned relations.
     * Cache takes ownership of out; we push a borrowed reference.
     * This enables K-fusion workers to cache and reuse intermediate joins,
     * reducing redundant computation across the K worker copies. */
    if (op->materialized && !projected_join) {
        col_mat_cache_insert(&sess->mat_cache, left, right, out);
#ifdef WL_PROFILE
        if (out->nrows == 0)
            sess->profile.join_empty_out++;
        sess->profile.join_compute_ns += now_ns() - _t0_join;
#endif
        if (right_filtered)
            col_rel_destroy(right_filtered);
        if (left_e.owned)
            col_rel_destroy(left);
        return eval_stack_push_delta(stack, out, false, result_is_delta);
    }
    if (left_e.owned)
        col_rel_destroy(left);
#ifdef WL_PROFILE
    if (out->nrows == 0)
        sess->profile.join_empty_out++;
    sess->profile.join_compute_ns += now_ns() - _t0_join;
#endif
    if (right_filtered)
        col_rel_destroy(right_filtered);
    return eval_stack_push_delta(stack, out, true, result_is_delta);
}

int
wl_columnar_antijoin_op(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    eval_entry_t left_e = eval_stack_pop(stack);
    if (!left_e.rel)
        return EINVAL;

    col_rel_t *right_filtered = NULL;
    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right) {
        /* If right relation doesn't exist, antijoin keeps all left rows */
        return eval_stack_push(stack, left_e.rel, left_e.owned);
    }

    /* Issue #386: antijoin filter caching is not yet implemented.
     * Antijoin always uses an ephemeral pool-allocated filtered relation, so
     * the per-iteration filter cost is O(N) — acceptable for current workloads
     * but a candidate for follow-up optimization. */
    if (op->right_filter_expr.size > 0) {
        col_rel_t *filtered
            = wl_columnar_filter_apply_right_filter(&op->right_filter_expr,
                right,
                sess->delta_pool, sess->intern);
        if (!filtered) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        right = filtered;
        right_filtered = filtered;
    }

    col_rel_t *left = left_e.rel;
    uint32_t kc = op->key_count;

    uint32_t *lk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    uint32_t *rk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    if (!lk || !rk) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    for (uint32_t k = 0; k < kc; k++) {
        lk[k] = col_op_resolve_key(left,
                op->left_keys ? op->left_keys[k] : NULL, "left", "ANTIJOIN",
                op->right_relation);
        rk[k] = col_op_resolve_key(right,
                op->right_keys ? op->right_keys[k] : NULL, "right", "ANTIJOIN",
                op->right_relation);
    }
    if (!col_join_key_types_compatible(left, lk, right, rk, kc)) {
        free(lk);
        free(rk);
        if (right_filtered)
            col_rel_destroy(right_filtered);
        if (left_e.owned)
            col_rel_destroy(left);
        return EINVAL;
    }

    col_rel_t *out = col_rel_pool_new_like(sess->delta_pool, "$antijoin", left);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }

    /* Hash antijoin: build hash set from right, iterate left. */
    uint32_t aj_nbuckets;
    if (col_join_bucket_count(right->nrows, &aj_nbuckets) != 0) {
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (right_filtered)
            col_rel_destroy(right_filtered);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    uint32_t *aj_head = (uint32_t *)calloc(aj_nbuckets, sizeof(uint32_t));
    uint32_t *aj_next
        = (uint32_t *)malloc((right->nrows + 1) * sizeof(uint32_t));
    if (!aj_head || !aj_next) {
        free(aj_head);
        free(aj_next);
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    for (uint32_t rr = 0; rr < right->nrows; rr++) {
        uint32_t h = col_join_hash_rel_keys(right, rr, rk, kc)
            & (aj_nbuckets - 1);
        aj_next[rr] = aj_head[h];
        aj_head[h] = rr + 1;
    }
    int aj_rc = 0;
    int64_t *lrow_buf = (int64_t *)malloc(
        sizeof(int64_t) * (left->ncols ? left->ncols : 1));
    if (!lrow_buf) {
        aj_rc = ENOMEM;
        goto antijoin_done;
    }
    for (uint32_t lr = 0; lr < left->nrows && aj_rc == 0; lr++) {
        uint32_t h = col_join_hash_rel_keys(left, lr, lk, kc)
            & (aj_nbuckets - 1);
        bool found = false;
        for (uint32_t e = aj_head[h]; e != 0 && !found; e = aj_next[e - 1]) {
            uint32_t rr = e - 1;
            if (col_join_keys_match_rel(left, lr, lk, right, rr, rk, kc))
                found = true;
        }
        if (!found) {
            for (uint32_t c = 0; c < left->ncols; c++)
                lrow_buf[c] = left->columns[c][lr];
            aj_rc = col_rel_append_row(out, lrow_buf);
        }
    }
    free(lrow_buf);
antijoin_done:
    free(aj_head);
    free(aj_next);
    if (aj_rc != 0) {
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return aj_rc;
    }
    free(lk);
    free(rk);
    if (left_e.owned)
        col_rel_destroy(left);
    if (right_filtered)
        col_rel_destroy(right_filtered);
    return eval_stack_push(stack, out, true);
}

/* --- SEMIJOIN ------------------------------------------------------------ */

int
wl_columnar_semijoin_op(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    col_rel_t *right_filtered = NULL;
    eval_entry_t left_e = eval_stack_pop(stack);
    if (!left_e.rel)
        return EINVAL;

    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right)
        return eval_stack_push(stack, left_e.rel, left_e.owned);

    /* Issue #386: semijoin filter caching is not yet implemented.
     * Semijoin always uses an ephemeral pool-allocated filtered relation, so
     * the per-iteration filter cost is O(N) — acceptable for current workloads
     * but a candidate for follow-up optimization. */
    if (op->right_filter_expr.size > 0) {
        col_rel_t *filtered
            = wl_columnar_filter_apply_right_filter(&op->right_filter_expr,
                right,
                sess->delta_pool, sess->intern);
        if (!filtered) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        right = filtered;
        right_filtered = filtered;
    }

    col_rel_t *left = left_e.rel;
    uint32_t kc = op->key_count;

    uint32_t *lk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    uint32_t *rk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    if (!lk || !rk) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    for (uint32_t k = 0; k < kc; k++) {
        lk[k] = col_op_resolve_key(left,
                op->left_keys ? op->left_keys[k] : NULL, "left", "SEMIJOIN",
                op->right_relation);
        rk[k] = col_op_resolve_key(right,
                op->right_keys ? op->right_keys[k] : NULL, "right", "SEMIJOIN",
                op->right_relation);
    }
    if (!col_join_key_types_compatible(left, lk, right, rk, kc)) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return EINVAL;
    }

    /* Output: project_indices selects output columns from left */
    uint32_t ocols = op->project_count ? op->project_count : left->ncols;
    col_rel_t *out
        = col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena, "$semijoin",
            ocols);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    if (col_join_set_left_output_types(out, left, op) != 0) {
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }

    /* Build hash set from right relation join keys: O(|R|) */
    uint32_t nbuckets;
    if (col_join_bucket_count(right->nrows, &nbuckets) != 0) {
        free(tmp);
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (right_filtered)
            col_rel_destroy(right_filtered);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    uint32_t *ht_head = (uint32_t *)calloc(nbuckets, sizeof(uint32_t));
    uint32_t *ht_next = (uint32_t *)malloc((right->nrows > 0 ? right->nrows : 1)
            * sizeof(uint32_t));
    if (!ht_head || !ht_next) {
        free(ht_head);
        free(ht_next);
        free(tmp);
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    for (uint32_t rr = 0; rr < right->nrows; rr++) {
        uint32_t h = col_join_hash_rel_keys(right, rr, rk, kc)
            & (nbuckets - 1);
        ht_next[rr] = ht_head[h];
        ht_head[h] = rr + 1; /* 1-based; 0 = end of chain */
    }

    int sj_rc = 0;
    uint32_t min_left = col_join_parallel_min_left_rows();
    if (min_left == 0)
        min_left = 1;
    uint32_t W = left->nrows / min_left;
    if (W > sess->num_workers)
        W = sess->num_workers;
    if (!sess->coordinator && W > 1 && right->nrows > 0) {
        int ensure_rc = wl_columnar_session_ensure_workqueue(sess, W);
        if (ensure_rc == 0) {
            col_semijoin_ctx_t *ctxs = (col_semijoin_ctx_t *)calloc(
                W, sizeof(col_semijoin_ctx_t));
            uint64_t *offsets = (uint64_t *)calloc(W + 1,
                    sizeof(uint64_t));
            uint32_t *left_hashes = (uint32_t *)malloc(
                sizeof(uint32_t) * (size_t)(left->nrows ? left->nrows : 1));
            if (ctxs && offsets) {
                atomic_int write_error = ATOMIC_VAR_INIT(0);
                uint32_t chunk = (left->nrows + W - 1u) / W;
                int prc = 0;
                for (uint32_t w = 0; w < W; w++) {
                    uint32_t begin = w * chunk;
                    uint32_t end = begin + chunk;
                    if (begin > left->nrows)
                        begin = left->nrows;
                    if (end > left->nrows)
                        end = left->nrows;
                    ctxs[w].left = left;
                    ctxs[w].right = right;
                    ctxs[w].lk = lk;
                    ctxs[w].rk = rk;
                    ctxs[w].kc = kc;
                    ctxs[w].op = op;
                    ctxs[w].ht_head = ht_head;
                    ctxs[w].ht_next = ht_next;
                    ctxs[w].nbuckets = nbuckets;
                    ctxs[w].begin = begin;
                    ctxs[w].end = end;
                    ctxs[w].left_hashes = left_hashes;
                    ctxs[w].write_error = &write_error;
                    if (wl_workqueue_submit(sess->wq,
                        col_semijoin_count_worker_fn, &ctxs[w]) != 0)
                        prc = ENOMEM;
                }
                wl_workqueue_wait_all(sess->wq);
                uint64_t total = 0;
                for (uint32_t w = 0; w < W; w++) {
                    offsets[w] = total;
                    total += ctxs[w].count;
                }
                offsets[W] = total;
                if (prc == 0 && total > UINT32_MAX)
                    prc = ENOMEM;
                if (prc == 0 && col_join_reserve_exact(out,
                    (uint32_t)total) != 0)
                    prc = ENOMEM;
                if (prc == 0) {
                    out->nrows = (uint32_t)total;
                    if (out->timestamps && total > 0)
                        memset(out->timestamps, 0,
                            (size_t)total * sizeof(col_delta_timestamp_t));
                    for (uint32_t w = 0; w < W; w++) {
                        ctxs[w].out = out;
                        ctxs[w].out_begin = offsets[w];
                        if (wl_workqueue_submit(sess->wq,
                            col_semijoin_fill_worker_fn, &ctxs[w]) != 0)
                            prc = ENOMEM;
                    }
                    wl_workqueue_wait_all(sess->wq);
                    if (prc == 0)
                        prc = atomic_load_explicit(&write_error,
                                memory_order_relaxed);
                }
                if (prc != 0)
                    out->nrows = 0;
                free(left_hashes);
                free(offsets);
                free(ctxs);
                if (prc == 0)
                    goto semijoin_done;
            } else {
                free(left_hashes);
                free(offsets);
                free(ctxs);
            }
        }
    }

    /* Probe: for each left row test membership, emit if found: O(|L|) */
    for (uint32_t lr = 0; lr < left->nrows && sj_rc == 0; lr++) {
        uint32_t h = col_join_hash_rel_keys(left, lr, lk, kc)
            & (nbuckets - 1);
        bool found = false;
        for (uint32_t e = ht_head[h]; e != 0 && !found; e = ht_next[e - 1]) {
            uint32_t rr = e - 1;
            if (col_join_keys_match_rel(left, lr, lk, right, rr, rk, kc))
                found = true;
        }
        if (found) {
            if (op->project_count > 0 && op->project_indices) {
                for (uint32_t c = 0; c < ocols; c++) {
                    uint32_t si = op->project_indices[c];
                    tmp[c] = (si < left->ncols) ? left->columns[si][lr] : 0;
                }
            } else {
                for (uint32_t c = 0; c < left->ncols; c++)
                    tmp[c] = left->columns[c][lr];
            }
            sj_rc = col_rel_append_row(out, tmp);
        }
    }

semijoin_done:
    free(ht_head);
    free(ht_next);
    if (sj_rc != 0) {
        free(tmp);
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return sj_rc;
    }

    free(tmp);
    free(lk);
    free(rk);
    if (left_e.owned)
        col_rel_destroy(left);
    if (right_filtered)
        col_rel_destroy(right_filtered);
    return eval_stack_push(stack, out, true);
}

/* --- DIFFERENTIAL JOIN --------------------------------------------------- */

/*
 * col_op_join_diff - Differential join with arrangement reuse (Issue #263).
 *
 * Key optimization over col_op_join:
 *   - Uses col_diff_arrangement_t as a persistent hash index
 *   - Only hashes NEW rows (delta) since last iteration: O(D) vs O(N)
 *   - Hash table persists across iterations within an epoch
 *
 * Guard: activated when sess->diff_operators_active is true
 *        (affected_strata < full_mask, i.e., partial insertion)
 *
 * Falls back to ephemeral hash table when:
 *   - No key columns (kc == 0)
 *   - Delta-substituted right relation (no persistent arrangement)
 *   - Diff arrangement creation/resize fails
 */
int
wl_columnar_join_diff_op(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    col_rel_t *right_filtered = NULL;
    eval_entry_t left_e = eval_stack_pop(stack);
    if (!left_e.rel)
        return EINVAL;

    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right) {
        uint32_t empty_cols = (op->project_count > 0 && op->project_indices)
            ? op->project_count : left_e.rel->ncols;
        col_rel_t *out = col_rel_pool_new_auto(sess->delta_pool,
                sess->eval_arena, "$join_diff_empty", empty_cols);
        if (!out) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        if (col_join_set_left_output_types(out, left_e.rel, op) != 0) {
            col_rel_destroy(out);
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        if (left_e.owned)
            col_rel_destroy(left_e.rel);
        return eval_stack_push_delta(stack, out, true, false);
    }

    /* Right-side delta substitution (same logic as col_op_join) */
    bool used_right_delta = false;
    if (op->delta_mode == WL_DELTA_FORCE_DELTA && op->right_relation) {
        char rdname[256];
        snprintf(rdname, sizeof(rdname), "$d$%s", op->right_relation);
        col_rel_t *rdelta = session_find_rel(sess, rdname);
        /* Issue #472: mirror VARIABLE op retraction-aware pattern —
         * fall back to $r$<name> when retraction_seeded at iteration 0. */
        if (!rdelta && sess->retraction_seeded
            && sess->current_iteration == 0) {
            if (retraction_rel_name(op->right_relation, rdname,
                sizeof(rdname)) == 0)
                rdelta = session_find_rel(sess, rdname);
        }
        if (rdelta && rdelta->nrows > 0) {
            right = rdelta;
            used_right_delta = true;
        } else if (sess->current_iteration > 0 || sess->delta_seeded
            || sess->retraction_seeded) {
            uint32_t ocols = col_join_output_width(left_e.rel, right, op);
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            col_rel_t *empty = col_rel_new_auto("$join_diff_empty", ocols);
            if (!empty)
                return ENOMEM;
            if (col_join_set_output_types(empty, left_e.rel, right, op) != 0) {
                col_rel_destroy(empty);
                return ENOMEM;
            }
            int push_rc = eval_stack_push(stack, empty, true);
            if (push_rc != 0)
                col_rel_destroy(empty);
            return push_rc;
        }
    } else if (op->delta_mode != WL_DELTA_FORCE_FULL && op->right_relation
        && (!left_e.is_delta || (sess->tdd_outbound_only_active
        && sess->current_iteration > 0))) {
        char rdname[256];
        snprintf(rdname, sizeof(rdname), "$d$%s", op->right_relation);
        col_rel_t *rdelta = session_find_rel(sess, rdname);
        if (rdelta && (((rdelta->nrows > 0
            && rdelta->nrows < right->nrows) || (rdelta->nrows > 0
            && sess->tdd_subpass_active))
            || (sess->tdd_outbound_only_active
            && sess->current_iteration > 0))) {
            right = rdelta;
            used_right_delta = true;
        }
    }
    /* Issue #472: Retraction right-pass (same as col_op_join). */
    if (!used_right_delta && sess->retraction_right_pass
        && sess->current_iteration == 0 && op->right_relation) {
        char rdname[256];
        if (retraction_rel_name(op->right_relation, rdname,
            sizeof(rdname)) == 0) {
            col_rel_t *rdelta = session_find_rel(sess, rdname);
            if (rdelta && rdelta->nrows > 0) {
                right = rdelta;
                used_right_delta = true;
            }
        }
    }

    /* Apply constant filter on right child (from FILTER wrappers collected
     * during plan generation).  Use session-level cache (Issue #386): the
     * filtered relation is owned by sess->filt_cache and must NOT be
     * destroyed here.  right_filtered remains NULL for the cached path. */
    if (op->right_filter_expr.size > 0 && op->right_relation
        && !used_right_delta) {
        col_rel_t *filtered = wl_columnar_filter_apply_right_filter_cached(sess,
                &op->right_filter_expr, op->right_relation, right);
        if (!filtered) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        right = filtered;
        /* right_filtered stays NULL: cache owns the relation */
    } else if (op->right_filter_expr.size > 0) {
        /* Delta path or no relation name: fall back to pool-allocated filter */
        col_rel_t *filtered =
            wl_columnar_filter_apply_right_filter(&op->right_filter_expr, right,
                sess->delta_pool, sess->intern);
        if (!filtered) {
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return ENOMEM;
        }
        right = filtered;
        right_filtered = filtered;
    }

    /* Materialization cache check */
    bool projected_join = op->project_count > 0 && op->project_indices;
    if (op->materialized && !projected_join) {
        col_rel_t *cached
            = col_mat_cache_lookup(&sess->mat_cache, left_e.rel, right);
        if (cached) {
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (left_e.owned)
                col_rel_destroy(left_e.rel);
            return eval_stack_push_delta(stack, cached, false,
                       left_e.is_delta || used_right_delta);
        }
    }

    uint32_t kc = op->key_count;
    col_rel_t *left = left_e.rel;

    /* Resolve key column positions */
    uint32_t *lk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    uint32_t *rk = (uint32_t *)malloc(sizeof(uint32_t) * (kc ? kc : 1));
    if (!lk || !rk) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    for (uint32_t k = 0; k < kc; k++) {
        lk[k] = col_op_resolve_key(left,
                op->left_keys ? op->left_keys[k] : NULL, "left", "JOIN(diff)",
                op->right_relation);
        rk[k] = col_op_resolve_key(right,
                op->right_keys ? op->right_keys[k] : NULL, "right",
                "JOIN(diff)",
                op->right_relation);
    }
    if (!col_join_key_types_compatible(left, lk, right, rk, kc)) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return EINVAL;
    }

    uint32_t ocols = col_join_output_width(left, right, op);
    /* Materialized results outlive the current delta-pool reset while they
     * remain in mat_cache, so cache-owned joins must be heap allocated. */
    col_rel_t *out = (op->materialized && !projected_join)
        ? col_rel_new_auto("$join_diff", ocols)
        : col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
            "$join_diff", ocols);
    if (!out) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    if (col_join_set_output_types(out, left, right, op) != 0) {
        free(lk);
        free(rk);
        col_rel_destroy(out);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }
    col_join_attach_ledger(sess, out);

    /* Backpressure check (Issue #224) */
    if (wl_mem_ledger_should_backpressure(&sess->mem_ledger,
        WL_MEM_SUBSYS_RELATION, 80)) {
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return eval_stack_push(stack, out, true);
    }

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        col_rel_destroy(out);
        free(lk);
        free(rk);
        if (left_e.owned)
            col_rel_destroy(left);
        return ENOMEM;
    }

    int join_rc = 0;

    /* DIFFERENTIAL PATH: persistent diff_arrangement for non-delta right.
     * The arrangement persists across iterations, only indexing new rows. */
    col_diff_arrangement_t *darr = NULL;
    if (kc > 0 && op->right_relation && !used_right_delta
        && op->right_filter_expr.size == 0)
        darr = col_session_get_diff_arrangement(sess, op->right_relation, rk,
                kc);

    if (darr
        && col_diff_arrangement_ensure_ht_capacity(darr, right->nrows) != 0)
        darr = NULL; /* capacity grow failed; fall through to ephemeral */

    if (darr) {
        /* Incrementally add new rows [indexed_rows, right->nrows) to hash */
        uint32_t indexed = darr->indexed_rows;
        uint32_t nbk = darr->nbuckets;
        for (uint32_t rr = indexed; rr < right->nrows; rr++) {
            uint32_t h = col_join_hash_rel_keys(right, rr, rk, kc) & (nbk - 1);
            darr->ht_next[rr] = darr->ht_head[h];
            darr->ht_head[h] = rr + 1; /* 1-based; 0 = end of chain */
        }
        darr->indexed_rows = right->nrows;
        darr->current_nrows = right->nrows;

        uint32_t min_left = col_join_parallel_min_left_rows();
        if (min_left == 0)
            min_left = 1;
        uint32_t W = left->nrows / min_left;
        if (W > sess->num_workers)
            W = sess->num_workers;
        if (!sess->coordinator && W > 1) {
            int ensure_rc = wl_columnar_session_ensure_workqueue(sess, W);
            if (ensure_rc != 0) {
                free(tmp);
                col_rel_destroy(out);
                free(lk);
                free(rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                if (left_e.owned)
                    col_rel_destroy(left);
                return ensure_rc;
            }
            col_join_keyed_ctx_t *ctxs = (col_join_keyed_ctx_t *)calloc(
                W, sizeof(col_join_keyed_ctx_t));
            uint64_t *offsets = (uint64_t *)calloc(W + 1, sizeof(uint64_t));
            uint32_t *left_hashes = (uint32_t *)malloc(
                sizeof(uint32_t)
                * (size_t)(left->nrows ? left->nrows : 1));
            if (!ctxs || !offsets) {
                free(ctxs);
                free(offsets);
                free(left_hashes);
                free(tmp);
                col_rel_destroy(out);
                free(lk);
                free(rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                if (left_e.owned)
                    col_rel_destroy(left);
                return ENOMEM;
            }
            uint32_t chunk = (left->nrows + W - 1u) / W;
            uint64_t pair_budget = wl_mem_ledger_bytes_remaining(
                &sess->mem_ledger) / 8u;
            if (left->nrows < WL_JOIN_PAIR_CACHE_MIN_LEFT_ROWS)
                pair_budget = 0;
            if (pair_budget > WL_JOIN_PAIR_CACHE_MAX_BYTES)
                pair_budget = WL_JOIN_PAIR_CACHE_MAX_BYTES;
            uint64_t pair_budget_per_worker = W > 0 ? pair_budget / W : 0;
            uint32_t pair_cap_limit = pair_budget_per_worker
                / sizeof(col_join_pair_ref_t) > UINT32_MAX
                ? UINT32_MAX
                : (uint32_t)(pair_budget_per_worker
                / sizeof(col_join_pair_ref_t));
            atomic_bool stop = ATOMIC_VAR_INIT(false);
            atomic_uint_fast64_t shared_count = ATOMIC_VAR_INIT(0);
            int prc = 0;
            for (uint32_t w = 0; w < W; w++) {
                uint32_t begin = w * chunk;
                uint32_t end = begin + chunk;
                if (begin > left->nrows)
                    begin = left->nrows;
                if (end > left->nrows)
                    end = left->nrows;
                ctxs[w].left = left;
                ctxs[w].right = right;
                ctxs[w].darr = darr;
                ctxs[w].lk = lk;
                ctxs[w].rk = rk;
                ctxs[w].kc = kc;
                ctxs[w].op = op;
                ctxs[w].begin = begin;
                ctxs[w].end = end;
                ctxs[w].limit = sess->join_output_limit;
                ctxs[w].left_hashes = left_hashes;
                ctxs[w].pair_cap_limit = pair_cap_limit;
                ctxs[w].pairs_complete = true;
                ctxs[w].stop = &stop;
                ctxs[w].shared_count = &shared_count;
                if (wl_workqueue_submit(sess->wq,
                    col_join_keyed_count_worker_fn, &ctxs[w]) != 0)
                    prc = ENOMEM;
            }
            wl_workqueue_wait_all(sess->wq);
            if (atomic_load_explicit(&stop, memory_order_relaxed)
                && prc == 0)
                prc = EOVERFLOW;
            uint64_t total = 0;
            for (uint32_t w = 0; w < W; w++) {
                if (ctxs[w].rc != 0 && prc == 0)
                    prc = ctxs[w].rc;
                offsets[w] = total;
                total += ctxs[w].count;
            }
            offsets[W] = total;
            if (prc == 0 && sess->join_output_limit > 0
                && total >= sess->join_output_limit)
                prc = EOVERFLOW;
            if (prc == 0 && total > UINT32_MAX)
                prc = ENOMEM;
            if (prc == 0 && total > out->capacity && ocols > 0) {
                uint64_t add_rows = total - out->capacity;
                uint64_t row_bytes = (uint64_t)ocols * sizeof(int64_t);
                uint64_t add_bytes = add_rows > UINT64_MAX / row_bytes
                    ? UINT64_MAX : add_rows * row_bytes;
                uint64_t budget = atomic_load_explicit(
                    &sess->mem_ledger.total_budget, memory_order_relaxed);
                uint64_t current = atomic_load_explicit(
                    &sess->mem_ledger.subsys_bytes[WL_MEM_SUBSYS_RELATION],
                    memory_order_relaxed);
                uint64_t cap = (budget
                    * wl_mem_subsys_pct[WL_MEM_SUBSYS_RELATION]) / 100u;
                uint64_t threshold = (cap * 80u) / 100u;
                if (budget > 0 && cap > 0
                    && (add_bytes > UINT64_MAX - current
                    || current + add_bytes >= threshold))
                    prc = EOVERFLOW;
            }
            if (prc == 0) {
                if (col_join_reserve_exact(out, (uint32_t)total) != 0) {
                    prc = ENOMEM;
                } else {
                    out->nrows = (uint32_t)total;
                    for (uint32_t w = 0; w < W; w++) {
                        ctxs[w].out = out;
                        ctxs[w].out_begin = offsets[w];
                        ctxs[w].rc = 0;
                        if (wl_workqueue_submit(sess->wq,
                            col_join_keyed_fill_worker_fn, &ctxs[w]) != 0)
                            prc = ENOMEM;
                    }
                    wl_workqueue_wait_all(sess->wq);
                    for (uint32_t w = 0; w < W; w++)
                        if (ctxs[w].rc != 0 && prc == 0)
                            prc = ctxs[w].rc;
                }
            }
            for (uint32_t w = 0; w < W; w++)
                free(ctxs[w].pairs);
            free(offsets);
            free(ctxs);
            free(left_hashes);
            if (prc != 0) {
                free(tmp);
                col_rel_destroy(out);
                free(lk);
                free(rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                if (left_e.owned)
                    col_rel_destroy(left);
                return prc;
            }
            goto join_success;
        }

        /* Probe left against the persistent diff arrangement hash table */
        for (uint32_t lr = 0; lr < left->nrows && join_rc == 0; lr++) {
            uint32_t h = col_join_hash_rel_keys(left, lr, lk, kc) & (nbk - 1);
            for (uint32_t e = darr->ht_head[h]; e != 0;
                e = darr->ht_next[e - 1]) {
                uint32_t rr = e - 1;
                if (!col_join_keys_match_rel(left, lr, lk, right, rr, rk, kc))
                    continue;
                join_rc = col_join_append_pair(out, left, lr, right, rr,
                        op->project_indices, op->project_count, tmp);
                if (join_rc != 0)
                    break;
                if (col_join_output_limit_reached(sess, out)
                    || col_join_inloop_backpressure(sess, out)) {
                    fprintf(stderr,
                        "join output limit reached (diff): %u rows "
                        "(limit=%llu)\n",
                        out->nrows,
                        (unsigned long long)sess->join_output_limit);
                    join_rc = EOVERFLOW;
                    break;
                }
            }
        }

        if (join_rc != 0) {
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (left_e.owned)
                col_rel_destroy(left);
            return join_rc;
        }
    } else {
        /* Ephemeral hash table fallback (same as col_op_join) */
        uint32_t nbuckets_ep;
        if (col_join_bucket_count(right->nrows, &nbuckets_ep) != 0) {
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (left_e.owned)
                col_rel_destroy(left);
            return ENOMEM;
        }
        uint32_t *ht_head_ep = (uint32_t *)calloc(nbuckets_ep,
                sizeof(uint32_t));
        uint32_t *ht_next_ep = (uint32_t *)malloc(
            (right->nrows > 0 ? right->nrows : 1) * sizeof(uint32_t));
        if (!ht_head_ep || !ht_next_ep) {
            free(ht_head_ep);
            free(ht_next_ep);
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (left_e.owned)
                col_rel_destroy(left);
            return ENOMEM;
        }
        for (uint32_t rr = 0; rr < right->nrows; rr++) {
            uint32_t h = col_join_hash_rel_keys(right, rr, rk, kc)
                & (nbuckets_ep - 1);
            ht_next_ep[rr] = ht_head_ep[h];
            ht_head_ep[h] = rr + 1;
        }
        for (uint32_t lr = 0; lr < left->nrows && join_rc == 0; lr++) {
            uint32_t h = col_join_hash_rel_keys(left, lr, lk, kc)
                & (nbuckets_ep - 1);
            for (uint32_t e = ht_head_ep[h]; e != 0;
                e = ht_next_ep[e - 1]) {
                uint32_t rr = e - 1;
                if (!col_join_keys_match_rel(left, lr, lk, right, rr, rk, kc))
                    continue;
                join_rc = col_join_append_pair(out, left, lr, right, rr,
                        op->project_indices, op->project_count, tmp);
                if (join_rc != 0)
                    break;
                if (col_join_output_limit_reached(sess, out)
                    || col_join_inloop_backpressure(sess, out)) {
                    fprintf(stderr,
                        "join output limit reached (diff): %u rows "
                        "(limit=%llu)\n",
                        out->nrows,
                        (unsigned long long)sess->join_output_limit);
                    join_rc = EOVERFLOW;
                    break;
                }
            }
        }
        free(ht_head_ep);
        free(ht_next_ep);
        if (join_rc != 0) {
            free(tmp);
            col_rel_destroy(out);
            free(lk);
            free(rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (left_e.owned)
                col_rel_destroy(left);
            return join_rc;
        }
    }

join_success:
    free(tmp);
    free(lk);
    free(rk);
    bool result_is_delta = projected_join ? false
        : (left_e.is_delta || used_right_delta);

    /* Materialization cache: insert BEFORE destroying left, because
     * col_mat_cache_key_content dereferences left to compute content hash. */
    if (op->materialized && !projected_join) {
        col_mat_cache_insert(&sess->mat_cache, left, right, out);
        if (left_e.owned)
            col_rel_destroy(left);
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return eval_stack_push_delta(stack, out, false, result_is_delta);
    }
    if (left_e.owned)
        col_rel_destroy(left);
    if (right_filtered)
        col_rel_destroy(right_filtered);
    return eval_stack_push_delta(stack, out, true, result_is_delta);
}

/* Compatibility entry points used by internal test fixtures and downstream
 * backend code while the operator implementations use the directory-aware
 * wl_columnar_* naming convention. */
int
col_op_join(const wl_plan_op_t *op, eval_stack_t *stack, wl_col_session_t *sess)
{
    return wl_columnar_join_op(op, stack, sess);
}

int
col_op_antijoin(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    return wl_columnar_antijoin_op(op, stack, sess);
}

int
col_op_semijoin(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    return wl_columnar_semijoin_op(op, stack, sess);
}

int
col_op_join_diff(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    return wl_columnar_join_diff_op(op, stack, sess);
}
