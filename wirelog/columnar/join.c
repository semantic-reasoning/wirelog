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
#include "columnar/diff_join_batch.h"
#include "columnar/join_batch.h"
#include "columnar/lftj.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef WL_TEST_JOIN_TIMESTAMP_HOOK
bool wl_columnar_join_test_enable_semijoin_timestamps;
#endif
#ifdef WL_TEST_JOIN_CACHE_HOOK
void (*wl_columnar_join_test_before_diff_commit)(
    wl_columnar_arrangement_diff_txn_t *);
bool wl_columnar_join_test_fail_next_key_scratch_alloc;
int (*wl_columnar_join_test_submit_override)(wl_work_queue_t *,
    void (*)(void *), void *);
uint32_t wl_columnar_join_test_last_keyed_workers;
void (*wl_columnar_join_test_before_pair_growth)(
    wl_columnar_memory_governor_t *, uint64_t, uint64_t);
bool wl_columnar_join_test_fail_pair_realloc;
atomic_bool wl_columnar_join_test_fail_pair_commit = ATOMIC_VAR_INIT(false);
uint32_t wl_columnar_join_test_pair_cap_limit_override;
bool wl_columnar_join_test_last_cross_ctx_admitted;
bool wl_columnar_join_test_last_semijoin_parallel_admitted;
bool wl_columnar_join_test_last_keyed_parallel_admitted;
#endif

static int
wl_columnar_join_submit(wl_work_queue_t *wq, void (*work_fn)(void *),
    void *ctx)
{
#ifdef WL_TEST_JOIN_CACHE_HOOK
    if (wl_columnar_join_test_submit_override)
        return wl_columnar_join_test_submit_override(wq, work_fn, ctx);
#endif
    return wl_workqueue_submit(wq, work_fn, ctx);
}

/* Retire the whole evaluator entry; refusal keeps its relation and segment
 * metadata together in the caller's vacated stack slot. */
static int
wl_columnar_join_dispose_left(eval_stack_t *stack, eval_entry_t *left,
    int primary_rc)
{
    int rc = eval_stack_dispose_entry(stack, left);
    return rc != 0 ? rc : primary_rc;
}

static int
wl_columnar_join_memory_error(wl_col_session_t *sess, int rc)
{
    if (rc == ENOSPC)
        sess->memory_budget_denied = true;
    return rc;
}

static int
wl_columnar_join_publish_after_left(eval_stack_t *stack, eval_entry_t *left,
    col_rel_t *out, bool is_delta)
{
    int rc = wl_columnar_join_dispose_left(stack, left, 0);
    if (rc == 0)
        rc = eval_stack_push_delta(stack, out, true, is_delta);
    /* @out has not escaped to the stack or cache if this push fails. */
    if (rc != 0)
        col_rel_destroy(out); /* Private result has never escaped this call. */
    return rc;
}

static wl_columnar_memory_governor_ref_t *
wl_columnar_join_effective_governor(const wl_col_session_t *sess,
    const col_rel_t *source)
{
    return sess && sess->memory_governor ? sess->memory_governor
                                         : (source ? source->memory_governor
                                                   : NULL);
}

wl_columnar_memory_governor_ref_t *
wl_columnar_join_output_governor(const wl_col_session_t *sess,
    const col_rel_t *left,
    const col_rel_t *right)
{
    if (sess && sess->memory_governor)
        return sess->memory_governor;
    if (left && left->memory_governor)
        return left->memory_governor;
    return right ? right->memory_governor : NULL;
}

typedef struct {
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_governor_ref_t *governor;
    uint32_t *left_keys;
    uint32_t *right_keys;
    bool admitted;
} wl_columnar_join_key_scratch_t;

static int
wl_columnar_join_key_scratch_alloc(wl_col_session_t *sess,
    const col_rel_t *left, const col_rel_t *right, uint32_t key_count,
    wl_columnar_join_key_scratch_t *scratch)
{
    uint64_t count = key_count;
    uint64_t bytes;
    wl_columnar_memory_admission_status_t status;
    if (!scratch)
        return EINVAL;
    memset(scratch, 0, sizeof(*scratch));
    wl_columnar_memory_reservation_init(&scratch->reservation);
    if (key_count == 0)
        return 0;
    scratch->governor = wl_columnar_join_output_governor(sess, left, right);
    if (!wl_columnar_memory_size_mul(count, 2u * sizeof(uint32_t), &bytes)
        || bytes > SIZE_MAX)
        return EOVERFLOW;
    if (scratch->governor) {
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(scratch->governor), bytes,
            &scratch->reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED && sess)
                sess->memory_budget_denied = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : ENOMEM;
        }
        scratch->admitted = true;
    }
    scratch->left_keys = (uint32_t *)malloc((size_t)count * sizeof(uint32_t));
#ifdef WL_TEST_JOIN_CACHE_HOOK
    if (scratch->left_keys &&
        wl_columnar_join_test_fail_next_key_scratch_alloc) {
        wl_columnar_join_test_fail_next_key_scratch_alloc = false;
        free(scratch->left_keys);
        scratch->left_keys = NULL;
        if (scratch->admitted) {
            (void)wl_columnar_memory_rollback(&scratch->reservation);
            scratch->admitted = false;
        }
        return ENOMEM;
    }
#endif
    scratch->right_keys = (uint32_t *)malloc((size_t)count * sizeof(uint32_t));
    if (!scratch->left_keys || !scratch->right_keys) {
        free(scratch->left_keys);
        free(scratch->right_keys);
        scratch->left_keys = NULL;
        scratch->right_keys = NULL;
        if (scratch->admitted) {
            (void)wl_columnar_memory_rollback(&scratch->reservation);
            scratch->admitted = false;
        }
        return ENOMEM;
    }
    return 0;
}

static void
wl_columnar_join_key_scratch_free(wl_columnar_join_key_scratch_t *scratch,
    uint32_t **keys)
{
    if (!scratch || !keys || !*keys)
        return;
    if (*keys == scratch->left_keys)
        scratch->left_keys = NULL;
    else if (*keys == scratch->right_keys)
        scratch->right_keys = NULL;
    free(*keys);
    *keys = NULL;
    if (!scratch->left_keys && !scratch->right_keys && scratch->admitted) {
        (void)wl_columnar_memory_rollback(&scratch->reservation);
        scratch->admitted = false;
    }
}

/* ENOMEM may leave the single original result publishable only when its
 * complete retained footprint was already committed to the effective
 * governor.  An unmanaged source is eligible only in an unmanaged session. */
static bool
wl_columnar_join_original_is_accounted(const col_rel_t *source,
    wl_columnar_memory_governor_ref_t *effective)
{
    uint64_t needed = 0;
    if (!source)
        return false;
    if (!effective)
        return source->memory_governor == NULL
               && source->retained_reserved_bytes == 0;
    return source->memory_governor == effective
           && source->retained_reservation.governor
           == wl_columnar_memory_governor_ref_get(effective)
           && atomic_load_explicit(&source->retained_reservation.state,
               memory_order_acquire) == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
           && col_rel_retained_live_bytes(source, &needed)
           && source->retained_reserved_bytes >= needed
           && source->retained_reservation.bytes >= needed;
}

static int
diff_txn_commit_after_push(eval_stack_t *stack,
    wl_columnar_arrangement_diff_txn_t *txn, col_mat_cache_t *cache,
    const col_rel_t *cached_result)
{
    if (!txn->session)
        return 0;
#ifdef WL_TEST_JOIN_CACHE_HOOK
    if (wl_columnar_join_test_before_diff_commit)
        wl_columnar_join_test_before_diff_commit(txn);
#endif
    int rc = wl_columnar_arrangement_diff_txn_commit(txn);
    if (rc == 0)
        return 0;

    col_mat_cache_remove_result(cache, cached_result);

    eval_entry_t entry;
    int dispose_rc = eval_stack_pop_relation(stack, &entry);
    if (dispose_rc == 0)
        dispose_rc = eval_stack_dispose_entry(stack, &entry);
    wl_columnar_arrangement_diff_txn_abort(txn);
    return dispose_rc != 0 ? dispose_rc : rc;
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

/* Construct join payload only after reserving its initial retained shape.
 * Unmanaged transient outputs may use the evaluation pool; governed and
 * retained outputs are always heap-owned so their reservation outlives the
 * pool epoch. */
static int
wl_columnar_join_new_output(wl_col_session_t *sess, const col_rel_t *left,
    const col_rel_t *right, const char *name, uint32_t ncols,
    uint32_t capacity, bool timestamps, bool retained, col_rel_t **out)
{
    wl_columnar_memory_governor_ref_t *governor;
    int rc = 0;

    if (!out)
        return EINVAL;
    *out = NULL;
    governor = wl_columnar_join_output_governor(sess, left, right);
    if (governor) {
        rc = wl_columnar_relation_new_auto_governed(name, ncols, capacity,
                timestamps, governor, out);
    } else if (retained) {
        *out = col_rel_new_auto(name, ncols);
        rc = *out ? 0 : ENOMEM;
        if (rc == 0 && capacity > (*out)->capacity)
            rc = col_rel_reserve_capacity_admitted(*out, capacity, NULL);
        if (rc == 0 && timestamps)
            rc = col_rel_enable_timestamps(*out);
    } else {
        *out = col_rel_pool_new_auto(sess->delta_pool, sess->eval_arena,
                name, ncols);
        rc = *out ? 0 : ENOMEM;
        if (rc == 0 && timestamps)
            rc = col_rel_enable_timestamps(*out);
    }
    if (rc != 0) {
        if (*out) {
            if (rc == ENOMEM && (*out)->memory_budget_denial_pending)
                rc = ENOSPC;
            col_rel_destroy(*out);
            *out = NULL;
        }
        if (rc == ENOSPC && sess)
            sess->memory_budget_denied = true;
        return rc;
    }
    col_join_attach_ledger(sess, *out);
    return 0;
}

int
wl_columnar_join_budget_denial(wl_col_session_t *sess, const col_rel_t *rel,
    int rc)
{
    if (rc == ENOMEM && rel && rel->memory_budget_denial_pending)
        rc = ENOSPC;
    if (rc == ENOSPC && sess)
        sess->memory_budget_denied = true;
    return rc;
}

static int
wl_columnar_join_output_error(wl_col_session_t *sess, const col_rel_t *out,
    int rc)
{
    /* The bounded relation sink already maps a reservation refusal to
     * ENOSPC; preserve its budget evidence before destroying the output. */
    if ((rc == ENOMEM || rc == ENOSPC) && out
        && out->memory_budget_denial_pending) {
        if (sess)
            sess->memory_budget_denied = true;
        return ENOSPC;
    }
    return rc;
}

bool
wl_columnar_join_output_limit_reached(wl_col_session_t *sess,
    const col_rel_t *out, uint64_t added_rows)
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
        uint_fast64_t n = atomic_load_explicit(sess->join_output_shared_count,
                memory_order_relaxed);
        if (added_rows == 0)
            return n >= limit;
        for (;;) {
            uint_fast64_t next = added_rows > UINT64_MAX - n
                ? UINT64_MAX : n + added_rows;
            if (atomic_compare_exchange_weak_explicit(
                    sess->join_output_shared_count, &n, next,
                    memory_order_relaxed, memory_order_relaxed))
                return next >= limit;
        }
    }
    return out && out->nrows >= limit;
}

bool
col_join_output_limit_reached(wl_col_session_t *sess, const col_rel_t *out)
{
    return wl_columnar_join_output_limit_reached(sess, out, 1);
}

static int
col_join_reserve_exact(col_rel_t *rel, uint32_t nrows)
{
    if (!rel)
        return EINVAL;
    /* The relation-level transaction resizes columns and timestamps together
     * and leaves all ownership/capacity metadata unchanged on failure. */
    return col_rel_reserve_capacity_admitted(rel, nrows, NULL);
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
    wl_columnar_memory_governor_ref_t *memory_governor;
    wl_columnar_memory_reservation_t pair_reservation;
    uint64_t pair_reserved_bytes;
    int pair_error;
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

uint32_t
col_join_output_width(const col_rel_t *left, const col_rel_t *right,
    const wl_plan_op_t *op)
{
    return (op && op->project_count > 0 && op->project_indices)
        ? op->project_count : left->ncols + right->ncols;
}

int
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
        if (source < left->ncols) {
            types[c] = left->column_types
                ? left->column_types[source] : WIRELOG_TYPE_INT64;
        } else if (source - left->ncols < right->ncols) {
            uint32_t source_col = source - left->ncols;
            types[c] = right->column_types
                ? right->column_types[source_col] : WIRELOG_TYPE_INT64;
        } else {
            /* Keep the existing invalid-projection value semantics while
             * avoiding an out-of-bounds read from the source schema. */
            types[c] = WIRELOG_TYPE_INT64;
        }
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
        types[c] = source < left->ncols && left->column_types
            ? left->column_types[source] : WIRELOG_TYPE_INT64;
    }
    int rc = col_rel_set_column_types(out, types, width);
    free(types);
    return rc;
}

bool
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

/* Parallel-fill writer: called from keyed fill workers on disjoint row
 * ranges of a fresh output, so it uses col_rel_set_raw and leaves the
 * single view-generation publication to the coordinator. */
int
col_join_write_pair_at(col_rel_t *out, uint64_t out_row,
    const col_rel_t *left, uint32_t lr, const col_rel_t *right, uint32_t rr,
    const uint32_t *project_indices, uint32_t project_count)
{
    if (project_count > 0 && project_indices) {
        for (uint32_t c = 0; c < project_count; c++) {
            int rc = col_rel_set_raw(out, (uint32_t)out_row, c,
                    col_join_pair_value(left, lr, right, rr,
                    project_indices[c]));
            if (rc != 0)
                return rc;
        }
    } else {
        for (uint32_t c = 0; c < left->ncols; c++) {
            int rc = col_rel_set_raw(out, (uint32_t)out_row, c,
                    left->columns[c][lr]);
            if (rc != 0)
                return rc;
        }
        for (uint32_t c = 0; c < right->ncols; c++) {
            int rc = col_rel_set_raw(out, (uint32_t)out_row, left->ncols + c,
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
            ctx->pair_error = EOVERFLOW;
            ctx->pairs_complete = false;
            return false;
        }
        if (ctx->pair_cap_limit > 0 && new_cap > ctx->pair_cap_limit)
            new_cap = ctx->pair_cap_limit;
        size_t max_pairs = SIZE_MAX / sizeof(col_join_pair_ref_t);
        if (new_cap <= ctx->pair_cap) {
            ctx->pairs_complete = false;
            return false;
        }
        if ((size_t)new_cap > max_pairs) {
            ctx->pair_error = EOVERFLOW;
            free(ctx->pairs);
            ctx->pairs = NULL;
            ctx->pair_count = 0;
            ctx->pair_cap = 0;
            ctx->pairs_complete = false;
            return false;
        }
        wl_columnar_memory_reservation_t pending;
        bool pending_valid = false;
        uint64_t new_bytes = 0;
        if (ctx->memory_governor) {
            if (!wl_columnar_memory_size_mul(new_cap,
                sizeof(col_join_pair_ref_t), &new_bytes)) {
                ctx->pair_error = EOVERFLOW;
                ctx->pairs_complete = false;
                return false;
            }
            wl_columnar_memory_reservation_init(&pending);
#ifdef WL_TEST_JOIN_CACHE_HOOK
            if (ctx->pair_reserved_bytes > 0
                && wl_columnar_join_test_before_pair_growth)
                wl_columnar_join_test_before_pair_growth(
                    wl_columnar_memory_governor_ref_get(
                        ctx->memory_governor),
                    ctx->pair_reserved_bytes, new_bytes);
#endif
            wl_columnar_memory_admission_status_t status =
                wl_columnar_memory_reserve_growth(
                wl_columnar_memory_governor_ref_get(
                    ctx->memory_governor),
                ctx->pair_reserved_bytes, new_bytes, &pending);
            if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
                ctx->pair_error =
                    status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                    ? ENOSPC
                    : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                    ? EOVERFLOW : ENOMEM;
                ctx->pairs_complete = false;
                return false;
            }
            pending_valid = true;
        }
        col_join_pair_ref_t *new_pairs = NULL;
#ifdef WL_TEST_JOIN_CACHE_HOOK
        if (ctx->pair_reserved_bytes > 0
            && wl_columnar_join_test_fail_pair_realloc) {
            wl_columnar_join_test_fail_pair_realloc = false;
        } else
#endif
        new_pairs = (col_join_pair_ref_t *)realloc(ctx->pairs,
                (size_t)new_cap * sizeof(col_join_pair_ref_t));
        if (!new_pairs) {
            if (pending_valid)
                (void)wl_columnar_memory_rollback(&pending);
            free(ctx->pairs);
            ctx->pairs = NULL;
            ctx->pair_count = 0;
            ctx->pair_cap = 0;
            ctx->pairs_complete = false;
            ctx->pair_error = ENOMEM;
            return false;
        }
        if (pending_valid) {
            wl_columnar_memory_reservation_t previous;
#ifdef WL_TEST_JOIN_CACHE_HOOK
            bool fail_commit = ctx->pair_reserved_bytes > 0
                && atomic_exchange_explicit(
                &wl_columnar_join_test_fail_pair_commit, false,
                memory_order_acq_rel);
            bool commit_ok = !fail_commit;
#else
            bool commit_ok = true;
#endif
            if (!commit_ok || !wl_columnar_memory_commit(&pending, ctx)) {
                free(new_pairs);
                (void)wl_columnar_memory_rollback(&pending);
                ctx->pairs = NULL;
                ctx->pair_count = 0;
                ctx->pair_cap = 0;
                ctx->pair_error = ENOMEM;
                ctx->pairs_complete = false;
                return false;
            }
            wl_columnar_memory_reservation_init(&previous);
            if (ctx->pair_reserved_bytes > 0
                && !wl_columnar_memory_reservation_move(
                    &previous, &ctx->pair_reservation))
                abort();
            if (!wl_columnar_memory_reservation_move(
                    &ctx->pair_reservation, &pending))
                abort();
            if (ctx->pair_reserved_bytes > 0
                && !wl_columnar_memory_release(&previous))
                abort();
            ctx->pair_reserved_bytes = new_bytes;
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
                    uint64_t seen = (uint64_t)atomic_fetch_add_explicit(
                        ctx->shared_count, 1024u, memory_order_relaxed)
                        + UINT64_C(1024);
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
        uint64_t seen = (uint64_t)atomic_fetch_add_explicit(ctx->shared_count,
                delta,
                memory_order_relaxed) + delta;
        if (ctx->limit > 0 && seen >= ctx->limit && ctx->stop)
            atomic_store_explicit(ctx->stop, true, memory_order_relaxed);
    }
    ctx->count = count;
    if (ctx->pair_error != 0)
        ctx->rc = ctx->pair_error;
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
                int rc = col_rel_set_raw(out, (uint32_t)out_row, c,
                        (si < left->ncols) ? left->columns[si][lr] : 0);
                if (rc != 0) {
                    atomic_store_explicit(ctx->write_error, rc,
                        memory_order_relaxed);
                    return;
                }
            }
        } else {
            for (uint32_t c = 0; c < left->ncols; c++) {
                int rc = col_rel_set_raw(out, (uint32_t)out_row, c,
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
                int rc = col_rel_set_raw(out, (uint32_t)oi, c,
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
                int rc = col_rel_set_raw(out, (uint32_t)oi, c,
                        left->columns[c][lr]);
                if (rc != 0) {
                    atomic_store_explicit(ctx->write_error, rc,
                        memory_order_relaxed);
                    return;
                }
            }
            for (uint32_t c = 0; c < right->ncols; c++) {
                int rc = col_rel_set_raw(out, (uint32_t)oi, left->ncols + c,
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

/* The output stays function-private until *outp is replaced. Every failure
 * after worker submission waits for all workers before destroying it. */
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
        return EOVERFLOW;

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
#ifdef WL_TEST_JOIN_CACHE_HOOK
    wl_columnar_join_test_last_cross_ctx_admitted = false;
#endif
    col_rel_t *out = *outp;
    int reserve_rc = col_join_reserve_exact(out, nrows);
    if (reserve_rc != 0)
        return wl_columnar_join_output_error(sess, out, reserve_rc);
    out->nrows = nrows;
    if (nrows > 0)
        wl_columnar_relation_touch_view(out);

    wl_columnar_memory_reservation_t ctx_reservation;
    bool ctx_admitted = false;
    wl_columnar_memory_reservation_init(&ctx_reservation);
    wl_columnar_memory_governor_ref_t *ctx_governor
        = wl_columnar_join_output_governor(sess, left, right);
    uint64_t ctx_bytes = 0;
    if (!wl_columnar_memory_size_mul(W, sizeof(col_join_cross_ctx_t),
        &ctx_bytes) || ctx_bytes > SIZE_MAX) {
        return EOVERFLOW;
    }
    if (ctx_governor) {
        wl_columnar_memory_admission_status_t status;
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(ctx_governor),
            ctx_bytes, &ctx_reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                sess->memory_budget_denied = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : ENOMEM;
        }
        ctx_admitted = true;
    }
#ifdef WL_TEST_JOIN_CACHE_HOOK
    wl_columnar_join_test_last_cross_ctx_admitted = ctx_admitted;
#endif
    col_join_cross_ctx_t *ctxs = (col_join_cross_ctx_t *)calloc(1,
            (size_t)ctx_bytes);
    if (!ctxs) {
        if (ctx_admitted)
            (void)wl_columnar_memory_rollback(&ctx_reservation);
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
        if (wl_columnar_join_submit(sess->wq, col_join_cross_fill_worker_fn,
            &ctxs[w]) != 0) {
            rc = ENOMEM;
            break;
        }
    }
    wl_workqueue_wait_all(sess->wq);
    if (rc == 0)
        rc = atomic_load_explicit(&write_error, memory_order_relaxed);
    free(ctxs);
    if (ctx_admitted)
        (void)wl_columnar_memory_rollback(&ctx_reservation);
    if (rc != 0) {
        return wl_columnar_join_output_error(sess, out, rc);
    }
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

/* Stage one joined pair into @row and append it under @batch, which holds
 * out's mutation lease for the whole serial probe loop.  Writing each cell
 * with col_rel_set took a mutation lease per cell (#2072); this takes one
 * per storage epoch and publishes one view advance per row. */
static int
col_join_append_pair(col_rel_append_batch_t *batch, const col_rel_t *left,
    uint32_t lr, const col_rel_t *right, uint32_t rr,
    const uint32_t *project_indices, uint32_t project_count, int64_t *row)
{
    if (project_count > 0 && project_indices) {
        for (uint32_t c = 0; c < project_count; c++)
            row[c] = col_join_pair_value(left, lr, right, rr,
                    project_indices[c]);
    } else {
        for (uint32_t c = 0; c < left->ncols; c++)
            row[c] = left->columns[c][lr];
        for (uint32_t c = 0; c < right->ncols; c++)
            row[left->ncols + c] = right->columns[c][rr];
    }
    return col_rel_append_batch_row(batch, row);
}

/* End a serial probe loop's batch right after the loop, before any exit
 * that could destroy, publish or replace out, folding its status into rc. */
static int
col_join_pair_batch_end(col_rel_append_batch_t *batch, int rc)
{
    int end_rc = col_rel_append_batch_end(batch, rc == 0);
    return rc != 0 ? rc : end_rc;
}

wl_columnar_join_selection_t
wl_columnar_join_select_right(const wl_plan_op_t *op,
    wl_col_session_t *sess, col_rel_t *right, bool left_is_delta)
{
    wl_columnar_join_selection_t selection = {
        .right = right,
        .used_right_delta = false,
        .force_empty = false
    };

    if (op->delta_mode == WL_DELTA_FORCE_DELTA && op->right_relation) {
        char rdname[256];
        snprintf(rdname, sizeof(rdname), "$d$%s", op->right_relation);
        col_rel_t *rdelta = session_find_rel(sess, rdname);
        /* At iteration zero, seeded retractions use $r$ when $d$ is absent. */
        if (!rdelta && sess->retraction_seeded
            && sess->current_iteration == 0) {
            if (retraction_rel_name(op->right_relation, rdname,
                sizeof(rdname)) == 0)
                rdelta = session_find_rel(sess, rdname);
        }
        if (rdelta && rdelta->nrows > 0) {
            selection.right = rdelta;
            selection.used_right_delta = true;
        } else if (sess->current_iteration > 0 || sess->delta_seeded
            || sess->retraction_seeded) {
            selection.force_empty = true;
        }
    } else if (op->delta_mode != WL_DELTA_FORCE_FULL && op->right_relation
        && (!left_is_delta || (sess->tdd_outbound_only_active
        && sess->current_iteration > 0))) {
        char rdname[256];
        snprintf(rdname, sizeof(rdname), "$d$%s", op->right_relation);
        col_rel_t *rdelta = session_find_rel(sess, rdname);
        if (rdelta && (((rdelta->nrows > 0
            && rdelta->nrows < right->nrows) || (rdelta->nrows > 0
            && sess->tdd_subpass_active))
            || (sess->tdd_outbound_only_active
            && sess->current_iteration > 0))) {
            selection.right = rdelta;
            selection.used_right_delta = true;
        }
    }

    /* Retraction right-pass deliberately also overrides FORCE_FULL. */
    if (!selection.force_empty && !selection.used_right_delta
        && sess->retraction_right_pass && sess->current_iteration == 0
        && op->right_relation) {
        char rdname[256];
        if (retraction_rel_name(op->right_relation, rdname,
            sizeof(rdname)) == 0) {
            col_rel_t *rdelta = session_find_rel(sess, rdname);
            if (rdelta && rdelta->nrows > 0) {
                selection.right = rdelta;
                selection.used_right_delta = true;
            }
        }
    }

    return selection;
}

int
wl_columnar_join_op(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    /* Raw destroys of out/copy below apply only before publication or after a
     * failed push. Successful cache insertion transfers out to the cache and
     * is undone with col_mat_cache_remove_result. right_filtered is the owned,
     * uncached fallback; this operator is synchronous and retires its probe
     * and worker users before each cleanup edge. */
    col_rel_t *right = NULL;
    bool bound_delta = false;
    if (sess->tdd_input_run) {
        int bound_rc = wl_columnar_eval_tdd_plan_resolve_input(sess, op, true,
                &right, &bound_delta);
        if (bound_rc != 0)
            return bound_rc;
    } else {
        right = session_find_rel(sess, op->right_relation);
    }
    eval_entry_t left_e;
    int pop_rc = eval_stack_pop_relation(stack, &left_e);
    if (pop_rc != 0)
        return pop_rc;

    /* right_filtered: non-NULL only when right was pool-allocated by
     * wl_columnar_filter_apply_right_filter (non-cached path: antijoin/semijoin callers).
     * For col_op_join we use wl_columnar_filter_apply_right_filter_cached; the cache owns
     * the filtered relation and we must NOT destroy it here. */
    col_rel_t *right_filtered = NULL;
    if (!right) {
        /* If right relation doesn't exist, join produces empty result (cross-product with nothing).
         * Similar to ANTIJOIN logic (which keeps all left rows on missing right).
         * This can occur in generated plans where optional relations may not exist. */
        uint32_t empty_cols = (op->project_count > 0 && op->project_indices)
            ? op->project_count : left_e.rel->ncols;
        col_rel_t *out = NULL;
        int create_rc = wl_columnar_join_new_output(sess, left_e.rel, NULL,
                "$join_empty", empty_cols, COL_REL_INIT_CAP, false, false,
                &out);
        if (create_rc != 0)
            return wl_columnar_join_dispose_left(stack, &left_e, create_rc);
        create_rc = col_join_set_left_output_types(out, left_e.rel, op);
        if (create_rc != 0) {
            col_rel_destroy(out);
            return wl_columnar_join_dispose_left(stack, &left_e, create_rc);
        }
        return wl_columnar_join_publish_after_left(stack, &left_e, out, false);
    }

#ifdef WL_PROFILE
    uint64_t _t0_join = now_ns();
    sess->profile.join_calls++;
#endif

    wl_columnar_join_selection_t selection = sess->tdd_input_run
        ? (wl_columnar_join_selection_t){right, bound_delta,
                                         bound_delta && right->nrows == 0}
        : wl_columnar_join_select_right(op, sess, right, left_e.is_delta);
    right = selection.right;
    bool used_right_delta = selection.used_right_delta;
    if (selection.force_empty) {
        /* FORCE_DELTA had no usable $d$/$r$ on a seeded round. */
        uint32_t ocols = col_join_output_width(left_e.rel, right, op);
        col_rel_t *empty = NULL;
        int create_rc = wl_columnar_join_new_output(sess, left_e.rel, right,
                "$join_empty", ocols, COL_REL_INIT_CAP, false, true,
                &empty);
        if (create_rc != 0)
            return wl_columnar_join_dispose_left(stack, &left_e, create_rc);
        create_rc = col_join_set_output_types(empty, left_e.rel, right, op);
        if (create_rc != 0) {
            col_rel_destroy(empty);
            return wl_columnar_join_dispose_left(stack, &left_e, create_rc);
        }
        return wl_columnar_join_publish_after_left(stack, &left_e, empty,
                   false);
    }

    /* Apply constant filter on right child (from FILTER wrappers collected
     * during plan generation).  Use session-level cache (Issue #386): the
     * filtered relation is owned by sess->filt_cache and must NOT be
     * destroyed here.  right_filtered remains NULL for the cached path.
     * The cache reports itself unavailable while a lease defers a rebuild
     * or blocks growth (Issue #1435); allocation and admission failures are
     * returned to the caller instead of being hidden by an owned fallback. */
    if (op->right_filter_expr.size > 0) {
        col_rel_t *filtered = NULL;
        bool cache_unavailable = false;
        int filter_rc = 0;
        if (op->right_relation && !used_right_delta) {
            col_filt_cache_pin_t filter_pin = { 0 };
            filter_rc =
                wl_columnar_filter_apply_right_filter_cached_pin_checked(
                sess, &op->right_filter_expr, op->right_relation, right,
                &filter_pin, &filtered, &cache_unavailable);
            col_filt_cache_pin_release(&filter_pin);
            if (filter_rc != 0) {
                int cleanup_rc = eval_stack_dispose_entry(stack, &left_e);
                return cleanup_rc != 0 ? cleanup_rc : filter_rc;
            }
            if (cache_unavailable)
                WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                    "filtered cache unavailable for %s, using owned filter",
                    op->right_relation);
        }
        if (filtered) {
            right = filtered;
            /* right_filtered stays NULL: cache owns the relation */
        } else if (cache_unavailable || !op->right_relation ||
            used_right_delta) {
            /* Delta path, no relation name, or cache unavailable:
             * pool-allocated filter owned by this op */
            filter_rc = wl_columnar_filter_apply_right_filter_governed_checked(
                &op->right_filter_expr, right, sess->delta_pool, sess->intern,
                sess->memory_governor ? sess->memory_governor :
                right->memory_governor, sess, &filtered);
            if (filter_rc != 0) {
                int cleanup_rc = eval_stack_dispose_entry(stack, &left_e);
                return cleanup_rc != 0 ? cleanup_rc : filter_rc;
            }
            right = filtered;
            right_filtered = filtered;
        } else if (!filtered) {
            int cleanup_rc = eval_stack_dispose_entry(stack, &left_e);
            return cleanup_rc != 0 ? cleanup_rc : EINVAL;
        }
    }

    /* A nonempty nullary left has no key column to probe. Empty left inputs
     * and nullary cross joins remain valid; match the bounded producer's
     * EINVAL before a cached result or one-shot probe can bypass validation. */
    if (op->key_count > 0 && left_e.rel->ncols == 0
        && left_e.rel->nrows > 0) {
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return wl_columnar_join_dispose_left(stack, &left_e, EINVAL);
    }

    /* Materialization cache: reuse previous join result when available.
     * Works with both stable (borrowed) and worker-owned relations since
     * the cache key is based on content hash, not ownership. This enables
     * cache reuse in K-fusion worker sessions, eliminating redundant joins. */
    bool projected_join = op->project_count > 0 && op->project_indices;
    /* An owned filtered fallback has a fresh identity for every operation.
    * It cannot hit again during the lease window, so bypass the
    * materialization cache rather than occupying a slot until eviction. */
    if (op->materialized && !sess->tdd_input_run && !projected_join &&
        !right_filtered) {
        col_mat_cache_pin_t cache_pin = { 0 };
        col_rel_t *cached
            = col_mat_cache_lookup_pin(&sess->mat_cache, left_e.rel, right,
                &cache_pin);
        if (cached) {
            col_rel_t *copy = NULL;
            int copy_rc = wl_columnar_relation_deep_copy_governed(cached,
                    &copy, wl_columnar_join_effective_governor(sess, cached));
            col_mat_cache_pin_release(&cache_pin);
#ifdef WL_PROFILE
            sess->profile.join_cache_hit_ns += now_ns() - _t0_join;
#endif
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (copy_rc != 0)
                return wl_columnar_join_dispose_left(stack, &left_e,
                           wl_columnar_join_memory_error(sess, copy_rc));
            return wl_columnar_join_publish_after_left(stack, &left_e, copy,
                       left_e.is_delta || used_right_delta);
        }
    }

    uint32_t kc = op->key_count;
    col_rel_t *left = left_e.rel;
    wl_columnar_memory_governor_ref_t *scratch_governor
        = wl_columnar_join_output_governor(sess, left, right);

    /* Resolve key column positions */
    wl_columnar_join_key_scratch_t key_scratch;
    int key_rc = wl_columnar_join_key_scratch_alloc(sess, left, right, kc,
            &key_scratch);
    uint32_t *lk = key_scratch.left_keys;
    uint32_t *rk = key_scratch.right_keys;
    if (key_rc != 0) {
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return wl_columnar_join_dispose_left(stack, &left_e, key_rc);
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
        wl_columnar_join_key_scratch_free(&key_scratch, &lk);
        wl_columnar_join_key_scratch_free(&key_scratch, &rk);
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return wl_columnar_join_dispose_left(stack, &left_e, EINVAL);
    }

    uint32_t ocols = col_join_output_width(left, right, op);
    /* Bounded keyed-join sub-batches (Issue #1446): opt-in through
     * WIRELOG_JOIN_BATCH_BYTES.  Eligible shapes run the resumable producer
     * over the pinned arrangement; excluded shapes fall back to the one-shot
     * join with the reason recorded, or fail with ENOTSUP in strict mode. */
    col_join_batch_eligibility_t batch_elig = col_join_batch_eligibility(sess,
            kc, used_right_delta, op->right_filter_expr.size != 0);
    bool bounded = batch_elig == COL_JOIN_BATCH_ELIGIBLE;
    if (sess->join_batch_bytes > 0 && !bounded) {
        if (sess->join_batch_strict) {
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_dispose_left(stack, &left_e, ENOTSUP);
        }
        col_join_batch_record_fallback(sess, batch_elig);
    }
    /* An empty input on either side has nothing to bound: the ordinary probe
     * yields zero rows in one pass, so do not dispatch the producer.  For an
     * empty right this also keeps the producer's lease from being a second
     * lease on the window pin's entry: the registry reports a pinned index
     * with indexed_rows == 0 unavailable (arrangement.c deferral clause,
     * tracked in #1500), which would turn an empty relation into ENOENT.  An
     * empty left can be a zero-column relation whose keys resolved to column
     * 0 by fallback, an index the producer rejects with EINVAL (#2170).  The
     * output is then not held for a producer, so it is not forced onto the
     * heap.  Decided after the strict and fallback handling above: it is
     * neither a fallback nor a strict-mode failure, and the eligibility
     * verdict stands. */
    if (bounded && (right->nrows == 0 || left->nrows == 0))
        bounded = false;
    /* Governed outputs and materialized results outlive the current
     * delta-pool reset, so both require heap-owned relation descriptors. */
    col_rel_t *out = NULL;
    int output_rc = wl_columnar_join_new_output(sess, left, right, "$join",
            ocols,
            COL_REL_INIT_CAP, false,
            bounded ||
            (op->materialized && !sess->tdd_input_run && !projected_join),
            &out);
    if (output_rc != 0) {
        wl_columnar_join_key_scratch_free(&key_scratch, &lk);
        wl_columnar_join_key_scratch_free(&key_scratch, &rk);
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return wl_columnar_join_dispose_left(stack, &left_e, output_rc);
    }
    output_rc = col_join_set_output_types(out, left, right, op);
    if (output_rc != 0) {
        wl_columnar_join_key_scratch_free(&key_scratch, &lk);
        wl_columnar_join_key_scratch_free(&key_scratch, &rk);
        col_rel_destroy(out);
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return wl_columnar_join_dispose_left(stack, &left_e, output_rc);
    }
    int64_t *tmp = NULL;

    /* BOOLEAN SPECIALIZATION (Issue #62): Fast-path for unary relations.
     * When right relation is unary (ncols == 1) and single join key,
     * use O(1) hash-set membership test instead of merge-join.
     * Profiling shows 37.7% of DOOP-class joins are unary; this path
     * provides ~30-40% speedup for such workloads. */
    bool right_is_unary = (right->ncols == 1);
    bool left_is_unary = (left->ncols == 1);
#ifdef WL_PROFILE
    if ((right_is_unary || left_is_unary) && kc == 1 && !bounded)
        sess->profile.join_unary++;
#endif
    if ((right_is_unary || left_is_unary) && kc == 1 && !bounded) {
        /* build: unary side as hash set; probe: non-unary side iterated.
         * When both are unary, right is preferred as build side. */
        col_rel_t *build = right_is_unary ? right : left;
        col_rel_t *probe = right_is_unary ? left : right;
        uint32_t build_kcol = right_is_unary ? rk[0] : lk[0];
        uint32_t probe_kcol = right_is_unary ? lk[0] : rk[0];
        wl_columnar_memory_reservation_t row_reservation;
        bool row_admitted = false;
        wl_columnar_memory_reservation_init(&row_reservation);
        if (scratch_governor) {
            uint64_t row_bytes = 0;
            wl_columnar_memory_admission_status_t status;
            if (!wl_columnar_memory_size_mul(ocols ? ocols : 1,
                sizeof(int64_t), &row_bytes)) {
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                return wl_columnar_join_dispose_left(stack, &left_e,
                           EOVERFLOW);
            }
            status = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(scratch_governor),
                row_bytes, &row_reservation);
            if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
                if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                    sess->memory_budget_denied = true;
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                return wl_columnar_join_dispose_left(stack, &left_e,
                           status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                               ? ENOSPC
                               : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                               ? EOVERFLOW : ENOMEM);
            }
            row_admitted = true;
        }
        tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
        if (!tmp) {
            if (row_admitted)
                (void)wl_columnar_memory_rollback(&row_reservation);
            col_rel_destroy(out);
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_dispose_left(stack, &left_e, ENOMEM);
        }

        /* Build hash set from the unary relation's single column. */
        uint32_t nbuckets;
        if (col_join_bucket_count(build->nrows, &nbuckets) != 0) {
            free(tmp);
            tmp = NULL;
            if (row_admitted)
                (void)wl_columnar_memory_rollback(&row_reservation);
            col_rel_destroy(out);
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_dispose_left(stack, &left_e, ENOMEM);
        }
        wl_columnar_memory_reservation_t hash_reservation;
        bool hash_admitted = false;
        wl_columnar_memory_reservation_init(&hash_reservation);
        if (scratch_governor) {
            uint64_t head_bytes = 0;
            uint64_t next_bytes = 0;
            uint64_t hash_bytes = 0;
            wl_columnar_memory_admission_status_t status;
            bool sized = wl_columnar_memory_size_mul(nbuckets,
                    sizeof(uint32_t), &head_bytes)
                && wl_columnar_memory_size_mul(
                build->nrows > 0 ? build->nrows : 1,
                sizeof(uint32_t), &next_bytes)
                && head_bytes <= SIZE_MAX && next_bytes <= SIZE_MAX
                && wl_columnar_memory_size_add(head_bytes, next_bytes,
                    &hash_bytes);
            if (!sized) {
                free(tmp);
                tmp = NULL;
                if (row_admitted)
                    (void)wl_columnar_memory_rollback(&row_reservation);
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                return wl_columnar_join_dispose_left(stack, &left_e,
                           EOVERFLOW);
            }
            status = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(scratch_governor),
                hash_bytes, &hash_reservation);
            if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
                if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                    sess->memory_budget_denied = true;
                free(tmp);
                tmp = NULL;
                if (row_admitted)
                    (void)wl_columnar_memory_rollback(&row_reservation);
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                return wl_columnar_join_dispose_left(stack, &left_e,
                           status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                               ? ENOSPC
                               : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                               ? EOVERFLOW
                               : ENOMEM);
            }
            hash_admitted = true;
        }
        uint32_t *ht_head = (uint32_t *)calloc(nbuckets, sizeof(uint32_t));
        uint32_t *ht_next = (uint32_t *)malloc(
            (build->nrows > 0 ? build->nrows : 1) * sizeof(uint32_t));
        if (!ht_head || !ht_next) {
            free(ht_head);
            free(ht_next);
            free(tmp);
            tmp = NULL;
            if (hash_admitted)
                (void)wl_columnar_memory_rollback(&hash_reservation);
            if (row_admitted)
                (void)wl_columnar_memory_rollback(&row_reservation);
            col_rel_destroy(out);
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_dispose_left(stack, &left_e, ENOMEM);
        }
        for (uint32_t bi = 0; bi < build->nrows; bi++) {
            uint32_t h = col_join_hash_rel_keys(build, bi, &build_kcol, 1);
            h &= (nbuckets - 1);
            ht_next[bi] = ht_head[h];
            ht_head[h] = bi + 1; /* 1-based; 0 = end of chain */
        }

        /* Probe: iterate non-unary side, test membership in hash set. */
        col_rel_append_batch_t pair_batch = { 0 };
        int join_rc = col_rel_append_batch_begin(&pair_batch, out);
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
                join_rc = col_join_append_pair(&pair_batch, left, lr, right,
                        rr, op->project_indices, op->project_count, tmp);
                if (join_rc != 0) {
                    WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_ERROR,
                        "join pair append failed with rc=%d at "
                        "unary join",
                        join_rc);
                    break;
                }
                if (col_join_output_limit_reached(sess, out)) {
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

        join_rc = col_join_pair_batch_end(&pair_batch, join_rc);
        free(ht_head);
        free(ht_next);
        if (hash_admitted)
            (void)wl_columnar_memory_rollback(&hash_reservation);
        if (join_rc != 0) {
            free(tmp);
            tmp = NULL;
            if (row_admitted)
                (void)wl_columnar_memory_rollback(&row_reservation);
            join_rc = wl_columnar_join_output_error(sess, out, join_rc);
            col_rel_destroy(out);
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_dispose_left(stack, &left_e, join_rc);
        }
        free(tmp);
        tmp = NULL;
        if (row_admitted)
            (void)wl_columnar_memory_rollback(&row_reservation);
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
            "Unary join completed, out->nrows=%u",
            out->nrows);
    } else {
        /* Standard merge-join for non-unary relations. */
        wl_columnar_memory_reservation_t tmp_reservation;
        bool tmp_admitted = false;
        wl_columnar_memory_reservation_init(&tmp_reservation);
        if (scratch_governor) {
            uint64_t tmp_bytes = 0;
            wl_columnar_memory_admission_status_t status;
            if (!wl_columnar_memory_size_mul(ocols ? ocols : 1,
                sizeof(int64_t), &tmp_bytes)) {
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                return wl_columnar_join_dispose_left(stack, &left_e,
                           EOVERFLOW);
            }
            status = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(scratch_governor),
                tmp_bytes, &tmp_reservation);
            if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
                if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                    sess->memory_budget_denied = true;
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                return wl_columnar_join_dispose_left(stack, &left_e,
                           status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                               ? ENOSPC
                               : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                               ? EOVERFLOW : ENOMEM);
            }
            tmp_admitted = true;
        }
#define WL_JOIN_RELEASE_TMP() \
        do { \
            free(tmp); \
            tmp = NULL; \
            if (tmp_admitted) { \
                (void)wl_columnar_memory_rollback(&tmp_reservation); \
                tmp_admitted = false; \
            } \
        } while (0)
        tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
        if (!tmp) {
            WL_JOIN_RELEASE_TMP();
            col_rel_destroy(out);
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_dispose_left(stack, &left_e, ENOMEM);
        }
        /* Hash join: use persistent arrangement for the full right relation.
         * Delta substitution and true "no arrangement" misses retain the
         * existing ephemeral hash table path.  Once the protected primary
         * probe admits the exact source, build/rebuild failures are real
         * errors and must not silently change execution mode. */
        col_arrangement_t *arr = NULL;
        col_arrangement_pin_t arr_pin = { 0 };
        col_arrangement_probe_bundle_t arr_bundle = { 0 };
        col_arrangement_probe_t *arr_probe = NULL;
        uint32_t nbuckets_ep = 0;
        uint32_t *ht_head_ep = NULL;
        uint32_t *ht_next_ep = NULL;
        wl_columnar_memory_reservation_t hash_reservation;
        bool hash_admitted = false;
        wl_columnar_memory_reservation_init(&hash_reservation);

        col_arrangement_probe_bundle_init(&arr_bundle);

        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
            "Standard merge-join starting - left=%u rows, right=%u rows, kc=%u",
            left->nrows, right->nrows, kc);

        if (!sess->tdd_input_run && !used_right_delta && op->right_relation &&
            kc > 0) {
            if (op->right_filter_expr.size == 0) {
                int probe_rc = col_arrangement_probe_bundle_acquire_primary(
                    &arr_bundle, &sess->base, right, rk, kc, &arr_probe);
                if (probe_rc == 0) {
                    right = (col_rel_t *)arr_probe->source;
                    arr = arr_probe->arr;
                } else if (probe_rc != ENOENT) {
                    WL_JOIN_RELEASE_TMP();
                    col_rel_destroy(out);
                    wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                    wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                    if (right_filtered)
                        col_rel_destroy(right_filtered);
                    return wl_columnar_join_dispose_left(stack, &left_e,
                               probe_rc);
                }
            } else {
                /* Issue #433: filtered right arrangement cache.
                 * `right` is normally the cached filtered relation from
                 * filt_cache; an owned fallback is deliberately ephemeral
                 * and must not enter either cache during a lease window.
                 * The cached path persists filt_arr across sub-passes to
                 * avoid rebuilding an ephemeral hash table. */
                if (!sess->coordinator && !right_filtered) {
                    int dependency_rc
                        = col_arrangement_probe_bundle_acquire_dependency(
                            &arr_bundle, right);
                    if (dependency_rc != 0) {
                        WL_JOIN_RELEASE_TMP();
                        col_rel_destroy(out);
                        wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                        wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                        if (right_filtered)
                            col_rel_destroy(right_filtered);
                        (void)col_arrangement_probe_bundle_release(
                            &arr_bundle);
                        return wl_columnar_join_dispose_left(stack, &left_e,
                                   dependency_rc);
                    }
                    uint64_t fhash =
                        wl_columnar_filter_fnv1a_hash(
                        op->right_filter_expr.data,
                        op->right_filter_expr.size);
                    arr = col_session_get_filt_arrangement(sess,
                            op->right_relation, fhash, right, rk, kc);
                }
            }
        } else if (!sess->tdd_input_run && used_right_delta &&
            op->right_relation && kc > 0) {
            int dependency_rc
                = col_arrangement_probe_bundle_acquire_dependency(
                    &arr_bundle, right);
            if (dependency_rc != 0) {
                WL_JOIN_RELEASE_TMP();
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                (void)col_arrangement_probe_bundle_release(&arr_bundle);
                return wl_columnar_join_dispose_left(stack, &left_e,
                           dependency_rc);
            }
            arr = col_session_get_delta_arrangement(sess, op->right_relation,
                    right, rk, kc);
        }

        if (arr)
            WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                "Using persistent arrangement");
        else
            WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                "No arrangement available, will use ephemeral hash table");

        if (!arr) {
            /* Ephemeral hash table (delta path or arrangement unavailable). */
            if (col_join_bucket_count(right->nrows, &nbuckets_ep) != 0) {
                (void)col_arrangement_probe_bundle_release(&arr_bundle);
                WL_JOIN_RELEASE_TMP();
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                return wl_columnar_join_dispose_left(stack, &left_e, ENOMEM);
            }
            if (scratch_governor) {
                uint64_t head_bytes = 0;
                uint64_t next_bytes = 0;
                uint64_t hash_bytes = 0;
                wl_columnar_memory_admission_status_t status;
                bool sized = wl_columnar_memory_size_mul(nbuckets_ep,
                        sizeof(uint32_t), &head_bytes)
                    && wl_columnar_memory_size_mul(
                    right->nrows > 0 ? right->nrows : 1,
                    sizeof(uint32_t), &next_bytes)
                    && head_bytes <= SIZE_MAX && next_bytes <= SIZE_MAX
                    && wl_columnar_memory_size_add(head_bytes, next_bytes,
                        &hash_bytes);
                if (!sized) {
                    (void)col_arrangement_probe_bundle_release(&arr_bundle);
                    WL_JOIN_RELEASE_TMP();
                    col_rel_destroy(out);
                    wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                    wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                    if (right_filtered)
                        col_rel_destroy(right_filtered);
                    return wl_columnar_join_dispose_left(stack, &left_e,
                               EOVERFLOW);
                }
                status = wl_columnar_memory_reserve_checked(
                    wl_columnar_memory_governor_ref_get(
                        scratch_governor),
                    hash_bytes, &hash_reservation);
                if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                    && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
                    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                        sess->memory_budget_denied = true;
                    (void)col_arrangement_probe_bundle_release(&arr_bundle);
                    WL_JOIN_RELEASE_TMP();
                    col_rel_destroy(out);
                    wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                    wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                    if (right_filtered)
                        col_rel_destroy(right_filtered);
                    return wl_columnar_join_dispose_left(stack, &left_e,
                               status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                                   ? ENOSPC
                                   : status
                               == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                                   ? EOVERFLOW
                                   : ENOMEM);
                }
                hash_admitted = true;
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
                if (hash_admitted)
                    (void)wl_columnar_memory_rollback(&hash_reservation);
                (void)col_arrangement_probe_bundle_release(&arr_bundle);
                WL_JOIN_RELEASE_TMP();
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                return wl_columnar_join_dispose_left(stack, &left_e, ENOMEM);
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
        wl_columnar_memory_reservation_t key_row_reservation;
        bool key_row_admitted = false;
        wl_columnar_memory_reservation_init(&key_row_reservation);
        if (arr) {
            uint64_t key_row_bytes = 0;
            wl_columnar_memory_admission_status_t status;
            if (!wl_columnar_memory_size_mul(
                    right->ncols > 0 ? right->ncols : 1,
                    sizeof(int64_t), &key_row_bytes)
                || key_row_bytes > SIZE_MAX) {
                int release_rc = arr_bundle.active
                    ? col_arrangement_probe_bundle_release(&arr_bundle) : 0;
                WL_JOIN_RELEASE_TMP();
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                return wl_columnar_join_dispose_left(stack, &left_e,
                           release_rc != 0 ? release_rc : EOVERFLOW);
            }
            if (scratch_governor) {
                status = wl_columnar_memory_reserve_checked(
                    wl_columnar_memory_governor_ref_get(scratch_governor),
                    key_row_bytes, &key_row_reservation);
                if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                    && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
                    int release_rc = arr_bundle.active
                        ? col_arrangement_probe_bundle_release(&arr_bundle)
                        : 0;
                    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                        sess->memory_budget_denied = true;
                    WL_JOIN_RELEASE_TMP();
                    col_rel_destroy(out);
                    wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                    wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                    if (right_filtered)
                        col_rel_destroy(right_filtered);
                    int status_rc =
                        status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                        ? ENOSPC
                        : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                        ? EOVERFLOW : ENOMEM;
                    return wl_columnar_join_dispose_left(stack, &left_e,
                               release_rc != 0 ? release_rc : status_rc);
                }
                key_row_admitted = true;
            }
            key_row = (int64_t *)malloc(
                (size_t)key_row_bytes);
            if (!key_row) {
                int release_rc = 0;
                if (key_row_admitted)
                    (void)wl_columnar_memory_rollback(
                        &key_row_reservation);
                if (arr_bundle.active)
                    release_rc = col_arrangement_probe_bundle_release(
                        &arr_bundle);
                col_arrangement_pin_release(&arr_pin);
                free(ht_head_ep);
                free(ht_next_ep);
                if (hash_admitted)
                    (void)wl_columnar_memory_rollback(&hash_reservation);
                WL_JOIN_RELEASE_TMP();
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                return wl_columnar_join_dispose_left(stack, &left_e,
                           release_rc != 0 ? release_rc : ENOMEM);
            }
        }

        int join_rc = 0;
        int join_overflow = 0;
        bool run_bounded = bounded && arr != NULL;
        if (bounded && !arr) {
            /* Eligible shape without a persistent arrangement: the ephemeral
             * hash path cannot be resumed, so it is a recorded fallback. */
            if (sess->join_batch_strict)
                join_rc = ENOTSUP;
            else
                col_join_batch_record_fallback(sess,
                    COL_JOIN_BATCH_EXCLUDED_NO_ARRANGEMENT);
        }
        if (run_bounded && join_rc == 0) {
            wl_columnar_continuation_t *cont = NULL;
            int create_rc = col_join_batch_producer_create(sess, op, left,
                    left_e.is_delta, lk, rk, kc, sess->join_batch_bytes,
                    &cont);
            if (create_rc == ENOTSUP && !sess->join_batch_strict) {
                /* One output row does not fit the batch budget. */
                col_join_batch_record_fallback(sess,
                    COL_JOIN_BATCH_EXCLUDED_ROW_TOO_LARGE);
                run_bounded = false;
            } else if (create_rc != 0) {
                join_rc = create_rc;
            } else {
                join_rc = col_join_batch_run_to_relation(cont, sess, out);
                wl_columnar_continuation_destroy(cont);
                if (join_rc != 0)
                    WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                        "Bounded join failed with rc=%d after %u rows",
                        join_rc, out->nrows);
            }
        }
        if (kc == 0 && col_join_should_parallelize_cross(sess, left, right)) {
            join_rc = col_join_parallel_cross(sess, left, right, op, &out,
                    &join_overflow);
            if (join_rc == EINVAL)
                join_rc = 0;
            else if (join_rc == 0 && join_overflow)
                join_rc = EOVERFLOW;
        } else if (!run_bounded) {
            col_rel_append_batch_t pair_batch = { 0 };
            if (join_rc == 0)
                join_rc = col_rel_append_batch_begin(&pair_batch, out);
            for (uint32_t lr = 0;
                lr < left->nrows && join_rc == 0; lr++) {
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
                            join_rc = col_join_append_pair(&pair_batch, left,
                                    lr, right, rr, op->project_indices,
                                    op->project_count, tmp);
                            if (join_rc != 0) {
                                /* Issue #1477: a governed output makes
                                 * rc=ENOMEM a routine budget denial, so this
                                 * goes through the structured logger rather
                                 * than unconditional consumer stderr. */
                                WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_ERROR,
                                    "join pair append failed in "
                                    "arrangement probe with rc=%d",
                                    join_rc);
                                break;
                            }
                            if (col_join_output_limit_reached(sess, out)) {
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
                        join_rc = col_join_append_pair(&pair_batch, left, lr,
                                right, rr, op->project_indices,
                                op->project_count, tmp);
                        if (join_rc != 0) {
                            WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_ERROR,
                                "join pair append failed in ephemeral "
                                "hash probe with rc=%d",
                                join_rc);
                            break;
                        }
                        if (col_join_output_limit_reached(sess, out)) {
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
            join_rc = col_join_pair_batch_end(&pair_batch, join_rc);
        }

        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
            "Merge-join loop completed, out->nrows=%u, rc=%d",
            out->nrows, join_rc);

        free(key_row);
        if (key_row_admitted)
            (void)wl_columnar_memory_rollback(&key_row_reservation);
        free(ht_head_ep);
        free(ht_next_ep);
        if (hash_admitted)
            (void)wl_columnar_memory_rollback(&hash_reservation);
        if (join_rc != 0) {
            int release_rc = 0;
            WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                "Merge-join failed with rc=%d, out->nrows=%u",
                join_rc, out->nrows);
            WL_JOIN_RELEASE_TMP();
            join_rc = wl_columnar_join_output_error(sess, out, join_rc);
            col_rel_destroy(out);
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            /* Release source readers before destroying their owned inputs. */
            if (arr_bundle.active)
                release_rc = col_arrangement_probe_bundle_release(
                    &arr_bundle);
            col_arrangement_pin_release(&arr_pin);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_dispose_left(stack, &left_e,
                       release_rc != 0 ? release_rc : join_rc);
        }
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG, "Merge-join succeeded");
        int release_rc = 0;
        if (arr_bundle.active)
            release_rc = col_arrangement_probe_bundle_release(&arr_bundle);
        col_arrangement_pin_release(&arr_pin);
        if (release_rc != 0) {
            WL_JOIN_RELEASE_TMP();
            col_rel_destroy(out);
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_dispose_left(stack, &left_e, release_rc);
        }
        WL_JOIN_RELEASE_TMP();
    }

    free(tmp);
#undef WL_JOIN_RELEASE_TMP
    wl_columnar_join_key_scratch_free(&key_scratch, &lk);
    wl_columnar_join_key_scratch_free(&key_scratch, &rk);
    /* Propagate delta flag: result is a delta if left was delta OR we used
     * right-delta. This ensures subsequent JOINs in the same rule plan know
     * whether to apply right-delta (they should NOT if we already used one). */
    bool result_is_delta = projected_join ? false
        : (left_e.is_delta || used_right_delta);

    /* Populate materialization cache when hint is set.  The cache owns the
     * computed result only after a successful insert, so retain an independent
     * copy for the evaluation stack.  This keeps cache lifetime entirely
     * inside this operation and avoids borrowed stack entries. */
    /* Owned filtered fallbacks are operation-local; see the ordinary path. */
    if (op->materialized && !sess->tdd_input_run && !projected_join &&
        !right_filtered) {
#ifdef WL_PROFILE
        if (out->nrows == 0)
            sess->profile.join_empty_out++;
        sess->profile.join_compute_ns += now_ns() - _t0_join;
#endif
        col_rel_t *copy = NULL;
        wl_columnar_memory_governor_ref_t *effective
            = wl_columnar_join_effective_governor(sess, out);
        int copy_rc = wl_columnar_relation_deep_copy_governed(out, &copy,
                effective);
        if (copy_rc != 0) {
            if ((copy_rc == ENOMEM || copy_rc == ENOSPC)
                && wl_columnar_join_original_is_accounted(out, effective))
                return wl_columnar_join_publish_after_left(stack, &left_e,
                           out, result_is_delta);
            col_rel_destroy(out);
            return wl_columnar_join_dispose_left(stack, &left_e,
                       wl_columnar_join_memory_error(sess, copy_rc));
        }
        int cache_rc = col_mat_cache_insert(&sess->mat_cache, left, right, out);
        bool publish_original = (cache_rc == ENOMEM || cache_rc == ENOSPC)
            && wl_columnar_join_original_is_accounted(out, effective);
        if (cache_rc != 0 && !publish_original) {
            col_rel_destroy(copy);
            col_rel_destroy(out);
            return wl_columnar_join_dispose_left(stack, &left_e,
                       wl_columnar_join_memory_error(sess, cache_rc));
        }
        int cleanup_rc = wl_columnar_join_dispose_left(stack, &left_e, 0);
        if (cleanup_rc != 0) {
            if (cache_rc == 0)
                col_mat_cache_remove_result(&sess->mat_cache, out);
            else
                col_rel_destroy(out);
            col_rel_destroy(copy);
            return cleanup_rc;
        }
        col_rel_t *publish = cache_rc == 0 ? copy : out;
        if (cache_rc != 0)
            col_rel_destroy(copy);
        int push_rc = eval_stack_push_delta(stack, publish, true,
                result_is_delta);
        if (push_rc != 0) {
            if (cache_rc == 0)
                col_mat_cache_remove_result(&sess->mat_cache, out);
            col_rel_destroy(publish);
        }
        return push_rc;
    }
#ifdef WL_PROFILE
    if (out->nrows == 0)
        sess->profile.join_empty_out++;
    sess->profile.join_compute_ns += now_ns() - _t0_join;
#endif
    if (right_filtered)
        col_rel_destroy(right_filtered);
    return wl_columnar_join_publish_after_left(stack, &left_e, out,
               result_is_delta);

}

/* --- PROBE SETS ---------------------------------------------------------- */

#ifdef WL_SESSION_TEST_HOOKS
/* Observation points for the SEMIJOIN/ANTIJOIN probe sets and emits.  They
 * also count the calls the standalone operators make.  TDD worker threads
 * can build probe sets and emit concurrently, so every update is a relaxed
 * atomic. */
atomic_uint_fast64_t wl_columnar_join_test_probe_set_build_calls;
atomic_uint_fast64_t wl_columnar_join_test_probe_set_build_denials;
atomic_uint_fast64_t wl_columnar_join_test_probe_set_reserved_at_entry;
atomic_uint_fast64_t wl_columnar_join_test_probe_set_admitted_bytes;
atomic_uint_fast64_t wl_columnar_join_test_probe_emits;
void (*wl_columnar_join_test_before_probe_emit)(const col_rel_t *left);
#endif

/* The admitted footprint of a probe set: head buckets plus next links. */
int
wl_columnar_join_probe_set_bytes(uint32_t nrows,
    wl_columnar_join_probe_next_t next_mode, uint64_t *bytes)
{
    uint32_t nbuckets;
    uint64_t head_bytes = 0;
    uint64_t next_bytes = 0;
    uint64_t next_len;

    if (!bytes)
        return EINVAL;
    if (col_join_bucket_count(nrows, &nbuckets) != 0)
        return ENOMEM;
    next_len = next_mode == WL_COLUMNAR_JOIN_PROBE_NEXT_ROWS_PLUS_ONE
        ? (uint64_t)nrows + 1u : (nrows > 0 ? nrows : 1u);
    if (!wl_columnar_memory_size_mul(nbuckets, sizeof(uint32_t), &head_bytes)
        || !wl_columnar_memory_size_mul(next_len, sizeof(uint32_t),
        &next_bytes)
        || head_bytes > SIZE_MAX || next_bytes > SIZE_MAX
        || !wl_columnar_memory_size_add(head_bytes, next_bytes, bytes))
        return EOVERFLOW;
    return 0;
}

int
wl_columnar_join_probe_set_build(wl_col_session_t *sess,
    wl_columnar_memory_governor_ref_t *governor, const col_rel_t *right,
    const uint32_t *rk, uint32_t kc, wl_columnar_join_probe_next_t next_mode,
    wl_columnar_join_probe_set_t *set)
{
    uint32_t nbuckets;
    size_t next_len;

    if (!set || !right)
        return EINVAL;
    memset(set, 0, sizeof(*set));
    wl_columnar_memory_reservation_init(&set->reservation);
#ifdef WL_SESSION_TEST_HOOKS
    atomic_fetch_add_explicit(&wl_columnar_join_test_probe_set_build_calls,
        1u, memory_order_relaxed);
    atomic_store_explicit(&wl_columnar_join_test_probe_set_reserved_at_entry,
        governor ? wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(governor)) : 0,
        memory_order_relaxed);
    atomic_store_explicit(&wl_columnar_join_test_probe_set_admitted_bytes, 0,
        memory_order_relaxed);
#endif
    if (col_join_bucket_count(right->nrows, &nbuckets) != 0)
        return ENOMEM;
    next_len = next_mode == WL_COLUMNAR_JOIN_PROBE_NEXT_ROWS_PLUS_ONE
        ? (size_t)right->nrows + 1u
        : (size_t)(right->nrows > 0 ? right->nrows : 1u);
    if (governor) {
        uint64_t hash_bytes = 0;
        wl_columnar_memory_admission_status_t status;
        if (wl_columnar_join_probe_set_bytes(right->nrows, next_mode,
            &hash_bytes) != 0)
            return EOVERFLOW;
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(governor), hash_bytes,
            &set->reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED && sess)
                sess->memory_budget_denied = true;
#ifdef WL_SESSION_TEST_HOOKS
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                atomic_fetch_add_explicit(
                    &wl_columnar_join_test_probe_set_build_denials, 1u,
                    memory_order_relaxed);
#endif
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                       ? ENOSPC
                       : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                       ? EOVERFLOW
                       : ENOMEM;
        }
        set->admitted = true;
#ifdef WL_SESSION_TEST_HOOKS
        atomic_store_explicit(&wl_columnar_join_test_probe_set_admitted_bytes,
            hash_bytes, memory_order_relaxed);
#endif
    }
    set->head = (uint32_t *)calloc(nbuckets, sizeof(uint32_t));
    set->next = (uint32_t *)malloc(next_len * sizeof(uint32_t));
    if (!set->head || !set->next) {
        wl_columnar_join_probe_set_release(set);
        return ENOMEM;
    }
    for (uint32_t rr = 0; rr < right->nrows; rr++) {
        uint32_t h = col_join_hash_rel_keys(right, rr, rk, kc)
            & (nbuckets - 1);
        set->next[rr] = set->head[h];
        set->head[h] = rr + 1; /* 1-based; 0 = end of chain */
    }
    set->right = right;
    set->rk = rk;
    set->kc = kc;
    set->nbuckets = nbuckets;
    set->next_mode = next_mode;
    set->built = true;
    return 0;
}

bool
wl_columnar_join_probe_set_contains(const wl_columnar_join_probe_set_t *set,
    const col_rel_t *left, uint32_t lr, const uint32_t *lk)
{
    uint32_t h = col_join_hash_rel_keys(left, lr, lk, set->kc)
        & (set->nbuckets - 1);
    for (uint32_t e = set->head[h]; e != 0; e = set->next[e - 1]) {
        if (col_join_keys_match_rel(left, lr, lk, set->right, e - 1,
            set->rk, set->kc))
            return true;
    }
    return false;
}

void
wl_columnar_join_probe_set_release(wl_columnar_join_probe_set_t *set)
{
    if (!set)
        return;
    free(set->head);
    free(set->next);
    set->head = NULL;
    set->next = NULL;
    if (set->admitted)
        (void)wl_columnar_memory_rollback(&set->reservation);
    set->admitted = false;
    set->built = false;
}

/* A prebuilt set must index the relation and key columns the emit
 * probes; otherwise its chains would answer for another relation.  It must
 * also have the emitting operator's next-link mode, which is part of that
 * operator's admitted footprint. */
static bool
wl_columnar_join_probe_set_matches(const wl_columnar_join_probe_set_t *set,
    const col_rel_t *right, const uint32_t *rk, uint32_t kc,
    wl_columnar_join_probe_next_t next_mode)
{
    if (set->right != right || set->kc != kc || set->next_mode != next_mode)
        return false;
    for (uint32_t k = 0; k < kc; k++) {
        if (set->rk[k] != rk[k])
            return false;
    }
    return true;
}

/* --- KEEP-ALL PASS-THROUGH (Issue #1370) --------------------------------- */

/* Whether a keep-all SEMIJOIN (@semijoin) or ANTIJOIN may return its input
 * entry instead of a copy.  Each condition keeps the input equal to the
 * copy the operator would build, as far as later operators can observe:
 *   - not a TDD bound run, which matches bound inputs by relation name;
 *   - a non-empty input without timestamps or compound metadata (the copy
 *     has neither);
 *   - column names present and non-NULL: a copy names every column, and a
 *     name lookup (col_rel_col_idx) on a relation without names fails;
 *   - SEMIJOIN only, because its copy is a fresh col0..colN relation with
 *     the left's types: an identity projection, column types present, no
 *     graph column, and each name exactly "col<i>", so every name lookup
 *     resolves as it would on the copy.  ANTIJOIN's copy is created like
 *     its left (col_rel_new_like), names, types and graph column included.
 * The input keeps what the copy would not carry and no later operator
 * compares: its relation name, and any sort metadata (sorted_nrows,
 * run_ends), with which a CONSOLIDATE may merge instead of sort. */
static bool
wl_columnar_filter_join_may_pass_through(const wl_plan_op_t *op,
    const wl_col_session_t *sess, const col_rel_t *left, bool semijoin)
{
    if (sess->tdd_input_run || left->nrows == 0 || left->timestamps
        || left->compound_kind != WIRELOG_COMPOUND_KIND_NONE
        || left->compound_arity_len != 0 || !left->col_names)
        return false;
    for (uint32_t c = 0; c < left->ncols; c++) {
        if (!left->col_names[c])
            return false;
    }
    if (!semijoin)
        return true;
#ifdef WL_TEST_JOIN_TIMESTAMP_HOOK
    if (wl_columnar_join_test_enable_semijoin_timestamps)
        return false;
#endif
    if (!left->column_types || left->has_graph_column)
        return false;
    if (op->project_count > 0) {
        if (op->project_count != left->ncols || !op->project_indices)
            return false;
        for (uint32_t c = 0; c < op->project_count; c++) {
            if (op->project_indices[c] != c)
                return false;
        }
    }
    for (uint32_t c = 0; c < left->ncols; c++) {
        char name[16];
        snprintf(name, sizeof(name), "col%u", c);
        if (strcmp(left->col_names[c], name) != 0)
            return false;
    }
    return true;
}

/* Build @set over @right in the operator's own next-link mode (the mode
 * its emit requires) and report in *@keeps_all whether the operator keeps
 * every row of @left: SEMIJOIN when every row is contained, ANTIJOIN when
 * none is.  The scan stops at the first row the operator drops.  On
 * success @set stays built for the emit; on failure it is not built. */
static int
wl_columnar_filter_join_scan_keeps_all(wl_col_session_t *sess,
    const col_rel_t *left, const col_rel_t *right, const uint32_t *lk,
    const uint32_t *rk, uint32_t kc, bool semijoin,
    wl_columnar_join_probe_set_t *set, bool *keeps_all)
{
    uint32_t lr = 0;
    int rc = wl_columnar_join_probe_set_build(sess,
            wl_columnar_join_output_governor(sess, left, right), right, rk,
            kc, semijoin ? WL_COLUMNAR_JOIN_PROBE_NEXT_ROWS
            : WL_COLUMNAR_JOIN_PROBE_NEXT_ROWS_PLUS_ONE, set);

    *keeps_all = false;
    if (rc != 0)
        return rc;
    while (lr < left->nrows
        && wl_columnar_join_probe_set_contains(set, left, lr, lk) == semijoin)
        lr++;
    *keeps_all = lr == left->nrows;
    return 0;
}

/* Publish @left_e itself as the operator's result.  It keeps its
 * ownership; it loses its CONCAT segment boundaries (a copy has none, so
 * a CONSOLIDATE sorts instead of merging) and is published as not a delta,
 * as the copy is. */
static int
wl_columnar_filter_join_pass_through(eval_stack_t *stack,
    eval_entry_t *left_e)
{
    free(left_e->seg_boundaries);
    left_e->seg_boundaries = NULL;
    left_e->seg_count = 0;
    left_e->is_delta = false;
    return eval_stack_repush_entry(stack, left_e);
}

/* --- ANTIJOIN ------------------------------------------------------------ */

int
wl_columnar_antijoin_new_output(const wl_plan_op_t *op,
    wl_col_session_t *sess, const col_rel_t *left, const col_rel_t *right,
    col_rel_t **out_p)
{
    col_rel_t *out = NULL;
    wl_columnar_memory_governor_ref_t *output_governor
        = wl_columnar_join_output_governor(sess, left, right);
    int output_rc;

    (void)op;
    *out_p = NULL;
    output_rc = output_governor
        ? wl_columnar_relation_new_like_governed_checked_mode("$antijoin",
            left, output_governor, false, &out)
        : ((out = col_rel_pool_new_like(sess->delta_pool, "$antijoin", left))
            ? 0 : ENOMEM);
    if (output_rc != 0) {
        if (output_rc == ENOSPC)
            sess->memory_budget_denied = true;
        return output_rc;
    }
    col_join_attach_ledger(sess, out);
    *out_p = out;
    return 0;
}

int
wl_columnar_antijoin_emit(const wl_plan_op_t *op, wl_col_session_t *sess,
    const col_rel_t *left, const col_rel_t *right, const uint32_t *lk,
    const uint32_t *rk, wl_columnar_join_probe_set_t *set, col_rel_t **out_p)
{
    wl_columnar_memory_governor_ref_t *scratch_governor
        = wl_columnar_join_output_governor(sess, left, right);
    uint32_t kc = op->key_count;
    wl_columnar_join_probe_set_t local_set;
    bool own_set = !set->built;

#ifdef WL_SESSION_TEST_HOOKS
    atomic_fetch_add_explicit(&wl_columnar_join_test_probe_emits, 1u,
        memory_order_relaxed);
    if (wl_columnar_join_test_before_probe_emit)
        wl_columnar_join_test_before_probe_emit(left);
#endif
    *out_p = NULL;
    if (!own_set && !wl_columnar_join_probe_set_matches(set, right, rk, kc,
        WL_COLUMNAR_JOIN_PROBE_NEXT_ROWS_PLUS_ONE))
        return EINVAL;
    col_rel_t *out = NULL;
    int output_rc = wl_columnar_antijoin_new_output(op, sess, left, right,
            &out);
    if (output_rc != 0)
        return output_rc;

    /* Hash antijoin: build hash set from right, iterate left. */
    if (own_set) {
        int build_rc = wl_columnar_join_probe_set_build(sess,
                scratch_governor, right, rk, kc,
                WL_COLUMNAR_JOIN_PROBE_NEXT_ROWS_PLUS_ONE, &local_set);
        if (build_rc != 0) {
            col_rel_destroy(out);
            return build_rc;
        }
        set = &local_set;
    }
    int aj_rc = 0;
    wl_columnar_memory_reservation_t aj_row_reservation;
    bool aj_row_admitted = false;
    wl_columnar_memory_reservation_init(&aj_row_reservation);
    if (scratch_governor) {
        uint64_t row_bytes = 0;
        wl_columnar_memory_admission_status_t status;
        if (!wl_columnar_memory_size_mul(
                left->ncols ? left->ncols : 1, sizeof(int64_t),
                &row_bytes)) {
            aj_rc = EOVERFLOW;
            goto antijoin_done;
        }
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(scratch_governor),
            row_bytes, &aj_row_reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                sess->memory_budget_denied = true;
            aj_rc = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : ENOMEM;
            goto antijoin_done;
        }
        aj_row_admitted = true;
    }
    int64_t *lrow_buf = (int64_t *)malloc(
        sizeof(int64_t) * (left->ncols ? left->ncols : 1));
    if (!lrow_buf) {
        aj_rc = ENOMEM;
        goto antijoin_done;
    }
    {
        /* #2072: one batch lease for the output rather than one mutation
         * set per row; it ends before out can be destroyed below. */
        col_rel_append_batch_t aj_batch = { 0 };
        aj_rc = col_rel_append_batch_begin(&aj_batch, out);
        for (uint32_t lr = 0; lr < left->nrows && aj_rc == 0; lr++) {
            if (!wl_columnar_join_probe_set_contains(set, left, lr, lk)) {
                for (uint32_t c = 0; c < left->ncols; c++)
                    lrow_buf[c] = left->columns[c][lr];
                aj_rc = col_rel_append_batch_row(&aj_batch, lrow_buf);
            }
        }
        aj_rc = col_join_pair_batch_end(&aj_batch, aj_rc);
    }
    free(lrow_buf);
antijoin_done:
    if (own_set)
        wl_columnar_join_probe_set_release(&local_set);
    if (aj_row_admitted)
        (void)wl_columnar_memory_rollback(&aj_row_reservation);
    if (aj_rc != 0) {
        aj_rc = wl_columnar_join_output_error(sess, out, aj_rc);
        col_rel_destroy(out);
        return aj_rc;
    }
    *out_p = out;
    return 0;
}

int
wl_columnar_antijoin_op(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    /* out and its copies remain private until push; right_filtered is the
     * uncached operation-owned fallback. Antijoin completes its synchronous
     * probes before cleanup, and no filtered relation is transferred to cache. */
    eval_entry_t left_e;
    int pop_rc = eval_stack_pop_relation(stack, &left_e);
    if (pop_rc != 0)
        return pop_rc;

    col_rel_t *right_filtered = NULL;
    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right) {
        /* If right relation doesn't exist, antijoin keeps all left rows */
        return eval_stack_repush_entry(stack, &left_e);
    }

    /* Issue #386: antijoin filter caching is not yet implemented.
     * Antijoin always uses an ephemeral pool-allocated filtered relation, so
     * the per-iteration filter cost is O(N) — acceptable for current workloads
     * but a candidate for follow-up optimization. */
    if (op->right_filter_expr.size > 0) {
        col_rel_t *filtered = NULL;
        int filter_rc = wl_columnar_filter_apply_right_filter_governed_checked(
            &op->right_filter_expr, right, sess->delta_pool, sess->intern,
            sess->memory_governor ? sess->memory_governor :
            right->memory_governor, sess, &filtered);
        if (filter_rc != 0) {
            int cleanup_rc = eval_stack_dispose_entry(stack, &left_e);
            return cleanup_rc != 0 ? cleanup_rc : filter_rc;
        }
        right = filtered;
        right_filtered = filtered;
    }

    col_rel_t *left = left_e.rel;
    uint32_t kc = op->key_count;

    wl_columnar_join_key_scratch_t key_scratch;
    int key_rc = wl_columnar_join_key_scratch_alloc(sess, left, right, kc,
            &key_scratch);
    uint32_t *lk = key_scratch.left_keys;
    uint32_t *rk = key_scratch.right_keys;
    if (key_rc != 0) {
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return wl_columnar_join_dispose_left(stack, &left_e, key_rc);
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
        wl_columnar_join_key_scratch_free(&key_scratch, &lk);
        wl_columnar_join_key_scratch_free(&key_scratch, &rk);
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return wl_columnar_join_dispose_left(stack, &left_e, EINVAL);
    }

    wl_columnar_join_probe_set_t set;
    col_rel_t *out = NULL;
    bool keeps_all = false;
    int emit_rc = 0;
    memset(&set, 0, sizeof(set));
    /* Issue #1370: a keep-all ANTIJOIN returns its input.  Otherwise the
     * set built for the scan is the one the emit probes. */
    if (wl_columnar_filter_join_may_pass_through(op, sess, left, false))
        emit_rc = wl_columnar_filter_join_scan_keeps_all(sess, left, right,
                lk, rk, kc, false, &set, &keeps_all);
    if (emit_rc == 0 && !keeps_all)
        emit_rc = wl_columnar_antijoin_emit(op, sess, left, right, lk, rk,
                &set, &out);
    wl_columnar_join_probe_set_release(&set);
    wl_columnar_join_key_scratch_free(&key_scratch, &lk);
    wl_columnar_join_key_scratch_free(&key_scratch, &rk);
    if (right_filtered)
        col_rel_destroy(right_filtered);
    if (emit_rc != 0)
        return wl_columnar_join_dispose_left(stack, &left_e, emit_rc);
    if (keeps_all)
        return wl_columnar_filter_join_pass_through(stack, &left_e);
    return wl_columnar_join_publish_after_left(stack, &left_e, out, false);
}

/* --- SEMIJOIN ------------------------------------------------------------ */

int
wl_columnar_semijoin_new_output(const wl_plan_op_t *op,
    wl_col_session_t *sess, const col_rel_t *left, const col_rel_t *right,
    col_rel_t **out_p)
{
    /* Output: project_indices selects output columns from left */
    uint32_t ocols = op->project_count ? op->project_count : left->ncols;
    col_rel_t *out = NULL;
    int output_rc;

    *out_p = NULL;
    output_rc = wl_columnar_join_new_output(sess, left, right, "$semijoin",
            ocols, COL_REL_INIT_CAP,
#ifdef WL_TEST_JOIN_TIMESTAMP_HOOK
            wl_columnar_join_test_enable_semijoin_timestamps,
#else
            false,
#endif
            false, &out);
    if (output_rc != 0)
        return output_rc;
    output_rc = col_join_set_left_output_types(out, left, op);
    if (output_rc != 0) {
        col_rel_destroy(out);
        return output_rc;
    }
    *out_p = out;
    return 0;
}

int
wl_columnar_semijoin_emit(const wl_plan_op_t *op, wl_col_session_t *sess,
    const col_rel_t *left, const col_rel_t *right, const uint32_t *lk,
    const uint32_t *rk, wl_columnar_join_probe_set_t *set, col_rel_t **out_p)
{
    wl_columnar_memory_governor_ref_t *scratch_governor
        = wl_columnar_join_output_governor(sess, left, right);
    uint32_t kc = op->key_count;
    wl_columnar_join_probe_set_t local_set;
    bool own_set = !set->built;

#ifdef WL_SESSION_TEST_HOOKS
    atomic_fetch_add_explicit(&wl_columnar_join_test_probe_emits, 1u,
        memory_order_relaxed);
    if (wl_columnar_join_test_before_probe_emit)
        wl_columnar_join_test_before_probe_emit(left);
#endif
    *out_p = NULL;
    if (!own_set && !wl_columnar_join_probe_set_matches(set, right, rk, kc,
        WL_COLUMNAR_JOIN_PROBE_NEXT_ROWS))
        return EINVAL;
    /* Output: project_indices selects output columns from left */
    uint32_t ocols = op->project_count ? op->project_count : left->ncols;
    col_rel_t *out = NULL;
    int output_rc = wl_columnar_semijoin_new_output(op, sess, left, right,
            &out);
    if (output_rc != 0)
        return output_rc;
    wl_columnar_memory_reservation_t sj_row_reservation;
    bool sj_row_admitted = false;
    wl_columnar_memory_reservation_init(&sj_row_reservation);
    if (scratch_governor) {
        uint64_t row_bytes = 0;
        wl_columnar_memory_admission_status_t status;
        if (!wl_columnar_memory_size_mul(ocols ? ocols : 1,
            sizeof(int64_t), &row_bytes)) {
            col_rel_destroy(out);
            return EOVERFLOW;
        }
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(scratch_governor),
            row_bytes, &sj_row_reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                sess->memory_budget_denied = true;
            col_rel_destroy(out);
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                       ? ENOSPC
                       : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                       ? EOVERFLOW : ENOMEM;
        }
        sj_row_admitted = true;
    }
    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        if (sj_row_admitted)
            (void)wl_columnar_memory_rollback(&sj_row_reservation);
        col_rel_destroy(out);
        return ENOMEM;
    }

    /* Build hash set from right relation join keys: O(|R|) */
    if (own_set) {
        int build_rc = wl_columnar_join_probe_set_build(sess,
                scratch_governor, right, rk, kc,
                WL_COLUMNAR_JOIN_PROBE_NEXT_ROWS, &local_set);
        if (build_rc != 0) {
            free(tmp);
            if (sj_row_admitted)
                (void)wl_columnar_memory_rollback(&sj_row_reservation);
            col_rel_destroy(out);
            return build_rc;
        }
        set = &local_set;
    }
    uint32_t *ht_head = set->head;
    uint32_t *ht_next = set->next;
    uint32_t nbuckets = set->nbuckets;

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
            wl_columnar_memory_reservation_t parallel_reservation;
            bool parallel_admitted = false;
            wl_columnar_memory_reservation_init(&parallel_reservation);
            uint64_t ctx_bytes = 0;
            uint64_t offset_count = (uint64_t)W + 1u;
            uint64_t offset_bytes = 0;
            uint64_t hash_bytes = 0;
            uint64_t total_bytes = 0;
            bool sized = wl_columnar_memory_size_mul(W,
                    sizeof(col_semijoin_ctx_t), &ctx_bytes)
                && wl_columnar_memory_size_mul(offset_count,
                    sizeof(uint64_t), &offset_bytes)
                && wl_columnar_memory_size_mul(
                left->nrows > 0 ? left->nrows : 1,
                sizeof(uint32_t), &hash_bytes)
                && wl_columnar_memory_size_add(ctx_bytes, offset_bytes,
                    &total_bytes)
                && wl_columnar_memory_size_add(total_bytes, hash_bytes,
                    &total_bytes)
                && ctx_bytes <= SIZE_MAX && offset_bytes <= SIZE_MAX
                && hash_bytes <= SIZE_MAX;
            if (!sized) {
                sj_rc = EOVERFLOW;
                goto semijoin_parallel_done;
            }
            if (scratch_governor) {
                wl_columnar_memory_admission_status_t status;
                status = wl_columnar_memory_reserve_checked(
                    wl_columnar_memory_governor_ref_get(
                        scratch_governor),
                    total_bytes, &parallel_reservation);
                if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                    && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
                    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                        sess->memory_budget_denied = true;
                    sj_rc = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                        ? ENOSPC
                        : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                        ? EOVERFLOW
                        : ENOMEM;
                    goto semijoin_parallel_done;
                }
                parallel_admitted = true;
            }
#ifdef WL_TEST_JOIN_CACHE_HOOK
            wl_columnar_join_test_last_semijoin_parallel_admitted
                = parallel_admitted;
#endif
            col_semijoin_ctx_t *ctxs = (col_semijoin_ctx_t *)calloc(1,
                    (size_t)ctx_bytes);
            uint64_t *offsets = (uint64_t *)calloc(1,
                    (size_t)offset_bytes);
            uint32_t *left_hashes = (uint32_t *)malloc((size_t)hash_bytes);
            if (ctxs && offsets && left_hashes) {
                atomic_int write_error = ATOMIC_VAR_INIT(0);
                uint32_t chunk = (uint32_t)(((uint64_t)left->nrows
                    + (uint64_t)W - 1u) / W);
                int prc = 0;
                for (uint32_t w = 0; w < W; w++) {
                    uint64_t begin_wide = (uint64_t)w * chunk;
                    uint64_t end_wide = begin_wide + chunk;
                    uint32_t begin = begin_wide > left->nrows
                        ? left->nrows : (uint32_t)begin_wide;
                    uint32_t end = end_wide > left->nrows
                        ? left->nrows : (uint32_t)end_wide;
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
                    if (wl_columnar_join_submit(sess->wq,
                        col_semijoin_count_worker_fn, &ctxs[w]) != 0)
                        prc = ENOMEM;
                }
                wl_workqueue_wait_all(sess->wq);
                uint64_t total = 0;
                for (uint32_t w = 0; w < W; w++) {
                    offsets[w] = total;
                    if (!wl_columnar_memory_size_add(total, ctxs[w].count,
                        &total)) {
                        prc = EOVERFLOW;
                        total = 0;
                        break;
                    }
                }
                offsets[W] = total;
                if (prc == 0 && total > UINT32_MAX)
                    prc = EOVERFLOW;
                if (prc == 0 && col_join_reserve_exact(out,
                    (uint32_t)total) != 0)
                    prc = ENOMEM;
                if (prc == 0) {
                    out->nrows = (uint32_t)total;
                    if (total > 0)
                        wl_columnar_relation_touch_view(out);
                    if (out->timestamps && total > 0)
                        memset(out->timestamps, 0,
                            (size_t)total * sizeof(col_delta_timestamp_t));
                    for (uint32_t w = 0; w < W; w++) {
                        ctxs[w].out = out;
                        ctxs[w].out_begin = offsets[w];
                        if (wl_columnar_join_submit(sess->wq,
                            col_semijoin_fill_worker_fn, &ctxs[w]) != 0)
                            prc = ENOMEM;
                    }
                    wl_workqueue_wait_all(sess->wq);
                    if (prc == 0)
                        prc = atomic_load_explicit(&write_error,
                                memory_order_relaxed);
                }
                if (prc != 0)
                    if (out->nrows != 0) {
                        out->nrows = 0;
                        wl_columnar_relation_touch_view(out);
                    }
                free(left_hashes);
                free(offsets);
                free(ctxs);
                if (parallel_admitted)
                    (void)wl_columnar_memory_rollback(
                        &parallel_reservation);
                parallel_admitted = false;
                if (prc == 0)
                    goto semijoin_done;
            } else {
                free(left_hashes);
                free(offsets);
                free(ctxs);
                if (parallel_admitted)
                    (void)wl_columnar_memory_rollback(
                        &parallel_reservation);
                parallel_admitted = false;
            }
semijoin_parallel_done:
            if (parallel_admitted)
                (void)wl_columnar_memory_rollback(&parallel_reservation);
            parallel_admitted = false;
            if (sj_rc != 0)
                goto semijoin_done;
        }
    }

    /* Probe: for each left row test membership, emit if found: O(|L|).
     * Reached only after the parallel attempt above, which writes out
     * directly, did not run or fell back.  #2072: the serial probe appends
     * under one batch lease rather than one mutation set per row; it ends
     * before out can be destroyed below. */
    {
        col_rel_append_batch_t sj_batch = { 0 };
        sj_rc = col_rel_append_batch_begin(&sj_batch, out);
        for (uint32_t lr = 0; lr < left->nrows && sj_rc == 0; lr++) {
            if (wl_columnar_join_probe_set_contains(set, left, lr, lk)) {
                if (op->project_count > 0 && op->project_indices) {
                    for (uint32_t c = 0; c < ocols; c++) {
                        uint32_t si = op->project_indices[c];
                        tmp[c] = (si < left->ncols)
                            ? left->columns[si][lr] : 0;
                    }
                } else {
                    for (uint32_t c = 0; c < left->ncols; c++)
                        tmp[c] = left->columns[c][lr];
                }
                sj_rc = col_rel_append_batch_row(&sj_batch, tmp);
            }
        }
        sj_rc = col_join_pair_batch_end(&sj_batch, sj_rc);
    }

semijoin_done:
    free(tmp);
    tmp = NULL;
    if (own_set)
        wl_columnar_join_probe_set_release(&local_set);
    if (sj_row_admitted)
        (void)wl_columnar_memory_rollback(&sj_row_reservation);
    if (sj_rc != 0) {
        sj_rc = wl_columnar_join_output_error(sess, out, sj_rc);
        col_rel_destroy(out);
        return sj_rc;
    }
    *out_p = out;
    return 0;
}

int
wl_columnar_semijoin_op(const wl_plan_op_t *op, eval_stack_t *stack,
    wl_col_session_t *sess)
{
    /* out and its copies remain private until push; right_filtered is the
     * uncached operation-owned fallback. Semijoin completes its synchronous
     * probes before cleanup, and no filtered relation is transferred to cache. */
    col_rel_t *right = NULL;
    bool bound_delta = false;
    if (sess->tdd_input_run) {
        int bound_rc = wl_columnar_eval_tdd_plan_resolve_input(sess, op, true,
                &right, &bound_delta);
        if (bound_rc != 0)
            return bound_rc;
    } else {
        right = session_find_rel(sess, op->right_relation);
    }
    col_rel_t *right_filtered = NULL;
    eval_entry_t left_e;
    int pop_rc = eval_stack_pop_relation(stack, &left_e);
    if (pop_rc != 0)
        return pop_rc;

    if (!right)
        return eval_stack_repush_entry(stack, &left_e);

    /* Issue #386: semijoin filter caching is not yet implemented.
     * Semijoin always uses an ephemeral pool-allocated filtered relation, so
     * the per-iteration filter cost is O(N) — acceptable for current workloads
     * but a candidate for follow-up optimization. */
    if (op->right_filter_expr.size > 0) {
        col_rel_t *filtered = NULL;
        int filter_rc = wl_columnar_filter_apply_right_filter_governed_checked(
            &op->right_filter_expr, right, sess->delta_pool, sess->intern,
            sess->memory_governor ? sess->memory_governor :
            right->memory_governor, sess, &filtered);
        if (filter_rc != 0) {
            int cleanup_rc = eval_stack_dispose_entry(stack, &left_e);
            return cleanup_rc != 0 ? cleanup_rc : filter_rc;
        }
        right = filtered;
        right_filtered = filtered;
    }

    col_rel_t *left = left_e.rel;
    uint32_t kc = op->key_count;

    wl_columnar_join_key_scratch_t key_scratch;
    int key_rc = wl_columnar_join_key_scratch_alloc(sess, left, right, kc,
            &key_scratch);
    uint32_t *lk = key_scratch.left_keys;
    uint32_t *rk = key_scratch.right_keys;
    if (key_rc != 0) {
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return wl_columnar_join_dispose_left(stack, &left_e, key_rc);
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
        wl_columnar_join_key_scratch_free(&key_scratch, &lk);
        wl_columnar_join_key_scratch_free(&key_scratch, &rk);
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return wl_columnar_join_dispose_left(stack, &left_e, EINVAL);
    }

    wl_columnar_join_probe_set_t set;
    col_rel_t *out = NULL;
    bool keeps_all = false;
    int emit_rc = 0;
    memset(&set, 0, sizeof(set));
    /* Issue #1370: a keep-all SEMIJOIN returns its input.  Otherwise the
     * set built for the scan is the one the emit probes. */
    if (wl_columnar_filter_join_may_pass_through(op, sess, left, true))
        emit_rc = wl_columnar_filter_join_scan_keeps_all(sess, left, right,
                lk, rk, kc, true, &set, &keeps_all);
    if (emit_rc == 0 && !keeps_all)
        emit_rc = wl_columnar_semijoin_emit(op, sess, left, right, lk, rk,
                &set, &out);
    wl_columnar_join_probe_set_release(&set);
    wl_columnar_join_key_scratch_free(&key_scratch, &lk);
    wl_columnar_join_key_scratch_free(&key_scratch, &rk);
    if (right_filtered)
        col_rel_destroy(right_filtered);
    if (emit_rc != 0)
        return wl_columnar_join_dispose_left(stack, &left_e, emit_rc);
    if (keeps_all)
        return wl_columnar_filter_join_pass_through(stack, &left_e);
    return wl_columnar_join_publish_after_left(stack, &left_e, out, false);
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
    /* out/copy are local until a successful stack push. Cache-owned originals
     * are removed through the cache API; worker/probe dependencies are joined
     * or released before local results and right_filtered are destroyed. */
    col_rel_t *right_filtered = NULL;
    eval_entry_t left_e;
    int pop_rc = eval_stack_pop_relation(stack, &left_e);
    if (pop_rc != 0)
        return pop_rc;

    col_rel_t *right = session_find_rel(sess, op->right_relation);
    if (!right) {
        uint32_t empty_cols = (op->project_count > 0 && op->project_indices)
            ? op->project_count : left_e.rel->ncols;
        col_rel_t *out = NULL;
        int create_rc = wl_columnar_join_new_output(sess, left_e.rel, NULL,
                "$join_diff_empty", empty_cols, COL_REL_INIT_CAP, false,
                false, &out);
        if (create_rc != 0)
            return wl_columnar_join_dispose_left(stack, &left_e, create_rc);
        create_rc = col_join_set_left_output_types(out, left_e.rel, op);
        if (create_rc != 0) {
            col_rel_destroy(out);
            return wl_columnar_join_dispose_left(stack, &left_e, create_rc);
        }
        return wl_columnar_join_publish_after_left(stack, &left_e, out, false);
    }

    wl_columnar_join_selection_t selection = wl_columnar_join_select_right(
        op, sess, right, left_e.is_delta);
    right = selection.right;
    bool used_right_delta = selection.used_right_delta;
    if (selection.force_empty) {
        uint32_t ocols = col_join_output_width(left_e.rel, right, op);
        col_rel_t *empty = NULL;
        int create_rc = wl_columnar_join_new_output(sess, left_e.rel, right,
                "$join_diff_empty", ocols, COL_REL_INIT_CAP, false, true,
                &empty);
        if (create_rc != 0)
            return wl_columnar_join_dispose_left(stack, &left_e, create_rc);
        /* The schema copy still reads the left input (#1376). */
        create_rc = col_join_set_output_types(empty, left_e.rel, right, op);
        if (create_rc != 0) {
            col_rel_destroy(empty);
            return wl_columnar_join_dispose_left(stack, &left_e, create_rc);
        }
        return wl_columnar_join_publish_after_left(stack, &left_e, empty,
                   false);
    }

    /* Apply constant filter on right child (from FILTER wrappers collected
     * during plan generation).  Use session-level cache (Issue #386): the
     * filtered relation is owned by sess->filt_cache and must NOT be
     * destroyed here.  right_filtered remains NULL for the cached path.
     * The cache reports itself unavailable while a lease defers a rebuild
     * or blocks growth (Issue #1435); allocation and admission failures are
     * returned instead of being hidden by an owned fallback. */
    if (op->right_filter_expr.size > 0) {
        col_rel_t *filtered = NULL;
        bool cache_unavailable = false;
        int filter_rc = 0;
        if (op->right_relation && !used_right_delta) {
            col_filt_cache_pin_t filter_pin = { 0 };
            filter_rc =
                wl_columnar_filter_apply_right_filter_cached_pin_checked(
                sess, &op->right_filter_expr, op->right_relation, right,
                &filter_pin, &filtered, &cache_unavailable);
            col_filt_cache_pin_release(&filter_pin);
            if (filter_rc != 0) {
                int cleanup_rc = eval_stack_dispose_entry(stack, &left_e);
                return cleanup_rc != 0 ? cleanup_rc : filter_rc;
            }
            if (cache_unavailable)
                WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_DEBUG,
                    "filtered cache unavailable for %s, using owned filter",
                    op->right_relation);
        }
        if (filtered) {
            right = filtered;
            /* right_filtered stays NULL: cache owns the relation */
        } else if (cache_unavailable || !op->right_relation ||
            used_right_delta) {
            /* Delta path, no relation name, or cache unavailable:
             * pool-allocated filter owned by this op */
            filter_rc = wl_columnar_filter_apply_right_filter_governed_checked(
                &op->right_filter_expr, right, sess->delta_pool, sess->intern,
                sess->memory_governor ? sess->memory_governor :
                right->memory_governor, sess, &filtered);
            if (filter_rc != 0) {
                int cleanup_rc = eval_stack_dispose_entry(stack, &left_e);
                return cleanup_rc != 0 ? cleanup_rc : filter_rc;
            }
            right = filtered;
            right_filtered = filtered;
        } else if (!filtered) {
            int cleanup_rc = eval_stack_dispose_entry(stack, &left_e);
            return cleanup_rc != 0 ? cleanup_rc : EINVAL;
        }
    }

    /* Materialization cache check */
    bool projected_join = op->project_count > 0 && op->project_indices;
    /* Owned filtered fallbacks are operation-local; see the ordinary path. */
    if (op->materialized && !projected_join && !right_filtered) {
        col_mat_cache_pin_t cache_pin = { 0 };
        col_rel_t *cached
            = col_mat_cache_lookup_pin(&sess->mat_cache, left_e.rel, right,
                &cache_pin);
        if (cached) {
            col_rel_t *copy = NULL;
            int copy_rc = wl_columnar_relation_deep_copy_governed(cached,
                    &copy, wl_columnar_join_effective_governor(sess, cached));
            col_mat_cache_pin_release(&cache_pin);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            if (copy_rc != 0)
                return wl_columnar_join_dispose_left(stack, &left_e,
                           wl_columnar_join_memory_error(sess, copy_rc));
            return wl_columnar_join_publish_after_left(stack, &left_e, copy,
                       left_e.is_delta || used_right_delta);
        }
    }

    uint32_t kc = op->key_count;
    col_rel_t *left = left_e.rel;
    wl_columnar_memory_governor_ref_t *scratch_governor
        = wl_columnar_join_output_governor(sess, left, right);

    /* Resolve key column positions */
    wl_columnar_join_key_scratch_t key_scratch;
    int key_rc = wl_columnar_join_key_scratch_alloc(sess, left, right, kc,
            &key_scratch);
    uint32_t *lk = key_scratch.left_keys;
    uint32_t *rk = key_scratch.right_keys;
    if (key_rc != 0) {
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return wl_columnar_join_dispose_left(stack, &left_e, key_rc);
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
        wl_columnar_join_key_scratch_free(&key_scratch, &lk);
        wl_columnar_join_key_scratch_free(&key_scratch, &rk);
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return wl_columnar_join_dispose_left(stack, &left_e, EINVAL);
    }

    /* Differential bounded mode uses a dedicated producer because the
     * differential registry has its own arrangement and a separate
     * generation lease.  Like the one-shot $join_diff below, its output
     * carries no timestamps (Issue #2173).  Materialized results remain on
     * the one-shot path until their cache transaction can be made
     * operation-atomic.  Excluded shapes are decided by the same check as
     * the ordinary join, then materialized; each falls back to the one-shot
     * join with its reason recorded, or fails with ENOTSUP in strict mode
     * (#1382). */
    col_join_batch_eligibility_t batch_elig = col_join_batch_eligibility(sess,
            kc, used_right_delta, op->right_filter_expr.size != 0);
    if (batch_elig == COL_JOIN_BATCH_ELIGIBLE && op->materialized)
        batch_elig = COL_JOIN_BATCH_EXCLUDED_MATERIALIZED;
    if (sess->join_batch_bytes > 0 && batch_elig != COL_JOIN_BATCH_ELIGIBLE) {
        if (sess->join_batch_strict) {
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_dispose_left(stack, &left_e, ENOTSUP);
        }
        col_join_batch_record_fallback(sess, batch_elig);
    }
    if (batch_elig == COL_JOIN_BATCH_ELIGIBLE) {
        wl_columnar_continuation_t *cont = NULL;
        col_rel_t *bounded_out = NULL;
        int bounded_rc = wl_columnar_join_new_output(sess, left, right,
                "$join_diff_batch", col_join_output_width(left, right, op),
                COL_REL_INIT_CAP, false, true, &bounded_out);
        if (bounded_rc == 0)
            bounded_rc = wl_columnar_join_budget_denial(sess, bounded_out,
                    col_join_set_output_types(bounded_out, left, right, op));
        if (bounded_rc == 0) {
            bounded_rc = col_diff_join_batch_producer_create(sess, op, left,
                    left_e.is_delta, lk, rk, kc, sess->join_batch_bytes, &cont);
            /* As in the ordinary join, a create ENOTSUP is recorded as
             * row-too-large: one output row does not fit the batch
             * budget. */
            if (bounded_rc == ENOTSUP && !sess->join_batch_strict)
                col_join_batch_record_fallback(sess,
                    COL_JOIN_BATCH_EXCLUDED_ROW_TOO_LARGE);
        }
        if (bounded_rc == 0) {
            bounded_rc = wl_columnar_join_budget_denial(sess, bounded_out,
                    col_join_batch_run_to_relation(cont, sess, bounded_out));
            wl_columnar_continuation_destroy(cont);
            cont = NULL;
        }
        if (bounded_rc == 0) {
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_publish_after_left(stack, &left_e,
                       bounded_out, left_e.is_delta);
        }
        if (cont)
            wl_columnar_continuation_destroy(cont);
        if (bounded_out)
            col_rel_destroy(bounded_out);
        if (sess->join_batch_strict || (bounded_rc != ENOTSUP
            && bounded_rc != ENOENT)) {
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_dispose_left(stack, &left_e, bounded_rc);
        }
    }

    uint32_t ocols = col_join_output_width(left, right, op);
    /* Materialized results outlive the current delta-pool reset while they
     * remain in mat_cache, so cache-owned joins must be heap allocated. */
    col_rel_t *out = NULL;
    int output_rc = wl_columnar_join_new_output(sess, left, right, "$join_diff",
            ocols, COL_REL_INIT_CAP, false,
            (op->materialized && !projected_join), &out);
    if (output_rc != 0) {
        wl_columnar_join_key_scratch_free(&key_scratch, &lk);
        wl_columnar_join_key_scratch_free(&key_scratch, &rk);
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return wl_columnar_join_dispose_left(stack, &left_e, output_rc);
    }
    output_rc = col_join_set_output_types(out, left, right, op);
    if (output_rc != 0) {
        wl_columnar_join_key_scratch_free(&key_scratch, &lk);
        wl_columnar_join_key_scratch_free(&key_scratch, &rk);
        col_rel_destroy(out);
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return wl_columnar_join_dispose_left(stack, &left_e, output_rc);
    }
    wl_columnar_memory_reservation_t tmp_reservation;
    bool tmp_admitted = false;
    wl_columnar_memory_reservation_init(&tmp_reservation);
    if (scratch_governor) {
        uint64_t tmp_bytes = 0;
        wl_columnar_memory_admission_status_t status;
        if (!wl_columnar_memory_size_mul(ocols ? ocols : 1,
            sizeof(int64_t), &tmp_bytes)) {
            col_rel_destroy(out);
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_dispose_left(stack, &left_e,
                       EOVERFLOW);
        }
        status = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(scratch_governor),
            tmp_bytes, &tmp_reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                sess->memory_budget_denied = true;
            col_rel_destroy(out);
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_dispose_left(stack, &left_e,
                       status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                           ? ENOSPC
                           : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                           ? EOVERFLOW : ENOMEM);
        }
        tmp_admitted = true;
    }
#define WL_DIFF_RELEASE_TMP() \
        do { \
            free(tmp); \
            tmp = NULL; \
            if (tmp_admitted) { \
                (void)wl_columnar_memory_rollback(&tmp_reservation); \
                tmp_admitted = false; \
            } \
        } while (0)
    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols ? ocols : 1));
    if (!tmp) {
        WL_DIFF_RELEASE_TMP();
        col_rel_destroy(out);
        wl_columnar_join_key_scratch_free(&key_scratch, &lk);
        wl_columnar_join_key_scratch_free(&key_scratch, &rk);
        if (right_filtered)
            col_rel_destroy(right_filtered);
        return wl_columnar_join_dispose_left(stack, &left_e, ENOMEM);
    }

    int join_rc = 0;
    col_arrangement_probe_bundle_t diff_bundle = { 0 };
    wl_columnar_arrangement_diff_txn_t diff_txn = { 0 };

    /* DIFFERENTIAL PATH: persistent diff_arrangement for non-delta right.
     * The arrangement persists across iterations, only indexing new rows. */
    col_diff_arrangement_t *darr = NULL;
    if (kc > 0 && op->right_relation && !used_right_delta
        && op->right_filter_expr.size == 0) {
        col_arrangement_probe_bundle_init(&diff_bundle);
        int dependency_rc = col_arrangement_probe_bundle_acquire_dependency(
            &diff_bundle, right);
        if (dependency_rc != 0) {
            WL_DIFF_RELEASE_TMP();
            col_rel_destroy(out);
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            (void)col_arrangement_probe_bundle_release(&diff_bundle);
            return wl_columnar_join_dispose_left(stack, &left_e, dependency_rc);
        }
        int txn_rc = wl_columnar_arrangement_diff_txn_begin(sess,
                op->right_relation, right, rk, kc, &diff_txn);
        if (txn_rc == 0)
            darr = diff_txn.working;
        else
            (void)col_arrangement_probe_bundle_release(&diff_bundle);
    }

    if (darr && darr->indexed_rows != right->nrows) {
        /* Relation snapshots change on mutation, so a partial same-token
         * index is never a supported state. Rebuild the private transaction
         * copy from row zero after token invalidation or unexpected drift. */
        wl_columnar_arrangement_diff_reset_state(darr);
        if (col_diff_arrangement_ensure_ht_capacity(darr, right->nrows) != 0) {
            wl_columnar_arrangement_diff_txn_abort(&diff_txn);
            darr = NULL; /* fall through to the ephemeral arrangement */
            (void)col_arrangement_probe_bundle_release(&diff_bundle);
        }
    }

    uint32_t nbk = darr ? darr->nbuckets : 0;
    if (darr && darr->indexed_rows != right->nrows) {
        for (uint32_t rr = 0; rr < right->nrows; rr++) {
            uint32_t h = col_join_hash_rel_keys(right, rr, rk, kc) & (nbk - 1);
            darr->ht_next[rr] = darr->ht_head[h];
            darr->ht_head[h] = rr + 1; /* 1-based; 0 = end of chain */
        }
        darr->indexed_rows = right->nrows;
    }

    if (darr) {
        darr->current_nrows = right->nrows;
        darr->source_snapshot = wl_columnar_relation_snapshot(right);

        uint32_t min_left = col_join_parallel_min_left_rows();
        if (min_left == 0)
            min_left = 1;
        uint32_t W = left->nrows / min_left;
        if (W > sess->num_workers)
            W = sess->num_workers;
        if (!sess->coordinator && W > 1) {
            int ensure_rc = wl_columnar_session_ensure_workqueue(sess, W);
            if (ensure_rc != 0) {
                WL_DIFF_RELEASE_TMP();
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                wl_columnar_arrangement_diff_txn_abort(&diff_txn);
                (void)col_arrangement_probe_bundle_release(&diff_bundle);
                return wl_columnar_join_dispose_left(stack, &left_e, ensure_rc);
            }
#ifdef WL_TEST_JOIN_CACHE_HOOK
            wl_columnar_join_test_last_keyed_workers = W;
#endif
            wl_columnar_memory_reservation_t parallel_reservation;
            bool parallel_admitted = false;
            wl_columnar_memory_reservation_init(&parallel_reservation);
            uint64_t ctx_bytes = 0;
            uint64_t offset_count = (uint64_t)W + 1u;
            uint64_t offset_bytes = 0;
            uint64_t hash_bytes = 0;
            uint64_t total_bytes = 0;
            bool sized = wl_columnar_memory_size_mul(W,
                    sizeof(col_join_keyed_ctx_t), &ctx_bytes)
                && wl_columnar_memory_size_mul(offset_count,
                    sizeof(uint64_t), &offset_bytes)
                && wl_columnar_memory_size_mul(
                left->nrows > 0 ? left->nrows : 1,
                sizeof(uint32_t), &hash_bytes)
                && wl_columnar_memory_size_add(ctx_bytes, offset_bytes,
                    &total_bytes)
                && wl_columnar_memory_size_add(total_bytes, hash_bytes,
                    &total_bytes)
                && ctx_bytes <= SIZE_MAX && offset_bytes <= SIZE_MAX
                && hash_bytes <= SIZE_MAX;
            if (!sized) {
                WL_DIFF_RELEASE_TMP();
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                wl_columnar_arrangement_diff_txn_abort(&diff_txn);
                (void)col_arrangement_probe_bundle_release(&diff_bundle);
                return wl_columnar_join_dispose_left(stack, &left_e,
                           EOVERFLOW);
            }
            if (scratch_governor) {
                wl_columnar_memory_admission_status_t status;
                status = wl_columnar_memory_reserve_checked(
                    wl_columnar_memory_governor_ref_get(
                        scratch_governor),
                    total_bytes, &parallel_reservation);
                if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                    && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
                    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                        sess->memory_budget_denied = true;
                    WL_DIFF_RELEASE_TMP();
                    col_rel_destroy(out);
                    wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                    wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                    if (right_filtered)
                        col_rel_destroy(right_filtered);
                    wl_columnar_arrangement_diff_txn_abort(&diff_txn);
                    (void)col_arrangement_probe_bundle_release(&diff_bundle);
                    return wl_columnar_join_dispose_left(stack, &left_e,
                               status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                                   ? ENOSPC
                                   : status
                               == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                                   ? EOVERFLOW
                                   : ENOMEM);
                }
                parallel_admitted = true;
            }
#ifdef WL_TEST_JOIN_CACHE_HOOK
            wl_columnar_join_test_last_keyed_parallel_admitted
                = parallel_admitted;
#endif
            col_join_keyed_ctx_t *ctxs = (col_join_keyed_ctx_t *)calloc(1,
                    (size_t)ctx_bytes);
            uint64_t *offsets = (uint64_t *)calloc(1,
                    (size_t)offset_bytes);
            uint32_t *left_hashes = (uint32_t *)malloc((size_t)hash_bytes);
            if (!ctxs || !offsets) {
                free(ctxs);
                free(offsets);
                free(left_hashes);
                if (parallel_admitted)
                    (void)wl_columnar_memory_rollback(
                        &parallel_reservation);
                WL_DIFF_RELEASE_TMP();
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                wl_columnar_arrangement_diff_txn_abort(&diff_txn);
                (void)col_arrangement_probe_bundle_release(&diff_bundle);
                return wl_columnar_join_dispose_left(stack, &left_e, ENOMEM);
            }
            uint32_t chunk = (uint32_t)(((uint64_t)left->nrows
                + (uint64_t)W - 1u) / W);
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
#ifdef WL_TEST_JOIN_CACHE_HOOK
            if (wl_columnar_join_test_pair_cap_limit_override > 0)
                pair_cap_limit = wl_columnar_join_test_pair_cap_limit_override;
#endif
            atomic_bool stop = ATOMIC_VAR_INIT(false);
            atomic_uint_fast64_t shared_count = ATOMIC_VAR_INIT(0);
            int prc = 0;
            for (uint32_t w = 0; w < W; w++) {
                uint64_t begin_wide = (uint64_t)w * chunk;
                uint64_t end_wide = begin_wide + chunk;
                uint32_t begin = begin_wide > left->nrows
                    ? left->nrows : (uint32_t)begin_wide;
                uint32_t end = end_wide > left->nrows
                    ? left->nrows : (uint32_t)end_wide;
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
                ctxs[w].memory_governor = scratch_governor;
                wl_columnar_memory_reservation_init(
                    &ctxs[w].pair_reservation);
                ctxs[w].pair_reserved_bytes = 0;
                ctxs[w].pair_error = 0;
                ctxs[w].stop = &stop;
                ctxs[w].shared_count = &shared_count;
                if (wl_columnar_join_submit(sess->wq,
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
                if (!wl_columnar_memory_size_add(total, ctxs[w].count,
                    &total)) {
                    prc = EOVERFLOW;
                    total = 0;
                    break;
                }
            }
            if (prc == ENOSPC)
                sess->memory_budget_denied = true;
            offsets[W] = total;
            if (prc == 0 && sess->join_output_limit > 0
                && total >= sess->join_output_limit)
                prc = EOVERFLOW;
            if (prc == 0 && total > UINT32_MAX)
                prc = EOVERFLOW;
            if (prc == 0) {
                if (col_join_reserve_exact(out, (uint32_t)total) != 0) {
                    prc = ENOMEM;
                } else {
                    out->nrows = (uint32_t)total;
                    if (total > 0)
                        wl_columnar_relation_touch_view(out);
                    for (uint32_t w = 0; w < W; w++) {
                        ctxs[w].out = out;
                        ctxs[w].out_begin = offsets[w];
                        ctxs[w].rc = 0;
                        if (wl_columnar_join_submit(sess->wq,
                            col_join_keyed_fill_worker_fn, &ctxs[w]) != 0)
                            prc = ENOMEM;
                    }
                    wl_workqueue_wait_all(sess->wq);
                    for (uint32_t w = 0; w < W; w++)
                        if (ctxs[w].rc != 0 && prc == 0)
                            prc = ctxs[w].rc;
                }
            }
            for (uint32_t w = 0; w < W; w++) {
                free(ctxs[w].pairs);
                if (ctxs[w].pair_reserved_bytes > 0
                    && !wl_columnar_memory_release(
                        &ctxs[w].pair_reservation))
                    abort();
            }
            free(offsets);
            free(ctxs);
            free(left_hashes);
            if (parallel_admitted)
                (void)wl_columnar_memory_rollback(&parallel_reservation);
            if (prc != 0) {
                WL_DIFF_RELEASE_TMP();
                prc = wl_columnar_join_output_error(sess, out, prc);
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                wl_columnar_arrangement_diff_txn_abort(&diff_txn);
                (void)col_arrangement_probe_bundle_release(&diff_bundle);
                return wl_columnar_join_dispose_left(stack, &left_e, prc);
            }
            goto join_success;
        }

        /* Probe left against the persistent diff arrangement hash table */
        col_rel_append_batch_t pair_batch = { 0 };
        if (join_rc == 0)
            join_rc = col_rel_append_batch_begin(&pair_batch, out);
        for (uint32_t lr = 0; lr < left->nrows && join_rc == 0; lr++) {
            uint32_t h = col_join_hash_rel_keys(left, lr, lk, kc) & (nbk - 1);
            for (uint32_t e = darr->ht_head[h]; e != 0;
                e = darr->ht_next[e - 1]) {
                uint32_t rr = e - 1;
                if (!col_join_keys_match_rel(left, lr, lk, right, rr, rk, kc))
                    continue;
                join_rc = col_join_append_pair(&pair_batch, left, lr, right,
                        rr, op->project_indices, op->project_count, tmp);
                if (join_rc != 0)
                    break;
                if (col_join_output_limit_reached(sess, out)) {
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
        join_rc = col_join_pair_batch_end(&pair_batch, join_rc);

        if (join_rc != 0) {
            WL_DIFF_RELEASE_TMP();
            join_rc = wl_columnar_join_output_error(sess, out, join_rc);
            col_rel_destroy(out);
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            wl_columnar_arrangement_diff_txn_abort(&diff_txn);
            (void)col_arrangement_probe_bundle_release(&diff_bundle);
            return wl_columnar_join_dispose_left(stack, &left_e, join_rc);
        }
    } else {
        /* Every fallback owns no transaction state, including capacity and
         * preparation failures that arrived here after an explicit abort. */
        wl_columnar_arrangement_diff_txn_abort(&diff_txn);
        /* Ephemeral hash table fallback (same as col_op_join) */
        uint32_t nbuckets_ep;
        if (col_join_bucket_count(right->nrows, &nbuckets_ep) != 0) {
            WL_DIFF_RELEASE_TMP();
            col_rel_destroy(out);
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_dispose_left(stack, &left_e, ENOMEM);
        }
        wl_columnar_memory_reservation_t hash_reservation;
        bool hash_admitted = false;
        wl_columnar_memory_reservation_init(&hash_reservation);
        if (scratch_governor) {
            uint64_t head_bytes = 0;
            uint64_t next_bytes = 0;
            uint64_t hash_bytes = 0;
            wl_columnar_memory_admission_status_t status;
            bool sized = wl_columnar_memory_size_mul(nbuckets_ep,
                    sizeof(uint32_t), &head_bytes)
                && wl_columnar_memory_size_mul(
                right->nrows > 0 ? right->nrows : 1,
                sizeof(uint32_t), &next_bytes)
                && head_bytes <= SIZE_MAX && next_bytes <= SIZE_MAX
                && wl_columnar_memory_size_add(head_bytes, next_bytes,
                    &hash_bytes);
            if (!sized) {
                WL_DIFF_RELEASE_TMP();
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                return wl_columnar_join_dispose_left(stack, &left_e,
                           EOVERFLOW);
            }
            status = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(scratch_governor),
                hash_bytes, &hash_reservation);
            if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
                if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                    sess->memory_budget_denied = true;
                WL_DIFF_RELEASE_TMP();
                col_rel_destroy(out);
                wl_columnar_join_key_scratch_free(&key_scratch, &lk);
                wl_columnar_join_key_scratch_free(&key_scratch, &rk);
                if (right_filtered)
                    col_rel_destroy(right_filtered);
                return wl_columnar_join_dispose_left(stack, &left_e,
                           status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                               ? ENOSPC
                               : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                               ? EOVERFLOW : ENOMEM);
            }
            hash_admitted = true;
        }
        uint32_t *ht_head_ep = (uint32_t *)calloc(nbuckets_ep,
                sizeof(uint32_t));
        uint32_t *ht_next_ep = (uint32_t *)malloc(
            (right->nrows > 0 ? right->nrows : 1) * sizeof(uint32_t));
        if (!ht_head_ep || !ht_next_ep) {
            free(ht_head_ep);
            free(ht_next_ep);
            if (hash_admitted)
                (void)wl_columnar_memory_rollback(&hash_reservation);
            WL_DIFF_RELEASE_TMP();
            col_rel_destroy(out);
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_dispose_left(stack, &left_e, ENOMEM);
        }
        for (uint32_t rr = 0; rr < right->nrows; rr++) {
            uint32_t h = col_join_hash_rel_keys(right, rr, rk, kc)
                & (nbuckets_ep - 1);
            ht_next_ep[rr] = ht_head_ep[h];
            ht_head_ep[h] = rr + 1;
        }
        col_rel_append_batch_t pair_batch = { 0 };
        if (join_rc == 0)
            join_rc = col_rel_append_batch_begin(&pair_batch, out);
        for (uint32_t lr = 0; lr < left->nrows && join_rc == 0; lr++) {
            uint32_t h = col_join_hash_rel_keys(left, lr, lk, kc)
                & (nbuckets_ep - 1);
            for (uint32_t e = ht_head_ep[h]; e != 0;
                e = ht_next_ep[e - 1]) {
                uint32_t rr = e - 1;
                if (!col_join_keys_match_rel(left, lr, lk, right, rr, rk, kc))
                    continue;
                join_rc = col_join_append_pair(&pair_batch, left, lr, right,
                        rr, op->project_indices, op->project_count, tmp);
                if (join_rc != 0)
                    break;
                if (col_join_output_limit_reached(sess, out)) {
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
        join_rc = col_join_pair_batch_end(&pair_batch, join_rc);
        free(ht_head_ep);
        free(ht_next_ep);
        if (hash_admitted)
            (void)wl_columnar_memory_rollback(&hash_reservation);
        if (join_rc != 0) {
            WL_DIFF_RELEASE_TMP();
            join_rc = wl_columnar_join_output_error(sess, out, join_rc);
            col_rel_destroy(out);
            wl_columnar_join_key_scratch_free(&key_scratch, &lk);
            wl_columnar_join_key_scratch_free(&key_scratch, &rk);
            if (right_filtered)
                col_rel_destroy(right_filtered);
            return wl_columnar_join_dispose_left(stack, &left_e, join_rc);
        }
    }

join_success:
    WL_DIFF_RELEASE_TMP();
#undef WL_DIFF_RELEASE_TMP
    wl_columnar_join_key_scratch_free(&key_scratch, &lk);
    wl_columnar_join_key_scratch_free(&key_scratch, &rk);
    bool result_is_delta = projected_join ? false
        : (left_e.is_delta || used_right_delta);

    /* Materialization cache: insert BEFORE destroying left, because
     * col_mat_cache_key_content dereferences left to compute content hash. */
    /* Owned filtered fallbacks are operation-local; see the ordinary path. */
    col_rel_t *publish = out;
    bool cached = false;
    int primary_rc = 0;
    if (op->materialized && !projected_join && !right_filtered) {
        col_rel_t *copy = NULL;
        wl_columnar_memory_governor_ref_t *effective
            = wl_columnar_join_effective_governor(sess, out);
        int copy_rc = wl_columnar_relation_deep_copy_governed(out, &copy,
                effective);
        if (copy_rc == 0) {
            int cache_rc = col_mat_cache_insert(&sess->mat_cache, left, right,
                    out);
            if (cache_rc == 0) {
                publish = copy;
                cached = true;
            } else if ((cache_rc == ENOMEM || cache_rc == ENOSPC)
                && wl_columnar_join_original_is_accounted(out, effective)) {
                col_rel_destroy(copy);
                copy = NULL;
            } else {
                col_rel_destroy(copy);
                copy = NULL;
                col_rel_destroy(out);
                out = NULL;
                publish = NULL;
                primary_rc = wl_columnar_join_memory_error(sess, cache_rc);
            }
        } else if ((copy_rc == ENOMEM || copy_rc == ENOSPC)
            && wl_columnar_join_original_is_accounted(out, effective)) {
            /* The existing committed token covers the one published owner. */
        } else {
            col_rel_destroy(out);
            out = NULL;
            publish = NULL;
            primary_rc = wl_columnar_join_memory_error(sess, copy_rc);
        }
    }
    /* End internal source readers before testing externally held input
     * ownership. The differential transaction remains abortable until push. */
    int release_rc = diff_bundle.active
        ? col_arrangement_probe_bundle_release(&diff_bundle) : 0;
    if (right_filtered)
        col_rel_destroy(right_filtered);
    if (release_rc == 0)
        release_rc = primary_rc;
    int cleanup_rc = wl_columnar_join_dispose_left(stack, &left_e, release_rc);
    if (cleanup_rc != 0) {
        wl_columnar_arrangement_diff_txn_abort(&diff_txn);
        if (cached)
            col_mat_cache_remove_result(&sess->mat_cache, out);
        if (publish)
            col_rel_destroy(publish);
        return cleanup_rc;
    }
    if (primary_rc != 0) {
        wl_columnar_arrangement_diff_txn_abort(&diff_txn);
        return primary_rc;
    }
    int push_rc = eval_stack_push_delta(stack, publish, true, result_is_delta);
    if (push_rc == 0)
        push_rc = diff_txn_commit_after_push(stack, &diff_txn,
                cached ? &sess->mat_cache : NULL, cached ? out : NULL);
    else {
        wl_columnar_arrangement_diff_txn_abort(&diff_txn);
        if (cached)
            col_mat_cache_remove_result(&sess->mat_cache, out);
        col_rel_destroy(publish);
    }
    return push_rc;

}

/* Controlled ordinary JOIN. Source admission inventory: ENTRY current142
* plus pin retirement20 <=192; canonical dispatch start12/step14 <=32.
* First ENTRY refusal160, initialized/next-refusal191 <= quantum256.
* Complete child-return caller73/constructor75 retain COLUMN256, typed
* constructor253 and B2 validation252. These are scalar-slot bounds, not
* allocation latency; private output and immutable registry/source contracts
* are required. Each cleanup/ACK/retry needs a fresh distinct HOST384. */
enum {
    WL_COLUMNAR_JOIN_ATTEMPT_WORK = 192,
    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK = 128,
    /* Two payments per first cleanup turn:64 current +128 future per
     * payment, as in existing B2 retirement. Named capacity: common10,
     * batch18, fixed scratch12, right source1, JOIN decision1. Common10 is
     * PROGRAM,BATCH,PHYSICAL,CREDIT,REFERENCE,SOURCE,CONTROL,FINISH,RESET,ACK;
     * JOIN bypasses common BATCH and ordinary DECIDE; BATCH shares are unused.
     * Child prepaid
     * interiors are distinct; unused named capacity is never refunded. */
    WL_COLUMNAR_JOIN_ATTEMPT_RETIRE_PAYMENTS = 84
};

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_join_test_attempt_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint64_t);
int wl_columnar_join_test_attempt_fail_allocation = -1;
#endif

static int
wl_columnar_join_attempt_guard(wl_evaluation_control_t *control, uint64_t id)
{
    return !control || !id
           || wl_evaluation_control_active_attempt_id(control) != id
        ? EINVAL : 0;
}

static int
wl_columnar_join_attempt_charge(wl_evaluation_control_t *control,
    uint64_t id, uint64_t work)
{
    int rc = wl_columnar_join_attempt_guard(control, id);
    return rc ? rc : wl_evaluation_control_charge(control, work);
}

static int
wl_columnar_join_attempt_tail(wl_evaluation_control_t *control,
    uint64_t id, unsigned phase, uint64_t progress)
{
    int rc = wl_columnar_join_attempt_guard(control, id);
    if (rc) return rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_join_test_attempt_checkpoint)
        wl_columnar_join_test_attempt_checkpoint(control, phase, progress);
#else
    (void)phase; (void)progress;
#endif
    return wl_columnar_join_attempt_charge(control, id, 0);
}

static int
wl_columnar_join_attempt_return(wl_evaluation_control_t *control,
    uint64_t id, int child_result)
{
    int tail = wl_columnar_join_attempt_charge(control, id, 0);
    return tail > 0 ? tail : child_result ? child_result : tail;
}

static void *
wl_columnar_join_attempt_allocate(size_t bytes)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_join_test_attempt_fail_allocation >= 0) {
        if (!wl_columnar_join_test_attempt_fail_allocation) return NULL;
        --wl_columnar_join_test_attempt_fail_allocation;
    }
#endif
    return malloc(bytes);
}

/* Raw lookup's name equality without a lazy hash rebuild or hidden scan.
* One registry slot and one byte each have their own actual admission. */
static int
wl_columnar_join_attempt_lookup(wl_col_session_t *session, const char *name,
    bool delta, col_rel_t **out, wl_evaluation_control_t *control, uint64_t id)
{
    *out = NULL;
    if (!name) return 0;
    for (uint32_t slot = 0; slot < session->nrels; ++slot) {
        int rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
        if (rc) return rc;
        col_rel_t *candidate = session->rels[slot];
        const char *actual = candidate ? candidate->name : NULL;
        rc = wl_columnar_join_attempt_tail(control, id, 5, slot);
        if (rc) return rc;
        if (!actual) continue;
        for (size_t byte = 0; ; ++byte) {
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) return rc;
            char wanted = !delta ? name[byte]
                : byte < 3 ? "$d$"[byte] : byte == 255 ? '\0' : name[byte - 3];
            char value = actual[byte];
            bool match = wanted == value;
            rc = wl_columnar_join_attempt_tail(control, id, 6, byte);
            if (rc) return rc;
            if (!match) break;
            if (!value) {
                *out = candidate; return 0;
            }
        }
    }
    return 0;
}

static int
wl_columnar_join_attempt_scratch(wl_columnar_join_scratch_t *scratch,
    uint64_t bytes, wl_columnar_memory_governor_ref_t *governor,
    wl_evaluation_control_t *control, uint64_t id)
{
    int rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) return rc;
    wl_columnar_memory_reservation_init(&scratch->credit);
    scratch->physical = NULL; scratch->governor = NULL;
    scratch->allocated = false; scratch->admitted = false;
    scratch->committed = false; scratch->ref_held = false;
    scratch->initialized = true;
    rc = wl_columnar_join_attempt_tail(control, id, 9, bytes);
    if (rc) return rc;
    if (!bytes) return 0;
    if (governor) {
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) return rc;
        scratch->governor = governor;
        wl_columnar_memory_governor_ref_retain(governor);
        scratch->ref_held = true;
        rc = wl_columnar_join_attempt_tail(control, id, 10, bytes);
        if (rc) return rc;
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
            wl_columnar_memory_governor_ref_get(governor), bytes,
            &scratch->credit, &status, control, id);
        if (rc) return rc;
        scratch->admitted = status == WL_COLUMNAR_MEMORY_ADMISSION_OK
            || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY;
        int admission = scratch->admitted ? 0
            : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
            : status ==
            WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
        rc = wl_columnar_join_attempt_return(control, id, admission);
        if (rc) return rc;
    }
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) return rc;
    scratch->physical = wl_columnar_join_attempt_allocate((size_t)bytes);
    scratch->allocated = scratch->physical != NULL;
    rc = wl_columnar_join_attempt_tail(control, id, 11, bytes);
    if (rc > 0 || scratch->allocated) {
        if (rc) return rc;
    } else return ENOMEM;
    if (scratch->admitted) {
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) return rc;
        rc = wl_columnar_join_attempt_tail(control, id, 12, bytes);
        if (rc) return rc;
        rc = wl_columnar_memory_governor_commit_attempt_controlled(
            &scratch->credit, scratch->physical, &scratch->committed, control,
            id);
        rc = wl_columnar_join_attempt_return(control, id, rc);
    }
    return rc;
}

static void
wl_columnar_join_attempt_status(const wl_columnar_filter_context_t *context,
    wl_columnar_filter_status_t *status)
{
    status->progress = WL_COLUMNAR_FILTER_PROGRESS_YIELD;
    status->execution_result = context->execution_result;
    status->cleanup_result = context->cleanup_result;
    status->semantic_result = context->semantic_result;
    status->committed = context->committed;
}

/* J3 written-source inventory: suffix classifier48+tail28+arbitration18+
 * next-refusal18=112; hash5+9+28+28+35+7=112; typed equality54 gives
 * match2+54+1+28+6+24+5=120. Each fits CELL128; nested original caller73
 * makes the largest new composed region193. Existing child COLUMN256,
 * constructor253/B2 validation252 remain limiting; no internal subtraction.
 * The three original scratch retirement capacities already exist in J2. */
typedef char wl_columnar_join_attempt_new_cells_fit[
        (112 <= WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK
        && 120 <= WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK
        && 193 <= WL_EVALUATION_CONTROL_QUANTUM) ? 1 : -1];

/* Exact-name precedence and the documented portable col<N> fallback domain.
 * Every examined registry/name/parser unit has its own original-ID admission. */
static int
wl_columnar_join_attempt_key(const wl_columnar_relation_scalar_source_t *source,
    const char *key, uint32_t *out, wl_evaluation_control_t *control,
    uint64_t id)
{
    *out = 0;
    if (!key || !source->names) return 0;
    for (uint32_t column = 0; column < source->width; ++column) {
        int rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
        if (rc) return rc;
        const char *name = source->names[column];
        rc = wl_columnar_join_attempt_tail(control, id, 21, column);
        if (rc) return rc;
        if (!name) continue;
        for (size_t byte = 0; ; ++byte) {
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) return rc;
            unsigned char wanted = (unsigned char)key[byte];
            unsigned char actual = (unsigned char)name[byte];
            bool equal = wanted == actual;
            rc = wl_columnar_join_attempt_tail(control, id, 22, byte);
            if (rc) return rc;
            if (!equal) break;
            if (!wanted) {
                if (column > INT_MAX) return ENOTSUP;
                *out = column; return 0;
            }
        }
    }
    for (unsigned byte = 0; byte < 3; ++byte) {
        int rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
        if (rc) return rc;
        bool equal = key[byte] == "col"[byte];
        rc = wl_columnar_join_attempt_tail(control, id, 23, byte);
        if (rc) return rc;
        if (!equal) return 0;
    }
    size_t byte = 3;
    bool initial = true, negative = false, digits = false, overflow = false;
    uint32_t value = 0;
    for (;;) {
        int rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
        if (rc) return rc;
        unsigned char ch = (unsigned char)key[byte];
        int typed = ch >= 128 ? ENOTSUP : 0;
        bool finished = typed != 0;
        bool whitespace = ch == ' ' || ch == '\t' || ch == '\n'
            || ch == '\r' || ch == '\f' || ch == '\v';
        if (!finished && initial && whitespace) {
            ++byte;
        } else if (!finished && initial && (ch == '+' || ch == '-')) {
            initial = false; negative = ch == '-'; ++byte;
        } else if (!finished && ch >= '0' && ch <= '9') {
            initial = false; digits = true;
            uint32_t digit = (uint32_t)(ch - '0');
            if (!overflow) {
                if (value > ((uint32_t)INT_MAX - digit) / 10u) overflow = true;
                else value = value * 10u + digit;
            }
            ++byte;
        } else if (!finished) {
            finished = true;
            if (!ch && digits && !negative && overflow) typed = ENOTSUP;
        }
        rc = wl_columnar_join_attempt_tail(control, id, 23, byte);
        if (rc > 0) return rc;
        if (typed) return typed;
        if (rc) return rc;
        if (finished) {
            if (!ch && digits && !negative && !overflow &&
                value < source->width)
                *out = value;
            return 0;
        }
    }
}

static int
wl_columnar_join_attempt_hash(const col_rel_t *relation, uint32_t row,
    const uint32_t *keys, uint32_t count, uint32_t *out,
    wl_evaluation_control_t *control, uint64_t id)
{
    uint32_t hash = 2166136261u;
    for (uint32_t key = 0; key < count; ++key) {
        int rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
        if (rc) return rc;
        uint32_t column = keys[key];
        int64_t bits = relation->columns[column][row];
        bool valid = !relation->column_types
            || relation->column_types[column] != WIRELOG_TYPE_FLOAT
            || wl_columnar_float_bits_valid(bits);
        if (valid) hash = wl_columnar_hash_value(hash, relation, column, bits);
        rc = wl_columnar_join_attempt_tail(control, id, 27, key);
        if (rc > 0) return rc;
        if (!valid) return EINVAL;
        if (rc) return rc;
    }
    *out = hash;
    return 0;
}

static int
wl_columnar_join_attempt_match(const col_rel_t *left, uint32_t left_row,
    const uint32_t *left_keys, const col_rel_t *right, uint32_t right_row,
    const uint32_t *right_keys, uint32_t count, bool *out,
    wl_evaluation_control_t *control, uint64_t id)
{
    *out = false;
    for (uint32_t key = 0; key < count; ++key) {
        int rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
        if (rc) return rc;
        uint32_t lc = left_keys[key], rc_column = right_keys[key];
        bool equal = wl_columnar_value_equal(left, lc,
                left->columns[lc][left_row], right, rc_column,
                right->columns[rc_column][right_row]);
        rc = wl_columnar_join_attempt_tail(control, id, 30, key);
        if (rc) return rc;
        if (!equal) return 0;
    }
    *out = true;
    return 0;
}

/* JOIN's common PROGRAM/PHYSICAL/CREDIT/REFERENCE retirement capacities are
 * prepaid before this point. The generic vector is disjoint from scratch[1],
 * which owns the output row. Preserve every owner before a later stop check. */
static int
wl_columnar_join_attempt_filter_storage(wl_columnar_filter_context_t *ctx,
    uint64_t slots, wl_columnar_memory_governor_ref_t *governor,
    wl_evaluation_control_t *control, uint64_t id)
{
    int rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) return rc;
    uint64_t bytes = 0;
    bool sized = wl_columnar_memory_size_mul(slots, sizeof(int64_t), &bytes)
        && bytes <= SIZE_MAX;
    rc = wl_columnar_join_attempt_tail(control, id, 40, slots);
    if (rc > 0 || sized) {
        if (rc) return rc;
    } else return EOVERFLOW;
    if (slots <= sizeof(ctx->inline_values) / sizeof(ctx->inline_values[0]))
        return 0;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) return rc;
    wl_columnar_memory_reservation_init(&ctx->credit);
    rc = wl_columnar_join_attempt_tail(control, id, 40, bytes);
    if (rc) return rc;
    if (governor) {
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) return rc;
        ctx->governor = governor;
        wl_columnar_memory_governor_ref_retain(governor);
        ctx->ref_held = true;
        rc = wl_columnar_join_attempt_tail(control, id, 41, bytes);
        if (rc) return rc;
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
            wl_columnar_memory_governor_ref_get(governor), bytes,
            &ctx->credit, &status, control, id);
        if (rc) return rc;
        ctx->admitted = status == WL_COLUMNAR_MEMORY_ADMISSION_OK
            || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY;
        int admission = ctx->admitted ? 0
            : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
            : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
            : EINVAL;
        rc = wl_columnar_join_attempt_return(control, id, admission);
        if (rc) return rc;
    }
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) return rc;
    int64_t *physical = wl_columnar_join_attempt_allocate((size_t)bytes);
    if (physical) {
        ctx->values = physical;
        ctx->heap = true;
    }
    rc = wl_columnar_join_attempt_tail(control, id, 42, bytes);
    if (rc > 0 || physical) {
        if (rc) return rc;
    } else return ENOMEM;
    if (ctx->admitted) {
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) return rc;
        rc = wl_columnar_memory_governor_commit_attempt_controlled(
            &ctx->credit, physical, &ctx->credit_committed, control, id);
        rc = wl_columnar_join_attempt_return(control, id, rc);
    }
    return rc;
}

/* Evaluate once in original right-row order, before key/schema/output work.
 * Numeric expression failure means FILTER false. Only passing rows undergo
 * raw typed-append-equivalent whole-row validation; source bits stay intact. */
static int
wl_columnar_join_attempt_right_selection(wl_columnar_filter_context_t *ctx,
    const wl_plan_expr_buffer_t *expression, uint32_t *selected,
    wl_evaluation_control_t *control, uint64_t id)
{
    int rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) return rc;
    wl_columnar_memory_governor_ref_t *governor = ctx->session->memory_governor
        ? ctx->session->memory_governor
        : ctx->join_right.descriptor->memory_governor;
    bool supported = false;
    rc = wl_columnar_join_attempt_tail(control, id, 43, expression->size);
    if (rc) return rc;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) return rc;
    rc = wl_columnar_expr_compile_attempt_controlled(expression->data,
            expression->size, governor, &ctx->program, &supported, control, id);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) return rc;
    if (!supported) return ENOTSUP;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) return rc;
    uint64_t slots = 0;
    bool sized = wl_columnar_memory_size_add(ctx->join_right.nrows,
            ctx->join_right.width, &slots);
    rc = wl_columnar_join_attempt_tail(control, id, 43, slots);
    if (rc > 0 || sized) {
        if (rc) return rc;
    } else return EOVERFLOW;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) return rc;
    rc = wl_columnar_join_attempt_filter_storage(ctx, slots, governor,
            control, id);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) return rc;
    int64_t *row = ctx->values + ctx->join_right.nrows;
    uint32_t count = 0;
    for (uint32_t r = 0; r < ctx->join_right.nrows; ++r) {
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) return rc;
        rc = wl_columnar_relation_scalar_source_gather_attempt_controlled(
            &ctx->join_right, r, row, ctx->join_right.width);
        rc = wl_columnar_join_attempt_return(control, id, rc);
        if (rc) return rc;
        int64_t value = 0;
        bool failed = false;
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) return rc;
        rc = wl_columnar_expr_eval_attempt_controlled(ctx->program, row,
                ctx->join_right.width, &value, &failed, control, id);
        rc = wl_columnar_join_attempt_return(control, id, rc);
        if (rc) return rc;
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
        if (rc) return rc;
        bool keep = !failed && value != 0;
        rc = wl_columnar_join_attempt_tail(control, id, 44, r);
        if (rc) return rc;
        if (!keep) continue;
        for (uint32_t c = 0; c < ctx->join_right.width; ++c) {
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) return rc;
            bool valid = !ctx->join_right.types
                || ctx->join_right.types[c] != WIRELOG_TYPE_FLOAT
                || wl_columnar_float_bits_valid(row[c]);
            rc = wl_columnar_join_attempt_tail(control, id, 45, c);
            if (rc > 0) return rc;
            if (!valid) return EINVAL;
            if (rc) return rc;
        }
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
        if (rc) return rc;
        ctx->values[count++] = r;
        rc = wl_columnar_join_attempt_tail(control, id, 46, r);
        if (rc) return rc;
    }
    *selected = count;
    return 0;
}

int
wl_columnar_join_op_attempt_start(const wl_plan_op_t *op,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_evaluation_control_t *control, uint64_t id,
    wl_columnar_filter_status_t *status)
{
    return wl_columnar_join_op_attempt_start_owned(op, frame, session, control,
               id, status, NULL);
}

int
wl_columnar_join_op_attempt_start_owned(const wl_plan_op_t *op,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_evaluation_control_t *control, uint64_t id,
    wl_columnar_filter_status_t *status, const void *owner)
{
    if (!op || !status) return EINVAL;
    if (owner && !control) return EINVAL;
    eval_stack_t *stack = NULL;
    int rc = wl_columnar_eval_stack_operator_stack_owned(frame, session, &stack,
            owner);
    if (rc) return rc;
    if (owner) {
        const wl_columnar_eval_plan_numeric_attempt_t *record = owner;
        if (record->control != control || record->attempt_id != id)
            return EINVAL;
    }
    if (!control) {
        int result = col_op_join(op, stack, session);
        status->progress = WL_COLUMNAR_FILTER_PROGRESS_DONE;
        status->execution_result = result; status->semantic_result = result;
        status->cleanup_result = 0; status->committed = result == 0;
        return 0;
    }
    rc = wl_columnar_join_attempt_guard(control, id);
    if (rc) return rc;
    wl_columnar_filter_context_t *ctx = NULL;
    rc = wl_columnar_eval_stack_filter_context_owned(frame, &ctx, owner);
    if (rc) return rc;
    /* Refused first admission has no live context or resource ownership. */
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) {
        status->progress = WL_COLUMNAR_FILTER_PROGRESS_DONE;
        status->execution_result = rc; status->semantic_result = rc;
        status->cleanup_result = 0; status->committed = false;
        return 0;
    }
    ctx->program = NULL; ctx->output = NULL; ctx->private_output = NULL;
    ctx->governor = NULL; ctx->values = ctx->inline_values;
    ctx->source_initialized = false; ctx->batch_initialized = false;
    ctx->drain_initialized = false; ctx->heap = false; ctx->admitted = false;
    ctx->credit_committed = false; ctx->ref_held = false;
    ctx->unary_started = false; ctx->committed = false;
    ctx->row = 0; ctx->width = 0; ctx->nrows = 0;
    ctx->execution_result = 0; ctx->cleanup_result = 0;
    ctx->semantic_result = 0;
    ctx->control = NULL; ctx->pin_owned = false; ctx->thread_valid = false;
    ctx->busy = false; ctx->frame = frame; ctx->session = session;
    ctx->attempt_id = id; ctx->join_right_initialized = false;
    ctx->join_retirement_cursor = 0; ctx->join_result_is_delta = false;
    ctx->join_decision_pending = false;
    for (unsigned i = 0; i < WL_COLUMNAR_JOIN_SCRATCH_COUNT; ++i) {
        wl_columnar_join_scratch_t *scratch = &ctx->join_scratch[i];
        scratch->initialized = false; scratch->allocated = false;
        scratch->admitted = false; scratch->committed = false;
        scratch->ref_held = false; scratch->physical = NULL;
        scratch->governor = NULL;
    }
    rc = wl_evaluation_control_retain(control);
    if (rc) return rc;
    ctx->control = control; ctx->pin_owned = true; ctx->identity = ctx;
#if defined(WL_HAVE_C11_THREADS)
    ctx->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    ctx->owner_thread = GetCurrentThreadId();
#else
    ctx->owner_thread = pthread_self();
#endif
    ctx->thread_valid = true; ctx->busy = true;
    ctx->join_initialized = true; ctx->join_decision_pending = true;
    ctx->stage = WL_COLUMNAR_FILTER_STAGE_RUNNING;
    bool supported = op->op == WL_PLAN_OP_JOIN
        && !op->materialized
        && (!op->project_count || op->project_indices)
        && !session->current_iteration && !session->diff_operators_active
        && !session->retraction_seeded
        && !session->retraction_right_pass && !session->tdd_input_run
        && !session->tdd_subpass_active && !session->tdd_outbound_only_active
        && !session->join_batch_bytes && !session->join_output_shared_count
        && !session->coordinator && session->num_workers <= 1;
    rc = wl_columnar_join_attempt_tail(control, id, 1, 0);
    if (rc > 0 || supported) {
        if (rc) goto done;
    } else {
        rc = ENOTSUP; goto done;
    }
    for (unsigned payment = 0;
        payment < WL_COLUMNAR_JOIN_ATTEMPT_RETIRE_PAYMENTS; ++payment){
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        rc = wl_columnar_join_attempt_tail(control, id, 2, payment);
        if (rc) goto done;
    }
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_tail(control, id, 3, 0);
    if (rc) goto done;
    rc = wl_columnar_eval_stack_unary_begin_attempt_context_owned(frame, ctx,
            control, id, owner);
    if (rc) goto done;
    ctx->unary_started = true;
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    const col_rel_t *left = NULL;
    col_rel_t **out_slot = NULL;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_eval_stack_unary_input_owned(frame, &left, owner);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_eval_stack_unary_output_owned(frame, &out_slot, owner);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc =
        wl_columnar_relation_scalar_source_init_attempt_controlled(&ctx->source,
            &ctx->source_initialized, control, id);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc =
        wl_columnar_relation_scalar_source_begin_attempt_controlled(
        &ctx->source, left, false);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    col_rel_t *right = NULL, *delta = NULL;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_lookup(session, op->right_relation, false,
            &right, control, id);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    bool left_delta = stack->items[stack->top - 1].is_delta;
    if (right && op->delta_mode != WL_DELTA_FORCE_FULL
        && (op->delta_mode == WL_DELTA_FORCE_DELTA || !left_delta)) {
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        rc = wl_columnar_join_attempt_lookup(session, op->right_relation, true,
                &delta, control, id);
        rc = wl_columnar_join_attempt_return(control, id, rc);
        if (rc) goto done;
    }
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    bool forced_empty = false, used_delta = false;
    if (right && op->delta_mode == WL_DELTA_FORCE_DELTA) {
        if (delta && delta->nrows) {
            right = delta; used_delta = true;
        }else forced_empty = session->delta_seeded;
    } else if (right && op->delta_mode != WL_DELTA_FORCE_FULL
        && !left_delta && delta && delta->nrows &&
        delta->nrows < right->nrows){
        right = delta; used_delta = true;
    }
    bool projected = op->project_count && op->project_indices;
    ctx->join_result_is_delta = right && !forced_empty && !projected
        && (left_delta || used_delta);
    rc = wl_columnar_join_attempt_tail(control, id, 7, used_delta);
    if (rc) goto done;
    if (right) {
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        rc =
            wl_columnar_relation_scalar_source_init_attempt_controlled(
            &ctx->join_right,
            &ctx->join_right_initialized, control, id);
        rc = wl_columnar_join_attempt_return(control, id, rc);
        if (rc) goto done;
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        rc =
            wl_columnar_relation_scalar_source_begin_attempt_controlled(
            &ctx->join_right, right, false);
        rc = wl_columnar_join_attempt_return(control, id, rc);
        if (rc) goto done;
    }
    uint32_t selected_rows = ctx->join_right.nrows;
    bool filtered_right = right && !forced_empty && op->right_filter_expr.size;
    if (filtered_right) {
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        rc = wl_columnar_join_attempt_right_selection(ctx,
                &op->right_filter_expr, &selected_rows, control, id);
        rc = wl_columnar_join_attempt_return(control, id, rc);
        if (rc) goto done;
    }
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    wl_columnar_memory_governor_ref_t *governor = session->memory_governor
        ? session->memory_governor : left->memory_governor
        ? left->memory_governor : right ? right->memory_governor : NULL;
    uint32_t *left_keys = NULL, *right_keys = NULL;
    bool keyed = op->key_count && right && !forced_empty;
    rc = wl_columnar_join_attempt_tail(control, id, 24, op->key_count);
    if (rc) goto done;
    if (keyed) {
        if (!ctx->source.width || !ctx->join_right.width) {
            rc = EINVAL; goto done;
        }
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        uint64_t key_bytes = 0;
        bool key_sized = wl_columnar_memory_size_mul(op->key_count,
                2u * sizeof(uint32_t), &key_bytes) && key_bytes <= SIZE_MAX;
        rc = wl_columnar_join_attempt_tail(control, id, 24, key_bytes);
        if (rc > 0 || key_sized) {
            if (rc) goto done;
        } else {
            rc = EOVERFLOW; goto done;
        }
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        rc = wl_columnar_join_attempt_scratch(&ctx->join_scratch[0], key_bytes,
                governor, control, id);
        rc = wl_columnar_join_attempt_return(control, id, rc);
        if (rc) goto done;
        left_keys = ctx->join_scratch[0].physical;
        right_keys = left_keys + op->key_count;
        for (uint32_t key = 0; key < op->key_count; ++key) {
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_WORK);
            if (rc) goto done;
            rc = wl_columnar_join_attempt_key(&ctx->source,
                    op->left_keys ? op->left_keys[key] : NULL,
                    &left_keys[key], control, id);
            rc = wl_columnar_join_attempt_return(control, id, rc);
            if (rc) goto done;
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_WORK);
            if (rc) goto done;
            rc = wl_columnar_join_attempt_key(&ctx->join_right,
                    op->right_keys ? op->right_keys[key] : NULL,
                    &right_keys[key], control, id);
            rc = wl_columnar_join_attempt_return(control, id, rc);
            if (rc) goto done;
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            wirelog_column_type_t lt = ctx->source.types
                ? ctx->source.types[left_keys[key]] : WIRELOG_TYPE_INT64;
            wirelog_column_type_t rt = ctx->join_right.types
                ? ctx->join_right.types[right_keys[key]] : WIRELOG_TYPE_INT64;
            bool compatible = (lt == WIRELOG_TYPE_FLOAT) ==
                (rt == WIRELOG_TYPE_FLOAT);
            rc = wl_columnar_join_attempt_tail(control, id, 25, key);
            if (rc > 0 || compatible) {
                if (rc) goto done;
            } else {
                rc = EINVAL; goto done;
            }
        }
    }
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    uint64_t width64 =
        projected ? op->project_count : (uint64_t)ctx->source.width
        + (right ? ctx->join_right.width : 0);
    uint64_t type_bytes = 0, padded = 0, row_bytes = 0, bytes = 0;
    bool sized = width64 <= UINT32_MAX
        && wl_columnar_memory_size_mul(width64, sizeof(wirelog_column_type_t),
            &type_bytes)
        && wl_columnar_memory_size_add(type_bytes, sizeof(int64_t) - 1u,
            &padded)
        && wl_columnar_memory_size_mul(width64, sizeof(int64_t), &row_bytes)
        && wl_columnar_memory_size_add(padded &
            ~(uint64_t)(sizeof(int64_t) - 1u), row_bytes, &bytes)
        && bytes <= SIZE_MAX;
    rc = wl_columnar_join_attempt_tail(control, id, 8, width64);
    if (rc > 0 || sized) {
        if (rc) goto done;
    } else {
        rc = EOVERFLOW; goto done;
    }
    ctx->width = (uint32_t)width64; ctx->nrows = ctx->source.nrows;

    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_scratch(&ctx->join_scratch[1], bytes,
            governor, control, id);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    wirelog_column_type_t *types = ctx->join_scratch[1].physical;
    int64_t *values = bytes ? (int64_t *)((unsigned char *)types
        + (size_t)(padded &
        ~(uint64_t)(sizeof(int64_t) - 1u))) : ctx->inline_values;
    for (uint32_t c = 0; c < ctx->width; ++c) {
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
        if (rc) goto done;
        uint32_t source = projected ? op->project_indices[c] : c;
        types[c] = source < ctx->source.width
            ? ctx->source.types ? ctx->source.types[source] : WIRELOG_TYPE_INT64
            : right && source - ctx->source.width < ctx->join_right.width
            ? ctx->join_right.types ? ctx->join_right.types[source -
                ctx->source.width] : WIRELOG_TYPE_INT64
            : WIRELOG_TYPE_INT64;
        rc = wl_columnar_join_attempt_tail(control, id, 13, c);
        if (rc) goto done;
    }
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_relation_new_typed_operator_attempt_controlled(
        !right || forced_empty ? "$join_empty" : "$join", ctx->width, types,
        governor, out_slot, control, id);
    ctx->output = *out_slot; ctx->private_output = *out_slot;
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    if (!right || forced_empty || !ctx->source.nrows ||
        !selected_rows) goto done;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    wl_columnar_filter_attempt_batch_init(&ctx->batch);
    ctx->batch_initialized = true;
    wl_columnar_relation_private_batch_drain_init(&ctx->batch_drain);
    ctx->drain_initialized = true;
    rc = wl_columnar_join_attempt_tail(control, id, 15, 0);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_relation_append_batch_begin_controlled(&ctx->batch,
            ctx->output, control);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    uint32_t buckets = 1;
    uint32_t *heads = NULL, *next = NULL;
    if (keyed) {
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        if (selected_rows > UINT32_MAX / 2u) {
            rc = ENOMEM; goto done;
        }
        uint32_t target = selected_rows * 2u;
        rc = wl_columnar_join_attempt_tail(control, id, 26, target);
        if (rc) goto done;
        while (buckets < target) {
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            bool fits = buckets <= UINT32_MAX / 2u;
            if (fits) buckets *= 2u;
            rc = wl_columnar_join_attempt_tail(control, id, 26, buckets);
            if (rc > 0 || fits) {
                if (rc) goto done;
            } else {
                rc = ENOMEM; goto done;
            }
        }
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        uint64_t slots = 0, hash_bytes = 0;
        bool hash_sized = wl_columnar_memory_size_add(buckets,
                ctx->join_right.nrows, &slots)
            && wl_columnar_memory_size_mul(slots, sizeof(uint32_t), &hash_bytes)
            && hash_bytes <= SIZE_MAX;
        rc = wl_columnar_join_attempt_tail(control, id, 26, hash_bytes);
        if (rc > 0 || hash_sized) {
            if (rc) goto done;
        } else {
            rc = EOVERFLOW; goto done;
        }
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        rc = wl_columnar_join_attempt_scratch(&ctx->join_scratch[2], hash_bytes,
                governor, control, id);
        rc = wl_columnar_join_attempt_return(control, id, rc);
        if (rc) goto done;
        heads = ctx->join_scratch[2].physical; next = heads + buckets;
        for (uint64_t slot = 0; slot < slots; ++slot) {
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            heads[slot] = UINT32_MAX;
            rc = wl_columnar_join_attempt_tail(control, id, 26, slot);
            if (rc) goto done;
        }
        for (uint32_t selected = 0; selected < selected_rows; ++selected) {
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_WORK);
            if (rc) goto done;
            uint32_t row = filtered_right
                ? (uint32_t)ctx->values[selected] : selected;
            uint32_t hash = 0;
            rc = wl_columnar_join_attempt_hash(right, row, right_keys,
                    op->key_count, &hash, control, id);
            rc = wl_columnar_join_attempt_return(control, id, rc);
            if (rc) goto done;
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            uint32_t bucket = hash & (buckets - 1u);
            next[row] = heads[bucket]; heads[bucket] = row;
            rc = wl_columnar_join_attempt_tail(control, id, 28, row);
            if (rc) goto done;
        }
    }
    for (uint32_t lr = 0; lr < ctx->source.nrows; ++lr) {
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        uint32_t first = 0;
        if (keyed) {
            uint32_t hash = 0;
            rc = wl_columnar_join_attempt_hash(left, lr, left_keys,
                    op->key_count, &hash, control, id);
            rc = wl_columnar_join_attempt_return(control, id, rc);
            if (rc) goto done;
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            first = heads[hash & (buckets - 1u)];
            rc = wl_columnar_join_attempt_tail(control, id, 29, lr);
            if (rc) goto done;
        }
        for (uint32_t selected = 0, rr = first;
            keyed ? rr < ctx->join_right.nrows : selected < selected_rows;
            rr = keyed ? next[rr] : rr + 1u, ++selected) {
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            if (!keyed) rr = filtered_right
                ? (uint32_t)ctx->values[selected] : selected;
            ctx->row = lr;
            rc = wl_columnar_join_attempt_tail(control, id, 17,
                    (uint64_t)lr * ctx->join_right.nrows + rr);
            if (rc) goto done;
            if (keyed) {
                rc = wl_columnar_join_attempt_charge(control, id,
                        WL_COLUMNAR_JOIN_ATTEMPT_WORK);
                if (rc) goto done;
                bool match = false;
                rc = wl_columnar_join_attempt_match(left, lr, left_keys,
                        right, rr, right_keys, op->key_count, &match, control,
                        id);
                rc = wl_columnar_join_attempt_return(control, id, rc);
                if (rc) goto done;
                if (!match) continue;
            }
            for (uint32_t c = 0; c < ctx->width; ++c) {
                rc = wl_columnar_join_attempt_charge(control, id,
                        WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
                if (rc) goto done;
                uint32_t source = projected ? op->project_indices[c] : c;
                values[c] = source < ctx->source.width
                    ? ctx->source.columns[source][lr]
                    : source - ctx->source.width < ctx->join_right.width
                    ? ctx->join_right.columns[source -
                        ctx->source.width][rr] : 0;
                rc = wl_columnar_join_attempt_tail(control, id, 18, c);
                if (rc) goto done;
            }
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_WORK);
            if (rc) goto done;
            rc =
                wl_columnar_relation_append_batch_numeric_row_operator_controlled
                (
                &ctx->batch, values, control, id);
            rc = wl_columnar_join_attempt_return(control, id, rc);
            if (rc) goto done;
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            if (ctx->output->nrows > session->join_output_peak)
                session->join_output_peak = ctx->output->nrows;
            bool limited = session->join_output_limit
                && ctx->output->nrows >= session->join_output_limit;
            rc = wl_columnar_join_attempt_tail(control, id, 20,
                    ctx->output->nrows);
            if (rc > 0 || !limited) {
                if (rc) goto done;
            } else {
                rc = EOVERFLOW; goto done;
            }
        }
    }
done:
    if (rc == ENOMEM && ctx->output &&
        ctx->output->memory_budget_denial_pending)
        rc = ENOSPC;
    if (rc == ENOSPC) session->memory_budget_denied = true;
    ctx->execution_result = rc; ctx->semantic_result = rc;
    ctx->stage = WL_COLUMNAR_FILTER_STAGE_JOIN_BATCH;
    ctx->busy = false;
    wl_columnar_join_attempt_status(ctx, status);
    return 0;
}

/* Controlled existence joins deliberately materialize valid scalar results.
 * They share the original JOIN owners/retirement, not its delta selection,
 * concatenated row shape, multiplicity or output-limit policy. */
int
wl_columnar_join_probe_op_attempt_start(const wl_plan_op_t *op,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_evaluation_control_t *control, uint64_t id,
    wl_columnar_filter_status_t *status)
{
    return wl_columnar_join_probe_op_attempt_start_owned(op, frame, session,
               control, id, status, NULL);
}

int
wl_columnar_join_probe_op_attempt_start_owned(const wl_plan_op_t *op,
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_evaluation_control_t *control, uint64_t id,
    wl_columnar_filter_status_t *status, const void *owner)
{
    if (!op || !status) return EINVAL;
    if (owner && !control) return EINVAL;
    eval_stack_t *stack = NULL;
    int rc = wl_columnar_eval_stack_operator_stack_owned(frame, session, &stack,
            owner);
    if (rc) return rc;
    if (owner) {
        const wl_columnar_eval_plan_numeric_attempt_t *record = owner;
        if (record->control != control || record->attempt_id != id)
            return EINVAL;
    }
    if (!control) {
        int result = op->op == WL_PLAN_OP_SEMIJOIN
            ? col_op_semijoin(op, stack, session)
            : op->op == WL_PLAN_OP_ANTIJOIN
            ? col_op_antijoin(op, stack, session) : EINVAL;
        status->progress = WL_COLUMNAR_FILTER_PROGRESS_DONE;
        status->execution_result = result; status->semantic_result = result;
        status->cleanup_result = 0; status->committed = result == 0;
        return 0;
    }
    rc = wl_columnar_join_attempt_guard(control, id);
    if (rc) return rc;
    wl_columnar_filter_context_t *ctx = NULL;
    rc = wl_columnar_eval_stack_filter_context_owned(frame, &ctx, owner);
    if (rc) return rc;
    /* Refused first admission has no live context or resource ownership. */
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) {
        status->progress = WL_COLUMNAR_FILTER_PROGRESS_DONE;
        status->execution_result = rc; status->semantic_result = rc;
        status->cleanup_result = 0; status->committed = false;
        return 0;
    }
    ctx->program = NULL; ctx->output = NULL; ctx->private_output = NULL;
    ctx->governor = NULL; ctx->values = ctx->inline_values;
    ctx->source_initialized = false; ctx->batch_initialized = false;
    ctx->drain_initialized = false; ctx->heap = false; ctx->admitted = false;
    ctx->credit_committed = false; ctx->ref_held = false;
    ctx->unary_started = false; ctx->committed = false;
    ctx->row = 0; ctx->width = 0; ctx->nrows = 0;
    ctx->execution_result = 0; ctx->cleanup_result = 0;
    ctx->semantic_result = 0;
    ctx->control = NULL; ctx->pin_owned = false; ctx->thread_valid = false;
    ctx->busy = false; ctx->frame = frame; ctx->session = session;
    ctx->attempt_id = id; ctx->join_right_initialized = false;
    ctx->join_retirement_cursor = 0; ctx->join_result_is_delta = false;
    ctx->join_decision_pending = false;
    for (unsigned i = 0; i < WL_COLUMNAR_JOIN_SCRATCH_COUNT; ++i) {
        wl_columnar_join_scratch_t *scratch = &ctx->join_scratch[i];
        scratch->initialized = false; scratch->allocated = false;
        scratch->admitted = false; scratch->committed = false;
        scratch->ref_held = false; scratch->physical = NULL;
        scratch->governor = NULL;
    }
    rc = wl_evaluation_control_retain(control);
    if (rc) return rc;
    ctx->control = control; ctx->pin_owned = true; ctx->identity = ctx;
#if defined(WL_HAVE_C11_THREADS)
    ctx->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    ctx->owner_thread = GetCurrentThreadId();
#else
    ctx->owner_thread = pthread_self();
#endif
    ctx->thread_valid = true; ctx->busy = true;
    ctx->join_initialized = true; ctx->join_decision_pending = true;
    ctx->stage = WL_COLUMNAR_FILTER_STAGE_RUNNING;
    bool semi = op->op == WL_PLAN_OP_SEMIJOIN;
    bool supported = (semi || op->op == WL_PLAN_OP_ANTIJOIN)
        && !op->right_filter_expr.size && !op->materialized
        && (!semi || !op->project_count || op->project_indices)
        && !session->current_iteration && !session->diff_operators_active
        && !session->retraction_seeded
        && !session->retraction_right_pass && !session->tdd_input_run
        && !session->tdd_subpass_active && !session->tdd_outbound_only_active
        && !session->join_batch_bytes && !session->join_output_shared_count
        && !session->coordinator && session->num_workers <= 1;
    rc = wl_columnar_join_attempt_tail(control, id, 1, 0);
    if (rc > 0 || supported) {
        if (rc) goto done;
    } else {
        rc = ENOTSUP; goto done;
    }
    for (unsigned payment = 0;
        payment < WL_COLUMNAR_JOIN_ATTEMPT_RETIRE_PAYMENTS; ++payment){
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        rc = wl_columnar_join_attempt_tail(control, id, 2, payment);
        if (rc) goto done;
    }
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_tail(control, id, 3, 0);
    if (rc) goto done;
    rc = wl_columnar_eval_stack_unary_begin_attempt_context_owned(frame, ctx,
            control, id, owner);
    if (rc) goto done;
    ctx->unary_started = true;
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    const col_rel_t *left = NULL;
    col_rel_t **out_slot = NULL;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_eval_stack_unary_input_owned(frame, &left, owner);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_eval_stack_unary_output_owned(frame, &out_slot, owner);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc =
        wl_columnar_relation_scalar_source_init_attempt_controlled(&ctx->source,
            &ctx->source_initialized, control, id);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc =
        wl_columnar_relation_scalar_source_begin_attempt_controlled(
        &ctx->source, left, false);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    if (ctx->source.timestamps) {
        rc = ENOTSUP; goto done;
    }
    col_rel_t *right = NULL;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_lookup(session, op->right_relation, false,
            &right, control, id);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    bool present = right != NULL;
    bool projected = semi && op->project_count;
    rc = wl_columnar_join_attempt_tail(control, id, 7, present);
    if (rc > 0 || present) {
        if (rc) goto done;
    } else {
        rc = ENOTSUP; goto done;
    }
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_relation_scalar_source_init_attempt_controlled(
        &ctx->join_right, &ctx->join_right_initialized, control, id);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_relation_scalar_source_begin_attempt_controlled(
        &ctx->join_right, right, false);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    if (ctx->join_right.timestamps) {
        rc = ENOTSUP; goto done;
    }
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    wl_columnar_memory_governor_ref_t *governor = session->memory_governor
        ? session->memory_governor : left->memory_governor
        ? left->memory_governor : right ? right->memory_governor : NULL;
    uint32_t *left_keys = NULL, *right_keys = NULL;
    bool keyed = op->key_count != 0;
    rc = wl_columnar_join_attempt_tail(control, id, 24, op->key_count);
    if (rc) goto done;
    if (keyed) {
        if (!ctx->source.width || !ctx->join_right.width) {
            rc = EINVAL; goto done;
        }
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        uint64_t key_bytes = 0;
        bool key_sized = wl_columnar_memory_size_mul(op->key_count,
                2u * sizeof(uint32_t), &key_bytes) && key_bytes <= SIZE_MAX;
        rc = wl_columnar_join_attempt_tail(control, id, 24, key_bytes);
        if (rc > 0 || key_sized) {
            if (rc) goto done;
        } else {
            rc = EOVERFLOW; goto done;
        }
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        rc = wl_columnar_join_attempt_scratch(&ctx->join_scratch[0], key_bytes,
                governor, control, id);
        rc = wl_columnar_join_attempt_return(control, id, rc);
        if (rc) goto done;
        left_keys = ctx->join_scratch[0].physical;
        right_keys = left_keys + op->key_count;
        for (uint32_t key = 0; key < op->key_count; ++key) {
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_WORK);
            if (rc) goto done;
            rc = wl_columnar_join_attempt_key(&ctx->source,
                    op->left_keys ? op->left_keys[key] : NULL,
                    &left_keys[key], control, id);
            rc = wl_columnar_join_attempt_return(control, id, rc);
            if (rc) goto done;
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_WORK);
            if (rc) goto done;
            rc = wl_columnar_join_attempt_key(&ctx->join_right,
                    op->right_keys ? op->right_keys[key] : NULL,
                    &right_keys[key], control, id);
            rc = wl_columnar_join_attempt_return(control, id, rc);
            if (rc) goto done;
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            wirelog_column_type_t lt = ctx->source.types
                ? ctx->source.types[left_keys[key]] : WIRELOG_TYPE_INT64;
            wirelog_column_type_t rt = ctx->join_right.types
                ? ctx->join_right.types[right_keys[key]] : WIRELOG_TYPE_INT64;
            bool compatible = (lt == WIRELOG_TYPE_FLOAT) ==
                (rt == WIRELOG_TYPE_FLOAT);
            rc = wl_columnar_join_attempt_tail(control, id, 25, key);
            if (rc > 0 || compatible) {
                if (rc) goto done;
            } else {
                rc = EINVAL; goto done;
            }
        }
    }
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    uint64_t width64 = projected ? op->project_count : ctx->source.width;
    uint64_t type_bytes = 0, padded = 0, row_bytes = 0, bytes = 0;
    bool sized = width64 <= UINT32_MAX
        && wl_columnar_memory_size_mul(semi ? width64 : 0,
            sizeof(wirelog_column_type_t),
            &type_bytes)
        && wl_columnar_memory_size_add(type_bytes, sizeof(int64_t) - 1u,
            &padded)
        && wl_columnar_memory_size_mul(width64, sizeof(int64_t), &row_bytes)
        && wl_columnar_memory_size_add(padded &
            ~(uint64_t)(sizeof(int64_t) - 1u), row_bytes, &bytes)
        && bytes <= SIZE_MAX;
    rc = wl_columnar_join_attempt_tail(control, id, 8, width64);
    if (rc > 0 || sized) {
        if (rc) goto done;
    } else {
        rc = EOVERFLOW; goto done;
    }
    ctx->width = (uint32_t)width64; ctx->nrows = ctx->source.nrows;

    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_scratch(&ctx->join_scratch[1], bytes,
            governor, control, id);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    wirelog_column_type_t *types = ctx->join_scratch[1].physical;
    int64_t *values = bytes ? (int64_t *)((unsigned char *)types
        + (size_t)(padded &
        ~(uint64_t)(sizeof(int64_t) - 1u))) : ctx->inline_values;
    if (semi) for (uint32_t c = 0; c < ctx->width; ++c) {
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            uint32_t source = projected ? op->project_indices[c] : c;
            types[c] = source < ctx->source.width && ctx->source.types
            ? ctx->source.types[source] : WIRELOG_TYPE_INT64;
            rc = wl_columnar_join_attempt_tail(control, id, 13, c);
            if (rc) goto done;
        }
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    if (semi)
        rc = wl_columnar_relation_new_typed_operator_attempt_controlled(
            "$semijoin", ctx->width, types, governor, out_slot, control, id);
    else
        rc =
            wl_columnar_relation_scalar_source_new_like_operator_attempt_controlled
            (
            &ctx->source, "$antijoin", governor, out_slot);
    ctx->output = *out_slot; ctx->private_output = *out_slot;
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    if (!ctx->source.nrows || (semi && !ctx->join_right.nrows)) goto done;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    wl_columnar_filter_attempt_batch_init(&ctx->batch);
    ctx->batch_initialized = true;
    wl_columnar_relation_private_batch_drain_init(&ctx->batch_drain);
    ctx->drain_initialized = true;
    rc = wl_columnar_join_attempt_tail(control, id, 15, 0);
    if (rc) goto done;
    rc = wl_columnar_join_attempt_charge(control, id,
            WL_COLUMNAR_JOIN_ATTEMPT_WORK);
    if (rc) goto done;
    rc = wl_columnar_relation_append_batch_begin_controlled(&ctx->batch,
            ctx->output, control);
    rc = wl_columnar_join_attempt_return(control, id, rc);
    if (rc) goto done;
    uint32_t buckets = 1;
    uint32_t *heads = NULL, *next = NULL;
    if (keyed && ctx->join_right.nrows) {
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        if (ctx->join_right.nrows > UINT32_MAX / 2u) {
            rc = ENOMEM; goto done;
        }
        uint32_t target = ctx->join_right.nrows * 2u;
        rc = wl_columnar_join_attempt_tail(control, id, 26, target);
        if (rc) goto done;
        while (buckets < target) {
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            bool fits = buckets <= UINT32_MAX / 2u;
            if (fits) buckets *= 2u;
            rc = wl_columnar_join_attempt_tail(control, id, 26, buckets);
            if (rc > 0 || fits) {
                if (rc) goto done;
            } else {
                rc = ENOMEM; goto done;
            }
        }
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        uint64_t slots = 0, hash_bytes = 0;
        bool hash_sized = wl_columnar_memory_size_add(buckets,
                ctx->join_right.nrows, &slots)
            && wl_columnar_memory_size_mul(slots, sizeof(uint32_t), &hash_bytes)
            && hash_bytes <= SIZE_MAX;
        rc = wl_columnar_join_attempt_tail(control, id, 26, hash_bytes);
        if (rc > 0 || hash_sized) {
            if (rc) goto done;
        } else {
            rc = EOVERFLOW; goto done;
        }
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        rc = wl_columnar_join_attempt_scratch(&ctx->join_scratch[2], hash_bytes,
                governor, control, id);
        rc = wl_columnar_join_attempt_return(control, id, rc);
        if (rc) goto done;
        heads = ctx->join_scratch[2].physical; next = heads + buckets;
        for (uint64_t slot = 0; slot < slots; ++slot) {
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            heads[slot] = UINT32_MAX;
            rc = wl_columnar_join_attempt_tail(control, id, 26, slot);
            if (rc) goto done;
        }
        for (uint32_t row = 0; row < ctx->join_right.nrows; ++row) {
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_WORK);
            if (rc) goto done;
            uint32_t hash = 0;
            rc = wl_columnar_join_attempt_hash(right, row, right_keys,
                    op->key_count, &hash, control, id);
            rc = wl_columnar_join_attempt_return(control, id, rc);
            if (rc) goto done;
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            uint32_t bucket = hash & (buckets - 1u);
            next[row] = heads[bucket]; heads[bucket] = row;
            rc = wl_columnar_join_attempt_tail(control, id, 28, row);
            if (rc) goto done;
        }
    }
    for (uint32_t lr = 0; lr < ctx->source.nrows; ++lr) {
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        uint32_t first = ctx->join_right.nrows ? 0 : UINT32_MAX;
        if (keyed && ctx->join_right.nrows) {
            uint32_t hash = 0;
            rc = wl_columnar_join_attempt_hash(left, lr, left_keys,
                    op->key_count, &hash, control, id);
            rc = wl_columnar_join_attempt_return(control, id, rc);
            if (rc) goto done;
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            first = heads[hash & (buckets - 1u)];
            rc = wl_columnar_join_attempt_tail(control, id, 29, lr);
            if (rc) goto done;
        }
        bool matched = false;
        for (uint32_t rr = first; rr < ctx->join_right.nrows;
            rr = keyed ? next[rr] : rr + 1u) {
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            ctx->row = lr;
            rc = wl_columnar_join_attempt_tail(control, id, 17,
                    (uint64_t)lr * ctx->join_right.nrows + rr);
            if (rc) goto done;
            if (keyed) {
                rc = wl_columnar_join_attempt_charge(control, id,
                        WL_COLUMNAR_JOIN_ATTEMPT_WORK);
                if (rc) goto done;
                bool match = false;
                rc = wl_columnar_join_attempt_match(left, lr, left_keys,
                        right, rr, right_keys, op->key_count, &match, control,
                        id);
                rc = wl_columnar_join_attempt_return(control, id, rc);
                if (rc) goto done;
                if (!match) continue;
            }
            matched = true;
            break;
        }
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        bool selected = matched == semi;
        rc = wl_columnar_join_attempt_tail(control, id, 31, lr);
        if (rc) goto done;
        if (!selected) continue;
        for (uint32_t c = 0; c < ctx->width; ++c) {
            rc = wl_columnar_join_attempt_charge(control, id,
                    WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
            if (rc) goto done;
            uint32_t source = projected ? op->project_indices[c] : c;
            values[c] = source < ctx->source.width
                ? ctx->source.columns[source][lr] : 0;
            rc = wl_columnar_join_attempt_tail(control, id, 18, c);
            if (rc) goto done;
        }
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_WORK);
        if (rc) goto done;
        rc = wl_columnar_relation_append_batch_numeric_row_operator_controlled(
            &ctx->batch, values, control, id);
        rc = wl_columnar_join_attempt_return(control, id, rc);
        if (rc) goto done;
        rc = wl_columnar_join_attempt_charge(control, id,
                WL_COLUMNAR_JOIN_ATTEMPT_CELL_WORK);
        if (rc) goto done;
        rc = wl_columnar_join_attempt_tail(control, id, 20, ctx->output->nrows);
        if (rc) goto done;
    }

done:
    if (rc == ENOMEM && ctx->output &&
        ctx->output->memory_budget_denial_pending)
        rc = ENOSPC;
    if (rc == ENOSPC) session->memory_budget_denied = true;
    ctx->execution_result = rc; ctx->semantic_result = rc;
    ctx->stage = WL_COLUMNAR_FILTER_STAGE_JOIN_BATCH;
    ctx->busy = false;
    wl_columnar_join_attempt_status(ctx, status);
    return 0;
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
