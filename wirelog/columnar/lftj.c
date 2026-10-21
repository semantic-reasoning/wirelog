/*
 * columnar/lftj.c - Leapfrog Triejoin Implementation (Issue #194)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Leapfrog Triejoin: worst-case optimal multi-way join for columnar backend.
 * Avoids large Cartesian intermediates produced by FNV-1a binary hash-join
 * cascades on DOOP 8-9 way joins.
 *
 * Reference: T. L. Veldhuizen, "Leapfrog Triejoin: A Simple, Worst-Case
 * Optimal Join Algorithm", ICDT 2014.
 */

#include "columnar/lftj.h"
#include "columnar/internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#if defined(_MSC_VER)
#define WL_COLUMNAR_LFTJ_NOINLINE __declspec(noinline)
#elif defined(__GNUC__) || defined(__clang__)
#define WL_COLUMNAR_LFTJ_NOINLINE __attribute__((noinline))
#else
#define WL_COLUMNAR_LFTJ_NOINLINE
#endif

#ifdef WL_TEST_LFTJ_ADMISSION_HOOKS
#if defined(_MSC_VER)
#define WL_COLUMNAR_LFTJ_TEST_TLS __declspec(thread)
#else
#define WL_COLUMNAR_LFTJ_TEST_TLS _Thread_local
#endif
static WL_COLUMNAR_LFTJ_TEST_TLS wl_columnar_lftj_test_hook_state_t
lftj_test_hook_state;

void
wl_columnar_lftj_test_fail_next_iters_alloc(void)
{
    lftj_test_hook_state.fail_iters_pending = true;
    lftj_test_hook_state.fail_iters_consumed = false;
}

void
wl_columnar_lftj_test_deny_next_inner_admission(void)
{
    lftj_test_hook_state.deny_inner_pending = true;
    lftj_test_hook_state.deny_inner_consumed = false;
}

void
wl_columnar_lftj_test_get_hook_state(
    wl_columnar_lftj_test_hook_state_t *out)
{
    if (out)
        *out = lftj_test_hook_state;
}

void
wl_columnar_lftj_test_clear_hooks(void)
{
    memset(&lftj_test_hook_state, 0, sizeof(lftj_test_hook_state));
}
#endif

/* ======================================================================== */
/* LFTJ Iterator                                                            */
/* ======================================================================== */

/*
 * lftj_iter_t: trie iterator over one sorted relation.
 *
 * Holds an owned sorted copy of the input data keyed on `key_col`.
 * `pos` tracks the current row index; pos == nrows means at-end.
 */
typedef struct {
    int64_t *sorted;  /* owned: sorted copy of data, row-major       */
    uint32_t nrows;   /* total row count                              */
    uint32_t ncols;   /* columns per row                              */
    uint32_t key_col; /* column used as join key                      */
    uint32_t pos;     /* current iterator position (nrows = at-end)   */
    wirelog_column_type_t key_type;
} lftj_iter_t;

/* Current key at iterator position (undefined when at-end). */
static inline int64_t
lftj_key(const lftj_iter_t *it)
{
    return it->sorted[(size_t)it->pos * it->ncols + it->key_col];
}

static inline int
lftj_compare_key(const lftj_iter_t *it, int64_t left, int64_t right)
{
    if (it->key_type == WIRELOG_TYPE_FLOAT)
        return wl_columnar_float_compare_bits(left, right);
    return left < right ? -1 : left > right ? 1 : 0;
}

/* True when iterator is exhausted. */
static inline bool
lftj_at_end(const lftj_iter_t *it)
{
    return it->pos >= it->nrows;
}

/*
 * lftj_seek: advance iterator to first row with key >= target.
 * Uses binary search in [pos, nrows); safe to call when at-end (no-op).
 */
static void
lftj_seek(lftj_iter_t *it, int64_t target)
{
    if (lftj_at_end(it))
        return;

    uint32_t lo = it->pos;
    uint32_t hi = it->nrows;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2u;
        int64_t midkey = it->sorted[(size_t)mid * it->ncols + it->key_col];
        if (lftj_compare_key(it, midkey, target) < 0)
            lo = mid + 1u;
        else
            hi = mid;
    }
    it->pos = lo;
}

/*
 * lftj_key_range: find the half-open range [*out_lo, *out_hi) of rows
 * starting at it->pos where the key equals `key`.
 * Requires iterator positioned at a row with key == `key` (post-seek).
 * Uses binary search (upper_bound) to find the exclusive end.
 */
static void
lftj_key_range(const lftj_iter_t *it, int64_t key, uint32_t *out_lo,
    uint32_t *out_hi)
{
    *out_lo = it->pos;
    uint32_t left = it->pos;
    uint32_t right = it->nrows;
    while (left < right) {
        uint32_t mid = left + (right - left) / 2u;
        int64_t midkey = it->sorted[(size_t)mid * it->ncols + it->key_col];
        if (lftj_compare_key(it, midkey, key) <= 0)
            left = mid + 1u;
        else
            right = mid;
    }
    *out_hi = left;
}

/*
 * lftj_iter_init: allocate and sort a copy of the input relation.
 * Returns 0 on success, ENOMEM on allocation failure.
 */
static int
lftj_iter_init(lftj_iter_t *it, const wl_lftj_input_t *inp,
    wirelog_column_type_t key_type)
{
    it->nrows = inp->nrows;
    it->ncols = inp->ncols;
    it->key_col = inp->key_col;
    it->pos = 0;
    it->key_type = key_type;
    it->sorted = NULL;

    if (inp->nrows == 0)
        return 0;

    size_t bytes = (size_t)inp->nrows * inp->ncols * sizeof(int64_t);
    it->sorted = (int64_t *)malloc(bytes);
    if (!it->sorted)
        return ENOMEM;

    memcpy(it->sorted, inp->data, bytes);

    /* #465: stable LSD radix sort by the single key column replaces the
     * platform-specific qsort_r path that did not exist on Android NDK
     * bionic libc.  Returning ENOMEM on radix scratch alloc failure
     * lets the caller surface OOM cleanly. */
    if (wl_columnar_relation_radix_sort_rows_by_key_typed(it->sorted,
        inp->nrows, inp->ncols, inp->key_col, key_type) != 0) {
        free(it->sorted);
        it->sorted = NULL;
        return ENOMEM;
    }
    return 0;
}

static void
lftj_iter_free(lftj_iter_t *it)
{
    free(it->sorted);
    it->sorted = NULL;
}

/* ======================================================================== */
/* Recursive Cartesian-Product Emitter                                      */
/* ======================================================================== */

/*
 * lftj_emit_product: recursively enumerate the Cartesian product of all
 * matching rows across k iterators for a single key value, emitting one
 * output row per combination.
 *
 * Output row layout (assembled in row_buf):
 *   [0]          = key (written once at depth 0)
 *   [1 .. w0)    = non-key columns from iters[0] for the current row
 *   [w0 .. w1)   = non-key columns from iters[1] for the current row
 *   ...
 *
 * @iters:      Array of k sorted iterators (positions stable during enum).
 * @k:          Total number of iterators.
 * @key:        Shared key value for this output batch.
 * @ranges:     Pre-computed [lo, hi) pairs per iterator (2*k elements).
 * @depth:      Current recursion depth (0-based relation index).
 * @row_buf:    Output row assembly buffer (total_cols elements).
 * @col_offset: Next write position in row_buf for this depth's columns.
 * @cb, @user:  Output callback and opaque context.
 * @total_cols: Total output column count (for the cb invocation).
 */
static int
lftj_emit_product(const lftj_iter_t *iters, uint32_t k, int64_t key,
    const uint32_t *ranges, uint32_t depth, int64_t *row_buf,
    uint32_t col_offset, wl_lftj_result_fn cb, void *user,
    uint32_t total_cols)
{
    if (depth == k) {
        cb(row_buf, total_cols, user);
        return 0;
    }

    const lftj_iter_t *it = &iters[depth];
    uint32_t lo = ranges[(size_t)depth * 2u];
    uint32_t hi = ranges[(size_t)depth * 2u + 1u];

    /* Key written once from depth 0. */
    if (depth == 0)
        row_buf[0] = key;

    uint32_t write_off = col_offset;
    for (uint32_t r = lo; r < hi; r++) {
        const int64_t *rp = it->sorted + (size_t)r * it->ncols;
        uint32_t w = write_off;
        for (uint32_t c = 0; c < it->ncols; c++) {
            if (c == it->key_col)
                continue;
            row_buf[w++] = rp[c];
        }
        int rc = lftj_emit_product(iters, k, key, ranges, depth + 1u, row_buf,
                w, cb, user, total_cols);
        if (rc != 0)
            return rc;
    }
    return 0;
}

/* ======================================================================== */
/* Public API                                                               */
/* ======================================================================== */

WL_COLUMNAR_LFTJ_NOINLINE int
wl_columnar_lftj_join_typed_governed(const wl_lftj_input_t *inputs,
    wirelog_column_type_t key_type, uint32_t k, wl_lftj_result_fn cb,
    void *user, wl_columnar_memory_governor_ref_t *governor)
{
    if (!inputs || k < 2u || k > WL_LFTJ_MAX_K || !cb)
        return EINVAL;

    /* Validate each input descriptor. */
    for (uint32_t i = 0; i < k; i++) {
        if (inputs[i].ncols == 0u || inputs[i].key_col >= inputs[i].ncols)
            return EINVAL;
        if (inputs[i].nrows > 0u && !inputs[i].data)
            return EINVAL;
    }

    /* Output column count: 1 (key) + sum of non-key columns per relation.
     * Guard against uint32_t overflow from pathologically wide relations. */
    uint32_t total_cols = 1u;
    for (uint32_t i = 0; i < k; i++) {
        uint32_t add = inputs[i].ncols - 1u;
        if (total_cols > UINT32_MAX - add)
            return EINVAL;
        total_cols += add;
    }

    uint64_t scratch_bytes = 0;
    uint64_t bytes = 0;
    if (!wl_columnar_memory_size_mul(k, sizeof(lftj_iter_t), &bytes)
        || !wl_columnar_memory_size_add(scratch_bytes, bytes,
        &scratch_bytes)
        || !wl_columnar_memory_size_mul(total_cols, sizeof(int64_t), &bytes)
        || !wl_columnar_memory_size_add(scratch_bytes, bytes,
        &scratch_bytes)
        || !wl_columnar_memory_size_mul(k, 2u * sizeof(uint32_t), &bytes)
        || !wl_columnar_memory_size_add(scratch_bytes, bytes,
        &scratch_bytes))
        return EOVERFLOW;
    for (uint32_t i = 0; i < k; i++) {
        if (!wl_columnar_memory_size_mul(inputs[i].nrows, inputs[i].ncols,
            &bytes)
            || !wl_columnar_memory_size_mul(bytes, sizeof(int64_t), &bytes)
            || !wl_columnar_memory_size_mul(bytes, 2u, &bytes)
            || !wl_columnar_memory_size_add(scratch_bytes, bytes,
            &scratch_bytes))
            return EOVERFLOW;
    }
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_reservation_init(&reservation);
    if (governor && scratch_bytes > 0) {
        wl_columnar_memory_governor_t *memory_governor
            = wl_columnar_memory_governor_ref_get(governor);
#ifdef WL_TEST_LFTJ_ADMISSION_HOOKS
        if (lftj_test_hook_state.deny_inner_pending) {
            uint64_t reserved = wl_columnar_memory_reserved(memory_governor);
            uint64_t previous = atomic_load_explicit(
                &memory_governor->usable_bytes, memory_order_relaxed);
            uint64_t limited = 0;
            lftj_test_hook_state.deny_inner_pending = false;
            lftj_test_hook_state.deny_inner_consumed = true;
            lftj_test_hook_state.previous_usable_bytes = previous;
            lftj_test_hook_state.reserved_before_inner = reserved;
            if (!wl_columnar_memory_size_add(reserved,
                scratch_bytes - 1u, &limited)) {
                lftj_test_hook_state.limited_usable_bytes = previous;
            } else {
                lftj_test_hook_state.limited_usable_bytes = limited;
                atomic_store_explicit(&memory_governor->usable_bytes,
                    limited, memory_order_relaxed);
            }
        }
#endif
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(memory_governor,
                scratch_bytes, &reservation);
#ifdef WL_TEST_LFTJ_ADMISSION_HOOKS
        if (lftj_test_hook_state.deny_inner_consumed)
            lftj_test_hook_state.inner_status = status;
#endif
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
    }

    /* Allocate and sort per-relation iterators. */
    int rc = 0;
    lftj_iter_t *iters = NULL;
#ifdef WL_TEST_LFTJ_ADMISSION_HOOKS
    if (lftj_test_hook_state.fail_iters_pending) {
        lftj_test_hook_state.fail_iters_pending = false;
        lftj_test_hook_state.fail_iters_consumed = true;
    } else
#endif
    iters = (lftj_iter_t *)calloc(k, sizeof(lftj_iter_t));
    if (!iters) {
        rc = ENOMEM;
        goto cleanup;
    }

    for (uint32_t i = 0; i < k; i++) {
        if (key_type == WIRELOG_TYPE_FLOAT) {
            for (uint32_t row = 0; row < inputs[i].nrows; row++) {
                int64_t bits = inputs[i].data[(size_t)row * inputs[i].ncols
                        + inputs[i].key_col];
                if (!wl_columnar_float_bits_valid(bits)) {
                    rc = EINVAL;
                    goto cleanup;
                }
            }
        }
        rc = lftj_iter_init(&iters[i], &inputs[i], key_type);
        if (rc != 0)
            goto cleanup;
    }

    /* Early exit if any relation is empty. */
    for (uint32_t i = 0; i < k; i++) {
        if (lftj_at_end(&iters[i]))
            goto cleanup; /* rc == 0: empty result is valid */
    }

    {
        /* Scratch buffers. */
        int64_t *row_buf = (int64_t *)malloc(total_cols * sizeof(int64_t));
        uint32_t *ranges
            = (uint32_t *)malloc((size_t)2u * k * sizeof(uint32_t));
        if (!row_buf || !ranges) {
            free(row_buf);
            free(ranges);
            rc = ENOMEM;
            goto cleanup;
        }

        /*
         * Leapfrog algorithm:
         *
         * Maintain max_key = maximum current key seen across all iterators.
         * In a single while-pass we seek every iterator to max_key:
         *   - If an iterator lands beyond max_key, update max_key and restart
         *     from iterator 0 (the inner restart loop handles this).
         *   - When all k iterators agree (inner loop completes without
         *     restart), emit the Cartesian product and advance all past key.
         */

        /* Find initial max_key. */
        int64_t max_key = lftj_key(&iters[0]);
        for (uint32_t i = 1u; i < k; i++) {
            int64_t ki = lftj_key(&iters[i]);
            if (lftj_compare_key(&iters[i], ki, max_key) > 0)
                max_key = ki;
        }

        for (;;) {
            /*
             * Seek all iterators to max_key.  If any overshoots, update
             * max_key and restart from iterator 0.
             */
            uint32_t i = 0;
            while (i < k) {
                lftj_seek(&iters[i], max_key);
                if (lftj_at_end(&iters[i]))
                    goto done;
                int64_t cur = lftj_key(&iters[i]);
                if (lftj_compare_key(&iters[i], cur, max_key) > 0) {
                    max_key = cur;
                    i = 0; /* restart */
                } else {
                    i++;
                }
            }

            /* All k iterators agree on max_key. */

            /* Compute per-iterator key ranges for the product enumeration. */
            for (uint32_t j = 0; j < k; j++)
                lftj_key_range(&iters[j], max_key, &ranges[(size_t)j * 2u],
                    &ranges[(size_t)j * 2u + 1u]);

            /* Emit Cartesian product of matching rows. */
            rc = lftj_emit_product(iters, k, max_key, ranges, 0, row_buf, 1u,
                    cb, user, total_cols);
            if (rc != 0)
                break;

            /*
             * Advance all iterators past max_key and compute next max_key.
             * If any iterator reaches end, the join is complete.
             */
            int64_t next_max = 0;
            bool have_next_max = false;
            bool any_end = false;
            for (uint32_t j = 0; j < k; j++) {
                iters[j].pos = ranges[j * 2u + 1u]; /* skip past range */
                if (lftj_at_end(&iters[j])) {
                    any_end = true;
                    break;
                }
                int64_t nk = lftj_key(&iters[j]);
                if (!have_next_max
                    || lftj_compare_key(&iters[j], nk, next_max) > 0) {
                    next_max = nk;
                    have_next_max = true;
                }
            }
            if (any_end)
                goto done;

            max_key = next_max;
        }

done:
        free(row_buf);
        free(ranges);
    }

cleanup:
    if (iters) {
        for (uint32_t i = 0; i < k; i++)
            lftj_iter_free(&iters[i]);
    }
    free(iters);
    wl_columnar_memory_release(&reservation);
    return rc;
}

int
wl_columnar_lftj_join_typed(const wl_lftj_input_t *inputs,
    wirelog_column_type_t key_type, uint32_t k, wl_lftj_result_fn cb,
    void *user)
{
    return wl_columnar_lftj_join_typed_governed(inputs, key_type, k, cb,
               user, NULL);
}

int
wl_lftj_join(const wl_lftj_input_t *inputs, uint32_t k, wl_lftj_result_fn cb,
    void *user)
{
    return wl_columnar_lftj_join_typed(inputs, WIRELOG_TYPE_INT64, k, cb,
               user);
}

/* Framed composed LFTJ uses private snapshots rather than the shared sorted
 * arrangement cache. That keeps cancellation/retry free of cache side effects. */
int
wl_columnar_ops_lftj_attempt_preflight(const wl_plan_op_t *op,
    wl_col_session_t *session)
{
    if (!op || op->op != WL_PLAN_OP_LFTJ || !op->opaque_data || !session)
        return EINVAL;
    if (session->tdd_input_run || session->diff_operators_active
        || session->current_iteration != 0)
        return ENOTSUP;
    const wl_plan_op_lftj_t *meta = op->opaque_data;
    if (meta->k < 2 || meta->k > WL_LFTJ_MAX_K || !meta->rel_names
        || !meta->key_cols)
        return EINVAL;
    uint32_t ncols = 0;
    wirelog_column_type_t key_type = WIRELOG_TYPE_INT64;
    for (uint32_t i = 0; i < meta->k; i++) {
        if (!meta->rel_names[i]) return EINVAL;
        col_rel_t *rel = session_find_rel(session, meta->rel_names[i]);
        if (!rel) return ENOENT;
        if (!rel->ncols || meta->key_cols[i] >= rel->ncols || rel->timestamps
            || rel->compound_kind != WIRELOG_COMPOUND_KIND_NONE)
            return ENOTSUP;
        if (rel->ncols > WL_COLUMNAR_LFTJ_CONTROLLED_MAX_COLUMNS - ncols)
            return ENOTSUP;
        ncols += rel->ncols;
        wirelog_column_type_t type = rel->column_types
            ? rel->column_types[meta->key_cols[i]] : WIRELOG_TYPE_INT64;
        if (i && type != key_type) return EINVAL;
        key_type = type;
    }
    return 0;
}

int
wl_columnar_ops_lftj_attempt_begin(wl_columnar_lftj_attempt_t *attempt,
    const wl_plan_op_t *op, wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_col_session_t *session, wl_evaluation_control_t *control,
    uint64_t attempt_id, const void *numeric_owner)
{
    if (!attempt || !frame || !session || !control || !attempt_id)
        return EINVAL;
    int rc = wl_columnar_ops_lftj_attempt_preflight(op, session);
    if (rc) return rc;
    memset(attempt, 0, sizeof(*attempt));
    attempt->identity = attempt;
    attempt->metadata = op->opaque_data;
    attempt->frame = frame;
    attempt->numeric_owner = numeric_owner;
    attempt->session = session;
    attempt->control = control;
    attempt->attempt_id = attempt_id;
    attempt->phase = WL_COLUMNAR_LFTJ_ATTEMPT_RESOLVE;
    wl_columnar_memory_reservation_init(&attempt->scratch_reservation);
    return 0;
}

/* PRODUCT_EMIT charges total_columns + 8 around the row append. The widest
 * supported output has 64 columns, so every atomic append fits this quantum. */
enum { WL_COLUMNAR_LFTJ_ACTION_UNITS = 72u };
_Static_assert(WL_COLUMNAR_LFTJ_ACTION_UNITS
    >= WL_COLUMNAR_LFTJ_CONTROLLED_MAX_COLUMNS + 8u,
    "LFTJ row append must fit within one action quantum");

static int
wl_columnar_lftj_attempt_charge(wl_columnar_lftj_attempt_t *a, uint64_t work)
{
    if (!a || a->identity != a || !a->control || !a->attempt_id
        || wl_evaluation_control_active_attempt_id(a->control) != a->attempt_id)
        return EINVAL;
    return wl_evaluation_control_charge(a->control, work);
}

static int
wl_columnar_lftj_attempt_compare(const wl_columnar_lftj_input_attempt_t *in,
    int64_t left, int64_t right)
{
    if (in->key_type == WIRELOG_TYPE_FLOAT)
        return wl_columnar_float_compare_bits(left, right);
    return left < right ? -1 : left > right ? 1 : 0;
}

static int64_t
wl_columnar_lftj_attempt_key(const wl_columnar_lftj_input_attempt_t *in,
    uint32_t row)
{
    return in->rows[(size_t)row * in->ncols + in->key_col];
}

static int
wl_columnar_lftj_attempt_fail(wl_columnar_lftj_attempt_t *a, int rc)
{
    if (!a->semantic_result) a->semantic_result = rc ? rc : EINVAL;
    a->phase = WL_COLUMNAR_LFTJ_ATTEMPT_RELEASE;
    return 0;
}

static int
wl_columnar_lftj_attempt_resolve(wl_columnar_lftj_attempt_t *a)
{
    if (!a->inputs) {
        int rc = wl_columnar_lftj_attempt_charge(a, 16);
        if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
        a->inputs = calloc(a->metadata->k, sizeof(*a->inputs));
        if (!a->inputs) return wl_columnar_lftj_attempt_fail(a, ENOMEM);
        return 0;
    }
    if (a->cursor < a->metadata->k) {
        int rc = wl_columnar_lftj_attempt_charge(a, 24);
        if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
        uint32_t i = a->cursor;
        wl_columnar_lftj_input_attempt_t *in = &a->inputs[i];
        col_rel_t *rel = session_find_rel(a->session,
                a->metadata->rel_names[i]);
        if (!rel) return wl_columnar_lftj_attempt_fail(a, ENOENT);
        uint32_t key = a->metadata->key_cols[i];
        if (!rel->ncols || key >= rel->ncols || rel->timestamps
            || rel->compound_kind != WIRELOG_COMPOUND_KIND_NONE)
            return wl_columnar_lftj_attempt_fail(a, ENOTSUP);
        in->relation = rel;
        in->nrows = rel->nrows;
        in->ncols = rel->ncols;
        in->key_col = key;
        in->key_type = rel->column_types ? rel->column_types[key]
            : WIRELOG_TYPE_INT64;
        if (i && in->key_type != a->inputs[0].key_type)
            return wl_columnar_lftj_attempt_fail(a, EINVAL);
        if (a->total_columns > WL_COLUMNAR_LFTJ_CONTROLLED_MAX_COLUMNS
            - in->ncols)
            return wl_columnar_lftj_attempt_fail(a, ENOTSUP);
        a->binary_offset[i] = a->total_columns;
        for (uint32_t c = 0; c < in->ncols; c++)
            a->output_types[a->total_columns + c] = rel->column_types
                ? (int64_t)rel->column_types[c] : WIRELOG_TYPE_INT64;
        a->total_columns += in->ncols;
        rc = col_rel_source_reader_acquire(rel, &in->reader);
        if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
        a->input_count = ++a->cursor;
        return 0;
    }
    uint64_t bytes = (uint64_t)a->metadata->k * sizeof(*a->inputs);
    uint64_t row_bytes;
    if (!wl_columnar_memory_size_mul(a->total_columns, sizeof(*a->row),
        &row_bytes)
        || !wl_columnar_memory_size_add(bytes, row_bytes, &bytes))
        return wl_columnar_lftj_attempt_fail(a, EOVERFLOW);
    for (uint32_t i = 0; i < a->input_count; i++) {
        uint64_t cells, pair_bytes;
        if (!wl_columnar_memory_size_mul(a->inputs[i].nrows,
            a->inputs[i].ncols, &cells)
            || !wl_columnar_memory_size_mul(cells, 2u * sizeof(int64_t),
            &pair_bytes)
            || !wl_columnar_memory_size_add(bytes, pair_bytes, &bytes))
            return wl_columnar_lftj_attempt_fail(a, EOVERFLOW);
    }
    a->scratch_bytes = bytes;
    if (a->session->memory_governor && bytes) {
        wl_columnar_memory_governor_t *g =
            wl_columnar_memory_governor_ref_get(a->session->memory_governor);
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(g, bytes,
                &a->scratch_reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                a->session->memory_budget_denied = true;
            return wl_columnar_lftj_attempt_fail(a,
                       status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL);
        }
        a->scratch_reserved = true;
    }
    a->row = malloc((size_t)(row_bytes ? row_bytes : sizeof(int64_t)));
    if (!a->row) return wl_columnar_lftj_attempt_fail(a, ENOMEM);
    a->cursor = 0;
    a->phase = WL_COLUMNAR_LFTJ_ATTEMPT_ALLOC;
    return 0;
}

static int
wl_columnar_lftj_attempt_alloc(wl_columnar_lftj_attempt_t *a)
{
    if (a->cursor < a->input_count) {
        int rc = wl_columnar_lftj_attempt_charge(a, 8);
        if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
        wl_columnar_lftj_input_attempt_t *in = &a->inputs[a->cursor];
        uint64_t cells, bytes;
        if (!wl_columnar_memory_size_mul(in->nrows, in->ncols, &cells)
            || !wl_columnar_memory_size_mul(cells, sizeof(int64_t), &bytes)
            || bytes > SIZE_MAX)
            return wl_columnar_lftj_attempt_fail(a, EOVERFLOW);
        if (bytes) {
            in->rows = malloc((size_t)bytes);
            in->scratch = malloc((size_t)bytes);
            if (!in->rows || !in->scratch)
                return wl_columnar_lftj_attempt_fail(a, ENOMEM);
        }
        ++a->cursor;
        return 0;
    }
    if (a->scratch_reserved) {
        if (!wl_columnar_memory_commit(&a->scratch_reservation, a))
            return wl_columnar_lftj_attempt_fail(a, EINVAL);
        a->scratch_committed = true;
    }
    a->cursor = 0;
    a->phase = WL_COLUMNAR_LFTJ_ATTEMPT_COPY;
    return 0;
}

static int
wl_columnar_lftj_attempt_copy(wl_columnar_lftj_attempt_t *a)
{
    if (a->cursor == a->input_count) {
        a->cursor = 0;
        a->phase = WL_COLUMNAR_LFTJ_ATTEMPT_JOIN;
        a->output_pass = false;
        a->join_phase = WL_COLUMNAR_LFTJ_JOIN_INPUT_CHECK;
        a->join_index = 0;
        return 0;
    }
    wl_columnar_lftj_input_attempt_t *in = &a->inputs[a->cursor];
    uint64_t cells = (uint64_t)in->nrows * in->ncols;
    uint64_t remain = cells - in->copy_cell;
    uint32_t count = remain < WL_COLUMNAR_LFTJ_ACTION_UNITS
        ? (uint32_t)remain : WL_COLUMNAR_LFTJ_ACTION_UNITS;
    if (count) {
        int rc = wl_columnar_lftj_attempt_charge(a, count);
        if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
        for (uint32_t i = 0; i < count; i++, in->copy_cell++) {
            uint32_t row = (uint32_t)(in->copy_cell / in->ncols);
            uint32_t col = (uint32_t)(in->copy_cell % in->ncols);
            int64_t value = col_rel_get(in->relation, row, col);
            if (col == in->key_col && in->key_type == WIRELOG_TYPE_FLOAT
                && !wl_columnar_float_bits_valid(value))
                return wl_columnar_lftj_attempt_fail(a, EINVAL);
            in->rows[in->copy_cell] = value;
        }
        return 0;
    }
    in->sort_width = 1;
    in->sort_run = 0;
    in->sort_copying = false;
    if (in->nrows <= 1) {
        ++a->cursor;
        return 0;
    }
    a->phase = WL_COLUMNAR_LFTJ_ATTEMPT_SORT;
    return 0;
}

static int
wl_columnar_lftj_attempt_sort(wl_columnar_lftj_attempt_t *a)
{
    wl_columnar_lftj_input_attempt_t *in = &a->inputs[a->cursor];
    uint32_t units = 0;
    while (units < WL_COLUMNAR_LFTJ_ACTION_UNITS) {
        if (in->sort_copying) {
            int rc = wl_columnar_lftj_attempt_charge(a, 1);
            if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
            size_t src = (size_t)in->sort_source_row * in->ncols
                + in->sort_source_col;
            size_t dst = (size_t)in->sort_dest * in->ncols
                + in->sort_source_col;
            in->scratch[dst] = in->rows[src];
            if (++in->sort_source_col == in->ncols) {
                in->sort_copying = false;
                in->sort_source_col = 0;
                ++in->sort_dest;
                if (in->sort_take_left) ++in->sort_left;
                else ++in->sort_right;
            }
            ++units;
            continue;
        }
        if (!in->sort_end || in->sort_dest == in->sort_end) {
            if (in->sort_end) in->sort_run = in->sort_end;
            if (in->sort_run >= in->nrows) {
                int64_t *tmp = in->rows; in->rows = in->scratch;
                in->scratch = tmp;
                if (in->sort_width >= in->nrows / 2u + (in->nrows & 1u)) {
                    ++a->cursor;
                    a->phase = a->cursor == a->input_count
                        ? WL_COLUMNAR_LFTJ_ATTEMPT_JOIN
                        : WL_COLUMNAR_LFTJ_ATTEMPT_COPY;
                    return 0;
                }
                in->sort_width *= 2u;
                in->sort_run = 0;
                in->sort_end = 0;
            }
            uint32_t left = in->sort_run;
            uint32_t remaining = in->nrows - left;
            in->sort_mid = left + (in->sort_width < remaining
                ? in->sort_width : remaining);
            remaining = in->nrows - in->sort_mid;
            in->sort_end = in->sort_mid + (in->sort_width < remaining
                ? in->sort_width : remaining);
            in->sort_left = left;
            in->sort_right = in->sort_mid;
            in->sort_dest = left;
            continue;
        }
        bool take_left;
        if (in->sort_left >= in->sort_mid) take_left = false;
        else if (in->sort_right >= in->sort_end) take_left = true;
        else {
            int rc = wl_columnar_lftj_attempt_charge(a, 1);
            if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
            int64_t lk = wl_columnar_lftj_attempt_key(in, in->sort_left);
            int64_t rk = wl_columnar_lftj_attempt_key(in, in->sort_right);
            take_left = wl_columnar_lftj_attempt_compare(in, lk, rk) <= 0;
            ++units;
        }
        in->sort_source_row = take_left ? in->sort_left : in->sort_right;
        in->sort_take_left = take_left;
        in->sort_copying = true;
    }
    return 0;
}

static int
wl_columnar_lftj_attempt_limit_output(wl_columnar_lftj_attempt_t *a)
{
    if (a->output_rows >= UINT32_MAX
        || (a->session->join_output_limit
        && a->output_rows >= a->session->join_output_limit))
        return EOVERFLOW;
    ++a->output_rows;
    return 0;
}

static int
wl_columnar_lftj_attempt_finish_scan(wl_columnar_lftj_attempt_t *a)
{
    a->phase = a->output_pass ? WL_COLUMNAR_LFTJ_ATTEMPT_OUTPUT_END
        : WL_COLUMNAR_LFTJ_ATTEMPT_OUTPUT_CREATE;
    return 0;
}

static int
wl_columnar_lftj_attempt_join(wl_columnar_lftj_attempt_t *a)
{
    if (!a->input_count) return wl_columnar_lftj_attempt_fail(a, EINVAL);
    if (a->join_phase == WL_COLUMNAR_LFTJ_JOIN_INPUT_CHECK) {
        int rc = wl_columnar_lftj_attempt_charge(a, 1);
        if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
        if (!a->inputs[a->join_index].nrows)
            return wl_columnar_lftj_attempt_finish_scan(a);
        if (++a->join_index == a->input_count) {
            a->join_index = 0;
            a->join_phase = WL_COLUMNAR_LFTJ_JOIN_INITIAL_MAX;
        }
        return 0;
    }
    switch (a->join_phase) {
    case WL_COLUMNAR_LFTJ_JOIN_INPUT_CHECK:
        return wl_columnar_lftj_attempt_fail(a, EINVAL);
    case WL_COLUMNAR_LFTJ_JOIN_INITIAL_MAX:
        if (!a->join_index) {
            int rc = wl_columnar_lftj_attempt_charge(a, 1);
            if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
            a->max_key = wl_columnar_lftj_attempt_key(&a->inputs[0],
                    a->inputs[0].position);
            a->join_index = 1;
            return 0;
        }
        if (a->join_index < a->input_count) {
            int rc = wl_columnar_lftj_attempt_charge(a, 1);
            if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
            wl_columnar_lftj_input_attempt_t *in = &a->inputs[a->join_index];
            int64_t key = wl_columnar_lftj_attempt_key(in, in->position);
            if (wl_columnar_lftj_attempt_compare(in, key, a->max_key) > 0)
                a->max_key = key;
            ++a->join_index;
            return 0;
        }
        a->join_index = 0;
        a->join_phase = WL_COLUMNAR_LFTJ_JOIN_SEEK_START;
        return 0;
    case WL_COLUMNAR_LFTJ_JOIN_SEEK_START:
        if (a->join_index == a->input_count) {
            a->join_index = 0;
            a->join_phase = WL_COLUMNAR_LFTJ_JOIN_RANGE_START;
            return 0;
        }
        a->seek_lo = a->inputs[a->join_index].position;
        a->seek_hi = a->inputs[a->join_index].nrows;
        a->join_phase = WL_COLUMNAR_LFTJ_JOIN_SEEK_SEARCH;
        return 0;
    case WL_COLUMNAR_LFTJ_JOIN_SEEK_SEARCH:
        if (a->seek_lo < a->seek_hi) {
            int rc = wl_columnar_lftj_attempt_charge(a, 1);
            if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
            a->seek_mid = a->seek_lo + (a->seek_hi - a->seek_lo) / 2u;
            wl_columnar_lftj_input_attempt_t *in = &a->inputs[a->join_index];
            int64_t key = wl_columnar_lftj_attempt_key(in, a->seek_mid);
            if (wl_columnar_lftj_attempt_compare(in, key, a->max_key) < 0)
                a->seek_lo = a->seek_mid + 1u;
            else a->seek_hi = a->seek_mid;
            return 0;
        }
        {
            wl_columnar_lftj_input_attempt_t *in = &a->inputs[a->join_index];
            in->position = a->seek_lo;
            if (in->position == in->nrows)
                return wl_columnar_lftj_attempt_finish_scan(a);
            int64_t actual = wl_columnar_lftj_attempt_key(in, in->position);
            if (wl_columnar_lftj_attempt_compare(in, actual, a->max_key) > 0) {
                a->max_key = actual;
                a->join_index = 0;
            } else ++a->join_index;
            a->join_phase = WL_COLUMNAR_LFTJ_JOIN_SEEK_START;
            return 0;
        }
    case WL_COLUMNAR_LFTJ_JOIN_RANGE_START:
        if (a->join_index == a->input_count) {
            a->product_depth = a->input_count - 1u;
            a->join_index = 0;
            a->join_phase = WL_COLUMNAR_LFTJ_JOIN_PRODUCT_START;
            return 0;
        }
        a->inputs[a->join_index].range_lo =
            a->inputs[a->join_index].position;
        a->seek_lo = a->inputs[a->join_index].position;
        a->seek_hi = a->inputs[a->join_index].nrows;
        a->join_phase = WL_COLUMNAR_LFTJ_JOIN_RANGE_SEARCH;
        return 0;
    case WL_COLUMNAR_LFTJ_JOIN_RANGE_SEARCH:
        if (a->seek_lo < a->seek_hi) {
            int rc = wl_columnar_lftj_attempt_charge(a, 1);
            if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
            a->seek_mid = a->seek_lo + (a->seek_hi - a->seek_lo) / 2u;
            wl_columnar_lftj_input_attempt_t *in = &a->inputs[a->join_index];
            int64_t key = wl_columnar_lftj_attempt_key(in, a->seek_mid);
            if (wl_columnar_lftj_attempt_compare(in, key, a->max_key) <= 0)
                a->seek_lo = a->seek_mid + 1u;
            else a->seek_hi = a->seek_mid;
            return 0;
        }
        a->inputs[a->join_index].range_hi = a->seek_lo;
        ++a->join_index;
        a->join_phase = WL_COLUMNAR_LFTJ_JOIN_RANGE_START;
        return 0;
    case WL_COLUMNAR_LFTJ_JOIN_PRODUCT_START:
        if (a->join_index < a->input_count) {
            int rc = wl_columnar_lftj_attempt_charge(a, 1);
            if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
            a->inputs[a->join_index].product_index
                = a->inputs[a->join_index].range_lo;
            ++a->join_index;
            return 0;
        }
        a->join_index = 0;
        a->fill_input = a->fill_col = 0;
        a->join_phase = a->output_pass
            ? WL_COLUMNAR_LFTJ_JOIN_PRODUCT_EMIT
            : WL_COLUMNAR_LFTJ_JOIN_PRODUCT_COUNT;
        return 0;
    case WL_COLUMNAR_LFTJ_JOIN_PRODUCT_COUNT: {
        int rc = wl_columnar_lftj_attempt_charge(a, 1);
        if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
        rc = wl_columnar_lftj_attempt_limit_output(a);
        if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
        a->join_phase = WL_COLUMNAR_LFTJ_JOIN_PRODUCT_ADVANCE;
        return 0;
    }
    case WL_COLUMNAR_LFTJ_JOIN_PRODUCT_EMIT:
        if (a->fill_input < a->input_count) {
            int rc = wl_columnar_lftj_attempt_charge(a, 1);
            if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
            wl_columnar_lftj_input_attempt_t *in = &a->inputs[a->fill_input];
            uint32_t col = a->fill_col;
            a->row[a->binary_offset[a->fill_input] + col]
                = col == in->key_col ? a->max_key
                : in->rows[(size_t)in->product_index * in->ncols + col];
            if (++a->fill_col == in->ncols) {
                a->fill_col = 0;
                ++a->fill_input;
            }
            return 0;
        }
        {
            int rc = wl_columnar_lftj_attempt_charge(a,
                    (uint64_t)a->total_columns + 8u);
            if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
            rc = col_rel_append_batch_row(&a->writer, a->row);
            if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
            ++a->output_written;
            a->join_phase = WL_COLUMNAR_LFTJ_JOIN_PRODUCT_ADVANCE;
            return 0;
        }
    case WL_COLUMNAR_LFTJ_JOIN_PRODUCT_ADVANCE: {
        int rc = wl_columnar_lftj_attempt_charge(a, 1);
        if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
        uint32_t d = a->product_depth;
        wl_columnar_lftj_input_attempt_t *in = &a->inputs[d];
        if (in->product_index + 1u < in->range_hi) {
            ++in->product_index;
            a->product_depth = a->input_count - 1u;
            a->fill_input = a->fill_col = 0;
            a->join_phase = a->output_pass
                ? WL_COLUMNAR_LFTJ_JOIN_PRODUCT_EMIT
                : WL_COLUMNAR_LFTJ_JOIN_PRODUCT_COUNT;
        } else if (d) {
            in->product_index = in->range_lo;
            --a->product_depth;
        } else {
            in->product_index = in->range_lo;
            a->join_index = 0;
            a->join_phase = WL_COLUMNAR_LFTJ_JOIN_ADVANCE_KEY;
        }
        return 0;
    }
    case WL_COLUMNAR_LFTJ_JOIN_ADVANCE_KEY:
        if (a->join_index == a->input_count) {
            a->join_index = 0;
            a->max_key = wl_columnar_lftj_attempt_key(&a->inputs[0],
                    a->inputs[0].position);
            a->join_index = 1;
            a->join_phase = WL_COLUMNAR_LFTJ_JOIN_INITIAL_MAX;
            return 0;
        }
        {
            int rc = wl_columnar_lftj_attempt_charge(a, 1);
            if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
            wl_columnar_lftj_input_attempt_t *in = &a->inputs[a->join_index];
            in->position = in->range_hi;
            if (in->position == in->nrows)
                return wl_columnar_lftj_attempt_finish_scan(a);
            ++a->join_index;
            return 0;
        }
    }
    return wl_columnar_lftj_attempt_fail(a, EINVAL);
}

static int
wl_columnar_lftj_attempt_release(wl_columnar_lftj_attempt_t *a, bool *done)
{
    if (a->writer_active) {
        int rc = col_rel_append_batch_end(&a->writer,
                a->semantic_result == 0);
        a->writer_active = false;
        if (rc && !a->semantic_result) a->semantic_result = rc;
        return 0;
    }
    if (a->output_owned && a->semantic_result) {
        if (col_rel_destroy_checked(a->output) != 0) return 0;
        a->output = NULL;
        a->output_owned = false;
        return 0;
    }
    for (uint32_t i = 0; a->inputs && i < a->input_count; i++) {
        wl_columnar_lftj_input_attempt_t *in = &a->inputs[i];
        if (in->reader.owner) {
            if (col_rel_source_reader_release(&in->reader) != 0) return 0;
            return 0;
        }
        if (in->rows || in->scratch) {
            free(in->rows); in->rows = NULL;
            free(in->scratch); in->scratch = NULL;
            return 0;
        }
    }
    if (a->scratch_reserved) {
        bool released = a->scratch_committed
            ? wl_columnar_memory_release(&a->scratch_reservation)
            : wl_columnar_memory_rollback(&a->scratch_reservation);
        if (!released) return 0;
        a->scratch_reserved = a->scratch_committed = false;
        return 0;
    }
    free(a->row); a->row = NULL;
    free(a->inputs); a->inputs = NULL;
    if (!a->semantic_result && a->output_owned) {
        eval_stack_t *stack = NULL;
        int rc = wl_columnar_eval_stack_operator_stack_owned(a->frame,
                a->session, &stack, a->numeric_owner);
        if (rc) {
            a->semantic_result = rc;
            return 0;
        }
        eval_entry_t entry = { .rel = a->output, .owned = true,
                               .kind = WL_COLUMNAR_EVAL_ENTRY_RELATION };
        rc = eval_stack_repush_entry(stack, &entry);
        if (rc) {
            a->semantic_result = rc;
            return 0;
        }
        a->output = NULL;
        a->output_owned = false;
        a->registered = true;
    }
    a->phase = WL_COLUMNAR_LFTJ_ATTEMPT_DONE;
    *done = true;
    return 0;
}

int
wl_columnar_ops_lftj_attempt_step(wl_columnar_lftj_attempt_t *a, bool *done)
{
    if (!done) return EINVAL;
    *done = false;
    if (!a || a->identity != a || !a->control || !a->session || !a->frame)
        return EINVAL;
    if (a->phase == WL_COLUMNAR_LFTJ_ATTEMPT_DONE) {
        *done = true;
        return 0;
    }
    if (a->phase == WL_COLUMNAR_LFTJ_ATTEMPT_RELEASE)
        return wl_columnar_lftj_attempt_release(a, done);
    int rc = wl_columnar_lftj_attempt_charge(a, 0);
    if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
    switch (a->phase) {
    case WL_COLUMNAR_LFTJ_ATTEMPT_RESOLVE:
        return wl_columnar_lftj_attempt_resolve(a);
    case WL_COLUMNAR_LFTJ_ATTEMPT_ALLOC:
        return wl_columnar_lftj_attempt_alloc(a);
    case WL_COLUMNAR_LFTJ_ATTEMPT_COPY:
        return wl_columnar_lftj_attempt_copy(a);
    case WL_COLUMNAR_LFTJ_ATTEMPT_SORT:
        return wl_columnar_lftj_attempt_sort(a);
    case WL_COLUMNAR_LFTJ_ATTEMPT_JOIN:
        return wl_columnar_lftj_attempt_join(a);
    case WL_COLUMNAR_LFTJ_ATTEMPT_OUTPUT_CREATE:
        rc = wl_columnar_relation_new_typed_codes_operator_attempt_controlled(
            "$lftj", a->total_columns, (uint32_t)a->output_rows,
            a->output_types, a->session->memory_governor, &a->output,
            a->control, a->attempt_id);
        if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
        a->output_owned = true;
        a->phase = WL_COLUMNAR_LFTJ_ATTEMPT_OUTPUT_BEGIN;
        return 0;
    case WL_COLUMNAR_LFTJ_ATTEMPT_OUTPUT_BEGIN:
        rc = col_rel_append_batch_begin(&a->writer, a->output);
        if (rc) return wl_columnar_lftj_attempt_fail(a, rc);
        a->writer_active = true;
        a->output_pass = true;
        a->output_written = 0;
        for (uint32_t i = 0; i < a->input_count; i++)
            a->inputs[i].position = 0;
        a->join_index = 0;
        a->join_phase = WL_COLUMNAR_LFTJ_JOIN_INPUT_CHECK;
        a->phase = WL_COLUMNAR_LFTJ_ATTEMPT_JOIN;
        return 0;
    case WL_COLUMNAR_LFTJ_ATTEMPT_OUTPUT_END:
        if (a->output_pass && a->output_written != a->output_rows)
            return wl_columnar_lftj_attempt_fail(a, EINVAL);
        a->phase = WL_COLUMNAR_LFTJ_ATTEMPT_RELEASE;
        return 0;
    case WL_COLUMNAR_LFTJ_ATTEMPT_OUTPUT_FILL:
        return wl_columnar_lftj_attempt_fail(a, EINVAL);
    case WL_COLUMNAR_LFTJ_ATTEMPT_RELEASE:
    case WL_COLUMNAR_LFTJ_ATTEMPT_DONE:
    case WL_COLUMNAR_LFTJ_ATTEMPT_INERT:
        return EINVAL;
    }
    return wl_columnar_lftj_attempt_fail(a, EINVAL);
}

int
wl_columnar_ops_lftj_attempt_abort(wl_columnar_lftj_attempt_t *a, bool *done)
{
    if (!done) return EINVAL;
    *done = false;
    if (!a || a->identity != a) {
        *done = true;
        return 0;
    }
    if (a->phase == WL_COLUMNAR_LFTJ_ATTEMPT_DONE) {
        *done = true;
        return 0;
    }
    if (!a->semantic_result)
        a->semantic_result = WL_EVALUATION_CONTROL_CANCELLED;
    a->phase = WL_COLUMNAR_LFTJ_ATTEMPT_RELEASE;
    return wl_columnar_lftj_attempt_release(a, done);
}
