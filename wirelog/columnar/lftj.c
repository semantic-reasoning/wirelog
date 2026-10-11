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
#ifdef WL_TEST_LFTJ_ADMISSION_HOOKS
size_t wl_columnar_lftj_test_iterator_size(void)
{
    return sizeof(lftj_iter_t);
}
#endif

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

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_lftj_test_checkpoint)(
    wl_columnar_lftj_record_t *, int, uint64_t);
#endif

int
wl_columnar_lftj_work(wl_columnar_lftj_record_t *record, uint64_t work)
{
    return record && record->ready && record->control
        ? wl_evaluation_control_charge(record->control, work) : EINVAL;
}

int
wl_columnar_lftj_tail(wl_columnar_lftj_record_t *record, int stage,
    uint64_t progress)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_lftj_test_checkpoint)
        wl_columnar_lftj_test_checkpoint(record, stage, progress);
#else
    (void)stage; (void)progress;
#endif
    return wl_columnar_lftj_work(record, 0);
}

int
wl_columnar_lftj_reserve(wl_columnar_lftj_record_t *record,
    wl_columnar_memory_reservation_t *token, uint64_t bytes)
{
    if (!record || !record->session || !record->session->memory_governor)
        return EINVAL;
    wl_columnar_memory_admission_status_t status;
    int rc = wl_columnar_memory_governor_reserve_checked_controlled(
        wl_columnar_memory_governor_ref_get(record->session->memory_governor),
        bytes, token, &status, record->control);
    if (rc) return rc;
    return status == WL_COLUMNAR_MEMORY_ADMISSION_OK
           || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY ? 0
        : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
        : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
}

int
wl_columnar_lftj_commit(wl_columnar_lftj_record_t *record,
    wl_columnar_memory_reservation_t *token, const void *payload)
{
    bool accepted = false;
    int rc = wl_columnar_memory_governor_commit_controlled(token, payload,
            &accepted, record->control);
    return rc ? rc : accepted ? 0 : EINVAL;
}

static int
wl_columnar_lftj_credit_retire(wl_columnar_memory_reservation_t *token)
{
    if (token->identity != token) return EINVAL;
    uint64_t state = atomic_load_explicit(&token->state, memory_order_acquire);
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
        || state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED) return 0;
    bool accepted = state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        ? wl_columnar_memory_release(token)
        : state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        && wl_columnar_memory_rollback(token);
    return accepted ? 0 : EINVAL;
}

/* Every owner remains reachable after refusal. Only successful physical
 * retirement clears its pointer; a failed original credit stays in the record.
 * Error cleanup is the checked mandatory drain, without a fresh allowance. */
int
wl_columnar_lftj_record_retire(wl_columnar_lftj_record_t *record, int primary,
    bool mandatory)
{
    if (!record || !record->ready) return EINVAL;
#define WL_LFTJ_RETIRE_STEP(work) do { \
            int step_rc = mandatory ? 0 : wl_columnar_lftj_work(record, (work)); \
            if (step_rc) return primary > 0 ? primary : step_rc; \
} while (0)
#define WL_LFTJ_RETIRE_TAIL(stage, progress) do { \
            int step_rc = mandatory ? 0 : wl_columnar_lftj_tail(record, stage, \
                    progress); \
            if (step_rc) return primary > 0 ? primary : step_rc; \
} while (0)
    lftj_iter_t *iters = record->iters;
    while (record->retiring < record->iter_count) {
        WL_LFTJ_RETIRE_STEP(1);
        free(iters[record->retiring].sorted);
        iters[record->retiring].sorted = NULL;
        record->retiring++;
        WL_LFTJ_RETIRE_TAIL(11, record->retiring);
    }
    WL_LFTJ_RETIRE_STEP(96);
    free(record->radix_temp); record->radix_temp = NULL;
    record->sort_bytes = 0;
    free(record->row); record->row = NULL;
    free(record->ranges); record->ranges = NULL;
    free(record->iters); record->iters = NULL;
    int rc = wl_columnar_lftj_credit_retire(&record->inner);
    if (rc) return primary > 0 ? primary : rc;
    WL_LFTJ_RETIRE_TAIL(11, 0);
    WL_LFTJ_RETIRE_STEP(1);
    free(record->cache_fresh.sorted); record->cache_fresh.sorted = NULL;
    if (record->cache_ledger_installed) {
        wl_mem_ledger_free(record->cache_fresh.ledger,
            WL_MEM_SUBSYS_ARRANGEMENT, record->cache_fresh.ledger_bytes);
        record->cache_fresh.ledger_bytes = 0;
        record->cache_ledger_installed = false;
    }
    free(record->cache_name); record->cache_name = NULL;
    /* Registry pending storage contains only unpinned raw metadata, no live
     * token. It is distinct from the session's published registry. */
    free(record->registry_new); record->registry_new = NULL;
    free(record->cache_old); record->cache_old = NULL;
    if (record->old_ledger_bytes) {
        wl_mem_ledger_free(&record->session->mem_ledger,
            WL_MEM_SUBSYS_ARRANGEMENT, record->old_ledger_bytes);
        record->old_ledger_bytes = 0;
    }
    WL_LFTJ_RETIRE_TAIL(11, 0);
    for (uint32_t i = 0; i < record->reader_count; i++) {
        WL_LFTJ_RETIRE_STEP(32);
        rc = wl_columnar_arrangement_sorted_probe_release_controlled(
            &record->probes[i], &record->readers[i], record->relations[i],
            mandatory ? NULL : record->control);
        if (rc) return primary > 0 ? primary : rc;
        WL_LFTJ_RETIRE_TAIL(11, i + 1u);
    }
    for (uint32_t i = 0; i < record->input_count; i++) {
        WL_LFTJ_RETIRE_STEP(1);
        if (!record->cache_backed[i]) free((void *)record->inputs[i].data);
        record->inputs[i].data = NULL;
        WL_LFTJ_RETIRE_TAIL(11, i + 1u);
    }
    WL_LFTJ_RETIRE_STEP(96);
    free(record->binary_row); record->binary_row = NULL;
    free(record->inputs); record->inputs = NULL;
    free(record->probes); record->probes = NULL;
    free(record->relations); record->relations = NULL;
    free(record->readers); record->readers = NULL;
    free(record->cache_backed); record->cache_backed = NULL;
    free(record->ncols); record->ncols = NULL;
    free(record->compact_offsets); record->compact_offsets = NULL;
    free(record->binary_offsets); record->binary_offsets = NULL;
    /* Cleared scalar prefixes prevent replay through retired array storage. */
    record->reader_count = record->input_count = record->iter_count = 0;
    record->retiring = 0;
    rc = wl_columnar_lftj_credit_retire(&record->outer);
    if (rc) return primary > 0 ? primary : rc;
    WL_LFTJ_RETIRE_TAIL(11, 0);
#undef WL_LFTJ_RETIRE_STEP
#undef WL_LFTJ_RETIRE_TAIL
    return primary;
}

static int
wl_columnar_lftj_seek_controlled(wl_columnar_lftj_record_t *record,
    lftj_iter_t *it, int64_t key)
{
    uint32_t lo = it->pos, hi = it->nrows;
    while (lo < hi) {
        int rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
        uint32_t mid = lo + (hi - lo) / 2u;
        if (lftj_compare_key(it,
            it->sorted[(size_t)mid * it->ncols + it->key_col], key) < 0)
            lo = mid + 1u;
        else hi = mid;
        it->pos = lo;
        rc = wl_columnar_lftj_tail(record, 7, mid + 1u); if (rc) return rc;
    }
    it->pos = lo;
    return 0;
}

static int
wl_columnar_lftj_range_controlled(wl_columnar_lftj_record_t *record,
    const lftj_iter_t *it, int64_t key, uint32_t *lo, uint32_t *hi)
{
    *lo = it->pos;
    uint32_t left = it->pos, right = it->nrows;
    while (left < right) {
        int rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
        uint32_t mid = left + (right - left) / 2u;
        if (lftj_compare_key(it,
            it->sorted[(size_t)mid * it->ncols + it->key_col], key) <= 0)
            left = mid + 1u;
        else right = mid;
        rc = wl_columnar_lftj_tail(record, 8, mid + 1u); if (rc) return rc;
    }
    *hi = left;
    return 0;
}

static int
wl_columnar_lftj_product_controlled(wl_columnar_lftj_record_t *record,
    uint32_t depth, uint32_t offset, int64_t key,
    int (*emit)(const int64_t *, uint32_t, void *), void *user)
{
    int rc = wl_columnar_lftj_work(record, 8); if (rc) return rc;
    if (depth == record->k) {
        if (record->kernel_visits == UINT64_MAX) return EOVERFLOW;
        record->kernel_visits++;
        rc = wl_columnar_lftj_tail(record, 9, record->kernel_visits);
        if (rc) return rc;
        return emit(record->row, record->compact_cols, user);
    }
    lftj_iter_t *iters = record->iters, *it = &iters[depth];
    uint32_t lo = record->ranges[(size_t)depth * 2u];
    uint32_t hi = record->ranges[(size_t)depth * 2u + 1u];
    if (!depth) record->row[0] = key;
    rc = wl_columnar_lftj_tail(record, 9, depth); if (rc) return rc;
    for (uint32_t row = lo; row < hi; row++) {
        uint32_t at = offset;
        for (uint32_t col = 0; col < it->ncols; col++) {
            rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
            if (col != it->key_col)
                record->row[at++] = it->sorted[(size_t)row * it->ncols + col];
            rc = wl_columnar_lftj_tail(record, 9,
                    (uint64_t)row * it->ncols + col + 1u); if (rc) return rc;
        }
        rc = wl_columnar_lftj_product_controlled(record, depth + 1u, at,
                key, emit, user); if (rc) return rc;
    }
    return 0;
}

int
wl_columnar_lftj_kernel_controlled(wl_columnar_lftj_record_t *record,
    int (*emit)(const int64_t *, uint32_t, void *), void *user)
{
    if (!record || !record->ready || !emit || !record->inputs
        || record->k < 2u || record->k > WL_LFTJ_MAX_K) return EINVAL;
    uint32_t k = record->k, total_cols = 1;
    uint64_t bytes = 0, scratch = 0;
    for (uint32_t i = 0; i < k; i++) {
        int rc = wl_columnar_lftj_work(record, 24); if (rc) return rc;
        const wl_lftj_input_t *input = &record->inputs[i];
        if (!input->ncols || input->key_col >= input->ncols
            || (input->nrows && !input->data)
            || total_cols > UINT32_MAX - (input->ncols - 1u)) return EINVAL;
        total_cols += input->ncols - 1u;
        if (!wl_columnar_memory_size_mul(input->nrows, input->ncols, &bytes)
            || !wl_columnar_memory_size_mul(bytes, 2u * sizeof(int64_t), &bytes)
            || !wl_columnar_memory_size_add(scratch, bytes, &scratch))
            return EOVERFLOW;
        rc = wl_columnar_lftj_tail(record, 6, i + 1u); if (rc) return rc;
    }
    if (!wl_columnar_memory_size_mul(k, sizeof(lftj_iter_t), &bytes)
        || !wl_columnar_memory_size_add(scratch, bytes, &scratch)
        || !wl_columnar_memory_size_mul(total_cols, sizeof(int64_t), &bytes)
        || !wl_columnar_memory_size_add(scratch, bytes, &scratch)
        || !wl_columnar_memory_size_mul(k, 2u * sizeof(uint32_t), &bytes)
        || !wl_columnar_memory_size_add(scratch, bytes, &scratch))
        return EOVERFLOW;
    int rc = wl_columnar_lftj_reserve(record, &record->inner, scratch);
    if (rc) return rc;
    rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
#ifdef WL_TEST_LFTJ_ADMISSION_HOOKS
    if (lftj_test_hook_state.fail_iters_pending) {
        lftj_test_hook_state.fail_iters_pending = false;
        lftj_test_hook_state.fail_iters_consumed = true;
        record->iters = NULL;
    } else
#endif
    record->iters = malloc((size_t)k * sizeof(lftj_iter_t));
    if (!record->iters) return ENOMEM;
    rc = wl_columnar_lftj_tail(record, 6, 0); if (rc) return rc;
    for (size_t at = 0; at < (size_t)k * sizeof(lftj_iter_t);) {
        size_t n = (size_t)k * sizeof(lftj_iter_t) - at;
        if (n > 128u) n = 128u;
        rc = wl_columnar_lftj_work(record, n); if (rc) return rc;
        memset((char *)record->iters + at, 0, n); at += n;
        rc = wl_columnar_lftj_tail(record, 6, at); if (rc) return rc;
    }
    rc = wl_columnar_lftj_commit(record, &record->inner, record->iters);
    if (rc) return rc;
    lftj_iter_t *iters = record->iters;
    for (uint32_t i = 0; i < k; i++) {
        const wl_lftj_input_t *input = &record->inputs[i];
        rc = wl_columnar_lftj_work(record, 16); if (rc) return rc;
        lftj_iter_t *it = &iters[i];
        it->nrows = input->nrows; it->ncols = input->ncols;
        it->key_col = input->key_col; it->key_type = record->key_type;
        it->pos = 0; record->iter_count = i + 1u;
        rc = wl_columnar_lftj_tail(record, 6, i + 1u); if (rc) return rc;
        if (record->key_type == WIRELOG_TYPE_FLOAT)
            for (uint32_t row = 0; row < input->nrows; row++) {
                rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
                if (!wl_columnar_float_bits_valid(input->data[
                        (size_t)row * input->ncols +
                        input->key_col])) return EINVAL;
                rc = wl_columnar_lftj_tail(record, 6, row + 1u);
                if (rc) return rc;
            }
        if (!input->nrows) continue;
        uint64_t cells;
        if (!wl_columnar_memory_size_mul(input->nrows, input->ncols, &cells)
            || !wl_columnar_memory_size_mul(cells, sizeof(int64_t), &bytes)
            || bytes > SIZE_MAX) return EOVERFLOW;
        rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
        it->sorted = malloc((size_t)bytes);
        if (!it->sorted) return ENOMEM;
        rc = wl_columnar_lftj_tail(record, 6, 0); if (rc) return rc;
        for (uint64_t cell = 0; cell < cells; cell++) {
            rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
            it->sorted[cell] = input->data[cell];
            rc = wl_columnar_lftj_tail(record, 6, cell + 1u); if (rc) return rc;
        }
        rc = wl_columnar_relation_radix_rows_controlled(it->sorted,
                it->nrows, it->ncols, it->key_col, it->key_type, record);
        if (rc) return rc;
    }
    /* Raw sorts every input before taking this early empty-input exit. */
    for (uint32_t i = 0; i < k; i++) {
        rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
        bool empty = lftj_at_end(&iters[i]);
        rc = wl_columnar_lftj_tail(record, 6, i + 1u); if (rc) return rc;
        if (empty) return 0;
    }
    record->compact_cols = total_cols;
    rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
    record->row = malloc((size_t)total_cols * sizeof(int64_t));
    if (!record->row) return ENOMEM;
    rc = wl_columnar_lftj_tail(record, 6, 0); if (rc) return rc;
    rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
    record->ranges = malloc((size_t)2u * k * sizeof(uint32_t));
    if (!record->ranges) return ENOMEM;
    rc = wl_columnar_lftj_tail(record, 6, 0); if (rc) return rc;
    for (;;) {
        rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
        int64_t key = lftj_key(&iters[0]);
        rc = wl_columnar_lftj_tail(record, 7, 0); if (rc) return rc;
        for (uint32_t i = 1; i < k; i++) {
            rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
            int64_t candidate = lftj_key(&iters[i]);
            if (lftj_compare_key(&iters[i], candidate,
                key) > 0) key = candidate;
            rc = wl_columnar_lftj_tail(record, 7, i + 1u); if (rc) return rc;
        }
        bool restart = false;
        for (uint32_t i = 0; i < k; i++) {
            rc = wl_columnar_lftj_seek_controlled(record, &iters[i], key);
            if (rc) return rc;
            rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
            bool end = lftj_at_end(&iters[i]);
            int comparison = end ? 0 : lftj_compare_key(&iters[i],
                    lftj_key(&iters[i]), key);
            rc = wl_columnar_lftj_tail(record, 7, i + 1u); if (rc) return rc;
            if (end) return 0;
            if (comparison > 0) {
                restart = true; break;
            }
        }
        if (restart) continue;
        for (uint32_t i = 0; i < k; i++) {
            rc = wl_columnar_lftj_range_controlled(record, &iters[i], key,
                    &record->ranges[(size_t)i * 2u],
                    &record->ranges[(size_t)i * 2u + 1u]);
            if (rc) return rc;
        }
        rc = wl_columnar_lftj_product_controlled(record, 0, 1, key, emit, user);
        if (rc) return rc;
        for (uint32_t i = 0; i < k; i++) {
            rc = wl_columnar_lftj_work(record, 1); if (rc) return rc;
            iters[i].pos = record->ranges[(size_t)i * 2u + 1u];
            bool end = lftj_at_end(&iters[i]);
            rc = wl_columnar_lftj_tail(record, 7, i + 1u); if (rc) return rc;
            if (end) return 0;
        }
    }
}
