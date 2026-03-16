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

#define _GNU_SOURCE /* Required for qsort_r on glibc */

#include "columnar/lftj.h"
#include "columnar/internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Platform-specific qsort_r comparator (sort by single key column)        */
/* ======================================================================== */

/*
 * lftj_row_cmp: compare two rows by a single key column.
 * Context is a uint32_t* pointing to the key column index.
 * Platform signatures match QSORT_R_CALL macro from internal.h.
 */
#ifdef __GLIBC__
static int
lftj_row_cmp(const void *a, const void *b, void *ctx)
{
    const uint32_t key_col = *(const uint32_t *)ctx;
    const int64_t ka = ((const int64_t *)a)[key_col];
    const int64_t kb = ((const int64_t *)b)[key_col];
    return (ka > kb) - (ka < kb);
}
#elif defined(_MSC_VER)
static int __cdecl lftj_row_cmp(void *ctx, const void *a, const void *b)
{
    const uint32_t key_col = *(const uint32_t *)ctx;
    const int64_t ka = ((const int64_t *)a)[key_col];
    const int64_t kb = ((const int64_t *)b)[key_col];
    return (ka > kb) - (ka < kb);
}
#else
static int
lftj_row_cmp(void *ctx, const void *a, const void *b)
{
    const uint32_t key_col = *(const uint32_t *)ctx;
    const int64_t ka = ((const int64_t *)a)[key_col];
    const int64_t kb = ((const int64_t *)b)[key_col];
    return (ka > kb) - (ka < kb);
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
} lftj_iter_t;

/* Current key at iterator position (undefined when at-end). */
static inline int64_t
lftj_key(const lftj_iter_t *it)
{
    return it->sorted[(size_t)it->pos * it->ncols + it->key_col];
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
        if (midkey < target)
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
        if (midkey <= key)
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
lftj_iter_init(lftj_iter_t *it, const wl_lftj_input_t *inp)
{
    it->nrows = inp->nrows;
    it->ncols = inp->ncols;
    it->key_col = inp->key_col;
    it->pos = 0;
    it->sorted = NULL;

    if (inp->nrows == 0)
        return 0;

    size_t bytes = (size_t)inp->nrows * inp->ncols * sizeof(int64_t);
    it->sorted = (int64_t *)malloc(bytes);
    if (!it->sorted)
        return ENOMEM;

    memcpy(it->sorted, inp->data, bytes);

    uint32_t key_col = inp->key_col;
    QSORT_R_CALL(it->sorted, inp->nrows, inp->ncols * sizeof(int64_t), &key_col,
                 lftj_row_cmp);
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

int
wl_lftj_join(const wl_lftj_input_t *inputs, uint32_t k, wl_lftj_result_fn cb,
             void *user)
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

    /* Allocate and sort per-relation iterators. */
    lftj_iter_t *iters = (lftj_iter_t *)calloc(k, sizeof(lftj_iter_t));
    if (!iters)
        return ENOMEM;

    int rc = 0;
    for (uint32_t i = 0; i < k; i++) {
        rc = lftj_iter_init(&iters[i], &inputs[i]);
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
            if (ki > max_key)
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
                if (cur > max_key) {
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
            int64_t next_max = INT64_MIN;
            bool any_end = false;
            for (uint32_t j = 0; j < k; j++) {
                iters[j].pos = ranges[j * 2u + 1u]; /* skip past range */
                if (lftj_at_end(&iters[j])) {
                    any_end = true;
                    break;
                }
                int64_t nk = lftj_key(&iters[j]);
                if (nk > next_max)
                    next_max = nk;
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
    for (uint32_t i = 0; i < k; i++)
        lftj_iter_free(&iters[i]);
    free(iters);
    return rc;
}
