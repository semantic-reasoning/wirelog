/*
 * columnar/mobius.c - wirelog Mobius / Z-set Weighted Operations
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Weighted join and delta Mobius formula for Z-set computation.
 * Extracted from backend/columnar_nanoarrow.c for modular compilation.
 */

#include "columnar/internal.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Mobius / Z-set Weighted JOIN                                              */
/* ======================================================================== */

/*
 * col_op_join_weighted - equi-join with multiplicity multiplication.
 *
 * Joins lhs and rhs on column index key_col (present in both).  For each
 * matching pair the output row is appended to dst and its timestamp
 * multiplicity is set to lhs_mult * rhs_mult.
 *
 * Output layout: all lhs columns followed by all rhs columns (key column
 * is duplicated; callers may project as needed).  dst->ncols is initialised
 * by this function; dst must be caller-allocated with ncols==0 on entry.
 *
 * Returns 0 on success, non-zero (ENOMEM / EINVAL) on error.
 */
int
col_op_join_weighted(const col_rel_t *lhs, const col_rel_t *rhs,
    uint32_t key_col, col_rel_t *dst)
{
    if (!lhs || !rhs || !dst)
        return EINVAL;
    if (key_col >= lhs->ncols || key_col >= rhs->ncols)
        return EINVAL;

    uint32_t ocols = lhs->ncols + rhs->ncols;
    dst->ncols = ocols;

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols > 0 ? ocols : 1));
    if (!tmp)
        return ENOMEM;

    int rc = 0;
    for (uint32_t li = 0; li < lhs->nrows && rc == 0; li++) {
        int64_t lrow_buf[COL_STACK_MAX];
        col_rel_row_copy_out(lhs, li, lrow_buf); const int64_t *lrow = lrow_buf;
        int64_t lmult = lhs->timestamps ? lhs->timestamps[li].multiplicity : 1;

        for (uint32_t ri = 0; ri < rhs->nrows && rc == 0; ri++) {
            int64_t rrow_buf[COL_STACK_MAX];
            col_rel_row_copy_out(rhs, ri, rrow_buf);
            const int64_t *rrow = rrow_buf;

            if (lrow[key_col] != rrow[key_col])
                continue;

            int64_t rmult
                = rhs->timestamps ? rhs->timestamps[ri].multiplicity : 1;

            memcpy(tmp, lrow, sizeof(int64_t) * lhs->ncols);
            memcpy(tmp + lhs->ncols, rrow, sizeof(int64_t) * rhs->ncols);

            /* Grow dst manually to keep data and timestamps in sync. */
            if (dst->nrows >= dst->capacity) {
                uint32_t new_cap = dst->capacity ? dst->capacity * 2 : 16;
                if (dst->columns) {
                    if (col_columns_realloc(dst->columns, ocols,
                        new_cap) != 0) {
                        rc = ENOMEM;
                        break;
                    }
                } else {
                    dst->columns = col_columns_alloc(ocols, new_cap);
                    if (!dst->columns) {
                        rc = ENOMEM;
                        break;
                    }
                }
                col_delta_timestamp_t *nt = (col_delta_timestamp_t *)realloc(
                    dst->timestamps,
                    (size_t)new_cap * sizeof(col_delta_timestamp_t));
                if (!nt) {
                    rc = ENOMEM;
                    break;
                }
                dst->timestamps = nt;
                dst->capacity = new_cap;
            }
            col_rel_row_copy_in(dst, dst->nrows, tmp);
            memset(&dst->timestamps[dst->nrows], 0,
                sizeof(col_delta_timestamp_t));
            dst->timestamps[dst->nrows].multiplicity = lmult * rmult;
            dst->nrows++;
        }
    }

    free(tmp);
    return rc;
}

/* ======================================================================== */
/* Mobius / Z-set Delta Formula                                             */
/* ======================================================================== */

/*
 * col_compute_delta_mobius:
 * Compute the Mobius delta between prev_collection and curr_collection.
 *
 * For each unique key (column 0) in the union of both relations:
 *   - key only in curr:  delta_mult = curr_mult
 *   - key only in prev:  delta_mult = -prev_mult
 *   - key in both:       delta_mult = curr_mult - prev_mult (skipped if 0)
 *
 * Both input relations must have timestamps != NULL.
 * out_delta must be caller-allocated, empty (nrows==0) on entry.
 *
 * Returns 0 on success, EINVAL on bad arguments, ENOMEM on allocation failure.
 */
int
col_compute_delta_mobius(const col_rel_t *prev_collection,
    const col_rel_t *curr_collection, col_rel_t *out_delta)
{
    if (!prev_collection || !curr_collection || !out_delta)
        return EINVAL;
    if (prev_collection->ncols == 0 || curr_collection->ncols == 0)
        return EINVAL;
    if (prev_collection->ncols != curr_collection->ncols)
        return EINVAL;

    uint32_t ncols = prev_collection->ncols;
    out_delta->ncols = ncols;

    /* Helper lambda (via inline block) to append a row+mult to out_delta. */
#define DELTA_APPEND(row_ptr, mult_val)                                       \
        do {                                                                      \
            if (out_delta->nrows >= out_delta->capacity) {                        \
                uint32_t new_cap                                                  \
                    = out_delta->capacity ? out_delta->capacity * 2 : 16;         \
                if (out_delta->columns) {                                         \
                    if (col_columns_realloc(out_delta->columns, ncols,             \
                        new_cap) != 0)                                            \
                    return ENOMEM;                                            \
                } else {                                                          \
                    out_delta->columns = col_columns_alloc(ncols, new_cap);        \
                    if (!out_delta->columns)                                       \
                    return ENOMEM;                                            \
                }                                                                 \
                col_delta_timestamp_t *nt = (col_delta_timestamp_t *)realloc(     \
                    out_delta->timestamps,                                        \
                    (size_t)new_cap * sizeof(col_delta_timestamp_t));             \
                if (!nt)                                                          \
                return ENOMEM;                                                \
                out_delta->timestamps = nt;                                       \
                out_delta->capacity = new_cap;                                    \
            }                                                                     \
            col_rel_row_copy_in(out_delta, out_delta->nrows, (row_ptr));          \
            col_delta_timestamp_t ts_;                                            \
            memset(&ts_, 0, sizeof(ts_));                                         \
            ts_.multiplicity = (mult_val);                                        \
            out_delta->timestamps[out_delta->nrows] = ts_;                        \
            out_delta->nrows++;                                                   \
        } while (0)

    /* Pass 1: iterate over curr; for each key look up in prev. */
    for (uint32_t ci = 0; ci < curr_collection->nrows; ci++) {
        int64_t crow_buf[COL_STACK_MAX];
        col_rel_row_copy_out(curr_collection, ci, crow_buf);
        const int64_t *crow = crow_buf;
        int64_t cmult = curr_collection->timestamps
                            ? curr_collection->timestamps[ci].multiplicity
                            : 1;

        /* Search prev for matching key (column 0). */
        int64_t pmult = 0;
        bool found_in_prev = false;
        for (uint32_t pi = 0; pi < prev_collection->nrows; pi++) {
            int64_t prow_buf[COL_STACK_MAX];
            col_rel_row_copy_out(prev_collection, pi, prow_buf);
            const int64_t *prow = prow_buf;
            if (prow[0] == crow[0]) {
                pmult = prev_collection->timestamps
                            ? prev_collection->timestamps[pi].multiplicity
                            : 1;
                found_in_prev = true;
                break;
            }
        }

        int64_t delta_mult = found_in_prev ? (cmult - pmult) : cmult;
        if (delta_mult != 0) {
            DELTA_APPEND(crow, delta_mult);
        }
    }

    /* Pass 2: iterate over prev; emit -prev_mult for keys absent in curr. */
    for (uint32_t pi = 0; pi < prev_collection->nrows; pi++) {
        int64_t prow_buf[COL_STACK_MAX];
        col_rel_row_copy_out(prev_collection, pi, prow_buf);
        const int64_t *prow = prow_buf;
        int64_t pmult = prev_collection->timestamps
                            ? prev_collection->timestamps[pi].multiplicity
                            : 1;

        bool found_in_curr = false;
        for (uint32_t ci = 0; ci < curr_collection->nrows; ci++) {
            int64_t crow_buf[COL_STACK_MAX];
            col_rel_row_copy_out(curr_collection, ci, crow_buf);
            const int64_t *crow = crow_buf;
            if (crow[0] == prow[0]) {
                found_in_curr = true;
                break;
            }
        }

        if (!found_in_curr) {
            int64_t delta_mult = -pmult;
            if (delta_mult != 0) {
                DELTA_APPEND(prow, delta_mult);
            }
        }
    }

#undef DELTA_APPEND

    return 0;
}
