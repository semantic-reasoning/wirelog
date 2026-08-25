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
 * by this function; dst must be caller-allocated and pristine on entry
 * (ncols==0, nrows==0, capacity==0, columns==NULL, timestamps==NULL), and
 * is rejected with EINVAL otherwise (Issues #1076, #1182).
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
    /* Issue #1076: dst->ncols is overwritten below, and the append path
     * indexes dst->columns[0..ocols) at dst->nrows.  A dst that already
     * carries rows, or whose columns array is narrower than ocols,
     * overflows both (ASan: heap-buffer-overflow WRITE at
     * internal.h:414).  ncols==0 alone does not bound nrows, capacity or
     * columns, so require the whole relation pristine. */
    if (dst->ncols != 0 || dst->nrows != 0 || dst->capacity != 0 ||
        dst->columns != NULL || dst->timestamps != NULL)
        return EINVAL;

    uint32_t ocols = lhs->ncols + rhs->ncols;
    dst->ncols = ocols;

    int64_t *tmp = (int64_t *)malloc(sizeof(int64_t) * (ocols > 0 ? ocols : 1));
    if (!tmp)
        return ENOMEM;

    /* Row scratch for each side (#1000).  col_rel_row_copy_out() writes
     * ->ncols values, so a bare int64_t[COL_STACK_MAX] overflows for
     * relations wider than 32 columns.  Both buffers are hoisted out of the
     * loops: the right-hand buffer is used by the inner loop, so allocating
     * it there would malloc once per (li, ri) pair. */
    col_row_buf_t lrb, rrb;
    int64_t *lptr = col_row_buf_init(&lrb, lhs->ncols);
    int64_t *rptr = col_row_buf_init(&rrb, rhs->ncols);
    if (!lptr || !rptr) {
        col_row_buf_release(&lrb);
        col_row_buf_release(&rrb);
        free(tmp);
        return ENOMEM;
    }

    int rc = 0;
    for (uint32_t li = 0; li < lhs->nrows && rc == 0; li++) {
        col_rel_row_copy_out(lhs, li, lptr);
        const int64_t *lrow = lptr;
        int64_t lmult = lhs->timestamps ? lhs->timestamps[li].multiplicity : 1;

        for (uint32_t ri = 0; ri < rhs->nrows && rc == 0; ri++) {
            col_rel_row_copy_out(rhs, ri, rptr);
            const int64_t *rrow = rptr;

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

    col_row_buf_release(&lrb);
    col_row_buf_release(&rrb);
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
 * out_delta must be caller-allocated and carry no data on entry
 * (nrows==0, capacity==0, columns==NULL, timestamps==NULL), and its ncols
 * must be either 0 or already equal to the input arity, because this function
 * overwrites ncols without resizing the arrays it also bounds.  Rejected with
 * EINVAL otherwise (Issues #1076, #1182).
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
    /* Issue #1076: see col_op_join_weighted.  "empty (nrows==0)" alone is
     * not sufficient -- an out_delta with nrows==0 but a pre-allocated
     * columns array narrower than ncols still overruns it on the first
     * append.  ncols itself is overwritten below, so it is deliberately
     * not required to be zero: every caller passes a typed relation. */
    if (out_delta->nrows != 0 || out_delta->capacity != 0 ||
        out_delta->columns != NULL || out_delta->timestamps != NULL)
        return EINVAL;
    /* Issue #1076: ncols is overwritten below, and it is also the length
     * bound for col_names, merge_columns and retract_backup_columns, which
     * this function does not touch.  Silently widening it would desync
     * those from what the caller allocated and corrupt the heap in
     * col_rel_free_contents().  A typed out_delta must therefore already
     * match the input arity; ncols == 0 (untyped) stays allowed. */
    if (out_delta->ncols != 0 &&
        out_delta->ncols != prev_collection->ncols)
        return EINVAL;

    uint32_t ncols = prev_collection->ncols;
    out_delta->ncols = ncols;

    /* Row scratch for each collection (#1000).  col_rel_row_copy_out()
     * writes ->ncols values, so a bare int64_t[COL_STACK_MAX] overflows past
     * 32 columns.  Both buffers are hoisted to function scope: each pass
     * scans one collection in its inner loop, so a per-iteration allocation
     * would malloc once per (ci, pi) pair.  Each buffer always holds a row
     * of the same collection, in both passes. */
    col_row_buf_t crb, prb;
    int64_t *cptr = col_row_buf_init(&crb, curr_collection->ncols);
    int64_t *pptr = col_row_buf_init(&prb, prev_collection->ncols);
    if (!cptr || !pptr) {
        col_row_buf_release(&crb);
        col_row_buf_release(&prb);
        return ENOMEM;
    }

    /* Helper lambda (via inline block) to append a row+mult to out_delta. */
#define DELTA_FAIL()                                                          \
        do {                                                                      \
            col_row_buf_release(&crb);                                            \
            col_row_buf_release(&prb);                                            \
            return ENOMEM;                                                        \
        } while (0)

#define DELTA_APPEND(row_ptr, mult_val)                                       \
        do {                                                                      \
            if (out_delta->nrows >= out_delta->capacity) {                        \
                uint32_t new_cap                                                  \
                    = out_delta->capacity ? out_delta->capacity * 2 : 16;         \
                if (out_delta->columns) {                                         \
                    if (col_columns_realloc(out_delta->columns, ncols,             \
                        new_cap) != 0)                                            \
                    DELTA_FAIL();                                             \
                } else {                                                          \
                    out_delta->columns = col_columns_alloc(ncols, new_cap);        \
                    if (!out_delta->columns)                                       \
                    DELTA_FAIL();                                             \
                }                                                                 \
                col_delta_timestamp_t *nt = (col_delta_timestamp_t *)realloc(     \
                    out_delta->timestamps,                                        \
                    (size_t)new_cap * sizeof(col_delta_timestamp_t));             \
                if (!nt)                                                          \
                DELTA_FAIL();                                                 \
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
        col_rel_row_copy_out(curr_collection, ci, cptr);
        const int64_t *crow = cptr;
        int64_t cmult = curr_collection->timestamps
                            ? curr_collection->timestamps[ci].multiplicity
                            : 1;

        /* Search prev for matching key (column 0). */
        int64_t pmult = 0;
        bool found_in_prev = false;
        for (uint32_t pi = 0; pi < prev_collection->nrows; pi++) {
            col_rel_row_copy_out(prev_collection, pi, pptr);
            const int64_t *prow = pptr;
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
        col_rel_row_copy_out(prev_collection, pi, pptr);
        const int64_t *prow = pptr;
        int64_t pmult = prev_collection->timestamps
                            ? prev_collection->timestamps[pi].multiplicity
                            : 1;

        bool found_in_curr = false;
        for (uint32_t ci = 0; ci < curr_collection->nrows; ci++) {
            col_rel_row_copy_out(curr_collection, ci, cptr);
            const int64_t *crow = cptr;
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
#undef DELTA_FAIL

    col_row_buf_release(&crb);
    col_row_buf_release(&prb);
    return 0;
}
