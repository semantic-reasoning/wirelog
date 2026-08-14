/*
 * columnar/diff.c - wirelog Differential Consolidation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#define _GNU_SOURCE

#include "columnar/internal.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

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
