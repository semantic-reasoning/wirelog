/*
 * columnar/eval_delta.c - stratum delta lifecycle
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#define _GNU_SOURCE

#include "columnar/internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static bool
col_row_in_sorted(const int64_t *sorted_data, uint32_t nrows, uint32_t ncols,
    const int64_t *row)
{
    if (!sorted_data || nrows == 0 || ncols == 0)
        return false;
    uint32_t lo = 0, hi = nrows;
    size_t row_bytes = sizeof(int64_t) * ncols;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        int cmp = memcmp(sorted_data + (size_t)mid * ncols, row, row_bytes);
        if (cmp == 0)
            return true;
        if (cmp < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return false;
}

/*
 * col_idb_consolidate: Sort + dedup one IDB relation in-place.
 *
 * Reuses the eval stack + col_op_consolidate operator so sort order
 * is consistent with the rest of the evaluation pipeline.
 */
static int
col_idb_consolidate(col_rel_t *r, wl_col_session_t *sess)
{
    eval_stack_t stk;
    eval_stack_init(&stk);
    int rc = eval_stack_push(&stk, r, false); /* borrowed */
    if (rc != 0)
        return rc;
    col_op_consolidate(&stk, sess);
    if (stk.top > 0) {
        eval_entry_t ce = eval_stack_pop(&stk);
        if (ce.owned && ce.rel != r) {
            col_columns_free(r->columns, r->ncols);
            r->columns = ce.rel->columns;
            r->nrows = ce.rel->nrows;
            r->capacity = ce.rel->capacity;
            ce.rel->columns = NULL;
            col_rel_destroy(ce.rel);
        }
    }
    return 0;
}

/*
 * col_stratum_step_with_delta: Evaluate one stratum and fire delta callbacks.
 *
 * Phase 2A algorithm (full re-eval + set diff):
 *   1. Snapshot each IDB relation's current sorted rows (prev state)
 *   2. Run col_eval_stratum (appends newly derived rows)
 *   3. Consolidate each IDB relation (sort + dedup)
 *   4. Fire delta_cb(+1) for each row in new state not found in prev state
 *   5. Free snapshots
 *
 * TODO(#809): Replace step 2 with semi-naive ΔR propagation.
 */

/*
 * col_stratum_step_retraction_nonrecursive: Retraction delta propagation
 *
 * (Issue #158) Semi-naive delta retraction for non-recursive strata.
 * Evaluates the stratum in retraction mode, using $r$<name> delta relations
 * to propagate only retractions (O(|Δ|)) instead of full re-evaluation.
 *
 * Algorithm:
 *   1. Set retraction_seeded = true
 *   2. Evaluate stratum (produces rows to retract in result buffer)
 *   3. For each IDB relation:
 *      - Find retraction candidates (rows produced by eval)
 *      - Remove those rows in-place (compact)
 *      - Fire delta_cb with diff=-1 for each removed row
 *   4. Reset retraction_seeded = false
 *
 * Falls back to full re-eval (col_stratum_step_with_delta) for recursive strata.
 *
 * Retained but not yet wired: col_stratum_step_with_delta() still uses full
 * re-evaluation for retraction pending Issue #158 (see the note at its head).
 * internal.h and tests/test_pointer_swap.c both name this function as the
 * specification for the pointer-swap contract, so it is kept rather than
 * deleted; UNUSED suppresses the warning until it is wired up.
 */
static int UNUSED
col_stratum_step_retraction_nonrecursive(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess,
    uint32_t stratum_idx)
{
    if (sp->is_recursive) {
        /* Recursive strata fall back to full re-eval */
        return col_stratum_step_with_delta(sp, sess, stratum_idx);
    }

    uint32_t rc_cnt = sp->relation_count;

    /* retract_data[ri] will hold the stolen post-consolidation pointer;
     * no malloc/memcpy needed — ownership is transferred from r->data. */
    int64_t **retract_data = (int64_t **)calloc(rc_cnt, sizeof(int64_t *));
    uint32_t *retract_nrows = (uint32_t *)calloc(rc_cnt, sizeof(uint32_t));
    if (!retract_data || !retract_nrows) {
        free(retract_data);
        free(retract_nrows);
        return ENOMEM;
    }

    /* Step 0: Pointer-swap original data into retract_backup fields (O(1))
     * and clear relation for retraction evaluation. */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->ncols == 0)
            continue;
        r->retract_backup_columns = r->columns;
        r->retract_backup_nrows = r->nrows;
        r->retract_backup_capacity = r->capacity;
        r->retract_backup_sorted_nrows = r->sorted_nrows;
        r->retract_backup_run_count = r->run_count;
        memcpy(r->retract_backup_run_ends, r->run_ends,
            sizeof(r->run_ends));
        r->columns = NULL;
        r->capacity = 0;
        r->nrows = 0;
        r->sorted_nrows = 0;
        r->run_count = 0;
    }

    /* Step 1: Enable retraction-seeded mode and evaluate stratum.
     * First pass (left): VARIABLE loads $r$, JOIN uses full right.
     * Issue #472: Second pass (right): VARIABLE loads full, JOIN uses $r$
     * on the right side.  This is needed for self-join rules where the
     * retracted EDB appears on both sides of a JOIN. */
    sess->retraction_seeded = true;
    sess->retraction_right_pass = false;
    int rc = col_eval_stratum(sp, sess, stratum_idx);

    /* Issue #472: Check if a second (right) pass is needed.
     * Scan relation plans for JOIN/SEMIJOIN ops whose right_relation has
     * a $r$ retraction delta.  If found, run a second pass so that
     * full(left) x $r$(right) produces additional retraction candidates. */
    if (rc == 0) {
        bool need_right_pass = false;
        for (uint32_t ri = 0; ri < sp->relation_count && !need_right_pass;
            ri++) {
            const wl_plan_relation_t *rp = &sp->relations[ri];
            for (uint32_t oi = 0; oi < rp->op_count; oi++) {
                const wl_plan_op_t *pop = &rp->ops[oi];
                if ((pop->op == WL_PLAN_OP_JOIN
                    || pop->op == WL_PLAN_OP_SEMIJOIN)
                    && pop->right_relation) {
                    char rname[256];
                    if (retraction_rel_name(pop->right_relation, rname,
                        sizeof(rname)) == 0) {
                        col_rel_t *rd = session_find_rel(sess, rname);
                        if (rd && rd->nrows > 0) {
                            need_right_pass = true;
                            break;
                        }
                    }
                }
            }
        }
        if (need_right_pass) {
            sess->retraction_right_pass = true;
            rc = col_eval_stratum(sp, sess, stratum_idx);
            sess->retraction_right_pass = false;
        }
    }
    sess->retraction_seeded = false;
    if (rc != 0) {
        /* Restore all backup pointers; free any eval-allocated buffers */
        for (uint32_t i = 0; i < rc_cnt; i++) {
            col_rel_t *r = session_find_rel(sess, sp->relations[i].name);
            if (!r || r->ncols == 0)
                continue;
            col_columns_free(r->columns, r->ncols);
            r->columns = r->retract_backup_columns;
            r->nrows = r->retract_backup_nrows;
            r->capacity = r->retract_backup_capacity;
            r->sorted_nrows = r->retract_backup_sorted_nrows;
            r->run_count = r->retract_backup_run_count;
            memcpy(r->run_ends, r->retract_backup_run_ends,
                sizeof(r->run_ends));
            r->retract_backup_columns = NULL;
            r->retract_backup_nrows = 0;
            r->retract_backup_capacity = 0;
            r->retract_backup_sorted_nrows = 0;
            r->retract_backup_run_count = 0;
        }
        free(retract_data);
        free(retract_nrows);
        return rc;
    }

    /* Steps 2+3: Per-relation: consolidate retraction candidates, steal the
     * buffer pointer (no malloc/memcpy), then swap back the original (O(1)). */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->ncols == 0)
            continue;

        if (r->nrows > 0) {
            rc = col_idb_consolidate(r, sess);
            if (rc != 0) {
                /* Restore any relations still holding backup state */
                for (uint32_t i = 0; i < rc_cnt; i++) {
                    col_rel_t *r2
                        = session_find_rel(sess, sp->relations[i].name);
                    if (!r2 || r2->ncols == 0)
                        continue;
                    if (r2->retract_backup_columns != NULL) {
                        col_columns_free(r2->columns, r2->ncols);
                        r2->columns = r2->retract_backup_columns;
                        r2->nrows = r2->retract_backup_nrows;
                        r2->capacity = r2->retract_backup_capacity;
                        r2->sorted_nrows = r2->retract_backup_sorted_nrows;
                        r2->run_count = r2->retract_backup_run_count;
                        memcpy(r2->run_ends, r2->retract_backup_run_ends,
                            sizeof(r2->run_ends));
                        r2->retract_backup_columns = NULL;
                        r2->retract_backup_nrows = 0;
                        r2->retract_backup_capacity = 0;
                        r2->retract_backup_sorted_nrows = 0;
                        r2->retract_backup_run_count = 0;
                    }
                    free(retract_data[i]);
                }
                free(retract_data);
                free(retract_nrows);
                return rc;
            }
            /* Steal: gather into flat buffer for col_row_in_sorted */
            uint32_t nc = r->ncols;
            int64_t *flat = (int64_t *)malloc(
                (size_t)r->nrows * nc * sizeof(int64_t));
            if (flat) {
                for (uint32_t row = 0; row < r->nrows; row++)
                    col_rel_row_copy_out(r, row, flat + (size_t)row * nc);
            }
            retract_data[ri] = flat;
            retract_nrows[ri] = flat ? r->nrows : 0;
            col_columns_free(r->columns, r->ncols);
            r->columns = NULL;
            r->capacity = 0;
            r->nrows = 0;
        }

        /* Free any eval-allocated buffer not stolen above (nrows==0 case) */
        col_columns_free(r->columns, r->ncols);
        r->columns = NULL;

        /* Swap back original data (O(1)) */
        r->columns = r->retract_backup_columns;
        r->nrows = r->retract_backup_nrows;
        r->capacity = r->retract_backup_capacity;
        r->sorted_nrows = r->retract_backup_sorted_nrows;
        r->run_count = r->retract_backup_run_count;
        memcpy(r->run_ends, r->retract_backup_run_ends,
            sizeof(r->run_ends));
        r->retract_backup_columns = NULL;
        r->retract_backup_nrows = 0;
        r->retract_backup_capacity = 0;
        r->retract_backup_sorted_nrows = 0;
        r->retract_backup_run_count = 0;
    }

    /* Step 4: Remove retracted rows and fire delta callbacks */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->ncols == 0 || retract_nrows[ri] == 0)
            continue;

        uint32_t ncols = r->ncols;
        int64_t row_stack[COL_STACK_MAX];
        int64_t *src_buf = row_stack;
        if (ncols > COL_STACK_MAX) {
            src_buf = (int64_t *)malloc(ncols * sizeof(int64_t));
            if (!src_buf) {
                for (uint32_t i = 0; i < rc_cnt; i++)
                    free(retract_data[i]);
                free(retract_data);
                free(retract_nrows);
                return ENOMEM;
            }
        }

        for (uint32_t del_idx = 0; del_idx < retract_nrows[ri]; del_idx++) {
            const int64_t *to_remove
                = retract_data[ri] + (size_t)del_idx * ncols;

            /* Find and remove this row in-place */
            uint32_t out_r = 0;
            bool found = false;
            for (uint32_t src_idx = 0; src_idx < r->nrows; src_idx++) {
                col_rel_row_copy_out(r, src_idx, src_buf);
                if (memcmp(src_buf, to_remove, sizeof(int64_t) * ncols)
                    == 0) {
                    /* Found matching row; skip it (removal) */
                    found = true;
                    /* Copy remaining rows forward */
                    for (uint32_t rest = src_idx + 1; rest < r->nrows;
                        rest++) {
                        col_rel_row_move(r, out_r, rest);
                        out_r++;
                    }
                    r->nrows = out_r;
                    break;
                } else {
                    /* Keep this row */
                    if (out_r != src_idx)
                        col_rel_row_copy_in(r, out_r, src_buf);
                    out_r++;
                }
            }

            /* Fire delta callback if row was actually removed */
            if (found && sess->delta_cb) {
                sess->delta_cb(r->name, to_remove, ncols, -1,
                    sess->delta_data);
            }
        }
        if (src_buf != row_stack)
            free(src_buf);
    }

    /* Cleanup: free stolen retraction buffers */
    for (uint32_t i = 0; i < rc_cnt; i++)
        free(retract_data[i]);
    free(retract_data);
    free(retract_nrows);
    return 0;
}

int
col_stratum_step_with_delta(const wl_plan_stratum_t *sp, wl_col_session_t *sess,
    uint32_t stratum_idx)
{
    /* Issue #158: For now, use full re-evaluation for retraction.
     * When retraction_seeded is set, the standard delta callback logic
     * compares prev state with new state (recomputed from affected input),
     * and diff=-1 callbacks are fired for removed tuples.
     * Future optimization: wire up col_stratum_step_retraction_nonrecursive
     * (implemented above, not yet called) for direct delta-only propagation
     * of retractions. */

    uint32_t rc_cnt = sp->relation_count;

    /* Allocate snapshot arrays */
    int64_t **prev_data = (int64_t **)calloc(rc_cnt, sizeof(int64_t *));
    uint32_t *prev_nrows = (uint32_t *)calloc(rc_cnt, sizeof(uint32_t));
    uint32_t *prev_ncols = (uint32_t *)calloc(rc_cnt, sizeof(uint32_t));
    if (!prev_data || !prev_nrows || !prev_ncols) {
        free(prev_data);
        free(prev_nrows);
        free(prev_ncols);
        return ENOMEM;
    }

    /* Step 1: pointer-swap snapshot for each IDB relation (issue #300).
     * Instead of malloc+memcpy, save the live data pointer and give the
     * relation a NULL buffer.  col_eval_stratum will allocate a fresh
     * buffer via append, so the old pointer stays valid for comparison. */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->ncols == 0)
            continue;
        prev_ncols[ri] = r->ncols;
        if (r->nrows > 0) {
            /* Gather into flat buffer for col_row_in_sorted */
            uint32_t nc = r->ncols;
            int64_t *flat = (int64_t *)malloc(
                (size_t)r->nrows * nc * sizeof(int64_t));
            if (flat) {
                for (uint32_t row = 0; row < r->nrows; row++)
                    col_rel_row_copy_out(r, row, flat + (size_t)row * nc);
            }
            prev_data[ri] = flat;
            prev_nrows[ri] = flat ? r->nrows : 0;
            /* Free and detach columns; eval will allocate fresh */
            col_columns_free(r->columns, r->ncols);
            r->columns = NULL;
            r->nrows = 0;
            r->capacity = 0;
            r->sorted_nrows = 0;
        } else {
            prev_nrows[ri] = 0;
        }
    }

    /* Step 2: evaluate stratum (appends new rows to IDB relations).
     * Issue #472: When retraction is in progress, temporarily clear
     * retraction_seeded and diff_operators_active during full re-eval.
     * The full re-eval + set-diff path must evaluate from clean EDB state
     * (with the removed row already gone), not from $r$ retraction deltas.
     * Only modify these flags when retraction_seeded was actually set;
     * otherwise, leave the normal evaluation path untouched to avoid
     * interfering with non-retraction steps (e.g., DOOP multi-worker). */
    bool saved_retraction_seeded = sess->retraction_seeded;
    bool saved_diff_operators_active = sess->diff_operators_active;
    if (saved_retraction_seeded) {
        sess->retraction_seeded = false;
        sess->retraction_right_pass = false;
        sess->diff_operators_active = false;
    }
    int rc = col_eval_stratum(sp, sess, stratum_idx);
    sess->retraction_seeded = saved_retraction_seeded;
    sess->diff_operators_active = saved_diff_operators_active;
    if (rc != 0)
        goto cleanup;

    /* Steps 3-4: consolidate each IDB relation, fire callbacks for new rows */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r)
            continue;

        /* Consolidate: sort + dedup so binary search is valid */
        rc = col_idb_consolidate(r, sess);
        if (rc != 0)
            goto cleanup;

        uint32_t ncols = r->ncols;

        /* Gather current state into flat buffer for col_row_in_sorted */
        int64_t *cur_flat = NULL;
        if (r->nrows > 0 && ncols > 0) {
            cur_flat = (int64_t *)malloc(
                (size_t)r->nrows * ncols * sizeof(int64_t));
            if (cur_flat) {
                for (uint32_t row = 0; row < r->nrows; row++)
                    col_rel_row_copy_out(r, row,
                        cur_flat + (size_t)row * ncols);
            }
        }

        /* Fire delta_cb(+1) for rows not present in prev sorted state */
        if (r->nrows > 0 && cur_flat) {
            for (uint32_t row = 0; row < r->nrows; row++) {
                const int64_t *rowp = cur_flat + (size_t)row * ncols;
                if (!col_row_in_sorted(prev_data[ri], prev_nrows[ri], ncols,
                    rowp)) {
                    sess->delta_cb(r->name, rowp, ncols, +1, sess->delta_data);
                }
            }
        }

        /* Fire delta_cb(-1) for rows present in prev sorted state but not in new
         */
        if (prev_nrows[ri] > 0) {
            for (uint32_t row = 0; row < prev_nrows[ri]; row++) {
                const int64_t *rowp
                    = prev_data[ri] + (size_t)row * prev_ncols[ri];
                if (!col_row_in_sorted(cur_flat, r->nrows, prev_ncols[ri],
                    rowp)) {
                    sess->delta_cb(r->name, rowp, prev_ncols[ri], -1,
                        sess->delta_data);
                }
            }
        }
        free(cur_flat);
    }

cleanup:
    for (uint32_t i = 0; i < rc_cnt; i++) {
        if (rc != 0 && prev_data[i]) {
            /* Error path: restore from flat snapshot into column-major */
            col_rel_t *r = session_find_rel(sess, sp->relations[i].name);
            if (r && !r->columns) {
                uint32_t nc = prev_ncols[i];
                uint32_t nr = prev_nrows[i];
                r->columns = col_columns_alloc(nc, nr > 0 ? nr : 1);
                if (r->columns) {
                    for (uint32_t row = 0; row < nr; row++) {
                        const int64_t *rowp
                            = prev_data[i] + (size_t)row * nc;
                        col_rel_row_copy_in(r, row, rowp);
                    }
                    r->nrows = nr;
                    r->capacity = nr > 0 ? nr : 1;
                    r->sorted_nrows = nr;
                    r->run_count = 1;
                    r->run_ends[0] = nr;
                }
                free(prev_data[i]);
                prev_data[i] = NULL;
            }
        }
        free(prev_data[i]);
    }
    free(prev_data);
    free(prev_nrows);
    free(prev_ncols);
    return rc;
}
