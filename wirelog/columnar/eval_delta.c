/*
 * columnar/eval_delta.c - stratum delta lifecycle
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#define _GNU_SOURCE

#include "columnar/internal.h"
#include "arena/compound_arena.h"
#include "../wirelog-internal.h"

#include <errno.h>
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static int
wl_delta_event_append(wl_col_session_t *sess, const char *relation,
    const int64_t *row, uint32_t ncols, int32_t diff)
{
    wl_col_delta_event_t *grown;
    int64_t *copy;
    size_t next = sess->delta_event_capacity;
    uint64_t row_bytes = 0;
    uint64_t array_bytes = 0;
    uint64_t new_reserved = sess->delta_event_reserved_bytes;
    wl_columnar_memory_reservation_t replacement;
    bool replacement_active = false;

    if (!wl_columnar_memory_size_mul(ncols, sizeof(*copy), &row_bytes))
        return EOVERFLOW;
    if (sess->delta_event_count == sess->delta_event_capacity) {
        next = next ? next * 2 : 16;
        if (next < sess->delta_event_capacity
            || !wl_columnar_memory_size_mul(next, sizeof(*grown),
            &array_bytes))
            return EOVERFLOW;
    }
    if (sess->delta_event_count == sess->delta_event_capacity)
        new_reserved = sess->delta_event_reserved_bytes
            + (array_bytes - sess->delta_event_capacity * sizeof(*grown));
    if (!wl_columnar_memory_size_add(new_reserved, row_bytes,
        &new_reserved))
        return EOVERFLOW;
    if (sess->memory_governor && new_reserved > 0) {
        wl_columnar_memory_reservation_init(&replacement);
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_growth(
                wl_columnar_memory_governor_ref_get(sess->memory_governor),
                sess->delta_event_reserved_bytes, new_reserved,
                &replacement);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
            return status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : ENOMEM;
        replacement_active = true;
    }
    if (sess->delta_event_count == sess->delta_event_capacity) {
        grown = (wl_col_delta_event_t *)realloc(sess->delta_events,
                next * sizeof(*grown));
        if (!grown) {
            if (replacement_active)
                wl_columnar_memory_rollback(&replacement);
            return ENOMEM;
        }
        sess->delta_events = grown;
        sess->delta_event_capacity = next;
    }
    copy = (int64_t *)malloc((size_t)row_bytes);
    if (!copy) {
        if (replacement_active)
            wl_columnar_memory_rollback(&replacement);
        return ENOMEM;
    }
    memcpy(copy, row, (size_t)row_bytes);
    if (replacement_active) {
        if (!wl_columnar_memory_commit(&replacement, sess)) {
            free(copy);
            wl_columnar_memory_rollback(&replacement);
            return EBUSY;
        }
        if (sess->delta_event_reserved_bytes == 0)
            wl_columnar_memory_reservation_init(
                &sess->delta_event_reservation);
        wl_columnar_memory_release(&sess->delta_event_reservation);
        wl_columnar_memory_reservation_move(&sess->delta_event_reservation,
            &replacement);
    }
    sess->delta_event_reserved_bytes = new_reserved;
    sess->delta_events[sess->delta_event_count++]
        = (wl_col_delta_event_t){ relation, copy, ncols, diff };
    return 0;
}

void
wl_columnar_delta_events_clear(wl_col_session_t *sess)
{
    if (!sess)
        return;
    for (size_t i = 0; i < sess->delta_event_count; i++)
        free(sess->delta_events[i].row);
    free(sess->delta_events);
    sess->delta_events = NULL;
    sess->delta_event_count = 0;
    sess->delta_event_capacity = 0;
    wl_columnar_memory_release(&sess->delta_event_reservation);
    sess->delta_event_reserved_bytes = 0;
}

void
wl_columnar_delta_events_publish(wl_col_session_t *sess)
{
    if (!sess || !sess->delta_cb)
        return;
    sess->delta_publish_active = true;
    sess->delta_publish_cancelled = false;
    for (size_t i = 0; i < sess->delta_event_count
        && !sess->delta_publish_cancelled; i++) {
        wirelog_on_delta_fn callback = sess->delta_cb;
        void *data = sess->delta_data;
        if (!callback)
            break;
        wl_col_delta_event_t *event = &sess->delta_events[i];
        callback(event->relation, event->row, event->ncols, event->diff, data);
    }
    sess->delta_publish_active = false;
}

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
 * wl_columnar_eval_delta_consolidate: Privately normalize and publish one IDB relation.
 * Untimestamped inputs reuse the consolidation operator; timestamped inputs
 * use typed set normalization that preserves complete row provenance.
 */

/*
 * col_stratum_step_with_delta: Evaluate one stratum and fire delta callbacks.
 *
 * Full re-evaluation and set-difference algorithm:
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
typedef struct {
    col_rel_t *rel;
    wl_columnar_source_access_writer_t writer;
    int64_t **empty_columns;
    col_delta_timestamp_t *empty_timestamps;
    uint64_t old_bytes;
    uint64_t incoming_bytes;
    bool growth_pending;
    bool growth_committed;
} wl_retraction_stage_t;

#ifdef WL_SESSION_TEST_HOOKS
int (*wl_columnar_eval_delta_test_retraction_eval)(
    const wl_plan_stratum_t *, wl_col_session_t *, uint32_t);
#endif

static int
wl_retraction_evaluate(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess, uint32_t stratum_idx)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_delta_test_retraction_eval)
        return wl_columnar_eval_delta_test_retraction_eval(sp, sess,
                   stratum_idx);
#endif
    return col_eval_stratum(sp, sess, stratum_idx);
}

static void
wl_retraction_stage_discard(wl_retraction_stage_t *stages, uint32_t count)
{
    if (!stages)
        return;
    /* Free the staged physical image before returning any reserved credit. */
    for (uint32_t i = 0; i < count; i++) {
        wl_retraction_stage_t *s = &stages[i];
        if (s->empty_columns)
            col_columns_free(s->empty_columns, s->rel->ncols);
        free(s->empty_timestamps);
        s->empty_columns = NULL;
        s->empty_timestamps = NULL;
    }
    for (uint32_t i = 0; i < count; i++) {
        wl_retraction_stage_t *s = &stages[i];
        if (s->growth_committed) {
            if (!wl_columnar_memory_reservation_downsize(
                    &s->rel->retained_reservation, s->old_bytes))
                abort();
            s->rel->retained_reserved_bytes = s->old_bytes;
        } else if (s->growth_pending
            && !wl_columnar_memory_rollback_growth(
                &s->rel->retained_reservation))
            abort();
        if (s->writer.owner)
            (void)wl_columnar_source_access_writer_release(&s->writer);
    }
    free(stages);
}

static int
wl_retraction_stage_prepare(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess, wl_retraction_stage_t **out)
{
    const uint32_t count = sp->relation_count;
    wl_retraction_stage_t *stages = calloc(count ? count : 1u,
            sizeof(*stages));
    int rc = ENOMEM;
    if (!stages)
        return ENOMEM;
    *out = NULL;
    for (uint32_t i = 0; i < count; i++) {
        wl_retraction_stage_t *s = &stages[i];
        col_rel_t *r = session_find_rel(sess, sp->relations[i].name);
        if (!r || r->ncols == 0)
            continue;
        for (uint32_t j = 0; j < i; j++)
            if (stages[j].rel == r) {
                rc = EBUSY;
                goto fail;
            }
        s->rel = r;
        rc = col_rel_source_writer_acquire(r, &s->writer);
        if (rc != 0)
            goto fail;
        if (r->pool_owned || r->arena_owned || r->col_shared
            || r->storage_owner != r
            || r->storage_owner_identity != r->relation_identity
            || r->storage_owner_generation != r->storage_generation
            || col_rel_storage_alias_borrow_count(r) != 0
            || r->retract_backup_columns || r->retract_backup_timestamps
            || r->retract_backup_capacity
            || r->retract_backup_timestamp_capacity
            || r->retract_backup_nrows || r->retract_backup_base_nrows
            || r->base_nrows > r->nrows || r->merge_columns
            || r->merge_buf_cap || !r->columns
            || r->nrows > r->capacity
            || (r->timestamps != NULL) != (r->timestamp_capacity != 0)
            || (r->timestamps && r->timestamp_capacity < r->nrows)
            || (r->memory_governor && !r->row_scratch)) {
            rc = EBUSY;
            goto fail;
        }
        for (uint32_t c = 0; c < r->ncols; c++)
            if (!r->columns[c]) {
                rc = EBUSY;
                goto fail;
            }
        if (!col_rel_retained_live_bytes(r, &s->old_bytes)) {
            rc = EBUSY;
            goto fail;
        }
        if (r->memory_governor
            && (s->old_bytes != r->retained_reserved_bytes
            || r->retained_reservation.bytes != s->old_bytes
            || r->retained_reservation.identity
            != &r->retained_reservation
            || r->retained_reservation.governor
            != wl_columnar_memory_governor_ref_get(
                r->memory_governor)
            || atomic_load_explicit(&r->retained_reservation.state,
            memory_order_acquire)
            != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
            || atomic_load_explicit(&r->retained_reservation.owner_bits,
            memory_order_acquire) != (uintptr_t)r)) {
            rc = EBUSY;
            goto fail;
        }
    }
    /* Every participant is canonical and token-complete before the first
     * growth reservation. Keep their writer leases only through publish. */
    for (uint32_t i = 0; i < count; i++) {
        wl_retraction_stage_t *s = &stages[i];
        col_rel_t *r = s->rel;
        if (!r)
            continue;
        uint64_t lanes, table, timestamps = 0;
        if (!wl_columnar_memory_size_mul(r->ncols, COL_REL_INIT_CAP,
            &lanes)
            || !wl_columnar_memory_size_mul(lanes, sizeof(int64_t),
            &lanes)
            || !wl_columnar_memory_size_mul(r->ncols,
            sizeof(int64_t *), &table)
            || !wl_columnar_memory_size_add(lanes, table,
            &s->incoming_bytes)
            || (r->timestamps && (!wl_columnar_memory_size_mul(
                COL_REL_INIT_CAP, sizeof(col_delta_timestamp_t),
                &timestamps)
            || !wl_columnar_memory_size_add(s->incoming_bytes,
            timestamps, &s->incoming_bytes)))) {
            rc = EOVERFLOW;
            goto fail;
        }
        if (r->memory_governor) {
            wl_columnar_memory_admission_status_t status
                = wl_columnar_memory_begin_growth(
                    &r->retained_reservation, s->incoming_bytes);
            if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
                && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
                rc = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                    : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                    ? EOVERFLOW : EBUSY;
                if (rc == ENOSPC) {
                    r->memory_budget_denial_pending = true;
                    sess->memory_budget_denied = true;
                }
                goto fail;
            }
            s->growth_pending = true;
        }
        s->empty_columns = col_columns_alloc(r->ncols, COL_REL_INIT_CAP);
        if (!s->empty_columns) {
            rc = ENOMEM;
            goto fail;
        }
        if (r->timestamps) {
            s->empty_timestamps = calloc(COL_REL_INIT_CAP,
                    sizeof(*s->empty_timestamps));
            if (!s->empty_timestamps) {
                rc = ENOMEM;
                goto fail;
            }
        }
    }
    /* All fallible preparation completed. Commit credits while every old
     * image and every staged allocation is still resident. */
    for (uint32_t i = 0; i < count; i++) {
        wl_retraction_stage_t *s = &stages[i];
        if (!s->rel || !s->growth_pending)
            continue;
        if (!wl_columnar_memory_commit_growth(
                &s->rel->retained_reservation)) {
            rc = EBUSY;
            goto fail;
        }
        s->growth_pending = false;
        s->growth_committed = true;
        s->rel->retained_reserved_bytes += s->incoming_bytes;
    }
    *out = stages;
    return 0;
fail:
    wl_retraction_stage_discard(stages, count);
    return rc;
}

static void
wl_retraction_restore(col_rel_t *r)
{
    if (!r || !r->retract_backup_columns)
        return;
    col_columns_free(r->columns, r->ncols);
    free(r->timestamps);
    col_columns_free(r->merge_columns, r->ncols);
    r->columns = NULL;
    r->timestamps = NULL;
    r->timestamp_capacity = 0;
    r->merge_columns = NULL;
    r->merge_buf_cap = 0;
    r->capacity = 0;
    r->nrows = 0;
    col_rel_retire_payload_credit(r);
    r->columns = r->retract_backup_columns;
    r->timestamps = r->retract_backup_timestamps;
    r->timestamp_capacity = r->retract_backup_timestamp_capacity;
    r->nrows = r->retract_backup_nrows;
    r->base_nrows = r->retract_backup_base_nrows;
    r->capacity = r->retract_backup_capacity;
    r->sorted_nrows = r->retract_backup_sorted_nrows;
    r->run_count = r->retract_backup_run_count;
    memcpy(r->run_ends, r->retract_backup_run_ends,
        sizeof(r->run_ends));
    r->retract_backup_columns = NULL;
    r->retract_backup_timestamps = NULL;
    r->retract_backup_timestamp_capacity = 0;
    r->retract_backup_nrows = 0;
    r->retract_backup_base_nrows = 0;
    r->retract_backup_capacity = 0;
    r->retract_backup_sorted_nrows = 0;
    r->retract_backup_run_count = 0;
    wl_columnar_relation_touch_replacement(r);
}

static void
wl_retraction_restore_all(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess)
{
    for (uint32_t i = 0; i < sp->relation_count; i++)
        wl_retraction_restore(session_find_rel(sess,
            sp->relations[i].name));
}

static int wl_columnar_eval_delta_timestamp_normalize(
    wl_col_session_t *sess, col_rel_t *rel);

static int
wl_retraction_consolidate_candidates(col_rel_t *r, wl_col_session_t *sess)
{
    if (r->nrows <= 1)
        return 0;
    if (r->timestamps)
        return wl_columnar_eval_delta_timestamp_normalize(sess, r);
    int rc = col_rel_radix_sort(r, 0, r->nrows);
    if (rc != 0)
        return rc;
    uint32_t out = 1;
    for (uint32_t row = 1; row < r->nrows; row++) {
        if (col_rel_row_cmp(r, out - 1u, row) == 0)
            continue;
        col_rel_row_move_raw(r, out, row);
        out++;
    }
    r->nrows = out;
    r->sorted_nrows = out;
    r->run_count = 1;
    r->run_ends[0] = out;
    return 0;
}

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
        free((void *)retract_data);
        free(retract_nrows);
        return ENOMEM;
    }

    wl_retraction_stage_t *stages = NULL;
    int rc = wl_retraction_stage_prepare(sp, sess, &stages);
    if (rc != 0) {
        free((void *)retract_data);
        free(retract_nrows);
        return rc;
    }

    /* Step 0: Pointer-swap original data into retract_backup fields (O(1))
     * and clear relation for retraction evaluation. */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        wl_retraction_stage_t *s = &stages[ri];
        col_rel_t *r = s->rel;
        if (!r)
            continue;
        r->retract_backup_columns = r->columns;
        r->retract_backup_timestamps = r->timestamps;
        r->retract_backup_timestamp_capacity = r->timestamp_capacity;
        r->retract_backup_nrows = r->nrows;
        r->retract_backup_base_nrows = r->base_nrows;
        r->retract_backup_capacity = r->capacity;
        r->retract_backup_sorted_nrows = r->sorted_nrows;
        r->retract_backup_run_count = r->run_count;
        memcpy(r->retract_backup_run_ends, r->run_ends,
            sizeof(r->run_ends));
        r->columns = s->empty_columns;
        r->timestamps = s->empty_timestamps;
        r->timestamp_capacity = s->empty_timestamps
            ? COL_REL_INIT_CAP : 0;
        s->empty_columns = NULL;
        s->empty_timestamps = NULL;
        r->capacity = COL_REL_INIT_CAP;
        r->nrows = 0;
        r->base_nrows = 0;
        r->sorted_nrows = 0;
        r->run_count = 0;
        /* Retraction publishes a temporary empty view.  Restoration below
         * must receive a fresh generation, never the saved one. */
        wl_columnar_relation_touch_replacement(r);
    }
    for (uint32_t ri = 0; ri < rc_cnt; ri++)
        if (stages[ri].writer.owner)
            (void)wl_columnar_source_access_writer_release(
                &stages[ri].writer);
    free(stages);

    /* Step 1: Enable retraction-seeded mode and evaluate stratum.
     * First pass (left): VARIABLE loads $r$, JOIN uses full right.
     * Issue #472: Second pass (right): VARIABLE loads full, JOIN uses $r$
     * on the right side.  This is needed for self-join rules where the
     * retracted EDB appears on both sides of a JOIN. */
    sess->retraction_seeded = true;
    sess->retraction_right_pass = false;
    rc = wl_retraction_evaluate(sp, sess, stratum_idx);

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
            rc = wl_retraction_evaluate(sp, sess, stratum_idx);
            sess->retraction_right_pass = false;
        }
    }
    sess->retraction_seeded = false;
    if (rc != 0) {
        wl_retraction_restore_all(sp, sess);
        free((void *)retract_data);
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
            rc = wl_retraction_consolidate_candidates(r, sess);
            if (rc != 0) {
                wl_retraction_restore_all(sp, sess);
                for (uint32_t i = 0; i < rc_cnt; i++)
                    free(retract_data[i]);
                free((void *)retract_data);
                free(retract_nrows);
                return rc;
            }
            /* Steal: gather into flat buffer for col_row_in_sorted */
            uint32_t nc = r->ncols;
            int64_t *flat = (int64_t *)malloc(
                (size_t)r->nrows * nc * sizeof(int64_t));
            if (!flat) {
                wl_retraction_restore_all(sp, sess);
                for (uint32_t i = 0; i < rc_cnt; i++)
                    free(retract_data[i]);
                free((void *)retract_data);
                free(retract_nrows);
                return ENOMEM;
            }
            for (uint32_t row = 0; row < r->nrows; row++)
                col_rel_row_copy_out(r, row, flat + (size_t)row * nc);
            retract_data[ri] = flat;
            retract_nrows[ri] = r->nrows;
        }
        wl_retraction_restore(r);
    }

    /* Prepare the only row buffer before any relation can lose a row. */
    uint32_t widest = 0;
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (r && retract_nrows[ri] && r->ncols > widest)
            widest = r->ncols;
    }
    int64_t row_stack[COL_STACK_MAX];
    int64_t *src_buf = row_stack;
    if (widest > COL_STACK_MAX) {
#if SIZE_MAX < UINT64_MAX
        if ((uint64_t)widest > SIZE_MAX / sizeof(int64_t)) {
            rc = EOVERFLOW;
        } else {
#endif
        src_buf = malloc((size_t)widest * sizeof(int64_t));
        if (!src_buf)
            rc = ENOMEM;
#if SIZE_MAX < UINT64_MAX
    }
#endif
        if (rc != 0) {
            for (uint32_t i = 0; i < rc_cnt; i++)
                free(retract_data[i]);
            free((void *)retract_data);
            free(retract_nrows);
            return rc;
        }
    }

    /* Step 4: Remove retracted rows and fire delta callbacks */
    for (uint32_t ri = 0; ri < rc_cnt; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (!r || r->ncols == 0 || retract_nrows[ri] == 0)
            continue;

        uint32_t ncols = r->ncols;
        bool removed_any = false;

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
                    removed_any = true;
                    /* Copy remaining rows forward */
                    for (uint32_t rest = src_idx + 1; rest < r->nrows;
                        rest++) {
                        col_rel_row_move_raw(r, out_r, rest);
                        if (r->timestamps)
                            r->timestamps[out_r] = r->timestamps[rest];
                        out_r++;
                    }
                    r->nrows = out_r;
                    if (src_idx < r->base_nrows)
                        r->base_nrows--;
                    wl_columnar_relation_touch_view(r);
                    break;
                } else {
                    /* Keep this row */
                    if (out_r != src_idx) {
                        col_rel_row_copy_in_raw(r, out_r, src_buf);
                        if (r->timestamps)
                            r->timestamps[out_r] = r->timestamps[src_idx];
                    }
                    out_r++;
                }
            }

            /* Fire delta callback if row was actually removed */
            if (found && sess->delta_cb) {
                sess->delta_cb(r->name, to_remove, ncols, -1,
                    sess->delta_data);
            }
        }
        if (removed_any)
            wl_columnar_eval_dedup_set_clear(r);
    }

    /* Cleanup: free stolen retraction buffers */
    if (src_buf != row_stack)
        free(src_buf);
    for (uint32_t i = 0; i < rc_cnt; i++)
        free(retract_data[i]);
    free((void *)retract_data);
    free(retract_nrows);
    return 0;
}

#ifdef WL_SESSION_TEST_HOOKS
int
wl_columnar_eval_delta_test_retraction_step(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess, uint32_t stratum_idx)
{
    return col_stratum_step_retraction_nonrecursive(sp, sess, stratum_idx);
}
#endif

typedef struct {
    char *name;
    uint64_t identity;
    int64_t *rows;
    uint32_t nrows;
    uint32_t ncols;
    bool detached;
    bool completed;
    bool alias;
    wl_columnar_source_access_reader_t capture_reader;
} wl_columnar_eval_delta_snapshot_t;

struct wl_columnar_eval_delta_rollback {
    wl_columnar_memory_reservation_t metadata_reservation;
    wl_columnar_memory_reservation_t payload_reservation;
    wl_columnar_memory_governor_ref_t *governor;
    wl_arena_compound_arena_gc_hold_t compound_hold;
    uint32_t count;
    bool active;
    bool payload_reserved;
    bool gc_requested;
    /* col_eval_stratum reached its completion point, so every head's content
     * below is final.  Restoring the pre-step snapshot over them would
     * un-retract facts this step correctly removed.  Content, not state: a
     * head can still be mid-consolidation on the paths that fail after
     * evaluation, but restore_flat only ever writes an empty head and an
     * empty one cannot be mid-consolidated. */
    bool retained;
    int evaluation_error;
    bool controlled_origin; /* Original tail padding; preserves error offset. */
    wl_columnar_eval_delta_snapshot_t entries[];
};

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_delta_test_after_eval)(wl_col_session_t *sess);
void (*wl_columnar_eval_delta_test_observer_boundary)(wl_col_session_t *sess,
    unsigned boundary);
WL_COLUMNAR_SESSION_HASH_TEST_THREAD_LOCAL void
(*wl_columnar_eval_delta_test_gather_checkpoint)(wl_evaluation_control_t *,
    const int64_t *, uint32_t, uint64_t);
WL_COLUMNAR_SESSION_HASH_TEST_THREAD_LOCAL void
(*wl_columnar_eval_delta_test_physical_disposal)(
    const wl_columnar_eval_delta_preparation_t *,
    wl_columnar_memory_reservation_t *);
#endif

static int
wl_columnar_eval_delta_reserve(wl_col_session_t *sess, uint64_t bytes,
    wl_columnar_memory_reservation_t *reservation)
{
    wl_columnar_memory_reservation_init(reservation);
    if (!sess->memory_governor || bytes == 0)
        return 0;
    wl_columnar_memory_admission_status_t status
        = wl_columnar_memory_reserve_checked(
            wl_columnar_memory_governor_ref_get(sess->memory_governor),
            bytes, reservation);
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_OK
        || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
        return 0;
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
        sess->memory_budget_denied = true;
    return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
        : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
}

typedef struct {
    const char *capture_name;
    char *name;
    int64_t *rows;
    int64_t *current;
    uint32_t nrows, ncols;
    uint32_t pending_nrows;
    uint32_t current_nrows, current_ncols;
    wl_columnar_source_access_reader_t reader;
} wl_columnar_eval_delta_observer_entry_t;

struct wl_columnar_eval_delta_observer {
    wl_columnar_memory_reservation_t metadata, payload, staging;
    wl_columnar_memory_governor_ref_t *governor;
    wl_arena_compound_arena_gc_hold_t compound_hold;
    uint64_t affected_mask, metadata_bytes, payload_bytes, staging_bytes;
    uint32_t count;
    bool active, evaluated, cancelled, delivered, gc_requested;
    bool controlled_origin; /* Original padding; raw calloc keeps false. */
    wl_col_delta_event_t *events;
    size_t event_count;
    wl_columnar_eval_delta_observer_entry_t entries[];
};

/* Immutable original object layouts, independent of current field padding. */
typedef struct {
    wl_columnar_memory_reservation_t metadata, payload, staging;
    wl_columnar_memory_governor_ref_t *governor;
    wl_arena_compound_arena_gc_hold_t compound_hold;
    uint64_t affected_mask, metadata_bytes, payload_bytes, staging_bytes;
    uint32_t count;
    bool active, evaluated, cancelled, delivered, gc_requested;
    wl_col_delta_event_t *events;
    size_t event_count;
    wl_columnar_eval_delta_observer_entry_t entries[];
} wl_columnar_eval_delta_observer_original_t;
typedef struct {
    wl_columnar_memory_reservation_t metadata_reservation, payload_reservation;
    wl_columnar_memory_governor_ref_t *governor;
    wl_arena_compound_arena_gc_hold_t compound_hold;
    uint32_t count;
    bool active, payload_reserved, gc_requested, retained;
    int evaluation_error;
    wl_columnar_eval_delta_snapshot_t entries[];
} wl_columnar_eval_delta_rollback_original_t;
_Static_assert(sizeof(wl_columnar_eval_delta_observer_t)
    == sizeof(wl_columnar_eval_delta_observer_original_t),
    "observer original size");
_Static_assert(_Alignof(wl_columnar_eval_delta_observer_t)
    == _Alignof(wl_columnar_eval_delta_observer_original_t),
    "observer original alignment");
_Static_assert(offsetof(wl_columnar_eval_delta_observer_t, events)
    == offsetof(wl_columnar_eval_delta_observer_original_t, events),
    "observer original offsets");
_Static_assert(sizeof(wl_columnar_eval_delta_rollback_t)
    == sizeof(wl_columnar_eval_delta_rollback_original_t),
    "rollback original size");
_Static_assert(_Alignof(wl_columnar_eval_delta_rollback_t)
    == _Alignof(wl_columnar_eval_delta_rollback_original_t),
    "rollback original alignment");
_Static_assert(offsetof(wl_columnar_eval_delta_rollback_t, retained)
    == offsetof(wl_columnar_eval_delta_rollback_original_t, retained),
    "rollback retained offset");
_Static_assert(offsetof(wl_columnar_eval_delta_rollback_t, evaluation_error)
    == offsetof(wl_columnar_eval_delta_rollback_original_t, evaluation_error),
    "rollback error offset");

/* Constructor ownership remains in the original frame until an explicit
 * checked handoff. Partial object bytes are never interpreted by this drain. */
int
wl_columnar_eval_delta_preparation_retire(
    wl_columnar_eval_delta_preparation_t *record, int result)
{
    if (!record || !record->ready) return result > 0 ? result : EINVAL;
    /* Phase precedes the accounting union and the physical object owner.
     * Here object is borrowed from the session, never owned by this journal. */
    if (record->phase == WL_COLUMNAR_EVAL_DELTA_PREPARATION_HOLD_BORROW) {
        if (!record->active || !record->object || record->cursor > 1u)
            return result > 0 ? result : EINVAL;
        int cleanup = 0;
        if (record->hold_borrow.arena)
            cleanup = wl_arena_compound_arena_borrow_release_controlled(
                &record->hold_borrow, NULL);
        else if (record->hold_borrow.identity) cleanup = EINVAL;
        if (cleanup) return result > 0 ? result : cleanup;
        wl_arena_compound_arena_gc_hold_t *hold = record->cursor
            ? &((wl_columnar_eval_delta_rollback_t *)record->object)->
            compound_hold
            : &((wl_columnar_eval_delta_observer_t *)record->object)->
            compound_hold;
        if (hold->arena) cleanup =
                wl_arena_compound_arena_gc_hold_release_controlled(hold, NULL);
        else if (hold->identity) cleanup = EINVAL;
        if (cleanup) return result > 0 ? result : cleanup;
        /* Both original tokens are now truly inert. The constructor's
         * accounting obligations were transferred before entering this phase.
         * This journal owns no physical object or reference after that move. */
        record->bytes = 0; record->total = 0; record->initialized = 0;
        record->object = NULL; record->cursor = 0; record->phase = 1;
        record->active = false; record->physical_disposed = true;
        return result;
    }
    if (!record->active) return result;
    if (record->phase > 2u) return result > 0 ? result : EINVAL;
    if (!record->physical_disposed) {
#ifdef WL_SESSION_TEST_HOOKS
        bool physical_object = record->object != NULL;
#endif
        free(record->object); record->object = NULL;
        record->physical_disposed = true;
#ifdef WL_SESSION_TEST_HOOKS
        if (physical_object && wl_columnar_eval_delta_test_physical_disposal)
            wl_columnar_eval_delta_test_physical_disposal(record,
                &record->metadata);
#endif
    }
    if (record->metadata.identity != &record->metadata)
        return result > 0 ? result : EINVAL;
    uint64_t state = atomic_load_explicit(&record->metadata.state,
            memory_order_acquire);
    bool retired = state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        ? wl_columnar_memory_rollback(&record->metadata)
        : state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
        ? wl_columnar_memory_release(&record->metadata)
        : (state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
        || state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED)
        && !record->metadata.bytes && !record->metadata.replacement_bytes;
    if (!retired) return result > 0 ? result : EINVAL;
    if (record->governor) {
        wl_columnar_memory_governor_ref_release(record->governor);
        record->governor = NULL;
    }
    record->bytes = 0; record->active = false;
    return result;
}

/* The metadata/physical owner has already moved to the persistent object.
 * Only dead constructor accounting scalars are borrowed for the original
 * temporary arena token. The frame metadata slot stays EMPTY throughout. */
int
wl_columnar_eval_delta_hold_controlled(wl_col_session_t *sess,
    wl_columnar_eval_delta_preparation_t *preparation, bool rollback_object,
    wl_evaluation_control_t *control)
{
    if (!sess || sess->coordinator || !preparation || !control) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    if (!preparation->ready || preparation->active
        || preparation->phase == WL_COLUMNAR_EVAL_DELTA_PREPARATION_HOLD_BORROW
        || preparation->object || preparation->governor || preparation->bytes
        || preparation->metadata.identity != &preparation->metadata
        || preparation->metadata.governor || preparation->metadata.bytes
        || preparation->metadata.replacement_bytes
        || preparation->metadata.transition_kind
        || atomic_load_explicit(&preparation->metadata.owner_bits,
        memory_order_relaxed)
        || atomic_load_explicit(&preparation->metadata.state,
        memory_order_acquire)
        != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY) return EBUSY;
    void *object =
        rollback_object ? (void *)sess->delta_rollback :
        (void *)sess->delta_observer;
    if (!object) return EINVAL;
    bool origin = rollback_object ? sess->delta_rollback->controlled_origin
        : sess->delta_observer->controlled_origin;
    wl_arena_compound_arena_gc_hold_t *hold = rollback_object
        ? &sess->delta_rollback->compound_hold :
        &sess->delta_observer->compound_hold;
    if (!origin || hold->arena || hold->identity) return EINVAL;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    if (!sess->compound_arena) return 0;
    rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    preparation->phase = WL_COLUMNAR_EVAL_DELTA_PREPARATION_HOLD_BORROW;
    preparation->object = object;
    preparation->cursor = rollback_object ? 1u : 0u;
    preparation->active = true; preparation->physical_disposed = true;
    preparation->hold_borrow.arena = NULL;
    preparation->hold_borrow.identity = 0;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    rc =
        wl_arena_compound_arena_gc_hold_acquire_controlled(sess->compound_arena,
            hold, &preparation->hold_borrow, control);
    if (rc) goto done;
    if (preparation->hold_borrow.arena || preparation->hold_borrow.identity) {
        rc = EINVAL; goto done;
    }
    /* Healthy completion keeps the embedded hold as the persistent owner. */
    preparation->bytes = 0; preparation->total = 0;
    preparation->initialized = 0;
    preparation->object = NULL; preparation->cursor = 0; preparation->phase = 1;
    preparation->active = false;
    return 0;
done:
    return wl_columnar_eval_delta_preparation_retire(preparation, rc);
}

/* Observer ordering is the raw native-byte row order, independently of a
 * relation's typed SET order. Each admitted library comparison is bounded. */
int
wl_columnar_eval_delta_compare_controlled(const int64_t *left,
    const int64_t *right, uint32_t ncols, int *comparison,
    wl_evaluation_control_t *control)
{
    if (!left || !right || !comparison || !control)
        return EINVAL;
    uint64_t bytes = (uint64_t)ncols * sizeof(int64_t);
    if (bytes > SIZE_MAX) return EOVERFLOW;
    int rc = wl_evaluation_control_charge(control, 1);
    if (rc) return rc;
    *comparison = 0;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    const unsigned char *a = (const unsigned char *)left;
    const unsigned char *b = (const unsigned char *)right;
    for (uint64_t pos = 0; pos < bytes;) {
        uint64_t n = bytes - pos;
        if (n > 128u) n = 128u;
        rc = wl_evaluation_control_charge(control, n);
        if (rc) return rc;
        int value = memcmp(a + pos, b + pos, (size_t)n);
        *comparison = value;
        pos += n;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        if (value) return 0;
    }
    return 0;
}

int
wl_columnar_eval_delta_swap_controlled(int64_t *rows, uint32_t a,
    uint32_t b, uint32_t ncols, wl_evaluation_control_t *control)
{
    if (!rows || !control) return EINVAL;
    uint32_t last = a > b ? a : b;
    if (((uint64_t)last + 1u) * ncols > SIZE_MAX / sizeof(int64_t))
        return EOVERFLOW;
    int rc = wl_evaluation_control_charge(control, 1);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc || a == b) return rc;
    for (uint32_t col = 0; col < ncols; col++) {
        rc = wl_evaluation_control_charge(control, 3);
        if (rc) return rc;
        int64_t value = rows[(size_t)a * ncols + col];
        rows[(size_t)a * ncols + col] = rows[(size_t)b * ncols + col];
        rows[(size_t)b * ncols + col] = value;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    return 0;
}

static int
wl_columnar_eval_delta_sift_controlled(int64_t *rows, uint32_t root,
    uint32_t count, uint32_t ncols, wl_evaluation_control_t *control)
{
    while (root < count / 2u) {
        int rc = wl_evaluation_control_charge(control, 8);
        if (rc) return rc;
        uint32_t child = 2u * root + 1u;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        int comparison;
        if (child + 1u < count) {
            rc = wl_columnar_eval_delta_compare_controlled(
                rows + (size_t)child * ncols,
                rows + (size_t)(child + 1u) * ncols, ncols,
                &comparison, control);
            if (rc) return rc;
            if (comparison < 0) child++;
        }
        rc = wl_columnar_eval_delta_compare_controlled(
            rows + (size_t)root * ncols, rows + (size_t)child * ncols,
            ncols, &comparison, control);
        if (rc) return rc;
        if (comparison >= 0) return 0;
        rc = wl_columnar_eval_delta_swap_controlled(rows, root, child,
                ncols, control);
        if (rc) return rc;
        root = child;
    }
    return 0;
}

int
wl_columnar_eval_delta_sort_controlled(int64_t *rows, uint32_t count,
    uint32_t ncols, wl_evaluation_control_t *control)
{
    if (!control || (count && !rows)) return EINVAL;
    if ((uint64_t)count * ncols > SIZE_MAX / sizeof(int64_t))
        return EOVERFLOW;
    for (uint32_t at = count / 2u; at > 0; at--) {
        int rc = wl_evaluation_control_charge(control, 2);
        if (rc) return rc;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        rc = wl_columnar_eval_delta_sift_controlled(rows, at - 1u,
                count, ncols, control);
        if (rc) return rc;
    }
    for (uint32_t end = count; end > 1u; end--) {
        int rc = wl_evaluation_control_charge(control, 2);
        if (rc) return rc;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        rc = wl_columnar_eval_delta_swap_controlled(rows, 0, end - 1u,
                ncols, control);
        if (rc) return rc;
        rc = wl_columnar_eval_delta_sift_controlled(rows, 0, end - 1u,
                ncols, control);
        if (rc) return rc;
    }
    return 0;
}

/* Names remain in the existing plan/registry or admitted object payload.
 * Count and compare one byte at a time without a speculative strlen. */
static int
wl_columnar_eval_delta_name_length_controlled(const char *name,
    size_t *length, wl_evaluation_control_t *control)
{
    if (!name || !length || !control) return EINVAL;
    for (size_t at = 0;; at++) {
        int rc = wl_evaluation_control_charge(control, 1);
        if (rc) return rc;
        bool terminal = !name[at];
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        if (terminal) {
            *length = at; return 0;
        }
        if (at == SIZE_MAX - 1u) return EOVERFLOW;
    }
}

static int
wl_columnar_eval_delta_name_equal_controlled(const char *a, const char *b,
    bool *equal, wl_evaluation_control_t *control)
{
    if (!a || !b || !equal || !control) return EINVAL;
    *equal = false;
    for (size_t at = 0;; at++) {
        int rc = wl_evaluation_control_charge(control, 2);
        if (rc) return rc;
        char av = a[at], bv = b[at];
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        if (av != bv) return 0;
        if (!av) {
            *equal = true; return 0;
        }
        if (at == SIZE_MAX - 1u) return EOVERFLOW;
    }
}

/* Preserve the legacy 256-byte private-name spelling and absent result when
 * the spelling does not fit. Prefix width is a literal caller parameter. */
static int
wl_columnar_eval_delta_private_lookup_controlled(wl_col_session_t *sess,
    const char *prefix, size_t prefix_length, const char *name,
    col_rel_t **out, wl_evaluation_control_t *control)
{
    if (!sess || !prefix || !name || !out || !control
        || prefix_length >= 256u) return EINVAL;
    *out = NULL;
    char spelling[256];
    int rc = wl_evaluation_control_charge(control, prefix_length);
    if (rc) return rc;
    memcpy(spelling, prefix, prefix_length);
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    for (size_t at = prefix_length; at < sizeof(spelling); at++) {
        rc = wl_evaluation_control_charge(control, 2); if (rc) return rc;
        char value = name[at - prefix_length]; spelling[at] = value;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        if (!value) return wl_columnar_join_controlled_lookup(sess,
                       spelling, out, control);
    }
    return 0;
}

static int
wl_columnar_eval_delta_source_shape_controlled(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    col_rel_t *relation, uint32_t *nrows, uint32_t *ncols,
    uint32_t *base_nrows_out,
    wl_evaluation_control_t *control)
{
    if (!frame || !relation || !nrows || !ncols || !base_nrows_out || !control)
        return EINVAL;
    wl_columnar_relation_source_group_t *sources =
        wl_columnar_eval_stack_cleanup_sources(frame);
    if (!sources || sources->descriptor_count ||
        sources->owner_count) return EBUSY;
    int rc = wl_columnar_relation_source_group_acquire_controlled(sources,
            relation, NULL, control);
    if (rc) return wl_columnar_relation_source_group_drain(sources, rc);
    rc = wl_evaluation_control_charge(control, 16);
    if (!rc && relation->nrows > relation->capacity) rc = EINVAL;
    if (!rc) {
        *nrows = relation->nrows; *ncols = relation->ncols;
        *base_nrows_out = relation->base_nrows;
        rc = wl_evaluation_control_charge(control, 0);
    }
    if (rc) return wl_columnar_relation_source_group_drain(sources, rc);
    rc = wl_columnar_relation_source_group_finish_controlled(sources, control);
    return rc ? wl_columnar_relation_source_group_drain(sources, rc) : 0;
}

static int
wl_columnar_eval_delta_reserve_controlled(wl_col_session_t *sess,
    uint64_t bytes,
    wl_columnar_memory_reservation_t *reservation,
    wl_evaluation_control_t *control)
{
    if (!sess || !reservation || !control) return EINVAL;
    if (!sess->memory_governor || !bytes) return 0;
    wl_columnar_memory_admission_status_t status =
        WL_COLUMNAR_MEMORY_ADMISSION_INVALID;
    int rc = wl_columnar_memory_governor_reserve_checked_controlled(
        wl_columnar_memory_governor_ref_get(sess->memory_governor), bytes,
        reservation, &status, control);
    if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
        sess->memory_budget_denied = true;
    if (rc) return rc;
    return status <= WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY ? 0
        : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
        : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
}

/* The original mode19 owns the transient raw flat image. The source pin is
 * fully retired before any caller may overwrite the name/SINGLE union. */
int
wl_columnar_eval_delta_seed_gather_controlled(wl_col_session_t *session,
    wl_columnar_eval_stack_cleanup_frame_t *frame, col_rel_t *dst,
    wl_columnar_eval_delta_seed_restore_t **out,
    wl_evaluation_control_t *control)
{
    if (!session || !frame || !dst || !out || *out || !control) return EINVAL;
    int rc = wl_columnar_eval_stack_cleanup_seed(session, frame, out, control);
    if (rc) return rc;
    wl_columnar_eval_delta_seed_restore_t *record = *out;
    rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    record->dst = dst;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    if (!dst->name) return 0;
    rc = wl_evaluation_control_charge(control, 4); if (rc) return rc;
    memcpy(record->shadow_name, "$in$", 4);
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    bool spelled = false;
    for (size_t at = 4; at < sizeof(record->shadow_name); at++) {
        rc = wl_evaluation_control_charge(control, 2); if (rc) return rc;
        char value = dst->name[at - 4]; record->shadow_name[at] = value;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        if (!value) {
            spelled = true; break;
        }
    }
    if (!spelled) return 0;
    rc = wl_columnar_join_controlled_lookup(session, record->shadow_name,
            &record->seed, control); if (rc || !record->seed) return rc;
    rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    record->phase = WL_COLUMNAR_OBSERVER_SEED_PIN;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    rc = wl_columnar_relation_reader_pair_acquire_controlled(record->seed,
            &record->seed_reader, control); if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    record->nrows = record->seed->nrows; record->ncols = record->seed->ncols;
    if (record->nrows > record->seed->capacity) return EINVAL;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    if (!record->nrows)
        return wl_columnar_relation_reader_pair_release_controlled(record->seed,
                   &record->seed_reader, control);
    rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    if (dst->ncols && dst->ncols != record->ncols) return EINVAL;
    if (!wl_columnar_memory_size_mul(record->nrows, record->ncols,
        &record->cells)
        || !wl_columnar_memory_size_mul(record->cells, sizeof(int64_t),
        &record->bytes)
        || record->bytes > SIZE_MAX) return EOVERFLOW;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    for (uint32_t col = 0; col < record->ncols; col++) {
        rc = wl_evaluation_control_charge(control, 2); if (rc) return rc;
        if (!record->seed->columns ||
            !record->seed->columns[col]) return EINVAL;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    record->phase = WL_COLUMNAR_OBSERVER_SEED_GATHER;
    record->row = 0; record->column = 0;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 1); if (rc) return rc;
    record->rows = (int64_t *)malloc((size_t)record->bytes);
    if (!record->rows) return ENOMEM;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    while (record->row < record->nrows) {
        if (!record->ncols) {
            rc = wl_evaluation_control_charge(control, 2); if (rc) return rc;
            record->row++;
            rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
            continue;
        }
        rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        size_t index = (size_t)record->row * record->ncols + record->column;
        record->rows[index] = col_rel_get(record->seed, record->row,
                record->column);
        record->column++;
        if (record->column == record->ncols) {
            record->column = 0; record->row++;
        }
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_eval_delta_test_gather_checkpoint)
            wl_columnar_eval_delta_test_gather_checkpoint(control, record->rows,
                record->ncols, (uint64_t)index + 1u);
#endif
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    return wl_columnar_relation_reader_pair_release_controlled(record->seed,
               &record->seed_reader, control);
}

/* Auxiliary retired/seed rows have their own durable original source group;
 * the observer's head reader cannot authorize their columns. */
int
wl_columnar_eval_delta_gather_controlled(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    col_rel_t *relation, uint32_t first, uint32_t count, uint32_t width,
    int64_t *rows, wl_evaluation_control_t *control)
{
    if (!frame || !relation || !rows || !control) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    wl_columnar_relation_source_group_t *sources =
        wl_columnar_eval_stack_cleanup_sources(frame);
    if (!sources || sources->descriptor_count || sources->owner_count)
        return EBUSY;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    rc = wl_columnar_relation_source_group_acquire_controlled(sources,
            relation, NULL, control);
    if (rc) return wl_columnar_relation_source_group_drain(sources, rc);
    rc = wl_evaluation_control_charge(control, 16);
    if (!rc && (relation->ncols != width || first > relation->nrows
        || count > relation->nrows - first ||
        relation->nrows > relation->capacity
        || ((uint64_t)count * width > SIZE_MAX / sizeof(int64_t)))) rc = EINVAL;
    if (!rc) rc = wl_evaluation_control_charge(control, 0);
    for (uint32_t col = 0; count && !rc && col < width; col++) {
        rc = wl_evaluation_control_charge(control, 2);
        if (!rc && (!relation->columns || !relation->columns[col])) rc = EINVAL;
        if (!rc) rc = wl_evaluation_control_charge(control, 0);
    }
    for (uint32_t row = 0; !rc && row < count; row++) {
        rc = wl_evaluation_control_charge(control, 2);
        if (!rc) rc = wl_evaluation_control_charge(control, 0);
        for (uint32_t col = 0; !rc && col < width; col++) {
            rc = wl_evaluation_control_charge(control, 2);
            if (rc) break;
            rows[(size_t)row * width + col] = relation->columns[col][first +
                    row];
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_columnar_eval_delta_test_gather_checkpoint)
                wl_columnar_eval_delta_test_gather_checkpoint(control, rows,
                    width, (uint64_t)row * width + col + 1u);
#endif
            rc = wl_evaluation_control_charge(control, 0);
        }
    }
    if (rc) return wl_columnar_relation_source_group_drain(sources, rc);
    rc = wl_columnar_relation_source_group_finish_controlled(sources, control);
    return rc ? wl_columnar_relation_source_group_drain(sources, rc) : 0;
}

static int
wl_columnar_eval_delta_copy_row_controlled(int64_t *dst, const int64_t *src,
    uint32_t ncols, wl_evaluation_control_t *control)
{
    for (uint32_t col = 0; col < ncols; col++) {
        int rc = wl_evaluation_control_charge(control, 2);
        if (rc) return rc;
        dst[col] = src[col];
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    return 0;
}

int
wl_columnar_eval_delta_normalize_controlled(int64_t *rows, uint32_t count,
    uint32_t ncols, uint32_t *normalized, wl_evaluation_control_t *control)
{
    if (!control || !normalized || (count && !rows)) return EINVAL;
    if ((uint64_t)count * ncols > SIZE_MAX / sizeof(int64_t))
        return EOVERFLOW;
    int rc = wl_evaluation_control_charge(control, 8);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    if (count < 2u || !ncols) {
        *normalized = count < 2u ? count : 1u;
        return 0;
    }
    rc = wl_columnar_eval_delta_sort_controlled(rows, count, ncols, control);
    if (rc) return rc;
    uint32_t out = 1;
    for (uint32_t at = 1; at < count; at++) {
        rc = wl_evaluation_control_charge(control, 4);
        if (rc) return rc;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        int comparison;
        rc = wl_columnar_eval_delta_compare_controlled(
            rows + (size_t)(out - 1u) * ncols,
            rows + (size_t)at * ncols, ncols, &comparison, control);
        if (rc) return rc;
        if (comparison) {
            if (out != at) {
                rc = wl_columnar_eval_delta_copy_row_controlled(
                    rows + (size_t)out * ncols,
                    rows + (size_t)at * ncols, ncols, control);
                if (rc) return rc;
            }
            out++;
        }
    }
    *normalized = out;
    return 0;
}

int
wl_columnar_eval_delta_subtract_controlled(int64_t *rows, uint32_t count,
    const int64_t *minus, uint32_t nminus, uint32_t ncols,
    uint32_t *remaining, wl_evaluation_control_t *control)
{
    if (!control || !remaining || (count && !rows) || (nminus && !minus))
        return EINVAL;
    if ((uint64_t)count * ncols > SIZE_MAX / sizeof(int64_t)
        || (uint64_t)nminus * ncols > SIZE_MAX / sizeof(int64_t))
        return EOVERFLOW;
    uint32_t out = 0, removed = 0;
    for (uint32_t at = 0; at < count; at++) {
        int rc = wl_evaluation_control_charge(control, 4);
        if (rc) return rc;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        int comparison = 1;
        while (removed < nminus) {
            rc = wl_evaluation_control_charge(control, 2);
            if (rc) return rc;
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) return rc;
            rc = wl_columnar_eval_delta_compare_controlled(
                minus + (size_t)removed * ncols,
                rows + (size_t)at * ncols, ncols, &comparison, control);
            if (rc) return rc;
            if (comparison >= 0) break;
            removed++;
        }
        if (removed < nminus && !comparison) {
            removed++;
            continue;
        }
        if (out != at) {
            rc = wl_columnar_eval_delta_copy_row_controlled(
                rows + (size_t)out * ncols, rows + (size_t)at * ncols,
                ncols, control);
            if (rc) return rc;
        }
        out++;
    }
    *remaining = out;
    return 0;
}

static void
wl_columnar_eval_delta_observer_swap(int64_t *rows, uint32_t a, uint32_t b,
    uint32_t ncols)
{
    for (uint32_t c = 0; c < ncols; c++) {
        int64_t tmp = rows[(size_t)a * ncols + c];
        rows[(size_t)a * ncols + c] = rows[(size_t)b * ncols + c];
        rows[(size_t)b * ncols + c] = tmp;
    }
}

static void
wl_columnar_eval_delta_observer_sift(int64_t *rows, uint32_t root,
    uint32_t count, uint32_t ncols)
{
    while (root < count / 2) {
        uint32_t child = root * 2 + 1;
        if (child + 1 < count && memcmp(rows + (size_t)child * ncols,
            rows + (size_t)(child + 1) * ncols,
            (size_t)ncols * sizeof(*rows)) < 0)
            child++;
        if (memcmp(rows + (size_t)root * ncols, rows + (size_t)child * ncols,
            (size_t)ncols * sizeof(*rows)) >= 0)
            break;
        wl_columnar_eval_delta_observer_swap(rows, root, child, ncols);
        root = child;
    }
}

/* An independent byte order is sufficient for exact row identity. Sort both
 * sets identically, without assuming the engine's typed sort uses this order. */
static void
wl_columnar_eval_delta_observer_sort(int64_t *rows, uint32_t count,
    uint32_t ncols)
{
    if (count < 2 || ncols == 0)
        return;
    for (uint32_t i = count / 2; i > 0; i--)
        wl_columnar_eval_delta_observer_sift(rows, i - 1, count, ncols);
    for (uint32_t end = count; end > 1; end--) {
        wl_columnar_eval_delta_observer_swap(rows, 0, end - 1, ncols);
        wl_columnar_eval_delta_observer_sift(rows, 0, end - 1, ncols);
    }
}

static uint32_t
wl_columnar_eval_delta_observer_normalize(int64_t *rows, uint32_t count,
    uint32_t ncols)
{
    if (count < 2)
        return count;
    if (ncols == 0)
        return 1;
    wl_columnar_eval_delta_observer_sort(rows, count, ncols);
    uint32_t out = 1;
    size_t width = (size_t)ncols * sizeof(*rows);
    for (uint32_t i = 1; i < count; i++) {
        if (memcmp(rows + (size_t)(out - 1) * ncols,
            rows + (size_t)i * ncols, width) != 0) {
            if (out != i)
                memcpy(rows + (size_t)out * ncols, rows + (size_t)i * ncols,
                    width);
            out++;
        }
    }
    return out;
}

/* Remove from @rows one copy of each row of @minus, both sorted by
 * wl_columnar_eval_delta_observer_sort.  Returns the remaining count. */
static uint32_t
wl_columnar_eval_delta_observer_subtract(int64_t *rows, uint32_t count,
    const int64_t *minus, uint32_t nminus, uint32_t ncols)
{
    size_t width = (size_t)ncols * sizeof(*rows);
    uint32_t out = 0, j = 0;

    for (uint32_t i = 0; i < count; i++) {
        int cmp = 1;
        while (j < nminus && (cmp = memcmp(minus + (size_t)j * ncols,
            rows + (size_t)i * ncols, width)) < 0)
            j++;
        if (j < nminus && cmp == 0) {
            j++;
            continue;
        }
        if (out != i)
            memmove(rows + (size_t)out * ncols, rows + (size_t)i * ncols,
                width);
        out++;
    }
    return out;
}

static bool
wl_columnar_eval_delta_observer_row_bytes(uint32_t nrows, uint32_t ncols,
    uint64_t *bytes)
{
    uint64_t cells;
    if (!wl_columnar_memory_size_mul(nrows, ncols, &cells))
        return false;
    /* Give an inhabited zero-column set a valid address for callbacks. */
    if (nrows && !cells)
        cells = 1;
    return wl_columnar_memory_size_mul(cells, sizeof(int64_t), bytes)
           && *bytes <= SIZE_MAX;
}

static void
wl_columnar_eval_delta_observer_release_token(
    wl_columnar_memory_reservation_t *token)
{
    uint64_t state = atomic_load_explicit(&token->state, memory_order_acquire);
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        || state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED) {
        bool released = wl_columnar_memory_release(token);
        assert(released);
        (void)released;
    }
    wl_columnar_memory_reservation_init(token);
}

/* Mandatory disposal keeps every credit at its actual original address. */
static int
wl_columnar_eval_delta_credit_retire(wl_columnar_memory_reservation_t *token)
{
    if (token->identity != token) return EINVAL;
    uint64_t state = atomic_load_explicit(&token->state, memory_order_acquire);
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED)
        return wl_columnar_memory_rollback(token) ? 0 : EINVAL;
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED)
        return wl_columnar_memory_release(token) ? 0 : EINVAL;
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED)
        return token->transition_kind == WL_COLUMNAR_MEMORY_TRANSITION_NONE
               && !token->replacement_bytes ? 0 : EINVAL;
    return state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
           && !token->governor && !token->bytes && !token->replacement_bytes
           && token->transition_kind == WL_COLUMNAR_MEMORY_TRANSITION_NONE
           && !atomic_load_explicit(&token->owner_bits, memory_order_relaxed)
        ? 0 : EINVAL;
}

/* Descriptor pins prevent registry removal. This mandatory lookup cannot
 * invoke the lazy registry hash builder or allocate an adapter token. */
static int
wl_columnar_eval_delta_reader_drain(wl_col_session_t *sess,
    wl_columnar_source_access_reader_t *reader)
{
    if (!reader->owner && !reader->secondary_owner) return 0;
    if (!reader->secondary_owner) return EINVAL;
    for (uint32_t i = 0; i < sess->nrels; i++) {
        col_rel_t *rel = sess->rels[i];
        if (rel && &rel->descriptor_access == reader->secondary_owner)
            return wl_columnar_relation_reader_pair_release_controlled(
                rel, reader, NULL);
    }
    return EINVAL;
}

static int
wl_columnar_eval_delta_metadata_dispose(wl_col_session_t *sess,
    void *object, wl_columnar_memory_reservation_t *metadata,
    wl_columnar_memory_governor_ref_t *governor, uint64_t bytes,
    bool observer_object)
{
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    wl_columnar_eval_delta_preparation_t *preparation = NULL;
    int rc = wl_columnar_eval_stack_cleanup_observer_mandatory(
        sess, &frame, &preparation);
    if (rc) return rc;
    bool transferred = governor
        ? wl_columnar_memory_reservation_move(&preparation->metadata, metadata)
        : wl_columnar_eval_delta_credit_retire(metadata) == 0;
    if (!transferred) {
        rc = EINVAL;
    } else {
        /* The reference obligation follows the real move, without retaining
         * a second reference. No fallible operation precedes physical NULL. */
        preparation->object = object; preparation->governor = governor;
        preparation->bytes = bytes; preparation->phase = 1;
        preparation->active = true; preparation->physical_disposed = false;
        if (observer_object) {
            sess->delta_observer = NULL;
            sess->delta_observer_reserved_bytes = 0;
        } else {
            sess->delta_rollback = NULL;
            sess->delta_rollback_reserved_bytes = 0;
        }
        free(preparation->object); preparation->object = NULL;
        preparation->physical_disposed = true;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_eval_delta_test_physical_disposal)
            wl_columnar_eval_delta_test_physical_disposal(preparation,
                &preparation->metadata);
#endif
    }
    int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
    return rc > 0 ? rc : cleanup;
}

static int
wl_columnar_eval_delta_observer_discard_controlled(wl_col_session_t *sess,
    wl_columnar_eval_delta_observer_t *observer)
{
    int rc = wl_columnar_eval_stack_cleanup_retry(sess);
    if (rc) return rc;
    for (uint32_t i = 0; i < observer->count; i++) {
        rc = wl_columnar_eval_delta_reader_drain(sess,
                &observer->entries[i].reader);
        if (rc) return rc;
    }
    for (uint32_t i = 0; i < observer->count; i++) {
        free(observer->entries[i].current); observer->entries[i].current = NULL;
        observer->entries[i].current_nrows =
            observer->entries[i].current_ncols = 0;
    }
    free(observer->events); observer->events = NULL; observer->event_count = 0;
    rc = wl_columnar_eval_delta_credit_retire(&observer->staging);
    if (rc) return rc;
    observer->staging_bytes = 0;
    for (uint32_t i = 0; i < observer->count; i++) {
        free(observer->entries[i].rows); observer->entries[i].rows = NULL;
        free(observer->entries[i].name); observer->entries[i].name = NULL;
    }
    rc = wl_columnar_eval_delta_credit_retire(&observer->payload);
    if (rc) return rc;
    observer->payload_bytes = 0;
    sess->delta_observer_reserved_bytes = observer->metadata_bytes;
    if (observer->compound_hold.arena) {
        rc = wl_arena_compound_arena_gc_hold_release(&observer->compound_hold);
        if (rc) return rc;
    }
    return wl_columnar_eval_delta_metadata_dispose(sess, observer,
               &observer->metadata, observer->governor,
               observer->metadata_bytes, true);
}

static int
wl_columnar_eval_delta_observer_readers(wl_col_session_t *sess,
    wl_columnar_eval_delta_observer_t *observer)
{
    for (uint32_t i = 0; i < observer->count; i++) {
        if (observer->controlled_origin) {
            int rc = wl_columnar_eval_delta_reader_drain(sess,
                    &observer->entries[i].reader);
            if (rc) return rc;
        } else if (observer->entries[i].reader.owner) {
            int rc =
                col_rel_source_reader_release(&observer->entries[i].reader);
            assert(rc == 0);
            (void)rc;
        }
    }
    return 0;
}

static int
wl_columnar_eval_delta_observer_clear_stage(wl_col_session_t *sess)
{
    wl_columnar_eval_delta_observer_t *observer = sess->delta_observer;
    for (uint32_t i = 0; i < observer->count; i++) {
        free(observer->entries[i].current);
        observer->entries[i].current = NULL;
        observer->entries[i].current_nrows = 0;
        observer->entries[i].current_ncols = 0;
    }
    free(observer->events);
    observer->events = NULL;
    observer->event_count = 0;
    if (observer->controlled_origin) {
        int rc = wl_columnar_eval_delta_credit_retire(&observer->staging);
        if (rc) return rc;
    } else wl_columnar_eval_delta_observer_release_token(&observer->staging);
    observer->staging_bytes = 0;
    sess->delta_observer_reserved_bytes = observer->metadata_bytes
        + observer->payload_bytes;
    return 0;
}

bool
wl_columnar_eval_delta_observer_active(const wl_col_session_t *sess)
{
    return sess->delta_observer && sess->delta_observer->active;
}

bool
wl_columnar_eval_delta_observer_evaluated(const wl_col_session_t *sess)
{
    return sess->delta_observer && sess->delta_observer->evaluated;
}

uint64_t
wl_columnar_eval_delta_observer_mask(const wl_col_session_t *sess)
{
    return sess->delta_observer->affected_mask;
}

void
wl_columnar_eval_delta_observer_set_active(wl_col_session_t *sess, bool active)
{
    if (sess->delta_observer)
        sess->delta_observer->active = active;
}

void
wl_columnar_eval_delta_observer_set_evaluated(wl_col_session_t *sess)
{
    if (sess->delta_observer) {
        sess->delta_observer->evaluated = true;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_eval_delta_test_observer_boundary)
            wl_columnar_eval_delta_test_observer_boundary(sess, 1);
#endif
    }
}

void
wl_columnar_eval_delta_observer_cancel(wl_col_session_t *sess)
{
    if (sess->delta_observer)
        sess->delta_observer->cancelled = true;
}

int
wl_columnar_eval_delta_observer_discard(wl_col_session_t *sess)
{
    wl_columnar_eval_delta_observer_t *observer = sess->delta_observer;
    if (!observer)
        return 0;
    if (observer->active || sess->delta_publish_active)
        return EBUSY;
    if (observer->controlled_origin)
        return wl_columnar_eval_delta_observer_discard_controlled(sess,
                   observer);
    int reader_rc = wl_columnar_eval_delta_observer_readers(sess, observer);
    if (reader_rc) return reader_rc;
    int stage_rc = wl_columnar_eval_delta_observer_clear_stage(sess);
    if (stage_rc) return stage_rc;
    for (uint32_t i = 0; i < observer->count; i++) {
        free(observer->entries[i].rows);
        free(observer->entries[i].name);
    }
    if (observer->compound_hold.arena) {
        int rc =
            wl_arena_compound_arena_gc_hold_release(&observer->compound_hold);
        assert(rc == 0);
        (void)rc;
    }
    wl_columnar_memory_governor_ref_t *governor = observer->governor;
    wl_columnar_memory_reservation_t metadata;
    wl_columnar_memory_reservation_init(&metadata);
    wl_columnar_eval_delta_observer_release_token(&observer->payload);
    if (governor) {
        bool moved = wl_columnar_memory_reservation_move(&metadata,
                &observer->metadata);
        assert(moved);
        (void)moved;
    }
    free(observer);
    sess->delta_observer = NULL;
    sess->delta_observer_reserved_bytes = 0;
    wl_columnar_eval_delta_observer_release_token(&metadata);
    if (governor)
        wl_columnar_memory_governor_ref_release(governor);
    return 0;
}

/* Allocate the raw observer extent with its reservation at the original
 * preparation address. The session takes ownership only after full header
 * initialization and real reservation transport. No reader is acquired here. */
int
wl_columnar_eval_delta_observer_allocate_controlled(wl_col_session_t *sess,
    uint64_t mask, wl_columnar_eval_delta_preparation_t *preparation,
    wl_evaluation_control_t *control)
{
    if (!sess || sess->coordinator || !sess->plan || !preparation
        || !preparation->ready || !control)
        return EINVAL;
    int rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    if (preparation->phase == WL_COLUMNAR_EVAL_DELTA_PREPARATION_HOLD_BORROW
        || sess->delta_observer || preparation->active || preparation->object
        || preparation->governor || preparation->metadata.bytes
        || preparation->metadata.identity != &preparation->metadata)
        return EBUSY;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    uint64_t count = 0, bytes;
    for (uint32_t si = 0; si < sess->plan->stratum_count; si++) {
        rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        if (!wl_columnar_memory_size_add(count,
            sess->plan->strata[si].relation_count, &count)) return EOVERFLOW;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    if (count > UINT32_MAX || !wl_columnar_memory_size_mul(count,
        sizeof(wl_columnar_eval_delta_observer_entry_t), &bytes)
        || !wl_columnar_memory_size_add(bytes,
        sizeof(wl_columnar_eval_delta_observer_t), &bytes)
        || bytes > SIZE_MAX) return EOVERFLOW;
    rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    preparation->active = true; preparation->phase = 1;
    preparation->bytes = bytes; preparation->total = (uint32_t)count;
    preparation->initialized = 0; preparation->cursor = 0;
    preparation->physical_disposed = false;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    if (sess->memory_governor) {
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_controlled(
            wl_columnar_memory_governor_ref_get(sess->memory_governor),
            bytes, &preparation->metadata, &status, control);
        if (rc) goto done;
        if (status > WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                sess->memory_budget_denied = true;
            rc = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL;
            goto done;
        }
    }
    rc = wl_evaluation_control_charge(control, 1); if (rc) goto done;
    preparation->object = malloc((size_t)bytes);
    if (!preparation->object) {
        rc = ENOMEM; goto done;
    }
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    for (uint64_t at = 0; at < bytes;) {
        uint64_t n = bytes - at; if (n > 128u) n = 128u;
        rc = wl_evaluation_control_charge(control, n); if (rc) goto done;
        memset((char *)preparation->object + at, 0, (size_t)n); at += n;
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    }
    wl_columnar_eval_delta_observer_t *observer = preparation->object;
    wl_columnar_memory_reservation_t *tokens[] = {
        &observer->metadata, &observer->payload, &observer->staging
    };
    for (uint32_t i = 0; i < 3u; i++) {
        rc = wl_evaluation_control_charge(control, 128); if (rc) goto done;
        wl_columnar_memory_reservation_init(tokens[i]);
        preparation->initialized++;
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    }
    rc = wl_evaluation_control_charge(control, 32); if (rc) goto done;
    observer->metadata_bytes = bytes; observer->affected_mask = mask;
    observer->controlled_origin = true;
    if (sess->memory_governor) {
        if (!wl_columnar_memory_reservation_move(&observer->metadata,
            &preparation->metadata)){
            rc = EINVAL; goto done;
        }
        observer->governor = sess->memory_governor;
        wl_columnar_memory_governor_ref_retain(observer->governor);
    }
    sess->delta_observer = observer;
    sess->delta_observer_reserved_bytes = bytes;
    preparation->object = NULL; preparation->bytes = 0;
    preparation->active = false; preparation->physical_disposed = true;
    return wl_evaluation_control_charge(control, 0);
done:
    return wl_columnar_eval_delta_preparation_retire(preparation, rc);
}

int
wl_columnar_eval_delta_observer_begin_controlled(wl_col_session_t *sess,
    uint64_t mask, wl_evaluation_control_t *control)
{
    if (!sess || sess->coordinator || !sess->plan || !control
        || !sess->serial_completion.pending) return EINVAL;
    if (sess->delta_observer)
        return sess->delta_observer->controlled_origin ? 0 : EINVAL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    wl_columnar_eval_delta_preparation_t *preparation = NULL;
    int rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_cleanup_observer(sess, frame, &preparation,
            control);
    if (!rc) rc = wl_columnar_eval_delta_observer_allocate_controlled(sess,
                mask, preparation, control);
    if (!rc) rc = wl_columnar_eval_delta_hold_controlled(sess, preparation,
                false, control);
    int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
    if (cleanup > 0 && rc <= 0) rc = cleanup;
    if (rc) goto fail;
    rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc) goto fail;
    wl_columnar_eval_delta_observer_t *observer = sess->delta_observer;
    uint64_t payload_bytes = 0;
    for (uint32_t si = 0; si < sess->plan->stratum_count; si++) {
        rc = wl_evaluation_control_charge(control, 8); if (rc) goto finish;
        const wl_plan_stratum_t *sp = &sess->plan->strata[si];
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            rc = wl_evaluation_control_charge(control, 8); if (rc) goto finish;
            const char *name = sp->relations[ri].name;
            if (!name) {
                rc = EINVAL; goto finish;
            }
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
            bool found = false;
            for (uint32_t i = 0; i < observer->count; i++) {
                rc = wl_evaluation_control_charge(control, 4);
                if (rc) goto finish;
                bool equal;
                rc = wl_columnar_eval_delta_name_equal_controlled(
                    observer->entries[i].capture_name, name, &equal, control);
                if (rc) goto finish;
                if (equal) {
                    found = true; break;
                }
            }
            if (found) continue;
            rc = wl_evaluation_control_charge(control, 16); if (rc) goto finish;
            wl_columnar_eval_delta_observer_entry_t *entry =
                &observer->entries[observer->count++];
            entry->capture_name = name;
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
            col_rel_t *rel = NULL;
            rc = wl_columnar_join_controlled_lookup(sess, name, &rel, control);
            if (rc) goto finish;
            if (rel) {
                rc = wl_columnar_relation_reader_pair_acquire_controlled(rel,
                        &entry->reader, control);
                if (rc) goto finish;
                rc = wl_evaluation_control_charge(control, 16);
                if (rc) goto finish;
                if (rel->nrows > rel->capacity) {
                    rc = EINVAL; goto finish;
                }
                entry->nrows = rel->nrows; entry->ncols = rel->ncols;
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) goto finish;
                col_rel_t *retired = NULL, *seed = NULL;
                rc = wl_columnar_eval_delta_private_lookup_controlled(sess,
                        "$out$", 5u, name, &retired, control);
                if (rc) goto finish;
                rc = wl_columnar_eval_delta_private_lookup_controlled(sess,
                        "$in$", 4u, name, &seed, control); if (rc) goto finish;
                uint32_t retired_rows = 0, auxiliary_width = 0, base_rows = 0;
                if (retired) {
                    rc = wl_columnar_eval_delta_source_shape_controlled(frame,
                            retired,
                            &retired_rows, &auxiliary_width, &base_rows,
                            control);
                    if (rc) goto finish;
                    if (retired_rows && auxiliary_width != entry->ncols) {
                        rc = EINVAL; goto finish;
                    }
                }
                if (seed) {
                    uint32_t seed_rows;
                    rc = wl_columnar_eval_delta_source_shape_controlled(frame,
                            seed,
                            &seed_rows, &auxiliary_width, &base_rows, control);
                    if (rc) goto finish;
                    if (base_rows < seed_rows) {
                        if (auxiliary_width != entry->ncols) {
                            rc = EINVAL; goto finish;
                        }
                        entry->pending_nrows = seed_rows - base_rows;
                    }
                }
                rc = wl_evaluation_control_charge(control, 16);
                if (rc) goto finish;
                if (retired_rows > UINT32_MAX - entry->nrows
                    || entry->pending_nrows >
                    UINT32_MAX - entry->nrows - retired_rows){
                    rc = EOVERFLOW; goto finish;
                }
                entry->nrows += retired_rows;
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) goto finish;
            }
            size_t length; uint64_t row_bytes;
            rc = wl_columnar_eval_delta_name_length_controlled(name, &length,
                    control);
            if (rc) goto finish;
            rc = wl_evaluation_control_charge(control, 16); if (rc) goto finish;
            if (!wl_columnar_eval_delta_observer_row_bytes(entry->nrows +
                entry->pending_nrows,
                entry->ncols, &row_bytes)
                || !wl_columnar_memory_size_add(payload_bytes, row_bytes,
                &payload_bytes)
                || !wl_columnar_memory_size_add(payload_bytes, length + 1u,
                &payload_bytes)){
                rc = EOVERFLOW; goto finish;
            }
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
        }
    }
    rc = wl_columnar_eval_delta_reserve_controlled(sess, payload_bytes,
            &observer->payload, control);
    if (rc) goto finish;
    rc = wl_evaluation_control_charge(control, 16); if (rc) goto finish;
    observer->payload_bytes = payload_bytes;
    if (!wl_columnar_memory_size_add(observer->metadata_bytes, payload_bytes,
        &sess->delta_observer_reserved_bytes)) {
        rc = EOVERFLOW; goto finish;
    }
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
    for (uint32_t i = 0; i < observer->count; i++) {
        rc = wl_evaluation_control_charge(control, 8); if (rc) goto finish;
        wl_columnar_eval_delta_observer_entry_t *entry = &observer->entries[i];
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
        size_t length;
        rc = wl_columnar_eval_delta_name_length_controlled(entry->capture_name,
                &length, control);
        if (rc) goto finish;
        rc = wl_evaluation_control_charge(control, 1); if (rc) goto finish;
        entry->name = malloc(length + 1u);
        if (!entry->name) {
            rc = ENOMEM; goto finish;
        }
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
        for (size_t at = 0; at <= length;) {
            size_t chunk = length + 1u - at; if (chunk > 128u) chunk = 128u;
            rc = wl_evaluation_control_charge(control, chunk);
            if (rc) goto finish;
            memcpy(entry->name + at, entry->capture_name + at, chunk);
            at += chunk;
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
        }
        if (entry->nrows) {
            uint64_t row_bytes;
            if (!wl_columnar_eval_delta_observer_row_bytes(entry->nrows +
                entry->pending_nrows,
                entry->ncols, &row_bytes)) {
                rc = EOVERFLOW; goto finish;
            }
            rc = wl_evaluation_control_charge(control, 1); if (rc) goto finish;
            entry->rows = malloc((size_t)row_bytes);
            if (!entry->rows) {
                rc = ENOMEM; goto finish;
            }
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
            col_rel_t *rel = NULL, *retired = NULL, *seed = NULL;
            rc = wl_columnar_join_controlled_lookup(sess, entry->name, &rel,
                    control);
            if (rc) goto finish;
            if (!rel || rel->nrows > entry->nrows) {
                rc = EINVAL; goto finish;
            }
            rc = wl_columnar_eval_delta_gather_controlled(frame, rel, 0,
                    rel->nrows,
                    entry->ncols, entry->rows, control); if (rc) goto finish;
            rc = wl_columnar_eval_delta_private_lookup_controlled(sess, "$out$",
                    5u,
                    entry->name, &retired, control); if (rc) goto finish;
            if (entry->nrows > rel->nrows) {
                if (!retired) {
                    rc = EINVAL; goto finish;
                }
                rc = wl_columnar_eval_delta_gather_controlled(frame, retired, 0,
                        entry->nrows - rel->nrows, entry->ncols,
                        entry->rows + (size_t)rel->nrows * entry->ncols,
                        control);
                if (rc) goto finish;
            }
            if (entry->pending_nrows) {
                rc = wl_columnar_eval_delta_private_lookup_controlled(sess,
                        "$in$", 4u,
                        entry->name, &seed, control); if (rc) goto finish;
                if (!seed) {
                    rc = EINVAL; goto finish;
                }
                uint32_t seed_rows, seed_width, seed_base;
                rc = wl_columnar_eval_delta_source_shape_controlled(frame, seed,
                        &seed_rows, &seed_width, &seed_base, control);
                if (rc) goto finish;
                if (seed_width != entry->ncols || seed_base > seed_rows
                    || seed_rows - seed_base != entry->pending_nrows) {
                    rc = EINVAL; goto finish;
                }
                int64_t *pending = entry->rows + (size_t)entry->nrows *
                    entry->ncols;
                rc = wl_columnar_eval_delta_gather_controlled(frame, seed,
                        seed_base,
                        entry->pending_nrows, entry->ncols, pending, control);
                if (rc) goto finish;
                rc = wl_columnar_eval_delta_sort_controlled(entry->rows,
                        entry->nrows,
                        entry->ncols, control); if (rc) goto finish;
                rc = wl_columnar_eval_delta_sort_controlled(pending,
                        entry->pending_nrows,
                        entry->ncols, control); if (rc) goto finish;
                uint32_t remaining;
                rc = wl_columnar_eval_delta_subtract_controlled(entry->rows,
                        entry->nrows,
                        pending, entry->pending_nrows, entry->ncols, &remaining,
                        control);
                if (rc) goto finish;
                entry->nrows = remaining;
            }
            uint32_t normalized;
            rc = wl_columnar_eval_delta_normalize_controlled(entry->rows,
                    entry->nrows,
                    entry->ncols, &normalized, control); if (rc) goto finish;
            entry->nrows = normalized;
        }
        entry->capture_name = NULL;
    }
    for (uint32_t i = 0; i < observer->count; i++) {
        rc = wl_evaluation_control_charge(control, 4); if (rc) goto finish;
        wl_columnar_eval_delta_observer_entry_t *entry = &observer->entries[i];
        if (entry->reader.owner || entry->reader.secondary_owner) {
            col_rel_t *rel = NULL;
            rc = wl_columnar_join_controlled_lookup(sess, entry->name, &rel,
                    control);
            if (rc) goto finish;
            if (!rel) {
                rc = EINVAL; goto finish;
            }
            rc = wl_columnar_relation_reader_pair_release_controlled(rel,
                    &entry->reader, control);
            if (rc) goto finish;
        }
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
    }
    if (observer->governor) {
        bool committed = false;
        rc = wl_columnar_memory_governor_commit_controlled(&observer->metadata,
                observer, &committed, control);
        if (rc || !committed) {
            if (!rc) rc = EINVAL; goto finish;
        }
        if (payload_bytes) {
            rc =
                wl_columnar_memory_governor_commit_controlled(
                &observer->payload,
                observer, &committed, control);
            if (rc || !committed) {
                if (!rc) rc = EINVAL; goto finish;
            }
        }
    }
finish:
    cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
    if (cleanup > 0 && rc <= 0) rc = cleanup;
    if (!rc) return 0;
fail:
    cleanup = wl_columnar_eval_delta_observer_discard(sess);
    return rc > 0 ? rc : cleanup > 0 ? cleanup : rc;
}

int
wl_columnar_eval_delta_observer_begin(wl_col_session_t *sess, uint64_t mask)
{
    if (sess->delta_observer)
        return 0;
    uint64_t count = 0, bytes, payload_bytes = 0;
    const wl_plan_t *plan = sess->plan;
    for (uint32_t si = 0; si < plan->stratum_count; si++)
        if (!wl_columnar_memory_size_add(count,
            plan->strata[si].relation_count, &count))
            return EOVERFLOW;
    if (count > UINT32_MAX || !wl_columnar_memory_size_mul(count,
        sizeof(wl_columnar_eval_delta_observer_entry_t), &bytes)
        || !wl_columnar_memory_size_add(bytes,
        sizeof(wl_columnar_eval_delta_observer_t), &bytes) || bytes > SIZE_MAX
        || bytes < sizeof(wl_columnar_eval_delta_observer_t))
        return EOVERFLOW;
    wl_columnar_memory_reservation_t metadata;
    int rc = wl_columnar_eval_delta_reserve(sess, bytes, &metadata);
    if (rc != 0)
        return rc;
    wl_columnar_eval_delta_observer_t *observer = calloc(1, (size_t)bytes);
    if (!observer) {
        wl_columnar_eval_delta_observer_release_token(&metadata);
        return ENOMEM;
    }
    wl_columnar_memory_reservation_init(&observer->metadata);
    wl_columnar_memory_reservation_init(&observer->payload);
    wl_columnar_memory_reservation_init(&observer->staging);
    observer->metadata_bytes = bytes;
    observer->affected_mask = mask;
    if (sess->memory_governor) {
        bool moved = wl_columnar_memory_reservation_move(&observer->metadata,
                &metadata);
        assert(moved);
        (void)moved;
        observer->governor = sess->memory_governor;
        wl_columnar_memory_governor_ref_retain(observer->governor);
    }
    sess->delta_observer = observer;
    sess->delta_observer_reserved_bytes = bytes;
    if (sess->compound_arena) {
        rc = wl_arena_compound_arena_gc_hold_acquire(sess->compound_arena,
                &observer->compound_hold);
        if (rc != 0)
            goto fail;
    }
    for (uint32_t si = 0; si < plan->stratum_count; si++) {
        const wl_plan_stratum_t *sp = &plan->strata[si];
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            const char *name = sp->relations[ri].name;
            if (!name) {
                rc = EINVAL;
                goto fail;
            }
            bool found = false;
            for (uint32_t i = 0; i < observer->count; i++)
                if (strcmp(observer->entries[i].capture_name, name) == 0)
                    found = true;
            if (found)
                continue;
            wl_columnar_eval_delta_observer_entry_t *entry =
                &observer->entries[observer->count++];
            entry->capture_name = name;
            col_rel_t *rel = session_find_rel(sess, name);
            if (rel) {
                rc = col_rel_source_reader_acquire(rel, &entry->reader);
                if (rc != 0)
                    goto fail;
                entry->nrows = rel->nrows;
                entry->ncols = rel->ncols;
                /* Issue #2091: the baseline is the committed model, not the
                 * head as the host has since written it.  Its retired rows
                 * go back in and its uncommitted inserts, staged after the
                 * baseline rows, come out once the rows are sorted. */
                col_rel_t *retired =
                    wl_columnar_session_retired_rows(sess, name);
                col_rel_t *seed = wl_columnar_session_seed_rows(sess, name);
                uint32_t retired_nrows = retired ? retired->nrows : 0;
                if ((retired_nrows && retired->ncols != rel->ncols)
                    || (seed && seed->base_nrows < seed->nrows
                    && seed->ncols != rel->ncols)) {
                    rc = EINVAL;
                    goto fail;
                }
                if (seed && seed->base_nrows < seed->nrows)
                    entry->pending_nrows = seed->nrows - seed->base_nrows;
                if (retired_nrows > UINT32_MAX - entry->nrows
                    || entry->pending_nrows
                    > UINT32_MAX - entry->nrows - retired_nrows) {
                    rc = EOVERFLOW;
                    goto fail;
                }
                entry->nrows += retired_nrows;
            }
            uint64_t row_bytes;
            if (!wl_columnar_eval_delta_observer_row_bytes(
                    entry->nrows + entry->pending_nrows, entry->ncols,
                    &row_bytes)
                || !wl_columnar_memory_size_add(payload_bytes, row_bytes,
                &payload_bytes)
                || !wl_columnar_memory_size_add(payload_bytes,
                strlen(name) + 1, &payload_bytes)) {
                rc = EOVERFLOW;
                goto fail;
            }
        }
    }
    if (!wl_columnar_memory_size_add(bytes, payload_bytes, &bytes)) {
        rc = EOVERFLOW;
        goto fail;
    }
    rc = wl_columnar_eval_delta_reserve(sess, payload_bytes,
            &observer->payload);
    if (rc != 0)
        goto fail;
    observer->payload_bytes = payload_bytes;
    sess->delta_observer_reserved_bytes = bytes;
    for (uint32_t i = 0; i < observer->count; i++) {
        wl_columnar_eval_delta_observer_entry_t *entry = &observer->entries[i];
        entry->name = wl_strdup(entry->capture_name);
        if (!entry->name) {
            rc = ENOMEM;
            goto fail;
        }
        if (entry->nrows) {
            uint64_t row_bytes;
            if (!wl_columnar_eval_delta_observer_row_bytes(
                    entry->nrows + entry->pending_nrows, entry->ncols,
                    &row_bytes)) {
                rc = EOVERFLOW;
                goto fail;
            }
            entry->rows = malloc((size_t)row_bytes);
            if (!entry->rows) {
                rc = ENOMEM;
                goto fail;
            }
            col_rel_t *rel = session_find_rel(sess, entry->name);
            for (uint32_t row = 0; row < rel->nrows; row++)
                for (uint32_t col = 0; col < entry->ncols; col++)
                    entry->rows[(size_t)row * entry->ncols + col]
                        = rel->columns[col][row];
            col_rel_t *retired =
                wl_columnar_session_retired_rows(sess, entry->name);
            for (uint32_t row = rel->nrows; row < entry->nrows; row++)
                col_rel_row_copy_out(retired, row - rel->nrows,
                    entry->rows + (size_t)row * entry->ncols);
            col_rel_t *seed =
                wl_columnar_session_seed_rows(sess, entry->name);
            if (entry->pending_nrows && seed) {
                int64_t *pending = entry->rows
                    + (size_t)entry->nrows * entry->ncols;
                for (uint32_t row = 0; row < entry->pending_nrows; row++)
                    col_rel_row_copy_out(seed, seed->base_nrows + row,
                        pending + (size_t)row * entry->ncols);
                wl_columnar_eval_delta_observer_sort(entry->rows,
                    entry->nrows, entry->ncols);
                wl_columnar_eval_delta_observer_sort(pending,
                    entry->pending_nrows, entry->ncols);
                entry->nrows = wl_columnar_eval_delta_observer_subtract(
                    entry->rows, entry->nrows, pending, entry->pending_nrows,
                    entry->ncols);
            }
            entry->nrows = wl_columnar_eval_delta_observer_normalize(
                entry->rows, entry->nrows, entry->ncols);
        }
        entry->capture_name = NULL;
    }
    rc = wl_columnar_eval_delta_observer_readers(sess, observer);
    if (rc) goto fail;
    if (observer->governor) {
        bool committed = wl_columnar_memory_commit(&observer->metadata,
                observer)
            && (!payload_bytes || wl_columnar_memory_commit(&observer->payload,
            observer));
        assert(committed);
        (void)committed;
    }
    return 0;
fail:
    (void)wl_columnar_eval_delta_observer_discard(sess);
    return rc;
}

static void
wl_columnar_eval_delta_observer_event(
    wl_columnar_eval_delta_observer_t *observer,
    const char *name, int64_t *row, uint32_t ncols, int32_t diff)
{
    observer->events[observer->event_count++] = (wl_col_delta_event_t){
        name, row, ncols, diff
    };
}

static int
wl_columnar_eval_delta_observer_readers_controlled(wl_col_session_t *sess,
    wl_columnar_eval_delta_observer_t *observer,
    wl_evaluation_control_t *control)
{
    for (uint32_t i = 0; i < observer->count; i++) {
        int rc = wl_evaluation_control_charge(control, 4); if (rc) return rc;
        wl_columnar_eval_delta_observer_entry_t *entry = &observer->entries[i];
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        if (entry->reader.owner || entry->reader.secondary_owner) {
            col_rel_t *rel = NULL;
            rc = wl_columnar_join_controlled_lookup(sess, entry->name, &rel,
                    control);
            if (rc) return rc;
            if (!rel) return EINVAL;
            rc = wl_columnar_relation_reader_pair_release_controlled(rel,
                    &entry->reader, control);
            if (rc) return rc;
        }
    }
    return 0;
}

static int
wl_columnar_eval_delta_observer_clear_stage_controlled(wl_col_session_t *sess,
    wl_columnar_eval_delta_observer_t *observer,
    wl_evaluation_control_t *control)
{
    int rc = wl_columnar_eval_delta_observer_readers_controlled(sess, observer,
            control);
    if (rc) return rc;
    for (uint32_t i = 0; i < observer->count; i++) {
        rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        free(observer->entries[i].current); observer->entries[i].current = NULL;
        observer->entries[i].current_nrows =
            observer->entries[i].current_ncols = 0;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
    free(observer->events); observer->events = NULL; observer->event_count = 0;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    /* Physical/credit disposal is the existing checked mandatory boundary.
     * Healthy physical prefixes above are charged; no forward scan hides in it. */
    rc = wl_columnar_eval_delta_credit_retire(&observer->staging);
    if (rc) return rc;
    observer->staging_bytes = 0;
    sess->delta_observer_reserved_bytes = observer->metadata_bytes +
        observer->payload_bytes;
    return wl_evaluation_control_charge(control, 0);
}

int
wl_columnar_eval_delta_observer_prepare_controlled(wl_col_session_t *sess,
    wl_evaluation_control_t *control)
{
    if (!sess || sess->coordinator || !control) return EINVAL;
    wl_columnar_eval_delta_observer_t *observer = sess->delta_observer;
    if (!observer || observer->cancelled || observer->delivered) return 0;
    int rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    if (!observer->controlled_origin || !observer->evaluated
        || observer->metadata_bytes < sizeof(*observer)
        || (observer->metadata_bytes - sizeof(*observer)) %
        sizeof(*observer->entries)
        || observer->count >
        (observer->metadata_bytes - sizeof(*observer)) /
        sizeof(*observer->entries))
        return EINVAL;
    uint64_t baseline_bytes;
    if (!wl_columnar_memory_size_add(observer->metadata_bytes,
        observer->payload_bytes, &baseline_bytes)) return EOVERFLOW;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc) return rc;
    rc = wl_columnar_eval_delta_observer_clear_stage_controlled(sess, observer,
            control);
    if (rc) goto finish;
    uint64_t bytes = 0, event_capacity = 0, event_bytes;
    for (uint32_t i = 0; i < observer->count; i++) {
        rc = wl_evaluation_control_charge(control, 16); if (rc) goto finish;
        wl_columnar_eval_delta_observer_entry_t *entry = &observer->entries[i];
        if (entry->capture_name || !entry->name) {
            rc = EINVAL; goto finish;
        }
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
        col_rel_t *rel = NULL;
        rc = wl_columnar_join_controlled_lookup(sess, entry->name, &rel,
                control);
        if (rc) goto finish;
        if (rel) {
            rc = wl_columnar_relation_reader_pair_acquire_controlled(rel,
                    &entry->reader, control);
            if (rc) goto finish;
            rc = wl_evaluation_control_charge(control, 16); if (rc) goto finish;
            if (rel->nrows > rel->capacity) {
                rc = EINVAL; goto finish;
            }
            entry->current_nrows = rel->nrows;
            entry->current_ncols = rel->ncols;
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
        }
        uint64_t row_bytes;
        rc = wl_evaluation_control_charge(control, 16); if (rc) goto finish;
        if (!wl_columnar_eval_delta_observer_row_bytes(entry->current_nrows,
            entry->current_ncols, &row_bytes)
            || !wl_columnar_memory_size_add(bytes, row_bytes, &bytes)
            || !wl_columnar_memory_size_add(event_capacity,
            (uint64_t)entry->nrows + entry->current_nrows, &event_capacity)){
            rc = EOVERFLOW; goto finish;
        }
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
    }
    rc = wl_evaluation_control_charge(control, 16); if (rc) goto finish;
    if (!wl_columnar_memory_size_mul(event_capacity, sizeof(*observer->events),
        &event_bytes)
        || event_bytes > SIZE_MAX || !wl_columnar_memory_size_add(bytes,
        event_bytes, &bytes)
        || bytes > SIZE_MAX || bytes > UINT64_MAX - baseline_bytes) {
        rc = EOVERFLOW; goto finish;
    }
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
    rc = wl_columnar_eval_delta_reserve_controlled(sess, bytes,
            &observer->staging, control);
    if (rc) goto finish;
    rc = wl_evaluation_control_charge(control, 8); if (rc) goto finish;
    observer->staging_bytes = bytes;
    sess->delta_observer_reserved_bytes = observer->metadata_bytes +
        observer->payload_bytes + bytes;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
    if (event_bytes) {
        rc = wl_evaluation_control_charge(control, 1); if (rc) goto finish;
        observer->events = malloc((size_t)event_bytes);
        if (!observer->events) {
            rc = ENOMEM; goto finish;
        }
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
    }
    for (uint32_t i = 0; i < observer->count; i++) {
        rc = wl_evaluation_control_charge(control, 8); if (rc) goto finish;
        wl_columnar_eval_delta_observer_entry_t *entry = &observer->entries[i];
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
        if (entry->current_nrows) {
            uint64_t row_bytes;
            if (!wl_columnar_eval_delta_observer_row_bytes(entry->current_nrows,
                entry->current_ncols, &row_bytes)) {
                rc = EOVERFLOW; goto finish;
            }
            rc = wl_evaluation_control_charge(control, 1); if (rc) goto finish;
            entry->current = malloc((size_t)row_bytes);
            if (!entry->current) {
                rc = ENOMEM; goto finish;
            }
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
            col_rel_t *rel = NULL;
            rc = wl_columnar_join_controlled_lookup(sess, entry->name, &rel,
                    control);
            if (rc) goto finish;
            if (!rel) {
                rc = EINVAL; goto finish;
            }
            rc = wl_columnar_eval_delta_gather_controlled(frame, rel, 0,
                    entry->current_nrows, entry->current_ncols, entry->current,
                    control);
            if (rc) goto finish;
            uint32_t normalized;
            rc = wl_columnar_eval_delta_normalize_controlled(entry->current,
                    entry->current_nrows, entry->current_ncols, &normalized,
                    control);
            if (rc) goto finish;
            entry->current_nrows = normalized;
        }
        uint32_t old = 0, current = 0;
        while (old < entry->nrows || current < entry->current_nrows) {
            rc = wl_evaluation_control_charge(control, 8); if (rc) goto finish;
            int comparison = old == entry->nrows ? 1
                : current == entry->current_nrows ? -1
                : entry->ncols != entry->current_ncols ? -1 : 0;
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
            if (!comparison) {
                rc = wl_columnar_eval_delta_compare_controlled(
                    entry->rows + (size_t)old * entry->ncols,
                    entry->current + (size_t)current * entry->current_ncols,
                    entry->ncols, &comparison, control);
                if (rc) goto finish;
            }
            if (comparison) {
                rc = wl_evaluation_control_charge(control, 8);
                if (rc) goto finish;
                if (observer->event_count >= event_capacity ||
                    !observer->events){
                    rc = EINVAL; goto finish;
                }
                bool removal = comparison < 0;
                observer->events[observer->event_count++] =
                    (wl_col_delta_event_t){
                    entry->name,
                    removal ? entry->rows + (size_t)old * entry->ncols
                        : entry->current + (size_t)current *
                    entry->current_ncols,
                    removal ? entry->ncols : entry->current_ncols,
                    removal ? -1 : 1
                };
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) goto finish;
            }
            if (comparison <= 0) old++;
            if (comparison >= 0) current++;
        }
    }
    rc = wl_columnar_eval_delta_observer_readers_controlled(sess, observer,
            control);
    if (rc) goto finish;
    if (observer->governor && bytes) {
        bool committed = false;
        rc = wl_columnar_memory_governor_commit_controlled(&observer->staging,
                observer, &committed, control);
        if (!rc && !committed) rc = EINVAL;
    }
finish:
    {
        int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
        if (cleanup > 0 && rc <= 0) rc = cleanup;
        if (rc) {
            cleanup = wl_columnar_eval_delta_observer_readers(sess, observer);
            if (!cleanup) cleanup =
                    wl_columnar_eval_delta_observer_clear_stage(sess);
            if (cleanup > 0 && rc <= 0) rc = cleanup;
        }
        return rc;
    }
}

int
wl_columnar_eval_delta_observer_prepare(wl_col_session_t *sess)
{
    wl_columnar_eval_delta_observer_t *observer = sess->delta_observer;
    if (!observer || observer->cancelled || observer->delivered)
        return 0;
    int rc = wl_columnar_eval_delta_observer_readers(sess, observer);
    if (rc) return rc;
    rc = wl_columnar_eval_delta_observer_clear_stage(sess);
    if (rc) return rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_delta_test_observer_boundary)
        wl_columnar_eval_delta_test_observer_boundary(sess, 2);
#endif
    uint64_t bytes = 0, event_capacity = 0, event_bytes;
    for (uint32_t i = 0; i < observer->count; i++) {
        wl_columnar_eval_delta_observer_entry_t *entry = &observer->entries[i];
        col_rel_t *rel = session_find_rel(sess, entry->name);
        if (rel) {
            rc = col_rel_source_reader_acquire(rel, &entry->reader);
            if (rc != 0)
                goto fail;
            entry->current_nrows = rel->nrows;
            entry->current_ncols = rel->ncols;
        }
        uint64_t row_bytes;
        if (!wl_columnar_eval_delta_observer_row_bytes(entry->current_nrows,
            entry->current_ncols, &row_bytes)
            || !wl_columnar_memory_size_add(bytes, row_bytes, &bytes)
            || !wl_columnar_memory_size_add(event_capacity,
            (uint64_t)entry->nrows + entry->current_nrows, &event_capacity)) {
            rc = EOVERFLOW;
            goto fail;
        }
    }
    if (!wl_columnar_memory_size_mul(event_capacity, sizeof(*observer->events),
        &event_bytes) || event_bytes > SIZE_MAX
        || !wl_columnar_memory_size_add(bytes, event_bytes, &bytes)
        || bytes > SIZE_MAX
        || bytes >
        UINT64_MAX - observer->metadata_bytes - observer->payload_bytes) {
        rc = EOVERFLOW;
        goto fail;
    }
    rc = wl_columnar_eval_delta_reserve(sess, bytes, &observer->staging);
    if (rc != 0)
        goto fail;
    observer->staging_bytes = bytes;
    sess->delta_observer_reserved_bytes += bytes;
    if (event_bytes) {
        observer->events = malloc((size_t)event_bytes);
        if (!observer->events) {
            rc = ENOMEM;
            goto fail;
        }
    }
    for (uint32_t i = 0; i < observer->count; i++) {
        wl_columnar_eval_delta_observer_entry_t *entry = &observer->entries[i];
        if (entry->current_nrows) {
            uint64_t row_bytes;
            if (!wl_columnar_eval_delta_observer_row_bytes(
                    entry->current_nrows, entry->current_ncols, &row_bytes)) {
                rc = EOVERFLOW;
                goto fail;
            }
            entry->current = malloc((size_t)row_bytes);
            if (!entry->current) {
                rc = ENOMEM;
                goto fail;
            }
            col_rel_t *rel = session_find_rel(sess, entry->name);
            for (uint32_t row = 0; row < entry->current_nrows; row++)
                for (uint32_t col = 0; col < entry->current_ncols; col++)
                    entry->current[(size_t)row * entry->current_ncols + col]
                        = rel->columns[col][row];
            entry->current_nrows = wl_columnar_eval_delta_observer_normalize(
                entry->current, entry->current_nrows, entry->current_ncols);
        }
        uint32_t old = 0, current = 0;
        while (old < entry->nrows || current < entry->current_nrows) {
            int cmp = old == entry->nrows ? 1
                : current == entry->current_nrows ? -1
                : entry->ncols != entry->current_ncols ? -1
                : memcmp(entry->rows + (size_t)old * entry->ncols,
                    entry->current + (size_t)current * entry->current_ncols,
                    (size_t)entry->ncols * sizeof(int64_t));
            if (cmp <= 0) {
                if (cmp < 0)
                    wl_columnar_eval_delta_observer_event(observer, entry->name,
                        entry->rows + (size_t)old * entry->ncols, entry->ncols,
                        -1);
                old++;
            }
            if (cmp >= 0) {
                if (cmp > 0)
                    wl_columnar_eval_delta_observer_event(observer, entry->name,
                        entry->current + (size_t)current * entry->current_ncols,
                        entry->current_ncols, +1);
                current++;
            }
        }
    }
    rc = wl_columnar_eval_delta_observer_readers(sess, observer);
    if (rc) goto fail;
    if (observer->governor && bytes) {
        bool committed = wl_columnar_memory_commit(&observer->staging,
                observer);
        assert(committed);
        (void)committed;
    }
    return 0;
fail:
    {
        int cleanup = wl_columnar_eval_delta_observer_readers(sess, observer);
        if (!cleanup) cleanup =
                wl_columnar_eval_delta_observer_clear_stage(sess);
        return rc > 0 ? rc : cleanup > 0 ? cleanup : rc;
    }
}

/* All event selection is admitted before the first callback. Host callback
 * work is excluded; cancellation there is sticky for the next host attempt. */
int
wl_columnar_eval_delta_observer_publish_controlled(wl_col_session_t *sess,
    wl_evaluation_control_t *control)
{
    if (!sess || !control || sess->coordinator) return EINVAL;
    wl_columnar_eval_delta_observer_t *observer = sess->delta_observer;
    if (!observer || observer->delivered) return 0;
    int rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    if (!observer->controlled_origin || !observer->evaluated
        || sess->delta_publish_active
        || (observer->event_count && !observer->events)) return EINVAL;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    for (size_t i = 0; i < observer->event_count; i++) {
        /* This pays both validation and the later fixed event selection. */
        rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        const wl_col_delta_event_t *event = &observer->events[i];
        if (!event->relation || !event->row
            || (event->diff != 1 && event->diff != -1)) return EINVAL;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    /* The last poll precedes the fixed admitted delivery tail. */
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    sess->delta_publish_active = true;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_delta_test_observer_boundary)
        wl_columnar_eval_delta_test_observer_boundary(sess, 2);
#endif
    for (size_t i = 0; i < observer->event_count && !observer->cancelled; i++) {
        wirelog_on_delta_fn callback = sess->delta_cb;
        void *data = sess->delta_data;
        if (!callback) {
            observer->cancelled = true; break;
        }
        const wl_col_delta_event_t *event = &observer->events[i];
        callback(event->relation, event->row, event->ncols, event->diff, data);
    }
    sess->delta_publish_active = false;
    observer->delivered = true;
    return 0;
}

void
wl_columnar_eval_delta_observer_publish(wl_col_session_t *sess)
{
    wl_columnar_eval_delta_observer_t *observer = sess->delta_observer;
    if (!observer || observer->delivered)
        return;
    sess->delta_publish_active = true;
    for (size_t i = 0; i < observer->event_count && !observer->cancelled; i++) {
        wirelog_on_delta_fn callback = sess->delta_cb;
        void *data = sess->delta_data;
        if (!callback) {
            observer->cancelled = true;
            break;
        }
        wl_col_delta_event_t *event = &observer->events[i];
        callback(event->relation, event->row, event->ncols, event->diff, data);
    }
    sess->delta_publish_active = false;
    observer->delivered = true;
}

int
wl_columnar_eval_delta_observer_finish(wl_col_session_t *sess)
{
    bool gc_requested = sess->delta_observer->gc_requested;
    sess->delta_observer->active = false;
    int rc = wl_columnar_eval_delta_observer_discard(sess);
    if (rc == 0 && gc_requested && !sess->coordinator && sess->compound_arena
        && sess->rotation_ops && sess->rotation_ops->gc_epoch_boundary)
        sess->rotation_ops->gc_epoch_boundary(sess);
    return rc;
}

bool
wl_columnar_eval_delta_rollback_active(const wl_col_session_t *sess)
{
    return sess && sess->delta_rollback && sess->delta_rollback->active;
}

bool
wl_columnar_eval_delta_defer_gc(wl_col_session_t *sess)
{
    if (!sess)
        return false;
    if (sess->delta_observer) {
        sess->delta_observer->gc_requested = true;
        return true;
    }
    if (!sess->delta_rollback)
        return false;
    sess->delta_rollback->gc_requested = true;
    return true;
}

int
wl_columnar_eval_delta_rollback_discard(wl_col_session_t *sess)
{
    wl_columnar_eval_delta_rollback_t *record;
    wl_columnar_memory_reservation_t metadata, payload;
    if (!sess)
        return EINVAL;
    record = sess->delta_rollback;
    if (!record)
        return 0;
    if (record->active || sess->cleanup_active)
        return EBUSY;
    if (record->controlled_origin) {
        int rc = wl_columnar_eval_stack_cleanup_retry(sess);
        if (rc) return rc;
        for (uint32_t i = 0; i < record->count; i++) {
            rc = wl_columnar_eval_delta_reader_drain(sess,
                    &record->entries[i].capture_reader);
            if (rc) return rc;
        }
        for (uint32_t i = 0; i < record->count; i++) {
            free(record->entries[i].rows); record->entries[i].rows = NULL;
            free(record->entries[i].name); record->entries[i].name = NULL;
        }
        rc = wl_columnar_eval_delta_credit_retire(&record->payload_reservation);
        if (rc) return rc;
        record->payload_reserved = false;
        if (record->compound_hold.arena) {
            rc =
                wl_arena_compound_arena_gc_hold_release(&record->compound_hold);
            if (rc) return rc;
        }
        return wl_columnar_eval_delta_metadata_dispose(sess, record,
                   &record->metadata_reservation, record->governor,
                   sess->delta_rollback_reserved_bytes, false);
    }
    if (sess->cleanup_pending) return EBUSY;
    for (uint32_t i = 0; i < record->count; i++) {
        wl_columnar_eval_delta_snapshot_t *entry = &record->entries[i];
        if (entry->capture_reader.owner) {
            int rc = col_rel_source_reader_release(&entry->capture_reader);
            if (rc != 0)
                return rc;
        }
    }
    wl_columnar_memory_governor_ref_t *governor = record->governor;
    bool payload_reserved = record->payload_reserved;
    wl_columnar_memory_reservation_init(&metadata);
    wl_columnar_memory_reservation_init(&payload);
    if (governor) {
        if (!wl_columnar_memory_reservation_move(&metadata,
            &record->metadata_reservation))
            return EINVAL;
        if (record->payload_reserved
            && !wl_columnar_memory_reservation_move(&payload,
            &record->payload_reservation)) {
            (void)wl_columnar_memory_reservation_move(
                &record->metadata_reservation, &metadata);
            return EINVAL;
        }
    }
    if (record->compound_hold.arena) {
        int rc =
            wl_arena_compound_arena_gc_hold_release(&record->compound_hold);
        if (rc != 0) {
            if (governor) {
                (void)wl_columnar_memory_reservation_move(
                    &record->metadata_reservation, &metadata);
                if (record->payload_reserved)
                    (void)wl_columnar_memory_reservation_move(
                        &record->payload_reservation, &payload);
            }
            return rc;
        }
    }
    for (uint32_t i = 0; i < record->count; i++) {
        free(record->entries[i].rows);
        free(record->entries[i].name);
    }
    sess->delta_rollback = NULL;
    sess->delta_rollback_reserved_bytes = 0;
    free(record);
    if (governor) {
        bool released = !payload_reserved ||
            wl_columnar_memory_release(&payload);
        assert(released);
        released = wl_columnar_memory_release(&metadata);
        assert(released);
        (void)released;
        wl_columnar_memory_governor_ref_release(governor);
    }
    return 0;
}

/* Bind the original active snapshot's publication flag before detaching its
 * protected canonical descriptor. No entry or authority is copied. */
int
wl_columnar_eval_delta_detach_target_controlled(wl_col_session_t *sess,
    const wl_columnar_eval_delta_rollback_t *record, uint32_t index,
    const col_rel_t *rel, bool **detached, bool **alias,
    wl_evaluation_control_t *control)
{
    if (!sess || !rel || !detached || !alias || !control) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    if (!record || sess->delta_rollback != record || !record->controlled_origin
        || !record->active || index >= record->count) return EINVAL;
    wl_columnar_eval_delta_snapshot_t *entry =
        &sess->delta_rollback->entries[index];
    if (!entry->identity || entry->identity != rel->relation_identity
        || entry->completed || entry->detached
        || entry->alias != (rel->storage_owner != rel)) return EINVAL;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    *detached = &entry->detached;
    *alias = &entry->alias;
    return 0;
}

/* The saved flat image belongs to this exact inactive recovery record.
 * Return scalar image descriptors only; authority never leaves its owner. */
int
wl_columnar_eval_delta_recovery_detach_target_controlled(wl_col_session_t *sess,
    const wl_columnar_eval_delta_rollback_t *record, uint32_t index,
    const col_rel_t *rel, bool **detached, bool **alias, uint32_t *saved_nrows,
    wl_evaluation_control_t *control)
{
    if (!sess || !rel || !detached || !alias || !saved_nrows ||
        !control) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 24); if (rc) return rc;
    if (!record || sess->delta_rollback != record || !record->controlled_origin
        || record->active || record->retained || !record->evaluation_error
        || index >= record->count) return EINVAL;
    wl_columnar_eval_delta_snapshot_t *entry =
        &sess->delta_rollback->entries[index];
    if (!entry->identity || entry->identity != rel->relation_identity
        || !entry->detached || entry->completed || entry->alias) return EINVAL;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    *detached = &entry->detached; *alias = &entry->alias;
    *saved_nrows = entry->nrows;
    return 0;
}

int
wl_columnar_eval_delta_restore_snapshot_controlled(wl_col_session_t *sess,
    const wl_columnar_eval_delta_rollback_t *record, uint32_t index,
    uint64_t expected_identity, bool overwrite, const int64_t **rows,
    uint32_t *nrows, uint32_t *ncols, bool **completed, bool **alias,
    wl_evaluation_control_t *control)
{
    if (!control || !rows || !nrows || !ncols || !completed ||
        !alias) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    if (!sess || !record || sess->delta_rollback != record ||
        !record->controlled_origin || record->active
        || record->retained || index >= record->count) return EINVAL;
    wl_columnar_eval_delta_snapshot_t *entry =
        &sess->delta_rollback->entries[index];
    if (!entry->detached || entry->completed || entry->alias || !entry->rows
        || !entry->nrows || !entry->ncols
        || entry->identity != expected_identity
        || overwrite != (record->evaluation_error != 0)) return EINVAL;
    uint64_t cells, bytes;
    if (!wl_columnar_memory_size_mul(entry->nrows, entry->ncols, &cells)
        || !wl_columnar_memory_size_mul(cells, sizeof(int64_t), &bytes)
        || bytes > SIZE_MAX) return EOVERFLOW;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    *rows = entry->rows; *nrows = entry->nrows; *ncols = entry->ncols;
    *completed = &entry->completed; *alias = &entry->alias;
    return 0;
}

/* Content has already been consumed. Only the actual original lease is
 * eligible for retry; a negative release tail leaves this phase armed. */
int
wl_columnar_eval_delta_rollback_empty_controlled(wl_col_session_t *sess,
    wl_columnar_eval_stack_cleanup_frame_t *frame, uint32_t index,
    col_rel_t *rel, wl_evaluation_control_t *control)
{
    if (!sess || !frame || !rel || !control || sess->cleanup_active != frame)
        return EINVAL;
    int rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    wl_columnar_eval_delta_rollback_t *record = sess->delta_rollback;
    if (!record || !record->controlled_origin || record->active ||
        record->retained
        || !record->evaluation_error || index >= record->count) return EINVAL;
    wl_columnar_eval_delta_snapshot_t *entry = &record->entries[index];
    wl_columnar_relation_bulk_append_t *bulk =
        wl_columnar_eval_stack_cleanup_bulk_append(frame);
    if (!entry->detached || entry->completed || entry->alias || entry->nrows
        || entry->identity != rel->relation_identity || rel->columns
        || !bulk ||
        bulk->borrowed_mode != WL_COLUMNAR_RELATION_BULK_DELTA_DETACH
        || bulk->dst != rel || bulk->active || bulk->old_columns
        || bulk->descriptor_writer.owner || bulk->writer.owner
        || bulk->detach.reader.owner || bulk->detach.old_shared
        || !bulk->detach.metadata_settled || !bulk->detach.payload_settled)
        return EINVAL;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    /* A settled real empty content decision; no replacement was allocated. */
    entry->completed = true; entry->alias = true;
    return wl_evaluation_control_charge(control, 0);
}

int
wl_columnar_eval_delta_rollback_lease_controlled(wl_col_session_t *sess,
    wl_columnar_eval_stack_cleanup_frame_t *frame, uint32_t index,
    col_rel_t *rel, wl_evaluation_control_t *control)
{
    if (!sess || !frame || !rel || !control) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    wl_columnar_eval_delta_rollback_t *record = sess->delta_rollback;
    if (!record || !record->controlled_origin || record->active
        || index >= record->count || sess->cleanup_active != frame)
        return EINVAL;
    wl_columnar_eval_delta_snapshot_t *entry = &record->entries[index];
    if (!entry->detached || !entry->completed || !entry->alias
        || entry->identity != rel->relation_identity) return EINVAL;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    wl_columnar_relation_source_group_t *group =
        wl_columnar_eval_stack_cleanup_sources(frame);
    if (!group || group->descriptor_count || group->owner_count) return EBUSY;
    rc = wl_columnar_relation_source_group_acquire_controlled(group, rel, NULL,
            control);
    if (rc) return wl_columnar_relation_source_group_drain(group, rc);
    rc = wl_evaluation_control_charge(control, 8);
    if (!rc && group->input_owner[0] >= group->owner_count) rc = EINVAL;
    bool self = !rc && group->owners[group->input_owner[0]].owner == rel;
    if (!rc) rc = wl_evaluation_control_charge(control, 0);
    if (!rc) rc = wl_columnar_relation_source_group_finish_controlled(group,
                control);
    if (rc) return wl_columnar_relation_source_group_drain(group, rc);
    /* Raw recovery retires a source lease only after self ownership. A live
    * alias keeps its real node, rather than releasing an unrelated root. */
    if (self) {
        rc = wl_columnar_session_retire_source_lease_controlled(sess, frame,
                rel, control);
        if (rc) return rc;
    }
    /* The fixed acknowledgment was admitted before retiring the real node. */
    entry->alias = false;
    return wl_evaluation_control_charge(control, 0);
}

/* The external replacement mode selects this record before either overlay
 * is read. Original stage pointers retire before its original credits. */
int
wl_columnar_eval_delta_replacement_retire(
    wl_columnar_eval_delta_replacement_t *record,
    wl_columnar_relation_bulk_append_t *bulk, eval_entry_t *candidate,
    wl_columnar_memory_governor_ref_t *governor, int result)
{
    if (record && record->phase >= WL_COLUMNAR_OBSERVER_IMAGE_CLEAR) {
        int cleanup = wl_columnar_relation_replacement_image_retire(record,
                bulk, NULL, 0);
        if (cleanup) return result > 0 ? result : cleanup;
        if (record->published) return result;
        /* True unpublished reversal now exposes the original stage purpose. */
    }
    if (!record || !bulk || !candidate || record->initialized_tokens > 3
        || (record->phase > 3
        && (record->phase < WL_COLUMNAR_OBSERVER_CANDIDATE_PAIR
        || record->phase > WL_COLUMNAR_OBSERVER_CANDIDATE_DONE)))
        return result > 0 ? result : EINVAL;
    if (record->phase >= WL_COLUMNAR_OBSERVER_CANDIDATE_PAIR) {
        int cleanup = wl_columnar_relation_observer_candidate_retire(record,
                bulk, candidate, governor, NULL, 0);
        if (cleanup) return result > 0 ? result : cleanup;
        /* Candidate completion proves the separately prepared stage is
         * complete. This is mandatory abandonment, not forward replay. */
        record->phase = 3;
    }
    int physical = wl_columnar_relation_replacement_stage_retire(record, bulk);
    if (physical) return result > 0 ? result : physical;
    wl_columnar_memory_reservation_t *temporary[5] = {
        &bulk->scratch, &bulk->metadata_pending, &bulk->previous,
        &bulk->old_metadata, &bulk->pending.fresh
    };
    for (uint32_t i = 0; i < 5; i++) {
        if (!temporary[i]->bytes && !temporary[i]->governor) continue;
        int cleanup = wl_columnar_eval_delta_credit_retire(temporary[i]);
        if (cleanup) return result > 0 ? result : cleanup;
    }
    if (candidate->rel) {
        /* The real stage and temporary image have retired. A complete
         * original result still needs checked embedded credit transport. */
        record->phase = 3;
        int cleanup = wl_columnar_relation_observer_candidate_retire(record,
                bulk, candidate, governor, NULL, 0);
        if (cleanup) return result > 0 ? result : cleanup;
    }
    col_rel_replacement_t *transaction = &record->transaction;
    wl_columnar_memory_reservation_t *credits[3] = {
        &transaction->reservation, &transaction->metadata_reservation,
        &transaction->dedup_reservation
    };
    for (uint32_t i = 0; i < record->initialized_tokens; i++) {
        int cleanup = wl_columnar_eval_delta_credit_retire(credits[i]);
        if (cleanup) return result > 0 ? result : cleanup;
    }
    if (transaction->writer.owner || transaction->writer.secondary_owner
        || transaction->writer_acquired) {
        int cleanup =
            wl_columnar_relation_replacement_writer_release_controlled(
            record->target, transaction, NULL);
        if (cleanup && result <= 0) result = cleanup;
    }
    return result;
}

static int
wl_columnar_eval_delta_rollback_retry_controlled(wl_col_session_t *sess,
    wl_evaluation_control_t *control)
{
    /* Drain consumed physical/credit work before inspecting the persistent
    * snapshot. A completed content decision may still own a real lease. */
    if (sess->cleanup_active) return EBUSY;
    int rc = wl_columnar_eval_stack_cleanup_retry(sess);
    if (rc) return rc;
    /* Host cleanup also reaches this dispatcher before backend BEGIN.
     * Only consumed physical drain above is legal outside a real attempt. */
    if (!control) return EBUSY;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc == EINVAL ? EBUSY : rc;
    wl_columnar_eval_delta_rollback_t *record = sess->delta_rollback;
    if (!record || record->active) return 0;
    if (!record->controlled_origin) return EINVAL;
    for (uint32_t i = 0; i < record->count; i++) {
        rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
        wl_columnar_eval_delta_snapshot_t *entry = &record->entries[i];
        bool lease_pending = entry->detached && entry->completed &&
            entry->alias;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        if (!entry->detached || (entry->completed && !lease_pending)) continue;
        col_rel_t *rel = NULL;
        rc = wl_columnar_join_controlled_lookup(sess, entry->name, &rel,
                control);
        if (rc) return rc;
        rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
        bool same = rel && rel->relation_identity == entry->identity;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        if (!same) {
            /* A consumed lease cannot be abandoned as an absent snapshot. */
            if (lease_pending) return EINVAL;
            entry->completed = true;
            continue;
        }
        if (record->retained && !lease_pending) {
            /* Final canonical content stays final, including empty heads. */
            entry->completed = true;
            continue;
        }
        wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
        rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
        if (rc) return rc;
        if (!lease_pending) {
            bool incomplete = record->evaluation_error != 0;
            wl_columnar_relation_bulk_append_t *bulk =
                wl_columnar_eval_stack_cleanup_bulk_append(frame);
            if (!bulk) rc = EINVAL;
            if (!rc && incomplete)
                rc = wl_columnar_relation_delta_recovery_detach_controlled(
                    bulk, sess, record, i, rel, control);
            if (!rc && incomplete && !entry->nrows)
                rc = wl_columnar_eval_delta_rollback_empty_controlled(
                    sess, frame, i, rel, control);
            if (!rc && (!incomplete || entry->nrows)) {
                /* DETACH and exact restoration have distinct BULK modes.
                 * Really retire the former before interpreting the latter. */
                int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
                if (cleanup) rc = cleanup;
                if (!rc) rc = wl_columnar_eval_stack_cleanup_acquire(
                        sess, &frame, control);
                if (!rc) {
                    bulk = wl_columnar_eval_stack_cleanup_bulk_append(frame);
                    rc = bulk ? wl_columnar_relation_restore_flat_controlled(
                        bulk, rel, sess, record, i, entry->identity,
                        incomplete, control) : EINVAL;
                }
            }
        }
        if (!rc) rc = wl_columnar_eval_delta_rollback_lease_controlled(
                sess, frame, i, rel, control);
        int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
        if (cleanup && rc <= 0) rc = cleanup;
        if (rc) return rc;
    }
    return wl_columnar_eval_delta_rollback_discard(sess);
}

int
wl_columnar_eval_delta_rollback_retry(wl_col_session_t *sess)
{
    if (!sess)
        return EINVAL;
    wl_columnar_eval_delta_rollback_t *record = sess->delta_rollback;
    if (!record || record->active)
        return 0;
    if (record->controlled_origin)
        return wl_columnar_eval_delta_rollback_retry_controlled(sess,
                   sess->base.evaluation_control);
    if (sess->cleanup_pending || sess->cleanup_active)
        return EBUSY;
    if (record->retained) {
        /* The stratum ran to completion, so an empty head is empty because
         * this step emptied it.  restore_flat already declines a populated
         * head, so restoring here would touch only the correctly-emptied
         * ones -- exactly the rows that must stay gone.  Mark every entry
         * done and fall through, so the record and its reservation are still
         * discarded on this call. */
        for (uint32_t i = 0; i < record->count; i++)
            record->entries[i].completed = true;
    }
    int result = 0;
    for (uint32_t i = 0; i < record->count; i++) {
        wl_columnar_eval_delta_snapshot_t *entry = &record->entries[i];
        if (!entry->detached || entry->completed)
            continue;
        col_rel_t *rel = session_find_rel(sess, entry->name);
        /* Replacement/removal is not an invitation to overwrite new state. */
        if (!rel || rel->relation_identity != entry->identity) {
            entry->completed = true;
            continue;
        }
        int rc;
        /* An incomplete evaluation may have populated this detached head
         * before failing on a later relation.  restore_flat intentionally
         * preserves populated storage for the completed-evaluation cleanup
         * path, but doing that here would leave a mixed old/new stratum.  Drop
         * the partial storage first so the captured pre-step snapshot is the
         * state that becomes visible again. */
        bool incomplete = !record->retained && record->evaluation_error != 0;
        if (incomplete && rel->columns
            && (entry->nrows == 0 || rel->storage_owner != rel)) {
            rc = wl_columnar_relation_delta_detach(rel, entry->identity);
            if (rc != 0) {
                if (result == 0)
                    result = rc;
                continue;
            }
        }
        if (incomplete && entry->nrows == 0)
            rc = 0;
        else
            rc = incomplete
                ? wl_columnar_relation_delta_restore_flat_overwrite(rel,
                    entry->identity, entry->rows, entry->nrows,
                    entry->ncols)
                : wl_columnar_relation_delta_restore_flat(rel,
                    entry->identity, entry->rows, entry->nrows,
                    entry->ncols);
        /* Retire any source lease only after the captured state is restored;
         * a failed detach or restore remains retryable. */
        if (rc == 0 && rel->storage_owner == rel)
            rc = wl_columnar_session_retire_source_lease(sess, rel);
        if (rc == 0)
            entry->completed = true;
        else if (result == 0)
            result = rc;
    }
    return result != 0 ? result : wl_columnar_eval_delta_rollback_discard(sess);
}

/* The original preparation record owns every partial constructor byte. */
static int
wl_columnar_eval_delta_rollback_allocate_controlled(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess, wl_columnar_eval_delta_preparation_t *prep,
    wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    if (!sp || !prep || !prep->ready || prep->active || prep->object
        || prep->governor || prep->metadata.bytes || sess->delta_rollback
        || prep->phase == WL_COLUMNAR_EVAL_DELTA_PREPARATION_HOLD_BORROW)
        return EBUSY;
    uint64_t bytes;
    if (!wl_columnar_memory_size_mul(sp->relation_count,
        sizeof(wl_columnar_eval_delta_snapshot_t), &bytes)
        || !wl_columnar_memory_size_add(bytes,
        sizeof(wl_columnar_eval_delta_rollback_t), &bytes)
        || bytes > SIZE_MAX) return EOVERFLOW;
    rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    prep->active = true; prep->phase = 1; prep->bytes = bytes;
    prep->total = sp->relation_count; prep->initialized = 0;
    prep->cursor = 0; prep->physical_disposed = false;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    if (sess->memory_governor) {
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_controlled(
            wl_columnar_memory_governor_ref_get(sess->memory_governor), bytes,
            &prep->metadata, &status, control);
        if (rc) goto done;
        if (status > WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                sess->memory_budget_denied = true;
            rc = status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status ==
                WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
            goto done;
        }
    }
    rc = wl_evaluation_control_charge(control, 1); if (rc) goto done;
    prep->object = malloc((size_t)bytes);
    if (!prep->object) {
        rc = ENOMEM; goto done;
    }
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    for (uint64_t at = 0; at < bytes;) {
        uint64_t n = bytes - at; if (n > 128u) n = 128u;
        rc = wl_evaluation_control_charge(control, n); if (rc) goto done;
        memset((char *)prep->object + at, 0, (size_t)n); at += n;
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    }
    wl_columnar_eval_delta_rollback_t *record = prep->object;
    wl_columnar_memory_reservation_t *tokens[] = {
        &record->metadata_reservation, &record->payload_reservation
    };
    for (uint32_t i = 0; i < 2u; i++) {
        rc = wl_evaluation_control_charge(control, 128); if (rc) goto done;
        wl_columnar_memory_reservation_init(tokens[i]); prep->initialized++;
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    }
    rc = wl_evaluation_control_charge(control, 32); if (rc) goto done;
    record->count = sp->relation_count; record->controlled_origin = true;
    if (sess->memory_governor) {
        if (!wl_columnar_memory_reservation_move(&record->metadata_reservation,
            &prep->metadata)){
            rc = EINVAL; goto done;
        }
        record->governor = sess->memory_governor;
        wl_columnar_memory_governor_ref_retain(record->governor);
    }
    sess->delta_rollback = record; sess->delta_rollback_reserved_bytes = bytes;
    prep->object = NULL; prep->bytes = 0; prep->active = false;
    prep->physical_disposed = true;
    return wl_evaluation_control_charge(control, 0);
done:
    return wl_columnar_eval_delta_preparation_retire(prep, rc);
}

int
wl_columnar_eval_delta_capture_controlled(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess, wl_evaluation_control_t *control)
{
    if (!sp || !sess || sess->coordinator || !control
        || sess->delta_rollback ||
        !sess->serial_completion.pending) return EINVAL;
    /* Match the raw new-stratum caller's content-final reset. Recovery of an
     * existing snapshot does not enter this constructor. */
    int start = wl_evaluation_control_charge(control, 8);
    if (start) return start;
    sess->eval_stratum_heads_final = false;
    start = wl_evaluation_control_charge(control, 0); if (start) return start;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    wl_columnar_eval_delta_preparation_t *prep = NULL;
    int rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_cleanup_observer(sess, frame, &prep, control);
    if (!rc) rc = wl_columnar_eval_delta_rollback_allocate_controlled(sp, sess,
                prep, control);
    if (!rc) rc = wl_columnar_eval_delta_hold_controlled(sess, prep, true,
                control);
    int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
    if (cleanup > 0 && rc <= 0) rc = cleanup;
    if (rc) goto fail;
    rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc) goto fail;
    wl_columnar_eval_delta_rollback_t *record = sess->delta_rollback;
    uint64_t payload_bytes = 0;
    for (uint32_t i = 0; i < record->count; i++) {
        rc = wl_evaluation_control_charge(control, 16); if (rc) goto finish;
        const char *name = sp->relations[i].name;
        if (!name) {
            rc = EINVAL; goto finish;
        }
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
        col_rel_t *rel = NULL;
        rc = wl_columnar_join_controlled_lookup(sess, name, &rel, control);
        if (rc) goto finish;
        if (!rel) continue;
        wl_columnar_eval_delta_snapshot_t *entry = &record->entries[i];
        rc = wl_columnar_relation_reader_pair_acquire_controlled(rel,
                &entry->capture_reader, control); if (rc) goto finish;
        rc = wl_evaluation_control_charge(control, 16); if (rc) goto finish;
        if (rel->nrows > rel->capacity) {
            rc = EINVAL; goto finish;
        }
        if (rel->ncols) {
            entry->identity = rel->relation_identity; entry->nrows = rel->nrows;
            entry->ncols = rel->ncols; entry->alias = rel->storage_owner != rel;
        }
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
        if (!entry->identity) continue;
        uint64_t cells, bytes, length;
        rc = wl_columnar_eval_delta_name_length_controlled(name, &length,
                control);
        if (rc) goto finish;
        if (!wl_columnar_memory_size_mul(entry->nrows, entry->ncols, &cells)
            || !wl_columnar_memory_size_mul(cells, sizeof(int64_t), &bytes)
            || bytes > SIZE_MAX
            || !wl_columnar_memory_size_add(payload_bytes, bytes,
            &payload_bytes)
            || !wl_columnar_memory_size_add(payload_bytes, length + 1u,
            &payload_bytes)){
            rc = EOVERFLOW; goto finish;
        }
    }
    uint64_t total;
    if (!wl_columnar_memory_size_add(sess->delta_rollback_reserved_bytes,
        payload_bytes, &total)){
        rc = EOVERFLOW; goto finish;
    }
    rc = wl_columnar_eval_delta_reserve_controlled(sess, payload_bytes,
            &record->payload_reservation, control); if (rc) goto finish;
    rc = wl_evaluation_control_charge(control, 8); if (rc) goto finish;
    record->payload_reserved = record->governor && payload_bytes != 0;
    sess->delta_rollback_reserved_bytes = total;
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
    for (uint32_t i = 0; i < record->count; i++) {
        rc = wl_evaluation_control_charge(control, 8); if (rc) goto finish;
        wl_columnar_eval_delta_snapshot_t *entry = &record->entries[i];
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
        if (!entry->identity) continue;
        uint64_t length;
        rc =
            wl_columnar_eval_delta_name_length_controlled(sp->relations[i].name,
                &length, control); if (rc) goto finish;
        if (length >= SIZE_MAX) {
            rc = EOVERFLOW; goto finish;
        }
        rc = wl_evaluation_control_charge(control, 1); if (rc) goto finish;
        entry->name = malloc((size_t)length + 1u);
        if (!entry->name) {
            rc = ENOMEM; goto finish;
        }
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
        for (uint64_t at = 0; at <= length;) {
            uint64_t n = length + 1u - at; if (n > 128u) n = 128u;
            rc = wl_evaluation_control_charge(control, n); if (rc) goto finish;
            memcpy(entry->name + at, sp->relations[i].name + at, (size_t)n);
            at += n;
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
        }
        if (entry->nrows) {
            uint64_t bytes;
            if (!wl_columnar_memory_size_mul((uint64_t)entry->nrows *
                entry->ncols,
                sizeof(int64_t), &bytes) || bytes > SIZE_MAX){
                rc = EOVERFLOW; goto finish;
            }
            rc = wl_evaluation_control_charge(control, 1); if (rc) goto finish;
            entry->rows = malloc((size_t)bytes);
            if (!entry->rows) {
                rc = ENOMEM; goto finish;
            }
            rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
            col_rel_t *rel = NULL;
            rc = wl_columnar_join_controlled_lookup(sess, entry->name, &rel,
                    control);
            if (rc) goto finish;
            if (!rel) {
                rc = EINVAL; goto finish;
            }
            rc = wl_columnar_eval_delta_gather_controlled(frame, rel, 0,
                    entry->nrows, entry->ncols, entry->rows, control);
            if (rc) goto finish;
        }
    }
    if (record->governor) {
        bool committed = false;
        rc =
            wl_columnar_memory_governor_commit_controlled(
            &record->metadata_reservation,
            record, &committed, control);
        if (rc || !committed) {
            if (!rc) rc = EINVAL; goto finish;
        }
        if (record->payload_reserved) {
            rc =
                wl_columnar_memory_governor_commit_controlled(
                &record->payload_reservation,
                record, &committed, control);
            if (rc || !committed) {
                if (!rc) rc = EINVAL; goto finish;
            }
        }
    }
    for (uint32_t i = 0; i < record->count; i++) {
        rc = wl_evaluation_control_charge(control, 8); if (rc) goto finish;
        wl_columnar_eval_delta_snapshot_t *entry = &record->entries[i];
        if (entry->capture_reader.owner ||
            entry->capture_reader.secondary_owner){
            col_rel_t *rel = NULL;
            rc = wl_columnar_join_controlled_lookup(sess, sp->relations[i].name,
                    &rel, control);
            if (rc) goto finish;
            if (!rel) {
                rc = EINVAL; goto finish;
            }
            rc = wl_columnar_relation_reader_pair_release_controlled(rel,
                    &entry->capture_reader, control); if (rc) goto finish;
        }
        rc = wl_evaluation_control_charge(control, 0); if (rc) goto finish;
    }
finish:
    cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
    if (cleanup > 0 && rc <= 0) rc = cleanup;
    if (!rc) {
        record->active = true; return 0;
    }
fail:
    cleanup = wl_columnar_eval_delta_rollback_discard(sess);
    return rc > 0 ? rc : cleanup > 0 ? cleanup : rc;
}

static int
wl_columnar_eval_delta_capture(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess)
{
    uint64_t metadata_bytes, payload_bytes = 0;
    wl_columnar_memory_reservation_t reservation;
    if (sess->delta_rollback || sess->cleanup_active)
        return EBUSY;
    if (!wl_columnar_memory_size_mul(sp->relation_count,
        sizeof(wl_columnar_eval_delta_snapshot_t), &metadata_bytes)
        || !wl_columnar_memory_size_add(metadata_bytes,
        sizeof(wl_columnar_eval_delta_rollback_t), &metadata_bytes)
        || metadata_bytes > SIZE_MAX
        || metadata_bytes < sizeof(wl_columnar_eval_delta_rollback_t))
        return EOVERFLOW;
    int rc = wl_columnar_eval_delta_reserve(sess, metadata_bytes, &reservation);
    if (rc != 0)
        return rc;
    wl_columnar_eval_delta_rollback_t *record = calloc(1,
            (size_t)metadata_bytes);
    if (!record) {
        if (sess->memory_governor)
            (void)wl_columnar_memory_rollback(&reservation);
        return ENOMEM;
    }
    wl_columnar_memory_reservation_init(&record->metadata_reservation);
    wl_columnar_memory_reservation_init(&record->payload_reservation);
    if (sess->memory_governor) {
        if (!wl_columnar_memory_reservation_move(&record->metadata_reservation,
            &reservation)) {
            (void)wl_columnar_memory_rollback(&reservation);
            free(record);
            return EINVAL;
        }
        record->governor = sess->memory_governor;
        wl_columnar_memory_governor_ref_retain(record->governor);
    }
    record->count = sp->relation_count;
    sess->delta_rollback = record;
    sess->delta_rollback_reserved_bytes = metadata_bytes;
    if (sess->compound_arena) {
        rc = wl_arena_compound_arena_gc_hold_acquire(sess->compound_arena,
                &record->compound_hold);
        if (rc != 0)
            goto fail;
    }
    for (uint32_t i = 0; i < record->count; i++) {
        wl_columnar_eval_delta_snapshot_t *entry = &record->entries[i];
        col_rel_t *rel = session_find_rel(sess, sp->relations[i].name);
        uint64_t cells = 0, bytes = 0;
        if (!rel || !rel->ncols)
            continue;
        rc = col_rel_source_reader_acquire(rel, &entry->capture_reader);
        if (rc != 0)
            goto fail;
        entry->identity = rel->relation_identity;
        entry->nrows = rel->nrows;
        entry->ncols = rel->ncols;
        entry->alias = rel->storage_owner != rel;
        if (!wl_columnar_memory_size_mul(entry->nrows, entry->ncols, &cells)
            || !wl_columnar_memory_size_mul(cells, sizeof(int64_t), &bytes)
            || bytes > SIZE_MAX
            || !wl_columnar_memory_size_add(payload_bytes, bytes,
            &payload_bytes)
            || !wl_columnar_memory_size_add(payload_bytes,
            strlen(sp->relations[i].name) + 1, &payload_bytes)) {
            rc = EOVERFLOW;
            goto fail;
        }
    }
    if (!wl_columnar_memory_size_add(metadata_bytes, payload_bytes,
        &metadata_bytes)) {
        rc = EOVERFLOW;
        goto fail;
    }
    rc = wl_columnar_eval_delta_reserve(sess, payload_bytes,
            &record->payload_reservation);
    if (rc != 0)
        goto fail;
    record->payload_reserved = record->governor && payload_bytes != 0;
    sess->delta_rollback_reserved_bytes = metadata_bytes;
    for (uint32_t i = 0; i < record->count; i++) {
        wl_columnar_eval_delta_snapshot_t *entry = &record->entries[i];
        if (!entry->identity)
            continue;
        entry->name = wl_strdup(sp->relations[i].name);
        if (!entry->name) {
            rc = ENOMEM;
            goto fail;
        }
        if (!entry->nrows)
            continue;
        entry->rows = malloc((size_t)entry->nrows * entry->ncols *
                sizeof(int64_t));
        if (!entry->rows) {
            rc = ENOMEM;
            goto fail;
        }
        col_rel_t *rel = session_find_rel(sess, entry->name);
        for (uint32_t row = 0; row < entry->nrows; row++)
            col_rel_row_copy_out(rel, row,
                entry->rows + (size_t)row * entry->ncols);
    }
    if (record->governor
        && (!wl_columnar_memory_commit(&record->metadata_reservation, record)
        || (record->payload_reserved
        && !wl_columnar_memory_commit(&record->payload_reservation, record)))) {
        rc = EINVAL;
        goto fail;
    }
    for (uint32_t i = 0; i < record->count; i++)
        if (record->entries[i].capture_reader.owner)
            (void)col_rel_source_reader_release(
                &record->entries[i].capture_reader);
    record->active = true;
    return 0;
fail:
    (void)wl_columnar_eval_delta_rollback_discard(sess);
    return rc;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_delta_test_consolidation_boundary)(
    wl_col_session_t *sess, col_rel_t *target, col_rel_t *candidate,
    unsigned boundary);
#endif

int
wl_columnar_eval_delta_timestamp_retire(wl_col_session_t *session,
    wl_columnar_eval_delta_timestamp_normalization_t *record,
    wl_evaluation_control_t *control, int primary)
{
    if (!session || !record ||
        record->initialized > 1) return primary > 0 ? primary : EINVAL;
    int rc;
    if (record->indices || record->accounting_installed) {
        if (record->accounting_installed
            && record->bytes > session->cleanup_reserved_bytes)
            return primary > 0 ? primary : EINVAL;
        if (control) {
            rc = wl_evaluation_control_charge(control, 32);
            if (rc) return primary > 0 ? primary : rc;
        }
        free(record->indices); record->indices = NULL;
        if (record->accounting_installed) {
            session->cleanup_reserved_bytes -= record->bytes;
            record->accounting_installed = false;
        }
        if (control) {
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) return primary > 0 ? primary : rc;
        }
    }
    /* Physical NULL precedes the original reservation's checked retirement.
     * This footer is mandatory cleanup, including a refused identity retry. */
    if (record->initialized) {
        rc = wl_columnar_eval_delta_credit_retire(&record->indices_reservation);
        if (rc) return primary > 0 ? primary : rc;
    }
    if (record->transaction.writer_acquired || record->transaction.writer.owner
        || record->transaction.writer.secondary_owner) {
        rc = wl_columnar_relation_replacement_writer_release_controlled(
            record->candidate, &record->transaction, control);
        if (rc) return primary > 0 ? primary : rc;
    }
    record->ready = false;
    return primary;
}

static int
wl_columnar_eval_delta_timestamp_cmp_controlled(
    wl_columnar_eval_delta_timestamp_normalization_t *record,
    uint32_t a, uint32_t b, bool original, int *out,
    wl_evaluation_control_t *control)
{
    col_rel_t *rel = record->candidate;
    *out = 0;
    for (uint32_t col = 0; col < rel->ncols; col++) {
        int rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        int64_t left = rel->columns[col][a], right = rel->columns[col][b];
        *out = rel->column_types && rel->column_types[col] == WIRELOG_TYPE_FLOAT
            ? wl_columnar_float_compare_bits(left, right)
            : (left > right) - (left < right);
        wl_columnar_relation_consolidate_checkpoint(control, 150,
            (uint64_t)col + 1u, NULL, rel);
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        if (*out) return 0;
    }
    if (original) {
        int rc = wl_evaluation_control_charge(control, 4); if (rc) return rc;
        *out = (record->indices[a] > record->indices[b])
            - (record->indices[a] < record->indices[b]);
        return wl_evaluation_control_charge(control, 0);
    }
    return 0;
}

static int
wl_columnar_eval_delta_timestamp_swap_controlled(
    wl_columnar_eval_delta_timestamp_normalization_t *record,
    uint32_t a, uint32_t b, wl_evaluation_control_t *control)
{
    col_rel_t *rel = record->candidate;
    for (uint32_t col = 0; col < rel->ncols; col++) {
        int rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        int64_t value = rel->columns[col][a];
        rel->columns[col][a] = rel->columns[col][b];
        rel->columns[col][b] = value;
        wl_columnar_relation_consolidate_checkpoint(control, 151,
            (uint64_t)col + 1u, NULL, rel);
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    int rc = wl_evaluation_control_charge(control, 32); if (rc) return rc;
    col_delta_timestamp_t timestamp = rel->timestamps[a];
    rel->timestamps[a] = rel->timestamps[b]; rel->timestamps[b] = timestamp;
    uint32_t index = record->indices[a];
    record->indices[a] = record->indices[b]; record->indices[b] = index;
    return wl_evaluation_control_charge(control, 0);
}

static int
wl_columnar_eval_delta_timestamp_sift_controlled(
    wl_columnar_eval_delta_timestamp_normalization_t *record,
    uint32_t root, uint32_t count, wl_evaluation_control_t *control)
{
    while (root < count / 2) {
        int rc = wl_evaluation_control_charge(control, 8); if (rc) return rc;
        uint32_t child = root * 2 + 1; int comparison;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        if (child + 1 < count) {
            rc = wl_columnar_eval_delta_timestamp_cmp_controlled(record,
                    child, child + 1, true, &comparison, control);
            if (rc) return rc;
            if (comparison < 0) child++;
        }
        rc = wl_columnar_eval_delta_timestamp_cmp_controlled(record,
                root, child, true, &comparison, control);
        if (rc) return rc;
        if (comparison >= 0) break;
        rc = wl_columnar_eval_delta_timestamp_swap_controlled(record, root,
                child, control);
        if (rc) return rc;
        root = child;
    }
    return 0;
}

int
wl_columnar_eval_delta_timestamp_normalize_controlled(wl_col_session_t *session,
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_eval_delta_timestamp_normalization_t *record,
    wl_evaluation_control_t *control)
{
    if (!session || !frame || !record || !control
        || record !=
        wl_columnar_eval_stack_cleanup_observer_timestamp_record(session, frame)
        || !record->ready || record->count < 2
        || record->initialized != 1 || record->indices ||
        record->accounting_installed
        || record->transaction.writer_acquired ||
        record->normalized) return EINVAL;
    col_rel_t *rel = record->candidate;
    int rc = wl_evaluation_control_charge(control, 64); if (rc) return rc;
    if (!rel || record->count != rel->nrows || rel->nrows > rel->capacity
        || !rel->timestamps || rel->timestamp_capacity < rel->nrows
        || rel->storage_owner != rel || rel->col_shared || rel->arena_owned
        || rel->pool_owned || !session->memory_governor
        || rel->memory_governor != session->memory_governor) return EINVAL;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    rc = wl_columnar_relation_replacement_writer_acquire_controlled(rel,
            &record->transaction, control);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    if (col_rel_storage_alias_borrow_count(rel)) return EBUSY;
    if (!wl_columnar_memory_size_mul(record->count, sizeof(uint32_t),
        &record->bytes)
        || record->bytes > SIZE_MAX
        || record->bytes >
        UINT64_MAX - session->cleanup_reserved_bytes) return EOVERFLOW;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    for (uint32_t col = 0; col < rel->ncols; col++) {
        rc = wl_evaluation_control_charge(control, 4); if (rc) return rc;
        if (!rel->columns || !rel->columns[col]) return EINVAL;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    rc = wl_columnar_eval_delta_reserve_controlled(session, record->bytes,
            &record->indices_reservation, control);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 1); if (rc) return rc;
    record->indices = malloc((size_t)record->bytes);
    if (!record->indices) return ENOMEM;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    bool committed = false;
    rc = wl_columnar_memory_governor_commit_controlled(
        &record->indices_reservation, record->indices, &committed, control);
    if (rc || !committed) return rc ? rc : EINVAL;
    rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    session->cleanup_reserved_bytes += record->bytes;
    record->accounting_installed = true;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    rc = wl_columnar_session_observer_mem_sample_controlled(session, frame,
            control);
    if (rc) return rc;
    for (uint32_t i = 0; i < record->count; i++) {
        rc = wl_evaluation_control_charge(control, 2); if (rc) return rc;
        record->indices[i] = i;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    for (record->root = record->count / 2; record->root > 0; record->root--) {
        rc = wl_evaluation_control_charge(control, 2); if (rc) return rc;
        rc = wl_columnar_eval_delta_timestamp_sift_controlled(record,
                record->root - 1, record->count, control);
        if (rc) return rc;
    }
    for (record->end = record->count; record->end > 1; record->end--) {
        rc = wl_evaluation_control_charge(control, 2); if (rc) return rc;
        rc = wl_columnar_eval_delta_timestamp_swap_controlled(record, 0,
                record->end - 1, control);
        if (!rc) rc = wl_columnar_eval_delta_timestamp_sift_controlled(record,
                    0, record->end - 1, control);
        if (rc) return rc;
    }
    record->output = record->count ? 1 : 0;
    for (record->row = 1; record->row < record->count; record->row++) {
        int comparison;
        rc = wl_evaluation_control_charge(control, 2); if (rc) return rc;
        rc = wl_columnar_eval_delta_timestamp_cmp_controlled(record,
                record->output - 1, record->row, false, &comparison, control);
        if (rc) return rc;
        if (!comparison) continue;
        if (record->output != record->row) {
            for (uint32_t col = 0; col < rel->ncols; col++) {
                rc = wl_evaluation_control_charge(control, 4);
                if (rc) return rc;
                rel->columns[col][record->output] =
                    rel->columns[col][record->row];
                wl_columnar_relation_consolidate_checkpoint(control, 152,
                    (uint64_t)col + 1u, NULL, rel);
                rc = wl_evaluation_control_charge(control, 0);
                if (rc) return rc;
            }
            rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
            rel->timestamps[record->output] = rel->timestamps[record->row];
            rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
        }
        record->output++;
    }
    rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    rel->nrows = record->output; rel->sorted_nrows = record->output;
    rel->run_count = 1; rel->run_ends[0] = record->output;
    record->normalized = true;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    rc = wl_columnar_eval_delta_timestamp_retire(session, record, control, 0);
    if (rc) return rc;
    return wl_columnar_eval_stack_cleanup_observer_timestamp_consumed(session,
               frame, control);
}

static int
wl_columnar_eval_delta_timestamp_cmp(const col_rel_t *rel,
    const uint32_t *original, uint32_t a, uint32_t b)
{
    int cmp = col_rel_row_cmp(rel, a, b);
    if (cmp != 0)
        return cmp;
    return (original[a] > original[b]) - (original[a] < original[b]);
}

static void
wl_columnar_eval_delta_timestamp_swap(col_rel_t *rel, uint32_t *original,
    uint32_t a, uint32_t b)
{
    for (uint32_t col = 0; col < rel->ncols; col++) {
        int64_t value = rel->columns[col][a];
        rel->columns[col][a] = rel->columns[col][b];
        rel->columns[col][b] = value;
    }
    col_delta_timestamp_t timestamp = rel->timestamps[a];
    rel->timestamps[a] = rel->timestamps[b];
    rel->timestamps[b] = timestamp;
    uint32_t index = original[a];
    original[a] = original[b];
    original[b] = index;
}

static void
wl_columnar_eval_delta_timestamp_sift(col_rel_t *rel, uint32_t *original,
    uint32_t root, uint32_t count)
{
    while (root < count / 2) {
        uint32_t child = root * 2 + 1;
        if (child + 1 < count && wl_columnar_eval_delta_timestamp_cmp(rel,
            original, child, child + 1) < 0)
            child++;
        if (wl_columnar_eval_delta_timestamp_cmp(rel, original, root, child)
            >= 0)
            break;
        wl_columnar_eval_delta_timestamp_swap(rel, original, root, child);
        root = child;
    }
}

/* Private set normalization preserves the complete provenance of the earliest
 * original representative. Shared sorting's timestamp repair is #1689. */
static int
wl_columnar_eval_delta_timestamp_normalize_locked(wl_col_session_t *sess,
    col_rel_t *rel)
{
    uint64_t bytes, temporary;
    if (!wl_columnar_memory_size_mul(rel->nrows, sizeof(uint32_t), &bytes)
        || bytes > SIZE_MAX
        || !wl_columnar_memory_size_add(sess->cleanup_reserved_bytes, bytes,
        &temporary))
        return EOVERFLOW;
    wl_columnar_memory_reservation_t reservation;
    int rc = wl_columnar_eval_delta_reserve(sess, bytes, &reservation);
    if (rc != 0)
        return rc;
    uint32_t *original = malloc((size_t)bytes);
    if (!original) {
        wl_columnar_eval_delta_observer_release_token(&reservation);
        return ENOMEM;
    }
    if (sess->memory_governor && bytes
        && !wl_columnar_memory_commit(&reservation, original)) {
        free(original);
        wl_columnar_eval_delta_observer_release_token(&reservation);
        return EINVAL;
    }
    sess->cleanup_reserved_bytes = temporary;
    col_session_mem_sample(sess);
    uint32_t count = rel->nrows;
    for (uint32_t i = 0; i < count; i++)
        original[i] = i;
    for (uint32_t root = count / 2; root > 0; root--)
        wl_columnar_eval_delta_timestamp_sift(rel, original, root - 1, count);
    for (uint32_t end = count; end > 1; end--) {
        wl_columnar_eval_delta_timestamp_swap(rel, original, 0, end - 1);
        wl_columnar_eval_delta_timestamp_sift(rel, original, 0, end - 1);
    }
    uint32_t out = 1;
    for (uint32_t row = 1; row < count; row++) {
        if (col_rel_row_cmp(rel, out - 1, row) == 0)
            continue;
        if (out != row) {
            col_rel_row_move_raw(rel, out, row);
            rel->timestamps[out] = rel->timestamps[row];
        }
        out++;
    }
    rel->nrows = out;
    rel->sorted_nrows = out;
    rel->run_count = 1;
    rel->run_ends[0] = out;
    free(original);
    sess->cleanup_reserved_bytes -= bytes;
    wl_columnar_eval_delta_observer_release_token(&reservation);
    return 0;
}

static int
wl_columnar_eval_delta_timestamp_normalize(wl_col_session_t *sess,
    col_rel_t *rel)
{
    wl_columnar_source_access_writer_t writer = { 0 };
    int rc = col_rel_source_writer_acquire(rel, &writer);
    if (rc != 0)
        return rc;
    if (col_rel_storage_alias_borrow_count(rel) != 0)
        rc = EBUSY;
    else
        rc = wl_columnar_eval_delta_timestamp_normalize_locked(sess, rel);
    int release_rc = wl_columnar_source_access_writer_release(&writer);
    return rc != 0 ? rc : release_rc;
}

/* One synchronous controlled caller: every gate, candidate and replacement
 * remains in the same original frame. Baseline locals are immutable scalars,
 * exported under the source pin before constructor bookkeeping reuses fields. */
static int
wl_columnar_eval_delta_consolidate_controlled(col_rel_t *target,
    wl_col_session_t *sess)
{
    wl_evaluation_control_t *control = sess->base.evaluation_control;
    int rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    wl_columnar_eval_delta_replacement_t *record = NULL;
    wl_columnar_relation_bulk_append_t *bulk = NULL;
    eval_entry_t *result = NULL;
    uint64_t identity = 0, view = 0, storage = 0; bool small = false;
    rc = wl_columnar_eval_stack_cleanup_acquire(sess, &frame, control);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_cleanup_observer_constructor(sess, frame,
            &record, &bulk, &result, control);
    if (rc) goto done;
    rc = wl_columnar_relation_observer_source_preflight_controlled(record,
            bulk, result, target, &identity, &view, &storage, &small, control);
    if (rc) goto done;
    if (small) {
        rc = wl_columnar_relation_observer_source_finish_controlled(record,
                bulk, result, control);
        goto done;
    }
    rc = wl_columnar_relation_observer_descriptor_from_preflight_controlled(
        record, bulk, result, target, sess->memory_governor, control);
    if (rc) goto done;
    rc = wl_columnar_relation_observer_schema_controlled(record, bulk, result,
            target, control);
    if (rc) goto done;
    rc = wl_columnar_relation_observer_types_controlled(record, bulk, result,
            target, control);
    if (rc) goto done;
    rc = wl_columnar_relation_observer_compound_controlled(record, bulk, result,
            target, control);
    if (rc) goto done;
    rc = wl_columnar_relation_observer_timestamps_controlled(record, bulk,
            result, target, control);
    if (rc) goto done;
    rc = wl_columnar_relation_observer_append_controlled(record, bulk, result,
            target, control);
    if (rc) goto done;
    col_rel_t *candidate = result->rel;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_delta_test_consolidation_boundary)
        wl_columnar_eval_delta_test_consolidation_boundary(sess, target,
            candidate, 1);
#endif
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    rc = wl_columnar_eval_stack_cleanup_observer_completed(sess, frame,
            control);
    if (rc) goto done;
    if (candidate->timestamps) {
        wl_columnar_eval_delta_timestamp_normalization_t *timestamp = NULL;
        rc = wl_columnar_eval_stack_cleanup_observer_timestamp(sess, frame,
                &timestamp, control);
        if (!rc) rc =
                wl_columnar_eval_delta_timestamp_normalize_controlled(sess,
                    frame, timestamp, control);
    } else rc = wl_columnar_merge_observer_normalize_controlled(sess, frame,
                control);
    if (rc) goto done;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_delta_test_consolidation_boundary)
        wl_columnar_eval_delta_test_consolidation_boundary(sess, target,
            candidate, 2);
#endif
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    record = NULL; bulk = NULL;
    rc = wl_columnar_eval_stack_cleanup_observer_normalized_replacement(sess,
            frame,
            &record, &bulk, control);
    if (rc) goto done;
    rc = wl_columnar_relation_replacement_prepare_controlled(record, bulk,
            target, candidate, control);
    if (rc) goto done;
    rc = wl_evaluation_control_charge(control, 16); if (rc) goto done;
    if (target->relation_identity != identity || target->view_generation != view
        || target->storage_generation != storage) {
        rc = EBUSY; goto done;
    }
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_delta_test_consolidation_boundary)
        wl_columnar_eval_delta_test_consolidation_boundary(sess, target,
            candidate, 3);
#endif
    rc = wl_evaluation_control_charge(control, 0); if (rc) goto done;
    rc = wl_columnar_relation_observer_candidate_retire(record, bulk, result,
            sess->memory_governor, control, 0);
    if (rc) goto done;
    rc = wl_columnar_relation_replacement_image_prepare_controlled(record, bulk,
            result, control);
    if (rc) goto done;
    rc = wl_columnar_relation_replacement_image_publish_controlled(record, bulk,
            control);
done:
    if (rc == ENOSPC) sess->memory_budget_denied = true;
    {
        int cleanup = wl_columnar_eval_stack_cleanup_finish(&frame);
        return rc > 0 ? rc : cleanup ? cleanup : rc;
    }
}

int
wl_columnar_eval_delta_consolidate(col_rel_t *target, wl_col_session_t *sess)
{
    if (!target || !sess) return EINVAL;
    if (sess->base.evaluation_control)
        return wl_columnar_eval_delta_consolidate_controlled(target, sess);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    wl_columnar_source_access_reader_t source_reader = { 0 };
    wl_columnar_source_access_reader_t candidate_reader = { 0 };
    col_rel_replacement_t replacement = { 0 };
    uint64_t identity = 0, view = 0, storage = 0;
    int rc = wl_columnar_eval_stack_cleanup_begin(sess, &frame);
    if (rc != 0)
        return rc;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
    rc = col_rel_source_reader_acquire(target, &source_reader);
    if (rc != 0)
        goto done;
    identity = target->relation_identity;
    view = target->view_generation;
    storage = target->storage_generation;
    if (!wl_columnar_relation_float_values_valid(target)) {
        rc = EINVAL;
        goto done;
    }
    if (target->nrows <= 1)
        goto done;
    col_rel_t *candidate = NULL;
    rc = wl_columnar_relation_new_like_governed_checked(&candidate,
            "$consol", target, sess->memory_governor);
    if (rc != 0) {
        if (rc == ENOSPC)
            sess->memory_budget_denied = true;
        goto done;
    }
    result->rel = candidate;
    result->owned = true;
    rc = col_rel_append_all(candidate, target, NULL);
    if (rc != 0)
        goto done;
    rc = col_rel_source_reader_release(&source_reader);
    if (rc != 0)
        goto done;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_delta_test_consolidation_boundary)
        wl_columnar_eval_delta_test_consolidation_boundary(sess, target,
            candidate, 1);
#endif
    if (candidate->timestamps) {
        rc = wl_columnar_eval_delta_timestamp_normalize(sess, candidate);
    } else {
        rc = eval_stack_push(stack, candidate, true);
        if (rc != 0)
            goto done;
        memset(result, 0, sizeof(*result));
        rc = col_op_consolidate(stack, sess);
        if (rc == 0)
            rc = eval_stack_pop_relation(stack, result);
    }
    if (rc != 0)
        goto done;
    candidate = result->rel;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_delta_test_consolidation_boundary)
        wl_columnar_eval_delta_test_consolidation_boundary(sess, target,
            candidate, 2);
#endif
    rc = col_rel_source_reader_acquire(candidate, &candidate_reader);
    if (rc != 0)
        goto done;
    rc = col_rel_prepare_replacement(target, candidate, &replacement);
    if (rc != 0)
        goto done;
    if (target->relation_identity != identity || target->view_generation != view
        || target->storage_generation != storage) {
        rc = EBUSY;
        goto done;
    }
    rc = col_rel_source_reader_release(&candidate_reader);
    if (rc != 0)
        goto done;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_delta_test_consolidation_boundary)
        wl_columnar_eval_delta_test_consolidation_boundary(sess, target,
            candidate, 3);
#endif
    rc = eval_entry_dispose(result);
    if (rc != 0)
        goto done;
    col_rel_commit_replacement_locked(target, &replacement);
done:
    if (source_reader.owner) {
        int release_rc = col_rel_source_reader_release(&source_reader);
        if (rc == 0)
            rc = release_rc;
    }
    if (candidate_reader.owner) {
        int release_rc = col_rel_source_reader_release(&candidate_reader);
        if (rc == 0)
            rc = release_rc;
    }
    col_rel_discard_replacement(&replacement);
    int cleanup_rc = wl_columnar_eval_stack_cleanup_finish(&frame);
    return cleanup_rc != 0 ? cleanup_rc : rc;
}

/* The caller always records the real evaluator disposition before recovery
 * or teardown. These fixed stores are mandatory cleanup, not forward work. */
int
wl_columnar_eval_delta_rollback_finish(wl_col_session_t *sess, int result)
{
    if (!sess || !sess->delta_rollback) return result > 0 ? result : EINVAL;
    sess->delta_rollback->active = false;
    sess->delta_rollback->retained = sess->eval_stratum_heads_final;
    sess->delta_rollback->evaluation_error = result;
    return result;
}

int
col_stratum_step_with_delta(const wl_plan_stratum_t *sp, wl_col_session_t *sess,
    uint32_t stratum_idx)
{
    /* The detach loop below can reach cleanup: without ever calling
     * col_eval_stratum, so clear the flag here rather than relying on that
     * call to do it.  Otherwise a stale true from the previous window would
     * skip a restore these heads need. */
    sess->eval_stratum_heads_final = false;
    int readiness_rc = wl_columnar_session_cleanup_ready(sess);
    if (readiness_rc != 0)
        return readiness_rc;

    /* Issue #158: For now, use full re-evaluation for retraction.
     * When retraction_seeded is set, the standard delta callback logic
     * compares prev state with new state (recomputed from affected input),
     * and diff=-1 callbacks are fired for removed tuples.
     * Future optimization: wire up col_stratum_step_retraction_nonrecursive
     * (implemented above, not yet called) for direct delta-only propagation
     * of retractions. */

    uint32_t rc_cnt = sp->relation_count;

    int rc = wl_columnar_eval_delta_capture(sp, sess);
    if (rc != 0)
        return rc;
    wl_columnar_eval_delta_rollback_t *rollback = sess->delta_rollback;
    /* Retire aliases before their roots. A refused later target leaves a
     * recoverable detached prefix, never an unowned snapshot. */
    for (unsigned pass = 0; pass < 2; pass++) {
        for (uint32_t i = 0; i < rc_cnt; i++) {
            wl_columnar_eval_delta_snapshot_t *entry = &rollback->entries[i];
            if (rollback->controlled_origin && entry->detached)
                continue;
            if (!entry->identity || entry->alias != (pass == 0))
                continue;
            col_rel_t *rel = session_find_rel(sess, entry->name);
            rc = wl_columnar_relation_delta_detach(rel, entry->identity);
            if (rc != 0)
                goto cleanup;
            entry->detached = true;
            rc = wl_columnar_session_retire_source_lease(sess, rel);
            if (rc != 0)
                goto cleanup;
        }
    }
    /* Issue #2091: detaching emptied each head, input rows included; give
     * every head its own input rows back before it is re-derived. */
    for (uint32_t i = 0; i < rc_cnt; i++) {
        wl_columnar_eval_delta_snapshot_t *entry = &rollback->entries[i];
        if (!entry->detached)
            continue;
        rc = wl_columnar_session_restore_seed(sess,
                session_find_rel(sess, entry->name));
        if (rc != 0)
            goto cleanup;
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
    rc = col_eval_stratum(sp, sess, stratum_idx);
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_delta_test_after_eval)
        wl_columnar_eval_delta_test_after_eval(sess);
#endif
    if (sess->cleanup_pending && rc == 0)
        rc = EBUSY;
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
        rc = wl_columnar_eval_delta_consolidate(r, sess);
        if (rc != 0)
            goto cleanup;

        /* A public step compares all outputs against its durable baseline
        * only after every stratum and final compaction have succeeded. */
        if (sess->delta_observer)
            continue;
        uint32_t ncols = r->ncols;

        /* Gather current state into flat buffer for col_row_in_sorted */
        int64_t *cur_flat = NULL;
        if (r->nrows > 0 && ncols > 0) {
            cur_flat = (int64_t *)malloc(
                (size_t)r->nrows * ncols * sizeof(int64_t));
            if (!cur_flat) {
                rc = ENOMEM;
                goto cleanup;
            }
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
                if (!col_row_in_sorted(rollback->entries[ri].rows,
                    rollback->entries[ri].nrows, ncols,
                    rowp)) {
                    rc = wl_delta_event_append(sess, r->name, rowp, ncols,
                            +1);
                    if (rc != 0) {
                        free(cur_flat);
                        cur_flat = NULL;
                        goto cleanup;
                    }
                }
            }
        }

        /* Fire delta_cb(-1) for rows present in prev sorted state but not in new
         */
        if (rollback->entries[ri].nrows > 0) {
            for (uint32_t row = 0; row < rollback->entries[ri].nrows; row++) {
                const int64_t *rowp
                    = rollback->entries[ri].rows + (size_t)row *
                    rollback->entries[ri].ncols;
                if (!col_row_in_sorted(cur_flat, r->nrows,
                    rollback->entries[ri].ncols,
                    rowp)) {
                    rc = wl_delta_event_append(sess, r->name, rowp,
                            rollback->entries[ri].ncols, -1);
                    if (rc != 0) {
                        free(cur_flat);
                        cur_flat = NULL;
                        goto cleanup;
                    }
                }
            }
        }
        free(cur_flat);
    }

cleanup:
    rc = wl_columnar_eval_delta_rollback_finish(sess, rc);
    if (rc != 0) {
        /* This clear drops nothing this function produced on the only
         * production caller.  session.c selects col_stratum_step_with_delta
         * only when sess->delta_observer is set -- the other textual call site
         * is inside the UNUSED retraction helper above, which nothing wires --
         * and the emission loop above
         * declines at its own "if (sess->delta_observer) continue" -- so
         * neither wl_delta_event_append call site has run.  sess->delta_events
         * is session-scoped and cleared once before the stratum loop, so an
         * earlier stratum's events are still discarded here; that holds for
         * every non-zero rc and is not specific to a refused removal.
         *
         * Direct callers in tests run without an observer and do reach the
         * emission loop.  For them, and for any caller that clears the
         * observer, retained heads plus a non-zero rc would mean the facts
         * moved and nothing announced it.  Re-emitting on the retry cannot
         * repair it: capture refuses while delta_rollback is live, and once
         * cleanup_ready discards the record the retry re-baselines against the
         * already-moved heads, so its set difference is empty.
         *
         * #1769 settled this as documentation, not code.  No public caller
         * reaches the emission body -- it declines at the observer check
         * above -- so the loss is a test-visible property of this path rather
         * than a contract wl_session_step owes anyone; emitting on the
         * failing path would change the callback contract for a public
         * caller that does not exist.  wirelog/session.h instead scopes
         * the retry guarantee to the whole-step observer baseline, which
         * covers both producers -- the cleanup_pending conversion above and
         * the refused terminal removal #1661 added.  A caller that clears
         * the observer and drives this function directly still loses the
         * events; that is the accepted cost. */
        wl_columnar_delta_events_clear(sess);
        int cleanup_rc = wl_columnar_session_cleanup_ready(sess);
        return cleanup_rc != 0 ? cleanup_rc : rc;
    }
    bool gc_requested = rollback->gc_requested;
    int discard_rc = wl_columnar_eval_delta_rollback_discard(sess);
    if (discard_rc != 0) {
        wl_columnar_delta_events_clear(sess);
        return discard_rc;
    }
    /* All frontier requests inside the protected evaluation window coalesce
     * into one explicit collection after the snapshots cease to need handles.
     * Failed recovery and generic hold release never replay collection. */
    if (gc_requested && !sess->coordinator && sess->compound_arena
        && sess->rotation_ops && sess->rotation_ops->gc_epoch_boundary)
        sess->rotation_ops->gc_epoch_boundary(sess);
    if (!sess->delta_event_transaction) {
        wl_columnar_delta_events_publish(sess);
        wl_columnar_delta_events_clear(sess);
    }
    return 0;
}
