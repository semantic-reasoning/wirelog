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
    wl_columnar_eval_delta_snapshot_t entries[];
};

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_delta_test_after_eval)(wl_col_session_t *sess);
void (*wl_columnar_eval_delta_test_observer_boundary)(wl_col_session_t *sess,
    unsigned boundary);
void (*wl_columnar_eval_delta_test_before_retire_credit)(wl_col_session_t *sess,
    unsigned credit, wl_columnar_memory_reservation_t *token);
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
    bool active, evaluated, cancelled, delivered, prepared, gc_requested;
    wl_columnar_eval_delta_observer_retire_t retirement;
    wl_col_delta_event_t *events;
    size_t event_count;
    wl_columnar_eval_delta_observer_entry_t entries[];
};

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

static int
wl_columnar_eval_delta_observer_release_credit(wl_col_session_t *sess,
    unsigned credit, wl_columnar_memory_reservation_t *token)
{
    uint64_t state = atomic_load_explicit(&token->state, memory_order_acquire);
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY)
        return 0;
    if (state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED) {
        wl_columnar_memory_reservation_init(token);
        return 0;
    }
    if (state != WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        && state != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED)
        return EINVAL;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_delta_test_before_retire_credit)
        wl_columnar_eval_delta_test_before_retire_credit(sess, credit, token);
#else
    (void)sess;
    (void)credit;
#endif
    if (!wl_columnar_memory_release(token))
        return EINVAL;
    wl_columnar_memory_reservation_init(token);
    return 0;
}

/* Short-lived local scratch reservations predate observer retirement. Keep
 * their established cleanup contract separate from the retryable observer
 * credits above. */
static void
wl_columnar_eval_delta_release_token(wl_columnar_memory_reservation_t *token)
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

static int
wl_columnar_eval_delta_observer_readers(
    wl_columnar_eval_delta_observer_t *observer)
{
    for (uint32_t i = 0; i < observer->count; i++) {
        if (observer->entries[i].reader.owner) {
            int rc =
                col_rel_source_reader_release(&observer->entries[i].reader);
            if (rc != 0)
                return rc;
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
    observer->prepared = false;
    int rc = wl_columnar_eval_delta_observer_release_credit(sess,
            WL_COLUMNAR_EVAL_DELTA_OBSERVER_CREDIT_STAGING,
            &observer->staging);
    if (rc == 0)
        observer->staging_bytes = 0;
    uint64_t total;
    if (!wl_columnar_memory_size_add(observer->metadata_bytes,
        observer->payload_bytes, &total)
        || !wl_columnar_memory_size_add(total, observer->staging_bytes,
        &total))
        total = UINT64_MAX;
    sess->delta_observer_reserved_bytes = total;
    return rc;
}

/* Persistent construction is separate from the published raw observer. All
 * registry/name/cell traversal below has a persistent cursor. */
enum wl_columnar_eval_delta_build_phase {
    WL_COLUMNAR_DELTA_BUILD_HEAD,
    WL_COLUMNAR_DELTA_BUILD_NAME,
    WL_COLUMNAR_DELTA_BUILD_LOOKUP,
    WL_COLUMNAR_DELTA_BUILD_SCHEMA,
    WL_COLUMNAR_DELTA_BUILD_ALLOC,
    WL_COLUMNAR_DELTA_BUILD_ZERO,
    WL_COLUMNAR_DELTA_BUILD_PAYLOAD,
    WL_COLUMNAR_DELTA_BUILD_DUP,
    WL_COLUMNAR_DELTA_BUILD_ENTRY,
    WL_COLUMNAR_DELTA_BUILD_NAME_COPY,
    WL_COLUMNAR_DELTA_BUILD_ROWS,
    WL_COLUMNAR_DELTA_BUILD_ACQUIRE,
    WL_COLUMNAR_DELTA_BUILD_COPY,
    WL_COLUMNAR_DELTA_BUILD_RELEASE,
    WL_COLUMNAR_DELTA_BUILD_PREFLIGHT_DUP,
    WL_COLUMNAR_DELTA_BUILD_SORT,
    WL_COLUMNAR_DELTA_BUILD_SUBTRACT,
    WL_COLUMNAR_DELTA_BUILD_UNIQUE,
    WL_COLUMNAR_DELTA_BUILD_COMMIT,
    WL_COLUMNAR_DELTA_BUILD_READY
};

struct wl_columnar_eval_delta_observer_build {
    uintptr_t identity;
    wl_col_session_t *session;
    wl_columnar_eval_delta_observer_t *observer;
    wl_columnar_memory_reservation_t metadata;
    wl_columnar_relation_reader_step_t reader;
    wl_columnar_memory_governor_ref_t *governor;
    int error;
    col_rel_t *sources[3];
    uint64_t mask, metadata_bytes, payload_bytes, entry_rows;
    size_t length, offset, zero;
    uint32_t si, ri, registry, kind, count, duplicate;
    uint32_t duplicate_si, duplicate_ri;
    uint32_t row, copied, sort_begin, sort_end, heap_size, heap_root,
        heap_child, i, j, out;
    int64_t value;
    unsigned phase;
    bool preflight, metadata_held, sorting_pending, heap_build, heap_sifting,
        heap_child_selected;
};

/* The published observer's merge compares normalized rows with memcmp. Keep
 * the resumable baseline in that same native-byte order. */
static int
wl_columnar_eval_delta_build_value_cmp(int64_t left, int64_t right)
{
    return memcmp(&left, &right, sizeof(left));
}

static bool
wl_columnar_eval_delta_build_schema(const col_rel_t *r)
{
    return r && r->storage_owner == r && r->relation_identity
           && r->relation_identity != UINT64_MAX
           && r->storage_owner_identity == r->relation_identity
           && r->storage_generation && r->storage_generation != UINT64_MAX
           && r->storage_owner_generation == r->storage_generation
           && r->ncols == 1 && r->nrows <= r->capacity
           && (!r->nrows || (r->columns && r->columns[0]))
           && (!r->column_types || r->column_types[0] == WIRELOG_TYPE_INT64)
           && !r->timestamps && r->compound_kind == WIRELOG_COMPOUND_KIND_NONE
           && !r->compound_count && !r->compound_arity_map
           && !r->compound_arity_len && !r->inline_physical_offset;
}

int
wl_columnar_eval_delta_observer_build_start(wl_col_session_t *sess,
    uint64_t mask)
{
    if (!sess || !sess->plan
        || (sess->plan->stratum_count && !sess->plan->strata)
        || (sess->nrels && !sess->rels))
        return EINVAL;
    if (sess->delta_observer_build || sess->delta_observer
        || sess->delta_publish_active || sess->coordinator
        || sess->delta_observer_retirement
        != WL_COLUMNAR_EVAL_DELTA_OBSERVER_RETIRE_NONE)
        return EBUSY;
    wl_columnar_eval_delta_observer_build_t *b = calloc(1, sizeof(*b));
    if (!b)
        return ENOMEM;
    b->identity = (uintptr_t)b;
    b->session = sess;
    b->mask = mask;
    b->preflight = true;
    wl_columnar_memory_reservation_init(&b->metadata);
    sess->delta_observer_build = b;
    return 0;
}

static int
wl_columnar_eval_delta_build_action(wl_col_session_t *sess,
    wl_columnar_eval_delta_observer_build_t *b)
{
    const wl_plan_t *plan = sess->plan;
    wl_columnar_eval_delta_observer_t *o = b->observer;
    wl_columnar_eval_delta_observer_entry_t *e =
        o && b->phase > WL_COLUMNAR_DELTA_BUILD_ZERO && o->count
        ? &o->entries[o->count - 1] : NULL;
    const char *name = b->si < plan->stratum_count
        && b->ri < plan->strata[b->si].relation_count
        ? plan->strata[b->si].relations[b->ri].name : NULL;
    switch (b->phase) {
    case WL_COLUMNAR_DELTA_BUILD_HEAD:
        if (b->si == plan->stratum_count) {
            b->phase = b->preflight ? WL_COLUMNAR_DELTA_BUILD_ALLOC
                : WL_COLUMNAR_DELTA_BUILD_COMMIT;
            return 0;
        }
        if (!plan->strata || (plan->strata[b->si].relation_count
            && !plan->strata[b->si].relations))
            return EINVAL;
        if (b->ri == plan->strata[b->si].relation_count) {
            b->si++;
            b->ri = 0;
            return 0;
        }
        if (!name)
            return EINVAL;
        b->length = 0;
        b->kind = 0;
        b->sources[0] = b->sources[1] = b->sources[2] = NULL;
        b->phase = WL_COLUMNAR_DELTA_BUILD_NAME;
        return 0;
    case WL_COLUMNAR_DELTA_BUILD_NAME:
        if (name[b->length]) {
            if (b->length == SIZE_MAX - 1)
                return EOVERFLOW;
            b->length++;
        } else {
            b->registry = 0;
            b->offset = 0;
            b->phase = WL_COLUMNAR_DELTA_BUILD_LOOKUP;
        }
        return 0;
    case WL_COLUMNAR_DELTA_BUILD_LOOKUP: {
        /* Raw seed/retired lookup uses a 256-byte prefixed name. Preserve
         * that absence rule without snprintf or an unbounded string scan. */
        size_t prefix = b->kind == 1 ? 5 : b->kind == 2 ? 4 : 0;
        if (b->registry == sess->nrels ||
            (prefix && b->length >= 256 - prefix)){
            b->phase = WL_COLUMNAR_DELTA_BUILD_SCHEMA;
            return 0;
        }
        col_rel_t *r = sess->rels[b->registry];
        if (!r || !r->name) {
            b->registry++;
            b->offset = 0;
            return 0;
        }
        const char *p = b->kind == 1 ? "$out$" : "$in$";
        char expected = b->offset < prefix ? p[b->offset]
            : name[b->offset - prefix];
        char actual = r->name[b->offset];
        if (expected != actual) {
            b->registry++;
            b->offset = 0;
        } else if (!actual) {
            b->sources[b->kind] = r;
            b->phase = WL_COLUMNAR_DELTA_BUILD_SCHEMA;
        } else {
            b->offset++;
        }
        return 0;
    }
    case WL_COLUMNAR_DELTA_BUILD_SCHEMA: {
        col_rel_t *r = b->sources[b->kind];
        if ((!b->kind && !r) || (r && !wl_columnar_eval_delta_build_schema(r)))
            return ENOTSUP;
        if (r && b->kind == 2 && r->base_nrows > r->nrows)
            return EINVAL;
        if (++b->kind != 3) {
            b->registry = 0;
            b->offset = 0;
            b->phase = WL_COLUMNAR_DELTA_BUILD_LOOKUP;
            return 0;
        }
        uint64_t rows = b->sources[0]->nrows;
        if (b->sources[1]) rows += b->sources[1]->nrows;
        if (b->sources[2]) rows += b->sources[2]->nrows -
                b->sources[2]->base_nrows;
        if (rows > UINT32_MAX || rows > SIZE_MAX / sizeof(int64_t))
            return EOVERFLOW;
        b->entry_rows = rows;
        if (b->preflight) {
            b->duplicate_si = 0;
            b->duplicate_ri = 0;
            b->offset = 0;
            b->phase = WL_COLUMNAR_DELTA_BUILD_PREFLIGHT_DUP;
        } else {
            b->duplicate = 0;
            b->offset = 0;
            b->phase = WL_COLUMNAR_DELTA_BUILD_DUP;
        }
        return 0;
    }
    case WL_COLUMNAR_DELTA_BUILD_PREFLIGHT_DUP:
        while (b->duplicate_si < plan->stratum_count) {
            const wl_plan_stratum_t *previous_stratum =
                &plan->strata[b->duplicate_si];
            if (b->duplicate_ri >= previous_stratum->relation_count) {
                b->duplicate_si++;
                b->duplicate_ri = 0;
                return 0;
            }
            if (b->duplicate_si > b->si
                || (b->duplicate_si == b->si
                && b->duplicate_ri >= b->ri))
                break;
            const char *previous =
                previous_stratum->relations[b->duplicate_ri].name;
            char current_char = name[b->offset];
            char previous_char = previous[b->offset];
            if (current_char != previous_char) {
                if (b->duplicate_ri + 1
                    < previous_stratum->relation_count) {
                    b->duplicate_ri++;
                } else {
                    b->duplicate_si++;
                    b->duplicate_ri = 0;
                }
                b->offset = 0;
                return 0;
            }
            if (!current_char) {
                b->ri++;
                b->phase = WL_COLUMNAR_DELTA_BUILD_HEAD;
                return 0;
            }
            b->offset++;
            return 0;
        }
        {
            uint64_t add = b->entry_rows * sizeof(int64_t);
            if (b->count == UINT32_MAX || b->length == SIZE_MAX
                || !wl_columnar_memory_size_add(add, b->length + 1, &add)
                || !wl_columnar_memory_size_add(b->payload_bytes, add,
                &b->payload_bytes))
                return EOVERFLOW;
            b->count++;
            b->ri++;
            b->phase = WL_COLUMNAR_DELTA_BUILD_HEAD;
        }
        return 0;
    case WL_COLUMNAR_DELTA_BUILD_ALLOC: {
        uint64_t bytes;
        if (!wl_columnar_memory_size_mul(b->count, sizeof(*e), &bytes)
            || !wl_columnar_memory_size_add(bytes, sizeof(*o), &bytes)
            || bytes > SIZE_MAX || b->payload_bytes > UINT64_MAX - bytes)
            return EOVERFLOW;
        int rc = wl_columnar_eval_delta_reserve(sess, bytes, &b->metadata);
        if (rc) return rc;
        b->metadata_held = sess->memory_governor != NULL;
        b->metadata_bytes = bytes;
        b->observer = malloc((size_t)bytes);
        if (!b->observer) return ENOMEM;
        b->zero = 0;
        b->phase = WL_COLUMNAR_DELTA_BUILD_ZERO;
        return 0;
    }
    case WL_COLUMNAR_DELTA_BUILD_ZERO:
        ((unsigned char *)o)[b->zero++] = 0;
        if (b->zero == b->metadata_bytes) {
            wl_columnar_memory_reservation_init(&o->metadata);
            wl_columnar_memory_reservation_init(&o->payload);
            wl_columnar_memory_reservation_init(&o->staging);
            if (b->metadata_held) {
                if (!wl_columnar_memory_reservation_move(&o->metadata,
                    &b->metadata))
                    return EINVAL;
                b->metadata_held = false;
                o->governor = sess->memory_governor;
                wl_columnar_memory_governor_ref_retain(o->governor);
            }
            o->metadata_bytes = b->metadata_bytes;
            o->affected_mask = b->mask;
            b->phase = WL_COLUMNAR_DELTA_BUILD_PAYLOAD;
        }
        return 0;
    case WL_COLUMNAR_DELTA_BUILD_PAYLOAD: {
        int rc = wl_columnar_eval_delta_reserve(sess, b->payload_bytes,
                &o->payload);
        if (rc) return rc;
        o->payload_bytes = b->payload_bytes;
        b->preflight = false;
        b->si = b->ri = 0;
        b->phase = WL_COLUMNAR_DELTA_BUILD_HEAD;
        return 0;
    }
    case WL_COLUMNAR_DELTA_BUILD_DUP:
        if (b->duplicate == o->count) {
            b->phase = WL_COLUMNAR_DELTA_BUILD_ENTRY;
        } else {
            char a = name[b->offset];
            char c = o->entries[b->duplicate].name[b->offset];
            if (a != c) {
                b->duplicate++; b->offset = 0;
            }else if (!a) {
                b->ri++; b->phase = WL_COLUMNAR_DELTA_BUILD_HEAD;
            }else b->offset++;
        }
        return 0;
    case WL_COLUMNAR_DELTA_BUILD_ENTRY:
        e = &o->entries[o->count];
        e->name = malloc(b->length + 1);
        if (!e->name) return ENOMEM;
        o->count++;
        e->ncols = 1;
        e->nrows = b->sources[0]->nrows
            + (b->sources[1] ? b->sources[1]->nrows : 0);
        e->pending_nrows = b->sources[2]
            ? b->sources[2]->nrows - b->sources[2]->base_nrows : 0;
        b->offset = 0;
        b->phase = WL_COLUMNAR_DELTA_BUILD_NAME_COPY;
        return 0;
    case WL_COLUMNAR_DELTA_BUILD_NAME_COPY:
        e->name[b->offset] = name[b->offset];
        if (b->offset++ == b->length) b->phase = WL_COLUMNAR_DELTA_BUILD_ROWS;
        return 0;
    case WL_COLUMNAR_DELTA_BUILD_ROWS:
        if (e->nrows + e->pending_nrows) {
            e->rows = malloc((size_t)(e->nrows + e->pending_nrows) *
                    sizeof(int64_t));
            if (!e->rows) return ENOMEM;
        }
        b->kind = b->row = b->copied = 0;
        b->phase = WL_COLUMNAR_DELTA_BUILD_ACQUIRE;
        return 0;
    case WL_COLUMNAR_DELTA_BUILD_COPY: {
        col_rel_t *r = b->sources[b->kind];
        uint32_t start = b->kind == 2 ? r->base_nrows : 0;
        if (b->row ==
            r->nrows - start) b->phase = WL_COLUMNAR_DELTA_BUILD_RELEASE;
        else e->rows[b->copied++] = r->columns[0][start + b->row++];
        return 0;
    }
    case WL_COLUMNAR_DELTA_BUILD_SORT:
        if (b->sort_end - b->sort_begin < 2
            || (!b->heap_build && !b->heap_sifting && b->heap_size <= 1)) {
            if (!b->sorting_pending && e->pending_nrows) {
                b->sorting_pending = true;
                b->sort_begin = e->nrows;
                b->sort_end = e->nrows + e->pending_nrows;
                b->heap_size = b->sort_end - b->sort_begin;
                b->i = b->heap_size / 2;
                b->heap_build = true;
                b->heap_sifting = b->heap_child_selected = false;
            } else {
                b->i = b->j = b->out = 0;
                b->phase = WL_COLUMNAR_DELTA_BUILD_SUBTRACT;
            }
        } else if (b->heap_build && !b->heap_sifting && !b->i) {
            b->heap_build = false;
            b->heap_size = b->sort_end - b->sort_begin;
        } else if (b->heap_build && !b->heap_sifting) {
            b->heap_root = --b->i;
            b->heap_sifting = true;
            b->heap_child_selected = false;
        } else if (!b->heap_build && !b->heap_sifting) {
            wl_columnar_eval_delta_observer_swap(e->rows, b->sort_begin,
                b->sort_begin + b->heap_size - 1, 1);
            b->heap_size--;
            if (b->heap_size > 1) {
                b->heap_root = 0;
                b->heap_sifting = true;
            }
        } else if (b->heap_root >= b->heap_size / 2) {
            b->heap_sifting = false;
            b->heap_child_selected = false;
        } else if (!b->heap_child_selected) {
            b->heap_child = b->heap_root * 2 + 1;
            if (b->heap_child + 1 < b->heap_size
                && wl_columnar_eval_delta_build_value_cmp(
                    e->rows[b->sort_begin + b->heap_child],
                    e->rows[b->sort_begin + b->heap_child + 1]) < 0)
                b->heap_child++;
            b->heap_child_selected = true;
        } else {
            if (wl_columnar_eval_delta_build_value_cmp(
                    e->rows[b->sort_begin + b->heap_root],
                    e->rows[b->sort_begin + b->heap_child]) >= 0) {
                b->heap_sifting = false;
            } else {
                wl_columnar_eval_delta_observer_swap(e->rows,
                    b->sort_begin + b->heap_root,
                    b->sort_begin + b->heap_child, 1);
                b->heap_root = b->heap_child;
            }
            b->heap_child_selected = false;
        }
        return 0;
    case WL_COLUMNAR_DELTA_BUILD_SUBTRACT:
        if (b->i == e->nrows) {
            e->nrows = b->out; b->i = b->out = 0;
            b->phase = WL_COLUMNAR_DELTA_BUILD_UNIQUE;
        } else if (b->j < e->pending_nrows
            && wl_columnar_eval_delta_build_value_cmp(
                e->rows[e->nrows + b->j], e->rows[b->i]) < 0) b->j++;
        else if (b->j < e->pending_nrows
            && wl_columnar_eval_delta_build_value_cmp(
                e->rows[e->nrows + b->j], e->rows[b->i]) == 0) {
            b->i++; b->j++;
        }else e->rows[b->out++] = e->rows[b->i++];
        return 0;
    case WL_COLUMNAR_DELTA_BUILD_UNIQUE:
        if (b->i == e->nrows) {
            e->nrows = b->out; b->ri++;
            b->phase = WL_COLUMNAR_DELTA_BUILD_HEAD;
        } else {
            int64_t v = e->rows[b->i++];
            if (!b->out || e->rows[b->out - 1] != v) e->rows[b->out++] = v;
        }
        return 0;
    case WL_COLUMNAR_DELTA_BUILD_COMMIT:
        if (o->governor && (!wl_columnar_memory_commit(&o->metadata, o)
            || (o->payload_bytes && !wl_columnar_memory_commit(&o->payload,
            o))))
            return EINVAL;
        b->phase = WL_COLUMNAR_DELTA_BUILD_READY;
        return 0;
    default:
        return EINVAL;
    }
}

int
wl_columnar_eval_delta_observer_build_advance(wl_col_session_t *sess,
    wl_evaluation_control_t *control, uint64_t id, bool *ready)
{
    if (!sess || !control || !ready || !id
        || wl_evaluation_control_active_attempt_id(control) != id)
        return EINVAL;
    *ready = false;
    wl_columnar_eval_delta_observer_build_t *b = sess->delta_observer_build;
    if (!b || b->identity != (uintptr_t)b || b->session != sess
        || sess->delta_observer)
        return EINVAL;
    if (b->error) return b->error;
    int rc;
    if (b->phase == WL_COLUMNAR_DELTA_BUILD_ACQUIRE
        || b->phase == WL_COLUMNAR_DELTA_BUILD_RELEASE) {
        col_rel_t *r = b->sources[b->kind];
        bool release = b->phase == WL_COLUMNAR_DELTA_BUILD_RELEASE;
        rc = r ? wl_columnar_relation_reader_step(r, &b->reader, release,
                control, id)
            : wl_evaluation_control_charge(control, 1);
        if (rc) return rc;
        if (!release && r) {
            b->row = 0;
            b->phase = WL_COLUMNAR_DELTA_BUILD_COPY;
        } else if (++b->kind == 3) {
            b->sort_begin = 0;
            b->sort_end = b->observer->entries[b->observer->count - 1].nrows;
            b->heap_size = b->sort_end;
            b->i = b->heap_size / 2;
            b->heap_build = true;
            b->heap_sifting = b->heap_child_selected = false;
            b->sorting_pending = false;
            b->phase = WL_COLUMNAR_DELTA_BUILD_SORT;
        } else b->phase = WL_COLUMNAR_DELTA_BUILD_ACQUIRE;
        if (wl_evaluation_control_active_attempt_id(control) !=
            id) return EINVAL;
        rc = wl_evaluation_control_charge(control, 0);
        return rc ? rc : EAGAIN;
    }
    rc = wl_evaluation_control_charge(control, 1);
    if (rc) return rc;
    if (b->phase == WL_COLUMNAR_DELTA_BUILD_READY) {
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
        sess->delta_observer = b->observer;
        sess->delta_observer_reserved_bytes = b->metadata_bytes +
            b->payload_bytes;
        sess->delta_observer_build = NULL;
        free(b);
        *ready = true;
        return 0;
    }
    int action = wl_columnar_eval_delta_build_action(sess, b);
    if (action > 0) b->error = action;
    if (wl_evaluation_control_active_attempt_id(control) != id) return EINVAL;
    rc = wl_evaluation_control_charge(control, 0);
    return rc > 0 ? rc : action ? action : rc ? rc : EAGAIN;
}

int
wl_columnar_eval_delta_observer_build_discard(wl_col_session_t *sess)
{
    if (!sess) return EINVAL;
    wl_columnar_eval_delta_observer_build_t *b = sess->delta_observer_build;
    if (!b) return 0;
    if (b->identity != (uintptr_t)b || b->session != sess) return EINVAL;
    if (b->reader.identity || b->reader.descriptor_held || b->reader.token.owner
        || b->reader.token.secondary_owner || b->reader.token.identity
        || b->reader.token.transferable || b->reader.token.thread_valid) {
        int rc = wl_columnar_relation_reader_step(b->sources[b->kind],
                &b->reader, true, NULL, 0);
        return rc ? rc : EAGAIN;
    }
    wl_columnar_eval_delta_observer_t *o = b->observer;
    if (o && b->zero < b->metadata_bytes) {
        free(o); b->observer = NULL;
        return EAGAIN;
    }
    if (o && o->count) {
        wl_columnar_eval_delta_observer_entry_t *e = &o->entries[o->count - 1];
        free(e->rows); free(e->name); o->count--;
        return EAGAIN;
    }
    if (o) {
        int rc = wl_columnar_eval_delta_observer_release_credit(sess,
                WL_COLUMNAR_EVAL_DELTA_OBSERVER_CREDIT_PAYLOAD, &o->payload);
        if (rc) return rc;
        if (o->governor) {
            if (!wl_columnar_memory_reservation_move(&b->metadata,
                &o->metadata))
                return EINVAL;
            b->metadata_held = true;
            b->governor = o->governor;
            o->governor = NULL;
        }
        free(o); b->observer = NULL;
        return EAGAIN;
    }
    if (b->metadata_held) {
        int rc = wl_columnar_eval_delta_observer_release_credit(sess,
                WL_COLUMNAR_EVAL_DELTA_OBSERVER_CREDIT_METADATA, &b->metadata);
        if (rc) return rc;
        b->metadata_held = false;
        return EAGAIN;
    }
    if (b->governor) {
        wl_columnar_memory_governor_ref_release(b->governor);
        b->governor = NULL;
        return EAGAIN;
    }
    sess->delta_observer_build = NULL;
    free(b);
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
    if (sess && sess->delta_observer_prepare)
        return EBUSY;
    wl_columnar_eval_delta_observer_t *observer = sess->delta_observer;
    if (observer && (observer->active || sess->delta_publish_active))
        return EBUSY;
    if (observer) {
        if (observer->retirement == WL_COLUMNAR_EVAL_DELTA_OBSERVER_RETIRE_NONE)
            observer->retirement =
                WL_COLUMNAR_EVAL_DELTA_OBSERVER_RETIRE_DISCARD;
        sess->delta_observer_retirement = observer->retirement;
        int rc = wl_columnar_eval_delta_observer_readers(observer);
        if (rc != 0)
            return rc;

        rc = wl_columnar_eval_delta_observer_clear_stage(sess);
        if (rc != 0)
            return rc;

        for (uint32_t i = 0; i < observer->count; i++) {
            free(observer->entries[i].rows);
            observer->entries[i].rows = NULL;
            free(observer->entries[i].name);
            observer->entries[i].name = NULL;
        }
        if (observer->compound_hold.arena) {
            rc = wl_arena_compound_arena_gc_hold_release(
                &observer->compound_hold);
            if (rc != 0)
                return rc;
        }
        rc = wl_columnar_eval_delta_observer_release_credit(sess,
                WL_COLUMNAR_EVAL_DELTA_OBSERVER_CREDIT_PAYLOAD,
                &observer->payload);
        if (rc != 0)
            return rc;
        observer->payload_bytes = 0;
        sess->delta_observer_reserved_bytes = observer->metadata_bytes;

        if (observer->governor) {
            if (!wl_columnar_memory_reservation_move(
                    &sess->delta_observer_retirement_metadata,
                    &observer->metadata))
                return EINVAL;
            sess->delta_observer_retirement_governor = observer->governor;
            observer->governor = NULL;
        }
        sess->delta_observer_retirement_gc = observer->gc_requested;
        free(observer);
        sess->delta_observer = NULL;
    }

    if (atomic_load_explicit(&sess->delta_observer_retirement_metadata.state,
        memory_order_acquire) != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY) {
        int rc = wl_columnar_eval_delta_observer_release_credit(sess,
                WL_COLUMNAR_EVAL_DELTA_OBSERVER_CREDIT_METADATA,
                &sess->delta_observer_retirement_metadata);
        if (rc != 0)
            return rc;
    }
    if (sess->delta_observer_retirement_governor) {
        wl_columnar_memory_governor_ref_release(
            sess->delta_observer_retirement_governor);
        sess->delta_observer_retirement_governor = NULL;
    }
    bool gc_requested = sess->delta_observer_retirement_gc;
    sess->delta_observer_retirement_gc = false;
    sess->delta_observer_retirement =
        WL_COLUMNAR_EVAL_DELTA_OBSERVER_RETIRE_NONE;
    sess->delta_observer_reserved_bytes = 0;
    if (gc_requested && !sess->coordinator && sess->compound_arena
        && sess->rotation_ops && sess->rotation_ops->gc_epoch_boundary)
        sess->rotation_ops->gc_epoch_boundary(sess);
    return 0;
}

int
wl_columnar_eval_delta_observer_begin(wl_col_session_t *sess, uint64_t mask)
{
    if (sess->delta_observer_build || sess->delta_observer_prepare)
        return EBUSY;
    if (sess->delta_observer) {
        if (sess->delta_observer->retirement
            != WL_COLUMNAR_EVAL_DELTA_OBSERVER_RETIRE_BEGIN_ABORT)
            return sess->delta_observer_retirement
                   == WL_COLUMNAR_EVAL_DELTA_OBSERVER_RETIRE_NONE
                ? 0 : EBUSY;
        int cleanup_rc = wl_columnar_eval_delta_observer_discard(sess);
        if (cleanup_rc != 0)
            return cleanup_rc;
    } else if (sess->delta_observer_retirement
        != WL_COLUMNAR_EVAL_DELTA_OBSERVER_RETIRE_NONE) {
        return EBUSY;
    }
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
        if (sess->memory_governor) {
            if (!wl_columnar_memory_reservation_move(
                    &sess->delta_observer_retirement_metadata, &metadata))
                return EINVAL;
            sess->delta_observer_retirement_governor = sess->memory_governor;
            wl_columnar_memory_governor_ref_retain(
                sess->delta_observer_retirement_governor);
            sess->delta_observer_retirement =
                WL_COLUMNAR_EVAL_DELTA_OBSERVER_RETIRE_BEGIN_ABORT;
            sess->delta_observer_reserved_bytes = bytes;
            (void)wl_columnar_eval_delta_observer_discard(sess);
        }
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
    rc = wl_columnar_eval_delta_observer_readers(observer);
    if (rc != 0)
        goto fail;
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
    if (sess->delta_observer)
        sess->delta_observer->retirement =
            WL_COLUMNAR_EVAL_DELTA_OBSERVER_RETIRE_BEGIN_ABORT;
    sess->delta_observer_retirement =
        WL_COLUMNAR_EVAL_DELTA_OBSERVER_RETIRE_BEGIN_ABORT;
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

/* Resumable preparation keeps the observer private until every current row
 * has been captured and the signed event buffer is complete. */
enum wl_columnar_eval_delta_prepare_phase {
    WL_COLUMNAR_DELTA_PREPARE_PREFLIGHT,
    WL_COLUMNAR_DELTA_PREPARE_CAPTURE,
    WL_COLUMNAR_DELTA_PREPARE_SORT,
    WL_COLUMNAR_DELTA_PREPARE_UNIQUE,
    WL_COLUMNAR_DELTA_PREPARE_MERGE,
    WL_COLUMNAR_DELTA_PREPARE_READY
};

struct wl_columnar_eval_delta_observer_prepare {
    uintptr_t identity;
    wl_col_session_t *session;
    wl_columnar_eval_delta_observer_t *observer;
    wl_columnar_relation_reader_step_t reader;
    uint64_t reserve_bytes, row_capacity;
    int error;
    col_rel_t *relation;
    uint32_t lookup_registry;
    size_t lookup_offset;
    uint32_t entry, row, old, current, heap_size, heap_root, heap_child;
    unsigned phase;
    bool reader_held, heap_build, heap_sifting, child_selected,
        sort_initialized, preflight_captured, relation_selected,
        lookup_done, commit_completed;
};

#ifdef WL_SESSION_TEST_HOOKS
bool wl_columnar_eval_delta_test_fail_prepare_current_alloc;
void (*wl_columnar_eval_delta_test_after_prepare_commit)(
    wl_evaluation_control_t *);
#endif

static bool
wl_columnar_eval_delta_prepare_lookup_step(wl_col_session_t *sess,
    wl_columnar_eval_delta_observer_prepare_t *p, const char *name)
{
    if (p->lookup_registry == sess->nrels) {
        p->relation = NULL;
        p->lookup_done = true;
        return true;
    }
    col_rel_t *r = sess->rels[p->lookup_registry];
    if (!r || !r->name) {
        p->lookup_registry++;
        p->lookup_offset = 0;
        return false;
    }
    char expected = name[p->lookup_offset];
    char actual = r->name[p->lookup_offset];
    if (expected != actual) {
        p->lookup_registry++;
        p->lookup_offset = 0;
    } else if (!actual) {
        p->relation = r;
        p->lookup_done = true;
        return true;
    } else p->lookup_offset++;
    return false;
}

static void
wl_columnar_eval_delta_prepare_lookup_reset(
    wl_columnar_eval_delta_observer_prepare_t *p)
{
    p->lookup_registry = 0;
    p->lookup_offset = 0;
    p->lookup_done = false;
    p->relation = NULL;
}

static int
wl_columnar_eval_delta_prepare_current_alloc(
    wl_columnar_eval_delta_observer_entry_t *entry)
{
    if (entry->current || !entry->current_nrows)
        return 0;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_delta_test_fail_prepare_current_alloc) {
        wl_columnar_eval_delta_test_fail_prepare_current_alloc = false;
        return ENOMEM;
    }
#endif
    entry->current = malloc((size_t)entry->current_nrows * sizeof(int64_t));
    return entry->current ? 0 : ENOMEM;
}

int
wl_columnar_eval_delta_observer_prepare_start(wl_col_session_t *sess)
{
    if (!sess || !sess->delta_observer || sess->delta_observer_prepare
        || sess->delta_observer_build || sess->delta_publish_active)
        return EBUSY;
    wl_columnar_eval_delta_observer_t *o = sess->delta_observer;
    if (o->cancelled || o->delivered)
        return EINVAL;
    int rc = wl_columnar_eval_delta_observer_readers(o);
    if (rc) return rc;
    rc = wl_columnar_eval_delta_observer_clear_stage(sess);
    if (rc) return rc;
    wl_columnar_eval_delta_observer_prepare_t *p = calloc(1, sizeof(*p));
    if (!p) return ENOMEM;
    p->identity = (uintptr_t)p;
    p->session = sess;
    p->observer = o;
    p->phase = WL_COLUMNAR_DELTA_PREPARE_PREFLIGHT;
    sess->delta_observer_prepare = p;
    return 0;
}

static int
wl_columnar_eval_delta_observer_prepare_advance_action(wl_col_session_t *sess,
    wl_evaluation_control_t *control, uint64_t id, bool *ready)
{
    if (!sess || !control || !id || !ready
        || wl_evaluation_control_active_attempt_id(control) != id)
        return EINVAL;
    *ready = false;
    wl_columnar_eval_delta_observer_prepare_t *p =
        sess->delta_observer_prepare;
    if (!p || p->identity != (uintptr_t)p || p->session != sess
        || p->observer != sess->delta_observer)
        return EINVAL;
    if (p->observer->cancelled)
        return ECANCELED;
    bool reader_action = p->relation_selected && !p->reader_held;
    if (p->reader_held) {
        reader_action = !p->reader.token.owner
            || (p->phase == WL_COLUMNAR_DELTA_PREPARE_PREFLIGHT
            && p->preflight_captured)
            || (p->phase == WL_COLUMNAR_DELTA_PREPARE_CAPTURE
            && p->row == p->observer->entries[p->entry].current_nrows);
    }
    int rc = reader_action ? 0 : wl_evaluation_control_charge(control, 1);
    if (rc) return rc;
    wl_columnar_eval_delta_observer_t *o = p->observer;
    if (p->phase == WL_COLUMNAR_DELTA_PREPARE_PREFLIGHT) {
        if (p->entry == o->count) {
            uint64_t bytes, event_bytes, current_bytes;
            if (!wl_columnar_memory_size_mul(p->row_capacity,
                sizeof(int64_t), &current_bytes)
                || !wl_columnar_memory_size_mul(p->row_capacity,
                sizeof(*o->events), &event_bytes)
                || !wl_columnar_memory_size_add(current_bytes, event_bytes,
                &bytes) || bytes > SIZE_MAX
                || bytes > UINT64_MAX - o->metadata_bytes - o->payload_bytes)
                return EOVERFLOW;
            rc = wl_columnar_eval_delta_reserve(sess, bytes, &o->staging);
            if (rc) return rc;
            o->staging_bytes = bytes;
            sess->delta_observer_reserved_bytes += bytes;
            if (event_bytes) {
                o->events = malloc((size_t)event_bytes);
                if (!o->events) return ENOMEM;
            }
            p->reserve_bytes = bytes;
            p->entry = 0;
            p->phase = WL_COLUMNAR_DELTA_PREPARE_CAPTURE;
        } else {
            wl_columnar_eval_delta_observer_entry_t *e = &o->entries[p->entry];
            if (!p->reader_held) {
                if (!p->relation_selected) {
                    if (e->ncols != 1) return ENOTSUP;
                    if (!p->lookup_done) {
                        (void)wl_columnar_eval_delta_prepare_lookup_step(sess,
                            p, e->name);
                        goto action_done;
                    }
                    if (!p->relation) {
                        e->current_nrows = 0; e->current_ncols = 1;
                        if (!wl_columnar_memory_size_add(p->row_capacity,
                            e->nrows, &p->row_capacity)) return EOVERFLOW;
                        p->entry++;
                        wl_columnar_eval_delta_prepare_lookup_reset(p);
                    } else p->relation_selected = true;
                } else {
                    rc = wl_columnar_relation_reader_step(p->relation,
                            &p->reader, false, control, id);
                    p->reader_held = p->reader.descriptor_held
                        || p->reader.token.owner;
                    if (rc) return rc;
                    p->reader_held = true;
                    p->relation_selected = false;
                }
            } else if (p->preflight_captured) {
                rc = wl_columnar_relation_reader_step(p->relation,
                        &p->reader, true, control, id);
                p->reader_held = p->reader.descriptor_held
                    || p->reader.token.owner;
                if (!p->reader_held) {
                    p->preflight_captured = false;
                    p->relation_selected = false;
                    p->relation = NULL;
                    p->entry++;
                    wl_columnar_eval_delta_prepare_lookup_reset(p);
                }
                if (rc) return rc;
            } else if (!p->reader.token.owner) {
                rc = wl_columnar_relation_reader_step(p->relation,
                        &p->reader, false, control, id);
                p->reader_held = p->reader.descriptor_held
                    || p->reader.token.owner;
                if (rc) return rc;
                p->reader_held = true;
            } else if (!p->preflight_captured) {
                if (!wl_columnar_eval_delta_build_schema(p->relation))
                    return ENOTSUP;
                e->current_nrows = p->relation->nrows;
                e->current_ncols = 1;
                if (!wl_columnar_memory_size_add(p->row_capacity,
                    (uint64_t)e->nrows + e->current_nrows,
                    &p->row_capacity)) return EOVERFLOW;
                p->preflight_captured = true;
            }
        }
    } else if (p->phase == WL_COLUMNAR_DELTA_PREPARE_CAPTURE) {
        if (p->entry == o->count) {
            p->entry = 0;
            p->phase = WL_COLUMNAR_DELTA_PREPARE_SORT;
        } else {
            wl_columnar_eval_delta_observer_entry_t *e = &o->entries[p->entry];
            if (!p->reader_held && !p->relation_selected) {
                if (!p->lookup_done) {
                    (void)wl_columnar_eval_delta_prepare_lookup_step(sess,
                        p, e->name);
                    goto action_done;
                }
                if (!p->relation || !e->current_nrows) {
                    p->entry++; p->row = 0;
                    wl_columnar_eval_delta_prepare_lookup_reset(p);
                } else p->relation_selected = true;
            } else if (!p->reader_held) {
                rc = wl_columnar_relation_reader_step(p->relation,
                        &p->reader, false, control, id);
                p->reader_held = p->reader.descriptor_held
                    || p->reader.token.owner;
                if (rc) return rc;
                p->reader_held = true;
                p->relation_selected = false;
                rc = wl_columnar_eval_delta_prepare_current_alloc(e);
                if (rc) return rc;
            } else if (p->row == e->current_nrows) {
                rc = wl_columnar_relation_reader_step(p->relation, &p->reader,
                        true, control, id);
                p->reader_held = p->reader.descriptor_held
                    || p->reader.token.owner;
                if (!p->reader_held) {
                    p->relation = NULL;
                    p->relation_selected = false;
                    p->entry++; p->row = 0;
                    wl_columnar_eval_delta_prepare_lookup_reset(p);
                }
                if (rc) return rc;
            } else if (!p->reader.token.owner) {
                rc = wl_columnar_relation_reader_step(p->relation,
                        &p->reader, false, control, id);
                p->reader_held = p->reader.descriptor_held
                    || p->reader.token.owner;
                if (rc) return rc;
                p->reader_held = true;
                rc = wl_columnar_eval_delta_prepare_current_alloc(e);
                if (rc) return rc;
            } else if (p->row < e->current_nrows) {
                e->current[p->row] = p->relation->columns[0][p->row];
                p->row++;
            }
        }
    } else if (p->phase == WL_COLUMNAR_DELTA_PREPARE_SORT) {
        if (p->entry == o->count) {
            p->entry = p->old = p->current = 0;
            p->phase = WL_COLUMNAR_DELTA_PREPARE_MERGE;
        } else {
            wl_columnar_eval_delta_observer_entry_t *e = &o->entries[p->entry];
            if (!p->sort_initialized) {
                p->heap_size = e->current_nrows;
                p->heap_root = p->heap_size / 2;
                p->heap_build = true;
                p->sort_initialized = true;
            }
            if (p->heap_size < 2 || (!p->heap_build && !p->heap_sifting
                && p->heap_size <= 1)) {
                p->old = p->current = 0;
                p->phase = WL_COLUMNAR_DELTA_PREPARE_UNIQUE;
            } else if (p->heap_build && !p->heap_sifting
                && !p->heap_root) {
                p->heap_build = false;
            } else {
                if (p->heap_build && !p->heap_sifting) {
                    p->heap_root--;
                    p->heap_sifting = true;
                    p->child_selected = false;
                } else if (!p->heap_build && !p->heap_sifting) {
                    int64_t t = e->current[0];
                    e->current[0] = e->current[p->heap_size - 1];
                    e->current[p->heap_size - 1] = t;
                    p->heap_size--;
                    if (p->heap_size > 1) {
                        p->heap_root = 0;
                        p->heap_sifting = true;
                    }
                } else if (p->heap_root >= p->heap_size / 2) {
                    p->heap_sifting = false;
                    p->child_selected = false;
                } else if (!p->child_selected) {
                    p->heap_child = p->heap_root * 2 + 1;
                    if (p->heap_child + 1 < p->heap_size
                        && memcmp(&e->current[p->heap_child],
                        &e->current[p->heap_child + 1],
                        sizeof(int64_t)) < 0)
                        p->heap_child++;
                    p->child_selected = true;
                } else {
                    if (memcmp(&e->current[p->heap_root],
                        &e->current[p->heap_child], sizeof(int64_t)) >= 0) {
                        p->heap_sifting = false;
                    } else {
                        int64_t t = e->current[p->heap_root];
                        e->current[p->heap_root] = e->current[p->heap_child];
                        e->current[p->heap_child] = t;
                        p->heap_root = p->heap_child;
                    }
                    p->child_selected = false;
                }
            }
        }
    } else if (p->phase == WL_COLUMNAR_DELTA_PREPARE_UNIQUE) {
        wl_columnar_eval_delta_observer_entry_t *e = &o->entries[p->entry];
        if (p->old == e->current_nrows) {
            e->current_nrows = p->current;
            p->entry++; p->heap_size = p->old = p->current = 0;
            p->sort_initialized = p->heap_build = p->heap_sifting = false;
            p->child_selected = false;
            p->phase = WL_COLUMNAR_DELTA_PREPARE_SORT;
        } else {
            int64_t v = e->current[p->old++];
            if (!p->current || e->current[p->current - 1] != v)
                e->current[p->current++] = v;
        }
    } else if (p->phase == WL_COLUMNAR_DELTA_PREPARE_MERGE) {
        if (p->entry == o->count) {
            p->phase = WL_COLUMNAR_DELTA_PREPARE_READY;
        } else {
            wl_columnar_eval_delta_observer_entry_t *e = &o->entries[p->entry];
            if (p->old == e->nrows && p->current == e->current_nrows) {
                p->entry++; p->old = p->current = 0;
            } else {
                int cmp = p->old == e->nrows ? 1
                    : p->current == e->current_nrows ? -1
                    : memcmp(e->rows + p->old, e->current + p->current,
                        sizeof(int64_t));
                if (cmp < 0) {
                    wl_columnar_eval_delta_observer_event(o, e->name,
                        e->rows + p->old++, 1, -1);
                } else if (cmp > 0) {
                    wl_columnar_eval_delta_observer_event(o, e->name,
                        e->current + p->current++, 1, +1);
                } else p->old++, p->current++;
            }
        }
    } else if (p->phase == WL_COLUMNAR_DELTA_PREPARE_READY) {
        if (!p->commit_completed) {
            if (o->governor && p->reserve_bytes
                && !wl_columnar_memory_commit(&o->staging, o)) return EINVAL;
            p->commit_completed = true;
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_columnar_eval_delta_test_after_prepare_commit)
                wl_columnar_eval_delta_test_after_prepare_commit(control);
#endif
            if (wl_evaluation_control_active_attempt_id(control) != id)
                return EINVAL;
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) return rc;
        } else {
            if (wl_evaluation_control_active_attempt_id(control) != id)
                return EINVAL;
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) return rc;
        }
        o->prepared = true;
        sess->delta_observer_prepare = NULL;
        free(p);
        *ready = true;
        return 0;
    } else return EINVAL;
action_done:
    if (wl_evaluation_control_active_attempt_id(control) != id) return EINVAL;
    rc = wl_evaluation_control_charge(control, 0);
    return rc ? rc : EAGAIN;
}

int
wl_columnar_eval_delta_observer_prepare_advance(wl_col_session_t *sess,
    wl_evaluation_control_t *control, uint64_t id, bool *ready)
{
    if (!sess || !control || !id || !ready
        || wl_evaluation_control_active_attempt_id(control) != id)
        return EINVAL;
    wl_columnar_eval_delta_observer_prepare_t *p =
        sess->delta_observer_prepare;
    if (!p || p->identity != (uintptr_t)p || p->session != sess
        || p->observer != sess->delta_observer)
        return EINVAL;
    *ready = false;
    if (p->error)
        return p->error;
    int rc = wl_columnar_eval_delta_observer_prepare_advance_action(sess,
            control, id, ready);
    if (rc > 0 && rc != EAGAIN
        && rc != WL_EVALUATION_CONTROL_WORK_LIMIT)
        p->error = rc;
    return rc;
}

int
wl_columnar_eval_delta_observer_prepare_discard(wl_col_session_t *sess)
{
    if (!sess) return EINVAL;
    wl_columnar_eval_delta_observer_prepare_t *p = sess->delta_observer_prepare;
    if (!p) return 0;
    if (p->identity != (uintptr_t)p || p->session != sess) return EINVAL;
    if (p->reader_held) {
        int rc = wl_columnar_relation_reader_step(p->relation, &p->reader, true,
                NULL, 0);
        if (rc) return rc;
        p->reader_held = p->preflight_captured = false;
        p->relation_selected = false;
        p->relation = NULL;
        return EAGAIN;
    }
    wl_columnar_eval_delta_observer_t *o = p->observer;
    int rc = wl_columnar_eval_delta_observer_readers(o);
    if (rc) return rc;
    for (uint32_t i = 0; i < o->count; i++) {
        free(o->entries[i].current);
        o->entries[i].current = NULL;
        o->entries[i].current_nrows = o->entries[i].current_ncols = 0;
    }
    free(o->events); o->events = NULL; o->event_count = 0; o->prepared = false;
    rc = wl_columnar_eval_delta_observer_release_credit(sess,
            WL_COLUMNAR_EVAL_DELTA_OBSERVER_CREDIT_STAGING, &o->staging);
    if (rc) return rc;
    o->staging_bytes = 0;
    sess->delta_observer_reserved_bytes = o->metadata_bytes + o->payload_bytes;
    sess->delta_observer_prepare = NULL;
    free(p);
    return 0;
}

int
wl_columnar_eval_delta_observer_prepare(wl_col_session_t *sess)
{
    if (sess && sess->delta_observer_prepare)
        return EBUSY;
    wl_columnar_eval_delta_observer_t *observer = sess->delta_observer;
    if (!observer || observer->cancelled || observer->delivered)
        return 0;
    if (observer->prepared)
        return 0;
    int clear_rc = wl_columnar_eval_delta_observer_readers(observer);
    if (clear_rc != 0)
        return clear_rc;
    clear_rc = wl_columnar_eval_delta_observer_clear_stage(sess);
    if (clear_rc != 0)
        return clear_rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_delta_test_observer_boundary)
        wl_columnar_eval_delta_test_observer_boundary(sess, 2);
#endif
    uint64_t bytes = 0, event_capacity = 0, event_bytes;
    int rc = 0;
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
    rc = wl_columnar_eval_delta_observer_readers(observer);
    if (rc != 0)
        goto fail;
    if (observer->governor && bytes) {
        bool committed = wl_columnar_memory_commit(&observer->staging,
                observer);
        assert(committed);
        (void)committed;
    }
    observer->prepared = true;
    return 0;
fail:
    if (wl_columnar_eval_delta_observer_readers(observer) == 0)
        (void)wl_columnar_eval_delta_observer_clear_stage(sess);
    return rc;
}

void
wl_columnar_eval_delta_observer_publish(wl_col_session_t *sess)
{
    wl_columnar_eval_delta_observer_t *observer = sess->delta_observer;
    if (!observer || observer->delivered || !observer->prepared)
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
    if (!sess->delta_observer)
        return wl_columnar_eval_delta_observer_discard(sess);
    sess->delta_observer->active = false;
    sess->delta_observer->retirement =
        WL_COLUMNAR_EVAL_DELTA_OBSERVER_RETIRE_COMPLETED_STEP;
    sess->delta_observer_retirement =
        WL_COLUMNAR_EVAL_DELTA_OBSERVER_RETIRE_COMPLETED_STEP;
    return wl_columnar_eval_delta_observer_discard(sess);
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
    if (record->active || sess->cleanup_active || sess->cleanup_pending)
        return EBUSY;
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

int
wl_columnar_eval_delta_rollback_retry(wl_col_session_t *sess)
{
    if (!sess)
        return EINVAL;
    wl_columnar_eval_delta_rollback_t *record = sess->delta_rollback;
    if (!record || record->active)
        return 0;
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
/* Boundaries in wl_columnar_eval_delta_consolidate: 0 once the candidate
 * exists, before the head is appended to it; 1 after the append; 2 after
 * normalization or consolidation; 3 after prepare_replacement. */
void (*wl_columnar_eval_delta_test_consolidation_boundary)(
    wl_col_session_t *sess, col_rel_t *target, col_rel_t *candidate,
    unsigned boundary);
#endif

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
        wl_columnar_eval_delta_release_token(&reservation);
        return ENOMEM;
    }
    if (sess->memory_governor && bytes
        && !wl_columnar_memory_commit(&reservation, original)) {
        free(original);
        wl_columnar_eval_delta_release_token(&reservation);
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
    wl_columnar_eval_delta_release_token(&reservation);
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

/* Maps the result of the candidate's growth or of prepare_replacement's
 * reservations on the head below.  A refusal there returns ENOMEM with the
 * denial recorded on @rel, or ENOSPC directly; either is reported as ENOSPC
 * with the session's denial flag (#2161).  Other failures pass through. */
static int
wl_columnar_eval_delta_budget_status(wl_col_session_t *sess,
    const col_rel_t *rel, int rc)
{
    if (rc == ENOMEM && rel && rel->memory_budget_denial_pending)
        rc = ENOSPC;
    if (rc == ENOSPC)
        sess->memory_budget_denied = true;
    return rc;
}

int
wl_columnar_eval_delta_consolidate(col_rel_t *target, wl_col_session_t *sess)
{
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
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_delta_test_consolidation_boundary)
        wl_columnar_eval_delta_test_consolidation_boundary(sess, target,
            candidate, 0);
#endif
    rc = wl_columnar_eval_delta_budget_status(sess, candidate,
            col_rel_append_all(candidate, target, NULL));
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
    /* A step does not clear the head's denial evidence, so an earlier
     * refusal may still be recorded; read only what prepare_replacement
     * records, then keep both.  Consolidation runs on the thread that owns
     * the session, which no other thread reads the flag from meanwhile. */
    bool prior_denial = target->memory_budget_denial_pending;
    target->memory_budget_denial_pending = false;
    rc = wl_columnar_eval_delta_budget_status(sess, target,
            col_rel_prepare_replacement(target, candidate, &replacement));
    target->memory_budget_denial_pending |= prior_denial;
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
    rollback->active = false;
    rollback->retained = sess->eval_stratum_heads_final;
    rollback->evaluation_error = rc;
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
