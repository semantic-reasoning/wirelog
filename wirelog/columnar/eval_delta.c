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
    if (sess->delta_event_count == sess->delta_event_capacity) {
        size_t next = sess->delta_event_capacity
            ? sess->delta_event_capacity * 2 : 16;
        grown = (wl_col_delta_event_t *)realloc(sess->delta_events,
                next * sizeof(*grown));
        if (!grown)
            return ENOMEM;
        sess->delta_events = grown;
        sess->delta_event_capacity = next;
    }
    copy = (int64_t *)malloc((size_t)ncols * sizeof(*copy));
    if (!copy)
        return ENOMEM;
    memcpy(copy, row, (size_t)ncols * sizeof(*copy));
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
        /* Retraction publishes a temporary empty view.  Restoration below
         * must receive a fresh generation, never the saved one. */
        wl_columnar_relation_touch_replacement(r);
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
            wl_columnar_relation_touch_replacement(r);
        }
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
            rc = wl_columnar_eval_delta_consolidate(r, sess);
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
                        wl_columnar_relation_touch_replacement(r2);
                    }
                    free(retract_data[i]);
                }
                free((void *)retract_data);
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
        wl_columnar_relation_touch_replacement(r);
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
                free((void *)retract_data);
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
                        col_rel_row_move_raw(r, out_r, rest);
                        out_r++;
                    }
                    r->nrows = out_r;
                    wl_columnar_relation_touch_view(r);
                    break;
                } else {
                    /* Keep this row */
                    if (out_r != src_idx)
                        col_rel_row_copy_in_raw(r, out_r, src_buf);
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
    free((void *)retract_data);
    free(retract_nrows);
    return 0;
}

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
    return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
        : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
}

typedef struct {
    const char *capture_name;
    char *name;
    int64_t *rows;
    int64_t *current;
    uint32_t nrows, ncols;
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
static uint32_t
wl_columnar_eval_delta_observer_normalize(int64_t *rows, uint32_t count,
    uint32_t ncols)
{
    if (count < 2)
        return count;
    if (ncols == 0)
        return 1;
    for (uint32_t i = count / 2; i > 0; i--)
        wl_columnar_eval_delta_observer_sift(rows, i - 1, count, ncols);
    for (uint32_t end = count; end > 1; end--) {
        wl_columnar_eval_delta_observer_swap(rows, 0, end - 1, ncols);
        wl_columnar_eval_delta_observer_sift(rows, 0, end - 1, ncols);
    }
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

static void
wl_columnar_eval_delta_observer_readers(
    wl_columnar_eval_delta_observer_t *observer)
{
    for (uint32_t i = 0; i < observer->count; i++) {
        if (observer->entries[i].reader.owner) {
            int rc =
                col_rel_source_reader_release(&observer->entries[i].reader);
            assert(rc == 0);
            (void)rc;
        }
    }
}

static void
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
    wl_columnar_eval_delta_observer_release_token(&observer->staging);
    observer->staging_bytes = 0;
    sess->delta_observer_reserved_bytes = observer->metadata_bytes
        + observer->payload_bytes;
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
    wl_columnar_eval_delta_observer_readers(observer);
    wl_columnar_eval_delta_observer_clear_stage(sess);
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
            }
            uint64_t row_bytes;
            if (!wl_columnar_eval_delta_observer_row_bytes(entry->nrows,
                entry->ncols, &row_bytes)
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
            if (!wl_columnar_eval_delta_observer_row_bytes(entry->nrows,
                entry->ncols, &row_bytes)) {
                rc = EOVERFLOW;
                goto fail;
            }
            entry->rows = malloc((size_t)row_bytes);
            if (!entry->rows) {
                rc = ENOMEM;
                goto fail;
            }
            col_rel_t *rel = session_find_rel(sess, entry->name);
            for (uint32_t row = 0; row < entry->nrows; row++)
                for (uint32_t col = 0; col < entry->ncols; col++)
                    entry->rows[(size_t)row * entry->ncols + col]
                        = rel->columns[col][row];
            entry->nrows = wl_columnar_eval_delta_observer_normalize(
                entry->rows, entry->nrows, entry->ncols);
        }
        entry->capture_name = NULL;
    }
    wl_columnar_eval_delta_observer_readers(observer);
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

int
wl_columnar_eval_delta_observer_prepare(wl_col_session_t *sess)
{
    wl_columnar_eval_delta_observer_t *observer = sess->delta_observer;
    if (!observer || observer->cancelled || observer->delivered)
        return 0;
    wl_columnar_eval_delta_observer_clear_stage(sess);
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
    wl_columnar_eval_delta_observer_readers(observer);
    if (observer->governor && bytes) {
        bool committed = wl_columnar_memory_commit(&observer->staging,
                observer);
        assert(committed);
        (void)committed;
    }
    return 0;
fail:
    wl_columnar_eval_delta_observer_readers(observer);
    wl_columnar_eval_delta_observer_clear_stage(sess);
    return rc;
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
        int rc = wl_columnar_relation_delta_restore_flat(rel, entry->identity,
                entry->rows, entry->nrows, entry->ncols);
        /* A populated shared view is partial progress with its own live
         * source lease. Preserve it, rather than attempting to retire it. */
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
    col_rel_t *candidate = wl_columnar_relation_new_like_governed(
        "$consol", target, sess->memory_governor);
    if (!candidate) {
        rc = ENOMEM;
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
            if (!entry->rows || entry->alias != (pass == 0))
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
        /* When retained is set the heads carry this step's result and are
        * correct, but the step still returns non-zero and the events for
        * that change are dropped here -- so the facts moved and nothing
        * announced it.  Propagating a refused removal (#1661) has to settle
        * that: either keep the events on this path or re-emit them on the
        * retry.  Note a failure before the emission loop above has no events
        * to clear, so this only bites on the paths that fail after it. */
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
