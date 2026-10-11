/*
 * columnar/eval_stack.c - Columnar operator evaluation stack
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "columnar/internal.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

void
eval_stack_init(eval_stack_t *s)
{
    memset(s, 0, sizeof(*s));
}

int
eval_stack_push(eval_stack_t *s, col_rel_t *r, bool owned)
{
    if (s->top >= COL_STACK_MAX)
        return ENOBUFS;
    s->items[s->top].rel = r;
    s->items[s->top].owned = owned;
    s->items[s->top].is_delta = false;
    s->items[s->top].seg_boundaries = NULL;
    s->items[s->top].seg_count = 0;
    s->items[s->top].kind = WL_COLUMNAR_EVAL_ENTRY_RELATION;
    s->items[s->top].continuation = NULL;
    s->top++;
    return 0;
}

int
eval_stack_push_continuation(eval_stack_t *s,
    wl_columnar_continuation_t *continuation)
{
    if (!s || !continuation)
        return EINVAL;
    if (s->top >= COL_STACK_MAX)
        return ENOBUFS;
    s->items[s->top].rel = NULL;
    s->items[s->top].owned = true;
    s->items[s->top].is_delta = false;
    s->items[s->top].seg_boundaries = NULL;
    s->items[s->top].seg_count = 0;
    s->items[s->top].kind = WL_COLUMNAR_EVAL_ENTRY_CONTINUATION;
    s->items[s->top].continuation = continuation;
    s->top++;
    return 0;
}

/* Push with explicit delta flag (used by VARIABLE and JOIN to tag delta results). */
int
eval_stack_push_delta(eval_stack_t *s, col_rel_t *r, bool owned, bool is_delta)
{
    int rc = eval_stack_push(s, r, owned);
    if (rc == 0)
        s->items[s->top - 1].is_delta = is_delta;
    return rc;
}

eval_entry_t
eval_stack_pop(eval_stack_t *s)
{
    eval_entry_t e = { 0 };
    if (s && s->top > 0)
        e = s->items[--s->top];
    return e;
}

int
eval_entry_dispose(eval_entry_t *entry)
{
    int rc;

    if (!entry)
        return 0;

    if (entry->kind == WL_COLUMNAR_EVAL_ENTRY_CONTINUATION) {
        wl_columnar_continuation_destroy(entry->continuation);
        entry->continuation = NULL;
    } else if (entry->owned) {
        rc = col_rel_destroy_checked(entry->rel);
        if (rc != 0)
            return rc;
        entry->rel = NULL;
    }

    free(entry->seg_boundaries);
    entry->seg_boundaries = NULL;
    entry->seg_count = 0;
    entry->owned = false;
    return 0;
}

int
eval_stack_repush_entry(eval_stack_t *s, eval_entry_t *entry)
{
    if (!s || !entry)
        return EINVAL;
    if (s->top >= COL_STACK_MAX)
        return ENOBUFS;
    s->items[s->top++] = *entry;
    memset(entry, 0, sizeof(*entry));
    return 0;
}

int
eval_stack_dispose_entry(eval_stack_t *s, eval_entry_t *entry)
{
    int rc;

    if (!entry)
        return EINVAL;
    rc = eval_entry_dispose(entry);
    if (rc == 0 || !s)
        return rc;
    if (eval_stack_repush_entry(s, entry) != 0)
        return ENOBUFS;
    return rc;
}

int
eval_stack_pop_relation(eval_stack_t *s, eval_entry_t *out)
{
    eval_entry_t *entry;
    int dispose_rc;

    if (!out)
        return EINVAL;
    memset(out, 0, sizeof(*out));
    if (!s || s->top == 0)
        return EINVAL;

    entry = &s->items[s->top - 1];
    if (entry->kind == WL_COLUMNAR_EVAL_ENTRY_CONTINUATION) {
        dispose_rc = eval_entry_dispose(entry);
        if (dispose_rc != 0)
            return dispose_rc;
        s->top--;
        return ENOTSUP;
    }
    if (entry->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION) {
        dispose_rc = eval_entry_dispose(entry);
        if (dispose_rc != 0)
            return dispose_rc;
        s->top--;
        return EINVAL;
    }
    /* Relation-only operators historically reject a NULL relation.  Keep
     * that boundary distinct from raw eval_stack_pop(), which is used by
     * top-level result collection to preserve the no-result value. */
    if (!entry->rel) {
        dispose_rc = eval_entry_dispose(entry);
        if (dispose_rc != 0)
            return dispose_rc;
        s->top--;
        return EINVAL;
    }
    *out = *entry;
    s->top--;
    return 0;
}

int
eval_stack_drain(eval_stack_t *s)
{
    int rc;

    if (!s)
        return EINVAL;

    while (s->top > 0) {
        rc = eval_entry_dispose(&s->items[s->top - 1]);
        if (rc != 0)
            return rc;
        s->top--;
    }
    return 0;
}

int
eval_stack_drain_to_session(eval_stack_t *s, wl_col_session_t *sess)
{
    int rc;

    if (!s || !sess)
        return EINVAL;

    for (;;) {
        rc = eval_stack_drain(s);
        if (rc != EBUSY)
            return rc;

        /* EBUSY is expected only for the owned relation at the top of the
         * stack.  Move that exact entry into the session-owned registry so
         * the evaluator may return without losing its ownership. */
        if (s->top == 0)
            return EFAULT;
        eval_entry_t *entry = &s->items[s->top - 1];
        if (entry->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
            || !entry->owned || !entry->rel)
            return EFAULT;
        if (!wl_columnar_deferred_relation_eligible(entry->rel)) {
            /* Pool/arena storage must never be recorded in the session
             * registry: it can disappear with the evaluator's allocator.
             * The exact entry is still on @s and still owned by it, so
             * propagate the refusal rather than aborting -- a library must
             * not kill its host over a recoverable cleanup refusal.  The
             * one caller in the library is col_kfusion_drain(), whose stack
             * does not outlive the evaluator; tests drain directly too.  It
             * does not abort on this return: it hands the remaining entries
             * to wl_columnar_session_retain_eval_stack(), best effort,
             * which stops at the first entry it cannot take.  On the serial
             * path that registry belongs to the parent session, which
             * retries the exact entry once readers release.  On the
             * parallel path it belongs to a shallow per-branch session copy
             * that is freed without merging the list back, so those entries
             * are never retried; see #1765. */
            fprintf(stderr,
                "wirelog: unsafe evaluator deferred relation\n");
            return EBUSY;
        }

        rc = wl_columnar_session_defer_relation(sess, entry->rel);
        if (rc != 0) {
            /* The relation is still owned by this stack and the entry is
             * still on it, so the caller can retry or unwind.  Admission is
             * allocation-free, so a failure here is a lifecycle invariant
             * worth reporting loudly -- but not worth aborting the host
             * process over. */
            fprintf(stderr,
                "wirelog: evaluator deferred relation admission failed: %d\n",
                rc);
            return rc;
        }
        free(entry->seg_boundaries);
        memset(entry, 0, sizeof(*entry));
        s->top--;
    }
}

/* Persistent cleanup ownership. Every evaluator caller uses this boundary
 * except K-Fusion, which still drains an unframed stack; see #1648. */
typedef struct wl_columnar_eval_stack_original_payload {
    eval_stack_t stack;
    eval_entry_t result;
    wl_columnar_relation_source_group_t join_sources;
    wl_columnar_relation_private_batch_t reduce_batch;
    wl_columnar_merge_consolidate_original_record_t consolidate;
    wl_columnar_relation_bulk_append_original_t bulk_append;
} wl_columnar_eval_stack_original_payload_t;
struct wl_columnar_eval_stack_cleanup_frame {
    wl_col_session_t *session;
    wl_columnar_eval_stack_cleanup_frame_t *next;
    union {
        struct {
            union {
                eval_stack_t stack;
                wl_columnar_session_hash_controlled_registry_t registry;
            };
            eval_entry_t result;
            union {
                struct {
                    wl_columnar_relation_source_group_t join_sources;
                    wl_columnar_relation_private_batch_t reduce_batch;
                };
                wl_columnar_merge_incremental_authority_t incremental;
                wl_columnar_merge_incremental_original_region_t
                    original_incremental;
            };
            union {
                wl_columnar_consolidate_record_t consolidate;
                wl_columnar_expr_operator_preparation_t preparation;
                wl_columnar_join_batch_record_t join_batch;
                wl_columnar_join_membership_record_t membership;
                wl_columnar_lftj_record_t lftj;
                wl_columnar_merge_incremental_scratch_t incremental_scratch;
                wl_columnar_session_promotion_scratch_t promotion;
                wl_arena_compound_arena_gc_record_t compound_gc;
            };
            wl_columnar_relation_bulk_append_t bulk_append;
        };
        wl_columnar_serial_output_t output;
        wl_columnar_eval_stack_original_payload_t original_payload;
    };
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_governor_ref_t *governor;
};
typedef struct wl_columnar_eval_stack_original_frame {
    wl_col_session_t *session;
    wl_columnar_eval_stack_cleanup_frame_t *next;
    wl_columnar_eval_stack_original_payload_t payload;
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_governor_ref_t *governor;
} wl_columnar_eval_stack_original_frame_t;
_Static_assert(sizeof(wl_columnar_expr_operator_preparation_t)
    <= sizeof(wl_columnar_consolidate_record_t),
    "operator must fit original workspace");
_Static_assert(_Alignof(wl_columnar_expr_operator_preparation_t)
    <= _Alignof(wl_columnar_consolidate_record_t),
    "operator alignment must fit workspace");
_Static_assert(sizeof(wl_columnar_lftj_record_t)
    <= sizeof(wl_columnar_consolidate_record_t),
    "LFTJ must fit original workspace");
_Static_assert(sizeof(wl_arena_compound_arena_gc_record_t)
    <= sizeof(wl_columnar_consolidate_record_t),
    "compound GC must fit original workspace");
_Static_assert(_Alignof(wl_arena_compound_arena_gc_record_t)
    <= _Alignof(wl_columnar_consolidate_record_t),
    "compound GC alignment must fit original workspace");
_Static_assert(_Alignof(wl_columnar_lftj_record_t)
    <= _Alignof(wl_columnar_consolidate_record_t),
    "LFTJ alignment must fit original workspace");
_Static_assert(sizeof(wl_columnar_join_membership_record_t)
    <= sizeof(wl_columnar_consolidate_record_t),
    "membership must fit original workspace");
_Static_assert(_Alignof(wl_columnar_join_membership_record_t)
    <= _Alignof(wl_columnar_consolidate_record_t),
    "membership alignment must fit original workspace");
_Static_assert(sizeof(wl_columnar_session_hash_controlled_registry_t)
    <= sizeof(eval_stack_t), "registry must fit retired original stack");
_Static_assert(sizeof(wl_columnar_serial_output_t)
    <= sizeof(wl_columnar_eval_stack_original_payload_t),
    "output must fit retired original evaluator payload");
_Static_assert(sizeof(struct wl_columnar_eval_stack_cleanup_frame)
    == sizeof(wl_columnar_eval_stack_original_frame_t),
    "phase overlays must preserve original frame size");
_Static_assert(_Alignof(struct wl_columnar_eval_stack_cleanup_frame)
    == _Alignof(wl_columnar_eval_stack_original_frame_t),
    "phase overlays must preserve original frame alignment");

static uint32_t
wl_columnar_eval_stack_cleanup_mode(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    wl_columnar_serial_completion_t *state = &frame->session->serial_completion;
    if (state->mode == WL_COLUMNAR_SERIAL_FRAME_INLINE_OUTPUT)
        return WL_COLUMNAR_SERIAL_FRAME_INLINE_OUTPUT;
    return state->mode_frame == frame ? state->mode
        : WL_COLUMNAR_SERIAL_FRAME_EVALUATION;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_stack_test_constructor_checkpoint)(
    wl_evaluation_control_t *, uint64_t);
bool wl_columnar_eval_stack_test_fail_allocation;
bool wl_columnar_eval_stack_test_fail_commit;
#endif

int
wl_columnar_eval_stack_cleanup_begin(wl_col_session_t *sess,
    wl_columnar_eval_stack_cleanup_frame_t **out)
{
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    uint64_t bytes;
    wl_evaluation_control_t *control =
        sess ? sess->base.evaluation_control : NULL;
    if (sess && sess->serial_completion.mode ==
        WL_COLUMNAR_SERIAL_FRAME_INLINE_OUTPUT) return EBUSY;
    int admission = control ? wl_evaluation_control_charge(control, 32) : 0;
    if (admission) return admission;
    if (!sess || !out || *out)
        return EINVAL;
    if (sess->cleanup_pending)
        return EBUSY;
    if (sess->cleanup_active_count >= WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES
        || sess->cleanup_pending_count
        >= WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES
        - sess->cleanup_active_count)
        return ENOBUFS;
    if (!wl_columnar_memory_size_add(sess->cleanup_reserved_bytes,
        sizeof(*frame), &bytes))
        return EOVERFLOW;
    if (control) {
        admission = wl_evaluation_control_charge(control, 0);
        if (admission) return admission;
    }
    wl_columnar_memory_reservation_init(&reservation);
    if (sess->memory_governor) {
        wl_columnar_memory_admission_status_t status;
        int rc = sess->base.evaluation_control
            ? wl_columnar_memory_governor_reserve_checked_controlled(
            wl_columnar_memory_governor_ref_get(sess->memory_governor),
            sizeof(*frame), &reservation, &status,
            sess->base.evaluation_control) : 0;
        if (!sess->base.evaluation_control)
            status = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(sess->memory_governor),
                sizeof(*frame), &reservation);
        if (rc) {
            if (atomic_load(&reservation.state)
                == WL_COLUMNAR_MEMORY_RESERVATION_RESERVED)
                (void)wl_columnar_memory_rollback(&reservation);
            return rc;
        }
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                sess->memory_budget_denied = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
        }
    }
    admission = control ? wl_evaluation_control_charge(control, 1) : 0;
    if (admission) {
        if (sess->memory_governor) (void)wl_columnar_memory_rollback(
                &reservation);
        return admission;
    }
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_stack_test_fail_allocation) {
        wl_columnar_eval_stack_test_fail_allocation = false;
        frame = NULL;
    } else
#endif
    frame = control ? malloc(sizeof(*frame)) : calloc(1, sizeof(*frame));
    if (!frame) {
        if (sess->memory_governor)
            (void)wl_columnar_memory_rollback(&reservation);
        return ENOMEM;
    }
    if (control) {
        int rc = wl_evaluation_control_charge(control, 0);
        for (size_t offset = 0; !rc && offset < sizeof(*frame);) {
            size_t count = sizeof(*frame) - offset;
            if (count > 128) count = 128;
            rc = wl_evaluation_control_charge(control, count);
            if (rc) break;
            memset((char *)frame + offset, 0, count);
            offset += count;
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_columnar_eval_stack_test_constructor_checkpoint)
                wl_columnar_eval_stack_test_constructor_checkpoint(control,
                    offset);
#endif
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) break;
            rc = wl_evaluation_control_charge(control, 8);
            if (rc) break;
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) break;
        }
        if (rc) {
            free(frame);
            if (sess->memory_governor)
                (void)wl_columnar_memory_rollback(&reservation);
            return rc;
        }
    }
    if (control) {
        admission = wl_evaluation_control_charge(control, 32);
        if (admission) {
            free(frame);
            if (sess->memory_governor) (void)wl_columnar_memory_rollback(
                    &reservation);
            return admission;
        }
    }
    frame->reduce_batch.ready = true;
    wl_columnar_memory_reservation_init(&frame->reservation);
    if (control) {
        admission = wl_evaluation_control_charge(control, 64);
        if (admission) {
            free(frame);
            if (sess->memory_governor) (void)wl_columnar_memory_rollback(
                    &reservation);
            return admission;
        }
    }
    frame->consolidate.sequence.metadata_admission.identity =
        &frame->consolidate.sequence.metadata_admission;
    atomic_store_explicit(&frame->consolidate.sequence.metadata_admission.state,
        WL_COLUMNAR_MEMORY_RESERVATION_EMPTY, memory_order_relaxed);
    frame->consolidate.sequence.workspace.admission.identity =
        &frame->consolidate.sequence.workspace.admission;
    atomic_store_explicit(
        &frame->consolidate.sequence.workspace.admission.state,
        WL_COLUMNAR_MEMORY_RESERVATION_EMPTY, memory_order_relaxed);
    frame->consolidate.merge_admission.identity =
        &frame->consolidate.merge_admission;
    atomic_store_explicit(&frame->consolidate.merge_admission.state,
        WL_COLUMNAR_MEMORY_RESERVATION_EMPTY, memory_order_relaxed);
    frame->consolidate.ready = true;
    frame->bulk_append.ready = true;
    if (control) {
        admission = wl_evaluation_control_charge(control, 0);
        if (admission) {
            free(frame);
            if (sess->memory_governor) (void)wl_columnar_memory_rollback(
                    &reservation);
            return admission;
        }
    }
    if (sess->memory_governor) {
        int rc = control ? wl_evaluation_control_charge(control, 32) : 0;
        if (rc) {
            free(frame); (void)wl_columnar_memory_rollback(&reservation);
            return rc;
        }
        if (!wl_columnar_memory_reservation_move(&frame->reservation,
            &reservation)) {
            free(frame);
            (void)wl_columnar_memory_rollback(&reservation);
            return EINVAL;
        }
        bool accepted;
        rc = control ? wl_evaluation_control_charge(control, 0) : 0;
#ifdef WL_SESSION_TEST_HOOKS
        if (!rc && wl_columnar_eval_stack_test_fail_commit) {
            wl_columnar_eval_stack_test_fail_commit = false;
            rc = ENOMEM;
        }
#endif
        if (!rc && control)
            rc = wl_columnar_memory_governor_commit_controlled(
                &frame->reservation, frame, &accepted, control);
        else if (!rc)
            accepted = wl_columnar_memory_commit(&frame->reservation, frame);
        if (rc || !accepted) {
            if (!wl_columnar_memory_reservation_move(&reservation,
                &frame->reservation)) abort();
            free(frame);
            uint64_t state = atomic_load(&reservation.state);
            bool released = state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
                ? wl_columnar_memory_release(&reservation)
                : wl_columnar_memory_rollback(&reservation);
            if (!released) abort();
            return rc ? rc : EINVAL;
        }
    }
    if (control) {
        int rc = wl_evaluation_control_charge(control, 96);
        if (!rc) rc = wl_evaluation_control_charge(control, 0);
        if (rc) {
            if (sess->memory_governor &&
                !wl_columnar_memory_reservation_move(&reservation,
                &frame->reservation)) abort();
            wl_columnar_memory_governor_ref_t *governor = sess->memory_governor;
            free(frame);
            if (governor) {
                if (!wl_columnar_memory_release(&reservation)) abort();
            }
            return rc;
        }
    }
    /* The admitted final publication includes this fixed retain and pointer
     * installation; no fallible control call follows the final cutoff. */
    if (sess->memory_governor) {
        frame->governor = sess->memory_governor;
        wl_columnar_memory_governor_ref_retain(frame->governor);
    }
    frame->session = sess;
    frame->next = sess->cleanup_active;
    sess->cleanup_active = frame;
    sess->cleanup_active_count++;
    sess->cleanup_reserved_bytes = bytes;
    *out = frame;
    return 0;
}

eval_stack_t *
wl_columnar_eval_stack_cleanup_stack(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return frame && wl_columnar_eval_stack_cleanup_mode(frame)
           != WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY
           && wl_columnar_eval_stack_cleanup_mode(frame) !=
           WL_COLUMNAR_SERIAL_FRAME_RETIRED
           && wl_columnar_eval_stack_cleanup_mode(frame) !=
           WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC
        ? &frame->stack : NULL;
}

eval_entry_t *
wl_columnar_eval_stack_cleanup_result(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return frame && wl_columnar_eval_stack_cleanup_mode(frame) !=
           WL_COLUMNAR_SERIAL_FRAME_RETIRED
           && wl_columnar_eval_stack_cleanup_mode(frame) !=
           WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC
        ? &frame->result : NULL;
}

static wl_columnar_join_batch_record_t *
wl_columnar_eval_stack_pipeline_stage(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    wl_columnar_join_batch_record_t *batch =
        wl_columnar_eval_stack_cleanup_join_batch(frame);
    return batch && batch->phase == WL_COLUMNAR_JOIN_BATCH_PIPELINE
           && batch->pipeline.ready
           && batch->pipeline.stage == WL_COLUMNAR_JOIN_PIPELINE_STAGE_TRANSFORM
        ? batch : NULL;
}

eval_entry_t *
wl_columnar_eval_stack_cleanup_pipeline_input(wl_col_session_t *session,
    eval_stack_t *stack)
{
    if (!session || !stack || !session->cleanup_active) return NULL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = session->cleanup_active;
    wl_columnar_join_batch_record_t *batch =
        wl_columnar_eval_stack_pipeline_stage(frame);
    if (!batch || frame->session != session || &frame->stack != stack
        || batch->pipeline.stage_slot >= COL_STACK_MAX
        || stack->top != batch->pipeline.stage_slot + 1u) return NULL;
    return &stack->items[batch->pipeline.stage_slot];
}

wl_columnar_relation_private_batch_t *
wl_columnar_eval_stack_cleanup_pipeline_private_batch(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    wl_columnar_join_batch_record_t *record =
        wl_columnar_eval_stack_cleanup_join_batch(frame);
    if (!record || record->phase != WL_COLUMNAR_JOIN_BATCH_PIPELINE
        || !record->pipeline.ready || !frame->reduce_batch.ready) return NULL;
    wl_columnar_relation_private_batch_t *writer = &frame->reduce_batch;
    if (wl_columnar_relation_private_batch_disposable(writer)) return writer;
    col_rel_t *relation = writer->batch.relation;
    bool result = frame->result.owned && frame->result.rel == relation;
    bool output = record->output && record->output->owned
        && record->output->rel == relation;
    bool scratch = record->pipeline.scratch_moved
        && record->pipeline.scratch_slot < COL_STACK_MAX
        && frame->stack.items[record->pipeline.scratch_slot].owned
        && frame->stack.items[record->pipeline.scratch_slot].rel == relation;
    return result || output || scratch ? writer : NULL;
}

int
wl_columnar_eval_stack_cleanup_join_owner(wl_col_session_t *sess,
    eval_stack_t *stack, wl_columnar_relation_source_group_t **group,
    eval_entry_t **result)
{
    if (!sess || !stack || !group || !result || !sess->cleanup_active)
        return EINVAL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = sess->cleanup_active;
    uint32_t mode = wl_columnar_eval_stack_cleanup_mode(frame);
    if (mode == WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC
        || mode == WL_COLUMNAR_SERIAL_FRAME_RETIRED
        || mode == WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY
        || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL
        || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL_INITIALIZING
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION_INITIALIZING)
        return EBUSY;
    wl_columnar_join_batch_record_t *stage =
        wl_columnar_eval_stack_pipeline_stage(frame);
    if (stage) {
        if (&frame->stack != stack || frame->session != sess
            || frame->result.rel || frame->result.continuation
            || frame->result.seg_boundaries || frame->result.owned
            || frame->bulk_append.active
            || frame->bulk_append.sources.descriptor_count
            || frame->bulk_append.sources.owner_count
            || !wl_columnar_eval_stack_cleanup_pipeline_input(sess, stack))
            return EBUSY;
        *group = &frame->bulk_append.sources;
        *result = &frame->result;
        return 0;
    }
    if (&frame->stack != stack || frame->session != sess
        || frame->result.rel || frame->result.continuation
        || frame->result.seg_boundaries || frame->result.owned
        || frame->join_sources.descriptor_count ||
        frame->join_sources.owner_count)
        return EBUSY;
    *group = &frame->join_sources;
    *result = &frame->result;
    return 0;
}

int
wl_columnar_eval_stack_cleanup_private_batch_owner(wl_col_session_t *sess,
    eval_stack_t *stack, wl_columnar_relation_private_batch_t **record,
    wl_evaluation_control_t *control)
{
    if (!sess || !stack || !record || *record || !control) return EINVAL;
    int rc = wl_evaluation_control_charge(control, 96);
    if (rc) return rc;
    wl_columnar_eval_stack_cleanup_frame_t *frame = sess->cleanup_active;
    if (!frame || frame->session != sess || &frame->stack != stack)
        return EINVAL;
    uint32_t mode = wl_columnar_eval_stack_cleanup_mode(frame);
    if (mode == WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC
        || mode == WL_COLUMNAR_SERIAL_FRAME_RETIRED
        || mode == WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY
        || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL
        || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL_INITIALIZING
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION_INITIALIZING)
        return EBUSY;
    bool pipeline = wl_columnar_eval_stack_pipeline_stage(frame) != NULL;
    if (frame->result.rel || frame->result.continuation
        || frame->result.seg_boundaries || frame->result.owned
        || (pipeline ? frame->bulk_append.active
        || frame->bulk_append.sources.descriptor_count
        || frame->bulk_append.sources.owner_count
            : frame->join_sources.descriptor_count
        || frame->join_sources.owner_count)) return EBUSY;
    wl_columnar_relation_private_batch_t *candidate = &frame->reduce_batch;
    if (!candidate->ready) return EINVAL;
    if (candidate->phase == WL_COLUMNAR_RELATION_PRIVATE_INSTALLING
        || candidate->phase == WL_COLUMNAR_RELATION_PRIVATE_HELD
        || candidate->phase == WL_COLUMNAR_RELATION_PRIVATE_RETIRING
        || candidate->phase == WL_COLUMNAR_RELATION_PRIVATE_RESETTING)
        return EBUSY;
    if (!wl_columnar_relation_private_batch_disposable(
            candidate)) return EINVAL;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    *record = candidate;
    return 0;
}

wl_columnar_relation_private_batch_t *
wl_columnar_eval_stack_cleanup_reduce_batch(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    if (!frame || !frame->session) return NULL;
    uint32_t mode = wl_columnar_eval_stack_cleanup_mode(frame);
    if (mode == WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC
        || mode == WL_COLUMNAR_SERIAL_FRAME_RETIRED
        || mode == WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY
        || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL
        || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL_INITIALIZING
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION_INITIALIZING
        || !frame->reduce_batch.ready) return NULL;
    wl_columnar_relation_private_batch_t *record = &frame->reduce_batch;
    if ((record->phase == WL_COLUMNAR_RELATION_PRIVATE_INSTALLING
        || record->phase == WL_COLUMNAR_RELATION_PRIVATE_HELD
        || record->phase == WL_COLUMNAR_RELATION_PRIVATE_RETIRING)
        && (frame->result.rel != record->batch.relation ||
        !frame->result.owned))
        return NULL;
    return record;
}

wl_columnar_consolidate_record_t *
wl_columnar_eval_stack_cleanup_consolidate(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return frame && frame->session
           && wl_columnar_eval_stack_cleanup_mode(frame) ==
           WL_COLUMNAR_SERIAL_FRAME_EVALUATION
           && frame->consolidate.ready
        ? &frame->consolidate : NULL;
}

static int
wl_columnar_eval_stack_workspace_retire(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    uint32_t mode = wl_columnar_eval_stack_cleanup_mode(frame);
    if (mode == WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC)
        return frame->session->serial_completion.payload_ready
            ? wl_arena_compound_arena_gc_retire(&frame->compound_gc, 0,
                   NULL) : 0;
    if (mode == WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC
        || mode == WL_COLUMNAR_SERIAL_FRAME_RETIRED
        || mode == WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY) return EBUSY;
    if (mode == WL_COLUMNAR_SERIAL_FRAME_WORKSPACE_INITIALIZING
        || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL_INITIALIZING
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION_INITIALIZING) return 0;
    if (mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION)
        return frame->session->serial_completion.payload_ready
            ? wl_columnar_session_promotion_retire(frame->session,
                   &frame->incremental,
                   &frame->promotion, &frame->bulk_append, &frame->result,
                   0) : 0;
    if (mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL)
        return frame->session->serial_completion.payload_ready
            ? wl_columnar_merge_incremental_retire(&frame->incremental,
                   &frame->incremental_scratch, 0) : 0;
    if (mode == WL_COLUMNAR_SERIAL_FRAME_LFTJ)
        return frame->session->serial_completion.payload_ready
               && frame->lftj.ready
            ? wl_columnar_lftj_record_retire(&frame->lftj, 0, true) : 0;
    if (mode == WL_COLUMNAR_SERIAL_FRAME_MEMBERSHIP)
        return frame->session->serial_completion.payload_ready
               && frame->membership.ready
            ? wl_columnar_join_membership_retire(&frame->membership,
                   0, true, NULL) : 0;
    if (mode == WL_COLUMNAR_SERIAL_FRAME_BATCH)
        return frame->session->serial_completion.payload_ready &&
               frame->join_batch.ready
            ? wl_columnar_join_batch_record_retire(&frame->join_batch, 0) : 0;
    if (mode == WL_COLUMNAR_SERIAL_FRAME_OPERATOR)
        return frame->session->serial_completion.payload_ready
               && frame->preparation.ready ? wl_columnar_expr_operator_retire(
            &frame->preparation, 0, true, NULL) : 0;
    return wl_columnar_relation_consolidate_retire(&frame->consolidate, 0);
}
static int
wl_columnar_eval_stack_workspace_clear(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_evaluation_control_t *control)
{
    uint32_t mode = wl_columnar_eval_stack_cleanup_mode(frame);
    /* These two regions also replaced the old source/private authority.
     * They may leave the frame only through complete checked disposal. */
    if (mode == WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC
        || mode == WL_COLUMNAR_SERIAL_FRAME_RETIRED
        || mode == WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY
        || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL
        || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL_INITIALIZING
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION_INITIALIZING)
        return EBUSY;
    int rc = wl_columnar_eval_stack_workspace_retire(frame);
    if (rc) return rc;
    wl_columnar_serial_completion_t *state = &frame->session->serial_completion;
    if (state->mode == WL_COLUMNAR_SERIAL_FRAME_INLINE_OUTPUT) return EBUSY;
    if (state->mode_frame && state->mode_frame != frame) return EBUSY;
    state->mode_frame = frame;
    state->mode = WL_COLUMNAR_SERIAL_FRAME_WORKSPACE_INITIALIZING;
    state->payload_ready = false;
    for (size_t at = 0; at < sizeof(frame->consolidate);) {
        size_t n = sizeof(frame->consolidate) - at;
        if (n > 128) n = 128;
        rc = wl_evaluation_control_charge(control, n);
        if (rc) return rc;
        memset((char *)&frame->consolidate + at, 0, n); at += n;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    return 0;
}
int
wl_columnar_eval_stack_cleanup_compound_gc(wl_col_session_t *session,
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_arena_compound_arena_gc_record_t **out, wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control, 64); if (rc) return rc;
    if (!session || !frame || !out || *out || !control
        || session->cleanup_active != frame || frame->session != session
        || wl_columnar_eval_stack_cleanup_mode(frame) !=
        WL_COLUMNAR_SERIAL_FRAME_EVALUATION
        || frame->stack.top || frame->result.rel || frame->result.owned
        || frame->result.continuation || frame->bulk_append.active
        || frame->join_sources.descriptor_count ||
        frame->join_sources.owner_count
        || !wl_columnar_relation_private_batch_disposable(&frame->reduce_batch))
        return EBUSY;
    for (uint32_t i = 0; i < 2; i++) {
        const wl_columnar_relation_source_group_descriptor_t *desc =
            &frame->join_sources.descriptors[i];
        const wl_columnar_relation_source_group_owner_t *owner =
            &frame->join_sources.owners[i];
        if (desc->reader_held || desc->writer_held || desc->journal ||
            owner->held
            || desc->reader.owner || desc->reader.secondary_owner
            || desc->writer.owner || desc->writer.secondary_owner
            || owner->reader.owner ||
            owner->reader.secondary_owner) return EBUSY;
    }
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    rc = wl_columnar_eval_stack_workspace_retire(frame); if (rc) return rc;
    wl_columnar_serial_completion_t *state = &session->serial_completion;
    if (state->mode_frame && state->mode_frame != frame) return EBUSY;
    state->mode_frame = frame;
    state->mode = WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC;
    state->payload_ready = false;
    for (size_t at = 0; at < sizeof(frame->compound_gc);) {
        size_t n = sizeof(frame->compound_gc) - at; if (n > 128) n = 128;
        rc = wl_evaluation_control_charge(control, n); if (rc) return rc;
        memset((char *)&frame->compound_gc + at, 0, n); at += n;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    frame->compound_gc.phase = 1; frame->compound_gc.ready = true;
    state->payload_ready = true; *out = &frame->compound_gc;
    return wl_evaluation_control_charge(control, 0);
}

#ifdef WL_SESSION_TEST_HOOKS
const wl_columnar_eval_serial_aggregate_scratch_t *
wl_columnar_eval_stack_test_aggregate_scratch(wl_col_session_t *session)
{
    if (!session || !session->cleanup_active) return NULL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = session->cleanup_active;
    if (frame != session->serial_completion.mode_frame
        || session->serial_completion.mode !=
        WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL
        || !session->serial_completion.payload_ready) return NULL;
    if (frame->session != session || !frame->incremental_scratch.ready
        || frame->incremental_scratch.lane != 5) return NULL;
    if (!frame->incremental_scratch.aggregate.ready) return NULL;
    return &frame->incremental_scratch.aggregate;
}
#endif

int
wl_columnar_eval_stack_cleanup_incremental_owner(wl_col_session_t *session,
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_merge_incremental_authority_t **out_authority,
    wl_columnar_merge_incremental_scratch_t **out_scratch,
    wl_evaluation_control_t *control)
{
    if (!session || !frame || frame->session != session || !control
        || !out_authority || *out_authority || !out_scratch || *out_scratch)
        return EINVAL;
    uint32_t mode = wl_columnar_eval_stack_cleanup_mode(frame);
    if (mode == WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC
        || mode == WL_COLUMNAR_SERIAL_FRAME_RETIRED
        || mode == WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY
        || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL
        || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL_INITIALIZING
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION_INITIALIZING)
        return EBUSY;
    int rc = wl_columnar_relation_bulk_append_retire(&frame->bulk_append, 0);
    if (rc || frame->bulk_append.active) return rc ? rc : EBUSY;
    rc = wl_columnar_relation_private_batch_retire(&frame->reduce_batch, 0);
    if (rc) return rc;
    if (!wl_columnar_relation_private_batch_disposable(&frame->reduce_batch))
        return EBUSY;
    rc = wl_columnar_eval_stack_workspace_retire(frame);
    if (rc) return rc;
    rc = wl_columnar_relation_source_group_drain(&frame->join_sources, 0);
    if (rc) return rc;
    wl_columnar_serial_completion_t *state = &session->serial_completion;
    if (state->mode_frame && state->mode_frame != frame) return EBUSY;
    state->mode_frame = frame;
    state->mode = WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL_INITIALIZING;
    state->payload_ready = false;
    /* Both old owners have really retired. No authority exists in either
     * partially cleared region until the final readiness transition. */
    void *regions[2] = {&frame->incremental, &frame->incremental_scratch};
    size_t extents[2] = {sizeof(frame->incremental),
                         sizeof(frame->incremental_scratch)};
    for (unsigned i = 0; i < 2; i++)
        for (size_t at = 0; at < extents[i];) {
            size_t n = extents[i] - at;
            if (n > 128) n = 128;
            rc = wl_evaluation_control_charge(control, n);
            if (rc) return rc;
            memset((char *)regions[i] + at, 0, n);
            at += n;
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) return rc;
        }
    rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    frame->incremental.ready = true;
    frame->incremental_scratch.ready = true;
    state->payload_ready = true;
    state->mode = WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL;
    *out_authority = &frame->incremental;
    *out_scratch = &frame->incremental_scratch;
    return wl_evaluation_control_charge(control, 0);
}

int
wl_columnar_eval_stack_cleanup_promotion_owner(wl_col_session_t *session,
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_merge_incremental_authority_t **out_authority,
    wl_columnar_session_promotion_scratch_t **out_scratch,
    wl_evaluation_control_t *control)
{
    if (!session || !frame || frame->session != session || !control
        || !out_authority || *out_authority || !out_scratch || *out_scratch)
        return EINVAL;
    uint32_t mode = wl_columnar_eval_stack_cleanup_mode(frame);
    if (mode == WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC
        || mode == WL_COLUMNAR_SERIAL_FRAME_RETIRED
        || mode == WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY
        || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL
        || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL_INITIALIZING
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION_INITIALIZING)
        return EBUSY;
    int rc = wl_columnar_relation_bulk_append_retire(&frame->bulk_append, 0);
    if (rc || frame->bulk_append.active) return rc ? rc : EBUSY;
    rc = wl_columnar_relation_private_batch_retire(&frame->reduce_batch, 0);
    if (rc) return rc;
    if (!wl_columnar_relation_private_batch_disposable(&frame->reduce_batch))
        return EBUSY;
    rc = wl_columnar_eval_stack_workspace_retire(frame);
    if (rc) return rc;
    rc = wl_columnar_relation_source_group_drain(&frame->join_sources, 0);
    if (rc) return rc;
    wl_columnar_serial_completion_t *state = &session->serial_completion;
    if (state->mode_frame && state->mode_frame != frame) return EBUSY;
    state->mode_frame = frame;
    state->mode = WL_COLUMNAR_SERIAL_FRAME_PROMOTION_INITIALIZING;
    state->payload_ready = false;
    /* Both old owners have really retired. No authority exists in either
     * partially cleared region until the final readiness transition. */
    void *regions[2] = {&frame->incremental, &frame->promotion};
    size_t extents[2] = {sizeof(frame->incremental),
                         sizeof(frame->promotion)};
    for (unsigned i = 0; i < 2; i++)
        for (size_t at = 0; at < extents[i];) {
            size_t n = extents[i] - at;
            if (n > 128) n = 128;
            rc = wl_evaluation_control_charge(control, n);
            if (rc) return rc;
            memset((char *)regions[i] + at, 0, n);
            at += n;
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) return rc;
        }
    rc = wl_evaluation_control_charge(control, 16);
    if (rc) return rc;
    frame->incremental.ready = true;
    frame->promotion.ready = true;
    state->payload_ready = true;
    state->mode = WL_COLUMNAR_SERIAL_FRAME_PROMOTION;
    *out_authority = &frame->incremental;
    *out_scratch = &frame->promotion;
    return wl_evaluation_control_charge(control, 0);
}

static bool
wl_columnar_eval_stack_cleanup_credit_terminal(
    const wl_columnar_memory_reservation_t *token, bool allow_uninitialized)
{
    uint64_t state = atomic_load_explicit(&token->state, memory_order_acquire);
    if (allow_uninitialized && !token->identity && !token->governor
        && !token->bytes && !token->replacement_bytes
        && state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY) return true;
    return token->identity == token
           && (state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
           || state == WL_COLUMNAR_MEMORY_RESERVATION_RELEASED);
}

int
wl_columnar_eval_stack_cleanup_promotion_rollback_owner(
    wl_col_session_t *session,
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_merge_incremental_authority_t **out_authority,
    wl_columnar_session_promotion_scratch_t **out_scratch)
{
    if (!session || !frame || frame->session != session
        || !out_authority || *out_authority || !out_scratch || *out_scratch)
        return EINVAL;
    wl_columnar_serial_completion_t *completion = &session->serial_completion;
    wl_columnar_eval_serial_recursive_record_t *journal = &session->recursive;
    if (completion->mode_frame != frame
        || completion->mode != WL_COLUMNAR_SERIAL_FRAME_REGISTRY
        || !journal->active || !journal->ready || !journal->delta_rels
        || journal->phase != WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REGISTRY_FAILED
        || journal->relation >= journal->relation_count
        || !journal->delta_rels[journal->relation] || frame->result.owned
        || (frame->result.rel &&
        frame->result.rel != journal->delta_rels[journal->relation]))
        return EINVAL;
    delta_pool_t *pool = session->delta_pool;
    uint64_t extent; uint32_t slots;
    if (wl_columnar_session_recursive_pool_extent(session, &extent, &slots)
        || journal->cursor >= slots)
        return EINVAL;
    col_rel_t *source = (col_rel_t *)(pool->slab + (size_t)journal->cursor *
        pool->slot_size);
    if (completion->payload_ready) {
        wl_columnar_session_hash_controlled_registry_t *registry =
            &frame->registry;
        if (registry->published ||
            registry->initialized_tokens > 5) return EINVAL;
        if (registry->ready) {
            int cleanup = wl_columnar_session_hash_registry_retire(registry, 0);
            if (cleanup) return cleanup;
            wl_columnar_memory_reservation_t *tokens[5] = {
                &registry->image.rels_reservation,
                &registry->image.hash_reservation,
                &registry->image.expected_rels_reservation,
                &registry->old_rels, &registry->old_hash
            };
            for (uint32_t i = 0; i < registry->initialized_tokens; i++)
                if (!wl_columnar_eval_stack_cleanup_credit_terminal(tokens[i],
                    false))
                    return EINVAL;
        } else if (registry->initialized_tokens) return EINVAL;
        if (registry->image.rels || registry->image.hash_head
            || registry->image.hash_next ||
            registry->image.expected_rel_contents
            || registry->retired_rels || registry->retired_head ||
            registry->retired_next)
            return EBUSY;
    }
    /* payload_ready=false is interrupted clear, before registry API entry;
     * its partially overwritten union bytes are never inspected. */
    wl_columnar_relation_bulk_append_t *bulk = &frame->bulk_append;
    if (!bulk->ready || bulk->active || bulk->borrowed_mode) return EBUSY;
    wl_columnar_memory_reservation_t *bulk_tokens[5] = {&bulk->scratch,
                                                        &bulk->metadata_pending,
                                                        &bulk->old_metadata,
                                                        &bulk->previous,
                                                        &bulk->pending.fresh};
    for (unsigned i = 0; i < 5; i++)
        if (!wl_columnar_eval_stack_cleanup_credit_terminal(bulk_tokens[i],
            true))
            return EINVAL;
    if (bulk->lengths || bulk->prepared_types || bulk->prepared_columns
        || bulk->prepared_timestamps || bulk->old_columns ||
        bulk->old_timestamps
        || bulk->prepared_schema_ok || bulk->old_schema_ok ||
        bulk->pending.growth
        || bulk->transaction_active || bulk->writer.owner ||
        bulk->descriptor_writer.owner)
        return EBUSY;
    completion->mode = WL_COLUMNAR_SERIAL_FRAME_PROMOTION_INITIALIZING;
    completion->payload_ready = false;
    /* All overwritten owners are proven terminal. This fixed infrastructure
     * clear is mandatory stopped-attempt cleanup, without fresh admission. */
    memset(&frame->incremental, 0, sizeof(frame->incremental));
    memset(&frame->promotion, 0, sizeof(frame->promotion));
    memset(&frame->stack, 0, sizeof(frame->stack));
    memset(bulk, 0, sizeof(*bulk));
    wl_columnar_memory_reservation_init(&bulk->scratch);
    bulk->ready = true; frame->incremental.ready = true;
    frame->promotion.ready = true;
    frame->promotion.source = source;
    frame->promotion.reversing = true;
    /* The journal phase denotes a real completed physical tombstone. Its
     * candidate remains the single delta-slot owner; these dispositions
     * prevent generic descriptor disposal before original gate acquisition. */
    frame->result.rel = journal->delta_rels[journal->relation];
    frame->result.owned = false;
    frame->promotion.phase = 8;
    frame->promotion.payload_owned = true;
    frame->promotion.tombstone = true;
    completion->payload_ready = true;
    completion->mode = WL_COLUMNAR_SERIAL_FRAME_PROMOTION;
    *out_authority = &frame->incremental; *out_scratch = &frame->promotion;
    return 0;
}

int
wl_columnar_eval_stack_cleanup_join_batch_owner(wl_col_session_t *session,
    eval_stack_t *stack, wl_columnar_join_batch_record_t **out,
    wl_evaluation_control_t *control)
{
    if (!session || !stack || !out || *out || !control) return EINVAL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = session->cleanup_active;
    if (!frame || frame->session != session || &frame->stack != stack)
        return EINVAL;
    int rc = wl_columnar_eval_stack_workspace_clear(frame, control);
    if (rc) return rc;
    wl_columnar_memory_reservation_t *tokens[] = {&frame->join_batch.pending,
                                                  &frame->join_batch.retired,
                                                  &frame->join_batch.descriptor};
    for (uint32_t i = 0; i < 3u; i++) {
        rc = wl_evaluation_control_charge(control, 96); if (rc) return rc;
        wl_columnar_memory_reservation_init(tokens[i]);
        frame->join_batch.initialized_tokens++;
        rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(control, 24); if (rc) return rc;
    frame->join_batch.session = session; frame->join_batch.control = control;
    frame->join_batch.ready = true;
    session->serial_completion.payload_ready = true;
    session->serial_completion.mode = WL_COLUMNAR_SERIAL_FRAME_BATCH;
    *out = &frame->join_batch;
    return wl_evaluation_control_charge(control, 0);
}
wl_columnar_join_batch_record_t *
wl_columnar_eval_stack_cleanup_join_batch(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return frame && frame->session
           && wl_columnar_eval_stack_cleanup_mode(frame) ==
           WL_COLUMNAR_SERIAL_FRAME_BATCH
           && frame->session->serial_completion.payload_ready &&
           frame->join_batch.ready
        ? &frame->join_batch : NULL;
}

int
wl_columnar_eval_stack_cleanup_lftj_owner(wl_col_session_t *session,
    eval_stack_t *stack, wl_columnar_lftj_record_t **out,
    wl_evaluation_control_t *control)
{
    if (!session || !stack || !out || *out || !control) return EINVAL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = session->cleanup_active;
    if (!frame || frame->session != session || &frame->stack != stack)
        return EINVAL;
    int rc = wl_columnar_eval_stack_workspace_clear(frame, control);
    if (rc) return rc;
    wl_columnar_memory_reservation_t *tokens[] = {
        &frame->lftj.outer, &frame->lftj.inner
    };
    for (uint32_t i = 0; i < 2u; i++) {
        rc = wl_evaluation_control_charge(control, 96);
        if (rc) return rc;
        wl_columnar_memory_reservation_init(tokens[i]);
        frame->lftj.initialized_tokens++;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(control, 24);
    if (rc) return rc;
    frame->lftj.session = session;
    frame->lftj.control = control;
    frame->lftj.ready = true;
    session->serial_completion.payload_ready = true;
    session->serial_completion.mode = WL_COLUMNAR_SERIAL_FRAME_LFTJ;
    *out = &frame->lftj;
    return wl_evaluation_control_charge(control, 0);
}

wl_columnar_lftj_record_t *
wl_columnar_eval_stack_cleanup_lftj(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return frame && frame->session
           && wl_columnar_eval_stack_cleanup_mode(frame)
           == WL_COLUMNAR_SERIAL_FRAME_LFTJ
           && frame->session->serial_completion.payload_ready
           && frame->lftj.ready ? &frame->lftj : NULL;
}

int
wl_columnar_eval_stack_cleanup_membership_owner(wl_col_session_t *session,
    eval_stack_t *stack, wl_columnar_join_membership_record_t **out,
    wl_evaluation_control_t *control)
{
    if (!session || !stack || !out || *out || !control) return EINVAL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = session->cleanup_active;
    if (!frame || frame->session != session || &frame->stack != stack)
        return EINVAL;
    int rc = wl_columnar_eval_stack_workspace_clear(frame, control);
    if (rc) return rc;
    wl_columnar_memory_reservation_t *tokens[] = {
        &frame->membership.keys, &frame->membership.hash,
        &frame->membership.row
    };
    for (uint32_t i = 0; i < 3u; i++) {
        rc = wl_evaluation_control_charge(control, 96);
        if (rc) return rc;
        wl_columnar_memory_reservation_init(tokens[i]);
        frame->membership.initialized_tokens++;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(control, 24);
    if (rc) return rc;
    frame->membership.session = session;
    frame->membership.control = control;
    frame->membership.ready = true;
    session->serial_completion.payload_ready = true;
    session->serial_completion.mode = WL_COLUMNAR_SERIAL_FRAME_MEMBERSHIP;
    *out = &frame->membership;
    return wl_evaluation_control_charge(control, 0);
}

wl_columnar_join_membership_record_t *
wl_columnar_eval_stack_cleanup_membership(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return frame && frame->session
           && wl_columnar_eval_stack_cleanup_mode(frame)
           == WL_COLUMNAR_SERIAL_FRAME_MEMBERSHIP
           && frame->session->serial_completion.payload_ready
           && frame->membership.ready ? &frame->membership : NULL;
}

wl_columnar_eval_stack_cleanup_frame_t *
wl_columnar_eval_stack_cleanup_join_batch_frame(
    wl_columnar_join_batch_record_t *record)
{
    if (!record || !record->ready || !record->session) return NULL;
    wl_columnar_eval_stack_cleanup_frame_t *frame =
        (wl_columnar_eval_stack_cleanup_frame_t *)((char *)record
        - offsetof(wl_columnar_eval_stack_cleanup_frame_t, join_batch));
    return frame->session == record->session
           && wl_columnar_eval_stack_cleanup_join_batch(frame) == record
        ? frame : NULL;
}

int
wl_columnar_eval_stack_cleanup_operator_preparation_owner(
    wl_col_session_t *sess,
    eval_stack_t *stack, wl_columnar_expr_operator_preparation_t **out,
    wl_evaluation_control_t *control)
{
    if (!sess || !stack || !out || *out || !control) return EINVAL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = sess->cleanup_active;
    if (!frame || frame->session != sess ||
        &frame->stack != stack) return EINVAL;
    wl_columnar_join_batch_record_t *stage =
        wl_columnar_eval_stack_pipeline_stage(frame);
    if (stage) {
        wl_columnar_expr_operator_preparation_t *preparation =
            &stage->pipeline.preparation;
        int rc = preparation->ready
            ? wl_columnar_expr_operator_retire(preparation, 0, false, control)
            : 0;
        if (rc) return rc;
        preparation->ready = false;
        for (size_t at = 0; at < sizeof(*preparation);) {
            size_t count = sizeof(*preparation) - at;
            if (count > 128u) count = 128u;
            rc = wl_evaluation_control_charge(control, count);
            if (rc) return rc;
            memset((char *)preparation + at, 0, count); at += count;
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) return rc;
        }
        wl_columnar_memory_reservation_t *tokens[] = {
            &preparation->scratch, &preparation->compile_admission,
            &preparation->retired_expression
        };
        for (uint32_t i = 0; i < 3u; i++) {
            rc = wl_evaluation_control_charge(control, 96);
            if (rc) return rc;
            wl_columnar_memory_reservation_init(tokens[i]);
            preparation->initialized_tokens++;
            rc = wl_evaluation_control_charge(control, 0);
            if (rc) return rc;
        }
        preparation->ready = true;
        *out = preparation;
        return 0;
    }
    int rc = wl_columnar_eval_stack_workspace_clear(frame, control);
    if (rc) return rc;
    wl_columnar_memory_reservation_t *tokens[] = {&frame->preparation.scratch,
                                                  &frame->preparation.
                                                  compile_admission,
                                                  &frame->preparation.
                                                  retired_expression};
    for (uint32_t i = 0; i < 3; i++) {
        rc = wl_evaluation_control_charge(control, 96);
        if (rc) return rc;
        wl_columnar_memory_reservation_init(tokens[i]);
        frame->preparation.initialized_tokens++;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    frame->preparation.ready = true;
    sess->serial_completion.payload_ready = true;
    sess->serial_completion.mode = WL_COLUMNAR_SERIAL_FRAME_OPERATOR;
    *out = &frame->preparation;
    return 0;
}
wl_columnar_expr_operator_preparation_t *
wl_columnar_eval_stack_cleanup_operator_preparation(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    wl_columnar_join_batch_record_t *stage =
        wl_columnar_eval_stack_pipeline_stage(frame);
    if (stage && frame->session->cleanup_active == frame
        && stage->pipeline.preparation.ready)
        return &stage->pipeline.preparation;
    return frame && frame->session && frame->session->cleanup_active == frame
           && wl_columnar_eval_stack_cleanup_mode(frame) ==
           WL_COLUMNAR_SERIAL_FRAME_OPERATOR
           && frame->session->serial_completion.payload_ready &&
           frame->preparation.ready
        ? &frame->preparation : NULL;
}

int
wl_columnar_eval_stack_cleanup_consolidate_prepare(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_evaluation_control_t *control)
{
    if (!frame || !control) return EINVAL;
    if (wl_columnar_eval_stack_cleanup_mode(frame) ==
        WL_COLUMNAR_SERIAL_FRAME_EVALUATION)
        return 0;
    int rc = wl_columnar_eval_stack_workspace_clear(frame, control);
    if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 192);
    if (rc) return rc;
    wl_columnar_memory_reservation_init(
        &frame->consolidate.sequence.metadata_admission);
    wl_columnar_memory_reservation_init(
        &frame->consolidate.sequence.workspace.admission);
    wl_columnar_memory_reservation_init(&frame->consolidate.merge_admission);
    frame->consolidate.ready = true;
    frame->session->serial_completion.payload_ready = true;
    frame->session->serial_completion.mode =
        WL_COLUMNAR_SERIAL_FRAME_EVALUATION;
    return wl_evaluation_control_charge(control, 0);
}

wl_columnar_relation_bulk_append_t *
wl_columnar_eval_stack_cleanup_bulk_append(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return frame && frame->session
           && wl_columnar_eval_stack_cleanup_mode(frame) !=
           WL_COLUMNAR_SERIAL_FRAME_RETIRED
           && wl_columnar_eval_stack_cleanup_mode(frame) !=
           WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC
           && frame->bulk_append.ready
        ? &frame->bulk_append : NULL;
}

wl_columnar_relation_source_group_t *
wl_columnar_eval_stack_cleanup_sources(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    if (!frame || !frame->session) return NULL;
    uint32_t mode = wl_columnar_eval_stack_cleanup_mode(frame);
    return mode == WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC
           || mode == WL_COLUMNAR_SERIAL_FRAME_RETIRED
           || mode == WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY
           || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL
           || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL_INITIALIZING
           || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION
           || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION_INITIALIZING
        ? NULL : &frame->join_sources;
}

static void
wl_columnar_eval_stack_cleanup_continuations(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    uint32_t mode = wl_columnar_eval_stack_cleanup_mode(frame);
    if (mode == WL_COLUMNAR_SERIAL_FRAME_OUTPUT
        || mode == WL_COLUMNAR_SERIAL_FRAME_RETIRED
        || mode == WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC) return;
    if (frame->result.kind == WL_COLUMNAR_EVAL_ENTRY_CONTINUATION)
        (void)eval_entry_dispose(&frame->result);
    if (mode == WL_COLUMNAR_SERIAL_FRAME_REGISTRY
        || mode == WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY) return;
    for (uint32_t i = 0; i < frame->stack.top; i++)
        if (frame->stack.items[i].kind == WL_COLUMNAR_EVAL_ENTRY_CONTINUATION)
            (void)eval_entry_dispose(&frame->stack.items[i]);
}

static int
wl_columnar_eval_stack_cleanup_dispose_payload(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    uint32_t mode = wl_columnar_eval_stack_cleanup_mode(frame);
    if (mode == WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC)
        return wl_columnar_eval_stack_workspace_retire(frame);
    if (mode == WL_COLUMNAR_SERIAL_FRAME_INLINE_OUTPUT) return EBUSY;
    if (mode == WL_COLUMNAR_SERIAL_FRAME_OUTPUT) {
        if (!frame->session->serial_completion.payload_ready) return 0;
        return wl_columnar_session_serial_output_drain(frame->session,
                   &frame->output.journal);
    }
    if (mode == WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY) {
        wl_columnar_eval_stack_cleanup_descriptor_published(frame);
        int rc = frame->session->serial_completion.payload_ready
            ? wl_columnar_session_hash_registry_retire(&frame->registry, 0) : 0;
        if (rc) return rc;
        if (frame->promotion.phase ==
            WL_COLUMNAR_SESSION_PROMOTION_CANONICAL_DESCRIPTOR)
            return wl_columnar_relation_descriptor_retire(&frame->bulk_append,
                       &frame->promotion, &frame->result, 0);
        return frame->promotion.phase || frame->result.rel ? EINVAL : 0;
    }
    if (mode == WL_COLUMNAR_SERIAL_FRAME_REGISTRY
        && frame->session->recursive.active
        && frame->session->recursive.phase ==
        WL_COLUMNAR_EVAL_SERIAL_RECURSIVE_REGISTRY_FAILED
        && frame->session->serial_completion.mode_frame == frame) {
        wl_columnar_merge_incremental_authority_t *authority = NULL;
        wl_columnar_session_promotion_scratch_t *scratch = NULL;
        int rc = wl_columnar_eval_stack_cleanup_promotion_rollback_owner(
            frame->session, frame, &authority, &scratch);
        if (rc) return rc;
        mode = wl_columnar_eval_stack_cleanup_mode(frame);
    }
    if (mode == WL_COLUMNAR_SERIAL_FRAME_REGISTRY) {
        int rc = frame->session->serial_completion.payload_ready
            ? wl_columnar_session_hash_registry_retire(&frame->registry, 0) : 0;
        if (rc) return rc;
        return eval_entry_dispose(&frame->result);
    }
    if (mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION_INITIALIZING) {
        int retired = wl_columnar_eval_stack_workspace_retire(frame);
        return retired ? retired : eval_stack_drain_to_session(&frame->stack,
                   frame->session);
    }
    int result = wl_columnar_relation_bulk_append_retire(&frame->bulk_append,
            0);
    if (result || frame->bulk_append.active) return result ? result : EINVAL;
    if (mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL
        || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL_INITIALIZING
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION_INITIALIZING) {
        result = wl_columnar_eval_stack_workspace_retire(frame);
        if (result) return result;
    } else {
        result = wl_columnar_relation_private_batch_retire(&frame->reduce_batch,
                0);
        if (result != 0) return result;
        if (!wl_columnar_relation_private_batch_disposable(
                &frame->reduce_batch))
            return EINVAL;
        result = wl_columnar_eval_stack_workspace_retire(frame);
        if (result) return result;
        result = wl_columnar_relation_source_group_drain(&frame->join_sources,
                0);
        if (result != 0)
            return result;
    }
    result = eval_entry_dispose(&frame->result);
    /* Visit lower entries even if a higher one refuses. Their destruction
     * can release dependencies needed by other pending frames. */
    for (uint32_t i = frame->stack.top; i > 0; i--) {
        int rc = eval_entry_dispose(&frame->stack.items[i - 1]);
        if (result == 0)
            result = rc;
    }
    if (result == 0)
        frame->stack.top = 0;
    return result;
}

/* Only real checked payload retirement establishes the external idle
 * witness. The original physical frame, reservation and governor ref survive. */
static int
wl_columnar_eval_stack_cleanup_dispose(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    if (wl_columnar_eval_stack_cleanup_mode(frame) ==
        WL_COLUMNAR_SERIAL_FRAME_RETIRED) return 0;
    int rc = wl_columnar_eval_stack_cleanup_dispose_payload(frame);
    if (!rc && frame->session->serial_completion.frame == frame) {
        wl_columnar_serial_completion_t *state =
            &frame->session->serial_completion;
        if (state->mode_frame && state->mode_frame != frame) return EBUSY;
        state->mode_frame = frame;
        state->mode = WL_COLUMNAR_SERIAL_FRAME_RETIRED;
        state->payload_ready = false;
    }
    return rc;
}

int
wl_columnar_eval_stack_cleanup_idle(wl_col_session_t *sess,
    wl_evaluation_control_t *control)
{
    if (!sess) return EINVAL;
    if (sess->cleanup_active || sess->cleanup_pending
        || sess->cleanup_active_count ||
        sess->cleanup_pending_count) return EBUSY;
    wl_columnar_serial_completion_t *state = &sess->serial_completion;
    wl_columnar_eval_stack_cleanup_frame_t *frame = state->frame;
    if (!frame) return 0;
    int rc = control ? wl_evaluation_control_charge(control, 32) : 0;
    if (!rc && control) rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    /* The serialized session retains this actual frame allocation. Do not
     * interpret its retired union to establish that no payload owner exists. */
    if (state->mode_frame != frame ||
        state->mode != WL_COLUMNAR_SERIAL_FRAME_RETIRED
        || state->payload_ready || frame->session != sess
        || sess->cleanup_reserved_bytes != sizeof(*frame)
        || frame->governor != sess->memory_governor) return EINVAL;
    wl_columnar_memory_reservation_t *token = &frame->reservation;
    if (token->identity != token || token->replacement_bytes
        || token->transition_kind) return EINVAL;
    uint64_t token_state = atomic_load_explicit(&token->state,
            memory_order_acquire);
    uint64_t owner = atomic_load_explicit(&token->owner_bits,
            memory_order_acquire);
    if (frame->governor)
        return token_state == WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED
               && token->governor ==
               wl_columnar_memory_governor_ref_get(frame->governor)
               && token->bytes == sizeof(*frame) &&
               owner == (uintptr_t)frame ? 0 : EINVAL;
    return token_state == WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
           && !token->governor && !token->bytes && !owner ? 0 : EINVAL;
}

int
wl_columnar_eval_stack_cleanup_acquire(wl_col_session_t *sess,
    wl_columnar_eval_stack_cleanup_frame_t **out,
    wl_evaluation_control_t *control)
{
    if (!sess || !out || *out) return EINVAL;
    if (sess->serial_completion.frame) {
        if (!sess->serial_completion.pending || !control) return EBUSY;
        int rc = wl_columnar_eval_stack_cleanup_idle(sess, control);
        return rc ? rc : wl_columnar_eval_stack_cleanup_resume(sess, out,
                   control);
    }
    if (sess->cleanup_active || sess->cleanup_pending) return EBUSY;
    int rc = wl_columnar_eval_stack_cleanup_begin(sess, out);
    if (!rc &&
        sess->serial_completion.pending) sess->serial_completion.frame = *out;
    return rc;
}

static int
wl_columnar_eval_stack_cleanup_free(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    /* Retaining the already retired last frame creates no second overlap.
     * Its actual physical admission remains charged until final retirement. */
    if (frame->session->serial_completion.mode ==
        WL_COLUMNAR_SERIAL_FRAME_INLINE_OUTPUT) return EBUSY;
    if (frame->session->serial_completion.frame == frame) return 0;
    if (frame->session->serial_completion.mode_frame == frame) {
        frame->session->serial_completion.mode_frame = NULL;
        frame->session->serial_completion.mode =
            WL_COLUMNAR_SERIAL_FRAME_EVALUATION;
        frame->session->serial_completion.payload_ready = false;
    }
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_governor_ref_t *governor = frame->governor;
    wl_columnar_memory_reservation_init(&reservation);
    if (governor && !wl_columnar_memory_reservation_move(&reservation,
        &frame->reservation))
        return EINVAL;
    frame->session->cleanup_reserved_bytes -= sizeof(*frame);
    free(frame);
    if (governor) {
        bool released = wl_columnar_memory_release(&reservation);
        assert(released);
        (void)released;
        wl_columnar_memory_governor_ref_release(governor);
    }
    return 0;
}

int
wl_columnar_eval_stack_cleanup_finish(
    wl_columnar_eval_stack_cleanup_frame_t **handle)
{
    if (!handle || !*handle)
        return EINVAL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = *handle;
    wl_col_session_t *sess = frame->session;
    if (sess->cleanup_active != frame)
        return EBUSY;
    sess->cleanup_active = frame->next;
    sess->cleanup_active_count--;
    *handle = NULL;
    wl_columnar_eval_stack_cleanup_continuations(frame);
    int rc = wl_columnar_eval_stack_cleanup_dispose(frame);
    if (rc == 0)
        rc = wl_columnar_eval_stack_cleanup_free(frame);
    if (rc != 0) {
        frame->next = sess->cleanup_pending;
        sess->cleanup_pending = frame;
        sess->cleanup_pending_count++;
    }
    return rc;
}

int
wl_columnar_eval_stack_cleanup_retry(wl_col_session_t *sess)
{
    if (!sess)
        return EINVAL;
    if (sess->cleanup_active)
        return EBUSY;
    /* Release readers held by continuations in every frame before trying
     * any relation, including dependencies between pending frames. */
    for (wl_columnar_eval_stack_cleanup_frame_t *frame = sess->cleanup_pending;
        frame; frame = frame->next)
        wl_columnar_eval_stack_cleanup_continuations(frame);
    int result = 0;
    /* Drain ALL source dependencies before attempting ANY pending image. */
    for (wl_columnar_eval_stack_cleanup_frame_t *frame = sess->cleanup_pending;
        frame; frame = frame->next){
        uint32_t mode = wl_columnar_eval_stack_cleanup_mode(frame);
        if (mode == WL_COLUMNAR_SERIAL_FRAME_OUTPUT
            || mode == WL_COLUMNAR_SERIAL_FRAME_RETIRED
            || mode == WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC
            || mode == WL_COLUMNAR_SERIAL_FRAME_REGISTRY
            || mode == WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY
            || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL
            || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL_INITIALIZING
            || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION
            || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION_INITIALIZING)
            continue;
        int rc = wl_columnar_relation_source_group_drain(&frame->join_sources,
                0);
        if (!result) result = rc;
    }
    wl_columnar_eval_stack_cleanup_frame_t **slot = &sess->cleanup_pending;
    while (*slot) {
        wl_columnar_eval_stack_cleanup_frame_t *frame = *slot;
        wl_columnar_eval_stack_cleanup_frame_t *next = frame->next;
        int rc = wl_columnar_eval_stack_cleanup_dispose(frame);
        if (rc == 0)
            rc = wl_columnar_eval_stack_cleanup_free(frame);
        if (rc == 0) {
            *slot = next;
            sess->cleanup_pending_count--;
        } else {
            if (result == 0)
                result = rc;
            slot = &frame->next;
        }
    }
    return result;
}

static int
wl_columnar_eval_stack_cleanup_clear_payload(
    wl_columnar_eval_stack_cleanup_frame_t *frame, void *payload, size_t bytes,
    uint32_t mode, wl_evaluation_control_t *control)
{
    wl_columnar_serial_completion_t *state = &frame->session->serial_completion;
    if (state->mode == WL_COLUMNAR_SERIAL_FRAME_INLINE_OUTPUT) return EBUSY;
    state->mode_frame = frame;
    state->mode = mode;
    state->payload_ready = false;
    for (size_t at = 0; at < bytes;) {
        size_t n = bytes - at;
        if (n > 128) n = 128;
        int rc = wl_evaluation_control_charge(control, n);
        if (rc) return rc;
        memset((char *)payload + at, 0, n);
        at += n;
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    state->payload_ready = true;
    return 0;
}

bool
wl_columnar_eval_stack_cleanup_registry_matches(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    const wl_columnar_session_hash_controlled_registry_t *registry)
{
    if (!frame || !frame->session) return false;
    wl_columnar_serial_completion_t *completion =
        &frame->session->serial_completion;
    if (completion->mode_frame != frame) return false;
    if (completion->mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION)
        return registry == NULL;
    if (completion->mode != WL_COLUMNAR_SERIAL_FRAME_REGISTRY
        && completion->mode !=
        WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY) return false;
    return completion->payload_ready ? registry ==
           &frame->registry : registry == NULL;
}

/* Valid original frame supplied by the canonical caller or disposer. The
 * bridge preadmits this at-most16-action terminal ownership disposition.
 * name_bytes/cursor were zeroed before the bridge and remain untouched. */
void
wl_columnar_eval_stack_cleanup_descriptor_published(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    wl_columnar_serial_completion_t *state = &frame->session->serial_completion;
    if (state->mode != WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY
        || state->mode_frame != frame || !state->payload_ready
        || frame->promotion.phase !=
        WL_COLUMNAR_SESSION_PROMOTION_CANONICAL_DESCRIPTOR
        || !frame->registry.published) return;
    frame->result.rel = NULL; frame->result.owned = false;
    frame->promotion.descriptor_moved = false;
    frame->promotion.descriptor_committed = false;
    frame->promotion.descriptor_ready = false;
    frame->promotion.governor_retained = false;
    frame->promotion.initialized_tokens = 0; frame->promotion.phase = 0;
    frame->promotion.descriptor_bytes = 0; frame->promotion.governor = NULL;
}

int
wl_columnar_eval_stack_cleanup_descriptor_registry(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_session_hash_controlled_registry_t **out,
    wl_evaluation_control_t *control)
{
    if (!frame || !out || *out || !control || !frame->session) return EINVAL;
    wl_columnar_serial_completion_t *state = &frame->session->serial_completion;
    int rc = wl_evaluation_control_charge(control, 64); if (rc) return rc;
    if (state->mode_frame != frame
        || state->mode != WL_COLUMNAR_SERIAL_FRAME_PROMOTION
        || !state->payload_ready || frame->result.rel || frame->result.owned
        || frame->stack.top != 1 || !frame->stack.items[0].owned
        || !frame->stack.items[0].rel
        || frame->stack.items[0].kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
        || frame->stack.items[0].seg_boundaries
        || frame->stack.items[0].continuation) return EINVAL;
    /* Result is inert during the true promotion retirement. The carrier
     * remains the sole physical owner on any refusal here. */
    rc = wl_columnar_eval_stack_workspace_retire(frame); if (rc) return rc;
    rc = wl_columnar_relation_bulk_append_retire(&frame->bulk_append, 0);
    if (rc) return rc;
    if (frame->bulk_append.active || frame->promotion.phase
        || frame->promotion.descriptor_moved ||
        frame->promotion.governor_retained
        || frame->incremental.active) return EBUSY;
    rc = wl_columnar_relation_descriptor_registry_validate(&frame->bulk_append,
            frame->stack.items[0].rel, control); if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 16); if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 64); if (rc) return rc;
    rc = wl_evaluation_control_charge(control, 0); if (rc) return rc;
    frame->result = frame->stack.items[0];
    frame->stack.items[0] = (eval_entry_t){0}; frame->stack.top = 0;
    frame->promotion.governor = frame->result.rel->memory_governor;
    frame->promotion.descriptor_bytes =
        frame->result.rel->descriptor_reserved_bytes;
    frame->promotion.name_bytes = frame->promotion.cursor = 0;
    frame->promotion.ready = true;
    frame->promotion.descriptor_moved = frame->promotion.descriptor_committed =
        true;
    frame->promotion.descriptor_ready = frame->promotion.governor_retained =
        true;
    frame->promotion.initialized_tokens = 5;
    frame->promotion.phase = WL_COLUMNAR_SESSION_PROMOTION_CANONICAL_DESCRIPTOR;
    state->mode = WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY;
    state->payload_ready = false;
    rc = wl_columnar_eval_stack_cleanup_clear_payload(frame, &frame->registry,
            sizeof(frame->registry),
            WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY, control);
    if (!rc) *out = &frame->registry;
    return rc;
}

int
wl_columnar_eval_stack_cleanup_registry(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_session_hash_controlled_registry_t **out,
    wl_evaluation_control_t *control)
{
    if (!frame || !out || !control) return EINVAL;
    uint32_t mode = wl_columnar_eval_stack_cleanup_mode(frame);
    if (mode == WL_COLUMNAR_SERIAL_FRAME_COMPOUND_GC
        || mode == WL_COLUMNAR_SERIAL_FRAME_RETIRED
        || mode == WL_COLUMNAR_SERIAL_FRAME_DESCRIPTOR_REGISTRY) return EBUSY;
    int rc = 0;
    if (mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION_INITIALIZING)
        rc = wl_columnar_eval_stack_workspace_retire(frame);
    if (!rc) rc = wl_columnar_relation_bulk_append_retire(&frame->bulk_append,
                0);
    if (!rc && frame->bulk_append.active) rc = EBUSY;
    bool incremental = mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL
        || mode == WL_COLUMNAR_SERIAL_FRAME_INCREMENTAL_INITIALIZING
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION
        || mode == WL_COLUMNAR_SERIAL_FRAME_PROMOTION_INITIALIZING;
    if (!incremental) {
        if (!rc) rc =
                wl_columnar_relation_private_batch_retire(&frame->reduce_batch,
                    0);
        if (!rc && !wl_columnar_relation_private_batch_disposable(
                &frame->reduce_batch)) rc = EBUSY;
    }
    if (!rc) rc = wl_columnar_eval_stack_workspace_retire(frame);
    if (!incremental && !rc)
        rc = wl_columnar_relation_source_group_drain(&frame->join_sources, 0);
    if (!rc) rc = eval_stack_drain(&frame->stack);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_cleanup_clear_payload(frame, &frame->registry,
            sizeof(frame->registry), WL_COLUMNAR_SERIAL_FRAME_REGISTRY,
            control);
    if (!rc) *out = &frame->registry;
    return rc;
}

int
wl_columnar_eval_stack_cleanup_output(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_serial_output_t **out, wl_evaluation_control_t *control)
{
    if (!frame || !out || !control) return EINVAL;
    int rc = wl_columnar_eval_stack_cleanup_dispose(frame);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_cleanup_clear_payload(frame, &frame->output,
            sizeof(frame->output), WL_COLUMNAR_SERIAL_FRAME_OUTPUT, control);
    if (!rc) *out = &frame->output;
    return rc;
}

bool
wl_columnar_eval_stack_output_journal_owned(wl_col_session_t *sess,
    const wl_columnar_serial_completion_t *journal)
{
    if (!sess || !journal) return false;
    wl_columnar_serial_completion_t *state = &sess->serial_completion;
    if (state->mode == WL_COLUMNAR_SERIAL_FRAME_INLINE_OUTPUT)
        return journal == state;
    if (state->mode != WL_COLUMNAR_SERIAL_FRAME_OUTPUT
        || !state->payload_ready || !state->frame
        || state->mode_frame != state->frame) return false;
    return state->frame->session == sess
           && journal == &state->frame->output.journal;
}

int
wl_columnar_eval_stack_cleanup_retained(wl_col_session_t *sess)
{
    if (!sess) return 0;
    if (sess->serial_completion.mode == WL_COLUMNAR_SERIAL_FRAME_INLINE_OUTPUT)
        return wl_columnar_session_serial_output_drain(sess,
                   &sess->serial_completion);
    if (!sess->serial_completion.frame) return 0;
    wl_columnar_eval_stack_cleanup_frame_t *frame =
        sess->serial_completion.frame;
    if (sess->cleanup_active || sess->cleanup_pending) return EBUSY;
    int rc = wl_columnar_eval_stack_cleanup_dispose(frame);
    if (rc) return rc;
    sess->serial_completion.frame = NULL;
    rc = wl_columnar_eval_stack_cleanup_free(frame);
    if (rc) sess->serial_completion.frame = frame;
    return rc;
}

int
wl_columnar_eval_stack_cleanup_retained_drain(wl_col_session_t *sess)
{
    if (sess && sess->serial_completion.mode ==
        WL_COLUMNAR_SERIAL_FRAME_INLINE_OUTPUT)
        return wl_columnar_session_serial_output_drain(sess,
                   &sess->serial_completion);
    return sess && sess->serial_completion.frame
        ? wl_columnar_eval_stack_cleanup_dispose(sess->serial_completion.frame)
        : 0;
}

int
wl_columnar_eval_stack_cleanup_resume(wl_col_session_t *sess,
    wl_columnar_eval_stack_cleanup_frame_t **out,
    wl_evaluation_control_t *control)
{
    if (!sess || !out || !control || sess->serial_completion.mode ==
        WL_COLUMNAR_SERIAL_FRAME_INLINE_OUTPUT || sess->cleanup_active
        || sess->cleanup_pending ||
        !sess->serial_completion.frame) return EBUSY;
    wl_columnar_eval_stack_cleanup_frame_t *frame =
        sess->serial_completion.frame;
    int rc = wl_columnar_eval_stack_cleanup_dispose(frame);
    if (rc) return rc;
    /* OUTPUT with readiness false is the safe discard disposition while the
     * wholly retired evaluator payload is initialized in bounded slices. */
    rc = wl_columnar_eval_stack_cleanup_clear_payload(frame,
            &frame->original_payload, sizeof(frame->original_payload),
            WL_COLUMNAR_SERIAL_FRAME_OUTPUT, control);
    if (rc) return rc;
    sess->serial_completion.payload_ready = false;
    wl_columnar_memory_reservation_t *tokens[3] = {
        &frame->consolidate.sequence.metadata_admission,
        &frame->consolidate.sequence.workspace.admission,
        &frame->consolidate.merge_admission
    };
    for (uint32_t i = 0; i < 3; i++) {
        rc = wl_evaluation_control_charge(control, 128);
        if (rc) return rc;
        wl_columnar_memory_reservation_init(tokens[i]);
        rc = wl_evaluation_control_charge(control, 0);
        if (rc) return rc;
    }
    rc = wl_evaluation_control_charge(control, 32);
    if (rc) return rc;
    frame->reduce_batch.ready = true;
    frame->consolidate.ready = true;
    frame->bulk_append.ready = true;
    sess->serial_completion.mode = WL_COLUMNAR_SERIAL_FRAME_EVALUATION;
    sess->serial_completion.payload_ready = true;
    rc = wl_evaluation_control_charge(control, 0);
    if (rc) return rc;
    frame->next = sess->cleanup_active;
    sess->cleanup_active = frame;
    sess->cleanup_active_count++;
    *out = frame;
    return 0;
}
