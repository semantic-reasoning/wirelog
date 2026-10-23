/*
 * columnar/eval_serial.c - serial stratum evaluator
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Serial (single-threaded) stratum evaluation extracted from
 * columnar/eval.c.
 */

#define _GNU_SOURCE

#include "columnar/internal.h"
#include "wirelog/util/log.h"

#include "../wirelog-internal.h"

#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Fresh-head continuation. Child calls return directly to the real host;
 * the completed grant remains borrowed until a later funded consume checks
 * its completed report. No report is inferred from a child's numeric errno.
 * Source accounting and first-capacity obligations are qualified against the
 * complete frozen implementation, including callers, before activation. */
enum {
    WL_COLUMNAR_EVAL_SERIAL_FRAMED_HOST_WORK = 384,
    WL_COLUMNAR_EVAL_SERIAL_FRAMED_BOOTSTRAP = 392,
    WL_COLUMNAR_EVAL_SERIAL_FRAMED_PAYMENT = 192,
    WL_COLUMNAR_EVAL_SERIAL_FRAMED_WORK = 256,
    WL_COLUMNAR_EVAL_SERIAL_FRAMED_CELL = 128
};

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_serial_test_framed_checkpoint)(
    wl_columnar_eval_serial_framed_attempt_t *);
int wl_columnar_eval_serial_test_framed_fail_allocation = -1;
#endif

static bool
wl_columnar_eval_serial_framed_thread(
    const wl_columnar_eval_serial_framed_attempt_t *ctx)
{
    if (!ctx->thread_valid) return false;
#if defined(WL_HAVE_C11_THREADS)
    return thrd_equal(ctx->owner_thread, thrd_current());
#elif defined(_WIN32) || defined(_WIN64)
    return ctx->owner_thread == GetCurrentThreadId();
#else
    return pthread_equal(ctx->owner_thread, pthread_self());
#endif
}

bool
wl_columnar_eval_serial_framed_owner_valid(
    const wl_columnar_eval_serial_framed_attempt_t *ctx,
    const wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return ctx && ctx->identity == ctx && ctx->frame == frame
           && ctx->session && ctx->control && ctx->attempt_id
           && ctx->pin_owned && ctx->plan && ctx->phase
           && ctx->phase <= WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
           && wl_columnar_eval_serial_framed_thread(ctx);
}

static int
wl_columnar_eval_serial_framed_validate(
    wl_columnar_eval_serial_framed_attempt_t *ctx)
{
    if (!ctx || !wl_columnar_eval_serial_framed_owner_valid(ctx, ctx->frame))
        return EINVAL;
    wl_columnar_eval_serial_framed_attempt_t *original = NULL;
    int rc = wl_columnar_eval_stack_framed_context(ctx->frame, ctx->session,
            &original);
    return rc ? rc : original == ctx ? 0 : EINVAL;
}

static int
wl_columnar_eval_serial_framed_fund(wl_evaluation_control_t *funding,
    uint64_t id, const wl_evaluation_control_t *operation)
{
    if (!funding || funding == operation || !id
        || wl_evaluation_control_active_attempt_id(funding) != id)
        return EINVAL;
    return wl_evaluation_control_charge(funding,
               WL_COLUMNAR_EVAL_SERIAL_FRAMED_HOST_WORK);
}

static int
wl_columnar_eval_serial_framed_charge(wl_evaluation_control_t *control,
    uint64_t id, uint64_t work)
{
    if (!control || !id
        || wl_evaluation_control_active_attempt_id(control) != id)
        return EINVAL;
    int rc = wl_evaluation_control_charge(control, work);
    if (rc) return rc;
    return wl_evaluation_control_active_attempt_id(control) == id ? 0 : EINVAL;
}

static int
wl_columnar_eval_serial_framed_tail(
    wl_columnar_eval_serial_framed_attempt_t *ctx)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_serial_test_framed_checkpoint)
        wl_columnar_eval_serial_test_framed_checkpoint(ctx);
#endif
    return wl_columnar_eval_serial_framed_charge(ctx->control,
               ctx->attempt_id, 0);
}

static void
wl_columnar_eval_serial_framed_fail(
    wl_columnar_eval_serial_framed_attempt_t *ctx, int result)
{
    if (!ctx->primary_result) ctx->primary_result = result;
    ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_BATCH_END;
}

static void
wl_columnar_eval_serial_framed_pending(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_columnar_eval_serial_framed_phase_t action,
    wl_evaluation_control_t *funding, uint64_t id)
{
    ctx->pending_action = action;
    ctx->pending_funding = funding;
    ctx->pending_funding_id = id;
    ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING;
}

static int
wl_columnar_eval_serial_framed_attempt_begin_internal(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    const wl_plan_relation_t *plan, wl_evaluation_control_t *control,
    uint64_t id, uint64_t operation_identity,
    bool controlled_materialized_batch_fallback,
    const wl_columnar_eval_private_bindings_t *private_bindings,
    bool capture_only,
    col_rel_t **capture_out,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (!frame || !session || !plan || !plan->name || !plan->ops
        || !plan->op_count || !control || !id || !operation_identity)
        return EINVAL;
    if (capture_only != (private_bindings != NULL)
        || capture_only != (capture_out != NULL)) return EINVAL;
    if (capture_only
        ? !wl_columnar_eval_plan_numeric_recursive_path_supported(plan)
        : plan->ops[plan->op_count-1].op != WL_PLAN_OP_CONSOLIDATE)
        return ENOTSUP;
    bool abort_supported = false;
    uint32_t child_end = capture_only ? plan->op_count - 1 : plan->op_count;
    int rc = wl_columnar_eval_plan_numeric_composed_abort_supported(plan, 0,
            child_end, &abort_supported);
    if (rc) return rc;
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    rc = wl_columnar_eval_stack_framed_context(frame, session, &ctx);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_cleanup_receipt_key_for_frame(frame,
            &ctx->cleanup_receipt_key);
    if (rc) return rc;
    if (ctx->cleanup_receipt_key.operation_attempt_id != id
        || ctx->cleanup_receipt_key.operation_id != operation_identity)
        return EINVAL;
    if (controlled_materialized_batch_fallback
        && (!session->join_batch_bytes || session->join_batch_strict
        || session->join_output_limit
        || !wl_columnar_session_controlled_batch_authorized(session, control,
        id, operation_identity)
        || plan->op_count != 5 || plan->ops[0].op != WL_PLAN_OP_VARIABLE
        || plan->ops[1].op != WL_PLAN_OP_JOIN
        || plan->ops[2].op != WL_PLAN_OP_FILTER
        || plan->ops[3].op != WL_PLAN_OP_MAP
        || plan->ops[4].op != WL_PLAN_OP_CONSOLIDATE)) return ENOTSUP;
    rc = wl_columnar_eval_stack_framed_fresh(frame, ctx);
    if (rc) return rc;
    rc = wl_columnar_eval_serial_framed_fund(funding, funding_id, control);
    if (rc) return rc;
    /* 392*192 = current50176 + future25088 = 98 named256 capacities:
     * stack/result disposal33, later inert proof33, B2drain18, workspace4,
     * candidate1, pin1, finalreset/emptyclose1, gate1, childACKparent1,
     * normalization1, sourceclear1, completedgrant borrow1, outcome latch1,
     * closeproof1.
     * All finish before the parent pin/marker; unused shares are forfeited. */
    for (unsigned i = 0; i < WL_COLUMNAR_EVAL_SERIAL_FRAMED_BOOTSTRAP; ++i) {
        rc = wl_columnar_eval_serial_framed_charge(control, id,
                WL_COLUMNAR_EVAL_SERIAL_FRAMED_PAYMENT);
        if (rc) return rc;
    }
    rc = wl_columnar_eval_serial_framed_charge(control, id,
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_WORK);
    if (rc) return rc;
    rc = wl_evaluation_control_retain(control);
    if (rc) return rc;
    /* The entire frame, including the embedded child, was already zeroed
     * before publication by the original frame allocator. */
    ctx->frame = frame; ctx->session = session; ctx->plan = plan;
    ctx->control = control; ctx->attempt_id = id;
    ctx->operation_identity = operation_identity; ctx->pin_owned = true;
    ctx->abort_supported = abort_supported;
    ctx->controlled_materialized_batch_fallback =
        controlled_materialized_batch_fallback;
    ctx->private_bindings = private_bindings;
    ctx->capture_out = capture_out;
    ctx->capture_only = capture_only;
    ctx->capture_done = false;
#if defined(WL_HAVE_C11_THREADS)
    ctx->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    ctx->owner_thread = GetCurrentThreadId();
#else
    ctx->owner_thread = pthread_self();
#endif
    ctx->thread_valid = true;
    ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_LOOKUP;
    ctx->identity = ctx;
    return 0;
}

int
wl_columnar_eval_serial_framed_attempt_begin(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    const wl_plan_relation_t *plan, wl_evaluation_control_t *control,
    uint64_t id, uint64_t operation_identity,
    bool controlled_materialized_batch_fallback,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    return wl_columnar_eval_serial_framed_attempt_begin_internal(frame,
               session, plan, control, id, operation_identity,
               controlled_materialized_batch_fallback, NULL, false, NULL,
               funding, funding_id);
}

int
wl_columnar_eval_serial_framed_attempt_begin_bound(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    const wl_plan_relation_t *plan, wl_evaluation_control_t *control,
    uint64_t id, uint64_t operation_identity,
    bool controlled_materialized_batch_fallback,
    const wl_columnar_eval_private_bindings_t *private_bindings,
    bool capture_only, col_rel_t **capture_out,
    wl_evaluation_control_t *funding,
    uint64_t funding_id)
{
    if (!private_bindings || !capture_only || !capture_out || *capture_out)
        return EINVAL;
    return wl_columnar_eval_serial_framed_attempt_begin_internal(frame,
               session, plan, control, id, operation_identity,
               controlled_materialized_batch_fallback, private_bindings,
               capture_only, capture_out, funding, funding_id);
}

static int
wl_columnar_eval_serial_framed_lookup(
    wl_columnar_eval_serial_framed_attempt_t *ctx)
{
    int rc = wl_columnar_eval_serial_framed_charge(ctx->control,
            ctx->attempt_id, WL_COLUMNAR_EVAL_SERIAL_FRAMED_CELL);
    if (rc) return rc;
    if (ctx->registry_slot == ctx->session->nrels) {
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_BEGIN;
        return 0;
    }
    if (!ctx->session->rels || ctx->registry_slot > ctx->session->nrels)
        return EINVAL;
    col_rel_t *rel = ctx->session->rels[ctx->registry_slot];
    if (!rel || !rel->name) {
        ++ctx->registry_slot; ctx->name_byte = 0;
        return 0;
    }
    unsigned char a = (unsigned char)rel->name[ctx->name_byte];
    unsigned char b = (unsigned char)ctx->plan->name[ctx->name_byte];
    rc = wl_columnar_eval_serial_framed_tail(ctx);
    if (rc) return rc;
    if (a != b) {
        ++ctx->registry_slot; ctx->name_byte = 0;
    } else if (!a) {
        if (!ctx->capture_only) return ENOTSUP;
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_BEGIN;
    }else if (ctx->name_byte == UINT32_MAX)return EOVERFLOW;
    else ++ctx->name_byte;
    return 0;
}

static int
wl_columnar_eval_serial_framed_scratch(
    wl_columnar_eval_serial_framed_attempt_t *ctx)
{
    wl_columnar_join_scratch_t *scratch = &ctx->scratch;
    int rc = wl_columnar_eval_serial_framed_charge(ctx->control,
            ctx->attempt_id, WL_COLUMNAR_EVAL_SERIAL_FRAMED_WORK);
    if (rc) return rc;
    uint64_t bytes = 0;
    if (!wl_columnar_memory_size_mul(ctx->source.width ? ctx->source.width : 1,
        sizeof(int64_t), &bytes) || bytes > SIZE_MAX) return EOVERFLOW;
    wl_columnar_memory_reservation_init(&scratch->credit);
    scratch->initialized = true;
    wl_columnar_memory_governor_ref_t *governor = ctx->session->memory_governor
        ? ctx->session->memory_governor : ctx->input.rel->memory_governor;
    rc = wl_columnar_eval_serial_framed_tail(ctx);
    if (rc) return rc;
    if (governor) {
        rc = wl_columnar_eval_serial_framed_charge(ctx->control,
                ctx->attempt_id, WL_COLUMNAR_EVAL_SERIAL_FRAMED_WORK);
        if (rc) return rc;
        scratch->governor = governor;
        wl_columnar_memory_governor_ref_retain(governor);
        scratch->ref_held = true;
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
            wl_columnar_memory_governor_ref_get(governor), bytes,
            &scratch->credit, &status, ctx->control, ctx->attempt_id);
        if (rc) return rc;
        scratch->admitted = status == WL_COLUMNAR_MEMORY_ADMISSION_OK
            || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY;
        rc = wl_columnar_eval_serial_framed_tail(ctx);
        if (rc) return rc;
        if (!scratch->admitted) return status ==
                   WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW : EINVAL;
    }
    rc = wl_columnar_eval_serial_framed_charge(ctx->control, ctx->attempt_id,
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_WORK);
    if (rc) return rc;
#ifdef WL_SESSION_TEST_HOOKS
    bool fail = wl_columnar_eval_serial_test_framed_fail_allocation == 0;
    if (wl_columnar_eval_serial_test_framed_fail_allocation > 0)
        --wl_columnar_eval_serial_test_framed_fail_allocation;
    scratch->physical = fail ? NULL : malloc((size_t)bytes);
#else
    scratch->physical = malloc((size_t)bytes);
#endif
    scratch->allocated = scratch->physical != NULL;
    rc = wl_columnar_eval_serial_framed_tail(ctx);
    if (rc) return rc;
    if (!scratch->allocated) return ENOMEM;
    if (scratch->admitted) {
        rc = wl_columnar_memory_governor_commit_attempt_controlled(
            &scratch->credit, scratch->physical, &scratch->committed,
            ctx->control, ctx->attempt_id);
        if (rc) return rc;
    }
    return wl_columnar_eval_serial_framed_tail(ctx);
}

int
wl_columnar_eval_serial_framed_attempt_step(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_serial_framed_validate(ctx);
    if (rc) return rc;
    if (ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING) return EBUSY;
    rc = wl_columnar_eval_serial_framed_fund(funding, funding_id, ctx->control);
    if (rc) return rc;
    switch (ctx->phase) {
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_LOOKUP:
        rc = wl_columnar_eval_serial_framed_lookup(ctx);
        if (rc) {
            if (!ctx->primary_result) ctx->primary_result = rc;
            ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_CLAIM;
        }
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_BEGIN:
        wl_columnar_eval_serial_framed_pending(ctx, ctx->phase, funding,
            funding_id);
        if (ctx->private_bindings)
            return wl_columnar_eval_plan_numeric_composed_attempt_begin_bound(
                &ctx->child, ctx->plan, 0, ctx->plan->op_count - 1, ctx->frame,
                ctx->session, 0, ctx->control, ctx->attempt_id, funding,
                funding_id, ctx->private_bindings);
        if (ctx->controlled_materialized_batch_fallback)
            return
                wl_columnar_eval_plan_numeric_composed_attempt_begin_batch_fallback
                (
                &ctx->child, ctx->plan, 0, ctx->plan->op_count, ctx->frame,
                ctx->session, 0, ctx->control, ctx->attempt_id, funding,
                funding_id);
        return wl_columnar_eval_plan_numeric_composed_attempt_begin(
            &ctx->child, ctx->plan, 0, ctx->plan->op_count, ctx->frame,
            ctx->session, 0, ctx->control, ctx->attempt_id, funding,
            funding_id);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD:
        if (ctx->child.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE) {
            if (!ctx->primary_result)
                ctx->primary_result = ctx->child.semantic_result;
            ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_CLAIM;
            return 0;
        }
        ctx->pending_inner_consume = ctx->child.phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING;
        wl_columnar_eval_serial_framed_pending(ctx, ctx->phase, funding,
            funding_id);
        if (ctx->pending_inner_consume)
            return wl_columnar_eval_plan_numeric_composed_consume_result(
                &ctx->child, ctx->inner_protocol, funding, funding_id);
        return wl_columnar_eval_plan_numeric_composed_attempt_step(
            &ctx->child, funding, funding_id);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_CLAIM: {
        bool materialize = !ctx->primary_result;
        if (materialize) {
            rc = wl_columnar_eval_serial_framed_charge(ctx->control,
                    ctx->attempt_id, WL_COLUMNAR_EVAL_SERIAL_FRAMED_WORK);
            if (rc) {
                ctx->primary_result = rc;
                return 0;
            }
        }
        rc = wl_columnar_eval_stack_framed_claim(ctx, materialize);
        if (rc) {
            if (!ctx->primary_result) ctx->primary_result = rc;
            ctx->cleanup_result = rc;
            return 0;
        }
        ctx->cleanup_result = 0;
        ctx->phase = materialize ? WL_COLUMNAR_EVAL_SERIAL_FRAMED_SOURCE_INIT
            : WL_COLUMNAR_EVAL_SERIAL_FRAMED_BATCH_END;
        return 0;
    }
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SOURCE_INIT:
        wl_columnar_eval_serial_framed_pending(ctx, ctx->phase, funding,
            funding_id);
        return wl_columnar_relation_scalar_source_init_attempt_controlled(
            &ctx->source, &ctx->source_initialized, ctx->control,
            ctx->attempt_id);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SOURCE_HOLD:
        wl_columnar_eval_serial_framed_pending(ctx, ctx->phase, funding,
            funding_id);
        return wl_columnar_relation_scalar_source_begin_attempt_controlled(
            &ctx->source, ctx->input.rel, false);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_VALIDATE: {
        rc = wl_columnar_eval_serial_framed_charge(ctx->control,
                ctx->attempt_id, WL_COLUMNAR_EVAL_SERIAL_FRAMED_CELL);
        if (rc) {
            wl_columnar_eval_serial_framed_fail(ctx, rc);
            return 0;
        }
        const col_rel_t *rel = ctx->source.descriptor;
        bool canonical = rel && !rel->timestamps
            && rel->compound_kind == WIRELOG_COMPOUND_KIND_NONE
            && rel->sorted_nrows == rel->nrows
            && rel->run_count == (rel->nrows != 0)
            && (!rel->nrows || rel->run_ends[0] == rel->nrows);
        rc = wl_columnar_eval_serial_framed_tail(ctx);
        if (rc || !canonical) {
            wl_columnar_eval_serial_framed_fail(ctx, rc ? rc : ENOTSUP);
            return 0;
        }
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_NEW;
        return 0;
    }
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_NEW:
        ctx->candidate_owned = true;
        wl_columnar_eval_serial_framed_pending(ctx, ctx->phase, funding,
            funding_id);
        return
            wl_columnar_relation_scalar_source_new_like_operator_attempt_controlled
                (&ctx->source, ctx->plan->name, ctx->session->memory_governor
                ? ctx->session->memory_governor :
                ctx->input.rel->memory_governor,
                &ctx->candidate);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SCRATCH:
        wl_columnar_eval_serial_framed_pending(ctx, ctx->phase, funding,
            funding_id);
        return wl_columnar_eval_serial_framed_scratch(ctx);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_BATCH_PREPARE:
        rc = wl_columnar_eval_serial_framed_charge(ctx->control,
                ctx->attempt_id, WL_COLUMNAR_EVAL_SERIAL_FRAMED_WORK);
        if (rc) {
            wl_columnar_eval_serial_framed_fail(ctx, rc);
            return 0;
        }
        wl_columnar_filter_attempt_batch_init(&ctx->batch);
        wl_columnar_relation_private_batch_drain_init(&ctx->drain);
        ctx->batch_initialized = true;
        rc = wl_columnar_eval_serial_framed_tail(ctx);
        if (rc) wl_columnar_eval_serial_framed_fail(ctx, rc);
        else ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_BATCH_BEGIN;
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_BATCH_BEGIN:
        wl_columnar_eval_serial_framed_pending(ctx, ctx->phase, funding,
            funding_id);
        return wl_columnar_relation_append_batch_begin_controlled(&ctx->batch,
                   ctx->candidate, ctx->control);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_GATHER:
        if (ctx->row == ctx->source.nrows) {
            ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_SORTED;
            return 0;
        }
        wl_columnar_eval_serial_framed_pending(ctx, ctx->phase, funding,
            funding_id);
        return wl_columnar_relation_scalar_source_gather_attempt_controlled(
            &ctx->source, ctx->row, ctx->scratch.physical, ctx->source.width);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_APPEND:
        wl_columnar_eval_serial_framed_pending(ctx, ctx->phase, funding,
            funding_id);
        return
            wl_columnar_relation_append_batch_selected_source_row_operator_controlled
                (&ctx->batch, &ctx->source, ctx->row, ctx->scratch.physical,
                ctx->source.width);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SORTED:
        wl_columnar_eval_serial_framed_pending(ctx, ctx->phase, funding,
            funding_id);
        return wl_columnar_relation_append_batch_mark_sorted_operator_controlled
               (
            &ctx->batch, ctx->control, ctx->attempt_id);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_BATCH_END:
        if (!ctx->batch_initialized) {
            ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_SOURCE_END;
            return 0;
        }
        wl_columnar_eval_serial_framed_pending(ctx, ctx->phase, funding,
            funding_id);
        return wl_columnar_relation_private_batch_drain_step(&ctx->drain,
                   &ctx->batch, ctx->primary_result, &ctx->batch_done);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SOURCE_END:
        if (!ctx->source_initialized) {
            ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_ENTRY_DRAIN;
            return 0;
        }
        wl_columnar_eval_serial_framed_pending(ctx, ctx->phase, funding,
            funding_id);
        return wl_columnar_relation_scalar_source_end(&ctx->source,
                   ctx->primary_result);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_ENTRY_DRAIN:
        if (ctx->cursor == COL_STACK_MAX+1) {
            ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_SCRATCH_PHYSICAL;
            return 0;
        }
        rc = wl_columnar_eval_stack_framed_dispose_slot(ctx, ctx->cursor);
        ctx->cleanup_result = rc;
        if (!rc) ++ctx->cursor;
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SCRATCH_PHYSICAL:
        if (ctx->scratch.allocated) {
            void *physical = ctx->scratch.physical;
            ctx->scratch.physical = NULL; ctx->scratch.allocated = false;
            free(physical);
        }
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_SCRATCH_CREDIT;
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SCRATCH_CREDIT:
        if (ctx->scratch.admitted) {
            bool released = ctx->scratch.committed
                ? wl_columnar_memory_release(&ctx->scratch.credit)
                : wl_columnar_memory_rollback(&ctx->scratch.credit);
            if (!released) {
                ctx->cleanup_result = EINVAL;
                return 0;
            }
            ctx->scratch.admitted = false;
        }
        ctx->cleanup_result = 0;
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_SCRATCH_REFERENCE;
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SCRATCH_REFERENCE:
        if (ctx->scratch.ref_held) {
            wl_columnar_memory_governor_ref_release(ctx->scratch.governor);
            ctx->scratch.ref_held = false; ctx->scratch.governor = NULL;
        }
        ctx->phase = ctx->materialized && !ctx->primary_result
            ? WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY
            : WL_COLUMNAR_EVAL_SERIAL_FRAMED_CANDIDATE_ABORT;
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY:
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_IMAGE:
        return wl_columnar_session_hash_registry_attempt_step(ctx);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_CANDIDATE_ABORT:
        if (ctx->candidate) {
            rc = col_rel_destroy_checked(ctx->candidate);
            if (rc) {
                ctx->cleanup_result = rc;
                return 0;
            }
            ctx->candidate = NULL;
        }
        ctx->candidate_owned = false; ctx->materialized = false;
        ctx->cleanup_result = 0;
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_GATE_RELEASE;
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_GATE_RELEASE:
        rc = wl_columnar_eval_stack_framed_release_gate(ctx);
        ctx->cleanup_result = rc;
        if (!rc) ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_ACK;
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_ACK:
        if (!ctx->child_bound) {
            ctx->cursor = 0;
            ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_SPARE_PROOF;
            return 0;
        }
        wl_columnar_eval_serial_framed_pending(ctx, ctx->phase, funding,
            funding_id);
        return wl_columnar_eval_plan_numeric_composed_attempt_ack(&ctx->child,
                   funding, funding_id);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SPARE_PROOF:
        /* This state is reached only after genuine child ACK (or no child).
         * The committed image still owns the old source registry until now. */
        if (ctx->publication.identity && ctx->publication.committed
            && !ctx->publication.drained) {
            if (ctx->child_bound || ctx->child.identity) return EBUSY;
            ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_IMAGE;
            return 0;
        }
        if (ctx->cursor == COL_STACK_MAX+1) {
            ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_NORMALIZE;
            return 0;
        }
        rc = wl_columnar_eval_stack_framed_prove_slot(ctx, ctx->cursor);
        ctx->cleanup_result = rc;
        if (!rc) ++ctx->cursor;
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_NORMALIZE:
        rc = wl_columnar_eval_stack_framed_normalize(ctx);
        ctx->cleanup_result = rc;
        if (!rc) ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_CLOSE_PROOF;
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_CLOSE_PROOF:
        rc = wl_columnar_eval_stack_framed_close_proof(ctx);
        ctx->cleanup_result = rc;
        if (!rc) {
            ctx->close_proven = true;
            ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_DONE;
        }
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_DONE:
        return 0;
    default:
        return EINVAL;
    }
}

int
wl_columnar_eval_serial_framed_consume_result(
    wl_columnar_eval_serial_framed_attempt_t *ctx, int child_protocol,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_serial_framed_validate(ctx);
    if (rc) return rc;
    if (ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
        || !ctx->pending_funding || ctx->pending_funding == funding
        || wl_evaluation_control_active_attempt_id(ctx->pending_funding))
        return EBUSY;
    wl_evaluation_control_report_t report;
    rc = wl_evaluation_control_report(ctx->pending_funding, &report);
    if (rc || report.attempt_id != ctx->pending_funding_id
        || report.cleanup_status || (report.execution_status
        && report.execution_status != EOVERFLOW)) return EINVAL;
    rc = wl_columnar_eval_serial_framed_fund(funding, funding_id, ctx->control);
    if (rc) return rc;
    wl_columnar_eval_serial_framed_phase_t action = ctx->pending_action;
    ctx->pending_funding = NULL; ctx->pending_funding_id = 0;
    ctx->phase = action;
    if (action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD) {
        /* A failed funded Cconsume has not consumed the prior child result.
         * Preserve that result and retry the genuine pending child. */
        if (!ctx->pending_inner_consume) ctx->inner_protocol = child_protocol;
        return 0;
    }
    if (report.result) return 0; /* Exact host disposition, fresh retry. */
    switch (action) {
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_BEGIN:
        ctx->child_bound = ctx->child.identity == &ctx->child;
        if (child_protocol) {
            if (!ctx->primary_result) ctx->primary_result = child_protocol;
            ctx->phase = ctx->child_bound ?
                WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD
                : WL_COLUMNAR_EVAL_SERIAL_FRAMED_CLAIM;
        } else if (!ctx->child_bound) {
            ctx->primary_result = EINVAL;
            ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_CLAIM;
        } else ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD;
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_ACK:
        if (child_protocol) {
            ctx->cleanup_result = child_protocol;
            return 0;
        }
        ctx->child_bound = false; ctx->cleanup_result = 0;
        ctx->cursor = 0;
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_SPARE_PROOF;
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_BATCH_END:
        ctx->cleanup_result = child_protocol;
        if (!child_protocol && ctx->batch_done) {
            ctx->batch_initialized = false;
            ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_SOURCE_END;
        }
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SOURCE_END:
        if (ctx->source.phase == WL_COLUMNAR_RELATION_SCALAR_SOURCE_DRAINED) {
            ctx->source_initialized = false; ctx->cleanup_result = 0;
            ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_ENTRY_DRAIN;
        } else ctx->cleanup_result = child_protocol ? child_protocol : EBUSY;
        return 0;
    default:
        break;
    }
    if (child_protocol) {
        if (child_protocol == ENOMEM && ctx->candidate
            && ctx->candidate->memory_budget_denial_pending)
            child_protocol = ENOSPC;
        if (child_protocol == ENOSPC) ctx->session->memory_budget_denied = true;
        wl_columnar_eval_serial_framed_fail(ctx, child_protocol);
        return 0;
    }
    switch (action) {
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SOURCE_INIT:
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_SOURCE_HOLD; break;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SOURCE_HOLD:
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_VALIDATE; break;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_NEW:
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_SCRATCH; break;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SCRATCH:
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_BATCH_PREPARE; break;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_BATCH_BEGIN:
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_GATHER; break;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_GATHER:
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_APPEND; break;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_APPEND:
        ++ctx->row;
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_GATHER; break;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SORTED:
        ctx->materialized = true;
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_BATCH_END; break;
    default:
        return EINVAL;
    }
    return 0;
}

int
wl_columnar_eval_serial_framed_attempt_publish(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_serial_framed_validate(ctx);
    if (rc) return rc;
    if (ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY) return EBUSY;
    rc = wl_columnar_eval_serial_framed_fund(funding, funding_id, ctx->control);
    if (rc) return rc;
    rc = wl_columnar_session_hash_registry_attempt_begin(ctx);
    if (ctx->publication.identity) {
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_IMAGE;
    } else if (rc) {
        if (!ctx->primary_result) ctx->primary_result = rc;
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_CANDIDATE_ABORT;
    }
    return 0;
}

int
wl_columnar_eval_serial_framed_attempt_discard(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_serial_framed_validate(ctx);
    if (rc) return rc;
    if (ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY) return EBUSY;
    rc = wl_columnar_eval_serial_framed_fund(funding, funding_id, ctx->control);
    if (rc) return rc;
    ctx->materialized = false;
    ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_CANDIDATE_ABORT;
    return 0;
}

int
wl_columnar_eval_serial_framed_attempt_status(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_columnar_eval_serial_framed_status_t *out,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_serial_framed_validate(ctx);
    if (rc || !out) return rc ? rc : EINVAL;
    rc = wl_columnar_eval_serial_framed_fund(funding, funding_id, ctx->control);
    if (rc) return rc;
    out->phase = ctx->phase; out->execution_result = ctx->primary_result;
    out->cleanup_result = ctx->cleanup_result;
    out->ready = ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY;
    out->retained = ctx->cleanup_result != 0; out->committed = ctx->committed;
    return 0;
}

int
wl_columnar_eval_serial_framed_attempt_close(
    wl_columnar_eval_serial_framed_attempt_t *ctx, int *result, bool *committed,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_serial_framed_validate(ctx);
    if (rc || !result || !committed) return rc ? rc : EINVAL;
    if (ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_DONE) return EBUSY;
    rc = wl_columnar_eval_serial_framed_fund(funding, funding_id, ctx->control);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_framed_close_ready(ctx);
    if (rc) return rc;
    wl_evaluation_control_t *control = ctx->control;
    wl_columnar_eval_stack_cleanup_frame_t *frame = ctx->frame;
    rc = wl_columnar_eval_stack_cleanup_receipt_prepare(frame,
            &ctx->cleanup_receipt_key, ctx->primary_result, ctx->committed);
    if (rc) return rc;
    void *identity = ctx->identity;
    bool pin_owned = ctx->pin_owned;
    bool thread_valid = ctx->thread_valid;
    wl_columnar_eval_serial_framed_phase_t phase = ctx->phase;
    *result = ctx->primary_result; *committed = ctx->committed;
    ctx->identity = NULL; ctx->control = NULL; ctx->pin_owned = false;
    ctx->thread_valid = false;
    ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_INERT;
    wl_columnar_eval_cleanup_outcome_t outcome
        = WL_COLUMNAR_EVAL_CLEANUP_OUTCOME_RETAINED_ACTIVE;
    rc = wl_columnar_eval_stack_cleanup_finish_receipted(&frame,
            &ctx->cleanup_receipt_key, &outcome);
    if (outcome == WL_COLUMNAR_EVAL_CLEANUP_OUTCOME_RETAINED_ACTIVE) {
        ctx->identity = identity; ctx->control = control;
        ctx->pin_owned = pin_owned; ctx->thread_valid = thread_valid;
        ctx->phase = phase;
        return rc;
    }
    wl_evaluation_control_release(control);
    return rc;
}

/* Distributed (TDD) stratum evaluation is implemented in columnar/eval.c. */
static col_frontier_t
col_frontier_compute(const col_rel_t *rel);

static bool
col_reduce_output_group_equal(const col_rel_t *rel, uint32_t a, uint32_t b,
    uint32_t group_by_count, uint32_t aggregate_index)
{
    for (uint32_t c = 0; c < group_by_count; c++) {
        uint32_t out_col = c >= aggregate_index ? c + 1 : c;
        if (col_rel_get(rel, a, out_col)
            != col_rel_get(rel, b, out_col))
            return false;
    }
    return true;
}

typedef struct {
    uint64_t hash;
    uint32_t row;
} col_group_slot_t;

static uint64_t
col_group_hash(const col_rel_t *rel, uint32_t row, uint32_t group_by_count,
    uint32_t aggregate_index)
{
    uint64_t hash = UINT64_C(1469598103934665603);
    for (uint32_t c = 0; c < group_by_count; c++) {
        uint32_t out_col = c >= aggregate_index ? c + 1 : c;
        hash ^= (uint64_t)col_rel_get(rel, row, out_col);
        hash *= UINT64_C(1099511628211);
    }
    return hash ? hash : 1;
}

static int
wl_columnar_eval_serial_canonicalize_aggregate_locked(col_rel_t *rel,
    const wl_plan_agg_spec_t *spec, const wl_intern_t *intern)
{
    if (!rel || !spec || !spec->has_spec || rel->nrows < 2)
        return 0;

    uint32_t gc = spec->group_by_count;
    uint32_t agg_index = spec->aggregate_index < rel->ncols
        ? spec->aggregate_index : gc;
    if (rel->ncols <= gc)
        return EINVAL;

    uint32_t old_nrows = rel->nrows;
    uint32_t out = 0;
    bool logical_changed = false;
    uint32_t map_cap = 1;
    uint64_t desired = (uint64_t)rel->nrows * 2U;
    while ((uint64_t)map_cap < desired && map_cap <= UINT32_MAX / 2U)
        map_cap <<= 1;
    if ((uint64_t)map_cap < desired)
        return ENOMEM;
    uint64_t group_bytes = 0;
    if (!wl_columnar_memory_size_mul(map_cap, sizeof(col_group_slot_t),
        &group_bytes))
        return EOVERFLOW;
    wl_columnar_memory_reservation_t group_reservation;
    wl_columnar_memory_reservation_init(&group_reservation);
    if (rel->memory_governor) {
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(rel->memory_governor),
                group_bytes, &group_reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
            return status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW
                : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                ? ENOSPC : ENOMEM;
    }
    col_group_slot_t *groups = (col_group_slot_t *)calloc(map_cap,
            sizeof(*groups));
    if (!groups) {
        wl_columnar_memory_release(&group_reservation);
        return ENOMEM;
    }
    uint32_t map_mask = map_cap - 1;
    for (uint32_t row = 0; row < rel->nrows; row++) {
        uint32_t found = UINT32_MAX;
        uint64_t hash = col_group_hash(rel, row, gc, agg_index);
        uint32_t slot = (uint32_t)hash & map_mask;
        while (groups[slot].hash != 0) {
            uint32_t keep = groups[slot].row;
            if (groups[slot].hash == hash
                && col_reduce_output_group_equal(rel, keep, row, gc,
                agg_index)) {
                found = keep;
                break;
            }
            slot = (slot + 1) & map_mask;
        }

        if (found != UINT32_MAX) {
            int64_t cur = col_rel_get(rel, found, agg_index);
            int64_t val = col_rel_get(rel, row, agg_index);
            /* Same comparator as col_op_reduce(): a fixpoint that ordered
             * lexicographically within an iteration and by intern id across
             * iterations would be worse than the original bug (#965). */
            bool better = col_agg_better(spec->fn, spec->operand_type,
                    intern, val, cur);
            if (better) {
                rel->columns[agg_index][found] = val;
                if (rel->timestamps)
                    rel->timestamps[found] = rel->timestamps[row];
                logical_changed = true;
            }
            continue;
        }

        if (out != row) {
            for (uint32_t c = 0; c < rel->ncols; c++)
                rel->columns[c][out] = rel->columns[c][row];
            if (rel->timestamps)
                rel->timestamps[out] = rel->timestamps[row];
            logical_changed = true;
        }
        groups[slot].hash = hash;
        groups[slot].row = out;
        out++;
    }

    if (logical_changed || out != old_nrows) {
        rel->nrows = out;
        rel->sorted_nrows = out;
        rel->base_nrows = out;
        rel->run_count = 0;
        wl_columnar_eval_dedup_set_clear(rel);
        memset(rel->run_ends, 0, sizeof(rel->run_ends));
        wl_columnar_relation_touch_view(rel);
    }

    free(groups);
    wl_columnar_memory_release(&group_reservation);

    return 0;
}

static int
wl_columnar_eval_serial_canonicalize_aggregate(col_rel_t *rel,
    const wl_plan_agg_spec_t *spec, const wl_intern_t *intern,
    wl_col_session_t *sess)
{
    if (!rel || !spec || !spec->has_spec || rel->nrows < 2)
        return 0;
    wl_columnar_source_access_writer_t writer = { 0 };
    int rc = col_rel_source_writer_acquire(rel, &writer);
    if (rc != 0)
        return rc;
    col_rel_t *owner = NULL;
    rc = col_rel_storage_owner_resolve(rel, &owner);
    if (rc == 0 && (owner != rel
        || col_rel_storage_alias_borrow_count(owner) != 0))
        rc = EBUSY;
    if (rc == 0)
        rc = wl_columnar_eval_serial_canonicalize_aggregate_locked(rel,
                spec, intern);
    int release_rc = wl_columnar_source_access_writer_release(&writer);
    if (rc == ENOSPC)
        sess->memory_budget_denied = true;
    return rc != 0 ? rc : release_rc;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_serial_test_before_aggregate)(wl_col_session_t *,
    col_rel_t *);
#endif

int
wl_columnar_eval_serial_canonicalize_aggregates(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess)
{
    /*
     * Reading the recorded specification rather than looking for a REDUCE in
     * rp->ops is the whole of #975: rewrite_multiway_delta() replaces a fused
     * relation's operators with a single K_FUSION and moves the originals into
     * its opaque_data, so the operator was not there to be found and this ran
     * on every recursive aggregate relation *except* the ones fusion applies
     * to.  wl_plan_relation_t.recursive_agg is filled at IR lowering, before
     * any rewrite runs.
     *
     * Timing is unchanged: this is the fixpoint, not per-iteration
     * consolidation.  A same-stratum consumer would therefore hold rows
     * derived from labels this reduction then dominates away, output that
     * contradicts itself.  #1021 settled that by refusing the shape at plan
     * time -- a recursive min/max aggregate may not share an SCC with any
     * other relation -- rather than by changing when this runs, so nothing
     * here had to move.  Moving the reduction into consolidation is still
     * open and would readmit the intra-round half of the shape; that is
     * deferred to milestone 0.70.0, see docs/SEMANTICS.md.
     *
     * The correction that matters to a reader of the old comment here:
     * wl_plan_stratum_t.is_monotone is no longer "hardcoded false and never
     * computed".  wl_plan_from_program() computes it (negation only).  Its
     * one consumer is the snapshot's incremental-route gate (#2114), which
     * reads a false value as "evaluate in full"; nothing here acts on it.
     */
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const wl_plan_relation_t *rp = &sp->relations[ri];
        if (!rp->recursive_agg.has_spec)
            continue;
        col_rel_t *rel = session_find_rel(sess, rp->name);
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_columnar_eval_serial_test_before_aggregate)
            wl_columnar_eval_serial_test_before_aggregate(sess, rel);
#endif
        int rc = wl_columnar_eval_serial_canonicalize_aggregate(rel,
                &rp->recursive_agg, sess->intern, sess);
        if (rc != 0)
            return rc;
        col_session_invalidate_arrangements(&sess->base, rp->name);
    }
    return 0;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_serial_test_after_plan)(wl_col_session_t *sess,
    eval_stack_t *stack, eval_entry_t *result);
void (*wl_columnar_eval_serial_test_before_delta_consolidate)(
    wl_col_session_t *sess, col_rel_t *source, col_rel_t *delta);
void (*wl_columnar_eval_serial_test_before_delta_clone)(
    wl_col_session_t *sess, col_rel_t *source);
void (*wl_columnar_eval_serial_test_before_delta_timestamps)(
    wl_col_session_t *sess, col_rel_t *source, col_rel_t *delta);
void (*wl_columnar_eval_serial_test_after_delta_stamp)(
    wl_col_session_t *sess, col_rel_t *source, col_rel_t *delta);
#endif

/* Cold bootstrap only; no live child is executed on this path. */
static int
wl_columnar_eval_serial_framed_bootstrap(const wl_plan_relation_t *rp,
    wl_col_session_t *sess,
    const wl_columnar_eval_serial_framed_host_request_t *request)
{
    /* Four192 payments reserve a distinct first256 known-empty close.
     * Every prefix ends before any frame owner; retries forfeit unused shares. */
    for (unsigned i = 0; i < 4; ++i) {
        int rc = wl_columnar_eval_serial_framed_charge(request->operation,
                request->operation_id, WL_COLUMNAR_EVAL_SERIAL_FRAMED_PAYMENT);
        if (rc) return rc;
    }
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    wl_columnar_eval_cleanup_receipt_key_t receipt_key = {0};
    int rc = wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled(
        sess, &frame, request->operation, request->operation_id,
        request->operation_identity, &receipt_key);
    if (rc) return rc;
    /* The allocator return is not a poll. Split its actual final publication
     * tail from parent prevalidation with the original saved-ID checkpoint. */
    rc = wl_columnar_eval_serial_framed_charge(request->operation,
            request->operation_id, 0);
    wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
    if (!rc) rc = wl_columnar_eval_stack_framed_context(frame, sess, &ctx);
    if (!rc && request->capture_only)
        rc = wl_columnar_eval_serial_framed_attempt_begin_bound(frame, sess,
                rp, request->operation, request->operation_id,
                request->operation_identity,
                request->controlled_materialized_batch_fallback,
                request->private_bindings, true, request->capture_out,
                request->funding, request->funding_id);
    else if (!rc)
        rc = wl_columnar_eval_serial_framed_attempt_begin(frame, sess, rp,
                request->operation, request->operation_id,
                request->operation_identity,
                request->controlled_materialized_batch_fallback,
                request->funding, request->funding_id);
    if (ctx && ctx->identity == ctx) {
        request->reply->kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_MORE;
        request->reply->cleanup_receipt_key = receipt_key;
        return rc;
    }
    /* Only this invocation's genuinely fresh, never-activated frame. Every
     * entry/continuation remains inert; final physical/credit/ref is mandatory. */
    if (wl_columnar_eval_stack_cleanup_receipt_cancel_begin(frame,
        &receipt_key)) abort();
    if (wl_columnar_eval_stack_cleanup_finish(&frame)) abort();
    return rc;
}

static int
wl_columnar_eval_serial_framed_bootstrap_receipt(
    const wl_columnar_eval_serial_framed_host_request_t *request)
{
    if (!request->previous_funding
        || request->previous_funding == request->funding
        || !request->previous_funding_id
        || wl_evaluation_control_active_attempt_id(request->previous_funding))
        return EINVAL;
    wl_evaluation_control_report_t report;
    int rc = wl_evaluation_control_report(request->previous_funding, &report);
    if (rc || report.attempt_id != request->previous_funding_id
        || report.cleanup_status || (report.execution_status
        && report.execution_status != EOVERFLOW)) return EINVAL;
    /* Exact association to rp/session/original generation and no replay is a
     * trusted serialized caller precondition; the report proves only its grant. */
    if (report.result) {
        request->reply->kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_MORE;
        return 0;
    }
    request->reply->execution_result = request->previous_protocol
        ? request->previous_protocol : EINVAL;
    request->reply->kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL;
    return 0;
}

static bool
wl_columnar_eval_serial_framed_key_equal(
    const wl_columnar_eval_cleanup_receipt_key_t *a,
    const wl_columnar_eval_cleanup_receipt_key_t *b)
{
    return a->session_generation == b->session_generation
           && a->receipt_id == b->receipt_id && a->plan_epoch == b->plan_epoch
           && a->operation_attempt_id == b->operation_attempt_id
           && a->operation_id == b->operation_id;
}

static int
wl_columnar_eval_serial_framed_receipt_action(
    wl_col_session_t *session,
    const wl_columnar_eval_serial_framed_host_request_t *request)
{
    wl_columnar_eval_serial_framed_host_reply_t *reply = request->reply;
    const wl_columnar_eval_cleanup_receipt_key_t *key = &request->receipt_key;
    if (!key->session_generation || !key->receipt_id || !key->plan_epoch
        || key->operation_attempt_id != request->operation_id
        || key->operation_id != request->operation_identity)
        return EINVAL;
    wl_columnar_eval_cleanup_receipt_t receipt;
    int rc = wl_columnar_eval_stack_cleanup_receipt_read(session, key,
            &receipt);
    if (rc) return rc;
    if (!wl_columnar_eval_serial_framed_key_equal(key, &receipt.key))
        return ENOENT;
    rc = wl_columnar_eval_serial_framed_fund(request->funding,
            request->funding_id, request->operation);
    if (rc) return rc;
    if (request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK) {
        if (receipt.state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED)
            return EBUSY;
        reply->execution_result = receipt.result;
        reply->cleanup_result = receipt.cleanup_result;
        reply->committed = receipt.committed;
        reply->cleanup_receipt_key = receipt.key;
        rc = wl_columnar_eval_stack_cleanup_receipt_ack(session, key);
        if (!rc) reply->kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL;
        return rc;
    }
    if (request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_RETRY
        || request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT) {
        if (receipt.state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_TRANSFERRED)
            rc = wl_columnar_eval_stack_cleanup_receipt_retry(session, key);
        else if (receipt.state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED)
            return EBUSY;
        /* A transferred receipt without its pending frame has no continuation
         * to report. Keep ENOENT distinct from cleanup that is still pending. */
        if (rc == ENOENT) return rc;
        if (rc && rc != ENOENT) {
            int read_rc = wl_columnar_eval_stack_cleanup_receipt_read(session,
                    key, &receipt);
            if (read_rc) return read_rc;
            reply->execution_result = receipt.result;
            reply->cleanup_result = receipt.cleanup_result;
            reply->committed = receipt.committed;
            reply->cleanup_receipt_key = receipt.key;
            reply->kind =
                WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL_CLEANUP_PENDING;
            return rc;
        }
        rc = wl_columnar_eval_stack_cleanup_receipt_read(session, key,
                &receipt);
        if (rc) return rc;
    } else if (request->action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS) {
        return EINVAL;
    }
    if (receipt.state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED
        && receipt.state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_TRANSFERRED)
        return EBUSY;
    reply->execution_result = receipt.result;
    reply->cleanup_result = receipt.cleanup_result;
    reply->committed = receipt.committed;
    reply->cleanup_receipt_key = receipt.key;
    reply->kind = receipt.state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED
        ? WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL
        : WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL_CLEANUP_PENDING;
    return 0;
}

static int
wl_columnar_eval_serial_framed_abort_step(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (!ctx->abort_requested) {
        if (!ctx->abort_supported) return ENOTSUP;
        if (!ctx->primary_result)
            ctx->primary_result = WL_EVALUATION_CONTROL_CANCELLED;
        ctx->abort_requested = true;
        wl_evaluation_control_request_cancel(ctx->control);
        (void)wl_evaluation_control_charge(ctx->control, 0);
    }
    if (ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING) return EBUSY;
    if (ctx->child_bound &&
        ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD) {
        if (ctx->child.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE)
            return wl_columnar_eval_serial_framed_attempt_step(ctx, funding,
                       funding_id);
        if (ctx->child.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING)
            return wl_columnar_eval_serial_framed_attempt_step(ctx, funding,
                       funding_id);
        if (!ctx->child.abort_requested) {
            if (ctx->child.phase < WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_READY)
                return wl_columnar_eval_serial_framed_attempt_step(ctx,
                           funding, funding_id);
            int rc = wl_columnar_eval_plan_numeric_composed_abort_request(
                &ctx->child, funding, funding_id);
            if (rc == EBUSY) return rc;
            return rc;
        }
        return wl_columnar_eval_serial_framed_attempt_step(ctx, funding,
                   funding_id);
    }
    switch (ctx->phase) {
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_LOOKUP:
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_CLAIM;
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_BEGIN:
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD:
        if (ctx->child_bound) return EBUSY;
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_CLAIM;
        return 0;
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY:
        return wl_columnar_eval_serial_framed_attempt_discard(ctx, funding,
                   funding_id);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_IMAGE:
        if (!ctx->publication.committed
            && ctx->publication.phase <
            WL_COLUMNAR_SESSION_HASH_REGISTRY_REVERSE) {
            ctx->publication.phase = WL_COLUMNAR_SESSION_HASH_REGISTRY_REVERSE;
            ctx->publication.move = 4;
        }
        return wl_columnar_eval_serial_framed_attempt_step(ctx, funding,
                   funding_id);
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_VALIDATE:
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_BATCH_PREPARE:
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SOURCE_INIT:
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SOURCE_HOLD:
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_NEW:
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SCRATCH:
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_BATCH_BEGIN:
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_GATHER:
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_APPEND:
    case WL_COLUMNAR_EVAL_SERIAL_FRAMED_SORTED:
        ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_BATCH_END;
        return 0;
    default:
        /* Cleanup phases own their exact cursor/receipt; resume one cell. */
        return wl_columnar_eval_serial_framed_attempt_step(ctx, funding,
                   funding_id);
    }
}

int
wl_columnar_eval_serial_framed_relation(const wl_plan_relation_t *rp,
    wl_col_session_t *sess, bool recursive,
    const wl_columnar_eval_serial_framed_host_request_t *request)
{
    if (request && request->reply) {
        request->reply->kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_REJECTED;
        request->reply->execution_result = EINVAL;
        request->reply->cleanup_result = 0;
        request->reply->committed = false;
        memset(&request->reply->cleanup_receipt_key, 0,
            sizeof(request->reply->cleanup_receipt_key));
        request->reply->status.phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_INERT;
        request->reply->status.execution_result = 0;
        request->reply->status.cleanup_result = 0;
        request->reply->status.ready = request->reply->status.retained =
            request->reply->status.committed = false;
    }
    if (!rp || !sess) return EINVAL;
    if (sess->base.evaluation_control) {
        if (!request) return ENOTSUP;
        if (!request->reply) return EINVAL;
        wl_columnar_eval_serial_framed_host_reply_t *reply = request->reply;
        if (request->operation != sess->base.evaluation_control
            || !request->operation_id || !request->operation_identity
            || (request->capture_only
            != (request->private_bindings != NULL))
            || (request->capture_only
            != (request->capture_out != NULL))
            || (request->action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_ADVANCE
            && request->action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS
            && request->action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT
            && request->action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_RETRY
            && request->action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK))
            return EINVAL;
        if (recursive) return ENOTSUP;
        int protocol = 0;
        if (sess->cleanup_active) {
            wl_columnar_eval_serial_framed_attempt_t *ctx = NULL;
            protocol = wl_columnar_eval_stack_framed_context(
                sess->cleanup_active, sess, &ctx);
            if (protocol) return protocol;
            if (!wl_columnar_eval_serial_framed_owner_valid(ctx,
                sess->cleanup_active)) return EBUSY;
            if (ctx->plan != rp || ctx->session != sess
                || ctx->control != request->operation
                || ctx->attempt_id != request->operation_id
                || ctx->operation_identity != request->operation_identity
                || ctx->controlled_materialized_batch_fallback
                != request->controlled_materialized_batch_fallback
                || ctx->private_bindings != request->private_bindings
                || ctx->capture_only != request->capture_only
                || ctx->capture_out != request->capture_out)
                return EINVAL;
            if (request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT
                && !ctx->abort_supported) return ENOTSUP;
            if (request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_RETRY
                || request->action ==
                WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK)
                return EBUSY;
            protocol = wl_columnar_eval_serial_framed_fund(request->funding,
                    request->funding_id, request->operation);
            if (protocol) return protocol;
            reply->cleanup_receipt_key = ctx->cleanup_receipt_key;
            /* Original may be stopped or turned over: its authenticated old
             * parent still owns cleanup. No fresh absence lookup is allowed. */
            if (request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS) {
                reply->kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_STATUS;
                return wl_columnar_eval_serial_framed_attempt_status(ctx,
                           &reply->status, request->funding,
                           request->funding_id);
            }
            if (ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING) {
                if (!request->previous_valid
                    || request->previous_kind !=
                    WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_MORE
                    || request->previous_funding != ctx->pending_funding
                    || request->previous_funding_id != ctx->pending_funding_id)
                    return EINVAL;
                reply->kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_MORE;
                protocol = wl_columnar_eval_serial_framed_consume_result(ctx,
                        request->previous_protocol, request->funding,
                        request->funding_id);
                if (!protocol && request->action ==
                    WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT) {
                    ctx->abort_requested = true;
                    if (!ctx->primary_result)
                        ctx->primary_result =
                            WL_EVALUATION_CONTROL_CANCELLED;
                    wl_evaluation_control_request_cancel(ctx->control);
                    (void)wl_evaluation_control_charge(ctx->control, 0);
                }
                return protocol;
            }
            reply->kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_MORE;
            if (request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT
                || ctx->abort_requested) {
                protocol = wl_columnar_eval_serial_framed_abort_step(ctx,
                        request->funding, request->funding_id);
                if (protocol || ctx->phase !=
                    WL_COLUMNAR_EVAL_SERIAL_FRAMED_DONE) return protocol;
            }
            if (ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY) {
                if (ctx->capture_only) {
                    if (ctx->capture_done || !request->capture_out
                        || request->capture_out != ctx->capture_out
                        || *request->capture_out || !ctx->candidate
                        || !ctx->candidate_owned)
                        return EINVAL;
                    *request->capture_out = ctx->candidate;
                    ctx->candidate = NULL;
                    ctx->candidate_owned = false;
                    ctx->capture_done = true;
                    return wl_columnar_eval_serial_framed_attempt_discard(
                        ctx, request->funding, request->funding_id);
                } else {
                    return wl_columnar_eval_serial_framed_attempt_publish(ctx,
                               request->funding, request->funding_id);
                }
            }
            if (ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_DONE) {
                protocol = wl_columnar_eval_serial_framed_attempt_close(ctx,
                        &reply->execution_result, &reply->committed,
                        request->funding, request->funding_id);
                if (!protocol) {
                    reply->kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL;
                } else {
                    wl_columnar_eval_cleanup_receipt_t receipt;
                    int receipt_rc =
                        wl_columnar_eval_stack_cleanup_receipt_read(sess,
                            &reply->cleanup_receipt_key, &receipt);
                    if (receipt_rc) return receipt_rc;
                    reply->execution_result = receipt.result;
                    reply->committed = receipt.committed;
                    reply->cleanup_result = receipt.cleanup_result;
                    if (receipt.state ==
                        WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_PREPARED) {
                        reply->kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_MORE;
                    } else {
                        reply->kind =
                            WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL_CLEANUP_PENDING;
                        return 0;
                    }
                }
                return protocol;
            }
            return wl_columnar_eval_serial_framed_attempt_step(ctx,
                       request->funding, request->funding_id);
        }
        if (request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS
            || request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT
            || request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_RETRY
            || request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK) {
            if (!request->receipt_key.receipt_id) return ENOENT;
            return wl_columnar_eval_serial_framed_receipt_action(sess, request);
        }
        protocol = wl_columnar_eval_serial_framed_fund(request->funding,
                request->funding_id, request->operation);
        if (protocol) return protocol;
        if (sess->cleanup_pending) return EBUSY;
        if (request->previous_valid && request->previous_kind ==
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_BOOTSTRAP_RECEIPT)
            return wl_columnar_eval_serial_framed_bootstrap_receipt(request);
        if (wl_evaluation_control_active_attempt_id(request->operation)
            != request->operation_id) return EINVAL;
        reply->kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_BOOTSTRAP_RECEIPT;
        if (sess->current_iteration
            || sess->retraction_seeded || sess->retraction_right_pass
            || sess->tdd_input_run || sess->tdd_subpass_active
            || sess->tdd_outbound_only_active || sess->diff_operators_active
            || (sess->join_batch_bytes
            && !request->controlled_materialized_batch_fallback)
            || sess->join_output_shared_count
            || sess->coordinator || sess->num_workers > 1)
            return ENOTSUP;
        if (!rp->name || !rp->ops || !rp->op_count) return EINVAL;
        if (request->capture_only
            ? !wl_columnar_eval_plan_numeric_recursive_path_supported(rp)
            : rp->ops[rp->op_count - 1].op != WL_PLAN_OP_CONSOLIDATE)
            return ENOTSUP;
        if (request->controlled_materialized_batch_fallback
            && (!sess->join_batch_bytes || sess->join_batch_strict
            || sess->join_output_limit
            || !wl_columnar_session_controlled_batch_authorized(sess,
            request->operation, request->operation_id,
            request->operation_identity)
            || rp->op_count != 5 || rp->ops[0].op != WL_PLAN_OP_VARIABLE
            || rp->ops[1].op != WL_PLAN_OP_JOIN
            || rp->ops[2].op != WL_PLAN_OP_FILTER
            || rp->ops[3].op != WL_PLAN_OP_MAP
            || rp->ops[4].op != WL_PLAN_OP_CONSOLIDATE)) return ENOTSUP;
        return wl_columnar_eval_serial_framed_bootstrap(rp, sess, request);
    }
    if (request) return EINVAL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    int rc = wl_columnar_eval_stack_cleanup_begin(sess, &frame);
    if (rc != 0)
        return rc;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    eval_entry_t *result = wl_columnar_eval_stack_cleanup_result(frame);
    rc = col_eval_relation_plan(rp, stack, sess);
    if (rc == 0 && stack->top != 0)
        *result = eval_stack_pop(stack);
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_serial_test_after_plan)
        wl_columnar_eval_serial_test_after_plan(sess, stack, result);
#endif
    if (rc != 0)
        goto done;
    if (result->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION) {
        rc = ENOTSUP;
        goto done;
    }
    rc = eval_stack_drain(stack);
    if (rc != 0 || !result->rel || (recursive && result->rel->nrows == 0))
        goto done;
    col_rel_t *target = session_find_rel(sess, rp->name);
    if (target) {
        if (result->owned) {
            /* Dispose the externally observable result before publication.
            * Otherwise a refused destroy after append duplicates rows on
            * an initial snapshot retry. The private copy never escapes. */
            col_rel_t *copy = NULL;
            rc = wl_columnar_relation_new_like_governed_checked(&copy,
                    "$publish", result->rel, sess->memory_governor);
            if (rc != 0) {
                if (rc == ENOSPC)
                    sess->memory_budget_denied = true;
                goto done;
            }
            rc = eval_stack_push(stack, copy, true);
            if (rc != 0) {
                col_rel_destroy(copy);
                goto done;
            }
            rc = col_rel_append_all(copy, result->rel, NULL);
            if (rc == 0)
                rc = eval_entry_dispose(result);
            if (rc != 0)
                goto done;
            *result = eval_stack_pop(stack);
        }
        if (recursive && target->ncols == 0 && result->rel->ncols > 0) {
            rc = col_rel_set_schema(target, result->rel->ncols,
                    (const char *const *)result->rel->col_names);
            if (rc != 0)
                goto done;
        }
        rc = col_rel_append_all(target, result->rel, sess->eval_arena);
        goto done;
    }
    if (result->owned) {
        rc = wl_columnar_relation_rename_checked(result->rel, rp->name);
        if (rc != 0)
            goto done;
    } else {
        col_rel_t *source = result->rel;
        wl_columnar_source_access_reader_t reader = { 0 };
        rc = col_rel_source_reader_acquire(source, &reader);
        if (rc != 0)
            goto done;
        wl_columnar_memory_governor_ref_t *governor = sess->memory_governor
            ? sess->memory_governor : source->memory_governor;
        col_rel_t *copy = NULL;
        rc = wl_columnar_relation_pool_new_like_governed_checked(&copy,
                sess->delta_pool, rp->name, source, governor);
        if (rc != 0) {
            if (rc == ENOSPC)
                sess->memory_budget_denied = true;
        } else {
            result->rel = copy;
            result->owned = true;
            rc = col_rel_append_all(copy, source, sess->eval_arena);
        }
        int release_rc = col_rel_source_reader_release(&reader);
        if (rc == 0)
            rc = release_rc;
        if (rc != 0)
            goto done;

    }
    rc = session_add_rel(sess, result->rel);
    if (rc == 0) {
        /* Pool promotion may have retired the original descriptor. */
        result->rel = NULL;
        result->owned = false;
    }
done:;
    int cleanup_rc = wl_columnar_eval_stack_cleanup_finish(&frame);
    return cleanup_rc != 0 ? cleanup_rc : rc;
}

/* Issue #2100: a rule head that also holds input -- inline facts or host
 * rows, which its seed shadow records -- keeps that input beside what its
 * rules derive, so it can repeat a derived row, and an input row inserted
 * more than once is there once per insert.  Rules and snapshots must see
 * such a row once, as they see a derived row, so sort and deduplicate each
 * such head of @sp, as the multi-worker non-recursive path's finalization
 * does for every head.  The seed shadow keeps the copies, so a removal still
 * takes out one insert at a time. */
int
wl_columnar_eval_consolidate_input_heads(const wl_plan_stratum_t *sp,
    wl_col_session_t *sess)
{
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        const char *name = sp->relations[ri].name;
        col_rel_t *seed = wl_columnar_session_seed_rows(sess, name);
        col_rel_t *head = session_find_rel(sess, name);
        if (!seed || seed->nrows == 0 || !head || head->nrows <= 1)
            continue;
        int rc = wl_columnar_eval_delta_consolidate(head, sess);
        if (rc != 0)
            return rc;
        col_session_invalidate_arrangements(&sess->base, name);
    }
    return 0;
}

/*
 * col_eval_stratum:
 * Evaluate one stratum, writing results into session relations.
 * Non-recursive strata are evaluated once.
 * Recursive strata use semi-naive fixed-point iteration.
 *
 * Returns 0 on success, non-zero on error.
 */
int
col_eval_stratum(const wl_plan_stratum_t *sp, wl_col_session_t *sess,
    uint32_t stratum_idx)
{
    if (!sp || !sess) return EINVAL;
    if (sess->base.evaluation_control) return ENOTSUP;
    /* Cleared here so every exit below reports "heads not final" unless one
     * of the two completion points is reached. */
    sess->eval_stratum_heads_final = false;
    if (sess->cleanup_active)
        return EBUSY;
    int readiness_rc = wl_columnar_session_cleanup_ready(sess);
    if (readiness_rc != 0)
        return readiness_rc;

    if (!sp->is_recursive) {
        /* Issue #914: a non-recursive stratum is always the base case. The
         * recursive-stratum loop leaves sess->current_iteration > 0 and never
         * resets it, which would wrongly trigger the base-case EDB VARIABLE
         * skip (eval.c base-case skip) and drop this stratum's head tuples.
         * Reset here so it covers both the parallel and sequential paths. */
        sess->current_iteration = 0;

        /* Non-recursive: evaluate each relation plan once */
        for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
            const wl_plan_relation_t *rp = &sp->relations[ri];
            int par_rc = wl_columnar_eval_nonrec_relation_parallel(rp, sess);
            if (par_rc == 0)
                continue;
            if (par_rc != EAGAIN) {
                if (getenv("WIRELOG_DEBUG_NONREC_TDD"))
                    fprintf(stderr, "nonrec_tdd failed rel=%s rc=%d\n",
                        rp->name ? rp->name : "(null)", par_rc);
                return par_rc;
            }

            int rc = wl_columnar_eval_serial_framed_relation(rp, sess,
                    false, NULL);
            if (rc != 0)
                return rc;
        }
        /* Issue #2100: this stratum's rows landed on top of the input. */
        int input_rc = wl_columnar_eval_consolidate_input_heads(sp, sess);
        if (input_rc != 0)
            return input_rc;
        /* Completion point for the non-recursive branch: the rule loop and
         * the input-head consolidation above are the last things that can
         * fail, so every head is final from here.  This must stay below them
         * -- setting it where the branch opens would report true on the
         * failures that return from them, and the caller would skip a
         * restore the heads still need.  Move this with the completion
         * boundary if either ever grows a new exit. */
        sess->eval_stratum_heads_final = true;
        col_mat_cache_release_pins(&sess->mat_cache);
        assert(sess->mat_cache.active_pins == 0);
        col_mat_cache_clear(&sess->mat_cache);
        assert(sess->mat_cache.active_pins == 0);
        col_session_mem_sample(sess); /* Issue #1380 */
        delta_pool_reset(sess->delta_pool);
        sess->rotation_ops->rotate_eval_arena(sess);

        /* Non-recursive stratum frontier: record convergence epoch and iteration.
        * Non-recursive strata always converge at iteration UINT32_MAX (no loop),
        * so store (outer_epoch, UINT32_MAX) to enable epoch-aware skip on next
        * incremental call. Always update both fields so same-epoch skip logic
        * fires correctly when the frontier persists across session_step calls. */
        sess->frontier_ops->record_stratum_convergence(sess, stratum_idx,
            sess->outer_epoch, UINT32_MAX);

        /* Issue #317: Report non-recursive convergence to coordinator's
         * progress tracker.  Non-recursive strata always converge at
         * UINT32_MAX (no iteration loop), matching the sentinel convention. */
        if (sess->coordinator) {
            wl_frontier_progress_record(&sess->coordinator->progress,
                sess->worker_id, stratum_idx, sess->outer_epoch, UINT32_MAX);
        }

        /* Non-recursive rule frontiers: mark each rule fully evaluated.
         * UINT32_MAX sentinel matches stratum frontier convention. */
        if (sess->plan) {
            uint32_t rule_base = 0;
            for (uint32_t si = 0; si < stratum_idx; si++)
                rule_base += sess->plan->strata[si].relation_count;
            for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
                uint32_t rule_idx = rule_base + ri;
                if (rule_idx < MAX_RULES) {
                    /* Issue #106: Conservative approach - always reset rule frontiers
                     * for affected strata to (current_epoch, UINT32_MAX) sentinel.
                     * UINT32_MAX prevents premature skip during re-evaluation. */
                    sess->frontier_ops->reset_rule_frontier(sess, rule_idx,
                        sess->outer_epoch);
                }
            }
        }

        return 0;
    }

    /* Issue #2100: the loop below merges each iteration's rows into the
     * head's sorted prefix and drops the ones already there, so a head that
     * enters it deduplicated leaves it deduplicated. */
    int input_rc = wl_columnar_eval_consolidate_input_heads(sp, sess);
    if (input_rc != 0)
        return input_rc;

    /*
     * Recursive stratum: semi-naive fixed-point iteration.
     * Iterate until no new tuples are produced.
     *
     * Pre-register empty IDB relations so that VARIABLE ops can find
     * them on the first iteration (before any tuples are produced).
     */
    for (uint32_t ri = 0; ri < sp->relation_count; ri++) {
        col_rel_t *existing = session_find_rel(sess, sp->relations[ri].name);
        if (!existing) {
            col_rel_t *empty = NULL;
            int rc = col_rel_alloc(&empty, sp->relations[ri].name);
            if (rc != 0)
                return ENOMEM;
            rc = session_add_rel(sess, empty);
            if (rc != 0) {
                col_rel_destroy(empty);
                return rc;
            }
        }
    }

    /*
     * Semi-naive fixed-point iteration with delta tracking.
     * VARIABLE ops prefer "$d$relname" delta relations (rows added in the
     * previous iteration). JOIN right-side lookups always use the full
     * relation by name, giving delta (left) x full (right) join semantics.
     */
    uint32_t nrels = sp->relation_count;
    uint64_t delta_scratch_bytes = 0;
    uint64_t bytes = 0;
    if (!wl_columnar_memory_size_mul(nrels, sizeof(col_rel_t *), &bytes)
        || !wl_columnar_memory_size_add(delta_scratch_bytes, bytes,
        &delta_scratch_bytes)
        || !wl_columnar_memory_size_mul(nrels, sizeof(uint32_t), &bytes)
        || !wl_columnar_memory_size_add(delta_scratch_bytes, bytes,
        &delta_scratch_bytes))
        return EOVERFLOW;
    wl_columnar_memory_reservation_t delta_reservation;
    wl_columnar_memory_reservation_init(&delta_reservation);
    if (sess->memory_governor) {
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(sess->memory_governor),
                delta_scratch_bytes, &delta_reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                sess->memory_budget_denied = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? EOVERFLOW
                : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                ? ENOSPC : ENOMEM;
        }
    }
    col_rel_t **delta_rels = (col_rel_t **)calloc(nrels, sizeof(col_rel_t *));
    if (!delta_rels) {
        wl_columnar_memory_release(&delta_reservation);
        return ENOMEM;
    }

    /* Allocate snap once before the iteration loop (Issue #367): hoisting
     * the allocation out of the outer loop eliminates 2 malloc/free calls
     * per iteration (14K+ iterations for CRDT). */
    uint32_t *snap = (uint32_t *)malloc(nrels * sizeof(uint32_t));
    if (!snap) {
        free((void *)delta_rels);
        wl_columnar_memory_release(&delta_reservation);
        return ENOMEM;
    }

    /* Sort pre-existing data in each IDB relation before iterating.
     * Handles the EDB+IDB case: when base facts are pre-loaded into a
     * relation that also appears as an IDB in a recursive rule, the loaded
     * facts may be in insertion order (unsorted).
     * col_op_consolidate_incremental_delta requires rel->data[0..snap) to be
     * sorted; an unsorted prefix causes the 2-pointer merge to miss duplicates,
     * producing spurious output rows. */
    for (uint32_t ri = 0; ri < nrels; ri++) {
        col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
        if (r && r->nrows > 1) {
            /* The comment above states the precondition this establishes.
             * A refused sort leaves the prefix unsorted, so continuing would
             * produce exactly the spurious rows it warns about. */
            int sort_rc = col_rel_radix_sort_int64(r);
            if (sort_rc != 0) {
                free((void *)delta_rels);
                free(snap);
                wl_columnar_memory_release(&delta_reservation);
                return sort_rc;
            }
        }
    }

    /* Initialize recursive stratum frontier to UINT32_MAX (not set sentinel)
     * only on the first evaluation (frontier==0 from calloc). If a prior
     * col_session_insert_incremental call preserved a real convergence frontier,
     * keep it so the per-iteration skip condition fires for iterations beyond
     * the prior convergence point. */
    sess->frontier_ops->init_stratum(sess, stratum_idx);

    /* Phase 4 (US-4-004): Compute the base global rule index for this stratum.
     * Rule indices are assigned by enumerating strata in order and relations
     * within each stratum, matching col_compute_affected_rules convention. */
    uint32_t rule_id_base = 0;
    if (sess->plan) {
        for (uint32_t si = 0;
            si < stratum_idx && si < sess->plan->stratum_count; si++) {
            rule_id_base += sess->plan->strata[si].relation_count;
        }
    }
    /* Clamp so rule_id_base + ri never exceeds MAX_RULES - 1 */
    if (rule_id_base >= MAX_RULES)
        rule_id_base = MAX_RULES;

    uint32_t iter;
    col_frontier_t strat_frontier = { 0, 0 };
    uint32_t final_eff_iter = 0; /* effective sub-pass index at convergence */

    /* Issue #282: Save the session-level diff_operators_active so we can
     * restore it after the recursive stratum evaluation completes.
     * Within the iteration loop, we override it per sub-pass. */
    bool saved_diff_operators_active = sess->diff_operators_active;

    /*
     * Stride-based semi-naive iteration (Issue #237).
     * Each outer iteration runs EVAL_STRIDE sub-passes, chaining the delta
     * from each sub-pass into the next.  Effective recursion depth covered:
     *   MAX_ITERATIONS * EVAL_STRIDE = 4096 * 8 = 32768
     * This allows deep-chain workloads (e.g. CRDT) to converge where they
     * previously hit the MAX_ITERATIONS cap and produced incomplete results.
     *
     * When EVAL_STRIDE == 1 the behaviour is identical to the prior single-
     * pass loop.
     */
    for (iter = 0; iter < MAX_ITERATIONS; iter++) {
        bool outer_any_new = false; /* any sub-pass produced new tuples  */
        int outer_rc = 0;           /* error propagated from inner loop  */
        bool publication_failed = false;
        bool stride_all_skipped
            = true; /* true until a sub-pass actually runs */
        bool outer_continue_next
            = false;            /* net-zero/all-empty: retry next outer */
        bool converged = false; /* fixed point reached                */

        /* snap[] is filled at the start of each sub-pass (nrows grows as new
         * tuples are appended).  The array is allocated once before this loop
         * (Issue #367) to avoid per-iteration malloc/free overhead. */

        /* ------------------------------------------------------------------ */
        /* Inner sub-pass loop: EVAL_STRIDE semi-naive passes per outer iter.  */
        /* ------------------------------------------------------------------ */
        for (uint32_t sub = 0; sub < EVAL_STRIDE; sub++) {
            uint32_t eff_iter = iter * EVAL_STRIDE + sub;

            /* Publish effective iteration so operators can distinguish base case
             * (eff_iter 0: FORCE_DELTA falls back to full) from delta case
             * (eff_iter > 0: FORCE_DELTA with absent delta → empty result). */
            sess->current_iteration = eff_iter;

            /* Issue #282: Enable differential operators for recursive iterations
             * > 0.  At eff_iter 0, we seed from full EDB relations (base case)
             * so arrangement reuse is not applicable.  From eff_iter 1 onward,
             * only the delta is processed (semi-naive), so col_op_join_diff can
             * build the hash table incrementally: O(D) instead of O(N).
             * diff_arr_entries persist across sub-passes and are invalidated by
             * col_session_invalidate_arrangements when the underlying relation
             * changes (called after consolidation each sub-pass).
             * The saved value is restored after the recursive stratum completes
             * so non-recursive strata and the outer session logic are unaffected. */
            sess->diff_operators_active
                = sess->diff_enabled && eff_iter > 0;

            /* Iteration skip based on frontier (convergence point of prior
             * evaluation).  Skip sub-pass when both hold:
             * 1. Same outer_epoch — frontier is valid for this insertion epoch
             * 2. eff_iter > frontier.iteration — already processed in this epoch
             * Once eff_iter exceeds the frontier for sub=0, it also exceeds for
             * all higher sub values, so the entire outer iteration effectively
             * continues to the next one. */
            if (sess->frontier_ops->should_skip_iteration(sess, stratum_idx,
                eff_iter)) {
                continue; /* skip this sub-pass */
            }

            stride_all_skipped = false; /* at least one sub-pass runs */

            /* Clear per-sub-pass delta arrangement cache (sequential eval path).
             * K-fusion workers manage their own darr caches independently. */
            col_session_free_delta_arrangements(sess);

            /* Register delta relations from previous sub-pass into session */
            for (uint32_t ri = 0; ri < nrels; ri++) {
                if (!delta_rels[ri])
                    continue;
                int rc = session_add_rel(sess, delta_rels[ri]);
                if (rc != 0) {
                    /* This delta is still private. Preserve its local owner
                     * for stride_error cleanup; replacement failure leaves the
                     * previous registered delta intact. */
                    outer_rc = rc;
                    publication_failed = true;
                    goto stride_error;
                }
                delta_rels[ri] = NULL; /* session now owns it */
            }

            /* Record per-relation row counts (O(1) snapshot — no data copy).
             * Re-filled each sub-pass because nrows changes after each pass. */
            for (uint32_t ri = 0; ri < nrels; ri++) {
                col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
                snap[ri] = r ? r->nrows : 0; /* O(1) count only, no copy */
            }

            /* Phase 3D: Frontier skip with multiplicities (US-3D-002).
             * Skip sub-pass if all delta relations have zero net multiplicity.
             * When detected, retry with the next outer iteration (same as the
             * prior single-pass behaviour). */
            if (eff_iter
                > 0) { /* only skip from effective iteration 1 onward */
                bool all_deltas_net_zero = true;
                for (uint32_t ri = 0; ri < nrels; ri++) {
                    const char *dname = sp->relations[ri].delta_name;
                    col_rel_t *delta = session_find_rel(sess, dname);
                    if (!delta || delta->nrows == 0) {
                        /* Empty delta: net multiplicity is zero */
                        continue;
                    }
                    /* Compute net multiplicity for this delta relation */
                    int64_t net_mult = 0;
                    for (uint32_t row = 0; row < delta->nrows; row++) {
                        net_mult += delta->timestamps[row].multiplicity;
                    }
                    if (net_mult != 0) {
                        all_deltas_net_zero = false;
                        break;
                    }
                }
                if (all_deltas_net_zero) {
                    /* All deltas zero net-multiplicity: skip to next outer iter */
                    outer_continue_next = true;
                    break;
                }
            }

            /* Stratum-level early exit: if all rules have empty forced deltas,
             * the sub-pass will produce no new facts. (Issue #81) */
            if (eff_iter > 0) {
                bool all_rules_empty = true;
                for (uint32_t ri = 0; ri < nrels; ri++) {
                    if (!has_empty_forced_delta(&sp->relations[ri], sess,
                        eff_iter)) {
                        all_rules_empty = false;
                        break;
                    }
                }
                if (all_rules_empty) {
                    outer_continue_next = true;
                    break;
                }
            }

            /* Single-pass semi-naive evaluation. VARIABLE prefers delta when it
             * is a strict subset of full (genuine new facts). JOIN propagates
             * the is_delta flag through results and applies right-delta when
             * left is full and a strictly-smaller right delta exists. */
            for (uint32_t ri = 0; ri < nrels; ri++) {
                const wl_plan_relation_t *rp = &sp->relations[ri];

                /* Issue #106 (US-106-003): Rule-level frontier skip with epoch
                 * gating.  Skip rule evaluation only when BOTH hold:
                 * 1. Same outer_epoch (prevents cross-epoch incorrect skips)
                 * 2. eff_iter > convergence point (already processed in epoch)
                 * Across epoch boundaries mismatch => always re-eval. */
                uint32_t rule_id = rule_id_base + ri;
                if (sess->frontier_ops->should_skip_rule(sess, rule_id,
                    eff_iter)) {
                    continue;
                }

                /* Pre-scan skip: if a FORCE_DELTA op references an empty or
                 * absent delta (eff_iter > 0), the plan produces 0 rows. */
                if (has_empty_forced_delta(rp, sess, eff_iter)) {
                    continue;
                }

                /* Previous-pass publication transferred every private
                 * delta before a rule may retain cleanup dependencies. */
                for (uint32_t di = 0; di < nrels; di++)
                    assert(delta_rels[di] == NULL);
                int rc = wl_columnar_eval_serial_framed_relation(rp, sess,
                        true, NULL);
                if (rc != 0) {
                    outer_rc = rc;
                    goto stride_error;
                }
            }

            /* Remove delta relations from session (evaluation is complete).
             * A successful removal is the normal case: every sub-pass after
             * the first removes the delta its predecessor published.  ENOENT
             * arises whenever a relation published no delta -- the first
             * sub-pass, a relation with no new rows, one absent from the
             * registry, or a delta that came back empty and was destroyed
             * unpublished.  A refusal keeps the registry owner, so nothing is
             * lost; it surfaces one sub-pass later if that relation publishes
             * again, and otherwise falls through to the terminal removal at
             * the end of the stratum, which does propagate.
             *
             * It is deliberately not propagated from here.  The terminal
             * removal is enough for the contract: every other exit from
             * col_eval_stratum already returns non-zero -- the two entry
             * guards, the recursive setup failures, the cleanup_pending
             * branch, stride_error, and the aggregate failure -- and the
             * non-recursive branch removes no delta at all, so the only
             * success exit that could report success with a reader-held
             * delta still registered is the terminal one.  That closes the
             * contract, though not the state: a refusal here followed by an
             * unrelated later stride_error still returns non-zero with the
             * delta registered.  What handles that depends on the caller:
             * col_session_snapshot sweeps every $d$ relation on its error
             * path, while col_session_step has no such sweep and leaves it
             * to the session's lease release at teardown.  The sweep is also
             * skipped when the session holds retained cleanup, which is the
             * case likeliest to have caused the refusal in the first place,
             * so do not read it as a general backstop.
             *
             * Propagating from here would also suppress work rather than
             * merely report it.  Between this refusal and the same-name
             * registration that surfaces it, a whole sub-pass runs and any
             * rows it derives are appended to head relations; restore_flat
             * declines a populated head, so no rollback undoes them.  The rc
             * and the completion flag would be identical either way, so the
             * only difference is head content this unit is not required to
             * change.  See #1661. */
            for (uint32_t ri = 0; ri < nrels; ri++) {
                const char *dname = sp->relations[ri].delta_name;
                (void)session_remove_rel(sess, dname);
            }

            /* Phase 4: Frontier is computed incrementally as deltas are created
             * because delta_rels[ri] may be set to NULL in the next sub-pass's
             * registration loop.  strat_frontier declared before outer loop. */

            /* Consolidate all IDB relations to remove duplicates and compute
             * delta as a byproduct.  snap[ri] marks the boundary between the
             * already-sorted prefix and unsorted new rows appended this pass. */
            bool any_new = false;
            for (uint32_t ri = 0; ri < nrels; ri++) {
                col_rel_t *r = session_find_rel(sess, sp->relations[ri].name);
                if (!r || snap[ri] >= r->nrows) {
                    continue; /* no new rows for this relation */
                }

                const char *dname = sp->relations[ri].delta_name;
                wl_columnar_memory_governor_ref_t *governor =
                    sess->memory_governor ? sess->memory_governor
                                          : r->memory_governor;
                wl_columnar_source_access_reader_t reader = { 0 };
                int reader_rc = col_rel_source_reader_acquire(r, &reader);
                if (reader_rc != 0) {
                    outer_rc = reader_rc;
                    goto stride_error;
                }
#ifdef WL_SESSION_TEST_HOOKS
                if (wl_columnar_eval_serial_test_before_delta_clone)
                    wl_columnar_eval_serial_test_before_delta_clone(sess, r);
#endif
                col_rel_t *delta = NULL;
                int clone_rc =
                    wl_columnar_relation_pool_new_like_governed_checked(
                    &delta, sess->delta_pool, dname, r, governor);
                int reader_release_rc =
                    col_rel_source_reader_release(&reader);
                if (reader_release_rc != 0 && delta) {
                    col_rel_destroy(delta);
                    delta = NULL;
                }
                if (clone_rc != 0 || !delta) {
                    outer_rc = reader_release_rc != 0 ? reader_release_rc
                                                      : clone_rc;
                    if (outer_rc == ENOSPC)
                        sess->memory_budget_denied = true;
                    goto stride_error;
                }

                /* Nullary pool clones have no initial row capacity, so make
                 * one timestamp slot admissible before consolidation can emit
                 * their first tuple. */
                if (delta->capacity == 0) {
                    int reserve_rc = col_rel_reserve_capacity_admitted(delta,
                            COL_REL_INIT_CAP, NULL);
                    if (reserve_rc != 0) {
                        col_rel_destroy(delta);
                        outer_rc = reserve_rc;
                        goto stride_error;
                    }
                }

                /* Reserve provenance storage before consolidation mutates the
                 * source.  This makes timestamp admission part of the same
                 * pre-mutation allocation contract as the delta rows. */
#ifdef WL_SESSION_TEST_HOOKS
                if (wl_columnar_eval_serial_test_before_delta_timestamps)
                    wl_columnar_eval_serial_test_before_delta_timestamps(
                        sess, r, delta);
#endif
                int timestamp_rc = col_rel_enable_timestamps(delta);
                if (timestamp_rc != 0) {
                    col_rel_destroy(delta);
                    outer_rc = timestamp_rc;
                    goto stride_error;
                }
#ifdef WL_SESSION_TEST_HOOKS
                if (wl_columnar_eval_serial_test_before_delta_consolidate)
                    wl_columnar_eval_serial_test_before_delta_consolidate(
                        sess, r, delta);
#endif

                /* Consolidate WITH delta output (no separate merge walk) */
                uint32_t cons_old = snap[ri];
                uint32_t cons_new = r->nrows - cons_old; /* delta count D */
                int fast_flag = 0;
                uint64_t cons_t0 = now_ns();
                int rc2 = col_op_consolidate_incremental_delta(r, snap[ri],
                        delta, &fast_flag);
                uint64_t cons_elapsed = now_ns() - cons_t0;
                if (rc2 != 0) {
                    /* The source may have been sorted before a later
                     * admission error; invalidate cached arrangements too. */
                    col_session_invalidate_arrangements(&sess->base,
                        sp->relations[ri].name);
                    col_rel_destroy(delta);
                    outer_rc = rc2;
                    goto stride_error;
                }

                sess->consolidation_ns += cons_elapsed;
                sess->consolidate_fast_hits += (uint64_t)fast_flag;
                sess->consolidate_slow_hits += (uint64_t)(1 - fast_flag);
                /* Invalidate arrangements for this relation only after the
                 * consolidation transaction has committed. */
                col_session_invalidate_arrangements(&sess->base,
                    sp->relations[ri].name);
                /* Per-call trace: WL_LOG=CONSOLIDATION:5 (or legacy
                 * WL_CONSOLIDATION_LOG presence shim). */
                WL_LOG(WL_LOG_SEC_CONSOLIDATION, WL_LOG_DEBUG,
                    "eff_iter=%u stratum=%u rel=%s N=%u D=%u "
                    "time_us=%.1f ratio=%.4f",
                    eff_iter, stratum_idx, sp->relations[ri].name,
                    cons_old, cons_new, (double)cons_elapsed / 1000.0,
                    cons_old > 0 ? (double)cons_new / (double)cons_old
                                     : 0.0);

                if (delta->nrows > 0) {
                    /* Stamp each new row with its provenance (eff_iter, stratum).
                     * worker=0 indicates the sequential (non-K-fusion) path.
                     *
                     * The array was allocated and admitted above by
                     * col_rel_enable_timestamps(); reallocating it here would
                     * leak that one and leave timestamp_capacity describing
                     * storage the governor never reserved.  It is calloc'd to
                     * capacity >= nrows, so the stamping loop can write
                     * straight into it. */
                    wl_columnar_relation_touch_storage(delta);
                    for (uint32_t ti = 0; ti < delta->nrows; ti++) {
                        delta->timestamps[ti].iteration = eff_iter;
                        delta->timestamps[ti].stratum = stratum_idx;
                        /* worker left zero: sequential evaluation path */
                        delta->timestamps[ti].multiplicity = 1;
                    }
#ifdef WL_SESSION_TEST_HOOKS
                    if (wl_columnar_eval_serial_test_after_delta_stamp)
                        wl_columnar_eval_serial_test_after_delta_stamp(
                            sess, r, delta);
#endif

                    /* Phase 4: Enable timestamp tracking on target relation to
                     * preserve provenance through consolidation.  This enables
                     * frontier computation to determine convergence. */
                    timestamp_rc = col_rel_enable_timestamps(r);
                    if (timestamp_rc != 0) {
                        col_rel_destroy(delta);
                        outer_rc = timestamp_rc;
                        goto stride_error;
                    }

                    delta_rels[ri] = delta;
                    any_new = true;

                    /* Phase 4: Compute frontier from this delta immediately.
                     * Must happen before next sub-pass registration sets
                     * delta_rels[ri]=NULL.
                     * frontier = MAX eff_iter that produced facts. */
                    col_frontier_t rel_frontier = col_frontier_compute(delta);
                    if (rel_frontier.iteration > strat_frontier.iteration
                        || (rel_frontier.iteration == strat_frontier.iteration
                        && rel_frontier.stratum > strat_frontier.stratum)) {
                        strat_frontier = rel_frontier;
                    }
                } else {
                    col_rel_destroy(delta);
                }
            }

            /* Issue #176: Per-sub-pass cache eviction for recursive strata.
             * Materialized join results may live in delta_pool, so release
             * them before resetting the pool.  Resetting first clears the
             * pool-owned marker and leaves the cache holding stale pointers.
             * Use configurable eviction threshold (cache_evict_threshold):
             * - If 0: clear entire cache each sub-pass (backward compatible)
             * - If > 0: evict LRU entries when cache exceeds threshold */
            if (sess->cache_evict_threshold == 0) {
                col_mat_cache_release_pins(&sess->mat_cache);
                assert(sess->mat_cache.active_pins == 0);
                col_mat_cache_clear(&sess->mat_cache);
                assert(sess->mat_cache.active_pins == 0);
            } else {
                col_mat_cache_release_pins(&sess->mat_cache);
                col_mat_cache_evict_until(&sess->mat_cache,
                    sess->cache_evict_threshold);
            }

            col_session_mem_sample(sess); /* Issue #1380 */

            delta_pool_reset(sess->delta_pool);
            sess->rotation_ops->rotate_eval_arena(sess);
            /* Issue #560: Advance the compound-arena epoch frontier at the
             * end of each recursive sub-pass iteration so the
             * epoch-frontier GC can reclaim handles whose multiplicity
             * dropped to zero in this iteration.  NULL-guard each link in
             * the dispatch chain because tests and early lifecycle paths
             * may operate without a compound arena or rotation strategy.
             * The compound_arena is borrowed from the coordinator (Issue
             * #579 / R-5); only the coordinator may mutate it, so
             * worker-context invocations skip the dispatch. */
            if (sess->coordinator == NULL
                && sess->compound_arena && sess->rotation_ops
                && sess->rotation_ops->gc_epoch_boundary) {
                sess->rotation_ops->gc_epoch_boundary(sess);
            }
            if (!any_new) {
                /* Fixed point reached: no new tuples in this sub-pass. */
                converged = true;
                break;
            }
            outer_any_new = true;
            final_eff_iter = eff_iter;
            continue; /* next sub-pass */

stride_error:
            break; /* exit inner loop; outer_rc carries the error */
        } /* end inner sub-pass loop */

        if (outer_rc != 0) {
            /* Issue #282: Restore diff_operators_active on error path */
            sess->diff_operators_active = saved_diff_operators_active;
            if (sess->cleanup_pending) {
                /* Retained rule entries may borrow any registered delta or
                 * cache. Only local bookkeeping can be released here. */
                for (uint32_t ri = 0; ri < nrels; ri++)
                    assert(delta_rels[ri] == NULL);
                free(snap);
                free((void *)delta_rels);
                wl_columnar_memory_release(&delta_reservation);
                return outer_rc;
            }
            for (uint32_t ri = 0; ri < nrels; ri++) {
                /* Publication is atomic per relation. Preserve the old owner
                 * and any successfully published prefix even if their readers
                 * have released before this cleanup executes. */
                /* Best effort: outer_rc already carries the failure. */
                if (!publication_failed)
                    (void)session_remove_rel(sess,
                        sp->relations[ri].delta_name);
                if (delta_rels[ri])
                    col_rel_destroy(delta_rels[ri]);
            }
            free(snap);
            free((void *)delta_rels);
            wl_columnar_memory_release(&delta_reservation);
            return outer_rc;
        }

        /* Dispatch on why the inner loop exited */
        if (stride_all_skipped)
            continue; /* all sub-passes were frontier-skipped: next outer iter */
        if (outer_continue_next)
            continue; /* net-zero or all-empty delta: retry next outer iter */
        if (converged || !outer_any_new)
            break; /* true convergence */
    } /* end outer stride loop */
    /* Issue #282: Restore session-level diff_operators_active.
     * The per-iteration override above applies only within the recursive
     * fixed-point loop.  Restoring the saved value ensures subsequent
     * non-recursive strata and outer session logic see the correct state. */
    sess->diff_operators_active = saved_diff_operators_active;

    sess->total_iterations = final_eff_iter;

    /* Issue #914: current_iteration is deliberately left at final_eff_iter (> 0)
     * here. The non-recursive branch of col_eval_stratum is responsible for
     * resetting it to 0 before its base-case evaluation; do not add a second
     * reset here (consensus declined that). See the non-recursive branch above. */

    /* Issue #106 (US-106-005): Record per-rule frontier at convergence with epoch.
     * When stratum converges at effective iteration I, record (outer_epoch, I).
     * On next incremental snapshot, skip fires when eff_iter > I. */
    for (uint32_t ri = 0; ri < nrels && rule_id_base + ri < MAX_RULES; ri++) {
        uint32_t rule_id = rule_id_base + ri;
        sess->frontier_ops->record_rule_convergence(sess, rule_id,
            sess->outer_epoch, final_eff_iter);
    }

    /* Phase 4: Update per-stratum frontier after recursive stratum evaluation.
     * strat_frontier was computed incrementally via delta timestamps (eff_iter),
     * so it already holds the effective iteration index at convergence. */
    if (strat_frontier.iteration != UINT32_MAX) {
        sess->frontier_ops->record_stratum_convergence(sess, stratum_idx,
            sess->outer_epoch, strat_frontier.iteration);

        /* Issue #317: Report recursive convergence to coordinator's progress
         * tracker so col_eval_stratum_multiworker can compute the global
         * minimum frontier after the workqueue barrier.
         *
         * A worker that records converged progress here can now return EBUSY
         * from the terminal removal below, leaving a stale converged slot.
         * progress.h offers only a per-stratum reset across all workers, so a
         * worker must not undo its own entry without clobbering its siblings.
         * Harmless today: the slot is read only after the barrier, which is
         * skipped when any worker rc is non-zero, and the next multiworker
         * call resets the stratum's slots first.  See #1661. */
        if (sess->coordinator) {
            wl_frontier_progress_record(&sess->coordinator->progress,
                sess->worker_id, stratum_idx, sess->outer_epoch,
                strat_frontier.iteration);
        }
    }

    int agg_rc = wl_columnar_eval_serial_canonicalize_aggregates(sp, sess);
    if (agg_rc != 0) {
        for (uint32_t ri = 0; ri < nrels; ri++) {
            if (delta_rels[ri])
                col_rel_destroy(delta_rels[ri]);
        }
        free(snap);
        free((void *)delta_rels);
        wl_columnar_memory_release(&delta_reservation);
        return agg_rc;
    }

    /* Completion point for the recursive branch: aggregate canonicalization
     * above is the last step here whose failure is propagated, so every
     * head's content is final from here.  The terminal delta removal below
     * can still be refused -- see the comment on it -- but that concerns a
     * $d$ delta, not a head.  Move this with that boundary if a step whose
     * failure is propagated is added below. */
    sess->eval_stratum_heads_final = true;

    /* Cleanup all delta relations after frontier has been computed.  A
     * refusal keeps the registry owner, and unlike the sub-pass removal
     * above it is returned: this is the only success exit that could
     * otherwise report success with a reader-held delta still registered,
     * which is the contract violation #1661 names.  See the enumeration at
     * the sub-pass site for why every other exit already returns non-zero.
     *
     * The refusal is captured and returned below the whole teardown rather
     * than from here, so no refusal path can skip delta_pool_reset,
     * rotate_eval_arena or the mat-cache release and leave state growing on
     * each retry.  The test pins that control flow reaches the arena
     * rotation, so every step up to it is covered against an early return;
     * the mat-cache release follows the rotation and rides on the single
     * bottom return alone, and nothing is pinned against a step being
     * skipped in place.
     *
     * It deliberately does not touch the frontier, and no reset is performed
     * here.  U1's comment claimed a propagated refusal must reset it or
     * should_skip_iteration would starve the retry.  That is false twice
     * over.  This sweep sits outside the stride loop, so the loop's skipping
     * cannot keep an evaluation from reaching it; and a fully skipped retry
     * cannot arise anyway, because both vtables skip only when eff_iter is
     * strictly greater than the recorded iteration, so effective iteration 0
     * is never skippable.
     * The refusal also happens below the completion point above, so the
     * heads are already final and a retry has nothing to re-derive -- it
     * needs this sweep, and it gets it.  The convergence record left
     * standing is true for the same reason the heads are final, and
     * col_session_get_frontier reports it more accurately for being left
     * alone. */
    int removal_rc = 0;
    for (uint32_t ri = 0; ri < nrels; ri++) {
        if (delta_rels[ri])
            col_rel_destroy(delta_rels[ri]);
        const char *dname = sp->relations[ri].delta_name;
        int rm = session_remove_rel(sess, dname);
        /* session_remove_rel returns exactly {0, EBUSY, ENOENT}, and
         * session.c launders any non-zero from col_rel_destroy_checked into a
         * literal EBUSY, so this is equivalent to "== EBUSY" today and cannot
         * be told apart from it by a test.  The negative form is a
         * forward-compatibility preference: a third code should propagate
         * rather than vanish.  ENOENT is the common case and not a failure --
         * a relation that published no delta this stratum has nothing to
         * remove.  Keep sweeping so a later relation's private delta is still
         * destroyed and its registration still cleared. */
        if (rm != 0 && rm != ENOENT && removal_rc == 0)
            removal_rc = rm;
    }
    free(snap);
    free((void *)delta_rels);
    wl_columnar_memory_release(&delta_reservation);
    col_session_mem_sample(sess); /* Issue #1380 */
    delta_pool_reset(sess->delta_pool);
    sess->rotation_ops->rotate_eval_arena(sess);
    col_mat_cache_release_pins(&sess->mat_cache);
    assert(sess->mat_cache.active_pins == 0);
    col_mat_cache_clear(&sess->mat_cache);
    assert(sess->mat_cache.active_pins == 0);

    /* One return, below the teardown, so no refusal path can skip
     * delta_pool_reset or rotate_eval_arena -- otherwise pool and arena usage
     * would grow on every retry. */
    return removal_rc;
}
static col_frontier_t
col_frontier_compute(const col_rel_t *rel)
{
    col_frontier_t f = { 0, 0 };

    /* Handle NULL or empty relation */
    if (!rel || rel->nrows == 0 || !rel->timestamps)
        return f;

    /* Initialize frontier to first row's timestamp */
    f.iteration = rel->timestamps[0].iteration;
    f.stratum = rel->timestamps[0].stratum;

    /* Find minimum (iteration, stratum) */
    for (uint32_t i = 1; i < rel->nrows; i++) {
        const col_delta_timestamp_t *ts = &rel->timestamps[i];
        if (ts->iteration < f.iteration
            || (ts->iteration == f.iteration && ts->stratum < f.stratum)) {
            f.iteration = ts->iteration;
            f.stratum = ts->stratum;
        }
    }

    return f;
}
