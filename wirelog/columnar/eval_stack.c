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
struct wl_columnar_eval_stack_cleanup_frame {
    wl_col_session_t *session;
    wl_columnar_eval_stack_cleanup_frame_t *next;
    eval_stack_t stack;
    eval_entry_t result;
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_governor_ref_t *governor;
};

int
wl_columnar_eval_stack_cleanup_begin(wl_col_session_t *sess,
    wl_columnar_eval_stack_cleanup_frame_t **out)
{
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    uint64_t bytes;
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
    wl_columnar_memory_reservation_init(&reservation);
    if (sess->memory_governor) {
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(sess->memory_governor),
                sizeof(*frame), &reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                sess->memory_budget_denied = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
        }
    }
    frame = calloc(1, sizeof(*frame));
    if (!frame) {
        if (sess->memory_governor)
            (void)wl_columnar_memory_rollback(&reservation);
        return ENOMEM;
    }
    wl_columnar_memory_reservation_init(&frame->reservation);
    if (sess->memory_governor) {
        if (!wl_columnar_memory_reservation_move(&frame->reservation,
            &reservation)) {
            (void)wl_columnar_memory_rollback(&reservation);
            free(frame);
            return EINVAL;
        }
        if (!wl_columnar_memory_commit(&frame->reservation, frame)) {
            (void)wl_columnar_memory_rollback(&frame->reservation);
            free(frame);
            return EINVAL;
        }
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

/* Original-attempt frame admission: logical source accounting, not latency.
 * G9/C13/T29 expand original-ID guard/charge/tail; token init10/move40,
 * size-add7, retain6/get3, verdict R10 and returned status19. Ordinary failure
 * F56 rounded64 includes all marker/pointer/retirement-selection/abort/return
 * decisions and full move40; only allocator, original physical-credit-ref
 * retirement internals, control internals and instrumentation are excluded.
 * ENTRY103<=112, SETUP135<=144, RESERVE156<=192, ALLOC128<=144,
 * ZERO202<=208, TOKEN122<=128, MOVE161<=176, COMMIT104<=128,
 * EXTERNAL133<=144 including BOTH T29 tails on one debit. Six terminal
 * publication slots include active_count arithmetic/store2, no later hook.
 * ZERO counts16*(address+store+increment arithmetic/store+test)=80 plus
 * setup/transitions, tail and NEXT refused debit through full F64.
 * Nested reserve last-poll return84+status19+T29+R10+C13+refusal3+F64=222;
 * nested commit final4+rc3+check6+C13+refusal3+F64=93. Conservative240.
 * Read-only invalid/first-refused bootstrap<=28 may run at consumed0.
 * Ungoverned928+224*N; governed1726+224*N, one failed total CAS adds84;
 * N=ceil(actual frame bytes/16). No unfinished frame field is inspected. */
enum {
    WL_COLUMNAR_EVAL_STACK_ENTRY_WORK = 128,
    WL_COLUMNAR_EVAL_STACK_SETUP_WORK = 160,
    WL_COLUMNAR_EVAL_STACK_RESERVE_WORK = 240,
    WL_COLUMNAR_EVAL_STACK_ALLOC_WORK = 160,
    WL_COLUMNAR_EVAL_STACK_ZERO_WORK = 224,
    WL_COLUMNAR_EVAL_STACK_TOKEN_WORK = 128,
    WL_COLUMNAR_EVAL_STACK_MOVE_WORK = 192,
    WL_COLUMNAR_EVAL_STACK_COMMIT_WORK = 160,
    WL_COLUMNAR_EVAL_STACK_EXTERNAL_WORK = 160,
};

typedef char wl_columnar_eval_stack_attempt_fees_fit_t[
        (WL_COLUMNAR_EVAL_STACK_ENTRY_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EVAL_STACK_SETUP_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EVAL_STACK_RESERVE_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EVAL_STACK_ALLOC_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EVAL_STACK_ZERO_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EVAL_STACK_TOKEN_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EVAL_STACK_MOVE_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EVAL_STACK_COMMIT_WORK <= WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EVAL_STACK_EXTERNAL_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM) ? 1 : -1];
typedef char wl_columnar_eval_stack_attempt_envelopes_fit_t[
        (UINT64_C(112) <= WL_COLUMNAR_EVAL_STACK_ENTRY_WORK
        && UINT64_C(144) <= WL_COLUMNAR_EVAL_STACK_SETUP_WORK
        && UINT64_C(192) <= WL_COLUMNAR_EVAL_STACK_RESERVE_WORK
        && UINT64_C(144) <= WL_COLUMNAR_EVAL_STACK_ALLOC_WORK
        && UINT64_C(208) <= WL_COLUMNAR_EVAL_STACK_ZERO_WORK
        && UINT64_C(128) <= WL_COLUMNAR_EVAL_STACK_TOKEN_WORK
        && UINT64_C(176) <= WL_COLUMNAR_EVAL_STACK_MOVE_WORK
        && UINT64_C(128) <= WL_COLUMNAR_EVAL_STACK_COMMIT_WORK
        && UINT64_C(144) <= WL_COLUMNAR_EVAL_STACK_EXTERNAL_WORK) ? 1 : -1];
typedef char wl_columnar_eval_stack_attempt_contiguous_fits_t[
        (UINT64_C(240) <= WL_EVALUATION_CONTROL_QUANTUM) ? 1 : -1];

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_stack_test_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint64_t);
int wl_columnar_eval_stack_test_fail_allocation = -1;

size_t
wl_columnar_eval_stack_test_frame_size(void)
{
    return sizeof(wl_columnar_eval_stack_cleanup_frame_t);
}
#endif

static void
wl_columnar_eval_stack_attempt_checkpoint(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_stack_test_checkpoint)
        wl_columnar_eval_stack_test_checkpoint(control, phase, progress);
#else
    (void)control;
    (void)phase;
    (void)progress;
#endif
}

static void *
wl_columnar_eval_stack_attempt_allocate(size_t bytes)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_stack_test_fail_allocation >= 0) {
        if (wl_columnar_eval_stack_test_fail_allocation == 0)
            return NULL;
        wl_columnar_eval_stack_test_fail_allocation--;
    }
#endif
    return malloc(bytes);
}

static int
wl_columnar_eval_stack_attempt_guard(wl_evaluation_control_t *control,
    uint64_t id)
{
    return !control || !id
           || wl_evaluation_control_active_attempt_id(control) != id
        ? EINVAL : 0;
}

static int
wl_columnar_eval_stack_attempt_charge(wl_evaluation_control_t *control,
    uint64_t id, uint64_t work)
{
    int rc = wl_columnar_eval_stack_attempt_guard(control, id);
    return rc ? rc : wl_evaluation_control_charge(control, work);
}

static int
wl_columnar_eval_stack_attempt_tail(wl_evaluation_control_t *control,
    uint64_t id, unsigned phase, uint64_t progress)
{
    int rc = wl_columnar_eval_stack_attempt_guard(control, id);
    if (rc)
        return rc;
    wl_columnar_eval_stack_attempt_checkpoint(control, phase, progress);
    return wl_columnar_eval_stack_attempt_charge(control, id, 0);
}

int
wl_columnar_eval_stack_cleanup_begin_attempt_controlled(wl_col_session_t *sess,
    wl_columnar_eval_stack_cleanup_frame_t **out,
    wl_evaluation_control_t *control, uint64_t attempt_id)
{
    if (!control)
        return wl_columnar_eval_stack_cleanup_begin(sess, out);
    if (!sess || !out || *out)
        return EINVAL;
    int rc = wl_columnar_eval_stack_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_ENTRY_WORK);
    if (rc)
        return rc;
    uint64_t bytes = 0;
    int verdict = sess->cleanup_pending ? EBUSY
        : sess->cleanup_active_count >=
        WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES
        || sess->cleanup_pending_count >=
        WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES
        - sess->cleanup_active_count ? ENOBUFS
        : !wl_columnar_memory_size_add(sess->cleanup_reserved_bytes,
            sizeof(wl_columnar_eval_stack_cleanup_frame_t),
            &bytes) ? EOVERFLOW : 0;
    rc = wl_columnar_eval_stack_attempt_tail(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_ENTRY_PHASE, bytes);
    if (rc > 0 || !verdict) {
        if (rc)
            return rc;
    }
    if (verdict)
        return verdict;
    rc = wl_columnar_eval_stack_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_SETUP_WORK);
    if (rc)
        return rc;
    wl_columnar_memory_reservation_t admission;
    wl_columnar_memory_reservation_init(&admission);
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    wl_columnar_memory_governor_ref_t *retained_ref = sess->memory_governor;
    bool local_held = false, frame_held = false, committed = false;
    if (retained_ref)
        wl_columnar_memory_governor_ref_retain(retained_ref);
    rc = wl_columnar_eval_stack_attempt_tail(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_TOKEN_PHASE, 0);
    if (rc)
        goto fail;
    if (retained_ref) {
        rc = wl_columnar_eval_stack_attempt_charge(control, attempt_id,
                WL_COLUMNAR_EVAL_STACK_RESERVE_WORK);
        if (rc)
            goto fail;
        wl_columnar_memory_admission_status_t status;
        rc = wl_columnar_memory_governor_reserve_checked_attempt_controlled(
            wl_columnar_memory_governor_ref_get(retained_ref), sizeof(*frame),
            &admission, &status, control, attempt_id);
        if (rc)
            goto fail;
        local_held = status == WL_COLUMNAR_MEMORY_ADMISSION_OK
            || status == WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY;
        verdict = local_held ? 0
            : status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
            : status ==
            WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW : EINVAL;
        if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
            sess->memory_budget_denied = true;
        rc = wl_columnar_eval_stack_attempt_tail(control, attempt_id,
                WL_COLUMNAR_EVAL_STACK_RESERVE_PHASE, sizeof(*frame));
        if (rc > 0 || !verdict) {
            if (rc)
                goto fail;
        }
        if (verdict) {
            rc = verdict;
            goto fail;
        }
    }
    rc = wl_columnar_eval_stack_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_ALLOC_WORK);
    if (rc)
        goto fail;
    frame = wl_columnar_eval_stack_attempt_allocate(sizeof(*frame));
    verdict = frame ? 0 : ENOMEM;
    rc = wl_columnar_eval_stack_attempt_tail(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_ALLOCATION_PHASE, sizeof(*frame));
    if (rc > 0 || !verdict) {
        if (rc)
            goto fail;
    }
    if (verdict) {
        rc = verdict;
        goto fail;
    }
    for (size_t offset = 0; offset < sizeof(*frame); ) {
        rc = wl_columnar_eval_stack_attempt_charge(control, attempt_id,
                WL_COLUMNAR_EVAL_STACK_ZERO_WORK);
        if (rc)
            goto fail;
        size_t count = sizeof(*frame) - offset;
        if (count > 16)
            count = 16;
        for (size_t i = 0; i < count; i++)
            ((unsigned char *)frame)[offset + i] = 0;
        offset += count;
        rc = wl_columnar_eval_stack_attempt_tail(control, attempt_id,
                WL_COLUMNAR_EVAL_STACK_ZERO_PHASE, offset);
        if (rc)
            goto fail;
    }
    rc = wl_columnar_eval_stack_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_TOKEN_WORK);
    if (rc)
        goto fail;
    wl_columnar_memory_reservation_init(&frame->reservation);
    rc = wl_columnar_eval_stack_attempt_tail(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_TOKEN_PHASE, sizeof(*frame));
    if (rc)
        goto fail;
    rc = wl_columnar_eval_stack_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_MOVE_WORK);
    if (rc)
        goto fail;
    if (local_held) {
        if (!wl_columnar_memory_reservation_move(&frame->reservation,
            &admission))
            abort();
        frame_held = true;
        local_held = false;
        frame->governor = retained_ref;
    }
    rc = wl_columnar_eval_stack_attempt_tail(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_MOVED_PHASE, sizeof(*frame));
    if (rc)
        goto fail;
    if (frame_held) {
        rc = wl_columnar_eval_stack_attempt_charge(control, attempt_id,
                WL_COLUMNAR_EVAL_STACK_COMMIT_WORK);
        if (rc)
            goto fail;
        rc = wl_columnar_memory_governor_commit_attempt_controlled(
            &frame->reservation, frame, &committed, control, attempt_id);
        if (rc)
            goto fail;
        if (!committed) {
            rc = EINVAL;
            goto fail;
        }
    }
    rc = wl_columnar_eval_stack_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_EXTERNAL_WORK);
    if (rc)
        goto fail;
    frame->session = sess;
    frame->next = sess->cleanup_active;
    rc = wl_columnar_eval_stack_attempt_tail(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_COMMITTED_PHASE, sizeof(*frame));
    if (rc)
        goto fail;
    rc = wl_columnar_eval_stack_attempt_tail(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_EXTERNAL_PHASE, bytes);
    if (rc)
        goto fail;
    sess->cleanup_active = frame;
    sess->cleanup_active_count++;
    sess->cleanup_reserved_bytes = bytes;
    *out = frame;
    return 0;
fail:
    if (frame_held) {
        if (!wl_columnar_memory_reservation_move(&admission,
            &frame->reservation))
            abort();
        local_held = true;
    }
    if (frame) {
        free(frame);
        wl_columnar_eval_stack_attempt_checkpoint(NULL,
            WL_COLUMNAR_EVAL_STACK_PHYSICAL_FREED_PHASE, 0);
    }
    if (local_held) {
        bool released = committed ? wl_columnar_memory_release(&admission)
            : wl_columnar_memory_rollback(&admission);
        if (!released)
            abort();
    }
    if (retained_ref)
        wl_columnar_memory_governor_ref_release(retained_ref);
    return rc;
}

eval_stack_t *
wl_columnar_eval_stack_cleanup_stack(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return frame ? &frame->stack : NULL;
}

eval_entry_t *
wl_columnar_eval_stack_cleanup_result(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return frame ? &frame->result : NULL;
}

static void
wl_columnar_eval_stack_cleanup_continuations(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    if (frame->result.kind == WL_COLUMNAR_EVAL_ENTRY_CONTINUATION)
        (void)eval_entry_dispose(&frame->result);
    for (uint32_t i = 0; i < frame->stack.top; i++)
        if (frame->stack.items[i].kind == WL_COLUMNAR_EVAL_ENTRY_CONTINUATION)
            (void)eval_entry_dispose(&frame->stack.items[i]);
}

static int
wl_columnar_eval_stack_cleanup_dispose(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    int result = eval_entry_dispose(&frame->result);
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

static int
wl_columnar_eval_stack_cleanup_free(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
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
