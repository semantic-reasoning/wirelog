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
typedef enum {
    WL_COLUMNAR_EVAL_STACK_UNARY_IDLE = 0,
    WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE,
    WL_COLUMNAR_EVAL_STACK_UNARY_COMMITTED,
    WL_COLUMNAR_EVAL_STACK_UNARY_RETAINED,
    WL_COLUMNAR_EVAL_STACK_UNARY_DONE,
} wl_columnar_eval_stack_unary_phase_t;

typedef char wl_columnar_eval_stack_unary_zero_defaults_t[
        (WL_COLUMNAR_EVAL_STACK_UNARY_IDLE == 0
        && WL_COLUMNAR_EVAL_ENTRY_RELATION == 0) ? 1 : -1];

struct wl_columnar_eval_stack_cleanup_frame {
    wl_col_session_t *session;
    wl_columnar_eval_stack_cleanup_frame_t *next;
    eval_stack_t stack;
    eval_entry_t result;
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_governor_ref_t *governor;
    const void *identity;
    wl_columnar_eval_stack_unary_phase_t unary_phase;
    eval_entry_t unary_output;
    eval_entry_t unary_retired;
    eval_entry_t unary_snapshot;
    uint32_t unary_index;
    uint32_t unary_top;
    wl_evaluation_control_t *unary_control;
    uint64_t unary_attempt_id;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t unary_owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD unary_owner_thread;
#else
    pthread_t unary_owner_thread;
#endif
    bool unary_thread_valid;
    bool unary_committed;
    bool unary_result_saved;
    int unary_first_result;
    int unary_cleanup_result;
    bool unary_alias_rejected;
    wl_columnar_filter_context_t filter;
    const void *numeric_plan_owner;
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
    frame->identity = frame;
    frame->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_IDLE;
    frame->numeric_plan_owner = NULL;
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
 * ZERO202<=208, TOKEN125<=128 (self/IDLE/segment-owner stores3), MOVE161<=176, COMMIT104<=128,
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
    frame->identity = frame;
    frame->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_IDLE;
    frame->numeric_plan_owner = NULL;
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

/* Unary logical source accounting, not instruction counts or latency.
 * Clear/copy7, inert14, frame-valid8, native-thread8, forward34,
 * original-matches23, result5; inherited G9/C13/T29/R10. Disposer14 counts
 * ordinary routing/stores/return, excluding only original physical internals.
 * FIRST_FINISH is distinct prepayment for later cleanup, not executed twice
 * during begin. Finish scaffold82 + grouped sanitizer136 + disposer14=232.
 * Scan prefix13, each4-scalar group14, each remainder cell5; late remainder
 * alias135 includes detach8/return. All32 no-match128, at31 no-match129.
 * No callback/poll or ownership change precedes proof and eight detach stores.
 * BEGIN_ENTRY133<=144, PREPARE112, COMMIT_ENTRY154<=160, HEAD77<=96,
 * ROW74<=96 including NEXT refused debit, TERMINAL93<=112 (21 field stores).
 * Input/output caller envelopes48/56, RETAINED96, DONE24, RESET64.
 * New generic cleanup guard success14/rejection15, disposer extension36,
 * continuation extension32 are ordinary work, not blanket excluded scans.
 * Begin544; commit448+128*n live entries; head alias320, indexj alias
 * 320+128*(j+1). First-refused begin/commit bootstrap26/58 may consume0.
 * Retry is bounded per invocation at the mandatory lifetime-cleanup boundary;
 * one old begin debit never pays arbitrary retries or a whole pending list.
 * Control/instrumentation/allocator and original physical/token/ref retirement
 * internals retain narrow exclusions. Future callers compose adjacent work. */
enum {
    WL_COLUMNAR_EVAL_STACK_UNARY_BEGIN_ENTRY_WORK = 160,
    WL_COLUMNAR_EVAL_STACK_UNARY_BEGIN_PREPARE_WORK = 128,
    WL_COLUMNAR_EVAL_STACK_UNARY_FIRST_FINISH_WORK = 256,
    WL_COLUMNAR_EVAL_STACK_UNARY_COMMIT_ENTRY_WORK = 192,
    WL_COLUMNAR_EVAL_STACK_UNARY_SCAN_HEAD_WORK = 128,
    WL_COLUMNAR_EVAL_STACK_UNARY_SCAN_ROW_WORK = 128,
    WL_COLUMNAR_EVAL_STACK_UNARY_TERMINAL_WORK = 128,
};

typedef char wl_columnar_eval_stack_unary_fees_fit_t[
        (WL_COLUMNAR_EVAL_STACK_UNARY_BEGIN_ENTRY_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EVAL_STACK_UNARY_BEGIN_PREPARE_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EVAL_STACK_UNARY_FIRST_FINISH_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EVAL_STACK_UNARY_COMMIT_ENTRY_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EVAL_STACK_UNARY_SCAN_HEAD_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EVAL_STACK_UNARY_SCAN_ROW_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_EVAL_STACK_UNARY_TERMINAL_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM) ? 1 : -1];
typedef char wl_columnar_eval_stack_unary_envelopes_fit_t[
        (UINT64_C(144) <= WL_COLUMNAR_EVAL_STACK_UNARY_BEGIN_ENTRY_WORK
        && UINT64_C(112) <= WL_COLUMNAR_EVAL_STACK_UNARY_BEGIN_PREPARE_WORK
        && UINT64_C(232) <= WL_COLUMNAR_EVAL_STACK_UNARY_FIRST_FINISH_WORK
        && UINT64_C(160) <= WL_COLUMNAR_EVAL_STACK_UNARY_COMMIT_ENTRY_WORK
        && UINT64_C(96) <= WL_COLUMNAR_EVAL_STACK_UNARY_SCAN_HEAD_WORK
        && UINT64_C(96) <= WL_COLUMNAR_EVAL_STACK_UNARY_SCAN_ROW_WORK
        && UINT64_C(112) <=
        WL_COLUMNAR_EVAL_STACK_UNARY_TERMINAL_WORK) ? 1 : -1];
typedef char wl_columnar_eval_stack_unary_contiguous_fits_t[
        (UINT64_C(232) <= WL_EVALUATION_CONTROL_QUANTUM
        && UINT64_C(136) <= WL_EVALUATION_CONTROL_QUANTUM
        && UINT64_C(125) <= WL_COLUMNAR_EVAL_STACK_TOKEN_WORK) ? 1 : -1];

static void
wl_columnar_eval_stack_unary_entry_clear(eval_entry_t *entry)
{
    entry->rel = NULL;
    entry->owned = false;
    entry->is_delta = false;
    entry->seg_boundaries = NULL;
    entry->seg_count = 0;
    entry->kind = WL_COLUMNAR_EVAL_ENTRY_RELATION;
    entry->continuation = NULL;
}

static void
wl_columnar_eval_stack_unary_entry_copy(eval_entry_t *out,
    const eval_entry_t *in)
{
    out->rel = in->rel;
    out->owned = in->owned;
    out->is_delta = in->is_delta;
    out->seg_boundaries = in->seg_boundaries;
    out->seg_count = in->seg_count;
    out->kind = in->kind;
    out->continuation = in->continuation;
}

static bool
wl_columnar_eval_stack_unary_entry_inert(const eval_entry_t *entry)
{
    return !entry->rel && !entry->owned && !entry->is_delta
           && !entry->seg_boundaries && !entry->seg_count
           && entry->kind == WL_COLUMNAR_EVAL_ENTRY_RELATION
           && !entry->continuation;
}

static bool
wl_columnar_eval_stack_unary_frame_valid(
    const wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return frame && frame->identity == frame && frame->session
           && frame->stack.top <= COL_STACK_MAX;
}

int
wl_columnar_eval_stack_numeric_owner(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner)
{
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)) return EINVAL;
    if (frame->numeric_plan_owner != owner) return EBUSY;
    if (owner && !wl_columnar_eval_plan_numeric_owner_valid(owner, frame))
        return EINVAL;
    return 0;
}

static bool
wl_columnar_eval_stack_unary_thread_valid(
    const wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    if (!frame->unary_thread_valid)
        return false;
#if defined(WL_HAVE_C11_THREADS)
    return thrd_equal(frame->unary_owner_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return frame->unary_owner_thread == GetCurrentThreadId();
#else
    return pthread_equal(frame->unary_owner_thread, pthread_self()) != 0;
#endif
}

static int
wl_columnar_eval_stack_unary_forward(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)
        || frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE
        || frame->session->cleanup_active != frame
        || !wl_columnar_eval_stack_unary_thread_valid(frame))
        return EINVAL;
    return wl_columnar_eval_stack_attempt_guard(frame->unary_control,
               frame->unary_attempt_id);
}

static void
wl_columnar_eval_stack_unary_binding_clear(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    wl_columnar_eval_stack_unary_entry_clear(&frame->unary_snapshot);
    frame->unary_control = NULL;
    frame->unary_attempt_id = 0;
    frame->unary_thread_valid = false;
}

int
wl_columnar_eval_stack_unary_begin_attempt_controlled(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_evaluation_control_t *control, uint64_t attempt_id)
{
    return wl_columnar_eval_stack_unary_begin_attempt_owned(frame, control,
               attempt_id, NULL);
}

int
wl_columnar_eval_stack_unary_begin_attempt_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_evaluation_control_t *control, uint64_t attempt_id, const void *owner)
{
    int authority = wl_columnar_eval_stack_numeric_owner(frame, owner);
    if (authority) return authority;
    if (!wl_columnar_eval_stack_unary_frame_valid(frame))
        return EINVAL;
    int rc = wl_columnar_eval_stack_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_UNARY_BEGIN_ENTRY_WORK);
    if (rc)
        return rc;
    int verdict = frame->session->cleanup_active != frame
        || frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_IDLE
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_output)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_retired)
        ? EBUSY : !frame->stack.top ? EINVAL : 0;
    if (!verdict) {
        const eval_entry_t *entry = &frame->stack.items[frame->stack.top - 1];
        verdict = entry->kind == WL_COLUMNAR_EVAL_ENTRY_CONTINUATION
            ? ENOTSUP : entry->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
            || !entry->rel || entry->continuation ? EINVAL : 0;
    }
    rc = wl_columnar_eval_stack_attempt_tail(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_UNARY_BEGIN_VALID_PHASE, frame->stack.top);
    if (rc > 0 || !verdict) {
        if (rc)
            return rc;
    }
    if (verdict)
        return verdict;
    rc = wl_columnar_eval_stack_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_UNARY_BEGIN_PREPARE_WORK);
    if (rc)
        return rc;
    /* Initialization belongs to PREPARE, mandatory first finish to this
     * separate allowance. Refusal exposes neither ACTIVE nor a slot borrow. */
    rc = wl_columnar_eval_stack_attempt_charge(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_UNARY_FIRST_FINISH_WORK);
    if (rc)
        return rc;
    frame->unary_top = frame->stack.top;
    frame->unary_index = frame->unary_top - 1;
    wl_columnar_eval_stack_unary_entry_copy(&frame->unary_snapshot,
        &frame->stack.items[frame->unary_index]);
    frame->unary_control = control;
    frame->unary_attempt_id = attempt_id;
#if defined(WL_HAVE_C11_THREADS)
    frame->unary_owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    frame->unary_owner_thread = GetCurrentThreadId();
#else
    frame->unary_owner_thread = pthread_self();
#endif
    frame->unary_thread_valid = true;
    frame->unary_committed = false;
    frame->unary_result_saved = false;
    frame->unary_first_result = 0;
    frame->unary_cleanup_result = 0;
    frame->unary_alias_rejected = false;
    rc = wl_columnar_eval_stack_attempt_tail(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_UNARY_BEGIN_PREPARED_PHASE,
            frame->unary_top);
    if (rc)
        goto fail;
    rc = wl_columnar_eval_stack_attempt_tail(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE_CUTOFF_PHASE, frame->unary_top);
    if (rc)
        goto fail;
    frame->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE;
    return 0;
fail:
    wl_columnar_eval_stack_unary_binding_clear(frame);
    frame->unary_index = 0;
    frame->unary_top = 0;
    return rc;
}

int
wl_columnar_eval_stack_unary_input(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const col_rel_t **out)
{
    return wl_columnar_eval_stack_unary_input_owned(frame, out, NULL);
}

int
wl_columnar_eval_stack_unary_input_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const col_rel_t **out,
    const void *owner)
{
    int authority = wl_columnar_eval_stack_numeric_owner(frame, owner);
    if (authority) return authority;
    if (!out)
        return EINVAL;
    int rc = wl_columnar_eval_stack_unary_forward(frame);
    if (rc)
        return rc;
    *out = frame->unary_snapshot.rel;
    return 0;
}

int
wl_columnar_eval_stack_unary_output(
    wl_columnar_eval_stack_cleanup_frame_t *frame, col_rel_t ***out_slot)
{
    return wl_columnar_eval_stack_unary_output_owned(frame, out_slot, NULL);
}

int
wl_columnar_eval_stack_unary_output_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, col_rel_t ***out_slot,
    const void *owner)
{
    int authority = wl_columnar_eval_stack_numeric_owner(frame, owner);
    if (authority) return authority;
    if (!out_slot)
        return EINVAL;
    int rc = wl_columnar_eval_stack_unary_forward(frame);
    if (rc)
        return rc;
    if (frame->unary_output.rel)
        return EINVAL;
    frame->unary_output.owned = true;
    frame->unary_output.is_delta = false;
    frame->unary_output.seg_boundaries = NULL;
    frame->unary_output.seg_count = 0;
    frame->unary_output.kind = WL_COLUMNAR_EVAL_ENTRY_RELATION;
    frame->unary_output.continuation = NULL;
    *out_slot = &frame->unary_output.rel;
    return 0;
}

static bool
wl_columnar_eval_stack_unary_original_matches(
    const wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    if (frame->stack.top != frame->unary_top || !frame->unary_top
        || frame->unary_index != frame->unary_top - 1)
        return false;
    const eval_entry_t *entry = &frame->stack.items[frame->unary_index];
    const eval_entry_t *saved = &frame->unary_snapshot;
    return entry->rel == saved->rel && entry->owned == saved->owned
           && entry->is_delta == saved->is_delta
           && entry->seg_boundaries == saved->seg_boundaries
           && entry->seg_count == saved->seg_count && entry->kind == saved->kind
           && entry->continuation == saved->continuation;
}

static void
wl_columnar_eval_stack_unary_alias_detach(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    wl_columnar_eval_stack_unary_entry_clear(&frame->unary_output);
    frame->unary_alias_rejected = true;
}

/* Mandatory first-finish sanitation. No callback/poll, candidate dereference,
 * destructor or credit operation. Only actual live entries are compared. */
static bool
wl_columnar_eval_stack_unary_output_sanitize(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    col_rel_t *candidate = frame->unary_output.rel;
    if (!candidate)
        return false;
    if (candidate == frame->unary_snapshot.rel
        || candidate == frame->result.rel ||
        candidate == frame->unary_retired.rel)
        goto alias;
    uint32_t limit = frame->stack.top - frame->stack.top % 4;
    uint32_t i = 0;
    for (; i < limit; i += 4)
        if (candidate == frame->stack.items[i].rel
            || candidate == frame->stack.items[i + 1].rel
            || candidate == frame->stack.items[i + 2].rel
            || candidate == frame->stack.items[i + 3].rel)
            goto alias;
    for (; i < frame->stack.top; i++)
        if (candidate == frame->stack.items[i].rel)
            goto alias;
    return false;
alias:
    wl_columnar_eval_stack_unary_alias_detach(frame);
    return true;
}

int
wl_columnar_eval_stack_unary_commit_attempt_controlled(
    wl_columnar_eval_stack_cleanup_frame_t *frame, bool *committed)
{
    return wl_columnar_eval_stack_unary_commit_attempt_owned(frame, committed,
               NULL);
}

int
wl_columnar_eval_stack_unary_commit_attempt_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, bool *committed,
    const void *owner)
{
    int authority = wl_columnar_eval_stack_numeric_owner(frame, owner);
    if (authority) return authority;
    if (!committed)
        return EINVAL;
    *committed = false;
    int rc = wl_columnar_eval_stack_unary_forward(frame);
    if (rc)
        return rc;
    wl_evaluation_control_t *control = frame->unary_control;
    uint64_t id = frame->unary_attempt_id;
    rc = wl_columnar_eval_stack_attempt_charge(control, id,
            WL_COLUMNAR_EVAL_STACK_UNARY_COMMIT_ENTRY_WORK);
    if (rc)
        return rc;
    eval_entry_t *output = &frame->unary_output;
    int verdict = !wl_columnar_eval_stack_unary_original_matches(frame)
        || !output->rel || !output->owned || output->is_delta
        || output->seg_boundaries || output->seg_count
        || output->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
        || output->continuation ? EINVAL : 0;
    rc = wl_columnar_eval_stack_attempt_tail(control, id,
            WL_COLUMNAR_EVAL_STACK_UNARY_COMMIT_VALID_PHASE, frame->unary_top);
    if (rc > 0 || !verdict) {
        if (rc)
            return rc;
    }
    if (verdict)
        return verdict;
    rc = wl_columnar_eval_stack_attempt_charge(control, id,
            WL_COLUMNAR_EVAL_STACK_UNARY_SCAN_HEAD_WORK);
    if (rc)
        return rc;
    col_rel_t *candidate = output->rel;
    bool alias = candidate == frame->unary_snapshot.rel
        || candidate == frame->result.rel ||
        candidate == frame->unary_retired.rel;
    if (alias)
        wl_columnar_eval_stack_unary_alias_detach(frame);
    rc = wl_columnar_eval_stack_attempt_tail(control, id,
            WL_COLUMNAR_EVAL_STACK_UNARY_COLLISION_PHASE, 0);
    if (rc > 0 || !alias) {
        if (rc)
            return rc;
    }
    if (alias)
        return EINVAL;
    for (uint32_t i = 0; i < frame->stack.top; i++) {
        rc = wl_columnar_eval_stack_attempt_charge(control, id,
                WL_COLUMNAR_EVAL_STACK_UNARY_SCAN_ROW_WORK);
        if (rc)
            return rc;
        alias = candidate == frame->stack.items[i].rel;
        if (alias)
            wl_columnar_eval_stack_unary_alias_detach(frame);
        rc = wl_columnar_eval_stack_attempt_tail(control, id,
                WL_COLUMNAR_EVAL_STACK_UNARY_COLLISION_PHASE, i + 1);
        if (rc > 0 || !alias) {
            if (rc)
                return rc;
        }
        if (alias)
            return EINVAL;
    }
    rc = wl_columnar_eval_stack_attempt_charge(control, id,
            WL_COLUMNAR_EVAL_STACK_UNARY_TERMINAL_WORK);
    if (rc)
        return rc;
    verdict = wl_columnar_eval_stack_unary_original_matches(frame) ? 0 : EINVAL;
    rc = wl_columnar_eval_stack_attempt_tail(control, id,
            WL_COLUMNAR_EVAL_STACK_UNARY_COMMIT_CUTOFF_PHASE, frame->unary_top);
    if (rc > 0 || !verdict) {
        if (rc)
            return rc;
    }
    if (verdict)
        return verdict;
    eval_entry_t *entry = &frame->stack.items[frame->unary_index];
    wl_columnar_eval_stack_unary_entry_copy(&frame->unary_retired, entry);
    wl_columnar_eval_stack_unary_entry_copy(entry, output);
    wl_columnar_eval_stack_unary_entry_clear(output);
    frame->unary_committed = true;
    frame->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_COMMITTED;
    *committed = true;
    return 0;
}

static int
wl_columnar_eval_stack_unary_result(
    const wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return frame->unary_first_result ? frame->unary_first_result
        : frame->unary_alias_rejected ? EINVAL : frame->unary_cleanup_result;
}

static int
wl_columnar_eval_stack_unary_finish_body(
    wl_columnar_eval_stack_cleanup_frame_t *frame, int initiating_result,
    bool sanitize)
{
    if (!wl_columnar_eval_stack_unary_frame_valid(frame))
        return EINVAL;
    if (frame->unary_phase == WL_COLUMNAR_EVAL_STACK_UNARY_IDLE)
        return EINVAL;
    if (frame->unary_phase == WL_COLUMNAR_EVAL_STACK_UNARY_DONE)
        return wl_columnar_eval_stack_unary_result(frame);
    bool first = frame->unary_phase == WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE
        || frame->unary_phase == WL_COLUMNAR_EVAL_STACK_UNARY_COMMITTED;
    if (first && !wl_columnar_eval_stack_unary_thread_valid(frame))
        return EINVAL;
    if (!first && frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_RETAINED)
        return EINVAL;
    if (!frame->unary_result_saved) {
        frame->unary_first_result = initiating_result;
        frame->unary_result_saved = true;
    }
    bool alias = first && !frame->unary_committed && sanitize
        && wl_columnar_eval_stack_unary_output_sanitize(frame);
    eval_entry_t *entry = frame->unary_committed ? &frame->unary_retired
        : &frame->unary_output;
    int rc = alias ? EINVAL : eval_entry_dispose(entry);
    if (rc == 0)
        wl_columnar_eval_stack_unary_entry_clear(entry);
    frame->unary_cleanup_result = rc;
    if (first)
        wl_columnar_eval_stack_unary_binding_clear(frame);
    frame->unary_phase = alias || rc == 0 ? WL_COLUMNAR_EVAL_STACK_UNARY_DONE
        : WL_COLUMNAR_EVAL_STACK_UNARY_RETAINED;
    return wl_columnar_eval_stack_unary_result(frame);
}

int
wl_columnar_eval_stack_unary_finish(
    wl_columnar_eval_stack_cleanup_frame_t *frame, int initiating_result)
{
    int rc = wl_columnar_eval_stack_numeric_owner(frame, NULL);
    if (rc) return rc;
    return wl_columnar_eval_stack_unary_finish_body(frame, initiating_result,
               true);
}

int
wl_columnar_eval_stack_unary_reset(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return wl_columnar_eval_stack_unary_reset_owned(frame, NULL);
}

int
wl_columnar_eval_stack_unary_reset_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner)
{
    if (!wl_columnar_eval_stack_unary_frame_valid(frame))
        return EINVAL;
    if (frame->numeric_plan_owner != owner) return EBUSY;
    if (owner && !wl_columnar_eval_plan_numeric_owner_valid(owner, frame))
        return EINVAL;
    if (frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_DONE
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_output)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_retired))
        return EBUSY;
    wl_columnar_eval_stack_unary_binding_clear(frame);
    frame->unary_index = 0;
    frame->unary_top = 0;
    frame->unary_committed = false;
    frame->unary_result_saved = false;
    frame->unary_first_result = 0;
    frame->unary_cleanup_result = 0;
    frame->unary_alias_rejected = false;
    frame->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_IDLE;
    return 0;
}

/* Private continuation bridge: protocol refusal does not consume output.
 * Generic callers always retain the original alias sanitation policy. */
int
wl_columnar_eval_stack_filter_finish(
    wl_columnar_eval_stack_cleanup_frame_t *frame, int result, int *semantic,
    bool *retained)
{
    return wl_columnar_eval_stack_filter_finish_owned(frame, result, semantic,
               retained, NULL);
}

int
wl_columnar_eval_stack_filter_finish_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, int result, int *semantic,
    bool *retained, const void *owner)
{
    if (!semantic || !retained
        || !wl_columnar_eval_stack_unary_frame_valid(frame))
        return EINVAL;
    if (frame->numeric_plan_owner != owner) return EBUSY;
    if (owner && !wl_columnar_eval_plan_numeric_owner_valid(owner, frame))
        return EINVAL;
    wl_columnar_filter_context_t *ctx = &frame->filter;
    if (ctx->identity != ctx || ctx->frame != frame
        || ctx->session != frame->session
        || frame->session->cleanup_active != frame || !ctx->thread_valid
        || !ctx->busy || !ctx->unary_started
        || ctx->stage != WL_COLUMNAR_FILTER_STAGE_FINISH)
        return EINVAL;
#if defined(WL_HAVE_C11_THREADS)
    if (!thrd_equal(ctx->owner_thread, thrd_current())) return EINVAL;
#elif defined(_WIN32) || defined(_WIN64)
    if (ctx->owner_thread != GetCurrentThreadId()) return EINVAL;
#else
    if (!pthread_equal(ctx->owner_thread, pthread_self())) return EINVAL;
#endif
    switch (frame->unary_phase) {
    case WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE:
        if (!wl_columnar_eval_stack_unary_thread_valid(frame)
            || frame->unary_committed
            || (frame->unary_output.rel
            && frame->unary_output.rel != ctx->private_output))
            return EINVAL;
        break;
    case WL_COLUMNAR_EVAL_STACK_UNARY_COMMITTED:
        if (!wl_columnar_eval_stack_unary_thread_valid(frame)
            || !frame->unary_committed)
            return EINVAL;
        break;
    case WL_COLUMNAR_EVAL_STACK_UNARY_RETAINED:
    case WL_COLUMNAR_EVAL_STACK_UNARY_DONE:
        break;
    default:
        return EINVAL;
    }
    /* No evaluation of the borrowed marker after physical disposal, even if
     * the original entry is retained and will require another host turn. */
    ctx->private_output = NULL;
    *semantic = wl_columnar_eval_stack_unary_finish_body(frame, result, false);
    ctx->cleanup_result = frame->unary_cleanup_result;
    *retained = frame->unary_phase == WL_COLUMNAR_EVAL_STACK_UNARY_RETAINED;
    return 0;
}

int
wl_columnar_eval_stack_filter_context(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_filter_context_t **out)
{
    return wl_columnar_eval_stack_filter_context_owned(frame, out, NULL);
}

int
wl_columnar_eval_stack_filter_context_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_filter_context_t **out, const void *owner)
{
    if (!out || !wl_columnar_eval_stack_unary_frame_valid(frame))
        return EINVAL;
    if (frame->numeric_plan_owner != owner) return EBUSY;
    if (owner && !wl_columnar_eval_plan_numeric_owner_valid(owner, frame))
        return EINVAL;
    *out = &frame->filter;
    return 0;
}

/* Operator entry validation is callback-free and consumes no authority.
 * IDLE frames have no creator-thread identity: callers serialize their use. */
int
wl_columnar_eval_stack_operator_stack(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    eval_stack_t **out)
{
    return wl_columnar_eval_stack_operator_stack_owned(frame, sess, out, NULL);
}

int
wl_columnar_eval_stack_operator_stack_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    eval_stack_t **out, const void *owner)
{
    int authority = wl_columnar_eval_stack_numeric_owner(frame, owner);
    if (authority) return authority;
    if (!out || !frame || !sess || frame->identity != frame
        || frame->session != sess || frame->stack.top > COL_STACK_MAX)
        return EINVAL;
    if (frame->filter.stage != WL_COLUMNAR_FILTER_STAGE_INERT
        || sess->cleanup_active != frame
        || frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_IDLE
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_output)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_retired)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_snapshot)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->result)
        || frame->unary_control || frame->unary_attempt_id
        || frame->unary_index || frame->unary_top || frame->unary_thread_valid
        || frame->unary_committed || frame->unary_result_saved
        || frame->unary_alias_rejected || frame->unary_first_result
        || frame->unary_cleanup_result)
        return EBUSY;
    if (!frame->stack.top)
        return EINVAL;
    const eval_entry_t *entry = &frame->stack.items[frame->stack.top - 1];
    if (entry->kind == WL_COLUMNAR_EVAL_ENTRY_CONTINUATION)
        return ENOTSUP;
    if (entry->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
        || entry->continuation || !entry->rel)
        return EINVAL;
    *out = &frame->stack;
    return 0;
}

int
wl_columnar_eval_stack_numeric_claim(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    const void *owner)
{
    if (!owner || !wl_columnar_eval_plan_numeric_owner_valid(owner, frame))
        return EINVAL;
    eval_stack_t *stack = NULL;
    int rc = wl_columnar_eval_stack_operator_stack(frame, sess, &stack);
    if (rc) return rc;
    if (stack->top != 1) return EINVAL;
    frame->numeric_plan_owner = owner;
    return 0;
}

int
wl_columnar_eval_stack_numeric_unbind(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    const void *owner)
{
    if (!owner) return EINVAL;
    eval_stack_t *stack = NULL;
    int rc = wl_columnar_eval_stack_operator_stack_owned(frame, sess, &stack,
            owner);
    if (rc) return rc;
    frame->numeric_plan_owner = NULL;
    return 0;
}

/* Source binding owns an initially empty frame. These separate routes leave
 * the tight nonempty numeric/unary validators unchanged. Registry and frame
 * access are serialized; raw borrowed pointers must not mutate this workspace. */
static int
wl_columnar_eval_stack_numeric_source_frame(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    const wl_columnar_eval_plan_numeric_attempt_t *owner, bool prepared)
{
    if (!frame || !sess || frame->identity != frame || frame->session != sess
        || !owner || !wl_columnar_eval_plan_numeric_owner_valid(owner, frame))
        return EINVAL;
    if (sess->cleanup_active != frame || frame->stack.top
        || frame->filter.stage != WL_COLUMNAR_FILTER_STAGE_INERT
        || frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_IDLE
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_output)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_retired)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_snapshot)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->stack.items[0])
        || frame->unary_control || frame->unary_attempt_id
        || frame->unary_index || frame->unary_top || frame->unary_thread_valid
        || frame->unary_committed || frame->unary_result_saved
        || frame->unary_alias_rejected || frame->unary_first_result
        || frame->unary_cleanup_result)
        return EBUSY;
    if (!prepared) {
        if (!wl_columnar_eval_stack_unary_entry_inert(&frame->result))
            return EBUSY;
    } else if (!frame->result.owned
        || frame->result.is_delta != owner->source_is_delta
        || frame->result.seg_boundaries || frame->result.seg_count
        || frame->result.kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
        || frame->result.continuation
        || owner->source_output != &frame->result.rel) {
        return EBUSY;
    }
    return 0;
}

int
wl_columnar_eval_stack_numeric_source_claim(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_columnar_eval_plan_numeric_attempt_t *owner)
{
    int rc = wl_columnar_eval_stack_numeric_source_frame(frame, sess, owner,
            false);
    if (rc) return rc;
    if (frame->numeric_plan_owner) return EBUSY;
    frame->numeric_plan_owner = owner;
    return 0;
}

int
wl_columnar_eval_stack_numeric_source_output(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_columnar_eval_plan_numeric_attempt_t *owner)
{
    int rc = wl_columnar_eval_stack_numeric_source_frame(frame, sess, owner,
            false);
    if (rc) return rc;
    if (frame->numeric_plan_owner != owner || !owner->source_mode
        || owner->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_SLOT
        || owner->source_slot_borrowed || owner->source_installed)
        return EBUSY;
    frame->result.rel = NULL; frame->result.owned = true;
    frame->result.is_delta = owner->source_is_delta;
    frame->result.seg_boundaries = NULL; frame->result.seg_count = 0;
    frame->result.kind = WL_COLUMNAR_EVAL_ENTRY_RELATION;
    frame->result.continuation = NULL;
    owner->source_output = &frame->result.rel;
    owner->source_slot_borrowed = true;
    return 0;
}

int
wl_columnar_eval_stack_numeric_source_install(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_columnar_eval_plan_numeric_attempt_t *owner)
{
    int rc = wl_columnar_eval_stack_numeric_source_frame(frame, sess, owner,
            owner && owner->source_slot_borrowed);
    if (rc) return rc;
    if (frame->numeric_plan_owner != owner || !owner->source_mode
        || owner->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_INSTALL
        || owner->source_initialized || owner->source_installed
        || owner->source_empty != owner->source_slot_borrowed)
        return EBUSY;
    col_rel_t *rel = owner->source_empty ? frame->result.rel
        : owner->source_selected;
    if (!rel) return EINVAL;
    /* All fallible/stopping work and the last saved-ID check precede this
     * callback-free ownership transfer. No destructor or debit follows it. */
    frame->stack.items[0].rel = rel;
    frame->stack.items[0].owned = owner->source_empty;
    frame->stack.items[0].is_delta = owner->source_is_delta;
    frame->stack.items[0].seg_boundaries = NULL;
    frame->stack.items[0].seg_count = 0;
    frame->stack.items[0].kind = WL_COLUMNAR_EVAL_ENTRY_RELATION;
    frame->stack.items[0].continuation = NULL;
    frame->result.rel = NULL; frame->result.owned = false;
    frame->result.is_delta = false; frame->result.seg_boundaries = NULL;
    frame->result.seg_count = 0;
    frame->result.kind = WL_COLUMNAR_EVAL_ENTRY_RELATION;
    frame->result.continuation = NULL;
    frame->stack.top = 1;
    owner->source_slot_borrowed = false;
    owner->source_output = NULL;
    owner->source_installed = true;
    return 0;
}

int
wl_columnar_eval_stack_numeric_source_release(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_columnar_eval_plan_numeric_attempt_t *owner)
{
    int rc = wl_columnar_eval_stack_numeric_source_frame(frame, sess, owner,
            owner && owner->source_slot_borrowed);
    if (rc) return rc;
    if (frame->numeric_plan_owner != owner || !owner->source_mode
        || owner->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE
        || owner->source_initialized || owner->source_installed)
        return EBUSY;
    /* The result, possibly owned/nonNULL, stays in the original frame.
     * Its later generic disposal is a separate caller action, not an ACK
     * suffix. On disposal refusal cleanup_finish consumes the handle and
     * transfers authority to session.cleanup_pending for cleanup_retry. */
    frame->numeric_plan_owner = NULL;
    owner->source_slot_borrowed = false;
    owner->source_output = NULL;
    return 0;
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
    if (frame->unary_output.kind == WL_COLUMNAR_EVAL_ENTRY_CONTINUATION)
        (void)eval_entry_dispose(&frame->unary_output);
    if (frame->unary_retired.kind == WL_COLUMNAR_EVAL_ENTRY_CONTINUATION)
        (void)eval_entry_dispose(&frame->unary_retired);
    for (uint32_t i = 0; i < frame->stack.top; i++)
        if (frame->stack.items[i].kind == WL_COLUMNAR_EVAL_ENTRY_CONTINUATION)
            (void)eval_entry_dispose(&frame->stack.items[i]);
}

static int
wl_columnar_eval_stack_cleanup_dispose(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    int result = eval_entry_dispose(&frame->result);
    int output_rc = eval_entry_dispose(&frame->unary_output);
    if (result == 0)
        result = output_rc;
    int retired_rc = eval_entry_dispose(&frame->unary_retired);
    if (result == 0)
        result = retired_rc;
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
    if (!wl_columnar_eval_stack_unary_frame_valid(frame))
        return EINVAL;
    if (frame->numeric_plan_owner
        || frame->filter.stage != WL_COLUMNAR_FILTER_STAGE_INERT
        || frame->unary_phase == WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE
        || frame->unary_phase == WL_COLUMNAR_EVAL_STACK_UNARY_COMMITTED)
        return EBUSY;
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
