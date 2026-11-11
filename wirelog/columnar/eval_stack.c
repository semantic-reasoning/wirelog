/*
 * columnar/eval_stack.c - Columnar operator evaluation stack
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "columnar/internal.h"
#include "columnar/join_pipeline.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef WL_SESSION_TEST_HOOKS
/* Observe real private operator publication before its input is retired. */
void (*wl_columnar_eval_stack_test_physical_publication)(wl_col_session_t *,
    const col_rel_t *, const col_rel_t *);
#endif

static wl_atomic_u64 wl_next_cleanup_receipt_session_generation = 1u;

int
wl_columnar_eval_stack_cleanup_receipt_session_generation_new(uint64_t *out)
{
    if (!out) return EINVAL;
    uint64_t current = atomic_load_explicit(
        &wl_next_cleanup_receipt_session_generation, memory_order_relaxed);
    for (;;) {
        if (!current || current == UINT64_MAX) return EOVERFLOW;
        uint64_t next = current + 1u;
        if (atomic_compare_exchange_weak_explicit(
                &wl_next_cleanup_receipt_session_generation, &current, next,
                memory_order_relaxed, memory_order_relaxed)) {
            *out = current;
            return 0;
        }
    }
}

#ifdef WL_SESSION_TEST_HOOKS
void
wl_columnar_eval_stack_test_receipt_next_session_generation(uint64_t next)
{
    atomic_store_explicit(&wl_next_cleanup_receipt_session_generation, next,
        memory_order_relaxed);
}
#endif

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

/* Dedicated binary owner; old unary state is only an exclusion gate. */
typedef struct wl_columnar_eval_stack_binary_context {
    struct wl_columnar_eval_stack_binary_context *identity;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    wl_col_session_t *session;
    wl_evaluation_control_t *control;
    uint64_t attempt_id;
    eval_entry_t snapshot[2], retired[2], output;
    uint32_t top, cursor;
    unsigned phase;
    int execution_result, cleanup_result;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    bool thread_valid, pin_owned, committed, result_saved, alias_rejected;
} wl_columnar_eval_stack_binary_context_t;

struct wl_columnar_eval_stack_cleanup_frame {
    wl_col_session_t *session;
    wl_columnar_eval_stack_unary_prepared_t *prepared_unary;
    wl_columnar_eval_stack_cleanup_frame_t *next;
    wl_columnar_eval_stack_cleanup_frame_t *pending_prev;
    uint64_t cleanup_receipt_id;
    uint64_t cleanup_receipt_plan_epoch;
    uint32_t cleanup_receipt_index;
    bool cleanup_receipt_index_valid;
    wl_columnar_eval_cleanup_receipt_key_t cleanup_receipt_index_key;
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
    bool unary_scalar_mode, unary_scalar_started, unary_scalar_prepared;
    bool unary_scalar_committed, unary_scalar_for_old;
    wl_columnar_relation_scalar_retirement_t unary_scalar_retirement;
    wl_columnar_relation_scalar_retirement_initializer_t
        unary_scalar_initializer;
    col_rel_t *unary_scalar_init_target;
    bool unary_scalar_cancel_clear, unary_scalar_drained, unary_scalar_busy;
    wl_columnar_filter_context_t filter;
    const void *numeric_plan_owner;
    const void *stream_host;
    eval_entry_t stream_original;
    const col_rel_t *stream_input;
    uint64_t stream_generation;
    uint32_t stream_top;
    unsigned stream_dispatch;
    bool stream_schema;
    wl_columnar_eval_stack_binary_context_t binary;
    wl_columnar_merge_concat_context_t concat;
    wl_columnar_eval_serial_framed_attempt_t framed;
    /* Nonowning exclusive ROW borrow; caller storage remains live to DONE. */
    wl_columnar_relation_row_debt_t *row_debt;
};

/* Native automatic receipts have a stable final address outside the logical
 * frame. Derive this allocation only from an authenticated actual frame. */
typedef enum {
    WL_COLUMNAR_EVAL_STACK_AUTO_UNUSED,
    WL_COLUMNAR_EVAL_STACK_AUTO_PUBLICATION,
    WL_COLUMNAR_EVAL_STACK_AUTO_FINISH,
    WL_COLUMNAR_EVAL_STACK_AUTO_WAIT_CONSUME,
    WL_COLUMNAR_EVAL_STACK_AUTO_WAIT_FINISH,
    WL_COLUMNAR_EVAL_STACK_AUTO_SKIPPED,
    WL_COLUMNAR_EVAL_STACK_AUTO_FINISHED
} wl_columnar_eval_stack_auto_phase_t;

typedef struct wl_columnar_eval_stack_auto {
    struct wl_columnar_eval_stack_auto *identity;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    wl_columnar_eval_stack_unary_prepared_t receipt;
    wl_evaluation_control_t *launch;
    uint64_t launch_identity, launch_id;
    const wl_plan_op_t *operation;
    const wl_columnar_eval_private_bindings_t *saved_binding;
    uint64_t saved_binding_generation;
    uint32_t cursor;
    wl_columnar_eval_stack_auto_phase_t phase;
    int launch_protocol, stop_result;
    bool busy, finish, abort_requested, declined;
} wl_columnar_eval_stack_auto_t;

typedef struct {
    wl_columnar_eval_stack_cleanup_frame_t frame;
    wl_columnar_eval_stack_auto_t automatic;
} wl_columnar_eval_stack_allocation_t;

#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
_Static_assert(offsetof(wl_columnar_eval_stack_allocation_t, frame) == 0,
    "frame must be allocation prefix");
_Static_assert(offsetof(wl_columnar_eval_stack_allocation_t, automatic)
    >= sizeof(wl_columnar_eval_stack_cleanup_frame_t),
    "native receipt must be external to logical frame");
_Static_assert(offsetof(wl_columnar_eval_stack_auto_t, receipt)
    % _Alignof(wl_columnar_eval_stack_unary_prepared_t) == 0,
    "native receipt alignment");
#endif

static wl_columnar_eval_stack_auto_t *
wl_columnar_eval_stack_automatic(wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return &((wl_columnar_eval_stack_allocation_t *)frame)->automatic;
}

bool
wl_columnar_eval_stack_auto_present(
    const wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    const wl_columnar_eval_stack_allocation_t *allocation = (const void *)frame;
    return frame->prepared_unary == &allocation->automatic.receipt;
}

/* Cold binding only: keep the saturated operator/source getters unchanged.
 * A parent exists before its child claims and after the child's real ACK. */
static int
wl_columnar_eval_stack_framed_admit(
    const wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner)
{
    if (frame && frame->prepared_unary) return EBUSY;
    if (!frame || frame->identity != frame || !frame->session
        || frame->stack.top > COL_STACK_MAX)
        return EINVAL;
    const wl_columnar_eval_serial_framed_attempt_t *ctx = &frame->framed;
    if (!ctx->identity) return 0;
    if (ctx->identity != ctx) return EINVAL;
    if (owner != &ctx->child) return EBUSY;
    return ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_BEGIN
           || (ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
           && ctx->pending_action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD_BEGIN)
        ? 0 : EBUSY;
}

int
wl_columnar_eval_stack_cleanup_begin(wl_col_session_t *sess,
    wl_columnar_eval_stack_cleanup_frame_t **out)
{
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    uint64_t bytes;
    if (!sess || !out || *out)
        return EINVAL;
    if (sess->cleanup_active &&
        (sess->cleanup_active->prepared_unary ||
        sess->cleanup_active->stream_dispatch)) return EBUSY;
    if (sess->cleanup_pending)
        return EBUSY;
    if (sess->cleanup_active_count >= WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES
        || sess->cleanup_pending_count
        >= WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES
        - sess->cleanup_active_count)
        return ENOBUFS;
    if (!wl_columnar_memory_size_add(sess->cleanup_reserved_bytes,
        sizeof(wl_columnar_eval_stack_allocation_t), &bytes))
        return EOVERFLOW;
    wl_columnar_memory_reservation_init(&reservation);
    if (sess->memory_governor) {
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(
                wl_columnar_memory_governor_ref_get(sess->memory_governor),
                sizeof(wl_columnar_eval_stack_allocation_t), &reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY) {
            if (status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED)
                sess->memory_budget_denied = true;
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED ? ENOSPC
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW ? EOVERFLOW
                : EINVAL;
        }
    }
    wl_columnar_eval_stack_allocation_t *allocation =
        calloc(1, sizeof(*allocation));
    if (!allocation) {
        if (sess->memory_governor)
            (void)wl_columnar_memory_rollback(&reservation);
        return ENOMEM;
    }
    frame = &allocation->frame;
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
    wl_columnar_eval_stack_auto_t *automatic =
        wl_columnar_eval_stack_automatic(frame);
    automatic->identity = automatic; automatic->frame = frame;
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
 * ENTRY104<=112, SETUP135<=144, RESERVE156<=192, ALLOC128<=144,
 * ZERO202<=208, TOKEN125<=128 (self/IDLE/segment-owner stores3), MOVE161<=176, COMMIT104<=128,
 * EXTERNAL133<=144 including BOTH T29 tails on one debit. Six terminal
 * publication slots include active_count arithmetic/store2, no later hook.
 * ZERO counts16*(address+store+increment arithmetic/store+test)=80 plus
 * setup/transitions, tail and NEXT refused debit through full F64.
 * Nested reserve last-poll return84+status19+T29+R10+C13+refusal3+F64=222;
 * nested commit final4+rc3+check6+C13+refusal3+F64=93. Conservative240.
 * Read-only invalid/first-refused bootstrap<=29 may run at consumed0.
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
unsigned wl_columnar_eval_stack_test_fail_cleanup_dispose;

size_t
wl_columnar_eval_stack_test_frame_size(void)
{
    return sizeof(wl_columnar_eval_stack_cleanup_frame_t);
}

size_t
wl_columnar_eval_stack_test_allocation_size(void)
{
    return sizeof(wl_columnar_eval_stack_allocation_t);
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
    return rc ? rc : wl_evaluation_control_charge_attempt(control, id,
               work);
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
    if (sess->cleanup_active &&
        (sess->cleanup_active->prepared_unary ||
        sess->cleanup_active->stream_dispatch)) return EBUSY;
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
            sizeof(wl_columnar_eval_stack_allocation_t),
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
            wl_columnar_memory_governor_ref_get(retained_ref),
            sizeof(wl_columnar_eval_stack_allocation_t),
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
                WL_COLUMNAR_EVAL_STACK_RESERVE_PHASE,
                sizeof(wl_columnar_eval_stack_allocation_t));
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
    frame =
        wl_columnar_eval_stack_attempt_allocate(
        sizeof(wl_columnar_eval_stack_allocation_t));
    verdict = frame ? 0 : ENOMEM;
    rc = wl_columnar_eval_stack_attempt_tail(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_ALLOCATION_PHASE,
            sizeof(wl_columnar_eval_stack_allocation_t));
    if (rc > 0 || !verdict) {
        if (rc)
            goto fail;
    }
    if (verdict) {
        rc = verdict;
        goto fail;
    }
    for (size_t offset =
        0; offset < sizeof(wl_columnar_eval_stack_allocation_t);
        ) {
        rc = wl_columnar_eval_stack_attempt_charge(control, attempt_id,
                WL_COLUMNAR_EVAL_STACK_ZERO_WORK);
        if (rc)
            goto fail;
        size_t count = sizeof(wl_columnar_eval_stack_allocation_t) - offset;
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
            WL_COLUMNAR_EVAL_STACK_TOKEN_PHASE,
            sizeof(wl_columnar_eval_stack_allocation_t));
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
            WL_COLUMNAR_EVAL_STACK_MOVED_PHASE,
            sizeof(wl_columnar_eval_stack_allocation_t));
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
    wl_columnar_eval_stack_auto_t *automatic =
        wl_columnar_eval_stack_automatic(frame);
    automatic->identity = automatic; automatic->frame = frame;
    frame->session = sess;
    frame->next = sess->cleanup_active;
    rc = wl_columnar_eval_stack_attempt_tail(control, attempt_id,
            WL_COLUMNAR_EVAL_STACK_COMMITTED_PHASE,
            sizeof(wl_columnar_eval_stack_allocation_t));
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

int
wl_columnar_eval_stack_numeric_setup_nonstream(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner)
{
    if (!owner) return EINVAL;
    int rc = wl_columnar_eval_stack_numeric_owner(frame, owner);
    if (rc) return rc;
    return frame->stream_host ? EBUSY : 0;
}

bool
wl_columnar_eval_stack_frame_mutation_blocked(
    const wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return frame && frame->prepared_unary;
}

/* Fold exclusion into the existing real ownership proof rather than stacking
 * another full validator on tight unary/retirement envelopes. */
int
wl_columnar_eval_stack_numeric_writable(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner)
{
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)) return EINVAL;
    if (frame->prepared_unary) return EBUSY;
    if (frame->numeric_plan_owner != owner) return EBUSY;
    if (owner && !wl_columnar_eval_plan_numeric_owner_valid(owner,
        frame)) return EINVAL;
    return 0;
}

bool
wl_columnar_eval_stack_metadata_result_disjoint(
    const wl_columnar_eval_stack_cleanup_frame_t *frame, const int *out)
{
    uintptr_t base = (uintptr_t)frame, dest = (uintptr_t)out;
    return dest < base ? base - dest >= sizeof(*out)
                      : dest - base >=
           sizeof(wl_columnar_eval_stack_allocation_t);
}

int
wl_columnar_eval_stack_numeric_metadata_empty(
    const wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner,
    const int *out)
{
    if (!frame || frame->identity != frame ||
        frame->numeric_plan_owner != owner)
        return EINVAL;
    if (!wl_columnar_eval_stack_metadata_result_disjoint(frame,
        out)) return EINVAL;
    if (frame->stack.top ||
        !wl_columnar_eval_stack_unary_entry_inert(&frame->result)
        || frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_IDLE
        || frame->filter.stage != WL_COLUMNAR_FILTER_STAGE_INERT
        || frame->binary.phase || frame->concat.identity || frame->stream_host)
        return EBUSY;
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
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)) return EINVAL;
    if (frame->filter.join_metadata_busy || frame->prepared_unary) return EBUSY;
    if (frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE
        || frame->session->cleanup_active != frame
        || !wl_columnar_eval_stack_unary_thread_valid(frame))
        return EINVAL;
    if (frame->stream_host && !frame->stream_dispatch) return EBUSY;
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

static int
wl_columnar_eval_stack_unary_begin_attempt_body(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_evaluation_control_t *control, uint64_t attempt_id);

int
wl_columnar_eval_stack_unary_begin_attempt_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_evaluation_control_t *control, uint64_t attempt_id, const void *owner)
{
    int authority = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (authority) return authority;
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)) return EINVAL;
    if (frame->stream_host) return EBUSY;
    if (frame->filter.stage != WL_COLUMNAR_FILTER_STAGE_INERT) return EBUSY;
    return wl_columnar_eval_stack_unary_begin_attempt_body(frame, control,
               attempt_id);
}

int
wl_columnar_eval_stack_unary_begin_attempt_context_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_filter_context_t *context, wl_evaluation_control_t *control,
    uint64_t attempt_id, const void *owner)
{
    int authority = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (authority) return authority;
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)) return EINVAL;
    if (context != &frame->filter) return EINVAL;
    if (context->identity != context || context->frame != frame
        || context->session != frame->session
        || frame->session->cleanup_active != frame
        || context->stage != WL_COLUMNAR_FILTER_STAGE_RUNNING
        || !context->busy || !context->thread_valid || !context->pin_owned
        || context->unary_started || !control || !attempt_id
        || context->control != control || context->attempt_id != attempt_id)
        return EINVAL;
#if defined(WL_HAVE_C11_THREADS)
    if (!thrd_equal(context->owner_thread, thrd_current())) return EINVAL;
#elif defined(_WIN32) || defined(_WIN64)
    if (context->owner_thread != GetCurrentThreadId()) return EINVAL;
#else
    if (!pthread_equal(context->owner_thread, pthread_self())) return EINVAL;
#endif
    return wl_columnar_eval_stack_unary_begin_attempt_body(frame, control,
               attempt_id);
}

static int
wl_columnar_eval_stack_unary_begin_attempt_body(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_evaluation_control_t *control, uint64_t attempt_id)
{
    if (frame->unary_scalar_busy || frame->unary_scalar_init_target
        || frame->unary_scalar_cancel_clear ||
        frame->unary_scalar_drained) return EBUSY;
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
    int authority = wl_columnar_eval_stack_numeric_writable(frame, owner);
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
    int authority = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (authority) return authority;
    if (!out_slot)
        return EINVAL;
    if (frame->unary_scalar_busy || frame->unary_scalar_init_target ||
        frame->unary_scalar_cancel_clear
        || frame->unary_scalar_drained) return EBUSY;
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
    if (frame->stream_host && candidate == frame->stream_original.rel)
        goto alias;
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

static int
wl_columnar_eval_stack_unary_commit_attempt_body(
    wl_columnar_eval_stack_cleanup_frame_t *, bool *, bool);

int
wl_columnar_eval_stack_unary_commit_attempt_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, bool *committed,
    const void *owner)
{
    int authority = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (authority) return authority;
    if (frame->stream_host && !frame->stream_dispatch) return EBUSY;
    return wl_columnar_eval_stack_unary_commit_attempt_body(frame, committed,
               false);
}

int
wl_columnar_eval_stack_unary_commit_numeric_join_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, bool *committed,
    const void *owner)
{
    if (wl_columnar_eval_stack_unary_frame_valid(frame)
        && (frame->filter.join_metadata_busy ||
        frame->prepared_unary)) return EBUSY;
    if (committed) *committed = false;
    int rc = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (rc) return rc;
    if (frame->stream_host) return EBUSY;
    bool delta = false;
    rc = wl_columnar_join_pipeline_numeric_delta_owned(owner, frame, &delta);
    if (rc) return rc;
    const wl_columnar_eval_plan_numeric_attempt_t *numeric = owner;
    if (delta != (!numeric->join_host_tail_count
        && !numeric->plan->ops[numeric->cursor].project_count
        && frame->unary_snapshot.is_delta)) return EINVAL;
    return wl_columnar_eval_stack_unary_commit_attempt_body(frame, committed,
               delta);
}

int
wl_columnar_eval_stack_unary_commit_join_attempt_context_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_filter_context_t *context, bool *committed, const void *owner)
{
    int authority = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (authority) return authority;
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)) return EINVAL;
    if (context != &frame->filter) return EINVAL;
    if (context->identity != context || context->frame != frame
        || context->session != frame->session
        || frame->session->cleanup_active != frame
        || context->stage != WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE
        || !context->busy || !context->thread_valid || !context->pin_owned
        || !context->unary_started || !context->join_decision_pending
        || !context->control || !context->attempt_id
        || context->control != frame->unary_control
        || context->attempt_id != frame->unary_attempt_id)
        return EINVAL;
#if defined(WL_HAVE_C11_THREADS)
    if (!thrd_equal(context->owner_thread, thrd_current())) return EINVAL;
#elif defined(_WIN32) || defined(_WIN64)
    if (context->owner_thread != GetCurrentThreadId()) return EINVAL;
#else
    if (!pthread_equal(context->owner_thread, pthread_self())) return EINVAL;
#endif
    bool delta = context->join_result_is_delta;
    return wl_columnar_eval_stack_unary_commit_attempt_body(frame, committed,
               delta);
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_stack_test_anti_metadata_committed)(
    const eval_entry_t *, const eval_entry_t *);
#endif

enum {
    WL_COLUMNAR_ANTI_META_CONTEXT_WORK = 224,
    WL_COLUMNAR_ANTI_META_STATE_WORK = 192,
    WL_COLUMNAR_ANTI_META_SCRATCH_WORK = 128,
    WL_COLUMNAR_ANTI_META_CUTOFF_WORK = 256
};
typedef char wl_columnar_anti_metadata_fits_t[
        WL_COLUMNAR_ANTI_META_CUTOFF_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM ? 1 : -1];

/* Original fixed validation is split from per-record scratch checks and the
* terminal metadata cutoff. Each original debit covers only that region;
* existing FIRST_FINISH256 pays the unchanged segment-only disposer. No
* relation or scalar retirement authority enters unary_retired here.
* Context224 covers fixed identity/op/phase/thread validation; STATE192
* covers inert-slot/reader/temporary checks; each SCRATCH128 covers one of
* three fixed records. CUTOFF256 covers original entry+snapshot comparisons,
* final tail, fixed transfer and the filter/status caller suffix. No region
* scans segments or rows. These logical envelopes do not bound allocator or
* instrumentation callback latency, and no whole-call quantum is claimed. */
static int
wl_columnar_eval_stack_anti_keep_all_body(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_filter_context_t *ctx, bool *committed,
    const wl_columnar_eval_plan_numeric_attempt_t *r)
{
    int rc = wl_columnar_eval_stack_attempt_charge(r->control, r->attempt_id,
            WL_COLUMNAR_ANTI_META_CONTEXT_WORK);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_attempt_charge(r->control, r->attempt_id,
            WL_COLUMNAR_ANTI_META_STATE_WORK);
    if (rc) return rc;
    if (frame->unary_scalar_mode || frame->unary_scalar_started
        || frame->unary_scalar_prepared || frame->unary_scalar_committed
        || frame->unary_committed
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_output)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_retired)
        || ctx->source_initialized || ctx->join_right_initialized
        || ctx->source_bare || ctx->join_right_bare || ctx->join_initialized
        || ctx->batch_initialized || ctx->program || ctx->scalar_program ||
        ctx->heap || ctx->admitted
        || ctx->ref_held || ctx->governor || ctx->output || ctx->private_output
        || ctx->table.programs || ctx->table.scalars ||
        ctx->table.scalar_allocated
        || ctx->table.scalar_admitted || ctx->table.scalar_initialized_slots
        || ctx->table.scalar_committed || ctx->table.allocated ||
        ctx->table.admitted
        || ctx->table.ref_held) return EBUSY;
    if ((ctx->source_empty_reader.identity &&
        (ctx->source_empty_reader.identity != &ctx->source_empty_reader
        || ctx->source_empty_reader.phase !=
        WL_COLUMNAR_RELATION_EMPTY_SOURCE_DRAINED))
        || (ctx->join_empty_reader.identity &&
        (ctx->join_empty_reader.identity != &ctx->join_empty_reader
        || ctx->join_empty_reader.phase !=
        WL_COLUMNAR_RELATION_EMPTY_SOURCE_DRAINED))) return EBUSY;
    for (unsigned i = 0; i < WL_COLUMNAR_JOIN_SCRATCH_COUNT; ++i) {
        rc = wl_columnar_eval_stack_attempt_charge(r->control, r->attempt_id,
                WL_COLUMNAR_ANTI_META_SCRATCH_WORK);
        if (rc) return rc;
        const wl_columnar_join_scratch_t *scratch = &ctx->join_scratch[i];
        if (scratch->initialized || scratch->allocated || scratch->admitted
            || scratch->ref_held || scratch->physical || scratch->governor)
            return EBUSY;
    }
    rc = wl_columnar_eval_stack_attempt_charge(r->control, r->attempt_id,
            WL_COLUMNAR_ANTI_META_CUTOFF_WORK);
    if (rc) return rc;
    if (!wl_columnar_eval_stack_unary_original_matches(frame)
        || !wl_columnar_relation_snapshot_valid(ctx->join_input_snapshot)
        || !wl_columnar_relation_snapshot_equal(ctx->join_input_snapshot,
        wl_columnar_relation_snapshot(frame->unary_snapshot.rel)))
        return EINVAL;
    rc = wl_columnar_eval_stack_attempt_tail(r->control, r->attempt_id,
            WL_COLUMNAR_EVAL_STACK_UNARY_COMMIT_CUTOFF_PHASE, frame->unary_top);
    if (rc) return rc;
    /* Irreversible, callback-free transfer: historical snapshot owns nothing. */
    eval_entry_t *entry = &frame->stack.items[frame->unary_index];
    frame->unary_retired.seg_boundaries = entry->seg_boundaries;
    frame->unary_retired.seg_count = entry->seg_count;
    entry->seg_boundaries = NULL; entry->seg_count = 0; entry->is_delta = false;
    frame->unary_committed = true;
    frame->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_COMMITTED;
    *committed = true;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_stack_test_anti_metadata_committed)
        wl_columnar_eval_stack_test_anti_metadata_committed(
            &frame->unary_retired, entry);
#endif
    return 0;
}

int
wl_columnar_eval_stack_unary_commit_anti_keep_all_context_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_filter_context_t *ctx, bool *committed, const void *owner)
{
    if (!owner || !committed) return EINVAL;
    int rc = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (rc) return rc;
    if (frame->stream_host || ctx != &frame->filter) return EBUSY;
    /* Refuse recursion/duplicate publication before old-control access. */
    if (ctx->join_metadata_busy || ctx->committed
        || frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE)
        return EBUSY;
    const wl_columnar_eval_plan_numeric_attempt_t *r = owner;
    bool child = r->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD
        || r->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD
        || ((r->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING
        || r->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING)
        && (r->pending_action == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD
        || r->pending_action ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD));
    if (!child || r->cursor >= r->end || !r->plan
        || r->plan->ops[r->cursor].op != WL_PLAN_OP_ANTIJOIN
        || ctx->join_operation != &r->plan->ops[r->cursor]
        || ctx->identity != ctx || ctx->frame != frame
        || ctx->session != frame->session ||
        frame->session->cleanup_active != frame
        || ctx->stage != WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE || !ctx->busy
        || !ctx->thread_valid || !ctx->pin_owned || !ctx->unary_started
        || !ctx->join_decision_pending || ctx->committed ||
        ctx->execution_result
        || ctx->join_disposition != WL_COLUMNAR_JOIN_DISPOSITION_ANTI_KEEP_ALL
        || ctx->control != r->control || ctx->attempt_id != r->attempt_id
        || frame->unary_control != r->control ||
        frame->unary_attempt_id != r->attempt_id)
        return EINVAL;
#if defined(WL_HAVE_C11_THREADS)
    if (!thrd_equal(ctx->owner_thread, thrd_current())) return EINVAL;
#elif defined(_WIN32) || defined(_WIN64)
    if (ctx->owner_thread != GetCurrentThreadId()) return EINVAL;
#else
    if (!pthread_equal(ctx->owner_thread, pthread_self())) return EINVAL;
#endif
    rc = wl_columnar_eval_stack_unary_forward(frame);
    if (rc) return rc;
    ctx->join_metadata_busy = true;
    rc = wl_columnar_eval_stack_anti_keep_all_body(frame, ctx, committed, r);
    ctx->join_metadata_busy = false;
    return rc;
}

static int
wl_columnar_eval_stack_unary_commit_attempt_body(
    wl_columnar_eval_stack_cleanup_frame_t *frame, bool *committed,
    bool publication_is_delta)
{
    if (wl_columnar_eval_stack_unary_frame_valid(frame)
        && (frame->filter.join_metadata_busy ||
        frame->prepared_unary)) return EBUSY;
    if (!committed)
        return EINVAL;
    *committed = false;
    if (frame->unary_scalar_busy || frame->unary_scalar_init_target ||
        frame->unary_scalar_cancel_clear
        || frame->unary_scalar_drained) return EBUSY;
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
    if (frame->unary_scalar_mode && frame->unary_snapshot.owned
        && (!frame->unary_scalar_prepared || !frame->unary_scalar_started
        || !frame->unary_scalar_retirement.ready
        || frame->unary_scalar_retirement.storage.relation
        != frame->unary_snapshot.rel)) return EBUSY;
    eval_entry_t *entry = &frame->stack.items[frame->unary_index];
    wl_columnar_eval_stack_unary_entry_copy(&frame->unary_retired, entry);
    wl_columnar_eval_stack_unary_entry_copy(entry, output);
    entry->is_delta = publication_is_delta;
    wl_columnar_eval_stack_unary_entry_clear(output);
    frame->unary_committed = true;
    frame->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_COMMITTED;
    *committed = true;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_stack_test_physical_publication)
        wl_columnar_eval_stack_test_physical_publication(frame->session,
            frame->unary_retired.rel, entry->rel);
#endif
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
    if (frame->filter.join_metadata_busy || frame->unary_scalar_mode)
        return EBUSY;
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
    int rc = wl_columnar_eval_stack_numeric_writable(frame, NULL);
    if (rc) return rc;
    return wl_columnar_eval_stack_unary_finish_body(frame, initiating_result,
               true);
}

/* Each whole-scalar clear is its own actual256 action. Initialization uses
 * the separately paid parent-bound BOOT/ZERO/BIND/RESET sibling; raw scalar
 * primitive source/tariffs remain unchanged. No old187 double-clear credit. */
static int
wl_columnar_eval_stack_unary_scalar_authority(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner)
{
    if (!owner) return EINVAL;
    int rc = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (rc) return rc;
    if (frame->stream_host || frame->session->cleanup_active != frame
        || frame->filter.stage != WL_COLUMNAR_FILTER_STAGE_INERT) return EBUSY;
    return 0;
}

static int
wl_columnar_eval_stack_unary_scalar_initialize(
    wl_columnar_eval_stack_cleanup_frame_t *frame, col_rel_t *target,
    bool for_old, wl_evaluation_control_t *control, uint64_t id)
{
    wl_columnar_relation_scalar_retirement_initializer_t *i =
        &frame->unary_scalar_initializer;
    if (i->busy) return EBUSY;
    if (!frame->unary_scalar_init_target) {
        if (!target || frame->unary_scalar_started) return EINVAL;
        frame->unary_scalar_init_target = target;
        frame->unary_scalar_for_old = for_old;
    }
    if (frame->unary_scalar_init_target != target
        || frame->unary_scalar_for_old != for_old) return EINVAL;
    if (!i->identity)
        return wl_columnar_relation_scalar_retirement_init_begin(i, frame,
                   &frame->unary_scalar_retirement, target, control, id);
    if (i->phase == WL_COLUMNAR_RELATION_SCALAR_INIT_DONE) {
        if (!frame->unary_scalar_started) return EINVAL;
        int rc = wl_columnar_relation_scalar_retirement_init_reset(i, frame,
                control, id);
        if (!rc) frame->unary_scalar_init_target = NULL;
        return rc;
    }
    bool begun = false;
    int rc = wl_columnar_relation_scalar_retirement_init_step(i, frame,
            &frame->unary_scalar_retirement, target, control, id, &begun);
    /* Actual begun handoff precedes any future RESET hook/refusal. */
    if (!rc && begun) frame->unary_scalar_started = true;
    return rc;
}

static int
wl_columnar_eval_stack_unary_prepare_scalar_retirement_owned_body(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner,
    bool *done)
{
    if (!done) return EINVAL;
    *done = false;
    int rc = wl_columnar_eval_stack_unary_scalar_authority(frame, owner);
    if (rc) return rc;
    if (frame->unary_scalar_initializer.busy || frame->unary_scalar_cancel_clear
        || frame->unary_scalar_drained) return EBUSY;
    const wl_columnar_eval_plan_numeric_attempt_t *numeric = owner;
    if (frame->unary_control != numeric->control
        || frame->unary_attempt_id != numeric->attempt_id) return EINVAL;
    rc = wl_columnar_eval_stack_unary_forward(frame);
    if (rc) return rc;
    const eval_entry_t *entry = &frame->unary_snapshot;
    if (entry->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION || entry->continuation
        || entry->seg_boundaries || entry->seg_count ||
        !entry->rel) return ENOTSUP;
    frame->unary_scalar_mode = true;
    if (!entry->owned) {
        rc = wl_evaluation_control_charge_attempt(numeric->control,
                numeric->attempt_id, 1);
        if (rc) return rc;
        frame->unary_scalar_prepared = true; *done = true;
        return 0;
    }
    if (!frame->unary_scalar_started || frame->unary_scalar_init_target)
        return wl_columnar_eval_stack_unary_scalar_initialize(frame, entry->rel,
                   true, numeric->control, numeric->attempt_id);
    rc = wl_evaluation_control_charge_attempt(numeric->control,
            numeric->attempt_id, 1);
    if (rc) return rc;
    rc = wl_columnar_relation_scalar_retirement_prepare(
        &frame->unary_scalar_retirement, numeric->control, numeric->attempt_id);
    if (rc == EAGAIN) return 0;
    if (rc) return rc;
    frame->unary_scalar_prepared = true; *done = true;
    return 0;
}

static int
wl_columnar_eval_stack_unary_finish_scalar_funded_owned_body(
    wl_columnar_eval_stack_cleanup_frame_t *frame, int initiating_result,
    const void *owner, wl_evaluation_control_t *funding, uint64_t funding_id,
    int *semantic, bool *done)
{
    if (!done || !semantic) return EINVAL;
    *done = false;
    int rc = wl_columnar_eval_stack_unary_scalar_authority(frame, owner);
    if (rc) return rc;
    if (frame->unary_scalar_initializer.busy) return EBUSY;
    const wl_columnar_eval_plan_numeric_attempt_t *numeric = owner;
    if (!funding || funding == numeric->control || !funding_id
        || wl_evaluation_control_active_attempt_id(funding) !=
        funding_id) return EINVAL;
    if (frame->unary_phase == WL_COLUMNAR_EVAL_STACK_UNARY_IDLE) return EINVAL;
    if (frame->unary_phase == WL_COLUMNAR_EVAL_STACK_UNARY_DONE) {
        *semantic = wl_columnar_eval_stack_unary_result(frame); *done = true;
        return 0;
    }
    bool first = frame->unary_phase == WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE
        || frame->unary_phase == WL_COLUMNAR_EVAL_STACK_UNARY_COMMITTED;
    if ((!first && frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_RETAINED)
        || (first &&
        !wl_columnar_eval_stack_unary_thread_valid(frame))) return EINVAL;
    if (frame->unary_committed && frame->unary_retired.owned
        && !frame->unary_scalar_prepared) return EINVAL;
    if (first) {
        rc = wl_evaluation_control_charge_attempt(funding, funding_id,
                4u*COL_STACK_MAX+16u);
        if (rc) return rc;
        frame->unary_scalar_mode = true;
        if (!frame->unary_result_saved) {
            frame->unary_first_result = initiating_result;
            frame->unary_result_saved = true;
        }
        if (!frame->unary_committed) (void)
            wl_columnar_eval_stack_unary_output_sanitize(frame);
        wl_columnar_eval_stack_unary_binding_clear(frame);
        frame->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_RETAINED;
        *semantic = wl_columnar_eval_stack_unary_result(frame); return 0;
    }
    *semantic = wl_columnar_eval_stack_unary_result(frame);
    wl_columnar_relation_scalar_retirement_initializer_t *i =
        &frame->unary_scalar_initializer;
    if (!frame->unary_committed && frame->unary_scalar_for_old) {
        if (frame->unary_scalar_init_target) {
            /* Pre-BIND cancellation owns no writer. A DONE receipt was already
             * handed to started and must survive its separate RESET action. */
            if (i->identity) rc =
                    wl_columnar_relation_scalar_retirement_init_reset(i, frame,
                        funding, funding_id);
            else rc = wl_evaluation_control_charge_attempt(funding, funding_id,
                        128);
            if (rc) return rc;
            frame->unary_scalar_init_target = NULL;
            if (!frame->unary_scalar_started) frame->unary_scalar_for_old =
                    false;
            return 0;
        }
        if (frame->unary_scalar_cancel_clear) {
            rc = wl_evaluation_control_charge_attempt(funding, funding_id, 256);
            if (rc) return rc;
            memset(&frame->unary_scalar_retirement, 0,
                sizeof(frame->unary_scalar_retirement));
            frame->unary_scalar_started = frame->unary_scalar_prepared = false;
            frame->unary_scalar_for_old = frame->unary_scalar_cancel_clear =
                false;
            return 0;
        }
        if (!frame->unary_scalar_started) return EINVAL;
        rc = wl_columnar_relation_scalar_retirement_release_claims_controlled(
            &frame->unary_scalar_retirement, funding, funding_id);
        if (rc) return rc;
        /* No hook separates released claims/invalid READY from CANCEL_CLEAR. */
        frame->unary_scalar_cancel_clear = true;
        frame->unary_scalar_prepared = false;
        return 0;
    }
    if (frame->unary_scalar_drained) {
        rc = wl_evaluation_control_charge_attempt(funding, funding_id, 256);
        if (rc) return rc;
        memset(&frame->unary_scalar_retirement, 0,
            sizeof(frame->unary_scalar_retirement));
        frame->unary_scalar_started = frame->unary_scalar_committed = false;
        frame->unary_scalar_for_old = frame->unary_scalar_drained = false;
        frame->unary_cleanup_result = 0;
        frame->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_DONE;
        *semantic = wl_columnar_eval_stack_unary_result(frame); *done = true;
        return 0;
    }
    eval_entry_t *entry =
        frame->unary_committed ? &frame->unary_retired : &frame->unary_output;
    if (!frame->unary_scalar_started || frame->unary_scalar_init_target) {
        if (entry->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
            || entry->continuation) return ENOTSUP;
        if (entry->seg_boundaries || entry->seg_count) {
            if (frame->unary_scalar_started ||
                frame->unary_scalar_init_target) return EBUSY;
            rc = wl_evaluation_control_charge_attempt(funding, funding_id, 128);
            if (rc) return rc;
            /* Segment storage belongs to the entry even when its relation
             * is borrowed. Keep payload and first-result ownership intact. */
            free(entry->seg_boundaries);
            entry->seg_boundaries = NULL; entry->seg_count = 0;
            return 0;
        }
        if (!entry->owned || !entry->rel) {
            if (frame->unary_scalar_init_target) return EINVAL;
            rc = wl_evaluation_control_charge_attempt(funding, funding_id, 128);
            if (rc) return rc;
            wl_columnar_eval_stack_unary_entry_clear(entry);
            frame->unary_cleanup_result = 0;
            frame->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_DONE;
            *semantic = wl_columnar_eval_stack_unary_result(frame);
            *done = true; return 0;
        }
        return wl_columnar_eval_stack_unary_scalar_initialize(frame, entry->rel,
                   false, funding, funding_id);
    }
    rc = wl_evaluation_control_charge_attempt(funding, funding_id, 1);
    if (rc) return rc;
    if (!frame->unary_scalar_committed) {
        rc =
            wl_columnar_relation_scalar_retirement_prepare(
            &frame->unary_scalar_retirement, funding, funding_id);
        if (rc == EAGAIN) return 0;
        if (rc) {
            frame->unary_cleanup_result = rc;
            *semantic = wl_columnar_eval_stack_unary_result(frame); return rc;
        }
        rc =
            wl_columnar_relation_scalar_retirement_commit(
            &frame->unary_scalar_retirement);
        if (rc) return rc;
        frame->unary_scalar_committed = true;
        wl_columnar_eval_stack_unary_entry_clear(entry);
        return 0;
    }
    rc =
        wl_columnar_relation_scalar_retirement_step(
        &frame->unary_scalar_retirement, funding, funding_id);
    if (rc == EAGAIN) return 0;
    if (rc) {
        frame->unary_cleanup_result = rc;
        *semantic = wl_columnar_eval_stack_unary_result(frame); return rc;
    }
    frame->unary_scalar_drained = true; frame->unary_cleanup_result = 0;
    return 0;
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
    if (frame->prepared_unary || frame->filter.join_metadata_busy ||
        frame->unary_scalar_busy) return EBUSY;
    if (frame->numeric_plan_owner != owner) return EBUSY;
    if (owner && !wl_columnar_eval_plan_numeric_owner_valid(owner, frame))
        return EINVAL;
    if (frame->stream_host
        && (frame->stream_dispatch != WL_COLUMNAR_JOIN_STREAM_STEP
        || frame->filter.stage != WL_COLUMNAR_FILTER_STAGE_RESET
        || !frame->filter.busy)) return EBUSY;
    if (frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_DONE
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_output)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_retired)
        || frame->unary_scalar_started || frame->unary_scalar_committed
        || frame->unary_scalar_init_target ||
        frame->unary_scalar_initializer.identity
        || frame->unary_scalar_cancel_clear || frame->unary_scalar_drained)
        return EBUSY;
    wl_columnar_eval_stack_unary_binding_clear(frame);
    frame->unary_index = 0;
    frame->unary_top = 0;
    frame->unary_committed = false;
    frame->unary_result_saved = false;
    frame->unary_first_result = 0;
    frame->unary_cleanup_result = 0;
    frame->unary_alias_rejected = false;
    frame->unary_scalar_mode = frame->unary_scalar_prepared = false;
    frame->unary_scalar_for_old = false;
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
    if (frame->prepared_unary) return EBUSY;
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
wl_columnar_eval_stack_filter_context_nonstream_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_filter_context_t **out, const void *owner)
{
    if (!frame || frame->stream_host) return EBUSY;
    return wl_columnar_eval_stack_filter_context_owned(frame, out, owner);
}

int
wl_columnar_eval_stack_filter_context_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_filter_context_t **out, const void *owner)
{
    if (!out || !wl_columnar_eval_stack_unary_frame_valid(frame))
        return EINVAL;
    if (frame->prepared_unary) return EBUSY;
    if (frame->numeric_plan_owner != owner) return EBUSY;
    if (owner && !wl_columnar_eval_plan_numeric_owner_valid(owner, frame))
        return EINVAL;
    if (frame->stream_host && !frame->stream_dispatch) return EBUSY;
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

static int
wl_columnar_eval_stack_operator_stack_body(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    eval_stack_t **out, const void *owner, bool consumer)
{
    if (frame && frame->prepared_unary) return EBUSY;
    if (frame && frame->unary_scalar_busy) return EBUSY;
    int authority = wl_columnar_eval_stack_numeric_owner(frame, owner);
    if (authority) return authority;
    if (frame->stream_host && !consumer) return EBUSY;
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
    if (!frame->stack.top) {
        const wl_columnar_eval_plan_numeric_attempt_t *record = owner;
        if (!record || !record->composed_mode
            || record->phase !=
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_LFTJ_CHILD)
            return EINVAL;
        *out = &frame->stack;
        return 0;
    }
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
wl_columnar_eval_stack_operator_stack_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    eval_stack_t **out, const void *owner)
{
    return wl_columnar_eval_stack_operator_stack_body(frame, sess, out, owner,
               false);
}

int
wl_columnar_eval_stack_consumer_stack_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    eval_stack_t **out, const void *owner, const wl_plan_op_t *op)
{
    int rc = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (rc) return rc;
    if (frame->stream_host
        && (frame->stream_dispatch != WL_COLUMNAR_JOIN_STREAM_START
        || !wl_columnar_join_pipeline_stream_authorized(owner, frame,
        frame->stream_host, WL_COLUMNAR_JOIN_STREAM_START, op,
        frame->stream_input, frame->stream_generation, frame->stream_schema)))
        return EBUSY;
    return wl_columnar_eval_stack_operator_stack_body(frame, sess, out, owner,
               true);
}

int
wl_columnar_eval_stack_stream_enter_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner,
    const void *host, const col_rel_t *input, uint64_t generation,
    bool schema, bool delta)
{
    int rc = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (rc) return rc;
    if (!wl_columnar_join_pipeline_stream_authorized(owner, frame, host,
        WL_COLUMNAR_JOIN_STREAM_ENTER, NULL, input, generation, schema))
        return EINVAL;
    eval_stack_t *stack = NULL;
    rc = wl_columnar_eval_stack_operator_stack_owned(frame, frame->session,
            &stack, owner);
    if (rc) return rc;
    if (frame->stream_host || frame->binary.phase || frame->concat.phase
        || !stack->top || !input || input->storage_owner != input
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->stream_original))
        return EBUSY;
    eval_entry_t *entry = &stack->items[stack->top - 1];
    if (entry->seg_boundaries || entry->seg_count
        || entry->rel == input || entry->rel->compound_kind
        != WIRELOG_COMPOUND_KIND_NONE) return ENOTSUP;
    const wl_columnar_eval_plan_numeric_attempt_t *numeric = owner;
    rc = wl_evaluation_control_charge_attempt(numeric->control,
            numeric->attempt_id, 128);
    if (rc) return rc;
    /* One serialized move. No callback, poll or allocation in this region. */
    frame->stream_original = *entry;
    wl_columnar_eval_stack_unary_entry_clear(entry);
    entry->rel = (col_rel_t *)input; entry->is_delta = delta;
    frame->stream_input = input; frame->stream_generation = generation;
    frame->stream_schema = schema; frame->stream_top = stack->top;
    frame->stream_host = host;
    return 0;
}

int
wl_columnar_eval_stack_stream_dispatch_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner,
    const void *host, const wl_plan_op_t *op, unsigned action, bool begin)
{
    int rc = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (rc) return rc;
    if (!host || frame->stream_host != host
        || frame->session->cleanup_active != frame
        || frame->stack.top != frame->stream_top
        || action < WL_COLUMNAR_JOIN_STREAM_START
        || action > WL_COLUMNAR_JOIN_STREAM_ACK
        || !wl_columnar_join_pipeline_stream_authorized(owner, frame, host,
        action, op, frame->stream_input, frame->stream_generation,
        frame->stream_schema)) return EINVAL;
    if (begin) {
        if (frame->stream_dispatch) return EBUSY;
        frame->stream_dispatch = action;
    } else {
        if (frame->stream_dispatch != action) return EINVAL;
        frame->stream_dispatch = 0;
    }
    return 0;
}

int
wl_columnar_eval_stack_stream_consumer_state_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner,
    const void *host, bool *live, bool *done)
{
    int rc = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (rc) return rc;
    if (!live || !done || !host || frame->stream_host != host
        || !frame->stream_dispatch || frame->session->cleanup_active != frame)
        return EINVAL;
    *live = frame->filter.stage != WL_COLUMNAR_FILTER_STAGE_INERT;
    *done = frame->filter.stage == WL_COLUMNAR_FILTER_STAGE_DONE;
    return 0;
}

/* Fixed bookkeeping is prepaid by the host: original forward admission or
* actual independent cleanup funding after a stop. The exact result moves to
* an inert host slot for bounded retirement; no relation storage moves. */
int
wl_columnar_eval_stack_stream_restore_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner,
    const void *host, eval_entry_t *out)
{
    int rc = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (rc) return rc;
    if (!out || !wl_columnar_eval_stack_unary_entry_inert(out)
        || !host || frame->stream_host != host || frame->stream_dispatch
        || frame->session->cleanup_active != frame
        || frame->stack.top != frame->stream_top || !frame->stream_top
        || frame->filter.stage != WL_COLUMNAR_FILTER_STAGE_INERT
        || frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_IDLE
        || frame->binary.phase || frame->concat.phase
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_output)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_retired)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_snapshot)
        || !wl_columnar_join_pipeline_stream_authorized(owner, frame, host,
        WL_COLUMNAR_JOIN_STREAM_RESTORE, NULL, frame->stream_input,
        frame->stream_generation, frame->stream_schema)) return EBUSY;
    eval_entry_t *entry = &frame->stack.items[frame->stream_top - 1];
    if (entry->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION || !entry->rel
        || entry->continuation || entry->seg_boundaries || entry->seg_count
        || entry->rel == frame->stream_original.rel
        || (entry->owned && entry->rel == frame->stream_input)) return EINVAL;
    *out = *entry;
    *entry = frame->stream_original;
    wl_columnar_eval_stack_unary_entry_clear(&frame->stream_original);
    frame->stream_host = NULL; frame->stream_input = NULL;
    frame->stream_generation = 0; frame->stream_top = 0;
    frame->stream_schema = false;
    return 0;
}

int
wl_columnar_eval_stack_numeric_claim(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    const void *owner)
{
    int admission = wl_columnar_eval_stack_framed_admit(frame, owner);
    if (admission) return admission;
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
    if (frame && frame->prepared_unary) return EBUSY;
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
    int admission = wl_columnar_eval_stack_framed_admit(frame, owner);
    if (admission) return admission;
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
        || owner->source_initialized || owner->source_bare
        || owner->source_installed
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
        || owner->source_initialized || owner->source_bare
        || owner->source_installed)
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
    return frame && !frame->prepared_unary && !frame->stream_host &&
           !frame->unary_scalar_busy &&
           !frame->unary_scalar_init_target
           && !frame->unary_scalar_cancel_clear &&
           !frame->unary_scalar_drained ? &frame->stack : NULL;
}

eval_entry_t *
wl_columnar_eval_stack_cleanup_result(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    return frame && !frame->prepared_unary && !frame->stream_host &&
           !frame->unary_scalar_busy &&
           !frame->unary_scalar_init_target
           && !frame->unary_scalar_cancel_clear &&
           !frame->unary_scalar_drained ? &frame->result : NULL;
}

static void
wl_columnar_eval_stack_cleanup_continuations(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    if (frame->row_debt) return;
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
    if (frame->row_debt) return EBUSY;
    if (frame->prepared_unary) return EBUSY;
    if (frame->stream_host) return EBUSY;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_stack_test_fail_cleanup_dispose) {
        wl_columnar_eval_stack_test_fail_cleanup_dispose--;
        return EBUSY;
    }
#endif
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
    if (frame->row_debt) return EBUSY;
    if (frame->prepared_unary) return EBUSY;
    if (frame->stream_host) return EBUSY;
    const wl_columnar_eval_stack_auto_t *automatic =
        wl_columnar_eval_stack_automatic(frame);
    if (automatic->launch || automatic->busy) return EBUSY;
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_governor_ref_t *governor = frame->governor;
    wl_columnar_eval_cleanup_receipt_t *receipt = NULL;
    if (frame->cleanup_receipt_id) {
        wl_columnar_eval_cleanup_receipt_key_t key = {0};
        key.session_generation =
            frame->session->cleanup_receipt_session_generation;
        key.receipt_id = frame->cleanup_receipt_id;
        key.plan_epoch = frame->cleanup_receipt_plan_epoch;
        if (frame->cleanup_receipt_index_valid) {
            uint32_t index = frame->cleanup_receipt_index;
            if (index >=
                WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES) return EINVAL;
            wl_columnar_eval_cleanup_receipt_t *candidate =
                &frame->session->cleanup_receipts[index];
            const wl_columnar_eval_cleanup_receipt_key_t *saved =
                &frame->cleanup_receipt_index_key;
            if (saved->receipt_id != key.receipt_id ||
                saved->plan_epoch != key.plan_epoch
                || saved->session_generation != key.session_generation
                || candidate->state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREE
                || candidate->key.receipt_id != saved->receipt_id
                || candidate->key.plan_epoch != saved->plan_epoch
                || candidate->key.session_generation !=
                saved->session_generation
                || candidate->key.operation_attempt_id !=
                saved->operation_attempt_id
                || candidate->key.operation_id !=
                saved->operation_id) return EINVAL;
            receipt = candidate;
        } else {
            for (unsigned i = 0;
                i < WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES; ++i) {
                wl_columnar_eval_cleanup_receipt_t *candidate
                    = &frame->session->cleanup_receipts[i];
                if (candidate->state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREE
                    && candidate->key.receipt_id == key.receipt_id
                    && candidate->key.session_generation ==
                    key.session_generation
                    && candidate->key.plan_epoch == key.plan_epoch) {
                    receipt = candidate;
                    break;
                }
            }
        }
        if (!receipt || (receipt->state !=
            WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_PREPARED
            && receipt->state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_TRANSFERRED))
            return EINVAL;
    }
    wl_columnar_memory_reservation_init(&reservation);
    if (governor && !wl_columnar_memory_reservation_move(&reservation,
        &frame->reservation))
        return EINVAL;
    if (receipt) {
        receipt->state = WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED;
        receipt->cleanup_result = 0;
        frame->session->cleanup_pending_owners[
            receipt - frame->session->cleanup_receipts] = NULL;
    }
    frame->session->cleanup_reserved_bytes -=
        sizeof(wl_columnar_eval_stack_allocation_t);
    free(frame);
    if (governor) {
        bool released = wl_columnar_memory_release(&reservation);
        assert(released);
        (void)released;
        wl_columnar_memory_governor_ref_release(governor);
    }
    return 0;
}

static bool
wl_columnar_eval_cleanup_receipt_key_equal(
    const wl_columnar_eval_cleanup_receipt_key_t *left,
    const wl_columnar_eval_cleanup_receipt_key_t *right)
{
    return left && right && left->session_generation
           && left->receipt_id && left->plan_epoch
           && left->operation_attempt_id && left->operation_id
           && left->session_generation == right->session_generation
           && left->receipt_id == right->receipt_id
           && left->plan_epoch == right->plan_epoch
           && left->operation_attempt_id == right->operation_attempt_id
           && left->operation_id == right->operation_id;
}

static wl_columnar_eval_cleanup_receipt_t *
wl_columnar_eval_cleanup_receipt_find(wl_col_session_t *session,
    const wl_columnar_eval_cleanup_receipt_key_t *key)
{
    if (!session || !key || !key->session_generation || !key->receipt_id
        || !key->plan_epoch || !key->operation_attempt_id
        || !key->operation_id
        || key->session_generation !=
        session->cleanup_receipt_session_generation)
        return NULL;
    uint32_t index = 0;
    int indexed = wl_columnar_session_unary_cleanup_receipt_index(session, key,
            &index);
    if (!indexed) return &session->cleanup_receipts[index];
    if (indexed != ENOENT) return NULL; /* Protected owner never rescans on refusal. */
    /* Fixed source extent, including a refused first receipt debit. The ID
     * is only a rejection filter; every candidate retains full key proof. */
#if WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES != 32u
#error "Update the complete receipt slot expansion for the new capacity"
#endif
#define WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(N) \
        if (session->cleanup_receipts[(N)].key.receipt_id == key->receipt_id \
            && session->cleanup_receipts[(N)].state \
            != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREE \
            && session->cleanup_receipts[(N)].key.session_generation \
            == key->session_generation \
            && session->cleanup_receipts[(N)].key.plan_epoch == key->plan_epoch \
            && session->cleanup_receipts[(N)].key.operation_attempt_id \
            == key->operation_attempt_id \
            && session->cleanup_receipts[(N)].key.operation_id \
            == key->operation_id) \
        return &session->cleanup_receipts[(N)]
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(0);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(1);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(2);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(3);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(4);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(5);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(6);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(7);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(8);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(9);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(10);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(11);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(12);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(13);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(14);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(15);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(16);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(17);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(18);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(19);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(20);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(21);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(22);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(23);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(24);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(25);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(26);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(27);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(28);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(29);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(30);
    WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT(31);
#undef WL_COLUMNAR_EVAL_STACK_RECEIPT_SLOT
    return NULL;
}

int
wl_columnar_eval_stack_cleanup_receipt_reserve(wl_col_session_t *session,
    uint64_t operation_attempt_id, uint64_t operation_id,
    wl_columnar_eval_cleanup_receipt_key_t *out)
{
    if (!session || !operation_attempt_id || !operation_id || !out)
        return EINVAL;
    memset(out, 0, sizeof(*out));
    for (unsigned i = 0; i < WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES; ++i)
        if (session->cleanup_receipts[i].state ==
            WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREE) {
            if (!session->cleanup_receipt_session_generation) {
                uint64_t generation = 0;
                int rc =
                    wl_columnar_eval_stack_cleanup_receipt_session_generation_new
                    (
                    &generation);
                if (rc) return rc;
                session->cleanup_receipt_session_generation = generation;
            }
            if (!session->cleanup_receipt_next_id)
                session->cleanup_receipt_next_id = 1;
            if (!session->cleanup_receipt_next_plan_epoch)
                session->cleanup_receipt_next_plan_epoch = 1;
            if (session->cleanup_receipt_next_id == UINT64_MAX
                || session->cleanup_receipt_next_plan_epoch == UINT64_MAX)
                return EOVERFLOW;
            wl_columnar_eval_cleanup_receipt_key_t key = {
                .session_generation =
                    session->cleanup_receipt_session_generation,
                .receipt_id = session->cleanup_receipt_next_id++,
                .plan_epoch = session->cleanup_receipt_next_plan_epoch++,
                .operation_attempt_id = operation_attempt_id,
                .operation_id = operation_id
            };
            wl_columnar_eval_cleanup_receipt_t *receipt
                = &session->cleanup_receipts[i];
            session->cleanup_pending_owners[i] = NULL;
            receipt->key = key;
            receipt->state = WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_RESERVED;
            receipt->result = 0;
            receipt->cleanup_result = 0;
            receipt->committed = false;
            *out = key;
            return 0;
        }
    return ENOBUFS;
}

int
wl_columnar_eval_stack_cleanup_begin_receipted_attempt_controlled(
    wl_col_session_t *session, wl_columnar_eval_stack_cleanup_frame_t **out,
    wl_evaluation_control_t *control, uint64_t operation_attempt_id,
    uint64_t operation_id,
    wl_columnar_eval_cleanup_receipt_key_t *key)
{
    if (!session || !out || *out || !operation_id || !key)
        return EINVAL;
    if (session->cleanup_active &&
        (session->cleanup_active->prepared_unary ||
        session->cleanup_active->stream_dispatch)) return EBUSY;
    memset(key, 0, sizeof(*key));
    int rc = wl_columnar_eval_stack_cleanup_receipt_reserve(session,
            operation_attempt_id, operation_id, key);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_cleanup_begin_attempt_controlled(session, out,
            control, operation_attempt_id);
    if (rc) {
        (void)wl_columnar_eval_stack_cleanup_receipt_release(session, key);
        memset(key, 0, sizeof(*key));
        return rc;
    }
    (*out)->cleanup_receipt_id = key->receipt_id;
    (*out)->cleanup_receipt_plan_epoch = key->plan_epoch;
    return 0;
}

int
wl_columnar_eval_stack_cleanup_receipt_cancel_begin(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    const wl_columnar_eval_cleanup_receipt_key_t *key)
{
    if (!frame || !frame->identity || !frame->session || !key
        || frame->cleanup_receipt_id != key->receipt_id
        || frame->cleanup_receipt_plan_epoch != key->plan_epoch)
        return EINVAL;
    if (frame->prepared_unary) return EBUSY;
    wl_columnar_eval_cleanup_receipt_t *receipt
        = wl_columnar_eval_cleanup_receipt_find(frame->session, key);
    if (!receipt || receipt->state !=
        WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_RESERVED)
        return EBUSY;
    frame->session->cleanup_pending_owners[receipt -
        frame->session->cleanup_receipts] = NULL;
    memset(receipt, 0, sizeof(*receipt));
    frame->cleanup_receipt_id = 0;
    frame->cleanup_receipt_plan_epoch = 0;
    return 0;
}

/* Financial classification only. The key getter repeats its actual complete
 * native/output proof; this observation supplies no mutation permission. */
bool
wl_columnar_eval_stack_cleanup_receipt_key_protected(
    wl_columnar_eval_stack_cleanup_frame_t *frame)
{
    if (!frame) return false;
    const wl_columnar_eval_stack_auto_t *automatic =
        wl_columnar_eval_stack_automatic(frame);
    return frame->prepared_unary || (automatic->identity == automatic
           && automatic->frame == frame
           && automatic->phase != WL_COLUMNAR_EVAL_STACK_AUTO_UNUSED);
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_stack_test_receipt_lookup_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint32_t);
#endif
static int
wl_columnar_eval_stack_receipt_lookup_tail(wl_evaluation_control_t *funding,
    uint64_t id, unsigned phase, uint32_t index)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_stack_test_receipt_lookup_checkpoint)
        wl_columnar_eval_stack_test_receipt_lookup_checkpoint(funding, phase,
            index);
#else
    (void)phase; (void)index;
#endif
    return wl_evaluation_control_charge_attempt(funding, id, 0);
}

/* One-time metered lookup, from this actual fresh frame's allocated key.
 * Outputs remain unpublished on every refusal, including a missing match. */
int
wl_columnar_eval_stack_cleanup_receipt_find_frame_attempt(
    wl_columnar_eval_stack_cleanup_frame_t *frame, uint32_t *index_out,
    wl_columnar_eval_cleanup_receipt_key_t *out,
    wl_evaluation_control_t *funding, uint64_t id)
{
    if (!frame || !frame->identity || !frame->session || !index_out || !out
        || !funding || funding == frame->session->base.evaluation_control ||
        !id)
        return EINVAL;
    int rc = wl_evaluation_control_charge_attempt(funding, id, 128);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_receipt_lookup_tail(funding, id, 0, 0);
    if (rc) return rc;
    if (frame != frame->session->cleanup_active || frame->prepared_unary
        || frame->stream_host || !frame->cleanup_receipt_id
        || !frame->cleanup_receipt_plan_epoch) return EINVAL;
    for (uint32_t i = 0; i < WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES; ++i) {
        rc = wl_evaluation_control_charge_attempt(funding, id, 192);
        if (rc) return rc;
        rc = wl_columnar_eval_stack_receipt_lookup_tail(funding, id, 1, i);
        if (rc) return rc;
        const wl_columnar_eval_cleanup_receipt_t *receipt =
            &frame->session->cleanup_receipts[i];
        if (receipt->state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREE
            || receipt->key.receipt_id != frame->cleanup_receipt_id
            || receipt->key.plan_epoch != frame->cleanup_receipt_plan_epoch
            || receipt->key.session_generation !=
            frame->session->cleanup_receipt_session_generation) continue;
        if (receipt->state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_RESERVED
            || !receipt->key.operation_attempt_id || !receipt->key.operation_id)
            return EINVAL;
        rc = wl_evaluation_control_charge_attempt(funding, id, 128);
        if (rc) return rc;
        rc = wl_columnar_eval_stack_receipt_lookup_tail(funding, id, 2, i);
        if (rc) return rc;
        *out = receipt->key; *index_out = i;
        return 0;
    }
    return ENOENT;
}

/* Fixed lookup is valid only after the real birth scan and owning bind.
 * Index is a location, never permission; every actual key field still matches. */
int
wl_columnar_eval_stack_cleanup_receipt_index_key_for_frame(
    wl_columnar_eval_stack_cleanup_frame_t *frame, uint32_t index,
    wl_columnar_eval_cleanup_receipt_key_t *out)
{
    if (!frame || !frame->identity || !frame->session || !out
        || index >= WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES
        || !frame->cleanup_receipt_id || !frame->cleanup_receipt_plan_epoch
        || frame->prepared_unary || frame->stream_dispatch) return EINVAL;
    const wl_columnar_eval_cleanup_receipt_t *receipt =
        &frame->session->cleanup_receipts[index];
    if (receipt->state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREE
        || receipt->key.receipt_id != frame->cleanup_receipt_id
        || receipt->key.plan_epoch != frame->cleanup_receipt_plan_epoch
        || receipt->key.session_generation !=
        frame->session->cleanup_receipt_session_generation
        || frame->framed.identity != &frame->framed
        || frame->framed.frame != frame ||
        frame->framed.session != frame->session
        || receipt->key.operation_attempt_id != frame->framed.attempt_id
        || receipt->key.operation_id != frame->framed.operation_identity)
        return EINVAL;
    *out = receipt->key;
    return 0;
}

int
wl_columnar_eval_stack_cleanup_receipt_frame_index_bind(
    wl_columnar_eval_stack_cleanup_frame_t *frame, uint32_t index,
    const wl_columnar_eval_cleanup_receipt_key_t *key)
{
    wl_columnar_eval_cleanup_receipt_key_t actual;
    if (wl_columnar_eval_stack_cleanup_receipt_index_key_for_frame(frame, index,
        &actual)
        || !wl_columnar_eval_cleanup_receipt_key_equal(key,
        &actual)) return EINVAL;
    frame->cleanup_receipt_index_key = actual;
    frame->cleanup_receipt_index = index;
    frame->cleanup_receipt_index_valid = true;
    return 0;
}

int
wl_columnar_eval_stack_cleanup_receipt_key_for_frame(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_eval_cleanup_receipt_key_t *out)
{
    if (!frame || !frame->identity || !frame->session || !out
        || !frame->cleanup_receipt_id || !frame->cleanup_receipt_plan_epoch)
        return EINVAL;
    const wl_columnar_eval_stack_auto_t *automatic =
        wl_columnar_eval_stack_automatic(frame);
    /* Bootstrap writes the embedded key before any native selection. Once
     * selected, the receipt tail remains protected through ACK and nesting. */
    if ((frame->prepared_unary || (automatic->identity == automatic
        && automatic->frame == frame
        && automatic->phase != WL_COLUMNAR_EVAL_STACK_AUTO_UNUSED))
        && !wl_columnar_eval_stack_protocol_copy_output(frame, out,
        sizeof(*out))) return EINVAL;
    for (unsigned i = 0; i < WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES; ++i) {
        wl_columnar_eval_cleanup_receipt_t *receipt
            = &frame->session->cleanup_receipts[i];
        if (receipt->state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREE
            && receipt->key.receipt_id == frame->cleanup_receipt_id
            && receipt->key.plan_epoch == frame->cleanup_receipt_plan_epoch
            && receipt->key.session_generation ==
            frame->session->cleanup_receipt_session_generation) {
            *out = receipt->key;
            return 0;
        }
    }
    return ENOENT;
}

int
wl_columnar_eval_stack_cleanup_receipt_release(wl_col_session_t *session,
    const wl_columnar_eval_cleanup_receipt_key_t *key)
{
    wl_columnar_eval_cleanup_receipt_t *receipt
        = wl_columnar_eval_cleanup_receipt_find(session, key);
    if (!receipt) return ENOENT;
    if (receipt->state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_RESERVED)
        return EBUSY;
    session->cleanup_pending_owners[receipt - session->cleanup_receipts] = NULL;
    memset(receipt, 0, sizeof(*receipt));
    return 0;
}

int
wl_columnar_eval_stack_cleanup_receipt_read(wl_col_session_t *session,
    const wl_columnar_eval_cleanup_receipt_key_t *key,
    wl_columnar_eval_cleanup_receipt_t *out)
{
    if (!out || !wl_columnar_eval_stack_marked_copy_output(session, out,
        sizeof(*out))) return EINVAL;
    memset(out, 0, sizeof(*out));
    wl_columnar_eval_cleanup_receipt_t *receipt
        = wl_columnar_eval_cleanup_receipt_find(session, key);
    if (!receipt) return ENOENT;
    *out = *receipt;
    return 0;
}

int
wl_columnar_eval_stack_cleanup_receipt_ack(wl_col_session_t *session,
    const wl_columnar_eval_cleanup_receipt_key_t *key)
{
    wl_columnar_eval_cleanup_receipt_t *receipt
        = wl_columnar_eval_cleanup_receipt_find(session, key);
    if (!receipt) return ENOENT;
    if (receipt->state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED)
        return EBUSY;
    session->cleanup_pending_owners[receipt - session->cleanup_receipts] = NULL;
    memset(receipt, 0, sizeof(*receipt));
    return 0;
}

wl_columnar_eval_cleanup_receipt_ack_result_t
wl_columnar_eval_stack_cleanup_receipt_ack_funded(wl_col_session_t *session,
    const wl_columnar_eval_cleanup_receipt_key_t *key,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    wl_columnar_eval_cleanup_receipt_t *receipt =
        wl_columnar_eval_cleanup_receipt_find(session, key);
    if (!receipt)
        return (wl_columnar_eval_cleanup_receipt_ack_result_t){ENOENT, 0};
    if (!funding || funding == session->base.evaluation_control || !funding_id
        || wl_evaluation_control_active_attempt_id(funding) != funding_id)
        return (wl_columnar_eval_cleanup_receipt_ack_result_t){EINVAL, 0};
    int rc = wl_evaluation_control_charge_attempt(funding, funding_id,
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_HOST_WORK);
    if (rc) return (wl_columnar_eval_cleanup_receipt_ack_result_t){rc, 0};
    if (receipt->state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED)
        return (wl_columnar_eval_cleanup_receipt_ack_result_t){EBUSY, 0};
    int result = receipt->result;
    session->cleanup_pending_owners[receipt - session->cleanup_receipts] = NULL;
    memset(receipt, 0, sizeof(*receipt));
    return (wl_columnar_eval_cleanup_receipt_ack_result_t){0, result};
}

int
wl_columnar_eval_stack_cleanup_receipt_prepare(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    const wl_columnar_eval_cleanup_receipt_key_t *key, int result,
    bool committed)
{
    if (!frame || !frame->identity || !frame->session
        || frame->cleanup_receipt_id != (key ? key->receipt_id : 0)
        || frame->cleanup_receipt_plan_epoch != (key ? key->plan_epoch : 0))
        return EINVAL;
    if (frame->prepared_unary) return EBUSY;
    wl_columnar_eval_cleanup_receipt_t *receipt
        = wl_columnar_eval_cleanup_receipt_find(frame->session, key);
    if (!receipt) return ENOENT;
    if (receipt->state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_PREPARED)
        return receipt->result == result && receipt->committed == committed
            ? 0 : EINVAL;
    if (receipt->state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_RESERVED)
        return EBUSY;
    receipt->result = result;
    receipt->committed = committed;
    receipt->cleanup_result = 0;
    receipt->state = WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_PREPARED;
    return 0;
}

static int
wl_columnar_eval_stack_cleanup_finish_internal(
    wl_columnar_eval_stack_cleanup_frame_t **handle,
    const wl_columnar_eval_cleanup_receipt_key_t *key,
    wl_columnar_eval_cleanup_outcome_t *outcome)
{
    if (!handle || !*handle)
        return EINVAL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = *handle;
    if (!wl_columnar_eval_stack_unary_frame_valid(frame))
        return EINVAL;
    if (frame->prepared_unary) return EBUSY;
    wl_columnar_eval_cleanup_receipt_t *receipt = NULL;
    if (frame->cleanup_receipt_id) {
        if (!key || frame->cleanup_receipt_id != key->receipt_id
            || frame->cleanup_receipt_plan_epoch != key->plan_epoch)
            return EBUSY;
        receipt = wl_columnar_eval_cleanup_receipt_find(frame->session, key);
        if (!receipt || receipt->state !=
            WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_PREPARED)
            return EINVAL;
        if (outcome)
            *outcome = WL_COLUMNAR_EVAL_CLEANUP_OUTCOME_RETAINED_ACTIVE;
    } else if (key) return EINVAL;
    if (frame->framed.identity || frame->numeric_plan_owner
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
    if (rc == 0) {
        if (outcome)
            *outcome = WL_COLUMNAR_EVAL_CLEANUP_OUTCOME_FREED;
        return 0;
    }
    if (receipt) {
        receipt->state = WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_TRANSFERRED;
        receipt->cleanup_result = rc;
    }
    if (outcome)
        *outcome = receipt
            ? WL_COLUMNAR_EVAL_CLEANUP_OUTCOME_TRANSFERRED_TO_PENDING
            : WL_COLUMNAR_EVAL_CLEANUP_OUTCOME_RETAINED_ACTIVE;
    if (sess->cleanup_pending_count >=
        WL_COLUMNAR_EVAL_STACK_CLEANUP_MAX_FRAMES) {
        /* Admission reserves this slot before frame begin. Preserve ownership
         * in the active list if an invariant has nevertheless drifted. */
        frame->next = sess->cleanup_active;
        sess->cleanup_active = frame;
        sess->cleanup_active_count++;
        *handle = frame;
        if (receipt) {
            receipt->state = WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_PREPARED;
            receipt->cleanup_result = 0;
        }
        if (outcome)
            *outcome = WL_COLUMNAR_EVAL_CLEANUP_OUTCOME_RETAINED_ACTIVE;
        return EOVERFLOW;
    }
    if (rc != 0) {
        frame->pending_prev = NULL;
        frame->next = sess->cleanup_pending;
        if (frame->next) frame->next->pending_prev = frame;
        sess->cleanup_pending = frame;
        if (receipt)
            sess->cleanup_pending_owners[receipt -
                sess->cleanup_receipts] = frame;
        sess->cleanup_pending_count++;
    }
    return rc;
}

int
wl_columnar_eval_stack_cleanup_finish(
    wl_columnar_eval_stack_cleanup_frame_t **handle)
{
    if (!handle || !*handle) return EINVAL;
    /* Tracked frames can only be consumed through their exact receipt. */
    if ((*handle)->cleanup_receipt_id) return EBUSY;
    return wl_columnar_eval_stack_cleanup_finish_internal(handle, NULL, NULL);
}

int
wl_columnar_eval_stack_cleanup_finish_receipted(
    wl_columnar_eval_stack_cleanup_frame_t **handle,
    const wl_columnar_eval_cleanup_receipt_key_t *key,
    wl_columnar_eval_cleanup_outcome_t *outcome)
{
    if (!outcome) return EINVAL;
    return wl_columnar_eval_stack_cleanup_finish_internal(handle, key, outcome);
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
        wl_columnar_eval_stack_cleanup_frame_t *previous = frame->pending_prev;
        int rc = wl_columnar_eval_stack_cleanup_dispose(frame);
        if (rc == 0)
            rc = wl_columnar_eval_stack_cleanup_free(frame);
        if (rc == 0) {
            *slot = next;
            if (next) next->pending_prev = previous;
            sess->cleanup_pending_count--;
        } else {
            if (result == 0)
                result = rc;
            slot = &frame->next;
        }
    }
    return result;
}

int
wl_columnar_eval_stack_cleanup_receipt_retry(wl_col_session_t *session,
    const wl_columnar_eval_cleanup_receipt_key_t *key)
{
    wl_columnar_eval_cleanup_receipt_t *receipt
        = wl_columnar_eval_cleanup_receipt_find(session, key);
    if (!receipt) return ENOENT;
    if (receipt->state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED)
        return 0;
    if (receipt->state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_TRANSFERRED)
        return EBUSY;
    if (session->cleanup_active) return EBUSY;
    wl_columnar_eval_stack_cleanup_frame_t **slot = &session->cleanup_pending;
    while (*slot && ((*slot)->cleanup_receipt_id != key->receipt_id
        || (*slot)->cleanup_receipt_plan_epoch != key->plan_epoch
        || (*slot)->session != session))
        slot = &(*slot)->next;
    wl_columnar_eval_stack_cleanup_frame_t *frame = *slot;
    if (!frame) return ENOENT;
    /* Continuation readers can cross pending frames, so release those held
     * references first. Only the frame named by this key is disposed/freed. */
    for (wl_columnar_eval_stack_cleanup_frame_t *pending =
        session->cleanup_pending; pending; pending = pending->next)
        wl_columnar_eval_stack_cleanup_continuations(pending);
    wl_columnar_eval_stack_cleanup_frame_t *next = frame->next;
    wl_columnar_eval_stack_cleanup_frame_t *previous = frame->pending_prev;
    int rc = wl_columnar_eval_stack_cleanup_dispose(frame);
    if (!rc) rc = wl_columnar_eval_stack_cleanup_free(frame);
    if (rc) {
        receipt->cleanup_result = rc;
        return rc;
    }
    *slot = next;
    if (next) next->pending_prev = previous;
    session->cleanup_pending_count--;
    return 0;
}

/* Only a freshly matched receipt and its session-owned target reach this
 * same-call terminal body. The caller saves live neighbors before free. */
/* This private terminal move retains every native/state check. Checked
 * self identities are invariant through its exclusive callback-free transfer;
 * their redundant same-value stores are intentionally omitted. */
static bool
wl_columnar_eval_stack_cleanup_receipt_reservation_move(
    wl_columnar_memory_reservation_t *destination,
    wl_columnar_memory_reservation_t *source)
{
    uint64_t destination_state;
    uint64_t state;
    if (!destination || !source || destination == source
        || destination->identity != destination
        || source->identity != source)
        return false;
    destination_state = atomic_load_explicit(&destination->state,
            memory_order_acquire);
    if (destination_state != WL_COLUMNAR_MEMORY_RESERVATION_EMPTY
        && destination_state != WL_COLUMNAR_MEMORY_RESERVATION_RELEASED)
        return false;
    state = atomic_load_explicit(&source->state, memory_order_acquire);
    if (state != WL_COLUMNAR_MEMORY_RESERVATION_RESERVED
        && state != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED)
        return false;
    destination->governor = source->governor;
    destination->bytes = source->bytes;
    destination->transition_kind = source->transition_kind;
    atomic_store_explicit(&destination->owner_bits,
        atomic_load_explicit(&source->owner_bits, memory_order_relaxed),
        memory_order_relaxed);
    atomic_store_explicit(&destination->state, state, memory_order_release);
    source->governor = NULL;
    source->bytes = 0;
    source->transition_kind = WL_COLUMNAR_MEMORY_TRANSITION_NONE;
    atomic_store_explicit(&source->owner_bits, 0, memory_order_relaxed);
    atomic_store_explicit(&source->state,
        WL_COLUMNAR_MEMORY_RESERVATION_EMPTY, memory_order_release);
    return true;
}

static int
wl_columnar_eval_stack_cleanup_receipt_free_target(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_eval_cleanup_receipt_t *receipt)
{
    if (frame->row_debt) return EBUSY;
    wl_columnar_memory_reservation_t reservation;
    wl_columnar_memory_governor_ref_t *governor = frame->governor;
    wl_columnar_memory_reservation_init(&reservation);
    if (governor &&
        !wl_columnar_eval_stack_cleanup_receipt_reservation_move(&reservation,
        &frame->reservation))
        return EINVAL;
    receipt->state = WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED;
    receipt->cleanup_result = 0;
    frame->session->cleanup_pending_owners[
        receipt - frame->session->cleanup_receipts] = NULL;
    frame->session->cleanup_reserved_bytes -=
        sizeof(wl_columnar_eval_stack_allocation_t);
    free(frame);
    if (governor) {
        bool released = wl_columnar_memory_release(&reservation);
        assert(released);
        (void)released;
        wl_columnar_memory_governor_ref_release(governor);
    }
    return 0;
}

static int
wl_columnar_eval_stack_cleanup_receipt_host_internal(
    const wl_plan_relation_t *rp,
    wl_col_session_t *session, bool recursive,
    const wl_columnar_eval_serial_framed_host_request_t *request,
    wl_session_source_finance_t *source)
{
    int source_rc = source ? wl_session_source_finance_charge(source,
            48) : 0;
    if (source_rc) return source_rc;
    if (request && !wl_columnar_eval_stack_host_geometry(session, request))
        return EINVAL;
    source_rc = source ? wl_session_source_finance_charge(source,
            72) : 0;
    if (source_rc) return source_rc;
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
    if (!rp || !session) return EINVAL;
    if (!session->base.evaluation_control) return EINVAL;
    if (!request) return ENOTSUP;
    if (!request->reply) return EINVAL;
    if (request->operation != session->base.evaluation_control
        || !request->operation_id || !request->operation_identity
        || (request->capture_only != (request->private_bindings != NULL))
        || (request->capture_only != (request->capture_out != NULL))
        || (request->action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_STATUS
        && request->action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT
        && request->action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_RETRY
        && request->action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK))
        return EINVAL;
    if (recursive) return ENOTSUP;
    if (session->cleanup_active) return EBUSY;
    const wl_columnar_eval_cleanup_receipt_key_t *key = &request->receipt_key;
    /* The old no-active selector gives receipt ID zero its own ENOENT. */
    if (!key->receipt_id) return ENOENT;
    if (!key->session_generation || !key->plan_epoch
        || key->operation_attempt_id != request->operation_id
        || key->operation_id != request->operation_identity)
        return EINVAL;
    source_rc = source ? wl_session_source_finance_charge(source,
            80) : 0;
    if (source_rc) return source_rc;
    if (source) {
        /* Keep the established gateway prefix in its own actual generation.
         * This80 cell covers checked settlement/rebegin and the next front. */
        source_rc = wl_session_source_finance_finish(source);
        if (source_rc) return source_rc;
        source_rc = wl_session_source_finance_rebegin(source);
        if (source_rc) return source_rc;
        source_rc = wl_session_source_finance_charge(source, 224);
    }
    wl_columnar_eval_cleanup_receipt_t *receipt = NULL;
    int lookup_protocol = 0;
    if (!source_rc) {
        receipt = wl_columnar_eval_cleanup_receipt_find(session, key);
        if (!receipt) lookup_protocol = ENOENT;
        else if (!request->funding || request->funding == request->operation
            || !request->funding_id
            || wl_evaluation_control_active_attempt_id(request->funding)
            != request->funding_id)
            lookup_protocol = EINVAL;
        /* A missing/invalid lookup also reaches the real post-lookup check.
         * Fresh224+32 has outstanding at most256, without assumed progress. */
        if (source) source_rc = wl_session_source_finance_charge(source, 32);
    }
    if (source && source->active) {
        int finish_rc = wl_session_source_finance_finish(source);
        if (!source_rc) source_rc = finish_rc;
    }
    if (source_rc) return source_rc;
    if (lookup_protocol) return lookup_protocol;
    /* No callback can invalidate this fresh full-key match at admission. */
    int rc = wl_evaluation_control_charge_attempt(request->funding,
            request->funding_id, WL_COLUMNAR_EVAL_SERIAL_FRAMED_HOST_WORK);
    if (rc) return rc;
    if (request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK) {
        if (receipt->state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED)
            return EBUSY;
    } else if (request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_RETRY
        || request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT) {
        if (receipt->state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_TRANSFERRED) {
            wl_columnar_eval_stack_cleanup_frame_t *frame =
                session->cleanup_pending_owners[receipt -
                    session->cleanup_receipts];
            if (!frame) return ENOENT;
            if (!wl_columnar_eval_stack_unary_frame_valid(frame)
                || frame->session != session
                || frame->cleanup_receipt_id != key->receipt_id
                || frame->cleanup_receipt_plan_epoch != key->plan_epoch)
                return EINVAL;
            if (frame->prepared_unary || frame->stream_host
                || frame->framed.identity || frame->numeric_plan_owner
                || frame->filter.stage != WL_COLUMNAR_FILTER_STAGE_INERT
                || frame->unary_phase == WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE
                || frame->unary_phase == WL_COLUMNAR_EVAL_STACK_UNARY_COMMITTED)
                return EBUSY;
            const wl_columnar_eval_stack_auto_t *automatic =
                wl_columnar_eval_stack_automatic(frame);
            if (automatic->identity != automatic || automatic->frame != frame)
                return EINVAL;
            if (automatic->launch || automatic->busy) return EBUSY;
            wl_columnar_eval_stack_cleanup_frame_t *previous =
                frame->pending_prev;
            wl_columnar_eval_stack_cleanup_frame_t *next = frame->next;
            if ((previous ? previous->next !=
                frame : session->cleanup_pending != frame)
                || (next && next->pending_prev != frame))
                return EINVAL;
            /* Finish consumed the only mutating handle and destroyed all
             * continuation owners before pending publication. Raw prepasses
             * remain; this funded target does not repeat the cross-frame scan. */
            rc = wl_columnar_eval_stack_cleanup_dispose(frame);
            if (!rc)
                rc = wl_columnar_eval_stack_cleanup_receipt_free_target(frame,
                        receipt);
            if (rc) {
                receipt->cleanup_result = rc;
                if (rc == ENOENT) return rc;
            } else {
                if (previous) previous->next = next;
                else session->cleanup_pending = next;
                if (next) next->pending_prev = previous;
                session->cleanup_pending_count--;
            }
        } else if (receipt->state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED)
            return EBUSY;
    } else if (receipt->state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED
        && receipt->state != WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_TRANSFERRED)
        return EBUSY;
    wl_columnar_eval_serial_framed_host_reply_t *reply = request->reply;
    reply->execution_result = receipt->result;
    reply->cleanup_result = receipt->cleanup_result;
    reply->committed = receipt->committed;
    reply->cleanup_receipt_key = receipt->key;
    reply->kind = receipt->state == WL_COLUMNAR_EVAL_CLEANUP_RECEIPT_FREED
        ? WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL
        : WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_TERMINAL_CLEANUP_PENDING;
    if (request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_RECEIPT_ACK) {
        session->cleanup_pending_owners[receipt -
            session->cleanup_receipts] = NULL;
        memset(receipt, 0, sizeof(*receipt));
    }
    return rc;
}

int
wl_columnar_eval_stack_cleanup_receipt_host(const wl_plan_relation_t *rp,
    wl_col_session_t *session, bool recursive,
    const wl_columnar_eval_serial_framed_host_request_t *request)
{
    return wl_columnar_eval_stack_cleanup_receipt_host_internal(rp, session,
               recursive, request, NULL);
}

int
wl_columnar_eval_stack_cleanup_receipt_host_source(const wl_plan_relation_t *rp,
    wl_col_session_t *session, bool recursive,
    const wl_columnar_eval_serial_framed_host_request_t *request,
    wl_session_source_finance_t *source)
{
    if (!source) return EINVAL;
    return wl_columnar_eval_stack_cleanup_receipt_host_internal(rp, session,
               recursive, request, source);
}

/* Binary replacement has its own original owner. The old ACTIVE phase is
 * only an exclusion gate; old unary bindings stay inert, including thread. */
#define WL_COLUMNAR_EVAL_STACK_BINARY_ENTRY_WORK 256u
#define WL_COLUMNAR_EVAL_STACK_BINARY_WORK 192u
#define WL_COLUMNAR_EVAL_STACK_BINARY_CELL_WORK 128u
#define WL_COLUMNAR_EVAL_STACK_BINARY_HOST_WORK 384u
#define WL_COLUMNAR_EVAL_STACK_BINARY_RETIRE_PAYMENTS 20u

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_stack_test_binary_checkpoint)(
    wl_evaluation_control_t *, unsigned, uint64_t);
#endif

static int
wl_columnar_eval_stack_binary_tail(wl_evaluation_control_t *control,
    uint64_t id, unsigned phase, uint64_t progress)
{
    int rc = wl_columnar_eval_stack_attempt_guard(control, id);
    if (rc) return rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_stack_test_binary_checkpoint)
        wl_columnar_eval_stack_test_binary_checkpoint(control, phase, progress);
#else
    (void)phase; (void)progress;
#endif
    return wl_columnar_eval_stack_attempt_charge(control, id, 0);
}

static bool
wl_columnar_eval_stack_binary_thread(
    const wl_columnar_eval_stack_binary_context_t *ctx)
{
    if (!ctx->thread_valid) return false;
#if defined(WL_HAVE_C11_THREADS)
    return thrd_equal(ctx->owner_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return ctx->owner_thread == GetCurrentThreadId();
#else
    return pthread_equal(ctx->owner_thread, pthread_self()) != 0;
#endif
}

static int
wl_columnar_eval_stack_binary_validate(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner)
{
    int rc = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (rc) return rc;
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)) return EINVAL;
    wl_columnar_eval_stack_binary_context_t *ctx = &frame->binary;
    if (ctx->identity != ctx || ctx->frame != frame
        || ctx->session != frame->session
        || frame->session->cleanup_active != frame || !ctx->control
        || !ctx->attempt_id || !ctx->pin_owned || !ctx->phase
        || frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE
        || frame->unary_thread_valid
        || !wl_columnar_eval_stack_binary_thread(ctx)) return EINVAL;
    return 0;
}

static int
wl_columnar_eval_stack_binary_fund(
    wl_columnar_eval_stack_binary_context_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id)
{
    if (!funding || funding == ctx->control || !id
        || wl_evaluation_control_active_attempt_id(funding) != id)
        return EINVAL;
    return wl_evaluation_control_charge_attempt(funding, id,
               WL_COLUMNAR_EVAL_STACK_BINARY_HOST_WORK);
}

static bool
wl_columnar_eval_stack_binary_entry_matches(const eval_entry_t *a,
    const eval_entry_t *b)
{
    return a->rel == b->rel && a->owned == b->owned
           && a->is_delta == b->is_delta
           && a->seg_boundaries == b->seg_boundaries
           && a->seg_count == b->seg_count && a->kind == b->kind
           && a->continuation == b->continuation;
}

int
wl_columnar_eval_stack_numeric_stack_claim(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    const void *owner, uint32_t expected_depth,
    wl_evaluation_control_t *control, uint64_t id)
{
    int admission = wl_columnar_eval_stack_framed_admit(frame, owner);
    if (admission) return admission;
    if (!expected_depth || expected_depth > COL_STACK_MAX || !owner
        || !wl_columnar_eval_plan_numeric_owner_valid(owner, frame))
        return EINVAL;
    const wl_columnar_eval_plan_numeric_attempt_t *record = owner;
    if (record->control != control || record->attempt_id != id) return EINVAL;
    eval_stack_t *stack = NULL;
    int rc = wl_columnar_eval_stack_attempt_charge(control, id,
            WL_COLUMNAR_EVAL_STACK_BINARY_ENTRY_WORK);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_operator_stack(frame, session, &stack);
    if (rc) return rc;
    if (stack->top != expected_depth) return EINVAL;
    for (uint32_t i = 0; i < expected_depth; ++i) {
        rc = wl_columnar_eval_stack_attempt_charge(control, id,
                WL_COLUMNAR_EVAL_STACK_BINARY_CELL_WORK);
        if (rc) return rc;
        const eval_entry_t *entry = &stack->items[i];
        int verdict = entry->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
            || !entry->rel || entry->continuation ? ENOTSUP : 0;
        rc = wl_columnar_eval_stack_binary_tail(control, id, 1, i);
        if (rc) return rc;
        if (verdict) return verdict;
    }
    rc = wl_columnar_eval_stack_attempt_charge(control, id,
            WL_COLUMNAR_EVAL_STACK_BINARY_WORK);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_binary_tail(control, id, 2, expected_depth);
    if (rc) return rc;
    frame->numeric_plan_owner = owner;
    return 0;
}

int
wl_columnar_eval_stack_binary_begin_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_evaluation_control_t *control, uint64_t id, const void *owner)
{
    int rc = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (rc) return rc;
    if (owner) {
        const wl_columnar_eval_plan_numeric_attempt_t *record = owner;
        if (record->control != control || record->attempt_id != id)
            return EINVAL;
    }
    rc = wl_columnar_eval_stack_attempt_charge(control, id,
            WL_COLUMNAR_EVAL_STACK_BINARY_ENTRY_WORK);
    if (rc) return rc;
    eval_stack_t *stack = NULL;
    rc = wl_columnar_eval_stack_operator_stack_owned(frame, session, &stack,
            owner);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_attempt_charge(control, id,
            WL_COLUMNAR_EVAL_STACK_BINARY_WORK);
    if (rc) return rc;
    wl_columnar_eval_stack_binary_context_t *ctx = &frame->binary;
    if (ctx->phase || ctx->identity || ctx->pin_owned || ctx->control
        || !wl_columnar_eval_stack_unary_entry_inert(&ctx->output)
        || !wl_columnar_eval_stack_unary_entry_inert(&ctx->retired[0])
        || !wl_columnar_eval_stack_unary_entry_inert(&ctx->retired[1]))
        return EBUSY;
    if (stack->top < 2) return ENOTSUP;
    uint32_t top = stack->top;
    for (uint32_t side = 0; side < 2; ++side) {
        rc = wl_columnar_eval_stack_attempt_charge(control, id,
                WL_COLUMNAR_EVAL_STACK_BINARY_WORK);
        if (rc) return rc;
        const eval_entry_t *input = &stack->items[top - 2 + side];
        if (input->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION || !input->rel
            || input->continuation) return ENOTSUP;
        for (uint32_t i = 0; i < top; ++i) {
            rc = wl_columnar_eval_stack_attempt_charge(control, id,
                    WL_COLUMNAR_EVAL_STACK_BINARY_CELL_WORK);
            if (rc) return rc;
            const eval_entry_t *other = &stack->items[i];
            if (other->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
                || !other->rel || other->continuation) return ENOTSUP;
            bool duplicate = i != top - 2 + side
                && input->seg_boundaries
                && input->seg_boundaries == other->seg_boundaries;
            bool relation_alias = false;
            if (i != top - 2 + side && other->rel
                && (input->owned || other->owned)) {
                const col_rel_t *left_owner = input->rel->storage_owner
                    ? input->rel->storage_owner : input->rel;
                const col_rel_t *right_owner = other->rel->storage_owner
                    ? other->rel->storage_owner : other->rel;
                relation_alias = left_owner == right_owner;
            }
            rc = wl_columnar_eval_stack_binary_tail(control, id, 3,
                    (uint64_t)side * COL_STACK_MAX + i);
            if (rc) return rc;
            if (duplicate || relation_alias) return ENOTSUP;
        }
    }
    /* Five distinct first capacities: candidate, left, right, pin release,
     * reset/ACK. Four192 payments each partition128 current +64 future.
     * No owning activation exists on a partial payment prefix. */
    for (unsigned i = 0;
        i < WL_COLUMNAR_EVAL_STACK_BINARY_RETIRE_PAYMENTS; ++i) {
        rc = wl_columnar_eval_stack_attempt_charge(control, id,
                WL_COLUMNAR_EVAL_STACK_BINARY_WORK);
        if (rc) return rc;
        rc = wl_columnar_eval_stack_binary_tail(control, id, 4, i);
        if (rc) return rc;
    }
    rc = wl_columnar_eval_stack_attempt_charge(control, id,
            WL_COLUMNAR_EVAL_STACK_BINARY_WORK);
    if (rc) return rc;
    rc = wl_evaluation_control_retain(control);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_binary_tail(control, id, 5, top);
    if (rc) {
        wl_evaluation_control_release(control); return rc;
    }
    ctx->identity = ctx; ctx->frame = frame; ctx->session = session;
    ctx->control = control; ctx->attempt_id = id; ctx->top = top;
    ctx->phase = 1; ctx->cursor = 0;
    ctx->committed = false; ctx->result_saved = false;
    ctx->execution_result = 0; ctx->cleanup_result = 0;
    ctx->alias_rejected = false; ctx->pin_owned = true;
#if defined(WL_HAVE_C11_THREADS)
    ctx->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    ctx->owner_thread = GetCurrentThreadId();
#else
    ctx->owner_thread = pthread_self();
#endif
    ctx->thread_valid = true;
    wl_columnar_eval_stack_unary_entry_copy(&ctx->snapshot[0],
        &stack->items[top - 2]);
    wl_columnar_eval_stack_unary_entry_copy(&ctx->snapshot[1],
        &stack->items[top - 1]);
    frame->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE;
    return 0;
}

int
wl_columnar_eval_stack_binary_input_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, unsigned side,
    const eval_entry_t **out, const void *owner)
{
    int rc = wl_columnar_eval_stack_binary_validate(frame, owner);
    if (rc) return rc;
    if (!out || side > 1 || frame->binary.phase != 1
        || frame->binary.committed) return EINVAL;
    rc = wl_columnar_eval_stack_attempt_guard(frame->binary.control,
            frame->binary.attempt_id);
    if (rc) return rc;
    *out = &frame->binary.snapshot[side];
    return 0;
}

int
wl_columnar_eval_stack_binary_output_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, col_rel_t ***out,
    const void *owner)
{
    int rc = wl_columnar_eval_stack_binary_validate(frame, owner);
    if (rc) return rc;
    if (!out || frame->binary.phase != 1 || frame->binary.committed
        || frame->binary.output.rel)
        return EINVAL;
    rc = wl_columnar_eval_stack_attempt_guard(frame->binary.control,
            frame->binary.attempt_id);
    if (rc) return rc;
    frame->binary.output.owned = true;
    *out = &frame->binary.output.rel;
    return 0;
}

int
wl_columnar_eval_stack_binary_commit_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, bool *committed,
    const void *owner)
{
    int rc = wl_columnar_eval_stack_binary_validate(frame, owner);
    if (rc) return rc;
    if (!committed || frame->binary.phase != 1 || frame->binary.committed)
        return EINVAL;
    *committed = false;
    wl_columnar_eval_stack_binary_context_t *ctx = &frame->binary;
    rc = wl_columnar_eval_stack_attempt_charge(ctx->control, ctx->attempt_id,
            WL_COLUMNAR_EVAL_STACK_BINARY_WORK);
    if (rc) return rc;
    if (!ctx->output.rel || !ctx->output.owned || ctx->output.is_delta
        || ctx->output.seg_boundaries || ctx->output.seg_count
        || ctx->output.kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
        || ctx->output.continuation || ctx->top < 2
        || ctx->top > COL_STACK_MAX || frame->stack.top != ctx->top)
        return EINVAL;
    for (uint32_t i = 0; i < ctx->top; ++i) {
        rc = wl_columnar_eval_stack_attempt_charge(ctx->control,
                ctx->attempt_id, WL_COLUMNAR_EVAL_STACK_BINARY_CELL_WORK);
        if (rc) return rc;
        bool alias = ctx->output.rel == frame->stack.items[i].rel;
        const col_rel_t *candidate_owner = ctx->output.rel->storage_owner
            ? ctx->output.rel->storage_owner : ctx->output.rel;
        const col_rel_t *input = frame->stack.items[i].rel;
        const col_rel_t *input_owner = input->storage_owner
            ? input->storage_owner : input;
        bool storage_alias = candidate_owner == input_owner;
        if (alias) {
            wl_columnar_eval_stack_unary_entry_clear(&ctx->output);
            ctx->alias_rejected = true;
        }
        rc = wl_columnar_eval_stack_binary_tail(ctx->control,
                ctx->attempt_id, 6, i);
        if (rc) return rc;
        if (alias) return EINVAL;
        if (storage_alias) return ENOTSUP;
    }
    rc = wl_columnar_eval_stack_attempt_charge(ctx->control, ctx->attempt_id,
            WL_COLUMNAR_EVAL_STACK_BINARY_WORK);
    if (rc) return rc;
    bool matches = wl_columnar_eval_stack_binary_entry_matches(
        &frame->stack.items[ctx->top - 2], &ctx->snapshot[0])
        && wl_columnar_eval_stack_binary_entry_matches(
        &frame->stack.items[ctx->top - 1], &ctx->snapshot[1]);
    rc = wl_columnar_eval_stack_binary_tail(ctx->control, ctx->attempt_id,
            7, ctx->top);
    if (rc) return rc;
    if (!matches) return EINVAL;
    wl_columnar_eval_stack_unary_entry_copy(&ctx->retired[0],
        &ctx->snapshot[0]);
    wl_columnar_eval_stack_unary_entry_copy(&ctx->retired[1],
        &ctx->snapshot[1]);
    wl_columnar_eval_stack_unary_entry_copy(
        &frame->stack.items[ctx->top - 2], &ctx->output);
    wl_columnar_eval_stack_unary_entry_clear(
        &frame->stack.items[ctx->top - 1]);
    wl_columnar_eval_stack_unary_entry_clear(&ctx->output);
    frame->stack.top = ctx->top - 1;
    ctx->committed = true;
    *committed = true;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_stack_test_physical_publication)
        wl_columnar_eval_stack_test_physical_publication(frame->session,
            ctx->retired[0].rel, frame->stack.items[ctx->top - 2].rel);
#endif
    return 0;
}

static void
wl_columnar_eval_stack_binary_status(
    const wl_columnar_eval_stack_binary_context_t *ctx,
    wl_columnar_filter_status_t *status)
{
    status->progress = ctx->phase == 3 ? WL_COLUMNAR_FILTER_PROGRESS_DONE
        : ctx->cleanup_result ? WL_COLUMNAR_FILTER_PROGRESS_RETAINED
        : WL_COLUMNAR_FILTER_PROGRESS_YIELD;
    status->execution_result = ctx->execution_result;
    status->cleanup_result = ctx->cleanup_result;
    status->semantic_result = ctx->execution_result ? ctx->execution_result
        : ctx->alias_rejected ? EINVAL : ctx->cleanup_result;
    status->committed = ctx->committed;
}

int
wl_columnar_eval_stack_binary_finish_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, int result,
    wl_columnar_filter_status_t *status, wl_evaluation_control_t *funding,
    uint64_t funding_id, const void *owner)
{
    int rc = wl_columnar_eval_stack_binary_validate(frame, owner);
    if (rc) return rc;
    if (!status) return EINVAL;
    wl_columnar_eval_stack_binary_context_t *ctx = &frame->binary;
    rc = wl_columnar_eval_stack_binary_fund(ctx, funding, funding_id);
    if (rc) return rc;
    if (!ctx->result_saved) {
        ctx->execution_result = result;
        ctx->result_saved = true; ctx->phase = 2;
    }
    if (ctx->phase == 3) {
        wl_columnar_eval_stack_binary_status(ctx, status); return 0;
    }
    eval_entry_t *entry = ctx->committed
        ? &ctx->retired[ctx->cursor] : &ctx->output;
    /* Abort sanitation is a bounded action of at most four pointer slots;
     * even a never-committed alias must not destroy an original input. */
    if (!ctx->committed && entry->rel && ctx->cursor < frame->stack.top) {
        for (unsigned n = 0; n < 4 && ctx->cursor < frame->stack.top; ++n) {
            bool alias = entry->rel == frame->stack.items[ctx->cursor].rel;
            ++ctx->cursor;
            if (alias) {
                wl_columnar_eval_stack_unary_entry_clear(entry);
                ctx->alias_rejected = true;
                ctx->phase = 3;
                break;
            }
        }
        wl_columnar_eval_stack_binary_status(ctx, status);
        return 0;
    }
    rc = eval_entry_dispose(entry);
    ctx->cleanup_result = rc;
    if (!rc) {
        wl_columnar_eval_stack_unary_entry_clear(entry);
        if (!ctx->committed || ++ctx->cursor == 2) ctx->phase = 3;
    }
    wl_columnar_eval_stack_binary_status(ctx, status);
    return 0;
}

int
wl_columnar_eval_stack_binary_ack_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_evaluation_control_t *funding, uint64_t funding_id, const void *owner)
{
    int rc = wl_columnar_eval_stack_binary_validate(frame, owner);
    if (rc) return rc;
    wl_columnar_eval_stack_binary_context_t *ctx = &frame->binary;
    if (ctx->phase != 3
        || !wl_columnar_eval_stack_unary_entry_inert(&ctx->output)
        || !wl_columnar_eval_stack_unary_entry_inert(&ctx->retired[0])
        || !wl_columnar_eval_stack_unary_entry_inert(&ctx->retired[1]))
        return EBUSY;
    rc = wl_columnar_eval_stack_binary_fund(ctx, funding, funding_id);
    if (rc) return rc;
    wl_evaluation_control_t *control = ctx->control;
    wl_columnar_eval_stack_unary_entry_clear(&ctx->snapshot[0]);
    wl_columnar_eval_stack_unary_entry_clear(&ctx->snapshot[1]);
    ctx->identity = NULL; ctx->frame = NULL; ctx->session = NULL;
    ctx->control = NULL; ctx->attempt_id = 0; ctx->top = 0;
    ctx->phase = 0; ctx->cursor = 0; ctx->thread_valid = false;
    ctx->pin_owned = false; ctx->committed = false; ctx->result_saved = false;
    ctx->execution_result = 0; ctx->cleanup_result = 0;
    ctx->alias_rejected = false;
    frame->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_IDLE;
    wl_evaluation_control_release(control);
    return 0;
}

/* Same original frame/owner admission, independent of old unary context. The
 * embedded context is borrowed; this getter transfers no authority. */
int
wl_columnar_eval_stack_concat_context_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_merge_concat_context_t **out, const void *owner)
{
    int rc = wl_columnar_eval_stack_numeric_writable(frame, owner);
    if (rc) return rc;
    if (!out || !wl_columnar_eval_stack_unary_frame_valid(frame)
        || frame->session->cleanup_active != frame) return EINVAL;
    *out = &frame->concat;
    return 0;
}

/* Composition pushes a later VARIABLE above original lower entries. These
* siblings do not broaden the legacy empty-source or top-one contracts. */
static int
wl_columnar_eval_stack_composed_source_frame(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    const wl_columnar_eval_plan_numeric_attempt_t *owner, bool prepared,
    bool target)
{
    if (frame && frame->prepared_unary) return EBUSY;
    if (!frame || !sess || frame->identity != frame || frame->session != sess
        || !owner || !wl_columnar_eval_plan_numeric_owner_valid(owner, frame)
        || !owner->composed_mode || owner->runtime_depth > COL_STACK_MAX)
        return EINVAL;
    if (frame->stream_host
        || frame->numeric_plan_owner != owner || sess->cleanup_active != frame
        || frame->stack.top != owner->runtime_depth
        || frame->filter.stage != WL_COLUMNAR_FILTER_STAGE_INERT
        || frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_IDLE
        || frame->binary.phase || frame->concat.identity
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_output)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_retired)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_snapshot)
        || frame->unary_control || frame->unary_attempt_id
        || frame->unary_index || frame->unary_top || frame->unary_thread_valid
        || frame->unary_committed || frame->unary_result_saved
        || frame->unary_alias_rejected || frame->unary_first_result
        || frame->unary_cleanup_result)
        return EBUSY;
    if (target && (frame->stack.top == COL_STACK_MAX
        || !wl_columnar_eval_stack_unary_entry_inert(
            &frame->stack.items[frame->stack.top]))) return EBUSY;
    if (!prepared) {
        if (!wl_columnar_eval_stack_unary_entry_inert(&frame->result))
            return EBUSY;
    } else if (!frame->result.owned
        || frame->result.is_delta != owner->source_is_delta
        || frame->result.seg_boundaries || frame->result.seg_count
        || frame->result.kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
        || frame->result.continuation
        || owner->source_output != &frame->result.rel) return EBUSY;
    return 0;
}

/* An explicit admission separates the caller's initialization from the
 * empty-frame claim and its rollback. No ownership before the last ID check. */
int
wl_columnar_eval_stack_composed_source_claim(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_columnar_eval_plan_numeric_attempt_t *owner)
{
    if (!owner || !wl_columnar_eval_plan_numeric_owner_valid(owner, frame)
        || !owner->composed_mode) return EINVAL;
    int rc = wl_columnar_eval_stack_attempt_charge(owner->control,
            owner->attempt_id, 256);
    if (rc) return rc;
    return wl_columnar_eval_stack_numeric_source_claim(frame, sess, owner);
}

int
wl_columnar_eval_stack_composed_source_output(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_columnar_eval_plan_numeric_attempt_t *owner)
{
    int rc = wl_columnar_eval_stack_composed_source_frame(frame, sess, owner,
            false, true);
    if (rc) return rc;
    if (owner->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_SLOT
        || owner->source_slot_borrowed || owner->source_installed)
        return EBUSY;
    /* Four current128/future64 payments buy the candidate's own first256
     * retirement capacity before its marker; retries use a new host grant. */
    for (unsigned i = 0; i < 4; ++i) {
        rc = wl_columnar_eval_stack_attempt_charge(owner->control,
                owner->attempt_id, 192);
        if (rc) return rc;
        rc = wl_columnar_eval_stack_attempt_charge(owner->control,
                owner->attempt_id, 0);
        if (rc) return rc;
    }
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
wl_columnar_eval_stack_composed_source_install(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_columnar_eval_plan_numeric_attempt_t *owner)
{
    int rc = wl_columnar_eval_stack_composed_source_frame(frame, sess, owner,
            owner && owner->source_slot_borrowed, true);
    if (rc) return rc;
    if (owner->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_INSTALL
        || owner->source_initialized || owner->source_bare
        || owner->source_installed
        || owner->source_empty != owner->source_slot_borrowed) return EBUSY;
    col_rel_t *rel = owner->source_empty ? frame->result.rel
        : owner->source_selected;
    if (!rel) return EINVAL;
    rc = wl_columnar_eval_stack_attempt_charge(owner->control,
            owner->attempt_id, 192);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_attempt_charge(owner->control,
            owner->attempt_id, 0);
    if (rc) return rc;
    /* Callback-free cutoff: all lower entries remain field-identical. */
    eval_entry_t *entry = &frame->stack.items[frame->stack.top];
    entry->rel = rel; entry->owned = owner->source_empty;
    entry->is_delta = owner->source_is_delta;
    entry->seg_boundaries = NULL; entry->seg_count = 0;
    entry->kind = WL_COLUMNAR_EVAL_ENTRY_RELATION;
    entry->continuation = NULL;
    wl_columnar_eval_stack_unary_entry_clear(&frame->result);
    ++frame->stack.top;
    owner->source_slot_borrowed = false; owner->source_output = NULL;
    owner->source_installed = true;
    return 0;
}

int
wl_columnar_eval_stack_composed_source_abort_validate(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_columnar_eval_plan_numeric_attempt_t *owner)
{
    int rc = wl_columnar_eval_stack_composed_source_frame(frame, sess, owner,
            true, true);
    if (rc) return rc;
    if (owner->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_SOURCE_ABORT
        || owner->source_initialized || owner->source_bare
        || !owner->source_slot_borrowed)
        return EBUSY;
    return 0;
}

int
wl_columnar_eval_stack_composed_source_abort_funded(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_columnar_eval_plan_numeric_attempt_t *owner)
{
    int rc = wl_columnar_eval_stack_composed_source_abort_validate(frame,
            sess, owner);
    if (rc) return rc;
    rc = eval_entry_dispose(&frame->result);
    if (rc) return rc;
    wl_columnar_eval_stack_unary_entry_clear(&frame->result);
    owner->source_slot_borrowed = false; owner->source_output = NULL;
    return 0;
}

int
wl_columnar_eval_stack_composed_source_abort(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_columnar_eval_plan_numeric_attempt_t *owner,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    int rc = wl_columnar_eval_stack_composed_source_abort_validate(frame,
            sess, owner);
    if (rc) return rc;
    if (!funding || funding == owner->control || !funding_id
        || wl_evaluation_control_active_attempt_id(funding) != funding_id)
        return EINVAL;
    rc = wl_evaluation_control_charge_attempt(funding, funding_id, 384);
    if (rc) return rc;
    return wl_columnar_eval_stack_composed_source_abort_funded(frame, sess,
               owner);
}

/* Same read-only prerequisites as release, without removing the owner. Keep
 * the existing release caller packet unchanged; ACK can now poll while it
 * still owns the frame. No cached permission replaces release's own proof. */
int
wl_columnar_eval_stack_composed_source_release_validate(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_columnar_eval_plan_numeric_attempt_t *owner)
{
    int rc = wl_columnar_eval_stack_composed_source_frame(frame, sess, owner,
            false, false);
    if (rc) return rc;
    if (owner->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE
        || owner->source_initialized || owner->source_bare
        || owner->source_slot_borrowed)
        return EBUSY;
    return 0;
}

int
wl_columnar_eval_stack_composed_source_release(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *sess,
    wl_columnar_eval_plan_numeric_attempt_t *owner)
{
    int rc = wl_columnar_eval_stack_composed_source_frame(frame, sess, owner,
            false, false);
    if (rc) return rc;
    if (owner->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE
        || owner->source_initialized || owner->source_bare
        || owner->source_slot_borrowed)
        return EBUSY;
    frame->numeric_plan_owner = NULL;
    return 0;
}

int
wl_columnar_eval_stack_framed_context(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_columnar_eval_serial_framed_attempt_t **out)
{
    if (!out || !wl_columnar_eval_stack_unary_frame_valid(frame)
        || frame->session != session) return EINVAL;
    if (frame->prepared_unary) return EBUSY;
    if (session->cleanup_active != frame) return EBUSY;
    *out = &frame->framed;
    return 0;
}

/* Output storage is caller owned. These arithmetic predicates inspect no
 * output bytes and permit exact adjacency without pointer-end overflow. */
bool
wl_columnar_eval_stack_copy_ranges_disjoint(const void *a, size_t a_size,
    const void *b, size_t b_size)
{
    uintptr_t first = (uintptr_t)a, second = (uintptr_t)b;
    return second < first ? first - second >= b_size
                          : second - first >= a_size;
}

/* These new ROW siblings do not alter the legacy context getter or the tight
 * scalar-retirement BIND contour. A returned context is not writer authority. */
int
wl_columnar_eval_stack_row_debt_context_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner,
    const wl_columnar_relation_row_debt_t *debt,
    wl_columnar_filter_context_t **out)
{
    int rc = wl_columnar_eval_stack_filter_context_nonstream_owned(frame, out,
            owner);
    if (rc) return rc;
    if (!debt || frame->row_debt != debt) return EBUSY;
    return 0;
}

int
wl_columnar_eval_stack_row_debt_bind_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner,
    wl_columnar_relation_row_debt_t *debt, uint64_t expected_nonce,
    wl_columnar_filter_context_t **out)
{
    /* Same-call terminal bind after full context/native/range proof. Direct
    * lineage is rechecked; no cached permission is accepted by ROW work. */
    if (!frame || frame->identity != frame || !owner || !debt || !out
        || frame->numeric_plan_owner != owner || frame->row_debt
        || frame->prepared_unary || frame->stream_host) return EINVAL;
    const wl_columnar_eval_plan_numeric_attempt_t *record = owner;
    wl_columnar_filter_context_t *ctx = &frame->filter;
    if (debt->identity != debt || !wl_columnar_relation_row_debt_thread(debt)
        || debt->phase != WL_COLUMNAR_RELATION_ROW_DEBT_IDLE
        || ctx->identity != ctx || ctx->frame != frame
        || ctx->session != frame->session || ctx->session != record->session
        || ctx->session->cleanup_active != frame || !ctx->pin_owned
        || ctx->busy || (ctx->stage != WL_COLUMNAR_FILTER_STAGE_BATCH
        && ctx->stage != WL_COLUMNAR_FILTER_STAGE_JOIN_BATCH)
        || ctx->control != record->control
        || ctx->attempt_id != record->attempt_id) return EINVAL;
    col_rel_t *target = ctx->output;
    if (!target || ctx->private_output != target
        || frame->unary_output.rel != target || !frame->unary_output.owned
        || frame->unary_output.kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
        || !ctx->batch_initialized
        || !ctx->drain_initialized || ctx->batch.identity != &ctx->batch
        || ctx->batch.raw.relation != target || ctx->batch.raw_mode
        || ctx->batch.phase != WL_COLUMNAR_RELATION_BATCH_HELD
        || !ctx->batch.raw.active || !expected_nonce
        || ctx->batch.raw.single.set.acquisition_nonce != expected_nonce
        || ctx->batch.raw.single.set.owner_count != 1
        || ctx->batch.raw.single.set.owners_acquired != 1
        || ctx->batch.raw.single.set.descriptors_acquired != 1) return EINVAL;
    int rc = wl_evaluation_control_charge_attempt(ctx->control,
            ctx->attempt_id, 0);
    if (rc) return rc;
    debt->frame = frame; debt->numeric_owner = owner; debt->context = ctx;
    debt->target = target; debt->original_control = ctx->control;
    debt->original_id = ctx->attempt_id;
    debt->batch_nonce = expected_nonce;
    debt->original_control_identity =
        wl_evaluation_control_identity(ctx->control);
    debt->origin_stage = ctx->stage;
    debt->phase = WL_COLUMNAR_RELATION_ROW_DEBT_FORWARD;
    frame->row_debt = debt; ctx->busy = true;
    *out = ctx;
    return 0;
}

int
wl_columnar_eval_stack_row_debt_unbind_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner,
    wl_columnar_relation_row_debt_t *debt)
{
    if (!frame || !debt || frame->identity != frame
        || frame->row_debt != debt || frame->numeric_plan_owner != owner
        || debt->identity != debt || debt->frame != frame
        || debt->numeric_owner != owner || debt->context != &frame->filter
        || !frame->filter.busy ||
        debt->phase != WL_COLUMNAR_RELATION_ROW_DEBT_UNBIND
        || debt->row_ready || debt->refund_ready || debt->downsize_ready
        || debt->image_columns || debt->image_timestamps
        || debt->image_extent || debt->image_cursor) return EINVAL;
    if (debt->capture_ready && (debt->capture.allocated
        || debt->capture.credit_held || debt->capture.ref_held)) return EINVAL;
    wl_columnar_filter_context_t *ctx = &frame->filter;
    if (!ctx->execution_result) ctx->execution_result = debt->execution_result;
    if (!ctx->semantic_result) ctx->semantic_result = ctx->execution_result;
    frame->row_debt = NULL;
    debt->frame = NULL; debt->numeric_owner = NULL; debt->context = NULL;
    debt->target = NULL; debt->original_control = NULL;
    debt->original_id = 0; debt->original_control_identity = 0;
    debt->batch_nonce = 0;
    debt->capture_ready = false; debt->intent_valid = false;
    debt->phase = WL_COLUMNAR_RELATION_ROW_DEBT_DONE;
    ctx->busy = false;
    return 0;
}

/* At most two subtraction-based comparisons between actual saved checks.
 * Supplied row values may alias protected destination columns: capture owns
 * that case. Full object-lifetime disjointness remains the C caller contract. */
int
wl_columnar_eval_stack_row_debt_geometry_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner,
    const wl_columnar_relation_row_debt_t *debt, const int64_t *values,
    uint32_t width, const wl_columnar_relation_row_debt_status_t *status,
    wl_evaluation_control_t *control, uint64_t id, bool forward)
{
    if (!frame || !owner || !debt || !status || !control || !id
        || (forward && !values)) return EINVAL;
    uint64_t row_bytes = (uint64_t)width * sizeof(*values);
    if (row_bytes > SIZE_MAX) return EOVERFLOW;
    const void *objects[] = {debt, status, values, frame, owner,
                             frame->filter.output};
    size_t lengths[] = {sizeof(*debt), sizeof(*status), (size_t)row_bytes,
                        sizeof(wl_columnar_eval_stack_allocation_t),
                        sizeof(wl_columnar_eval_plan_numeric_attempt_t),
                        sizeof(col_rel_t)};
    for (unsigned i = 0; i < 6; ++i) {
        int rc = wl_evaluation_control_charge_attempt(control, id, 0);
        if (rc) return rc;
        if ((!objects[i] && lengths[i]) || (lengths[i]
            && lengths[i] - 1 > UINTPTR_MAX - (uintptr_t)objects[i]))
            return EINVAL;
    }
    static const unsigned pairs[][2] = {
        {0, 1}, {0, 2}, {0, 3}, {0, 4}, {0, 5},
        {1, 2}, {1, 3}, {1, 4}, {1, 5}
    };
    for (unsigned i = 0; i < sizeof(pairs) / sizeof(pairs[0]); i += 2) {
        int rc = wl_evaluation_control_charge_attempt(control, id, 0);
        if (rc) return rc;
        for (unsigned j = i; j < i + 2 && j < sizeof(pairs)/sizeof(pairs[0]);
            ++j) {
            unsigned a = pairs[j][0], b = pairs[j][1];
            if (lengths[a] && lengths[b]
                && !wl_columnar_eval_stack_copy_ranges_disjoint(objects[a],
                lengths[a], objects[b], lengths[b])) return EINVAL;
        }
    }
    return wl_evaluation_control_charge_attempt(control, id, 0);
}

static bool
wl_columnar_eval_stack_frame_copy_output(
    const wl_columnar_eval_stack_cleanup_frame_t *frame,
    const void *out, size_t bytes)
{
    return out && (uintptr_t)out <= UINTPTR_MAX - bytes
           && wl_columnar_eval_stack_copy_ranges_disjoint(frame,
               sizeof(wl_columnar_eval_stack_allocation_t),
               out, bytes);
}

bool
wl_columnar_eval_stack_protocol_copy_output(
    const wl_columnar_eval_stack_cleanup_frame_t *frame,
    const void *out, size_t bytes)
{
    /* Expand this first range directly: STATUS retains its original384
     * envelope including the complete four-binding owner proof. */
    uintptr_t base = (uintptr_t)frame, dest = (uintptr_t)out;
    return out && dest <= UINTPTR_MAX - bytes
           && (dest < base ? base - dest >= bytes
                          : dest - base >=
           sizeof(wl_columnar_eval_stack_allocation_t))
           && (!frame->prepared_unary
           || wl_columnar_eval_stack_copy_ranges_disjoint(
               frame->prepared_unary, sizeof(*frame->prepared_unary), out,
               bytes))
           && (!frame->numeric_plan_owner
           || wl_columnar_eval_stack_copy_ranges_disjoint(
               frame->numeric_plan_owner,
               sizeof(wl_columnar_eval_plan_numeric_attempt_t), out, bytes));
}

bool
wl_columnar_eval_stack_marked_copy_output(const wl_col_session_t *session,
    const void *out, size_t bytes)
{
    if (!session || !session->cleanup_active) return true;
    const wl_columnar_eval_stack_cleanup_frame_t *frame =
        session->cleanup_active;
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)
        || frame->session != session) return false;
    return wl_columnar_eval_stack_protocol_copy_output(frame, out, bytes);
}

static bool
wl_columnar_eval_stack_host_range(
    const wl_columnar_eval_stack_cleanup_frame_t *frame,
    const void *out, size_t bytes)
{
    const wl_columnar_eval_stack_allocation_t *allocation = (const void *)frame;
    return wl_columnar_eval_stack_frame_copy_output(frame, out, bytes)
           && (!frame->prepared_unary
           || frame->prepared_unary == &allocation->automatic.receipt
           || wl_columnar_eval_stack_copy_ranges_disjoint(
               frame->prepared_unary, sizeof(*frame->prepared_unary), out,
               bytes))
           && (!frame->numeric_plan_owner
           || frame->numeric_plan_owner == &frame->framed.child
           || wl_columnar_eval_stack_copy_ranges_disjoint(
               frame->numeric_plan_owner,
               sizeof(wl_columnar_eval_plan_numeric_attempt_t), out, bytes));
}

bool
wl_columnar_eval_stack_host_geometry(const wl_col_session_t *session,
    const wl_columnar_eval_serial_framed_host_request_t *request)
{
    const wl_columnar_eval_stack_cleanup_frame_t *frame = session
        ? session->cleanup_active : NULL;
    if (frame && (frame->identity != frame || frame->session != session
        || frame->stack.top > COL_STACK_MAX
        || !wl_columnar_eval_stack_host_range(frame, request,
        sizeof(*request))))
        return false;
    const wl_columnar_eval_serial_framed_host_reply_t *reply = request->reply;
    return !reply || ((!frame || wl_columnar_eval_stack_host_range(frame,
           reply, sizeof(*reply)))
           && wl_columnar_eval_stack_copy_ranges_disjoint(request,
           sizeof(*request),
           reply, sizeof(*reply)));
}

int
wl_columnar_eval_stack_framed_authenticate(
    const wl_columnar_eval_stack_cleanup_frame_t *frame,
    const wl_col_session_t *session,
    const wl_columnar_eval_serial_framed_attempt_t *expected)
{
    if (!frame || !session || !expected) return EINVAL;
    if (session->cleanup_active != frame) return EBUSY;
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)
        || frame->session != session ||
        &frame->framed != expected) return EINVAL;
    return wl_columnar_eval_serial_framed_owner_valid(expected,
               frame) ? 0 : EINVAL;
}

int
wl_columnar_eval_stack_framed_writable(
    const wl_columnar_eval_stack_cleanup_frame_t *frame,
    const wl_col_session_t *session,
    const wl_columnar_eval_serial_framed_attempt_t *expected)
{
    if (!frame || !session || !expected) return EINVAL;
    if (session->cleanup_active != frame) return EBUSY;
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)
        || frame->session != session ||
        &frame->framed != expected) return EINVAL;
    if (frame->prepared_unary) return EBUSY;
    return wl_columnar_eval_serial_framed_owner_valid(expected,
               frame) ? 0 : EINVAL;
}

static void
wl_columnar_eval_stack_framed_status_copy(
    const wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_columnar_eval_serial_framed_status_t *out)
{
    out->phase = ctx->phase; out->execution_result = ctx->primary_result;
    out->cleanup_result = ctx->cleanup_result;
    out->ready = ctx->phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_READY;
    out->retained = ctx->cleanup_result != 0; out->committed = ctx->committed;
}

int
wl_columnar_eval_stack_framed_inspect(
    const wl_columnar_eval_stack_cleanup_frame_t *frame,
    const wl_col_session_t *session, wl_columnar_eval_serial_framed_view_t *out)
{
    if (!frame || !session || !out) return EINVAL;
    if (session->cleanup_active != frame) return EBUSY;
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)
        || frame->session != session) return EINVAL;
    if (!wl_columnar_eval_stack_protocol_copy_output(frame, out,
        sizeof(*out))) return EINVAL;
    const wl_columnar_eval_serial_framed_attempt_t *ctx = &frame->framed;
    /* Host inspection retains the old host's invalid-owner/native mapping. */
    if (!wl_columnar_eval_serial_framed_owner_valid(ctx, frame)) return EBUSY;
    out->plan = ctx->plan; out->session = ctx->session;
    out->control = ctx->control;
    out->private_bindings = ctx->private_bindings;
    out->capture_out = (const void *)ctx->capture_out;
    out->attempt_id = ctx->attempt_id;
    out->operation_identity = ctx->operation_identity;
    out->capture_only = ctx->capture_only;
    out->controlled_materialized_batch_fallback =
        ctx->controlled_materialized_batch_fallback;
    out->abort_supported = ctx->abort_supported;
    out->prepared_unary_present = frame->prepared_unary != NULL;
    out->cleanup_receipt_key = ctx->cleanup_receipt_key;
    wl_columnar_eval_stack_framed_status_copy(ctx, &out->status);
    return 0;
}

static bool
wl_columnar_eval_stack_framed_key_matches(
    const wl_columnar_eval_stack_cleanup_frame_t *frame,
    const wl_col_session_t *session,
    const wl_columnar_eval_serial_framed_attempt_t *ctx,
    const wl_columnar_eval_cleanup_receipt_key_t *key)
{
    return key->session_generation ==
           session->cleanup_receipt_session_generation
           && key->receipt_id == frame->cleanup_receipt_id
           && key->plan_epoch == frame->cleanup_receipt_plan_epoch
           && key->operation_attempt_id == ctx->attempt_id
           && key->operation_id == ctx->operation_identity
           && wl_columnar_eval_cleanup_receipt_key_equal(key,
               &ctx->cleanup_receipt_key);
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_stack_test_framed_status_checkpoint)(
    wl_columnar_eval_stack_cleanup_frame_t *, wl_col_session_t *);
#endif

int
wl_columnar_eval_stack_framed_status(
    wl_columnar_eval_stack_cleanup_frame_t *frame, wl_col_session_t *session,
    wl_columnar_eval_serial_framed_status_t *out,
    wl_columnar_eval_cleanup_receipt_key_t *key_out,
    wl_evaluation_control_t *funding, uint64_t funding_id)
{
    if (!frame || !session) return EINVAL;
    if (session->cleanup_active != frame) return EBUSY;
    const wl_columnar_eval_serial_framed_attempt_t *ctx = &frame->framed;
    int rc = wl_columnar_eval_stack_framed_authenticate(frame, session, ctx);
    if (rc) return rc;
    if (!wl_columnar_eval_stack_protocol_copy_output(frame, out, sizeof(*out))
        || !wl_columnar_eval_stack_protocol_copy_output(frame, key_out,
        sizeof(*key_out))
        || !wl_columnar_eval_stack_copy_ranges_disjoint(out, sizeof(*out),
        key_out, sizeof(*key_out)))
        return EINVAL;
    wl_evaluation_control_t *control = ctx->control;
    uint64_t identity = wl_evaluation_control_identity(control);
    uint64_t id = ctx->attempt_id, operation = ctx->operation_identity;
    wl_columnar_eval_cleanup_receipt_key_t key = ctx->cleanup_receipt_key;
    if (!wl_columnar_eval_stack_framed_key_matches(frame, session, ctx, &key)
        || !funding || funding == control || !funding_id
        || wl_evaluation_control_active_attempt_id(funding) !=
        funding_id) return EINVAL;
    rc = wl_evaluation_control_charge_attempt(funding, funding_id,
            WL_COLUMNAR_EVAL_SERIAL_FRAMED_HOST_WORK);
    if (rc) return rc;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_stack_test_framed_status_checkpoint)
        wl_columnar_eval_stack_test_framed_status_checkpoint(frame, session);
#endif
    /* The checkpoint can genuinely retire/recreate the frame. Prove live
     * lineage before ANY saved frame/context access, then prove generation. */
    if (session->cleanup_active != frame) return EBUSY;
    rc = wl_columnar_eval_stack_framed_authenticate(frame, session, ctx);
    if (rc) return rc;
    if (ctx->control != control ||
        wl_evaluation_control_identity(control) != identity
        || ctx->attempt_id != id || ctx->operation_identity != operation
        || !wl_columnar_eval_stack_framed_key_matches(frame, session, ctx, &key)
        || wl_evaluation_control_active_attempt_id(funding) !=
        funding_id) return EINVAL;
    if (!wl_columnar_eval_stack_protocol_copy_output(frame, out, sizeof(*out))
        || !wl_columnar_eval_stack_protocol_copy_output(frame, key_out,
        sizeof(*key_out))) return EINVAL;
    wl_columnar_eval_stack_framed_status_copy(ctx, out);
    *key_out = key;
    return 0;
}

#ifdef WL_SESSION_TEST_HOOKS
int
wl_columnar_eval_stack_test_unary_output_segments_owned(
    wl_columnar_eval_stack_cleanup_frame_t *f, const void *owner,
    uint32_t *segments, uint32_t count)
{
    int rc = wl_columnar_eval_stack_numeric_writable(f, owner);
    if (rc) return rc;
    wl_columnar_filter_context_t *ctx = &f->filter;
    if (!segments || count != 1
        || !wl_columnar_eval_stack_frame_copy_output(f, segments,
        2 * sizeof(*segments))
        || !wl_columnar_eval_stack_copy_ranges_disjoint(owner,
        sizeof(wl_columnar_eval_plan_numeric_attempt_t), segments,
        2 * sizeof(*segments)))
        return EINVAL;
    if (!wl_columnar_eval_stack_unary_thread_valid(f)
        || f->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE ||
        f->unary_committed
        || f->unary_scalar_mode || f->unary_scalar_started ||
        f->unary_scalar_busy
        || f->unary_scalar_prepared || f->unary_scalar_committed ||
        f->unary_scalar_for_old
        || f->unary_scalar_init_target || f->unary_scalar_cancel_clear ||
        f->unary_scalar_drained
        || f->unary_scalar_initializer.identity ||
        f->unary_scalar_retirement.storage.identity
        || ctx->identity != ctx || ctx->frame != f || ctx->session != f->session
        || ctx->stage != WL_COLUMNAR_FILTER_STAGE_FINISH || ctx->busy
        || !ctx->thread_valid || !ctx->unary_started || ctx->committed
        || ctx->control || ctx->pin_owned || ctx->output
        || ctx->attempt_id != f->unary_attempt_id
        || !f->unary_output.owned || !f->unary_output.rel
        || f->unary_output.kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
        || f->unary_output.continuation || f->unary_output.seg_boundaries
        || f->unary_output.seg_count ||
        ctx->private_output != f->unary_output.rel
        || segments == f->unary_snapshot.seg_boundaries
        || segments[0] || segments[1] != f->unary_output.rel->nrows)
        return EINVAL;
#if defined(WL_HAVE_C11_THREADS)
    if (!thrd_equal(ctx->owner_thread, thrd_current())) return EINVAL;
#elif defined(_WIN32) || defined(_WIN64)
    if (ctx->owner_thread != GetCurrentThreadId()) return EINVAL;
#else
    if (!pthread_equal(ctx->owner_thread, pthread_self())) return EINVAL;
#endif
    f->unary_output.seg_boundaries = segments; f->unary_output.seg_count = 1;
    return 0;
}

int
wl_columnar_eval_stack_test_unary_observe(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner,
    wl_columnar_eval_stack_test_unary_observation_t *out)
{
    int rc = wl_columnar_eval_stack_numeric_owner(frame, owner);
    if (rc) return rc;
    if (frame->session->cleanup_active != frame ||
        !wl_columnar_eval_stack_protocol_copy_output(frame, out,
        sizeof(*out))) return EINVAL;
    const eval_entry_t *entry =
        frame->unary_committed ? &frame->unary_retired : &frame->unary_output;
    out->relation = entry->rel; out->segments = entry->seg_boundaries;
    out->continuation = entry->continuation;
    out->segment_count = entry->seg_count;
    out->kind = (unsigned)entry->kind;
    out->phase = (unsigned)frame->unary_phase;
    out->first_result = frame->unary_first_result; out->owned = entry->owned;
    out->is_delta = entry->is_delta; out->committed = frame->unary_committed;
    out->result_saved = frame->unary_result_saved;
    out->snapshot_relation = frame->unary_snapshot.rel;
    out->snapshot_segments = frame->unary_snapshot.seg_boundaries;
    out->snapshot_segment_count = frame->unary_snapshot.seg_count;
    out->scalar_init_phase = frame->unary_scalar_initializer.phase;
    out->scalar_started = frame->unary_scalar_started;
    /* The staged initializer may not have zeroed the whole raw scalar yet.
     * Only actual BIND makes those fields meaningful to an observer. */
    out->scalar_phase = frame->unary_scalar_started
        ? frame->unary_scalar_retirement.phase : 0;
    out->managed_phase = frame->unary_scalar_started
        ? (unsigned)frame->unary_scalar_retirement.storage.phase : 0;
    out->scalar_ready = frame->unary_scalar_started
        && frame->unary_scalar_retirement.ready;
    out->scalar_ledger_pending = frame->unary_scalar_started
        && frame->unary_scalar_retirement.ledger
        && (frame->unary_scalar_retirement.ledger_bytes
        || frame->unary_scalar_retirement.ledger_timestamp_bytes);
    for (unsigned i = 0; i < 4; ++i) {
        out->credit_started[i] = frame->unary_scalar_started
            && frame->unary_scalar_retirement.storage.receipt_started[i];
        out->credit_empty[i] = frame->unary_scalar_started
            && frame->unary_scalar_retirement.storage.token_empty[i];
    }
    return 0;
}
#endif

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_columnar_eval_stack_test_unary_prepared_checkpoint)(
    wl_columnar_eval_stack_unary_prepared_t *);
#endif

static bool
wl_columnar_eval_stack_prepared_thread(
    const wl_columnar_eval_stack_unary_prepared_t *p)
{
    if (!p->thread_valid) return false;
#if defined(WL_HAVE_C11_THREADS)
    return thrd_equal(p->owner_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return p->owner_thread == GetCurrentThreadId();
#else
    return pthread_equal(p->owner_thread, pthread_self()) != 0;
#endif
}

static bool
wl_columnar_eval_stack_prepared_reader_inert(
    const wl_columnar_source_access_reader_t *r)
{
    return !r->owner && !r->secondary_owner && !r->identity && !r->thread_valid
           && !r->transferable && !r->acquire_stage && !r->release_stage;
}

static bool
wl_columnar_eval_stack_prepared_work_inert(
    const wl_columnar_eval_stack_unary_prepared_t *p)
{
    const wl_columnar_eval_stack_unary_prepared_work_t *w = &p->work;
    return wl_columnar_eval_stack_unary_entry_inert(&w->original)
           && wl_columnar_eval_stack_unary_entry_inert(&w->output)
           && wl_columnar_eval_stack_prepared_reader_inert(&w->old_reader)
           && wl_columnar_eval_stack_prepared_reader_inert(&w->output_reader)
           && !w->old_snapshot.relation_identity &&
           !w->old_snapshot.view_generation
           && !w->old_snapshot.storage_generation &&
           !w->output_snapshot.relation_identity
           && !w->output_snapshot.view_generation &&
           !w->output_snapshot.storage_generation
           && !w->old_canonical && !w->output_canonical
           && !w->old_identity && !w->old_generation && !w->output_identity &&
           !w->output_generation;
}

static bool
wl_columnar_eval_stack_prepared_terminal_inert(
    const wl_columnar_eval_stack_unary_prepared_t *p)
{
    return !p->frame && !p->session && !p->numeric_owner && !p->plan &&
           !p->operation
           && !p->control && !p->control_identity && !p->attempt_id &&
           !p->control_ref
           && !p->old_held && !p->output_held && !p->commit_prepaid &&
           !p->scalar_ready
           && !p->top && !p->index && !p->numeric_cursor && !p->numeric_phase &&
           !p->alias_cursor
           && !p->alias_complete && !p->needs_sanitize && !p->disposition
           && wl_columnar_eval_stack_prepared_work_inert(p);
}

/* Marker lifetime, not a recursive full parent validator, protects this
 * immutable binding. Every generic parent mutation must respect the marker. */
static int
wl_columnar_eval_stack_prepared_validate(
    const wl_columnar_eval_stack_unary_prepared_t *p)
{
    if (!p || p->identity != p ||
        !wl_columnar_eval_stack_prepared_thread(p)) return EINVAL;
    if (p->busy) return EBUSY;
    if (!p->session || !p->frame || !p->numeric_owner || !p->control ||
        !p->control_ref) return EINVAL;
    if (p->session->cleanup_active != p->frame) return EBUSY;
    const wl_columnar_eval_stack_cleanup_frame_t *f = p->frame;
    const wl_columnar_eval_plan_numeric_attempt_t *n = p->numeric_owner;
    if (f->identity != f || f->session != p->session || f->prepared_unary != p
        || f->numeric_plan_owner != n || f->unary_control != p->control
        || f->unary_attempt_id != p->attempt_id || n->identity != n
        || n->control != p->control || n->attempt_id != p->attempt_id
        || n->phase != p->numeric_phase || n->cursor != p->numeric_cursor
        || n->plan != p->plan) return EINVAL;
    return 0;
}

static int
wl_columnar_eval_stack_prepared_funding_check(
    const wl_columnar_eval_stack_unary_prepared_t *p,
    wl_evaluation_control_t *funding, uint64_t id)
{
    if (!funding || !id || funding == p->control
        || wl_evaluation_control_identity(funding) == p->control_identity
        || wl_evaluation_control_active_attempt_id(funding) !=
        id) return EINVAL;
    return 0;
}

static void
wl_columnar_eval_stack_prepared_checkpoint(
    wl_columnar_eval_stack_unary_prepared_t *p)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_columnar_eval_stack_test_unary_prepared_checkpoint)
        wl_columnar_eval_stack_test_unary_prepared_checkpoint(p);
#else
    (void)p;
#endif
}

static bool
wl_columnar_eval_stack_prepared_entry_equal(const eval_entry_t *a,
    const eval_entry_t *b)
{
    return a->rel == b->rel && a->owned == b->owned &&
           a->is_delta == b->is_delta
           && a->seg_boundaries == b->seg_boundaries &&
           a->seg_count == b->seg_count
           && a->kind == b->kind && a->continuation == b->continuation;
}

static int
wl_columnar_eval_stack_prepared_entries(
    const wl_columnar_eval_stack_unary_prepared_t *p)
{
    const wl_columnar_eval_stack_cleanup_frame_t *f = p->frame;
    if (f->stack.top != p->top || f->unary_top != p->top ||
        f->unary_index != p->index
        || !p->top || p->index != p->top - 1 ||
        f->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE
        || !wl_columnar_eval_stack_prepared_entry_equal(
            &f->stack.items[p->index], &p->work.original)
        || !wl_columnar_eval_stack_prepared_entry_equal(&f->unary_snapshot,
        &p->work.original)
        || !wl_columnar_eval_stack_prepared_entry_equal(&f->unary_output,
        &p->work.output)) return EINVAL;
    return 0;
}

static int
wl_columnar_eval_stack_prepared_scalar_ready(
    const wl_columnar_eval_stack_unary_prepared_t *p)
{
    const wl_columnar_eval_stack_cleanup_frame_t *f = p->frame;
    if (!f->unary_scalar_mode || !f->unary_scalar_started ||
        !f->unary_scalar_prepared
        || !f->unary_scalar_for_old || f->unary_scalar_committed
        || f->unary_scalar_init_target || f->unary_scalar_initializer.identity
        || f->unary_scalar_busy || f->unary_scalar_cancel_clear ||
        f->unary_scalar_drained)
        return EINVAL;
    return wl_columnar_relation_scalar_retirement_ready_validate(
        &f->unary_scalar_retirement,
        p->work.original.rel, p->work.old_snapshot);
}

static int
wl_columnar_eval_stack_prepared_reader_valid(const col_rel_t *rel,
    const wl_columnar_source_access_reader_t *reader,
    col_relation_snapshot_t snapshot,
    const col_rel_t *canonical, uint64_t identity, uint64_t generation)
{
    if (!rel || !canonical || reader->identity != (uintptr_t)reader
        || reader->acquire_stage || reader->release_stage ||
        reader->transferable
        || !wl_columnar_source_access_reader_thread_equal(reader)
        || reader->secondary_owner != &rel->descriptor_access
        || !wl_columnar_relation_snapshot_equal(snapshot,
        wl_columnar_relation_snapshot(rel))) return EINVAL;
    col_rel_t *actual = NULL;
    int rc = col_rel_storage_owner_resolve(rel, &actual);
    if (rc) return rc;
    if (actual != canonical || actual->relation_identity != identity
        || actual->storage_generation != generation ||
        reader->owner != &actual->source_access) return EINVAL;
    uint64_t descriptor = atomic_load_explicit(&reader->secondary_owner->state,
            memory_order_acquire);
    uint64_t storage = atomic_load_explicit(&reader->owner->state,
            memory_order_acquire);
    return !descriptor || descriptor == WL_COLUMNAR_SOURCE_ACCESS_WRITER
           || !storage ||
           storage == WL_COLUMNAR_SOURCE_ACCESS_WRITER ? EINVAL : 0;
}

int
wl_columnar_eval_stack_unary_prepared_begin(
    wl_columnar_eval_stack_unary_prepared_t *p,
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    const wl_columnar_eval_plan_numeric_attempt_t *owner)
{
    if (!p || !frame || !owner) return EINVAL;
    int rc = wl_columnar_eval_stack_numeric_owner(frame, owner);
    if (rc) return rc;
    if (!wl_columnar_eval_stack_frame_copy_output(frame, p, sizeof(*p))
        || !wl_columnar_eval_stack_copy_ranges_disjoint(owner, sizeof(*owner),
        p, sizeof(*p))) return EINVAL;
    if (p->identity && p->identity != p) return EINVAL;
    if (p->busy) return EBUSY;
    if (p->identity && (p->phase != WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED
        || !wl_columnar_eval_stack_prepared_thread(p)
        || !wl_columnar_eval_stack_prepared_terminal_inert(p))) return EBUSY;
    if (!p->identity && (p->phase != WL_COLUMNAR_EVAL_STACK_PREPARED_INERT
        || p->thread_valid ||
        !wl_columnar_eval_stack_prepared_terminal_inert(p))) return EINVAL;
    if (frame->prepared_unary || frame->stream_host ||
        frame->filter.stage != WL_COLUMNAR_FILTER_STAGE_INERT
        || frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE
        || !wl_columnar_eval_stack_unary_thread_valid(frame) ||
        frame->unary_control != owner->control
        || frame->unary_attempt_id != owner->attempt_id || !owner->plan ||
        !owner->plan->ops
        || owner->cursor >= owner->end || owner->end > owner->plan->op_count
        || !frame->unary_top || frame->unary_top != frame->stack.top
        || frame->unary_index != frame->unary_top - 1
        || frame->unary_scalar_busy || frame->unary_scalar_init_target
        || frame->unary_scalar_cancel_clear || frame->unary_scalar_drained
        || !wl_columnar_eval_stack_unary_entry_inert(
            &frame->unary_retired)) return EBUSY;
    /* Cheap eligibility is not authority. Full real writer proof is a later
     * independently paid action before PREPARED/SEALED can be exposed. */
    bool owned = frame->unary_snapshot.owned;
    if (owned && (!frame->unary_scalar_mode || !frame->unary_scalar_prepared
        || !frame->unary_scalar_started || !frame->unary_scalar_for_old
        || !frame->unary_scalar_retirement.ready ||
        frame->unary_scalar_committed
        || frame->unary_scalar_retirement.committed
        || frame->unary_scalar_retirement.storage.relation !=
        frame->unary_snapshot.rel)) return EBUSY;
    rc = wl_evaluation_control_charge_attempt(owner->control, owner->attempt_id,
            256);
    if (rc) return rc;
    rc = wl_evaluation_control_retain(owner->control);
    if (rc) return rc;
    /* Charge/retain are callback-free. No premarker callback can retire the
     * genuine already-authenticated owner; new hooks run only under marker. */
    memset(p, 0, sizeof(*p));
    p->frame = frame; p->session = frame->session; p->numeric_owner = owner;
    p->plan = owner->plan; p->numeric_cursor = owner->cursor;
    p->numeric_phase = owner->phase;
    p->operation = &owner->plan->ops[owner->cursor];
    p->control = owner->control;
    p->control_identity = wl_evaluation_control_identity(owner->control);
    p->attempt_id = owner->attempt_id; p->control_ref = true;
    p->scalar_ready = owned; p->top = frame->unary_top;
    p->index = frame->unary_index;
#if defined(WL_HAVE_C11_THREADS)
    p->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    p->owner_thread = GetCurrentThreadId();
#else
    p->owner_thread = pthread_self();
#endif
    p->thread_valid = true; p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_HEAD;
    p->identity = p; frame->prepared_unary = p;
    return 0;
}

static int
wl_columnar_eval_stack_prepared_join_begin_core(
    wl_columnar_eval_stack_unary_prepared_t *p,
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    const wl_columnar_eval_plan_numeric_attempt_t *owner)
{
    int rc;
    const wl_columnar_filter_context_t *ctx = &frame->filter;
    if (p->identity && p->identity != p) return EINVAL;
    if (p->busy) return EBUSY;
    if (p->identity && (p->phase != WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED
        || !wl_columnar_eval_stack_prepared_thread(p)
        || !wl_columnar_eval_stack_prepared_terminal_inert(p))) return EBUSY;
    if (!p->identity && (p->phase != WL_COLUMNAR_EVAL_STACK_PREPARED_INERT
        || p->thread_valid ||
        !wl_columnar_eval_stack_prepared_terminal_inert(p))) return EINVAL;
    if (frame->prepared_unary || frame->stream_host ||
        ctx->identity != ctx || ctx->frame != frame ||
        ctx->session != frame->session
        || ctx->stage != WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE || ctx->busy
        || !ctx->thread_valid || !ctx->pin_owned || !ctx->unary_started
        || !ctx->join_decision_pending || ctx->control != owner->control
        || ctx->attempt_id != owner->attempt_id
        || frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE
        || !wl_columnar_eval_stack_unary_thread_valid(frame) ||
        frame->unary_control != owner->control
        || frame->unary_attempt_id != owner->attempt_id || !owner->plan ||
        !owner->plan->ops
        || owner->cursor >= owner->end || owner->end > owner->plan->op_count
        || !frame->unary_top || frame->unary_top != frame->stack.top
        || frame->unary_index != frame->unary_top - 1
        || frame->unary_scalar_busy || frame->unary_scalar_init_target
        || frame->unary_scalar_cancel_clear || frame->unary_scalar_drained
        || !wl_columnar_eval_stack_unary_entry_inert(
            &frame->unary_retired)) return EBUSY;
    rc = wl_evaluation_control_charge_attempt(owner->control, owner->attempt_id,
            256);
    if (rc) return rc;
    rc = wl_evaluation_control_retain(owner->control);
    if (rc) return rc;
    /* Charge/retain are callback-free. No premarker callback can retire the
     * genuine already-authenticated owner; new hooks run only under marker. */
    memset(p, 0, sizeof(*p));
    p->frame = frame; p->session = frame->session; p->numeric_owner = owner;
    p->plan = owner->plan; p->numeric_cursor = owner->cursor;
    p->numeric_phase = owner->phase;
    p->operation = &owner->plan->ops[owner->cursor];
    p->control = owner->control;
    p->control_identity = wl_evaluation_control_identity(owner->control);
    p->attempt_id = owner->attempt_id; p->control_ref = true;
    p->top = frame->unary_top;
    p->index = frame->unary_index;
#if defined(WL_HAVE_C11_THREADS)
    p->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    p->owner_thread = GetCurrentThreadId();
#else
    p->owner_thread = pthread_self();
#endif
    p->thread_valid = true;
    p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_HEAD;
    p->identity = p; frame->prepared_unary = p;
    return 0;
}

int
wl_columnar_eval_stack_unary_prepared_join_begin(
    wl_columnar_eval_stack_unary_prepared_t *p,
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    const wl_columnar_eval_plan_numeric_attempt_t *owner)
{
    if (!p || !frame || !owner) return EINVAL;
    int rc = wl_columnar_eval_stack_numeric_owner(frame, owner);
    if (rc) return rc;
    if (!wl_columnar_eval_stack_frame_copy_output(frame, p, sizeof(*p))
        || !wl_columnar_eval_stack_copy_ranges_disjoint(owner, sizeof(*owner),
        p, sizeof(*p))) return EINVAL;
    return wl_columnar_eval_stack_prepared_join_begin_core(p, frame, owner);
}

static bool
wl_columnar_eval_stack_prepared_join_thread(
    const wl_columnar_filter_context_t *ctx)
{
#if defined(WL_HAVE_C11_THREADS)
    return thrd_equal(ctx->owner_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return ctx->owner_thread == GetCurrentThreadId();
#else
    return pthread_equal(ctx->owner_thread, pthread_self()) != 0;
#endif
}

/* Refusal-only fixed proof. In particular, cleanup must not poll the old
 * original: its genuine successor may already be active on that handle. */
static int
wl_columnar_eval_stack_prepared_join_confirm(
    const wl_columnar_eval_stack_unary_prepared_t *p)
{
    const wl_columnar_eval_stack_cleanup_frame_t *f = p->frame;
    const wl_columnar_eval_plan_numeric_attempt_t *n = p->numeric_owner;
    const wl_columnar_filter_context_t *ctx = &f->filter;
    if (ctx->identity != ctx || ctx->frame != f || ctx->session != p->session
        || p->session->cleanup_active != f
        || ctx->stage != WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE
        || ctx->busy || !ctx->thread_valid
        || !wl_columnar_eval_stack_prepared_join_thread(ctx)
        || !ctx->pin_owned || !ctx->unary_started || !ctx->join_decision_pending
        || ctx->control != p->control || ctx->attempt_id != p->attempt_id
        || ctx->committed || ctx->execution_result || ctx->semantic_result
        || ctx->cleanup_result || ctx->join_metadata_busy
        || f->unary_control != p->control ||
        f->unary_attempt_id != p->attempt_id)
        return EINVAL;
    bool child = n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD
        || n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD
        || ((n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING
        || n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING)
        && (n->pending_action == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD
        || n->pending_action ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD));
    if (!child || !p->plan || !p->plan->ops || n->cursor >= n->end
        || n->end > p->plan->op_count
        || &p->plan->ops[n->cursor] != p->operation
        || ctx->join_operation != p->operation ||
        p->operation->op != WL_PLAN_OP_JOIN
        || ctx->join_disposition != WL_COLUMNAR_JOIN_DISPOSITION_REPLACE
        || ctx->join_input_snapshot.relation_identity
        || ctx->join_input_snapshot.view_generation
        || ctx->join_input_snapshot.storage_generation
        || ctx->private_output != f->unary_output.rel
        || ctx->output != f->unary_output.rel
        || f->unary_scalar_mode || f->unary_scalar_started ||
        f->unary_scalar_prepared
        || f->unary_scalar_committed || f->unary_scalar_for_old
        || f->unary_scalar_initializer.identity ||
        f->unary_scalar_retirement.storage.identity)
        return EINVAL;
    return p->join_mode && p->publication_is_delta != ctx->join_result_is_delta
           ? EINVAL : 0;
}

static bool
wl_columnar_eval_stack_prepared_empty_source_inert(
    const wl_columnar_relation_empty_source_t *source)
{
    return (!source->identity
           && source->phase == WL_COLUMNAR_RELATION_EMPTY_SOURCE_INERT)
           || (source->identity == source
           && source->phase == WL_COLUMNAR_RELATION_EMPTY_SOURCE_DRAINED);
}

static int
wl_columnar_eval_stack_prepared_context_state(
    const wl_columnar_filter_context_t *ctx)
{
    if (ctx->source_initialized || ctx->join_right_initialized
        || ctx->source_bare || ctx->join_right_bare || ctx->join_initialized
        || ctx->batch_initialized || ctx->program || ctx->scalar_program
        || ctx->heap || ctx->admitted || ctx->ref_held || ctx->governor
        || ctx->table.programs || ctx->table.scalars ||
        ctx->table.scalar_allocated
        || ctx->table.scalar_admitted || ctx->table.scalar_initialized_slots
        || ctx->table.scalar_committed || ctx->table.allocated ||
        ctx->table.admitted
        || ctx->table.ref_held || ctx->table.governor
        || !wl_columnar_eval_stack_prepared_empty_source_inert(
            &ctx->source_empty_reader)
        || !wl_columnar_eval_stack_prepared_empty_source_inert(
            &ctx->join_empty_reader))
        return EINVAL;
    return 0;
}

static int
wl_columnar_eval_stack_prepared_join_state(
    const wl_columnar_eval_stack_unary_prepared_t *p)
{
    return wl_columnar_eval_stack_prepared_context_state(&p->frame->filter);
}

static int
wl_columnar_eval_stack_prepared_context_scratch(
    const wl_columnar_filter_context_t *ctx, unsigned index)
{
    if (index >= WL_COLUMNAR_JOIN_SCRATCH_COUNT) return EINVAL;
    const wl_columnar_join_scratch_t *s = &ctx->join_scratch[index];
    return s->initialized || s->allocated || s->admitted || s->committed
           || s->ref_held || s->physical || s->governor ? EINVAL : 0;
}

static int
wl_columnar_eval_stack_prepared_join_scratch(
    const wl_columnar_eval_stack_unary_prepared_t *p, unsigned index)
{
    return wl_columnar_eval_stack_prepared_context_scratch(&p->frame->filter,
               index);
}

/* A new special192 confirmation is funded only for an attempted ModeB whose
 * first original-funded proof never completed. Confirmed contexts and ModeA
 * retain their original128 cancellation. No unsupported context is modified. */
static void
wl_columnar_eval_stack_prepared_join_error(
    wl_columnar_eval_stack_unary_prepared_t *p)
{
    if (p->join_mode && !p->committed && p->first_result) {
        wl_columnar_filter_context_t *ctx = &p->frame->filter;
        if (!ctx->execution_result) ctx->execution_result = p->first_result;
        if (!ctx->semantic_result) ctx->semantic_result = p->first_result;
    }
}

static int
wl_columnar_eval_stack_prepared_old_validate(
    wl_columnar_eval_stack_unary_prepared_t *p)
{
    int rc = wl_columnar_eval_stack_prepared_entries(p);
    if (rc) return rc;
    if (p->scalar_ready) return wl_columnar_eval_stack_prepared_scalar_ready(p);
    if (!p->old_held) return EINVAL;
    return wl_columnar_eval_stack_prepared_reader_valid(p->work.original.rel,
               &p->work.old_reader, p->work.old_snapshot, p->work.old_canonical,
               p->work.old_identity, p->work.old_generation);
}

static int
wl_columnar_eval_stack_prepared_output_validate(
    wl_columnar_eval_stack_unary_prepared_t *p)
{
    int rc = wl_columnar_eval_stack_prepared_entries(p);
    if (rc) return rc;
    if (!p->output_held) return EINVAL;
    return wl_columnar_eval_stack_prepared_reader_valid(p->work.output.rel,
               &p->work.output_reader, p->work.output_snapshot,
               p->work.output_canonical,
               p->work.output_identity, p->work.output_generation);
}

/* Enum order is private. All actual action indices are checked after the
 * checkpoint; no designated-array extension is required by MSVC. */
static const uint16_t wl_columnar_eval_stack_prepared_fees[] = {
    0,                                  /* INERT */
    192,                                /* HEAD */
    192, 192, 128, 128, 128,             /* JOIN header/state/scratch */
    128, 128, 128, 128, 192, 192, 128,   /* aliases/readers/validation/READY */
    192, 192, 128, 128, 128,             /* JOIN seal header/state/scratch */
    192, 192, 128                       /* old/output seal, latch */
};
typedef char wl_columnar_eval_stack_prepared_fee_count_fits_t[
        sizeof(wl_columnar_eval_stack_prepared_fees)
        / sizeof(wl_columnar_eval_stack_prepared_fees[0])
        == WL_COLUMNAR_EVAL_STACK_PREPARED_SEALED ? 1 : -1];
typedef char wl_columnar_eval_stack_prepared_scratch_count_fits_t[
        WL_COLUMNAR_JOIN_SCRATCH_COUNT == 3 ? 1 : -1];

static int
wl_columnar_eval_stack_prepared_step_body(
    wl_columnar_eval_stack_unary_prepared_t *p)
{
    wl_columnar_eval_stack_cleanup_frame_t *f = p->frame;
    if (p->phase < WL_COLUMNAR_EVAL_STACK_PREPARED_HEAD
        || p->phase >= WL_COLUMNAR_EVAL_STACK_PREPARED_SEALED) return EINVAL;
    uint64_t fee = wl_columnar_eval_stack_prepared_fees[p->phase];
    int rc = wl_evaluation_control_charge_attempt(p->control, p->attempt_id,
            fee);
    if (rc) return rc;
    switch (p->phase) {
    case WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_HEAD:
    case WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SEAL_HEAD:
        rc = wl_columnar_eval_stack_prepared_join_confirm(p);
        if (!rc) {
            p->join_mode = true;
            p->publication_is_delta = f->filter.join_result_is_delta;
            p->phase = p->phase == WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_HEAD
                ? WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_STATE
                : WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SEAL_STATE;
        }
        return rc;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_STATE:
    case WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SEAL_STATE:
        rc = wl_columnar_eval_stack_prepared_join_state(p);
        if (!rc) p->phase = p->phase ==
                WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_STATE
            ? WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SCRATCH0
            : WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SEAL_SCRATCH0;
        return rc;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SCRATCH0:
    case WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SCRATCH1:
    case WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SCRATCH2:
        rc = wl_columnar_eval_stack_prepared_join_scratch(p,
                p->phase - WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SCRATCH0);
        if (!rc) p->phase = p->phase ==
                WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SCRATCH2
            ? WL_COLUMNAR_EVAL_STACK_PREPARED_HEAD : p->phase + 1;
        return rc;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SEAL_SCRATCH0:
    case WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SEAL_SCRATCH1:
    case WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SEAL_SCRATCH2:
        rc = wl_columnar_eval_stack_prepared_join_scratch(p,
                p->phase - WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SEAL_SCRATCH0);
        if (!rc) p->phase = p->phase ==
                WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SEAL_SCRATCH2
            ? WL_COLUMNAR_EVAL_STACK_PREPARED_SEAL_OLD : p->phase + 1;
        return rc;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_HEAD:
        if (!wl_columnar_eval_stack_unary_original_matches(f) ||
            !f->unary_output.rel
            || !f->unary_output.owned || f->unary_output.is_delta ||
            f->unary_output.seg_boundaries
            || f->unary_output.seg_count ||
            f->unary_output.kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
            || f->unary_output.continuation) return EINVAL;
        wl_columnar_eval_stack_unary_entry_copy(&p->work.original,
            &f->unary_snapshot);
        wl_columnar_eval_stack_unary_entry_copy(&p->work.output,
            &f->unary_output);
        p->needs_sanitize = true;
        p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_ALIAS_FIXED; return 0;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_ALIAS_FIXED:
        if (p->work.output.rel == p->work.original.rel ||
            p->work.output.rel == f->result.rel
            || p->work.output.rel == f->unary_retired.rel) {
            wl_columnar_eval_stack_unary_alias_detach(f); return EINVAL;
        }
        p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_ALIAS_ROWS; return 0;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_ALIAS_ROWS:
        if (p->alias_cursor >= p->top) return EINVAL;
        if (p->work.output.rel == f->stack.items[p->alias_cursor].rel) {
            wl_columnar_eval_stack_unary_alias_detach(f); return EINVAL;
        }
        if (++p->alias_cursor == p->top) {
            p->alias_complete = true; p->needs_sanitize = false;
            p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_ACQUIRE_OLD;
        }
        return 0;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_ACQUIRE_OLD: {
        if (p->scalar_ready) {
            p->work.old_snapshot =
                wl_columnar_relation_snapshot(p->work.original.rel);
            p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_ACQUIRE_OUTPUT; return 0;
        }
        bool done = false;
        rc = col_rel_source_reader_acquire_step(p->work.original.rel,
                &p->work.old_reader, &done);
        if (!rc && done) {
            p->old_held = true;
            p->work.old_snapshot =
                wl_columnar_relation_snapshot(p->work.original.rel);
            p->work.old_canonical = p->work.original.rel->storage_owner;
            p->work.old_identity = p->work.old_canonical->relation_identity;
            p->work.old_generation = p->work.old_canonical->storage_generation;
            p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_ACQUIRE_OUTPUT;
        }
        return rc;
    }
    case WL_COLUMNAR_EVAL_STACK_PREPARED_ACQUIRE_OUTPUT: {
        bool done = false;
        rc = col_rel_source_reader_acquire_step(p->work.output.rel,
                &p->work.output_reader, &done);
        if (!rc && done) {
            p->output_held = true;
            p->work.output_snapshot =
                wl_columnar_relation_snapshot(p->work.output.rel);
            p->work.output_canonical = p->work.output.rel->storage_owner;
            p->work.output_identity =
                p->work.output_canonical->relation_identity;
            p->work.output_generation =
                p->work.output_canonical->storage_generation;
            p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_VALIDATE_OLD;
        }
        return rc;
    }
    case WL_COLUMNAR_EVAL_STACK_PREPARED_VALIDATE_OLD:
    case WL_COLUMNAR_EVAL_STACK_PREPARED_SEAL_OLD:
        rc = wl_columnar_eval_stack_prepared_old_validate(p);
        if (!rc) p->phase = p->phase ==
                WL_COLUMNAR_EVAL_STACK_PREPARED_VALIDATE_OLD
            ? WL_COLUMNAR_EVAL_STACK_PREPARED_VALIDATE_OUTPUT :
                WL_COLUMNAR_EVAL_STACK_PREPARED_SEAL_OUTPUT;
        return rc;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_VALIDATE_OUTPUT:
    case WL_COLUMNAR_EVAL_STACK_PREPARED_SEAL_OUTPUT:
        rc = wl_columnar_eval_stack_prepared_output_validate(p);
        if (!rc) p->phase = p->phase ==
                WL_COLUMNAR_EVAL_STACK_PREPARED_VALIDATE_OUTPUT
            ? WL_COLUMNAR_EVAL_STACK_PREPARED_READY :
                WL_COLUMNAR_EVAL_STACK_PREPARED_SEAL_LATCH;
        return rc;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_READY:
        p->phase = p->join_mode ? WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_SEAL_HEAD
            : WL_COLUMNAR_EVAL_STACK_PREPARED_SEAL_OLD; return 0;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_SEAL_LATCH:
        rc = wl_evaluation_control_charge_attempt(p->control, p->attempt_id,
                128);
        if (!rc) {
            p->commit_prepaid = true;
            p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_SEALED;
        }
        return rc;
    default: return EBUSY;
    }
}

int
wl_columnar_eval_stack_unary_prepared_step(
    wl_columnar_eval_stack_unary_prepared_t *p)
{
    int rc = wl_columnar_eval_stack_prepared_validate(p);
    if (rc) return rc;
    if (p->phase < WL_COLUMNAR_EVAL_STACK_PREPARED_HEAD
        || p->phase >= WL_COLUMNAR_EVAL_STACK_PREPARED_SEALED ||
        p->result_saved) return EBUSY;
    p->busy = true; wl_columnar_eval_stack_prepared_checkpoint(p);
    rc = wl_columnar_eval_stack_prepared_step_body(p);
    if (rc && rc != EAGAIN && !p->result_saved) {
        p->result_saved = true; p->first_result = rc;
    }
    p->busy = false; return rc;
}

void
wl_columnar_eval_stack_unary_commit_prepared_nofail(
    wl_columnar_eval_stack_unary_prepared_t *p)
{
    wl_columnar_eval_stack_cleanup_frame_t *f = p->frame;
    eval_entry_t *entry = &f->stack.items[p->index];
    wl_columnar_eval_stack_unary_entry_copy(&f->unary_retired, entry);
    wl_columnar_eval_stack_unary_entry_copy(entry, &f->unary_output);
    entry->is_delta = p->publication_is_delta;
    wl_columnar_eval_stack_unary_entry_clear(&f->unary_output);
    f->unary_committed = true;
    f->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_COMMITTED;
    p->committed = true; p->result_saved = true; p->first_result = 0;
    p->commit_prepaid = false;
    p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_COMMITTED_DRAIN;
    if (p->join_mode) {
        wl_columnar_filter_context_t *ctx = &f->filter;
        ctx->committed = true; ctx->execution_result = 0;
        ctx->semantic_result = 0;
        ctx->join_result_is_delta = false;
        ctx->join_disposition = WL_COLUMNAR_JOIN_DISPOSITION_REPLACE;
        ctx->join_operation = NULL;
        ctx->join_input_snapshot = (col_relation_snapshot_t){0};
        ctx->join_decision_pending = false;
        ctx->stage = WL_COLUMNAR_FILTER_STAGE_CONTROL;
    }
}

int
wl_columnar_eval_stack_unary_prepared_cancel(
    wl_columnar_eval_stack_unary_prepared_t *p,
    int result, wl_evaluation_control_t *funding, uint64_t id)
{
    int rc = wl_columnar_eval_stack_prepared_validate(p);
    if (rc) return rc;
    if (p->phase >=
        WL_COLUMNAR_EVAL_STACK_PREPARED_CANCEL_SANITIZE) return EBUSY;
    if (!p->committed && !result && (p->result_saved || !p->alias_complete
        || (p->phase != WL_COLUMNAR_EVAL_STACK_PREPARED_READY
        && p->phase != WL_COLUMNAR_EVAL_STACK_PREPARED_SEALED))) return EINVAL;
    rc = wl_columnar_eval_stack_prepared_funding_check(p, funding, id);
    if (rc) return rc;
    bool confirm = p->phase == WL_COLUMNAR_EVAL_STACK_PREPARED_JOIN_HEAD
        && !p->join_mode;
    p->busy = true; wl_columnar_eval_stack_prepared_checkpoint(p);
    rc = wl_evaluation_control_charge_attempt(funding, id, confirm ? 192 : 128);
    if (!rc) {
        if (!p->result_saved) {
            p->result_saved = true; p->first_result = result;
        }
        if (confirm && !wl_columnar_eval_stack_prepared_join_confirm(p)) {
            p->join_mode = true;
            p->publication_is_delta = p->frame->filter.join_result_is_delta;
        }
        wl_columnar_eval_stack_prepared_join_error(p);
        p->disposition = p->committed ? WL_COLUMNAR_EVAL_STACK_PREPARED_NONE
            : p->first_result ? WL_COLUMNAR_EVAL_STACK_PREPARED_ABORT :
            WL_COLUMNAR_EVAL_STACK_PREPARED_RESUME;
        p->commit_prepaid = false;
        p->needs_sanitize = !p->committed && !p->alias_complete;
        p->phase =
            p->needs_sanitize ? WL_COLUMNAR_EVAL_STACK_PREPARED_CANCEL_SANITIZE
            : WL_COLUMNAR_EVAL_STACK_PREPARED_DRAIN_OUTPUT;
    }
    p->busy = false; return rc;
}

int
wl_columnar_eval_stack_unary_prepared_drain(
    wl_columnar_eval_stack_unary_prepared_t *p,
    wl_evaluation_control_t *funding, uint64_t id)
{
    int rc = wl_columnar_eval_stack_prepared_validate(p);
    if (rc) return rc;
    if (p->phase == WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY) return EBUSY;
    if (p->phase < WL_COLUMNAR_EVAL_STACK_PREPARED_COMMITTED_DRAIN
        || p->phase > WL_COLUMNAR_EVAL_STACK_PREPARED_CLEAR_WORK) return EBUSY;
    rc = wl_columnar_eval_stack_prepared_funding_check(p, funding, id);
    if (rc) return rc;
    p->busy = true; wl_columnar_eval_stack_prepared_checkpoint(p);
    rc = wl_evaluation_control_charge_attempt(funding, id,
            p->phase ==
            WL_COLUMNAR_EVAL_STACK_PREPARED_CANCEL_SANITIZE ? 256 : 128);
    if (!rc) switch (p->phase) {
        case WL_COLUMNAR_EVAL_STACK_PREPARED_COMMITTED_DRAIN:
            p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_DRAIN_OUTPUT; break;
        case WL_COLUMNAR_EVAL_STACK_PREPARED_CANCEL_SANITIZE:
            (void)wl_columnar_eval_stack_unary_output_sanitize(p->frame);
            p->needs_sanitize = false;
            p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_DRAIN_OUTPUT; break;
        case WL_COLUMNAR_EVAL_STACK_PREPARED_DRAIN_OUTPUT:
        case WL_COLUMNAR_EVAL_STACK_PREPARED_DRAIN_OLD: {
            bool output = p->phase ==
                WL_COLUMNAR_EVAL_STACK_PREPARED_DRAIN_OUTPUT;
            wl_columnar_source_access_reader_t *reader =
                output ? &p->work.output_reader : &p->work.old_reader;
            bool done = wl_columnar_eval_stack_prepared_reader_inert(reader);
            if (!done) rc =
                    wl_columnar_source_access_reader_release_step(reader,
                        &done);
            if (!rc && done) {
                if (output) {
                    p->output_held = false;
                    p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_DRAIN_OLD;
                }else {
                    p->old_held = false;
                    p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_CLEAR_WORK;
                }
            }
            break;
        }
        case WL_COLUMNAR_EVAL_STACK_PREPARED_CLEAR_WORK:
            memset(&p->work, 0, sizeof(p->work));
            p->alias_cursor = 0; p->alias_complete = false;
            p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY; break;
        default: rc = EINVAL; break;
        }
    if (rc && rc != EAGAIN) p->cleanup_result = rc;
    else if (!rc) p->cleanup_result = 0;
    p->busy = false; return rc;
}

int
wl_columnar_eval_stack_unary_prepared_ack(
    wl_columnar_eval_stack_unary_prepared_t *p,
    wl_evaluation_control_t *funding, uint64_t id)
{
    int rc = wl_columnar_eval_stack_prepared_validate(p);
    if (rc) return rc;
    if (p->phase != WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY
        || !wl_columnar_eval_stack_prepared_work_inert(p) || p->old_held ||
        p->output_held) return EBUSY;
    rc = wl_columnar_eval_stack_prepared_funding_check(p, funding, id);
    if (rc) return rc;
    p->busy = true; wl_columnar_eval_stack_prepared_checkpoint(p);
    rc = wl_evaluation_control_charge_attempt(funding, id, 192);
    if (!rc) {
        wl_evaluation_control_t *control = p->control;
        p->frame->prepared_unary = NULL;
        p->frame = NULL; p->session = NULL; p->numeric_owner = NULL;
        p->plan = NULL; p->operation = NULL; p->control = NULL;
        p->control_identity = p->attempt_id = 0; p->control_ref = false;
        p->numeric_cursor = p->top = p->index = p->alias_cursor = 0;
        p->numeric_phase = 0;
        p->scalar_ready = p->needs_sanitize = p->alias_complete = false;
        p->commit_prepaid = false;
        p->disposition = WL_COLUMNAR_EVAL_STACK_PREPARED_NONE;
        p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED;
        wl_evaluation_control_release(control);
    }
    p->busy = false; return rc;
}

int
wl_columnar_eval_stack_unary_prepared_query(
    const wl_columnar_eval_stack_unary_prepared_t *p,
    wl_columnar_eval_stack_unary_prepared_status_t *out)
{
    if (!p || !out || (uintptr_t)out > UINTPTR_MAX - sizeof(*out)
        || !wl_columnar_eval_stack_copy_ranges_disjoint(p, sizeof(*p), out,
        sizeof(*out))) return EINVAL;
    if (p->identity != p ||
        !wl_columnar_eval_stack_prepared_thread(p)) return EINVAL;
    if (p->busy) return EBUSY;
    if (p->phase == WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED) {
        if (!wl_columnar_eval_stack_prepared_terminal_inert(p)) return EINVAL;
    } else {
        int rc = wl_columnar_eval_stack_prepared_validate(p);
        if (rc) return rc;
        if (!wl_columnar_eval_stack_frame_copy_output(p->frame, out,
            sizeof(*out))
            || !wl_columnar_eval_stack_copy_ranges_disjoint(p->numeric_owner,
            sizeof(*p->numeric_owner), out, sizeof(*out))) return EINVAL;
    }
    out->phase = p->phase; out->disposition = p->disposition;
    out->alias_cursor = p->alias_cursor;
    out->first_result = p->first_result;
    out->cleanup_result = p->cleanup_result;
    out->committed = p->committed; out->result_saved = p->result_saved;
    out->alias_complete = p->alias_complete; out->old_held = p->old_held;
    out->output_held = p->output_held;
    return 0;
}

/* The old publication receipt is ACKED before this independent cleanup lease.
 * Original control ownership is retained, but only distinct funding is polled. */
static int
wl_columnar_eval_stack_prepared_join_finish_begin_core(
    wl_columnar_eval_stack_unary_prepared_t *p,
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    const wl_columnar_eval_plan_numeric_attempt_t *owner,
    wl_evaluation_control_t *funding, uint64_t id)
{
    int rc;
    if (p->identity != p || !wl_columnar_eval_stack_prepared_thread(p))
        return EINVAL;
    if (p->busy || p->phase != WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED
        || !wl_columnar_eval_stack_prepared_terminal_inert(p)) return EBUSY;
    const wl_columnar_filter_context_t *ctx = &frame->filter;
    if (frame->prepared_unary || frame->stream_host
        || ctx->identity != ctx || ctx->frame != frame ||
        ctx->session != frame->session
        || ctx->stage != WL_COLUMNAR_FILTER_STAGE_FINISH || ctx->busy
        || !ctx->thread_valid || !ctx->unary_started || ctx->control ||
        ctx->pin_owned
        || ctx->output ||
        (frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE
        && frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_COMMITTED)
        || !wl_columnar_eval_stack_unary_thread_valid(frame)
        || frame->unary_control != owner->control
        || frame->unary_attempt_id != owner->attempt_id || !owner->plan
        || !owner->plan->ops || owner->cursor >= owner->end
        || owner->end > owner->plan->op_count
        || !frame->unary_top || frame->unary_top != frame->stack.top
        || frame->unary_index != frame->unary_top - 1
        || frame->unary_scalar_busy || frame->unary_scalar_init_target
        || frame->unary_scalar_cancel_clear || frame->unary_scalar_drained)
        return EBUSY;
    if (!funding || !id || funding == owner->control
        || wl_evaluation_control_identity(funding) ==
        wl_evaluation_control_identity(owner->control)
        || wl_evaluation_control_active_attempt_id(funding) !=
        id) return EINVAL;
    rc = wl_evaluation_control_charge_attempt(funding, id, 256);
    if (rc) return rc;
    rc = wl_evaluation_control_retain(owner->control);
    if (rc) return rc;
    /* Work is genuinely inert; preserve native final-address provenance.
     * No whole receipt clear and no copied live resource is introduced. */
    p->frame = frame; p->session = frame->session; p->numeric_owner = owner;
    p->plan = owner->plan; p->operation = &owner->plan->ops[owner->cursor];
    p->numeric_cursor = owner->cursor; p->numeric_phase = owner->phase;
    p->control = owner->control;
    p->control_identity = wl_evaluation_control_identity(owner->control);
    p->attempt_id = owner->attempt_id; p->control_ref = true;
    p->top = frame->unary_top; p->index = frame->unary_index;
    p->first_result = p->cleanup_result = 0;
    p->committed = p->result_saved = p->join_mode = p->publication_is_delta =
        false;
    p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_HEAD;
    frame->prepared_unary = p;
    return 0;
}

int
wl_columnar_eval_stack_unary_prepared_join_finish_begin(
    wl_columnar_eval_stack_unary_prepared_t *p,
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    const wl_columnar_eval_plan_numeric_attempt_t *owner,
    wl_evaluation_control_t *funding, uint64_t id)
{
    if (!p || !frame || !owner) return EINVAL;
    int rc = wl_columnar_eval_stack_numeric_owner(frame, owner);
    if (rc) return rc;
    if (!wl_columnar_eval_stack_frame_copy_output(frame, p, sizeof(*p))
        || !wl_columnar_eval_stack_copy_ranges_disjoint(owner, sizeof(*owner),
        p, sizeof(*p))) return EINVAL;
    return wl_columnar_eval_stack_prepared_join_finish_begin_core(p, frame,
               owner, funding, id);
}

static bool
wl_columnar_eval_stack_prepared_operator_finish_kind(wl_plan_op_type_t op)
{
    switch (op) {
    case WL_PLAN_OP_FILTER: case WL_PLAN_OP_MAP: case WL_PLAN_OP_JOIN:
    case WL_PLAN_OP_SEMIJOIN: case WL_PLAN_OP_ANTIJOIN: case WL_PLAN_OP_REDUCE:
    case WL_PLAN_OP_CONSOLIDATE: return true;
    default: return false;
    }
}

static bool
wl_columnar_eval_stack_prepared_operator_finish_child(
    const wl_columnar_eval_plan_numeric_attempt_t *n)
{
    return n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD
           || n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD
           || (n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING
           && n->pending_action == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD)
           || (n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING
           && n->pending_action ==
           WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD);
}

/* Read-only first packet. The caller already proved the actual numeric owner
 * and excluded its embedded framed child before touching a financier. */
static int
wl_columnar_eval_stack_prepared_operator_finish_context(
    const wl_columnar_eval_stack_cleanup_frame_t *f,
    const wl_columnar_eval_plan_numeric_attempt_t *n)
{
    const wl_columnar_filter_context_t *ctx = &f->filter;
    if (f->stream_host || f->row_debt || ctx->identity != ctx || ctx->frame != f
        || ctx->session != f->session || ctx->session != n->session
        || f->session->cleanup_active != f || ctx->busy || !ctx->thread_valid
        || !wl_columnar_eval_stack_prepared_join_thread(ctx)
        || ctx->stage != WL_COLUMNAR_FILTER_STAGE_FINISH || !ctx->unary_started
        || ctx->control || ctx->pin_owned || ctx->output
        || ctx->attempt_id != n->attempt_id || f->unary_control != n->control
        || f->unary_attempt_id != n->attempt_id) return EBUSY;
    return 0;
}

/* Separate fixed packet; no width/schema scan and no old control access. */
static int
wl_columnar_eval_stack_prepared_operator_finish_state(
    const wl_columnar_eval_stack_cleanup_frame_t *f,
    const wl_columnar_eval_plan_numeric_attempt_t *n)
{
    if (!wl_columnar_eval_stack_prepared_operator_finish_child(n)
        || !n->plan || !n->plan->ops || n->cursor >= n->end
        || n->end > n->plan->op_count
        || !wl_columnar_eval_stack_prepared_operator_finish_kind(
            n->plan->ops[n->cursor].op)
        || (f->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE
        && f->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_COMMITTED)
        || !wl_columnar_eval_stack_unary_thread_valid(f)
        || !f->unary_top || f->unary_top != f->stack.top
        || f->unary_index != f->unary_top - 1
        || f->unary_scalar_mode || f->unary_scalar_started
        || f->unary_scalar_prepared || f->unary_scalar_committed
        || f->unary_scalar_for_old || f->unary_scalar_busy
        || f->unary_scalar_init_target || f->unary_scalar_cancel_clear
        || f->unary_scalar_drained || f->unary_scalar_initializer.identity
        || f->unary_scalar_retirement.storage.identity) return EBUSY;
    return 0;
}

int
wl_columnar_eval_stack_unary_prepared_operator_finish_begin(
    wl_columnar_eval_stack_unary_prepared_t *p,
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    const wl_columnar_eval_plan_numeric_attempt_t *owner,
    wl_evaluation_control_t *funding, uint64_t id)
{
    if (!p || !frame || !owner) return EINVAL;
    int rc = wl_columnar_eval_stack_numeric_owner(frame, owner);
    if (rc) return rc;
    /* Immutable actual owner address, never a cached registration flag. */
    if (owner == &frame->framed.child) return EBUSY;
    rc = wl_columnar_eval_stack_prepared_operator_finish_context(frame, owner);
    if (rc) return rc;
    if (!funding || !id || funding == frame->unary_control) return EINVAL;
    rc = wl_evaluation_control_charge_attempt(funding, id, 0);
    if (rc) return rc;
    if (frame->prepared_unary) return EBUSY;
    const wl_plan_relation_t *plan = owner->plan;
    uint32_t cursor = owner->cursor;
    wl_columnar_eval_plan_numeric_phase_t phase = owner->phase;
    if (!wl_columnar_eval_stack_frame_copy_output(frame, p, sizeof(*p))
        || !wl_columnar_eval_stack_copy_ranges_disjoint(owner, sizeof(*owner),
        p, sizeof(*p))) return EINVAL;
    rc = wl_evaluation_control_charge_attempt(funding, id, 0);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_prepared_operator_finish_state(frame, owner);
    if (rc) return rc;
    rc = wl_evaluation_control_charge_attempt(funding, id, 0);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_prepared_context_state(&frame->filter);
    if (rc) return rc;
    rc = wl_evaluation_control_charge_attempt(funding, id, 0);
    if (rc) return rc;
    for (unsigned i = 0; i < WL_COLUMNAR_JOIN_SCRATCH_COUNT; ++i) {
        rc = wl_columnar_eval_stack_prepared_context_scratch(&frame->filter, i);
        if (rc) return rc;
        rc = wl_evaluation_control_charge_attempt(funding, id, 0);
        if (rc) return rc;
    }
    const col_rel_t *relations[] = {frame->unary_output.rel,
                                    frame->unary_retired.rel,
                                    frame->stack.items[frame->unary_index].rel};
    for (unsigned i = 0; i < 3; ++i) {
        if (relations[i] && !wl_columnar_eval_stack_copy_ranges_disjoint(
                relations[i], sizeof(*relations[i]), p,
                sizeof(*p))) return EINVAL;
        rc = wl_evaluation_control_charge_attempt(funding, id, 0);
        if (rc) return rc;
    }
    if (p->busy || (p->identity && (p->identity != p
        || p->phase != WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED
        || !wl_columnar_eval_stack_prepared_thread(p)))) return EBUSY;
    if (!p->identity && (p->phase != WL_COLUMNAR_EVAL_STACK_PREPARED_INERT
        || p->thread_valid)) return EINVAL;
    if (!wl_columnar_eval_stack_prepared_terminal_inert(p)) return EBUSY;
    rc = wl_evaluation_control_charge_attempt(funding, id, 0);
    if (rc) return rc;
    /* No callback occurred. Recheck canonical lineage before publishing the
    * lease; financial admission never substitutes for this actual proof. */
    rc = wl_columnar_eval_stack_numeric_owner(frame, owner);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_prepared_operator_finish_context(frame, owner);
    if (rc) return rc;
    if (owner->plan != plan || owner->cursor != cursor
        || owner->phase != phase) return EBUSY;
    rc = wl_evaluation_control_charge_attempt(funding, id, 0);
    if (rc) return rc;
    rc = wl_evaluation_control_charge_attempt(funding, id, 256);
    if (rc) return rc;
    rc = wl_evaluation_control_retain(owner->control);
    if (rc) return rc;
    p->frame = frame; p->session = frame->session; p->numeric_owner = owner;
    p->plan = owner->plan; p->operation = &owner->plan->ops[owner->cursor];
    p->numeric_cursor = owner->cursor; p->numeric_phase = owner->phase;
    p->control = owner->control;
    p->control_identity = wl_evaluation_control_identity(owner->control);
    p->attempt_id = owner->attempt_id; p->control_ref = true;
    p->top = frame->unary_top; p->index = frame->unary_index;
    p->first_result = p->cleanup_result = 0;
    p->committed = p->result_saved = p->join_mode = p->publication_is_delta =
        false;
    p->scalar_ready = p->old_held = p->output_held = false;
    p->alias_complete = p->needs_sanitize = p->commit_prepaid = false;
    p->disposition = WL_COLUMNAR_EVAL_STACK_PREPARED_NONE;
#if defined(WL_HAVE_C11_THREADS)
    p->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    p->owner_thread = GetCurrentThreadId();
#else
    p->owner_thread = pthread_self();
#endif
    p->thread_valid = true;
    p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_HEAD;
    p->identity = p; frame->prepared_unary = p;
    return 0;
}

static int
wl_columnar_eval_stack_prepared_finish_head(
    wl_columnar_eval_stack_unary_prepared_t *p)
{
    wl_columnar_eval_stack_cleanup_frame_t *f = p->frame;
    const wl_columnar_eval_plan_numeric_attempt_t *n = p->numeric_owner;
    wl_columnar_filter_context_t *ctx = &f->filter;
    bool child = n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD
        || n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD
        || ((n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_PENDING
        || n->phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING)
        && (n->pending_action == WL_COLUMNAR_EVAL_PLAN_NUMERIC_CHILD
        || n->pending_action ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD));
    if (!child || ctx->identity != ctx || ctx->frame != f ||
        ctx->session != p->session
        || ctx->stage != WL_COLUMNAR_FILTER_STAGE_FINISH || ctx->busy
        || !ctx->thread_valid ||
        !wl_columnar_eval_stack_prepared_join_thread(ctx)
        || !ctx->unary_started || ctx->control || ctx->pin_owned || ctx->output
        || ctx->attempt_id != p->attempt_id ||
        ctx->committed != f->unary_committed
        || ctx->semantic_result != ctx->execution_result ||
        ctx->join_metadata_busy
        || ctx->join_operation || ctx->join_decision_pending ||
        ctx->join_result_is_delta
        || ctx->join_disposition != WL_COLUMNAR_JOIN_DISPOSITION_REPLACE
        || ctx->join_input_snapshot.relation_identity
        || ctx->join_input_snapshot.view_generation ||
        ctx->join_input_snapshot.storage_generation
        || p->operation->op != WL_PLAN_OP_JOIN
        || ctx->private_output != (f->unary_committed
            ? f->stack.items[p->index].rel : f->unary_output.rel)
        || f->unary_scalar_mode || f->unary_scalar_started ||
        f->unary_scalar_prepared
        || f->unary_scalar_committed || f->unary_scalar_for_old
        || f->unary_scalar_initializer.identity ||
        f->unary_scalar_retirement.storage.identity)
        return EINVAL;
    eval_entry_t *entry =
        f->unary_committed ? &f->unary_retired : &f->unary_output;
    if (entry->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION || entry->continuation)
        return ENOTSUP;
    wl_columnar_eval_stack_unary_entry_copy(&p->work.original, entry);
    p->work.output.rel = ctx->private_output;
    if (entry->rel) p->work.old_snapshot =
            wl_columnar_relation_snapshot(entry->rel);
    p->first_result =
        f->unary_result_saved ? f->unary_first_result : ctx->execution_result;
    p->result_saved = true; p->committed = f->unary_committed;
    if (!f->unary_result_saved) {
        f->unary_first_result = p->first_result; f->unary_result_saved = true;
    }
    p->join_mode = true;
    ctx->private_output = NULL;
    f->unary_scalar_mode = true;
    f->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_RETAINED;
    p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_SEGMENTS;
    return 0;
}

static int
wl_columnar_eval_stack_prepared_operator_finish_head(
    wl_columnar_eval_stack_unary_prepared_t *p,
    wl_evaluation_control_t *funding, uint64_t id)
{
    wl_columnar_eval_stack_cleanup_frame_t *f = p->frame;
    const wl_columnar_eval_plan_numeric_attempt_t *n = p->numeric_owner;
    wl_columnar_filter_context_t *ctx = &f->filter;
    int rc = wl_columnar_eval_stack_prepared_operator_finish_context(f, n);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_prepared_operator_finish_state(f, n);
    if (rc) return rc;
    if (p->operation != &n->plan->ops[n->cursor]
        || ctx->committed != f->unary_committed
        || ctx->semantic_result != ctx->execution_result
        || ctx->join_metadata_busy || ctx->join_operation
        || ctx->join_decision_pending || ctx->join_result_is_delta
        || ctx->join_disposition != WL_COLUMNAR_JOIN_DISPOSITION_REPLACE
        || ctx->join_input_snapshot.relation_identity
        || ctx->join_input_snapshot.view_generation
        || ctx->join_input_snapshot.storage_generation) return EINVAL;
    rc = wl_evaluation_control_charge_attempt(funding, id, 0);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_prepared_context_state(ctx);
    if (rc) return rc;
    rc = wl_evaluation_control_charge_attempt(funding, id, 0);
    if (rc) return rc;
    for (unsigned i = 0; i < WL_COLUMNAR_JOIN_SCRATCH_COUNT; ++i) {
        rc = wl_columnar_eval_stack_prepared_context_scratch(ctx, i);
        if (rc) return rc;
        rc = wl_evaluation_control_charge_attempt(funding, id, 0);
        if (rc) return rc;
    }
    eval_entry_t *live = &f->stack.items[p->index];
    eval_entry_t *entry =
        f->unary_committed ? &f->unary_retired : &f->unary_output;
    bool keep_all = f->unary_committed && !ctx->private_output
        && p->operation->op == WL_PLAN_OP_ANTIJOIN;
    if (entry->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION || entry->continuation)
        return ENOTSUP;
    if (keep_all) {
        const eval_entry_t *old = &f->unary_snapshot;
        if (!wl_columnar_eval_stack_unary_entry_inert(&f->unary_output)
            || entry->rel || entry->owned || entry->is_delta
            || !old->rel || live->rel != old->rel
            || live->owned != old->owned || live->kind != old->kind
            || live->continuation != old->continuation
            || live->is_delta || live->seg_boundaries || live->seg_count
            || entry->seg_boundaries != old->seg_boundaries
            || entry->seg_count != old->seg_count) return EINVAL;
    } else if (f->unary_committed) {
        if (!ctx->private_output || ctx->private_output != live->rel
            || !live->owned || live->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
            || live->continuation) return EINVAL;
    } else {
        if (!wl_columnar_eval_stack_prepared_entry_equal(live,
            &f->unary_snapshot)
            || ctx->private_output != entry->rel
            || (entry->rel && !entry->owned)) return EINVAL;
        if (!entry->rel && !wl_columnar_eval_stack_unary_entry_inert(entry))
            return EINVAL;
    }
    rc = wl_evaluation_control_charge_attempt(funding, id, 0);
    if (rc) return rc;
    /* This fixed handoff owns the real selected entry. No physical free or
     * failing operation follows until the caller's actual footer. */
    wl_columnar_eval_stack_unary_entry_copy(&p->work.original, entry);
    p->work.output.rel = ctx->private_output;
    if (entry->rel)
        p->work.old_snapshot = wl_columnar_relation_snapshot(entry->rel);
    p->first_result = f->unary_result_saved ? f->unary_first_result
        : ctx->execution_result;
    p->result_saved = true; p->committed = f->unary_committed;
    if (!f->unary_result_saved) {
        f->unary_first_result = p->first_result; f->unary_result_saved = true;
    }
    /* In the FINISH phase this existing field records paid HEAD completion;
     * it does not authorize an ordinary JOIN publication action. */
    p->join_mode = true;
    ctx->private_output = NULL;
    f->unary_scalar_mode = true;
    f->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_RETAINED;
    p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_SEGMENTS;
    return 0;
}

static uint64_t
wl_columnar_eval_stack_prepared_finish_fee(
    const wl_columnar_eval_stack_unary_prepared_t *p)
{
    switch (p->phase) {
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_HEAD:
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_COMMIT: return 192;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_INIT:
        return p->frame->unary_scalar_initializer.phase ==
               WL_COLUMNAR_RELATION_SCALAR_INIT_DONE
               ? 128 : 192;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_PREPARE:
        switch (p->frame->unary_scalar_retirement.phase) {
        case 0: return 256;
        case 1: case 2: return 192;
        default: return 128;
        }
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DISPOSE:
        return p->frame->unary_scalar_retirement.phase == 12 ? 128 : 256;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_CLEAR_SCALAR:
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_CLEAR: return 256;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_CLAIMS:
        /* The existing controlled claim-release supplies the real256. */
        return 0;
    default: return 128;
    }
}

static int
wl_columnar_eval_stack_prepared_finish_body(
    wl_columnar_eval_stack_unary_prepared_t *p,
    wl_evaluation_control_t *funding, uint64_t id)
{
    wl_columnar_eval_stack_cleanup_frame_t *f = p->frame;
    wl_columnar_relation_scalar_retirement_t *s = &f->unary_scalar_retirement;
    switch (p->phase) {
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_HEAD:
        return wl_columnar_eval_stack_prepared_finish_head(p);
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_SEGMENTS: {
        eval_entry_t *entry =
            f->unary_committed ? &f->unary_retired : &f->unary_output;
        uint32_t *segments = entry->seg_boundaries;
        if (!f->unary_committed && segments
            && segments ==
            f->stack.items[p->index].seg_boundaries) return EINVAL;
        /* Dispose an irreversible legal array prefix. No stale shadow may
         * resurrect it during decline; a different live ABORT array survives. */
        entry->seg_boundaries = NULL; entry->seg_count = 0;
        p->work.original.seg_boundaries = NULL; p->work.original.seg_count = 0;
        if (segments && f->unary_snapshot.seg_boundaries == segments) {
            f->unary_snapshot.seg_boundaries = NULL;
            f->unary_snapshot.seg_count = 0;
        }
        free(segments);
        p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_SELECT;
        return 0;
    }
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_SELECT: {
        eval_entry_t *entry =
            f->unary_committed ? &f->unary_retired : &f->unary_output;
        if (!entry->owned || !entry->rel) {
            wl_columnar_eval_stack_unary_entry_clear(entry);
            wl_columnar_eval_stack_unary_entry_clear(&p->work.original);
            wl_columnar_eval_stack_unary_entry_clear(&f->unary_snapshot);
            p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_CLEAR_SCALAR;
        } else p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_INIT;
        return 0;
    }
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_INIT: {
        int rc = wl_columnar_eval_stack_unary_scalar_initialize(f,
                p->work.original.rel, false, funding, id);
        if (!rc && f->unary_scalar_started && !f->unary_scalar_init_target)
            p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_PREPARE;
        return rc;
    }
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_PREPARE: {
        int rc = wl_columnar_relation_scalar_retirement_prepare_finite(s,
                funding, id);
        if (!rc &&
            s->ready) p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_COMMIT;
        return rc;
    }
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_COMMIT: {
        eval_entry_t *entry =
            f->unary_committed ? &f->unary_retired : &f->unary_output;
        if (!wl_columnar_eval_stack_prepared_entry_equal(entry,
            &p->work.original))
            return EINVAL;
        int rc = wl_columnar_relation_scalar_retirement_ready_validate(s,
                entry->rel, p->work.old_snapshot);
        if (rc) return rc;
        rc = wl_columnar_relation_scalar_retirement_commit(s);
        if (rc) return rc;
        /* Real destruction authority has moved, but no physical free occurs
         * until a later funded action. Keep only immutable control binding. */
        wl_columnar_eval_stack_unary_entry_clear(entry);
        wl_columnar_eval_stack_unary_entry_clear(&p->work.original);
        wl_columnar_eval_stack_unary_entry_clear(&p->work.output);
        wl_columnar_eval_stack_unary_entry_clear(&f->unary_snapshot);
        f->unary_scalar_committed = true;
        p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DISPOSE;
        return 0;
    }
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DISPOSE: {
        int rc = s->phase == 12
            ? wl_columnar_relation_scalar_retirement_ledger_step_finite(s,
                funding, id)
            : wl_columnar_relation_scalar_retirement_step(s, funding, id);
        if (!rc &&
            s->storage.phase == WL_COLUMNAR_RELATION_MANAGED_RETIREMENT_DONE)
            p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_CLEAR_SCALAR;
        return rc;
    }
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_CLEAR_SCALAR:
        if (f->unary_scalar_init_target || f->unary_scalar_initializer.identity
            || (f->unary_scalar_started
            && s->storage.phase !=
            WL_COLUMNAR_RELATION_MANAGED_RETIREMENT_DONE))
            return EBUSY;
        memset(s, 0, sizeof(*s));
        f->unary_scalar_mode = f->unary_scalar_started =
            f->unary_scalar_prepared = false;
        f->unary_scalar_committed = f->unary_scalar_for_old = false;
        f->unary_scalar_cancel_clear = f->unary_scalar_drained = false;
        f->unary_cleanup_result = 0;
        f->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_DONE;
        f->filter.semantic_result = p->first_result;
        f->filter.cleanup_result = 0;
        f->filter.stage = WL_COLUMNAR_FILTER_STAGE_RESET;
        p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_CLEAR_WORK;
        return 0;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_INIT: {
        int rc = f->unary_scalar_initializer.identity
            ? wl_columnar_relation_scalar_retirement_init_reset(
            &f->unary_scalar_initializer, f, funding, id) : 0;
        if (rc) return rc;
        f->unary_scalar_init_target = NULL;
        p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_CLAIMS;
        return 0;
    }
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_CLAIMS: {
        int rc = s->storage.identity
            ? wl_columnar_relation_scalar_retirement_release_claims_controlled(
            s, funding, id)
            : wl_evaluation_control_charge_attempt(funding, id, 256);
        if (rc) return rc;
        p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_CLEAR;
        return 0;
    }
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_CLEAR:
        memset(s, 0, sizeof(*s));
        f->unary_scalar_started = f->unary_scalar_prepared =
            f->unary_scalar_committed = false;
        f->unary_scalar_for_old = f->unary_scalar_cancel_clear =
            f->unary_scalar_drained = false;
        p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_RESTORE;
        return 0;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_RESTORE:
        f->filter.private_output = p->work.output.rel;
        f->unary_scalar_mode = false;
        f->unary_phase =
            f->unary_committed ? WL_COLUMNAR_EVAL_STACK_UNARY_COMMITTED
            : WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE;
        p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_CLEAR_WORK;
        return 0;
    case WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_CLEAR_WORK:
        memset(&p->work, 0, sizeof(p->work));
        p->phase = WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY;
        return 0;
    default: return EINVAL;
    }
}

int
wl_columnar_eval_stack_unary_prepared_join_finish_step(
    wl_columnar_eval_stack_unary_prepared_t *p,
    wl_evaluation_control_t *funding, uint64_t id)
{
    int rc = wl_columnar_eval_stack_prepared_validate(p);
    if (rc) return rc;
    if (p->phase < WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_HEAD
        || p->phase >
        WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_RESTORE) return EBUSY;
    rc = wl_columnar_eval_stack_prepared_funding_check(p, funding, id);
    if (rc) return rc;
    p->busy = true; wl_columnar_eval_stack_prepared_checkpoint(p);
    rc = wl_evaluation_control_charge_attempt(funding, id,
            wl_columnar_eval_stack_prepared_finish_fee(p));
    if (!rc) rc = wl_columnar_eval_stack_prepared_finish_body(p, funding, id);
    if (rc && rc != EAGAIN
        && (p->disposition != WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINED
        || !p->cleanup_result)) p->cleanup_result = rc;
    else if (!rc &&
        p->disposition != WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINED)
        p->cleanup_result = 0;
    p->busy = false; return rc;
}

int
wl_columnar_eval_stack_unary_prepared_join_finish_decline(
    wl_columnar_eval_stack_unary_prepared_t *p,
    wl_evaluation_control_t *funding, uint64_t id)
{
    int rc = wl_columnar_eval_stack_prepared_validate(p);
    if (rc) return rc;
    if (p->phase < WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_HEAD
        || p->phase > WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_COMMIT
        || p->frame->unary_scalar_committed) return EBUSY;
    rc = wl_columnar_eval_stack_prepared_funding_check(p, funding, id);
    if (rc) return rc;
    p->busy = true; wl_columnar_eval_stack_prepared_checkpoint(p);
    rc = wl_evaluation_control_charge_attempt(funding, id, 128);
    if (!rc) {
        p->disposition = WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINED;
        p->phase =
            !p->join_mode ? WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_CLEAR_WORK
            : WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_INIT;
    }
    p->busy = false; return rc;
}

int
wl_columnar_eval_stack_unary_prepared_operator_finish_step(
    wl_columnar_eval_stack_unary_prepared_t *p,
    wl_evaluation_control_t *funding, uint64_t id)
{
    int rc = wl_columnar_eval_stack_prepared_validate(p);
    if (rc) return rc;
    if (p->numeric_owner == &p->frame->framed.child) return EBUSY;
    if (p->phase < WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_HEAD
        || p->phase > WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_DECLINE_RESTORE)
        return EBUSY;
    rc = wl_columnar_eval_stack_prepared_funding_check(p, funding, id);
    if (rc) return rc;
    rc = wl_evaluation_control_charge_attempt(funding, id, 0);
    if (rc) return rc;
    if (p->phase != WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_HEAD)
        return wl_columnar_eval_stack_unary_prepared_join_finish_step(p,
                   funding, id);
    /* Observer has no permission. Its reentry sees busy; after it returns
     * repeat canonical proof before any HEAD admission or ownership handoff. */
    p->busy = true;
    wl_columnar_eval_stack_prepared_checkpoint(p);
    p->busy = false;
    rc = wl_columnar_eval_stack_prepared_validate(p);
    if (rc) return rc;
    if (p->numeric_owner == &p->frame->framed.child) return EBUSY;
    rc = wl_columnar_eval_stack_prepared_funding_check(p, funding, id);
    if (rc) return rc;
    rc = wl_evaluation_control_charge_attempt(funding, id, 0);
    if (rc) return rc;
    rc = wl_evaluation_control_charge_attempt(funding, id, 192);
    if (rc) return rc;
    p->busy = true;
    rc = wl_columnar_eval_stack_prepared_operator_finish_head(p, funding, id);
    if (rc) p->cleanup_result = rc;
    else p->cleanup_result = 0;
    p->busy = false;
    int paid = wl_evaluation_control_charge_attempt(funding, id, 0);
    return rc ? rc : paid;
}

int
wl_columnar_eval_stack_unary_prepared_operator_finish_decline(
    wl_columnar_eval_stack_unary_prepared_t *p,
    wl_evaluation_control_t *funding, uint64_t id)
{
    int rc = wl_columnar_eval_stack_prepared_validate(p);
    if (rc) return rc;
    if (p->numeric_owner == &p->frame->framed.child) return EBUSY;
    rc = wl_columnar_eval_stack_prepared_funding_check(p, funding, id);
    if (rc) return rc;
    rc = wl_evaluation_control_charge_attempt(funding, id, 0);
    if (rc) return rc;
    return wl_columnar_eval_stack_unary_prepared_join_finish_decline(p,
               funding, id);
}

/* Each support leaf owns its actual fixed work. Original preparation and
 * distinct cleanup primitives retain their separate admissions. */
static int
wl_columnar_eval_stack_auto_fund(wl_evaluation_control_t *funding, uint64_t id,
    const wl_evaluation_control_t *original, uint64_t work)
{
    if (!funding || !id || funding == original
        || wl_evaluation_control_active_attempt_id(funding) != id)
        return EINVAL;
    return wl_evaluation_control_charge_attempt(funding, id, work);
}

static int
wl_columnar_eval_stack_auto_bind(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_columnar_eval_stack_auto_t *a,
    wl_evaluation_control_t *funding, uint64_t id)
{
    int rc = wl_columnar_eval_stack_auto_fund(funding, id, ctx->control, 192);
    if (rc) return rc;
    wl_columnar_eval_stack_cleanup_frame_t *f = ctx->frame;
    const wl_columnar_eval_plan_numeric_attempt_t *n = &ctx->child;
    if (a->identity != a || a->frame != f || &f->framed != ctx
        || f->identity != f || f->session != ctx->session
        || ctx->session->cleanup_active != f
        || !wl_columnar_eval_serial_framed_owner_valid(ctx, f)
        || f->numeric_plan_owner != n
        || !wl_columnar_eval_plan_numeric_owner_valid(n, f)
        || n->session != ctx->session || n->control != ctx->control
        || n->attempt_id != ctx->attempt_id || n->plan != ctx->plan
        || n->private_bindings != ctx->private_bindings
        || !n->plan || !n->plan->ops || n->cursor >= n->end
        || n->end > n->plan->op_count || n->composed_busy)
        return EINVAL;
    if (ctx->capture_out && !wl_columnar_eval_stack_frame_copy_output(f,
        (const void *)ctx->capture_out,
        sizeof(*ctx->capture_out))) return EINVAL;
    if (a->busy) return EBUSY;
    const wl_columnar_eval_private_bindings_t *bindings = n->private_bindings;
    if (a->saved_binding) {
        if (a->saved_binding != bindings
            || a->saved_binding_generation != bindings->generation)
            return EINVAL;
    } else if (bindings) {
        if (bindings->dynamic_entries
            ? !wl_columnar_session_observed_bindings_authorized(ctx->session,
            bindings)
            : bindings->identity != (uintptr_t)bindings
            || bindings->session != ctx->session || bindings->plan != ctx->plan
            || bindings->control != ctx->control
            || bindings->attempt_id != ctx->attempt_id || !bindings->count
            || bindings->count > WL_COLUMNAR_EVAL_PRIVATE_BINDING_MAX
            || wl_evaluation_control_active_attempt_id(ctx->control)
            != ctx->attempt_id)
            return EINVAL;
        a->saved_binding = bindings;
        a->saved_binding_generation = bindings->generation;
    }
    return 0;
}

int
wl_columnar_eval_stack_auto_select(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id, bool *selected)
{
    *selected = false;
    /* Coarse discriminator belongs to the already paid framed CHILD turn. */
    const wl_columnar_eval_plan_numeric_attempt_t *n = &ctx->child;
    if (n->phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD
        || !n->plan || n->cursor >= n->end
        || n->plan->ops[n->cursor].op != WL_PLAN_OP_JOIN) return 0;
    int rc = wl_columnar_eval_stack_auto_fund(funding, id, ctx->control, 128);
    if (rc) return rc;
    wl_columnar_eval_stack_cleanup_frame_t *f = ctx->frame;
    wl_columnar_eval_stack_auto_t *a = wl_columnar_eval_stack_automatic(f);
    rc = wl_columnar_eval_stack_auto_bind(ctx, a, funding, id);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_auto_fund(funding, id, ctx->control, 128);
    if (rc) return rc;
    const wl_columnar_filter_context_t *c = &f->filter;
    const wl_plan_op_t *op = &n->plan->ops[n->cursor];
    if (a->launch || f->prepared_unary || f->stream_host || c->busy)
        return EBUSY;
    bool same = a->operation == op && a->cursor == n->cursor;
    bool finish = same && a->phase == WL_COLUMNAR_EVAL_STACK_AUTO_WAIT_FINISH
        && c->stage == WL_COLUMNAR_FILTER_STAGE_FINISH
        && !c->control && !c->pin_owned && !c->output;
    bool publication = (!same || a->phase == WL_COLUMNAR_EVAL_STACK_AUTO_UNUSED)
        && c->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE
        && c->join_disposition == WL_COLUMNAR_JOIN_DISPOSITION_REPLACE
        && !c->execution_result && !c->semantic_result && !c->cleanup_result
        && f->unary_phase == WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE;
    if (!finish && !publication) return 0;
    rc = wl_columnar_eval_stack_auto_fund(funding, id, ctx->control, 128);
    if (rc) return rc;
    if (!wl_columnar_eval_stack_copy_ranges_disjoint(f, sizeof(*f),
        &a->receipt, sizeof(a->receipt))
        || !wl_columnar_eval_stack_copy_ranges_disjoint(n, sizeof(*n),
        &a->receipt, sizeof(a->receipt))) return EINVAL;
    rc = wl_evaluation_control_retain(funding);
    if (rc) return rc;
    a->launch = funding; a->launch_id = id;
    a->launch_identity = wl_evaluation_control_identity(funding);
    a->operation = op; a->cursor = n->cursor;
    a->abort_requested = finish && a->abort_requested;
    a->finish = finish; a->declined = false;
    a->stop_result = 0;
    a->busy = true;
    ctx->pending_action = WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD;
    ctx->pending_funding = funding; ctx->pending_funding_id = id;
    ctx->pending_inner_consume = false;
    ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING;
    rc = finish ? wl_columnar_eval_stack_prepared_join_finish_begin_core(
        &a->receipt, f, n, funding, id)
        : wl_columnar_eval_stack_prepared_join_begin_core(&a->receipt, f, n);
    a->launch_protocol = rc;
    a->phase = rc ? WL_COLUMNAR_EVAL_STACK_AUTO_WAIT_CONSUME
        : finish ? WL_COLUMNAR_EVAL_STACK_AUTO_FINISH :
        WL_COLUMNAR_EVAL_STACK_AUTO_PUBLICATION;
    a->busy = false;
    *selected = true;
    return rc;
}

int
wl_columnar_eval_stack_auto_maintain(
    wl_columnar_eval_stack_cleanup_frame_t *f, wl_col_session_t *session,
    const wl_columnar_eval_serial_framed_host_request_t *request)
{
    int rc = wl_columnar_eval_stack_auto_fund(request->funding,
            request->funding_id, request->operation, 128);
    if (rc) return rc;
    wl_columnar_eval_stack_auto_t *a = wl_columnar_eval_stack_automatic(f);
    wl_columnar_eval_serial_framed_attempt_t *ctx = &f->framed;
    if (f->session != session || f->prepared_unary != &a->receipt
        || (a->phase != WL_COLUMNAR_EVAL_STACK_AUTO_PUBLICATION
        && a->phase != WL_COLUMNAR_EVAL_STACK_AUTO_FINISH)) return EBUSY;
    rc = wl_columnar_eval_stack_auto_bind(ctx, a, request->funding,
            request->funding_id);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_auto_fund(request->funding,
            request->funding_id, request->operation, 192);
    if (rc) return rc;
    wl_evaluation_control_report_t report;
    if (!request->previous_valid || request->previous_kind !=
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_MORE
        || request->previous_funding != a->launch
        || request->previous_funding_id != a->launch_id
        || request->previous_protocol != a->launch_protocol
        || ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
        || ctx->pending_action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD
        || ctx->pending_funding != a->launch
        || ctx->pending_funding_id != a->launch_id
        || request->funding == a->launch
        || wl_evaluation_control_identity(a->launch) != a->launch_identity
        || wl_evaluation_control_active_attempt_id(a->launch)
        || wl_evaluation_control_report(a->launch, &report)
        || report.attempt_id != a->launch_id || report.cleanup_status
        || (report.execution_status && report.execution_status != EOVERFLOW)
        || ctx->child.cursor != a->cursor
        || &ctx->child.plan->ops[a->cursor] != a->operation) return EINVAL;
    request->reply->kind = WL_COLUMNAR_EVAL_SERIAL_FRAMED_REPLY_PREPARED_MORE;
    rc = wl_columnar_eval_stack_auto_fund(request->funding,
            request->funding_id, request->operation, 128);
    if (rc) return rc;
    /* Pay the entire return suffix before invoking any primitive or hook. */
    rc = wl_columnar_eval_stack_auto_fund(request->funding,
            request->funding_id, request->operation, 128);
    if (rc) return rc;
    a->busy = true;
    if (request->action == WL_COLUMNAR_EVAL_SERIAL_FRAMED_ABORT) {
        a->abort_requested = true;
        if (wl_evaluation_control_active_attempt_id(request->operation)
            == request->operation_id) {
            wl_evaluation_control_request_cancel(request->operation);
            if (!a->finish && !a->receipt.committed && !a->stop_result)
                a->stop_result = wl_evaluation_control_charge_attempt(
                    request->operation, request->operation_id, 0);
        }
    }
    wl_columnar_eval_stack_unary_prepared_t *p = &a->receipt;
    if (p->phase == WL_COLUMNAR_EVAL_STACK_PREPARED_ACK_READY) {
        rc = wl_columnar_eval_stack_unary_prepared_ack(p, request->funding,
                request->funding_id);
        if (!rc) a->phase = WL_COLUMNAR_EVAL_STACK_AUTO_WAIT_CONSUME;
    } else if (a->finish) {
        if (a->declined &&
            p->phase >= WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_HEAD
            && p->phase <= WL_COLUMNAR_EVAL_STACK_PREPARED_FINISH_COMMIT)
            rc = wl_columnar_eval_stack_unary_prepared_join_finish_decline(p,
                    request->funding, request->funding_id);
        else rc = wl_columnar_eval_stack_unary_prepared_join_finish_step(p,
                    request->funding, request->funding_id);
        if (rc == ENOTSUP) a->declined = true;
    } else if (p->phase >= WL_COLUMNAR_EVAL_STACK_PREPARED_COMMITTED_DRAIN) {
        rc = wl_columnar_eval_stack_unary_prepared_drain(p, request->funding,
                request->funding_id);
    } else if ((p->result_saved && p->first_result) || a->stop_result
        || a->abort_requested) {
        rc = wl_columnar_eval_stack_unary_prepared_cancel(p,
                p->result_saved ? p->first_result : a->stop_result
                ? a->stop_result : WL_EVALUATION_CONTROL_CANCELLED,
                request->funding, request->funding_id);
    } else if (p->phase == WL_COLUMNAR_EVAL_STACK_PREPARED_SEALED) {
        rc = wl_evaluation_control_charge_attempt(p->control, p->attempt_id, 0);
        if (!rc) {
            wl_columnar_eval_stack_unary_commit_prepared_nofail(p);
#ifdef WL_SESSION_TEST_HOOKS
            if (wl_columnar_eval_stack_test_physical_publication)
                wl_columnar_eval_stack_test_physical_publication(f->session,
                    f->unary_retired.rel, f->stack.items[p->index].rel);
#endif
        } else {
            a->stop_result = rc;
            rc = wl_columnar_eval_stack_unary_prepared_cancel(p,
                    a->stop_result, request->funding, request->funding_id);
        }
    } else rc = wl_columnar_eval_stack_unary_prepared_step(p);
    a->busy = false;
    return rc;
}

int
wl_columnar_eval_stack_auto_consume(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id, bool abort_intent,
    bool *consumed)
{
    *consumed = false;
    wl_columnar_eval_stack_auto_t *a =
        wl_columnar_eval_stack_automatic(ctx->frame);
    if (!a->launch) return 0;
    int rc = wl_columnar_eval_stack_auto_fund(funding, id, ctx->control, 128);
    if (rc) return rc;
    if (a->identity != a || a->frame != ctx->frame || a->busy
        || a->phase != WL_COLUMNAR_EVAL_STACK_AUTO_WAIT_CONSUME
        || ctx->frame->prepared_unary || ctx->pending_funding != a->launch
        || ctx->pending_funding_id != a->launch_id
        || ctx->pending_action != WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD)
        return EBUSY;
    /* Preparation can fail without a separate ABORT request. Carry that
     * genuine original cause before a reset can make a later poll ambiguous.
     * A refused support-funded FINISH constructor remains retryable. */
    a->abort_requested = a->abort_requested || abort_intent
        || (a->receipt.result_saved && a->receipt.first_result)
        || (!a->finish && a->launch_protocol);
    if (a->abort_requested) {
        wl_columnar_eval_plan_numeric_attempt_t *n = &ctx->child;
        wl_columnar_filter_context_t *c = &ctx->frame->filter;
        int cause = ctx->primary_result ? ctx->primary_result
            : n->semantic_result ? n->semantic_result
            : c->execution_result ? c->execution_result
            : c->semantic_result ? c->semantic_result
            : a->receipt.result_saved && a->receipt.first_result
            ? a->receipt.first_result : a->stop_result ? a->stop_result
            : !a->finish && a->launch_protocol ? a->launch_protocol
            : WL_EVALUATION_CONTROL_CANCELLED;
        if (!ctx->primary_result) ctx->primary_result = cause;
        if (!n->semantic_result) n->semantic_result = cause;
        if (!c->execution_result)
            c->execution_result =
                c->semantic_result ? c->semantic_result : cause;
        if (!c->semantic_result) c->semantic_result = c->execution_result;
        if (abort_intent &&
            wl_evaluation_control_active_attempt_id(ctx->control)
            == ctx->attempt_id)
            wl_evaluation_control_request_cancel(ctx->control);
    }
    wl_evaluation_control_t *launch = a->launch;
    a->launch = NULL; a->launch_id = a->launch_identity = 0;
    a->phase = a->launch_protocol ? (a->finish
        ? WL_COLUMNAR_EVAL_STACK_AUTO_WAIT_FINISH :
        WL_COLUMNAR_EVAL_STACK_AUTO_SKIPPED)
        : a->finish ? WL_COLUMNAR_EVAL_STACK_AUTO_FINISHED :
        WL_COLUMNAR_EVAL_STACK_AUTO_WAIT_FINISH;
    /* The child itself was frozen, so it has no new inner receipt to consume. */
    ctx->inner_protocol = 0;
    ctx->pending_funding = NULL; ctx->pending_funding_id = 0;
    ctx->phase = WL_COLUMNAR_EVAL_SERIAL_FRAMED_CHILD;
    wl_evaluation_control_release(launch);
    *consumed = true;
    return 0;
}

/* Called only after the actual facade authenticated this native frame/context.
 * Semantic error alone is not authority: the registered consumed receipt and
 * exact completed JOIN remain necessary throughout the real error drain. */
bool
wl_columnar_eval_stack_auto_error_drain(
    const wl_columnar_eval_serial_framed_attempt_t *ctx)
{
    const wl_columnar_eval_stack_allocation_t *allocation =
        (const void *)ctx->frame;
    const wl_columnar_eval_stack_auto_t *a = &allocation->automatic;
    if (a->identity != a || a->frame != ctx->frame || !a->abort_requested
        || !ctx->primary_result || !ctx->plan ||
        a->cursor >= ctx->plan->op_count
        || a->operation != &ctx->plan->ops[a->cursor]
        || a->operation->op != WL_PLAN_OP_JOIN || a->launch || a->busy)
        return false;
    if (!((a->receipt.identity == &a->receipt
        && a->receipt.phase == WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED)
        || (a->phase == WL_COLUMNAR_EVAL_STACK_AUTO_SKIPPED &&
        a->launch_protocol
        && !a->receipt.identity
        && a->receipt.phase == WL_COLUMNAR_EVAL_STACK_PREPARED_INERT)))
        return false;
    if (ctx->child_bound) {
        const wl_columnar_eval_plan_numeric_attempt_t *n = &ctx->child;
        if (n->identity != n || n->frame != ctx->frame || n->plan != ctx->plan
            || ctx->frame->numeric_plan_owner != n || !n->semantic_result)
            return false;
        wl_columnar_eval_plan_numeric_phase_t phase = n->phase ==
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING ? n->pending_action :
            n->phase;
        if (phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
            || phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE)
            /* A nonzero semantic result short-circuits the normal cursor++
             * in UNARY_ADVANCE; the failed JOIN is still current. */
            return n->cursor == a->cursor;
        if (n->cursor != a->cursor) return false;
        if (phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ACK
            || phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ADVANCE)
            return true;
        if (phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD)
            return false;
        unsigned stage = ctx->frame->filter.stage;
        return stage == WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE
               || stage == WL_COLUMNAR_FILTER_STAGE_CONTROL
               || stage == WL_COLUMNAR_FILTER_STAGE_FINISH
               || stage == WL_COLUMNAR_FILTER_STAGE_RESET
               || stage == WL_COLUMNAR_FILTER_STAGE_DONE;
    }
    wl_columnar_eval_serial_framed_phase_t phase = ctx->phase ==
        WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING ? ctx->pending_action :
        ctx->phase;
    return phase == WL_COLUMNAR_EVAL_SERIAL_FRAMED_CLAIM
           || (phase >= WL_COLUMNAR_EVAL_SERIAL_FRAMED_BATCH_END
           && phase <= WL_COLUMNAR_EVAL_SERIAL_FRAMED_SCRATCH_REFERENCE)
           || (phase >= WL_COLUMNAR_EVAL_SERIAL_FRAMED_CANDIDATE_ABORT
           && phase <= WL_COLUMNAR_EVAL_SERIAL_FRAMED_DONE);
}

int
wl_columnar_eval_stack_auto_cleanup_debt(const wl_col_session_t *session,
    const wl_columnar_eval_private_bindings_t *bindings,
    uint64_t operation_identity)
{
    const wl_columnar_eval_stack_cleanup_frame_t *frame = session
        ? session->cleanup_active : NULL;
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)
        || frame->session != session || frame->prepared_unary || !bindings)
        return 0;
    const wl_columnar_eval_serial_framed_attempt_t *ctx = &frame->framed;
    if (ctx->identity != ctx || ctx->frame != frame || ctx->session != session
        || !ctx->pin_owned || !ctx->thread_valid || !ctx->child_bound
        || !ctx->control || !ctx->attempt_id
        || ctx->private_bindings != bindings || ctx->plan != bindings->plan
        || ctx->control != bindings->control ||
        ctx->attempt_id != bindings->attempt_id
        || ctx->operation_identity != operation_identity || !ctx->primary_result
        || !ctx->plan || !ctx->plan->ops) return 0;
    const wl_columnar_eval_plan_numeric_attempt_t *n = &ctx->child;
    const wl_columnar_eval_stack_auto_t *a =
        &((const wl_columnar_eval_stack_allocation_t *)frame)->automatic;
    if (frame->numeric_plan_owner != n || n->identity != n || n->frame != frame
        || n->session != session || n->plan != ctx->plan ||
        n->control != ctx->control
        || n->attempt_id != ctx->attempt_id || !n->thread_valid ||
        !n->semantic_result
        || n->private_bindings != bindings || a->saved_binding != bindings
        || a->saved_binding_generation != bindings->generation)
        return 0;
#if defined(WL_HAVE_C11_THREADS)
    if (!thrd_equal(ctx->owner_thread, n->owner_thread)) return 0;
#elif defined(_WIN32) || defined(_WIN64)
    if (ctx->owner_thread != n->owner_thread) return 0;
#else
    if (!pthread_equal(ctx->owner_thread, n->owner_thread)) return 0;
#endif
    if (a->identity != a || a->frame != frame || !a->abort_requested
        || a->launch || a->busy || a->cursor >= ctx->plan->op_count
        || a->operation != &ctx->plan->ops[a->cursor]
        || a->operation->op != WL_PLAN_OP_JOIN || n->cursor != a->cursor)
        return 0;
    bool acked = a->receipt.identity == &a->receipt
        && a->receipt.phase == WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED;
    if (a->phase == WL_COLUMNAR_EVAL_STACK_AUTO_SKIPPED) {
        if (a->finish || !a->launch_protocol || (!acked && (a->receipt.identity
            || a->receipt.phase !=
            WL_COLUMNAR_EVAL_STACK_PREPARED_INERT))) return 0;
    } else if (!acked || (a->phase != WL_COLUMNAR_EVAL_STACK_AUTO_WAIT_FINISH
        && a->phase != WL_COLUMNAR_EVAL_STACK_AUTO_FINISHED)) return 0;
    wl_columnar_eval_plan_numeric_phase_t phase = n->phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING ? n->pending_action :
        n->phase;
    if (phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD) {
        unsigned stage = frame->filter.stage;
        if (stage != WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE
            && stage != WL_COLUMNAR_FILTER_STAGE_CONTROL
            && stage != WL_COLUMNAR_FILTER_STAGE_FINISH
            && stage != WL_COLUMNAR_FILTER_STAGE_RESET
            && stage != WL_COLUMNAR_FILTER_STAGE_DONE) return 0;
    } else if (phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ACK
        && phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_ADVANCE
        && phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_RELEASE
        && phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE) return 0;
    return ctx->primary_result;
}

bool
wl_columnar_eval_stack_auto_cleanup_current(const wl_col_session_t *session)
{
    const wl_columnar_eval_stack_cleanup_frame_t *frame = session
        ? session->cleanup_active : NULL;
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)
        || frame->session != session) return false;
    const wl_columnar_eval_serial_framed_attempt_t *ctx = &frame->framed;
    if (!ctx->thread_valid || !ctx->child.thread_valid) return false;
#if defined(WL_HAVE_C11_THREADS)
    return thrd_equal(ctx->owner_thread, thrd_current())
           && thrd_equal(ctx->child.owner_thread, thrd_current());
#elif defined(_WIN32) || defined(_WIN64)
    return ctx->owner_thread == GetCurrentThreadId()
           && ctx->child.owner_thread == GetCurrentThreadId();
#else
    return pthread_equal(ctx->owner_thread, pthread_self())
           && pthread_equal(ctx->child.owner_thread, pthread_self());
#endif
}

int
wl_columnar_eval_stack_auto_cleanup_shape(const wl_col_session_t *session,
    const wl_columnar_eval_serial_framed_attempt_t *ctx)
{
    const wl_columnar_eval_stack_cleanup_frame_t *frame = session
        ? session->cleanup_active : NULL;
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)
        || frame->session != session || ctx != &frame->framed
        || frame->prepared_unary) return EINVAL;
    const wl_columnar_eval_plan_numeric_attempt_t *n = &ctx->child;
    if (!ctx->phase || ctx->phase > WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
        || !n->composed_mode || n->composed_busy
        || n->runtime_depth > COL_STACK_MAX ||
        n->preflight_depth > COL_STACK_MAX
        || n->cursor < n->begin || n->cursor >= n->end
        || !n->plan || n->end > n->plan->op_count
        || n->source_initialized || n->source_bare || n->join_host
        || n->join_host_admitted || n->join_host_governor || n->lftj.identity
        || n->lftj_metadata.phase != WL_COLUMNAR_LFTJ_METADATA_INERT)
        return EINVAL;
    wl_columnar_eval_plan_numeric_phase_t phase = n->phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING ? n->pending_action :
        n->phase;
    if (phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD) {
        const wl_columnar_filter_context_t *c = &frame->filter;
        const wl_columnar_eval_stack_auto_t *a =
            &((const wl_columnar_eval_stack_allocation_t *)frame)->automatic;
        if (c->identity != c || c->frame != frame || c->session != session
            || c->busy || !c->thread_valid || c->attempt_id != ctx->attempt_id)
            return EINVAL;
        if (c->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE
            && (!c->pin_owned || !c->join_decision_pending
            || c->join_operation != a->operation || c->control != ctx->control
            || c->join_disposition != WL_COLUMNAR_JOIN_DISPOSITION_REPLACE
            || c->committed || frame->unary_committed || !c->execution_result
            || c->execution_result != c->semantic_result)) return EINVAL;
    }
    return 0;
}

int
wl_columnar_eval_stack_auto_cleanup_ready(const wl_col_session_t *session)
{
    const wl_columnar_eval_stack_cleanup_frame_t *frame = session
        ? session->cleanup_active : NULL;
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)
        || frame->session != session || frame->prepared_unary) return EINVAL;
    const wl_columnar_eval_serial_framed_attempt_t *ctx = &frame->framed;
    const wl_columnar_eval_plan_numeric_attempt_t *n = &ctx->child;
    if (!ctx->thread_valid || !n->thread_valid) return EBUSY;
#if defined(WL_HAVE_C11_THREADS)
    if (!thrd_equal(ctx->owner_thread, thrd_current())
        || !thrd_equal(n->owner_thread, thrd_current())) return EBUSY;
#elif defined(_WIN32) || defined(_WIN64)
    if (ctx->owner_thread != GetCurrentThreadId()
        || n->owner_thread != GetCurrentThreadId()) return EBUSY;
#else
    if (!pthread_equal(ctx->owner_thread, pthread_self())
        || !pthread_equal(n->owner_thread, pthread_self())) return EBUSY;
#endif
    if (!ctx->phase || ctx->phase > WL_COLUMNAR_EVAL_SERIAL_FRAMED_PENDING
        || !n->composed_mode || n->composed_busy
        || n->runtime_depth > COL_STACK_MAX
        || n->preflight_depth > COL_STACK_MAX
        || n->cursor < n->begin || n->cursor >= n->end
        || !n->plan || n->end > n->plan->op_count
        || n->source_initialized || n->source_bare || n->join_host
        || n->join_host_admitted || n->join_host_governor || n->lftj.identity
        || n->lftj_metadata.phase != WL_COLUMNAR_LFTJ_METADATA_INERT)
        return EINVAL;
    wl_columnar_eval_plan_numeric_phase_t phase = n->phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING ? n->pending_action :
        n->phase;
    if (phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD) {
        const wl_columnar_filter_context_t *c = &frame->filter;
        const wl_columnar_eval_stack_auto_t *a =
            &((const wl_columnar_eval_stack_allocation_t *)frame)->automatic;
        if (c->identity != c || c->frame != frame || c->session != session
            || c->busy || !c->thread_valid || c->attempt_id != ctx->attempt_id)
            return EINVAL;
        if (c->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE
            && (!c->pin_owned || !c->join_decision_pending
            || c->join_operation != a->operation || c->control != ctx->control
            || c->join_disposition != WL_COLUMNAR_JOIN_DISPOSITION_REPLACE
            || c->committed || frame->unary_committed || !c->execution_result
            || c->execution_result != c->semantic_result)) return EINVAL;
    }
    return 0;
}

int
wl_columnar_eval_stack_auto_cleanup_admit(
    const wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id)
{
    if (!ctx || !ctx->session || ctx->session->cleanup_active != ctx->frame)
        return EINVAL;
    int rc = wl_columnar_eval_stack_framed_authenticate(ctx->frame,
            ctx->session, ctx);
    if (rc) return rc;
    rc = wl_columnar_eval_stack_auto_fund(funding, id, ctx->control,
            WL_COLUMNAR_EVAL_PLAN_NUMERIC_HOST_WORK);
    if (rc) return rc;
    wl_columnar_eval_stack_cleanup_frame_t *frame = ctx->frame;
    const wl_columnar_eval_stack_auto_t *a =
        wl_columnar_eval_stack_automatic(frame);
    const wl_columnar_eval_plan_numeric_attempt_t *n = &ctx->child;
    if (!ctx->child_bound || frame->prepared_unary
        || !wl_columnar_eval_stack_auto_error_drain(ctx)
        || !wl_columnar_eval_plan_numeric_owner_valid(n, frame)
        || n->session != ctx->session || n->control != ctx->control
        || n->attempt_id != ctx->attempt_id || n->plan != ctx->plan
        || !n->composed_mode || n->composed_busy
        || n->runtime_depth > COL_STACK_MAX ||
        n->preflight_depth > COL_STACK_MAX
        || n->cursor < n->begin || n->cursor >= n->end
        || n->end > n->plan->op_count || n->source_initialized || n->source_bare
        || n->join_host || n->join_host_admitted || n->join_host_governor
        || n->lftj.identity
        || n->lftj_metadata.phase != WL_COLUMNAR_LFTJ_METADATA_INERT)
        return EINVAL;
    const wl_columnar_eval_private_bindings_t *bindings = a->saved_binding;
    if (!bindings || n->private_bindings != bindings
        || ctx->private_bindings != bindings
        || bindings->generation != a->saved_binding_generation)
        return EINVAL;
    if (bindings->dynamic_entries
        ? !wl_columnar_session_observed_bindings_cleanup_authorized(
            ctx->session, bindings, ctx->plan, ctx->control, ctx->attempt_id,
            a->saved_binding_generation)
        : bindings->identity != (uintptr_t)bindings
        || bindings->session != ctx->session || bindings->plan != ctx->plan
        || bindings->control != ctx->control
        || bindings->attempt_id != ctx->attempt_id || !bindings->count
        || bindings->count > WL_COLUMNAR_EVAL_PRIVATE_BINDING_MAX)
        return EINVAL;
    /* Real ACK validates and clears every owning field before assigning ACKED.
     * Fresh INERT and failed pre-activation constructors preserve that state.
     * All legal mutations install nonterminal phases callback-free; the real
     * history proof above rejects them and any marker/launch/busy owner. */
    if (a->phase == WL_COLUMNAR_EVAL_STACK_AUTO_SKIPPED) {
        if (a->finish || !a->launch_protocol) return EINVAL;
    } else if (a->receipt.identity != &a->receipt
        || a->receipt.phase != WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED
        || (a->phase != WL_COLUMNAR_EVAL_STACK_AUTO_WAIT_FINISH
        && a->phase != WL_COLUMNAR_EVAL_STACK_AUTO_FINISHED))
        return EINVAL;
    wl_columnar_eval_plan_numeric_phase_t phase = n->phase ==
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_PENDING ? n->pending_action
        : n->phase;
    if (phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_UNARY_CHILD) {
        wl_columnar_filter_context_t *c = &frame->filter;
        if (c->identity != c || c->frame != frame || c->session != ctx->session
            || c->busy || !c->thread_valid || c->attempt_id != ctx->attempt_id)
            return EINVAL;
        if (c->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_DECIDE
            && (!c->pin_owned || !c->join_decision_pending
            || c->join_operation != a->operation || c->control != ctx->control
            || c->join_disposition != WL_COLUMNAR_JOIN_DISPOSITION_REPLACE
            || c->committed || frame->unary_committed
            || !c->execution_result ||
            c->execution_result != c->semantic_result))
            return EINVAL;
    }
    return 0;
}

/* The caller has just successfully ACKed this real framed numeric child.
 * No callback or yield separates its ACK proof and these lifetime clears. */
void
wl_columnar_eval_stack_auto_child_acked(
    wl_columnar_eval_serial_framed_attempt_t *ctx)
{
    wl_columnar_eval_stack_auto_t *a =
        wl_columnar_eval_stack_automatic(ctx->frame);
    a->saved_binding = NULL;
    a->saved_binding_generation = 0;
}

int
wl_columnar_eval_stack_auto_close_ready(
    wl_columnar_eval_serial_framed_attempt_t *ctx,
    wl_evaluation_control_t *funding, uint64_t id)
{
    int rc = wl_columnar_eval_stack_auto_fund(funding, id, ctx->control, 128);
    if (rc) return rc;
    wl_columnar_eval_stack_auto_t *a =
        wl_columnar_eval_stack_automatic(ctx->frame);
    if (a->identity != a || a->frame != ctx->frame || a->busy || a->launch
        || ctx->frame->prepared_unary) return EBUSY;
    if (!a->receipt.identity)
        return a->receipt.phase ==
               WL_COLUMNAR_EVAL_STACK_PREPARED_INERT ? 0 : EINVAL;
    return a->receipt.identity == &a->receipt
           && a->receipt.phase == WL_COLUMNAR_EVAL_STACK_PREPARED_ACKED
           && wl_columnar_eval_stack_prepared_terminal_inert(&a->receipt)
        ? 0 : EBUSY;
}

#ifdef WL_SESSION_TEST_HOOKS
/* Test-only bounded copied observation. Pointer values are comparison tokens,
 * never permits to query/mutate a native receipt via the manual API. */
int
wl_columnar_eval_stack_test_auto_observe(
    const wl_columnar_eval_stack_cleanup_frame_t *f,
    const wl_col_session_t *session,
    wl_columnar_eval_stack_test_auto_observation_t *out)
{
    if (!f || !session || session->cleanup_active != f
        || !wl_columnar_eval_stack_unary_frame_valid(f)
        || f->session != session
        || !wl_columnar_eval_stack_protocol_copy_output(f, out, sizeof(*out)))
        return EINVAL;
    const wl_columnar_eval_stack_allocation_t *allocation = (const void *)f;
    const wl_columnar_eval_stack_auto_t *a = &allocation->automatic;
    if (a->identity != a || a->frame != f) return EINVAL;
    out->receipt = &a->receipt; out->launch = a->launch;
    out->launch_id = a->launch_id;
    out->saved_binding = a->saved_binding;
    out->saved_binding_generation = a->saved_binding_generation;
    out->operation = a->operation;
    out->phase = a->phase; out->receipt_phase = a->receipt.phase;
    out->parent_phase = f->framed.phase;
    out->numeric_phase = f->framed.child.phase;
    out->cursor = f->framed.child.cursor;
    out->launch_protocol = a->launch_protocol;
    out->first_result = a->receipt.first_result;
    out->busy = a->busy; out->committed = a->receipt.committed;
    out->finish = a->finish; out->abort_requested = a->abort_requested;
    return 0;
}
#endif

int
wl_columnar_eval_stack_framed_fresh(
    wl_columnar_eval_stack_cleanup_frame_t *frame,
    wl_columnar_eval_serial_framed_attempt_t *ctx)
{
    if (!ctx || !wl_columnar_eval_stack_unary_frame_valid(frame)
        || &frame->framed != ctx) return EINVAL;
    if (frame->prepared_unary) return EBUSY;
    if (ctx->identity || ctx->phase || frame->numeric_plan_owner
        || frame->stack.top || frame->unary_phase !=
        WL_COLUMNAR_EVAL_STACK_UNARY_IDLE
        || frame->filter.stage != WL_COLUMNAR_FILTER_STAGE_INERT
        || frame->binary.phase || frame->concat.phase
        || frame->unary_control || frame->unary_attempt_id
        || frame->unary_thread_valid || frame->unary_committed
        || frame->unary_result_saved || frame->unary_first_result
        || frame->unary_cleanup_result || frame->unary_alias_rejected
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->result)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_output)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_retired)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_snapshot))
        return EBUSY;
    return 0;
}

static int
wl_columnar_eval_stack_framed_workspace(
    wl_columnar_eval_serial_framed_attempt_t *ctx)
{
    if (!ctx || !wl_columnar_eval_serial_framed_owner_valid(ctx, ctx->frame))
        return EINVAL;
    wl_columnar_eval_stack_cleanup_frame_t *frame = ctx->frame;
    if (!wl_columnar_eval_stack_unary_frame_valid(frame)
        || &frame->framed != ctx) return EINVAL;
    if (frame->session != ctx->session
        || ctx->session->cleanup_active != frame) return EBUSY;
    if (frame->prepared_unary) return EBUSY;
    if (frame->stream_host
        || frame->numeric_plan_owner != (ctx->child_bound ? &ctx->child : NULL)
        || frame->filter.stage != WL_COLUMNAR_FILTER_STAGE_INERT
        || frame->binary.phase || frame->concat.phase
        || frame->unary_control || frame->unary_attempt_id
        || frame->unary_thread_valid || frame->unary_committed
        || frame->unary_result_saved || frame->unary_first_result
        || frame->unary_cleanup_result || frame->unary_alias_rejected
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_output)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_retired)
        || !wl_columnar_eval_stack_unary_entry_inert(&frame->unary_snapshot))
        return EBUSY;
    return 0;
}

int
wl_columnar_eval_stack_framed_claim(
    wl_columnar_eval_serial_framed_attempt_t *ctx, bool materialize)
{
    int rc = wl_columnar_eval_stack_framed_workspace(ctx);
    if (rc) return rc;
    if (ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_CLAIM)
        return EBUSY;
    wl_columnar_eval_stack_cleanup_frame_t *frame = ctx->frame;
    if (ctx->gate_owned || frame->unary_phase !=
        WL_COLUMNAR_EVAL_STACK_UNARY_IDLE) return EBUSY;
    if (ctx->child_bound && (ctx->child.phase !=
        WL_COLUMNAR_EVAL_PLAN_NUMERIC_COMPOSE_DONE
        || frame->stack.top != ctx->child.runtime_depth)) return EBUSY;
    if (materialize) {
        if (!ctx->child_bound || ctx->child.semantic_result
            || frame->stack.top != 1
            || !wl_columnar_eval_stack_unary_entry_inert(&frame->result))
            return EINVAL;
        eval_entry_t *entry = &frame->stack.items[0];
        if (entry->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION
            || !entry->owned || !entry->rel || entry->is_delta
            || entry->continuation || entry->seg_boundaries || entry->seg_count)
            return ENOTSUP;
        ctx->input = *entry;
    }
    frame->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE;
    ctx->gate_owned = true;
    return 0;
}

int
wl_columnar_eval_stack_framed_dispose_slot(
    wl_columnar_eval_serial_framed_attempt_t *ctx, uint32_t slot)
{
    int rc = wl_columnar_eval_stack_framed_workspace(ctx);
    if (rc) return rc;
    if (ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_ENTRY_DRAIN)
        return EBUSY;
    wl_columnar_eval_stack_cleanup_frame_t *frame = ctx->frame;
    if (slot > COL_STACK_MAX || !ctx->gate_owned
        || frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE)
        return EINVAL;
    eval_entry_t *entry = slot == COL_STACK_MAX ? &frame->result
        : &frame->stack.items[slot];
    if (entry->kind != WL_COLUMNAR_EVAL_ENTRY_RELATION || entry->continuation)
        return ENOTSUP;
    bool input = slot == 0 && ctx->input.rel == entry->rel;
    rc = eval_entry_dispose(entry);
    if (rc) return rc;
    *entry = (eval_entry_t){0};
    if (input) ctx->input = (eval_entry_t){0};
    return 0;
}

int
wl_columnar_eval_stack_framed_release_gate(
    wl_columnar_eval_serial_framed_attempt_t *ctx)
{
    int rc = wl_columnar_eval_stack_framed_workspace(ctx);
    if (rc) return rc;
    if (ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_GATE_RELEASE)
        return EBUSY;
    if (!ctx->gate_owned || ctx->source_initialized || ctx->batch_initialized
        || ctx->input.rel || ctx->input.seg_boundaries
        || ctx->frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_ACTIVE)
        return EBUSY;
    ctx->frame->unary_phase = WL_COLUMNAR_EVAL_STACK_UNARY_IDLE;
    ctx->gate_owned = false;
    return 0;
}

int
wl_columnar_eval_stack_framed_prove_slot(
    wl_columnar_eval_serial_framed_attempt_t *ctx, uint32_t slot)
{
    int rc = wl_columnar_eval_stack_framed_workspace(ctx);
    if (rc) return rc;
    if (ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_SPARE_PROOF)
        return EBUSY;
    if (slot > COL_STACK_MAX || ctx->child_bound || ctx->gate_owned)
        return EINVAL;
    const eval_entry_t *entry = slot == COL_STACK_MAX ? &ctx->frame->result
        : &ctx->frame->stack.items[slot];
    return wl_columnar_eval_stack_unary_entry_inert(entry) ? 0 : EBUSY;
}

int
wl_columnar_eval_stack_framed_normalize(
    wl_columnar_eval_serial_framed_attempt_t *ctx)
{
    int rc = wl_columnar_eval_stack_framed_workspace(ctx);
    if (rc) return rc;
    if (ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_NORMALIZE)
        return EBUSY;
    if (ctx->child_bound || ctx->gate_owned || ctx->cursor != COL_STACK_MAX+1
        || ctx->frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_IDLE)
        return EBUSY;
    ctx->frame->stack.top = 0;
    return 0;
}

int
wl_columnar_eval_stack_framed_close_proof(
    wl_columnar_eval_serial_framed_attempt_t *ctx)
{
    int rc = wl_columnar_eval_stack_framed_workspace(ctx);
    if (rc) return rc;
    if (ctx->phase != WL_COLUMNAR_EVAL_SERIAL_FRAMED_CLOSE_PROOF)
        return EBUSY;
    wl_columnar_eval_stack_cleanup_frame_t *frame = ctx->frame;
    if (frame->stack.top ||
        !wl_columnar_eval_stack_unary_entry_inert(&frame->result)
        || frame->unary_phase != WL_COLUMNAR_EVAL_STACK_UNARY_IDLE
        || ctx->child_bound || ctx->child.identity || ctx->gate_owned
        || ctx->source_initialized || ctx->batch_initialized
        || ctx->candidate || ctx->candidate_owned || ctx->pending_funding
        || ctx->scratch.allocated || ctx->scratch.admitted
        || ctx->scratch.ref_held || ctx->input.rel
        || (ctx->publication.identity && !ctx->publication.drained)
        || ctx->cursor != COL_STACK_MAX+1)
        return EBUSY;
    if (frame->governor && (frame->reservation.identity != &frame->reservation
        || atomic_load_explicit(&frame->reservation.state,
        memory_order_acquire) != WL_COLUMNAR_MEMORY_RESERVATION_COMMITTED))
        return EINVAL;
    return 0;
}

/* Consume the separately established seal. Parent identity/native/original
 * residence and LIFO are checked by the final API before HOST admission.
 * No valid serialized parent API or old claim can mutate a sealed empty frame. */
int
wl_columnar_eval_stack_framed_close_ready(
    wl_columnar_eval_serial_framed_attempt_t *ctx)
{
    return ctx && ctx->close_proven && ctx->phase ==
           WL_COLUMNAR_EVAL_SERIAL_FRAMED_DONE ? 0 : EBUSY;
}

int
wl_columnar_eval_stack_unary_prepare_scalar_retirement_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, const void *owner,
    bool *done)
{
    int rc = wl_columnar_eval_stack_unary_scalar_authority(frame, owner);
    if (rc) return rc;
    if (frame->unary_scalar_busy) return EBUSY;
    frame->unary_scalar_busy = true;
    rc =
        wl_columnar_eval_stack_unary_prepare_scalar_retirement_owned_body(frame,
            owner, done);
    frame->unary_scalar_busy = false; return rc;
}

int
wl_columnar_eval_stack_unary_finish_scalar_funded_owned(
    wl_columnar_eval_stack_cleanup_frame_t *frame, int initiating_result,
    const void *owner, wl_evaluation_control_t *funding, uint64_t funding_id,
    int *semantic, bool *done)
{
    int rc = wl_columnar_eval_stack_unary_scalar_authority(frame, owner);
    if (rc) return rc;
    if (frame->unary_scalar_busy) return EBUSY;
    frame->unary_scalar_busy = true;
    rc = wl_columnar_eval_stack_unary_finish_scalar_funded_owned_body(frame,
            initiating_result, owner, funding, funding_id, semantic, done);
    frame->unary_scalar_busy = false; return rc;
}
