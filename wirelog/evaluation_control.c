/*
 * evaluation_control.c - retained evaluation-control primitives (#1819)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */
#include "evaluation_control.h"
#include "columnar/mem_ledger.h"
#include "thread.h"

#include <errno.h>
#include <stdlib.h>

struct wl_evaluation_control {
    wl_mutex_t mutex;
    wl_atomic_u64 references;
    wl_atomic_u64 cancelled;
    wl_atomic_u64 active_attempt_id;
    uint64_t identity;
    const void *owner;
    uint64_t configured_limit;
    uint64_t attempt_id;
    uint64_t attempt_limit;
    uint64_t consumed;
    wl_evaluation_control_stop_t stop;
    int accounting_error;
    bool active;
    wl_evaluation_control_report_t completed;
    const wl_evaluation_control_source_bank_t *source_bank_class;
};

static wl_atomic_u64 wl_next_evaluation_control_identity = 1;

static int
wl_evaluation_control_next_identity(uint64_t *identity)
{
    uint64_t observed = atomic_load_explicit(
        &wl_next_evaluation_control_identity, memory_order_relaxed);
    for (;;) {
        if (!observed || observed == UINT64_MAX)
            return EOVERFLOW;
        if (atomic_compare_exchange_weak_explicit(
                &wl_next_evaluation_control_identity, &observed,
                observed + 1, memory_order_relaxed, memory_order_relaxed)) {
            *identity = observed;
            return 0;
        }
    }
}

#ifdef WL_SESSION_TEST_HOOKS
int
wl_evaluation_control_test_seed_next_identity(uint64_t value)
{
    atomic_store_explicit(&wl_next_evaluation_control_identity, value,
        memory_order_relaxed);
    return 0;
}
#endif

int
wl_evaluation_control_create(uint64_t limit, wl_evaluation_control_t **out)
{
    if (!out)
        return EINVAL;
    *out = NULL;
    uint64_t identity;
    int rc = wl_evaluation_control_next_identity(&identity);
    if (rc != 0)
        return rc;
    wl_evaluation_control_t *control = calloc(1, sizeof(*control));
    if (!control)
        return ENOMEM;
    if (wl_mutex_init(&control->mutex) != 0) {
        free(control);
        return ENOMEM;
    }
    atomic_init(&control->references, 1);
    atomic_init(&control->cancelled, 0);
    atomic_init(&control->active_attempt_id, 0);
    control->identity = identity;
    control->configured_limit = limit;
    *out = control;
    return 0;
}

uint64_t
wl_evaluation_control_identity(const wl_evaluation_control_t *control)
{
    return control ? control->identity : 0;
}

int
wl_evaluation_control_retain(wl_evaluation_control_t *control)
{
    if (!control)
        return EINVAL;
    /* RMW read also uses Interlocked on MSVC; no volatile publication. */
    uint64_t count = (uint64_t)atomic_fetch_add_explicit(
        &control->references, 0, memory_order_relaxed);
    do {
        /* Keeps the portability shim's signed RMW arithmetic in range. */
        if (count >= UINT32_MAX)
            return EOVERFLOW;
    } while (!atomic_compare_exchange_weak_explicit(&control->references,
        &count, count + 1, memory_order_relaxed, memory_order_relaxed));
    return 0;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_evaluation_control_test_retain_claim)(wl_evaluation_control_t *);
#endif

int
wl_evaluation_control_retain_try(wl_evaluation_control_t *control)
{
    if (!control)
        return EINVAL;
    uint64_t count = (uint64_t)atomic_fetch_add_explicit(
        &control->references, 0, memory_order_relaxed);
    if (!count)
        return EINVAL;
    if (count >= UINT32_MAX)
        return EOVERFLOW;
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_evaluation_control_test_retain_claim)
        wl_evaluation_control_test_retain_claim(control);
#endif
    return atomic_compare_exchange_strong_explicit(&control->references,
               &count, count + 1, memory_order_relaxed, memory_order_relaxed)
        ? 0 : EAGAIN;
}

void
wl_evaluation_control_release(wl_evaluation_control_t *control)
{
    if (!control)
        return;
    if (atomic_fetch_sub_explicit(&control->references, 1,
        memory_order_release) == 1) {
        (void)atomic_fetch_add_explicit(&control->references, 0,
            memory_order_acquire);
        wl_mutex_destroy(&control->mutex);
        free(control);
    }
}

int
wl_evaluation_control_attach(wl_evaluation_control_t *control,
    const void *owner)
{
    if (!control || !owner)
        return EINVAL;
    wl_mutex_lock(&control->mutex);
    int rc = control->owner ? EBUSY : wl_evaluation_control_retain(control);
    if (rc == 0)
        control->owner = owner;
    wl_mutex_unlock(&control->mutex);
    return rc;
}

int
wl_evaluation_control_detach(wl_evaluation_control_t *control,
    const void *owner)
{
    if (!control || !owner)
        return EINVAL;
    wl_mutex_lock(&control->mutex);
    int rc = control->owner != owner ? EINVAL : control->active ? EBUSY : 0;
    if (rc == 0)
        control->owner = NULL;
    wl_mutex_unlock(&control->mutex);
    if (rc == 0)
        wl_evaluation_control_release(control);
    return rc;
}

int
wl_evaluation_control_transfer(wl_evaluation_control_t *control,
    const void *owner, const void *next_owner)
{
    if (!control || !owner || !next_owner)
        return EINVAL;
    wl_mutex_lock(&control->mutex);
    int rc = control->owner != owner ? EINVAL : control->active ? EBUSY : 0;
    if (rc == 0)
        control->owner = next_owner;
    wl_mutex_unlock(&control->mutex);
    return rc;
}

int
wl_evaluation_control_configure(wl_evaluation_control_t *control,
    uint64_t limit)
{
    if (!control)
        return EINVAL;
    wl_mutex_lock(&control->mutex);
    int rc = control->active ? EBUSY : 0;
    if (rc == 0)
        control->configured_limit = limit;
    wl_mutex_unlock(&control->mutex);
    return rc;
}

int
wl_evaluation_control_reset(wl_evaluation_control_t *control)
{
    if (!control)
        return EINVAL;
    wl_mutex_lock(&control->mutex);
    int rc = control->active ? EBUSY : 0;
    if (rc == 0)
        atomic_exchange_explicit(&control->cancelled, 0, memory_order_relaxed);
    wl_mutex_unlock(&control->mutex);
    return rc;
}

void
wl_evaluation_control_request_cancel(wl_evaluation_control_t *control)
{
    if (control)
        atomic_exchange_explicit(&control->cancelled, 1, memory_order_relaxed);
}

int
wl_evaluation_control_begin(wl_evaluation_control_t *control,
    const void *owner)
{
    if (!control || !owner)
        return EINVAL;
    wl_mutex_lock(&control->mutex);
    int rc = control->owner != owner ? EINVAL : control->active ? EBUSY : 0;
    if (rc == 0 && control->attempt_id == UINT64_MAX)
        rc = EOVERFLOW;
    if (rc == 0) {
        control->active = true;
        control->attempt_id++;
        control->attempt_limit = control->configured_limit;
        control->consumed = 0;
        control->stop = WL_EVALUATION_CONTROL_NONE;
        control->accounting_error = 0;
        (void)atomic_exchange_explicit(&control->active_attempt_id,
            control->attempt_id, memory_order_release);
    }
    wl_mutex_unlock(&control->mutex);
    return rc;
}

uint64_t
wl_evaluation_control_active_attempt_id(const wl_evaluation_control_t *control)
{
    if (!control)
        return 0;
#ifdef _MSC_VER
    /* Logical const: neutral Interlocked read of privately mutable storage. */
    return (uint64_t)atomic_fetch_add_explicit(
        (wl_atomic_u64 *)&control->active_attempt_id, UINT64_C(0),
        memory_order_acquire);
#else
    return atomic_load_explicit(&control->active_attempt_id,
               memory_order_acquire);
#endif
}

#ifdef WL_SESSION_TEST_HOOKS
int
wl_evaluation_control_test_active_consumed(wl_evaluation_control_t *control,
    uint64_t expected_attempt_id, uint64_t *out)
{
    if (!control || !out || !expected_attempt_id) return EINVAL;
    uintptr_t source = (uintptr_t)control, dest = (uintptr_t)out;
    if (source > UINTPTR_MAX - sizeof(*control)
        || dest > UINTPTR_MAX - sizeof(*out)
        || (dest < source + sizeof(*control)
        && source < dest + sizeof(*out))) return EINVAL;
    wl_mutex_lock(&control->mutex);
    int rc = !control->active || control->attempt_id != expected_attempt_id
        ? EINVAL : 0;
    if (!rc) *out = control->consumed;
    wl_mutex_unlock(&control->mutex);
    return rc;
}

int
wl_evaluation_control_test_seed_attempt_id(wl_evaluation_control_t *control,
    const void *owner, uint64_t value)
{
    if (!control || !owner)
        return EINVAL;
    wl_mutex_lock(&control->mutex);
    int rc = control->owner != owner ? EINVAL : control->active ? EBUSY
        : value < control->attempt_id ? EINVAL : 0;
    if (rc == 0)
        control->attempt_id = value;
    wl_mutex_unlock(&control->mutex);
    return rc;
}
#endif

static int
wl_evaluation_control_charge_locked(wl_evaluation_control_t *control,
    uint64_t work)
{
    int rc = 0;
    if (!control->active) {
        rc = EINVAL;
    } else if (control->accounting_error) {
        rc = control->accounting_error;
    } else if (control->stop != WL_EVALUATION_CONTROL_NONE) {
        rc = control->stop;
    } else if (atomic_fetch_add_explicit(&control->cancelled, 0,
        memory_order_relaxed) != 0) {
        control->stop = WL_EVALUATION_CONTROL_CANCELLED;
        rc = control->stop;
    } else if (control->attempt_limit != 0
        && work > control->attempt_limit - control->consumed) {
        control->stop = WL_EVALUATION_CONTROL_WORK_LIMIT;
        rc = control->stop;
    } else if (work > UINT64_MAX - control->consumed) {
        control->accounting_error = EOVERFLOW;
        rc = EOVERFLOW;
    } else {
        control->consumed += work;
    }
    return rc;
}

int
wl_evaluation_control_charge(wl_evaluation_control_t *control,
    uint64_t work)
{
    if (!control)
        return EINVAL;
    wl_mutex_lock(&control->mutex);
    int rc = wl_evaluation_control_charge_locked(control, work);
    wl_mutex_unlock(&control->mutex);
    return rc;
}

int
wl_evaluation_control_charge_attempt(wl_evaluation_control_t *control,
    uint64_t expected_attempt_id, uint64_t work)
{
    if (!control || !expected_attempt_id)
        return EINVAL;
    wl_mutex_lock(&control->mutex);
    int rc = control->active && control->attempt_id == expected_attempt_id
        ? wl_evaluation_control_charge_locked(control, work) : EINVAL;
    wl_mutex_unlock(&control->mutex);
    return rc;
}

static bool
wl_evaluation_control_is_stop(int status)
{
    return status == WL_EVALUATION_CONTROL_CANCELLED
           || status == WL_EVALUATION_CONTROL_WORK_LIMIT;
}

int
wl_evaluation_control_finish(wl_evaluation_control_t *control,
    const void *owner, int execution_status, int cleanup_status)
{
    if (!control || !owner)
        return EINVAL;
    wl_mutex_lock(&control->mutex);
    if (control->owner != owner || !control->active) {
        wl_mutex_unlock(&control->mutex);
        return EINVAL;
    }
    if (wl_evaluation_control_is_stop(execution_status)) {
        if (control->stop == WL_EVALUATION_CONTROL_NONE)
            control->stop = (wl_evaluation_control_stop_t)execution_status;
        execution_status = 0;
    }
    if (execution_status == 0)
        execution_status = control->accounting_error;
    int result = execution_status != 0 ? execution_status
        : cleanup_status != 0 ? cleanup_status : (int)control->stop;
    control->completed = (wl_evaluation_control_report_t){
        .attempt_id = control->attempt_id,
        .limit = control->attempt_limit,
        .consumed = control->consumed,
        .stop_reason = control->stop,
        .execution_status = execution_status,
        .cleanup_status = cleanup_status,
        .result = result,
    };
    control->active = false;
    (void)atomic_exchange_explicit(&control->active_attempt_id, 0,
        memory_order_release);
    wl_mutex_unlock(&control->mutex);
    return result;
}

int
wl_evaluation_control_report(wl_evaluation_control_t *control,
    wl_evaluation_control_report_t *out)
{
    if (!control || !out)
        return EINVAL;
    wl_mutex_lock(&control->mutex);
    int rc = control->completed.attempt_id == 0 ? ENOENT : 0;
    if (rc == 0)
        *out = control->completed;
    wl_mutex_unlock(&control->mutex);
    return rc;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_evaluation_control_test_finance_birth_claim)(void);
unsigned wl_evaluation_control_test_finance_birth_fail;
#endif

static void *
wl_evaluation_control_lexical_finance_allocate(size_t bytes)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_evaluation_control_test_finance_birth_fail == 1) {
        wl_evaluation_control_test_finance_birth_fail = 0;
        return NULL;
    }
#endif
    return calloc(1, bytes);
}

static int
wl_evaluation_control_lexical_finance_mutex_init(wl_mutex_t *mutex)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_evaluation_control_test_finance_birth_fail == 2) {
        wl_evaluation_control_test_finance_birth_fail = 0;
        return ENOMEM;
    }
#endif
    return wl_mutex_init(mutex);
}

static wl_evaluation_control_lexical_finance_birth_result_t
wl_evaluation_control_attached_birth_try(uint64_t limit,
    const void *owner)
{
    wl_evaluation_control_lexical_finance_birth_result_t out = {0};
    if (!owner) {
        out.protocol = EINVAL; return out;
    }
    uint64_t identity =
        atomic_load_explicit(&wl_next_evaluation_control_identity,
            memory_order_relaxed);
    if (!identity || identity == UINT64_MAX) {
        out.protocol = EOVERFLOW; return out;
    }
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_evaluation_control_test_finance_birth_claim)
        wl_evaluation_control_test_finance_birth_claim();
#endif
    if (!atomic_compare_exchange_strong_explicit(
            &wl_next_evaluation_control_identity,
            &identity, identity + 1, memory_order_relaxed,
            memory_order_relaxed)) {
        out.protocol = EAGAIN; return out;
    }
    wl_evaluation_control_t *control =
        wl_evaluation_control_lexical_finance_allocate(sizeof(*control));
    if (!control) {
        out.protocol = ENOMEM; return out;
    }
    if (wl_evaluation_control_lexical_finance_mutex_init(&control->mutex)) {
        free(control); out.protocol = ENOMEM; return out;
    }
    /* Both obligations are established here on this fresh allocation:
     * creation and this exact owner's attachment. No borrowed ref is reset. */
    atomic_init(&control->references, 2);
    atomic_init(&control->cancelled, 0);
    atomic_init(&control->active_attempt_id, 0);
    control->owner = owner; control->identity = identity;
    control->configured_limit = limit;
    out.protocol = wl_evaluation_control_begin(control, owner);
    if (out.protocol) {
        /* Fresh private BEGIN cannot have a foreign owner or active attempt.
         * No allocation or association has been published to the caller. */
        control->owner = NULL;
        wl_evaluation_control_release(control);
        wl_evaluation_control_release(control);
        return out;
    }
    out.control = control; out.identity = identity;
    out.id = wl_evaluation_control_active_attempt_id(control);
    return out;
}

/* Positive-only fresh Source generation. No existing controller or prior
 * cancellation/report can enter: the pointer stays private until fully born. */
wl_evaluation_control_lexical_finance_birth_result_t
wl_evaluation_control_lexical_finance_fresh_birth_try(uint64_t limit,
    const void *owner)
{
    wl_evaluation_control_lexical_finance_birth_result_t out = {0};
    if (!owner || !limit) {
        out.protocol = EINVAL; return out;
    }
    uint64_t identity =
        atomic_load_explicit(&wl_next_evaluation_control_identity,
            memory_order_relaxed);
    if (!identity || identity == UINT64_MAX) {
        out.protocol = EOVERFLOW; return out;
    }
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_evaluation_control_test_finance_birth_claim)
        wl_evaluation_control_test_finance_birth_claim();
#endif
    if (!atomic_compare_exchange_strong_explicit(
            &wl_next_evaluation_control_identity,
            &identity, identity + 1, memory_order_relaxed,
            memory_order_relaxed)){
        out.protocol = EAGAIN; return out;
    }
    wl_evaluation_control_t *control =
        wl_evaluation_control_lexical_finance_allocate(sizeof(*control));
    if (!control) {
        out.protocol = ENOMEM; return out;
    }
    if (wl_evaluation_control_lexical_finance_mutex_init(&control->mutex)) {
        free(control); out.protocol = ENOMEM; return out;
    }
    atomic_init(&control->references, 2);
    atomic_init(&control->cancelled, 0);
    control->owner = owner;
    control->identity = identity;
    control->configured_limit = limit;
    control->attempt_id = 1;
    control->attempt_limit = limit;
    control->consumed = 0;
    control->stop = WL_EVALUATION_CONTROL_NONE;
    control->accounting_error = 0;
    control->active = true;
    control->source_bank_class = NULL;
    control->completed = (wl_evaluation_control_report_t){
        .attempt_id = 0, .limit = 0, .consumed = 0,
        .stop_reason = WL_EVALUATION_CONTROL_NONE,
        .execution_status = 0, .cleanup_status = 0, .result = 0
    };
    atomic_init(&control->active_attempt_id, 1);
    out.control = control; out.identity = identity; out.id = 1;
    return out;
}

wl_evaluation_control_lexical_finance_birth_result_t
wl_evaluation_control_lexical_finance_birth_try(uint64_t limit,
    const void *owner)
{
    if (!limit) {
        wl_evaluation_control_lexical_finance_birth_result_t out = {
            .protocol = EINVAL,
        };
        return out;
    }
    return wl_evaluation_control_attached_birth_try(limit, owner);
}

wl_evaluation_control_lexical_finance_birth_result_t
wl_evaluation_control_private_financier_birth_try(uint64_t limit,
    const void *owner)
{
    return wl_evaluation_control_attached_birth_try(limit, owner);
}

wl_evaluation_control_lexical_finance_finish_result_t
wl_evaluation_control_lexical_finance_finish_owned(
    wl_evaluation_control_t *control, const void *owner, size_t owner_bytes,
    uint64_t saved_id, uint64_t limit, uint64_t charged)
{
    wl_evaluation_control_lexical_finance_finish_result_t out = {0};
    out.protocol = EINVAL;
    if (!control || !owner || !owner_bytes || !saved_id || !limit
        || charged > limit) return out;
    uintptr_t c = (uintptr_t)control, o = (uintptr_t)owner;
    if (sizeof(*control) > UINTPTR_MAX - c || owner_bytes > UINTPTR_MAX - o)
        return out;
    if (c < o + owner_bytes && o < c + sizeof(*control)) return out;
    if (wl_mutex_lock(&control->mutex)) {
        out.protocol = EIO; return out;
    }
    uint64_t references = (uint64_t)atomic_fetch_add_explicit(
        &control->references, 0, memory_order_relaxed);
    if (control->source_bank_class || control->owner != owner
        || control->configured_limit != limit || control->attempt_limit != limit
        || control->consumed != charged || control->accounting_error
        || !control->active || control->attempt_id != saved_id
        || wl_evaluation_control_active_attempt_id(control) != saved_id
        || (control->stop != WL_EVALUATION_CONTROL_NONE
        && control->stop != WL_EVALUATION_CONTROL_CANCELLED
        && control->stop != WL_EVALUATION_CONTROL_WORK_LIMIT)) goto refused;
    if (references != 2) {
        out.protocol = EBUSY; goto refused;
    }
    /* This is the actual financial commit authorization. No callback,
     * unlock, new controller or ref handoff separates it from completion. */
    out.checkpoint_result = wl_evaluation_control_charge_locked(control, 0);
    out.result = (int)control->stop;
    control->completed = (wl_evaluation_control_report_t){
        .attempt_id = control->attempt_id, .limit = control->attempt_limit,
        .consumed = control->consumed, .stop_reason = control->stop,
        .execution_status = 0, .cleanup_status = 0, .result = out.result,
    };
    control->active = false;
    uint64_t previous = (uint64_t)atomic_exchange_explicit(
        &control->active_attempt_id, 0, memory_order_release);
    if (previous != saved_id || control->completed.attempt_id != saved_id
        || control->completed.limit != limit ||
        control->completed.consumed != charged
        || control->completed.execution_status ||
        control->completed.cleanup_status
        || control->completed.stop_reason != out.result
        || control->completed.result != out.result
        || out.checkpoint_result != out.result) abort();
    out.report = control->completed; out.finished = true; out.protocol = 0;
refused:
    if (wl_mutex_unlock(&control->mutex)) abort();
    return out;
}

wl_evaluation_control_lexical_finance_result_t
wl_evaluation_control_lexical_finance_retire_active_owned(
    wl_evaluation_control_t *control, const void *owner, size_t owner_bytes,
    uint64_t saved_id, uint64_t limit, uint64_t charged)
{
    wl_evaluation_control_lexical_finance_result_t out = {0};
    out.protocol = EINVAL;
    if (!control || !owner || !owner_bytes || !saved_id || !limit
        || charged > limit) return out;
    uintptr_t c = (uintptr_t)control, o = (uintptr_t)owner;
    if (sizeof(*control) > UINTPTR_MAX - c || owner_bytes > UINTPTR_MAX - o)
        return out;
    if (c < o + owner_bytes && o < c + sizeof(*control)) return out;
    if (wl_mutex_lock(&control->mutex)) {
        out.protocol = EIO; return out;
    }
    uint64_t references = (uint64_t)atomic_fetch_add_explicit(
        &control->references, 0, memory_order_relaxed);
    if (control->source_bank_class || control->owner != owner
        || control->configured_limit != limit || control->attempt_limit != limit
        || control->consumed != charged || control->accounting_error
        || !control->active || control->attempt_id != saved_id
        || wl_evaluation_control_active_attempt_id(control) != saved_id
        || (control->stop != WL_EVALUATION_CONTROL_NONE
        && control->stop != WL_EVALUATION_CONTROL_CANCELLED
        && control->stop != WL_EVALUATION_CONTROL_WORK_LIMIT)) goto refused;
    if (references != 2) {
        out.protocol = EBUSY; goto refused;
    }
    /* The private caller keeps the creation/attachment pair unexposed
     * through both drops. This is final financial retirement, never a
     * Native handoff or a transferable two-reference permit. */
    out.checkpoint_result = wl_evaluation_control_charge_locked(control, 0);
    out.result = (int)control->stop;
    control->completed = (wl_evaluation_control_report_t){
        .attempt_id = control->attempt_id, .limit = control->attempt_limit,
        .consumed = control->consumed, .stop_reason = control->stop,
        .execution_status = 0, .cleanup_status = 0, .result = out.result,
    };
    control->active = false;
    uint64_t previous = (uint64_t)atomic_exchange_explicit(
        &control->active_attempt_id, 0, memory_order_release);
    if (previous != saved_id) abort();
    out.report = control->completed; out.report_valid = true;
    control->owner = NULL;
    if (wl_mutex_unlock(&control->mutex)) abort();
    wl_evaluation_control_release(control);
    wl_evaluation_control_release(control);
    out.closed = true; out.protocol = 0;
    return out;
refused:
    if (wl_mutex_unlock(&control->mutex)) abort();
    return out;
}

static int
wl_evaluation_control_lexical_finance_arguments(
    wl_evaluation_control_t *control,
    const wl_evaluation_control_lexical_finance_expectation_t *e)
{
    if (!control || !e) return EINVAL;
    uintptr_t c = (uintptr_t)control, x = (uintptr_t)e;
    if (sizeof(*control) > UINTPTR_MAX - c || sizeof(*e) > UINTPTR_MAX - x)
        return EINVAL;
    uintptr_t ce = c + sizeof(*control), xe = x + sizeof(*e);
    if (c < xe && x < ce) return EINVAL;
    if (!e->owner || !e->scope || !e->owner_bytes || !e->scope_bytes
        || !e->identity || !e->limit || e->charged > e->limit
        || (e->active ? !e->id : e->id != 0)) return EINVAL;
    uintptr_t o = (uintptr_t)e->owner, s = (uintptr_t)e->scope;
    if (e->owner_bytes > UINTPTR_MAX - o || e->scope_bytes > UINTPTR_MAX - s)
        return EINVAL;
    uintptr_t oe = o + e->owner_bytes, se = s + e->scope_bytes;
    if ((c < oe && o < ce) || (c < se && s < ce)
        || (x < oe && o < xe) || (x < se && s < xe) || (o < se && s < oe))
        return EINVAL;
    return 0;
}

/* The actual mutex is held. Read-only even on refusal: in particular there
 * is no charge(0), report rewrite or detach of a different generation. */
static int
wl_evaluation_control_lexical_finance_prove_locked(
    wl_evaluation_control_t *control,
    const wl_evaluation_control_lexical_finance_expectation_t *e)
{
    if (control->identity != e->identity || control->source_bank_class
        || control->owner != e->owner || control->configured_limit != e->limit
        || control->attempt_limit != e->limit || control->consumed != e->charged
        || control->accounting_error || !control->attempt_id
        || (control->stop != WL_EVALUATION_CONTROL_NONE
        && control->stop != WL_EVALUATION_CONTROL_CANCELLED
        && control->stop != WL_EVALUATION_CONTROL_WORK_LIMIT)) return EINVAL;
    if (atomic_fetch_add_explicit(&control->references, 0,
        memory_order_relaxed) != 2) return EBUSY;
    uint64_t active = (uint64_t)atomic_fetch_add_explicit(
        &control->active_attempt_id, 0, memory_order_relaxed);
    if (e->active)
        return control->active && active == e->id &&
               control->attempt_id == e->id ? 0 : EINVAL;
    const wl_evaluation_control_report_t *report = &control->completed;
    return !control->active && !active &&
           report->attempt_id == control->attempt_id
           && report->limit == e->limit && report->consumed == e->charged
           && report->stop_reason == control->stop && !report->execution_status
           && !report->cleanup_status &&
           report->result == (int)control->stop ? 0 : EINVAL;
}

int
wl_evaluation_control_lexical_finance_prove_checked(
    wl_evaluation_control_t *control,
    const wl_evaluation_control_lexical_finance_expectation_t *e)
{
    int rc = wl_evaluation_control_lexical_finance_arguments(control, e);
    if (rc) return rc;
    if (wl_mutex_lock(&control->mutex)) return EIO;
    rc = wl_evaluation_control_lexical_finance_prove_locked(control, e);
    wl_mutex_unlock(&control->mutex);
    return rc;
}

wl_evaluation_control_lexical_finance_result_t
wl_evaluation_control_lexical_finance_close_checked(
    wl_evaluation_control_t *control,
    const wl_evaluation_control_lexical_finance_expectation_t *e)
{
    wl_evaluation_control_lexical_finance_result_t out = {0};
    out.protocol = wl_evaluation_control_lexical_finance_arguments(control, e);
    if (out.protocol) return out;
    if (wl_mutex_lock(&control->mutex)) {
        out.protocol = EIO; return out;
    }
    out.protocol = wl_evaluation_control_lexical_finance_prove_locked(control,
            e);
    if (out.protocol) {
        wl_mutex_unlock(&control->mutex);
        return out;
    }
    if (e->active) {
        /* Exact owner/ref/id proof above precedes this genuine checkpoint.
         * Holding the same mutex makes the saved generation stable; use the
         * real charge kernel directly, never recursively lock charge_attempt.
         * A sticky stop still requires mandatory financial retirement. */
        out.checkpoint_result = wl_evaluation_control_charge_locked(control, 0);
        control->completed = (wl_evaluation_control_report_t){
            .attempt_id = control->attempt_id,
            .limit = control->attempt_limit,
            .consumed = control->consumed,
            .stop_reason = control->stop,
            .execution_status = 0,
            .cleanup_status = 0,
            .result = (int)control->stop,
        };
        control->active = false;
        (void)atomic_exchange_explicit(&control->active_attempt_id, 0,
            memory_order_release);
    }
    out.report = control->completed; out.report_valid = true;
    out.result = out.report.result;
    control->owner = NULL;
    wl_mutex_unlock(&control->mutex);
    /* The proven private creation+attachment pair cannot race a borrower.
     * These are the real two reference drops, not a replacement allocation. */
    wl_evaluation_control_release(control);
    wl_evaluation_control_release(control);
    out.closed = true;
    return out;
}

/* Sealed financial class: no controller or saved-ID accessor/loan exists.
 * The original/BODY controller APIs retain their monotone ID contract. */
enum { WL_EVALUATION_CONTROL_SOURCE_LANES = 2 };
#define WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY \
        (UINT64_C(3) * WL_EVALUATION_CONTROL_QUANTUM)

#if defined(_MSC_VER)
#define WL_EVALUATION_CONTROL_SOURCE_THREAD_LOCAL __declspec(thread)
#else
#define WL_EVALUATION_CONTROL_SOURCE_THREAD_LOCAL _Thread_local
#endif
/* Identity cookie only, never a TLS BANK/context/control issuer. */
static WL_EVALUATION_CONTROL_SOURCE_THREAD_LOCAL unsigned char
wl_evaluation_control_source_thread_cookie;
#undef WL_EVALUATION_CONTROL_SOURCE_THREAD_LOCAL

typedef struct {
    wl_evaluation_control_t *control;
    wl_evaluation_control_source_context_t *context;
    uint64_t id;
    uint64_t charged_total;
    bool running;
    bool callback_active;
    bool report_verified;
} wl_evaluation_control_source_lane_state_t;

struct wl_evaluation_control_source_bank {
    struct wl_evaluation_control_source_bank *identity;
    struct wl_evaluation_control_source_bank **owner_slot;
    wl_mutex_t mutex;
    bool closing;
    wl_evaluation_control_source_lane_state_t
        lanes[WL_EVALUATION_CONTROL_SOURCE_LANES];
};

static int
wl_evaluation_control_source_bank_lock(
    wl_evaluation_control_source_bank_t *bank)
{
    return wl_mutex_lock(&bank->mutex) == 0 ? 0 : EIO;
}

static void
wl_evaluation_control_source_bank_unlock(
    wl_evaluation_control_source_bank_t *bank)
{
    if (wl_mutex_unlock(&bank->mutex)) abort();
}

/* BANK mutex is held. This private entry accepts a genuine BANK and lane,
 * never a native-control/owner pair supplied by an original/BODY caller.
 * Class marker, lane membership, real owner/ref2 and quiescence all agree.
 * New native/mutex/atomic fronts here are ordinary source, not exact-nine. */
static int
wl_evaluation_control_source_bank_rearm_phase(
    wl_evaluation_control_source_bank_t *bank, unsigned index)
{
    if (!bank || bank->identity != bank
        || index >= WL_EVALUATION_CONTROL_SOURCE_LANES) return EINVAL;
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    wl_evaluation_control_t *control = lane->control;
    if (!control || !lane->context || lane->running || lane->callback_active
        || (lane->id && !lane->report_verified)) return EBUSY;
    if (wl_mutex_lock(&control->mutex)) return EIO;
#ifdef _MSC_VER
    uint64_t references = (uint64_t)atomic_fetch_add_explicit(
        &control->references, UINT64_C(0), memory_order_acquire);
#else
    uint64_t references = atomic_load_explicit(&control->references,
            memory_order_acquire);
#endif
    int rc = control->source_bank_class != bank || control->owner != lane
        || control->active || references != UINT64_C(2)
        || wl_evaluation_control_active_attempt_id(control) != 0
        ? EBUSY : 0;
    if (!rc) {
        /* No raw control/ID has ever left this sealed class. The preceding
         * phase's actual report was checked and every callback returned.
         * Generic/original/BODY instances have markerNULL and cannot enter. */
        control->attempt_id = 0;
        lane->id = 0;
        lane->charged_total = 0;
        lane->report_verified = false;
    }
    if (wl_mutex_unlock(&control->mutex)) abort();
    return rc;
}

#ifdef WL_SESSION_TEST_HOOKS
bool wl_evaluation_control_test_source_early_rearm_ref_once;

static int
wl_evaluation_control_source_bank_test_early_rearm(
    wl_evaluation_control_source_bank_t *bank, unsigned index)
{
    bool extra = wl_evaluation_control_test_source_early_rearm_ref_once;
    wl_evaluation_control_test_source_early_rearm_ref_once = false;
    wl_evaluation_control_t *control = bank->lanes[index].control;
    if (extra) {
        int rc = wl_evaluation_control_retain(control);
        if (rc) return rc;
    }
    int rc = wl_evaluation_control_source_bank_rearm_phase(bank, index);
    if (extra) wl_evaluation_control_release(control);
    return rc;
}
#define WL_SOURCE_EARLY_REARM(bank, index) \
        wl_evaluation_control_source_bank_test_early_rearm((bank), (index))
#else
#define WL_SOURCE_EARLY_REARM(bank, index) \
        wl_evaluation_control_source_bank_rearm_phase((bank), (index))
#endif

/* BANK mutex held. Each successful actual charge is recorded exactly once. */
static int
wl_evaluation_control_source_bank_charge(
    wl_evaluation_control_source_lane_state_t *lane, uint64_t work)
{
    int rc = wl_evaluation_control_charge_attempt(lane->control, lane->id,
            work);
    if (!rc) lane->charged_total += work;
    return rc;
}

static int
wl_evaluation_control_source_bank_finish_phase(
    wl_evaluation_control_source_lane_state_t *lane)
{
    /* The lane remains active even after a refused payment. This real
     * financial checkpoint preserves that diagnostic and adds no payment. */
    (void)wl_evaluation_control_charge_attempt(lane->control, lane->id, 0);
    int finished = wl_evaluation_control_finish(lane->control, lane, 0, 0);
    wl_evaluation_control_report_t report;
    if (wl_evaluation_control_report(lane->control, &report)
        || wl_evaluation_control_active_attempt_id(lane->control)
        || report.attempt_id != lane->id
        || report.limit != WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
        || report.consumed != lane->charged_total
        || report.execution_status || report.cleanup_status
        || report.stop_reason != finished || report.result != finished)
        abort();
    lane->report_verified = true;
    lane->running = false;
    return finished;
}

/* Constructor-only financial commit. The canonical BANK mutex is held;
 * neither this lane nor its control is a Native loan. Keep its actual
 * checkpoint, publication and report readback under one control lock. */
static int
wl_evaluation_control_source_bank_finish_constructor_phase(
    wl_evaluation_control_source_bank_t *bank, unsigned index,
    const wl_evaluation_control_source_context_t *context)
{
    if (!bank || bank->identity != bank
        || index >= WL_EVALUATION_CONTROL_SOURCE_LANES || !context
        || context->identity != context || context->bank != bank
        || context->lane != index
        || context->thread_cookie !=
        &wl_evaluation_control_source_thread_cookie)
        abort();
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    wl_evaluation_control_t *control = lane->control;
    if (lane->context != context || !lane->running || lane->callback_active
        || !lane->id || !control) abort();
    if (wl_mutex_lock(&control->mutex)) abort();
    uint64_t references = (uint64_t)atomic_fetch_add_explicit(
        &control->references, 0, memory_order_relaxed);
    if (control->source_bank_class != bank || control->owner != lane
        || !control->active || control->attempt_id != lane->id
        || wl_evaluation_control_active_attempt_id(control) != lane->id
        || control->configured_limit !=
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
        || control->attempt_limit != WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
        || control->consumed != lane->charged_total
        || control->consumed > control->attempt_limit || references != 2
        || control->accounting_error
        || (control->stop != WL_EVALUATION_CONTROL_NONE
        && control->stop != WL_EVALUATION_CONTROL_CANCELLED
        && control->stop != WL_EVALUATION_CONTROL_WORK_LIMIT)) abort();
    /* Same lock order and real saved-generation observation as the old
     * phase, without a second FINISH/report lock or inactive-ID read. */
    (void)wl_evaluation_control_charge_locked(control, 0);
    int execution_status = control->accounting_error;
    int finished = execution_status ? execution_status : (int)control->stop;
    control->completed = (wl_evaluation_control_report_t){
        .attempt_id = control->attempt_id,
        .limit = control->attempt_limit,
        .consumed = control->consumed,
        .stop_reason = control->stop,
        .execution_status = execution_status,
        .cleanup_status = 0,
        .result = finished,
    };
    control->active = false;
    uint64_t previous = (uint64_t)atomic_exchange_explicit(
        &control->active_attempt_id, 0, memory_order_release);
    if (previous != lane->id
        || control->completed.attempt_id != lane->id
        || control->completed.limit !=
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
        || control->completed.consumed != lane->charged_total
        || control->completed.execution_status ||
        control->completed.cleanup_status
        || control->completed.stop_reason != finished
        || control->completed.result != finished) abort();
    lane->report_verified = true; lane->running = false;
    if (wl_mutex_unlock(&control->mutex)) abort();
    return finished;
}

/* Used only while BANK birth is unpublished and no BEGIN has occurred. */
static void
wl_evaluation_control_source_bank_unwind_birth(
    wl_evaluation_control_source_bank_t *bank, unsigned created)
{
    for (unsigned i = 0; i < created; ++i) {
        wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[i];
        if (wl_evaluation_control_detach(lane->control, lane)) abort();
        wl_evaluation_control_release(lane->control);
    }
    wl_mutex_destroy(&bank->mutex);
    free(bank);
}

int
wl_evaluation_control_source_bank_create(
    wl_evaluation_control_source_bank_t **out)
{
    if (!out || *out) return EINVAL;
    wl_evaluation_control_source_bank_t *bank = calloc(1, sizeof(*bank));
    if (!bank) return ENOMEM;
    if (wl_mutex_init(&bank->mutex)) {
        free(bank);
        return ENOMEM;
    }
    bank->identity = bank;
    bank->owner_slot = out;
    for (unsigned i = 0; i < WL_EVALUATION_CONTROL_SOURCE_LANES; ++i) {
        wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[i];
        int rc = wl_evaluation_control_create(
            WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY, &lane->control);
        if (rc) {
            wl_evaluation_control_source_bank_unwind_birth(bank, i);
            return rc;
        }
        rc = wl_evaluation_control_attach(lane->control, lane);
        if (rc) {
            wl_evaluation_control_release(lane->control);
            wl_evaluation_control_source_bank_unwind_birth(bank, i);
            return rc;
        }
        /* The only marker writer: fresh controller, stable private owner,
         * no BEGIN, raw controller/ID still unpublished and never exported. */
        lane->control->source_bank_class = bank;
    }
    *out = bank;
    return 0;
}

int
wl_evaluation_control_source_context_begin(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    wl_evaluation_control_source_context_t *context)
{
    if (!bank || bank->identity != bank || !context
        || context->identity || context->bank || context->lane
        || context->thread_cookie
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES)
        return EINVAL;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (bank->closing || lane->context || lane->running
        || lane->callback_active) rc = EBUSY;
    if (!rc) {
        context->identity = context;
        context->bank = bank;
        context->lane = (unsigned)index;
        context->thread_cookie = &wl_evaluation_control_source_thread_cookie;
        lane->context = context;
    }
    wl_evaluation_control_source_bank_unlock(bank);
    return rc;
}

int
wl_evaluation_control_source_context_end(
    wl_evaluation_control_source_context_t *context)
{
    if (!context || context->identity != context || !context->bank
        || context->lane >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || context->thread_cookie !=
        &wl_evaluation_control_source_thread_cookie)
        return EINVAL;
    wl_evaluation_control_source_bank_t *bank = context->bank;
    if (bank->identity != bank) return EINVAL;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[context->lane];
    if (lane->context != context) rc = EINVAL;
    else if (lane->running || lane->callback_active
        || wl_evaluation_control_active_attempt_id(lane->control)
        || (lane->id && !lane->report_verified)) rc = EBUSY;
    if (!rc) {
        lane->context = NULL;
        context->identity = NULL;
        context->bank = NULL;
        context->lane = 0;
        context->thread_cookie = NULL;
    }
    wl_evaluation_control_source_bank_unlock(bank);
    return rc;
}

wl_evaluation_control_source_phase_result_t
wl_evaluation_control_source_bank_run(
    wl_evaluation_control_source_context_t *context,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn action, void *payload)
{
    wl_evaluation_control_source_phase_result_t result = {EINVAL, 0, false};
    if (!context || context->identity != context || !context->bank
        || context->lane >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || context->thread_cookie != &wl_evaluation_control_source_thread_cookie
        || !policy || !action || !policy->bootstrap_work
        || !policy->current_work || !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->current_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM)
        return result;
    wl_evaluation_control_source_bank_t *bank = context->bank;
    if (bank->identity != bank || payload == bank || payload == context)
        return result;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) {
        result.financial_result = rc; return result;
    }
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[context->lane];
    if (bank->closing || lane->context != context || lane->running
        || lane->callback_active) {
        wl_evaluation_control_source_bank_unlock(bank);
        result.financial_result = EBUSY;
        return result;
    }
    rc = wl_evaluation_control_source_bank_rearm_phase(bank, context->lane);
    if (!rc) rc = wl_evaluation_control_begin(lane->control, lane);
    if (rc) {
        wl_evaluation_control_source_bank_unlock(bank);
        result.financial_result = rc;
        return result;
    }
    lane->id = wl_evaluation_control_active_attempt_id(lane->control);
    if (!lane->id) abort();
    lane->running = true;
    rc = wl_evaluation_control_source_bank_charge(lane, policy->bootstrap_work);
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->current_work);
    if (!rc) {
        lane->callback_active = true;
        wl_evaluation_control_source_bank_unlock(bank);
        /* SOURCE-only action: no bank/context/native-ID is an argument or
         * stored callback. A constructor executes immediately after CURRENT.
         * No BODY/user hook may be called by the admitted action. */
        result.action_result = action(payload);
        result.action_called = true;
        if (wl_evaluation_control_source_bank_lock(bank)) abort();
        lane->callback_active = false;
        rc = wl_evaluation_control_source_bank_charge(lane, policy->close_work);
    }
    int finished = wl_evaluation_control_source_bank_finish_phase(lane);
    result.financial_result = rc ? rc : finished;
    wl_evaluation_control_source_bank_unlock(bank);
    return result;
}

/* Synchronous SOURCE constructor activation. The canonical context never
 * leaves this stack; its lane association is cleared under the same mutex
 * after real phase settlement, including failed admissions/actions. */
wl_evaluation_control_source_phase_result_t
wl_evaluation_control_source_bank_run_lexical(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn action, void *payload)
{
    wl_evaluation_control_source_phase_result_t result = {EINVAL, 0, false};
    if (!bank || bank->identity != bank
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || !policy || !action || payload == bank
        || !policy->bootstrap_work || !policy->current_work
        || !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->current_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM)
        return result;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) {
        result.financial_result = rc; return result;
    }
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (bank->closing || lane->context || lane->running
        || lane->callback_active) {
        wl_evaluation_control_source_bank_unlock(bank);
        result.financial_result = EBUSY;
        return result;
    }
    wl_evaluation_control_source_context_t context = {
        .identity = &context, .bank = bank, .lane = (unsigned)index,
        .thread_cookie = &wl_evaluation_control_source_thread_cookie
    };
    lane->context = &context;
    rc = wl_evaluation_control_source_bank_rearm_phase(bank, (unsigned)index);
    if (!rc) rc = wl_evaluation_control_begin(lane->control, lane);
    if (rc) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        result.financial_result = rc;
        return result;
    }
    lane->id = wl_evaluation_control_active_attempt_id(lane->control);
    if (!lane->id) abort();
    lane->running = true;
    rc = wl_evaluation_control_source_bank_charge(lane, policy->bootstrap_work);
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->current_work);
    if (!rc) {
        lane->callback_active = true;
        wl_evaluation_control_source_bank_unlock(bank);
        result.action_result = action(payload);
        result.action_called = true;
        if (wl_evaluation_control_source_bank_lock(bank)) abort();
        lane->callback_active = false;
        rc = wl_evaluation_control_source_bank_charge(lane, policy->close_work);
    }
    int finished = wl_evaluation_control_source_bank_finish_phase(lane);
    result.financial_result = rc ? rc : finished;
    lane->context = NULL;
    wl_evaluation_control_source_bank_unlock(bank);
    return result;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_evaluation_control_test_source_pair_checkpoint)(void);
#endif

/* Private Source construction uses this additive issuer; the general
 * lexical and pair kernels retain their literal lifecycle. */
wl_evaluation_control_source_phase_result_t
wl_evaluation_control_source_bank_run_lexical_constructor(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn action, void *payload)
{
    wl_evaluation_control_source_phase_result_t result = {EINVAL, 0, false};
    if (!bank || bank->identity != bank
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || !policy || !action || payload == bank
        || !policy->bootstrap_work || !policy->current_work
        || !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->current_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM)
        return result;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) {
        result.financial_result = rc; return result;
    }
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (bank->closing || lane->context || lane->running
        || lane->callback_active) {
        wl_evaluation_control_source_bank_unlock(bank);
        result.financial_result = EBUSY;
        return result;
    }
    wl_evaluation_control_source_context_t context = {
        .identity = &context, .bank = bank, .lane = (unsigned)index,
        .thread_cookie = &wl_evaluation_control_source_thread_cookie
    };
    lane->context = &context;
    rc = wl_evaluation_control_source_bank_rearm_phase(bank, (unsigned)index);
    if (!rc) rc = wl_evaluation_control_begin(lane->control, lane);
    if (rc) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        result.financial_result = rc;
        return result;
    }
    lane->id = wl_evaluation_control_active_attempt_id(lane->control);
    if (!lane->id) abort();
    lane->running = true;
    rc = wl_evaluation_control_source_bank_charge(lane, policy->bootstrap_work);
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->current_work);
    if (!rc) {
        lane->callback_active = true;
        wl_evaluation_control_source_bank_unlock(bank);
        result.action_result = action(payload);
        result.action_called = true;
        if (wl_evaluation_control_source_bank_lock(bank)) abort();
        lane->callback_active = false;
        rc = wl_evaluation_control_source_bank_charge(lane, policy->close_work);
    }
    int finished = wl_evaluation_control_source_bank_finish_constructor_phase(
        bank, (unsigned)index, &context);
    result.financial_result = rc ? rc : finished;
    lane->context = NULL;
    wl_evaluation_control_source_bank_unlock(bank);
    return result;
}

wl_evaluation_control_source_pair_result_t
wl_evaluation_control_source_bank_run_lexical_pair(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    const wl_evaluation_control_source_pair_policy_t *policy,
    wl_evaluation_control_source_action_fn first, void *first_payload,
    wl_evaluation_control_source_action_fn second, void *second_payload)
{
    wl_evaluation_control_source_pair_result_t result = {EINVAL, 0, 0, false,
                                                         false};
    if (!bank || bank->identity != bank
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || !policy || !first || !second || first_payload == bank ||
        second_payload == bank
        || !policy->bootstrap_work || !policy->first_work ||
        !policy->second_work || !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->first_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->second_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM) return result;
    /* Each validated term is at most Q. Their first three terms fit 3Q
     * without overflow; subtraction tests the fourth before adding it. */
    uint64_t first_three = policy->bootstrap_work + policy->first_work +
        policy->second_work;
    if (policy->close_work >
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY - first_three)
        return result;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) {
        result.financial_result = rc; return result;
    }
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (bank->closing || lane->context || lane->running ||
        lane->callback_active){
        wl_evaluation_control_source_bank_unlock(bank);
        result.financial_result = EBUSY; return result;
    }
    wl_evaluation_control_source_context_t context = {
        .identity = &context, .bank = bank, .lane = (unsigned)index,
        .thread_cookie = &wl_evaluation_control_source_thread_cookie
    };
    lane->context = &context;
    rc = wl_evaluation_control_source_bank_rearm_phase(bank, (unsigned)index);
    if (!rc) rc = wl_evaluation_control_begin(lane->control, lane);
    if (rc) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        result.financial_result = rc; return result;
    }
    lane->id = wl_evaluation_control_active_attempt_id(lane->control);
    if (!lane->id) abort();
    lane->running = true;
    rc = wl_evaluation_control_source_bank_charge(lane, policy->bootstrap_work);
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->first_work);
    if (!rc) {
        lane->callback_active = true;
        wl_evaluation_control_source_bank_unlock(bank);
        result.first_result = first(first_payload);
        result.first_called = true;
        if (wl_evaluation_control_source_bank_lock(bank)) abort();
        lane->callback_active = false;
        /* This is an actual check on the same still-active saved BANK
         * generation. Neither FINISH nor report/rearm stands in for it. */
        rc = wl_evaluation_control_source_bank_charge(lane, 0);
#ifdef WL_SESSION_TEST_HOOKS
        if (!rc && wl_evaluation_control_test_source_pair_checkpoint)
            wl_evaluation_control_test_source_pair_checkpoint();
#endif
        if (!rc && !result.first_result) {
            rc = wl_evaluation_control_source_bank_charge(lane,
                    policy->second_work);
            if (!rc) {
                lane->callback_active = true;
                wl_evaluation_control_source_bank_unlock(bank);
                result.second_result = second(second_payload);
                result.second_called = true;
                if (wl_evaluation_control_source_bank_lock(bank)) abort();
                lane->callback_active = false;
            }
        }
        if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                    policy->close_work);
    }
    int finished = wl_evaluation_control_source_bank_finish_phase(lane);
    result.financial_result = rc ? rc : finished;
    lane->context = NULL;
    wl_evaluation_control_source_bank_unlock(bank);
    return result;
}

#ifdef WL_SESSION_TEST_HOOKS
unsigned wl_evaluation_control_test_source_final_cancel_at;
unsigned wl_evaluation_control_test_source_final_geometry_fault;

static void
wl_evaluation_control_source_final_test_geometry(
    const wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_extent_t *payload)
{
    unsigned fault = wl_evaluation_control_test_source_final_geometry_fault;
    if (fault && fault <= WL_EVALUATION_CONTROL_SOURCE_LANES) {
        wl_evaluation_control_test_source_final_geometry_fault = 0;
        payload->base = (const unsigned char *)bank->lanes[fault - 1].control +
            1;
        payload->bytes = 1;
    }
}
#define WL_SOURCE_FINAL_GEOMETRY(type, name, input, bank) \
        type name = *(input); \
        (wl_evaluation_control_source_final_test_geometry)((bank), \
        &name.payload)
static bool
wl_evaluation_control_source_final_test_closed(
    const wl_evaluation_control_source_bank_t *bank,
    const wl_evaluation_control_source_lane_state_t *lane,
    unsigned cancelled)
{
    wl_evaluation_control_t *control = lane->control;
    const wl_evaluation_control_report_t *r = &control->completed;
    return control->source_bank_class == bank && control->owner == lane
           && !control->active &&
           !wl_evaluation_control_active_attempt_id(control)
           && control->identity && control->attempt_id == lane->id
           && control->configured_limit ==
           WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
           && control->attempt_limit ==
           WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
           && control->consumed == lane->charged_total &&
           !control->accounting_error
           && control->stop == WL_EVALUATION_CONTROL_CANCELLED
           && atomic_fetch_add_explicit(&control->references, 0,
               memory_order_relaxed) == 2
           && atomic_fetch_add_explicit(&control->cancelled, 0,
               memory_order_relaxed) == cancelled
           && r->attempt_id == lane->id && r->limit == control->attempt_limit
           && r->consumed == lane->charged_total && !r->execution_status &&
           !r->cleanup_status
           && r->stop_reason == WL_EVALUATION_CONTROL_CANCELLED
           && r->result == WL_EVALUATION_CONTROL_CANCELLED;
}

unsigned wl_evaluation_control_test_source_early_report_fault;

int
wl_evaluation_control_test_source_early_report(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    wl_evaluation_control_report_t *out)
{
    if (!bank || bank->identity != bank || !out
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES) return EINVAL;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    unsigned fault = wl_evaluation_control_test_source_early_report_fault;
    wl_evaluation_control_test_source_early_report_fault = 0;
    if (fault == 1 || fault == 2)
        out =
            (wl_evaluation_control_report_t *)((unsigned char *)bank->lanes[
                fault - 1].control + 1);
    uintptr_t begin = (uintptr_t)out;
    if (sizeof(*out) > UINTPTR_MAX - begin) {
        wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
    }
    uintptr_t end = begin + sizeof(*out);
    const void *allocations[3] = {bank, bank->lanes[0].control,
                                  bank->lanes[1].control};
    const size_t sizes[3] = {sizeof(*bank), sizeof(wl_evaluation_control_t),
                             sizeof(wl_evaluation_control_t)};
    for (unsigned i = 0; i < 3; ++i) {
        uintptr_t address = (uintptr_t)allocations[i];
        if (!address || sizes[i] > UINTPTR_MAX - address
            || (begin < address + sizes[i] && address < end)) {
            wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
        }
    }
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (bank->closing || lane->context || lane->running || lane->callback_active
        || !lane->control || !lane->id || !lane->report_verified) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    wl_evaluation_control_t *control = lane->control;
    if (wl_mutex_lock(&control->mutex)) {
        wl_evaluation_control_source_bank_unlock(bank); return EIO;
    }
    bool extra_ref = false;
    if (fault == 3) {
        rc = wl_evaluation_control_retain(control);
        if (rc) {
            if (wl_mutex_unlock(&control->mutex)) abort();
            wl_evaluation_control_source_bank_unlock(bank); return rc;
        }
        extra_ref = true;
    }
    const wl_evaluation_control_report_t *r = &control->completed;
    bool valid = control->source_bank_class == bank && control->owner == lane
        && atomic_fetch_add_explicit(&control->references, 0,
            memory_order_relaxed) == 2
        && !control->active && !wl_evaluation_control_active_attempt_id(control)
        && control->identity && control->attempt_id == lane->id
        && control->configured_limit ==
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
        && control->attempt_limit == WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
        && control->consumed == lane->charged_total &&
        control->consumed <= control->attempt_limit
        && !control->accounting_error
        && (control->stop == WL_EVALUATION_CONTROL_NONE
        || control->stop == WL_EVALUATION_CONTROL_CANCELLED
        || control->stop == WL_EVALUATION_CONTROL_WORK_LIMIT)
        && r->attempt_id == lane->id && r->limit == control->attempt_limit
        && r->consumed == control->consumed && !r->execution_status &&
        !r->cleanup_status
        && r->stop_reason == control->stop && r->result == (int)control->stop;
    if (valid) *out = *r;
    if (wl_mutex_unlock(&control->mutex)) abort();
    if (extra_ref) wl_evaluation_control_release(control);
    wl_evaluation_control_source_bank_unlock(bank);
    return valid ? 0 : EINVAL;
}

int
wl_evaluation_control_test_source_final_reset_cancel(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index)
{
    if (!bank || bank->identity != bank
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES) return EINVAL;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (bank->closing || lane->context || lane->running || lane->callback_active
        || !lane->control || !lane->id || !lane->report_verified) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    wl_evaluation_control_t *control = lane->control;
    if (wl_mutex_lock(&control->mutex)) {
        wl_evaluation_control_source_bank_unlock(bank); return EIO;
    }
    bool valid = wl_evaluation_control_source_final_test_closed(bank, lane, 1);
    uint64_t identity = control->identity;
    wl_evaluation_control_report_t frozen = control->completed;
    if (wl_mutex_unlock(&control->mutex)) abort();
    /* reset acquires the control mutex itself. BANK remains locked, so no
     * other phase can start between actual validation, reset and readback. */
    rc = valid ? wl_evaluation_control_reset(control) : EINVAL;
    if (!rc) {
        if (wl_mutex_lock(&control->mutex)) abort();
        const wl_evaluation_control_report_t *r = &control->completed;
        if (!wl_evaluation_control_source_final_test_closed(bank, lane, 0)
            || control->identity != identity ||
            r->attempt_id != frozen.attempt_id
            || r->limit != frozen.limit || r->consumed != frozen.consumed
            || r->stop_reason != frozen.stop_reason ||
            r->result != frozen.result
            || r->execution_status != frozen.execution_status
            || r->cleanup_status != frozen.cleanup_status) abort();
        if (wl_mutex_unlock(&control->mutex)) abort();
    }
    wl_evaluation_control_source_bank_unlock(bank);
    return rc;
}

static void
wl_evaluation_control_source_final_test_event(
    wl_evaluation_control_source_lane_state_t *lane,
    unsigned event)
{
    if (wl_evaluation_control_test_source_final_cancel_at == event) {
        wl_evaluation_control_test_source_final_cancel_at = 0;
        wl_evaluation_control_request_cancel(lane->control);
    }
}
#define WL_SOURCE_FINAL_TEST_EVENT(lane, event) \
        wl_evaluation_control_source_final_test_event(lane, event)
#else
#define WL_SOURCE_FINAL_TEST_EVENT(lane, event) ((void)0)
#define WL_SOURCE_FINAL_GEOMETRY(type, name, input, \
            bank) const type name = *(input)
#endif

/* FINAL geometry is fixed financial metadata, not Native authority.  Each
 * group is followed by the real saved BANK checkpoint before the next group.
 * Inputs are accessible immutable typed objects throughout this call. */
static bool
wl_evaluation_control_source_extent_valid(
    wl_evaluation_control_source_extent_t e)
{
    return e.base && e.bytes && e.bytes <= UINTPTR_MAX - (uintptr_t)e.base;
}

static bool
wl_evaluation_control_source_extent_overlap(
    wl_evaluation_control_source_extent_t a,
    wl_evaluation_control_source_extent_t b)
{
    uintptr_t x = (uintptr_t)a.base, y = (uintptr_t)b.base;
    return x < y + b.bytes && y < x + a.bytes;
}

static int
wl_evaluation_control_source_final_cut(
    wl_evaluation_control_source_lane_state_t *lane,
    int *financial)
{
    int rc = wl_evaluation_control_source_bank_charge(lane, 0);
    if (rc) *financial = rc;
    return rc;
}

static int
wl_evaluation_control_source_final_geometry(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_state_t *lane,
    const wl_evaluation_control_source_extent_t *objects, unsigned count,
    wl_evaluation_control_source_extent_t geometry,
    wl_evaluation_control_source_extent_t policy,
    wl_evaluation_control_t *normal_source,
    const wl_evaluation_control_lexical_finance_expectation_t *expectation,
    int *financial)
{
    if (!wl_evaluation_control_source_extent_valid(geometry)
        || !wl_evaluation_control_source_extent_valid(policy)) return EINVAL;
    for (unsigned i = 0; i < count; ++i) {
        if (!wl_evaluation_control_source_extent_valid(
                objects[i])) return EINVAL;
        int rc = wl_evaluation_control_source_final_cut(lane, financial);
        if (rc) return rc;
    }
    /* Snapshot storage and immutable input policy must not alias writable
     * objects. Their complete values are copied before any callback too. */
    for (unsigned i = 0; i < count; ++i) {
        if (wl_evaluation_control_source_extent_overlap(objects[i], geometry)
            || wl_evaluation_control_source_extent_overlap(objects[i],
            policy)) return EINVAL;
        for (unsigned j = 0; j < i; ++j)
            if (wl_evaluation_control_source_extent_overlap(objects[i],
                objects[j])) return EINVAL;
        int rc = wl_evaluation_control_source_final_cut(lane, financial);
        if (rc) return rc;
    }
    wl_evaluation_control_source_extent_t private_objects[4] = {
        {bank, sizeof(*bank)},
        {bank->lanes[0].control, sizeof(wl_evaluation_control_t)},
        {bank->lanes[1].control, sizeof(wl_evaluation_control_t)},
        {normal_source, normal_source ? sizeof(*normal_source) : 0}
    };
    unsigned private_count = normal_source ? 4u : 3u;
    for (unsigned i = 0; i < private_count; ++i) {
        if (!wl_evaluation_control_source_extent_valid(
                private_objects[i])) return EINVAL;
        if (wl_evaluation_control_source_extent_overlap(private_objects[i],
            geometry)
            || wl_evaluation_control_source_extent_overlap(private_objects[i],
            policy)) return EINVAL;
        for (unsigned j = 0; j < count; ++j)
            if (wl_evaluation_control_source_extent_overlap(private_objects[i],
                objects[j])) return EINVAL;
        int rc = wl_evaluation_control_source_final_cut(lane, financial);
        if (rc) return rc;
    }
    if (normal_source) {
        int rc =
            wl_evaluation_control_lexical_finance_prove_checked(normal_source,
                expectation);
        if (rc) return rc;
        return wl_evaluation_control_source_final_cut(lane, financial);
    }
    return 0;
}

wl_evaluation_control_source_final_pair_result_t
wl_evaluation_control_source_bank_run_lexical_pair_final(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    const wl_evaluation_control_source_pair_policy_t *policy,
    const wl_evaluation_control_source_final_pair_geometry_t *geometry,
    wl_evaluation_control_source_action_fn first,
    wl_evaluation_control_source_action_fn second)
{
    wl_evaluation_control_source_final_pair_result_t result =
    {.financial_result = EINVAL};
    if (!bank || bank->identity != bank
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || !policy || !geometry || !first || !second
        || !policy->bootstrap_work || !policy->first_work
        || !policy->second_work || !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->first_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->second_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM) return result;
    uint64_t first_three = policy->bootstrap_work + policy->first_work +
        policy->second_work;
    if (policy->close_work >
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY - first_three)
        return result;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) {
        result.financial_result = rc; return result;
    }
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (bank->closing || lane->context || lane->running ||
        lane->callback_active){
        wl_evaluation_control_source_bank_unlock(bank);
        result.financial_result = EBUSY; return result;
    }
    wl_evaluation_control_source_context_t context = {
        .identity = &context, .bank = bank, .lane = (unsigned)index,
        .thread_cookie = &wl_evaluation_control_source_thread_cookie
    };
    lane->context = &context;
    rc = wl_evaluation_control_source_bank_rearm_phase(bank, (unsigned)index);
    if (!rc) rc = wl_evaluation_control_begin(lane->control, lane);
    if (rc) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        result.financial_result = rc; return result;
    }
    result.phase_started = true;
    lane->id = wl_evaluation_control_active_attempt_id(lane->control);
    if (!lane->id) abort();
    lane->running = true;
    WL_SOURCE_FINAL_TEST_EVENT(lane,
        WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_BEGIN);
    rc = wl_evaluation_control_source_bank_charge(lane, policy->bootstrap_work);
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->first_work);
    if (!rc) {
        WL_SOURCE_FINAL_GEOMETRY(
            wl_evaluation_control_source_final_pair_geometry_t, g, geometry,
            bank);
        const wl_evaluation_control_source_pair_policy_t paid = *policy;
        const wl_evaluation_control_source_extent_t objects[5] = {
            g.session, g.record, g.scope, g.payload, g.journal
        };
        int check = !g.source_control
            || g.source_expectation.owner != g.record.base
            || g.source_expectation.owner_bytes != g.record.bytes
            || g.source_expectation.scope != g.scope.base
            || g.source_expectation.scope_bytes != g.scope.bytes ? EINVAL : 0;
        if (!check) check = wl_evaluation_control_source_final_geometry(bank,
                    lane,
                    objects, 5,
                    (wl_evaluation_control_source_extent_t){geometry,
                                                            sizeof(*geometry)},
                    (wl_evaluation_control_source_extent_t){policy,
                                                            sizeof(*policy)},
                    g.source_control, &g.source_expectation, &rc);
        if (check) result.first_result = check;
        else {
            lane->callback_active = true;
            wl_evaluation_control_source_bank_unlock(bank);
            result.first_result = first((void *)g.scope.base);
            result.first_called = true;
            if (wl_evaluation_control_source_bank_lock(bank)) abort();
            lane->callback_active = false;
            WL_SOURCE_FINAL_TEST_EVENT(lane,
                WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_FIRST);
            /* A may have freed the normal Source. Never follow that saved
             * control again. Only BANK owns this real bridge checkpoint. */
            rc = wl_evaluation_control_source_bank_charge(lane, 0);
#ifdef WL_SESSION_TEST_HOOKS
            if (!rc && wl_evaluation_control_test_source_pair_checkpoint)
                wl_evaluation_control_test_source_pair_checkpoint();
#endif
            if (!rc && !result.first_result) {
                rc = wl_evaluation_control_source_bank_charge(lane,
                        paid.second_work);
                if (!rc) {
                    lane->callback_active = true;
                    wl_evaluation_control_source_bank_unlock(bank);
                    result.second_result = second((void *)g.payload.base);
                    result.second_called = true;
                    if (wl_evaluation_control_source_bank_lock(bank)) abort();
                    lane->callback_active = false;
                    WL_SOURCE_FINAL_TEST_EVENT(lane,
                        WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_SECOND);
                }
            }
            if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                        paid.close_work);
        }
    }
    int finished =
        wl_evaluation_control_source_bank_finish_constructor_phase(bank,
            (unsigned)index, &context);
    result.financial_result = rc ? rc : finished;
    lane->context = NULL;
    result.phase_finished = true;
    wl_evaluation_control_source_bank_unlock(bank);
    return result;
}

wl_evaluation_control_source_final_result_t
wl_evaluation_control_source_bank_run_lexical_final(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    const wl_evaluation_control_source_phase_policy_t *policy,
    const wl_evaluation_control_source_final_geometry_t *geometry,
    wl_evaluation_control_source_action_fn action)
{
    wl_evaluation_control_source_final_result_t result =
    {.financial_result = EINVAL};
    if (!bank || bank->identity != bank
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || !policy || !geometry || !action
        || !policy->bootstrap_work || !policy->current_work ||
        !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->current_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM) return result;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) {
        result.financial_result = rc; return result;
    }
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (bank->closing || lane->context || lane->running ||
        lane->callback_active){
        wl_evaluation_control_source_bank_unlock(bank);
        result.financial_result = EBUSY; return result;
    }
    wl_evaluation_control_source_context_t context = {
        .identity = &context, .bank = bank, .lane = (unsigned)index,
        .thread_cookie = &wl_evaluation_control_source_thread_cookie
    };
    lane->context = &context;
    rc = wl_evaluation_control_source_bank_rearm_phase(bank, (unsigned)index);
    if (!rc) rc = wl_evaluation_control_begin(lane->control, lane);
    if (rc) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        result.financial_result = rc; return result;
    }
    result.phase_started = true;
    lane->id = wl_evaluation_control_active_attempt_id(lane->control);
    if (!lane->id) abort();
    lane->running = true;
    WL_SOURCE_FINAL_TEST_EVENT(lane,
        WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_BEGIN);
    rc = wl_evaluation_control_source_bank_charge(lane, policy->bootstrap_work);
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->current_work);
    if (!rc) {
        WL_SOURCE_FINAL_GEOMETRY(wl_evaluation_control_source_final_geometry_t,
            g, geometry, bank);
        const wl_evaluation_control_source_phase_policy_t paid = *policy;
        const wl_evaluation_control_source_extent_t objects[3] = {g.session,
                                                                  g.payload,
                                                                  g.journal};
        int check = wl_evaluation_control_source_final_geometry(bank, lane,
                objects, 3,
                (wl_evaluation_control_source_extent_t){geometry,
                                                        sizeof(*geometry)},
                (wl_evaluation_control_source_extent_t){policy,
                                                        sizeof(*policy)}, NULL,
                NULL, &rc);
        if (check) result.action_result = check;
        else {
            lane->callback_active = true;
            wl_evaluation_control_source_bank_unlock(bank);
            result.action_result = action((void *)g.payload.base);
            result.action_called = true;
            if (wl_evaluation_control_source_bank_lock(bank)) abort();
            lane->callback_active = false;
            WL_SOURCE_FINAL_TEST_EVENT(lane,
                WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_ACTION);
            rc = wl_evaluation_control_source_bank_charge(lane,
                    paid.close_work);
        }
    }
    int finished =
        wl_evaluation_control_source_bank_finish_constructor_phase(bank,
            (unsigned)index, &context);
    result.financial_result = rc ? rc : finished;
    lane->context = NULL;
    result.phase_finished = true;
    wl_evaluation_control_source_bank_unlock(bank);
    return result;
}

/* Additive sealed issuer. Rearm has proved this actual private lane while
 * the BANK mutex remains held. The new check observes its own real BEGIN. */
typedef struct {
    int protocol, checkpoint;
    bool started;
} wl_evaluation_control_source_early_begin_t;

#ifdef WL_SESSION_TEST_HOOKS
bool wl_evaluation_control_test_source_early_cancel_once;
#endif

static wl_evaluation_control_source_early_begin_t
wl_evaluation_control_source_bank_begin_checkpoint(
    wl_evaluation_control_source_lane_state_t *lane)
{
    wl_evaluation_control_source_early_begin_t result = {0};
    wl_evaluation_control_t *control = lane->control;
    wl_mutex_lock(&control->mutex);
    int rc = control->owner != lane ? EINVAL : control->active ? EBUSY : 0;
    if (!rc && control->attempt_id == UINT64_MAX) rc = EOVERFLOW;
    if (!rc) {
        control->active = true;
        ++control->attempt_id;
        control->attempt_limit = control->configured_limit;
        control->consumed = 0;
        control->stop = WL_EVALUATION_CONTROL_NONE;
        control->accounting_error = 0;
        (void)atomic_exchange_explicit(&control->active_attempt_id,
            control->attempt_id, memory_order_release);
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_evaluation_control_test_source_early_cancel_once) {
            wl_evaluation_control_test_source_early_cancel_once = false;
            wl_evaluation_control_request_cancel(control);
        }
#endif
        result.checkpoint = wl_evaluation_control_charge_locked(control, 0);
        lane->id = control->attempt_id;
        lane->running = true;
        result.started = true;
    }
    result.protocol = rc;
    wl_mutex_unlock(&control->mutex);
    return result;
}

wl_evaluation_control_source_early_result_t
wl_evaluation_control_source_bank_run_lexical_early(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn action, void *payload)
{
    wl_evaluation_control_source_early_result_t result = {
        .protocol = EINVAL, .financial_result = EINVAL
    };
    if (!bank || bank->identity != bank
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || !policy || !action || payload == bank
        || !policy->bootstrap_work || !policy->current_work
        || !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->current_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM)
        return result;
    result.protocol = 0;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) {
        result.protocol = rc; result.financial_result = rc; return result;
    }
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (bank->closing || lane->context || lane->running
        || lane->callback_active) {
        wl_evaluation_control_source_bank_unlock(bank);
        result.protocol = EBUSY; result.financial_result = EBUSY;
        return result;
    }
    wl_evaluation_control_source_context_t context = {
        .identity = &context, .bank = bank, .lane = (unsigned)index,
        .thread_cookie = &wl_evaluation_control_source_thread_cookie
    };
    lane->context = &context;
    rc = WL_SOURCE_EARLY_REARM(bank, (unsigned)index);
    if (rc) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        result.protocol = rc; result.financial_result = rc;
        return result;
    }
    wl_evaluation_control_source_early_begin_t begun =
        wl_evaluation_control_source_bank_begin_checkpoint(lane);
    result.begin_protocol = begun.protocol;
    result.checkpoint_result = begun.checkpoint;
    if (!begun.started) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        result.protocol = begun.protocol;
        result.financial_result = begun.protocol;
        return result;
    }
    result.phase_started = true;
    rc = begun.checkpoint;
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->bootstrap_work);
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->current_work);
    if (!rc) {
        lane->callback_active = true;
        wl_evaluation_control_source_bank_unlock(bank);
        result.first_result = action(payload);
        result.first_called = true;
        if (wl_evaluation_control_source_bank_lock(bank)) abort();
        lane->callback_active = false;
        WL_SOURCE_FINAL_TEST_EVENT(lane,
            WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_ACTION);
        rc = wl_evaluation_control_source_bank_charge(lane, policy->close_work);
    }
    int finished = wl_evaluation_control_source_bank_finish_phase(lane);
    result.financial_result = rc ? rc : finished;
    lane->context = NULL;
    result.phase_finished = true;
    wl_evaluation_control_source_bank_unlock(bank);
    return result;
}

wl_evaluation_control_source_early_result_t
wl_evaluation_control_source_bank_run_lexical_constructor_early(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn action, void *payload)
{
    wl_evaluation_control_source_early_result_t result = {
        .protocol = EINVAL, .financial_result = EINVAL
    };
    if (!bank || bank->identity != bank
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || !policy || !action || payload == bank
        || !policy->bootstrap_work || !policy->current_work
        || !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->current_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM)
        return result;
    result.protocol = 0;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) {
        result.protocol = rc; result.financial_result = rc; return result;
    }
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (bank->closing || lane->context || lane->running
        || lane->callback_active) {
        wl_evaluation_control_source_bank_unlock(bank);
        result.protocol = EBUSY; result.financial_result = EBUSY;
        return result;
    }
    wl_evaluation_control_source_context_t context = {
        .identity = &context, .bank = bank, .lane = (unsigned)index,
        .thread_cookie = &wl_evaluation_control_source_thread_cookie
    };
    lane->context = &context;
    rc = WL_SOURCE_EARLY_REARM(bank, (unsigned)index);
    if (rc) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        result.protocol = rc; result.financial_result = rc;
        return result;
    }
    wl_evaluation_control_source_early_begin_t begun =
        wl_evaluation_control_source_bank_begin_checkpoint(lane);
    result.begin_protocol = begun.protocol;
    result.checkpoint_result = begun.checkpoint;
    if (!begun.started) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        result.protocol = begun.protocol;
        result.financial_result = begun.protocol;
        return result;
    }
    result.phase_started = true;
    rc = begun.checkpoint;
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->bootstrap_work);
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->current_work);
    if (!rc) {
        lane->callback_active = true;
        wl_evaluation_control_source_bank_unlock(bank);
        result.first_result = action(payload);
        result.first_called = true;
        if (wl_evaluation_control_source_bank_lock(bank)) abort();
        lane->callback_active = false;
        WL_SOURCE_FINAL_TEST_EVENT(lane,
            WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_ACTION);
        rc = wl_evaluation_control_source_bank_charge(lane, policy->close_work);
    }
    int finished = wl_evaluation_control_source_bank_finish_constructor_phase(
        bank, (unsigned)index, &context);
    result.financial_result = rc ? rc : finished;
    lane->context = NULL;
    result.phase_finished = true;
    wl_evaluation_control_source_bank_unlock(bank);
    return result;
}

wl_evaluation_control_source_early_result_t
wl_evaluation_control_source_bank_run_lexical_pair_early(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    const wl_evaluation_control_source_pair_policy_t *policy,
    wl_evaluation_control_source_action_fn first, void *first_payload,
    wl_evaluation_control_source_action_fn second, void *second_payload)
{
    wl_evaluation_control_source_early_result_t result = {
        .protocol = EINVAL, .financial_result = EINVAL
    };
    if (!bank || bank->identity != bank
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || !policy || !first || !second || first_payload == bank ||
        second_payload == bank
        || !policy->bootstrap_work || !policy->first_work ||
        !policy->second_work || !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->first_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->second_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM) return result;
    /* Each validated term is at most Q. Their first three terms fit 3Q
     * without overflow; subtraction tests the fourth before adding it. */
    uint64_t first_three = policy->bootstrap_work + policy->first_work +
        policy->second_work;
    if (policy->close_work >
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY - first_three)
        return result;
    result.protocol = 0;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) {
        result.protocol = rc; result.financial_result = rc; return result;
    }
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (bank->closing || lane->context || lane->running ||
        lane->callback_active){
        wl_evaluation_control_source_bank_unlock(bank);
        result.protocol = EBUSY; result.financial_result = EBUSY; return result;
    }
    wl_evaluation_control_source_context_t context = {
        .identity = &context, .bank = bank, .lane = (unsigned)index,
        .thread_cookie = &wl_evaluation_control_source_thread_cookie
    };
    lane->context = &context;
    rc = WL_SOURCE_EARLY_REARM(bank, (unsigned)index);
    if (rc) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        result.protocol = rc; result.financial_result = rc;
        return result;
    }
    wl_evaluation_control_source_early_begin_t begun =
        wl_evaluation_control_source_bank_begin_checkpoint(lane);
    result.begin_protocol = begun.protocol;
    result.checkpoint_result = begun.checkpoint;
    if (!begun.started) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        result.protocol = begun.protocol;
        result.financial_result = begun.protocol;
        return result;
    }
    result.phase_started = true;
    rc = begun.checkpoint;
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->bootstrap_work);
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->first_work);
    if (!rc) {
        lane->callback_active = true;
        wl_evaluation_control_source_bank_unlock(bank);
        result.first_result = first(first_payload);
        result.first_called = true;
        if (wl_evaluation_control_source_bank_lock(bank)) abort();
        lane->callback_active = false;
        WL_SOURCE_FINAL_TEST_EVENT(lane,
            WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_FIRST);
        /* This is an actual check on the same still-active saved BANK
         * generation. Neither FINISH nor report/rearm stands in for it. */
        rc = wl_evaluation_control_source_bank_charge(lane, 0);
#ifdef WL_SESSION_TEST_HOOKS
        if (!rc && wl_evaluation_control_test_source_pair_checkpoint)
            wl_evaluation_control_test_source_pair_checkpoint();
#endif
        if (!rc && !result.first_result) {
            rc = wl_evaluation_control_source_bank_charge(lane,
                    policy->second_work);
            if (!rc) {
                lane->callback_active = true;
                wl_evaluation_control_source_bank_unlock(bank);
                result.second_result = second(second_payload);
                result.second_called = true;
                if (wl_evaluation_control_source_bank_lock(bank)) abort();
                lane->callback_active = false;
            }
        }
        if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                    policy->close_work);
    }
    int finished = wl_evaluation_control_source_bank_finish_phase(lane);
    result.financial_result = rc ? rc : finished;
    lane->context = NULL;
    result.phase_finished = true;
    wl_evaluation_control_source_bank_unlock(bank);
    return result;
}

wl_evaluation_control_source_early_result_t
wl_evaluation_control_source_bank_run_lexical_final_early(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    const wl_evaluation_control_source_phase_policy_t *policy,
    const wl_evaluation_control_source_final_geometry_t *geometry,
    wl_evaluation_control_source_action_fn action)
{
    wl_evaluation_control_source_early_result_t result = {
        .protocol = EINVAL, .financial_result = EINVAL
    };
    if (!bank || bank->identity != bank
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || !policy || !geometry || !action
        || !policy->bootstrap_work || !policy->current_work ||
        !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->current_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM) return result;
    result.protocol = 0;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) {
        result.protocol = rc; result.financial_result = rc; return result;
    }
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (bank->closing || lane->context || lane->running ||
        lane->callback_active){
        wl_evaluation_control_source_bank_unlock(bank);
        result.protocol = EBUSY; result.financial_result = EBUSY; return result;
    }
    wl_evaluation_control_source_context_t context = {
        .identity = &context, .bank = bank, .lane = (unsigned)index,
        .thread_cookie = &wl_evaluation_control_source_thread_cookie
    };
    lane->context = &context;
    rc = WL_SOURCE_EARLY_REARM(bank, (unsigned)index);
    if (rc) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        result.protocol = rc; result.financial_result = rc;
        return result;
    }
    wl_evaluation_control_source_early_begin_t begun =
        wl_evaluation_control_source_bank_begin_checkpoint(lane);
    result.begin_protocol = begun.protocol;
    result.checkpoint_result = begun.checkpoint;
    if (!begun.started) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        result.protocol = begun.protocol;
        result.financial_result = begun.protocol;
        return result;
    }
    result.phase_started = true;
    rc = begun.checkpoint;
    WL_SOURCE_FINAL_TEST_EVENT(lane,
        WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_BEGIN);
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->bootstrap_work);
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->current_work);
    if (!rc) {
        WL_SOURCE_FINAL_GEOMETRY(wl_evaluation_control_source_final_geometry_t,
            g, geometry, bank);
        const wl_evaluation_control_source_phase_policy_t paid = *policy;
        const wl_evaluation_control_source_extent_t objects[3] = {g.session,
                                                                  g.payload,
                                                                  g.journal};
        int check = wl_evaluation_control_source_final_geometry(bank, lane,
                objects, 3,
                (wl_evaluation_control_source_extent_t){geometry,
                                                        sizeof(*geometry)},
                (wl_evaluation_control_source_extent_t){policy,
                                                        sizeof(*policy)}, NULL,
                NULL, &rc);
        if (check) result.first_result = check;
        else {
            lane->callback_active = true;
            wl_evaluation_control_source_bank_unlock(bank);
            result.first_result = action((void *)g.payload.base);
            result.first_called = true;
            if (wl_evaluation_control_source_bank_lock(bank)) abort();
            lane->callback_active = false;
            WL_SOURCE_FINAL_TEST_EVENT(lane,
                WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_ACTION);
            rc = wl_evaluation_control_source_bank_charge(lane,
                    paid.close_work);
        }
    }
    int finished =
        wl_evaluation_control_source_bank_finish_constructor_phase(bank,
            (unsigned)index, &context);
    result.financial_result = rc ? rc : finished;
    lane->context = NULL;
    result.phase_finished = true;
    wl_evaluation_control_source_bank_unlock(bank);
    return result;
}

wl_evaluation_control_source_early_result_t
wl_evaluation_control_source_bank_run_lexical_pair_final_early(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    const wl_evaluation_control_source_pair_policy_t *policy,
    const wl_evaluation_control_source_final_pair_geometry_t *geometry,
    wl_evaluation_control_source_action_fn first,
    wl_evaluation_control_source_action_fn second)
{
    wl_evaluation_control_source_early_result_t result = {
        .protocol = EINVAL, .financial_result = EINVAL
    };
    if (!bank || bank->identity != bank
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || !policy || !geometry || !first || !second
        || !policy->bootstrap_work || !policy->first_work
        || !policy->second_work || !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->first_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->second_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM) return result;
    uint64_t first_three = policy->bootstrap_work + policy->first_work +
        policy->second_work;
    if (policy->close_work >
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY - first_three)
        return result;
    result.protocol = 0;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) {
        result.protocol = rc; result.financial_result = rc; return result;
    }
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (bank->closing || lane->context || lane->running ||
        lane->callback_active){
        wl_evaluation_control_source_bank_unlock(bank);
        result.protocol = EBUSY; result.financial_result = EBUSY; return result;
    }
    wl_evaluation_control_source_context_t context = {
        .identity = &context, .bank = bank, .lane = (unsigned)index,
        .thread_cookie = &wl_evaluation_control_source_thread_cookie
    };
    lane->context = &context;
    rc = WL_SOURCE_EARLY_REARM(bank, (unsigned)index);
    if (rc) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        result.protocol = rc; result.financial_result = rc;
        return result;
    }
    wl_evaluation_control_source_early_begin_t begun =
        wl_evaluation_control_source_bank_begin_checkpoint(lane);
    result.begin_protocol = begun.protocol;
    result.checkpoint_result = begun.checkpoint;
    if (!begun.started) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        result.protocol = begun.protocol;
        result.financial_result = begun.protocol;
        return result;
    }
    result.phase_started = true;
    rc = begun.checkpoint;
    WL_SOURCE_FINAL_TEST_EVENT(lane,
        WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_BEGIN);
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->bootstrap_work);
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->first_work);
    if (!rc) {
        WL_SOURCE_FINAL_GEOMETRY(
            wl_evaluation_control_source_final_pair_geometry_t, g, geometry,
            bank);
        const wl_evaluation_control_source_pair_policy_t paid = *policy;
        const wl_evaluation_control_source_extent_t objects[5] = {
            g.session, g.record, g.scope, g.payload, g.journal
        };
        int check = !g.source_control
            || g.source_expectation.owner != g.record.base
            || g.source_expectation.owner_bytes != g.record.bytes
            || g.source_expectation.scope != g.scope.base
            || g.source_expectation.scope_bytes != g.scope.bytes ? EINVAL : 0;
        if (!check) check = wl_evaluation_control_source_final_geometry(bank,
                    lane,
                    objects, 5,
                    (wl_evaluation_control_source_extent_t){geometry,
                                                            sizeof(*geometry)},
                    (wl_evaluation_control_source_extent_t){policy,
                                                            sizeof(*policy)},
                    g.source_control, &g.source_expectation, &rc);
        if (check) result.first_result = check;
        else {
            lane->callback_active = true;
            wl_evaluation_control_source_bank_unlock(bank);
            result.first_result = first((void *)g.scope.base);
            result.first_called = true;
            if (wl_evaluation_control_source_bank_lock(bank)) abort();
            lane->callback_active = false;
            WL_SOURCE_FINAL_TEST_EVENT(lane,
                WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_FIRST);
            /* A may have freed the normal Source. Never follow that saved
             * control again. Only BANK owns this real bridge checkpoint. */
            rc = wl_evaluation_control_source_bank_charge(lane, 0);
#ifdef WL_SESSION_TEST_HOOKS
            if (!rc && wl_evaluation_control_test_source_pair_checkpoint)
                wl_evaluation_control_test_source_pair_checkpoint();
#endif
            if (!rc && !result.first_result) {
                rc = wl_evaluation_control_source_bank_charge(lane,
                        paid.second_work);
                if (!rc) {
                    lane->callback_active = true;
                    wl_evaluation_control_source_bank_unlock(bank);
                    result.second_result = second((void *)g.payload.base);
                    result.second_called = true;
                    if (wl_evaluation_control_source_bank_lock(bank)) abort();
                    lane->callback_active = false;
                    WL_SOURCE_FINAL_TEST_EVENT(lane,
                        WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_SECOND);
                }
            }
            if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                        paid.close_work);
        }
    }
    int finished =
        wl_evaluation_control_source_bank_finish_constructor_phase(bank,
            (unsigned)index, &context);
    result.financial_result = rc ? rc : finished;
    lane->context = NULL;
    result.phase_finished = true;
    wl_evaluation_control_source_bank_unlock(bank);
    return result;
}

int
wl_evaluation_control_source_bank_destroy(
    wl_evaluation_control_source_bank_t **slot)
{
    if (!slot) return EINVAL;
    wl_evaluation_control_source_bank_t *bank = *slot;
    if (!bank) return 0;
    if (bank->identity != bank || bank->owner_slot != slot) return EINVAL;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    for (unsigned i = 0; i < WL_EVALUATION_CONTROL_SOURCE_LANES; ++i) {
        wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[i];
        if (lane->context || lane->running || lane->callback_active
            || wl_evaluation_control_active_attempt_id(lane->control)
            || (lane->id && !lane->report_verified)) {
            wl_evaluation_control_source_bank_unlock(bank);
            return EBUSY;
        }
    }
    /* Candidate source ownership: BOOT256 covers actual incoming/rearm;
     * CURRENT24 covers its separate monetary transition; CLOSE160 covers
     * report/retirement and the fixed admission tail. All source intervals
     * and simultaneous slack are re-audited against this complete literal.
     * Capacity3Q is configuration, not payment. No Session is read here. */
    wl_evaluation_control_source_context_t final_context = {0};
    unsigned index = WL_EVALUATION_CONTROL_SOURCE_CLOSE;
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    final_context.identity = &final_context;
    final_context.bank = bank;
    final_context.lane = index;
    final_context.thread_cookie = &wl_evaluation_control_source_thread_cookie;
    lane->context = &final_context;
    rc = wl_evaluation_control_source_bank_rearm_phase(bank, index);
    if (!rc) rc = wl_evaluation_control_begin(lane->control, lane);
    if (rc) {
        lane->context = NULL;
        wl_evaluation_control_source_bank_unlock(bank);
        return rc;
    }
    lane->id = wl_evaluation_control_active_attempt_id(lane->control);
    if (!lane->id) abort();
    lane->running = true;
    rc = wl_evaluation_control_source_bank_charge(lane,
            WL_EVALUATION_CONTROL_QUANTUM);
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                UINT64_C(24));
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                UINT64_C(160));
    int finished = wl_evaluation_control_source_bank_finish_phase(lane);
    lane->context = NULL;
    if (rc || finished) {
        wl_evaluation_control_source_bank_unlock(bank);
        return rc ? rc : finished;
    }
    bank->closing = true;
    wl_evaluation_control_source_bank_unlock(bank);
    for (unsigned i = 0; i < WL_EVALUATION_CONTROL_SOURCE_LANES; ++i) {
        lane = &bank->lanes[i];
        if (wl_evaluation_control_detach(lane->control, lane)) abort();
        wl_evaluation_control_release(lane->control);
    }
    bank->identity = NULL;
    wl_mutex_destroy(&bank->mutex);
    free(bank);
    *slot = NULL;
    return 0;
}

/* Generation-bound siblings; existing generic transitions remain unchanged. */
/* Separate fully counted entry. Generic finish ABI/semantics are unchanged. */
wl_evaluation_control_attempt_finish_result_t
wl_evaluation_control_finish_attempt(wl_evaluation_control_t *control,
    const void *owner, uint64_t expected_id, int execution_status,
    int cleanup_status)
{
    wl_evaluation_control_attempt_finish_result_t out = {0};
    if (!control || !owner || !expected_id) {
        out.protocol = EINVAL;
        return out;
    }
    wl_mutex_lock(&control->mutex);
    if (control->owner != owner) {
        out.protocol = EINVAL;
        wl_mutex_unlock(&control->mutex);
        return out;
    }
    if (control->attempt_id != expected_id) {
        out.kind = WL_EVALUATION_CONTROL_ATTEMPT_FINISH_SUCCESSOR_MISMATCH;
        wl_mutex_unlock(&control->mutex);
        return out;
    }
    if (!control->active) {
        if (control->completed.attempt_id != expected_id) {
            out.protocol = EINVAL;
        } else {
            out.kind = WL_EVALUATION_CONTROL_ATTEMPT_FINISH_ALREADY_ENDED;
            out.report = control->completed;
            out.report_valid = true;
        }
        wl_mutex_unlock(&control->mutex);
        return out;
    }
    if (wl_evaluation_control_is_stop(execution_status)) {
        if (control->stop == WL_EVALUATION_CONTROL_NONE)
            control->stop = (wl_evaluation_control_stop_t)execution_status;
        execution_status = 0;
    }
    if (execution_status == 0)
        execution_status = control->accounting_error;
    int result = execution_status != 0 ? execution_status
        : cleanup_status != 0 ? cleanup_status : (int)control->stop;
    control->completed = (wl_evaluation_control_report_t){
        .attempt_id = control->attempt_id,
        .limit = control->attempt_limit,
        .consumed = control->consumed,
        .stop_reason = control->stop,
        .execution_status = execution_status,
        .cleanup_status = cleanup_status,
        .result = result,
    };
    control->active = false;
    (void)atomic_exchange_explicit(&control->active_attempt_id, 0,
        memory_order_release);
    out.kind = WL_EVALUATION_CONTROL_ATTEMPT_FINISH_OWN_FINISHED;
    out.report = control->completed;
    out.report_valid = true;
    wl_mutex_unlock(&control->mutex);
    return out;
}

/* Fully counted sibling. The produced ID is copied before mutex release. */
wl_evaluation_control_attempt_begin_result_t
wl_evaluation_control_begin_attempt(wl_evaluation_control_t *control,
    const void *owner)
{
    wl_evaluation_control_attempt_begin_result_t out = {0};
    if (!control || !owner) {
        out.protocol = EINVAL;
        return out;
    }
    wl_mutex_lock(&control->mutex);
    int rc = control->owner != owner ? EINVAL : control->active ? EBUSY : 0;
    if (rc == 0 && control->attempt_id == UINT64_MAX)
        rc = EOVERFLOW;
    if (rc == 0) {
        control->active = true;
        control->attempt_id++;
        control->attempt_limit = control->configured_limit;
        control->consumed = 0;
        control->stop = WL_EVALUATION_CONTROL_NONE;
        control->accounting_error = 0;
        (void)atomic_exchange_explicit(&control->active_attempt_id,
            control->attempt_id, memory_order_release);
        out.attempt_id = control->attempt_id;
    }
    out.protocol = rc;
    wl_mutex_unlock(&control->mutex);
    return out;
}
