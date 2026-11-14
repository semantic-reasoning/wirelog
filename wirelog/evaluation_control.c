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
#include <stddef.h>
#include <stdlib.h>
#ifdef WL_SESSION_TEST_HOOKS
#include <string.h>
#endif

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

/* The private active-generation observation requires an aligned uint64
 * field. MSVC's default C mode has no _Static_assert, so it checks the same
 * layout through the repository's typedef-array form. */
#if defined(_MSC_VER)
typedef char wl_evaluation_control_active_generation_layout_t
    [(sizeof(wl_atomic_u64) == 8 && __alignof(wl_atomic_u64) >= 8
        && offsetof(wl_evaluation_control_t, active_attempt_id) % 8 == 0)
    ? 1 : -1];
#else
_Static_assert(sizeof(wl_atomic_u64) == 8 && _Alignof(wl_atomic_u64) >= 8
    && offsetof(wl_evaluation_control_t, active_attempt_id) % 8 == 0,
    "The private active-generation observation requires an aligned uint64 field");
#endif

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

void
wl_evaluation_control_release_nonfinal(wl_evaluation_control_t *control)
{
    if (!control) abort();
#ifdef _MSC_VER
    uint64_t previous = (uint64_t)_InterlockedExchangeAdd64(
        (volatile __int64 *)&control->references, -1);
#else
    uint64_t previous = atomic_fetch_sub_explicit(&control->references, 1,
            memory_order_release);
#endif
    /* Caller relinquishes its own ref while an independently owned live
     * attachment pins the allocation. Only the returned scalar is read. */
    if (previous < UINT64_C(2)) abort();
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

wl_evaluation_control_completion_generation_t
wl_evaluation_control_completion_generation_observe(
    wl_evaluation_control_t *control,
    uint64_t expected_allocation_identity)
{
    wl_evaluation_control_completion_generation_t out = {.protocol = EINVAL};
    if (!control || !expected_allocation_identity) return out;
    if (wl_mutex_lock(&control->mutex)) {
        out.protocol = EIO;
        return out;
    }
    if (control->identity == expected_allocation_identity) {
        out.active_id = control->active ? control->attempt_id : 0;
        out.completed_id = control->completed.attempt_id;
        out.protocol = out.completed_id ? 0 : ENOENT;
    }
    wl_mutex_unlock(&control->mutex);
    return out;
}

#ifdef WL_SESSION_TEST_HOOKS
void (*wl_evaluation_control_test_finance_birth_claim)(void);
unsigned wl_evaluation_control_test_finance_birth_fail;
uint64_t wl_evaluation_control_test_finance_birth_attempt_id;
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
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_evaluation_control_test_finance_birth_attempt_id) {
        uint64_t seed = wl_evaluation_control_test_finance_birth_attempt_id;
        wl_evaluation_control_test_finance_birth_attempt_id = 0;
        if (wl_evaluation_control_test_seed_attempt_id(control, owner, seed))
            abort();
    }
#endif
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
/* Enforced sealed-callback precondition: no nested BANK of any lane. */
static WL_EVALUATION_CONTROL_SOURCE_THREAD_LOCAL bool
    wl_evaluation_control_locked_metadata_action;
#undef WL_EVALUATION_CONTROL_SOURCE_THREAD_LOCAL

typedef struct {
    wl_evaluation_control_t *control;
    wl_evaluation_control_source_context_t *context;
    uint64_t id;
    uint64_t charged_total;
    bool running;
    bool callback_active;
    bool report_verified;
    bool observation_active;
    wl_evaluation_control_source_financial_state_t retained_state;
} wl_evaluation_control_source_lane_state_t;

struct wl_evaluation_control_source_bank {
    struct wl_evaluation_control_source_bank *identity;
    struct wl_evaluation_control_source_bank **owner_slot;
    wl_mutex_t mutex;
    bool closing;
    wl_evaluation_control_source_lane_state_t
        lanes[WL_EVALUATION_CONTROL_SOURCE_LANES];
    /* OP only. Each BUILD packet initializes its members before publishing
     * their phase; no pending member is read in Q or before its BUILD. */
    wl_evaluation_control_report_t pending_report;
};

static int
wl_evaluation_control_source_bank_lock(
    wl_evaluation_control_source_bank_t *bank)
{
    if (wl_evaluation_control_locked_metadata_action) return EBUSY;
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
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || !control || !lane->context || lane->running || lane->callback_active
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
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running
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
#ifdef WL_SESSION_TEST_HOOKS
bool wl_evaluation_control_test_source_lexical_cancel_after_action;
bool wl_evaluation_control_test_source_lexical_cancel_before_action;
#endif

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
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running
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
#ifdef WL_SESSION_TEST_HOOKS
    if (!rc && wl_evaluation_control_test_source_lexical_cancel_before_action) {
        wl_evaluation_control_test_source_lexical_cancel_before_action = false;
        wl_evaluation_control_request_cancel(lane->control);
    }
#endif
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->current_work);
    if (!rc) {
        lane->callback_active = true;
        wl_evaluation_control_source_bank_unlock(bank);
        result.action_result = action(payload);
        result.action_called = true;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_evaluation_control_test_source_lexical_cancel_after_action) {
            wl_evaluation_control_test_source_lexical_cancel_after_action =
                false;
            wl_evaluation_control_request_cancel(lane->control);
        }
#endif
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

/* Scalar sibling for the public STEP financial-continuation handoff. The
 * same generic FINISH/report verification and lexical BANK lifetime remain. */
int
wl_evaluation_control_source_bank_run_metadata(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_action_fn action, void *payload,
    wl_evaluation_control_source_metadata_outcome_t *outcome)
{
    if (!bank || bank->identity != bank
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || !policy || !action || !outcome || payload == bank
        || !policy->bootstrap_work || !policy->current_work
        || !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->current_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM)
        return EINVAL;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) {
        return rc;
    }
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running
        || lane->callback_active) {
        wl_evaluation_control_source_bank_unlock(bank);
        return EBUSY;
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
        return rc;
    }
    lane->id = wl_evaluation_control_active_attempt_id(lane->control);
    if (!lane->id) abort();
    lane->running = true;
    rc = wl_evaluation_control_source_bank_charge(lane, policy->bootstrap_work);
#ifdef WL_SESSION_TEST_HOOKS
    if (!rc && wl_evaluation_control_test_source_lexical_cancel_before_action) {
        wl_evaluation_control_test_source_lexical_cancel_before_action = false;
        wl_evaluation_control_request_cancel(lane->control);
    }
#endif
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->current_work);
    if (!rc) {
        lane->callback_active = true;
        wl_evaluation_control_locked_metadata_action = true;
        outcome->action_result = action(payload);
        wl_evaluation_control_locked_metadata_action = false;
        outcome->action_called = true;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_evaluation_control_test_source_lexical_cancel_after_action) {
            wl_evaluation_control_test_source_lexical_cancel_after_action =
                false;
            wl_evaluation_control_request_cancel(lane->control);
        }
#endif
        lane->callback_active = false;
        rc = wl_evaluation_control_source_bank_charge(lane, policy->close_work);
    }
    int finished = wl_evaluation_control_source_bank_finish_phase(lane);
    lane->context = NULL;
    wl_evaluation_control_source_bank_unlock(bank);
    return rc ? rc : finished;
}

#ifdef WL_SESSION_TEST_HOOKS
unsigned wl_evaluation_control_test_source_metadata_cancel_at;
unsigned wl_evaluation_control_test_source_observation_cancel_at;

static void
wl_evaluation_control_source_observation_test_cancel(
    wl_evaluation_control_source_lane_state_t *lane, unsigned checkpoint)
{
    if (wl_evaluation_control_test_source_observation_cancel_at == checkpoint) {
        wl_evaluation_control_test_source_observation_cancel_at = 0;
        wl_evaluation_control_request_cancel(lane->control);
    }
}

static void
wl_evaluation_control_source_metadata_test_cancel(
    wl_evaluation_control_source_lane_state_t *lane, unsigned checkpoint)
{
    if (wl_evaluation_control_test_source_metadata_cancel_at == checkpoint) {
        wl_evaluation_control_test_source_metadata_cancel_at = 0;
        wl_evaluation_control_request_cancel(lane->control);
    }
}
#endif

/* The actual uint64 field has the width/alignment assertions in mem_ledger.h.
 * Publishing inactive needs release ordering, not a returned old RMW value.
 * MSVC's exact-width Interlocked exchange supplies at least that ordering. */
static void
wl_evaluation_control_financier_publish_inactive(
    wl_evaluation_control_t *control)
{
    control->active = false;
#ifdef _MSC_VER
    (void)_InterlockedExchange64(
        (volatile __int64 *)&control->active_attempt_id, 0);
#else
    atomic_store_explicit(&control->active_attempt_id, 0, memory_order_release);
#endif
}

/* BANK is locked and its actual lane was selected by the checked issuer.
 * This shares the real controller critical section for owned private rearm,
 * BEGIN, the first locked zero and its refusal FINISH/readback. Ordinary
 * counted source throughout; it is not an exact-nine Native interior. */
static int
wl_evaluation_control_source_metadata_start(
    wl_evaluation_control_source_bank_t *bank, unsigned index,
    wl_evaluation_control_source_metadata_entry_t *entry, bool observe_only)
{
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    wl_evaluation_control_t *control = lane->control;
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || !control || lane->context || lane->running || lane->callback_active
        || (lane->id && !lane->report_verified)) return EBUSY;
    if (wl_mutex_lock(&control->mutex)) return EIO;
#ifdef _MSC_VER
    uint64_t references = (uint64_t)atomic_fetch_add_explicit(
        &control->references, UINT64_C(0), memory_order_acquire);
#else
    uint64_t references = atomic_load_explicit(&control->references,
            memory_order_acquire);
#endif
    if (control->source_bank_class != bank || control->owner != lane
        || references != UINT64_C(2) || control->active
        || wl_evaluation_control_active_attempt_id(control)
        || control->configured_limit !=
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY) {
        if (wl_mutex_unlock(&control->mutex)) abort();
        return EBUSY;
    }
    /* This exact Source class alone may reuse its private id space, after
     * the prior actual report and ref2/quiescence proof. No Original reset. */
    control->attempt_id = 0;
    lane->id = 0; lane->charged_total = 0; lane->report_verified = false;
    control->active = true;
    ++control->attempt_id;
    control->attempt_limit = control->configured_limit;
    control->consumed = 0; control->stop = WL_EVALUATION_CONTROL_NONE;
    control->accounting_error = 0;
    (void)atomic_exchange_explicit(&control->active_attempt_id,
        control->attempt_id, memory_order_release);
    lane->id = control->attempt_id; lane->running = true;
    entry->begun = true;
#ifdef WL_SESSION_TEST_HOOKS
    wl_evaluation_control_source_metadata_test_cancel(lane, 1);
#endif
    /* Same actual locked charge as charge_attempt after the exact generation
     * was just published under this still-held mutex. Cancellation is sticky. */
    int rc = wl_evaluation_control_charge_locked(control, 0);
    entry->checkpoint_result = rc;
    if (rc || observe_only) {
        int execution = control->accounting_error;
        int result = execution ? execution : (int)control->stop;
        control->completed = (wl_evaluation_control_report_t){
            .attempt_id = control->attempt_id, .limit = control->attempt_limit,
            .consumed = control->consumed, .stop_reason = control->stop,
            .execution_status = execution, .cleanup_status = 0, .result = result
        };
        wl_evaluation_control_financier_publish_inactive(control);
        const wl_evaluation_control_report_t *report = &control->completed;
        if (report->attempt_id != lane->id
            || report->limit != WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
            || report->consumed != lane->charged_total
            || report->stop_reason != rc || report->execution_status
            || report->cleanup_status || report->result != rc
            || control->active ||
            wl_evaluation_control_active_attempt_id(control))
            abort();
        lane->report_verified = true; lane->running = false;
        entry->settled = true;
    }
    if (wl_mutex_unlock(&control->mutex)) abort();
    return rc;
}

/* Only a live issued BANK is accepted here; no arbitrary controller input.
 * All non-atomic state of these private controllers is serialized by BANK:
 * no API loans a callable control/owner/id pair, unlocked ordinary callbacks
 * retain running/context/callback exclusion, and unlocked cancellation touches
 * only the atomic flag. Creation is unpublished; teardown first marks closing.
 * Generic controller/BANK methods retain their existing mutexes/readbacks. */
int
wl_evaluation_control_source_metadata_enter_issued(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_context_t *context)
{
    if (wl_evaluation_control_locked_metadata_action) return -EBUSY;
    if (wl_mutex_lock(&bank->mutex)) return -EIO;
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[WL_EVALUATION_CONTROL_SOURCE_OPERATION];
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || !lane->control || lane->context || lane->running
        || lane->callback_active || (lane->id && !lane->report_verified)) {
        wl_evaluation_control_source_bank_unlock(bank); return -EBUSY;
    }
    wl_evaluation_control_t *control = lane->control;
#ifdef _MSC_VER
    uint64_t references = (uint64_t)_InterlockedCompareExchange64(
        (volatile __int64 *)&control->references, 0, 0);
#else
    uint64_t references = atomic_load_explicit(&control->references,
            memory_order_acquire);
#endif
    if (control->source_bank_class != bank || control->owner != lane
        || references != UINT64_C(2) || control->active
        || wl_evaluation_control_active_attempt_id(control)
        || control->configured_limit !=
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY) {
        wl_evaluation_control_source_bank_unlock(bank); return -EBUSY;
    }
    /* Actual proved Source-only private rearm/BEGIN, not an Original reset. */
    control->attempt_id = 1; lane->id = 1;
    lane->charged_total = 0; lane->report_verified = false;
    control->active = true; control->attempt_limit =
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY;
    control->consumed = 0; control->stop = WL_EVALUATION_CONTROL_NONE;
    control->accounting_error = 0;
#ifdef _MSC_VER
    (void)_InterlockedExchange64(
        (volatile __int64 *)&control->active_attempt_id, 1);
#else
    atomic_store_explicit(&control->active_attempt_id, 1, memory_order_release);
#endif
    lane->running = true;
#ifdef WL_SESSION_TEST_HOOKS
    wl_evaluation_control_source_metadata_test_cancel(lane, 1);
#endif
    /* The exclusive fresh stores prove active/id/limit/count/accounting and
     * zero price. This actual RMW is the remaining charge0 observation. */
#ifdef _MSC_VER
    uint64_t cancelled = (uint64_t)_InterlockedExchangeAdd64(
        (volatile __int64 *)&control->cancelled, 0);
#else
    uint64_t cancelled = atomic_fetch_add_explicit(&control->cancelled, 0,
            memory_order_relaxed);
#endif
    if (cancelled) {
        control->stop = WL_EVALUATION_CONTROL_CANCELLED;
        /* Every field is the real just-initialized/observed state, under
         * BANK exclusion with no intervening callback or non-atomic writer. */
        control->completed = (wl_evaluation_control_report_t){
            .attempt_id = 1,
            .limit = WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY,
            .consumed = 0, .stop_reason = WL_EVALUATION_CONTROL_CANCELLED,
            .execution_status = 0, .cleanup_status = 0,
            .result = WL_EVALUATION_CONTROL_CANCELLED
        };
        control->active = false;
#ifdef _MSC_VER
        (void)_InterlockedExchange64(
            (volatile __int64 *)&control->active_attempt_id, 0);
#else
        atomic_store_explicit(&control->active_attempt_id, 0,
            memory_order_release);
#endif
        lane->report_verified = true; lane->running = false;
        if (wl_mutex_unlock(&bank->mutex)) abort();
        return WL_EVALUATION_CONTROL_SOURCE_METADATA_INITIAL_CANCELLED;
    }
    context->identity = context; context->bank = bank;
    context->lane = WL_EVALUATION_CONTROL_SOURCE_OPERATION;
    context->thread_cookie = &wl_evaluation_control_source_thread_cookie;
    lane->context = context;
    wl_evaluation_control_source_bank_unlock(bank);
    return WL_EVALUATION_CONTROL_SOURCE_METADATA_OPEN;
}

/* BANK alone serializes this unloaned fixed family. Generic Normal and
 * Original APIs retain their independent mutex and accounting contracts. */
static int
wl_evaluation_control_source_observation_zero_locked(
    wl_evaluation_control_source_lane_state_t *lane)
{
    wl_evaluation_control_t *control = lane->control;
    if (control->stop != WL_EVALUATION_CONTROL_NONE) return (int)control->stop;
#ifdef _MSC_VER
    uint64_t cancelled = (uint64_t)_InterlockedExchangeAdd64(
        (volatile __int64 *)&control->cancelled, 0);
#else
    uint64_t cancelled = atomic_fetch_add_explicit(&control->cancelled, 0,
            memory_order_relaxed);
#endif
    if (cancelled) control->stop = WL_EVALUATION_CONTROL_CANCELLED;
    return (int)control->stop;
}

static void
wl_evaluation_control_source_observation_settle_locked(
    wl_evaluation_control_source_lane_state_t *lane,
    wl_evaluation_control_source_context_t *context)
{
    wl_evaluation_control_t *control = lane->control;
    control->completed = (wl_evaluation_control_report_t){
        .attempt_id = 1, .limit = WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY,
        .consumed = lane->charged_total, .stop_reason = control->stop,
        .execution_status = 0, .cleanup_status = 0, .result = (int)control->stop
    };
    wl_evaluation_control_financier_publish_inactive(control);
    lane->report_verified = true; lane->running = false;
    lane->observation_active = false; lane->context = NULL;
    if (context) context->identity = NULL;
}

/* Success holds BANK exclusion. The birth slot is part of the proof, so an
* alias containing the same BANK pointer cannot settle the stable owner. */
static int
wl_evaluation_control_source_retained_lock(
    wl_evaluation_control_source_bank_t **slot,
    wl_evaluation_control_source_lane_state_t **out)
{
    if (!slot || !*slot || (*slot)->identity != *slot
        || (*slot)->owner_slot != slot) return EINVAL;
    wl_evaluation_control_source_bank_t *bank = *slot;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[WL_EVALUATION_CONTROL_SOURCE_OPERATION];
    wl_evaluation_control_t *control = lane->control;
    if (bank->closing || !control || control->source_bank_class != bank
        || control->owner != lane
        || atomic_load_explicit(&control->references, memory_order_acquire) != 2
        || control->configured_limit !=
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
        || lane->context || lane->callback_active || lane->observation_active) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    *out = lane;
    return 0;
}

static bool
wl_evaluation_control_source_retained_active(
    const wl_evaluation_control_source_lane_state_t *lane)
{
    const wl_evaluation_control_t *control = lane->control;
    return lane->running && !lane->report_verified && lane->id == 1
           && control->active && control->attempt_id == lane->id
           && wl_evaluation_control_active_attempt_id(lane->control) == lane->id
           && control->attempt_limit ==
           WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
           && control->consumed == lane->charged_total
           && control->consumed <= control->attempt_limit
           && !control->accounting_error;
}

int
wl_evaluation_control_source_standby_prepare_owned(
    wl_evaluation_control_source_bank_t **slot,
    wl_evaluation_control_source_retained_output_t *out)
{
    if (!out) return EINVAL;
    wl_evaluation_control_source_lane_state_t *lane;
    int rc = wl_evaluation_control_source_retained_lock(slot, &lane);
    if (rc) return rc;
    wl_evaluation_control_source_bank_t *bank = *slot;
    wl_evaluation_control_t *control = lane->control;
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || lane->running || control->active
        || wl_evaluation_control_active_attempt_id(control)
        || (lane->id && !lane->report_verified)) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    control->attempt_id = 1; lane->id = 1;
    lane->charged_total = 0; lane->report_verified = false;
    control->active = true;
    control->attempt_limit = WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY;
    control->consumed = 0; control->stop = WL_EVALUATION_CONTROL_NONE;
    control->accounting_error = 0;
    atomic_store_explicit(&control->active_attempt_id, 1, memory_order_release);
    lane->running = true;
    lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_STANDBY;
#ifdef WL_SESSION_TEST_HOOKS
    wl_evaluation_control_source_observation_test_cancel(lane, 1);
#endif
    rc = wl_evaluation_control_source_observation_zero_locked(lane);
    if (rc) lane->retained_state =
            WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY;
    out->financial_result = rc;
    out->state = lane->retained_state;
    out->opened = !rc;
    out->report_verified = lane->report_verified;
    wl_evaluation_control_source_bank_unlock(bank);
    return 0;
}

int
wl_evaluation_control_source_metadata_enter_retained_owned(
    wl_evaluation_control_source_bank_t **slot,
    wl_evaluation_control_source_context_t *context,
    wl_evaluation_control_source_retained_output_t *out)
{
    if (!out || !context || context->identity || context->bank
        || context->lane || context->thread_cookie) return EINVAL;
    wl_evaluation_control_source_lane_state_t *lane;
    int rc = wl_evaluation_control_source_retained_lock(slot, &lane);
    if (rc) return rc;
    wl_evaluation_control_source_bank_t *bank = *slot;
    wl_evaluation_control_t *control = lane->control;
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || lane->running || control->active
        || wl_evaluation_control_active_attempt_id(control)
        || (lane->id && !lane->report_verified)) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    control->attempt_id = 1; lane->id = 1;
    lane->charged_total = 0; lane->report_verified = false;
    control->active = true;
    control->attempt_limit = WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY;
    control->consumed = 0; control->stop = WL_EVALUATION_CONTROL_NONE;
    control->accounting_error = 0;
    atomic_store_explicit(&control->active_attempt_id, 1, memory_order_release);
    lane->running = true;
    lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_METADATA;
#ifdef WL_SESSION_TEST_HOOKS
    wl_evaluation_control_source_metadata_test_cancel(lane, 1);
#endif
    rc = wl_evaluation_control_source_observation_zero_locked(lane);
    if (rc) lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY;
    else {
        context->identity = context; context->bank = bank;
        context->lane = WL_EVALUATION_CONTROL_SOURCE_OPERATION;
        context->thread_cookie = &wl_evaluation_control_source_thread_cookie;
        lane->context = context;
    }
    out->financial_result = rc;
    out->state = lane->retained_state;
    out->opened = !rc;
    out->report_verified = false;
    wl_evaluation_control_source_bank_unlock(bank);
    return 0;
}

/* Continue only an actual retained metadata activation. The ordinary active
 * proof admits its real metadata extent, not just observation's fixed ten. */
static bool
wl_evaluation_control_source_retained_metadata_valid(
    const wl_evaluation_control_source_bank_t *bank,
    const wl_evaluation_control_source_lane_state_t *lane)
{
    const wl_evaluation_control_t *control = lane->control;
    return bank->identity == bank && !bank->closing && bank->owner_slot
           && *bank->owner_slot == bank
           && control && control->source_bank_class == bank
           && control->owner == lane
           && atomic_load_explicit(&control->references,
               memory_order_acquire) == 2
           && control->configured_limit ==
           WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
           && wl_evaluation_control_source_retained_active(lane);
}

/* Only zero observation projects the closed STANDBY invariant. Every writer
 * into that variant proves the full tuple; legacy writers require Q before
 * mutation. Positive attach, FRAME and FINISH/ACK still check the full tuple. */
int
wl_evaluation_control_source_standby_observe_owned(
    wl_evaluation_control_source_bank_t **slot,
    wl_evaluation_control_source_retained_output_t *out)
{
    if (!out || !slot) return EINVAL;
    wl_evaluation_control_source_bank_t *bank = *slot;
    if (!bank || bank->identity != bank ||
        bank->owner_slot != slot) return EINVAL;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[WL_EVALUATION_CONTROL_SOURCE_OPERATION];
    wl_evaluation_control_t *control = lane->control;
    if (bank->closing || !control || control->source_bank_class != bank
        || control->owner != lane
        || atomic_load_explicit(&control->references,
        memory_order_acquire) != UINT64_C(2)) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    wl_evaluation_control_source_financial_state_t state = lane->retained_state;
    if (state != WL_EVALUATION_CONTROL_SOURCE_STANDBY
        && state != WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY) {
        /* Preserve the legacy busy-before-variant refusal, including a real
        * FRAME context. A mode tag cannot hide an occupied legacy owner. */
        rc = lane->context || lane->callback_active || lane->observation_active
            || control->configured_limit !=
            WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
            ? EBUSY : EINVAL;
        wl_evaluation_control_source_bank_unlock(bank); return rc;
    }
    if (lane->id != 1) {
        wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
    }
#ifdef _MSC_VER
    uint64_t active = (uint64_t)_InterlockedCompareExchange64(
        (volatile __int64 *)&control->active_attempt_id, 0, 0);
#else
    uint64_t active = atomic_load_explicit(&control->active_attempt_id,
            memory_order_acquire);
#endif
    if (active != lane->id) {
        wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
    }
#ifdef WL_SESSION_TEST_HOOKS
    wl_evaluation_control_source_observation_test_cancel(lane, 4);
#endif
    rc = wl_evaluation_control_source_observation_zero_locked(lane);
    if (rc) {
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY;
        out->financial_result = rc;
        out->state = WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY;
        out->opened = false;
    } else {
        out->financial_result = 0;
        out->state = state;
        out->opened = true;
    }
    /* Both real active variants have no FINISH report. Report ACK is proved
     * only in full handoff; no negative result stands in for that receipt. */
    out->report_verified = false;
    wl_evaluation_control_source_bank_unlock(bank);
    return 0;
}

/* STANDBY is the closed, uncharged variant: only full checked prepare enters
 * it. Legacy and fresh metadata writers require Q, ordinary attach leaves it
 * for FRAME, and this zero-only conversion leaves it for METADATA. No writer
 * may alter its extent/accounting/report or automatic-context facts in place.
 * Actual class/ref/owner/saved/current checks remain mandatory here; the full
 * positive metadata continuation independently authenticates its live tuple. */
int
wl_evaluation_control_source_metadata_attach_retained_owned(
    wl_evaluation_control_source_bank_t **slot,
    wl_evaluation_control_source_context_t *context,
    wl_evaluation_control_source_retained_output_t *out)
{
    if (!out || !context || context->identity || context->bank
        || context->lane || context->thread_cookie || !slot) return EINVAL;
    wl_evaluation_control_source_bank_t *bank = *slot;
    if (!bank || bank->identity != bank ||
        bank->owner_slot != slot) return EINVAL;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[WL_EVALUATION_CONTROL_SOURCE_OPERATION];
    wl_evaluation_control_t *control = lane->control;
    if (bank->closing || !control || control->source_bank_class != bank
        || control->owner != lane
        || atomic_load_explicit(&control->references,
        memory_order_acquire) != 2) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_STANDBY) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    if (lane->id != 1) {
        wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
    }
#ifdef _MSC_VER
    uint64_t active = (uint64_t)_InterlockedCompareExchange64(
        (volatile __int64 *)&control->active_attempt_id, 0, 0);
#else
    uint64_t active = atomic_load_explicit(&control->active_attempt_id,
            memory_order_acquire);
#endif
    if (active != lane->id) {
        wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
    }
#ifdef WL_SESSION_TEST_HOOKS
    wl_evaluation_control_source_metadata_test_cancel(lane, 1);
#endif
    rc = wl_evaluation_control_source_observation_zero_locked(lane);
    if (rc) lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY;
    else {
        context->identity = context; context->bank = bank;
        context->lane = WL_EVALUATION_CONTROL_SOURCE_OPERATION;
        context->thread_cookie = &wl_evaluation_control_source_thread_cookie;
        lane->context = context;
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_METADATA;
    }
    out->financial_result = rc;
    out->state = lane->retained_state;
    out->opened = !rc;
    out->report_verified = false;
    wl_evaluation_control_source_bank_unlock(bank);
    return 0;
}

#ifdef WL_SESSION_TEST_HOOKS
static int
wl_evaluation_control_test_source_retained_rejection(
    wl_evaluation_control_source_bank_t **slot, unsigned fault,
    wl_evaluation_control_source_retained_output_t *out, unsigned target)
{
    if (!out || fault < 1 || fault > 6) return EINVAL;
    wl_evaluation_control_source_lane_state_t *lane;
    int rc = wl_evaluation_control_source_retained_lock(slot, &lane);
    if (rc) return rc;
    wl_evaluation_control_source_bank_t *bank = *slot;
    bool allowed = target == 2 ?
        lane->retained_state >= WL_EVALUATION_CONTROL_SOURCE_PF_FROZEN
        && lane->retained_state <= WL_EVALUATION_CONTROL_SOURCE_PF_VERIFIED :
        (lane->retained_state == WL_EVALUATION_CONTROL_SOURCE_STANDBY
        || lane->retained_state == WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY)
        && wl_evaluation_control_source_retained_active(lane);
    if (!allowed || target > 2) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    wl_evaluation_control_t *control = lane->control;
    const wl_evaluation_control_source_bank_t *original_class =
        control->source_bank_class;
    const void *owner = control->owner;
    uint64_t id = lane->id, consumed = control->consumed, active = 0;
    wl_evaluation_control_stop_t stop = control->stop;
    bool verified = lane->report_verified;
    wl_evaluation_control_source_financial_state_t state = lane->retained_state;
    int other_owner = 0;
    if (fault == 1) control->source_bank_class = NULL;
    if (fault == 2) control->owner = &other_owner;
    if (fault == 3 && wl_evaluation_control_retain(control)) abort();
    if (fault == 4) lane->id = 2;
    if (fault ==
        5) active = atomic_exchange_explicit(&control->active_attempt_id,
                UINT64_C(2), memory_order_release);
    if (fault == 6) lane->control = NULL;
    wl_evaluation_control_source_bank_unlock(bank);
    /* Actual API locks BANK; only this test owns the temporary bad tuple. */
    if (target == 2)
        rc = wl_evaluation_control_source_financial_progress_owned(slot, out);
    else if (target == 1) {
        wl_evaluation_control_source_context_t context = {0};
        rc = wl_evaluation_control_source_metadata_attach_retained_owned(slot,
                &context, out);
        if (context.identity || context.bank || context.thread_cookie
            || context.lane) abort();
    } else rc = wl_evaluation_control_source_standby_observe_owned(slot, out);
    if (wl_evaluation_control_source_bank_lock(bank)) abort();
    control->source_bank_class = original_class; control->owner = owner;
    if (fault == 3) wl_evaluation_control_release(control);
    lane->id = id; lane->control = control;
    if (fault == 5) (void)atomic_exchange_explicit(&control->active_attempt_id,
            active, memory_order_release);
    if (control->consumed != consumed || control->stop != stop
        || lane->retained_state != state || lane->context ||
        lane->report_verified != verified
        || lane->observation_active || lane->callback_active || !lane->running)
        abort();
    wl_evaluation_control_source_bank_unlock(bank);
    return rc;
}

int
wl_evaluation_control_test_source_standby_rejection(
    wl_evaluation_control_source_bank_t **slot, unsigned fault,
    wl_evaluation_control_source_retained_output_t *out)
{
    return wl_evaluation_control_test_source_retained_rejection(slot, fault,
               out, 0);
}

int
wl_evaluation_control_test_source_metadata_attach_rejection(
    wl_evaluation_control_source_bank_t **slot, unsigned fault,
    wl_evaluation_control_source_retained_output_t *out)
{
    return wl_evaluation_control_test_source_retained_rejection(slot, fault,
               out, 1);
}

int
wl_evaluation_control_test_source_financial_rejection(
    wl_evaluation_control_source_bank_t **slot, unsigned fault,
    wl_evaluation_control_source_retained_output_t *out)
{
    return wl_evaluation_control_test_source_retained_rejection(slot, fault,
               out, 2);
}
#endif

int
wl_evaluation_control_source_standby_attach_owned(
    wl_evaluation_control_source_bank_t **slot,
    wl_evaluation_control_source_context_t *context,
    wl_evaluation_control_source_retained_output_t *out)
{
    if (!out) return EINVAL;
    if (!context || context->identity || context->bank || context->lane
        || context->thread_cookie) return EINVAL;
    wl_evaluation_control_source_lane_state_t *lane;
    int rc = wl_evaluation_control_source_retained_lock(slot, &lane);
    if (rc) return rc;
    wl_evaluation_control_source_bank_t *bank = *slot;
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_STANDBY
        || !wl_evaluation_control_source_retained_active(lane)
        || lane->charged_total) {
        wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
    } else {
#ifdef WL_SESSION_TEST_HOOKS
        wl_evaluation_control_source_observation_test_cancel(lane, 2);
#endif
        rc = wl_evaluation_control_source_observation_zero_locked(lane);
        if (!rc) {
            lane->control->consumed =
                WL_EVALUATION_CONTROL_SOURCE_OBSERVATION_WORK;
            lane->charged_total = WL_EVALUATION_CONTROL_SOURCE_OBSERVATION_WORK;
#ifdef WL_SESSION_TEST_HOOKS
            wl_evaluation_control_source_observation_test_cancel(lane, 3);
#endif
            rc = wl_evaluation_control_source_observation_zero_locked(lane);
        }
        if (rc) {
            lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY;
        } else {
            context->bank = bank;
            context->lane = WL_EVALUATION_CONTROL_SOURCE_OPERATION;
            context->thread_cookie =
                &wl_evaluation_control_source_thread_cookie;
            context->identity = context;
            lane->context = context; lane->observation_active = true;
            lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_FRAME;
        }
        out->financial_result = rc;
        out->state = lane->retained_state;
        out->opened = !rc;
        out->report_verified = lane->report_verified;
    }
    wl_evaluation_control_source_bank_unlock(bank);
    return 0;
}

int
wl_evaluation_control_source_standby_handoff_owned(
    wl_evaluation_control_source_bank_t **slot,
    wl_evaluation_control_source_retained_output_t *out)
{
    if (!out) return EINVAL;
    wl_evaluation_control_source_lane_state_t *lane;
    int rc = wl_evaluation_control_source_retained_lock(slot, &lane);
    if (rc) return rc;
    wl_evaluation_control_source_bank_t *bank = *slot;
    wl_evaluation_control_t *control = lane->control;
    int financial;
    if (lane->retained_state >= WL_EVALUATION_CONTROL_SOURCE_PF_FROZEN
        && lane->retained_state <= WL_EVALUATION_CONTROL_SOURCE_PF_VERIFIED) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    if (lane->retained_state == WL_EVALUATION_CONTROL_SOURCE_FINISH_PENDING) {
        const wl_evaluation_control_report_t *r = &control->completed;
        if (!lane->running || !lane->report_verified || control->active
            || wl_evaluation_control_active_attempt_id(control)
            || r->attempt_id != lane->id || r->limit != control->attempt_limit
            || r->consumed != lane->charged_total ||
            r->stop_reason != control->stop
            || r->execution_status || r->cleanup_status ||
            r->result != (int)control->stop) {
            wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
        }
        financial = r->result;
        lane->running = false;
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_QUIESCENT;
    } else if ((lane->retained_state == WL_EVALUATION_CONTROL_SOURCE_STANDBY
        || lane->retained_state == WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY)
        && wl_evaluation_control_source_retained_active(lane)) {
        financial = wl_evaluation_control_source_observation_zero_locked(lane);
        wl_evaluation_control_source_observation_settle_locked(lane, NULL);
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_FINISH_PENDING;
        /* Keep legacy issuers excluded until this report is really ACKed. */
        lane->running = true;
    } else {
        wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
    }
    out->financial_result = financial;
    out->state = lane->retained_state;
    out->opened = false;
    out->report_verified = lane->report_verified;
    wl_evaluation_control_source_bank_unlock(bank);
    return 0;
}

/* Each PF publisher closes its complete phase invariant under BANK. Legacy
 * writers and readers require Q; retained FRAME/METADATA and old handoff cannot
 * enter a claimed PF phase. Only atomic cancellation may change after freeze;
 * it does not mutate the frozen stop or the unloaned controller/report facts. */
int
wl_evaluation_control_source_financial_progress_owned(
    wl_evaluation_control_source_bank_t **slot,
    wl_evaluation_control_source_retained_output_t *out)
{
    if (!out || !slot) return EINVAL;
    wl_evaluation_control_source_bank_t *bank = *slot;
    if (!bank || bank->identity != bank ||
        bank->owner_slot != slot) return EINVAL;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[WL_EVALUATION_CONTROL_SOURCE_OPERATION];
    wl_evaluation_control_t *control = lane->control;
    if (bank->closing || !control || control->source_bank_class != bank
        || control->owner != lane
        || atomic_load_explicit(&control->references,
        memory_order_acquire) != 2) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    wl_evaluation_control_source_financial_state_t state = lane->retained_state;
    bool active;
    switch (state) {
    case WL_EVALUATION_CONTROL_SOURCE_STANDBY:
    case WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY:
    case WL_EVALUATION_CONTROL_SOURCE_PF_FROZEN:
    case WL_EVALUATION_CONTROL_SOURCE_PF_HEADER_BUILT:
    case WL_EVALUATION_CONTROL_SOURCE_PF_BODY_BUILT:
        active = true; break;
    case WL_EVALUATION_CONTROL_SOURCE_FINISH_PENDING:
    case WL_EVALUATION_CONTROL_SOURCE_PF_PUBLISHED:
    case WL_EVALUATION_CONTROL_SOURCE_PF_HEADER_CHECKED:
    case WL_EVALUATION_CONTROL_SOURCE_PF_TOTALS_CHECKED:
    case WL_EVALUATION_CONTROL_SOURCE_PF_VERIFIED:
        active = false; break;
    default:
        wl_evaluation_control_source_bank_unlock(bank);
        return state == WL_EVALUATION_CONTROL_SOURCE_QUIESCENT ? EINVAL : EBUSY;
    }
    if (lane->id != 1) {
        wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
    }
#ifdef _MSC_VER
    uint64_t current = (uint64_t)_InterlockedCompareExchange64(
        (volatile __int64 *)&control->active_attempt_id, 0, 0);
#else
    uint64_t current = atomic_load_explicit(&control->active_attempt_id,
            memory_order_acquire);
#endif
    if (current != (active ? lane->id : 0)) {
        wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
    }
    wl_evaluation_control_report_t *pending = &bank->pending_report;
    wl_evaluation_control_report_t *report = &control->completed;
    switch (state) {
    case WL_EVALUATION_CONTROL_SOURCE_STANDBY:
        (void)wl_evaluation_control_source_observation_zero_locked(lane);
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_PF_FROZEN;
        break;
    case WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY:
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_PF_FROZEN;
        break;
    case WL_EVALUATION_CONTROL_SOURCE_PF_FROZEN:
        pending->attempt_id = control->attempt_id;
        pending->limit = control->attempt_limit;
        pending->consumed = control->consumed;
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_PF_HEADER_BUILT;
        break;
    case WL_EVALUATION_CONTROL_SOURCE_PF_HEADER_BUILT:
        pending->stop_reason = control->stop;
        pending->execution_status = 0;
        pending->cleanup_status = 0;
        pending->result = (int)control->stop;
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_PF_BODY_BUILT;
        break;
    case WL_EVALUATION_CONTROL_SOURCE_PF_BODY_BUILT:
        report->attempt_id = pending->attempt_id;
        report->limit = pending->limit;
        report->consumed = pending->consumed;
        report->stop_reason = pending->stop_reason;
        report->execution_status = pending->execution_status;
        report->cleanup_status = pending->cleanup_status;
        report->result = pending->result;
        wl_evaluation_control_financier_publish_inactive(control);
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_PF_PUBLISHED;
        break;
    case WL_EVALUATION_CONTROL_SOURCE_PF_PUBLISHED:
        if (report->attempt_id != lane->id ||
            report->limit != control->attempt_limit) {
            wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
        }
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_PF_HEADER_CHECKED;
        break;
    case WL_EVALUATION_CONTROL_SOURCE_PF_HEADER_CHECKED:
        if (report->consumed != lane->charged_total ||
            report->stop_reason != control->stop) {
            wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
        }
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_PF_TOTALS_CHECKED;
        break;
    case WL_EVALUATION_CONTROL_SOURCE_PF_TOTALS_CHECKED:
        if (report->execution_status || report->cleanup_status
            || report->result != (int)control->stop) {
            wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
        }
        lane->report_verified = true;
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_PF_VERIFIED;
        break;
    case WL_EVALUATION_CONTROL_SOURCE_FINISH_PENDING:
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_PF_VERIFIED;
        break;
    case WL_EVALUATION_CONTROL_SOURCE_PF_VERIFIED:
        lane->running = false;
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_QUIESCENT;
        break;
    default: abort(); /* The locked phase selector above proved this row. */
    }
    out->financial_result = (int)control->stop;
    out->state = lane->retained_state;
    out->opened = false;
    out->report_verified = lane->report_verified;
    wl_evaluation_control_source_bank_unlock(bank);
    return 0;
}

#ifdef WL_SESSION_TEST_HOOKS
int
wl_evaluation_control_test_source_financial_view(
    wl_evaluation_control_source_bank_t **slot,
    wl_evaluation_control_source_financial_view_t *out)
{
    if (!out || !slot || !*slot || (*slot)->identity != *slot
        || (*slot)->owner_slot != slot) return EINVAL;
    wl_evaluation_control_source_bank_t *bank = *slot;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[WL_EVALUATION_CONTROL_SOURCE_OPERATION];
    if (!lane->control || lane->context || lane->callback_active
        || lane->observation_active || bank->closing) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    out->state = lane->retained_state;
    out->active_id = wl_evaluation_control_active_attempt_id(lane->control);
    out->charged = lane->charged_total;
    out->bank_bytes = sizeof(*bank);
    out->verified = lane->report_verified;
    out->history = lane->control->completed;
    wl_evaluation_control_source_bank_unlock(bank);
    return 0;
}

int
wl_evaluation_control_test_source_financial_cancel(
    wl_evaluation_control_source_bank_t **slot)
{
    if (!slot || !*slot || (*slot)->identity != *slot
        || (*slot)->owner_slot != slot) return EINVAL;
    wl_evaluation_control_source_bank_t *bank = *slot;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[WL_EVALUATION_CONTROL_SOURCE_OPERATION];
    if (!lane->control || lane->control->source_bank_class != bank
        || bank->closing) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    wl_evaluation_control_request_cancel(lane->control);
    wl_evaluation_control_source_bank_unlock(bank);
    return 0;
}

/* Only corrupt the field this exact readback packet has not yet checked.
 * Earlier closed predicates and all runtime identity facts stay intact. */
int
wl_evaluation_control_test_source_financial_readback_rejection(
    wl_evaluation_control_source_bank_t **slot, unsigned fault,
    wl_evaluation_control_source_retained_output_t *out)
{
    if (!out || fault < 1 || fault > 7) return EINVAL;
    wl_evaluation_control_source_lane_state_t *lane;
    int rc = wl_evaluation_control_source_retained_lock(slot, &lane);
    if (rc) return rc;
    wl_evaluation_control_source_bank_t *bank = *slot;
    wl_evaluation_control_source_financial_state_t state = lane->retained_state;
    if (state != (fault <= 2 ? WL_EVALUATION_CONTROL_SOURCE_PF_PUBLISHED :
        fault <= 4 ? WL_EVALUATION_CONTROL_SOURCE_PF_HEADER_CHECKED :
        WL_EVALUATION_CONTROL_SOURCE_PF_TOTALS_CHECKED)) {
        wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
    }
    wl_evaluation_control_report_t *r = &lane->control->completed;
    uint64_t saved_count = 0;
    int saved_status = 0;
    wl_evaluation_control_stop_t saved_stop = r->stop_reason;
    switch (fault) {
    case 1: saved_count = r->attempt_id; r->attempt_id = saved_count + 1; break;
    case 2: saved_count = r->limit; r->limit = saved_count + 1; break;
    case 3: saved_count = r->consumed; r->consumed = saved_count + 1; break;
    case 4: r->stop_reason = saved_stop == WL_EVALUATION_CONTROL_NONE ?
            WL_EVALUATION_CONTROL_CANCELLED : WL_EVALUATION_CONTROL_NONE; break;
    case 5: saved_status = r->execution_status; r->execution_status = EINVAL;
        break;
    case 6: saved_status = r->cleanup_status; r->cleanup_status = EINVAL; break;
    case 7: saved_status = r->result; r->result = EINVAL; break;
    default: abort();
    }
    wl_evaluation_control_source_bank_unlock(bank);
    rc = wl_evaluation_control_source_financial_progress_owned(slot, out);
    if (wl_evaluation_control_source_bank_lock(bank)) abort();
    if (rc != EINVAL || lane->retained_state != state || lane->report_verified
        || wl_evaluation_control_active_attempt_id(lane->control)) abort();
    switch (fault) {
    case 1: r->attempt_id = saved_count; break;
    case 2: r->limit = saved_count; break;
    case 3: r->consumed = saved_count; break;
    case 4: r->stop_reason = saved_stop; break;
    case 5: r->execution_status = saved_status; break;
    case 6: r->cleanup_status = saved_status; break;
    case 7: r->result = saved_status; break;
    default: abort();
    }
    wl_evaluation_control_source_bank_unlock(bank);
    return rc;
}
#endif

int
wl_evaluation_control_source_observation_open(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_context_t *context)
{
    if (wl_evaluation_control_locked_metadata_action) return -EBUSY;
    if (wl_mutex_lock(&bank->mutex)) return -EIO;
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[WL_EVALUATION_CONTROL_SOURCE_OPERATION];
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || !lane->control || lane->context || lane->running
        || lane->callback_active || (lane->id && !lane->report_verified)) {
        wl_evaluation_control_source_bank_unlock(bank); return -EBUSY;
    }
    wl_evaluation_control_t *control = lane->control;
#ifdef _MSC_VER
    uint64_t references = (uint64_t)_InterlockedCompareExchange64(
        (volatile __int64 *)&control->references, 0, 0);
#else
    uint64_t references = atomic_load_explicit(&control->references,
            memory_order_acquire);
#endif
    if (control->source_bank_class != bank || control->owner != lane
        || references != UINT64_C(2) || control->active
        || wl_evaluation_control_active_attempt_id(control)
        || control->configured_limit !=
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY) {
        wl_evaluation_control_source_bank_unlock(bank); return -EBUSY;
    }
    /* Actual proved Source-only private rearm/BEGIN, not an Original reset. */
    control->attempt_id = 1; lane->id = 1;
    lane->charged_total = 0; lane->report_verified = false;
    control->active = true; control->attempt_limit =
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY;
    control->consumed = 0; control->stop = WL_EVALUATION_CONTROL_NONE;
    control->accounting_error = 0;
#ifdef _MSC_VER
    (void)_InterlockedExchange64(
        (volatile __int64 *)&control->active_attempt_id, 1);
#else
    atomic_store_explicit(&control->active_attempt_id, 1, memory_order_release);
#endif
    lane->running = true;
#ifdef WL_SESSION_TEST_HOOKS
    wl_evaluation_control_source_observation_test_cancel(lane, 1);
#endif
    int rc = wl_evaluation_control_source_observation_zero_locked(lane);
    if (!rc) {
#ifdef WL_SESSION_TEST_HOOKS
        wl_evaluation_control_source_observation_test_cancel(lane, 2);
#endif
        /* This real zero is the fixed positive10's cancellation observation.
         * Exclusive fresh0/limit768 prove its arithmetic cannot deny. */
        rc = wl_evaluation_control_source_observation_zero_locked(lane);
    }
    if (!rc) {
        control->consumed = WL_EVALUATION_CONTROL_SOURCE_OBSERVATION_WORK;
        lane->charged_total = WL_EVALUATION_CONTROL_SOURCE_OBSERVATION_WORK;
#ifdef WL_SESSION_TEST_HOOKS
        wl_evaluation_control_source_observation_test_cancel(lane, 3);
#endif
        rc = wl_evaluation_control_source_observation_zero_locked(lane);
    }
    if (rc) {
        wl_evaluation_control_source_observation_settle_locked(lane, NULL);
        if (wl_mutex_unlock(&bank->mutex)) abort();
        return WL_EVALUATION_CONTROL_SOURCE_METADATA_INITIAL_CANCELLED;
    }
    context->bank = bank;
    context->lane = WL_EVALUATION_CONTROL_SOURCE_OPERATION;
    context->thread_cookie = &wl_evaluation_control_source_thread_cookie;
    context->identity = context;
    lane->context = context; lane->observation_active = true;
    wl_evaluation_control_source_bank_unlock(bank);
    return WL_EVALUATION_CONTROL_SOURCE_METADATA_OPEN;
}

static int
wl_evaluation_control_source_observation_continue(
    wl_evaluation_control_source_context_t *context, bool finish)
{
    if (!context || context->identity != context || !context->bank
        || context->lane !=
        WL_EVALUATION_CONTROL_SOURCE_OPERATION) return EINVAL;
    if (context->thread_cookie != &wl_evaluation_control_source_thread_cookie)
        return EBUSY;
    wl_evaluation_control_source_bank_t *bank = context->bank;
    if (bank->identity != bank) return EINVAL;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[WL_EVALUATION_CONTROL_SOURCE_OPERATION];
    wl_evaluation_control_t *control = lane->control;
    if (bank->closing || !lane->observation_active || lane->context != context
        || !lane->running || lane->callback_active || lane->id != 1
        || !control || control->source_bank_class != bank ||
        control->owner != lane
        || !control->active || control->attempt_id != lane->id
        || wl_evaluation_control_active_attempt_id(control) != lane->id
        || control->consumed != WL_EVALUATION_CONTROL_SOURCE_OBSERVATION_WORK
        || lane->charged_total != control->consumed
        || control->attempt_limit != WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
        || control->configured_limit !=
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
        || control->accounting_error) {
        wl_evaluation_control_source_bank_unlock(bank); return EINVAL;
    }
#ifdef WL_SESSION_TEST_HOOKS
    wl_evaluation_control_source_observation_test_cancel(lane, finish ? 5 : 4);
#endif
    rc = wl_evaluation_control_source_observation_zero_locked(lane);
    if (lane->retained_state == WL_EVALUATION_CONTROL_SOURCE_FRAME) {
        if (rc) {
            /* Preserve the genuine active generation and exact admitted
             * extent, while unlinking this still-live automatic frame. */
            lane->context = NULL; lane->observation_active = false;
            context->identity = NULL;
            lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_STOPPED_STANDBY;
        } else if (finish) {
            wl_evaluation_control_source_observation_settle_locked(lane,
                context);
            lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_FINISH_PENDING;
            lane->running = true;
        }
    } else if (rc || finish) {
        wl_evaluation_control_source_observation_settle_locked(lane, context);
    }
    wl_evaluation_control_source_bank_unlock(bank);
    return rc;
}

int
wl_evaluation_control_source_observation_poll(
    wl_evaluation_control_source_context_t *context)
{
    return wl_evaluation_control_source_observation_continue(context, false);
}

int
wl_evaluation_control_source_observation_end(
    wl_evaluation_control_source_context_t *context)
{
    return wl_evaluation_control_source_observation_continue(context, true);
}

/* Fixed CLOSE sibling: an actual destroy owner never relabels OP. */
int
wl_evaluation_control_source_metadata_enter_close_owned(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_context_t *context)
{
    if (wl_evaluation_control_locked_metadata_action) return -EBUSY;
    if (wl_mutex_lock(&bank->mutex)) return -EIO;
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[WL_EVALUATION_CONTROL_SOURCE_CLOSE];
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || !lane->control || lane->context || lane->running
        || lane->callback_active || (lane->id && !lane->report_verified)) {
        wl_evaluation_control_source_bank_unlock(bank); return -EBUSY;
    }
    wl_evaluation_control_t *control = lane->control;
#ifdef _MSC_VER
    uint64_t references = (uint64_t)_InterlockedCompareExchange64(
        (volatile __int64 *)&control->references, 0, 0);
#else
    uint64_t references = atomic_load_explicit(&control->references,
            memory_order_acquire);
#endif
    if (control->source_bank_class != bank || control->owner != lane
        || references != UINT64_C(2) || control->active
        || wl_evaluation_control_active_attempt_id(control)
        || control->configured_limit !=
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY) {
        wl_evaluation_control_source_bank_unlock(bank); return -EBUSY;
    }
    /* Actual proved Source-only private rearm/BEGIN, not an Original reset. */
    control->attempt_id = 1; lane->id = 1;
    lane->charged_total = 0; lane->report_verified = false;
    control->active = true; control->attempt_limit =
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY;
    control->consumed = 0; control->stop = WL_EVALUATION_CONTROL_NONE;
    control->accounting_error = 0;
#ifdef _MSC_VER
    (void)_InterlockedExchange64(
        (volatile __int64 *)&control->active_attempt_id, 1);
#else
    atomic_store_explicit(&control->active_attempt_id, 1, memory_order_release);
#endif
    lane->running = true;
#ifdef WL_SESSION_TEST_HOOKS
    wl_evaluation_control_source_metadata_test_cancel(lane, 1);
#endif
    /* The exclusive fresh stores prove active/id/limit/count/accounting and
     * zero price. This actual RMW is the remaining charge0 observation. */
#ifdef _MSC_VER
    uint64_t cancelled = (uint64_t)_InterlockedExchangeAdd64(
        (volatile __int64 *)&control->cancelled, 0);
#else
    uint64_t cancelled = atomic_fetch_add_explicit(&control->cancelled, 0,
            memory_order_relaxed);
#endif
    if (cancelled) {
        control->stop = WL_EVALUATION_CONTROL_CANCELLED;
        /* Every field is the real just-initialized/observed state, under
         * BANK exclusion with no intervening callback or non-atomic writer. */
        control->completed = (wl_evaluation_control_report_t){
            .attempt_id = 1,
            .limit = WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY,
            .consumed = 0, .stop_reason = WL_EVALUATION_CONTROL_CANCELLED,
            .execution_status = 0, .cleanup_status = 0,
            .result = WL_EVALUATION_CONTROL_CANCELLED
        };
        control->active = false;
#ifdef _MSC_VER
        (void)_InterlockedExchange64(
            (volatile __int64 *)&control->active_attempt_id, 0);
#else
        atomic_store_explicit(&control->active_attempt_id, 0,
            memory_order_release);
#endif
        lane->report_verified = true; lane->running = false;
        if (wl_mutex_unlock(&bank->mutex)) abort();
        return WL_EVALUATION_CONTROL_SOURCE_METADATA_INITIAL_CANCELLED;
    }
    context->identity = context; context->bank = bank;
    context->lane = WL_EVALUATION_CONTROL_SOURCE_CLOSE;
    context->thread_cookie = &wl_evaluation_control_source_thread_cookie;
    lane->context = context;
    wl_evaluation_control_source_bank_unlock(bank);
    return WL_EVALUATION_CONTROL_SOURCE_METADATA_OPEN;
}

/* Fixed observation class: no recipe, callback, state read or Native body.
 * The local context never leaves this owning frame or supplies authority. */
int
wl_evaluation_control_source_operation_observe_owned(
    wl_evaluation_control_source_bank_t *bank)
{
    wl_evaluation_control_source_context_t context;
    int entered = wl_evaluation_control_source_metadata_enter_issued(bank,
            &context);
    if (entered) return entered <
               0 ? -entered : WL_EVALUATION_CONTROL_CANCELLED;
    /* OPEN must settle before this automatic context dies. Lock failure is
     * an infrastructure invariant failure, never a dangling public return. */
    if (wl_evaluation_control_source_bank_lock(bank)) abort();
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[WL_EVALUATION_CONTROL_SOURCE_OPERATION];
    if (lane->context != &context || !lane->running || lane->callback_active
        || context.identity != &context) abort();
#ifdef WL_SESSION_TEST_HOOKS
    wl_evaluation_control_source_metadata_test_cancel(lane, 2);
#endif
    int rc = wl_evaluation_control_source_bank_charge(lane,
            WL_EVALUATION_CONTROL_SOURCE_OBSERVATION_WORK);
#ifdef WL_SESSION_TEST_HOOKS
    if (!rc) wl_evaluation_control_source_metadata_test_cancel(lane, 3);
#endif
    int finished = wl_evaluation_control_source_bank_finish_phase(lane);
    lane->context = NULL;
    context.identity = NULL;
    wl_evaluation_control_source_bank_unlock(bank);
    return rc ? rc : finished;
}

#ifdef WL_SESSION_TEST_HOOKS
static void
wl_evaluation_control_source_metadata_probe_state(
    wl_evaluation_control_source_lane_state_t *lane,
    wl_evaluation_control_source_metadata_probe_state_t *out)
{
    wl_evaluation_control_t *control = lane->control;
    if (wl_mutex_lock(&control->mutex)) abort();
    out->counter = control->attempt_id;
    out->active_id = wl_evaluation_control_active_attempt_id(control);
    out->consumed = control->consumed;
    out->report = control->completed;
    if (wl_mutex_unlock(&control->mutex)) abort();
    out->lane_id = lane->id; out->charged = lane->charged_total;
    out->verified = lane->report_verified;
}

int
wl_evaluation_control_test_source_metadata_start_rejection(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index, unsigned fault,
    wl_evaluation_control_t *foreign, unsigned issuer,
    wl_evaluation_control_source_metadata_probe_t *out)
{
    if (!bank || bank->identity != bank || !out || fault < 1 || fault > 6
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || (fault == 5 && !foreign) || issuer > 2) return EINVAL;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    wl_evaluation_control_t *owned = lane->control;
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    if (lane->context || lane->running || lane->callback_active
        || !lane->id || !lane->report_verified) abort();
    /* Actual private counter changed by the existing test API. A refused
     * proof must not silently reset it while preserving an old id1 report. */
    if (wl_evaluation_control_test_seed_attempt_id(owned, lane, 7)) abort();
    int other_owner = 0;
    bool verified = lane->report_verified;
    if (fault == 1 && wl_evaluation_control_retain(owned)) abort();
    if (fault == 2 && wl_evaluation_control_transfer(owned, lane, &other_owner))
        abort();
    if (fault == 3 && wl_evaluation_control_begin(owned, lane)) abort();
    if (fault == 4) lane->report_verified = false;
    if (fault == 5) lane->control = foreign; /* Invalid borrowed Normal class. */
    if (fault == 6 && wl_evaluation_control_configure(owned,
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY - 1)) abort();
    wl_evaluation_control_source_metadata_probe_state(lane, &out->before);
    if (issuer) {
        /* Setup and snapshots still mutate/read under BANK. The actual
         * externally serialized invalid fixture is then presented through
         * the real issued entry, which acquires the same BANK exclusion. */
        if (index != WL_EVALUATION_CONTROL_SOURCE_OPERATION) abort();
        wl_evaluation_control_source_bank_unlock(bank);
        wl_evaluation_control_source_context_t context = {0};
        if (issuer == 1)
            rc = wl_evaluation_control_source_metadata_enter_issued(bank,
                    &context);
        else {
            wl_evaluation_control_source_retained_output_t retained;
            unsigned char unchanged[sizeof(retained)];
            memset(&retained, 0x5a, sizeof(retained));
            memcpy(unchanged, &retained, sizeof(retained));
            rc = -wl_evaluation_control_source_metadata_enter_retained_owned(
                bank->owner_slot, &context, &retained);
            if (context.identity || memcmp(unchanged, &retained,
                sizeof(retained))) abort();
        }
        if (rc >= 0) abort(); /* Every fixture must refuse before BEGIN. */
        if (wl_evaluation_control_source_bank_lock(bank)) abort();
    } else rc = wl_evaluation_control_source_metadata_start(bank,
                (unsigned)index,
                &out->entry, false);
    wl_evaluation_control_source_metadata_probe_state(lane, &out->after);
    /* Restore actual invalid fixture transitions honestly, after observing
     * the refusal. No such transition is a positive SOURCE/Native issuer. */
    if (fault == 1) wl_evaluation_control_release(owned);
    if (fault == 2 && wl_evaluation_control_transfer(owned, &other_owner, lane))
        abort();
    if (fault == 3) {
        if (wl_evaluation_control_finish(owned, lane, 0, 0)) abort();
        wl_evaluation_control_report_t report;
        if (wl_evaluation_control_report(owned, &report)
            || wl_evaluation_control_active_attempt_id(owned)
            || report.limit != WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY
            || report.consumed || report.stop_reason || report.execution_status
            || report.cleanup_status || report.result) abort();
        lane->id = report.attempt_id; lane->charged_total = report.consumed;
        lane->report_verified = true;
    }
    if (fault == 4) lane->report_verified = verified;
    if (fault == 5) lane->control = owned;
    if (fault == 6 && wl_evaluation_control_configure(owned,
        WL_EVALUATION_CONTROL_SOURCE_PHASE_CAPACITY)) abort();
    wl_evaluation_control_source_bank_unlock(bank);
    return rc;
}
#endif

/* Unlike the compatibility full-request issuer, this entry observes the real
 * saved BANK generation before preparing the live semantic tuple. Preparation
 * is sealed fixed metadata, never Native/user work. Every zero is an actual
 * charge_attempt on this generation; a refused zero supplies no cut. */
int
wl_evaluation_control_source_bank_run_metadata_recipe(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_metadata_prepare_fn prepare,
    const void *recipe,
    wl_evaluation_control_source_metadata_preparation_t *prepared)
{
    if (!bank || bank->identity != bank
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || !policy || !prepare || !recipe || recipe == bank || !prepared
        || !policy->bootstrap_work || !policy->current_work
        || !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->current_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM)
        return EINVAL;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running
        || lane->callback_active) {
        wl_evaluation_control_source_bank_unlock(bank);
        return EBUSY;
    }
    rc = wl_evaluation_control_source_metadata_start(bank, (unsigned)index,
            &prepared->entry, false);
    if (rc) {
        /* Every begun refusal was really settled. No context was published,
         * no BOOT was paid, and no preparation/action ran. */
        wl_evaluation_control_source_bank_unlock(bank);
        return rc;
    }
    wl_evaluation_control_source_context_t context = {
        .identity = &context, .bank = bank, .lane = (unsigned)index,
        .thread_cookie = &wl_evaluation_control_source_thread_cookie
    };
    lane->context = &context;
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->bootstrap_work);
    for (unsigned group = 0; !rc && group < 3; ++group) {
        lane->callback_active = true;
        wl_evaluation_control_locked_metadata_action = true;
        prepared->preparation_protocol = prepare(recipe, group, prepared);
        wl_evaluation_control_locked_metadata_action = false;
        lane->callback_active = false;
        if (prepared->preparation_protocol) {
            rc = prepared->preparation_protocol;
            break;
        }
        prepared->groups_completed = group + 1;
#ifdef WL_SESSION_TEST_HOOKS
        wl_evaluation_control_source_metadata_test_cancel(lane, group + 2);
#endif
        rc = wl_evaluation_control_source_bank_charge(lane, 0);
    }
#ifdef WL_SESSION_TEST_HOOKS
    if (!rc && wl_evaluation_control_test_source_lexical_cancel_before_action) {
        wl_evaluation_control_test_source_lexical_cancel_before_action = false;
        wl_evaluation_control_request_cancel(lane->control);
    }
#endif
    if (!rc && (!prepared->action || !prepared->result_published)) rc = EINVAL;
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->current_work);
    if (!rc) {
        lane->callback_active = true;
        wl_evaluation_control_locked_metadata_action = true;
        prepared->outcome.action_result = prepared->action(prepared->payload);
        wl_evaluation_control_locked_metadata_action = false;
        prepared->outcome.action_called = true;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_evaluation_control_test_source_lexical_cancel_after_action) {
            wl_evaluation_control_test_source_lexical_cancel_after_action =
                false;
            wl_evaluation_control_request_cancel(lane->control);
        }
#endif
        lane->callback_active = false;
        rc = wl_evaluation_control_source_bank_charge(lane, policy->close_work);
    }
    int finished = wl_evaluation_control_source_bank_finish_phase(lane);
    prepared->entry.settled = true;
    lane->context = NULL;
    wl_evaluation_control_source_bank_unlock(bank);
    return rc ? rc : finished;
}

/* The only active context returned to a caller belongs to issued entry.
 * Authentication precedes parameter rejection: an actual owned phase must
 * settle even when the continuation recipe/policy is refused. */
static int
wl_evaluation_control_source_metadata_continue_mode(
    wl_evaluation_control_source_context_t *context,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_metadata_prepare_fn prepare,
    const void *recipe,
    wl_evaluation_control_source_metadata_preparation_t *prepared,
    bool retained, wl_evaluation_control_source_retained_output_t *out)
{
    if (!context || context->identity != context || !context->bank
        || context->lane != WL_EVALUATION_CONTROL_SOURCE_OPERATION)
        return EINVAL;
    if (context->thread_cookie != &wl_evaluation_control_source_thread_cookie)
        return retained ? EBUSY : EINVAL;
    wl_evaluation_control_source_bank_t *bank = context->bank;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[WL_EVALUATION_CONTROL_SOURCE_OPERATION];
    if (lane->retained_state !=
        (retained ? WL_EVALUATION_CONTROL_SOURCE_METADATA
            : WL_EVALUATION_CONTROL_SOURCE_QUIESCENT)
        || lane->context != context || !lane->running || lane->callback_active
        || lane->observation_active || (retained &&
        !wl_evaluation_control_source_retained_metadata_valid(bank, lane))) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    int protocol = 0;
    if (prepared) {
        prepared->entry.begun = true;
        prepared->entry.checkpoint_result = 0; /* Actual prior OPEN. */
        if (retained) {
            prepared->entry.settled = false;
            prepared->groups_completed = 0; prepared->preparation_protocol = 0;
            prepared->action = NULL; prepared->payload = NULL;
            prepared->outcome.action_result = 0;
            prepared->outcome.action_called = false;
            prepared->result_published = false;
        }
    }
    if (!policy || !prepare || !recipe || recipe == bank || !prepared
        || !policy->bootstrap_work || !policy->current_work ||
        !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->current_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM
        || (retained && !out)) {
        protocol = EINVAL; rc = EINVAL; goto settle;
    }
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->bootstrap_work);
    for (unsigned group = 0; !rc && group < 3; ++group) {
        lane->callback_active = true;
        wl_evaluation_control_locked_metadata_action = true;
        prepared->preparation_protocol = prepare(recipe, group, prepared);
        wl_evaluation_control_locked_metadata_action = false;
        lane->callback_active = false;
        if (prepared->preparation_protocol) {
            protocol = prepared->preparation_protocol > 0 ?
                prepared->preparation_protocol : EINVAL;
            rc = prepared->preparation_protocol;
            break;
        }
        prepared->groups_completed = group + 1;
#ifdef WL_SESSION_TEST_HOOKS
        wl_evaluation_control_source_metadata_test_cancel(lane, group + 2);
#endif
        rc = wl_evaluation_control_source_bank_charge(lane, 0);
    }
#ifdef WL_SESSION_TEST_HOOKS
    if (!rc && wl_evaluation_control_test_source_lexical_cancel_before_action) {
        wl_evaluation_control_test_source_lexical_cancel_before_action = false;
        wl_evaluation_control_request_cancel(lane->control);
    }
#endif
    if (!rc && (!prepared->action || !prepared->result_published)) {
        protocol = EINVAL; rc = EINVAL;
    }
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->current_work);
    if (!rc) {
        lane->callback_active = true;
        wl_evaluation_control_locked_metadata_action = true;
        prepared->outcome.action_result = prepared->action(prepared->payload);
        wl_evaluation_control_locked_metadata_action = false;
        prepared->outcome.action_called = true;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_evaluation_control_test_source_lexical_cancel_after_action) {
            wl_evaluation_control_test_source_lexical_cancel_after_action =
                false;
            wl_evaluation_control_request_cancel(lane->control);
        }
#endif
        lane->callback_active = false;
        rc = wl_evaluation_control_source_bank_charge(lane, policy->close_work);
    }
settle:;
    int finished = wl_evaluation_control_source_bank_finish_phase(lane);
    if (prepared) prepared->entry.settled = true;
    lane->context = NULL;
    context->identity = NULL;
    if (retained) {
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_FINISH_PENDING;
        lane->running = true;
        if (out) {
            out->financial_result = finished;
            out->state = lane->retained_state;
            out->opened = false; out->report_verified = lane->report_verified;
        }
    }
    wl_evaluation_control_source_bank_unlock(bank);
    return retained ? protocol : rc ? rc : finished;
}

int
wl_evaluation_control_source_metadata_continue_issued(
    wl_evaluation_control_source_context_t *context,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_metadata_prepare_fn prepare,
    const void *recipe,
    wl_evaluation_control_source_metadata_preparation_t *prepared)
{
    return wl_evaluation_control_source_metadata_continue_mode(context, policy,
               prepare, recipe, prepared, false, NULL);
}

int
wl_evaluation_control_source_metadata_continue_retained_owned(
    wl_evaluation_control_source_context_t *context,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_metadata_prepare_fn prepare,
    const void *recipe,
    wl_evaluation_control_source_metadata_preparation_t *prepared,
    wl_evaluation_control_source_retained_output_t *out)
{
    return wl_evaluation_control_source_metadata_continue_mode(context, policy,
               prepare, recipe, prepared, true, out);
}

/* Authenticate and settle the actual fixed CLOSE lane. */
int
wl_evaluation_control_source_metadata_continue_close_owned(
    wl_evaluation_control_source_context_t *context,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_metadata_prepare_fn prepare,
    const void *recipe,
    wl_evaluation_control_source_metadata_preparation_t *prepared)
{
    if (!context || context->identity != context || !context->bank
        || context->lane != WL_EVALUATION_CONTROL_SOURCE_CLOSE
        || context->thread_cookie !=
        &wl_evaluation_control_source_thread_cookie)
        return EINVAL;
    wl_evaluation_control_source_bank_t *bank = context->bank;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[WL_EVALUATION_CONTROL_SOURCE_CLOSE];
    if (lane->context != context || !lane->running || lane->callback_active) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    if (prepared) {
        prepared->entry.begun = true;
        prepared->entry.checkpoint_result = 0; /* Actual prior OPEN. */
    }
    if (!policy || !prepare || !recipe || recipe == bank || !prepared
        || !policy->bootstrap_work || !policy->current_work ||
        !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->current_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM) {
        rc = EINVAL; goto settle;
    }
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->bootstrap_work);
    for (unsigned group = 0; !rc && group < 3; ++group) {
        lane->callback_active = true;
        wl_evaluation_control_locked_metadata_action = true;
        prepared->preparation_protocol = prepare(recipe, group, prepared);
        wl_evaluation_control_locked_metadata_action = false;
        lane->callback_active = false;
        if (prepared->preparation_protocol) {
            rc = prepared->preparation_protocol;
            break;
        }
        prepared->groups_completed = group + 1;
#ifdef WL_SESSION_TEST_HOOKS
        wl_evaluation_control_source_metadata_test_cancel(lane, group + 2);
#endif
        rc = wl_evaluation_control_source_bank_charge(lane, 0);
    }
#ifdef WL_SESSION_TEST_HOOKS
    if (!rc && wl_evaluation_control_test_source_lexical_cancel_before_action) {
        wl_evaluation_control_test_source_lexical_cancel_before_action = false;
        wl_evaluation_control_request_cancel(lane->control);
    }
#endif
    if (!rc && (!prepared->action || !prepared->result_published)) rc = EINVAL;
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->current_work);
    if (!rc) {
        lane->callback_active = true;
        wl_evaluation_control_locked_metadata_action = true;
        prepared->outcome.action_result = prepared->action(prepared->payload);
        wl_evaluation_control_locked_metadata_action = false;
        prepared->outcome.action_called = true;
#ifdef WL_SESSION_TEST_HOOKS
        if (wl_evaluation_control_test_source_lexical_cancel_after_action) {
            wl_evaluation_control_test_source_lexical_cancel_after_action =
                false;
            wl_evaluation_control_request_cancel(lane->control);
        }
#endif
        lane->callback_active = false;
        rc = wl_evaluation_control_source_bank_charge(lane, policy->close_work);
    }
settle:;
    int finished = wl_evaluation_control_source_bank_finish_phase(lane);
    if (prepared) prepared->entry.settled = true;
    lane->context = NULL;
    context->identity = NULL;
    wl_evaluation_control_source_bank_unlock(bank);
    return rc ? rc : finished;
}

static int
wl_evaluation_control_source_metadata_bind_continue_mode(
    wl_evaluation_control_source_context_t *context,
    wl_evaluation_control_source_metadata_bind_prepare_fn prepare,
    wl_evaluation_control_source_metadata_bind_fn bind, void *root,
    wl_evaluation_control_source_metadata_bind_outcome_t *out, bool retained,
    wl_evaluation_control_source_retained_output_t *retained_out)
{
    if (!context || context->identity != context || !context->bank
        || context->lane !=
        WL_EVALUATION_CONTROL_SOURCE_OPERATION) return EINVAL;
    if (context->thread_cookie != &wl_evaluation_control_source_thread_cookie)
        return retained ? EBUSY : EINVAL;
    wl_evaluation_control_source_bank_t *bank = context->bank;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane =
        &bank->lanes[WL_EVALUATION_CONTROL_SOURCE_OPERATION];
    if (lane->retained_state !=
        (retained ? WL_EVALUATION_CONTROL_SOURCE_METADATA
            : WL_EVALUATION_CONTROL_SOURCE_QUIESCENT)
        || lane->context != context || !lane->running || lane->callback_active
        || lane->observation_active || (retained &&
        !wl_evaluation_control_source_retained_metadata_valid(bank, lane))) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    int protocol = 0;
    if (out) {
        out->cursor = NULL; out->before_cursor = 0; out->after_cursor = 0;
        out->preparation_protocol = 0; out->role_called = false;
        out->complete_selected = false; out->settled = false;
    }
    if (!prepare || !bind || !root || root == bank || !out
        || (retained && !retained_out)) {
        protocol = EINVAL; rc = EINVAL; goto settle;
    }
    for (unsigned group = 0; !rc && group < 2; ++group) {
        lane->callback_active = true;
        wl_evaluation_control_locked_metadata_action = true;
        out->preparation_protocol = prepare(root, group, out);
        wl_evaluation_control_locked_metadata_action = false;
        lane->callback_active = false;
        if (out->preparation_protocol) {
            protocol = out->preparation_protocol > 0 ?
                out->preparation_protocol : EINVAL;
            rc = out->preparation_protocol; break;
        }
#ifdef WL_SESSION_TEST_HOOKS
        wl_evaluation_control_source_metadata_test_cancel(lane, group + 2);
#endif
        rc = wl_evaluation_control_source_bank_charge(lane, 0);
    }
    if (!rc && (!out->cursor || out->before_cursor > 4
        || *out->cursor != out->before_cursor)) {
        protocol = EINVAL; rc = EINVAL;
    }
    if (!rc && out->before_cursor < 4) {
        rc = wl_evaluation_control_source_bank_charge(lane, 10);
        if (!rc) {
            lane->callback_active = true;
            wl_evaluation_control_locked_metadata_action = true;
            out->preparation_protocol = bind(root, out->before_cursor);
            wl_evaluation_control_locked_metadata_action = false;
            lane->callback_active = false;
            out->role_called = true;
            out->after_cursor = *out->cursor;
            if (out->preparation_protocol) {
                protocol = out->preparation_protocol > 0 ?
                    out->preparation_protocol : EINVAL;
                rc = out->preparation_protocol;
            } else if (out->after_cursor != out->before_cursor + 1) {
                protocol = EINVAL; rc = EINVAL;
            }
#ifdef WL_SESSION_TEST_HOOKS
            wl_evaluation_control_source_metadata_test_cancel(lane, 4);
#endif
            if (!rc) rc = wl_evaluation_control_source_bank_charge(lane, 0);
        }
    } else if (!rc) {
        out->after_cursor = out->before_cursor;
        out->complete_selected = true;
    }
settle:;
    int finished = wl_evaluation_control_source_bank_finish_phase(lane);
    if (out) out->settled = true;
    lane->context = NULL; context->identity = NULL;
    if (retained) {
        lane->retained_state = WL_EVALUATION_CONTROL_SOURCE_FINISH_PENDING;
        lane->running = true;
        if (retained_out) {
            retained_out->financial_result = finished;
            retained_out->state = lane->retained_state;
            retained_out->opened = false;
            retained_out->report_verified = lane->report_verified;
        }
    }
    wl_evaluation_control_source_bank_unlock(bank);
    return retained ? protocol : rc ? rc : finished;
}

int
wl_evaluation_control_source_metadata_bind_continue_issued(
    wl_evaluation_control_source_context_t *context,
    wl_evaluation_control_source_metadata_bind_prepare_fn prepare,
    wl_evaluation_control_source_metadata_bind_fn bind, void *root,
    wl_evaluation_control_source_metadata_bind_outcome_t *out)
{
    return wl_evaluation_control_source_metadata_bind_continue_mode(context,
               prepare, bind, root, out, false, NULL);
}

int
wl_evaluation_control_source_metadata_bind_continue_retained_owned(
    wl_evaluation_control_source_context_t *context,
    wl_evaluation_control_source_metadata_bind_prepare_fn prepare,
    wl_evaluation_control_source_metadata_bind_fn bind, void *root,
    wl_evaluation_control_source_metadata_bind_outcome_t *outcome,
    wl_evaluation_control_source_retained_output_t *out)
{
    return wl_evaluation_control_source_metadata_bind_continue_mode(context,
               prepare, bind, root, outcome, true, out);
}

/* Bootstrap recipes before any private financier/resource birth. Cursor
 * progress is real caller-owned metadata and never a Native permission. The
 * bank activation/context remains automatic and settled on every return. */
int
wl_evaluation_control_source_bank_bind_metadata_roles(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    const wl_evaluation_control_source_phase_policy_t *policy,
    wl_evaluation_control_source_metadata_bind_fn bind, void *root,
    unsigned *cursor)
{
    if (!bank || bank->identity != bank
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || !policy || !bind || !root || root == bank || !cursor || *cursor >= 4
        || !policy->bootstrap_work || !policy->current_work
        || !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->current_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM) return EINVAL;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running
        || lane->callback_active) {
        wl_evaluation_control_source_bank_unlock(bank);
        return EBUSY;
    }
    wl_evaluation_control_source_metadata_entry_t entry = {0};
    rc = wl_evaluation_control_source_metadata_start(bank, (unsigned)index,
            &entry, false);
    if (rc) {
        wl_evaluation_control_source_bank_unlock(bank);
        return rc;
    }
    wl_evaluation_control_source_context_t context = {
        .identity = &context, .bank = bank, .lane = (unsigned)index,
        .thread_cookie = &wl_evaluation_control_source_thread_cookie
    };
    lane->context = &context;
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->bootstrap_work);
    /* Always select CURRENT once, including resumed cursors2..4. */
    for (unsigned packet = 0; !rc && packet < 4; ++packet) {
        if (packet == 2) rc = wl_evaluation_control_source_bank_charge(lane,
                    policy->current_work);
        if (rc || packet != *cursor) continue;
        lane->callback_active = true;
        wl_evaluation_control_locked_metadata_action = true;
        rc = bind(root, packet);
        wl_evaluation_control_locked_metadata_action = false;
        lane->callback_active = false;
        if (!rc && *cursor != packet + 1) rc = EINVAL;
        if (rc) break;
#ifdef WL_SESSION_TEST_HOOKS
        wl_evaluation_control_source_metadata_test_cancel(lane, packet + 2);
#endif
        rc = wl_evaluation_control_source_bank_charge(lane, 0);
    }
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->close_work);
    int finished = wl_evaluation_control_source_bank_finish_phase(lane);
    lane->context = NULL;
    wl_evaluation_control_source_bank_unlock(bank);
    return rc ? rc : finished;
}

/* A real no-coupon financial observation, not the positive policy runner
* with invented zero fees. Every begun path settles under the same mutex;
* no context or action is published, including successful observation. */
int
wl_evaluation_control_source_bank_observe_metadata(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    wl_evaluation_control_source_metadata_entry_t *entry)
{
    if (!bank || bank->identity != bank || !entry
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES) return EINVAL;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) return rc;
    if (bank->closing) {
        wl_evaluation_control_source_bank_unlock(bank); return EBUSY;
    }
    rc = wl_evaluation_control_source_metadata_start(bank, (unsigned)index,
            entry, true);
    wl_evaluation_control_source_bank_unlock(bank);
    return rc;
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
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running
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
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running ||
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
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running ||
        lane->callback_active
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
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running ||
        lane->callback_active
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
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running ||
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
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running ||
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
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT) {
        result.protocol = EBUSY; return result;
    }
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
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running
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
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running
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
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running ||
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
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running ||
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
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running ||
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

#ifdef WL_SESSION_TEST_HOOKS
bool wl_evaluation_control_test_source_bank_destroy_cancel;
#endif

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
        if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
            || lane->context || lane->running || lane->callback_active
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
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_evaluation_control_test_source_bank_destroy_cancel) {
        wl_evaluation_control_test_source_bank_destroy_cancel = false;
        wl_evaluation_control_request_cancel(lane->control);
    }
#endif
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
    /* Explicit semantic fields bound fixed FINISH bookkeeping without a
     * bytewise aggregate initializer or padding-byte contract. */
    wl_evaluation_control_attempt_finish_result_t out;
    out.kind = WL_EVALUATION_CONTROL_ATTEMPT_FINISH_REJECTED;
    out.protocol = 0;
    out.report_valid = false;
    out.report.attempt_id = 0;
    out.report.limit = 0;
    out.report.consumed = 0;
    out.report.stop_reason = 0;
    out.report.execution_status = 0;
    out.report.cleanup_status = 0;
    out.report.result = 0;
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
            out.report.attempt_id = control->completed.attempt_id;
            out.report.limit = control->completed.limit;
            out.report.consumed = control->completed.consumed;
            out.report.stop_reason = control->completed.stop_reason;
            out.report.execution_status = control->completed.execution_status;
            out.report.cleanup_status = control->completed.cleanup_status;
            out.report.result = control->completed.result;
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
    out.report.attempt_id = control->completed.attempt_id;
    out.report.limit = control->completed.limit;
    out.report.consumed = control->completed.consumed;
    out.report.stop_reason = control->completed.stop_reason;
    out.report.execution_status = control->completed.execution_status;
    out.report.cleanup_status = control->completed.cleanup_status;
    out.report.result = control->completed.result;
    out.report_valid = true;
    wl_mutex_unlock(&control->mutex);
    return out;
}

int
wl_evaluation_control_detach_attempt(wl_evaluation_control_t *control,
    const void *owner, uint64_t expected_id)
{
    if (!control || !owner || !expected_id) return EINVAL;
    wl_mutex_lock(&control->mutex);
    int rc = control->owner != owner ? EINVAL
        : control->attempt_id != expected_id ? EBUSY
        : control->active ? EBUSY
        : control->completed.attempt_id != expected_id ? EINVAL : 0;
    if (!rc) control->owner = NULL;
    wl_mutex_unlock(&control->mutex);
    if (!rc) wl_evaluation_control_release(control);
    return rc;
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

wl_evaluation_control_lexical_finance_checkpoint_result_t
wl_evaluation_control_lexical_finance_checkpoint_owned(
    wl_evaluation_control_t *control, const void *owner, size_t owner_bytes,
    uint64_t identity, uint64_t id, uint64_t limit, uint64_t charged)
{
    wl_evaluation_control_lexical_finance_checkpoint_result_t out = {
        .protocol = EINVAL
    };
    if (!control || !owner || !owner_bytes || !identity || !id || !limit
        || charged > limit) return out;
    uintptr_t c = (uintptr_t)control, o = (uintptr_t)owner;
    if (sizeof(*control) > UINTPTR_MAX - c || owner_bytes > UINTPTR_MAX - o
        || (c < o + owner_bytes && o < c + sizeof(*control))) return out;
    if (wl_mutex_lock(&control->mutex)) {
        out.protocol = EIO; return out;
    }
    if (control->identity != identity || control->source_bank_class
        || control->owner != owner || control->configured_limit != limit
        || control->attempt_limit != limit || control->consumed != charged
        || control->accounting_error || !control->active
        || control->attempt_id != id
        || wl_evaluation_control_active_attempt_id(control) != id
        || (control->stop != WL_EVALUATION_CONTROL_NONE
        && control->stop != WL_EVALUATION_CONTROL_CANCELLED
        && control->stop != WL_EVALUATION_CONTROL_WORK_LIMIT)) goto done;
    if (atomic_fetch_add_explicit(&control->references, 0,
        memory_order_relaxed) != 2){
        out.protocol = EBUSY; goto done;
    }
    out.checkpoint_result = wl_evaluation_control_charge_locked(control, 0);
    out.checked = true; out.protocol = 0;
done:
    if (wl_mutex_unlock(&control->mutex)) abort();
    return out;
}

#ifdef WL_SESSION_TEST_HOOKS
unsigned wl_evaluation_control_test_source_close_cancel_at;

static void
wl_evaluation_control_source_close_test_cancel(
    const wl_evaluation_control_source_close_borrow_t *borrow, unsigned event)
{
    if (wl_evaluation_control_test_source_close_cancel_at == event) {
        wl_evaluation_control_test_source_close_cancel_at = 0;
        wl_evaluation_control_request_cancel(borrow->control);
    }
}
#endif

wl_evaluation_control_source_close_early_result_t
wl_evaluation_control_source_bank_run_lexical_close_early(
    wl_evaluation_control_source_bank_t *bank,
    wl_evaluation_control_source_lane_t index,
    const wl_evaluation_control_source_phase_policy_t *policy,
    const wl_evaluation_control_source_close_borrow_t *borrow,
    wl_evaluation_control_source_action_fn action, void *payload)
{
    wl_evaluation_control_source_close_early_result_t out = {0};
    out.protocol = EINVAL;
    if (!bank || bank->identity != bank
        || (unsigned)index >= WL_EVALUATION_CONTROL_SOURCE_LANES
        || !policy || !borrow || !action || payload == bank
        || !borrow->control || !borrow->id || !borrow->identity
        || (const void *)borrow->control == (const void *)bank
        || borrow->control->source_bank_class
        || wl_evaluation_control_identity(borrow->control) != borrow->identity
        || !policy->bootstrap_work || !policy->current_work ||
        !policy->close_work
        || policy->bootstrap_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->current_work > WL_EVALUATION_CONTROL_QUANTUM
        || policy->close_work > WL_EVALUATION_CONTROL_QUANTUM) return out;
    out.protocol = 0;
    int rc = wl_evaluation_control_source_bank_lock(bank);
    if (rc) {
        out.protocol = rc; out.phase.protocol = rc;
        out.phase.financial_result = rc; return out;
    }
    wl_evaluation_control_source_lane_state_t *lane = &bank->lanes[index];
    if (lane->retained_state != WL_EVALUATION_CONTROL_SOURCE_QUIESCENT
        || bank->closing || lane->context || lane->running ||
        lane->callback_active){
        wl_evaluation_control_source_bank_unlock(bank);
        out.protocol = EBUSY; out.phase.protocol = EBUSY;
        out.phase.financial_result = EBUSY; return out;
    }
    wl_evaluation_control_source_context_t context = {
        .identity = &context, .bank = bank, .lane = (unsigned)index,
        .thread_cookie = &wl_evaluation_control_source_thread_cookie
    };
    lane->context = &context;
    /* This same-activation borrow remains private and exclusively owned.
     * A source stop never creates a BANK BEGIN or work permission. */
#ifdef WL_SESSION_TEST_HOOKS
    wl_evaluation_control_source_close_test_cancel(borrow,
        WL_EVALUATION_CONTROL_TEST_SOURCE_CLOSE_ACQUIRE);
#endif
    out.acquisition_called = true;
    out.acquisition_result = wl_evaluation_control_charge_attempt(
        borrow->control, borrow->id, 0);
    if (out.acquisition_result) goto unlock;
    rc = WL_SOURCE_EARLY_REARM(bank, (unsigned)index);
    if (rc) {
        out.protocol = rc; out.phase.protocol = rc;
        out.phase.financial_result = rc; goto unbegun_handoff;
    }
    wl_evaluation_control_source_early_begin_t begun =
        wl_evaluation_control_source_bank_begin_checkpoint(lane);
    out.phase.begin_protocol = begun.protocol;
    out.phase.checkpoint_result = begun.checkpoint;
    if (!begun.started) {
        out.protocol = begun.protocol; out.phase.protocol = begun.protocol;
        out.phase.financial_result = begun.protocol; goto unbegun_handoff;
    }
    out.phase.phase_started = true;
    rc = begun.checkpoint;
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->bootstrap_work);
    if (!rc) rc = wl_evaluation_control_source_bank_charge(lane,
                policy->current_work);
    if (!rc) {
        lane->callback_active = true;
        wl_evaluation_control_source_bank_unlock(bank);
        out.phase.first_result = action(payload);
        out.phase.first_called = true;
        if (wl_evaluation_control_source_bank_lock(bank)) abort();
        lane->callback_active = false;
        /* A may already have freed Source. Never follow its borrowed pointer
         * after entry, including a strict A refusal with ownership retained. */
        WL_SOURCE_FINAL_TEST_EVENT(lane,
            WL_EVALUATION_CONTROL_TEST_SOURCE_FINAL_AFTER_ACTION);
        rc = wl_evaluation_control_source_bank_charge(lane, policy->close_work);
    } else {
#ifdef WL_SESSION_TEST_HOOKS
        wl_evaluation_control_source_close_test_cancel(borrow,
            WL_EVALUATION_CONTROL_TEST_SOURCE_CLOSE_PREFINISH);
#endif
        out.unwind_handoff_called = true;
        out.unwind_handoff_result = wl_evaluation_control_charge_attempt(
            borrow->control, borrow->id, 0);
    }
    {
        int finished =
            wl_evaluation_control_source_bank_finish_constructor_phase(
            bank, (unsigned)index, &context);
        out.phase.financial_result = rc ? rc : finished;
        out.phase.phase_finished = true;
    }
    goto unlock;
unbegun_handoff:
#ifdef WL_SESSION_TEST_HOOKS
    wl_evaluation_control_source_close_test_cancel(borrow,
        WL_EVALUATION_CONTROL_TEST_SOURCE_CLOSE_PREFINISH);
#endif
    out.unwind_handoff_called = true;
    out.unwind_handoff_result = wl_evaluation_control_charge_attempt(
        borrow->control, borrow->id, 0);
unlock:
    lane->context = NULL;
    wl_evaluation_control_source_bank_unlock(bank);
    return out;
}

wl_evaluation_control_attempt_begin_result_t
wl_evaluation_control_private_financier_rebegin(
    wl_evaluation_control_t *control,
    const void *owner, uint64_t allocation_identity, uint64_t completed_id,
    uint64_t positive_limit)
{
    wl_evaluation_control_attempt_begin_result_t out = {EINVAL, 0};
    if (!control || !owner || !allocation_identity || !completed_id
        || !positive_limit) return out;
    if (wl_mutex_lock(&control->mutex)) {
        out.protocol = EIO; return out;
    }
    if (control->source_bank_class || control->identity != allocation_identity
        || control->owner != owner || control->attempt_id != completed_id
        || control->completed.attempt_id != completed_id) goto done;
    if (control->active || atomic_load_explicit(&control->active_attempt_id,
        memory_order_acquire) || atomic_load_explicit(&control->references,
        memory_order_acquire) != UINT64_C(2)) {
        out.protocol = EBUSY; goto done;
    }
    if (completed_id == UINT64_MAX) {
        out.protocol = EOVERFLOW; goto done;
    }
    control->configured_limit = control->attempt_limit = positive_limit;
    control->consumed = 0;
    control->stop = WL_EVALUATION_CONTROL_NONE;
    control->accounting_error = 0;
    (void)atomic_exchange_explicit(&control->cancelled, 0,
        memory_order_relaxed);
    control->attempt_id = completed_id + 1;
    control->active = true;
    (void)atomic_exchange_explicit(&control->active_attempt_id,
        control->attempt_id, memory_order_release);
    out.protocol = 0; out.attempt_id = control->attempt_id;
done:
    wl_mutex_unlock(&control->mutex);
    return out;
}

int
wl_evaluation_control_private_financier_parked_owned(
    wl_evaluation_control_t *control, const void *owner,
    uint64_t allocation_identity, uint64_t completed_id)
{
    if (!control || !owner || !allocation_identity || !completed_id)
        return EINVAL;
    if (wl_mutex_lock(&control->mutex)) return EIO;
    int rc = EINVAL;
    if (control->source_bank_class || control->identity != allocation_identity
        || control->owner != owner) goto done;
    if (control->active || atomic_load_explicit(&control->active_attempt_id,
        memory_order_acquire) || atomic_load_explicit(&control->references,
        memory_order_acquire) != UINT64_C(2)) {
        rc = EBUSY; goto done;
    }
    if (control->attempt_id != completed_id
        || control->completed.attempt_id != completed_id
        || !control->configured_limit || !control->attempt_limit) goto done;
    rc = 0;
done:
    wl_mutex_unlock(&control->mutex);
    return rc;
}

int
wl_evaluation_control_financier_finish_commit(wl_evaluation_control_t *control,
    const void *owner, uint64_t expected_id)
{
    if (!control || !owner || !expected_id) return EINVAL;
    if (wl_mutex_lock(&control->mutex)) return EIO;
    if (control->source_bank_class || control->owner != owner) {
        wl_mutex_unlock(&control->mutex);
        return EINVAL;
    }
    if (control->attempt_id != expected_id) {
        wl_mutex_unlock(&control->mutex);
        return EBUSY;
    }
    if (!control->active) {
        if (control->completed.attempt_id != expected_id) {
            wl_mutex_unlock(&control->mutex);
            return EINVAL;
        }
    } else {
        /* Like generic FINISH this does not poll cancellation. */
        control->completed = (wl_evaluation_control_report_t){
            .attempt_id = expected_id, .limit = control->attempt_limit,
            .consumed = control->consumed, .stop_reason = control->stop,
            .execution_status = control->accounting_error, .cleanup_status = 0,
            .result = control->accounting_error ? control->accounting_error
                : (int)control->stop,
        };
        wl_evaluation_control_financier_publish_inactive(control);
    }
    wl_mutex_unlock(&control->mutex);
    return 0;
}

int
wl_evaluation_control_financier_report_owned(wl_evaluation_control_t *control,
    const void *owner, uint64_t expected_id,
    wl_evaluation_control_report_t *out)
{
    if (!control || !owner || !expected_id || !out) return EINVAL;
    if (wl_mutex_lock(&control->mutex)) return EIO;
    if (control->source_bank_class || control->owner != owner) {
        wl_mutex_unlock(&control->mutex);
        return EINVAL;
    }
    if (control->active || control->attempt_id != expected_id
        || control->completed.attempt_id != expected_id) {
        wl_mutex_unlock(&control->mutex);
        return EBUSY;
    }
    *out = control->completed;
    wl_mutex_unlock(&control->mutex);
    return 0;
}

int
wl_evaluation_control_construction_error_finish_commit(
    wl_evaluation_control_t *control, const void *owner, uint64_t expected_id,
    int positive_error)
{
    if (!control || !owner || !expected_id || positive_error <= 0)
        return -EINVAL;
    if (wl_mutex_lock(&control->mutex)) return -EIO;
    if (control->source_bank_class || control->owner != owner) {
        wl_mutex_unlock(&control->mutex);
        return -EINVAL;
    }
    if (control->attempt_id != expected_id) {
        wl_mutex_unlock(&control->mutex);
        return WL_EVALUATION_CONTROL_ATTEMPT_FINISH_SUCCESSOR_MISMATCH;
    }
    if (!control->active) {
        if (control->completed.attempt_id != expected_id) {
            wl_mutex_unlock(&control->mutex);
            return -EINVAL;
        }
        wl_mutex_unlock(&control->mutex);
        return WL_EVALUATION_CONTROL_ATTEMPT_FINISH_ALREADY_ENDED;
    }
    control->completed = (wl_evaluation_control_report_t){
        .attempt_id = expected_id, .limit = control->attempt_limit,
        .consumed = control->consumed, .stop_reason = control->stop,
        .execution_status = positive_error, .cleanup_status = 0,
        .result = positive_error,
    };
    control->active = false;
#ifdef _MSC_VER
    (void)_InterlockedExchange64(
        (volatile __int64 *)&control->active_attempt_id, 0);
#else
    atomic_store_explicit(&control->active_attempt_id, 0, memory_order_release);
#endif
    wl_mutex_unlock(&control->mutex);
    return WL_EVALUATION_CONTROL_ATTEMPT_FINISH_OWN_FINISHED;
}
