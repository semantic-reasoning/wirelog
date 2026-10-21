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
    const void *owner;
    uint64_t configured_limit;
    uint64_t attempt_id;
    uint64_t attempt_limit;
    uint64_t consumed;
    wl_evaluation_control_stop_t stop;
    int accounting_error;
    bool active;
    wl_evaluation_control_report_t completed;
};

int
wl_evaluation_control_create(uint64_t limit, wl_evaluation_control_t **out)
{
    if (!out)
        return EINVAL;
    *out = NULL;
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
    control->configured_limit = limit;
    *out = control;
    return 0;
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
