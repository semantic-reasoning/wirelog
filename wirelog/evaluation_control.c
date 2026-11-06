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
