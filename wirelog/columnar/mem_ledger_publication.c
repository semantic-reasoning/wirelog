/*
 * columnar/mem_ledger_publication.c - controlled finite ledger publication
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */
#include "columnar/mem_ledger_publication.h"

#include <errno.h>

/* Logical source ceilings (expand helpers; assignment/predicate/dispatch,
 * atomic/thread primitive and return each cost one; no call-instruction slot):
 * prepare: 42 arguments/first checkpoint +50 validate/copy/checkpoint
 *          +17 final checkpoint/publication =109.
 * initial: origin11 +guards5 +started1 +active guard8 +stale return1 =26.
 * apply: origin/guards16 +active8 +gate4 +request validation15
 *        +two(count guards2 +kind predicate1 +alloc event33) +publish2 =117.
 * release: origin11 +APPLIED guards5 +gate4 +release/consume/return3 =23.
 * round: checkpoint/caller12 +expected1 +strong/result3 +claim guards2
 *        +failed loopback1 +next active guard/stale return9 =28.
 * Error alternatives fit these ceilings; release HELD is cheaper than APPLIED.
 * ALLOC event33 includes two saturating_add7/local1/update_peak6, guards4
 * and return1; FREE17 and zero3 are cheaper. Apply conservatively combines
 * FREE validation with ALLOC arithmetic. These are admitted bounds, not exact
 * executed operation counts. Charge/poll scheduling and test hooks excluded;
 * saved-attempt validation, error consume/return and loopback included.
 * Reversible preparation admits its future work separately:109 then166.
 * Caller record initialization and actual relation/lease work are separate.
 */
enum {
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_PREPARE_WORK = 109,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_INITIAL_WORK = 26,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_APPLY_WORK = 117,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_RELEASE_WORK = 23,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ROUND_WORK = 28,
};

typedef char wl_columnar_mem_ledger_publication_admission_bound[
        (WL_COLUMNAR_MEM_LEDGER_PUBLICATION_PREPARE_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_MEM_LEDGER_PUBLICATION_INITIAL_WORK
        + WL_COLUMNAR_MEM_LEDGER_PUBLICATION_APPLY_WORK
        + WL_COLUMNAR_MEM_LEDGER_PUBLICATION_RELEASE_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM
        && WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ROUND_WORK <=
        WL_EVALUATION_CONTROL_QUANTUM) ? 1 : -1];

#ifdef WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TEST_HOOKS
void (*wl_columnar_mem_ledger_publication_test_checkpoint)(
    wl_evaluation_control_t *, wl_columnar_mem_ledger_publication_prepared_t *,
    unsigned);
#endif

static uint64_t
wl_columnar_mem_ledger_publication_gate_read(wl_mem_ledger_t *ledger)
{
#ifdef _MSC_VER
    return (uint64_t)atomic_fetch_add_explicit(&ledger->writer_gate,
               UINT64_C(0), memory_order_acquire);
#else
    return atomic_load_explicit(&ledger->writer_gate, memory_order_acquire);
#endif
}

static bool
wl_columnar_mem_ledger_publication_strong(wl_mem_ledger_t *ledger,
    uint64_t *expected)
{
#ifdef _MSC_VER
    return atomic_compare_exchange_weak_explicit(&ledger->writer_gate,
               expected, UINT64_C(1), memory_order_acquire,
               memory_order_relaxed);
#else
    return atomic_compare_exchange_strong_explicit(&ledger->writer_gate,
               expected, UINT64_C(1), memory_order_acquire,
               memory_order_relaxed);
#endif
}

static bool
wl_columnar_mem_ledger_publication_origin(
    const wl_columnar_mem_ledger_publication_prepared_t *prepared)
{
    if (!prepared || prepared->identity != prepared || !prepared->ledger
        || !prepared->control || !prepared->thread_valid)
        return false;
#if defined(WL_HAVE_C11_THREADS)
    if (!thrd_equal(prepared->owner_thread, thrd_current()))
        return false;
#elif defined(_WIN32) || defined(_WIN64)
    if (prepared->owner_thread != GetCurrentThreadId())
        return false;
#else
    if (!pthread_equal(prepared->owner_thread, pthread_self()))
        return false;
#endif
    return true;
}

static bool
wl_columnar_mem_ledger_publication_request_valid(
    const wl_columnar_mem_ledger_publication_request_t *request)
{
    return request->subsystem >= 0 && request->subsystem < WL_MEM_SUBSYS_COUNT
           && (request->kind == WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ALLOC
           || request->kind == WL_COLUMNAR_MEM_LEDGER_PUBLICATION_FREE);
}

/* Callback first, saved identity before zero poll. No successor admission. */
static int
wl_columnar_mem_ledger_publication_checkpoint(wl_evaluation_control_t *control,
    wl_columnar_mem_ledger_publication_prepared_t *prepared, uint64_t saved_id,
    unsigned phase)
{
#ifdef WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TEST_HOOKS
    if (wl_columnar_mem_ledger_publication_test_checkpoint)
        wl_columnar_mem_ledger_publication_test_checkpoint(control, prepared,
            phase);
#else
    (void)prepared;
    (void)phase;
#endif
    uint64_t active = wl_evaluation_control_active_attempt_id(control);
    if (!active || active != saved_id)
        return EINVAL;
    return wl_evaluation_control_charge(control, 0);
}

int
wl_columnar_mem_ledger_publication_prepare_controlled(wl_mem_ledger_t *ledger,
    const wl_columnar_mem_ledger_publication_request_t *requests,
    unsigned count,
    wl_columnar_mem_ledger_publication_prepared_t *prepared,
    wl_evaluation_control_t *control)
{
    int rc = wl_evaluation_control_charge(control,
            WL_COLUMNAR_MEM_LEDGER_PUBLICATION_PREPARE_WORK);
    if (rc)
        return rc;
    if (!ledger || !prepared || count > 2 || (count && !requests)
        || prepared->identity || prepared->ledger || prepared->control
        || prepared->attempt_id || prepared->prepaid_apply
        || prepared->prepaid_release || prepared->prepaid_initial
        || prepared->count || prepared->thread_valid
        || prepared->acquisition_started
        || prepared->phase != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_INERT
        || prepared->requests[0].subsystem || prepared->requests[0].kind
        || prepared->requests[0].bytes || prepared->requests[1].subsystem
        || prepared->requests[1].kind || prepared->requests[1].bytes)
        return EINVAL;
    uint64_t saved_id = wl_evaluation_control_active_attempt_id(control);
    if (!saved_id)
        return EINVAL;
    rc = wl_columnar_mem_ledger_publication_checkpoint(control, prepared,
            saved_id, WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ARGUMENTS);
    if (rc)
        return rc;
    if ((count > 0 &&
        !wl_columnar_mem_ledger_publication_request_valid(&requests[0]))
        || (count > 1 &&
        !wl_columnar_mem_ledger_publication_request_valid(&requests[1])))
        return EINVAL;
    prepared->identity = prepared;
    prepared->ledger = ledger;
    prepared->control = control;
    prepared->attempt_id = saved_id;
    prepared->prepaid_apply = 0;
    prepared->prepaid_release = 0;
    prepared->prepaid_initial = 0;
    prepared->count = count;
    prepared->thread_valid = true;
    prepared->acquisition_started = false;
    prepared->phase = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_INERT;
#if defined(WL_HAVE_C11_THREADS)
    prepared->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    prepared->owner_thread = GetCurrentThreadId();
#else
    prepared->owner_thread = pthread_self();
#endif
    if (count > 0) {
        prepared->requests[0].subsystem = requests[0].subsystem;
        prepared->requests[0].kind = requests[0].kind;
        prepared->requests[0].bytes = requests[0].bytes;
    } else {
        prepared->requests[0].subsystem = 0;
        prepared->requests[0].kind = 0;
        prepared->requests[0].bytes = 0;
    }
    if (count > 1) {
        prepared->requests[1].subsystem = requests[1].subsystem;
        prepared->requests[1].kind = requests[1].kind;
        prepared->requests[1].bytes = requests[1].bytes;
    } else {
        prepared->requests[1].subsystem = 0;
        prepared->requests[1].kind = 0;
        prepared->requests[1].bytes = 0;
    }
    rc = wl_columnar_mem_ledger_publication_checkpoint(control, prepared,
            saved_id, WL_COLUMNAR_MEM_LEDGER_PUBLICATION_PREPARED);
    if (rc)
        goto consume;
    rc = wl_evaluation_control_charge(control,
            WL_COLUMNAR_MEM_LEDGER_PUBLICATION_INITIAL_WORK
            + WL_COLUMNAR_MEM_LEDGER_PUBLICATION_APPLY_WORK
            + WL_COLUMNAR_MEM_LEDGER_PUBLICATION_RELEASE_WORK);
    if (rc)
        goto consume;
    rc = wl_columnar_mem_ledger_publication_checkpoint(control, prepared,
            saved_id, WL_COLUMNAR_MEM_LEDGER_PUBLICATION_READY_CUTOFF);
    if (rc)
        goto consume;
    prepared->prepaid_apply = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_APPLY_WORK;
    prepared->prepaid_release = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_RELEASE_WORK;
    prepared->prepaid_initial = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_INITIAL_WORK;
    prepared->phase = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_READY;
    return 0;
consume:
    prepared->phase = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_CONSUMED;
    return rc;
}

int
wl_columnar_mem_ledger_publication_acquire_controlled(
    wl_columnar_mem_ledger_publication_prepared_t *prepared)
{
    if (!wl_columnar_mem_ledger_publication_origin(prepared)
        || prepared->phase != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_READY
        || prepared->acquisition_started
        || prepared->prepaid_initial
        != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_INITIAL_WORK)
        return EINVAL;
    prepared->acquisition_started = true;
    for (;;) {
        /* First guard prepaid during prepare, subsequent guards by priorR. */
        uint64_t active =
            wl_evaluation_control_active_attempt_id(prepared->control);
        if (!active || active != prepared->attempt_id)
            return EINVAL;
        int rc = wl_evaluation_control_charge(prepared->control,
                WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ROUND_WORK);
        if (rc)
            return rc;
        rc = wl_columnar_mem_ledger_publication_checkpoint(prepared->control,
                prepared, prepared->attempt_id,
                WL_COLUMNAR_MEM_LEDGER_PUBLICATION_PRECAS);
        if (rc)
            return rc;
        uint64_t expected = 0;
        bool claimed =
            wl_columnar_mem_ledger_publication_strong(prepared->ledger,
                &expected);
        if (claimed) {
            prepared->phase = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_HELD;
            return 0;
        }
    }
}

int
wl_columnar_mem_ledger_publication_apply(
    wl_columnar_mem_ledger_publication_prepared_t *prepared)
{
    if (!wl_columnar_mem_ledger_publication_origin(prepared)
        || prepared->phase != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_HELD
        || prepared->prepaid_apply !=
        WL_COLUMNAR_MEM_LEDGER_PUBLICATION_APPLY_WORK
        || prepared->count > 2)
        return EINVAL;
    uint64_t active =
        wl_evaluation_control_active_attempt_id(prepared->control);
    if (!active || active != prepared->attempt_id)
        return EINVAL;
    if (wl_columnar_mem_ledger_publication_gate_read(prepared->ledger) != 1)
        return EINVAL;
    /* Validate both before the first protected write, preserving order. */
    if ((prepared->count > 0 &&
        !wl_columnar_mem_ledger_publication_request_valid(
            &prepared->requests[0]))
        || (prepared->count > 1 &&
        !wl_columnar_mem_ledger_publication_request_valid(
            &prepared->requests[1])))
        return EINVAL;
    if (prepared->count > 0)
        wl_columnar_mem_ledger_event_locked(prepared->ledger,
            prepared->requests[0].subsystem, prepared->requests[0].bytes,
            prepared->requests[0].kind ==
            WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ALLOC);
    if (prepared->count > 1)
        wl_columnar_mem_ledger_event_locked(prepared->ledger,
            prepared->requests[1].subsystem, prepared->requests[1].bytes,
            prepared->requests[1].kind ==
            WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ALLOC);
    prepared->phase = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_APPLIED;
    return 0;
}

int
wl_columnar_mem_ledger_publication_release(
    wl_columnar_mem_ledger_publication_prepared_t *prepared)
{
    if (!wl_columnar_mem_ledger_publication_origin(prepared)
        || (prepared->phase != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_HELD
        && prepared->phase != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_APPLIED)
        || prepared->prepaid_release
        != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_RELEASE_WORK)
        return EINVAL;
    if (wl_columnar_mem_ledger_publication_gate_read(prepared->ledger) != 1)
        return EINVAL;
#ifdef _MSC_VER
    (void)atomic_exchange_explicit(&prepared->ledger->writer_gate, 0,
        memory_order_release);
#else
    atomic_store_explicit(&prepared->ledger->writer_gate, 0,
        memory_order_release);
#endif
    prepared->phase = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_CONSUMED;
    return 0;
}

int
wl_columnar_mem_ledger_publication_cancel(
    wl_columnar_mem_ledger_publication_prepared_t *prepared)
{
    if (!wl_columnar_mem_ledger_publication_origin(prepared))
        return EINVAL;
    if (prepared->phase == WL_COLUMNAR_MEM_LEDGER_PUBLICATION_HELD)
        return wl_columnar_mem_ledger_publication_release(prepared);
    if (prepared->phase != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_READY)
        return EINVAL;
    prepared->phase = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_CONSUMED;
    return 0;
}
