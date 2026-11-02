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

/* Finite, cancel-only sibling. The caller immediately applies/releases a
 * successful claim; failed admission never retries inside this invocation. */
int
wl_columnar_mem_ledger_publication_try_acquire_controlled(
    wl_columnar_mem_ledger_publication_prepared_t *prepared)
{
    if (!wl_columnar_mem_ledger_publication_origin(prepared)
        || prepared->phase != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_READY
        || prepared->acquisition_started
        || prepared->prepaid_initial
        != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_INITIAL_WORK)
        return EINVAL;
    prepared->acquisition_started = true;
    uint64_t active =
        wl_evaluation_control_active_attempt_id(prepared->control);
    if (!active || active != prepared->attempt_id) return EINVAL;
    int rc = wl_evaluation_control_charge_attempt(prepared->control,
            prepared->attempt_id,
            WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ROUND_WORK);
    if (rc) return rc;
    rc = wl_columnar_mem_ledger_publication_checkpoint(prepared->control,
            prepared, prepared->attempt_id,
            WL_COLUMNAR_MEM_LEDGER_PUBLICATION_PRECAS);
    if (rc) return rc;
    uint64_t expected = 0;
    if (!wl_columnar_mem_ledger_publication_strong(prepared->ledger, &expected))
        return EAGAIN;
    prepared->phase = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_HELD;
    return 0;
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

/* Finite sibling for three ordered events on at most two actual ledgers.
 * Future capacities are actually paid before READY; none is a fake counter.
 * Existing max2 publication record, tariffs and source bodies stay unchanged.
 * The acquire pair never invokes a hook while any writer gate is held.
 * Logical source envelopes (control/instrumentation internals excluded):
 * INIT192: meaningful record/native/ref setup<=88, fixed argument/charge
 * continuation/refusal/initialization loops<=64, count0 sort/tail<=24 =>176.
 * COPY192 per requested slot: validation7+coalesce12+four-field copy/count5,
 * loop/error/leave/saved-attempt/caller<=50, canonical sort<=12 =>86.
 * ACQUIRE192: origin/credit guards24+saved attempt/zero16+three slot checks45
 * +canonical checks22+twoCAS/rollback/state40+caller24 =>171.
 * APPLY192: three ordered(event33+dispatch15)=144+phase/credit/caller24=168.
 * RELEASE96: two gate groups24+checked cancel/common40+terminal24=88;
 * failed second-gate rollback spends12, subsequent no-gate cancel<=64 of84.
 * ACK192: explicit3slot/twoledger/meaningful field clear<=80+origin/ref/caller
 * <=80 =>160. No bulk opaque clear or hook occurs under held ledger gates.
 * Actual success is864+192*count through separate positive debits<=192;
 * total call work is not256. Initial immutable address guards are the fixed
 * read-only protocol-bootstrap refusal boundary inventoried at entry. */
enum {
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_INIT_WORK = 192,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_COPY_WORK = 192,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_ACQUIRE_WORK = 192,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_APPLY_WORK = 192,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_RELEASE_WORK = 96,
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_ACK_WORK = 192
};
#ifdef WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TEST_HOOKS
void (*wl_columnar_mem_ledger_publication_transfer_test_checkpoint)(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *,
    wl_evaluation_control_t *, unsigned);
#define WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_HOOK(p, c, phase) do { \
            if (wl_columnar_mem_ledger_publication_transfer_test_checkpoint) \
            wl_columnar_mem_ledger_publication_transfer_test_checkpoint(p, c, \
                phase); \
} while (0)
#else
#define WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_HOOK(p, c, phase) ((void)0)
#endif

static bool
wl_columnar_mem_ledger_publication_transfer_overlap(const void *a, size_t an,
    const void *b, size_t bn)
{
    if (!a || !b || !an || !bn) return false;
    uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
    if (an - 1 > UINTPTR_MAX - x || bn - 1 > UINTPTR_MAX - y) return true;
    return x <= y + bn - 1 && y <= x + an - 1;
}

static bool
wl_columnar_mem_ledger_publication_transfer_origin(
    const wl_columnar_mem_ledger_publication_transfer_prepared_t *p)
{
    if (!p || p->identity != p || !p->control_ref || !p->control ||
        !p->thread_valid)
        return false;
#if defined(WL_HAVE_C11_THREADS)
    return thrd_equal(p->owner_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return p->owner_thread == GetCurrentThreadId();
#else
    return pthread_equal(p->owner_thread, pthread_self()) != 0;
#endif
}

static int
wl_columnar_mem_ledger_publication_transfer_enter(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *p)
{
    if (!p) return EINVAL;
    if (p->busy) return EBUSY;
    if (!wl_columnar_mem_ledger_publication_transfer_origin(p)) return EINVAL;
    p->busy = true; return 0;
}

static int
wl_columnar_mem_ledger_publication_transfer_leave(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *p, int rc)
{
    p->busy = false; return rc;
}

static int
wl_columnar_mem_ledger_publication_transfer_error(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *p, int rc)
{
    if (!p->semantic_result) p->semantic_result = rc;
    p->phase = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_CANCEL_ONLY;
    return wl_columnar_mem_ledger_publication_transfer_leave(p, rc);
}

static void
wl_columnar_mem_ledger_publication_transfer_unlock(wl_mem_ledger_t *ledger)
{
#ifdef _MSC_VER
    (void)atomic_exchange_explicit(&ledger->writer_gate, 0,
        memory_order_release);
#else
    atomic_store_explicit(&ledger->writer_gate, 0, memory_order_release);
#endif
}

int
wl_columnar_mem_ledger_publication_transfer_prepare_controlled(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *p,
    const wl_columnar_mem_ledger_publication_transfer_request_t *requests,
    unsigned count, wl_evaluation_control_t *control, uint64_t id)
{
    /* Fixed read-only safety bootstrap, <=240 source operations including
     * seven overlap calls<=140, loop/overflow/inert/origin checks<=80 and
     * known-control-base checks<=20. No mutation/hook/debit/retain, no target
     * ledger contents and no semantic request work in this refusal boundary.
     * Opaque control allocation size is unknown; full disjointness remains
     * the caller's contract. Base checks deliberately claim nothing stronger. */
    if (!p || !control || !id || count > 3 ||
        (count && !requests)) return EINVAL;
    uintptr_t record_base = (uintptr_t)p, request_base = (uintptr_t)requests;
    size_t request_bytes = count * sizeof(*requests);
    if (sizeof(*p)-1 > UINTPTR_MAX-record_base
        || (request_bytes &&
        request_bytes-1 > UINTPTR_MAX-request_base)) return EINVAL;
    if (p->busy) return EBUSY;
    if (p->identity && p->identity != p) return EINVAL;
    if (p->identity || p->control || p->control_ref || p->thread_valid
        || p->held_mask || p->sealed
        || p->phase !=
        WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_INERT) return EBUSY;
    if (wl_columnar_mem_ledger_publication_transfer_overlap(p, sizeof(*p),
        requests, request_bytes))
        return EINVAL;
    uintptr_t control_base = (uintptr_t)control;
    if ((control_base >= record_base &&
        control_base <= record_base+sizeof(*p)-1)
        || (request_bytes && control_base >= request_base &&
        control_base <= request_base+request_bytes-1))
        return EINVAL;
    for (unsigned i = 0; i < count; ++i) {
        if (wl_columnar_mem_ledger_publication_transfer_overlap(p, sizeof(*p),
            requests[i].ledger,
            sizeof(wl_mem_ledger_t))
            || wl_columnar_mem_ledger_publication_transfer_overlap(requests,
            request_bytes,
            requests[i].ledger, sizeof(wl_mem_ledger_t))) return EINVAL;
    }
    uint64_t control_identity = wl_evaluation_control_identity(control);
    p->busy = true;
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_HOOK(p, control, 1);
    int rc = wl_evaluation_control_charge_attempt(control, id,
            WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_INIT_WORK);
    if (rc) return wl_columnar_mem_ledger_publication_transfer_leave(p, rc);
    rc = wl_evaluation_control_charge_attempt(control, id,
            WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_RELEASE_WORK);
    if (!rc) rc = wl_evaluation_control_charge_attempt(control, id,
                WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_ACK_WORK);
    if (!rc && (wl_evaluation_control_identity(control) != control_identity
        || wl_evaluation_control_active_attempt_id(control) != id)) rc = EINVAL;
    if (!rc) rc = wl_evaluation_control_charge_attempt(control, id, 0);
    if (rc) return wl_columnar_mem_ledger_publication_transfer_leave(p, rc);
    rc = wl_evaluation_control_retain(control);
    if (rc) return wl_columnar_mem_ledger_publication_transfer_leave(p, rc);
    p->identity = p; p->control = control; p->control_ref = true;
    p->control_identity = control_identity; p->attempt_id = id;
#if defined(WL_HAVE_C11_THREADS)
    p->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    p->owner_thread = GetCurrentThreadId();
#else
    p->owner_thread = pthread_self();
#endif
    p->thread_valid = true; p->acquisition_started = false; p->sealed = false;
    p->count = 0; p->unique_count = 0; p->held_mask = 0;
    p->prepaid_acquire = p->prepaid_apply = 0;
    p->prepaid_release =
        WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_RELEASE_WORK;
    p->prepaid_ack = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_ACK_WORK;
    p->semantic_result = 0;
    p->phase = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_CANCEL_ONLY;
    p->ledgers[0] = p->ledgers[1] = NULL;
    for (unsigned i = 0; i < 3; ++i)
        p->requests[i] =
            (wl_columnar_mem_ledger_publication_transfer_request_t){0};
    for (unsigned i = 0; i < count; ++i) {
        rc = wl_evaluation_control_charge_attempt(control, id,
                WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_COPY_WORK);
        if (rc) return wl_columnar_mem_ledger_publication_transfer_error(p, rc);
        if (!requests[i].ledger ||
            !wl_columnar_mem_ledger_publication_request_valid(
                &requests[i].event))
            return wl_columnar_mem_ledger_publication_transfer_error(p, EINVAL);
        if (requests[i].ledger != p->ledgers[0] &&
            requests[i].ledger != p->ledgers[1]){
            if (p->unique_count ==
                2) return wl_columnar_mem_ledger_publication_transfer_error(p,
                           EINVAL);
            p->ledgers[p->unique_count++] = requests[i].ledger;
        }
        p->requests[i] = requests[i]; p->count++;
    }
    if (p->unique_count == 2 &&
        (uintptr_t)p->ledgers[0] > (uintptr_t)p->ledgers[1]){
        wl_mem_ledger_t *swap = p->ledgers[0]; p->ledgers[0] = p->ledgers[1];
        p->ledgers[1] = swap;
    }
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_HOOK(p, control, 2);
    rc = wl_evaluation_control_charge_attempt(control, id,
            WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_APPLY_WORK);
    if (rc) return wl_columnar_mem_ledger_publication_transfer_error(p, rc);
    p->prepaid_apply = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_APPLY_WORK;
    rc = wl_evaluation_control_charge_attempt(control, id,
            WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_ACQUIRE_WORK);
    if (rc) return wl_columnar_mem_ledger_publication_transfer_error(p, rc);
    p->prepaid_acquire =
        WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_ACQUIRE_WORK;
    p->phase = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_READY;
    return wl_columnar_mem_ledger_publication_transfer_leave(p, 0);
}

int
wl_columnar_mem_ledger_publication_transfer_acquire_pair(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *p)
{
    int rc = wl_columnar_mem_ledger_publication_transfer_enter(p);
    if (rc) return rc;
    if (p->phase != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_READY ||
        p->acquisition_started
        || p->prepaid_acquire !=
        WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_ACQUIRE_WORK
        || p->prepaid_apply !=
        WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_APPLY_WORK
        || p->prepaid_release !=
        WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_RELEASE_WORK
        || p->prepaid_ack !=
        WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_ACK_WORK)
        return wl_columnar_mem_ledger_publication_transfer_leave(p, EBUSY);
    p->acquisition_started = true; p->prepaid_acquire = 0;
    WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_HOOK(p, p->control, 3);
    if (wl_evaluation_control_identity(p->control) != p->control_identity
        || wl_evaluation_control_active_attempt_id(p->control) != p->attempt_id)
        return wl_columnar_mem_ledger_publication_transfer_error(p, EINVAL);
    rc = wl_evaluation_control_charge_attempt(p->control, p->attempt_id, 0);
    if (rc) return wl_columnar_mem_ledger_publication_transfer_error(p, rc);
    if (p->count > 3 || p->unique_count > 2 || p->unique_count > p->count
        || p->held_mask || p->sealed
        || (!p->unique_count && (p->ledgers[0] || p->ledgers[1]))
        || (p->unique_count == 1 && p->ledgers[1])
        || (p->unique_count && !p->ledgers[0])
        || (p->unique_count == 2 && (!p->ledgers[1]
        || (uintptr_t)p->ledgers[0] >= (uintptr_t)p->ledgers[1])))
        return wl_columnar_mem_ledger_publication_transfer_error(p, EINVAL);
    for (unsigned i = 0; i < p->count; ++i)
        if (!wl_columnar_mem_ledger_publication_request_valid(
                &p->requests[i].event)
            || !p->requests[i].ledger
            || (p->requests[i].ledger != p->ledgers[0] &&
            p->requests[i].ledger != p->ledgers[1]))
            return wl_columnar_mem_ledger_publication_transfer_error(p, EINVAL);


    /* SEALED: no call below can poll, charge, hook, allocate, log or wait.
     * A failed gate1 releases gate0 immediately, before ordinary error return. */
    uint64_t expected = 0;
    if (p->unique_count &&
        !wl_columnar_mem_ledger_publication_strong(p->ledgers[0], &expected))
        return wl_columnar_mem_ledger_publication_transfer_error(p, EAGAIN);
    if (p->unique_count) p->held_mask = 1;
    if (p->unique_count == 2) {
        expected = 0;
        if (!wl_columnar_mem_ledger_publication_strong(p->ledgers[1],
            &expected)){
            wl_columnar_mem_ledger_publication_transfer_unlock(p->ledgers[0]);
            p->held_mask = 0; p->prepaid_release -= 12;
            return wl_columnar_mem_ledger_publication_transfer_error(p, EAGAIN);
        }
        p->held_mask = 3;
    }
    p->sealed = true;
    p->phase = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_HELD;
    return wl_columnar_mem_ledger_publication_transfer_leave(p, 0);
}

void
wl_columnar_mem_ledger_publication_transfer_apply_prepared_nofail(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *p)
{
    /* Valid original sealed HELD capability only, immediate no-hook interval. */
    p->busy = true;
    for (unsigned i = 0; i < p->count; ++i)
        wl_columnar_mem_ledger_event_locked(p->requests[i].ledger,
            p->requests[i].event.subsystem, p->requests[i].event.bytes,
            p->requests[i].event.kind ==
            WL_COLUMNAR_MEM_LEDGER_PUBLICATION_ALLOC);
    p->prepaid_apply = 0;
    p->phase = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_APPLIED;
    p->busy = false;
}

void
wl_columnar_mem_ledger_publication_transfer_release_prepared_nofail(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *p)
{
    p->busy = true;
    if (p->held_mask &
        2) wl_columnar_mem_ledger_publication_transfer_unlock(p->ledgers[1]);
    if (p->held_mask &
        1) wl_columnar_mem_ledger_publication_transfer_unlock(p->ledgers[0]);
    p->held_mask = 0; p->sealed = false; p->prepaid_release = 0;
    p->prepaid_acquire = p->prepaid_apply = 0;
    p->phase = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_CONSUMED;
    p->busy = false;
}

int
wl_columnar_mem_ledger_publication_transfer_cancel(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *p)
{
    int rc = wl_columnar_mem_ledger_publication_transfer_enter(p);
    if (rc) return rc;
    if ((p->phase != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_READY
        && p->phase != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_CANCEL_ONLY
        && p->phase != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_HELD)
        || p->prepaid_release < 64)
        return wl_columnar_mem_ledger_publication_transfer_leave(p, EBUSY);
    wl_columnar_mem_ledger_publication_transfer_release_prepared_nofail(p);
    return 0;
}

int
wl_columnar_mem_ledger_publication_transfer_ack(
    wl_columnar_mem_ledger_publication_transfer_prepared_t *p)
{
    int rc = wl_columnar_mem_ledger_publication_transfer_enter(p);
    if (rc) return rc;
    if (p->phase != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_CONSUMED ||
        p->held_mask
        || p->sealed ||
        p->prepaid_ack != WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_ACK_WORK)
        return wl_columnar_mem_ledger_publication_transfer_leave(p, EBUSY);
    wl_evaluation_control_t *owned = p->control;
    for (unsigned i = 0; i < 3; ++i)
        p->requests[i] =
            (wl_columnar_mem_ledger_publication_transfer_request_t){0};
    p->ledgers[0] = p->ledgers[1] = NULL; p->count = p->unique_count = 0;
    p->prepaid_acquire = p->prepaid_apply = p->prepaid_release =
        p->prepaid_ack = 0;
    p->identity = NULL; p->control = NULL; p->control_ref = false;
    p->control_identity = p->attempt_id = 0;
    p->thread_valid = p->acquisition_started = false; p->semantic_result = 0;
    p->phase = WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_INERT;
    wl_evaluation_control_release(owned);
    return wl_columnar_mem_ledger_publication_transfer_leave(p, 0);
}

int
wl_columnar_mem_ledger_publication_transfer_query(
    const wl_columnar_mem_ledger_publication_transfer_prepared_t *p,
    wl_columnar_mem_ledger_publication_transfer_status_t *status)
{
    if (!p) return EINVAL;
    if (p->busy) return EBUSY;
    if (!wl_columnar_mem_ledger_publication_transfer_origin(p)) return EINVAL;
    if (!status || wl_columnar_mem_ledger_publication_transfer_overlap(p,
        sizeof(*p), status, sizeof(*status))
        || wl_columnar_mem_ledger_publication_transfer_overlap(status,
        sizeof(*status), p->ledgers[0], sizeof(wl_mem_ledger_t))
        || wl_columnar_mem_ledger_publication_transfer_overlap(status,
        sizeof(*status), p->ledgers[1], sizeof(wl_mem_ledger_t))
        || wl_columnar_mem_ledger_publication_transfer_overlap(status,
        sizeof(*status), p->control, 1))
        return EINVAL;
    *status = (wl_columnar_mem_ledger_publication_transfer_status_t){
        p->phase, p->count, p->unique_count, p->held_mask,
        p->control_identity, p->attempt_id, p->semantic_result
    };
    return 0;
}
#undef WL_COLUMNAR_MEM_LEDGER_PUBLICATION_TRANSFER_HOOK
