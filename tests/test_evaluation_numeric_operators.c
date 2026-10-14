/* #1821 cooperative numeric FILTER: semantic, lifetime and literal-budget
 * fixtures. Each operator dispatch returns to an outer FIFO. Independent jobs
 * witness the scheduling boundary; an inline while(YIELD) cannot reuse this
 * per-dispatch contract. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/thread.h"
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define HOST_QUEUE_CAPACITY 8u

typedef struct host_queue host_queue_t;
typedef struct host_job host_job_t;
struct host_job {
    void (*dispatch)(host_queue_t *, host_job_t *);
    void *data;
    bool queued;
};
struct host_queue {
    host_job_t *jobs[HOST_QUEUE_CAPACITY];
    unsigned head, count;
    uint64_t turn;
};

typedef struct {
    host_job_t job;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    wl_col_session_t *session;
    const wl_plan_op_t *op;
    wl_evaluation_control_t *external_control;
    uint64_t original_id;
    wl_columnar_filter_status_t status;
    int protocol_result;
    bool started, parked, done;
    uint64_t last_turn;
} host_filter_entry_t;

static int
host_enqueue(host_queue_t *queue, host_job_t *job)
{
    if (!queue || !job || job->queued) return EINVAL;
    if (queue->count == HOST_QUEUE_CAPACITY) return ENOBUFS;
    unsigned index = (queue->head + queue->count) % HOST_QUEUE_CAPACITY;
    queue->jobs[index] = job;
    ++queue->count;
    job->queued = true;
    return 0;
}

/* This canonical adapter does not own or retire the frame/control. The host
 * entry remains reachable on protocol/queue errors and RETAINED. Report and
 * generic frame destruction are separate host jobs after acknowledged DONE. */
static void
host_filter_dispatch(host_queue_t *queue, host_job_t *job)
{
    host_filter_entry_t *entry = job->data;
    wl_columnar_filter_status_t status;
    int protocol = entry->started
        ? wl_columnar_filter_op_attempt_step(entry->frame, &status)
        : wl_columnar_filter_op_attempt_start(entry->op, entry->frame,
            entry->session, entry->external_control, entry->original_id,
            &status);
    entry->last_turn = queue->turn;
    if (protocol) {
        entry->protocol_result = protocol;
        entry->parked = true;
        return;
    }
    entry->status = status;
    entry->started = true;
    if (status.progress == WL_COLUMNAR_FILTER_PROGRESS_YIELD) {
        protocol = host_enqueue(queue, job);
        entry->protocol_result = protocol;
        entry->parked = protocol != 0;
        return;
    }
    if (status.progress == WL_COLUMNAR_FILTER_PROGRESS_RETAINED) {
        entry->protocol_result = 0;
        entry->parked = true;
        return;
    }
    if (status.progress != WL_COLUMNAR_FILTER_PROGRESS_DONE) {
        entry->protocol_result = EINVAL;
        entry->parked = true;
        return;
    }
    protocol = wl_columnar_filter_op_attempt_ack(entry->frame);
    entry->protocol_result = protocol;
    entry->parked = protocol != 0;
    entry->done = protocol == 0;
    return;
}

/* Report owns an independently retained live reference, not an expired
 * external-control pointer. A hook may instead supply copied historical data.
 * The attached owner key remains a serialized host lifecycle responsibility. */
typedef struct {
    host_job_t job;
    host_filter_entry_t *filter;
    wl_evaluation_control_t *control;
    const void *owner;
    uint64_t original_id;
    wl_evaluation_control_report_t historical;
    int protocol_result;
    int continuation_result;
    bool ref_owned, historical_available, done, parked;
} host_report_entry_t;

static void
host_report_dispatch(host_queue_t *queue, host_job_t *job)
{
    (void)queue;
    host_report_entry_t *entry = job->data;
    if (!entry->filter->done) {
        entry->protocol_result = EBUSY;
        entry->parked = true;
        return;
    }
    entry->continuation_result = entry->filter->status.semantic_result;
    if (entry->ref_owned) {
        if (!entry->control || !entry->owner || !entry->original_id) {
            entry->protocol_result = EINVAL;
            entry->parked = true;
            return;
        }
        wl_evaluation_control_t *control = entry->control;
        uint64_t active = wl_evaluation_control_active_attempt_id(control);
        bool attempted = active == entry->original_id;
        int finish_result = 0;
        if (attempted)
            finish_result = wl_evaluation_control_finish(control, entry->owner,
                    entry->filter->status.execution_result,
                    entry->filter->status.cleanup_result);
        /* Finish returns the completed semantic result, including negatives
         * and positive errors. Verify actual completion before releasing ref. */
        uint64_t after = attempted
            ? wl_evaluation_control_active_attempt_id(control) : active;
        wl_evaluation_control_report_t report;
        int report_result = wl_evaluation_control_report(control, &report);
        if (attempted && (after || report_result
            || report.attempt_id != entry->original_id
            || report.result != finish_result)) {
            entry->protocol_result = finish_result ? finish_result : EINVAL;
            entry->parked = true;
            return;
        }
        if (!report_result && report.attempt_id == entry->original_id) {
            entry->historical = report;
            entry->historical_available = true;
        }
        /* An overwritten current slot does not erase genuine earlier history. */
        /* A successor report is never relabeled as the original result. */
        entry->control = NULL;
        entry->ref_owned = false;
        wl_evaluation_control_release(control);
    }
    entry->protocol_result = 0;
    entry->parked = false;
    entry->done = true;
    return;
}

/* This certificate's destruction fixture uses exactly one frame and at most
 * one relation stack entry, no continuation entries or other active/pending
 * frames. General stack/pending-list closure is not certified by this job.
 * Post-unlink refusal transfers authority to session.pending, not the handle. */
typedef struct {
    host_job_t job;
    host_filter_entry_t *filter;
    wl_col_session_t *session;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    int protocol_result;
    bool pending, done, parked;
} host_destroy_entry_t;

static void
host_destroy_dispatch(host_queue_t *queue, host_job_t *job)
{
    (void)queue;
    host_destroy_entry_t *entry = job->data;
    if (!entry->filter->done || !entry->session
        || entry->session->cleanup_active_count > 1
        || entry->session->cleanup_pending_count > 1) {
        entry->protocol_result = EBUSY;
        entry->parked = true;
        return;
    }
    int rc;
    if (entry->pending) {
        if (entry->frame || entry->session->cleanup_active) {
            entry->protocol_result = EBUSY;
            entry->parked = true;
            return;
        }
        rc = wl_columnar_eval_stack_cleanup_retry(entry->session);
        entry->pending = entry->session->cleanup_pending_count != 0;
    } else {
        if (!entry->frame || entry->session->cleanup_pending_count) {
            entry->protocol_result = EINVAL;
            entry->parked = true;
            return;
        }
        eval_stack_t *stack =
            wl_columnar_eval_stack_cleanup_stack(entry->frame);
        if (stack->top > 1 || (stack->top
            && stack->items[0].kind != WL_COLUMNAR_EVAL_ENTRY_RELATION)) {
            entry->protocol_result = ENOTSUP;
            entry->parked = true;
            return;
        }
        /* Erase the adapter's borrowed pointer before any physical disposal. */
        entry->filter->frame = NULL;
        rc = wl_columnar_eval_stack_cleanup_finish(&entry->frame);
        if (entry->frame)
            entry->filter->frame = entry->frame;
        else
            entry->pending = entry->session->cleanup_pending_count != 0;
    }
    entry->protocol_result = rc;
    entry->parked = rc != 0 || entry->pending || entry->frame != NULL;
    entry->done = !entry->parked;
    return;
}

/* Independent FIFO sentinel proves another job runs between filter turns. */
typedef struct {
    host_job_t job;
    uint64_t visits, last_turn;
    int protocol_result;
} host_sentinel_entry_t;

static void
host_sentinel_dispatch(host_queue_t *queue, host_job_t *job)
{
    host_sentinel_entry_t *entry = job->data;
    ++entry->visits;
    entry->last_turn = queue->turn;
    entry->protocol_result = 0;
    return;
}

/* A credit finances one separate scheduling action and one future dispatch.
 * The host owns every referenced job independently and keeps it alive until
 * all unused credits have been consumed or cancelled. Admission requires an
 * exclusive caller-owned control reference throughout the call. */
typedef enum {
    WL_COLUMNAR_FILTER_HOST_REPORT,
    WL_COLUMNAR_FILTER_HOST_DESTROY,
    WL_COLUMNAR_FILTER_HOST_RETRY,
    WL_COLUMNAR_FILTER_HOST_SENTINEL,
    WL_COLUMNAR_FILTER_HOST_CALLER_ACTION
} wl_columnar_filter_host_lifecycle_kind_t;

static void wl_columnar_filter_host_caller_action_dispatch(host_queue_t *,
    host_job_t *);

typedef enum {
    WL_COLUMNAR_FILTER_HOST_EMPTY,
    WL_COLUMNAR_FILTER_HOST_PREPARING,
    WL_COLUMNAR_FILTER_HOST_ADMITTED,
    WL_COLUMNAR_FILTER_HOST_SPENT,
    WL_COLUMNAR_FILTER_HOST_CANCELLED
} wl_columnar_filter_host_lifecycle_phase_t;

typedef struct wl_columnar_filter_host_lifecycle_credit {
    struct wl_columnar_filter_host_lifecycle_credit *identity;
    host_job_t *job;
    wl_columnar_filter_host_lifecycle_kind_t kind;
    wl_columnar_filter_host_lifecycle_phase_t phase;
    uint64_t original_id, future_work;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    bool thread_valid;
} wl_columnar_filter_host_lifecycle_credit_t;

enum {
    WL_COLUMNAR_FILTER_HOST_SETUP_WORK = 128,
    WL_COLUMNAR_FILTER_HOST_SCHEDULE_AND_CLOSE_WORK = 128,
    WL_COLUMNAR_FILTER_HOST_JOB_WORK = 256,
    WL_COLUMNAR_FILTER_HOST_SENTINEL_WORK = 35
};

static bool
wl_columnar_filter_host_lifecycle_thread(
    const wl_columnar_filter_host_lifecycle_credit_t *credit)
{
    if (!credit->thread_valid) return false;
#if defined(WL_HAVE_C11_THREADS)
    return thrd_equal(credit->owner_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return credit->owner_thread == GetCurrentThreadId();
#else
    return pthread_equal(credit->owner_thread, pthread_self()) != 0;
#endif
}

static bool
wl_columnar_filter_host_lifecycle_job(host_job_t *job,
    wl_columnar_filter_host_lifecycle_kind_t kind)
{
    if (!job || !job->data) return false;
    switch (kind) {
    case WL_COLUMNAR_FILTER_HOST_REPORT:
        return job->dispatch == host_report_dispatch;
    case WL_COLUMNAR_FILTER_HOST_DESTROY:
        return job->dispatch == host_destroy_dispatch;
    case WL_COLUMNAR_FILTER_HOST_RETRY:
        return job->dispatch == host_filter_dispatch
               || job->dispatch == host_destroy_dispatch;
    case WL_COLUMNAR_FILTER_HOST_SENTINEL:
        return job->dispatch == host_sentinel_dispatch;
    case WL_COLUMNAR_FILTER_HOST_CALLER_ACTION:
        return job->dispatch == wl_columnar_filter_host_caller_action_dispatch;
    default:
        return false;
    }
}

static int
wl_columnar_filter_host_lifecycle_charge(wl_evaluation_control_t *control,
    uint64_t original_id, uint64_t work)
{
    if (!control || !original_id
        || wl_evaluation_control_active_attempt_id(control) != original_id)
        return EINVAL;
    return wl_evaluation_control_charge(control, work);
}

/* Instrumentation may end the original attempt, never release the caller's
 * exclusive reference. Every later debit checks the saved original ID. */
static void (*wl_columnar_filter_host_lifecycle_checkpoint)(
    wl_evaluation_control_t *, unsigned);

static int
wl_columnar_filter_host_lifecycle_admit(
    wl_columnar_filter_host_lifecycle_credit_t *credit, host_job_t *job,
    wl_columnar_filter_host_lifecycle_kind_t kind,
    wl_evaluation_control_t *control, uint64_t original_id)
{
    if (!credit || credit->identity || credit->job || credit->kind
        || credit->phase || credit->original_id || credit->future_work
        || credit->thread_valid || !control || !original_id
        || !wl_columnar_filter_host_lifecycle_job(job, kind))
        return EINVAL;
    int rc = wl_evaluation_control_retain(control);
    if (rc) return rc;
    wl_evaluation_control_t *pin = control;
    rc = wl_columnar_filter_host_lifecycle_charge(pin, original_id,
            WL_COLUMNAR_FILTER_HOST_SETUP_WORK);
    if (rc) goto close;
    credit->identity = credit;
    credit->job = job;
    credit->kind = kind;
    credit->original_id = original_id;
    credit->future_work = kind == WL_COLUMNAR_FILTER_HOST_SENTINEL
        ? WL_COLUMNAR_FILTER_HOST_SENTINEL_WORK
        : WL_COLUMNAR_FILTER_HOST_JOB_WORK;
    credit->phase = WL_COLUMNAR_FILTER_HOST_PREPARING;
#if defined(WL_HAVE_C11_THREADS)
    credit->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    credit->owner_thread = GetCurrentThreadId();
#else
    credit->owner_thread = pthread_self();
#endif
    credit->thread_valid = true;
    if (wl_columnar_filter_host_lifecycle_checkpoint)
        wl_columnar_filter_host_lifecycle_checkpoint(pin, 0);
    rc = wl_columnar_filter_host_lifecycle_charge(pin, original_id, 0);
    if (rc) goto cancel;
    rc = wl_columnar_filter_host_lifecycle_charge(pin, original_id,
            WL_COLUMNAR_FILTER_HOST_SCHEDULE_AND_CLOSE_WORK);
    if (rc) goto cancel;
    if (wl_columnar_filter_host_lifecycle_checkpoint)
        wl_columnar_filter_host_lifecycle_checkpoint(pin, 1);
    rc = wl_columnar_filter_host_lifecycle_charge(pin, original_id,
            credit->future_work);
    if (rc) goto cancel;
    /* No callback intervenes between successful payment and publication. */
    credit->phase = WL_COLUMNAR_FILTER_HOST_ADMITTED;
    goto close;
cancel:
    credit->job = NULL;
    credit->phase = WL_COLUMNAR_FILTER_HOST_CANCELLED;
close:
    control = pin;
    pin = NULL;
    wl_evaluation_control_release(control);
    return rc;
}

/* Valid records fund exactly one invocation. A terminal/copied/wrong-thread
 * diagnostic call requires separate caller funding, as does credit exhaustion.
 * Enqueue does not dispatch inline; the externally owned job stays reachable
 * on refusal and needs a distinct pre-admitted credit for another attempt. */
static int
wl_columnar_filter_host_lifecycle_schedule(host_queue_t *queue,
    wl_columnar_filter_host_lifecycle_credit_t *credit)
{
    if (!credit || credit->identity != credit
        || credit->phase != WL_COLUMNAR_FILTER_HOST_ADMITTED
        || !wl_columnar_filter_host_lifecycle_thread(credit))
        return EINVAL;
    host_job_t *job = credit->job;
    credit->phase = WL_COLUMNAR_FILTER_HOST_SPENT;
    credit->job = NULL;
    if (!wl_columnar_filter_host_lifecycle_job(job, credit->kind))
        return EINVAL;
    return host_enqueue(queue, job);
}

static int
wl_columnar_filter_host_lifecycle_cancel(
    wl_columnar_filter_host_lifecycle_credit_t *credit)
{
    if (!credit || credit->identity != credit
        || credit->phase != WL_COLUMNAR_FILTER_HOST_ADMITTED
        || !wl_columnar_filter_host_lifecycle_thread(credit))
        return EINVAL;
    credit->phase = WL_COLUMNAR_FILTER_HOST_CANCELLED;
    credit->job = NULL;
    return 0;
}

typedef enum {
    WL_COLUMNAR_FILTER_HOST_CALLER_INERT,
    WL_COLUMNAR_FILTER_HOST_CALLER_READY,
    WL_COLUMNAR_FILTER_HOST_CALLER_LAUNCHED,
    WL_COLUMNAR_FILTER_HOST_CALLER_ACKNOWLEDGED,
    WL_COLUMNAR_FILTER_HOST_CALLER_REPORTED,
    WL_COLUMNAR_FILTER_HOST_CALLER_DESTROYED,
    WL_COLUMNAR_FILTER_HOST_CALLER_CLOSED
} wl_columnar_filter_host_caller_phase_t;

typedef struct wl_columnar_filter_host_caller {
    struct wl_columnar_filter_host_caller *identity;
    host_filter_entry_t *filter;
    wl_col_session_t *session;
    wl_evaluation_control_t *operator_control, *funding_control;
    uint64_t operator_id, funding_id;
    wl_columnar_filter_host_caller_phase_t phase;
    wl_columnar_filter_host_lifecycle_credit_t *pending_credit;
    host_job_t *pending_job;
    int protocol_result;
    bool result_saved;
#if defined(WL_HAVE_C11_THREADS)
    thrd_t owner_thread;
#elif defined(_WIN32) || defined(_WIN64)
    DWORD owner_thread;
#else
    pthread_t owner_thread;
#endif
    bool thread_valid;
} wl_columnar_filter_host_caller_t;

static bool wl_columnar_filter_host_caller_action_belongs(host_job_t *,
    const wl_columnar_filter_host_caller_t *);

static bool
wl_columnar_filter_host_caller_thread(
    const wl_columnar_filter_host_caller_t *caller)
{
    if (!caller || caller->identity != caller || !caller->thread_valid)
        return false;
#if defined(WL_HAVE_C11_THREADS)
    return thrd_equal(caller->owner_thread, thrd_current()) != 0;
#elif defined(_WIN32) || defined(_WIN64)
    return caller->owner_thread == GetCurrentThreadId();
#else
    return pthread_equal(caller->owner_thread, pthread_self()) != 0;
#endif
}

/* Preparation is a local transaction. The original caller is inert on every
 * failure; two equal control pointers still designate two acquired references.
 * Inputs and caller/filter/session storage remain externally owned. */
static int
wl_columnar_filter_host_caller_prepare(wl_columnar_filter_host_caller_t *caller,
    host_filter_entry_t *filter, wl_evaluation_control_t *operator_control,
    uint64_t operator_id, wl_evaluation_control_t *funding_control,
    uint64_t funding_id)
{
    wl_evaluation_control_t *op_pin = NULL, *funding_pin = NULL;
    bool op_acquired = false, funding_acquired = false;
    int rc;
    if (!caller || caller->identity || caller->filter || caller->session
        || caller->operator_control || caller->funding_control
        || caller->operator_id || caller->funding_id || caller->phase
        || caller->pending_credit || caller->pending_job
        || caller->protocol_result || caller->result_saved ||
        caller->thread_valid
        || !filter || !filter->frame || !filter->session || !filter->op
        || filter->job.dispatch != host_filter_dispatch
        || filter->job.data != filter || filter->job.queued || filter->started
        || filter->done || !operator_control || !funding_control
        || !operator_id || !funding_id
        || filter->external_control != operator_control
        || filter->original_id != operator_id)
        return EINVAL;
    rc = wl_evaluation_control_retain(operator_control);
    if (rc) goto rollback;
    op_pin = operator_control;
    op_acquired = true;
    rc = wl_evaluation_control_retain(funding_control);
    if (rc) goto rollback;
    funding_pin = funding_control;
    funding_acquired = true;
    rc = wl_columnar_filter_host_lifecycle_charge(funding_pin, funding_id,
            WL_COLUMNAR_FILTER_HOST_SETUP_WORK);
    if (rc) goto rollback;
    if (wl_evaluation_control_active_attempt_id(op_pin) != operator_id
        || wl_evaluation_control_active_attempt_id(funding_pin) != funding_id) {
        rc = EINVAL;
        goto rollback;
    }
    /* No fallible operation or callback follows ownership transfer. */
    caller->identity = caller;
    caller->filter = filter;
    caller->session = filter->session;
    caller->operator_control = op_pin;
    caller->funding_control = funding_pin;
    caller->operator_id = operator_id;
    caller->funding_id = funding_id;
#if defined(WL_HAVE_C11_THREADS)
    caller->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    caller->owner_thread = GetCurrentThreadId();
#else
    caller->owner_thread = pthread_self();
#endif
    caller->thread_valid = true;
    op_pin = NULL;
    op_acquired = false;
    funding_pin = NULL;
    funding_acquired = false;
    caller->phase = WL_COLUMNAR_FILTER_HOST_CALLER_READY;
    return 0;
rollback:
    if (funding_acquired) {
        wl_evaluation_control_t *released = funding_pin;
        funding_pin = NULL;
        funding_acquired = false;
        wl_evaluation_control_release(released);
    }
    if (op_acquired) {
        wl_evaluation_control_t *released = op_pin;
        op_pin = NULL;
        op_acquired = false;
        wl_evaluation_control_release(released);
    }
    return rc;
}

static int
wl_columnar_filter_host_caller_admit_one(
    wl_columnar_filter_host_caller_t *caller,
    wl_columnar_filter_host_lifecycle_credit_t *credit, host_job_t *job,
    wl_columnar_filter_host_lifecycle_kind_t kind)
{
    if (!wl_columnar_filter_host_caller_thread(caller)
        || caller->phase == WL_COLUMNAR_FILTER_HOST_CALLER_INERT
        || caller->phase == WL_COLUMNAR_FILTER_HOST_CALLER_CLOSED
        || !caller->funding_control)
        return EINVAL;
    return wl_columnar_filter_host_lifecycle_admit(credit, job, kind,
               caller->funding_control, caller->funding_id);
}

/* external_lifetime_ready is a trusted assertion by the serialized external
 * owner of the COMPLETE finite handle ledger. These helpers do not infer global
 * quiescence from pending_job, SPENT, or dispatch completion. The owner freezes
 * new dependencies until destruction/close returns, and retains current action,
 * caller and session storage through all result writes and the real host return. */
static int
wl_columnar_filter_host_caller_schedule_one(
    wl_columnar_filter_host_caller_t *caller,
    host_queue_t *queue, wl_columnar_filter_host_lifecycle_credit_t *credit,
    bool external_lifetime_ready)
{
    if (!wl_columnar_filter_host_caller_thread(caller) || !credit
        || credit->identity != credit ||
        credit->original_id != caller->funding_id
        || credit->phase != WL_COLUMNAR_FILTER_HOST_ADMITTED)
        return EINVAL;
    host_job_t *job = credit->job;
    bool valid = wl_columnar_filter_host_lifecycle_job(job, credit->kind);
    if (valid) {
        switch (credit->kind) {
        case WL_COLUMNAR_FILTER_HOST_REPORT:
            valid = caller->phase == WL_COLUMNAR_FILTER_HOST_CALLER_ACKNOWLEDGED
                && ((host_report_entry_t *)job->data)->filter == caller->filter;
            break;
        case WL_COLUMNAR_FILTER_HOST_DESTROY:
            valid = external_lifetime_ready
                && caller->phase == WL_COLUMNAR_FILTER_HOST_CALLER_REPORTED
                && ((host_destroy_entry_t *)job->data)->filter ==
                caller->filter;
            break;
        case WL_COLUMNAR_FILTER_HOST_RETRY:
            if (job->dispatch == host_filter_dispatch)
                valid = job->data == caller->filter && caller->filter->started
                    && caller->filter->parked && !caller->filter->done;
            else
                valid = external_lifetime_ready
                    && job->dispatch == host_destroy_dispatch
                    && ((host_destroy_entry_t *)job->data)->filter ==
                    caller->filter
                    && ((host_destroy_entry_t *)job->data)->pending;
            break;
        case WL_COLUMNAR_FILTER_HOST_SENTINEL:
            break;
        case WL_COLUMNAR_FILTER_HOST_CALLER_ACTION:
            valid = wl_columnar_filter_host_caller_action_belongs(job, caller);
            break;
        default:
            valid = false;
            break;
        }
    }
    if (!valid) {
        int rc = wl_columnar_filter_host_lifecycle_cancel(credit);
        return rc ? rc : EBUSY;
    }
    caller->pending_credit = credit;
    caller->pending_job = job;
    return wl_columnar_filter_host_lifecycle_schedule(queue, credit);
}

static int
wl_columnar_filter_host_caller_cancel_one(
    wl_columnar_filter_host_caller_t *caller,
    wl_columnar_filter_host_lifecycle_credit_t *credit)
{
    if (!wl_columnar_filter_host_caller_thread(caller) || !credit
        || credit->original_id != caller->funding_id)
        return EINVAL;
    return wl_columnar_filter_host_lifecycle_cancel(credit);
}

typedef enum {
    WL_COLUMNAR_FILTER_HOST_CALLER_LAUNCH,
    WL_COLUMNAR_FILTER_HOST_CALLER_OBSERVE_FILTER,
    WL_COLUMNAR_FILTER_HOST_CALLER_OBSERVE_REPORT,
    WL_COLUMNAR_FILTER_HOST_CALLER_OBSERVE_DESTROY,
    WL_COLUMNAR_FILTER_HOST_CALLER_CLOSE_OPERATOR,
    WL_COLUMNAR_FILTER_HOST_CALLER_CLOSE_FUNDING,
    WL_COLUMNAR_FILTER_HOST_CALLER_CLOSE_HANDLE,
    WL_COLUMNAR_FILTER_HOST_CALLER_DIAGNOSTIC
} wl_columnar_filter_host_caller_operation_t;

typedef struct wl_columnar_filter_host_caller_action {
    host_job_t job;
    struct wl_columnar_filter_host_caller_action *identity;
    wl_columnar_filter_host_caller_t *caller;
    wl_columnar_filter_host_caller_operation_t operation;
    host_job_t *target;
    /* Trusted COMPLETE external ledger assertion; not a checked certificate. */
    bool external_lifetime_ready;
    int protocol_result;
    bool done;
} wl_columnar_filter_host_caller_action_t;

static bool
wl_columnar_filter_host_caller_action_belongs(host_job_t *job,
    const wl_columnar_filter_host_caller_t *caller)
{
    wl_columnar_filter_host_caller_action_t *action = job->data;
    return action->identity == action && &action->job == job
           && action->caller == caller && !action->done;
}

static void
wl_columnar_filter_host_caller_action_dispatch(host_queue_t *queue,
    host_job_t *job)
{
    wl_columnar_filter_host_caller_action_t *action = job->data;
    wl_columnar_filter_host_caller_t *caller = action->caller;
    int rc = EINVAL;
    if (action->identity != action || &action->job != job || action->done
        || !wl_columnar_filter_host_caller_thread(caller))
        goto done;
    switch (action->operation) {
    case WL_COLUMNAR_FILTER_HOST_CALLER_LAUNCH:
        if (caller->phase != WL_COLUMNAR_FILTER_HOST_CALLER_READY
            || !caller->operator_control || caller->filter->started
            || caller->filter->done
            || wl_evaluation_control_active_attempt_id(caller->operator_control)
            != caller->operator_id)
            break;
        rc = host_enqueue(queue, &caller->filter->job);
        if (!rc) caller->phase = WL_COLUMNAR_FILTER_HOST_CALLER_LAUNCHED;
        break;
    case WL_COLUMNAR_FILTER_HOST_CALLER_OBSERVE_FILTER:
        if (caller->phase != WL_COLUMNAR_FILTER_HOST_CALLER_LAUNCHED
            || !action->target || action->target != &caller->filter->job
            || action->target->queued)
            break;
        rc = caller->filter->protocol_result;
        if (caller->filter->done)
            caller->phase = WL_COLUMNAR_FILTER_HOST_CALLER_ACKNOWLEDGED;
        break;
    case WL_COLUMNAR_FILTER_HOST_CALLER_OBSERVE_REPORT:
        if (caller->phase != WL_COLUMNAR_FILTER_HOST_CALLER_ACKNOWLEDGED
            || !action->target || action->target->queued
            || action->target->dispatch != host_report_dispatch)
            break;
        {
            host_report_entry_t *report = action->target->data;
            if (!report || report->filter != caller->filter) break;
            rc = report->protocol_result;
            if (report->done) caller->phase =
                    WL_COLUMNAR_FILTER_HOST_CALLER_REPORTED;
        }
        break;
    case WL_COLUMNAR_FILTER_HOST_CALLER_OBSERVE_DESTROY:
        if (caller->phase != WL_COLUMNAR_FILTER_HOST_CALLER_REPORTED
            || !action->target || action->target->queued
            || action->target->dispatch != host_destroy_dispatch)
            break;
        {
            host_destroy_entry_t *destroy = action->target->data;
            if (!destroy || destroy->filter != caller->filter) break;
            rc = destroy->protocol_result;
            if (destroy->done) caller->phase =
                    WL_COLUMNAR_FILTER_HOST_CALLER_DESTROYED;
        }
        break;
    case WL_COLUMNAR_FILTER_HOST_CALLER_CLOSE_OPERATOR:
    case WL_COLUMNAR_FILTER_HOST_CALLER_CLOSE_FUNDING:
        if (!action->external_lifetime_ready) break;
        {
            bool operator_ref = action->operation ==
                WL_COLUMNAR_FILTER_HOST_CALLER_CLOSE_OPERATOR;
            wl_evaluation_control_t *released = operator_ref
                ? caller->operator_control : caller->funding_control;
            if (!released) break;
            if (operator_ref) caller->operator_control = NULL;
            else caller->funding_control = NULL;
            wl_evaluation_control_release(released);
            rc = 0;
            if (!caller->operator_control && !caller->funding_control)
                caller->phase = WL_COLUMNAR_FILTER_HOST_CALLER_CLOSED;
        }
        break;
    case WL_COLUMNAR_FILTER_HOST_CALLER_CLOSE_HANDLE:
        if (!action->external_lifetime_ready) break;
        caller->pending_credit = NULL;
        caller->pending_job = NULL;
        rc = 0;
        break;
    case WL_COLUMNAR_FILTER_HOST_CALLER_DIAGNOSTIC:
        rc = EAGAIN;
        break;
    default:
        break;
    }
    if (!caller->result_saved && rc) {
        caller->protocol_result = rc;
        caller->result_saved = true;
    }
done:
    action->protocol_result = rc;
    action->done = true;
    return;
}

/* One actual host turn invokes exactly one job, then returns to the caller.
 * Fixtures enqueue an independent job between every continuation dispatch.
 * Queue storage and every job/frame owner remain alive while parked. */
static int
host_turn(host_queue_t *queue)
{
    if (!queue || !queue->count) return ENOENT;
    host_job_t *job = queue->jobs[queue->head];
    queue->head = (queue->head + 1u) % HOST_QUEUE_CAPACITY;
    --queue->count;
    job->queued = false;
    ++queue->turn;
    job->dispatch(queue, job);
    return 0;
}

/* Actual behavioral, fault and independent literal fixtures follow only
 * after the frozen-source dispatch/caller count gate accepts this driver. */

/* Everything below is test setup, snapshots, assertions and the external
 * scheduler instrumentation. It is outside each actual host_turn. It does not
 * extend the bounded engine turn or replenish a consumed scheduling credit. */
#define CHECK(c) do { if (!(c)) { \
                          fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, \
                              #c); exit(1); \
                      } } while (0)

#define WL_COLUMNAR_FILTER_TEST_ACTIONS 16u
#define WL_COLUMNAR_FILTER_TEST_SENTINELS 32u
#define WL_COLUMNAR_FILTER_TEST_TRACE_CAP 65536u

/* Required by the direct governor object's MEMORY instrumentation. This
 * fixture uses its explicit release-refusal seam; phase hooks are inert. */
void
wl_columnar_memory_test_hook(wl_columnar_memory_reservation_t *reservation,
    unsigned phase)
{
    (void)reservation;
    (void)phase;
}

typedef struct {
    uint8_t bytes[4096];
    uint8_t tags[4096];
    uint32_t size, instructions, row_reads, digits;
} wl_columnar_filter_test_program_t;

typedef struct {
    unsigned label;
    bool admitted, scheduled, enqueue_failed, returned, bindings_cleared;
} wl_columnar_filter_test_ledger_row_t;

typedef struct {
    wl_col_session_t *session;
    host_queue_t queue;
    host_filter_entry_t filter;
    host_report_entry_t report;
    host_destroy_entry_t destroy;
    wl_columnar_filter_host_caller_t caller;
    wl_columnar_filter_host_caller_action_t actions[
        WL_COLUMNAR_FILTER_TEST_ACTIONS];
    wl_columnar_filter_host_lifecycle_credit_t credits[
        WL_COLUMNAR_FILTER_TEST_ACTIONS];
    wl_columnar_filter_test_ledger_row_t ledger[WL_COLUMNAR_FILTER_TEST_ACTIONS];
    host_sentinel_entry_t sentinels[WL_COLUMNAR_FILTER_TEST_SENTINELS];
    wl_columnar_filter_host_lifecycle_credit_t sentinel_credits[
        WL_COLUMNAR_FILTER_TEST_SENTINELS];
    wl_columnar_filter_test_ledger_row_t sentinel_ledger[
        WL_COLUMNAR_FILTER_TEST_SENTINELS];
    wl_plan_op_t op;
    wl_evaluation_control_t *target, *funding;
    int target_owner, funding_owner;
    uint64_t target_id, funding_id;
    uint64_t source_reserved, frame_reserved, host_admitted;
    unsigned sentinel_next, filter_turns, finish_turns, reset_turns;
    col_rel_t *input;
    bool same_control, input_owned;
} wl_columnar_filter_test_scenario_t;

static wl_col_session_t *
wl_columnar_filter_test_session(void)
{
    wl_col_session_t *session = calloc(1, sizeof(*session));
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = 16u * 1024u * 1024u,
        .usable_bytes = 16u * 1024u * 1024u,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK,
    };
    CHECK(session);
    wl_mem_ledger_init(&session->mem_ledger, 0);
    session->memory_governor =
        wl_columnar_memory_governor_ref_create(&resolution);
    CHECK(session->memory_governor);
    return session;
}

static uint64_t
wl_columnar_filter_test_reserved(const wl_col_session_t *session)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(session->memory_governor));
}

static wl_evaluation_control_t *
wl_columnar_filter_test_control(uint64_t limit, const void *owner)
{
    wl_evaluation_control_t *control = NULL;
    CHECK(wl_evaluation_control_create(limit, &control) == 0);
    CHECK(wl_evaluation_control_attach(control, owner) == 0);
    CHECK(wl_evaluation_control_begin(control, owner) == 0);
    return control;
}

static col_rel_t *
wl_columnar_filter_test_source(uint32_t width, uint32_t rows, bool typed,
    bool timestamps, bool long_names)
{
    char **names = calloc(width ? width : 1, sizeof(*names));
    CHECK(names);
    for (uint32_t c = 0; c < width; ++c) {
        names[c] = malloc(96);
        CHECK(names[c]);
        if (long_names)
            snprintf(names[c], 96,
                "long_column_%u_with_several_eight_byte_chunks", c);
        else
            snprintf(names[c], 96, "col%u", c);
    }
    col_rel_t *relation = NULL;
    CHECK(col_rel_alloc(&relation, "numeric original") == 0 && relation);
    CHECK(col_rel_set_schema(relation, width, (const char *const *)names) == 0);
    for (uint32_t c = 0; c < width; ++c) free(names[c]);
    free(names);
    if (typed && width) {
        wirelog_column_type_t *types = malloc(width * sizeof(*types));
        CHECK(types);
        for (uint32_t c = 0; c < width; ++c)
            types[c] = c == width - 1 ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64;
        CHECK(col_rel_set_column_types(relation, types, width) == 0);
        free(types);
    }
    int64_t *values = calloc(width ? width : 1, sizeof(*values));
    CHECK(values);
    for (uint32_t r = 0; r < rows; ++r) {
        for (uint32_t c = 0; c < width; ++c) {
            values[c] = (int64_t)(r % 7) - 3 + (int64_t)c * 11;
            if (typed && c == width - 1)
                values[c] = wl_columnar_float_to_bits(r ? (double)(r % 7) -
                        3.0 : -0.0);
        }
        CHECK(col_rel_append_row(relation, values) == 0);
    }
    free(values);
    if (timestamps) {
        CHECK(col_rel_enable_timestamps(relation) == 0);
        for (uint32_t r = 0; r < rows; ++r) {
            relation->timestamps[r].iteration = r % 11;
            relation->timestamps[r].stratum = r % 3;
            relation->timestamps[r].worker = r % 5;
            relation->timestamps[r]._reserved = r % 13;
            relation->timestamps[r].multiplicity = r % 2 ? -3 : 5;
        }
    }
    relation->has_graph_column = width != 0;
    relation->graph_col_idx = width ? width - 1 : 0;
    relation->declared_ncols = width;
    return relation;
}

static void
wl_columnar_filter_test_opcode(wl_columnar_filter_test_program_t *program,
    uint8_t opcode)
{
    CHECK(program->size < sizeof(program->bytes));
    program->bytes[program->size++] = opcode;
    program->tags[program->instructions] = opcode;
    ++program->instructions;
}

static void
wl_columnar_filter_test_constant(wl_columnar_filter_test_program_t *program,
    uint8_t opcode, int64_t value)
{
    wl_columnar_filter_test_opcode(program, opcode);
    CHECK(program->size + sizeof(value) <= sizeof(program->bytes));
    memcpy(program->bytes + program->size, &value, sizeof(value));
    program->size += sizeof(value);
}

static void
wl_columnar_filter_test_variable(wl_columnar_filter_test_program_t *program,
    uint8_t opcode, unsigned column)
{
    char name[16];
    snprintf(name, sizeof(name), "col%u", column);
    uint16_t length = (uint16_t)strlen(name);
    wl_columnar_filter_test_opcode(program, opcode);
    CHECK(program->size + sizeof(length) + length <= sizeof(program->bytes));
    memcpy(program->bytes + program->size, &length, sizeof(length));
    program->size += sizeof(length);
    memcpy(program->bytes + program->size, name, length);
    program->size += length;
    program->digits += length - 3u;
    if (opcode == WL_PLAN_EXPR_VAR_FLOAT) ++program->row_reads;
}

static void
wl_columnar_filter_test_action_init(
    wl_columnar_filter_test_scenario_t *scenario,
    unsigned index, wl_columnar_filter_host_caller_operation_t operation,
    host_job_t *target)
{
    wl_columnar_filter_host_caller_action_t *action = &scenario->actions[index];
    action->identity = action;
    action->caller = &scenario->caller;
    action->operation = operation;
    action->target = target;
    action->job.dispatch = wl_columnar_filter_host_caller_action_dispatch;
    action->job.data = action;
}

static void
wl_columnar_filter_test_prepare(wl_columnar_filter_test_scenario_t *scenario,
    col_rel_t *input, bool owned, wl_columnar_filter_test_program_t *program,
    uint64_t limit, bool same_control)
{
    memset(scenario, 0, sizeof(*scenario));
    scenario->session = wl_columnar_filter_test_session();
    scenario->input = input;
    scenario->input_owned = owned;
    scenario->source_reserved =
        wl_columnar_filter_test_reserved(scenario->session);
    CHECK(wl_columnar_eval_stack_cleanup_begin(scenario->session,
        &scenario->destroy.frame) == 0);
    scenario->filter.frame = scenario->destroy.frame; /* Borrowed, not another owner. */
    eval_stack_t *stack =
        wl_columnar_eval_stack_cleanup_stack(scenario->destroy.frame);
    CHECK(eval_stack_push(stack, input, owned) == 0);
    scenario->frame_reserved =
        wl_columnar_filter_test_reserved(scenario->session);
    scenario->op.op = WL_PLAN_OP_FILTER;
    scenario->op.filter_expr.data = program &&
        program->size ? program->bytes : NULL;
    scenario->op.filter_expr.size = program ? program->size : 0;
    scenario->target = wl_columnar_filter_test_control(limit,
            &scenario->target_owner);
    scenario->target_id =
        wl_evaluation_control_active_attempt_id(scenario->target);
    scenario->same_control = same_control;
    if (same_control) {
        scenario->funding = scenario->target;
        CHECK(wl_evaluation_control_retain(scenario->funding) == 0);
    } else {
        scenario->funding = wl_columnar_filter_test_control(0,
                &scenario->funding_owner);
    }
    scenario->funding_id =
        wl_evaluation_control_active_attempt_id(scenario->funding);
    scenario->filter.session = scenario->session;
    scenario->filter.op = &scenario->op;
    scenario->filter.external_control = scenario->target;
    scenario->filter.original_id = scenario->target_id;
    scenario->filter.job.dispatch = host_filter_dispatch;
    scenario->filter.job.data = &scenario->filter;
    scenario->report.filter = &scenario->filter;
    scenario->report.control = scenario->target;
    scenario->report.owner = &scenario->target_owner;
    scenario->report.original_id = scenario->target_id;
    CHECK(wl_evaluation_control_retain(scenario->target) == 0);
    scenario->report.ref_owned = true;
    scenario->report.job.dispatch = host_report_dispatch;
    scenario->report.job.data = &scenario->report;
    scenario->destroy.filter = &scenario->filter;
    scenario->destroy.session = scenario->session;
    scenario->destroy.job.dispatch = host_destroy_dispatch;
    scenario->destroy.job.data = &scenario->destroy;
    CHECK(wl_columnar_filter_host_caller_prepare(&scenario->caller,
        &scenario->filter, scenario->target, scenario->target_id,
        scenario->funding, scenario->funding_id) == 0);
    scenario->host_admitted = 128;
    wl_columnar_filter_test_action_init(scenario, 0,
        WL_COLUMNAR_FILTER_HOST_CALLER_LAUNCH, NULL);
    wl_columnar_filter_test_action_init(scenario, 1,
        WL_COLUMNAR_FILTER_HOST_CALLER_OBSERVE_FILTER, &scenario->filter.job);
    wl_columnar_filter_test_action_init(scenario, 3,
        WL_COLUMNAR_FILTER_HOST_CALLER_OBSERVE_REPORT, &scenario->report.job);
    wl_columnar_filter_test_action_init(scenario, 5,
        WL_COLUMNAR_FILTER_HOST_CALLER_OBSERVE_DESTROY, &scenario->destroy.job);
    wl_columnar_filter_test_action_init(scenario, 6,
        WL_COLUMNAR_FILTER_HOST_CALLER_CLOSE_HANDLE, NULL);
    wl_columnar_filter_test_action_init(scenario, 7,
        WL_COLUMNAR_FILTER_HOST_CALLER_CLOSE_OPERATOR, NULL);
    wl_columnar_filter_test_action_init(scenario, 8,
        WL_COLUMNAR_FILTER_HOST_CALLER_CLOSE_FUNDING, NULL);
    for (unsigned i = 9; i < 12; ++i)
        wl_columnar_filter_test_action_init(scenario, i,
            WL_COLUMNAR_FILTER_HOST_CALLER_DIAGNOSTIC, NULL);
    for (unsigned i = 0; i < WL_COLUMNAR_FILTER_TEST_ACTIONS; ++i) {
        host_job_t *job = &scenario->actions[i].job;
        wl_columnar_filter_host_lifecycle_kind_t kind =
            WL_COLUMNAR_FILTER_HOST_CALLER_ACTION;
        if (i == 2) {
            job = &scenario->report.job; kind = WL_COLUMNAR_FILTER_HOST_REPORT;
        }
        if (i == 4) {
            job = &scenario->destroy.job;
            kind = WL_COLUMNAR_FILTER_HOST_DESTROY;
        }
        if (i >= 12) {
            job = &scenario->filter.job; kind = WL_COLUMNAR_FILTER_HOST_RETRY;
        }
        if (i == 15) job = &scenario->destroy.job;
        CHECK(wl_columnar_filter_host_caller_admit_one(&scenario->caller,
            &scenario->credits[i], job, kind) == 0);
        scenario->ledger[i].label = 100u + i;
        scenario->ledger[i].admitted = true;
        scenario->host_admitted += 512;
    }
    for (unsigned i = 0; i < WL_COLUMNAR_FILTER_TEST_SENTINELS; ++i) {
        host_sentinel_entry_t *sentinel = &scenario->sentinels[i];
        sentinel->job.dispatch = host_sentinel_dispatch;
        sentinel->job.data = sentinel;
        CHECK(wl_columnar_filter_host_caller_admit_one(&scenario->caller,
            &scenario->sentinel_credits[i], &sentinel->job,
            WL_COLUMNAR_FILTER_HOST_SENTINEL) == 0);
        scenario->sentinel_ledger[i].label = 200u + i;
        scenario->sentinel_ledger[i].admitted = true;
        scenario->host_admitted += 291;
    }
}

static void
wl_columnar_filter_test_schedule(wl_columnar_filter_test_scenario_t *scenario,
    unsigned index, bool lifetime_ready)
{
    CHECK(index < WL_COLUMNAR_FILTER_TEST_ACTIONS);
    CHECK(wl_columnar_filter_host_caller_schedule_one(&scenario->caller,
        &scenario->queue, &scenario->credits[index], lifetime_ready) == 0);
    scenario->ledger[index].scheduled = true;
}

static void
wl_columnar_filter_test_one_turn(wl_columnar_filter_test_scenario_t *scenario)
{
    CHECK(scenario->queue.count);
    host_job_t *job = scenario->queue.jobs[scenario->queue.head];
    bool filter_job = job == &scenario->filter.job;
    wl_columnar_filter_context_t *ctx = NULL;
    if (filter_job && scenario->filter.started) {
        CHECK(wl_columnar_eval_stack_filter_context(scenario->filter.frame,
            &ctx) == 0);
        if (ctx->stage == WL_COLUMNAR_FILTER_STAGE_FINISH) {
            ++scenario->finish_turns;
            if (scenario->input_owned) scenario->input = NULL;
        }
        if (ctx->stage ==
            WL_COLUMNAR_FILTER_STAGE_RESET) ++scenario->reset_turns;
    }
    CHECK(host_turn(&scenario->queue) == 0); /* Actual return before instrumentation. */
    if (filter_job) ++scenario->filter_turns;
    for (unsigned i = 0; i < WL_COLUMNAR_FILTER_TEST_ACTIONS; ++i) {
        host_job_t *expected = &scenario->actions[i].job;
        if (i == 2) expected = &scenario->report.job;
        if (i == 4) expected = &scenario->destroy.job;
        if (i >= 12) expected = &scenario->filter.job;
        if (i == 15) expected = &scenario->destroy.job;
        if (scenario->ledger[i].scheduled && expected == job)
            scenario->ledger[i].returned = true;
    }
    for (unsigned i = 0; i < WL_COLUMNAR_FILTER_TEST_SENTINELS; ++i)
        if (job == &scenario->sentinels[i].job) {
            CHECK(scenario->sentinels[i].visits == 1);
            scenario->sentinel_ledger[i].returned = true;
        }
}

static void
wl_columnar_filter_test_sentinel(wl_columnar_filter_test_scenario_t *scenario)
{
    unsigned index = scenario->sentinel_next++;
    CHECK(index < WL_COLUMNAR_FILTER_TEST_SENTINELS);
    CHECK(wl_columnar_filter_host_caller_schedule_one(&scenario->caller,
        &scenario->queue, &scenario->sentinel_credits[index], false) == 0);
    scenario->sentinel_ledger[index].scheduled = true;
}

static void
wl_columnar_filter_test_drain(wl_columnar_filter_test_scenario_t *scenario)
{
    unsigned bound = 0;
    while (scenario->queue.count) {
        CHECK(++bound < 128);
        bool sentinel = scenario->queue.jobs[scenario->queue.head]->dispatch ==
            host_sentinel_dispatch;
        wl_columnar_filter_test_one_turn(scenario);
        if (sentinel && scenario->filter.job.queued)
            wl_columnar_filter_test_sentinel(scenario);
    }
    CHECK(scenario->filter.done || scenario->filter.parked);
}

static void
wl_columnar_filter_test_run(wl_columnar_filter_test_scenario_t *scenario)
{
    wl_columnar_filter_test_schedule(scenario, 0, false);
    wl_columnar_filter_test_one_turn(scenario); /* LAUNCH enqueues only FILTER. */
    wl_columnar_filter_test_sentinel(scenario);
    wl_columnar_filter_test_drain(scenario);
}

static void
wl_columnar_filter_test_cancel_unused(
    wl_columnar_filter_test_scenario_t *scenario,
    unsigned index)
{
    wl_columnar_filter_test_ledger_row_t *row = &scenario->ledger[index];
    if (!row->scheduled &&
        scenario->credits[index].phase == WL_COLUMNAR_FILTER_HOST_ADMITTED){
        CHECK(wl_columnar_filter_host_caller_cancel_one(&scenario->caller,
            &scenario->credits[index]) == 0);
        if (index != 2 && index != 4 && index < 12) {
            scenario->actions[index].target = NULL;
            scenario->actions[index].caller = NULL;
        }
        row->bindings_cleared = true;
    }
}

static void
wl_columnar_filter_test_clear_returned(
    wl_columnar_filter_test_scenario_t *scenario,
    unsigned index)
{
    CHECK(scenario->ledger[index].scheduled &&
        scenario->ledger[index].returned);
    if (index != 2 && index != 4 && index < 12) {
        CHECK(scenario->actions[index].done);
        scenario->actions[index].target = NULL;
        scenario->actions[index].caller = NULL;
    }
    scenario->ledger[index].bindings_cleared = true;
}

static wl_evaluation_control_report_t
wl_columnar_filter_test_complete(wl_columnar_filter_test_scenario_t *scenario)
{
    CHECK(scenario->filter.done && !scenario->filter.parked &&
        !scenario->queue.count);
    wl_columnar_filter_test_schedule(scenario, 1, false);
    wl_columnar_filter_test_one_turn(scenario);
    CHECK(scenario->actions[1].done &&
        scenario->actions[1].protocol_result == 0);
    CHECK(scenario->caller.phase ==
        WL_COLUMNAR_FILTER_HOST_CALLER_ACKNOWLEDGED);
    wl_columnar_filter_test_schedule(scenario, 2, false);
    wl_columnar_filter_test_one_turn(scenario);
    CHECK(scenario->report.done && !scenario->report.parked &&
        !scenario->report.ref_owned);
    CHECK(scenario->report.historical_available);
    CHECK(scenario->report.historical.attempt_id == scenario->target_id);
    wl_evaluation_control_report_t result = scenario->report.historical;
    wl_columnar_filter_test_schedule(scenario, 3, false);
    wl_columnar_filter_test_one_turn(scenario);
    CHECK(scenario->caller.phase == WL_COLUMNAR_FILTER_HOST_CALLER_REPORTED);
    for (unsigned i = 0; i < WL_COLUMNAR_FILTER_TEST_ACTIONS; ++i) {
        if (i == 4 || i == 5 || i == 6 || i == 7 || i == 8 || i == 15) continue;
        if (scenario->ledger[i].scheduled)
            wl_columnar_filter_test_clear_returned(scenario, i);
        else
            wl_columnar_filter_test_cancel_unused(scenario, i);
    }
    for (unsigned i = 0; i < WL_COLUMNAR_FILTER_TEST_SENTINELS; ++i) {
        if (scenario->sentinel_ledger[i].scheduled) {
            CHECK(scenario->sentinel_ledger[i].returned);
        } else {
            CHECK(wl_columnar_filter_host_caller_cancel_one(&scenario->caller,
                &scenario->sentinel_credits[i]) == 0);
        }
        scenario->sentinel_ledger[i].bindings_cleared = true;
    }
    /* Future close/observe records refer only to live caller/entry storage.
     * The frame has one owner, destroy.frame, and one adapter borrow that the
     * checked dispatcher erases before consumption. Pending retry15 also reads
     * that same live destroy entry; it stores no independent frame pointer. */
    scenario->actions[5].external_lifetime_ready = true;
    scenario->actions[6].external_lifetime_ready = true;
    scenario->actions[7].external_lifetime_ready = true;
    scenario->actions[8].external_lifetime_ready = true;
    wl_columnar_filter_test_schedule(scenario, 4, true);
    wl_columnar_filter_test_one_turn(scenario);
    if (scenario->destroy.pending) {
        CHECK(!scenario->destroy.frame && !scenario->filter.frame);
        wl_columnar_filter_test_schedule(scenario, 15, true);
        wl_columnar_filter_test_one_turn(scenario);
        wl_columnar_filter_test_clear_returned(scenario, 15);
    } else {
        wl_columnar_filter_test_cancel_unused(scenario, 15);
    }
    CHECK(scenario->destroy.done && !scenario->destroy.frame &&
        !scenario->filter.frame);
    CHECK(!scenario->session->cleanup_active &&
        !scenario->session->cleanup_pending);
    CHECK(wl_columnar_filter_test_reserved(scenario->session) ==
        scenario->source_reserved);
    wl_columnar_filter_test_schedule(scenario, 5, false);
    wl_columnar_filter_test_one_turn(scenario);
    CHECK(scenario->caller.phase == WL_COLUMNAR_FILTER_HOST_CALLER_DESTROYED);
    wl_columnar_filter_test_clear_returned(scenario, 4);
    wl_columnar_filter_test_clear_returned(scenario, 5);
    scenario->report.filter = NULL;
    scenario->destroy.filter = NULL;
    scenario->destroy.session = NULL;
    wl_columnar_filter_test_schedule(scenario, 6, true);
    wl_columnar_filter_test_one_turn(scenario);
    wl_columnar_filter_test_clear_returned(scenario, 6);
    wl_columnar_filter_test_schedule(scenario, 7, true);
    wl_columnar_filter_test_one_turn(scenario);
    CHECK(!scenario->caller.operator_control);
    wl_columnar_filter_test_clear_returned(scenario, 7);
    wl_columnar_filter_test_schedule(scenario, 8, true);
    wl_columnar_filter_test_one_turn(scenario);
    CHECK(scenario->caller.phase == WL_COLUMNAR_FILTER_HOST_CALLER_CLOSED);
    wl_columnar_filter_test_clear_returned(scenario, 8);
    CHECK(!scenario->queue.count);
    for (unsigned i = 0; i < WL_COLUMNAR_FILTER_TEST_ACTIONS; ++i)
        CHECK(scenario->ledger[i].bindings_cleared);
    if (!scenario->same_control) {
        CHECK(wl_evaluation_control_finish(scenario->funding,
            &scenario->funding_owner, 0, 0) == 0);
        wl_evaluation_control_report_t funding_report;
        CHECK(wl_evaluation_control_report(scenario->funding,
            &funding_report) == 0);
        CHECK(funding_report.attempt_id == scenario->funding_id);
        CHECK(funding_report.consumed == scenario->host_admitted);
        CHECK(wl_evaluation_control_detach(scenario->funding,
            &scenario->funding_owner) == 0);
    }
    if (wl_evaluation_control_active_attempt_id(scenario->target)) {
        /* A restarted successor must be completed before inspecting its report. */
        uint64_t successor_id =
            wl_evaluation_control_active_attempt_id(scenario->target);
        CHECK(successor_id != scenario->target_id);
        CHECK(wl_evaluation_control_finish(scenario->target,
            &scenario->target_owner, 0, 0) == 0);
        wl_evaluation_control_report_t successor;
        CHECK(wl_evaluation_control_report(scenario->target, &successor) == 0);
        CHECK(successor.attempt_id == successor_id && successor.consumed == 0);
    }
    CHECK(wl_evaluation_control_detach(scenario->target,
        &scenario->target_owner) == 0);
    wl_evaluation_control_t *target = scenario->target,
        *funding = scenario->funding;
    scenario->target = NULL;
    scenario->funding = NULL;
    scenario->filter.external_control = NULL;
    wl_evaluation_control_release(target);
    wl_evaluation_control_release(funding);
    if (!scenario->input_owned) {
        CHECK(scenario->input);
        CHECK(col_rel_destroy_checked(scenario->input) == 0);
    }
    scenario->input = NULL;
    CHECK(wl_columnar_memory_governor_ref_is_sole(
            scenario->session->memory_governor));
    wl_columnar_memory_governor_ref_release(scenario->session->memory_governor);
    free(scenario->session);
    scenario->session = NULL;
    return result;
}

static void
wl_columnar_filter_test_equal(const col_rel_t *actual,
    const col_rel_t *expected)
{
    CHECK(actual && expected && actual->ncols == expected->ncols &&
        actual->nrows == expected->nrows);
    CHECK(actual->has_graph_column == expected->has_graph_column);
    CHECK(actual->graph_col_idx == expected->graph_col_idx);
    CHECK(actual->declared_ncols == expected->declared_ncols);
    CHECK((actual->column_types != NULL) == (expected->column_types != NULL));
    CHECK((actual->timestamps != NULL) == (expected->timestamps != NULL));
    for (uint32_t c = 0; c < actual->ncols; ++c) {
        CHECK(strcmp(actual->col_names[c], expected->col_names[c]) == 0);
        CHECK(strcmp(actual->schema.children[c]->format,
            expected->schema.children[c]->format) == 0);
        if (actual->column_types) CHECK(actual->column_types[c] ==
                expected->column_types[c]);
        for (uint32_t r = 0; r < actual->nrows; ++r)
            CHECK(actual->columns[c][r] == expected->columns[c][r]);
    }
    if (actual->timestamps)
        for (uint32_t r = 0; r < actual->nrows; ++r) {
            const col_delta_timestamp_t *a = &actual->timestamps[r],
                *b = &expected->timestamps[r];
            CHECK(a->iteration == b->iteration && a->stratum == b->stratum);
            CHECK(a->worker == b->worker && a->_reserved == b->_reserved &&
                a->multiplicity == b->multiplicity);
        }
}

static wl_evaluation_control_report_t
wl_columnar_filter_test_parity(uint32_t width, uint32_t rows, bool typed,
    bool timestamps, bool long_names,
    wl_columnar_filter_test_program_t *program,
    uint64_t limit, bool same_control)
{
    wl_col_session_t *raw_session = wl_columnar_filter_test_session();
    wl_columnar_eval_stack_cleanup_frame_t *raw_frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(raw_session, &raw_frame) == 0);
    eval_stack_t *raw_stack = wl_columnar_eval_stack_cleanup_stack(raw_frame);
    col_rel_t *raw_input = wl_columnar_filter_test_source(width, rows, typed,
            timestamps, long_names);
    CHECK(eval_stack_push(raw_stack, raw_input, true) == 0);
    wl_plan_op_t raw_op = {0};
    raw_op.op = WL_PLAN_OP_FILTER;
    raw_op.filter_expr.data = program && program->size ? program->bytes : NULL;
    raw_op.filter_expr.size = program ? program->size : 0;
    CHECK(wl_columnar_filter_op(&raw_op, raw_stack, raw_session) == 0);
    CHECK(raw_stack->top == 1 && raw_stack->items[0].rel);
    wl_columnar_filter_test_scenario_t *scenario = calloc(1, sizeof(*scenario));
    CHECK(scenario);
    col_rel_t *input = wl_columnar_filter_test_source(width, rows, typed,
            timestamps, long_names);
    wl_columnar_filter_test_prepare(scenario, input, true, program, limit,
        same_control);
    wl_columnar_filter_test_run(scenario);
    CHECK(scenario->filter.done &&
        scenario->filter.status.semantic_result == 0);
    CHECK(scenario->filter.status.committed && scenario->reset_turns == 1);
    eval_stack_t *actual_stack =
        wl_columnar_eval_stack_cleanup_stack(scenario->filter.frame);
    CHECK(actual_stack->top == 1);
    wl_columnar_filter_test_equal(actual_stack->items[0].rel,
        raw_stack->items[0].rel);
    CHECK(scenario->sentinel_next > 1 && scenario->filter_turns > 1);
    wl_evaluation_control_report_t report =
        wl_columnar_filter_test_complete(scenario);
    CHECK(report.result == 0);
    free(scenario);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&raw_frame) == 0 && !raw_frame);
    CHECK(!wl_columnar_filter_test_reserved(raw_session));
    CHECK(wl_columnar_memory_governor_ref_is_sole(
            raw_session->memory_governor));
    wl_columnar_memory_governor_ref_release(raw_session->memory_governor);
    free(raw_session);
    return report;
}

typedef struct {
    uint64_t *prefix;
    size_t count, capacity;
    uint64_t total;
} wl_columnar_filter_test_literal_t;

static void
wl_columnar_filter_test_literal_add(wl_columnar_filter_test_literal_t *literal,
    uint64_t fee)
{
    CHECK(fee && fee <= 256 && literal->count < literal->capacity);
    literal->total += fee;
    literal->prefix[literal->count++] = literal->total;
}

static void
wl_columnar_filter_test_literal_repeat(
    wl_columnar_filter_test_literal_t *literal,
    uint64_t fee, uint64_t count)
{
    for (uint64_t i = 0; i < count; ++i)
        wl_columnar_filter_test_literal_add(literal, fee);
}

static unsigned
wl_columnar_filter_test_digits(unsigned value)
{
    unsigned digits = 1;
    while (value >= 10) {
        value /= 10; ++digits;
    }
    return digits;
}

static void
wl_columnar_filter_test_literal_reserve(
    wl_columnar_filter_test_literal_t *literal)
{
    static const uint64_t fees[] = {74, 54, 84, 29};
    for (size_t i = 0; i < sizeof(fees) / sizeof(fees[0]); ++i)
        wl_columnar_filter_test_literal_add(literal, fees[i]);
}

static void
wl_columnar_filter_test_literal_commit(
    wl_columnar_filter_test_literal_t *literal)
{
    static const uint64_t fees[] = {74, 54, 29};
    for (size_t i = 0; i < sizeof(fees) / sizeof(fees[0]); ++i)
        wl_columnar_filter_test_literal_add(literal, fees[i]);
}

/* Independent scalar original-single schedule, one uncontended nonce round. */
static void
wl_columnar_filter_test_literal_single(
    wl_columnar_filter_test_literal_t *literal)
{
    static const uint64_t fees[] = {94, 19, 39, 39, 45, 52, 45, 21, 144};
    for (size_t i = 0; i < sizeof(fees) / sizeof(fees[0]); ++i)
        wl_columnar_filter_test_literal_add(literal, fees[i]);
}

static void
wl_columnar_filter_test_literal_constructor(
    wl_columnar_filter_test_literal_t *literal,
    uint32_t width, bool typed, bool timestamps, bool long_names)
{
#define L(v) wl_columnar_filter_test_literal_add(literal, (v))
#define R(v, n) wl_columnar_filter_test_literal_repeat(literal, (v), (n))
    L(128); L(128); L(128); L(192);
    R(96, 8); /* "$filter", including its terminator. */
    R(96, 6); L(96);
    if (typed && width) R(96, 2);
    R(96, 4); L(128);
    for (uint32_t c = 0; c < width; ++c) {
        uint64_t length = long_names
            ? strlen("long_column__with_several_eight_byte_chunks") +
            wl_columnar_filter_test_digits(c) + 1u
            : 4u + wl_columnar_filter_test_digits(c);
        L(96); R(96, length); R(96, 3);
    }
    L(96); L(160); wl_columnar_filter_test_literal_reserve(literal);
    L(112); R(224, (sizeof(col_rel_t) + 7u) / 8u);
    R(96, 5); L(112); L(224); L(80); L(112); L(96);
    L(160); wl_columnar_filter_test_literal_reserve(literal);
    L(160);
    if (width || timestamps) wl_columnar_filter_test_literal_reserve(literal);
    if (width) {
        L(112); R(192, (width + 7u) / 8u); R(112, width);
        L(112); /* Governor-backed row scratch. */
        L(112); R(192, (width + 7u) / 8u);
        for (uint32_t c = 0; c < width; ++c) {
            uint64_t length = long_names
                ? strlen("long_column__with_several_eight_byte_chunks") +
                wl_columnar_filter_test_digits(c) + 1u
                : 4u + wl_columnar_filter_test_digits(c);
            L(96); R(96, length); L(112); R(224, (length + 7u) / 8u);
        }
    }
    if (typed && width) {
        L(112); R(96, width);
    }
    if (timestamps) {
        L(112); R(224, (64u * sizeof(col_delta_timestamp_t) + 7u) / 8u);
    }
    L(96); L(112); L(224);
    if (width) {
        L(112); R(192, (width + 7u) / 8u);
        for (uint32_t c = 0; c < width; ++c) {
            uint64_t length = long_names
                ? strlen("long_column__with_several_eight_byte_chunks") +
                wl_columnar_filter_test_digits(c) + 1u
                : 4u + wl_columnar_filter_test_digits(c);
            L(128); L(112);
            if (typed) L(96);
            L(224); L(96); R(96, length); L(112); R(224, (length + 7u) / 8u);
        }
    }
    for (unsigned token = 0; token < 3; ++token) {
        L(128);
        if (token < 2 || width || timestamps)
            wl_columnar_filter_test_literal_commit(literal);
    }
    L(220); L(128);
#undef R
#undef L
}

static void
wl_columnar_filter_test_literal_selected(
    wl_columnar_filter_test_literal_t *literal,
    uint32_t width, uint32_t selected_before, uint32_t capacity,
    bool timestamps,
    bool renewal)
{
#define L(v) wl_columnar_filter_test_literal_add(literal, (v))
#define R(v, n) wl_columnar_filter_test_literal_repeat(literal, (v), (n))
    L(192); L(192); L(256);
    if (timestamps) L(128);
    L(128); L(220); L(80);
    if (width > 16) {
        L(80); wl_columnar_filter_test_literal_reserve(literal);
        wl_columnar_filter_test_literal_commit(literal); L(32);
    }
    R(36, width);
    if (renewal) {
        L(128); wl_columnar_filter_test_literal_single(literal); L(220);
    }
    L(63); L(103); R(36, (uint64_t)width * 2u);
    if (selected_before < capacity) {
        R(36, width);
        if (timestamps) R(183, (sizeof(col_delta_timestamp_t) + 7u) / 8u);
        L(48);
    } else {
        R(83, 5); L(76);
        if (width ||
            timestamps) wl_columnar_filter_test_literal_reserve(literal);
        if (width) {
            L(32); /* Pointer table allocation; lanes are initialized individually. */
            for (uint32_t c = 0; c < width; ++c) {
                L(32); R(183, selected_before); L(36);
            }
        }
        if (timestamps) {
            L(32);
            R(183,
                (uint64_t)capacity *
                ((sizeof(col_delta_timestamp_t) + 7u) / 8u));
            R(183, (sizeof(col_delta_timestamp_t) + 7u) / 8u);
        }
        L(69); L(151);
        if (width || timestamps) {
            L(26); L(228);
        }
        L(48);
    }
    L(192); L(256);
    if (timestamps) L(128);
#undef R
#undef L
}

/* selection: 0 all selected, 1 all rejected, 2 integer col0 >0. Geometry
 * comes from declared fixture dimensions/sizeof, never a production fee enum
 * or hook-total oracle. The literal ordering is exercised at actual limits. */
static wl_columnar_filter_test_literal_t
wl_columnar_filter_test_literal(uint32_t width, uint32_t rows, bool typed,
    bool timestamps, bool long_names,
    const wl_columnar_filter_test_program_t *program,
    unsigned selection)
{
    wl_columnar_filter_test_literal_t literal = {0};
    literal.capacity = 262144;
    literal.prefix = malloc(literal.capacity * sizeof(*literal.prefix));
    CHECK(literal.prefix);
#define L(v) wl_columnar_filter_test_literal_add(&literal, (v))
#define R(v, n) wl_columnar_filter_test_literal_repeat(&literal, (v), (n))
    L(224); R(192, 10); L(192); R(192, 2); L(192);
    L(160); L(128); L(256); L(192); L(192);
    L(192); L(192); L(256); L(192); L(256);
    R(96, 1); R(144, 1); L(224); R(96, 1); R(144, 1);
    L(224); L(192); R(224, (width + 7u) / 8u);
    L(192); L(128); L(192); L(192); L(192); L(192);
    wl_columnar_filter_test_literal_constructor(&literal, width, typed,
        timestamps, long_names);
    R(192, 2); L(192);
    if (width > 16) {
        L(192); wl_columnar_filter_test_literal_reserve(&literal);
        L(192); L(192); wl_columnar_filter_test_literal_commit(&literal);
    }
    L(192); L(192);
    if (program && program->size) {
        L(192);
        /* Same encoded-input oracle as the independently landed numeric unit.
         * Preserve ordered positive fees rather than summing hook labels. */
        L(128);
        for (unsigned pass = 0; pass < 2; ++pass) {
            if (pass) {
                L(224); L(224);
                wl_columnar_filter_test_literal_reserve(&literal); R(224, 2);
            }
            uint32_t pos = 0;
            while (pos < program->size) {
                L(192); L(224);
                uint8_t tag = program->bytes[pos];
                uint32_t length = 1, digits = 0;
                if (tag == WL_PLAN_EXPR_CONST_INT ||
                    tag == WL_PLAN_EXPR_CONST_FLOAT) length = 9;
                if (tag == WL_PLAN_EXPR_VAR || tag == WL_PLAN_EXPR_VAR_FLOAT) {
                    uint16_t name_length;
                    memcpy(&name_length, program->bytes + pos + 1,
                        sizeof(name_length));
                    length = 3u + name_length;
                    digits = name_length - 3u;
                }
                R(224, length); R(192, digits);
                if (pass) L(192);
                pos += length;
            }
        }
        L(224); L(224); wl_columnar_filter_test_literal_commit(&literal);
        L(160);
    }
    R(192, 22); L(192); L(192); L(170);
    wl_columnar_filter_test_literal_single(&literal);
    uint32_t selected = 0, capacity = 64;
    bool transitioned = false;
    for (uint32_t row = 0; row < rows; ++row) {
        L(128); L(192); L(128); R(224, (width + 7u) / 8u);
        if (program && program->size) {
            L(192); L(96);
            for (uint32_t i = 0; i < program->instructions; ++i) {
                L(64); L(224);
                if (program->tags[i] == WL_PLAN_EXPR_VAR_FLOAT) L(160);
            }
            L(96);
        }
        L(192);
        bool passed = selection == 0 || (selection == 2 && row % 7u > 3u);
        if (passed) {
            L(192);
            bool renewal = transitioned && selected >= capacity;
            wl_columnar_filter_test_literal_selected(&literal, width, selected,
                capacity, timestamps, renewal);
            if (selected >= capacity) {
                capacity *= 2u; transitioned = true;
            }
            ++selected;
        }
    }
    L(192); L(128); L(128); L(128); /* Original one-entry commit scan/cutoff. */
#undef R
#undef L
    return literal;
}

static void
wl_columnar_filter_test_budget(uint32_t width, uint32_t rows, bool typed,
    bool timestamps, bool long_names,
    wl_columnar_filter_test_program_t *program,
    unsigned selection, bool every_prefix)
{
    wl_columnar_filter_test_literal_t literal = wl_columnar_filter_test_literal(
        width, rows, typed, timestamps, long_names, program, selection);
    wl_evaluation_control_report_t unlimited = wl_columnar_filter_test_parity(
        width, rows, typed, timestamps, long_names, program, 0, false);
    if (unlimited.consumed != literal.total)
        fprintf(stderr,
            "literal mismatch width=%u rows=%u typed=%d ts=%d actual=%llu expected=%llu\n",
            width, rows, (int)typed, (int)timestamps,
            (unsigned long long)unlimited.consumed,
            (unsigned long long)literal.total);
    CHECK(unlimited.limit == 0 && unlimited.consumed == literal.total);
    size_t first = every_prefix ? 0 : literal.count - 1;
    for (size_t i = first; i < literal.count; ++i) {
        for (unsigned below = 0; below < 2; ++below) {
            uint64_t limit = literal.prefix[i] - below;
            wl_columnar_filter_test_scenario_t *scenario = calloc(1,
                    sizeof(*scenario));
            CHECK(scenario);
            col_rel_t *input = wl_columnar_filter_test_source(width, rows,
                    typed, timestamps, long_names);
            wl_columnar_filter_test_prepare(scenario, input, true, program,
                limit, false);
            wl_columnar_filter_test_run(scenario);
            CHECK(scenario->filter.done);
            int expected = !below &&
                i + 1 == literal.count ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT;
            CHECK(scenario->filter.status.semantic_result == expected);
            CHECK(scenario->filter.status.committed == (expected == 0));
            eval_stack_t *stack =
                wl_columnar_eval_stack_cleanup_stack(scenario->filter.frame);
            CHECK(stack->top == 1 && stack->items[0].owned);
            if (expected) {
                CHECK(stack->items[0].rel->nrows == rows &&
                    stack->items[0].rel->ncols == width);
                CHECK(strcmp(stack->items[0].rel->name,
                    "numeric original") == 0);
            }
            wl_evaluation_control_report_t report =
                wl_columnar_filter_test_complete(scenario);
            uint64_t consumed = below ? (i ? literal.prefix[i -
                1] : 0) : literal.prefix[i];
            CHECK(report.limit == limit && report.consumed == consumed &&
                report.result == expected);
            free(scenario);
        }
    }
    wl_evaluation_control_report_t above = wl_columnar_filter_test_parity(
        width, rows, typed, timestamps, long_names, program, literal.total + 1,
        false);
    CHECK(above.consumed == literal.total);
    free(literal.prefix);
}

static void
wl_columnar_filter_test_no_first_admission(void)
{
    wl_columnar_filter_test_scenario_t *scenario = calloc(1, sizeof(*scenario));
    CHECK(scenario);
    col_rel_t *input = wl_columnar_filter_test_source(1, 1, false, false,
            false);
    wl_columnar_filter_test_prepare(scenario, input, true, NULL, 1, false);
    wl_columnar_filter_test_run(scenario);
    CHECK(scenario->filter.done && !scenario->filter.status.committed);
    CHECK(scenario->filter.status.execution_result ==
        WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(scenario->finish_turns == 1 && scenario->reset_turns == 1);
    wl_evaluation_control_report_t report =
        wl_columnar_filter_test_complete(scenario);
    CHECK(report.limit == 1 && report.consumed == 0 &&
        report.result == WL_EVALUATION_CONTROL_WORK_LIMIT);
    free(scenario);
}

typedef struct {
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    wl_evaluation_control_t *funding;
    int owner;
    int protocol, semantic;
    bool retained;
} wl_columnar_filter_test_native_t;

static void *
wl_columnar_filter_test_native_finish(void *argument)
{
    wl_columnar_filter_test_native_t *native = argument;
    /* An independently admitted finite diagnostic, never the spent FINISH
     * allowance. Native creation/join and assertions are test instrumentation. */
    CHECK(wl_evaluation_control_charge(native->funding, 256) == 0);
    native->protocol = wl_columnar_eval_stack_filter_finish(native->frame, 0,
            &native->semantic, &native->retained);
    return NULL;
}

static void
wl_columnar_filter_test_busy_finish(void)
{
    wl_columnar_filter_test_scenario_t *scenario = calloc(1, sizeof(*scenario));
    CHECK(scenario);
    col_rel_t *input = wl_columnar_filter_test_source(1, 3, false, true, false);
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(wl_columnar_source_access_reader_acquire(&input->source_access,
        &reader) == 0);
    wl_columnar_filter_test_prepare(scenario, input, true, NULL, 0, false);
    wl_columnar_filter_test_schedule(scenario, 0, false);
    wl_columnar_filter_test_one_turn(scenario);
    wl_columnar_filter_test_sentinel(scenario);
    bool protocol_checked = false;
    unsigned turns = 0;
    while (scenario->queue.count) {
        CHECK(++turns < 128);
        host_job_t *job = scenario->queue.jobs[scenario->queue.head];
        wl_columnar_filter_context_t *ctx = NULL;
        if (job == &scenario->filter.job && scenario->filter.started) {
            CHECK(wl_columnar_eval_stack_filter_context(scenario->filter.frame,
                &ctx) == 0);
            if (ctx->stage == WL_COLUMNAR_FILTER_STAGE_FINISH &&
                !protocol_checked){
                CHECK(!ctx->control && !ctx->pin_owned && !ctx->busy);
                ctx->busy = true; /* Paused exclusive test invocation; restored before dispatch. */
                int owner;
                wl_evaluation_control_t *diagnostic =
                    wl_columnar_filter_test_control(256, &owner);
                CHECK(wl_evaluation_control_charge(diagnostic, 256) == 0);
                int semantic = 987;
                CHECK(wl_columnar_eval_stack_filter_finish(
                        scenario->filter.frame, 0,
                        &semantic, NULL) == EINVAL && semantic == 987);
                CHECK(wl_evaluation_control_finish(diagnostic, &owner, 0,
                    0) == 0);
                CHECK(wl_evaluation_control_detach(diagnostic, &owner) == 0);
                wl_evaluation_control_release(diagnostic);
                wl_columnar_filter_test_native_t native = {
                    .frame = scenario->filter.frame, .semantic = 123,
                    .retained = true,
                };
                native.funding = wl_columnar_filter_test_control(256,
                        &native.owner);
                wl_thread_t thread;
                CHECK(wl_thread_create(&thread,
                    wl_columnar_filter_test_native_finish, &native) == 0);
                CHECK(wl_thread_join(&thread) == 0);
                CHECK(native.protocol == EINVAL && native.semantic == 123 &&
                    native.retained);
                CHECK(ctx->busy &&
                    ctx->stage == WL_COLUMNAR_FILTER_STAGE_FINISH);
                CHECK(wl_evaluation_control_finish(native.funding,
                    &native.owner, 0, 0) == 0);
                wl_evaluation_control_report_t report;
                CHECK(wl_evaluation_control_report(native.funding,
                    &report) == 0 && report.consumed == 256);
                CHECK(wl_evaluation_control_detach(native.funding,
                    &native.owner) == 0);
                wl_evaluation_control_release(native.funding);
                ctx->busy = false;
                protocol_checked = true;
            }
        }
        bool sentinel = job->dispatch == host_sentinel_dispatch;
        wl_columnar_filter_test_one_turn(scenario);
        if (sentinel &&
            scenario->filter.job.queued) wl_columnar_filter_test_sentinel(
                scenario);
    }
    CHECK(protocol_checked && scenario->filter.parked &&
        !scenario->filter.done);
    CHECK(scenario->filter.status.progress ==
        WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
    CHECK(scenario->filter.status.execution_result == 0 &&
        scenario->filter.status.cleanup_result == EBUSY);
    CHECK(scenario->finish_turns == 1 && scenario->reset_turns == 0 &&
        !scenario->filter.job.queued);
    for (unsigned retry = 12; retry < 14; ++retry) {
        wl_columnar_filter_test_schedule(scenario, retry, false);
        wl_columnar_filter_test_sentinel(scenario);
        wl_columnar_filter_test_drain(scenario);
        CHECK(scenario->filter.parked && scenario->reset_turns == 0 &&
            !scenario->filter.job.queued);
    }
    CHECK(scenario->finish_turns == 3);
    CHECK(wl_columnar_source_access_reader_release(&reader) == 0);
    wl_columnar_filter_test_schedule(scenario, 14, false);
    wl_columnar_filter_test_sentinel(scenario);
    wl_columnar_filter_test_drain(scenario);
    CHECK(scenario->filter.done && scenario->finish_turns == 4 &&
        scenario->reset_turns == 1);
    CHECK(scenario->filter.status.semantic_result == 0 &&
        scenario->filter.status.cleanup_result == 0);
    wl_evaluation_control_report_t report =
        wl_columnar_filter_test_complete(scenario);
    CHECK(report.result == 0);
    free(scenario);
}

typedef struct {
    unsigned family, phase;
    uint64_t progress;
    uint32_t row;
} wl_columnar_filter_test_event_t;

static wl_columnar_filter_test_scenario_t *wl_columnar_filter_test_watched;
static wl_columnar_filter_test_event_t wl_columnar_filter_test_events[
    WL_COLUMNAR_FILTER_TEST_TRACE_CAP];
static unsigned wl_columnar_filter_test_event_count,
    wl_columnar_filter_test_stop;
static unsigned wl_columnar_filter_test_stop_mode,
    wl_columnar_filter_test_successor_callbacks;
static bool wl_columnar_filter_test_restarted;

static void
wl_columnar_filter_test_event(wl_evaluation_control_t *control,
    unsigned family, unsigned phase, uint64_t progress)
{
    wl_columnar_filter_test_scenario_t *scenario =
        wl_columnar_filter_test_watched;
    if (!scenario || !control || control != scenario->target) return;
    if (wl_columnar_filter_test_restarted) {
        ++wl_columnar_filter_test_successor_callbacks;
        return;
    }
    CHECK(wl_evaluation_control_active_attempt_id(control) ==
        scenario->target_id);
    CHECK(wl_columnar_filter_test_event_count <
        WL_COLUMNAR_FILTER_TEST_TRACE_CAP);
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(wl_columnar_eval_stack_filter_context(scenario->filter.frame,
        &ctx) == 0);
    wl_columnar_filter_test_events[wl_columnar_filter_test_event_count++] =
        (wl_columnar_filter_test_event_t){family, phase, progress, ctx->row};
    if (!wl_columnar_filter_test_stop
        || wl_columnar_filter_test_event_count != wl_columnar_filter_test_stop)
        return;
    if (wl_columnar_filter_test_stop_mode == 0) {
        wl_evaluation_control_request_cancel(control);
    } else if (wl_columnar_filter_test_stop_mode == 1) {
        CHECK(wl_evaluation_control_charge(control, UINT64_MAX) == EOVERFLOW);
    } else {
        CHECK(wl_evaluation_control_finish(control, &scenario->target_owner, 0,
            0) == 0);
        CHECK(wl_evaluation_control_report(control,
            &scenario->report.historical) == 0);
        scenario->report.historical_available = true;
        CHECK(wl_evaluation_control_reset(control) == 0);
        CHECK(wl_evaluation_control_begin(control,
            &scenario->target_owner) == 0);
        CHECK(wl_evaluation_control_active_attempt_id(control) !=
            scenario->target_id);
        wl_columnar_filter_test_restarted = true;
    }
}

static void
wl_columnar_filter_test_filter_hook(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress)
{
    wl_columnar_filter_test_event(control, 1, phase, progress);
}

static void
wl_columnar_filter_test_relation_hook(wl_evaluation_control_t *control,
    uint64_t work)
{
    CHECK(work <= 256);
    wl_columnar_filter_test_event(control, 2, (unsigned)work, 0);
}

static void
wl_columnar_filter_test_expr_hook(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress, bool failed)
{
    (void)failed;
    wl_columnar_filter_test_event(control, 3, phase, progress);
}

static void
wl_columnar_filter_test_frame_hook(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress)
{
    wl_columnar_filter_test_event(control, 4, phase, progress);
}

static void
wl_columnar_filter_test_governor_work(wl_evaluation_control_t *control,
    uint64_t work)
{
    CHECK(work <= 256);
    wl_columnar_filter_test_event(control, 5, (unsigned)work, 0);
}

static void
wl_columnar_filter_test_governor_hook(wl_evaluation_control_t *control,
    wl_columnar_memory_reservation_t *reservation, unsigned phase)
{
    (void)reservation;
    wl_columnar_filter_test_event(control, 6, phase, 0);
}

static void
wl_columnar_filter_test_trace_begin(
    wl_columnar_filter_test_scenario_t *scenario,
    unsigned stop, unsigned mode)
{
    wl_columnar_filter_test_watched = scenario;
    wl_columnar_filter_test_event_count = 0;
    wl_columnar_filter_test_stop = stop;
    wl_columnar_filter_test_stop_mode = mode;
    wl_columnar_filter_test_successor_callbacks = 0;
    wl_columnar_filter_test_restarted = false;
    wl_columnar_filter_test_attempt_checkpoint =
        wl_columnar_filter_test_filter_hook;
    wl_columnar_relation_test_work_checkpoint =
        wl_columnar_filter_test_relation_hook;
    wl_columnar_expr_test_checkpoint = wl_columnar_filter_test_expr_hook;
    wl_columnar_eval_stack_test_checkpoint = wl_columnar_filter_test_frame_hook;
    wl_columnar_memory_governor_test_work_checkpoint =
        wl_columnar_filter_test_governor_work;
    wl_columnar_memory_governor_test_checkpoint =
        wl_columnar_filter_test_governor_hook;
}

static void
wl_columnar_filter_test_trace_end(void)
{
    wl_columnar_filter_test_attempt_checkpoint = NULL;
    wl_columnar_relation_test_work_checkpoint = NULL;
    wl_columnar_expr_test_checkpoint = NULL;
    wl_columnar_eval_stack_test_checkpoint = NULL;
    wl_columnar_memory_governor_test_work_checkpoint = NULL;
    wl_columnar_memory_governor_test_checkpoint = NULL;
    wl_columnar_filter_test_watched = NULL;
}

static void
wl_columnar_filter_test_faults(uint32_t width, uint32_t rows, bool timestamps,
    wl_columnar_filter_test_program_t *program, bool exhaustive)
{
    wl_columnar_filter_test_scenario_t *baseline = calloc(1, sizeof(*baseline));
    CHECK(baseline);
    wl_columnar_filter_test_prepare(baseline,
        wl_columnar_filter_test_source(width, rows, false, timestamps, false),
        true, program, 0, false);
    wl_columnar_filter_test_trace_begin(baseline, 0, 0);
    wl_columnar_filter_test_run(baseline);
    CHECK(baseline->filter.done && !baseline->filter.status.semantic_result);
    unsigned count = wl_columnar_filter_test_event_count;
    wl_columnar_filter_test_event_t *events = malloc(count * sizeof(*events));
    CHECK(events && count);
    memcpy(events, wl_columnar_filter_test_events, count * sizeof(*events));
    wl_columnar_filter_test_trace_end();
    wl_columnar_filter_test_complete(baseline);
    free(baseline);
    unsigned exercised = 0;
    for (unsigned event = 0; event < count; ++event) {
        /* Small fixtures test every actual occurrence. Larger growth fixtures
         * test first/last of each real family/phase at each growth boundary;
         * repeating byte/cell occurrences are covered by the source proof and
         * independent literal-prefix tests, not falsely called exhaustive. */
        bool selected = exhaustive;
        if (!selected &&
            (events[event].row == 0 || events[event].row == 64 ||
            events[event].row == 128)){
            bool first = true, last = true;
            for (unsigned j = 0; j < count; ++j) {
                if (events[j].family != events[event].family ||
                    events[j].phase != events[event].phase
                    || events[j].row != events[event].row) continue;
                if (j < event) first = false;
                if (j > event) last = false;
            }
            selected = first || last;
        }
        if (!selected) continue;
        ++exercised;
        for (unsigned mode = 0; mode < 3; ++mode) {
            wl_columnar_filter_test_scenario_t *scenario = calloc(1,
                    sizeof(*scenario));
            CHECK(scenario);
            wl_columnar_filter_test_prepare(scenario,
                wl_columnar_filter_test_source(width, rows, false, timestamps,
                false),
                true, program, 0, false);
            wl_columnar_filter_test_trace_begin(scenario, event + 1, mode);
            wl_columnar_filter_test_run(scenario);
            CHECK(scenario->filter.done);
            CHECK(wl_columnar_filter_test_event_count >= event + 1);
            CHECK(!wl_columnar_filter_test_successor_callbacks);
            int expected = mode == 0 ? WL_EVALUATION_CONTROL_CANCELLED : mode ==
                1 ? EOVERFLOW : EINVAL;
            CHECK(scenario->filter.status.execution_result == expected);
            wl_columnar_filter_test_trace_end();
            wl_evaluation_control_report_t report =
                wl_columnar_filter_test_complete(scenario);
            CHECK(report.attempt_id == scenario->target_id);
            if (mode < 2) CHECK(report.result == expected &&
                    report.consumed > 0);
            else CHECK(report.result == 0 && report.consumed > 0);
            free(scenario);
        }
    }
    printf(
        "fault checkpoints width=%u rows=%u actual=%u exercised=%u modes=3\n",
        width, rows, count, exercised);
    free(events);
}

static wl_columnar_memory_reservation_t wl_columnar_filter_test_peer;
static bool wl_columnar_filter_test_peer_inserted,
    wl_columnar_filter_test_peer_rolled_back;
static bool wl_columnar_filter_test_peer_collided;
static uint64_t wl_columnar_filter_test_peer_before;

static void
wl_columnar_filter_test_cas_before(wl_evaluation_control_t *control,
    wl_columnar_memory_governor_t *governor, uint64_t observed, bool overflow)
{
    (void)control; (void)observed;
    CHECK(!overflow);
    if (wl_columnar_filter_test_peer_inserted) return;
    wl_columnar_filter_test_peer_inserted = true;
    wl_columnar_memory_reservation_init(&wl_columnar_filter_test_peer);
    wl_columnar_filter_test_peer_before = wl_columnar_memory_reserved(governor);
    CHECK(wl_columnar_memory_reserve(governor, 17,
        &wl_columnar_filter_test_peer));
    CHECK(wl_columnar_memory_reserved(governor) ==
        wl_columnar_filter_test_peer_before + 17);
}

static void
wl_columnar_filter_test_cas_after(wl_evaluation_control_t *control,
    wl_columnar_memory_governor_t *governor, bool success, bool overflow)
{
    (void)control;
    if (wl_columnar_filter_test_peer_rolled_back) return;
    CHECK(wl_columnar_filter_test_peer_inserted && !overflow);
    if (!wl_columnar_filter_test_peer_collided) {
        CHECK(!success);
        CHECK(wl_columnar_memory_reserved(governor) ==
            wl_columnar_filter_test_peer_before + 17);
        wl_columnar_filter_test_peer_collided = true;
        return; /* Keep the peer until the next genuine strong CAS succeeds. */
    }
    CHECK(success);
    uint64_t with_peer = wl_columnar_memory_reserved(governor);
    CHECK(with_peer ==
        wl_columnar_filter_test_peer_before + 17 + sizeof(col_rel_t) + 8);
    CHECK(wl_columnar_memory_rollback(&wl_columnar_filter_test_peer));
    CHECK(wl_columnar_memory_reserved(governor) == with_peer - 17);
    wl_columnar_filter_test_peer_rolled_back = true;
}

static void
wl_columnar_filter_test_peer_collision(void)
{
    wl_columnar_filter_test_scenario_t *scenario = calloc(1, sizeof(*scenario));
    CHECK(scenario);
    wl_columnar_filter_test_prepare(scenario,
        wl_columnar_filter_test_source(1, 1, false, false, false), true, NULL,
        0, false);
    wl_columnar_filter_test_peer_inserted = false;
    wl_columnar_filter_test_peer_rolled_back = false;
    wl_columnar_filter_test_peer_collided = false;
    wl_columnar_memory_governor_test_before_total_cas =
        wl_columnar_filter_test_cas_before;
    wl_columnar_memory_governor_test_after_total_cas =
        wl_columnar_filter_test_cas_after;
    wl_columnar_filter_test_run(scenario);
    wl_columnar_memory_governor_test_before_total_cas = NULL;
    wl_columnar_memory_governor_test_after_total_cas = NULL;
    CHECK(wl_columnar_filter_test_peer_inserted &&
        wl_columnar_filter_test_peer_rolled_back);
    CHECK(scenario->filter.done && !scenario->filter.status.semantic_result);
    wl_columnar_filter_test_literal_t literal =
        wl_columnar_filter_test_literal(1, 1, false, false, false, NULL, 0);
    wl_evaluation_control_report_t report =
        wl_columnar_filter_test_complete(scenario);
    CHECK(report.consumed == literal.total + 84); /* One additional failed strong-CAS admission round. */
    free(literal.prefix);
    free(scenario);
}

static void
wl_columnar_filter_test_credit_retry(void)
{
    wl_columnar_filter_test_scenario_t *scenario = calloc(1, sizeof(*scenario));
    CHECK(scenario);
    wl_columnar_filter_test_prepare(scenario,
        wl_columnar_filter_test_source(17, 1, false, false, false), true, NULL,
        0, false);
    wl_columnar_filter_test_schedule(scenario, 0, false);
    wl_columnar_filter_test_one_turn(scenario);
    wl_columnar_filter_test_sentinel(scenario);
    bool armed = false;
    unsigned turns = 0;
    while (scenario->queue.count) {
        CHECK(++turns < 128);
        host_job_t *job = scenario->queue.jobs[scenario->queue.head];
        if (job == &scenario->filter.job && scenario->filter.started) {
            wl_columnar_filter_context_t *ctx = NULL;
            CHECK(wl_columnar_eval_stack_filter_context(scenario->filter.frame,
                &ctx) == 0);
            if (!armed && ctx->stage == WL_COLUMNAR_FILTER_STAGE_CREDIT) {
                CHECK(ctx->admitted && !ctx->heap &&
                    ctx->values == ctx->inline_values);
                wl_columnar_memory_governor_test_refuse_next_release();
                armed = true;
            }
        }
        bool sentinel = job->dispatch == host_sentinel_dispatch;
        wl_columnar_filter_test_one_turn(scenario);
        if (sentinel &&
            scenario->filter.job.queued) wl_columnar_filter_test_sentinel(
                scenario);
    }
    CHECK(armed && scenario->filter.parked && !scenario->filter.done);
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(wl_columnar_eval_stack_filter_context(scenario->filter.frame,
        &ctx) == 0);
    CHECK(ctx->stage == WL_COLUMNAR_FILTER_STAGE_CREDIT && ctx->admitted &&
        !ctx->heap);
    uint64_t held = wl_columnar_filter_test_reserved(scenario->session);
    wl_columnar_filter_test_schedule(scenario, 12, false);
    wl_columnar_filter_test_sentinel(scenario);
    wl_columnar_filter_test_drain(scenario);
    CHECK(scenario->filter.done &&
        wl_columnar_filter_test_reserved(scenario->session) < held);
    wl_evaluation_control_report_t report =
        wl_columnar_filter_test_complete(scenario);
    CHECK(report.result == 0);
    free(scenario);
}

static void
wl_columnar_filter_test_allocations(wl_columnar_filter_test_program_t *program)
{
    wl_columnar_filter_test_scenario_t *baseline = calloc(1, sizeof(*baseline));
    CHECK(baseline);
    wl_columnar_filter_test_prepare(baseline,
        wl_columnar_filter_test_source(17, 65, false, true, true), true,
        program, 0, false);
    wl_columnar_relation_test_constructor_fail_allocation = UINT32_MAX;
    wl_columnar_relation_test_growth_fail_allocation = UINT32_MAX;
    wl_columnar_expr_test_fail_allocation = 100000;
    wl_columnar_filter_test_attempt_fail_allocation = 100000;
    wl_columnar_filter_test_run(baseline);
    CHECK(baseline->filter.done && !baseline->filter.status.semantic_result);
    unsigned counts[] = {
        UINT32_MAX - wl_columnar_relation_test_constructor_fail_allocation,
        UINT32_MAX - wl_columnar_relation_test_growth_fail_allocation,
        (unsigned)(100000 - wl_columnar_expr_test_fail_allocation),
        (unsigned)(100000 - wl_columnar_filter_test_attempt_fail_allocation),
    };
    wl_columnar_relation_test_constructor_fail_allocation = 0;
    wl_columnar_relation_test_growth_fail_allocation = 0;
    wl_columnar_expr_test_fail_allocation = -1;
    wl_columnar_filter_test_attempt_fail_allocation = -1;
    wl_columnar_filter_test_complete(baseline);
    free(baseline);
    for (unsigned family = 0; family < 4; ++family) {
        CHECK(counts[family]);
        for (unsigned ordinal = 0; ordinal < counts[family]; ++ordinal) {
            wl_columnar_filter_test_scenario_t *scenario = calloc(1,
                    sizeof(*scenario));
            CHECK(scenario);
            wl_columnar_filter_test_prepare(scenario,
                wl_columnar_filter_test_source(17, 65, false, true, true), true,
                program, 0, false);
            if (family ==
                0) wl_columnar_relation_test_constructor_fail_allocation =
                    ordinal + 1;
            if (family ==
                1) wl_columnar_relation_test_growth_fail_allocation = ordinal +
                    1;
            if (family ==
                2) wl_columnar_expr_test_fail_allocation = (int)ordinal;
            if (family ==
                3) wl_columnar_filter_test_attempt_fail_allocation =
                    (int)ordinal;
            wl_columnar_filter_test_run(scenario);
            wl_columnar_relation_test_constructor_fail_allocation = 0;
            wl_columnar_relation_test_growth_fail_allocation = 0;
            wl_columnar_expr_test_fail_allocation = -1;
            wl_columnar_filter_test_attempt_fail_allocation = -1;
            CHECK(scenario->filter.done &&
                scenario->filter.status.execution_result == ENOMEM);
            CHECK(!scenario->filter.status.committed &&
                !scenario->session->memory_budget_denied);
            wl_evaluation_control_report_t report =
                wl_columnar_filter_test_complete(scenario);
            CHECK(report.result == ENOMEM && report.consumed > 0);
            free(scenario);
        }
        printf("allocation family=%u reached=%u injected=%u\n", family,
            counts[family], counts[family]);
    }
}

static void
wl_columnar_filter_test_host_credit_boundaries(void)
{
    static const uint64_t limits[] = {1, 127, 128, 255, 256, 290, 291, 511, 512,
                                      0};
    for (unsigned sentinel_kind = 0; sentinel_kind < 2; ++sentinel_kind)
        for (size_t i = 0; i < sizeof(limits) / sizeof(limits[0]); ++i) {
            int owner;
            uint64_t limit = limits[i], total = sentinel_kind ? 291 : 512;
            wl_evaluation_control_t *control =
                wl_columnar_filter_test_control(limit, &owner);
            uint64_t id = wl_evaluation_control_active_attempt_id(control);
            host_sentinel_entry_t sentinel = {0};
            sentinel.job.dispatch = host_sentinel_dispatch;
            sentinel.job.data = &sentinel;
            wl_columnar_filter_host_caller_action_t diagnostic = {0};
            diagnostic.identity = &diagnostic;
            diagnostic.job.dispatch =
                wl_columnar_filter_host_caller_action_dispatch;
            diagnostic.job.data = &diagnostic;
            /* Genuine original job storage, deliberately NULL caller. The
             * dedicated dispatcher normalizes this finite protocol diagnosis. */
            host_job_t *job = sentinel_kind ? &sentinel.job : &diagnostic.job;
            wl_columnar_filter_host_lifecycle_credit_t credit = {0};
            int rc = wl_columnar_filter_host_lifecycle_admit(&credit, job,
                    sentinel_kind ? WL_COLUMNAR_FILTER_HOST_SENTINEL :
                    WL_COLUMNAR_FILTER_HOST_CALLER_ACTION,
                    control, id);
            bool full = !limit || limit >= total;
            CHECK(rc == (full ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT));
            uint64_t used = full ? total : limit < 128 ? 0 : limit <
                256 ? 128 : 256;
            if (full) {
                host_queue_t queue = {0};
                CHECK(wl_columnar_filter_host_lifecycle_schedule(&queue,
                    &credit) == 0);
                CHECK(credit.phase == WL_COLUMNAR_FILTER_HOST_SPENT &&
                    !credit.job);
                CHECK(host_turn(&queue) == 0 && !queue.count);
                if (sentinel_kind) CHECK(sentinel.visits == 1);
                else CHECK(diagnostic.done &&
                        diagnostic.protocol_result == EINVAL);
            } else if (used) {
                CHECK(credit.phase == WL_COLUMNAR_FILTER_HOST_CANCELLED &&
                    !credit.job);
            } else {
                CHECK(!credit.identity && !credit.job &&
                    credit.phase == WL_COLUMNAR_FILTER_HOST_EMPTY);
            }
            CHECK(wl_evaluation_control_finish(control, &owner, 0, 0) == rc);
            wl_evaluation_control_report_t report;
            CHECK(wl_evaluation_control_report(control, &report) == 0);
            CHECK(report.attempt_id == id && report.limit == limit &&
                report.consumed == used);
            CHECK(wl_evaluation_control_detach(control, &owner) == 0);
            wl_evaluation_control_release(control);
        }
    /* A copied original credit is rejected without consuming its original.
     * The diagnostic has its own budget; a spent credit never pays another call. */
    int owner, diagnostic_owner;
    wl_evaluation_control_t *control = wl_columnar_filter_test_control(291,
            &owner);
    host_sentinel_entry_t sentinel = {0};
    sentinel.job.dispatch = host_sentinel_dispatch;
    sentinel.job.data = &sentinel;
    wl_columnar_filter_host_lifecycle_credit_t credit = {0};
    CHECK(wl_columnar_filter_host_lifecycle_admit(&credit, &sentinel.job,
        WL_COLUMNAR_FILTER_HOST_SENTINEL, control,
        wl_evaluation_control_active_attempt_id(control)) == 0);
    wl_evaluation_control_t *diagnosis = wl_columnar_filter_test_control(256,
            &diagnostic_owner);
    CHECK(wl_evaluation_control_charge(diagnosis, 256) == 0);
    wl_columnar_filter_host_lifecycle_credit_t copy = credit;
    host_queue_t queue = {0};
    CHECK(wl_columnar_filter_host_lifecycle_schedule(&queue, &copy) == EINVAL);
    CHECK(!queue.count && credit.phase == WL_COLUMNAR_FILTER_HOST_ADMITTED);
    CHECK(wl_columnar_filter_host_lifecycle_cancel(&credit) == 0);
    CHECK(credit.phase == WL_COLUMNAR_FILTER_HOST_CANCELLED && !credit.job);
    CHECK(wl_evaluation_control_finish(diagnosis, &diagnostic_owner, 0,
        0) == 0);
    CHECK(wl_evaluation_control_detach(diagnosis, &diagnostic_owner) == 0);
    wl_evaluation_control_release(diagnosis);
    CHECK(wl_evaluation_control_finish(control, &owner, 0, 0) == 0);
    CHECK(wl_evaluation_control_detach(control, &owner) == 0);
    wl_evaluation_control_release(control);
}

static void
wl_columnar_filter_test_nonfinite(wl_columnar_filter_test_program_t *program,
    bool referenced)
{
    wl_col_session_t *raw_session = wl_columnar_filter_test_session();
    wl_columnar_eval_stack_cleanup_frame_t *raw_frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(raw_session, &raw_frame) == 0);
    eval_stack_t *raw_stack = wl_columnar_eval_stack_cleanup_stack(raw_frame);
    col_rel_t *raw_input = wl_columnar_filter_test_source(2, 5, true, false,
            false);
    /* Deliberate invalid-payload route audit, not an assertion that a public
     * producer accepts nonfinite FLOAT rows. Do not force a different raw path. */
    raw_input->columns[1][4] = wl_columnar_float_to_bits(NAN);
    CHECK(eval_stack_push(raw_stack, raw_input, true) == 0);
    wl_plan_op_t op = {.op = WL_PLAN_OP_FILTER};
    op.filter_expr.data = program->bytes;
    op.filter_expr.size = program->size;
    int raw_result = wl_columnar_filter_op(&op, raw_stack, raw_session);
    wl_columnar_filter_test_scenario_t *scenario = calloc(1, sizeof(*scenario));
    CHECK(scenario);
    col_rel_t *input = wl_columnar_filter_test_source(2, 5, true, false, false);
    input->columns[1][4] = wl_columnar_float_to_bits(NAN);
    wl_columnar_filter_test_prepare(scenario, input, true, program, 0, false);
    wl_columnar_filter_test_run(scenario);
    CHECK(scenario->filter.done);
    int controlled_result = scenario->filter.status.execution_result;
    if (raw_result != controlled_result) {
        fprintf(stderr,
            "nonfinite raw route discrepancy referenced=%d raw=%d controlled=%d\n",
            (int)referenced, raw_result, controlled_result);
        CHECK(raw_result == controlled_result); /* Requires architectural adjudication. */
    }
    if (!raw_result) {
        eval_stack_t *actual =
            wl_columnar_eval_stack_cleanup_stack(scenario->filter.frame);
        CHECK(raw_stack->top == 1 && actual->top == 1);
        wl_columnar_filter_test_equal(actual->items[0].rel,
            raw_stack->items[0].rel);
    }
    wl_evaluation_control_report_t report =
        wl_columnar_filter_test_complete(scenario);
    CHECK(report.result == controlled_result);
    free(scenario);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&raw_frame) == 0);
    CHECK(!wl_columnar_filter_test_reserved(raw_session));
    wl_columnar_memory_governor_ref_release(raw_session->memory_governor);
    free(raw_session);
}

static void
wl_columnar_filter_test_unsupported(void)
{
    for (unsigned rows = 0; rows < 2; ++rows) {
        wl_columnar_filter_test_program_t program = {0};
        wl_columnar_filter_test_opcode(&program, WL_PLAN_EXPR_CONST_STR);
        wl_columnar_filter_test_scenario_t *scenario = calloc(1,
                sizeof(*scenario));
        CHECK(scenario);
        wl_columnar_filter_test_prepare(scenario,
            wl_columnar_filter_test_source(1, rows, false, false, false), true,
            &program, 0, false);
        wl_columnar_filter_test_run(scenario);
        CHECK(scenario->filter.done &&
            scenario->filter.status.execution_result == ENOTSUP);
        CHECK(!scenario->filter.status.committed);
        wl_evaluation_control_report_t report =
            wl_columnar_filter_test_complete(scenario);
        CHECK(report.result == ENOTSUP && report.consumed > 0);
        free(scenario);
    }
}

/* Exercise real original table ownership before MAP forward execution exists.
 * Setup/observation are outside the bounded FIFO turns. The independent
 * setup control prepays each first table turn before publishing any owner. */
static void
wl_columnar_filter_test_table_retirement(unsigned slots, unsigned mode,
    bool refuse)
{
    wl_columnar_filter_test_scenario_t *scenario = calloc(1, sizeof(*scenario));
    CHECK(scenario);
    wl_columnar_filter_test_prepare(scenario,
        wl_columnar_filter_test_source(1, 0, false, false, false), true, NULL,
        0, false);
    wl_columnar_filter_test_schedule(scenario, 0, false);
    wl_columnar_filter_test_one_turn(scenario);
    wl_columnar_filter_test_one_turn(scenario); /* START returned, cleanup queued. */
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(wl_columnar_eval_stack_filter_context(scenario->filter.frame,
        &ctx) == 0);
    CHECK(ctx->stage == WL_COLUMNAR_FILTER_STAGE_BATCH);
    wl_columnar_filter_program_table_t *table = &ctx->table;
    int setup_owner;
    wl_evaluation_control_t *setup = wl_columnar_filter_test_control(0,
            &setup_owner);
    uint64_t setup_id = wl_evaluation_control_active_attempt_id(setup);
    for (unsigned i = 0; i < 6; ++i)
        CHECK(wl_evaluation_control_charge(setup, 256) == 0);
    wl_columnar_memory_reservation_init(&table->credit);
    table->initialized = true;
    if (mode) {
        table->governor = scenario->session->memory_governor;
        wl_columnar_memory_governor_ref_retain(table->governor);
        table->ref_held = true;
        CHECK(wl_columnar_memory_reserve(
                wl_columnar_memory_governor_ref_get(table->governor),
                7 * sizeof(*table->programs), &table->credit));
        table->admitted = true;
    }
    table->programs = malloc(7 * sizeof(*table->programs));
    CHECK(table->programs);
    table->allocated = true;
    if (mode == 2) {
        CHECK(wl_columnar_memory_commit(&table->credit, table->programs));
        table->committed = true;
    }
    wl_columnar_filter_test_program_t program = {0};
    wl_columnar_filter_test_constant(&program, WL_PLAN_EXPR_CONST_INT, 7);
    for (unsigned i = 0; i < slots; ++i) {
        CHECK(wl_evaluation_control_charge(setup, 256) == 0);
        table->programs[i] = NULL;
        ++table->initialized_slots;
        if (i % 2 == 0) {
            bool supported = false;
            CHECK(wl_columnar_expr_compile_attempt_controlled(program.bytes,
                program.size, table->governor, &table->programs[i], &supported,
                setup, setup_id) == 0);
            CHECK(supported && table->programs[i]);
        }
    }
    ctx->stage = WL_COLUMNAR_FILTER_STAGE_TABLE_PROGRAM;
    unsigned freed = 0, turns = 0;
    bool refused = false;
    while (scenario->queue.count) {
        CHECK(++turns < 128);
        bool program_turn = ctx->stage == WL_COLUMNAR_FILTER_STAGE_TABLE_PROGRAM
            && ctx->table.retirement_cursor < slots;
        if (refuse && !refused &&
            ctx->stage == WL_COLUMNAR_FILTER_STAGE_TABLE_CREDIT) {
            CHECK(!table->allocated && !table->programs && table->admitted);
            wl_columnar_memory_governor_test_refuse_next_release();
            refused = true;
        }
        wl_columnar_filter_test_one_turn(scenario);
        if (program_turn) {
            ++freed;
            CHECK(table->retirement_cursor == freed);
            CHECK(table->programs[freed - 1] == NULL);
        }
    }
    CHECK(freed == slots);
    if (refuse) {
        CHECK(refused && scenario->filter.parked && table->admitted);
        CHECK(ctx->stage == WL_COLUMNAR_FILTER_STAGE_TABLE_CREDIT);
        wl_columnar_filter_test_schedule(scenario, 12, false);
        wl_columnar_filter_test_drain(scenario);
    }
    CHECK(scenario->filter.done && !table->programs && !table->allocated);
    CHECK(!table->admitted && !table->ref_held && !table->governor);
    CHECK(!table->initialized_slots && !table->retirement_cursor &&
        !table->committed);
    CHECK(wl_evaluation_control_finish(setup, &setup_owner, 0, 0) == 0);
    CHECK(wl_evaluation_control_detach(setup, &setup_owner) == 0);
    wl_evaluation_control_release(setup);
    wl_evaluation_control_report_t report =
        wl_columnar_filter_test_complete(scenario);
    CHECK(report.result == 0);
    free(scenario);
}

int
main(void)
{
    printf(
        "numeric FILTER ABI descriptor=%zu frame=%zu program_owner=%zu instruction=%zu timestamp=%zu\n",
        sizeof(col_rel_t), wl_columnar_eval_stack_test_frame_size(),
        wl_columnar_expr_test_attempt_numeric_owner_size(),
        wl_columnar_expr_test_attempt_numeric_instruction_size(),
        sizeof(col_delta_timestamp_t));
    wl_columnar_filter_test_program_t yes = {0}, no = {0}, mixed = {0},
        floating = {0};
    wl_columnar_filter_test_constant(&yes, WL_PLAN_EXPR_CONST_INT, 1);
    wl_columnar_filter_test_constant(&no, WL_PLAN_EXPR_CONST_INT, 0);
    wl_columnar_filter_test_variable(&mixed, WL_PLAN_EXPR_VAR, 0);
    wl_columnar_filter_test_constant(&mixed, WL_PLAN_EXPR_CONST_INT, 0);
    wl_columnar_filter_test_opcode(&mixed, WL_PLAN_EXPR_CMP_GT);
    wl_columnar_filter_test_variable(&floating, WL_PLAN_EXPR_VAR_FLOAT, 1);
    wl_columnar_filter_test_constant(&floating, WL_PLAN_EXPR_CONST_FLOAT,
        wl_columnar_float_to_bits(-4.0));
    wl_columnar_filter_test_opcode(&floating, WL_PLAN_EXPR_CMP_FLOAT_GT);
    wl_columnar_filter_test_parity(0, 0, false, false, false, NULL, 0, false);
    wl_columnar_filter_test_parity(0, 3, false, true, false, &yes, 0, false);
    wl_columnar_filter_test_parity(1, 0, false, false, false, &yes, 0, false);
    wl_columnar_filter_test_parity(1, 15, false, false, false, &mixed, 0,
        false);
    wl_columnar_filter_test_parity(17, 133, true, true, true, &mixed, 0, false);
    wl_columnar_filter_test_parity(37, 270, true, true, true, NULL, 0, false);
    wl_columnar_filter_test_parity(2, 15, true, false, false, &floating, 0,
        false);
    wl_columnar_filter_test_parity(1, 15, false, false, false, &no, 0, false);
    wl_columnar_filter_test_literal_t same = wl_columnar_filter_test_literal(1,
            1, false, false, false, NULL, 0);
    wl_evaluation_control_report_t shared = wl_columnar_filter_test_parity(1, 1,
            false, false, false, NULL, 0, true);
    CHECK(shared.consumed == same.total + 128 + 16u * 512u + 32u * 291u);
    free(same.prefix);
    wl_columnar_filter_test_budget(0, 0, false, false, false, NULL, 0, true);
    wl_columnar_filter_test_budget(1, 1, false, true, false, &yes, 0, false);
    wl_columnar_filter_test_budget(17, 65, true, true, true, NULL, 0, false);
    wl_columnar_filter_test_budget(17, 133, false, true, false, &yes, 0, false);
    wl_columnar_filter_test_budget(1, 3, false, false, false, &no, 1, false);
    for (unsigned mode = 0; mode < 3; ++mode) {
        wl_columnar_filter_test_table_retirement(0, mode, false);
        wl_columnar_filter_test_table_retirement(3, mode, mode == 2);
    }
    wl_columnar_filter_test_no_first_admission();
    wl_columnar_filter_test_busy_finish();
    wl_columnar_filter_test_credit_retry();
    wl_columnar_filter_test_host_credit_boundaries();
    wl_columnar_filter_test_peer_collision();
    wl_columnar_filter_test_unsupported();
    wl_columnar_filter_test_nonfinite(&mixed, false);
    wl_columnar_filter_test_nonfinite(&floating, true);
    wl_columnar_filter_test_faults(1, 1, true, &yes, true);
    wl_columnar_filter_test_faults(17, 133, true, &yes, false);
    wl_columnar_filter_test_allocations(&yes);
    printf(
        "numeric FILTER: semantic, literal, FIFO, retention, authority and fault witnesses passed\n");
    return 0;
}
