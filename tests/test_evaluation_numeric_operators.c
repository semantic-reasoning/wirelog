/* #1821 cooperative numeric FILTER: semantic, lifetime and literal-budget
 * fixtures. Each operator dispatch returns to an outer FIFO. Independent jobs
 * witness the scheduling boundary; an inline while(YIELD) cannot reuse this
 * per-dispatch contract. */
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/wirelog.h"
#include "../wirelog/thread.h"
#include <errno.h>
#include <math.h>
#include "../wirelog/thread.h"
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
    int (*start)(const wl_plan_op_t *, wl_columnar_eval_stack_cleanup_frame_t *,
        wl_col_session_t *, wl_evaluation_control_t *, uint64_t,
        wl_columnar_filter_status_t *);
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
        : entry->start(entry->op, entry->frame,
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
#define WL_COLUMNAR_FILTER_TEST_SENTINELS 48u
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

/* Dedicated legal physical provenance cases: 1=all, 2=positive col0, 3=none.
 * Legacy adversarial sentinel fixtures and literal budgets remain unchanged. */
static unsigned wl_columnar_filter_test_physical_pattern;

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
            if (wl_columnar_filter_test_physical_pattern) {
                const int64_t weights[] = {0, 1, -1, 3, -4};
                relation->timestamps[r] = (col_delta_timestamp_t){
                    .iteration = 101 + r, .stratum = 2 * r,
                    .worker = r % 3, ._reserved = 0,
                    .multiplicity = weights[r % 5]
                };
            }
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
    scenario->filter.start = wl_columnar_filter_op_attempt_start;
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
    if (wl_columnar_filter_test_physical_pattern) {
        const col_rel_t *actual = actual_stack->items[0].rel;
        const int64_t weights[] = {0, 1, -1, 3, -4};
        uint32_t selected = 0;
        CHECK(timestamps && actual->timestamps && !typed);
        for (uint32_t r = 0; r < rows; ++r) {
            bool keep = wl_columnar_filter_test_physical_pattern == 1
                || (wl_columnar_filter_test_physical_pattern == 2 && r % 7 > 3);
            if (!keep) continue;
            CHECK(selected < actual->nrows);
            const col_delta_timestamp_t *ts = &actual->timestamps[selected];
            CHECK(ts->iteration == 101 + r && ts->stratum == 2 * r
                && ts->worker == r % 3 && ts->_reserved == 0
                && ts->multiplicity == weights[r % 5]);
            for (uint32_t c = 0; c < width; ++c)
                CHECK(actual->columns[c][selected]
                    == (int64_t)(r % 7) - 3 + (int64_t)c * 11);
            ++selected;
        }
        CHECK(actual->nrows == selected);
    }
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
    R(192, 36); L(192); L(192); L(170);
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
wl_columnar_filter_test_busy_finish(bool map)
{
    wl_columnar_filter_test_scenario_t *scenario = calloc(1, sizeof(*scenario));
    CHECK(scenario);
    col_rel_t *input = wl_columnar_filter_test_source(1, 3, false, true, false);
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(wl_columnar_source_access_reader_acquire(&input->source_access,
        &reader) == 0);
    wl_columnar_filter_test_prepare(scenario, input, true, NULL, 0, false);
    if (map) {
        scenario->op.op = WL_PLAN_OP_MAP;
        scenario->op.project_count = 2;
        scenario->filter.start = wl_columnar_filter_map_attempt_start;
    }
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
wl_columnar_filter_test_credit_retry(bool map)
{
    wl_columnar_filter_test_scenario_t *scenario = calloc(1, sizeof(*scenario));
    CHECK(scenario);
    wl_columnar_filter_test_prepare(scenario,
        wl_columnar_filter_test_source(17, 1, false, false, false), true, NULL,
        0, false);
    if (map) {
        scenario->op.op = WL_PLAN_OP_MAP;
        scenario->op.project_count = 2;
        scenario->filter.start = wl_columnar_filter_map_attempt_start;
    }
    wl_columnar_filter_stage_t stage = map
        ? WL_COLUMNAR_FILTER_STAGE_TABLE_CREDIT :
        WL_COLUMNAR_FILTER_STAGE_CREDIT;
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
            if (!armed && ctx->stage == stage) {
                CHECK(map ? ctx->table.admitted && !ctx->table.programs
                    : ctx->admitted && !ctx->heap &&
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
    CHECK(ctx->stage == stage);
    CHECK(map ? ctx->table.admitted && !ctx->table.programs
        : ctx->admitted && !ctx->heap);
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
    bool referenced, bool map)
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
    wl_plan_expr_buffer_t expression = {.data = program->bytes,
                                        .size = program->size};
    if (map) {
        op.op = WL_PLAN_OP_MAP; op.project_count = 2;
        op.map_expr_count = 1; op.map_exprs = &expression;
    }
    int raw_result = map ? col_op_map(&op, raw_stack, raw_session)
        : wl_columnar_filter_op(&op, raw_stack, raw_session);
    wl_columnar_filter_test_scenario_t *scenario = calloc(1, sizeof(*scenario));
    CHECK(scenario);
    col_rel_t *input = wl_columnar_filter_test_source(2, 5, true, false, false);
    input->columns[1][4] = wl_columnar_float_to_bits(NAN);
    wl_columnar_filter_test_prepare(scenario, input, true, program, 0, false);
    if (map) {
        scenario->op = op;
        scenario->filter.start = wl_columnar_filter_map_attempt_start;
    }
    wl_columnar_filter_test_run(scenario);
    CHECK(scenario->filter.done);
    int controlled_result = scenario->filter.status.execution_result;
    if (map) CHECK(raw_result == (referenced ? ERANGE : EINVAL));
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

static void
wl_columnar_filter_test_map_prepare(
    wl_columnar_filter_test_scenario_t *scenario,
    uint32_t width, uint32_t rows, bool typed, bool timestamps,
    const wl_plan_op_t *op, uint64_t limit)
{
    wl_columnar_filter_test_prepare(scenario,
        wl_columnar_filter_test_source(width, rows, typed, timestamps, false),
        true, NULL, limit, false);
    scenario->op = *op;
    scenario->filter.start = wl_columnar_filter_map_attempt_start;
}

static void
wl_columnar_filter_test_map_run(wl_columnar_filter_test_scenario_t *scenario)
{
    wl_columnar_filter_test_schedule(scenario, 0, false);
    wl_columnar_filter_test_one_turn(scenario);
    wl_columnar_filter_test_sentinel(scenario);
    unsigned turns = 0;
    while (scenario->queue.count) {
        CHECK(++turns < 4096);
        bool sentinel = scenario->queue.jobs[scenario->queue.head]->dispatch
            == host_sentinel_dispatch;
        wl_columnar_filter_test_one_turn(scenario);
        if (sentinel && scenario->filter.job.queued &&
            scenario->sentinel_next < 4)
            wl_columnar_filter_test_sentinel(scenario);
    }
    CHECK(scenario->filter.done || scenario->filter.parked);
}

static void
wl_columnar_filter_test_map_original(
    wl_columnar_filter_test_scenario_t *scenario,
    const eval_entry_t *original)
{
    eval_stack_t *stack =
        wl_columnar_eval_stack_cleanup_stack(scenario->filter.frame);
    CHECK(stack->top == 1);
    const eval_entry_t *entry = &stack->items[0];
    CHECK(entry->kind == original->kind && entry->rel == original->rel
        && entry->owned == original->owned &&
        entry->continuation == original->continuation);
    CHECK(entry->is_delta == original->is_delta &&
        entry->seg_boundaries == original->seg_boundaries &&
        entry->seg_count == original->seg_count);
}

static wl_evaluation_control_report_t
wl_columnar_filter_test_map_case(uint32_t width, uint32_t rows,
    uint32_t outputs,
    unsigned mode, uint32_t expr_count, bool typed, uint64_t limit,
    int expected)
{
    wl_columnar_filter_test_program_t *programs = calloc(outputs ? outputs : 1,
            sizeof(*programs));
    wl_plan_expr_buffer_t *expressions = calloc(expr_count ? expr_count : 1,
            sizeof(*expressions));
    uint32_t *indices = calloc(outputs ? outputs : 1, sizeof(*indices));
    CHECK(programs && expressions && indices);
    for (uint32_t c = 0; c < outputs; ++c) {
        indices[c] = c + 1 == outputs ? width + 3 : width ? (c + 1) % width : 0;
        if (mode && c < expr_count && c % 2 == 0) {
            wl_columnar_filter_test_program_t *program = &programs[c];
            if (mode == 1 || mode == 3) {
                wl_columnar_filter_test_variable(program, WL_PLAN_EXPR_VAR, 0);
                wl_columnar_filter_test_constant(program,
                    WL_PLAN_EXPR_CONST_INT, mode == 3 ? 0 : c + 7);
                wl_columnar_filter_test_opcode(program,
                    mode ==
                    3 ? WL_PLAN_EXPR_ARITH_DIV : WL_PLAN_EXPR_ARITH_ADD);
            } else if (mode == 5) {
                wl_columnar_filter_test_variable(program,
                    WL_PLAN_EXPR_VAR_FLOAT, width - 1);
                wl_columnar_filter_test_constant(program,
                    WL_PLAN_EXPR_CONST_FLOAT, wl_columnar_float_to_bits(0.25));
                wl_columnar_filter_test_opcode(program, WL_PLAN_EXPR_ARITH_ADD);
            } else if (mode == 6) {
                wl_columnar_filter_test_constant(program,
                    WL_PLAN_EXPR_CONST_INT, INT64_MAX);
                wl_columnar_filter_test_constant(program,
                    WL_PLAN_EXPR_CONST_INT, 1);
                wl_columnar_filter_test_opcode(program, WL_PLAN_EXPR_ARITH_ADD);
            } else if (mode == 2) {
                wl_columnar_filter_test_constant(program,
                    WL_PLAN_EXPR_CONST_FLOAT,
                    wl_columnar_float_to_bits(-0.5));
            } else {
                wl_columnar_filter_test_opcode(program, WL_PLAN_EXPR_CONST_STR);
            }
            expressions[c].data = program->bytes;
            expressions[c].size = program->size;
        }
    }
    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_MAP;
    op.project_count = outputs;
    op.project_indices = indices;
    op.map_exprs = mode ? expressions : NULL;
    op.map_expr_count = expr_count;
    wl_columnar_filter_test_scenario_t *scenario = calloc(1, sizeof(*scenario));
    CHECK(scenario);
    wl_columnar_filter_test_map_prepare(scenario, width, rows, typed, true, &op,
        limit);
    eval_stack_t *stack =
        wl_columnar_eval_stack_cleanup_stack(scenario->filter.frame);
    eval_entry_t original = stack->items[0];
    wl_columnar_filter_test_map_run(scenario);
    CHECK(scenario->filter.done &&
        scenario->filter.status.execution_result == expected);
    if (!expected) {
        wl_col_session_t *raw_session = wl_columnar_filter_test_session();
        eval_stack_t raw_stack = {0};
        CHECK(eval_stack_push(&raw_stack,
            wl_columnar_filter_test_source(width, rows, typed, true, false),
            true) == 0);
        CHECK(col_op_map(&op, &raw_stack, raw_session) == 0);
        CHECK(scenario->filter.status.committed && stack->top == 1);
        wl_columnar_filter_test_equal(stack->items[0].rel,
            raw_stack.items[0].rel);
        CHECK((stack->items[0].rel->column_types != NULL) == (outputs != 0));
        CHECK(!stack->items[0].rel->timestamps);
        CHECK(eval_stack_dispose_entry(&raw_stack, &raw_stack.items[0]) == 0);
        CHECK(!wl_columnar_filter_test_reserved(raw_session));
        wl_columnar_memory_governor_ref_release(raw_session->memory_governor);
        free(raw_session);
    } else {
        CHECK(!scenario->filter.status.committed);
        wl_columnar_filter_test_map_original(scenario, &original);
        if (expected == ERANGE) {
            wl_col_session_t *raw_session = wl_columnar_filter_test_session();
            eval_stack_t raw_stack = {0};
            CHECK(eval_stack_push(&raw_stack,
                wl_columnar_filter_test_source(width, rows, typed, true, false),
                true) == 0);
            CHECK(col_op_map(&op, &raw_stack, raw_session) == ERANGE);
            CHECK(!raw_stack.top &&
                !wl_columnar_filter_test_reserved(raw_session));
            wl_columnar_memory_governor_ref_release(
                raw_session->memory_governor);
            free(raw_session);
        }
    }
    wl_evaluation_control_report_t report =
        wl_columnar_filter_test_complete(scenario);
    CHECK(report.result == expected);
    free(scenario); free(indices); free(expressions); free(programs);
    return report;
}

static void
map_schema_types(bool computed, uint32_t rows)
{
    const wirelog_column_type_t source_types[] = {
        WIRELOG_TYPE_INT32, WIRELOG_TYPE_INT64, WIRELOG_TYPE_UINT32,
        WIRELOG_TYPE_UINT64, WIRELOG_TYPE_FLOAT, WIRELOG_TYPE_STRING,
        WIRELOG_TYPE_BOOL,
    };
    uint32_t indices[] = {4, 0, 2, 3, 5, 6, 4, 99};
    const wirelog_column_type_t projected[] = {
        WIRELOG_TYPE_FLOAT, WIRELOG_TYPE_INT32, WIRELOG_TYPE_UINT32,
        WIRELOG_TYPE_UINT64, WIRELOG_TYPE_STRING, WIRELOG_TYPE_BOOL,
        WIRELOG_TYPE_FLOAT, WIRELOG_TYPE_INT64,
    };
    const wirelog_column_type_t calculated[] = {
        WIRELOG_TYPE_FLOAT, WIRELOG_TYPE_BOOL, WIRELOG_TYPE_INT64,
        WIRELOG_TYPE_STRING,
    };
    wl_columnar_filter_test_program_t programs[4] = {{0}};
    wl_plan_expr_buffer_t expressions[4] = {{0}};
    for (unsigned c = 0; c < 2; ++c) {
        wl_columnar_filter_test_variable(&programs[c], WL_PLAN_EXPR_VAR_FLOAT,
            4);
        wl_columnar_filter_test_constant(&programs[c], WL_PLAN_EXPR_CONST_FLOAT,
            wl_columnar_float_to_bits(0.0));
        wl_columnar_filter_test_opcode(&programs[c], c
            ? WL_PLAN_EXPR_CMP_FLOAT_LT : WL_PLAN_EXPR_ARITH_FLOAT_ADD);
    }
    wl_columnar_filter_test_variable(&programs[2], WL_PLAN_EXPR_VAR, 0);
    wl_columnar_filter_test_constant(&programs[2], WL_PLAN_EXPR_CONST_INT, 1);
    wl_columnar_filter_test_opcode(&programs[2], WL_PLAN_EXPR_ARITH_ADD);
    wl_columnar_filter_test_variable(&programs[3], WL_PLAN_EXPR_VAR, 5);
    for (unsigned c = 0; c < 4; ++c)
        expressions[c] = (wl_plan_expr_buffer_t){programs[c].bytes,
                                                 programs[c].size};
    wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = computed ? 4 : 8,
                       .project_indices = computed ? NULL : indices,
                       .map_exprs = computed ? expressions : NULL,
                       .map_expr_count = computed ? 4 : 0};
    col_rel_t *input = wl_columnar_filter_test_source(7, 0, false, false,
            false);
    CHECK(!col_rel_set_column_types(input, source_types, 7));
    for (uint32_t r = 0; r < rows; ++r) {
        int64_t values[] = {7 + r, -3, 9, 11, 0, 17, r % 2};
        values[4] = wl_columnar_float_to_bits((double)r - 1.0);
        CHECK(!col_rel_append_row(input, values));
    }
    if (rows > 1) input->columns[4][1] = wl_columnar_float_to_bits(-0.0);
    col_rel_t *raw_input = NULL;
    CHECK(!col_rel_deep_copy(input, &raw_input, NULL));
    wl_columnar_filter_test_scenario_t *scenario = calloc(1, sizeof(*scenario));
    CHECK(scenario);
    wl_columnar_filter_test_prepare(scenario, input, true, NULL, 0, false);
    scenario->op = op;
    scenario->filter.start = wl_columnar_filter_map_attempt_start;
    wl_columnar_filter_test_map_run(scenario);
    CHECK(scenario->filter.done && !scenario->filter.status.execution_result);
    eval_stack_t *actual =
        wl_columnar_eval_stack_cleanup_stack(scenario->filter.frame);
    CHECK(actual->top == 1 && actual->items[0].rel->nrows == rows);
    col_rel_t *result = actual->items[0].rel;
    CHECK(result->column_types && result->ncols == op.project_count);
    for (uint32_t c = 0; c < result->ncols; ++c)
        CHECK(result->column_types[c] ==
            (computed ? calculated[c] : projected[c]));
    for (uint32_t r = 0; r < rows; ++r) {
        CHECK(result->columns[0][r] ==
            wl_columnar_float_to_bits((double)r - 1.0));
        if (computed) {
            CHECK(result->columns[1][r] == (r == 0));
            CHECK(result->columns[2][r] == 8 + r &&
                result->columns[3][r] == 17);
        } else {
            CHECK(result->columns[1][r] == 7 + r && result->columns[2][r] == 9);
            CHECK(result->columns[3][r] == 11 && result->columns[4][r] == 17);
            CHECK(result->columns[5][r] == r % 2 && result->columns[7][r] == 0);
            CHECK(result->columns[6][r] == result->columns[0][r]);
        }
    }
    wl_col_session_t *raw_session = wl_columnar_filter_test_session();
    eval_stack_t raw = {0}; CHECK(!eval_stack_push(&raw, raw_input, true));
    CHECK(!col_op_map(&op, &raw, raw_session));
    wl_columnar_filter_test_equal(result, raw.items[0].rel);
    CHECK(!eval_stack_drain(&raw) &&
        !wl_columnar_filter_test_reserved(raw_session));
    wl_columnar_memory_governor_ref_release(raw_session->memory_governor);
    free(raw_session);
    wl_columnar_filter_test_complete(scenario); free(scenario);
}

static void
map_omitted_nonfinite(void)
{
    wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = 1};
    wl_columnar_filter_test_scenario_t *scenario = calloc(1, sizeof(*scenario));
    CHECK(scenario);
    wl_columnar_filter_test_map_prepare(scenario, 2, 3, true, false, &op, 0);
    scenario->input->columns[1][2] = wl_columnar_float_to_bits(NAN);
    wl_columnar_filter_test_map_run(scenario);
    CHECK(scenario->filter.done && !scenario->filter.status.execution_result);
    col_rel_t *result =
        wl_columnar_eval_stack_cleanup_stack(
        scenario->filter.frame)->items[0].rel;
    CHECK(result->column_types &&
        result->column_types[0] == WIRELOG_TYPE_INT64);
    CHECK(result->nrows == 3);
    for (uint32_t r = 0; r < 3;
        ++r) CHECK(result->columns[0][r] == (int64_t)r - 3);
    wl_col_session_t *session = wl_columnar_filter_test_session();
    col_rel_t *input = wl_columnar_filter_test_source(2, 3, true, false, false);
    input->columns[1][2] = wl_columnar_float_to_bits(NAN);
    eval_stack_t raw = {0}; CHECK(!eval_stack_push(&raw, input, true));
    CHECK(!col_op_map(&op, &raw, session));
    wl_columnar_filter_test_equal(result, raw.items[0].rel);
    CHECK(!eval_stack_drain(&raw) &&
        !wl_columnar_filter_test_reserved(session));
    wl_columnar_memory_governor_ref_release(session->memory_governor);
    free(session);
    wl_columnar_filter_test_complete(scenario); free(scenario);
}

static int
map_schema_callback(const wirelog_extension_value_t *arguments, uint32_t count,
    wirelog_extension_value_t *result, void *context)
{
    (void)arguments; CHECK(count == 0); ++*(unsigned *)context;
    result->type = WIRELOG_EXTENSION_VALUE_INT64;
    result->size = sizeof(int64_t);
    result->as.int64_value = 29;
    return 0;
}

static void
map_unresolved_schema(void)
{
    unsigned calls = 0;
    wirelog_extension_registry_t *registry =
        wirelog_extension_registry_create();
    CHECK(registry);
    wirelog_extension_descriptor_t descriptor = {
        .abi_version = WIRELOG_EXTENSION_ABI_VERSION,
        .size = sizeof(descriptor),
        .name = "schema", .arity = 0,
        .result_type = WIRELOG_EXTENSION_VALUE_INT64,
        .invoke = map_schema_callback, .user_data = &calls,
    };
    CHECK(!wirelog_extension_register(registry, &descriptor));
    wirelog_extension_snapshot_t *snapshot =
        wirelog_extension_snapshot_acquire(registry);
    CHECK(snapshot);
    wl_columnar_filter_test_program_t numeric = {0};
    wl_columnar_filter_test_constant(&numeric, WL_PLAN_EXPR_CONST_FLOAT,
        wl_columnar_float_to_bits(-2.5));
    uint8_t callback[] = {WL_PLAN_EXPR_EXTENSION_CALL, 6, 0, 's', 'c', 'h', 'e',
                          'm', 'a', 0, 0, 0, 0};
    wl_plan_expr_buffer_t expressions[] = {{numeric.bytes, numeric.size},
                                           {callback, sizeof(callback)}};
    wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = 2,
                       .map_exprs = expressions, .map_expr_count = 2};
    wl_col_session_t *session = wl_columnar_filter_test_session();
    session->base.extension_snapshot = snapshot;
    eval_stack_t stack = {0};
    CHECK(!eval_stack_push(&stack,
        wl_columnar_filter_test_source(1, 3, false, false, false), true));
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer);
    CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(
            session->memory_governor),
        16u * 1024u * 1024u - 1u, &peer));
    CHECK(col_op_map(&op, &stack,
        session) == ENOSPC && !calls && stack.top == 1);
    CHECK(wl_columnar_memory_rollback(&peer));
    session->memory_budget_denied = false;
    CHECK(!col_op_map(&op, &stack, session));
    CHECK(calls == 3 && stack.top == 1 && stack.items[0].rel->nrows == 3);
    CHECK(!stack.items[0].rel->column_types); /* Whole legacy fallback, not fabricated partial types. */
    for (uint32_t r = 0; r < 3; ++r) {
        CHECK(stack.items[0].rel->columns[0][r] ==
            wl_columnar_float_to_bits(-2.5));
        CHECK(stack.items[0].rel->columns[1][r] == 29);
    }
    CHECK(!eval_stack_drain(&stack) &&
        !wl_columnar_filter_test_reserved(session));
    session->base.extension_snapshot = NULL;
    wl_columnar_memory_governor_ref_release(session->memory_governor);
    free(session);
    wirelog_extension_snapshot_release(snapshot);
    CHECK(!wirelog_extension_unregister(registry, "schema"));
    CHECK(!wirelog_extension_registry_destroy(registry));
}

static void
map_codes_constructor(void)
{
    int owner;
    wl_evaluation_control_t *control = wl_columnar_filter_test_control(0,
            &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    int64_t codes[] = {WIRELOG_TYPE_INT32, WIRELOG_TYPE_FLOAT,
                       WIRELOG_TYPE_BOOL};
    col_rel_t *out = NULL;
    CHECK(!wl_columnar_relation_new_typed_codes_operator_attempt_controlled(
            "$map", 3, 129,
            codes, NULL, &out, control, id));
    CHECK(out && out->capacity == 129 && out->column_types);
    for (unsigned c = 0; c < 3; ++c) CHECK(out->column_types[c] == codes[c]);
    codes[1] = WIRELOG_TYPE_STRING; /* Output metadata must not borrow scratch. */
    CHECK(out->column_types[1] == WIRELOG_TYPE_FLOAT); col_rel_destroy(out);
    out = NULL;
    const int64_t invalid[] = {-1, INT64_MAX, (int64_t)WIRELOG_TYPE_BOOL + 1};
    for (unsigned i = 0; i < 3; ++i) {
        CHECK(wl_columnar_relation_new_typed_codes_operator_attempt_controlled(
                "$map", 1, 0,
                &invalid[i], NULL, &out, control, id) == EINVAL && !out);
    }
    CHECK(!wl_columnar_relation_new_typed_codes_operator_attempt_controlled(
            "$map", 0, 0,
            NULL, NULL, &out, control, id));
    CHECK(out && !out->ncols && out->capacity == 0); col_rel_destroy(out);
    out = NULL;
    CHECK(wl_columnar_relation_new_typed_codes_operator_attempt_controlled(
            "$map", 3, 1,
            codes, NULL, &out, control, id + 1) == EINVAL && !out);
    CHECK(!wl_evaluation_control_finish(control, &owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(control, &owner));
    wl_evaluation_control_release(control);
}

static void
wl_columnar_filter_test_map_faults(uint32_t width, uint32_t rows)
{
    wl_columnar_filter_test_program_t program = {0};
    wl_columnar_filter_test_constant(&program, WL_PLAN_EXPR_CONST_INT, 3);
    wl_plan_expr_buffer_t expressions[2] = {{0}};
    expressions[0].data = program.bytes; expressions[0].size = program.size;
    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_MAP; op.project_count = width + 1;
    op.map_exprs = expressions; op.map_expr_count = 2;
    unsigned count = 0, exercised = 0;
    wl_columnar_filter_test_event_t *events = NULL;
    for (unsigned event = 0; event <= count; ++event) {
        if (event && width > 1) {
            const wl_columnar_filter_test_event_t *item = &events[event - 1];
            bool selected = false;
            if (item->row == 0 || item->row == 64 || item->row == 128) {
                bool first = true, last = true;
                for (unsigned j = 0; j < count; ++j) {
                    if (events[j].family != item->family ||
                        events[j].phase != item->phase ||
                        events[j].row != item->row) continue;
                    if (j < event - 1) first = false;
                    if (j > event - 1) last = false;
                }
                selected = first || last;
            }
            if (!selected) continue;
        }
        if (event) ++exercised;
        unsigned modes = event ? 3 : 1;
        for (unsigned mode = 0; mode < modes; ++mode) {
            wl_columnar_filter_test_scenario_t *scenario = calloc(1,
                    sizeof(*scenario));
            CHECK(scenario);
            wl_columnar_filter_test_map_prepare(scenario, width, rows, false,
                false,
                &op, 0);
            eval_stack_t *stack =
                wl_columnar_eval_stack_cleanup_stack(scenario->filter.frame);
            eval_entry_t original = stack->items[0];
            wl_columnar_filter_test_trace_begin(scenario, event, mode);
            wl_columnar_filter_test_map_run(scenario);
            CHECK(scenario->filter.done &&
                !wl_columnar_filter_test_successor_callbacks);
            if (!event) {
                count = wl_columnar_filter_test_event_count;
                CHECK(count && !scenario->filter.status.execution_result);
                events = malloc(count * sizeof(*events));
                CHECK(events);
                memcpy(events, wl_columnar_filter_test_events,
                    count * sizeof(*events));
            } else {
                CHECK(wl_columnar_filter_test_event_count >= event);
                int expected = mode == 0 ? WL_EVALUATION_CONTROL_CANCELLED
                    : mode == 1 ? EOVERFLOW : EINVAL;
                CHECK(scenario->filter.status.execution_result == expected);
                wl_columnar_filter_test_map_original(scenario, &original);
            }
            wl_columnar_filter_test_trace_end();
            wl_columnar_filter_test_complete(scenario);
            free(scenario);
        }
    }
    printf(
        "MAP fault checkpoints width=%u rows=%u actual=%u exercised=%u modes=3\n",
        width, rows, count, exercised);
    free(events);
}

static void
wl_columnar_filter_test_map_allocations(void)
{
    wl_columnar_filter_test_program_t program = {0};
    wl_columnar_filter_test_constant(&program, WL_PLAN_EXPR_CONST_INT, 9);
    wl_plan_expr_buffer_t expressions[17] = {{0}};
    for (unsigned i = 0; i < 17; ++i) {
        expressions[i].data = program.bytes; expressions[i].size = program.size;
    }
    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_MAP; op.project_count = 17;
    op.map_exprs = expressions; op.map_expr_count = 17;
    unsigned counts[3] = {0};
    for (unsigned family = 0; family < 3; ++family) {
        for (unsigned ordinal = 0; ordinal <= counts[family]; ++ordinal) {
            wl_columnar_filter_test_scenario_t *scenario = calloc(1,
                    sizeof(*scenario));
            CHECK(scenario);
            wl_columnar_filter_test_map_prepare(scenario, 17, 2, false, true,
                &op, 0);
            if (family ==
                0) wl_columnar_relation_test_constructor_fail_allocation =
                    ordinal ? ordinal : UINT32_MAX;
            if (family ==
                1) wl_columnar_expr_test_fail_allocation =
                    ordinal ? (int)ordinal - 1 : 100000;
            if (family ==
                2) wl_columnar_filter_test_attempt_fail_allocation =
                    ordinal ? (int)ordinal - 1 : 100000;
            wl_columnar_filter_test_map_run(scenario);
            if (!ordinal) {
                counts[family] = family ==
                    0 ? UINT32_MAX -
                    wl_columnar_relation_test_constructor_fail_allocation
                    : family ==
                    1 ? (unsigned)(100000 -
                    wl_columnar_expr_test_fail_allocation)
                    : (unsigned)(100000 -
                    wl_columnar_filter_test_attempt_fail_allocation);
                CHECK(counts[family] &&
                    scenario->filter.status.execution_result == 0);
            } else {
                CHECK(scenario->filter.status.execution_result == ENOMEM);
                CHECK(!scenario->filter.status.committed &&
                    !scenario->session->memory_budget_denied);
            }
            wl_columnar_relation_test_constructor_fail_allocation = 0;
            wl_columnar_expr_test_fail_allocation = -1;
            wl_columnar_filter_test_attempt_fail_allocation = -1;
            wl_columnar_filter_test_complete(scenario);
            free(scenario);
        }
        printf("MAP allocation family=%u reached=%u injected=%u\n", family,
            counts[family], counts[family]);
    }
}

static void
wl_columnar_filter_test_map_literal_bytes(wl_columnar_filter_test_literal_t *s,
    uint64_t bytes)
{
    wl_columnar_filter_test_literal_repeat(s, 224, (bytes + 7u) / 8u);
}

static void
wl_columnar_filter_test_map_literal_name(wl_columnar_filter_test_literal_t *s,
    uint32_t column)
{
    wl_columnar_filter_test_literal_add(s, 96);
    for (unsigned i = 0; i < wl_columnar_filter_test_digits(column); i++)
        wl_columnar_filter_test_literal_add(s, 96);
    wl_columnar_filter_test_literal_add(s, 192);
}

static void
wl_columnar_filter_test_map_literal_auto(const char *name, uint32_t width,
    bool governed,
    wl_columnar_filter_test_literal_t *s)
{
#define AUTO_R(v) wl_columnar_filter_test_literal_add(s, (v))
#define AUTO_A(v) wl_columnar_filter_test_literal_add(s, (v))
#define AUTO_POINTERS() do { for (uint32_t pos = 0; pos < width; \
                                 pos += 8) { AUTO_R(192); } } while (0)
#define AUTO_RESERVE(nonzero) do { AUTO_R(160); \
                                   if (governed && (nonzero)) { AUTO_R(74); \
                                                                AUTO_R(54); \
                                                                AUTO_R(84); \
                                                                AUTO_R(29); \
                                   } } while (0)
    AUTO_R(128);
    for (uint32_t c = 0; c < width; ++c) AUTO_R(128);
    AUTO_R(128); AUTO_R(128); AUTO_R(192);
    uint64_t name_bytes = strlen(name) + 1;
    for (uint64_t i = 0; i < name_bytes; i++) AUTO_R(96);
    for (unsigned i = 0; i < 6; i++) AUTO_R(96);
    AUTO_R(96); /* typed decision */
    if (width) {
        AUTO_R(96); AUTO_R(96);
    }
    AUTO_R(96); AUTO_R(96); AUTO_R(96);
    if (governed) AUTO_R(96); /* scratch payload */
    AUTO_R(128);
    for (uint32_t i = 0; i < width; i++) {
        wl_columnar_filter_test_map_literal_name(s, i);
        AUTO_R(96); AUTO_R(96); AUTO_R(96);
    }
    AUTO_R(96); AUTO_RESERVE(true); AUTO_A(112);
    wl_columnar_filter_test_map_literal_bytes(s, sizeof(col_rel_t));
    for (unsigned i = 0; i < 5; i++) AUTO_R(96);
    AUTO_A(112); wl_columnar_filter_test_map_literal_bytes(s, name_bytes);
    AUTO_R(80); AUTO_R(112); AUTO_R(96);
    AUTO_RESERVE(true); AUTO_RESERVE(width != 0);
    if (width) {
        AUTO_A(112); AUTO_POINTERS();
        for (uint32_t i = 0; i < width; i++) AUTO_A(112);
        if (governed) AUTO_A(112);
        AUTO_A(112); AUTO_POINTERS();
        for (uint32_t i = 0; i < width; i++) {
            wl_columnar_filter_test_map_literal_name(s, i); AUTO_A(112);
            wl_columnar_filter_test_map_literal_bytes(s,
                4u + wl_columnar_filter_test_digits(i));
        }
    }
    if (width) {
        AUTO_A(112);
        for (uint32_t c = 0; c < width; ++c) AUTO_R(96);
    }
    AUTO_R(96); AUTO_A(112); wl_columnar_filter_test_map_literal_bytes(s, 3);
    if (width) {
        AUTO_A(112); AUTO_POINTERS();
        for (uint32_t i = 0; i < width; i++) {
            AUTO_A(128); AUTO_A(112);
            AUTO_R(96); /* Typed Arrow format selection before byte copy. */
            wl_columnar_filter_test_map_literal_bytes(s, 2);
            wl_columnar_filter_test_map_literal_name(s, i); AUTO_A(112);
            wl_columnar_filter_test_map_literal_bytes(s,
                4u + wl_columnar_filter_test_digits(i));
        }
    }
    for (unsigned i = 0; i < 3; i++) {
        AUTO_R(128);
        if (governed && (i < 2 || width)) {
            AUTO_R(74); AUTO_R(54); AUTO_R(29);
        }
    }
    AUTO_R(220); AUTO_R(128);
#undef AUTO_RESERVE
#undef AUTO_POINTERS
#undef AUTO_A
#undef AUTO_R
}

static void
wl_columnar_filter_test_map_literal_row(
    wl_columnar_filter_test_literal_t *literal,
    uint32_t width, uint32_t selected_before, uint32_t capacity,
    bool timestamps,
    bool renewal)
{
#define L(v) wl_columnar_filter_test_literal_add(literal, (v))
#define R(v, n) wl_columnar_filter_test_literal_repeat(literal, (v), (n))
    L(192);
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

#undef R
#undef L
}

static void
wl_columnar_filter_test_map_literal_compile(
    wl_columnar_filter_test_literal_t *literal,
    const wl_columnar_filter_test_program_t *program)
{
#define L(v) wl_columnar_filter_test_literal_add(literal, (v))
#define R(v, n) wl_columnar_filter_test_literal_repeat(literal, (v), (n))
    L(128);
    for (unsigned pass = 0; pass < 2; ++pass){
        if (pass){
            L(224); L(224);
            wl_columnar_filter_test_literal_reserve(literal); R(224, 2);
        }
        uint32_t pos = 0;
        while (pos < program->size){
            L(192); L(224);
            uint8_t tag = program->bytes[pos];
            uint32_t length = 1, digits = 0;
            if (tag == WL_PLAN_EXPR_CONST_INT ||
                tag == WL_PLAN_EXPR_CONST_FLOAT) length = 9;
            if (tag == WL_PLAN_EXPR_VAR || tag == WL_PLAN_EXPR_VAR_FLOAT){
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
    L(224); L(224); wl_columnar_filter_test_literal_commit(literal);
    L(160);
#undef R
#undef L
}

/* Independent ordered fee schedule for known typed MAP results. Geometry
 * and expression bytecode are fixture inputs; no measured total or production
 * fee enum is used to construct the oracle. */
static wl_columnar_filter_test_literal_t
wl_columnar_filter_test_map_literal(uint32_t width, uint32_t rows,
    uint32_t outputs, const wl_columnar_filter_test_program_t *program)
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
    L(96); L(144); L(224); L(96); L(144);
    L(224); L(192); R(224, (width + 7u) / 8u);
    L(192); L(128); L(192); L(192);
    R(192, 2); L(192);
    if ((uint64_t)width + outputs > 16) {
        L(192); wl_columnar_filter_test_literal_reserve(&literal);
        L(192); L(192); wl_columnar_filter_test_literal_commit(&literal);
    }
    L(192); R(256, 6); L(192);
    if (outputs) {
        L(192); L(192); wl_columnar_filter_test_literal_reserve(&literal);
        L(192); L(192); wl_columnar_filter_test_literal_commit(&literal);
    }
    for (uint32_t c = 0; c < outputs; ++c) {
        L(256); L(192);
        if (program && c == 0) {
            L(192);
            wl_columnar_filter_test_map_literal_compile(&literal, program);
        }
    }
    R(192, 36);
    for (uint32_t c = 0; c < outputs; ++c) {
        L(192);
        if (program && c == 0) L(128);
        L(192);
    }
    L(192); L(192);
    wl_columnar_filter_test_map_literal_auto("$map", outputs, true, &literal);
    L(192); L(192); L(170);
    wl_columnar_filter_test_literal_single(&literal);
    for (uint32_t row = 0; row < rows; ++row) {
        L(128); L(192); L(128); R(224, (width + 7u) / 8u);
        for (uint32_t c = 0; c < outputs; ++c) {
            L(192);
            if (program && c == 0) {
                L(192); L(96);
                for (uint32_t i = 0; i < program->instructions; ++i) {
                    L(64); L(224);
                    if (program->tags[i] == WL_PLAN_EXPR_VAR_FLOAT) L(160);
                }
                L(96);
            }
            L(192);
        }
        L(192);
        wl_columnar_filter_test_map_literal_row(&literal, outputs, row, rows,
            false, false);
    }
    L(192); L(128); L(128); L(128);
#undef R
#undef L
    return literal;
}

static void
wl_columnar_filter_test_map_budget(uint32_t width, uint32_t rows,
    uint32_t outputs, bool expression, bool every_prefix)
{
    wl_columnar_filter_test_program_t program = {0};
    wl_columnar_filter_test_constant(&program, WL_PLAN_EXPR_CONST_INT, 11);
    wl_plan_expr_buffer_t expr = {0};
    expr.data = program.bytes; expr.size = program.size;
    wl_plan_op_t op = {0};
    op.op = WL_PLAN_OP_MAP; op.project_count = outputs;
    if (expression) {
        op.map_exprs = &expr; op.map_expr_count = 1;
    }
    wl_columnar_filter_test_literal_t literal =
        wl_columnar_filter_test_map_literal(
        width, rows, outputs, expression ? &program : NULL);
    for (size_t prefix = every_prefix ? 0 : literal.count - 1;
        prefix < literal.count; ++prefix){
        for (int delta = -1; delta <= 1; ++delta) {
            uint64_t limit = literal.prefix[prefix] + delta;
            wl_columnar_filter_test_scenario_t *scenario = calloc(1,
                    sizeof(*scenario));
            CHECK(scenario);
            wl_columnar_filter_test_map_prepare(scenario, width, rows, false,
                false, &op, limit);
            eval_stack_t *stack =
                wl_columnar_eval_stack_cleanup_stack(scenario->filter.frame);
            eval_entry_t original = stack->items[0];
            wl_columnar_filter_test_map_run(scenario);
            int expected = limit <
                literal.total ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0;
            CHECK(scenario->filter.done &&
                scenario->filter.status.execution_result == expected);
            if (expected) wl_columnar_filter_test_map_original(scenario,
                    &original);
            wl_evaluation_control_report_t report =
                wl_columnar_filter_test_complete(scenario);
            uint64_t consumed = 0;
            for (size_t i = 0; i < literal.count && literal.prefix[i] <= limit;
                ++i)
                consumed = literal.prefix[i];
            if (report.consumed != consumed)
                fprintf(stderr,
                    "MAP literal width=%u rows=%u outputs=%u prefix=%zu limit=%llu expected=%llu got=%llu total=%llu\n",
                    width, rows, outputs, prefix, (unsigned long long)limit,
                    (unsigned long long)consumed,
                    (unsigned long long)report.consumed,
                    (unsigned long long)literal.total);
            CHECK(report.consumed == consumed && report.result == expected);
            free(scenario);
        }
    }
    free(literal.prefix);
}

static void
wl_columnar_filter_test_map_failed_table_hook(wl_evaluation_control_t *control,
    unsigned phase, uint64_t progress)
{
    if (phase == 103) wl_columnar_filter_test_event(control, 1, phase,
            progress);
}

static void
wl_columnar_filter_test_map_error_priority(void)
{
    for (unsigned mode = 0; mode < 3; ++mode) {
        wl_plan_op_t op = {0};
        op.op = WL_PLAN_OP_MAP; op.project_count = 2;
        wl_columnar_filter_test_scenario_t *scenario = calloc(1,
                sizeof(*scenario));
        CHECK(scenario);
        wl_columnar_filter_test_map_prepare(scenario, 1, 1, false, false, &op,
            0);
        eval_stack_t *stack =
            wl_columnar_eval_stack_cleanup_stack(scenario->filter.frame);
        eval_entry_t original = stack->items[0];
        wl_columnar_filter_test_trace_begin(scenario, 1, mode);
        wl_columnar_relation_test_work_checkpoint = NULL;
        wl_columnar_expr_test_checkpoint = NULL;
        wl_columnar_eval_stack_test_checkpoint = NULL;
        wl_columnar_memory_governor_test_work_checkpoint = NULL;
        wl_columnar_memory_governor_test_checkpoint = NULL;
        wl_columnar_filter_test_attempt_checkpoint =
            wl_columnar_filter_test_map_failed_table_hook;
        wl_columnar_filter_test_attempt_fail_allocation = 0;
        wl_columnar_filter_test_map_run(scenario);
        wl_columnar_filter_test_attempt_fail_allocation = -1;
        CHECK(wl_columnar_filter_test_event_count == 1);
        CHECK(!wl_columnar_filter_test_successor_callbacks);
        int expected = mode == 0 ? ENOMEM : mode == 1 ? EOVERFLOW : EINVAL;
        CHECK(scenario->filter.done &&
            scenario->filter.status.execution_result == expected);
        CHECK(!scenario->session->memory_budget_denied);
        wl_columnar_filter_test_map_original(scenario, &original);
        wl_columnar_filter_test_trace_end();
        wl_columnar_filter_test_complete(scenario);
        free(scenario);
    }
}

/* Compilation/table errors now precede output construction. Simultaneous
* injected failures prove the later constructor is not entered at all. */
static void
map_schema_error_precedence(void)
{
    wl_columnar_filter_test_program_t program = {0};
    wl_columnar_filter_test_constant(&program, WL_PLAN_EXPR_CONST_INT, 1);
    wl_plan_expr_buffer_t expression = {program.bytes, program.size};
    wl_plan_op_t op = {.op = WL_PLAN_OP_MAP, .project_count = 2,
                       .map_exprs = &expression, .map_expr_count = 1};
    for (unsigned mode = 0; mode < 2; ++mode) {
        wl_columnar_filter_test_scenario_t *scenario = calloc(1,
                sizeof(*scenario));
        CHECK(scenario);
        wl_columnar_filter_test_map_prepare(scenario, 1, 1, false, false, &op,
            0);
        eval_entry_t original =
            wl_columnar_eval_stack_cleanup_stack(
            scenario->filter.frame)->items[0];
        wl_columnar_relation_test_constructor_fail_allocation = 1;
        wl_columnar_expr_test_fail_allocation = 0;
        if (mode) wl_columnar_filter_test_attempt_fail_allocation = 0;
        wl_columnar_filter_test_map_run(scenario);
        CHECK(scenario->filter.done &&
            scenario->filter.status.execution_result == ENOMEM);
        CHECK(wl_columnar_relation_test_constructor_fail_allocation == 1);
        CHECK(wl_columnar_expr_test_fail_allocation == (mode ? 0 : -1));
        wl_columnar_filter_test_map_original(scenario, &original);
        wl_columnar_relation_test_constructor_fail_allocation = 0;
        wl_columnar_expr_test_fail_allocation = -1;
        wl_columnar_filter_test_attempt_fail_allocation = -1;
        wl_columnar_filter_test_complete(scenario); free(scenario);
    }
}

/* The frame keeps its original governor; detach only the session's output
 * policy during MAP to exercise its ungoverned capacity/growth branch. */
static void
wl_columnar_filter_test_map_ungoverned(void)
{
    unsigned reached = 0;
    for (unsigned ordinal = 0; ordinal <= reached; ++ordinal) {
        wl_plan_op_t op = {0};
        op.op = WL_PLAN_OP_MAP; op.project_count = 17;
        wl_columnar_filter_test_scenario_t *scenario = calloc(1,
                sizeof(*scenario));
        CHECK(scenario);
        wl_columnar_filter_test_map_prepare(scenario, 17, 133, false, false,
            &op, 0);
        eval_stack_t *stack =
            wl_columnar_eval_stack_cleanup_stack(scenario->filter.frame);
        eval_entry_t original = stack->items[0];
        wl_columnar_memory_governor_ref_t *governor =
            scenario->session->memory_governor;
        scenario->session->memory_governor = NULL;
        wl_columnar_relation_test_growth_fail_allocation =
            ordinal ? ordinal : UINT32_MAX;
        wl_columnar_filter_test_map_run(scenario);
        if (!ordinal) reached = UINT32_MAX -
                wl_columnar_relation_test_growth_fail_allocation;
        wl_columnar_relation_test_growth_fail_allocation = 0;
        scenario->session->memory_governor = governor;
        CHECK(scenario->filter.done);
        if (ordinal) {
            CHECK(scenario->filter.status.execution_result == ENOMEM);
            wl_columnar_filter_test_map_original(scenario, &original);
        } else {
            CHECK(!scenario->filter.status.execution_result);
            CHECK(stack->items[0].rel->nrows == 133 &&
                stack->items[0].rel->ncols == 17);
            col_rel_t *expected = wl_columnar_filter_test_source(17, 133, false,
                    false, false);
            for (uint32_t c = 0; c < 17; ++c)
                for (uint32_t r = 0; r < 133; ++r)
                    CHECK(stack->items[0].rel->columns[c][r] ==
                        expected->columns[c][r]);
            CHECK(col_rel_destroy_checked(expected) == 0);
        }
        wl_columnar_filter_test_complete(scenario);
        free(scenario);
    }
    CHECK(reached > 0);
    printf("MAP ungoverned growth allocations reached=%u injected=%u\n",
        reached, reached);
}

/* J1 component driver: setup is caller-owned work; each cleanup dispatch gets
 * a fresh grant from distinct funding and returns before the next queue turn. */
typedef struct {
    host_job_t job;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    wl_columnar_filter_status_t status;
    wl_evaluation_control_t *funding;
    bool done, ack;
    unsigned turns;
} join_workspace_job_t;

static void
join_workspace_dispatch(host_queue_t *queue, host_job_t *job)
{
    join_workspace_job_t *entry = job->data;
    CHECK(wl_evaluation_control_charge(entry->funding, 384) == 0);
    if (entry->ack) {
        CHECK(wl_columnar_filter_op_attempt_ack(entry->frame) == 0);
        entry->done = true;
    } else {
        CHECK(wl_columnar_filter_op_attempt_step(entry->frame,
            &entry->status) == 0);
        entry->ack = entry->status.progress == WL_COLUMNAR_FILTER_PROGRESS_DONE;
        CHECK(entry->ack ||
            entry->status.progress == WL_COLUMNAR_FILTER_PROGRESS_YIELD
            || entry->status.progress == WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
    }
    ++entry->turns;
    if (!entry->done &&
        entry->status.progress != WL_COLUMNAR_FILTER_PROGRESS_RETAINED)
        CHECK(host_enqueue(queue, job) == 0);
}

static void *
join_workspace_wrong_thread(void *data)
{
    wl_columnar_filter_context_t *ctx = data;
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_context_owned(ctx->frame,
        ctx, ctx->control, ctx->attempt_id, NULL) == EINVAL);
    return NULL;
}

static bool join_batch_release_refused;
static int
join_batch_release_once(wl_columnar_relation_mutation_single_t *single,
    unsigned slot)
{
    CHECK(slot == 1 && single->set.owners_acquired == 1
        && single->set.descriptors_acquired == 1);
    CHECK(!join_batch_release_refused);
    join_batch_release_refused = true;
    return EBUSY;
}

static void
join_workspace_fixture(unsigned mode)
{
    wl_col_session_t *session = wl_columnar_filter_test_session();
    wl_columnar_eval_stack_cleanup_frame_t *frame = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(session, &frame) == 0);
    uint64_t frame_credit = wl_columnar_filter_test_reserved(session);
    col_rel_t *input = wl_columnar_filter_test_source(17, 1, true, false,
            false);
    col_rel_t *right = wl_columnar_filter_test_source(1, 1, false, false,
            false);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(frame);
    CHECK(eval_stack_push(stack, input, true) == 0);
    int operation_owner = 0;
    wl_evaluation_control_t *operation = wl_columnar_filter_test_control(0,
            &operation_owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(operation);
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(wl_columnar_eval_stack_filter_context(frame, &ctx) == 0);
    ctx->identity = ctx; ctx->frame = frame; ctx->session = session;
    ctx->control = operation; ctx->attempt_id = id;
    CHECK(wl_evaluation_control_retain(operation) == 0);
    ctx->pin_owned = true; ctx->thread_valid = true; ctx->busy = true;
#if defined(WL_HAVE_C11_THREADS)
    ctx->owner_thread = thrd_current();
#elif defined(_WIN32) || defined(_WIN64)
    ctx->owner_thread = GetCurrentThreadId();
#else
    ctx->owner_thread = pthread_self();
#endif
    ctx->values = ctx->inline_values;
    ctx->stage = WL_COLUMNAR_FILTER_STAGE_RUNNING;
    /* The ordinary route refuses the real pre-unary activation gap. */
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_controlled(frame,
        operation, id) == EBUSY);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_owned(frame, operation, id,
        NULL) == EBUSY);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_context_owned(frame,
        (wl_columnar_filter_context_t *)(uintptr_t)1, operation, id,
        NULL) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_context_owned(frame, ctx,
        operation, id + 1, NULL) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_context_owned(frame, ctx,
        NULL, id, NULL) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_context_owned(frame, ctx,
        operation, id, &operation_owner) == EBUSY);
    wl_thread_t wrong_thread;
    CHECK(wl_thread_create(&wrong_thread, join_workspace_wrong_thread,
        ctx) == 0);
    CHECK(wl_thread_join(&wrong_thread) == 0);
    wl_columnar_eval_stack_cleanup_frame_t *inner = NULL;
    CHECK(wl_columnar_eval_stack_cleanup_begin(session, &inner) == 0);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_context_owned(frame, ctx,
        operation, id, NULL) == EINVAL);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&inner) == 0 && !inner);
    CHECK(!ctx->unary_started && stack->top == 1 &&
        stack->items[0].rel == input);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_context_owned(frame, ctx,
        operation, id, NULL) == 0);
    ctx->unary_started = true;
    col_rel_t **slot = NULL;
    CHECK(wl_columnar_eval_stack_unary_output(frame, &slot) == 0);
    CHECK(wl_columnar_relation_new_typed_operator_attempt_controlled("$join",
        17,
        input->column_types, session->memory_governor, slot, operation,
        id) == 0);
    ctx->output = *slot; ctx->private_output = *slot;
    CHECK(wl_columnar_relation_scalar_source_init_attempt_controlled(
            &ctx->source,
            &ctx->source_initialized, operation, id) == 0);
    CHECK(wl_columnar_relation_scalar_source_begin_attempt_controlled(
            &ctx->source, input, false) == 0);
    CHECK(wl_columnar_relation_scalar_source_init_attempt_controlled(
            &ctx->join_right,
            &ctx->join_right_initialized, operation, id) == 0);
    CHECK(wl_columnar_relation_scalar_source_begin_attempt_controlled(
            &ctx->join_right, right, false) == 0);
    wl_columnar_relation_private_batch_drain_init(&ctx->batch_drain);
    ctx->drain_initialized = true; ctx->batch_initialized = true;
    wl_columnar_source_access_reader_t reader = {0};
    if (mode == 2) CHECK(col_rel_source_reader_acquire(ctx->output,
            &reader) == 0);
    int begin_rc =
        wl_columnar_relation_append_batch_begin_controlled(&ctx->batch,
            ctx->output, operation);
    CHECK(begin_rc == (mode == 2 ? EBUSY : 0));
    if (mode == 2) CHECK(col_rel_source_reader_release(&reader) == 0);
    ctx->join_initialized = true;
    for (unsigned i = 0; i < WL_COLUMNAR_JOIN_SCRATCH_COUNT; ++i) {
        wl_columnar_join_scratch_t *scratch = &ctx->join_scratch[i];
        if (mode == 0 && i == 2) continue; /* Genuine never-initialized slot. */
        wl_columnar_memory_reservation_init(&scratch->credit);
        scratch->initialized = true;
        scratch->governor = session->memory_governor;
        wl_columnar_memory_governor_ref_retain(scratch->governor);
        scratch->ref_held = true;
        if (i == 0 && mode == 0) continue; /* Original reference only. */
        CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(
                scratch->governor), 17, &scratch->credit));
        scratch->admitted = true;
        if (i != 1) {
            scratch->physical = malloc(17); CHECK(scratch->physical);
            scratch->allocated = true;
            CHECK(wl_columnar_memory_commit(&scratch->credit,
                scratch->physical));
            scratch->committed = true;
        }
    }
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer);
    CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(
            session->memory_governor), 19, &peer));
    uint64_t before = wl_columnar_filter_test_reserved(session);
    wl_evaluation_control_request_cancel(operation);
    CHECK(wl_evaluation_control_charge(operation,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_evaluation_control_finish(operation, &operation_owner, 0,
        0) == WL_EVALUATION_CONTROL_CANCELLED);
    CHECK(wl_evaluation_control_begin(operation, &operation_owner) == 0);
    CHECK(wl_evaluation_control_active_attempt_id(operation) != id);
    ctx->execution_result = mode ==
        2 ? begin_rc : WL_EVALUATION_CONTROL_CANCELLED;
    ctx->semantic_result = ctx->execution_result; ctx->busy = false;
    ctx->stage = WL_COLUMNAR_FILTER_STAGE_JOIN_BATCH;
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == EBUSY && frame);
    int funding_owner = 0;
    wl_evaluation_control_t *funding = wl_columnar_filter_test_control(0,
            &funding_owner);
    join_workspace_job_t entry = {.frame = frame, .funding = funding};
    host_queue_t queue = {0};
    entry.job.dispatch = join_workspace_dispatch; entry.job.data = &entry;
    CHECK(host_enqueue(&queue, &entry.job) == 0);
    bool refused = false, witnessed_batch = false, witnessed_right = false;
    join_batch_release_refused = false;
    while (queue.count) {
        CHECK(entry.turns < 64);
        if (ctx->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_BATCH) {
            CHECK(ctx->join_right_initialized && ctx->join_initialized);
            CHECK(ctx->join_scratch[0].ref_held);
            witnessed_batch = true;
        }
        if (ctx->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_RIGHT_SOURCE) {
            CHECK(!ctx->batch_initialized && ctx->join_scratch[0].ref_held);
            witnessed_right = true;
        }
        if (mode == 3 && !join_batch_release_refused
            && ctx->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_BATCH
            && ctx->batch_drain.phase ==
            WL_COLUMNAR_RELATION_PRIVATE_BATCH_OWNER) {
            wl_columnar_relation_test_single_release = join_batch_release_once;
        }
        if (mode == 1 && !refused &&
            ctx->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_CREDIT) {
            CHECK(!ctx->join_scratch[0].allocated &&
                ctx->join_scratch[0].admitted);
            wl_columnar_memory_governor_test_refuse_next_release();
            refused = true;
        }
        CHECK(host_turn(&queue) == 0); /* A real return between retire phases. */
        if (entry.status.progress == WL_COLUMNAR_FILTER_PROGRESS_RETAINED) {
            CHECK(!queue.count);
            CHECK(ctx->join_scratch[0].admitted &&
                ctx->join_scratch[0].ref_held);
            if (mode == 3) {
                CHECK(join_batch_release_refused
                    && ctx->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_BATCH);
                CHECK(ctx->join_scratch[0].allocated &&
                    ctx->join_scratch[0].physical);
                CHECK(ctx->join_right_initialized && ctx->source_initialized);
                CHECK(ctx->batch.raw.single.set.owners_acquired == 1
                    && ctx->batch.raw.single.set.descriptors_acquired == 1);
                wl_columnar_relation_test_single_release = NULL;
            } else {
                CHECK(mode == 1 && refused && !ctx->join_scratch[0].physical);
            }
            CHECK(host_enqueue(&queue, &entry.job) == 0); /* Fresh funding on retry. */
        }
    }
    CHECK(wl_evaluation_control_finish(funding, &funding_owner, 0, 0) == 0);
    wl_evaluation_control_report_t funding_report;
    CHECK(wl_evaluation_control_report(funding, &funding_report) == 0);
    CHECK(funding_report.consumed == (uint64_t)entry.turns * 384);
    CHECK(wl_evaluation_control_detach(funding, &funding_owner) == 0);
    wl_evaluation_control_release(funding);
    CHECK(join_batch_release_refused == (mode == 3));
    CHECK(entry.done && witnessed_batch && witnessed_right &&
        (refused == (mode == 1)));
    CHECK(entry.status.semantic_result ==
        (mode == 2 ? EBUSY : WL_EVALUATION_CONTROL_CANCELLED));
    CHECK(stack->top == 1 && stack->items[0].rel == input &&
        stack->items[0].owned);
    CHECK(!ctx->join_initialized && !ctx->join_right_initialized);
    CHECK(wl_columnar_filter_test_reserved(session) == frame_credit + 19);
    CHECK(wl_columnar_filter_test_reserved(session) < before);
    CHECK(wl_evaluation_control_finish(operation, &operation_owner, 0, 0) == 0);
    wl_evaluation_control_report_t successor;
    CHECK(wl_evaluation_control_report(operation,
        &successor) == 0 && successor.consumed == 0);
    CHECK(wl_evaluation_control_detach(operation, &operation_owner) == 0);
    wl_evaluation_control_release(operation);
    CHECK(wl_columnar_memory_rollback(&peer));
    CHECK(col_rel_destroy_checked(right) == 0);
    CHECK(wl_columnar_eval_stack_cleanup_finish(&frame) == 0 && !frame);
    CHECK(!wl_columnar_filter_test_reserved(session));
    CHECK(wl_columnar_memory_governor_ref_is_sole(session->memory_governor));
    wl_columnar_memory_governor_ref_release(session->memory_governor);
    free(session);
}

static wl_columnar_eval_stack_cleanup_frame_t *join_activation_frame;
static bool join_activation_seen;

static void
join_activation_hook(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)progress;
    if (phase != 3 || join_activation_seen) return;
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(wl_columnar_eval_stack_filter_context(join_activation_frame,
        &ctx) == 0);
    CHECK(ctx->busy && !ctx->unary_started);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_controlled(
            join_activation_frame,
            control, ctx->attempt_id) == EBUSY);
    CHECK(ctx->busy && !ctx->unary_started);
    join_activation_seen = true;
}

static void
join_activation_fixtures(void)
{
    for (unsigned map = 0; map < 2; ++map) {
        wl_columnar_filter_test_scenario_t *scenario = calloc(1,
                sizeof(*scenario));
        CHECK(scenario);
        wl_columnar_filter_test_prepare(scenario,
            wl_columnar_filter_test_source(1, 1, false, false, false), true,
            NULL, 0, false);
        if (map) {
            scenario->op.op = WL_PLAN_OP_MAP;
            scenario->op.project_count = 1;
            scenario->filter.start = wl_columnar_filter_map_attempt_start;
        }
        join_activation_frame = scenario->filter.frame;
        join_activation_seen = false;
        wl_columnar_filter_test_attempt_checkpoint = join_activation_hook;
        wl_columnar_filter_test_run(scenario);
        wl_columnar_filter_test_attempt_checkpoint = NULL;
        CHECK(join_activation_seen && scenario->filter.done &&
            !scenario->filter.status.semantic_result);
        (void)wl_columnar_filter_test_complete(scenario);
        join_activation_frame = NULL;
        free(scenario);
    }
}

/* A real running operator context is the only route to its unary admission. */
static wl_columnar_filter_test_scenario_t *admission_scenario;
static bool admission_seen;

static void
admission_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)progress;
    if (phase != 3) return;
    CHECK(!admission_seen);
    admission_seen = true;
    wl_columnar_eval_stack_cleanup_frame_t *frame =
        admission_scenario->filter.frame;
    wl_columnar_filter_context_t *context = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context(frame, &context));
    uint64_t id = wl_evaluation_control_active_attempt_id(control);
    CHECK(context->busy && !context->unary_started);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_controlled(frame,
        control, id) == EBUSY);
    wl_columnar_filter_context_t copy = *context;
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_context_owned(frame,
        &copy, control, id, NULL) == EINVAL);
    CHECK(wl_columnar_eval_stack_unary_begin_attempt_context_owned(frame,
        context, control, id + 1, NULL) == EINVAL);
    CHECK(context->busy && !context->unary_started);
}

static void
admission_cases(void)
{
    for (unsigned map = 0; map < 2; ++map) {
        wl_columnar_filter_test_scenario_t *scenario = calloc(1,
                sizeof(*scenario));
        CHECK(scenario);
        wl_plan_op_t op = {0}; op.project_count = 1;
        op.op = map ? WL_PLAN_OP_MAP : WL_PLAN_OP_FILTER;
        wl_columnar_filter_test_map_prepare(scenario, 1, 2, false, false,
            &op, 0);
        if (!map) scenario->filter.start = wl_columnar_filter_op_attempt_start;
        admission_scenario = scenario; admission_seen = false;
        wl_columnar_filter_test_attempt_checkpoint = admission_checkpoint;
        wl_columnar_filter_test_map_run(scenario);
        wl_columnar_filter_test_attempt_checkpoint = NULL;
        CHECK(admission_seen);
        wl_evaluation_control_report_t report =
            wl_columnar_filter_test_complete(scenario);
        CHECK(!report.result); free(scenario);
    }
    admission_scenario = NULL;
}

/* COUNT returns cardinalities even when its ignored operand is FLOAT. The
 * operand payloads are deliberately unrelated to the expected counts. */
static col_rel_t *
raw_count_input(bool grouped, bool empty, unsigned poison)
{
    col_rel_t *input = wl_columnar_filter_test_source(grouped ? 2 : 1, 0,
            false, false, false);
    input->has_graph_column = false; input->graph_col_idx = 0;
    wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64, WIRELOG_TYPE_FLOAT};
    CHECK(!col_rel_set_column_types(input, grouped ? types : types + 1,
        grouped ? 2 : 1));
    const int64_t keys[] = {2, 1, 2, 1, 2};
    const double values[] = {-7.5, 2.0, 100.0, -4.0, 9.0};
    for (unsigned r = 0; !empty && r < 5; ++r) {
        int64_t row[] = {keys[r], wl_columnar_float_to_bits(values[r])};
        CHECK(!col_rel_append_row(input, grouped ? row : row + 1));
    }
    if (!empty && poison)
        input->columns[grouped ? 1 : 0][0] = wl_columnar_float_to_bits(
            poison == 1 ? NAN : INFINITY);
    return input;
}

static void
raw_count_result_domains(void)
{
    wl_columnar_filter_test_program_t ignored = {0};
    wl_columnar_filter_test_constant(&ignored, WL_PLAN_EXPR_CONST_FLOAT,
        wl_columnar_float_to_bits(1.0));
    wl_columnar_filter_test_constant(&ignored, WL_PLAN_EXPR_CONST_FLOAT,
        wl_columnar_float_to_bits(0.0));
    wl_columnar_filter_test_opcode(&ignored, WL_PLAN_EXPR_ARITH_FLOAT_DIV);
    const wl_plan_agg_operand_t domains[] = {WL_PLAN_AGG_OPERAND_UNKNOWN,
                                             WL_PLAN_AGG_OPERAND_SCALAR,
                                             WL_PLAN_AGG_OPERAND_STRING,
                                             WL_PLAN_AGG_OPERAND_FLOAT};
    for (unsigned grouped = 0; grouped < 2; ++grouped)
        for (unsigned empty = 0; empty < 2; ++empty)
            for (unsigned poison = 0; poison < 3; ++poison)
                for (unsigned computed = 0; computed < 2; ++computed)
                    for (unsigned d = 0; d < 4; ++d)
                        for (unsigned placement = 0; placement < 3;
                            ++placement){
                            col_rel_t *input = raw_count_input(grouped != 0,
                                    empty != 0, poison);
                            wl_col_session_t *session =
                                wl_columnar_filter_test_session();
                            wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE,
                                               .agg_fn = WIRELOG_AGG_COUNT,
                                               .agg_operand_type = domains[d],
                                               .group_by_count = grouped,
                                               .aggregate_index = placement ==
                                                   2 ? 99 : placement};
                            if (computed) op.agg_expr =
                                    (wl_plan_expr_buffer_t){ignored.bytes,
                                                            ignored.size};
                            eval_stack_t stack = {0};
                            CHECK(!eval_stack_push(&stack, input, false));
                            CHECK(!col_op_reduce(&op, &stack, session));
                            CHECK(stack.top == 1 && stack.items[0].owned);
                            col_rel_t *out = stack.items[0].rel;
                            uint32_t agg = op.aggregate_index <
                                grouped + 1 ? op.aggregate_index : grouped;
                            CHECK(out->ncols == grouped + 1 &&
                                out->nrows == (empty ? 0u : grouped ? 2u : 1u));
                            CHECK(out->column_types &&
                                out->column_types[agg] == WIRELOG_TYPE_INT64 &&
                                !out->timestamps && !out->has_graph_column);
                            for (uint32_t c = 0; c < out->ncols; ++c) {
                                char name[16];
                                snprintf(name, sizeof(name), "col%u", c);
                                CHECK(!strcmp(out->col_names[c], name));
                            }
                            if (!empty) {
                                CHECK(out->columns[agg][0] ==
                                    (grouped ? 3 : 5));
                                if (grouped) {
                                    CHECK(out->columns[agg][1] == 2);
                                    CHECK(out->columns[1 - agg][0] == 2 &&
                                        out->columns[1 - agg][1] == 1);
                                }
                            }
                            CHECK(!eval_stack_drain(&stack));
                            col_rel_destroy(input);
                            CHECK(!wl_columnar_filter_test_reserved(session));
                            wl_columnar_memory_governor_ref_release(
                                session->memory_governor); free(session);
                        }
    /* Ignoring the value never permits an invalid selected FLOAT group key. */
    col_rel_t *float_key = wl_columnar_filter_test_source(2, 0, false, false,
            false);
    wirelog_column_type_t types[] = {WIRELOG_TYPE_FLOAT, WIRELOG_TYPE_FLOAT};
    CHECK(!col_rel_set_column_types(float_key, types, 2));
    int64_t row[] = {wl_columnar_float_to_bits(1.0),
                     wl_columnar_float_to_bits(-7.5)};
    CHECK(!col_rel_append_row(float_key, row));
    float_key->columns[0][0] = wl_columnar_float_to_bits(NAN);
    wl_col_session_t session = {0}; wl_mem_ledger_init(&session.mem_ledger, 0);
    eval_stack_t stack = {0}; CHECK(!eval_stack_push(&stack, float_key, false));
    wl_plan_op_t op = {.op = WL_PLAN_OP_REDUCE, .agg_fn = WIRELOG_AGG_COUNT,
                       .agg_operand_type = WL_PLAN_AGG_OPERAND_FLOAT,
                       .group_by_count = 1, .aggregate_index = 1};
    CHECK(col_op_reduce(&op, &stack, &session) == EINVAL && !stack.top);
    col_rel_destroy(float_key);
}

static void
raw_count_generated_bodies(void)
{
    const char *queries[] = {
        ".decl input(v:float)\n.decl output(n:int64)\noutput(count(v)) :- input(v).\n",
        ".decl input(g:int64,v:float)\n.decl output(g:int64,n:int64)\noutput(g,count(v)) :- input(g,v).\n",
        ".decl input(g:int64,v:float)\n.decl output(n:int64,g:int64)\noutput(count(v),g) :- input(g,v).\n",
        ".decl input(v:float)\n.decl output(n:int64)\noutput(count(v/0.0)) :- input(v).\n"
    };
    for (unsigned shape = 0; shape < 4; ++shape) {
        wirelog_error_t error;
        wirelog_program_t *program = wirelog_parse_string(queries[shape],
                &error);
        CHECK(program);
        wl_plan_t *plan = NULL; CHECK(!wl_plan_from_program(program, &plan));
        wl_plan_relation_t body = {0};
        for (uint32_t s = 0; s < plan->stratum_count; ++s)
            for (uint32_t r = 0; r < plan->strata[s].relation_count; ++r)
                if (!strcmp(plan->strata[s].relations[r].name, "output")) {
                    body = plan->strata[s].relations[r];
                    body.ops = plan->strata[s].bodies[r].ops;
                    body.op_count = plan->strata[s].bodies[r].op_count;
                }
        CHECK(body.op_count >= 3 && body.ops[0].op == WL_PLAN_OP_VARIABLE &&
            body.ops[body.op_count - 1].op == WL_PLAN_OP_CONSOLIDATE);
        unsigned reductions = 0;
        for (uint32_t i = 0; i < body.op_count; ++i)
            if (body.ops[i].op == WL_PLAN_OP_REDUCE) {
                ++reductions;
                CHECK(body.ops[i].agg_fn == WIRELOG_AGG_COUNT &&
                    body.ops[i].agg_operand_type == WL_PLAN_AGG_OPERAND_FLOAT &&
                    body.ops[i].agg_result_type == WL_PLAN_AGG_OPERAND_SCALAR);
            }
        CHECK(reductions == 1);
        bool grouped = shape == 1 || shape == 2;
        for (unsigned empty = 0; empty < 2; ++empty)
            for (unsigned poison = 0; poison < 3; ++poison) {
                col_rel_t *input = raw_count_input(grouped, empty != 0, poison);
                CHECK(!wl_columnar_relation_rename_checked(input,
                    body.ops[0].relation_name));
                wl_col_session_t session = {0};
                wl_mem_ledger_init(&session.mem_ledger, 0);
                col_rel_t *registry[] = {input}; session.rels = registry;
                session.nrels = 1;
                eval_stack_t stack = {0};
                CHECK(!col_eval_relation_plan(&body, &stack, &session));
                CHECK(stack.top == 1 && stack.items[0].owned &&
                    !stack.items[0].is_delta);
                col_rel_t *out = stack.items[0].rel;
                CHECK(out->nrows == (empty ? 0u : grouped ? 2u : 1u) &&
                    out->ncols == (grouped ? 2u : 1u));
                /* The raw owned <=1-row CONS path stamps one run even
                 * for an empty relation; this is not controlled CONS's cache. */
                CHECK(out->column_types && out->sorted_nrows == out->nrows &&
                    out->run_count == 1 && out->run_ends[0] == out->nrows);
                for (uint32_t c = 0; c < out->ncols;
                    ++c) CHECK(out->column_types[c] == WIRELOG_TYPE_INT64);
                for (uint32_t r = 0; r < out->nrows; ++r) {
                    if (!grouped) CHECK(out->columns[0][r] == 5);
                    else {
                        uint32_t agg = shape == 2 ? 0 : 1;
                        CHECK(out->columns[agg][r] == (int64_t)r + 2 &&
                            out->columns[1 - agg][r] == (int64_t)r + 1);
                    }
                }
                CHECK(!eval_stack_drain(&stack));
                session_rel_free_hash(&session); col_rel_destroy(input);
            }
        wl_plan_free(plan); wirelog_program_free(program);
    }
}

/* The standalone and genuine numeric-owned routes use one child per host
 * action. ACK is separately scheduled; a witness is ahead of every retry. */
typedef struct {
    wl_col_session_t *session;
    col_rel_t *input;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    wl_evaluation_control_t *operation, *funding;
    uint64_t operation_id, funding_id;
    int operation_owner, funding_owner;
    wl_plan_op_t op;
    wl_plan_relation_t plan;
    wl_columnar_eval_plan_numeric_attempt_t record;
    const void *owner;
    wl_columnar_filter_status_t status;
    unsigned phase, turns, witnesses;
    bool claim_pending, pending;
} reduce_fixture_t;

static int
reduce_child_action(reduce_fixture_t *f)
{
    if (!f->phase)
        return wl_columnar_ops_reduce_attempt_start_owned(&f->op, f->frame,
                   f->session, f->operation, f->operation_id, &f->status,
                   f->owner);
    if (f->phase == 1)
        return wl_columnar_filter_op_attempt_step_owned(f->frame, &f->status,
                   f->owner);
    return wl_columnar_filter_op_attempt_ack_owned(f->frame, f->owner);
}

static int
reduce_dispatch(reduce_fixture_t *f)
{
    if (wl_evaluation_control_active_attempt_id(f->funding) != f->funding_id)
        return EINVAL;
    int rc = wl_evaluation_control_charge(f->funding, 384);
    return rc ? rc : reduce_child_action(f);
}

static void
reduce_fixture_init(reduce_fixture_t *f, col_rel_t *input, bool owned,
    uint64_t limit, bool claim)
{
    memset(f, 0, sizeof(*f));
    f->session = wl_columnar_filter_test_session(); f->input = input;
    f->operation = wl_columnar_filter_test_control(limit, &f->operation_owner);
    f->funding = wl_columnar_filter_test_control(0, &f->funding_owner);
    f->operation_id = wl_evaluation_control_active_attempt_id(f->operation);
    f->funding_id = wl_evaluation_control_active_attempt_id(f->funding);
    CHECK(!wl_columnar_eval_stack_cleanup_begin(f->session, &f->frame));
    CHECK(!eval_stack_push(wl_columnar_eval_stack_cleanup_stack(f->frame),
        input, owned));
    f->op.op = WL_PLAN_OP_REDUCE;
    f->plan.ops = &f->op; f->plan.op_count = 1;
    f->claim_pending = claim;
    wl_columnar_eval_plan_numeric_attempt_init(&f->record);
}

/* Plan descriptors are complete before genuine claim and stay immutable
 * until numeric ACK. Claim is external setup, never inside child dispatch. */
static void
reduce_fixture_claim(reduce_fixture_t *f)
{
    CHECK(f->claim_pending && !f->owner);
    CHECK(!wl_columnar_eval_plan_numeric_attempt_begin(&f->record, &f->plan,
        0, 1, f->frame, f->session, f->operation, f->operation_id,
        f->funding, f->funding_id));
    f->owner = &f->record; f->claim_pending = false;
}

typedef int (*reduce_child_callback_t)(reduce_fixture_t *);

static int
reduce_start_child(reduce_fixture_t *f)
{
    return wl_columnar_ops_reduce_attempt_start_owned(&f->op, f->frame,
               f->session, f->operation, f->operation_id, &f->status, f->owner);
}

static int
reduce_step_child(reduce_fixture_t *f)
{
    return wl_columnar_filter_op_attempt_step_owned(f->frame, &f->status,
               f->owner);
}

static int
reduce_ack_child(reduce_fixture_t *f)
{
    return wl_columnar_filter_op_attempt_ack_owned(f->frame, f->owner);
}

/* Consume child status on a later, separately funded action. The direct
 * child-return turn performs no phase/status consume or inline ACK. */
static int
reduce_engine_dispatch(reduce_fixture_t *f)
{
    /* Readonly selection precedes HOST. No status/owner mutation on refusal. */
    reduce_child_callback_t child = !f->phase ? reduce_start_child
        : f->phase == 1 ? reduce_step_child : reduce_ack_child;
    if (wl_evaluation_control_active_attempt_id(f->funding) != f->funding_id)
        return EINVAL;
    int rc = wl_evaluation_control_charge(f->funding, 384);
    if (rc) return rc;
    if (!f->pending) {
        f->pending = true;
        return child(f);
    }
    f->pending = false;
    f->phase = f->phase == 2 ? 3
        : f->status.progress == WL_COLUMNAR_FILTER_PROGRESS_DONE ? 2 : 1;
    return 0;
}

/* An engine callback performs one funded child or consume action and returns
* through the existing real host_turn. Requeue suffix and that host return
* belong to its source envelope; the next queue pop belongs to a new turn. */
static void
reduce_host_engine(host_queue_t *queue, host_job_t *job)
{
    reduce_fixture_t *f = job->data;
    CHECK(f->turns == f->witnesses && ++f->turns < 10000);
    CHECK(!reduce_engine_dispatch(f));
    if (f->phase != 3) {
        CHECK(queue->count < HOST_QUEUE_CAPACITY && !job->queued);
        queue->jobs[(queue->head + queue->count) % HOST_QUEUE_CAPACITY] = job;
        ++queue->count;
        job->queued = true;
    }
}

static void
reduce_host_witness(host_queue_t *queue, host_job_t *job)
{
    reduce_fixture_t *f = job->data;
    CHECK(++f->witnesses == f->turns);
    if (f->phase != 3) CHECK(!host_enqueue(queue, job));
}

static void
reduce_run(reduce_fixture_t *f)
{
    if (f->claim_pending) reduce_fixture_claim(f);
    host_queue_t queue = {0};
    host_job_t engine = {.dispatch = reduce_host_engine, .data = f};
    host_job_t witness = {.dispatch = reduce_host_witness, .data = f};
    CHECK(!host_enqueue(&queue, &engine));
    CHECK(!host_enqueue(&queue, &witness));
    while (queue.count) CHECK(!host_turn(&queue));
    CHECK(!engine.queued && !witness.queued && f->phase == 3);
    CHECK(f->turns == f->witnesses);
}

static void
reduce_fixture_close(reduce_fixture_t *f, bool input_owned)
{
    CHECK(f->phase == 3);
    if (f->owner) {
        wl_evaluation_control_request_cancel(f->operation);
        for (unsigned turns = 0;
            f->record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE; ++turns) {
            CHECK(turns < 10);
            CHECK(!wl_columnar_eval_plan_numeric_attempt_step(&f->record,
                f->funding, f->funding_id));
        }
        CHECK(!f->record.completed);
        CHECK(!wl_columnar_eval_plan_numeric_attempt_ack(&f->record,
            f->funding, f->funding_id));
        f->owner = NULL;
    }
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&f->frame));
    CHECK(!wl_columnar_filter_test_reserved(f->session));
    int result = f->status.semantic_result;
    if (wl_evaluation_control_active_attempt_id(f->operation)) {
        int actual = wl_evaluation_control_finish(f->operation,
                &f->operation_owner, result, 0);
        CHECK(actual == result || actual == WL_EVALUATION_CONTROL_CANCELLED);
    }
    CHECK(!wl_evaluation_control_finish(f->funding, &f->funding_owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(f->operation, &f->operation_owner));
    CHECK(!wl_evaluation_control_detach(f->funding, &f->funding_owner));
    wl_evaluation_control_release(f->operation);
    wl_evaluation_control_release(f->funding);
    wl_columnar_memory_governor_ref_release(f->session->memory_governor);
    free(f->session);
    if (!input_owned) col_rel_destroy(f->input);
}

static col_rel_t *
reduce_literal_input(bool floating, bool empty)
{
    col_rel_t *input = wl_columnar_filter_test_source(2, 0, true, false, false);
    wirelog_column_type_t types[] = {WIRELOG_TYPE_INT64,
                                     floating ? WIRELOG_TYPE_FLOAT :
                                     WIRELOG_TYPE_INT64};
    CHECK(!col_rel_set_column_types(input, types, 2));
    int64_t keys[] = {2, 1, 2, 1, 2, 3};
    int64_t operands[] = {-3, 10, 5, -4, -2, 0};
    for (unsigned r = 0; !empty && r < 6; ++r) {
        int64_t row[] = {keys[r], floating
            ? wl_columnar_float_to_bits((double)operands[r]) : operands[r]};
        CHECK(!col_rel_append_row(input, row));
    }
    return input;
}

static void
reduce_literal_matrix(void)
{
    const wirelog_agg_fn_t functions[] = {WIRELOG_AGG_COUNT, WIRELOG_AGG_SUM,
                                          WIRELOG_AGG_MIN, WIRELOG_AGG_MAX,
                                          WIRELOG_AGG_AVG};
    const int64_t answers[5][3] = {{3, 2, 1}, {0, 6, 0}, {-3, -4, 0},
                                   {5, 10, 0}, {0, 3, 0}};
    for (unsigned kind = 0; kind < 5; ++kind)
        for (unsigned fp = 0; fp < 2; ++fp) {
            if (kind == 4 && !fp) continue;
            for (unsigned empty = 0; empty < 2; ++empty)
                for (unsigned ai = 0; ai < 3; ++ai)
                    for (unsigned claim = 0; claim < 2; ++claim) {
                        col_rel_t *input = reduce_literal_input(fp != 0,
                                empty != 0);
                        CHECK(!col_rel_enable_timestamps(input));
                        const int64_t physical_weights[] = {0, 1, -1, 3, -4};
                        for (uint32_t row = 0; row < input->nrows; ++row)
                            input->timestamps[row] = (col_delta_timestamp_t){
                                .iteration = 100-row, .stratum = row%3,
                                .worker = row%2, ._reserved = 0,
                                .multiplicity = physical_weights[row%5]
                            };
                        reduce_fixture_t f;
                        reduce_fixture_init(&f, input, false, 0, claim != 0);
                        f.op.agg_fn = functions[kind];
                        f.op.agg_operand_type = fp ? WL_PLAN_AGG_OPERAND_FLOAT
                            : WL_PLAN_AGG_OPERAND_SCALAR;
                        f.op.group_by_count = 1; f.op.aggregate_index = ai;
                        eval_stack_t raw = {0};
                        CHECK(!eval_stack_push(&raw, input, false));
                        CHECK(!col_op_reduce(&f.op, &raw, f.session));
                        reduce_run(&f);
                        CHECK(!f.status.semantic_result && f.status.committed);
                        eval_stack_t *stack =
                            wl_columnar_eval_stack_cleanup_stack(f.frame);
                        CHECK(stack->top == 1 && stack->items[0].owned &&
                            !stack->items[0].is_delta);
                        col_rel_t *actual = stack->items[0].rel;
                        CHECK(actual->ncols == 2 &&
                            actual->nrows == (empty ? 0u : 3u));
                        CHECK(!actual->has_graph_column && !actual->timestamps);
                        unsigned agg = ai < 2 ? ai : 1, key = 1u - agg;
                        CHECK(actual->column_types[agg] == (fp && kind
                            ? WIRELOG_TYPE_FLOAT : WIRELOG_TYPE_INT64));
                        const int64_t keys[] = {2, 1, 3};
                        for (unsigned r = 0; !empty && r < 3; ++r) {
                            int64_t expected = fp && kind
                                ? wl_columnar_float_to_bits(
                                (double)answers[kind][r])
                                : answers[kind][r];
                            CHECK(actual->columns[key][r] == keys[r]);
                            CHECK(actual->columns[agg][r] == expected);
                            for (unsigned c = 0; c < 2; ++c)
                                CHECK(actual->columns[c][r] ==
                                    raw.items[0].rel->columns[c][r]);
                        }
                        CHECK(!eval_stack_drain(&raw));
                        reduce_fixture_close(&f, false);
                    }
        }
}

static void
reduce_error_order(void)
{
    for (unsigned mode = 0; mode < 5; ++mode) {
        col_rel_t *input = wl_columnar_filter_test_source(2, 0, false, false,
                false);
        wirelog_column_type_t types[] = {WIRELOG_TYPE_FLOAT,
                                         WIRELOG_TYPE_INT64};
        CHECK(!col_rel_set_column_types(input, types, 2));
        for (unsigned r = 0; r < 3; ++r) {
            int64_t row[] = {wl_columnar_float_to_bits(0.0),
                             mode < 2 ? (r ? 0 : 1) : r == 1 ? INT64_MAX : 1};
            if (mode == 1) row[1] = 0;
            CHECK(!col_rel_append_row(input, row));
        }
        if (mode < 3) input->columns[0][0] = wl_columnar_float_to_bits(NAN);
        if (mode ==
            3) input->columns[0][2] = wl_columnar_float_to_bits(INFINITY);
        wl_columnar_filter_test_program_t expression = {0};
        wl_columnar_filter_test_variable(&expression, WL_PLAN_EXPR_VAR, 1);
        wl_columnar_filter_test_variable(&expression, WL_PLAN_EXPR_VAR, 1);
        wl_columnar_filter_test_opcode(&expression, WL_PLAN_EXPR_ARITH_DIV);
        reduce_fixture_t f;
        reduce_fixture_init(&f, input, false, 0, (mode & 1u) || mode == 2);
        f.op.agg_fn = WIRELOG_AGG_SUM;
        f.op.agg_operand_type = WL_PLAN_AGG_OPERAND_SCALAR;
        f.op.group_by_count = 1; f.op.aggregate_index = 1;
        if (mode < 2) {
            f.op.agg_expr.data = expression.bytes;
            f.op.agg_expr.size = expression.size;
        }
        eval_stack_t raw = {0}; CHECK(!eval_stack_push(&raw, input, false));
        int expected = mode == 0 || mode == 2 ? EINVAL : ERANGE;
        CHECK(col_op_reduce(&f.op, &raw, f.session) == expected);
        CHECK(!eval_stack_drain(&raw));
        reduce_run(&f);
        CHECK(f.status.semantic_result == expected && !f.status.committed);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
        CHECK(stack->top == 1 && stack->items[0].rel == input &&
            !stack->items[0].owned);
        reduce_fixture_close(&f, false);
    }
}

static unsigned reduce_cancel_phase;
static bool reduce_cancel_seen;
static wl_columnar_filter_context_t *reduce_observed_context;
static void
reduce_cancel_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    if (phase == reduce_cancel_phase && progress >= (phase == 47 ? 256u : 0u)) {
        if (phase == 47) CHECK(!reduce_observed_context->output->nrows);
        reduce_cancel_seen = true;
        wl_evaluation_control_request_cancel(control);
    }
}

static void
reduce_stop_matrix(void)
{
    const unsigned phases[] = {1, 2, 3, 9, 10, 11, 12, 30, 31, 32, 33, 35,
                               36, 39, 40, 41, 42, 43, 44, 45, 46, 47, 15, 48};
    for (unsigned test = 0; test < sizeof(phases) / sizeof(phases[0]); ++test) {
        col_rel_t *input = wl_columnar_filter_test_source(2, 600, false, false,
                false);
        for (unsigned r = 0; r < 600; ++r) {
            input->columns[0][r] = 1; input->columns[1][r] = 1;
        }
        reduce_fixture_t f; reduce_fixture_init(&f, input, false, 0, false);
        f.op.agg_fn = WIRELOG_AGG_SUM;
        f.op.agg_operand_type = WL_PLAN_AGG_OPERAND_SCALAR;
        f.op.group_by_count = 1; f.op.aggregate_index = 1;
        CHECK(!wl_columnar_eval_stack_filter_context(f.frame,
            &reduce_observed_context));
        reduce_cancel_phase = phases[test]; reduce_cancel_seen = false;
        wl_columnar_ops_test_reduce_attempt_checkpoint =
            reduce_cancel_checkpoint;
        reduce_run(&f);
        wl_columnar_ops_test_reduce_attempt_checkpoint = NULL;
        CHECK(reduce_cancel_seen && !f.status.committed &&
            f.status.semantic_result == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
            input);
        CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
            f.status.semantic_result, 0) == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(!wl_evaluation_control_reset(f.operation));
        CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
        f.operation_id = wl_evaluation_control_active_attempt_id(f.operation);
        f.phase = 0; reduce_run(&f);
        CHECK(!f.status.semantic_result && f.status.committed);
        col_rel_t *out =
            wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel;
        CHECK(out->nrows == 1 && out->columns[1][0] == 600);
        reduce_fixture_close(&f, false);
    }
    reduce_observed_context = NULL;
}

static void
reduce_collisions(void)
{
    col_rel_t *input = wl_columnar_filter_test_source(3, 96, false, false,
            false);
    uint64_t seed = UINT64_C(1469598103934665603),
        prime = UINT64_C(1099511628211);
    for (unsigned r = 0; r < 96; ++r) {
        input->columns[0][r] = r;
        input->columns[1][r] = (int64_t)((seed * prime) ^ ((seed ^ r) * prime));
        input->columns[2][r] = r + 7;
    }
    reduce_fixture_t f; reduce_fixture_init(&f, input, false, 0, true);
    f.op.agg_fn = WIRELOG_AGG_SUM;
    f.op.agg_operand_type = WL_PLAN_AGG_OPERAND_SCALAR;
    f.op.group_by_count = 2; f.op.aggregate_index = 1;
    reduce_run(&f); CHECK(!f.status.semantic_result && f.status.committed);
    col_rel_t *out =
        wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel;
    CHECK(out->nrows == 96 && out->ncols == 3);
    for (unsigned r = 0; r < 96; ++r) {
        CHECK(out->columns[0][r] == input->columns[0][r]);
        CHECK(out->columns[2][r] == input->columns[1][r]);
        CHECK(out->columns[1][r] == (int64_t)r + 7);
    }
    reduce_fixture_close(&f, false);
}

static void
reduce_owned_authority(void)
{
    reduce_fixture_t f;
    reduce_fixture_init(&f, reduce_literal_input(false, false), false, 0, true);
    f.op.agg_fn = WIRELOG_AGG_COUNT; f.op.group_by_count = 1;
    reduce_fixture_claim(&f);
    wl_columnar_filter_status_t status, sentinel;
    memset(&status, 0x5a, sizeof(status));
    memcpy(&sentinel, &status, sizeof(status));
    CHECK(wl_columnar_ops_reduce_attempt_start_owned(&f.op, f.frame, f.session,
        NULL, 0, &status, &f.record) == EINVAL);
    CHECK(!memcmp(&status, &sentinel, sizeof(status)));
    wl_columnar_eval_plan_numeric_attempt_t copy = f.record;
    CHECK(wl_columnar_ops_reduce_attempt_start_owned(&f.op, f.frame, f.session,
        f.operation, f.operation_id, &status, &copy) == EBUSY);
    CHECK(!memcmp(&status, &sentinel, sizeof(status)));
    CHECK(wl_columnar_ops_reduce_attempt_start_owned(&f.op, f.frame, f.session,
        f.operation, f.operation_id + 1, &status, &f.record) == EINVAL);
    CHECK(!memcmp(&status, &sentinel, sizeof(status)));
    CHECK(wl_columnar_ops_reduce_attempt_start_owned(&f.op, f.frame, f.session,
        f.funding, f.operation_id, &status, &f.record) == EINVAL);
    CHECK(!memcmp(&status, &sentinel, sizeof(status)));
    reduce_run(&f); CHECK(f.status.committed && !f.status.semantic_result);
    reduce_fixture_close(&f, false);
}

static void
reduce_allocation_and_domains(void)
{
    for (unsigned failure = 0; failure < 3; ++failure) {
        reduce_fixture_t f;
        reduce_fixture_init(&f, reduce_literal_input(false, false), false, 0,
            false);
        f.op.agg_fn = WIRELOG_AGG_SUM; f.op.group_by_count = 1;
        f.op.agg_operand_type = WL_PLAN_AGG_OPERAND_SCALAR;
        wl_columnar_ops_test_reduce_attempt_fail_allocation = (int)failure;
        reduce_run(&f);
        wl_columnar_ops_test_reduce_attempt_fail_allocation = -1;
        CHECK(f.status.semantic_result == ENOMEM && !f.status.committed);
        CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
            f.input);
        reduce_fixture_close(&f, false);
    }
    for (unsigned mode = 0; mode < 8; ++mode) {
        reduce_fixture_t f;
        reduce_fixture_init(&f, reduce_literal_input(true, false), false, 0,
            false);
        f.op.agg_fn = mode < 4 ? WIRELOG_AGG_COUNT : WIRELOG_AGG_MIN;
        f.op.agg_operand_type = mode < 4 ? (wl_plan_agg_operand_t)mode
            : WL_PLAN_AGG_OPERAND_STRING;
        f.op.group_by_count = 1;
        if (mode < 4) f.input->columns[1][0] = wl_columnar_float_to_bits(NAN);
        if (mode == 5) {
            f.op.agg_fn = WIRELOG_AGG_AVG;
            f.op.agg_operand_type = WL_PLAN_AGG_OPERAND_SCALAR;
        }
        if (mode == 6) {
            f.op.agg_fn = WIRELOG_AGG_COUNT; f.op.materialized = true;
        }
        if (mode == 7) {
            f.op.agg_fn = WIRELOG_AGG_COUNT; f.op.group_by_count = UINT32_MAX;
        }
        reduce_run(&f);
        CHECK(f.status.semantic_result ==
            (mode < 4 ? 0 : mode == 7 ? EOVERFLOW : ENOTSUP));
        CHECK(f.status.committed == (mode < 4));
        reduce_fixture_close(&f, false);
    }
}

static void
reduce_retained_resources(void)
{
    reduce_fixture_t f;
    reduce_fixture_init(&f, reduce_literal_input(false, false), false, 0,
        false);
    f.op.agg_fn = WIRELOG_AGG_COUNT; f.op.group_by_count = 1;
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context(f.frame, &ctx));
    for (unsigned turns = 0;
        ctx->stage != WL_COLUMNAR_FILTER_STAGE_JOIN_CREDIT ||
        ctx->join_retirement_cursor;
        ++turns) {
        CHECK(turns < 1000 && !reduce_dispatch(&f)); f.phase = 1;
    }
    wl_columnar_join_scratch_t *scratch = &ctx->join_scratch[0];
    CHECK(scratch->admitted && scratch->committed && scratch->ref_held &&
        !scratch->physical);
    uint64_t reserved = wl_columnar_filter_test_reserved(f.session);
    uint64_t bytes = scratch->credit.bytes;
    wl_columnar_memory_governor_test_refuse_next_release();
    CHECK(!reduce_dispatch(&f));
    CHECK(f.status.progress == WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
    CHECK(scratch->admitted && scratch->ref_held &&
        scratch->credit.bytes == bytes);
    CHECK(wl_columnar_filter_test_reserved(f.session) == reserved);
    wl_evaluation_control_t *saved = f.funding;
    uint64_t saved_id = f.funding_id;
    for (unsigned allowance = 383; allowance <= 384; ++allowance) {
        int host_owner;
        f.funding = wl_columnar_filter_test_control(allowance, &host_owner);
        f.funding_id = wl_evaluation_control_active_attempt_id(f.funding);
        int rc = reduce_dispatch(&f);
        CHECK(rc == (allowance == 383 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        if (allowance == 383) CHECK(scratch->admitted &&
                ctx->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_CREDIT);
        CHECK(wl_evaluation_control_finish(f.funding, &host_owner, 0, 0) ==
            (allowance == 383 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        CHECK(!wl_evaluation_control_detach(f.funding, &host_owner));
        wl_evaluation_control_release(f.funding);
    }
    f.funding = saved; f.funding_id = saved_id;
    CHECK(!scratch->admitted && scratch->ref_held);
    CHECK(wl_columnar_filter_test_reserved(f.session) == reserved - bytes);
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    CHECK(!wl_evaluation_control_reset(f.operation));
    CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
    reduce_run(&f);
    CHECK(f.status.semantic_result == EINVAL && !f.status.committed);
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation, &report) &&
        report.attempt_id == f.operation_id + 1 && !report.consumed);
    CHECK(!wl_evaluation_control_reset(f.operation));
    CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
    f.operation_id = wl_evaluation_control_active_attempt_id(f.operation);
    f.phase = 0; reduce_run(&f);
    CHECK(f.status.committed && !f.status.semantic_result);
    reduce_fixture_close(&f, false);

    col_rel_t *input = reduce_literal_input(false, false);
    wl_columnar_source_access_reader_t reader = {0};
    CHECK(!wl_columnar_source_access_reader_acquire(&input->source_access,
        &reader));
    reduce_fixture_init(&f, input, true, 0, false);
    f.op.agg_fn = WIRELOG_AGG_COUNT; f.op.group_by_count = 1;
    for (unsigned turns = 0; ; ++turns) {
        CHECK(turns < 1000 && !reduce_dispatch(&f)); f.phase = 1;
        if (f.status.progress == WL_COLUMNAR_FILTER_PROGRESS_RETAINED) break;
        CHECK(f.status.progress == WL_COLUMNAR_FILTER_PROGRESS_YIELD);
    }
    CHECK(f.status.committed && !f.status.execution_result &&
        f.status.cleanup_result == EBUSY && f.status.semantic_result == EBUSY);
    col_rel_t *published =
        wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel;
    uint64_t generation = published->storage_generation;
    CHECK(!reduce_dispatch(&f));
    CHECK(f.status.progress == WL_COLUMNAR_FILTER_PROGRESS_RETAINED &&
        f.status.committed && !f.status.execution_result &&
        f.status.cleanup_result == EBUSY && f.status.semantic_result == EBUSY);
    CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
        published &&
        published->storage_generation == generation);
    CHECK(!wl_columnar_source_access_reader_release(&reader));
    reduce_run(&f);
    CHECK(f.status.committed && !f.status.semantic_result &&
        !f.status.cleanup_result);
    reduce_fixture_close(&f, true);
}

static void
reduce_shape_and_expression(void)
{
    for (unsigned mode = 0; mode < 7; ++mode) {
        col_rel_t *input = mode == 0
            ? wl_columnar_filter_test_source(0, 3, false, false, false)
            : mode >= 4 ? wl_columnar_filter_test_source(2, 0, false, false,
                false)
            : reduce_literal_input(mode == 2 || mode == 3, false);
        reduce_fixture_t f;
        reduce_fixture_init(&f, input, false, 0, (mode & 1u) || mode == 2);
        f.op.agg_fn = mode == 2 ||
            mode == 3 ? WIRELOG_AGG_SUM : WIRELOG_AGG_COUNT;
        f.op.agg_operand_type = mode == 2 || mode == 3
            ? WL_PLAN_AGG_OPERAND_FLOAT : WL_PLAN_AGG_OPERAND_SCALAR;
        f.op.group_by_count = mode ? 1 : 0; f.op.aggregate_index = 1;
        wl_columnar_filter_test_program_t expression = {0};
        wl_columnar_filter_test_variable(&expression, WL_PLAN_EXPR_VAR_FLOAT,
            1);
        if (mode == 2) {
            f.op.agg_expr.data = expression.bytes;
            f.op.agg_expr.size = expression.size;
        }
        uint32_t out_of_range = 99;
        if (mode == 3) {
            /* Missing operand fallback is bits1, not numeric1.0. */
            f.op.group_by_count = 2; f.op.aggregate_index = 2;
        }
        if (mode >= 4) {
            wirelog_column_type_t types[] = {WIRELOG_TYPE_FLOAT,
                                             WIRELOG_TYPE_INT64};
            CHECK(!col_rel_set_column_types(input, types, 2));
            for (unsigned r = 0; r < 6; ++r) {
                int64_t row[] = {wl_columnar_float_to_bits(0.0), r};
                CHECK(!col_rel_append_row(input, row));
            }
            for (uint32_t r = 0; r < input->nrows; ++r)
                input->columns[0][r] = wl_columnar_float_to_bits(r &
                        1u ? -0.0 : 0.0);
            if (mode == 5) {
                f.op.group_by_indices = &out_of_range;
                for (uint32_t r = 1; r < input->nrows;
                    ++r) input->columns[0][r] = 0;
                input->columns[0][0] = wl_columnar_float_to_bits(NAN);
            }
            if (mode ==
                6) input->columns[0][0] = wl_columnar_float_to_bits(NAN);
        }
        reduce_run(&f);
        CHECK(f.status.semantic_result == (mode == 6 ? EINVAL : 0));
        if (mode != 6) {
            col_rel_t *out =
                wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel;
            CHECK(f.status.committed);
            if (!mode) CHECK(out->nrows == 1 && out->ncols == 1 &&
                    out->columns[0][0] == 3);
            if (mode == 1) CHECK(out->nrows == 3 && out->columns[1][0] == 3);
            if (mode == 2) CHECK(out->nrows == 3 &&
                    out->columns[1][0] == wl_columnar_float_to_bits(0.0));
            if (mode == 3) for (uint32_t r = 0; r < out->nrows;
                    ++r) CHECK(out->columns[2][r] == 1);
            if (mode == 4) CHECK(out->nrows == 1 && out->columns[0][0] == 0 &&
                    out->columns[1][0] == 6);
            if (mode == 5) CHECK(out->nrows == 2 &&
                    out->column_types[0] == WIRELOG_TYPE_INT64 &&
                    out->columns[0][0] == wl_columnar_float_to_bits(NAN) &&
                    out->columns[1][0] == 1);
        }
        reduce_fixture_close(&f, false);
    }
}

static void
reduce_wide_and_float_overflow(void)
{
    const uint32_t widths[] = {1, 17, 129};
    const int64_t operands[] = {-3, 10, 5, -4, -2, 0};
    for (unsigned test = 0; test < 3; ++test) {
        uint32_t width = widths[test], gc = width - 1;
        col_rel_t *input = wl_columnar_filter_test_source(width, 6, false,
                false, false);
        for (uint32_t r = 0; r < 6; ++r) {
            for (uint32_t c = 0; c < gc; ++c) input->columns[c][r] = c + 7;
            input->columns[gc][r] = operands[r];
        }
        reduce_fixture_t f; reduce_fixture_init(&f, input, false, 0, true);
        f.op.agg_fn = WIRELOG_AGG_SUM;
        f.op.agg_operand_type = WL_PLAN_AGG_OPERAND_SCALAR;
        f.op.group_by_count = gc; f.op.aggregate_index = gc / 2;
        reduce_run(&f); CHECK(f.status.committed && !f.status.semantic_result);
        col_rel_t *out =
            wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel;
        CHECK(out->ncols == width && out->nrows == 1);
        for (uint32_t c = 0; c < width; ++c) {
            uint32_t k = c > f.op.aggregate_index ? c - 1 : c;
            CHECK(out->column_types[c] == WIRELOG_TYPE_INT64);
            CHECK(out->columns[c][0] ==
                (c == f.op.aggregate_index ? 6 : (int64_t)k+7));
        }
        reduce_fixture_close(&f, false);
    }
    for (unsigned average = 0; average < 2; ++average) {
        col_rel_t *input = reduce_literal_input(true, false);
        for (uint32_t r = 0; r < input->nrows; ++r) {
            input->columns[0][r] = 0;
            input->columns[1][r] = wl_columnar_float_to_bits(r <
                    2 ? 1.0e308 : -1.0e308);
        }
        reduce_fixture_t f; reduce_fixture_init(&f, input, false, 0, false);
        f.op.agg_fn = average ? WIRELOG_AGG_AVG : WIRELOG_AGG_SUM;
        f.op.agg_operand_type = WL_PLAN_AGG_OPERAND_FLOAT;
        f.op.group_by_count = 1;
        reduce_run(&f);
        CHECK(!f.status.committed && f.status.semantic_result == ERANGE);
        CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
            input);
        reduce_fixture_close(&f, false);
    }
}

static void
reduce_group_native_types(void)
{
    const wirelog_column_type_t types[] = {WIRELOG_TYPE_INT32,
                                           WIRELOG_TYPE_INT64,
                                           WIRELOG_TYPE_UINT32,
                                           WIRELOG_TYPE_UINT64,
                                           WIRELOG_TYPE_FLOAT,
                                           WIRELOG_TYPE_STRING,
                                           WIRELOG_TYPE_BOOL,
                                           WIRELOG_TYPE_INT64};
    uint32_t keys[] = {6, 4, 0, 2, 3, 5, 1, 4};
    col_rel_t *input = wl_columnar_filter_test_source(8, 0, false, false,
            false);
    CHECK(!col_rel_set_column_types(input, types, 8));
    for (unsigned r = 0; r < 2; ++r) {
        int64_t row[] = {-7, -9, 11, 13,
                         wl_columnar_float_to_bits(r ? -0.0 : 0.0), 19, 3, 99};
        CHECK(!col_rel_append_row(input, row));
    }
    reduce_fixture_t f; reduce_fixture_init(&f, input, false, 0, true);
    f.op.agg_fn = WIRELOG_AGG_COUNT; f.op.group_by_count = 8;
    f.op.group_by_indices = keys; f.op.aggregate_index = 4;
    reduce_run(&f); CHECK(f.status.committed && !f.status.semantic_result);
    col_rel_t *out =
        wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel;
    CHECK(out->ncols == 9 && out->nrows == 1);
    for (unsigned c = 0; c < 9; ++c) {
        if (c == 4) {
            CHECK(out->column_types[c] == WIRELOG_TYPE_INT64 &&
                out->columns[c][0] == 2); continue;
        }
        uint32_t k = c > 4 ? c-1 : c;
        CHECK(out->column_types[c] == types[keys[k]]);
        CHECK(out->columns[c][0] ==
            (keys[k] == 4 ? 0 : input->columns[keys[k]][0]));
    }
    reduce_fixture_close(&f, false);
}

static void
reduce_entry_limits(void)
{
    for (unsigned limit = 191; limit <= 192; ++limit) {
        reduce_fixture_t f;
        reduce_fixture_init(&f, reduce_literal_input(false, false), false,
            limit, false);
        f.op.agg_fn = WIRELOG_AGG_COUNT; f.op.group_by_count = 1;
        reduce_run(&f);
        CHECK(!f.status.committed &&
            f.status.semantic_result == WL_EVALUATION_CONTROL_WORK_LIMIT);
        CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
            f.input);
        CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
            f.status.semantic_result, 0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.operation, &report));
        CHECK(report.attempt_id == f.operation_id &&
            report.consumed == (limit == 191 ? 0u : 192u));
        reduce_fixture_close(&f, false);
    }
}

static void
reduce_quota_retry(void)
{
    reduce_fixture_t f;
    reduce_fixture_init(&f, reduce_literal_input(false, false), false, 0,
        false);
    f.op.agg_fn = WIRELOG_AGG_COUNT; f.op.group_by_count = 1;
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer);
    uint64_t bytes = 16u*1024u*1024u-
        wl_columnar_filter_test_reserved(f.session)-1u;
    CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(
            f.session->memory_governor), bytes, &peer));
    reduce_run(&f);
    CHECK(!f.status.committed && f.status.semantic_result == ENOSPC &&
        f.session->memory_budget_denied);
    CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
        f.input);
    CHECK(wl_columnar_memory_rollback(&peer));
    CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner, ENOSPC,
        0) == ENOSPC);
    CHECK(!wl_evaluation_control_reset(f.operation));
    CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
    f.operation_id = wl_evaluation_control_active_attempt_id(f.operation);
    f.phase = 0;
    reduce_run(&f); CHECK(f.status.committed && !f.status.semantic_result);
    reduce_fixture_close(&f, false);
}

static void
reduce_all_reached_allocations(void)
{
    unsigned counts[4] = {0};
    wl_columnar_filter_test_program_t expression = {0};
    wl_columnar_filter_test_variable(&expression, WL_PLAN_EXPR_VAR, 1);
    for (unsigned pass = 0; pass < 2; ++pass)
        for (unsigned family = 0; family < (pass ? 4u : 1u); ++family)
            for (unsigned ordinal = 0; ordinal < (pass ? counts[family] : 1u);
                ++ordinal){
                col_rel_t *input = wl_columnar_filter_test_source(2, 96, false,
                        false, false);
                for (unsigned r = 0; r < 96; ++r) {
                    input->columns[0][r] = r; input->columns[1][r] = 1;
                }
                reduce_fixture_t f;
                reduce_fixture_init(&f, input, false, 0, true);
                f.op.agg_fn = WIRELOG_AGG_SUM;
                f.op.agg_operand_type = WL_PLAN_AGG_OPERAND_SCALAR;
                f.op.group_by_count = 1; f.op.aggregate_index = 1;
                f.op.agg_expr.data = expression.bytes;
                f.op.agg_expr.size = expression.size;
                if (!pass ||
                    family ==
                    0) wl_columnar_relation_test_constructor_fail_allocation =
                        pass ? ordinal+1u : UINT32_MAX;
                if (!pass ||
                    family ==
                    1) wl_columnar_relation_test_growth_fail_allocation =
                        pass ? ordinal+1u : UINT32_MAX;
                if (!pass ||
                    family ==
                    2) wl_columnar_expr_test_fail_allocation =
                        pass ? (int)ordinal : 100000;
                if (!pass ||
                    family ==
                    3) wl_columnar_ops_test_reduce_attempt_fail_allocation =
                        pass ? (int)ordinal : 100000;
                reduce_run(&f);
                if (!pass) {
                    counts[0] = UINT32_MAX-
                        wl_columnar_relation_test_constructor_fail_allocation;
                    counts[1] = UINT32_MAX-
                        wl_columnar_relation_test_growth_fail_allocation;
                    counts[2] = (unsigned)(100000-
                        wl_columnar_expr_test_fail_allocation);
                    counts[3] = (unsigned)(100000-
                        wl_columnar_ops_test_reduce_attempt_fail_allocation);
                    CHECK(f.status.committed && !f.status.semantic_result);
                } else {
                    CHECK(!f.status.committed &&
                        f.status.semantic_result == ENOMEM);
                    CHECK(wl_columnar_eval_stack_cleanup_stack(
                            f.frame)->items[0].rel == input);
                }
                wl_columnar_relation_test_constructor_fail_allocation = 0;
                wl_columnar_relation_test_growth_fail_allocation = 0;
                wl_columnar_expr_test_fail_allocation = -1;
                wl_columnar_ops_test_reduce_attempt_fail_allocation = -1;
                reduce_fixture_close(&f, false);
            }
    CHECK(counts[0] && counts[1] && counts[2] && counts[3] == 3);
}

static unsigned reduce_expression_stop_phase;
static bool reduce_expression_stop_seen;
static void
reduce_expression_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress, bool failed)
{
    (void)progress; (void)failed;
    if (control && phase == reduce_expression_stop_phase) {
        reduce_expression_stop_seen = true;
        wl_evaluation_control_request_cancel(control);
    }
}

static void
reduce_expression_stops_and_ignored_count(void)
{
    const unsigned phases[] = {11, 14, 15, 16, 17, 18, 20, 21, 22};
    wl_columnar_filter_test_program_t expression = {0};
    wl_columnar_filter_test_variable(&expression, WL_PLAN_EXPR_VAR_FLOAT, 1);
    for (unsigned i = 0; i < 16; ++i) {
        wl_columnar_filter_test_constant(&expression, WL_PLAN_EXPR_CONST_FLOAT,
            wl_columnar_float_to_bits(0.0));
        wl_columnar_filter_test_opcode(&expression,
            WL_PLAN_EXPR_ARITH_FLOAT_ADD);
    }
    for (unsigned test = 0; test < sizeof(phases)/sizeof(phases[0]); ++test) {
        reduce_fixture_t f;
        reduce_fixture_init(&f, reduce_literal_input(true, false), false, 0,
            true);
        f.op.agg_fn = WIRELOG_AGG_SUM;
        f.op.agg_operand_type = WL_PLAN_AGG_OPERAND_FLOAT;
        f.op.group_by_count = 1; f.op.aggregate_index = 1;
        f.op.agg_expr.data = expression.bytes;
        f.op.agg_expr.size = expression.size;
        reduce_expression_stop_phase = phases[test];
        reduce_expression_stop_seen = false;
        wl_columnar_expr_test_checkpoint = reduce_expression_checkpoint;
        reduce_run(&f); wl_columnar_expr_test_checkpoint = NULL;
        CHECK(reduce_expression_stop_seen && !f.status.committed &&
            f.status.semantic_result == WL_EVALUATION_CONTROL_CANCELLED);
        CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
            f.input);
        reduce_fixture_close(&f, false);
    }
    reduce_fixture_t f;
    reduce_fixture_init(&f, reduce_literal_input(true, false), false, 0, true);
    f.op.agg_fn = WIRELOG_AGG_COUNT;
    f.op.agg_operand_type = WL_PLAN_AGG_OPERAND_FLOAT;
    f.op.group_by_count = 1; f.op.aggregate_index = 1;
    f.op.agg_expr.data = expression.bytes; f.op.agg_expr.size = expression.size;
    f.input->columns[1][0] = wl_columnar_float_to_bits(NAN);
    wl_columnar_expr_test_fail_allocation = 0;
    reduce_run(&f); CHECK(wl_columnar_expr_test_fail_allocation == 0);
    wl_columnar_expr_test_fail_allocation = -1;
    CHECK(f.status.committed && !f.status.semantic_result);
    col_rel_t *out =
        wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel;
    CHECK(out->nrows == 3 && out->columns[1][0] == 3);
    reduce_fixture_close(&f, false);
}

static void
reduce_engine_host_refusal(void)
{
    reduce_fixture_t f;
    reduce_fixture_init(&f, reduce_literal_input(false, false), false, 0,
        false);
    f.op.agg_fn = WIRELOG_AGG_COUNT; f.op.group_by_count = 1;
    f.op.aggregate_index = 1;
    wl_columnar_filter_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_filter_context(f.frame, &ctx));
    wl_columnar_filter_status_t sentinel;
    memset(&f.status, 0x5a, sizeof(f.status));
    memcpy(&sentinel, &f.status, sizeof(sentinel));
    wl_evaluation_control_t *saved = f.funding;
    uint64_t saved_id = f.funding_id;
    for (unsigned allowance = 383; allowance <= 384; ++allowance) {
        int host_owner;
        f.funding = wl_columnar_filter_test_control(allowance, &host_owner);
        f.funding_id = wl_evaluation_control_active_attempt_id(f.funding);
        int rc = reduce_engine_dispatch(&f);
        CHECK(rc == (allowance == 383 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        if (allowance == 383) {
            CHECK(!f.pending && !f.phase &&
                ctx->stage == WL_COLUMNAR_FILTER_STAGE_INERT);
            CHECK(!memcmp(&f.status, &sentinel, sizeof(sentinel)));
            CHECK(wl_columnar_eval_stack_cleanup_stack(f.frame)->items[0].rel ==
                f.input);
        } else CHECK(f.pending && !f.phase &&
                ctx->stage == WL_COLUMNAR_FILTER_STAGE_JOIN_BATCH);
        CHECK(wl_evaluation_control_finish(f.funding, &host_owner, 0, 0) ==
            (allowance == 383 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.funding, &report));
        CHECK(report.attempt_id == f.funding_id &&
            report.consumed == (allowance == 383 ? 0u : 384u));
        CHECK(!wl_evaluation_control_detach(f.funding, &host_owner));
        wl_evaluation_control_release(f.funding);
    }
    f.funding = saved; f.funding_id = saved_id;
    reduce_run(&f);
    CHECK(!f.status.semantic_result && f.status.committed && !f.pending);
    CHECK(wl_columnar_eval_stack_cleanup_stack(
            f.frame)->items[0].rel->columns[1][0] == 3);
    reduce_fixture_close(&f, false);
}

/* Binary authority/transaction fixtures. Each finish/ACK enters one actual
* host action and returns before the external scheduler inspects status. */
typedef struct {
    wl_col_session_t *session;
    wl_columnar_eval_stack_cleanup_frame_t *frame;
    wl_evaluation_control_t *operation, *funding;
    uint64_t operation_id, funding_id;
    int operation_owner, funding_owner;
    wl_columnar_eval_plan_numeric_attempt_t record;
    wl_plan_op_t op;
    wl_plan_relation_t plan;
    wl_columnar_filter_status_t status;
    col_rel_t *inputs[COL_STACK_MAX];
    eval_entry_t before[COL_STACK_MAX];
    unsigned depth;
    bool owned, bound;
    int execution_result;
} binary_fixture_t;

static bool
binary_test_entry_equal(const eval_entry_t *a, const eval_entry_t *b)
{
    return a->rel == b->rel && a->owned == b->owned
           && a->is_delta == b->is_delta && a->seg_count == b->seg_count
           && a->seg_boundaries == b->seg_boundaries && a->kind == b->kind
           && a->continuation == b->continuation;
}

static void
binary_fixture_init(binary_fixture_t *f, unsigned depth, bool owned,
    bool bound, uint64_t limit)
{
    memset(f, 0, sizeof(*f));
    f->session = wl_columnar_filter_test_session();
    f->operation = wl_columnar_filter_test_control(limit, &f->operation_owner);
    f->funding = wl_columnar_filter_test_control(0, &f->funding_owner);
    f->operation_id = wl_evaluation_control_active_attempt_id(f->operation);
    f->funding_id = wl_evaluation_control_active_attempt_id(f->funding);
    CHECK(!wl_columnar_eval_stack_cleanup_begin(f->session, &f->frame));
    CHECK(f->session->cleanup_reserved_bytes &&
        wl_columnar_filter_test_reserved(f->session) ==
        f->session->cleanup_reserved_bytes);
    f->depth = depth; f->owned = owned; f->bound = bound;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
    for (unsigned i = 0; i < depth; ++i) {
        f->inputs[i] = wl_columnar_filter_test_source(1, 3, false, false,
                false);
        CHECK(!eval_stack_push(stack, f->inputs[i], owned && i + 2 >= depth));
        eval_entry_t *entry = &stack->items[i];
        entry->is_delta = i % 2 != 0;
        entry->seg_boundaries = malloc(2 * sizeof(uint32_t));
        CHECK(entry->seg_boundaries);
        entry->seg_count = 1; entry->seg_boundaries[0] = 0;
        entry->seg_boundaries[1] = 3; f->before[i] = *entry;
    }
    f->op.op = WL_PLAN_OP_CONCAT;
    f->plan.ops = &f->op; f->plan.op_count = 1;
    wl_columnar_eval_plan_numeric_attempt_init(&f->record);
    if (bound) CHECK(!wl_columnar_eval_plan_numeric_stack_attempt_begin(
                &f->record, &f->plan, 0, 1, f->frame, f->session, depth,
                f->operation, f->operation_id, f->funding, f->funding_id));
}

static const void *
binary_owner(binary_fixture_t *f)
{
    return f->bound ? &f->record : NULL;
}

static int
binary_host_finish(binary_fixture_t *f)
{
    return wl_columnar_eval_stack_binary_finish_owned(f->frame,
               f->execution_result, &f->status, f->funding, f->funding_id,
               binary_owner(f));
}

static int
binary_host_ack(binary_fixture_t *f)
{
    return wl_columnar_eval_stack_binary_ack_owned(f->frame,
               f->funding, f->funding_id, binary_owner(f));
}

static int
binary_record_turn(binary_fixture_t *f)
{
    return wl_columnar_eval_plan_numeric_attempt_step(&f->record, f->funding,
               f->funding_id);
}

static void
binary_run(binary_fixture_t *f)
{
    unsigned witnesses = 0;
    do {
        ++witnesses;
        CHECK(witnesses < 1000);
        CHECK(!binary_host_finish(f));
    } while (f->status.progress != WL_COLUMNAR_FILTER_PROGRESS_DONE);
    CHECK(!binary_host_ack(f));
}

static void
binary_fixture_close(binary_fixture_t *f, bool committed)
{
    (void)committed;
    if (f->bound) {
        /* CONCAT remains unsupported in this ownership prerequisite. Retire
         * the genuine record through actual stopped preflight, never forge
         * a phase or clear the owner through a borrowed pointer. */
        wl_evaluation_control_request_cancel(f->operation);
        for (unsigned turns = 0;
            f->record.phase != WL_COLUMNAR_EVAL_PLAN_NUMERIC_DONE; ++turns){
            CHECK(turns < 100 && !binary_record_turn(f));
        }
        CHECK(!wl_columnar_eval_plan_numeric_attempt_ack(&f->record,
            f->funding, f->funding_id));
    }
    CHECK(!wl_columnar_eval_stack_cleanup_finish(&f->frame));
    for (unsigned i = 0; i < f->depth; ++i)
        if (!f->owned || i + 2 < f->depth)
            col_rel_destroy(f->inputs[i]);
    CHECK(!f->session->cleanup_reserved_bytes &&
        !wl_columnar_filter_test_reserved(f->session));
    if (wl_evaluation_control_active_attempt_id(f->operation))
        (void)wl_evaluation_control_finish(f->operation, &f->operation_owner,
            f->execution_result, 0);
    CHECK(!wl_evaluation_control_finish(f->funding, &f->funding_owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(f->operation, &f->operation_owner));
    CHECK(!wl_evaluation_control_detach(f->funding, &f->funding_owner));
    wl_evaluation_control_release(f->operation);
    wl_evaluation_control_release(f->funding);
    wl_columnar_memory_governor_ref_release(f->session->memory_governor);
    free(f->session);
}

static void
binary_assert_originals(binary_fixture_t *f)
{
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f->frame);
    CHECK(stack->top == f->depth);
    for (unsigned i = 0; i < f->depth; ++i)
        CHECK(binary_test_entry_equal(&stack->items[i], &f->before[i]));
}

static void
binary_test_transactions(void)
{
    for (unsigned bound = 0; bound < 2; ++bound)
        for (unsigned owned = 0; owned < 2; ++owned)
            for (unsigned publish = 0; publish < 2; ++publish)
                for (unsigned depth = 2; depth <= 3; ++depth) {
                    binary_fixture_t f;
                    binary_fixture_init(&f, depth, owned, bound, 0);
                    CHECK(!wl_columnar_eval_stack_binary_begin_owned(f.frame,
                        f.session, f.operation, f.operation_id,
                        binary_owner(&f)));
                    for (unsigned side = 0; side < 2; ++side) {
                        const eval_entry_t *entry = NULL;
                        CHECK(!wl_columnar_eval_stack_binary_input_owned(
                                f.frame, side, &entry, binary_owner(&f)));
                        CHECK(binary_test_entry_equal(entry,
                            &f.before[depth - 2 + side]));
                    }
                    col_rel_t **slot = NULL;
                    CHECK(!wl_columnar_eval_stack_binary_output_owned(f.frame,
                        &slot, binary_owner(&f)));
                    col_rel_t *candidate = wl_columnar_filter_test_source(1,
                            2, false, false, false);
                    *slot = candidate;
                    if (publish) {
                        bool committed = false;
                        CHECK(!wl_columnar_eval_stack_binary_commit_owned(
                                f.frame, &committed,
                                binary_owner(&f)) && committed);
                        eval_stack_t *stack =
                            wl_columnar_eval_stack_cleanup_stack(f.frame);
                        CHECK(stack->top == depth - 1);
                        CHECK(stack->items[depth - 2].rel == candidate &&
                            stack->items[depth - 2].owned &&
                            !stack->items[depth - 2].is_delta &&
                            !stack->items[depth - 2].seg_boundaries &&
                            !stack->items[depth - 2].seg_count);
                        for (unsigned i = 0; i + 2 < depth; ++i)
                            CHECK(binary_test_entry_equal(&stack->items[i],
                                &f.before[i]));
                        CHECK(wl_columnar_eval_stack_binary_output_owned(
                                f.frame, &slot, binary_owner(&f)) == EINVAL);
                    } else {
                        f.execution_result = ERANGE;
                        binary_assert_originals(&f);
                    }
                    binary_run(&f);
                    CHECK(f.status.committed == (publish != 0));
                    CHECK(f.status.execution_result == (publish ? 0 : ERANGE));
                    CHECK(f.status.semantic_result == (publish ? 0 : ERANGE));
                    if (!publish) binary_assert_originals(&f);
                    binary_fixture_close(&f, publish);
                }
}

static void
binary_test_gate(void)
{
    for (unsigned bound = 0; bound < 2; ++bound) {
        binary_fixture_t f; binary_fixture_init(&f, 2, false, bound, 0);
        CHECK(!wl_columnar_eval_stack_binary_begin_owned(f.frame, f.session,
            f.operation, f.operation_id, binary_owner(&f)));
        const col_rel_t *input = NULL; col_rel_t **output = NULL;
        bool committed = false;
        CHECK(wl_columnar_eval_stack_unary_begin_attempt_owned(f.frame,
            f.operation, f.operation_id, binary_owner(&f)) == EBUSY);
        CHECK(wl_columnar_eval_stack_unary_input_owned(f.frame, &input,
            binary_owner(&f)) != 0);
        CHECK(wl_columnar_eval_stack_unary_output_owned(f.frame, &output,
            binary_owner(&f)) != 0);
        CHECK(wl_columnar_eval_stack_unary_commit_attempt_owned(f.frame,
            &committed, binary_owner(&f)) != 0 && !committed);
        CHECK(wl_columnar_eval_stack_unary_finish(f.frame, 0) != 0);
        CHECK(wl_columnar_eval_stack_unary_reset_owned(f.frame,
            binary_owner(&f)) != 0);
        CHECK(wl_columnar_eval_stack_cleanup_finish(&f.frame) == EBUSY);
        eval_stack_t *stack = NULL;
        CHECK(wl_columnar_eval_stack_operator_stack_owned(f.frame, f.session,
            &stack, binary_owner(&f)) == EBUSY);
        wl_columnar_filter_context_t *ctx = NULL;
        CHECK(!wl_columnar_eval_stack_filter_context_owned(f.frame, &ctx,
            binary_owner(&f)) && ctx);
        CHECK(!wl_columnar_filter_op_attempt_ack_owned(f.frame,
            binary_owner(&f)));
        CHECK(wl_columnar_eval_stack_binary_begin_owned(f.frame, f.session,
            f.operation, f.operation_id, binary_owner(&f)) != 0);
        binary_assert_originals(&f);
        binary_run(&f); binary_fixture_close(&f, false);
    }
}

static unsigned binary_cancel_phase;
static void
binary_cancel_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)progress;
    if (phase == binary_cancel_phase)
        wl_evaluation_control_request_cancel(control);
}

static void
binary_test_cutoff_and_limits(void)
{
    /* Literal source-written schedules, independent of production constants:
     * begin=256+192+2*(192+2*128)+20*192+192=5376.
     * commit=192+2*128+192=640. No runtime hook derives these fees. */
    const uint64_t limits[] = {255, 256, 5375, 5376, 6015, 6016};
    const uint64_t consumed[] = {0, 256, 5184, 5376, 5824, 6016};
    for (unsigned i = 0; i < 6; ++i) {
        binary_fixture_t f; binary_fixture_init(&f, 2, false, false, limits[i]);
        int rc = wl_columnar_eval_stack_binary_begin_owned(f.frame, f.session,
                f.operation, f.operation_id, NULL);
        bool began = i >= 3, committed = false;
        CHECK(rc == (began ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT));
        if (began) {
            if (i >= 4) {
                col_rel_t **slot = NULL;
                CHECK(!wl_columnar_eval_stack_binary_output_owned(f.frame,
                    &slot, NULL));
                *slot = wl_columnar_filter_test_source(1, 1, false, false,
                        false);
                rc = wl_columnar_eval_stack_binary_commit_owned(f.frame,
                        &committed, NULL);
                CHECK(rc == (i == 5 ? 0 : WL_EVALUATION_CONTROL_WORK_LIMIT));
                CHECK(committed == (i == 5));
            }
            f.execution_result = rc;
            binary_run(&f);
        }
        if (!committed) binary_assert_originals(&f);
        CHECK(wl_evaluation_control_finish(f.operation, &f.operation_owner,
            rc, 0) == rc);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.operation, &report));
        CHECK(report.attempt_id == f.operation_id &&
            report.consumed == consumed[i]);
        binary_fixture_close(&f, committed);
    }
    for (unsigned phase = 3; phase <= 7; ++phase) {
        binary_fixture_t f; binary_fixture_init(&f, 2, false, false, 0);
        binary_cancel_phase = phase;
        wl_columnar_eval_stack_test_binary_checkpoint =
            binary_cancel_checkpoint;
        int rc = wl_columnar_eval_stack_binary_begin_owned(f.frame, f.session,
                f.operation, f.operation_id, NULL);
        if (phase < 6) CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED);
        else {
            CHECK(!rc); col_rel_t **slot = NULL;
            CHECK(!wl_columnar_eval_stack_binary_output_owned(f.frame,
                &slot, NULL));
            *slot = wl_columnar_filter_test_source(1, 1, false, false, false);
            bool committed = false;
            rc = wl_columnar_eval_stack_binary_commit_owned(f.frame,
                    &committed, NULL);
            CHECK(rc == WL_EVALUATION_CONTROL_CANCELLED && !committed);
            wl_columnar_eval_stack_test_binary_checkpoint = NULL;
            f.execution_result = rc; binary_run(&f);
        }
        wl_columnar_eval_stack_test_binary_checkpoint = NULL;
        binary_assert_originals(&f); binary_fixture_close(&f, false);
    }
}

static void
binary_test_aliases(void)
{
    for (unsigned shared = 0; shared < 3; ++shared) {
        binary_fixture_t f; binary_fixture_init(&f, 3, false, false, 0);
        eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(f.frame);
        if (shared == 0 || shared == 1) {
            free(stack->items[2].seg_boundaries);
            stack->items[2].seg_boundaries =
                stack->items[shared].seg_boundaries;
            f.before[2] = stack->items[2];
        } else {
            col_rel_destroy(f.inputs[2]); f.inputs[2] = NULL;
            stack->items[2].rel = stack->items[1].rel;
            f.before[2] = stack->items[2];
        }
        int rc = wl_columnar_eval_stack_binary_begin_owned(f.frame, f.session,
                f.operation, f.operation_id, NULL);
        if (shared < 2) {
            CHECK(rc == ENOTSUP); binary_assert_originals(&f);
            stack->items[2].seg_boundaries = NULL;
        } else {
            CHECK(!rc); binary_run(&f); binary_assert_originals(&f);
        }
        binary_fixture_close(&f, false);
    }
    for (unsigned commit = 0; commit < 2; ++commit) {
        binary_fixture_t f; binary_fixture_init(&f, 2, false, false, 0);
        CHECK(!wl_columnar_eval_stack_binary_begin_owned(f.frame, f.session,
            f.operation, f.operation_id, NULL));
        col_rel_t **slot = NULL;
        CHECK(!wl_columnar_eval_stack_binary_output_owned(f.frame, &slot,
            NULL));
        *slot = f.inputs[0];
        if (commit) {
            bool published = false;
            CHECK(wl_columnar_eval_stack_binary_commit_owned(f.frame,
                &published, NULL) == EINVAL && !published);
        }
        binary_run(&f);
        CHECK(f.status.semantic_result == EINVAL && !f.status.committed);
        binary_assert_originals(&f); binary_fixture_close(&f, false);
    }
}

static void
binary_test_retained(void)
{
    for (unsigned mask = 1; mask < 4; ++mask) {
        binary_fixture_t f; binary_fixture_init(&f, 2, true, true, 0);
        wl_columnar_source_access_reader_t readers[2] = {{0}, {0}};
        for (unsigned i = 0; i < 2; ++i)
            if (mask & (1u <<
                i)) CHECK(!wl_columnar_source_access_reader_acquire(
                        &f.inputs[i]->source_access, &readers[i]));
        CHECK(!wl_columnar_eval_stack_binary_begin_owned(f.frame, f.session,
            f.operation, f.operation_id, binary_owner(&f)));
        col_rel_t **slot = NULL;
        CHECK(!wl_columnar_eval_stack_binary_output_owned(f.frame, &slot,
            binary_owner(&f)));
        col_rel_t *out = wl_columnar_filter_test_source(1, 1, false, false,
                false);
        *slot = out; bool committed = false;
        CHECK(!wl_columnar_eval_stack_binary_commit_owned(f.frame, &committed,
            binary_owner(&f)) && committed);
        uint64_t generation = out->storage_generation;
        for (unsigned i = 0; i < 2; ++i) {
            CHECK(!binary_host_finish(&f));
            if (mask & (1u << i)) {
                CHECK(f.status.progress == WL_COLUMNAR_FILTER_PROGRESS_RETAINED
                    && f.status.cleanup_result == EBUSY &&
                    f.status.semantic_result == EBUSY && f.status.committed);
                CHECK(!binary_host_finish(&f));
                CHECK(f.status.progress ==
                    WL_COLUMNAR_FILTER_PROGRESS_RETAINED);
                CHECK(out->storage_generation == generation &&
                    wl_columnar_eval_stack_cleanup_stack(
                        f.frame)->items[0].rel == out);
                CHECK(!wl_columnar_source_access_reader_release(&readers[i]));
                CHECK(!binary_host_finish(&f));
            }
        }
        CHECK(f.status.progress == WL_COLUMNAR_FILTER_PROGRESS_DONE &&
            !f.status.semantic_result && !f.status.cleanup_result);
        CHECK(!binary_host_ack(&f));
        binary_fixture_close(&f, true);
    }
}

static void
binary_test_claim_and_host(void)
{
    const unsigned depths[] = {1, 2, 3, COL_STACK_MAX};
    for (unsigned i = 0; i < 4; ++i) {
        binary_fixture_t f; binary_fixture_init(&f, depths[i], false, false, 0);
        const unsigned bad[] = {0, COL_STACK_MAX + 1, depths[i] == 1 ? 2 : 1};
        for (unsigned j = 0; j < 3; ++j) {
            CHECK(wl_columnar_eval_plan_numeric_stack_attempt_begin(&f.record,
                &f.plan, 0, 1, f.frame, f.session, bad[j], f.operation,
                f.operation_id, f.funding, f.funding_id) == EINVAL);
            CHECK(!f.record.identity && !f.record.control);
            binary_assert_originals(&f);
        }
        CHECK(!wl_columnar_eval_plan_numeric_stack_attempt_begin(&f.record,
            &f.plan, 0, 1, f.frame, f.session, depths[i], f.operation,
            f.operation_id, f.funding, f.funding_id));
        f.bound = true; binary_fixture_close(&f, false);
    }
    binary_fixture_t f; binary_fixture_init(&f, 2, false, true, 0);
    wl_columnar_eval_plan_numeric_attempt_t copy = f.record;
    CHECK(wl_columnar_eval_stack_binary_begin_owned(f.frame, f.session,
        f.operation, f.operation_id, &copy) != 0);
    CHECK(wl_columnar_eval_stack_binary_begin_owned(f.frame, f.session,
        f.funding, f.funding_id, &f.record) == EINVAL);
    CHECK(!wl_columnar_eval_stack_binary_begin_owned(f.frame, f.session,
        f.operation, f.operation_id, &f.record));
    wl_evaluation_control_t *saved = f.funding;
    uint64_t saved_id = f.funding_id;
    for (unsigned allowance = 383; allowance <= 384; ++allowance) {
        int owner;
        f.funding = wl_columnar_filter_test_control(allowance, &owner);
        f.funding_id = wl_evaluation_control_active_attempt_id(f.funding);
        memset(&f.status, 0x5a, sizeof(f.status));
        wl_columnar_filter_status_t sentinel = f.status;
        CHECK(binary_host_finish(&f) == (allowance == 383
                ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        if (allowance == 383) CHECK(!memcmp(&sentinel, &f.status,
                sizeof(sentinel)));
        else CHECK(f.status.progress == WL_COLUMNAR_FILTER_PROGRESS_DONE);
        CHECK(wl_evaluation_control_finish(f.funding, &owner, 0, 0) ==
            (allowance == 383 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.funding, &report) &&
            report.consumed == (allowance == 383 ? 0u : 384u));
        CHECK(!wl_evaluation_control_detach(f.funding, &owner));
        wl_evaluation_control_release(f.funding);
    }
    f.funding = saved; f.funding_id = saved_id;
    CHECK(!binary_host_ack(&f)); binary_fixture_close(&f, false);
}

static void
test_binary_replacement(void)
{
    binary_test_transactions(); binary_test_gate();
    binary_test_cutoff_and_limits(); binary_test_aliases();
    binary_test_retained(); binary_test_claim_and_host();
}

static void *
binary_foreign_thread(void *arg)
{
    binary_fixture_t *f = arg;
    const eval_entry_t *input = NULL;
    CHECK(wl_columnar_eval_stack_binary_input_owned(f->frame, 0, &input,
        binary_owner(f)) != 0 && !input);
    CHECK(binary_host_finish(f) != 0);
    CHECK(binary_host_ack(f) != 0);
    return NULL;
}

static void
binary_test_native_and_lifo(void)
{
    for (unsigned bound = 0; bound < 2; ++bound) {
        binary_fixture_t f; binary_fixture_init(&f, 2, false, bound, 0);
        CHECK(!wl_columnar_eval_stack_binary_begin_owned(f.frame, f.session,
            f.operation, f.operation_id, binary_owner(&f)));
        wl_thread_t thread;
        CHECK(!wl_thread_create(&thread, binary_foreign_thread, &f));
        CHECK(!wl_thread_join(&thread));
        binary_assert_originals(&f);
        wl_columnar_eval_stack_cleanup_frame_t *inner = NULL;
        CHECK(!wl_columnar_eval_stack_cleanup_begin(f.session, &inner));
        CHECK(binary_host_finish(&f) != 0);
        CHECK(binary_host_ack(&f) != 0);
        CHECK(!wl_columnar_eval_stack_cleanup_finish(&inner));
        binary_assert_originals(&f); binary_run(&f);
        binary_fixture_close(&f, false);
    }
}

static void
binary_test_turnover(void)
{
    binary_fixture_t f; binary_fixture_init(&f, 2, false, true, 0);
    CHECK(!wl_columnar_eval_stack_binary_begin_owned(f.frame, f.session,
        f.operation, f.operation_id, binary_owner(&f)));
    col_rel_t **slot = NULL;
    CHECK(!wl_columnar_eval_stack_binary_output_owned(f.frame, &slot,
        binary_owner(&f)));
    *slot = wl_columnar_filter_test_source(1, 1, false, false, false);
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    CHECK(!wl_evaluation_control_reset(f.operation));
    CHECK(!wl_evaluation_control_begin(f.operation, &f.operation_owner));
    binary_run(&f);
    binary_assert_originals(&f);
    CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.operation, &report));
    CHECK(report.attempt_id == f.operation_id + 1 && !report.consumed);
    binary_fixture_close(&f, false);
}

/* CONCAT's actual host adapter returns directly from one child. Status is
 * consumed in a later action; each action gets a newly begun funding owner. */
typedef struct {
    binary_fixture_t binary;
    unsigned phase, child_calls, consumes;
    bool pending;
} concat_fixture_t;

static col_rel_t *
concat_source(uint32_t width, uint32_t rows, int type, unsigned side)
{
    col_rel_t *rel = wl_columnar_filter_test_source(width, 0, false, false,
            true);
    if (type >= 0 && width) {
        wirelog_column_type_t *types = malloc(width * sizeof(*types));
        CHECK(types);
        for (uint32_t c = 0; c < width;
            ++c) types[c] = (wirelog_column_type_t)type;
        CHECK(!col_rel_set_column_types(rel, types, width)); free(types);
    }
    int64_t *values = malloc((width ? width : 1) * sizeof(*values));
    CHECK(values);
    for (uint32_t row = 0; row < rows; ++row) {
        for (uint32_t c = 0; c < width; ++c) {
            int64_t value = (int64_t)row - 3 + 20 * side + 11 * c;
            values[c] = type == WIRELOG_TYPE_FLOAT
                ? wl_columnar_float_to_bits(!row && !c ? -0.0 : (double)value)
                : value;
        }
        CHECK(!col_rel_append_row(rel, values));
    }
    free(values);
    /* Legacy valid -0 source bits deliberately survive until controlled copy. */
    if (width && rows && type == WIRELOG_TYPE_FLOAT)
        rel->columns[0][0] = (int64_t)UINT64_C(0x8000000000000000);
    if (width && !side) {
        rel->has_graph_column = true; rel->graph_col_idx = width - 1;
        rel->declared_ncols = width + 2;
    }
    return rel;
}

static void
concat_fixture_init(concat_fixture_t *f, uint32_t width, uint32_t left_rows,
    uint32_t right_rows, int left_type, int right_type, bool owned, bool bound,
    uint64_t limit)
{
    memset(f, 0, sizeof(*f));
    binary_fixture_t *b = &f->binary;
    binary_fixture_init(b, 3, owned, false, limit);
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(b->frame);
    for (unsigned side = 0; side < 2; ++side) {
        unsigned i = side + 1;
        col_rel_destroy(b->inputs[i]);
        b->inputs[i] = concat_source(width, side ? right_rows : left_rows,
                side ? right_type : left_type, side);
        stack->items[i].rel = b->inputs[i];
        stack->items[i].seg_boundaries[1] = side ? right_rows : left_rows;
        b->before[i] = stack->items[i];
    }
    if (bound) {
        b->bound = true;
        CHECK(!wl_columnar_eval_plan_numeric_stack_attempt_begin(&b->record,
            &b->plan, 0, 1, b->frame, b->session, 3, b->operation,
            b->operation_id, b->funding, b->funding_id));
    }
}

static int
concat_engine_dispatch(concat_fixture_t *f, wl_evaluation_control_t *funding,
    uint64_t id)
{
    binary_fixture_t *b = &f->binary;
    if (wl_evaluation_control_active_attempt_id(funding) != id) return EINVAL;
    int rc = wl_evaluation_control_charge(funding, 384);
    if (rc) return rc;
    if (f->pending) {
        ++f->consumes; f->pending = false;
        f->phase = f->phase == 2 ? 3
            : b->status.progress == WL_COLUMNAR_FILTER_PROGRESS_DONE ? 2 : 1;
        return 0;
    }
    f->pending = true; ++f->child_calls;
    if (!f->phase)
        return wl_columnar_merge_concat_attempt_start_owned(&b->op, b->frame,
                   b->session, b->operation, b->operation_id, &b->status,
                   funding, id, binary_owner(b));
    if (f->phase == 1)
        return wl_columnar_merge_concat_attempt_step_owned(b->frame, &b->status,
                   funding, id, binary_owner(b));
    return wl_columnar_merge_concat_attempt_ack_owned(b->frame, funding, id,
               binary_owner(b));
}

static int
concat_host_turn(concat_fixture_t *f)
{
    int owner = 0;
    wl_evaluation_control_t *funding = wl_columnar_filter_test_control(0,
            &owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(funding);
    int rc = concat_engine_dispatch(f, funding, id);
    CHECK(!wl_evaluation_control_finish(funding, &owner, 0, 0));
    CHECK(!wl_evaluation_control_detach(funding, &owner));
    wl_evaluation_control_release(funding);
    return rc;
}

static void
concat_run(concat_fixture_t *f)
{
    for (unsigned turns = 0; f->phase != 3; ++turns)
        CHECK(turns < 1000 && !concat_host_turn(f));
    CHECK(f->child_calls == f->consumes);
}

static void
concat_literal_result(concat_fixture_t *f, uint32_t width, uint32_t left_rows,
    uint32_t right_rows, int left_type, int right_type)
{
    binary_fixture_t *b = &f->binary;
    eval_stack_t *stack = wl_columnar_eval_stack_cleanup_stack(b->frame);
    CHECK(stack->top == 2 && binary_test_entry_equal(&stack->items[0],
        &b->before[0]));
    eval_entry_t *entry = &stack->items[1];
    col_rel_t *out = entry->rel;
    CHECK(entry->owned && !entry->is_delta && !entry->seg_boundaries &&
        !entry->seg_count);
    CHECK(out && !strcmp(out->name,
        "$concat") && out->schema_ok && !out->timestamps);
    CHECK(out->ncols == width && out->nrows == left_rows + right_rows);
    CHECK(out->has_graph_column == (width != 0));
    if (width) CHECK(out->graph_col_idx == width - 1 &&
            out->declared_ncols == width + 2);
    int type = left_type >= 0 ? left_type : right_rows ? right_type : -1;
    CHECK((out->column_types != NULL) == (width && type >= 0));
    for (uint32_t c = 0; c < width; ++c) {
        char name[96]; snprintf(name, sizeof(name),
            "long_column_%u_with_several_eight_byte_chunks", c);
        CHECK(!strcmp(out->col_names[c], name));
        if (type >=
            0) CHECK(out->column_types[c] == (wirelog_column_type_t)type);
        for (unsigned side = 0; side < 2; ++side)
            for (uint32_t row = 0; row < (side ? right_rows : left_rows);
                ++row){
                int64_t expected = (int64_t)row - 3 + 20 * side + 11 * c;
                if (type == WIRELOG_TYPE_FLOAT)
                    expected = wl_columnar_float_to_bits(!row &&
                            !c ? 0.0 : (double)expected);
                CHECK(out->columns[c][(side ? left_rows : 0) + row] ==
                    expected);
            }
    }
}

static void
concat_parity_case(uint32_t width, uint32_t left_rows, uint32_t right_rows,
    int left_type, int right_type, bool owned, bool bound)
{
    concat_fixture_t f;
    concat_fixture_init(&f, width, left_rows, right_rows, left_type, right_type,
        owned, bound, 0);
    /* Independent relations and raw operator; no controlled result as oracle. */
    col_rel_t *a = concat_source(width, left_rows, left_type, 0);
    col_rel_t *b = concat_source(width, right_rows, right_type, 1);
    eval_stack_t raw = {0};
    CHECK(!eval_stack_push(&raw, a, false) && !eval_stack_push(&raw, b, false));
    int expected = col_op_concat(&raw, f.binary.session);
    concat_run(&f);
    CHECK(f.binary.status.semantic_result == expected);
    if (!expected) {
        CHECK(f.binary.status.committed && !f.binary.status.cleanup_result);
        concat_literal_result(&f, width, left_rows, right_rows, left_type,
            right_type);
        CHECK(raw.top == 1);
        wl_columnar_filter_test_equal(wl_columnar_eval_stack_cleanup_stack(
                f.binary.frame)->items[1].rel, raw.items[0].rel);
    } else {
        CHECK(!f.binary.status.committed);
        binary_assert_originals(&f.binary);
    }
    CHECK(!eval_stack_drain(&raw));
    col_rel_destroy(a); col_rel_destroy(b);
    binary_fixture_close(&f.binary, !expected);
}

static void
concat_literal_matrix(void)
{
    const uint32_t widths[] = { 0, 1, 17, 129 };
    for (unsigned w = 0; w < 4; ++w)
        for (unsigned empty = 0; empty < 4; ++empty)
            for (unsigned owned = 0; owned < 2; ++owned)
                for (unsigned bound = 0; bound < 2; ++bound)
                    concat_parity_case(widths[w], empty & 1 ? 0 : 3,
                        empty & 2 ? 0 : 2, -1, -1, owned, bound);
    for (int left = -1; left <= WIRELOG_TYPE_BOOL; ++left)
        for (int right = -1; right <= WIRELOG_TYPE_BOOL; ++right)
            for (unsigned empty = 0; empty < 4; ++empty)
                concat_parity_case(1, empty & 1 ? 0 : 3, empty & 2 ? 0 : 2,
                    left, right, (left + 1) % 2, (right + 1) % 2);
}

static unsigned concat_stop_phase;
static bool concat_stop_hit;
static void
concat_cancel_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t progress)
{
    (void)progress;
    if (phase == concat_stop_phase && !concat_stop_hit) {
        concat_stop_hit = true; wl_evaluation_control_request_cancel(control);
    }
}

static void
concat_stop_and_failure(void)
{
    const unsigned phases[] = {1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 20, 21, 22,
                               23, 24, 25, 26, 27};
    for (unsigned i = 0; i < sizeof(phases)/sizeof(phases[0]); ++i) {
        concat_fixture_t f; concat_fixture_init(&f, 129, 3, 2,
            WIRELOG_TYPE_FLOAT, WIRELOG_TYPE_FLOAT, true, true, 0);
        concat_stop_phase = phases[i]; concat_stop_hit = false;
        wl_columnar_merge_test_concat_checkpoint = concat_cancel_checkpoint;
        concat_run(&f); wl_columnar_merge_test_concat_checkpoint = NULL;
        CHECK(concat_stop_hit && !f.binary.status.committed &&
            f.binary.status.semantic_result == WL_EVALUATION_CONTROL_CANCELLED);
        binary_assert_originals(&f.binary);
        binary_fixture_close(&f.binary, false);
    }
    for (unsigned side = 0; side < 2; ++side)
        for (unsigned invalid = 0; invalid < 2; ++invalid) {
            concat_fixture_t f; concat_fixture_init(&f, 1, 3, 2,
                WIRELOG_TYPE_FLOAT, WIRELOG_TYPE_FLOAT, true, false, 0);
            f.binary.inputs[side + 1]->columns[0][1] = (int64_t)(invalid
                ? UINT64_C(0x7ff8000000000001) : UINT64_C(0xfff0000000000000));
            concat_run(&f);
            CHECK(f.binary.status.semantic_result == EINVAL &&
                !f.binary.status.committed);
            binary_assert_originals(&f.binary);
            binary_fixture_close(&f.binary, false);
        }
    /* Named source schedule: bootstrap8*192=1536; Adepth3 begin5632
     * reaches7168 (last192 starts6976); resource100*192 reaches26368
     * (last192 starts26176). Each pair brackets an actual admission. */
    const uint64_t limits[] = {191, 192, 1535, 1536, 1791, 1792, 7167, 7168,
                               7359, 7360, 26367, 26368};
    const uint64_t consumed[] = {0, 192, 1344, 1536, 1536, 1792, 6976, 7168,
                                 7168, 7360, 26176, 26368};
    for (unsigned i = 0; i < sizeof(limits)/sizeof(limits[0]); ++i) {
        concat_fixture_t f;
        concat_fixture_init(&f, 17, 3, 2, -1, -1, false, false, limits[i]);
        concat_run(&f);
        CHECK(!f.binary.status.committed &&
            f.binary.status.semantic_result ==
            WL_EVALUATION_CONTROL_WORK_LIMIT);
        binary_assert_originals(&f.binary);
        CHECK(wl_evaluation_control_finish(f.binary.operation,
            &f.binary.operation_owner,
            0, 0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.binary.operation, &report));
        CHECK(report.attempt_id == f.binary.operation_id &&
            report.consumed == consumed[i]);
        binary_fixture_close(&f.binary, false);
    }
    concat_fixture_t f;
    concat_fixture_init(&f, 17, 3, 2, -1, -1, true, true, 0);
    wl_columnar_merge_test_concat_fail_allocation = 0;
    concat_run(&f); wl_columnar_merge_test_concat_fail_allocation = -1;
    CHECK(!f.binary.status.committed &&
        f.binary.status.semantic_result == ENOMEM);
    binary_assert_originals(&f.binary); binary_fixture_close(&f.binary, false);
}

static void
concat_retained_and_turnover(void)
{
    for (unsigned mask = 1; mask < 4; ++mask) {
        concat_fixture_t f;
        concat_fixture_init(&f, 17, 3, 2, -1, -1, true, true, 0);
        wl_columnar_source_access_reader_t readers[2] = {{0}, {0}};
        for (unsigned side = 0; side < 2; ++side)
            if (mask & (1u <<
                side)) CHECK(!wl_columnar_source_access_reader_acquire(
                        &f.binary.inputs[side + 1]->source_access,
                        &readers[side]));
        unsigned released = 0;
        col_rel_t *published = NULL;
        for (unsigned turns = 0; f.phase != 3; ++turns) {
            CHECK(turns < 1000 && !concat_host_turn(&f));
            if (f.binary.status.progress !=
                WL_COLUMNAR_FILTER_PROGRESS_RETAINED) continue;
            CHECK(f.binary.status.committed &&
                f.binary.status.semantic_result == EBUSY);
            col_rel_t *out =
                wl_columnar_eval_stack_cleanup_stack(
                f.binary.frame)->items[1].rel;
            CHECK(!published || published == out); published = out;
            if (f.pending) continue;
            for (unsigned side = 0; side < 2; ++side)
                if ((mask & (1u << side)) && !(released & (1u << side))) {
                    CHECK(!wl_columnar_source_access_reader_release(
                            &readers[side]));
                    released |= 1u << side; break;
                }
        }
        CHECK(released == mask && !f.binary.status.semantic_result);
        concat_literal_result(&f, 17, 3, 2, -1, -1);
        binary_fixture_close(&f.binary, true);
    }
    concat_fixture_t f;
    concat_fixture_init(&f, 17, 3, 2, -1, -1, false, false, 0);
    CHECK(!concat_host_turn(&f));
    CHECK(!wl_evaluation_control_finish(f.binary.operation,
        &f.binary.operation_owner, 0, 0));
    CHECK(!wl_evaluation_control_reset(f.binary.operation));
    CHECK(!wl_evaluation_control_begin(f.binary.operation,
        &f.binary.operation_owner));
    concat_run(&f);
    CHECK(!f.binary.status.committed &&
        f.binary.status.semantic_result == EINVAL);
    binary_assert_originals(&f.binary);
    CHECK(!wl_evaluation_control_finish(f.binary.operation,
        &f.binary.operation_owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.binary.operation,
        &report) && !report.consumed);
    binary_fixture_close(&f.binary, false);
}

static void
concat_resource_and_compatibility(void)
{
    /* Same borrowed descriptor twice, with independently owned segment arrays. */
    concat_fixture_t same;
    concat_fixture_init(&same, 1, 3, 3, -1, -1, false, false, 0);
    eval_stack_t *stack =
        wl_columnar_eval_stack_cleanup_stack(same.binary.frame);
    col_rel_destroy(same.binary.inputs[2]);
    same.binary.inputs[2] = same.binary.inputs[1];
    stack->items[2].rel = same.binary.inputs[1];
    same.binary.before[2] = stack->items[2];
    concat_run(&same);
    CHECK(!same.binary.status.semantic_result && same.binary.status.committed);
    col_rel_t *out = stack->items[1].rel;
    CHECK(out->nrows == 6);
    for (unsigned row = 0; row < 6;
        ++row) CHECK(out->columns[0][row] == (int64_t)(row % 3) - 3);
    /* Frame entries have retired both arrays; only one borrowed descriptor. */
    same.binary.inputs[2] = NULL;
    binary_fixture_close(&same.binary, true);

    concat_fixture_t f;
    concat_fixture_init(&f, 17, 3, 2, -1, -1, true, true, 0);
    wl_columnar_merge_concat_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_concat_context_owned(f.binary.frame, &ctx,
        binary_owner(&f.binary)));
    while (ctx->phase != 6 || f.pending) CHECK(!concat_host_turn(&f));
    CHECK(ctx->scratch.admitted && ctx->scratch.ref_held &&
        !ctx->scratch.physical);
    uint64_t reserved = wl_columnar_filter_test_reserved(f.binary.session);
    uint64_t bytes = ctx->scratch.credit.bytes;
    wl_columnar_memory_governor_test_refuse_next_release();
    CHECK(!concat_host_turn(&f));
    CHECK(f.binary.status.progress == WL_COLUMNAR_FILTER_PROGRESS_RETAINED &&
        ctx->scratch.admitted && ctx->scratch.credit.bytes == bytes &&
        wl_columnar_filter_test_reserved(f.binary.session) == reserved);
    CHECK(!concat_host_turn(&f)); /* consume retained status */
    wl_columnar_filter_status_t sentinel = f.binary.status;
    int host_owner;
    wl_evaluation_control_t *host = wl_columnar_filter_test_control(383,
            &host_owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(host);
    CHECK(wl_columnar_merge_concat_attempt_step_owned(f.binary.frame,
        &f.binary.status,
        host, id, binary_owner(&f.binary)) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!memcmp(&sentinel, &f.binary.status,
        sizeof(sentinel)) && ctx->scratch.admitted);
    CHECK(wl_evaluation_control_finish(host, &host_owner, 0,
        0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_detach(host, &host_owner));
    wl_evaluation_control_release(host);
    concat_run(&f);
    CHECK(!f.binary.status.semantic_result && f.binary.status.committed);
    binary_fixture_close(&f.binary, true);

    /* Discover reached allocation sites only, then fail every site. Fees are
    * independent source-written literals above, never measured from this. */
    unsigned counts[2];
    concat_fixture_t probe;
    concat_fixture_init(&probe, 17, 70, 71, -1, -1, true, false, 0);
    wl_columnar_relation_test_constructor_fail_allocation = UINT32_MAX;
    wl_columnar_relation_test_growth_fail_allocation = UINT32_MAX;
    concat_run(&probe);
    counts[0] = UINT32_MAX -
        wl_columnar_relation_test_constructor_fail_allocation;
    counts[1] = UINT32_MAX - wl_columnar_relation_test_growth_fail_allocation;
    wl_columnar_relation_test_constructor_fail_allocation = 0;
    wl_columnar_relation_test_growth_fail_allocation = 0;
    CHECK(counts[0] && counts[1] && probe.binary.status.committed);
    binary_fixture_close(&probe.binary, true);
    for (unsigned family = 0; family < 2; ++family)
        for (unsigned ordinal = 1; ordinal <= counts[family]; ++ordinal) {
            concat_fixture_t a;
            concat_fixture_init(&a, 17, 70, 71, -1, -1, true, false, 0);
            if (!family) wl_columnar_relation_test_constructor_fail_allocation =
                    ordinal;
            else wl_columnar_relation_test_growth_fail_allocation = ordinal;
            concat_run(&a);
            wl_columnar_relation_test_constructor_fail_allocation = 0;
            wl_columnar_relation_test_growth_fail_allocation = 0;
            CHECK(!a.binary.status.committed &&
                a.binary.status.semantic_result == ENOMEM);
            binary_assert_originals(&a.binary);
            binary_fixture_close(&a.binary, false);
        }
    /* Unsupported scalar dispatch domains remain explicit and preserve inputs. */
    for (unsigned mode = 0; mode < 3; ++mode) {
        concat_fixture_t a;
        concat_fixture_init(&a, 1, 3, 2, -1, -1, false, false, 0);
        if (!mode) a.binary.session->current_iteration = 1;
        else if (mode ==
            1) CHECK(!col_rel_enable_timestamps(a.binary.inputs[1]));
        else a.binary.op.materialized = true;
        concat_run(&a);
        if (mode == 1) {
            CHECK(a.binary.status.committed &&
                !a.binary.status.semantic_result);
            concat_literal_result(&a, 1, 3, 2, -1, -1);
            binary_fixture_close(&a.binary, true);
        } else {
            CHECK(!a.binary.status.committed &&
                a.binary.status.semantic_result == ENOTSUP);
            binary_assert_originals(&a.binary);
            binary_fixture_close(&a.binary, false);
        }
    }
    /* NULL operation control retains actual raw two-entry/K-copy behavior. */
    concat_fixture_t raw;
    concat_fixture_init(&raw, 1, 3, 2, -1, -1, false, false, 0);
    CHECK(!wl_columnar_merge_concat_attempt_start(&raw.binary.op,
        raw.binary.frame,
        raw.binary.session, NULL, 0, &raw.binary.status, raw.binary.funding,
        raw.binary.funding_id));
    CHECK(raw.binary.status.committed &&
        raw.binary.status.progress == WL_COLUMNAR_FILTER_PROGRESS_DONE);
    stack = wl_columnar_eval_stack_cleanup_stack(raw.binary.frame);
    CHECK(stack->top == 2 && stack->items[1].seg_count == 2);
    binary_fixture_close(&raw.binary, true);
}

static void
concat_raw_single_marker(void)
{
    binary_fixture_t f; binary_fixture_init(&f, 1, false, false, 0);
    CHECK(!wl_columnar_merge_concat_attempt_start(&f.op, f.frame, f.session,
        NULL, 0, &f.status, f.funding, f.funding_id));
    CHECK(!f.status.semantic_result &&
        f.status.progress == WL_COLUMNAR_FILTER_PROGRESS_DONE);
    binary_assert_originals(&f); binary_fixture_close(&f, true);
}

static wl_col_session_t *concat_quota_session;
static wl_columnar_memory_reservation_t *concat_quota_peer;
static bool concat_quota_hit;
static void
concat_quota_checkpoint(wl_evaluation_control_t *control, unsigned phase,
    uint64_t row)
{
    (void)control;
    if (phase != 25 || row != 63 || concat_quota_hit) return;
    concat_quota_hit = true;
    uint64_t reserved = wl_columnar_filter_test_reserved(concat_quota_session);
    CHECK(reserved < 16u * 1024u * 1024u);
    CHECK(wl_columnar_memory_reserve(wl_columnar_memory_governor_ref_get(
            concat_quota_session->memory_governor),
        16u * 1024u * 1024u - reserved, concat_quota_peer));
}

static void
concat_quota_retry(void)
{
    concat_fixture_t f;
    concat_fixture_init(&f, 17, 70, 71, -1, -1, false, false, 0);
    wl_columnar_memory_reservation_t peer;
    wl_columnar_memory_reservation_init(&peer);
    concat_quota_peer = &peer; concat_quota_session = f.binary.session;
    concat_quota_hit = false;
    wl_columnar_merge_test_concat_checkpoint = concat_quota_checkpoint;
    concat_run(&f); wl_columnar_merge_test_concat_checkpoint = NULL;
    CHECK(concat_quota_hit && f.binary.status.semantic_result == ENOSPC &&
        !f.binary.status.committed && f.binary.session->memory_budget_denied);
    binary_assert_originals(&f.binary);
    CHECK(wl_columnar_filter_test_reserved(f.binary.session) ==
        f.binary.session->cleanup_reserved_bytes + peer.bytes);
    CHECK(wl_columnar_memory_release(&peer));
    CHECK(wl_evaluation_control_finish(f.binary.operation,
        &f.binary.operation_owner,
        ENOSPC, 0) == ENOSPC);
    CHECK(!wl_evaluation_control_reset(f.binary.operation));
    CHECK(!wl_evaluation_control_begin(f.binary.operation,
        &f.binary.operation_owner));
    f.binary.operation_id =
        wl_evaluation_control_active_attempt_id(f.binary.operation);
    f.binary.session->memory_budget_denied = false;
    f.phase = 0; f.pending = false; f.child_calls = f.consumes = 0;
    concat_run(&f);
    CHECK(!f.binary.status.semantic_result && f.binary.status.committed);
    concat_literal_result(&f, 17, 70, 71, -1, -1);
    binary_fixture_close(&f.binary, true);
    concat_quota_session = NULL; concat_quota_peer = NULL;
}

static void
concat_nested_host_funding(void)
{
    concat_fixture_t f;
    concat_fixture_init(&f, 17, 3, 2, -1, -1, true, true, 0);
    wl_columnar_merge_concat_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_concat_context_owned(f.binary.frame, &ctx,
        binary_owner(&f.binary)));
    /* Actual engine refuses before pending/counters/status mutation. */
    int host_owner;
    wl_evaluation_control_t *host = wl_columnar_filter_test_control(383,
            &host_owner);
    uint64_t id = wl_evaluation_control_active_attempt_id(host);
    CHECK(concat_engine_dispatch(&f, host,
        id) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!f.pending && !f.child_calls && !ctx->identity);
    CHECK(wl_evaluation_control_finish(host, &host_owner, 0,
        0) == WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(!wl_evaluation_control_detach(host, &host_owner));
    wl_evaluation_control_release(host);
    while (ctx->phase != 10 || f.pending) CHECK(!concat_host_turn(&f));
    for (unsigned action = 0; action < 2; ++action) {
        if (action) {
            while (ctx->phase != 11 || f.pending) CHECK(!concat_host_turn(&f));
        }
        for (unsigned grant = 767; grant <= 768; ++grant) {
            wl_columnar_filter_status_t before = f.binary.status;
            host = wl_columnar_filter_test_control(grant, &host_owner);
            id = wl_evaluation_control_active_attempt_id(host);
            int rc =
                action ?
                wl_columnar_merge_concat_attempt_ack_owned(f.binary.frame,
                    host, id, binary_owner(&f.binary))
                : wl_columnar_merge_concat_attempt_step_owned(f.binary.frame,
                    &f.binary.status, host, id, binary_owner(&f.binary));
            CHECK(rc == (grant == 767 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
            if (grant == 767) CHECK(!memcmp(&before, &f.binary.status,
                    sizeof(before)));
            CHECK(wl_evaluation_control_finish(host, &host_owner, 0, 0) ==
                (grant == 767 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
            wl_evaluation_control_report_t report;
            CHECK(!wl_evaluation_control_report(host,
                &report) && report.attempt_id == id &&
                report.consumed == (grant == 767 ? 384u : 768u));
            CHECK(!wl_evaluation_control_detach(host, &host_owner));
            wl_evaluation_control_release(host);
        }
    }
    /* Direct acknowledged child is complete; no second fixture ACK. */
    CHECK(!ctx->identity && !ctx->phase);
    binary_fixture_close(&f.binary, true);
}

static concat_fixture_t *concat_turnover_fixture;
static bool concat_turnover_hit;
static void
concat_reserve_turnover(wl_evaluation_control_t *control, unsigned phase,
    uint64_t bytes)
{
    (void)bytes;
    if (phase != 27 || concat_turnover_hit) return;
    concat_turnover_hit = true;
    binary_fixture_t *b = &concat_turnover_fixture->binary;
    CHECK(control == b->operation);
    CHECK(!wl_evaluation_control_finish(control, &b->operation_owner, 0, 0));
    CHECK(!wl_evaluation_control_reset(control));
    CHECK(!wl_evaluation_control_begin(control, &b->operation_owner));
}

static void *
concat_foreign_thread(void *arg)
{
    concat_fixture_t *f = arg;
    wl_columnar_filter_status_t sentinel = {.semantic_result = 919};
    CHECK(wl_columnar_merge_concat_attempt_step_owned(f->binary.frame,
        &sentinel, f->binary.funding, f->binary.funding_id,
        binary_owner(&f->binary)) != 0 && sentinel.semantic_result == 919);
    CHECK(wl_columnar_merge_concat_attempt_ack_owned(f->binary.frame,
        f->binary.funding, f->binary.funding_id,
        binary_owner(&f->binary)) != 0);
    return NULL;
}

static void
concat_authority_and_reserve_stop(void)
{
    for (unsigned bound = 0; bound < 2; ++bound) {
        concat_fixture_t f;
        concat_fixture_init(&f, 17, 3, 2, -1, -1, true, bound, 0);
        CHECK(!concat_host_turn(&f));
        wl_thread_t thread;
        CHECK(!wl_thread_create(&thread, concat_foreign_thread, &f));
        CHECK(!wl_thread_join(&thread));
        wl_columnar_eval_stack_cleanup_frame_t *inner = NULL;
        CHECK(!wl_columnar_eval_stack_cleanup_begin(f.binary.session, &inner));
        wl_columnar_filter_status_t sentinel = {.semantic_result = 919};
        CHECK(wl_columnar_merge_concat_attempt_step_owned(f.binary.frame,
            &sentinel, f.binary.funding, f.binary.funding_id,
            binary_owner(&f.binary)) != 0 && sentinel.semantic_result == 919);
        CHECK(!wl_columnar_eval_stack_cleanup_finish(&inner));
        concat_run(&f);
        CHECK(f.binary.status.committed && !f.binary.status.semantic_result);
        binary_fixture_close(&f.binary, true);
    }
    concat_fixture_t f;
    concat_fixture_init(&f, 129, 3, 2, -1, -1, true, false, 0);
    concat_turnover_fixture = &f; concat_turnover_hit = false;
    wl_columnar_merge_test_concat_checkpoint = concat_reserve_turnover;
    concat_run(&f); wl_columnar_merge_test_concat_checkpoint = NULL;
    CHECK(concat_turnover_hit && !f.binary.status.committed &&
        f.binary.status.semantic_result == EINVAL);
    binary_assert_originals(&f.binary);
    CHECK(wl_columnar_filter_test_reserved(f.binary.session) ==
        f.binary.session->cleanup_reserved_bytes);
    CHECK(!wl_evaluation_control_finish(f.binary.operation,
        &f.binary.operation_owner, 0, 0));
    wl_evaluation_control_report_t report;
    CHECK(!wl_evaluation_control_report(f.binary.operation, &report) &&
        report.attempt_id == f.binary.operation_id + 1 && !report.consumed);
    binary_fixture_close(&f.binary, false);
    concat_turnover_fixture = NULL;
}

static void
concat_inert_ack_funding(void)
{
    /* Genuine source owner claims an empty frame without operation debit.
     * No source forward action occurs and no numeric phase is forged. */
    binary_fixture_t f; binary_fixture_init(&f, 0, false, false, 0);
    wl_plan_op_t ops[] = {
        {.op = WL_PLAN_OP_VARIABLE, .relation_name = "unused"},
        {.op = WL_PLAN_OP_MAP}
    };
    f.plan.ops = ops; f.plan.op_count = 2;
    CHECK(!wl_columnar_eval_plan_numeric_source_attempt_begin(&f.record,
        &f.plan, 0, 2, f.frame, f.session, f.operation, f.operation_id,
        f.funding, f.funding_id));
    f.bound = true;
    wl_columnar_merge_concat_context_t *ctx = NULL;
    CHECK(!wl_columnar_eval_stack_concat_context_owned(f.frame, &ctx,
        &f.record));
    for (unsigned generation = 0; generation < 2; ++generation) {
        uint64_t id = wl_evaluation_control_active_attempt_id(f.operation);
        CHECK(id == f.operation_id + generation);
        CHECK(wl_columnar_merge_concat_attempt_ack_owned(f.frame,
            f.operation, id, &f.record) == EINVAL);
        CHECK(!ctx->identity && !ctx->phase &&
            f.record.phase == WL_COLUMNAR_EVAL_PLAN_NUMERIC_SOURCE_PREFLIGHT &&
            !wl_columnar_eval_stack_cleanup_stack(f.frame)->top);
        CHECK(!wl_evaluation_control_finish(f.operation, &f.operation_owner, 0,
            0));
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(f.operation, &report) &&
            report.attempt_id == id && !report.consumed);
        if (!generation) {
            CHECK(!wl_evaluation_control_reset(f.operation));
            CHECK(!wl_evaluation_control_begin(f.operation,
                &f.operation_owner));
        }
    }
    for (unsigned grant = 383; grant <= 384; ++grant) {
        int host_owner;
        wl_evaluation_control_t *host = wl_columnar_filter_test_control(grant,
                &host_owner);
        uint64_t id = wl_evaluation_control_active_attempt_id(host);
        CHECK(wl_columnar_merge_concat_attempt_ack_owned(f.frame, host, id,
            &f.record) ==
            (grant == 383 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        CHECK(!ctx->identity && !ctx->phase);
        CHECK(wl_evaluation_control_finish(host, &host_owner, 0, 0) ==
            (grant == 383 ? WL_EVALUATION_CONTROL_WORK_LIMIT : 0));
        wl_evaluation_control_report_t report;
        CHECK(!wl_evaluation_control_report(host,
            &report) && report.attempt_id == id &&
            report.consumed == (grant == 383 ? 0u : 384u));
        CHECK(!wl_evaluation_control_detach(host, &host_owner));
        wl_evaluation_control_release(host);
    }
    binary_fixture_close(&f, false);
}

int
main(void)
{
    concat_inert_ack_funding();
    concat_raw_single_marker(); concat_authority_and_reserve_stop();
    concat_quota_retry(); concat_nested_host_funding();
    concat_literal_matrix(); concat_stop_and_failure();
    concat_retained_and_turnover(); concat_resource_and_compatibility();
    test_binary_replacement(); binary_test_native_and_lifo();
    binary_test_turnover();
    reduce_literal_matrix(); reduce_error_order(); reduce_stop_matrix();
    reduce_collisions(); reduce_owned_authority();
    reduce_allocation_and_domains(); reduce_retained_resources();
    reduce_shape_and_expression(); reduce_wide_and_float_overflow();
    reduce_group_native_types(); reduce_entry_limits(); reduce_quota_retry();
    reduce_all_reached_allocations();
    reduce_expression_stops_and_ignored_count(); reduce_engine_host_refusal();
    raw_count_result_domains(); raw_count_generated_bodies();
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
    const uint32_t physical_widths[] = {0, 3, 17};
    for (unsigned w = 0; w < 3; ++w)
        for (unsigned selection = 1; selection <= 3; ++selection) {
            if (!physical_widths[w] && selection == 2) continue;
            wl_columnar_filter_test_physical_pattern = selection;
            wl_columnar_filter_test_parity(physical_widths[w], 14, false,
                true, false, selection == 1 ? &yes
                : selection == 2 ? &mixed : &no, 0, false);
        }
    wl_columnar_filter_test_physical_pattern = 0;
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
    CHECK(shared.consumed == same.total + 128 + 16u * 512u + 48u * 291u);
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
    wl_columnar_filter_test_busy_finish(false);
    wl_columnar_filter_test_busy_finish(true);
    wl_columnar_filter_test_credit_retry(false);
    wl_columnar_filter_test_credit_retry(true);
    wl_columnar_filter_test_host_credit_boundaries();
    wl_columnar_filter_test_peer_collision();
    wl_columnar_filter_test_unsupported();
    wl_columnar_filter_test_nonfinite(&mixed, false, false);
    wl_columnar_filter_test_nonfinite(&mixed, false, true);
    wl_columnar_filter_test_nonfinite(&floating, true, false);
    wl_columnar_filter_test_nonfinite(&floating, true, true);
    wl_columnar_filter_test_faults(1, 1, true, &yes, true);
    wl_columnar_filter_test_faults(17, 133, true, &yes, false);
    wl_columnar_filter_test_allocations(&yes);
    for (unsigned computed = 0; computed < 2; ++computed) {
        map_schema_types(computed != 0, 0);
        map_schema_types(computed != 0, 3);
    }
    map_omitted_nonfinite(); map_unresolved_schema(); map_codes_constructor();
    map_schema_error_precedence();
    const uint32_t map_widths[] = {0, 1, 17, 37, 129};
    for (unsigned i = 0; i < 5; ++i) {
        uint32_t width = map_widths[i];
        wl_columnar_filter_test_map_case(width, 0, width, 0, width + 2, false,
            0, 0);
        wl_columnar_filter_test_map_case(width, 3, width, 0, width, true, 0, 0);
        wl_columnar_filter_test_map_case(width, 133, width + 1, 1,
            width / 2 + 1, false, 0, 0);
        wl_columnar_filter_test_map_case(width, 3, width, 2, width + 2, true, 0,
            0);
    }
    wl_columnar_filter_test_map_case(17, 270, 37, 5, 37, true, 0, 0);
    wl_columnar_filter_test_map_case(1, 3, 2, 6, 2, false, 0, ERANGE);
    wl_columnar_filter_test_map_case(1, 3, 2, 3, 2, false, 0, ERANGE);
    wl_columnar_filter_test_map_case(1, 0, 2, 4, 2, false, 0, ENOTSUP);
    wl_columnar_filter_test_map_case(1, 3, 2, 4, 2, false, 0, ENOTSUP);
    wl_evaluation_control_report_t map_first = wl_columnar_filter_test_map_case(
        1, 1, 2, 1, 2, false, 1, WL_EVALUATION_CONTROL_WORK_LIMIT);
    CHECK(map_first.consumed == 0);
    wl_columnar_filter_test_map_budget(0, 0, 0, false, true);
    wl_columnar_filter_test_map_budget(1, 1, 2, true, true);
    wl_columnar_filter_test_map_budget(17, 133, 37, true, false);
    wl_columnar_filter_test_map_faults(1, 1);
    wl_columnar_filter_test_map_faults(17, 133);
    wl_columnar_filter_test_map_allocations();
    wl_columnar_filter_test_map_error_priority();
    wl_columnar_filter_test_map_ungoverned();
    for (unsigned mode = 0; mode < 4; ++mode) join_workspace_fixture(mode);
    join_activation_fixtures();
    admission_cases();
    printf(
        "numeric FILTER: semantic, literal, FIFO, retention, authority and fault witnesses passed\n");
    return 0;
}
