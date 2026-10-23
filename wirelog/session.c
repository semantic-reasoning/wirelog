/*
 * session.c - wirelog session wrapper implementation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.  If not, see
 * <https://www.gnu.org/licenses/lgpl-3.0.html>.
 */

#include "session.h"
#include "backend.h"
#include "thread.h"
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>

struct wl_session_admission {
    wl_mutex_t mutex;
    wl_cond_t idle;
    size_t active_operations;
    bool controlled_step_pending;
    bool controlled_step_complete;
    bool closing;
    wl_session_exclusive_attempt_t *exclusive_attempt;
};

#if defined(_MSC_VER)
#define WL_SESSION_THREAD_LOCAL __declspec(thread)
#else
#define WL_SESSION_THREAD_LOCAL _Thread_local
#endif
static WL_SESSION_THREAD_LOCAL unsigned char wl_session_thread_cookie;
#ifdef WL_SESSION_TEST_HOOKS
static WL_SESSION_THREAD_LOCAL bool wl_session_fail_next_admission_lock;
static WL_SESSION_THREAD_LOCAL bool wl_session_fail_next_admission_unlock;
#endif
#undef WL_SESSION_THREAD_LOCAL

static void
wl_session_gate_fatal(void)
{
    abort();
}

static void
wl_session_checked_unlock(wl_session_admission_t *admission)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_session_fail_next_admission_unlock) {
        wl_session_fail_next_admission_unlock = false;
        wl_session_gate_fatal();
    }
#endif
    if (wl_mutex_unlock(&admission->mutex) != 0)
        wl_session_gate_fatal();
}

static int
wl_session_admission_lock(wl_session_admission_t *admission)
{
#ifdef WL_SESSION_TEST_HOOKS
    if (wl_session_fail_next_admission_lock) {
        wl_session_fail_next_admission_lock = false;
        return -1;
    }
#endif
    return wl_mutex_lock(&admission->mutex);
}

#ifdef WL_SESSION_TEST_HOOKS
void
wl_session_testhook_fail_next_admission_lock(void)
{
    wl_session_fail_next_admission_lock = true;
}

void
wl_session_testhook_fail_next_admission_unlock(void)
{
    wl_session_fail_next_admission_unlock = true;
}
#endif

static wl_session_admission_t *
wl_session_admission_create(void)
{
    wl_session_admission_t *admission =
        (wl_session_admission_t *)calloc(1, sizeof(*admission));
    if (!admission)
        return NULL;
    if (wl_mutex_init(&admission->mutex) != 0) {
        free(admission);
        return NULL;
    }
    if (wl_cond_init(&admission->idle) != 0) {
        wl_mutex_destroy(&admission->mutex);
        free(admission);
        return NULL;
    }
    return admission;
}

static void
wl_session_admission_destroy(wl_session_admission_t *admission)
{
    if (!admission)
        return;
    wl_cond_destroy(&admission->idle);
    wl_mutex_destroy(&admission->mutex);
    free(admission);
}

static int
wl_session_operation_begin(wl_session_t *session)
{
    wl_session_admission_t *admission = session->operation_admission;
    if (!admission)
        return 0;

    if (wl_session_admission_lock(admission) != 0)
        return EIO;
    if (admission->closing || admission->controlled_step_pending
        || admission->controlled_step_complete ||
        admission->exclusive_attempt) {
        wl_session_checked_unlock(admission);
        return EBUSY;
    }
    admission->active_operations++;
    wl_session_checked_unlock(admission);
    return 0;
}

static void wl_session_operation_end(wl_session_t *session);

static int
wl_session_operation_begin_insert(wl_session_t *session,
    const char *relation, uint32_t num_rows, uint32_t num_cols)
{
    wl_session_admission_t *admission = session->operation_admission;
    bool controlled_complete;

    if (!admission)
        return 0;
    if (wl_session_admission_lock(admission) != 0)
        return EIO;
    if (admission->closing || admission->controlled_step_pending
        || admission->exclusive_attempt
        || (admission->controlled_step_complete
        && admission->active_operations != 0)) {
        wl_session_checked_unlock(admission);
        return EBUSY;
    }
    controlled_complete = admission->controlled_step_complete;
    admission->active_operations++;
    wl_session_checked_unlock(admission);

    if (controlled_complete
        && (!session->backend
        || !session->backend->session_controlled_step_authorize_insert
        || !session->backend->session_controlled_step_authorize_insert(
            session, relation, num_rows, num_cols))) {
        wl_session_operation_end(session);
        return EBUSY;
    }
    return 0;
}

/* A controlled step owns the session across calls while its framed host
 * continuation is pending. Admit the first call only from a quiescent
 * session; a resume is the only public operation allowed through the gate. */
static int
wl_session_operation_begin_step(wl_session_t *session)
{
    wl_session_admission_t *admission = session->operation_admission;
    if (!admission)
        return 0;

    if (wl_session_admission_lock(admission) != 0)
        return EIO;
    if (admission->closing || admission->active_operations != 0
        || admission->exclusive_attempt) {
        wl_session_checked_unlock(admission);
        return EBUSY;
    }
    admission->controlled_step_pending = true;
    admission->active_operations++;
    wl_session_checked_unlock(admission);
    return 0;
}

/* A completed controlled initial STEP permits read-only snapshot delivery.
 * Keep the whole callback batch inside the same admission gate so mutations,
 * another STEP, and a concurrent snapshot cannot observe a partial batch. */
static int
wl_session_operation_begin_controlled_snapshot(wl_session_t *session)
{
    wl_session_admission_t *admission = session->operation_admission;
    if (!admission)
        return ENOTSUP;

    if (wl_session_admission_lock(admission) != 0)
        return EIO;
    int rc = 0;
    if (admission->closing || admission->active_operations != 0
        || admission->controlled_step_pending || admission->exclusive_attempt)
        rc = EBUSY;
    else {
        wl_controlled_step_state_t state =
            session->backend && session->backend->session_controlled_step_state
            ? session->backend->session_controlled_step_state(session)
            : WL_CONTROLLED_STEP_IDLE;
        if (state == WL_CONTROLLED_STEP_COMPLETE
            && admission->controlled_step_complete)
            admission->active_operations++;
        else
            rc = state == WL_CONTROLLED_STEP_PENDING
                || state == WL_CONTROLLED_STEP_RETRYABLE
                || state == WL_CONTROLLED_STEP_INVALIDATED ? EBUSY : ENOTSUP;
    }
    wl_session_checked_unlock(admission);
    return rc;
}

static void
wl_session_operation_end_step(wl_session_t *session,
    wl_controlled_step_state_t state, bool continuation_pending)
{
    wl_session_admission_t *admission = session->operation_admission;
    if (!admission)
        return;

    if (wl_session_admission_lock(admission) != 0)
        wl_session_gate_fatal();
    admission->active_operations--;
    /* Only a STEP that returned EAGAIN with its live operation identity has
     * transferred a continuation to this caller. A backend EBUSY caused by a
     * separately parked frame must not poison session admission as though it
     * were a continuation created by this STEP call. */
    admission->controlled_step_pending = continuation_pending;
    if (state == WL_CONTROLLED_STEP_COMPLETE
        || state == WL_CONTROLLED_STEP_RETRYABLE
        || state == WL_CONTROLLED_STEP_INVALIDATED)
        admission->controlled_step_complete = true;
    else if (state == WL_CONTROLLED_STEP_IDLE)
        admission->controlled_step_complete = false;
    if (admission->closing && admission->active_operations == 0)
        if (wl_cond_signal(&admission->idle) != 0)
            wl_session_gate_fatal();
    wl_session_checked_unlock(admission);
}

static void
wl_session_operation_end(wl_session_t *session)
{
    wl_session_admission_t *admission = session->operation_admission;
    if (!admission)
        return;

    if (wl_session_admission_lock(admission) != 0)
        wl_session_gate_fatal();
    admission->active_operations--;
    if (admission->closing && admission->active_operations == 0)
        if (wl_cond_signal(&admission->idle) != 0)
            wl_session_gate_fatal();
    wl_session_checked_unlock(admission);
}

static void
wl_session_admission_close_and_wait(wl_session_t *session,
    wl_session_admission_t *admission)
{
    if (wl_session_admission_lock(admission) != 0)
        wl_session_gate_fatal();
    admission->closing = true;
    wl_session_checked_unlock(admission);
#ifdef WL_SESSION_TEST_HOOKS
    wl_session_testhook_admission_closed(session);
#endif
    if (wl_session_admission_lock(admission) != 0)
        wl_session_gate_fatal();
    while (admission->active_operations != 0)
        if (wl_cond_wait(&admission->idle, &admission->mutex) != 0)
            wl_session_gate_fatal();
    wl_session_checked_unlock(admission);
}

static bool
wl_session_exclusive_owner(const wl_session_exclusive_attempt_t *attempt)
{
    return attempt && attempt->self == attempt
           && attempt->owner_cookie == &wl_session_thread_cookie;
}

int
wl_session_exclusive_attempt_acquire(wl_session_t *session,
    wl_session_exclusive_attempt_t *attempt)
{
    wl_session_admission_t *admission;
    if (!session || !attempt || attempt->self || attempt->session
        || attempt->held || attempt->entered || attempt->exit_pending
        || !session->owns_operation_admission)
        return EINVAL;
    admission = session->operation_admission;
    if (!admission) return EINVAL;
    if (wl_session_admission_lock(admission) != 0)
        return EIO;
    if (admission->closing || admission->active_operations != 0
        || admission->controlled_step_pending
        || admission->controlled_step_complete ||
        admission->exclusive_attempt) {
        wl_session_checked_unlock(admission);
        return EBUSY;
    }
    attempt->self = attempt;
    attempt->session = session;
    attempt->owner_cookie = &wl_session_thread_cookie;
    attempt->held = true;
    admission->exclusive_attempt = attempt;
    wl_session_checked_unlock(admission);
    return 0;
}

int
wl_session_exclusive_attempt_enter(wl_session_exclusive_attempt_t *attempt)
{
    wl_session_admission_t *admission;
    if (!wl_session_exclusive_owner(attempt) || !attempt->held
        || attempt->entered || attempt->exit_pending
        || !attempt->session->operation_admission)
        return EINVAL;
    admission = attempt->session->operation_admission;
    if (wl_session_admission_lock(admission) != 0)
        return EIO;
    if (admission->closing || admission->exclusive_attempt != attempt
        || admission->active_operations != 0) {
        wl_session_checked_unlock(admission);
        return EBUSY;
    }
    admission->active_operations = 1;
    attempt->entered = true;
    wl_session_checked_unlock(admission);
    return 0;
}

int
wl_session_exclusive_attempt_mark_exit_pending(
    wl_session_exclusive_attempt_t *attempt)
{
    if (!wl_session_exclusive_owner(attempt) || !attempt->held
        || !attempt->entered || attempt->exit_pending)
        return EINVAL;
    attempt->exit_pending = true;
    return 0;
}

int
wl_session_exclusive_attempt_leave(wl_session_exclusive_attempt_t *attempt)
{
    wl_session_admission_t *admission;
    if (!wl_session_exclusive_owner(attempt) || !attempt->held
        || !attempt->entered || !attempt->exit_pending
        || !attempt->session->operation_admission)
        return EINVAL;
    admission = attempt->session->operation_admission;
    if (wl_session_admission_lock(admission) != 0)
        return EIO;
    if (admission->exclusive_attempt != attempt
        || admission->active_operations != 1)
        wl_session_gate_fatal();
    admission->active_operations = 0;
    attempt->entered = false;
    attempt->exit_pending = false;
    if (admission->closing)
        wl_session_gate_fatal();
    wl_session_checked_unlock(admission);
    return 0;
}

int
wl_session_exclusive_attempt_release(wl_session_exclusive_attempt_t *attempt)
{
    wl_session_admission_t *admission;
    if (!wl_session_exclusive_owner(attempt) || !attempt->held
        || attempt->entered || attempt->exit_pending
        || !attempt->session->operation_admission)
        return EINVAL;
    admission = attempt->session->operation_admission;
    if (wl_session_admission_lock(admission) != 0)
        return EIO;
    if (admission->exclusive_attempt != attempt
        || admission->active_operations != 0)
        wl_session_gate_fatal();
    admission->exclusive_attempt = NULL;
    attempt->held = false;
    attempt->session = NULL;
    attempt->owner_cookie = NULL;
    attempt->self = NULL;
    wl_session_checked_unlock(admission);
    return 0;
}

void
wl_session_options_init(wl_session_options_t *options)
{
    if (!options)
        return;
    options->size = (uint32_t)sizeof(*options);
    options->version = WL_SESSION_OPTIONS_VERSION;
    options->windows_job_handle = NULL;
    options->memory_governor = NULL;
    options->evaluation_control = NULL;
    options->spill_directory = NULL;
    options->spill_quota_bytes = 0;
    options->spill_max_row_bytes = 0;
}

#ifdef WL_SESSION_TEST_HOOKS
/* Only compiled into direct-source test executables (never libwirelog):
 * a per-thread default substituted when a creator passes no options, so
 * facade paths without an options parameter can run under an injected
 * governor. */
#if defined(_MSC_VER)
#define WL_SESSION_THREAD_LOCAL __declspec(thread)
#else
#define WL_SESSION_THREAD_LOCAL _Thread_local
#endif
static WL_SESSION_THREAD_LOCAL const wl_session_options_t *testhook_options;
static wl_session_testhook_fn testhook_admission_closed;
static wl_session_testhook_fn testhook_before_workqueue_drain;
static wl_session_testhook_fn testhook_after_worker_lease_release;
#undef WL_SESSION_THREAD_LOCAL

void
wl_session_testhook_set_default_options(const wl_session_options_t *options)
{
    testhook_options = options;
}

const wl_session_options_t *
wl_session_testhook_default_options(void)
{
    return testhook_options;
}

void
wl_session_testhook_set_admission_closed(wl_session_testhook_fn fn)
{
    testhook_admission_closed = fn;
}

void
wl_session_testhook_admission_closed(wl_session_t *session)
{
    if (testhook_admission_closed)
        testhook_admission_closed(session);
}

void
wl_session_testhook_set_before_workqueue_drain(wl_session_testhook_fn fn)
{
    testhook_before_workqueue_drain = fn;
}

void
wl_session_testhook_before_workqueue_drain(wl_session_t *session)
{
    if (testhook_before_workqueue_drain)
        testhook_before_workqueue_drain(session);
}

void
wl_session_testhook_set_after_worker_lease_release(wl_session_testhook_fn fn)
{
    testhook_after_worker_lease_release = fn;
}

void
wl_session_testhook_after_worker_lease_release(wl_session_t *session)
{
    if (testhook_after_worker_lease_release)
        testhook_after_worker_lease_release(session);
}
#endif

static int
wl_session_options_normalize(const wl_session_options_t *options,
    wl_session_options_t *normalized)
{
    /* Frozen prefixes: never access old storage through a current lvalue. */
    typedef struct {
        uint32_t size;
        uint32_t version;
        void *windows_job_handle;
        wl_columnar_memory_governor_ref_t *memory_governor;
    } wl_session_options_v2_t;
    typedef struct {
        uint32_t size;
        uint32_t version;
        void *windows_job_handle;
        wl_columnar_memory_governor_ref_t *memory_governor;
        wl_evaluation_control_t *evaluation_control;
    } wl_session_options_v3_t;
    uint32_t size;
    uint32_t version;
    wl_session_options_init(normalized);
    if (!options)
        return 1;
    memcpy(&size, options, sizeof(size));
    if (size < sizeof(size) + sizeof(version))
        return 0;
    memcpy(&version, (const char *)options + sizeof(size), sizeof(version));
    if (version == 2 && size >= sizeof(wl_session_options_v2_t)) {
        wl_session_options_v2_t legacy;
        memcpy(&legacy, options, sizeof(legacy));
        normalized->windows_job_handle = legacy.windows_job_handle;
        normalized->memory_governor = legacy.memory_governor;
        return 1;
    }
    if (version == 3 && size >= sizeof(wl_session_options_v3_t)) {
        wl_session_options_v3_t legacy;
        memcpy(&legacy, options, sizeof(legacy));
        normalized->windows_job_handle = legacy.windows_job_handle;
        normalized->memory_governor = legacy.memory_governor;
        normalized->evaluation_control = legacy.evaluation_control;
        return 1;
    }
    if (version != WL_SESSION_OPTIONS_VERSION || size < sizeof(*normalized))
        return 0;
    memcpy(normalized, options, sizeof(*normalized));
    normalized->size = (uint32_t)sizeof(*normalized);
    return 1;
}

int
wl_session_create(const wl_compute_backend_t *backend, const wl_plan_t *plan,
    uint32_t num_workers, wl_session_t **out)
{
    return wl_session_create_with_snapshot_options(backend, plan,
               num_workers, NULL, NULL, out);
}

int
wl_session_create_with_snapshot(const wl_compute_backend_t *backend,
    const wl_plan_t *plan, uint32_t num_workers,
    wirelog_extension_snapshot_t *snapshot, wl_session_t **out)
{
    return wl_session_create_with_snapshot_options(backend, plan,
               num_workers, snapshot, NULL, out);
}

int
wl_session_create_with_options(const wl_compute_backend_t *backend,
    const wl_plan_t *plan, uint32_t num_workers,
    const wl_session_options_t *options, wl_session_t **out)
{
    return wl_session_create_with_snapshot_options(backend, plan,
               num_workers, NULL, options, out);
}

int
wl_session_create_with_snapshot_options(const wl_compute_backend_t *backend,
    const wl_plan_t *plan, uint32_t num_workers,
    wirelog_extension_snapshot_t *snapshot,
    const wl_session_options_t *options, wl_session_t **out)
{
    int rc;
    wl_session_admission_t *admission;
    wl_session_options_t normalized;
    wl_evaluation_control_t *control;
#ifdef WL_SESSION_TEST_HOOKS
    if (!options)
        options = testhook_options;
#endif
    if (!out)
        return -1;
    *out = NULL;
    if (!backend || !backend->session_create
        || !wl_session_options_normalize(options, &normalized))
        return -1;
    if (normalized.spill_directory) {
        if (!normalized.spill_directory[0] || !normalized.spill_quota_bytes
            || normalized.spill_max_row_bytes < 32)
            return EINVAL;
    } else if (normalized.spill_quota_bytes || normalized.spill_max_row_bytes) {
        return EINVAL;
    }
    control = normalized.evaluation_control;
    if ((control || normalized.spill_directory)
        && !backend->session_create_with_options)
        return ENOTSUP;

    admission = wl_session_admission_create();
    if (!admission)
        return ENOMEM;
    if (control) {
        rc = wl_evaluation_control_attach(control, admission);
        if (rc != 0) {
            wl_session_admission_destroy(admission);
            return rc;
        }
    }

    if (options && backend->session_create_with_options)
        rc = backend->session_create_with_options(plan, num_workers,
                &normalized, out);
    else
        rc = backend->session_create(plan, num_workers, out);
    if (rc == 0 && *out) {
        /* Ensure the backend pointer is correctly bound */
        (*out)->backend = backend;
        (*out)->operation_admission = admission;
        (*out)->owns_operation_admission = true;
        (*out)->evaluation_control = control;
        (*out)->owns_evaluation_control = control != NULL;
        if (snapshot) {
            wirelog_extension_snapshot_retain(snapshot);
            (*out)->extension_snapshot = snapshot;
            (*out)->owns_extension_snapshot = true;
        }
    } else {
        if (control)
            (void)wl_evaluation_control_detach(control, admission);
        wl_session_admission_destroy(admission);
        if (rc == 0)
            rc = -1;
    }
    return rc;
}

static void
wl_session_destroy_owned(wl_session_t *session,
    wl_session_admission_t *admission)
{
    const wl_compute_backend_t *backend;
    wirelog_extension_snapshot_t *snapshot;
    wl_evaluation_control_t *control;
    if (!session)
        return;
    if (admission)
        wl_session_admission_close_and_wait(session, admission);
    backend = session->backend;
    snapshot = session->owns_extension_snapshot
        ? session->extension_snapshot : NULL;
    control = session->owns_evaluation_control
        ? session->evaluation_control : NULL;
    if (backend && backend->session_destroy)
        backend->session_destroy(session);
    wirelog_extension_snapshot_release(snapshot);
    if (control)
        (void)wl_evaluation_control_detach(control, admission);
    wl_session_admission_destroy(admission);
}

int
wl_session_try_destroy(wl_session_t **session_slot)
{
    wl_session_t *session;
    wl_session_admission_t *admission;
    if (!session_slot)
        return EINVAL;
    session = *session_slot;
    if (!session)
        return 0;
    if (!session->owns_operation_admission)
        return EINVAL;
    admission = session->operation_admission;
    if (!admission) return EINVAL;
    if (wl_session_admission_lock(admission) != 0)
        return EIO;
    if (admission->closing || admission->active_operations != 0
        || admission->exclusive_attempt || admission->controlled_step_pending) {
        wl_session_checked_unlock(admission);
        return EBUSY;
    }
    admission->closing = true;
    wl_session_checked_unlock(admission);
    *session_slot = NULL;
    wl_session_destroy_owned(session, admission);
    return 0;
}

void
wl_session_destroy(wl_session_t *session)
{
    wl_session_admission_t *admission;
    if (!session)
        return;
    admission = session->owns_operation_admission
        ? session->operation_admission : NULL;
    if (admission) {
        if (wl_session_admission_lock(admission) != 0)
            wl_session_gate_fatal();
        bool parked = admission->exclusive_attempt != NULL
            || admission->controlled_step_pending;
        wl_session_checked_unlock(admission);
        if (parked) {
            fputs("wl_session_destroy: live attempt; use "
                "wl_session_try_destroy after completion/ACK\n", stderr);
            wl_session_gate_fatal();
        }
    }
    wl_session_destroy_owned(session, admission);
}

int
wl_session_insert(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    int rc;
    if (!session)
        return -1;
    rc = wl_session_operation_begin_insert(session, relation, num_rows,
            num_cols);
    if (rc != 0)
        return rc;
    if (!session->backend || !session->backend->session_insert
        || session->input_load_failed)
        rc = -1;
    else {
        if (session->backend->session_budget_denial_clear)
            session->backend->session_budget_denial_clear(session);
        rc = session->backend->session_insert(session, relation, data,
                num_rows, num_cols);
    }
    wl_session_operation_end(session);
    return rc;
}

wl_columnar_memory_governor_t *
wl_session_memory_governor(wl_session_t *session)
{
    wl_columnar_memory_governor_t *governor = NULL;
    if (!session)
        return NULL;
    if (wl_session_operation_begin(session) != 0)
        return NULL;
    if (session->backend && session->backend->session_memory_governor)
        governor = session->backend->session_memory_governor(session);
    wl_session_operation_end(session);
    return governor;
}

int
wl_session_make_compound(wl_session_t *session, const char *functor,
    uint32_t arity, const wirelog_compound_arg_t *args,
    uint64_t *handle_out)
{
    int rc;
    if (!session)
        return EINVAL;
    rc = wl_session_operation_begin(session);
    if (rc != 0)
        return rc;
    if (!session->backend || !session->backend->session_make_compound)
        rc = EINVAL;
    else
        rc = session->backend->session_make_compound(session, functor, arity,
                args, handle_out);
    wl_session_operation_end(session);
    return rc;
}

int
wl_session_remove(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    int rc;
    if (!session)
        return -1;
    rc = wl_session_operation_begin(session);
    if (rc != 0)
        return rc;
    if (!session->backend || !session->backend->session_remove)
        rc = -1;
    else {
        if (session->backend->session_budget_denial_clear)
            session->backend->session_budget_denial_clear(session);
        rc = session->backend->session_remove(session, relation, data,
                num_rows, num_cols);
    }
    wl_session_operation_end(session);
    return rc;
}

int
wl_session_step(wl_session_t *session)
{
    int rc;
    if (!session)
        return -1;
    bool controlled = session->evaluation_control != NULL;
    /* A non-columnar backend must opt in to the controlled STEP contract;
     * otherwise invoking its raw callback could run unbounded work. */
    if (controlled && (!session->backend
        || !session->backend->session_controlled_step_state))
        return ENOTSUP;
    rc = controlled ? wl_session_operation_begin_step(session)
                    : wl_session_operation_begin(session);
    if (rc != 0)
        return rc;
    if (!session->backend || !session->backend->session_step
        || session->input_load_failed)
        rc = -1;
    else
        rc = session->backend->session_step(session);
    if (controlled) {
        wl_controlled_step_state_t state =
            session->backend
            && session->backend->session_controlled_step_state
            ? session->backend->session_controlled_step_state(session)
            : WL_CONTROLLED_STEP_IDLE;
        bool pending = (rc == EAGAIN
            && wl_evaluation_control_active_attempt_id(
                session->evaluation_control) != 0)
            || (rc == WL_EVALUATION_CONTROL_WORK_LIMIT
            && state == WL_CONTROLLED_STEP_PENDING);
        if (rc == EAGAIN && !pending)
            rc = EIO;
        wl_session_operation_end_step(session, state, pending);
    } else {
        wl_session_operation_end(session);
    }
    return rc;
}

void
wl_session_set_delta_cb(wl_session_t *session, wirelog_on_delta_fn callback,
    void *user_data)
{
    if (!session)
        return;
    if (wl_session_delta_callback_is_current(session)) {
        if (session->backend && session->backend->session_set_delta_cb)
            session->backend->session_set_delta_cb(session, callback,
                user_data);
        return;
    }
    if (wl_session_operation_begin(session) != 0)
        return;
    if (session->backend && session->backend->session_set_delta_cb)
        session->backend->session_set_delta_cb(session, callback, user_data);
    wl_session_operation_end(session);
}

int
wl_session_snapshot(wl_session_t *session, wirelog_on_tuple_fn callback,
    void *user_data)
{
    int rc;
    if (!session)
        return -1;
    bool controlled = session->evaluation_control != NULL;
    rc = controlled ? wl_session_operation_begin_controlled_snapshot(session)
        : wl_session_operation_begin(session);
    if (rc != 0)
        return rc;
    if (!session->backend || !session->backend->session_snapshot
        || session->input_load_failed)
        rc = -1;
    else if (controlled && !callback)
        rc = EINVAL;
    else
        rc = session->backend->session_snapshot(session, callback, user_data);
    wl_session_operation_end(session);
    return rc;
}

bool
wl_session_budget_denied(wl_session_t *session)
{
    bool denied;

    if (!session || wl_session_operation_begin(session) != 0)
        return false;
    denied = session->backend && session->backend->session_budget_denied
        && session->backend->session_budget_denied(session);
    wl_session_operation_end(session);
    return denied;
}
