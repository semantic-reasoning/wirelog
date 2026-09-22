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
#include "thread.h"
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>

struct wl_session_admission {
    wl_mutex_t mutex;
    wl_cond_t idle;
    size_t active_operations;
    bool closing;
};

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

    wl_mutex_lock(&admission->mutex);
    if (admission->closing) {
        wl_mutex_unlock(&admission->mutex);
        return EBUSY;
    }
    admission->active_operations++;
    wl_mutex_unlock(&admission->mutex);
    return 0;
}

static void
wl_session_operation_end(wl_session_t *session)
{
    wl_session_admission_t *admission = session->operation_admission;
    if (!admission)
        return;

    wl_mutex_lock(&admission->mutex);
    admission->active_operations--;
    if (admission->closing && admission->active_operations == 0)
        wl_cond_signal(&admission->idle);
    wl_mutex_unlock(&admission->mutex);
}

static void
wl_session_admission_close_and_wait(wl_session_t *session,
    wl_session_admission_t *admission)
{
    wl_mutex_lock(&admission->mutex);
    admission->closing = true;
    wl_mutex_unlock(&admission->mutex);
#ifdef WL_SESSION_TEST_HOOKS
    wl_session_testhook_admission_closed(session);
#endif
    wl_mutex_lock(&admission->mutex);
    while (admission->active_operations != 0)
        wl_cond_wait(&admission->idle, &admission->mutex);
    wl_mutex_unlock(&admission->mutex);
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
    /* Frozen v2 storage: never access its tail through a v3 typed lvalue. */
    typedef struct {
        uint32_t size;
        uint32_t version;
        void *windows_job_handle;
        wl_columnar_memory_governor_ref_t *memory_governor;
    } wl_session_options_v2_t;
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
    control = normalized.evaluation_control;
    if (control && !backend->session_create_with_options)
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

void
wl_session_destroy(wl_session_t *session)
{
    const wl_compute_backend_t *backend;
    wirelog_extension_snapshot_t *snapshot;
    wl_session_admission_t *admission;
    wl_evaluation_control_t *control;
    if (!session)
        return;
    admission = session->owns_operation_admission
        ? session->operation_admission : NULL;
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
wl_session_insert(wl_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    int rc;
    if (!session)
        return -1;
    rc = wl_session_operation_begin(session);
    if (rc != 0)
        return rc;
    if (!session->backend || !session->backend->session_insert
        || session->input_load_failed)
        rc = -1;
    else
        rc = session->backend->session_insert(session, relation, data,
                num_rows, num_cols);
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
    else
        rc = session->backend->session_remove(session, relation, data,
                num_rows, num_cols);
    wl_session_operation_end(session);
    return rc;
}

int
wl_session_step(wl_session_t *session)
{
    int rc;
    if (!session)
        return -1;
    rc = wl_session_operation_begin(session);
    if (rc != 0)
        return rc;
    if (!session->backend || !session->backend->session_step
        || session->input_load_failed)
        rc = -1;
    else if (session->evaluation_control)
        rc = ENOTSUP; /* #1820-#1822 replace after complete enforcement. */
    else
        rc = session->backend->session_step(session);
    wl_session_operation_end(session);
    return rc;
}

void
wl_session_set_delta_cb(wl_session_t *session, wirelog_on_delta_fn callback,
    void *user_data)
{
    if (!session)
        return;
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
    rc = wl_session_operation_begin(session);
    if (rc != 0)
        return rc;
    if (!session->backend || !session->backend->session_snapshot
        || session->input_load_failed)
        rc = -1;
    else if (session->evaluation_control)
        rc = ENOTSUP; /* Never silently evaluate an attached control. */
    else
        rc = session->backend->session_snapshot(session, callback, user_data);
    wl_session_operation_end(session);
    return rc;
}
