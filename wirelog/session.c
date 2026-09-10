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
wl_session_admission_close_and_wait(wl_session_admission_t *admission)
{
    wl_mutex_lock(&admission->mutex);
    admission->closing = true;
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
#endif

static int
wl_session_options_valid(const wl_session_options_t *options)
{
    if (!options)
        return 1;
    return options->version == WL_SESSION_OPTIONS_VERSION
           && options->size >= (uint32_t)sizeof(*options);
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
#ifdef WL_SESSION_TEST_HOOKS
    if (!options)
        options = testhook_options;
#endif
    if (!backend || !backend->session_create || !out
        || !wl_session_options_valid(options))
        return -1;

    admission = wl_session_admission_create();
    if (!admission)
        return ENOMEM;

    if (options && backend->session_create_with_options)
        rc = backend->session_create_with_options(plan, num_workers, options,
                out);
    else
        rc = backend->session_create(plan, num_workers, out);
    if (rc == 0 && *out) {
        /* Ensure the backend pointer is correctly bound */
        (*out)->backend = backend;
        (*out)->operation_admission = admission;
        (*out)->owns_operation_admission = true;
        if (snapshot) {
            wirelog_extension_snapshot_retain(snapshot);
            (*out)->extension_snapshot = snapshot;
            (*out)->owns_extension_snapshot = true;
        }
    } else {
        wl_session_admission_destroy(admission);
    }
    return rc;
}

void
wl_session_destroy(wl_session_t *session)
{
    const wl_compute_backend_t *backend;
    wirelog_extension_snapshot_t *snapshot;
    wl_session_admission_t *admission;
    if (!session)
        return;
    admission = session->owns_operation_admission
        ? session->operation_admission : NULL;
    if (admission)
        wl_session_admission_close_and_wait(admission);
    backend = session->backend;
    snapshot = session->owns_extension_snapshot
        ? session->extension_snapshot : NULL;
    if (backend && backend->session_destroy)
        backend->session_destroy(session);
    wirelog_extension_snapshot_release(snapshot);
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
    else
        rc = session->backend->session_snapshot(session, callback, user_data);
    wl_session_operation_end(session);
    return rc;
}
