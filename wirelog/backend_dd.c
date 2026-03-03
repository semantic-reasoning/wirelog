/*
 * backend_dd.c - Differential Dataflow compute backend
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

#include "backend.h"
#include "session.h"
#include "ffi/dd_ffi.h"

#include <stdlib.h>

/**
 * DD-specific session state bounding the base wl_session_t.
 */
typedef struct {
    wl_session_t base;

    /* DD FFI specific state */
    wl_dd_worker_t *worker;
    const wl_ffi_plan_t *plan;

    /* For incremental/delta callbacks, if DD supports it later */
    wl_on_delta_fn delta_cb;
    void *delta_user_data;
} wl_dd_session_t;

static int
dd_session_create(const wl_ffi_plan_t *plan, uint32_t num_workers,
                  wl_session_t **out)
{
    wl_dd_session_t *s;
    wl_dd_worker_t *worker;

    if (!plan || !out)
        return -1;

    worker = wl_dd_worker_create(num_workers);
    if (!worker)
        return -1;

    s = (wl_dd_session_t *)calloc(1, sizeof(wl_dd_session_t));
    if (!s) {
        wl_dd_worker_destroy(worker);
        return -1;
    }

    s->worker = worker;
    s->plan = plan;

    *out = (wl_session_t *)s;
    return 0;
}

static void
dd_session_destroy(wl_session_t *session)
{
    wl_dd_session_t *s = (wl_dd_session_t *)session;
    if (s) {
        if (s->worker) {
            wl_dd_worker_destroy(s->worker);
        }
        free(s);
    }
}

static int
dd_session_insert(wl_session_t *session, const char *relation,
                  const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    wl_dd_session_t *s = (wl_dd_session_t *)session;
    if (!s || !s->worker)
        return -1;

    /* The DD backend expects data insertion via wl_dd_load_edb */
    return wl_dd_load_edb(s->worker, relation, data, num_rows, num_cols);
}

static int
dd_session_remove(wl_session_t *session, const char *relation,
                  const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    /* The DD backend currently doesn't expose a remove API in dd_ffi.h
     * Once it does, this will map to it. Returning error for now. */
    (void)session;
    (void)relation;
    (void)data;
    (void)num_rows;
    (void)num_cols;
    return -1;
}

static int
dd_session_step(wl_session_t *session)
{
    /* The DD backend batch mode executes and returns everything in wl_dd_execute_cb.
     * We don't have an incremental step function exposed via FFI yet. */
    (void)session;
    return -1; /* Not supported in DD backend natively yet */
}

static void
dd_session_set_delta_cb(wl_session_t *session, wl_on_delta_fn callback,
                        void *user_data)
{
    wl_dd_session_t *s = (wl_dd_session_t *)session;
    if (s) {
        s->delta_cb = callback;
        s->delta_user_data = user_data;
    }
}

static int
dd_session_snapshot(wl_session_t *session, wl_on_tuple_fn callback,
                    void *user_data)
{
    wl_dd_session_t *s = (wl_dd_session_t *)session;
    if (!s || !s->worker || !s->plan)
        return -1;

    return wl_dd_execute_cb(s->plan, s->worker, (wl_dd_on_tuple_fn)callback,
                            user_data);
}

static const wl_compute_backend_t dd_backend_vtable = {
    .name = "dd",
    .session_create = dd_session_create,
    .session_destroy = dd_session_destroy,
    .session_insert = dd_session_insert,
    .session_remove = dd_session_remove,
    .session_step = dd_session_step,
    .session_set_delta_cb = dd_session_set_delta_cb,
    .session_snapshot = dd_session_snapshot,
};

const wl_compute_backend_t *
wl_backend_dd(void)
{
    return &dd_backend_vtable;
}
