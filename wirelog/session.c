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
#include <stddef.h>

int
wl_session_create(const wl_compute_backend_t *backend,
                  const wl_ffi_plan_t *plan, uint32_t num_workers,
                  wl_session_t **out)
{
    int rc;
    if (!backend || !backend->session_create || !out)
        return -1;

    rc = backend->session_create(plan, num_workers, out);
    if (rc == 0 && *out) {
        /* Ensure the backend pointer is correctly bound */
        (*out)->backend = backend;
    }
    return rc;
}

void
wl_session_destroy(wl_session_t *session)
{
    if (!session || !session->backend || !session->backend->session_destroy)
        return;
    session->backend->session_destroy(session);
}

int
wl_session_insert(wl_session_t *session, const char *relation,
                  const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    if (!session || !session->backend || !session->backend->session_insert)
        return -1;
    return session->backend->session_insert(session, relation, data, num_rows,
                                            num_cols);
}

int
wl_session_remove(wl_session_t *session, const char *relation,
                  const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    if (!session || !session->backend || !session->backend->session_remove)
        return -1;
    return session->backend->session_remove(session, relation, data, num_rows,
                                            num_cols);
}

int
wl_session_step(wl_session_t *session)
{
    if (!session || !session->backend || !session->backend->session_step)
        return -1;
    return session->backend->session_step(session);
}

void
wl_session_set_delta_cb(wl_session_t *session, wl_on_delta_fn callback,
                        void *user_data)
{
    if (!session || !session->backend
        || !session->backend->session_set_delta_cb)
        return;
    session->backend->session_set_delta_cb(session, callback, user_data);
}

int
wl_session_snapshot(wl_session_t *session, wl_on_tuple_fn callback,
                    void *user_data)
{
    if (!session || !session->backend || !session->backend->session_snapshot)
        return -1;
    return session->backend->session_snapshot(session, callback, user_data);
}
