/*
 * session.h - wirelog session management
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

#ifndef WIRELOG_SESSION_H
#define WIRELOG_SESSION_H

#ifdef __cplusplus
extern "C" {
#endif

#include "backend.h"

/*
 * Note: concrete session types embed wl_session_t as their first field,
 * or allocate a struct where the backend pointer is tracked. For our purposes,
 * wl_session_t just needs to house the backend pointer so wrapper functions
 * can dispatch.
 */
struct wl_session {
    const wl_compute_backend_t *backend;
};

/* Wrapper functions that delegate to the backend vtable */

/**
 * wl_session_create:
 * @backend:      The compute backend to use for this session.
 * @plan:         The compiled FFI execution plan.
 * @num_workers:  Number of worker threads to spawn for execution.
 * @out:          (out) Pointer to the newly created session handle on success.
 *
 * Initialize a new execution session using the provided backend vtable.
 *
 * Returns:
 *    0 on success.
 *   -1 on allocation failure or backend initialization error.
 */
int
wl_session_create(const wl_compute_backend_t *backend,
                  const wl_ffi_plan_t *plan, uint32_t num_workers,
                  wl_session_t **out);

/**
 * wl_session_destroy:
 * @session:  The session to destroy (NULL-safe).
 *
 * Destroy an active execution session and release all associated resources.
 */
void
wl_session_destroy(wl_session_t *session);

/**
 * wl_session_insert:
 * @session:   The active execution session.
 * @relation:  The null-terminated name of the relation to receive facts.
 * @data:      Row-major array of int64_t values.
 * @num_rows:  Number of rows to insert.
 * @num_cols:  Number of columns per row.
 *
 * Insert initial input facts into an execution session.
 * Must be called before stepping or execution.
 *
 * Returns:
 *    0 on success.
 *   -1 if the backend or relation is invalid, or if an insertion error occurs.
 */
int
wl_session_insert(wl_session_t *session, const char *relation,
                  const int64_t *data, uint32_t num_rows, uint32_t num_cols);

/**
 * wl_session_remove:
 * @session:   The active execution session.
 * @relation:  The null-terminated name of the relation to revoke facts from.
 * @data:      Row-major array of int64_t values.
 * @num_rows:  Number of rows to remove.
 * @num_cols:  Number of columns per row.
 *
 * Remove/retract previously inserted input facts.
 * Note: Support depends on whether the underlying backend is incremental.
 *
 * Returns:
 *    0 on success.
 *   -1 on error or if the backend does not support retraction.
 */
int
wl_session_remove(wl_session_t *session, const char *relation,
                  const int64_t *data, uint32_t num_rows, uint32_t num_cols);

/**
 * wl_session_step:
 * @session:  The active execution session.
 *
 * Advance an incremental session, evaluating any newly inserted/removed facts.
 * Triggers the delta callback registered via wl_session_set_delta_cb.
 *
 * Returns:
 *    0 on success.
 *   -1 if the backend does not support incremental stepping or an error occurs.
 */
int
wl_session_step(wl_session_t *session);

/**
 * wl_session_set_delta_cb:
 * @session:    The active execution session.
 * @callback:   The delta callback function to invoke upon state changes.
 * @user_data:  Opaque user data pointer passed to the callback.
 *
 * Register a callback for receiving incremental updates (insertions/removals)
 * on output records as the session advances via wl_session_step.
 */
void
wl_session_set_delta_cb(wl_session_t *session, wl_on_delta_fn callback,
                        void *user_data);

/**
 * wl_session_snapshot:
 * @session:    The active execution session.
 * @callback:   The callback function to invoke for each computed output tuple.
 * @user_data:  Opaque user data pointer passed to the callback.
 *
 * Evaluate the current state of the backend as a single batch and yield all
 * output tuples (IDB derivations) to the provided callback synchronously.
 *
 * Returns:
 *    0 on success.
 *   -1 on execution failure or invalid session.
 */
int
wl_session_snapshot(wl_session_t *session, wl_on_tuple_fn callback,
                    void *user_data);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_SESSION_H */
