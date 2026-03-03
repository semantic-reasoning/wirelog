/*
 * backend.h - wirelog execution backend abstraction
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

#ifndef WIRELOG_BACKEND_H
#define WIRELOG_BACKEND_H

#ifdef __cplusplus
extern "C" {
#endif

#include "ffi/dd_ffi.h"

#include <stdint.h>
#include <stdbool.h>

/**
 * wl_session_t:
 *
 * Opaque handle to an active execution session.
 */
typedef struct wl_session wl_session_t;

/**
 * wl_on_tuple_fn:
 *
 * Callback invoked once per computed output tuple.
 *
 * @relation:  Null-terminated output relation name.
 * @row:       Array of int64_t values (length = @ncols).
 * @ncols:     Number of columns in the row.
 * @user_data: Opaque pointer passed through from the caller.
 */
typedef void (*wl_on_tuple_fn)(const char *relation, const int64_t *row,
                               uint32_t ncols, void *user_data);

/**
 * wl_on_delta_fn:
 *
 * Callback invoked when a tuple is inserted/removed in an incremental session.
 *
 * @relation:  Null-terminated output relation name.
 * @row:       Array of int64_t values (length = @ncols).
 * @ncols:     Number of columns in the row.
 * @diff:      +1 for insertion, -1 for removal.
 * @user_data: Opaque pointer passed through from the caller.
 */
typedef void (*wl_on_delta_fn)(const char *relation, const int64_t *row,
                               uint32_t ncols, int32_t diff, void *user_data);

/**
 * wl_compute_backend_t:
 *
 * Vtable for execution operations, abstracting over backend platforms
 * (e.g., Differential Dataflow, nanoarrow). Allow for future engine
 * swappability while maintaining the same C-level session semantics.
 *
 * @name: Identifier for this backend (e.g., "dd").
 * @session_create: Initialize a session for a given ir plan.
 * @session_destroy: Deallocate session state.
 * @session_insert: Insert EDB facts.
 * @session_remove: Retract EDB facts (for incremental backends).
 * @session_step: Perform one epoch advance (incremental).
 * @session_set_delta_cb: Set differential callback on updates.
 * @session_snapshot: Batch execution evaluating full rules synchronously.
 */
typedef struct {
    const char *name;

    int (*session_create)(const wl_ffi_plan_t *plan, uint32_t num_workers,
                          wl_session_t **out);
    void (*session_destroy)(wl_session_t *session);

    int (*session_insert)(wl_session_t *session, const char *relation,
                          const int64_t *data, uint32_t num_rows,
                          uint32_t num_cols);

    int (*session_remove)(wl_session_t *session, const char *relation,
                          const int64_t *data, uint32_t num_rows,
                          uint32_t num_cols);

    int (*session_step)(wl_session_t *session);

    void (*session_set_delta_cb)(wl_session_t *session, wl_on_delta_fn callback,
                                 void *user_data);

    int (*session_snapshot)(wl_session_t *session, wl_on_tuple_fn callback,
                            void *user_data);
} wl_compute_backend_t;

/**
 * wl_backend_dd:
 *
 * Obtain the singleton static vtable instance for the Differential
 * Dataflow (Rust-based) pipeline compute backend.
 *
 * Returns: Pointer to the Differential Dataflow compute backend vtable.
 */
const wl_compute_backend_t *
wl_backend_dd(void);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_BACKEND_H */
