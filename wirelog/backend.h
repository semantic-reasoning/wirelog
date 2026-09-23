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

#include "exec_plan.h"
#include "columnar/memory_governor.h"
#include "session_options.h"
#include "wirelog/wirelog-types.h"

#include <stdint.h>
#include <stdbool.h>

/* wirelog_on_tuple_fn / wirelog_on_delta_fn now live in wirelog-types.h (single source of truth). */

/**
 * wl_session_t:
 *
 * Opaque handle to an active execution session.
 */
typedef struct wl_session wl_session_t;

/**
 * wl_compute_backend_t:
 *
 * Vtable for execution operations, abstracting over backend platforms
 * (e.g., columnar nanoarrow, FPGA). Allow for future engine
 * swappability while maintaining the same C-level session semantics.
 *
 * @name: Identifier for this backend (e.g., "columnar").
 * @session_create: Initialize a session for a given ir plan.
 * @session_destroy: Deallocate session state.
 * @session_insert: Insert EDB facts.
 * @session_make_compound: Allocate a side-tier compound handle.
 * @session_remove: Retract EDB facts (for incremental backends).
 * @session_step: Perform one epoch advance (incremental).
 * @session_set_delta_cb: Set delta callback on updates.
 * @session_snapshot: Batch execution evaluating full rules synchronously.
 */
typedef struct {
    const char *name;

    int (*session_create)(const wl_plan_t *plan, uint32_t num_workers,
        wl_session_t **out);
    void (*session_destroy)(wl_session_t *session);

    int (*session_insert)(wl_session_t *session, const char *relation,
        const int64_t *data, uint32_t num_rows,
        uint32_t num_cols);

    int (*session_make_compound)(wl_session_t *session, const char *functor,
        uint32_t arity, const wirelog_compound_arg_t *args,
        uint64_t *handle_out);

    int (*session_remove)(wl_session_t *session, const char *relation,
        const int64_t *data, uint32_t num_rows,
        uint32_t num_cols);

    int (*session_step)(wl_session_t *session);

    void (*session_set_delta_cb)(wl_session_t *session,
        wirelog_on_delta_fn callback,
        void *user_data);

    int (*session_snapshot)(wl_session_t *session, wirelog_on_tuple_fn callback,
        void *user_data);

    /* Internal hook used by bounded built-in input loaders. */
    wl_columnar_memory_governor_t *(*session_memory_governor)(
        wl_session_t *session);

    /* Optional internal creation path. Legacy backends leave this NULL.
     * The wrapper passes a normalized, borrowed v3 prefix and owns control
     * attachment. A requested control is rejected if this hook is absent.
     * Controlled evaluation remains unavailable until #1820-#1822. */
    int (*session_create_with_options)(const wl_plan_t *plan,
        uint32_t num_workers, const wl_session_options_t *options,
        wl_session_t **out);

    /* Optional query for a backend-specific memory-budget denial from the
     * most recent operation. Backends without governed admission leave this
     * NULL. */
    bool (*session_budget_denied)(wl_session_t *session);
} wl_compute_backend_t;

/**
 * wl_backend_columnar:
 *
 * Obtain the singleton static vtable instance for the columnar nanoarrow
 * compute backend.
 *
 * Returns: Pointer to the columnar compute backend vtable.
 */
const wl_compute_backend_t *
wl_backend_columnar(void);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_BACKEND_H */
