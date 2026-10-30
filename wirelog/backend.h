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

/* Permit callback replacement from the callback thread while retaining the
 * ordinary session admission gate for every other caller. */
wl_session_t *wl_session_delta_callback_enter(wl_session_t *session);
void wl_session_delta_callback_leave(wl_session_t *previous);
bool wl_session_delta_callback_is_current(const wl_session_t *session);

/* Internal state of an attached-control STEP continuation. */
typedef enum {
    WL_CONTROLLED_STEP_IDLE = 0,
    WL_CONTROLLED_STEP_PENDING,
    WL_CONTROLLED_STEP_COMPLETE,
    /* A completed recursive route may admit only backend-approved input
     * appends before its next controlled STEP. */
    WL_CONTROLLED_STEP_RETRYABLE,
    /* Provenance changed outside the authorized retryable input path. */
    WL_CONTROLLED_STEP_INVALIDATED,
    /* Completed public state with legal, unevaluated input mutations. */
    WL_CONTROLLED_STEP_DIRTY,
    /* A stopped synchronous SNAPSHOT may only be retried as SNAPSHOT. */
    WL_CONTROLLED_STEP_SNAPSHOT_RETRYABLE,
} wl_controlled_step_state_t;

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
     * The wrapper passes a normalized, borrowed v4 prefix and owns control
     * attachment. Requested control or spill is rejected if this hook is absent.
     * Controlled evaluation remains unavailable until #1820-#1822. */
    int (*session_create_with_options)(const wl_plan_t *plan,
        uint32_t num_workers, const wl_session_options_t *options,
        wl_session_t **out);

    /* Optional query for a backend-specific memory-budget denial from the
     * most recent operation. The opaque pointer is the backend's session
     * representation. Backends without governed admission leave this NULL. */
    bool (*session_budget_denied)(const void *session);
    /* Optional reset before a new fact mutation. Sessions are externally
     * serialized; the result query then describes that operation alone. */
    void (*session_budget_denial_clear)(void *session);

    /* Optional logical-step state query. PENDING keeps generic session APIs
     * closed between stopped attempts; COMPLETE rejects later mutations. */
    wl_controlled_step_state_t (*session_controlled_step_state)(
        const wl_session_t *session);
    /* Pure preauthorization for the narrowly supported input append that can
     * follow a completed controlled STEP. Called with a public operation
     * reservation active and without the admission mutex held. */
    bool (*session_controlled_step_authorize_insert)(
        const wl_session_t *session, const char *relation,
        uint32_t num_rows, uint32_t num_cols);
    /* Validate a narrow post-completion mutation with an operation reservation
     * active and no admission mutex held. Used after append authorization
     * declines; backends without these mutation hooks remain closed. */
    int (*session_controlled_mutation_admit)(wl_session_t *session,
        bool insert, const char *relation, uint32_t num_rows,
        uint32_t num_cols);
    /* Record a controlled successor mutation after the backend committed it. */
    void (*session_controlled_mutation_committed)(wl_session_t *session,
        bool insert, const char *relation, uint32_t num_rows,
        uint32_t num_cols);
    /* Opt in explicitly before a controlled SNAPSHOT may evaluate fresh or
     * dirty inputs. A read-only completed-snapshot backend need not opt in. */
    bool session_controlled_snapshot_evaluates;
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
