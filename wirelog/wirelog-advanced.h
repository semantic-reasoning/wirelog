/*
 * wirelog-advanced.h - wirelog fine-grained session API (Issue #717 / #703)
 *
 * Copyright (C) CleverPlant
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Licensed under LGPL-3.0-or-later
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

/**
 * @file wirelog-advanced.h
 *
 * Fine-grained session API.  This header is the public counterpart to the
 * convenience facade in <wirelog/wirelog-easy.h>: it lets a host pick the
 * compute backend, the worker count, and drive the session through the
 * same insert / remove / step / snapshot / set_delta_cb / make_compound
 * surface that wirelog_easy wraps.
 *
 * Status: Current.  The signatures and semantics here are stable for the
 * 0.x series and are intended to freeze at 1.0; see docs/SEMANTICS.md and
 * stable-release-plan.md §3.
 *
 * The internal wl_session_* primitives in wirelog/session.h are not part
 * of the installed public surface and may change without notice between
 * minor releases.  External users must use this header (or wirelog-easy.h).
 */

#ifndef WIRELOG_ADVANCED_H
#define WIRELOG_ADVANCED_H

#ifdef __cplusplus
extern "C" {
#endif

#include "wirelog/wirelog.h"
#include "wirelog/wirelog-types.h"

#include <stdint.h>

/**
 * wirelog_session_t:
 *
 * Opaque advanced-session handle.  Allocated by wirelog_session_create()
 * and released by wirelog_session_destroy().  The pointer width and
 * struct layout are unspecified; callers must not sizeof, dereference,
 * or cast the handle.
 *
 * Thread safety: A wirelog_session_t is NOT thread-safe.  Any two
 * concurrent calls on the same session pointer race on the underlying
 * program, plan and session state.  Callers MUST serialize access (e.g.
 * with an external mutex) if they share a session across threads.
 * Independent sessions created by separate wirelog_session_create()
 * calls may be used on different threads provided no derived state
 * (rows, callback user_data, intern ids) is shared between them.
 */
typedef struct wirelog_session wirelog_session_t;

/**
 * wirelog_backend_kind_t:
 *
 * Selector for the compute backend used by a session.  Backends are
 * ABI-additive: new values may appear in future minor releases, but
 * existing values never change.  Callers that switch on this enum
 * should default to a conservative path for unknown values.
 *
 * - WIRELOG_BACKEND_DEFAULT: let the engine pick.  Today this maps to
 *   the columnar backend; future releases may change the default
 *   without breaking ABI.
 * - WIRELOG_BACKEND_COLUMNAR: explicit columnar (Pure C11) backend.
 */
typedef enum {
    WIRELOG_BACKEND_DEFAULT  = 0,
    WIRELOG_BACKEND_COLUMNAR = 1,
} wirelog_backend_kind_t;

/**
 * wirelog_session_create:
 * @program:     Parsed and (optionally) optimized program.  The session
 *               BORROWS the program; the caller retains ownership and
 *               MUST keep it alive for the lifetime of the session and
 *               free it via wirelog_program_free() AFTER calling
 *               wirelog_session_destroy().
 * @backend:     Compute backend selector.  Pass WIRELOG_BACKEND_DEFAULT
 *               unless a specific backend is required.
 * @num_workers: Worker count.  0 maps to 1 (single-threaded).
 * @out:         (out) Receives the new session handle on success; set
 *               to NULL on any error.
 *
 * Build the execution plan, instantiate the chosen backend, and seed
 * any inline `.dl` facts declared in @program into the session as base
 * rows (the same contract documented for wirelog_easy_open in #718 — see
 * docs/SEMANTICS.md "Inline `.dl` facts").  All work happens before
 * this call returns; there is no lazy-build path on the advanced API.
 *
 * Optimizer choice: this call assumes @program has already been run
 * through whichever optimizer passes the caller wants.  Use
 * wirelog_optimize() (the existing public entry point) before
 * wirelog_session_create() if you want fusion / JPP / SIP applied.
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on bad arguments or
 * backend selection, WIRELOG_ERR_INVALID_IR on plan generation failure,
 * WIRELOG_ERR_MEMORY on allocation failure.
 */
wirelog_error_t
wirelog_session_create(wirelog_program_t *program,
    wirelog_backend_kind_t backend, uint32_t num_workers,
    wirelog_session_t **out);

/**
 * wirelog_session_destroy:
 * @session: Session to free (NULL-safe).
 *
 * Release the session and its plan.  Does NOT free the program passed
 * to wirelog_session_create(); the caller still owns it.
 */
void
wirelog_session_destroy(wirelog_session_t *session);

/**
 * wirelog_session_insert:
 * @session:  Session.
 * @relation: Relation name (must not be NULL).
 * @data:     Row-major int64_t values; length = @num_rows * @num_cols.
 * @num_rows: Number of rows to insert.
 * @num_cols: Number of columns per row (must match the relation arity).
 *
 * Insert rows into @relation.  Each insert increments z-set
 * multiplicity by +1; a host that re-inserts a row already present
 * from inline `.dl` seeding will observe multiplicity +2 (see
 * docs/SEMANTICS.md).
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on failure.
 */
wirelog_error_t
wirelog_session_insert(wirelog_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols);

/**
 * wirelog_session_remove:
 * @session:  Session.
 * @relation: Relation name (must not be NULL).
 * @data:     Row-major int64_t values; length = @num_rows * @num_cols.
 * @num_rows: Number of rows to retract.
 * @num_cols: Number of columns per row.
 *
 * Retract rows from @relation.  Each remove decrements z-set
 * multiplicity by -1.
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on failure or if
 * the backend does not support retraction.
 */
wirelog_error_t
wirelog_session_remove(wirelog_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols);

/**
 * wirelog_session_step:
 * @session: Session.
 *
 * Advance the session by one step.  Triggers the delta callback (if
 * registered) for any insertions/removals derived since the previous
 * step.
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on failure.
 */
wirelog_error_t
wirelog_session_step(wirelog_session_t *session);

/**
 * wirelog_session_set_delta_cb:
 * @session:   Session.
 * @callback:  Delta callback (may be NULL to clear).
 * @user_data: Opaque user data passed to @callback.
 *
 * Register a delta callback.  Inline `.dl` facts loaded by
 * wirelog_session_create() are already in base by the time this call
 * runs, so the first wirelog_session_step() after registration does
 * NOT emit synthetic deltas for static rows.
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on NULL @session.
 */
wirelog_error_t
wirelog_session_set_delta_cb(wirelog_session_t *session,
    wirelog_on_delta_fn callback, void *user_data);

/**
 * wirelog_session_snapshot:
 * @session:   Session.
 * @callback:  Per-tuple callback (must not be NULL).
 * @user_data: Opaque user data passed to @callback.
 *
 * Evaluate the current state and forward every IDB tuple to @callback.
 * Like wirelog_easy_snapshot(), this is an *evaluating* call; do NOT mix
 * with wirelog_session_step() on the same insert batch (step() and
 * snapshot() each derive IDB rows independently and combining them
 * produces duplicates).
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on failure.
 */
wirelog_error_t
wirelog_session_snapshot(wirelog_session_t *session,
    wirelog_on_tuple_fn callback,
    void *user_data);

/**
 * wirelog_session_make_compound:
 * @session:    Session.
 * @functor:    Compound functor name.
 * @arity:      Number of compound arguments.
 * @args:       Argument values; length = @arity.
 * @handle_out: (out) Session-local compound handle on success;
 *              WIRELOG_COMPOUND_HANDLE_NULL on failure.
 *
 * Allocate a handle-backed side-tier compound and append its row to
 * the auto-created `__compound_<functor>_<arity>` side relation.
 * Handles are valid only for this session; they MUST NOT be persisted
 * across wirelog_session_destroy() / wirelog_session_create() pairs.
 *
 * Returns: WIRELOG_OK on success; WIRELOG_ERR_COMPOUND_SATURATED if
 * the arena epoch saturated; WIRELOG_ERR_COMPOUND_BUSY if the arena is
 * temporarily frozen; WIRELOG_ERR_MEMORY on allocation failure;
 * WIRELOG_ERR_EXEC on invalid arguments or session-side errors.
 */
wirelog_error_t
wirelog_session_make_compound(wirelog_session_t *session, const char *functor,
    uint32_t arity, const wirelog_compound_arg_t *args, uint64_t *handle_out);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_ADVANCED_H */
