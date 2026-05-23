/*
 * wirelog-easy.h - wirelog convenience facade (Issue #441)
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
 * @file wirelog-easy.h
 * @brief Convenience facade API; one-call session lifecycle for embedders.
 */

#ifndef WIRELOG_WL_EASY_H
#define WIRELOG_WL_EASY_H

#include "wirelog/wirelog-export.h"

#ifdef __cplusplus
extern "C" {
#endif

#include "wirelog/wirelog.h"
#include "wirelog/wirelog-types.h"

#include <stdbool.h>
#include <stdint.h>

/**
 * wirelog_easy_session_t:
 *
 * Opaque convenience handle that bundles parse, optimize, plan and session
 * lifecycle in a single object.  Use wirelog_easy_open() / wirelog_easy_close() to
 * manage instances.
 *
 * Thread safety:
 *   A wirelog_easy_session_t is NOT thread-safe.  Any two concurrent calls on the
 *   same session pointer — including read-only helpers like wirelog_easy_intern()
 *   or wirelog_easy_snapshot() — race on the underlying program, plan and
 *   session state.  Callers MUST serialize access (e.g. with an external
 *   mutex) if they share a session across threads.
 *
 *   Independent sessions created by separate wirelog_easy_open() calls may be
 *   used on different threads provided the caller does not share any
 *   derived state (rows, callback user_data, intern ids) between them.
 */
typedef struct wirelog_easy_session wirelog_easy_session_t;

/**
 * wirelog_easy_open_opts_t:
 *
 * Optional configuration for wirelog_easy_open_opts().  All fields are
 * optional; pass NULL for all defaults (equivalent to wirelog_easy_open).
 *
 * The struct is ABI-versioned via @size.  Callers MUST initialize it
 * with the WIRELOG_EASY_OPEN_OPTS_INIT macro (or assign
 * `.size = sizeof(wirelog_easy_open_opts_t)` explicitly) so future field
 * additions remain ABI-safe.  Library validates `size` on entry and
 * returns WIRELOG_ERR_EXEC if it is smaller than the runtime
 * sizeof(wirelog_easy_open_opts_t) compiled into libwirelog.
 *
 * @size:         sizeof(wirelog_easy_open_opts_t) at the caller's compile
 *                time.  REQUIRED.  Use WIRELOG_EASY_OPEN_OPTS_INIT.
 * @num_workers:  Number of worker threads for the underlying session.
 *                0 (the default) is permanently mapped to 1; do not
 *                rely on a different default in future releases.
 * @eager_build:  If true, force plan + session build before
 *                wirelog_easy_open_opts() returns, so parse/plan/session
 *                errors surface synchronously.  If false (default),
 *                the build is deferred until the first
 *                insert/remove/step/set_delta_cb/snapshot call
 *                (legacy wirelog_easy_open behavior).
 * @_reserved:    Reserved for future use; must be NULL.  Non-NULL
 *                returns WIRELOG_ERR_EXEC.
 *
 * Lifetime: opts is read once during wirelog_easy_open_opts() and may be
 * freed by the caller immediately after the call returns.
 */
typedef struct {
    uint32_t size;
    uint32_t num_workers;
    bool eager_build;
    const void *_reserved;
} wirelog_easy_open_opts_t;

#define WIRELOG_EASY_OPEN_OPTS_INIT \
        { .size = sizeof(wirelog_easy_open_opts_t), \
          .num_workers = 0, \
          .eager_build = false, \
          ._reserved = NULL }

/**
 * wirelog_easy_open_opts:
 * @dl_src: Datalog source text (must not be NULL).
 * @opts:   Optional configuration (may be NULL → defaults).  If
 *          non-NULL, must have @size set to
 *          sizeof(wirelog_easy_open_opts_t) (use WIRELOG_EASY_OPEN_OPTS_INIT).
 * @out:    (out) Receives the new session handle on success.
 *
 * Same contract as wirelog_easy_open() but with optional configuration.
 * wirelog_easy_open(dl, out) is equivalent to
 * wirelog_easy_open_opts(dl, NULL, out).
 *
 * Error codes (in addition to wirelog_easy_open's):
 *   WIRELOG_ERR_EXEC      - opts->size too small / opts->_reserved non-NULL
 *   WIRELOG_ERR_INVALID_IR - eager_build set and plan generation failed
 *   WIRELOG_ERR_MEMORY    - eager_build set and session allocation failed
 *
 * Returns: WIRELOG_OK on success; *out set to NULL on any error.
 */
WIRELOG_API wirelog_error_t
wirelog_easy_open_opts(const char *dl_src,
    const wirelog_easy_open_opts_t *opts,
    wirelog_easy_session_t **out);

/**
 * wirelog_easy_open:
 * @dl_src: Datalog source text (must not be NULL).
 * @out:    (out) Receives the new session handle on success.
 *
 * Parse @dl_src, run the standard optimizer pipeline, and return a session
 * handle.  The execution plan and underlying session are
 * built lazily on the first call to wirelog_easy_insert/remove/step/set_delta_cb.
 * Symbol interning via wirelog_easy_intern() may happen before or after that
 * first step-class call; the intern table is shared through the whole
 * session lifetime.
 *
 * Inline `.dl` facts (e.g. `edge("a", "b").`) are seeded into the session
 * as base rows at first lazy build, before any host delta callback can be
 * installed.  Snapshots and IDB derivations therefore observe them.  Host
 * inserts of the same row add a second multiplicity (z-set semantics): a
 * subsequent wirelog_easy_remove() decrements one multiplicity, so a host that
 * mirrors a static fact and later retracts must mirror the retract too.
 * Hosts that need to know which rows are already present from the source
 * can read them via wirelog_program_get_facts().
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_PARSE on parse failure,
 * WIRELOG_ERR_MEMORY on allocation failure, or another wirelog_error_t on
 * other failure modes.  *out is set to NULL on error.
 */
WIRELOG_API wirelog_error_t
wirelog_easy_open(const char *dl_src, wirelog_easy_session_t **out);

/**
 * wirelog_easy_close:
 * @s: Session to free (NULL-safe).
 *
 * Release the session, plan, program, and intern table in the correct order.
 */
WIRELOG_API void
wirelog_easy_close(wirelog_easy_session_t *s);

/**
 * wirelog_easy_intern:
 * @s:   Open session (must not be NULL).
 * @sym: Symbol to intern (must not be NULL).
 *
 * Intern @sym into the program's intern table and return its id.  May be
 * called at any point in a session's lifetime — before or after the plan
 * has been built — because the program's intern table is aliased through
 * the plan and session, so a new id is immediately visible to any running
 * backend.  Calling this function repeatedly for the same string returns
 * the same id, so symbol ids remain stable across the run regardless of
 * call ordering.
 *
 * Returns: non-negative ID on success, -1 on NULL args or internal error.
 */
WIRELOG_API int64_t
wirelog_easy_intern(wirelog_easy_session_t *s, const char *sym);

/**
 * wirelog_easy_make_compound:
 * @s:          Open wirelog_easy session.
 * @functor:    Compound functor name, e.g. "metadata".
 * @arity:      Number of compound arguments.
 * @args:       Row values for arg0..arg{arity-1}.
 * @handle_out: (out) Receives a session-local compound handle.
 *
 * Allocate a handle-backed side-tier compound in the session-local compound
 * arena and insert its side-relation row (`__compound_<functor>_<arity>`).
 * The returned handle is valid only for this session.  Callers that rotate
 * sessions must persist source-level argument values and rebuild handles in
 * the fresh session; do not persist handles across `wirelog_easy_open()` calls.
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_COMPOUND_SATURATED when the arena
 * refuses allocation after epoch saturation, WIRELOG_ERR_COMPOUND_BUSY when it
 * is temporarily frozen, WIRELOG_ERR_MEMORY for allocation failure, or
 * WIRELOG_ERR_EXEC for invalid arguments, plan/session build failure,
 * side-relation registration failure, or insertion failure.
 */
WIRELOG_API wirelog_error_t
wirelog_easy_make_compound(wirelog_easy_session_t *s, const char *functor,
    uint32_t arity, const wirelog_compound_arg_t *args, uint64_t *handle_out);

/**
 * wirelog_easy_insert:
 * @s:        Open session.
 * @relation: Relation name (must not be NULL).
 * @row:      Row of @ncols int64_t values.
 * @ncols:    Number of columns.
 *
 * Insert a single row into @relation, lazily building the plan and session
 * on first call.
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on failure.
 */
WIRELOG_API wirelog_error_t
wirelog_easy_insert(wirelog_easy_session_t *s, const char *relation,
    const int64_t *row,
    uint32_t ncols);

/**
 * wirelog_easy_remove:
 * @s:        Open session.
 * @relation: Relation name (must not be NULL).
 * @row:      Row of @ncols int64_t values.
 * @ncols:    Number of columns.
 *
 * Remove a single row from @relation.
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on failure.
 */
WIRELOG_API wirelog_error_t
wirelog_easy_remove(wirelog_easy_session_t *s, const char *relation,
    const int64_t *row,
    uint32_t ncols);

/**
 * wirelog_easy_insert_sym:
 * @s:        Open session.
 * @relation: Relation name.
 * @...:      NULL-terminated list of (const char *) symbols, max 16.
 *
 * Variadic helper that interns each symbol then inserts the resulting row.
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on failure.
 */
WIRELOG_API wirelog_error_t
wirelog_easy_insert_sym(wirelog_easy_session_t *s, const char *relation, ...);

/**
 * wirelog_easy_remove_sym:
 *
 * Variadic counterpart of wirelog_easy_insert_sym for retraction.
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on failure.
 */
WIRELOG_API wirelog_error_t
wirelog_easy_remove_sym(wirelog_easy_session_t *s, const char *relation, ...);

/**
 * wirelog_easy_step:
 * @s: Open session.
 *
 * Advance the session by one step, lazily building the plan if needed.
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on failure.
 */
WIRELOG_API wirelog_error_t
wirelog_easy_step(wirelog_easy_session_t *s);

/**
 * wirelog_easy_set_delta_cb:
 * @s:         Open session.
 * @cb:        Delta callback (may be NULL to clear).
 * @user_data: Opaque user data passed back to @cb.
 *
 * Register a delta callback.  This eagerly builds the plan and underlying
 * session (lazy-init path), which may fail; the return value propagates
 * that error to the caller.  Symbol interning via wirelog_easy_intern() may
 * happen before OR after this call.
 *
 * Returns: WIRELOG_OK on success, or a wirelog_error_t describing the
 * plan/session build failure.  A NULL @s returns WIRELOG_ERR_EXEC.
 */
WIRELOG_API wirelog_error_t
wirelog_easy_set_delta_cb(wirelog_easy_session_t *s, wirelog_on_delta_fn cb,
    void *user_data);

/**
 * wirelog_easy_print_delta:
 * @relation:  Relation name from a delta event.
 * @row:       Row of @ncols int64_t values.
 * @ncols:     Number of columns.
 * @diff:      +1 (insertion) or -1 (removal).
 * @user_data: MUST be a wirelog_easy_session_t* (not the program intern).
 *
 * Convenience delta callback that prints "+" / "-" followed by the relation
 * and the columns formatted according to the parsed schema.  STRING columns
 * are reverse-interned; integer/bool columns are printed as %PRId64.  If a
 * STRING column cannot be reverse-interned, the function calls abort() to
 * prevent silent corruption (NDEBUG-safe — assert() would compile out).
 */
WIRELOG_API void
wirelog_easy_print_delta(const char *relation, const int64_t *row,
    uint32_t ncols,
    int32_t diff, void *user_data);

/**
 * wirelog_easy_banner:
 * @label: Section label.
 *
 * Print a small "=== @label ===" banner used by examples.
 */
WIRELOG_API void
wirelog_easy_banner(const char *label);

/**
 * wirelog_easy_snapshot:
 * @s:         Open session.
 * @relation:  Relation to filter on (must not be NULL).
 * @cb:        Callback invoked once per matching tuple.
 * @user_data: Opaque user data passed back to @cb.
 *
 * Take a snapshot of the current state and forward only tuples whose
 * relation name matches @relation to @cb.
 *
 * IMPORTANT: wirelog_easy_snapshot() is an *evaluating* call — the underlying
 * columnar backend re-evaluates every stratum and emits the resulting IDB
 * rows.  Do NOT call wirelog_easy_step() followed by wirelog_easy_snapshot() on the
 * same insert batch: step() already derives and appends the IDB rows, and
 * a subsequent snapshot() will re-derive and append again, producing
 * duplicated tuples.  Choose one mode per batch:
 *   - Incremental / delta mode: wirelog_easy_set_delta_cb() + wirelog_easy_step()
 *   - Query mode:               wirelog_easy_snapshot() (no prior step)
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on failure.
 */
WIRELOG_API wirelog_error_t
wirelog_easy_snapshot(wirelog_easy_session_t *s, const char *relation,
    wirelog_on_tuple_fn cb,
    void *user_data);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_WL_EASY_H */
