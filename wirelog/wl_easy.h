/*
 * wl_easy.h - wirelog convenience facade (Issue #441)
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

#ifndef WIRELOG_WL_EASY_H
#define WIRELOG_WL_EASY_H

#ifdef __cplusplus
extern "C" {
#endif

#include "wirelog/wirelog.h"

#include <stdint.h>

/* Forward-declared callback typedefs (mirrors wirelog/backend.h, which is
 * not part of the public install set).  Definitions must remain in lock-step
 * with backend.h. */
typedef void (*wl_on_tuple_fn)(const char *relation, const int64_t *row,
    uint32_t ncols, void *user_data);

typedef void (*wl_on_delta_fn)(const char *relation, const int64_t *row,
    uint32_t ncols, int32_t diff, void *user_data);

/**
 * wl_easy_session_t:
 *
 * Opaque convenience handle that bundles parse, optimize, plan and session
 * lifecycle in a single object.  Use wl_easy_open() / wl_easy_close() to
 * manage instances.
 *
 * Thread safety:
 *   A wl_easy_session_t is NOT thread-safe.  Any two concurrent calls on the
 *   same session pointer — including read-only helpers like wl_easy_intern()
 *   or wl_easy_snapshot() — race on the underlying program, plan and
 *   session state.  Callers MUST serialize access (e.g. with an external
 *   mutex) if they share a session across threads.
 *
 *   Independent sessions created by separate wl_easy_open() calls may be
 *   used on different threads provided the caller does not share any
 *   derived state (rows, callback user_data, intern ids) between them.
 */
typedef struct wl_easy_session wl_easy_session_t;

/**
 * wl_easy_open:
 * @dl_src: Datalog source text (must not be NULL).
 * @out:    (out) Receives the new session handle on success.
 *
 * Parse @dl_src, run the standard optimizer passes (fusion, jpp, sip), and
 * return a session handle.  The execution plan and underlying session are
 * built lazily on the first call to wl_easy_insert/remove/step/set_delta_cb.
 * Symbol interning via wl_easy_intern() may happen before or after that
 * first step-class call; the intern table is shared through the whole
 * session lifetime.
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_PARSE on parse failure,
 * WIRELOG_ERR_MEMORY on allocation failure, or another wirelog_error_t on
 * other failure modes.  *out is set to NULL on error.
 */
wirelog_error_t
wl_easy_open(const char *dl_src, wl_easy_session_t **out);

/**
 * wl_easy_close:
 * @s: Session to free (NULL-safe).
 *
 * Release the session, plan, program, and intern table in the correct order.
 */
void
wl_easy_close(wl_easy_session_t *s);

/**
 * wl_easy_intern:
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
int64_t
wl_easy_intern(wl_easy_session_t *s, const char *sym);

/**
 * wl_easy_insert:
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
wirelog_error_t
wl_easy_insert(wl_easy_session_t *s, const char *relation, const int64_t *row,
    uint32_t ncols);

/**
 * wl_easy_remove:
 * @s:        Open session.
 * @relation: Relation name (must not be NULL).
 * @row:      Row of @ncols int64_t values.
 * @ncols:    Number of columns.
 *
 * Remove a single row from @relation.
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on failure.
 */
wirelog_error_t
wl_easy_remove(wl_easy_session_t *s, const char *relation, const int64_t *row,
    uint32_t ncols);

/**
 * wl_easy_insert_sym:
 * @s:        Open session.
 * @relation: Relation name.
 * @...:      NULL-terminated list of (const char *) symbols, max 16.
 *
 * Variadic helper that interns each symbol then inserts the resulting row.
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on failure.
 */
wirelog_error_t
wl_easy_insert_sym(wl_easy_session_t *s, const char *relation, ...);

/**
 * wl_easy_remove_sym:
 *
 * Variadic counterpart of wl_easy_insert_sym for retraction.
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on failure.
 */
wirelog_error_t
wl_easy_remove_sym(wl_easy_session_t *s, const char *relation, ...);

/**
 * wl_easy_step:
 * @s: Open session.
 *
 * Advance the session by one step, lazily building the plan if needed.
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on failure.
 */
wirelog_error_t
wl_easy_step(wl_easy_session_t *s);

/**
 * wl_easy_set_delta_cb:
 * @s:         Open session.
 * @cb:        Delta callback (may be NULL to clear).
 * @user_data: Opaque user data passed back to @cb.
 *
 * Register a delta callback.  This eagerly builds the plan/session like the
 * other step-class calls, so all wl_easy_intern() calls must precede this.
 */
void
wl_easy_set_delta_cb(wl_easy_session_t *s, wl_on_delta_fn cb, void *user_data);

/**
 * wl_easy_print_delta:
 * @relation:  Relation name from a delta event.
 * @row:       Row of @ncols int64_t values.
 * @ncols:     Number of columns.
 * @diff:      +1 (insertion) or -1 (removal).
 * @user_data: MUST be a wl_easy_session_t* (not the program intern).
 *
 * Convenience delta callback that prints "+" / "-" followed by the relation
 * and the columns formatted according to the parsed schema.  STRING columns
 * are reverse-interned; integer/bool columns are printed as %PRId64.  If a
 * STRING column cannot be reverse-interned, the function calls abort() to
 * prevent silent corruption (NDEBUG-safe — assert() would compile out).
 */
void
wl_easy_print_delta(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data);

/**
 * wl_easy_banner:
 * @label: Section label.
 *
 * Print a small "=== @label ===" banner used by examples.
 */
void
wl_easy_banner(const char *label);

/**
 * wl_easy_snapshot:
 * @s:         Open session.
 * @relation:  Relation to filter on (must not be NULL).
 * @cb:        Callback invoked once per matching tuple.
 * @user_data: Opaque user data passed back to @cb.
 *
 * Take a snapshot of the current state and forward only tuples whose
 * relation name matches @relation to @cb.
 *
 * IMPORTANT: wl_easy_snapshot() is an *evaluating* call — the underlying
 * columnar backend re-evaluates every stratum and emits the resulting IDB
 * rows.  Do NOT call wl_easy_step() followed by wl_easy_snapshot() on the
 * same insert batch: step() already derives and appends the IDB rows, and
 * a subsequent snapshot() will re-derive and append again, producing
 * duplicated tuples.  Choose one mode per batch:
 *   - Incremental / delta mode: wl_easy_set_delta_cb() + wl_easy_step()
 *   - Query mode:               wl_easy_snapshot() (no prior step)
 *
 * Returns: WIRELOG_OK on success, WIRELOG_ERR_EXEC on failure.
 */
wirelog_error_t
wl_easy_snapshot(wl_easy_session_t *s, const char *relation, wl_on_tuple_fn cb,
    void *user_data);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_WL_EASY_H */
