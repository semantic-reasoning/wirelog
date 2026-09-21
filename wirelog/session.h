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
#include "wirelog/wirelog-extension.h"

typedef struct wl_session_admission wl_session_admission_t;

/*
 * Note: concrete session types embed wl_session_t as their first field,
 * or allocate a struct where the backend pointer is tracked. For our purposes,
 * wl_session_t just needs to house the backend pointer so wrapper functions
 * can dispatch.
 */
struct wl_session {
    const wl_compute_backend_t *backend;
    /* Owned reference; workers borrow this pointer and never release it. */
    wirelog_extension_snapshot_t *extension_snapshot;
    bool owns_extension_snapshot;
    /*
     * A failed streamed input load leaves the session unsuitable for
     * evaluation.  Keep this internal poison bit until the session is
     * destroyed; otherwise a later snapshot could evaluate truncated facts.
     */
    bool input_load_failed;
    /*
     * Heap-owned because columnar worker sessions begin as bitwise copies of
     * the coordinator.  Workers clear both fields and never close or destroy
     * the coordinator's admission state.
     */
    wl_session_admission_t *operation_admission;
    bool owns_operation_admission;
};

/* Wrapper functions that delegate to the backend vtable */

/*
 * NOTE: This header is INTERNAL — it is not part of the installed
 * public surface (see install_headers in the top-level meson.build).
 * Public users must use <wirelog/wirelog-easy.h> (or, equivalently, the
 * umbrella <wirelog/wirelog.h>).  The wl_session_* primitives below
 * back the wirelog_easy_* facade and are exercised by in-tree tests; they
 * are not promised to external consumers and may change without notice.
 */

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
wl_session_create(const wl_compute_backend_t *backend, const wl_plan_t *plan,
    uint32_t num_workers, wl_session_t **out);

/**
 * wl_session_create_with_snapshot:
 * Create a session while retaining an optional scalar-addon snapshot.
 * The caller retains ownership of its input reference.
 */
int
wl_session_create_with_snapshot(const wl_compute_backend_t *backend,
    const wl_plan_t *plan, uint32_t num_workers,
    wirelog_extension_snapshot_t *snapshot, wl_session_t **out);

/* Internal host path.  @options is borrowed for the duration of creation. */
int
wl_session_create_with_options(const wl_compute_backend_t *backend,
    const wl_plan_t *plan, uint32_t num_workers,
    const wl_session_options_t *options, wl_session_t **out);

int
wl_session_create_with_snapshot_options(const wl_compute_backend_t *backend,
    const wl_plan_t *plan, uint32_t num_workers,
    wirelog_extension_snapshot_t *snapshot,
    const wl_session_options_t *options, wl_session_t **out);

#ifdef WL_SESSION_TEST_HOOKS
/* Test-only, per-thread default options substituted when a creator passes
 * NULL options.  Borrowed for as long as it stays installed; install NULL
 * to clear.  Never compiled into libwirelog. */
void
wl_session_testhook_set_default_options(const wl_session_options_t *options);
const wl_session_options_t *
wl_session_testhook_default_options(void);

typedef void (*wl_session_testhook_fn)(wl_session_t *session);

void
wl_session_testhook_set_admission_closed(wl_session_testhook_fn fn);
void
wl_session_testhook_admission_closed(wl_session_t *session);
void
wl_session_testhook_set_before_workqueue_drain(wl_session_testhook_fn fn);
void
wl_session_testhook_before_workqueue_drain(wl_session_t *session);
void
wl_session_testhook_set_after_worker_lease_release(wl_session_testhook_fn fn);
void
wl_session_testhook_after_worker_lease_release(wl_session_t *session);
#endif

/**
 * wl_session_destroy:
 * @session:  The session to destroy (NULL-safe).
 *
 * Destroy an active execution session and release all associated resources.
 * Callers must not invoke this function from a session operation or from a
 * tuple/delta callback; such reentrant destruction is unsupported.  The
 * operation admission guard waits for admitted work to finish before backend
 * teardown and therefore cannot safely wait for the operation that called it.
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

/* Internal: return the backend's shared admission governor, when available. */
wl_columnar_memory_governor_t *
wl_session_memory_governor(wl_session_t *session);

/**
 * wl_session_make_compound:
 * @session:    The active execution session.
 * @functor:    Compound functor name.
 * @arity:      Number of compound arguments.
 * @args:       Argument values and declared scalar types.
 * @handle_out: (out) Newly allocated session-local handle.
 *
 * Allocate a handle-backed side-tier compound and append its side-relation row.
 *
 * Returns:
 *    0 on success.
 *    ENOSPC if the compound arena is saturated.
 *    EBUSY if the compound arena is temporarily frozen, an observed step is
 *    pending retry, or a delta callback is running.
 *    ENOMEM on allocation failure.
 *    EINVAL or another errno-style value for invalid arguments/backend errors.
 */
int
wl_session_make_compound(wl_session_t *session, const char *functor,
    uint32_t arity, const wirelog_compound_arg_t *args, uint64_t *handle_out);

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
 * The columnar backend delivers the net change from the start of the whole
 * step only after evaluation and event preparation succeed. A step that
 * fails evaluation, compaction or event preparation delivers nothing, including
 * for relations it already finished: the error can surface after the outputs
 * have moved, so a non-zero return is not evidence that the outputs are
 * unchanged. The baseline the net change is measured against survives such a
 * failure, so a later successful wl_session_step reports the change relative
 * to the state before the failed step -- retry rather than treat the failure
 * as a lost update. The net change is then measured from that first attempt,
 * not from the retry. Cancelling notifications before the retry completes is
 * the one case retrying cannot recover; see wl_session_set_delta_cb. Until
 * completion, snapshot and nonzero input mutation (including compound
 * construction) return EBUSY. Zero-row insert/remove remain no-ops.
 * Reentrant step, snapshot and input mutation from delta callbacks return EBUSY.
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
 * Replacing the callback during delivery affects the remaining events. Setting
 * it to NULL cancels remaining notifications for the current step, even if a
 * callback is registered again before that step completes. Re-enabling then
 * observes subsequent steps. Cancellation does not cancel evaluation: a failed
 * step still requires retry, and once notifications are cancelled that retry
 * reports nothing at all -- neither the changes the cancelled step had already
 * applied nor any the retry newly derives. Re-read the outputs with
 * wl_session_snapshot after the retry completes to resynchronize. Cancelling
 * from inside delivery is different: that step still succeeds and only the
 * events after the cancelling one are dropped. Callback row/name pointers
 * are valid only for the callback invocation. Destruction from a callback
 * remains unsupported.
 */
void
wl_session_set_delta_cb(wl_session_t *session, wirelog_on_delta_fn callback,
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
wl_session_snapshot(wl_session_t *session, wirelog_on_tuple_fn callback,
    void *user_data);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_SESSION_H */
