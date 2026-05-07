/*
 * wirelog_advanced.c - implementation of the fine-grained session API.
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

#include "wirelog/wirelog-advanced.h"

#include "wirelog/backend.h"
#include "wirelog/exec_plan.h"
#include "wirelog/exec_plan_gen.h"
#include "wirelog/session.h"
#include "wirelog/session_facts.h"

#include <errno.h>
#include <stdlib.h>

struct wirelog_session {
    wirelog_program_t *prog; /* borrowed; caller frees after destroy */
    wl_plan_t *plan;         /* owned */
    wl_session_t *inner;     /* owned */
};

static const wl_compute_backend_t *
resolve_backend(wirelog_backend_kind_t kind)
{
    switch (kind) {
    case WIRELOG_BACKEND_DEFAULT:
    case WIRELOG_BACKEND_COLUMNAR:
        return wl_backend_columnar();
    default:
        return NULL;
    }
}

wirelog_error_t
wirelog_session_create(wirelog_program_t *program,
    wirelog_backend_kind_t backend, uint32_t num_workers,
    wirelog_session_t **out)
{
    if (!program || !out)
        return WIRELOG_ERR_EXEC;
    *out = NULL;

    const wl_compute_backend_t *be = resolve_backend(backend);
    if (!be)
        return WIRELOG_ERR_EXEC;

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(program, &plan);
    if (rc != 0 || !plan) {
        if (plan)
            wl_plan_free(plan);
        return WIRELOG_ERR_INVALID_IR;
    }

    wl_session_t *inner = NULL;
    rc = wl_session_create(be, plan, num_workers > 0 ? num_workers : 1,
            &inner);
    if (rc != 0 || !inner) {
        wl_plan_free(plan);
        return (rc == ENOMEM) ? WIRELOG_ERR_MEMORY : WIRELOG_ERR_EXEC;
    }

    /* Issue #718 contract: inline `.dl` facts must be observable in
     * snapshots and IDB derivations without further host action.  Seed
     * them eagerly here (delta callback is not yet installed, so the
     * non-incremental insert path is taken and no synthetic static
     * deltas appear on the first step()). */
    rc = wl_session_load_facts(inner, program);
    if (rc != 0) {
        wl_session_destroy(inner);
        wl_plan_free(plan);
        return WIRELOG_ERR_EXEC;
    }

    wirelog_session_t *s
        = (wirelog_session_t *)calloc(1, sizeof(wirelog_session_t));
    if (!s) {
        wl_session_destroy(inner);
        wl_plan_free(plan);
        return WIRELOG_ERR_MEMORY;
    }
    s->prog = program;
    s->plan = plan;
    s->inner = inner;
    *out = s;
    return WIRELOG_OK;
}

void
wirelog_session_destroy(wirelog_session_t *session)
{
    if (!session)
        return;
    if (session->inner)
        wl_session_destroy(session->inner);
    if (session->plan)
        wl_plan_free(session->plan);
    /* session->prog is borrowed; caller frees after destroy. */
    free(session);
}

wirelog_error_t
wirelog_session_insert(wirelog_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    if (!session || !relation || !data)
        return WIRELOG_ERR_EXEC;
    int rc = wl_session_insert(session->inner, relation, data, num_rows,
            num_cols);
    return (rc == 0) ? WIRELOG_OK : WIRELOG_ERR_EXEC;
}

wirelog_error_t
wirelog_session_remove(wirelog_session_t *session, const char *relation,
    const int64_t *data, uint32_t num_rows, uint32_t num_cols)
{
    if (!session || !relation || !data)
        return WIRELOG_ERR_EXEC;
    int rc = wl_session_remove(session->inner, relation, data, num_rows,
            num_cols);
    return (rc == 0) ? WIRELOG_OK : WIRELOG_ERR_EXEC;
}

wirelog_error_t
wirelog_session_step(wirelog_session_t *session)
{
    if (!session)
        return WIRELOG_ERR_EXEC;
    int rc = wl_session_step(session->inner);
    return (rc == 0) ? WIRELOG_OK : WIRELOG_ERR_EXEC;
}

wirelog_error_t
wirelog_session_set_delta_cb(wirelog_session_t *session,
    wl_on_delta_fn callback, void *user_data)
{
    if (!session)
        return WIRELOG_ERR_EXEC;
    wl_session_set_delta_cb(session->inner, callback, user_data);
    return WIRELOG_OK;
}

wirelog_error_t
wirelog_session_snapshot(wirelog_session_t *session, wl_on_tuple_fn callback,
    void *user_data)
{
    if (!session || !callback)
        return WIRELOG_ERR_EXEC;
    int rc = wl_session_snapshot(session->inner, callback, user_data);
    return (rc == 0) ? WIRELOG_OK : WIRELOG_ERR_EXEC;
}

wirelog_error_t
wirelog_session_make_compound(wirelog_session_t *session, const char *functor,
    uint32_t arity, const wirelog_compound_arg_t *args, uint64_t *handle_out)
{
    if (handle_out)
        *handle_out = WIRELOG_COMPOUND_HANDLE_NULL;
    if (!session || !functor || !args || arity == 0 || !handle_out)
        return WIRELOG_ERR_EXEC;

    int rc = wl_session_make_compound(session->inner, functor, arity, args,
            handle_out);
    if (rc == 0)
        return WIRELOG_OK;
    if (rc == ENOSPC)
        return WIRELOG_ERR_COMPOUND_SATURATED;
    if (rc == EBUSY)
        return WIRELOG_ERR_COMPOUND_BUSY;
    if (rc == ENOMEM)
        return WIRELOG_ERR_MEMORY;
    return WIRELOG_ERR_EXEC;
}
