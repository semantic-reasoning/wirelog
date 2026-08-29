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
#include "wirelog/ir/program.h"
#include "wirelog/session.h"
#include "wirelog/session_facts.h"
#include "wirelog/wirelog-internal.h"
#include "wirelog/columnar/internal.h"

#include <errno.h>
#include <limits.h>
#include <math.h>
#include <string.h>
#include <stdlib.h>

struct wirelog_session {
    wirelog_program_t *prog; /* borrowed; caller frees after destroy */
    wl_plan_t *plan;         /* owned */
    wl_session_t *inner;     /* owned */
    wirelog_on_typed_tuple_fn typed_delta_cb;
    void *typed_delta_data;
    bool typed_callback_failed;
};

static const wirelog_column_type_t *session_relation_types(
    const wirelog_session_t *session, const char *relation, uint32_t *count);

static wirelog_column_type_t
session_relation_type_at(const wirelog_session_t *session,
    const char *relation, uint32_t column)
{
    uint32_t count = 0;
    const wirelog_column_type_t *types
        = session_relation_types(session, relation, &count);
    if (types && column < count)
        return types[column];
    if (!session || !session->prog || !relation)
        return WIRELOG_TYPE_INT64;
    for (uint32_t i = 0; i < session->prog->relation_count; i++) {
        wl_ir_relation_info_t *info = &session->prog->relations[i];
        if (info->name && strcmp(info->name, relation) == 0) {
            if (info->columns && column < info->column_count)
                return info->columns[column].type;
            break;
        }
    }
    return WIRELOG_TYPE_INT64;
}

static const wirelog_column_type_t *
session_relation_types(const wirelog_session_t *session, const char *relation,
    uint32_t *count)
{
    if (count)
        *count = 0;
    if (!session || !session->plan || !relation || !count)
        return NULL;
    for (uint32_t i = 0; i < session->plan->edb_count; i++) {
        if (session->plan->edb_relations[i]
            && strcmp(session->plan->edb_relations[i], relation) == 0) {
            *count = session->plan->edb_column_type_counts
                ? session->plan->edb_column_type_counts[i] : 0;
            return session->plan->edb_column_types
                ? session->plan->edb_column_types[i] : NULL;
        }
    }
    return NULL;
}

struct typed_layout {
    uint32_t logical_ncols;
    uint32_t physical_nlanes;
    uint32_t *types;
    uint32_t *lane_offsets;
    uint32_t *physical_types;
};

static void
typed_layout_free(struct typed_layout *layout)
{
    if (!layout)
        return;
    free(layout->types);
    free(layout->lane_offsets);
    free(layout->physical_types);
    memset(layout, 0, sizeof(*layout));
}

/* 0 = valid, -1 = no schema, 1 = allocation failure. */
static int
typed_layout_init(const wirelog_session_t *session, const char *relation,
    struct typed_layout *layout)
{
    if (!session || !relation || !layout)
        return -1;
    memset(layout, 0, sizeof(*layout));
    wl_ir_relation_info_t *info = NULL;
    if (session->prog) {
        for (uint32_t i = 0; i < session->prog->relation_count; i++) {
            if (session->prog->relations[i].name
                && strcmp(session->prog->relations[i].name, relation) == 0) {
                info = &session->prog->relations[i];
                break;
            }
        }
    }
    uint32_t logical = info ? info->column_count : 0;
    uint32_t physical = info ? wl_ir_relation_physical_width(info) : 0;
    const wirelog_column_type_t *plan_types = NULL;
    if (!info)
        plan_types = session_relation_types(session, relation, &physical);
    if (!info && plan_types)
        logical = physical;
    if (logical == 0 || physical == 0)
        return -1;
    layout->logical_ncols = logical;
    layout->physical_nlanes = physical;
    layout->types = calloc(logical, sizeof(*layout->types));
    layout->lane_offsets = calloc(logical, sizeof(*layout->lane_offsets));
    layout->physical_types = calloc(physical, sizeof(*layout->physical_types));
    if (!layout->types || !layout->lane_offsets || !layout->physical_types) {
        typed_layout_free(layout);
        return 1;
    }
    if (!info) {
        for (uint32_t c = 0; c < logical; c++) {
            layout->types[c] = (uint32_t)plan_types[c];
            layout->lane_offsets[c] = c;
            layout->physical_types[c] = layout->types[c];
        }
        return 0;
    }
    uint32_t cursor = 0;
    for (uint32_t c = 0; c < logical; c++) {
        const wirelog_column_t *column = &info->columns[c];
        uint32_t width = column->compound_kind == WIRELOG_COMPOUND_KIND_INLINE
            ? column->compound_arity : 1;
        uint32_t offset = column->compound_kind == WIRELOG_COMPOUND_KIND_INLINE
            ? column->compound_inline_col_offset : cursor;
        layout->types[c] = (uint32_t)column->type;
        layout->lane_offsets[c] = offset;
        for (uint32_t s = 0; s < width; s++) {
            uint32_t slot = offset + s;
            if (slot >= physical) {
                typed_layout_free(layout);
                return -1;
            }
            layout->physical_types[slot] = info->slot_types
                && c * 4 + s < info->slot_type_count
                && info->slot_type_declared
                && info->slot_type_declared[c * 4 + s]
                ? (uint32_t)info->slot_types[c * 4 + s]
                : (uint32_t)column->type;
        }
        cursor = offset + width;
    }
    return 0;
}

static bool
session_relation_has_float(const wirelog_session_t *session,
    const char *relation)
{
    uint32_t count = 0;
    const wirelog_column_type_t *types
        = session_relation_types(session, relation, &count);
    for (uint32_t i = 0; types && i < count; i++)
        if (types[i] == WIRELOG_TYPE_FLOAT)
            return true;
    return false;
}

static bool
session_has_float_relation(const wirelog_session_t *session)
{
    if (!session || !session->prog)
        return false;
    for (uint32_t i = 0; i < session->prog->relation_count; i++) {
        const wl_ir_relation_info_t *info = &session->prog->relations[i];
        if (info->has_float_compound_slots)
            return true;
        for (uint32_t c = 0; info->columns && c < info->column_count; c++)
            if (info->columns[c].type == WIRELOG_TYPE_FLOAT)
                return true;
    }
    return false;
}

static uint32_t
typed_layout_logical_for_slot(const struct typed_layout *layout,
    uint32_t slot)
{
    for (uint32_t c = 0; c < layout->logical_ncols; c++) {
        uint32_t next = c + 1 < layout->logical_ncols
            ? layout->lane_offsets[c + 1] : layout->physical_nlanes;
        if (slot >= layout->lane_offsets[c] && slot < next)
            return c;
    }
    return UINT32_MAX;
}

static void
typed_error_set(wirelog_typed_error_v1_t *error,
    wirelog_typed_error_code_t code, uint32_t row, uint32_t column,
    const char *message)
{
    if (!error || error->struct_size < sizeof(*error))
        return;
    error->code = (uint32_t)code;
    error->row_index = row;
    error->logical_col = column;
    if (error->message && error->message_capacity > 0) {
        size_t n = strlen(message ? message : "");
        if (n >= error->message_capacity)
            n = error->message_capacity - 1;
        memcpy(error->message, message ? message : "", n);
        error->message[n] = '\0';
    }
}

static wirelog_error_t
session_typed_rows(wirelog_session_t *session, const char *relation,
    const wirelog_typed_row_v1_t *rows, uint32_t num_rows,
    wirelog_typed_error_v1_t *error, bool remove)
{
    if (error && error->struct_size >= sizeof(*error)) {
        error->code = WIRELOG_TYPED_ERROR_NONE;
        error->row_index = error->logical_col = UINT32_MAX;
        if (error->message && error->message_capacity > 0)
            error->message[0] = '\0';
    }
    if (!session || !relation || (num_rows > 0 && !rows)) {
        typed_error_set(error, WIRELOG_TYPED_ERROR_DESCRIPTOR, UINT32_MAX,
            UINT32_MAX, "null typed row batch");
        return WIRELOG_ERR_EXEC;
    }
    struct typed_layout layout = { 0 };
    int layout_rc = typed_layout_init(session, relation, &layout);
    if (layout_rc != 0) {
        if (layout_rc > 0)
            return WIRELOG_ERR_MEMORY;
        typed_error_set(error, WIRELOG_TYPED_ERROR_SCHEMA, UINT32_MAX,
            UINT32_MAX, "unknown relation schema");
        return WIRELOG_ERR_EXEC;
    }
    uint32_t physical_nlanes = 0;
    for (uint32_t r = 0; r < num_rows; r++) {
        const wirelog_typed_row_v1_t *row = &rows[r];
        if (row->struct_size < sizeof(*row) || row->abi_version != 1
            || row->reserved != 0
            || row->logical_ncols != layout.logical_ncols
            || row->physical_nlanes == 0
            || row->physical_nlanes != layout.physical_nlanes
            || row->physical_stride != row->physical_nlanes
            || !row->types || !row->lane_offsets || !row->physical_types
            || !row->lanes) {
            typed_layout_free(&layout);
            typed_error_set(error, WIRELOG_TYPED_ERROR_DESCRIPTOR, r,
                UINT32_MAX, "invalid typed row descriptor");
            return WIRELOG_ERR_EXEC;
        }
        if (r == 0)
            physical_nlanes = row->physical_nlanes;
        if (row->physical_nlanes != physical_nlanes) {
            typed_layout_free(&layout);
            typed_error_set(error, WIRELOG_TYPED_ERROR_SCHEMA, r,
                UINT32_MAX, "inconsistent physical row width");
            return WIRELOG_ERR_EXEC;
        }
        for (uint32_t c = 0; c < layout.logical_ncols; c++) {
            if (row->types[c] != layout.types[c]
                || row->lane_offsets[c] != layout.lane_offsets[c]) {
                typed_layout_free(&layout);
                typed_error_set(error, WIRELOG_TYPED_ERROR_SCHEMA, r, c,
                    "typed row schema mismatch");
                return WIRELOG_ERR_EXEC;
            }
        }
        for (uint32_t c = 0; c < layout.physical_nlanes; c++) {
            if (row->physical_types[c] != layout.physical_types[c]) {
                typed_layout_free(&layout);
                typed_error_set(error, WIRELOG_TYPED_ERROR_SCHEMA, r,
                    UINT32_MAX,
                    "typed physical schema mismatch");
                return WIRELOG_ERR_EXEC;
            }
            uint64_t lane = row->lanes[c];
            uint32_t type = layout.physical_types[c];
            bool valid = true;
            switch (type) {
            case WIRELOG_TYPE_INT32: {
                int64_t value;
                memcpy(&value, &lane, sizeof(value));
                valid = value >= INT32_MIN && value <= INT32_MAX;
                break;
            }
            case WIRELOG_TYPE_UINT32:
                valid = lane <= UINT32_MAX;
                break;
            case WIRELOG_TYPE_BOOL:
                valid = lane <= UINT64_C(1);
                break;
            case WIRELOG_TYPE_FLOAT: {
                double value;
                memcpy(&value, &lane, sizeof(value));
                valid = isfinite(value);
                break;
            }
            case WIRELOG_TYPE_INT64:
            case WIRELOG_TYPE_UINT64:
            case WIRELOG_TYPE_STRING:
                break;
            default:
                valid = false;
                break;
            }
            if (!valid) {
                uint32_t logical_col = typed_layout_logical_for_slot(&layout,
                        c);
                typed_layout_free(&layout);
                typed_error_set(error, WIRELOG_TYPED_ERROR_VALUE, r,
                    logical_col, type == WIRELOG_TYPE_FLOAT
                        ? "non-finite float" : "invalid typed lane");
                return WIRELOG_ERR_EXEC;
            }
        }
    }
    if (num_rows == 0) {
        typed_layout_free(&layout);
        return WIRELOG_OK;
    }
    if (num_rows > SIZE_MAX / physical_nlanes
        || (size_t)num_rows * physical_nlanes > SIZE_MAX / sizeof(int64_t)) {
        typed_error_set(error, WIRELOG_TYPED_ERROR_DESCRIPTOR, UINT32_MAX,
            UINT32_MAX, "typed row batch is too large");
        typed_layout_free(&layout);
        return WIRELOG_ERR_EXEC;
    }
    size_t total = (size_t)num_rows * physical_nlanes;
    int64_t *converted = (int64_t *)malloc(total ? total * sizeof(*converted)
                                                  : sizeof(*converted));
    if (!converted){
        typed_layout_free(&layout);
        return WIRELOG_ERR_MEMORY;
    }
    for (uint32_t r = 0; r < num_rows; r++) {
        for (uint32_t c = 0; c < physical_nlanes; c++) {
            uint64_t lane = rows[r].lanes[c];
            if (layout.physical_types[c] == WIRELOG_TYPE_FLOAT) {
                double value;
                memcpy(&value, &lane, sizeof(value));
                if (value == 0.0)
                    lane = UINT64_C(0);
            }
            size_t index = (size_t)r * physical_nlanes + c;
            if (index >= total) {
                free(converted);
                typed_layout_free(&layout);
                typed_error_set(error, WIRELOG_TYPED_ERROR_DESCRIPTOR, r, c,
                    "typed row lane index overflow");
                return WIRELOG_ERR_EXEC;
            }
            memcpy(&converted[index], &lane, sizeof(lane));
        }
    }
    int rc = 0;
    for (uint32_t r = 0; r < num_rows && rc == 0; r++) {
        int64_t *row = &converted[(size_t)r * physical_nlanes];
        rc = remove ? wl_session_remove(session->inner, relation, row, 1,
                physical_nlanes)
            : wl_session_insert(session->inner, relation, row, 1,
                physical_nlanes);
        if (rc != 0) {
            for (uint32_t rollback = 0; rollback < r; rollback++) {
                int inverse = remove
                    ? wl_session_insert(session->inner, relation,
                        &converted[(size_t)rollback * physical_nlanes], 1,
                        physical_nlanes)
                    : wl_session_remove(session->inner, relation,
                        &converted[(size_t)rollback * physical_nlanes], 1,
                        physical_nlanes);
                if (inverse != 0)
                    break;
            }
        }
    }
    free(converted);
    typed_layout_free(&layout);
    if (rc == 0)
        return WIRELOG_OK;
    return rc == ENOMEM ? WIRELOG_ERR_MEMORY : WIRELOG_ERR_EXEC;
}

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

static wirelog_error_t
wirelog_session_create_impl(wirelog_program_t *program,
    wirelog_backend_kind_t backend, uint32_t num_workers,
    wirelog_extension_snapshot_t *snapshot, wirelog_session_t **out)
{
    if (!out)
        return WIRELOG_ERR_EXEC;
    *out = NULL;
    if (!program)
        return WIRELOG_ERR_EXEC;

    const wl_compute_backend_t *be = resolve_backend(backend);
    if (!be)
        return WIRELOG_ERR_EXEC;

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program_with_snapshot(program, snapshot, &plan);
    if (rc != 0 || !plan) {
        if (plan)
            wl_plan_free(plan);
        return WIRELOG_ERR_INVALID_IR;
    }

    wl_session_t *inner = NULL;
    rc = wl_session_create_with_snapshot(be, plan,
            num_workers > 0 ? num_workers : 1, snapshot, &inner);
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

wirelog_error_t
wirelog_session_create(wirelog_program_t *program,
    wirelog_backend_kind_t backend, uint32_t num_workers,
    wirelog_session_t **out)
{
    return wirelog_session_create_impl(program, backend, num_workers, NULL,
               out);
}

wirelog_error_t
wirelog_session_create_with_snapshot(wirelog_program_t *program,
    wirelog_backend_kind_t backend, uint32_t num_workers,
    wirelog_extension_snapshot_t *snapshot, wirelog_session_t **out)
{
    return wirelog_session_create_impl(program, backend, num_workers,
               snapshot, out);
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
    if (session_relation_has_float(session, relation))
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
    if (session_relation_has_float(session, relation))
        return WIRELOG_ERR_EXEC;
    int rc = wl_session_remove(session->inner, relation, data, num_rows,
            num_cols);
    return (rc == 0) ? WIRELOG_OK : WIRELOG_ERR_EXEC;
}

wirelog_error_t
wirelog_session_insert_typed(wirelog_session_t *session, const char *relation,
    const wirelog_typed_row_v1_t *rows, uint32_t num_rows,
    wirelog_typed_error_v1_t *error)
{
    return session_typed_rows(session, relation, rows, num_rows, error, false);
}

wirelog_error_t
wirelog_session_remove_typed(wirelog_session_t *session, const char *relation,
    const wirelog_typed_row_v1_t *rows, uint32_t num_rows,
    wirelog_typed_error_v1_t *error)
{
    return session_typed_rows(session, relation, rows, num_rows, error, true);
}

wirelog_error_t
wirelog_session_step(wirelog_session_t *session)
{
    if (!session)
        return WIRELOG_ERR_EXEC;
    int rc = wl_session_step(session->inner);
    if (rc != 0)
        wl_extension_error_set_expr_status(
            COL_SESSION(session->inner)->extension_expr_status);
    return (rc == 0 && !session->typed_callback_failed) ? WIRELOG_OK
                                                        : WIRELOG_ERR_EXEC;
}

wirelog_error_t
wirelog_session_set_delta_cb(wirelog_session_t *session,
    wirelog_on_delta_fn callback, void *user_data)
{
    if (!session)
        return WIRELOG_ERR_EXEC;
    if (callback && session_has_float_relation(session))
        return WIRELOG_ERR_EXEC;
    session->typed_delta_cb = NULL;
    session->typed_delta_data = NULL;
    wl_session_set_delta_cb(session->inner, callback, user_data);
    return WIRELOG_OK;
}

static bool
typed_emit(wirelog_session_t *session, const char *relation,
    const int64_t *legacy_row, uint32_t ncols, int32_t diff,
    wirelog_on_typed_tuple_fn callback, void *user_data)
{
    struct typed_layout layout = { 0 };
    int layout_rc = typed_layout_init(session, relation, &layout);
    if (layout_rc > 0) {
        session->typed_callback_failed = true;
        return false;
    }
    bool have_layout = layout_rc == 0
        && layout.physical_nlanes == ncols;
    if (!have_layout && layout.types)
        typed_layout_free(&layout);
    if (!have_layout) {
        memset(&layout, 0, sizeof(layout));
        layout.logical_ncols = ncols;
        layout.physical_nlanes = ncols;
        layout.types = ncols ? calloc(ncols, sizeof(*layout.types)) : NULL;
        layout.lane_offsets = ncols
            ? calloc(ncols, sizeof(*layout.lane_offsets)) : NULL;
        layout.physical_types = ncols
            ? calloc(ncols, sizeof(*layout.physical_types)) : NULL;
        if (ncols && (!layout.types || !layout.lane_offsets
            || !layout.physical_types))
            goto failed;
        for (uint32_t c = 0; c < ncols; c++) {
            layout.types[c] = (uint32_t)session_relation_type_at(
                session, relation, c);
            layout.lane_offsets[c] = c;
            layout.physical_types[c] = layout.types[c];
        }
    }
    wirelog_typed_row_v1_t row = {
        sizeof(row), 1, 0, layout.logical_ncols, layout.physical_nlanes,
        layout.physical_nlanes, layout.types, layout.lane_offsets,
        layout.physical_types, (const uint64_t *)legacy_row
    };
    callback(relation, &row, diff, user_data);
    typed_layout_free(&layout);
    return true;
failed:
    session->typed_callback_failed = true;
    typed_layout_free(&layout);
    return false;
}

static void
typed_delta_bridge(const char *relation, const int64_t *row, uint32_t ncols,
    int32_t diff, void *user_data)
{
    wirelog_session_t *session = user_data;
    if (session->typed_delta_cb)
        (void)typed_emit(session, relation, row, ncols, diff,
            session->typed_delta_cb, session->typed_delta_data);
}

wirelog_error_t
wirelog_session_set_typed_delta_cb(wirelog_session_t *session,
    wirelog_on_typed_tuple_fn callback, void *user_data)
{
    if (!session)
        return WIRELOG_ERR_EXEC;
    session->typed_callback_failed = false;
    if (!callback) {
        session->typed_delta_cb = NULL;
        session->typed_delta_data = NULL;
        wl_session_set_delta_cb(session->inner, NULL, NULL);
        return WIRELOG_OK;
    }
    session->typed_delta_cb = callback;
    session->typed_delta_data = user_data;
    wl_session_set_delta_cb(session->inner,
        callback ? typed_delta_bridge : NULL,
        callback ? session : NULL);
    return WIRELOG_OK;
}

struct typed_snapshot_context {
    wirelog_session_t *session;
    wirelog_on_typed_tuple_fn callback;
    void *user_data;
};

static void
typed_snapshot_bridge(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct typed_snapshot_context *ctx = user_data;
    typed_emit(ctx->session, relation, row, ncols, 1, ctx->callback,
        ctx->user_data);
}

wirelog_error_t
wirelog_session_snapshot_typed(wirelog_session_t *session,
    wirelog_on_typed_tuple_fn callback, void *user_data)
{
    if (!session || !callback)
        return WIRELOG_ERR_EXEC;
    session->typed_callback_failed = false;
    struct typed_snapshot_context ctx = { session, callback, user_data };
    int rc = wl_session_snapshot(session->inner, typed_snapshot_bridge, &ctx);
    if (rc != 0)
        wl_extension_error_set_expr_status(
            COL_SESSION(session->inner)->extension_expr_status);
    return rc == 0 && !session->typed_callback_failed ? WIRELOG_OK
                                                      : WIRELOG_ERR_EXEC;
}

wirelog_error_t
wirelog_session_snapshot(wirelog_session_t *session,
    wirelog_on_tuple_fn callback,
    void *user_data)
{
    if (!session || !callback)
        return WIRELOG_ERR_EXEC;
    if (session_has_float_relation(session))
        return WIRELOG_ERR_EXEC;
    int rc = wl_session_snapshot(session->inner, callback, user_data);
    if (rc != 0)
        wl_extension_error_set_expr_status(
            COL_SESSION(session->inner)->extension_expr_status);
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
    for (uint32_t i = 0; i < arity; i++)
        if (args[i].type == WIRELOG_TYPE_FLOAT)
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

wirelog_error_t
wirelog_session_make_compound_typed(wirelog_session_t *session,
    const char *functor, uint32_t arity,
    const wirelog_typed_compound_arg_v1_t *args, uint32_t arg_count,
    uint64_t *handle_out)
{
    if (handle_out)
        *handle_out = WIRELOG_COMPOUND_HANDLE_NULL;
    if (!session || !functor || !args || arity == 0 || arg_count != arity
        || !handle_out)
        return WIRELOG_ERR_EXEC;
    wirelog_compound_arg_t *converted
        = calloc(arity, sizeof(*converted));
    if (!converted)
        return WIRELOG_ERR_MEMORY;
    for (uint32_t i = 0; i < arity; i++) {
        uint64_t bits = args[i].bits;
        if (args[i].type == WIRELOG_TYPE_FLOAT) {
            double value;
            memcpy(&value, &bits, sizeof(value));
            if (!isfinite(value)) {
                free(converted);
                return WIRELOG_ERR_EXEC;
            }
            if (value == 0.0)
                bits = UINT64_C(0);
        }
        converted[i].type = (wirelog_column_type_t)args[i].type;
        memcpy(&converted[i].value, &bits, sizeof(bits));
    }
    int rc = wl_session_make_compound(session->inner, functor, arity,
            converted, handle_out);
    free(converted);
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
