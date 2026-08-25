/*
 * io_ctx.c - I/O Context Accessors
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Implements the wirelog_io_ctx_t accessor functions declared in
 * io_adapter.h (#451) and the test constructor/destructor from
 * io_ctx_internal.h.
 *
 * Part of #446 (I/O adapter umbrella).
 */

#include "wirelog/io/io_ctx_internal.h"

#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------------ */
/* Production Constructor (from relation metadata)                          */
/* ------------------------------------------------------------------------ */

wirelog_io_ctx_t *
wirelog_io_ctx_create_for_relation(const wl_ir_relation_info_t *rel,
    wl_intern_t *intern)
{
    if (!rel)
        return NULL;

    wirelog_io_ctx_t *ctx = (wirelog_io_ctx_t *)calloc(1, sizeof(*ctx));
    if (!ctx)
        return NULL;

    ctx->relation_name = rel->name;                          /* borrowed */
    /* Physical width (#985).  num_cols is the adapter contract's row stride:
     * docs/io-adapters.md requires read() to produce
     * *out_nrows * wirelog_io_ctx_num_cols(ctx) elements, and
     * wl_session_load_input_files() inserts at exactly that stride.  An
     * inline compound column occupies compound_arity slots, so the logical
     * rel->column_count named a stride the storage does not use. */
    ctx->num_cols = wl_ir_relation_physical_width(rel);
    ctx->param_keys = (const char **)rel->input_param_names;      /* borrowed */
    ctx->param_values = (const char **)rel->input_param_values;   /* borrowed */
    ctx->num_params = rel->input_param_count;
    ctx->intern = intern;                                    /* borrowed */
    ctx->platform_ctx = NULL;

    if (ctx->num_cols > 0 && rel->columns) {
        ctx->col_types = (wirelog_column_type_t *)malloc(
            ctx->num_cols * sizeof(wirelog_column_type_t));
        if (!ctx->col_types) {
            free(ctx);
            return NULL;
        }
        /* col_types is physical-indexed too, so it stays parallel to
         * num_cols.  The load-bearing part of this loop is the *shift*: a
         * scalar column after an inline compound must report its declared
         * type at the compound's physical position, not at its logical one,
         * or a trailing `symbol` lands on an inline slot and the file is
         * read under the wrong types.
         *
         * Legacy slash declarations keep every inline slot at INT64.  An
         * explicit declaration such as `pair(symbol, int64) inline` carries
         * its per-slot types in rel->slot_types, so the input path can load
         * interned symbols as well as numeric cells.  The slots remain
         * ordinary int64 storage; the type only selects how the adapter
         * decodes and renders each value.
         *
         * An untyped slash declaration still rejects symbolic input, because
         * its all-INT64 contract gives the adapter no authority to intern a
         * text field. */
        uint32_t k = 0;
        for (uint32_t i = 0; i < rel->column_count; i++) {
            if (rel->columns[i].compound_kind
                == WIRELOG_COMPOUND_KIND_INLINE) {
                /* Issue #1037: a `.decl` may name each slot's type as
                 * `f(symbol, int64) inline`.  When it does, the side array
                 * carries them at col*4+slot; a symbol slot reads as
                 * WIRELOG_TYPE_STRING, which csv_reader.c already interns.
                 * `f/arity` leaves slot_types NULL and keeps meaning
                 * all-int64, so nothing that parses today moves. */
                for (uint32_t j = 0; j < rel->columns[i].compound_arity; j++) {
                    wirelog_column_type_t st = WIRELOG_TYPE_INT64;
                    if (rel->slot_types && (i * 4 + j) < rel->slot_type_count)
                        st = rel->slot_types[i * 4 + j];
                    ctx->col_types[k++] = st;
                }
            } else {
                ctx->col_types[k++] = rel->columns[i].type;
            }
        }
    }

    return ctx;
}

/* ------------------------------------------------------------------------ */
/* Test Constructor / Destructor                                            */
/* ------------------------------------------------------------------------ */

wirelog_io_ctx_t *
wirelog_io_ctx_create_test(const char *relation_name,
    const wirelog_column_type_t *col_types, uint32_t num_cols,
    const char **param_keys, const char **param_values,
    uint32_t num_params, wl_intern_t *intern)
{
    wirelog_io_ctx_t *ctx = (wirelog_io_ctx_t *)calloc(1, sizeof(*ctx));
    if (!ctx)
        return NULL;

    ctx->relation_name = relation_name;
    ctx->num_cols = num_cols;
    ctx->num_params = num_params;
    ctx->param_keys = param_keys;
    ctx->param_values = param_values;
    ctx->intern = intern;
    ctx->platform_ctx = NULL;

    if (num_cols > 0 && col_types) {
        ctx->col_types = (wirelog_column_type_t *)malloc(
            num_cols * sizeof(wirelog_column_type_t));
        if (!ctx->col_types) {
            free(ctx);
            return NULL;
        }
        memcpy(ctx->col_types, col_types,
            num_cols * sizeof(wirelog_column_type_t));
    }

    return ctx;
}

void
wirelog_io_ctx_destroy(wirelog_io_ctx_t *ctx)
{
    if (!ctx)
        return;
    free(ctx->col_types);
    free(ctx);
}

/* ------------------------------------------------------------------------ */
/* Accessors                                                                */
/* ------------------------------------------------------------------------ */

const char *
wirelog_io_ctx_relation_name(const wirelog_io_ctx_t *ctx)
{
    if (!ctx)
        return NULL;
    return ctx->relation_name;
}

uint32_t
wirelog_io_ctx_num_cols(const wirelog_io_ctx_t *ctx)
{
    if (!ctx)
        return 0;
    return ctx->num_cols;
}

wirelog_column_type_t
wirelog_io_ctx_col_type(const wirelog_io_ctx_t *ctx, uint32_t col)
{
    if (!ctx || col >= ctx->num_cols)
        return (wirelog_column_type_t)-1;
    return ctx->col_types[col];
}

const char *
wirelog_io_ctx_param(const wirelog_io_ctx_t *ctx, const char *key)
{
    if (!ctx || !key)
        return NULL;
    for (uint32_t i = 0; i < ctx->num_params; i++) {
        if (strcmp(ctx->param_keys[i], key) == 0)
            return ctx->param_values[i];
    }
    return NULL;
}

int64_t
wirelog_io_ctx_intern_string(wirelog_io_ctx_t *ctx, const char *utf8)
{
    if (!ctx || !ctx->intern)
        return -1;
    int64_t id = wl_intern_put(ctx->intern, utf8);
    if (id < 0)
        return -1;
    /* Return 1-based IDs so callers can treat 0 as invalid/absent. */
    return id + 1;
}

void *
wirelog_io_ctx_platform(const wirelog_io_ctx_t *ctx)
{
    if (!ctx)
        return NULL;
    return ctx->platform_ctx;
}

int
wirelog_io_ctx_set_platform(wirelog_io_ctx_t *ctx, void *ptr)
{
    if (!ctx)
        return -1;
    ctx->platform_ctx = ptr;
    return 0;
}
