/*
 * io_ctx.c - I/O Context Accessors
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Implements the wl_io_ctx_t accessor functions declared in
 * io_adapter.h (#451) and the test constructor/destructor from
 * io_ctx_internal.h.
 *
 * Part of #446 (I/O adapter umbrella).
 */

#include "wirelog/io/io_ctx_internal.h"

#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------------ */
/* Test Constructor / Destructor                                            */
/* ------------------------------------------------------------------------ */

wl_io_ctx_t *
wl_io_ctx_create_test(const char *relation_name,
    const wirelog_column_type_t *col_types, uint32_t num_cols,
    const char **param_keys, const char **param_values,
    uint32_t num_params, wl_intern_t *intern)
{
    wl_io_ctx_t *ctx = (wl_io_ctx_t *)calloc(1, sizeof(*ctx));
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
wl_io_ctx_destroy(wl_io_ctx_t *ctx)
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
wl_io_ctx_relation_name(const wl_io_ctx_t *ctx)
{
    if (!ctx)
        return NULL;
    return ctx->relation_name;
}

uint32_t
wl_io_ctx_num_cols(const wl_io_ctx_t *ctx)
{
    if (!ctx)
        return 0;
    return ctx->num_cols;
}

wirelog_column_type_t
wl_io_ctx_col_type(const wl_io_ctx_t *ctx, uint32_t col)
{
    if (!ctx || col >= ctx->num_cols)
        return (wirelog_column_type_t)-1;
    return ctx->col_types[col];
}

const char *
wl_io_ctx_param(const wl_io_ctx_t *ctx, const char *key)
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
wl_io_ctx_intern_string(wl_io_ctx_t *ctx, const char *utf8)
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
wl_io_ctx_platform(const wl_io_ctx_t *ctx)
{
    if (!ctx)
        return NULL;
    return ctx->platform_ctx;
}

int
wl_io_ctx_set_platform(wl_io_ctx_t *ctx, void *ptr)
{
    if (!ctx)
        return -1;
    ctx->platform_ctx = ptr;
    return 0;
}
