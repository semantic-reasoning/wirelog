/*
 * io_ctx_internal.h - I/O Context Internal Definition
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * INTERNAL HEADER - not installed, not part of public API.
 * Defines the concrete wl_io_ctx struct so that the opaque typedef
 * in io_adapter.h is preserved for library consumers.
 *
 * Part of #446 (I/O adapter umbrella).
 */

#ifndef WIRELOG_IO_IO_CTX_INTERNAL_H
#define WIRELOG_IO_IO_CTX_INTERNAL_H

#include "wirelog/io/io_adapter.h"
#include "wirelog/intern.h"
#include "wirelog/ir/program.h"  /* for wl_ir_relation_info_t */

struct wl_io_ctx {
    const char             *relation_name;
    uint32_t num_cols;
    wirelog_column_type_t  *col_types;       /* owned copy */
    const char            **param_keys;      /* borrowed */
    const char            **param_values;    /* borrowed */
    uint32_t num_params;
    wl_intern_t            *intern;          /* borrowed, not owned */
    void                   *platform_ctx;
};

wl_io_ctx_t *wl_io_ctx_create_test(
    const char *relation_name,
    const wirelog_column_type_t *col_types, uint32_t num_cols,
    const char **param_keys, const char **param_values, uint32_t num_params,
    wl_intern_t *intern);

void wl_io_ctx_destroy(wl_io_ctx_t *ctx);

wl_io_ctx_t *wl_io_ctx_create_for_relation(
    const wl_ir_relation_info_t *rel,
    wl_intern_t *intern);

#endif /* WIRELOG_IO_IO_CTX_INTERNAL_H */
