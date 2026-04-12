/*
 * csv_adapter.c - Built-in CSV/TSV I/O Adapter
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
 *
 * Wraps wl_csv_read_file / wl_csv_read_file_via_ctx behind the
 * wl_io_adapter_t vtable for auto-registration in the I/O registry.
 *
 * Part of #446 (I/O adapter umbrella).
 */

#include "wirelog/io/io_adapter.h"
#include "wirelog/io/io_ctx_internal.h"
#include "wirelog/io/csv_reader.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#if !defined(_WIN32) && !defined(_WIN64)
#include <unistd.h>
#else
#include <direct.h>
#define getcwd _getcwd
#endif

/* ======================================================================== */
/* Intern Trampoline                                                        */
/* ======================================================================== */

/*
 * The built-in CSV adapter calls wl_intern_put directly (0-based IDs)
 * to stay consistent with the legacy wl_csv_read_file_ex path.
 * External adapters should use wl_io_ctx_intern_string (1-based) instead.
 */
static int64_t
csv_intern_trampoline(void *opaque, const char *str)
{
    wl_io_ctx_t *ctx = (wl_io_ctx_t *)opaque;
    if (!ctx || !ctx->intern)
        return -1;
    return wl_intern_put(ctx->intern, str);
}

/* ======================================================================== */
/* csv_read callback                                                        */
/* ======================================================================== */

static int
csv_read(wl_io_ctx_t *ctx, int64_t **out_data,
    uint32_t *out_nrows, void *user_data)
{
    (void)user_data;

    /* ---- filename (required) ---- */
    const char *filename = wl_io_ctx_param(ctx, "filename");
    if (!filename)
        return -1;

    /* ---- delimiter (default: tab) ---- */
    char delimiter = '\t';
    const char *delim_str = wl_io_ctx_param(ctx, "delimiter");
    if (delim_str) {
        if (strcmp(delim_str, "\\t") == 0)
            delimiter = '\t';
        else
            delimiter = delim_str[0];
    }

    /* ---- path resolution (absolute, then cwd-fallback) ---- */
    const char *resolved_path = filename;
    char resolved_buf[4096];

    FILE *test_f = fopen(filename, "r");
    if (!test_f && filename[0] != '/') {
        char cwd[4096];
        if (getcwd(cwd, sizeof(cwd)) != NULL) {
            snprintf(resolved_buf, sizeof(resolved_buf), "%s/%s", cwd,
                filename);
            test_f = fopen(resolved_buf, "r");
            if (test_f) {
                resolved_path = resolved_buf;
                fclose(test_f);
            }
        }
    } else if (test_f) {
        fclose(test_f);
    }

    /* ---- check for STRING columns ---- */
    uint32_t num_cols = wl_io_ctx_num_cols(ctx);
    int has_string = 0;
    for (uint32_t i = 0; i < num_cols; i++) {
        if (wl_io_ctx_col_type(ctx, i) == WIRELOG_TYPE_STRING) {
            has_string = 1;
            break;
        }
    }

    if (has_string) {
        /* Build col_types array from context */
        wirelog_column_type_t *types = malloc(num_cols * sizeof(*types));
        if (!types)
            return -1;
        for (uint32_t i = 0; i < num_cols; i++)
            types[i] = wl_io_ctx_col_type(ctx, i);

        uint32_t out_ncols = 0;
        int rc = wl_csv_read_file_via_ctx(resolved_path, delimiter,
                types, num_cols,
                out_data, out_nrows, &out_ncols,
                csv_intern_trampoline, ctx);
        free(types);
        return rc;
    }

    /* Integer-only path */
    uint32_t out_ncols = 0;
    return wl_csv_read_file(resolved_path, delimiter,
               out_data, out_nrows, &out_ncols);
}

/* ======================================================================== */
/* Adapter Definition                                                       */
/* ======================================================================== */

const wl_io_adapter_t wl_csv_adapter = {
    .abi_version = WL_IO_ABI_VERSION,
    .scheme = "csv",
    .description = "Built-in CSV/TSV file reader",
    .read = csv_read,
    .validate = NULL,
    .user_data = NULL,
};
