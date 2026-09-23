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
 * wirelog_io_adapter_t vtable for auto-registration in the I/O registry.
 *
 * Part of #446 (I/O adapter umbrella).
 */

#include "wirelog/io/io_adapter.h"
#include "wirelog/io/io_ctx_internal.h"
#include "wirelog/io/csv_reader.h"
#include "wirelog/io/csv_adapter_internal.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
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
 * External adapters should use wirelog_io_ctx_intern_string (1-based) instead.
 */
static int
csv_intern_trampoline(void *opaque, const char *str, int64_t *out_id)
{
    wirelog_io_ctx_t *ctx = (wirelog_io_ctx_t *)opaque;
    if (!ctx || !ctx->intern || !out_id)
        return EINVAL;
    return wl_intern_put_checked(ctx->intern, str, out_id);
}

static const char *
csv_resolve_path(wirelog_io_ctx_t *ctx, char *resolved_buf,
    size_t resolved_size)
{
    const char *filename = wirelog_io_ctx_param(ctx, "filename");
    if (!filename)
        return NULL;

    FILE *test_f = fopen(filename, "r");
    if (!test_f && filename[0] != '/') {
        char cwd[4096];
        if (getcwd(cwd, sizeof(cwd)) != NULL) {
            snprintf(resolved_buf, resolved_size, "%s/%s", cwd, filename);
            test_f = fopen(resolved_buf, "r");
            if (test_f) {
                fclose(test_f);
                return resolved_buf;
            }
        }
    } else if (test_f) {
        fclose(test_f);
    }
    return filename;
}

static char
csv_delimiter(wirelog_io_ctx_t *ctx)
{
    const char *delim_str = wirelog_io_ctx_param(ctx, "delimiter");
    if (!delim_str)
        return '	';
    return strcmp(delim_str, "\\t") == 0 ? '	': delim_str[0];
}

static int
csv_types(wirelog_io_ctx_t *ctx, wirelog_column_type_t **out_types,
    wl_columnar_memory_reservation_t *reservation, bool *reserved)
{
    if (!ctx || !out_types || !reservation || !reserved)
        return WL_CSV_ERR_ARGS;

    uint32_t num_cols = wirelog_io_ctx_num_cols(ctx);
    uint64_t types_bytes;
    *out_types = NULL;
    *reserved = false;
    if (num_cols == 0
        || !wl_columnar_memory_size_mul(num_cols,
        sizeof(wirelog_column_type_t), &types_bytes)
        || types_bytes > SIZE_MAX)
        return WL_CSV_ERR_OVERFLOW;

    if (ctx->memory_governor) {
        wl_columnar_memory_admission_status_t status
            = wl_columnar_memory_reserve_checked(ctx->memory_governor,
                types_bytes, reservation);
        if (status != WL_COLUMNAR_MEMORY_ADMISSION_OK
            && status != WL_COLUMNAR_MEMORY_ADMISSION_ADVISORY)
            return status == WL_COLUMNAR_MEMORY_ADMISSION_DENIED
                ? WL_CSV_ERR_BUDGET
                : status == WL_COLUMNAR_MEMORY_ADMISSION_OVERFLOW
                ? WL_CSV_ERR_OVERFLOW : WL_CSV_ERR_ARGS;
        *reserved = true;
    }

    wirelog_column_type_t *types = (wirelog_column_type_t *)malloc(
        (size_t)types_bytes);
    if (!types) {
        if (*reserved) {
            bool released = wl_columnar_memory_release(reservation);
            *reserved = false;
            if (!released)
                abort();
        }
        return WL_CSV_ERR_MEMORY;
    }
    for (uint32_t i = 0; i < num_cols; i++)
        types[i] = wirelog_io_ctx_col_type(ctx, i);
    *out_types = types;
    return WL_CSV_OK;
}

int
wl_csv_adapter_stream_read(wirelog_io_ctx_t *ctx, uint32_t max_batch_rows,
    wl_csv_batch_cb batch_cb, void *opaque)
{
    if (!ctx || !batch_cb || max_batch_rows == 0)
        return WL_CSV_ERR_ARGS;

    char resolved_buf[4096];
    const char *path = csv_resolve_path(ctx, resolved_buf,
            sizeof(resolved_buf));
    if (!path)
        return WL_CSV_ERR_ARGS;

    wirelog_column_type_t *types = NULL;
    wl_columnar_memory_reservation_t types_reservation;
    bool types_reserved = false;
    wl_columnar_memory_reservation_init(&types_reservation);
    int rc = csv_types(ctx, &types, &types_reservation, &types_reserved);
    if (rc == WL_CSV_OK) {
        rc = wl_csv_read_file_via_ctx_stream_admitted(path, csv_delimiter(ctx),
                types,
                wirelog_io_ctx_num_cols(ctx), max_batch_rows, batch_cb, opaque,
                csv_intern_trampoline, ctx, ctx->memory_governor);
    }
    free(types);
    if (types_reserved && !wl_columnar_memory_release(&types_reservation))
        abort();
    return rc;
}

/* ======================================================================== */
/* csv_read callback                                                        */
/* ======================================================================== */

static int
csv_read(wirelog_io_ctx_t *ctx, int64_t **out_data,
    uint32_t *out_nrows, void *user_data)
{
    (void)user_data;

    /* ---- filename (required) ---- */
    const char *filename = wirelog_io_ctx_param(ctx, "filename");
    if (!filename)
        return -1;

    /* ---- delimiter (default: tab) ---- */
    char delimiter = '\t';
    const char *delim_str = wirelog_io_ctx_param(ctx, "delimiter");
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
    uint32_t num_cols = wirelog_io_ctx_num_cols(ctx);
    int has_string = 0;
    for (uint32_t i = 0; i < num_cols; i++) {
        if (wirelog_io_ctx_col_type(ctx, i) == WIRELOG_TYPE_STRING
            || wirelog_io_ctx_col_type(ctx, i) == WIRELOG_TYPE_FLOAT) {
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
            types[i] = wirelog_io_ctx_col_type(ctx, i);

        uint32_t out_ncols = 0;
        int rc = wl_csv_read_file_via_ctx(resolved_path, delimiter,
                types, num_cols,
                out_data, out_nrows, &out_ncols,
                csv_intern_trampoline, ctx);
        free(types);
        return rc;
    }

    /*
     * Integer-only path.
     *
     * wl_csv_read_file() takes no expected width: it auto-detects the
     * column count from the file's first line and only reports it via
     * out_ncols.  The caller (wl_session_load_input_files) then inserts
     * the rows at the relation's *declared* arity, so a file whose width
     * disagrees with the .decl is packed at one stride and read back at
     * another -- a heap over-read when the file is narrower, silently
     * re-strided tuples when it is wider (#977).
     *
     * The invariant being restored is the adapter contract itself:
     * docs/io-adapters.md requires read() to produce a buffer of
     * *out_nrows * wirelog_io_ctx_num_cols(ctx) elements, and
     * wirelog_io_ctx_num_cols() is the relation's PHYSICAL width
     * (io/io_ctx.c) -- the very stride wl_session_load_input_files()
     * inserts at.  This branch was violating the contract it publishes.
     *
     * Physical, not the declared rel->column_count: an `inline` compound
     * column occupies compound_arity slots, so a file for
     * `.decl inp(id: int64, p: pair/2 inline, s: symbol)` carries four
     * fields against three declared columns (#985).  This branch needs no
     * special case for that -- it compares the detected width against
     * num_cols and never sees column_count -- but the number it is
     * comparing against is no longer the declared one.
     *
     * -2 is the code wl_csv_read_file_via_ctx() uses for a width
     * mismatch, reused for consistency of the return value only.  Note
     * that path's own width check is presently unreachable (#982), so
     * this is not an appeal to it as a working precedent.
     *
     * An empty file yields nrows == 0 / ncols == 0 with rc == 0; there is
     * no row to mis-stride and an empty .input file is legal, so the
     * check only applies once a width has actually been detected.
     */
    uint32_t out_ncols = 0;
    int rc = wl_csv_read_file(resolved_path, delimiter,
            out_data, out_nrows, &out_ncols);
    if (rc == 0 && *out_nrows > 0 && out_ncols != num_cols) {
        free(*out_data);
        *out_data = NULL;
        *out_nrows = 0;
        return -2; /* arity mismatch */
    }
    return rc;
}

/* ======================================================================== */
/* Adapter Definition                                                       */
/* ======================================================================== */

const wirelog_io_adapter_t wl_csv_adapter = {
    .abi_version = WIRELOG_IO_ABI_VERSION,
    .scheme = "csv",
    .description = "Built-in CSV/TSV file reader",
    .read = csv_read,
    .validate = NULL,
    .user_data = NULL,
};
