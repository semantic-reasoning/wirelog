/*
 * facts_loader.c - Bulk EDB Fact Loading via Rust FFI
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Implements wirelog_load_all_facts(), which bridges the parser's inline
 * fact storage with the DD execution engine.  This file is compiled only
 * into targets that link the Rust FFI (rust_ffi_dep), keeping the core
 * IR library free of Rust dependencies.
 */

#include "dd_ffi.h"
#include "../../io/csv_reader.h"
#include "../../ir/program.h"
#include "../../wirelog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int
wirelog_load_all_facts(const wirelog_program_t *prog, void *worker)
{
    if (!prog || !worker)
        return -1;

    wl_dd_worker_t *w = (wl_dd_worker_t *)worker;

    for (uint32_t i = 0; i < prog->relation_count; i++) {
        const wl_ir_relation_info_t *rel = &prog->relations[i];
        if (rel->fact_count == 0)
            continue;

        int rc = wl_dd_load_edb(w, rel->name, rel->fact_data, rel->fact_count,
                                rel->column_count);
        if (rc != 0)
            return -1;
    }

    return 0;
}

/* Look up a parameter value by name from .input directive */
static const char *
input_param_get(const wl_ir_relation_info_t *rel, const char *name)
{
    for (uint32_t i = 0; i < rel->input_param_count; i++) {
        if (strcmp(rel->input_param_names[i], name) == 0)
            return rel->input_param_values[i];
    }
    return NULL;
}

/* Check if any column in the relation has string type */
static bool
has_string_columns(const wl_ir_relation_info_t *rel)
{
    for (uint32_t i = 0; i < rel->column_count; i++) {
        if (rel->columns[i].type == WIRELOG_TYPE_STRING)
            return true;
    }
    return false;
}

/* Build column type array from relation metadata */
static wirelog_column_type_t *
build_col_types(const wl_ir_relation_info_t *rel)
{
    wirelog_column_type_t *types = (wirelog_column_type_t *)malloc(
        rel->column_count * sizeof(wirelog_column_type_t));
    if (!types)
        return NULL;
    for (uint32_t i = 0; i < rel->column_count; i++)
        types[i] = rel->columns[i].type;
    return types;
}

#define INPUT_LINE_BUF 4096
#define INPUT_INITIAL_CAP 64

/* Load a CSV file with string columns using line-by-line parsing */
static int
load_csv_with_strings(const char *filename, char delimiter,
                      const wl_ir_relation_info_t *rel, wl_intern_t *intern,
                      int64_t **out_data, uint32_t *out_nrows)
{
    FILE *f = fopen(filename, "r");
    if (!f)
        return -1;

    wirelog_column_type_t *col_types = build_col_types(rel);
    if (!col_types) {
        fclose(f);
        return -1;
    }

    uint32_t ncols = rel->column_count;
    uint32_t capacity = INPUT_INITIAL_CAP;
    int64_t *buf
        = (int64_t *)malloc((size_t)capacity * ncols * sizeof(int64_t));
    if (!buf) {
        free(col_types);
        fclose(f);
        return -1;
    }

    uint32_t nrows = 0;
    char line[INPUT_LINE_BUF];

    while (fgets(line, sizeof(line), f)) {
        /* Strip trailing newline */
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
            line[--len] = '\0';
        if (len == 0)
            continue;

        /* Grow buffer if needed */
        if (nrows >= capacity) {
            capacity *= 2;
            int64_t *tmp = (int64_t *)realloc(buf, (size_t)capacity * ncols
                                                       * sizeof(int64_t));
            if (!tmp) {
                free(buf);
                free(col_types);
                fclose(f);
                return -1;
            }
            buf = tmp;
        }

        uint32_t count = 0;
        int rc
            = wl_csv_parse_line_ex(line, delimiter, col_types, ncols,
                                   &buf[(size_t)nrows * ncols], &count, intern);
        if (rc != 0 || count != ncols) {
            free(buf);
            free(col_types);
            fclose(f);
            return -1;
        }
        nrows++;
    }

    fclose(f);
    free(col_types);

    *out_data = buf;
    *out_nrows = nrows;
    return 0;
}

int
wirelog_load_input_files(const wirelog_program_t *prog, void *worker)
{
    if (!prog || !worker)
        return -1;

    wl_dd_worker_t *w = (wl_dd_worker_t *)worker;

    for (uint32_t i = 0; i < prog->relation_count; i++) {
        const wl_ir_relation_info_t *rel = &prog->relations[i];
        if (!rel->has_input)
            continue;

        const char *filename = input_param_get(rel, "filename");
        if (!filename)
            return -1; /* .input without filename */

        const char *delim_str = input_param_get(rel, "delimiter");
        char delimiter = ','; /* default */
        if (delim_str && delim_str[0] != '\0') {
            if (strcmp(delim_str, "\\t") == 0)
                delimiter = '\t';
            else
                delimiter = delim_str[0];
        }

        int64_t *data = NULL;
        uint32_t nrows = 0;
        int rc;

        if (has_string_columns(rel)) {
            /* Use type-aware parser for string columns */
            rc = load_csv_with_strings(filename, delimiter, rel, prog->intern,
                                       &data, &nrows);
            if (rc != 0)
                return -1;

            if (nrows > 0) {
                rc = wl_dd_load_edb(w, rel->name, data, nrows,
                                    rel->column_count);
                free(data);
                if (rc != 0)
                    return -1;
            } else {
                free(data);
            }
        } else {
            /* All-integer: use fast bulk reader */
            uint32_t ncols = 0;
            rc = wl_csv_read_file(filename, delimiter, &data, &nrows, &ncols);
            if (rc != 0)
                return -1;

            if (nrows > 0) {
                rc = wl_dd_load_edb(w, rel->name, data, nrows, ncols);
                free(data);
                if (rc != 0)
                    return -1;
            } else {
                free(data);
            }
        }
    }

    return 0;
}
