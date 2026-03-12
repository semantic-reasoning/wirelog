/*
 * session_facts.c - wirelog Fact-Loading Session Helpers
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Backend-agnostic fact-loading helpers that replace the deleted
 * DD-specific wirelog_load_all_facts() and wirelog_load_input_files().
 */

#include "session_facts.h"

#include "io/csv_reader.h"
#include "ir/program.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int
wl_session_load_facts(wl_session_t *sess, const struct wirelog_program *prog)
{
    if (!sess || !prog)
        return -1;

    for (uint32_t i = 0; i < prog->relation_count; i++) {
        const wl_ir_relation_info_t *rel = &prog->relations[i];
        if (!rel->name || rel->fact_count == 0 || !rel->fact_data)
            continue;

        int rc = wl_session_insert(sess, rel->name, rel->fact_data,
                                   rel->fact_count, rel->column_count);
        if (rc != 0) {
            fprintf(stderr, "error: failed to load facts for '%s'\n",
                    rel->name);
            return -1;
        }
    }

    return 0;
}

int
wl_session_load_input_files(wl_session_t *sess,
                            const struct wirelog_program *prog)
{
    if (!sess || !prog)
        return -1;

    for (uint32_t i = 0; i < prog->relation_count; i++) {
        const wl_ir_relation_info_t *rel = &prog->relations[i];
        if (!rel->name || !rel->has_input)
            continue;

        /* Extract filename parameter */
        const char *filename = NULL;
        char delimiter = '\t'; /* default delimiter */

        for (uint32_t p = 0; p < rel->input_param_count; p++) {
            if (rel->input_param_names[p]
                && strcmp(rel->input_param_names[p], "filename") == 0) {
                filename = rel->input_param_values[p];
            } else if (rel->input_param_names[p]
                       && strcmp(rel->input_param_names[p], "delimiter") == 0
                       && rel->input_param_values[p]) {
                /* Handle escaped delimiter strings */
                const char *dv = rel->input_param_values[p];
                if (strcmp(dv, "\\t") == 0)
                    delimiter = '\t';
                else if (strlen(dv) > 0)
                    delimiter = dv[0];
            }
        }

        if (!filename) {
            fprintf(stderr, "error: .input for '%s' missing filename\n",
                    rel->name);
            return -1;
        }

        /* Try to resolve file path:
         * 1. If absolute path, use as-is
         * 2. If relative path, try from current working directory
         * 3. Try relative to cwd (fopen will do this automatically)
         */
        const char *resolved_path = filename;
        char resolved_buf[4096];

        /* Check if file exists at the given path */
        FILE *test_f = fopen(filename, "r");
        if (!test_f && filename[0] != '/') {
            /* Relative path failed, try with current working directory */
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

        /* Read CSV file */
        int64_t *data = NULL;
        uint32_t nrows = 0;
        uint32_t ncols = 0;

        /* Check if relation has any string/symbol columns */
        bool has_string_cols = false;
        for (uint32_t c = 0; c < rel->column_count; c++) {
            if (rel->columns[c].type == WIRELOG_TYPE_STRING) {
                has_string_cols = true;
                break;
            }
        }

        int rc;
        if (has_string_cols) {
            /* Use extended reader for mixed types */
            if (!prog->intern) {
                fprintf(stderr,
                        "error: no intern table available for relation '%s'\n",
                        rel->name);
                return -1;
            }

            /* Extract column types from relation */
            wirelog_column_type_t *col_types = (wirelog_column_type_t *)malloc(
                rel->column_count * sizeof(wirelog_column_type_t));
            if (!col_types) {
                fprintf(stderr, "error: memory allocation failed\n");
                return -1;
            }
            for (uint32_t c = 0; c < rel->column_count; c++) {
                col_types[c] = rel->columns[c].type;
            }

            rc = wl_csv_read_file_ex(resolved_path, delimiter, col_types,
                                     rel->column_count, &data, &nrows, &ncols,
                                     prog->intern);
            free(col_types);
        } else {
            /* Use basic reader for integer-only columns */
            rc = wl_csv_read_file(resolved_path, delimiter, &data, &nrows,
                                  &ncols);
        }

        if (rc != 0) {
            fprintf(stderr, "error: failed to read '%s' for relation '%s'\n",
                    filename, rel->name);
            return -1;
        }

        if (nrows > 0 && data) {
            rc = wl_session_insert(sess, rel->name, data, nrows, ncols);
            free(data);
            if (rc != 0) {
                fprintf(stderr,
                        "error: failed to insert data from '%s' for '%s'\n",
                        filename, rel->name);
                return -1;
            }
        } else {
            free(data);
        }
    }

    return 0;
}
