/*
 * csv_reader.c - wirelog CSV Reader
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Minimal CSV parser for loading EDB facts from .input directives.
 */

#include "csv_reader.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int
wl_csv_parse_line(const char *line, char delimiter, int64_t *values,
                  uint32_t max_cols, uint32_t *count)
{
    if (!line || !values || !count)
        return -1;

    *count = 0;

    /* Skip leading whitespace (but not the delimiter itself) */
    while (*line && *line != delimiter && isspace((unsigned char)*line))
        line++;

    /* Empty line */
    if (*line == '\0')
        return 0;

    const char *p = line;
    while (*p) {
        /* Skip whitespace before value (but not the delimiter) */
        while (*p && *p != delimiter && isspace((unsigned char)*p))
            p++;

        if (*p == '\0')
            break;

        if (*count >= max_cols)
            return -2;

        char *end;
        int64_t val = strtoll(p, &end, 10);

        if (end == p)
            return -1; /* not a valid integer */

        values[*count] = val;
        (*count)++;

        /* Skip whitespace after value (but not the delimiter) */
        while (*end && *end != delimiter && isspace((unsigned char)*end))
            end++;

        if (*end == delimiter) {
            p = end + 1;
        } else if (*end == '\0' || *end == '\n' || *end == '\r') {
            break;
        } else {
            return -1; /* unexpected character */
        }
    }

    return 0;
}

/* Initial capacity for row buffer */
#define CSV_INITIAL_CAPACITY 64
#define CSV_MAX_COLS 256
#define CSV_LINE_BUF 4096

int
wl_csv_read_file(const char *path, char delimiter, int64_t **data,
                 uint32_t *nrows, uint32_t *ncols)
{
    if (!path || !data || !nrows || !ncols)
        return -1;

    FILE *f = fopen(path, "r");
    if (!f)
        return -1;

    *data = NULL;
    *nrows = 0;
    *ncols = 0;

    uint32_t capacity = CSV_INITIAL_CAPACITY;
    uint32_t cols_expected = 0;
    int64_t *buf = NULL;
    char line[CSV_LINE_BUF];

    while (fgets(line, sizeof(line), f)) {
        /* Strip trailing newline */
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r'))
            line[--len] = '\0';

        /* Skip empty lines */
        if (len == 0)
            continue;

        int64_t row_values[CSV_MAX_COLS];
        uint32_t col_count = 0;
        int rc = wl_csv_parse_line(line, delimiter, row_values, CSV_MAX_COLS,
                                   &col_count);
        if (rc != 0 || col_count == 0) {
            free(buf);
            fclose(f);
            return -2;
        }

        /* First row determines column count */
        if (*nrows == 0) {
            cols_expected = col_count;
            buf = (int64_t *)malloc((size_t)capacity * cols_expected
                                    * sizeof(int64_t));
            if (!buf) {
                fclose(f);
                return -3;
            }
        } else if (col_count != cols_expected) {
            free(buf);
            fclose(f);
            return -2; /* inconsistent column count */
        }

        /* Grow buffer if needed */
        if (*nrows >= capacity) {
            capacity *= 2;
            int64_t *tmp = (int64_t *)realloc(
                buf, (size_t)capacity * cols_expected * sizeof(int64_t));
            if (!tmp) {
                free(buf);
                fclose(f);
                return -3;
            }
            buf = tmp;
        }

        /* Copy row into flat array */
        memcpy(&buf[(size_t)*nrows * cols_expected], row_values,
               (size_t)cols_expected * sizeof(int64_t));
        (*nrows)++;
    }

    fclose(f);

    *data = buf;
    *ncols = cols_expected;
    return 0;
}
