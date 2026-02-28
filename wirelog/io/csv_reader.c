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

/* ======================================================================== */
/* Extended Line Parser (mixed int/string)                                  */
/* ======================================================================== */

int
wl_csv_parse_line_ex(const char *line, char delimiter,
                     const wirelog_column_type_t *col_types, uint32_t num_cols,
                     int64_t *values, uint32_t *count, wl_intern_t *intern)
{
    if (!line || !col_types || !values || !count || num_cols == 0)
        return -1;

    *count = 0;
    const char *p = line;

    for (uint32_t col = 0; col < num_cols; col++) {
        /* Skip leading whitespace */
        while (*p && *p != delimiter && *p != '"' && isspace((unsigned char)*p))
            p++;

        if (*p == '\0' && col < num_cols - 1)
            return -2; /* too few columns */

        if (col_types[col] == WIRELOG_TYPE_STRING) {
            /* Parse string field: quoted or unquoted */
            char strbuf[4096];
            size_t slen = 0;

            if (*p == '"') {
                /* Quoted field: read until closing quote */
                p++; /* skip opening quote */
                while (*p && *p != '"') {
                    if (slen < sizeof(strbuf) - 1)
                        strbuf[slen++] = *p;
                    p++;
                }
                if (*p == '"')
                    p++; /* skip closing quote */
            } else {
                /* Unquoted field: read until delimiter or end */
                while (*p && *p != delimiter && *p != '\n' && *p != '\r') {
                    if (slen < sizeof(strbuf) - 1)
                        strbuf[slen++] = *p;
                    p++;
                }
                /* Trim trailing whitespace */
                while (slen > 0 && isspace((unsigned char)strbuf[slen - 1]))
                    slen--;
            }
            strbuf[slen] = '\0';

            if (!intern)
                return -1;
            values[col] = wl_intern_put(intern, strbuf);
            if (values[col] < 0)
                return -1;
        } else {
            /* Parse integer field */
            char *end;
            values[col] = strtoll(p, &end, 10);
            if (end == p)
                return -1; /* not a valid integer */
            p = end;

            /* Skip trailing whitespace */
            while (*p && *p != delimiter && isspace((unsigned char)*p))
                p++;
        }

        (*count)++;

        /* Skip delimiter between fields */
        if (col < num_cols - 1) {
            if (*p == delimiter)
                p++;
        }
    }

    return 0;
}
