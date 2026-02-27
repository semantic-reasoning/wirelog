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
