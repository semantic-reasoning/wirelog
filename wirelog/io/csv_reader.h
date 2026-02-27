/*
 * csv_reader.h - wirelog CSV Reader
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 * Minimal CSV parser for loading EDB facts from .input directives.
 */

#ifndef WIRELOG_IO_CSV_READER_H
#define WIRELOG_IO_CSV_READER_H

#include <stdint.h>

/**
 * wl_csv_parse_line:
 * @line:      NUL-terminated line of text (no trailing newline expected).
 * @delimiter: Field separator character (e.g. ',' or '\t').
 * @values:    Output buffer for parsed int64_t values.
 * @max_cols:  Capacity of @values buffer.
 * @count:     (out) Number of values parsed.
 *
 * Parse a single CSV line into an array of int64_t values.
 * Whitespace around values is trimmed.  Empty lines produce count=0.
 *
 * Returns:
 *    0: Success.
 *   -1: Invalid arguments (NULL pointers).
 *   -2: Too many columns (exceeds @max_cols).
 */
int
wl_csv_parse_line(const char *line, char delimiter, int64_t *values,
                  uint32_t max_cols, uint32_t *count);

#endif /* WIRELOG_IO_CSV_READER_H */
