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

/**
 * wl_csv_read_file:
 * @path:      Path to the CSV file.
 * @delimiter: Field separator character.
 * @data:      (out) Allocated row-major int64_t array (caller frees).
 * @nrows:     (out) Number of rows read.
 * @ncols:     (out) Number of columns (from first non-empty line).
 *
 * Read an entire CSV file into a flat int64_t array.  Empty lines are
 * skipped.  All non-empty lines must have the same number of columns.
 *
 * Returns:
 *    0: Success.
 *   -1: Invalid arguments or file open error.
 *   -2: Parse error or inconsistent column count.
 *   -3: Memory allocation failure.
 */
int
wl_csv_read_file(const char *path, char delimiter, int64_t **data,
                 uint32_t *nrows, uint32_t *ncols);

#endif /* WIRELOG_IO_CSV_READER_H */
