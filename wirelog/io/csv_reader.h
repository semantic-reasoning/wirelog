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

#include "../wirelog-types.h"
#include "../intern.h"

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

/**
 * wl_csv_parse_line_ex:
 * @line:      NUL-terminated line of text.
 * @delimiter: Field separator character.
 * @col_types: Array of column types (STRING columns are interned).
 * @num_cols:  Number of columns expected (length of @col_types).
 * @values:    Output buffer for parsed int64_t values (length >= @num_cols).
 * @count:     (out) Number of values parsed.
 * @intern:    Intern table for string columns (must not be NULL if any
 *             column is STRING).
 *
 * Parse a CSV line with mixed integer and string columns.  Integer
 * columns are parsed as int64_t; string columns are interned and
 * stored as int64_t IDs.  Quoted strings (double-quote delimited) are
 * supported; quotes are stripped before interning.
 *
 * Returns:
 *    0: Success.
 *   -1: Invalid arguments or parse error.
 *   -2: Column count mismatch.
 */
int
wl_csv_parse_line_ex(const char *line, char delimiter,
    const wirelog_column_type_t *col_types, uint32_t num_cols,
    int64_t *values, uint32_t *count, wl_intern_t *intern);

/**
 * wl_csv_read_file_ex:
 * @path:      Path to the CSV file.
 * @delimiter: Field separator character.
 * @col_types: Array of column types (mixed int/string support).
 * @num_cols:  Number of columns expected.
 * @data:      (out) Allocated row-major int64_t array (caller frees).
 * @nrows:     (out) Number of rows read.
 * @ncols:     (out) Number of columns (from @num_cols).
 * @intern:    Intern table for string columns (must not be NULL).
 *
 * Read a CSV file with mixed integer and string columns. String columns
 * are interned; integer columns are parsed as int64_t. All non-empty lines
 * must have exactly @num_cols fields.
 *
 * Returns:
 *    0: Success.
 *   -1: Invalid arguments or file open error.
 *   -2: Parse error or inconsistent column count.
 *   -3: Memory allocation failure.
 */
int
wl_csv_read_file_ex(const char *path, char delimiter,
    const wirelog_column_type_t *col_types, uint32_t num_cols,
    int64_t **data, uint32_t *nrows, uint32_t *ncols,
    wl_intern_t *intern);

/**
 * wl_csv_read_file_via_ctx:
 * Callback-based variant for I/O adapter integration (#455).
 * @intern_cb: called for each STRING cell, must return an int64_t id.
 * @opaque:    passed verbatim to intern_cb.
 *
 * Otherwise identical to wl_csv_read_file_ex.
 * DO NOT DELETE -- Path A rollback target
 */
int
wl_csv_read_file_via_ctx(
    const char *filepath,
    char delimiter,
    const wirelog_column_type_t *col_types,
    uint32_t num_cols,
    int64_t **out_data,
    uint32_t *out_nrows,
    uint32_t *out_ncols,
    int64_t (*intern_cb)(void *opaque, const char *str),
    void *opaque);

#endif /* WIRELOG_IO_CSV_READER_H */
