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

#include <stddef.h>
#include <stdint.h>

/**
 * WL_CSV_MAX_LINE:
 *
 * Hard ceiling on the length of a single physical line, in bytes.
 *
 * The readers assemble each line into a growable buffer, so the only
 * thing standing between a malformed file and unbounded allocation is
 * this limit.  Exceeding it is an error (%WL_CSV_ERR_LINE_TOO_LONG);
 * the alternative -- reading as much as fits and treating the tail as
 * the next line -- fabricates tuples, which is the bug this ceiling
 * exists to avoid re-introducing (#953).
 *
 * The check is applied before the buffer grows, so a line ten times
 * this long is refused having allocated at most this much.
 */
#define WL_CSV_MAX_LINE ((size_t)16 * 1024 * 1024)

/**
 * WL_CSV_READ_CHUNK:
 *
 * Size of the staging buffer each reader pulls file bytes into.  A line
 * lying wholly inside one chunk is parsed in place; only a line
 * straddling a chunk boundary is copied into the growable spill buffer.
 *
 * Exposed so that tests can pin the framing behaviour at exactly this
 * boundary rather than at a hard-coded number that would silently stop
 * being a boundary if the chunk size were retuned.
 */
#define WL_CSV_READ_CHUNK ((size_t)65536)

/**
 * Return codes shared by the CSV readers and parsers.
 */
#define WL_CSV_OK 0
#define WL_CSV_ERR_ARGS (-1)          /* NULL args, open failure, bad field */
#define WL_CSV_ERR_PARSE (-2)         /* column count mismatch / capacity */
#define WL_CSV_ERR_MEMORY (-3)        /* allocation failure */
#define WL_CSV_ERR_LINE_TOO_LONG (-4) /* line exceeds WL_CSV_MAX_LINE */
#define WL_CSV_ERR_EMBEDDED_NUL (-5)  /* NUL byte inside a line */

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
 * Lines of any length are read whole, up to %WL_CSV_MAX_LINE.
 *
 * Returns:
 *    0: Success.
 *   -1: Invalid arguments, file open error, or read error.
 *   -2: Parse error or inconsistent column count.
 *   -3: Memory allocation failure.
 *   -4: A line exceeds %WL_CSV_MAX_LINE.
 *   -5: A line contains an embedded NUL byte.
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
 * @values:    Output buffer for parsed int64_t values.
 * @max_cols:  Capacity of @values.  @num_cols > @max_cols is rejected
 *             rather than written past (#997), as in wl_csv_parse_line().
 * @count:     (out) Number of values parsed.
 * @intern:    Intern table for string columns (must not be NULL if any
 *             column is STRING).
 *
 * Parse a CSV line with mixed integer and string columns.  Integer
 * columns are parsed as int64_t; string columns are interned and
 * stored as int64_t IDs.  Quoted strings (double-quote delimited) are
 * supported; quotes are stripped before interning.
 *
 * There is no upper bound on @num_cols beyond the buffer the caller
 * supplies.
 *
 * @line is never modified and string fields of any length are interned
 * whole; the scratch buffer used to NUL-terminate each field is
 * allocated and released internally (#953).  Callers reading a whole
 * file should use wl_csv_read_file_ex()/wl_csv_read_file_via_ctx(),
 * which reuse one scratch buffer across every line.
 *
 * Returns:
 *    0: Success.
 *   -1: Invalid arguments or parse error.
 *   -2: Column count mismatch, or @num_cols exceeds @max_cols.
 *   -3: Memory allocation failure.
 */
int
wl_csv_parse_line_ex(const char *line, char delimiter,
    const wirelog_column_type_t *col_types, uint32_t num_cols,
    int64_t *values, uint32_t max_cols, uint32_t *count, wl_intern_t *intern);

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
 * must have exactly @num_cols fields.  Lines and fields of any length
 * are read whole, up to %WL_CSV_MAX_LINE.
 *
 * @num_cols is not capped: the row buffer is sized from it once per file
 * and the parser is told that size as its capacity (#997).  Contrast
 * wl_csv_read_file(), which auto-detects its width and therefore still
 * parses into a fixed 256-column frame array.
 *
 * Returns:
 *    0: Success.
 *   -1: Invalid arguments, file open error, or read error.
 *   -2: Parse error or inconsistent column count.
 *   -3: Memory allocation failure.
 *   -4: A line exceeds %WL_CSV_MAX_LINE.
 *   -5: A line contains an embedded NUL byte.
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
