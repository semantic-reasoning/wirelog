/*
 * driver.h - wirelog CLI Driver Internal API
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 * Declares utilities used by the CLI driver executable.
 */

#ifndef WIRELOG_CLI_DRIVER_H
#define WIRELOG_CLI_DRIVER_H

#include <stdint.h>
#include <stdio.h>

/**
 * wl_read_file:
 * @path: Path to the file to read.
 *
 * Read the entire contents of a file into a NUL-terminated string.
 * Caller must free the returned string.
 *
 * Returns: Allocated string on success, NULL on error.
 */
char *
wl_read_file(const char *path);

/**
 * wl_print_tuple:
 * @relation: Relation name.
 * @row:      Array of int64_t column values.
 * @ncols:    Number of columns.
 * @out:      Output stream.
 *
 * Print a single result tuple in "relation(v1, v2, ...)" format.
 */
void
wl_print_tuple(const char *relation, const int64_t *row, uint32_t ncols,
               FILE *out);

/**
 * wl_run_pipeline:
 * @source:      Datalog source text.
 * @num_workers: Number of worker threads.
 * @out:         Output stream for result tuples.
 *
 * Run the full Datalog pipeline: parse -> optimize -> plan ->
 * load inline facts -> execute -> print results.
 *
 * Returns: 0 on success, non-zero on error.
 */
int
wl_run_pipeline(const char *source, uint32_t num_workers, FILE *out);

#endif /* WIRELOG_CLI_DRIVER_H */
