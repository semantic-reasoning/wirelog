/*
 * wirelog.h - Embedded-to-Enterprise Datalog Engine
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.  If not, see
 * <https://www.gnu.org/licenses/lgpl-3.0.html>.
 */

#ifndef WIRELOG_H
#define WIRELOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* ======================================================================== */
/* Version Information                                                      */
/* ======================================================================== */

#define WIRELOG_VERSION_MAJOR 0
#define WIRELOG_VERSION_MINOR 1
#define WIRELOG_VERSION_PATCH 0

#define WIRELOG_VERSION                                          \
    (WIRELOG_VERSION_MAJOR * 10000 + WIRELOG_VERSION_MINOR * 100 \
     + WIRELOG_VERSION_PATCH)

/* ======================================================================== */
/* Type Definitions                                                         */
/* ======================================================================== */

/**
 * wirelog_program_t:
 *
 * Opaque handle to a parsed Datalog program.
 */
typedef struct wirelog_program wirelog_program_t;

/**
 * wirelog_result_t:
 *
 * Opaque handle to evaluation results.
 */
typedef struct wirelog_result wirelog_result_t;

/**
 * wirelog_executor_t:
 *
 * Opaque handle to the execution engine.
 */
typedef struct wirelog_executor wirelog_executor_t;

/**
 * wirelog_error_t:
 *
 * Error code enumeration
 */
typedef enum {
    WIRELOG_OK = 0,
    WIRELOG_ERR_PARSE = 1,
    WIRELOG_ERR_INVALID_IR = 2,
    WIRELOG_ERR_EXEC = 3,
    WIRELOG_ERR_MEMORY = 4,
    WIRELOG_ERR_IO = 5,
    WIRELOG_ERR_UNKNOWN = 255,
} wirelog_error_t;

/* ======================================================================== */
/* Parser API                                                               */
/* ======================================================================== */

wirelog_program_t *
wirelog_parse(const char *filename, wirelog_error_t *error);

wirelog_program_t *
wirelog_parse_string(const char *program_text, wirelog_error_t *error);

void
wirelog_program_free(wirelog_program_t *program);

/* ======================================================================== */
/* Symbol Interning                                                         */
/* ======================================================================== */

typedef struct wl_intern wl_intern_t;

/**
 * Get the program's symbol intern table for reverse-mapping
 * interned integer IDs back to their original strings.
 *
 * @param prog  Compiled program
 * @return Intern table (owned by program, do not free), or NULL
 */
const wl_intern_t *
wirelog_program_get_intern(const wirelog_program_t *prog);

/* ======================================================================== */
/* Fact Extraction API                                                      */
/* ======================================================================== */

/**
 * Extract inline facts for a relation from the compiled program.
 *
 * Returns a flat row-major int64_t array of all facts declared for
 * the given relation.  Caller must free the returned array.
 *
 * @param prog       Compiled program
 * @param relation   Relation name (e.g., "edge")
 * @param data       Output: allocated array of int64_t values (caller frees)
 * @param num_rows   Output: number of fact tuples
 * @param num_cols   Output: number of columns per tuple
 * @return 0 on success, -1 on error (unknown relation), 1 if no facts
 */
int
wirelog_program_get_facts(const wirelog_program_t *prog, const char *relation,
                          int64_t **data, uint32_t *num_rows,
                          uint32_t *num_cols);

/**
 * Load all inline facts from the program into a DD worker.
 *
 * Iterates over all relations with inline facts (fact_count > 0) and
 * calls wl_dd_load_edb() for each.  This bridges the parser's fact
 * storage with the DD execution engine.
 *
 * @param prog    Compiled program with inline facts
 * @param worker  DD worker handle to load facts into
 * @return 0 on success, -1 on error (NULL args or EDB load failure)
 */
int
wirelog_load_all_facts(const wirelog_program_t *prog, void *worker);

/**
 * Load CSV files for all relations with .input directives.
 *
 * Iterates over all relations with has_input=true, reads the "filename"
 * and "delimiter" parameters, parses the CSV file, and loads the data
 * into the DD worker via wl_dd_load_edb().
 *
 * @param prog    Compiled program with .input directives
 * @param worker  DD worker handle to load facts into
 * @return 0 on success, -1 on error (NULL args, missing file, parse error)
 */
int
wirelog_load_input_files(const wirelog_program_t *prog, void *worker);

/* ======================================================================== */
/* Optimizer API                                                            */
/* ======================================================================== */

bool
wirelog_optimize(wirelog_program_t *program, wirelog_error_t *error);

void
wirelog_optimizer_debug(const wirelog_program_t *program);

/* ======================================================================== */
/* Executor API                                                             */
/* ======================================================================== */

wirelog_executor_t *
wirelog_executor_create(wirelog_program_t *program, wirelog_error_t *error);

void
wirelog_executor_free(wirelog_executor_t *executor);

bool
wirelog_load_facts_from_csv(wirelog_executor_t *executor,
                            const char *relation_name, const char *csv_file,
                            wirelog_error_t *error);

wirelog_result_t *
wirelog_evaluate(wirelog_executor_t *executor, wirelog_error_t *error);

/* ======================================================================== */
/* Result API                                                               */
/* ======================================================================== */

const void *
wirelog_result_get_relation(const wirelog_result_t *result,
                            const char *relation_name);

uint64_t
wirelog_result_relation_cardinality(const wirelog_result_t *result,
                                    const char *relation_name);

bool
wirelog_result_write_csv(const wirelog_result_t *result,
                         const char *relation_name, const char *output_file,
                         wirelog_error_t *error);

void
wirelog_result_free(wirelog_result_t *result);

/* ======================================================================== */
/* Utility API                                                              */
/* ======================================================================== */

const char *
wirelog_version_string(void);

const char *
wirelog_error_string(wirelog_error_t error);

bool
wirelog_config_embedded(void);

bool
wirelog_config_ipc(void);

bool
wirelog_config_threads(void);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_H */
