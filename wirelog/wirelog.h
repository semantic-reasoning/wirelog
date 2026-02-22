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

#define WIRELOG_VERSION \
    (WIRELOG_VERSION_MAJOR * 10000 + \
     WIRELOG_VERSION_MINOR * 100 + \
     WIRELOG_VERSION_PATCH)

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

wirelog_program_t*
wirelog_parse(const char *filename, wirelog_error_t *error);

wirelog_program_t*
wirelog_parse_string(const char *program_text, wirelog_error_t *error);

void
wirelog_program_free(wirelog_program_t *program);

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

wirelog_executor_t*
wirelog_executor_create(wirelog_program_t *program, wirelog_error_t *error);

void
wirelog_executor_free(wirelog_executor_t *executor);

bool
wirelog_load_facts_from_csv(wirelog_executor_t *executor,
                            const char *relation_name,
                            const char *csv_file,
                            wirelog_error_t *error);

wirelog_result_t*
wirelog_evaluate(wirelog_executor_t *executor, wirelog_error_t *error);

/* ======================================================================== */
/* Result API                                                               */
/* ======================================================================== */

const void*
wirelog_result_get_relation(const wirelog_result_t *result,
                            const char *relation_name);

uint64_t
wirelog_result_relation_cardinality(const wirelog_result_t *result,
                                    const char *relation_name);

bool
wirelog_result_write_csv(const wirelog_result_t *result,
                         const char *relation_name,
                         const char *output_file,
                         wirelog_error_t *error);

void
wirelog_result_free(wirelog_result_t *result);

/* ======================================================================== */
/* Utility API                                                              */
/* ======================================================================== */

const char*
wirelog_version_string(void);

const char*
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
