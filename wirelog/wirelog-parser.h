/*
 * wirelog-parser.h - wirelog Parser API
 *
 * Copyright (C) CleverPlant
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Licensed under LGPL-3.0-or-later
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

#ifndef WIRELOG_PARSER_H
#define WIRELOG_PARSER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "wirelog.h"
#include "wirelog-types.h"

/* ======================================================================== */
/* Parser Error Information                                                 */
/* ======================================================================== */

/**
 * wirelog_parse_error_t:
 *
 * Detailed parse error information
 */
typedef struct {
    wirelog_error_t error_code;
    const char *message;
    uint32_t line;
    uint32_t column;
    const char *source;
} wirelog_parse_error_t;

/* ======================================================================== */
/* Parsing                                                                  */
/* ======================================================================== */
/* Note: wirelog_parse() and wirelog_parse_string() live in wirelog.h —
 * this header only adds the error-info variant on top of the core API. */

/**
 * wirelog_parse_with_error_info:
 * @filename: Path to .dl file
 * @error_info: (out): Detailed error information
 *
 * Parse a Datalog program with detailed error reporting.
 *
 * Returns: (transfer full): Parsed program, or NULL on error
 */
wirelog_program_t *
wirelog_parse_with_error_info(const char *filename,
    wirelog_parse_error_t *error_info);

/* ======================================================================== */
/* Program Inspection                                                       */
/* ======================================================================== */

/**
 * wirelog_program_get_stratum_count:
 * @program: Parsed program
 *
 * Get the number of strata in the program.
 *
 * Returns: Number of strata (always >= 1)
 */
uint32_t
wirelog_program_get_stratum_count(const wirelog_program_t *program);

/**
 * wirelog_program_get_stratum:
 * @program: Parsed program
 * @stratum_id: Stratum index (0-based)
 *
 * Get stratum information.
 *
 * Returns: (transfer none): Stratum info, or NULL if invalid ID
 */
const wirelog_stratum_t *
wirelog_program_get_stratum(const wirelog_program_t *program,
    uint32_t stratum_id);

/**
 * wirelog_program_get_rule_count:
 * @program: Parsed program
 *
 * Get the total number of rules in the program.
 *
 * Returns: Total rule count
 */
uint32_t
wirelog_program_get_rule_count(const wirelog_program_t *program);

/**
 * wirelog_program_get_schema:
 * @program: Parsed program
 * @relation_name: Relation to query
 *
 * Get the schema (column definitions) of a relation.
 *
 * Returns: (transfer none): Schema info, or NULL if not found
 */
const wirelog_schema_t *
wirelog_program_get_schema(const wirelog_program_t *program,
    const char *relation_name);

/**
 * wirelog_program_is_stratified:
 * @program: Parsed program
 *
 * Check if the program is stratified (negation-safe).
 *
 * Returns: true if stratified, false otherwise
 */
bool
wirelog_program_is_stratified(const wirelog_program_t *program);

/* Note: wirelog_program_free() lives in wirelog.h. */

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_PARSER_H */
