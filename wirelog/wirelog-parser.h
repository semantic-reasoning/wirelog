/*
 * wirelog-parser.h - wirelog Parser API
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

/**
 * wirelog_parse:
 * @filename: Path to .dl file
 * @error: (out) (optional): Error code
 *
 * Parse a Datalog program from a file.
 *
 * Supported Datalog features:
 * - Facts (extensional database)
 * - Rules with recursion
 * - Negation (stratified)
 * - Aggregation (COUNT, SUM, MIN, MAX, AVG)
 * - Built-in predicates (=, <, >, <=, >=, !=)
 * - Comments (% line comments)
 *
 * Returns: (transfer full): Parsed program, or NULL on error
 */
wirelog_program_t*
wirelog_parse(const char *filename, wirelog_error_t *error);

/**
 * wirelog_parse_string:
 * @program_text: Datalog program as string
 * @error: (out) (optional): Error code
 *
 * Parse a Datalog program from a string.
 *
 * Returns: (transfer full): Parsed program, or NULL on error
 */
wirelog_program_t*
wirelog_parse_string(const char *program_text, wirelog_error_t *error);

/**
 * wirelog_parse_with_error_info:
 * @filename: Path to .dl file
 * @error_info: (out): Detailed error information
 *
 * Parse a Datalog program with detailed error reporting.
 *
 * Returns: (transfer full): Parsed program, or NULL on error
 */
wirelog_program_t*
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
const wirelog_stratum_t*
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
const wirelog_schema_t*
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

/* ======================================================================== */
/* Cleanup                                                                  */
/* ======================================================================== */

/**
 * wirelog_program_free:
 * @program: (transfer full): Program to free
 *
 * Free a parsed program and all associated resources.
 */
void
wirelog_program_free(wirelog_program_t *program);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_PARSER_H */
