/*
 * api.c - wirelog Public API Implementation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Implements all public API functions declared in wirelog-parser.h and wirelog.h:
 * - wirelog_parse_string (full implementation)
 * - wirelog_parse (stub: file I/O deferred)
 * - wirelog_parse_with_error_info (stub: file I/O deferred)
 * - wirelog_program_get_rule_count
 * - wirelog_program_get_schema
 * - wirelog_program_get_stratum_count
 * - wirelog_program_get_stratum
 * - wirelog_program_is_stratified
 * - wirelog_program_free
 */

#include "program.h"
#include "stratify.h"
#include "../parser/parser.h"
#include "../wirelog-parser.h"

#include <string.h>

/* ======================================================================== */
/* Parsing                                                                  */
/* ======================================================================== */

wirelog_program_t *
wirelog_parse_string(const char *program_text, wirelog_error_t *error)
{
    if (!program_text) {
        if (error)
            *error = WIRELOG_ERR_PARSE;
        return NULL;
    }

    char errbuf[512] = { 0 };
    wl_ast_node_t *ast = wl_parse_string(program_text, errbuf, sizeof(errbuf));
    if (!ast) {
        if (error)
            *error = WIRELOG_ERR_PARSE;
        return NULL;
    }

    struct wirelog_program *prog = wl_program_create();
    if (!prog) {
        wl_ast_node_free(ast);
        if (error)
            *error = WIRELOG_ERR_MEMORY;
        return NULL;
    }

    prog->ast = ast;

    if (wl_program_collect_metadata(prog, ast) != 0) {
        wl_program_free(prog);
        if (error)
            *error = WIRELOG_ERR_PARSE;
        return NULL;
    }

    if (wl_program_convert_rules(prog, ast) != 0) {
        wl_program_free(prog);
        if (error)
            *error = WIRELOG_ERR_PARSE;
        return NULL;
    }

    if (wl_program_merge_unions(prog) != 0) {
        wl_program_free(prog);
        if (error)
            *error = WIRELOG_ERR_MEMORY;
        return NULL;
    }

    wl_program_build_schemas(prog);

    int strat_rc = wl_program_stratify(prog);
    if (strat_rc == -2) {
        wl_program_free(prog);
        if (error)
            *error = WIRELOG_ERR_PARSE;
        return NULL;
    }
    if (strat_rc == -1) {
        wl_program_free(prog);
        if (error)
            *error = WIRELOG_ERR_MEMORY;
        return NULL;
    }

    if (error)
        *error = WIRELOG_OK;
    return prog;
}

wirelog_program_t *
wirelog_parse(const char *filename, wirelog_error_t *error)
{
    (void)filename;
    if (error)
        *error = WIRELOG_ERR_IO;
    return NULL;
}

wirelog_program_t *
wirelog_parse_with_error_info(const char *filename,
                              wirelog_parse_error_t *error_info)
{
    (void)filename;
    if (error_info) {
        error_info->error_code = WIRELOG_ERR_IO;
        error_info->message = "File-based parsing not yet implemented";
        error_info->line = 0;
        error_info->column = 0;
        error_info->source = NULL;
    }
    return NULL;
}

/* ======================================================================== */
/* Program Inspection                                                       */
/* ======================================================================== */

uint32_t
wirelog_program_get_rule_count(const wirelog_program_t *program)
{
    if (!program)
        return 0;
    return program->rule_count;
}

const wirelog_schema_t *
wirelog_program_get_schema(const wirelog_program_t *program,
                           const char *relation_name)
{
    if (!program || !relation_name || !program->schemas)
        return NULL;

    for (uint32_t i = 0; i < program->relation_count; i++) {
        if (program->schemas[i].relation_name
            && strcmp(program->schemas[i].relation_name, relation_name) == 0) {
            return &program->schemas[i];
        }
    }
    return NULL;
}

uint32_t
wirelog_program_get_stratum_count(const wirelog_program_t *program)
{
    if (!program)
        return 0;
    return program->stratum_count;
}

const wirelog_stratum_t *
wirelog_program_get_stratum(const wirelog_program_t *program,
                            uint32_t stratum_id)
{
    if (!program || stratum_id >= program->stratum_count)
        return NULL;
    return &program->strata[stratum_id];
}

bool
wirelog_program_is_stratified(const wirelog_program_t *program)
{
    if (!program)
        return false;
    return program->is_stratified;
}

/* ======================================================================== */
/* Cleanup                                                                  */
/* ======================================================================== */

void
wirelog_program_free(wirelog_program_t *program)
{
    wl_program_free(program);
}
