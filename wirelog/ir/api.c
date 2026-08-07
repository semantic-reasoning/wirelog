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
 * - wirelog_program_get_relation_ir
 * - wirelog_program_free
 */

#include "program.h"
#include "stratify.h"
#include "../parser/parser.h"
#include "../util/log.h"
#include "../wirelog-parser.h"

#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Parsing                                                                  */
/* ======================================================================== */

wirelog_program_t *
wirelog_parse_string(const char *program_text, wirelog_error_t *error)
{
    /* Issue #973: the post-parse stages below reject some programs and explain
     * why through WL_LOG(WL_LOG_SEC_PARSER, WL_LOG_ERROR, ...) -- the
     * __graph_metadata arity guard, the #920 unsafe-variable check, and the
     * one-aggregate-per-head check, all in ir/program.c.  Until now none of
     * those messages could reach anyone: wl_log_thresholds is a zero-init
     * global read only by wl_log_init(), whose two call sites
     * (exec_plan_gen.c, columnar/session.c) both run strictly after parsing,
     * so the process returned before the logger existed and no WL_LOG value
     * could help.  Initializing here makes WL_LOG=PARSER:1 actually work.
     *
     * This does not change default output -- the gate is LVL <= threshold and
     * the default WL_LOG_NONE (0) still suppresses WL_LOG_ERROR (1).  A user
     * who sets nothing still sees only "Parse error" from cli/driver.c; giving
     * them a message by default requires surfacing the errbuf below, which is
     * issue #979.  This is the opt-in half of that.
     *
     * Thread-safety caveat, and note this is a *new* exposure rather than
     * purely an inherited one.  wl_log_init() fcloses the old sink, NULLs it
     * and memsets the thresholds before repopulating, all without a lock.
     * With WL_LOG_FILE set, a thread parsing here can therefore fclose a
     * FILE* another thread is mid-fprintf on.  Parsing used to be race-free
     * against a concurrently logging thread; it no longer is.  (There is no
     * NULL-sink deref -- wl_log_sink_get_() falls back to stderr.)  Callers
     * that parse concurrently with evaluation should parse before starting
     * worker threads, which is already the usual shape.
     *
     * Cost: with WL_LOG_FILE set this reopens the sink on every call, so a
     * caller parsing many programs in a loop pays an fopen+fclose each time.
     * Unset -- the default -- it is just a threshold recompute. */
    if (!program_text) {
        if (error)
            *error = WIRELOG_ERR_PARSE;
        return NULL;
    }

    wl_log_init();

    char errbuf[512] = { 0 };
    wl_parser_ast_node_t *ast
        = wl_parser_parse_string(program_text, errbuf, sizeof(errbuf));
    if (!ast) {
        if (error)
            *error = WIRELOG_ERR_PARSE;
        return NULL;
    }

    struct wirelog_program *prog = wl_ir_program_create();
    if (!prog) {
        wl_parser_ast_node_free(ast);
        if (error)
            *error = WIRELOG_ERR_MEMORY;
        return NULL;
    }

    prog->ast = ast;

    if (wl_ir_program_collect_metadata(prog, ast) != 0) {
        wl_ir_program_free(prog);
        if (error)
            *error = WIRELOG_ERR_PARSE;
        return NULL;
    }

    if (wl_ir_program_convert_rules(prog, ast) != 0) {
        wl_ir_program_free(prog);
        if (error)
            *error = WIRELOG_ERR_PARSE;
        return NULL;
    }

    if (wl_ir_program_merge_unions(prog) != 0) {
        wl_ir_program_free(prog);
        if (error)
            *error = WIRELOG_ERR_MEMORY;
        return NULL;
    }

    wl_ir_program_build_schemas(prog);

    int strat_rc = wl_ir_stratify_program(prog);
    if (strat_rc == -2) {
        wl_ir_program_free(prog);
        if (error)
            *error = WIRELOG_ERR_PARSE;
        return NULL;
    }
    if (strat_rc == -1) {
        wl_ir_program_free(prog);
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

const wirelog_ir_node_t *
wirelog_program_get_relation_ir(const wirelog_program_t *program,
    const char *relation_name)
{
    if (!program || !relation_name || !program->relations
        || !program->relation_irs)
        return NULL;

    for (uint32_t i = 0; i < program->relation_count; i++) {
        if (program->relations[i].name
            && strcmp(program->relations[i].name, relation_name) == 0) {
            return program->relation_irs[i];
        }
    }

    return NULL;
}

/* ======================================================================== */
/* Fact Extraction                                                          */
/* ======================================================================== */

int
wirelog_program_get_facts(const wirelog_program_t *prog, const char *relation,
    int64_t **data, uint32_t *num_rows,
    uint32_t *num_cols)
{
    if (!prog || !relation || !data || !num_rows || !num_cols)
        return -1;

    *data = NULL;
    *num_rows = 0;
    *num_cols = 0;

    /* Find the relation */
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (prog->relations[i].name
            && strcmp(prog->relations[i].name, relation) == 0) {
            const wl_ir_relation_info_t *rel = &prog->relations[i];
            if (rel->fact_count == 0)
                return 1;

            uint32_t ncols = rel->column_count;
            uint32_t total = rel->fact_count * ncols;
            int64_t *copy = (int64_t *)malloc(total * sizeof(int64_t));
            if (!copy)
                return -1;
            memcpy(copy, rel->fact_data, total * sizeof(int64_t));

            *data = copy;
            *num_rows = rel->fact_count;
            *num_cols = ncols;
            return 0;
        }
    }

    return -1; /* relation not found */
}

/* ======================================================================== */
/* Symbol Interning                                                         */
/* ======================================================================== */

const wl_intern_t *
wirelog_program_get_intern(const wirelog_program_t *prog)
{
    if (!prog)
        return NULL;
    return prog->intern;
}

/* ======================================================================== */
/* Cleanup                                                                  */
/* ======================================================================== */

void
wirelog_program_free(wirelog_program_t *program)
{
    wl_ir_program_free(program);
}
