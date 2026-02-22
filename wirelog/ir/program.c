/*
 * program.c - wirelog Program Implementation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Implements program creation, metadata collection from AST (Pass 1),
 * schema/stratum synthesis.
 */

#include "program.h"
#include "../parser/ast.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INITIAL_CAPACITY 8

/* ======================================================================== */
/* Column Type Mapping                                                      */
/* ======================================================================== */

static wirelog_column_type_t
type_name_to_column_type(const char *type_name)
{
    if (!type_name) return WIRELOG_TYPE_INT32;

    if (strcmp(type_name, "int32") == 0) return WIRELOG_TYPE_INT32;
    if (strcmp(type_name, "int64") == 0) return WIRELOG_TYPE_INT64;
    if (strcmp(type_name, "uint32") == 0) return WIRELOG_TYPE_UINT32;
    if (strcmp(type_name, "uint64") == 0) return WIRELOG_TYPE_UINT64;
    if (strcmp(type_name, "float") == 0) return WIRELOG_TYPE_FLOAT;
    if (strcmp(type_name, "string") == 0) return WIRELOG_TYPE_STRING;
    if (strcmp(type_name, "bool") == 0) return WIRELOG_TYPE_BOOL;

    /* Default to int32 for unknown types */
    return WIRELOG_TYPE_INT32;
}

/* ======================================================================== */
/* Program Create / Free                                                    */
/* ======================================================================== */

struct wirelog_program*
wl_program_create(void)
{
    struct wirelog_program *prog = (struct wirelog_program *)calloc(
        1, sizeof(struct wirelog_program));
    return prog;
}

static void
relation_info_free(wl_relation_info_t *info)
{
    if (!info) return;
    free(info->name);
    if (info->columns) {
        for (uint32_t i = 0; i < info->column_count; i++) {
            free((char *)info->columns[i].name);
        }
        free(info->columns);
    }
    if (info->input_param_names) {
        for (uint32_t i = 0; i < info->input_param_count; i++) {
            free(info->input_param_names[i]);
            free(info->input_param_values[i]);
        }
        free(info->input_param_names);
        free(info->input_param_values);
    }
}

static void
rule_ir_free(wl_rule_ir_t *rule)
{
    if (!rule) return;
    free(rule->head_relation);
    wl_ir_node_free(rule->ir_root);
}

void
wl_program_free(struct wirelog_program *program)
{
    if (!program) return;

    /* Free relations */
    if (program->relations) {
        for (uint32_t i = 0; i < program->relation_count; i++) {
            relation_info_free(&program->relations[i]);
        }
        free(program->relations);
    }

    /* Free schemas (column names are shared with relations, don't double-free) */
    if (program->schemas) {
        free(program->schemas);
    }

    /* Free strata */
    if (program->strata) {
        for (uint32_t i = 0; i < program->stratum_count; i++) {
            free((void *)program->strata[i].rule_names);
        }
        free(program->strata);
    }

    /* Free rules */
    if (program->rules) {
        for (uint32_t i = 0; i < program->rule_count; i++) {
            rule_ir_free(&program->rules[i]);
        }
        free(program->rules);
    }

    /* Free merged relation IRs */
    if (program->relation_irs) {
        for (uint32_t i = 0; i < program->relation_count; i++) {
            /* Only free if it's a UNION wrapper we created (not a rule's ir_root) */
            if (program->relation_irs[i] &&
                program->relation_irs[i]->type == WIRELOG_IR_UNION) {
                /* UNION node children are rule ir_roots, don't free them again */
                free(program->relation_irs[i]->children);
                program->relation_irs[i]->children = NULL;
                program->relation_irs[i]->child_count = 0;
                wl_ir_node_free(program->relation_irs[i]);
            }
            /* Non-UNION entries point directly to rule ir_roots, freed above */
        }
        free(program->relation_irs);
    }

    /* Free AST */
    if (program->ast) {
        wl_ast_node_free(program->ast);
    }

    free(program);
}

/* ======================================================================== */
/* Relation Lookup / Add                                                    */
/* ======================================================================== */

static wl_relation_info_t*
find_relation(struct wirelog_program *prog, const char *name)
{
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (prog->relations[i].name && strcmp(prog->relations[i].name, name) == 0) {
            return &prog->relations[i];
        }
    }
    return NULL;
}

static wl_relation_info_t*
add_relation(struct wirelog_program *prog, const char *name)
{
    if (prog->relation_count >= prog->relation_capacity) {
        uint32_t new_cap = prog->relation_capacity == 0
            ? INITIAL_CAPACITY
            : prog->relation_capacity * 2;
        wl_relation_info_t *new_rels = (wl_relation_info_t *)realloc(
            prog->relations, new_cap * sizeof(wl_relation_info_t));
        if (!new_rels) return NULL;
        prog->relations = new_rels;
        prog->relation_capacity = new_cap;
    }

    wl_relation_info_t *rel = &prog->relations[prog->relation_count++];
    memset(rel, 0, sizeof(wl_relation_info_t));
    rel->name = strdup_safe(name);
    return rel;
}

static void
add_rule_placeholder(struct wirelog_program *prog, const char *head_name)
{
    if (prog->rule_count >= prog->rule_capacity) {
        uint32_t new_cap = prog->rule_capacity == 0
            ? INITIAL_CAPACITY
            : prog->rule_capacity * 2;
        wl_rule_ir_t *new_rules = (wl_rule_ir_t *)realloc(
            prog->rules, new_cap * sizeof(wl_rule_ir_t));
        if (!new_rules) return;
        prog->rules = new_rules;
        prog->rule_capacity = new_cap;
    }

    wl_rule_ir_t *rule = &prog->rules[prog->rule_count++];
    memset(rule, 0, sizeof(wl_rule_ir_t));
    rule->head_relation = strdup_safe(head_name);
}

/* ======================================================================== */
/* Pass 1: Metadata Collection                                              */
/* ======================================================================== */

static int
collect_decl(struct wirelog_program *prog, const wl_ast_node_t *decl_node)
{
    if (!decl_node->name) return -1;

    wl_relation_info_t *rel = find_relation(prog, decl_node->name);
    if (!rel) {
        rel = add_relation(prog, decl_node->name);
        if (!rel) return -1;
    }

    /* Extract typed params as columns */
    uint32_t col_count = 0;
    for (uint32_t i = 0; i < decl_node->child_count; i++) {
        if (decl_node->children[i]->type == WL_NODE_TYPED_PARAM) {
            col_count++;
        }
    }

    if (col_count > 0) {
        rel->columns = (wirelog_column_t *)calloc(col_count, sizeof(wirelog_column_t));
        if (!rel->columns) return -1;
        rel->column_count = col_count;

        uint32_t idx = 0;
        for (uint32_t i = 0; i < decl_node->child_count; i++) {
            wl_ast_node_t *param = decl_node->children[i];
            if (param->type == WL_NODE_TYPED_PARAM) {
                rel->columns[idx].name = strdup_safe(param->name);
                rel->columns[idx].type = type_name_to_column_type(param->type_name);
                idx++;
            }
        }
    }

    return 0;
}

static int
collect_input(struct wirelog_program *prog, const wl_ast_node_t *input_node)
{
    if (!input_node->name) return -1;

    wl_relation_info_t *rel = find_relation(prog, input_node->name);
    if (!rel) {
        rel = add_relation(prog, input_node->name);
        if (!rel) return -1;
    }

    rel->has_input = true;

    /* Extract input parameters */
    uint32_t param_count = 0;
    for (uint32_t i = 0; i < input_node->child_count; i++) {
        if (input_node->children[i]->type == WL_NODE_INPUT_PARAM) {
            param_count++;
        }
    }

    if (param_count > 0) {
        rel->input_param_names = (char **)calloc(param_count, sizeof(char *));
        rel->input_param_values = (char **)calloc(param_count, sizeof(char *));
        if (!rel->input_param_names || !rel->input_param_values) return -1;
        rel->input_param_count = param_count;

        uint32_t idx = 0;
        for (uint32_t i = 0; i < input_node->child_count; i++) {
            wl_ast_node_t *param = input_node->children[i];
            if (param->type == WL_NODE_INPUT_PARAM) {
                rel->input_param_names[idx] = strdup_safe(param->name);
                rel->input_param_values[idx] = strdup_safe(param->str_value);
                idx++;
            }
        }
    }

    return 0;
}

static int
collect_output(struct wirelog_program *prog, const wl_ast_node_t *output_node)
{
    if (!output_node->name) return -1;

    wl_relation_info_t *rel = find_relation(prog, output_node->name);
    if (!rel) {
        rel = add_relation(prog, output_node->name);
        if (!rel) return -1;
    }

    rel->has_output = true;
    return 0;
}

static int
collect_printsize(struct wirelog_program *prog, const wl_ast_node_t *ps_node)
{
    if (!ps_node->name) return -1;

    wl_relation_info_t *rel = find_relation(prog, ps_node->name);
    if (!rel) {
        rel = add_relation(prog, ps_node->name);
        if (!rel) return -1;
    }

    rel->has_printsize = true;
    return 0;
}

static int
collect_rule(struct wirelog_program *prog, const wl_ast_node_t *rule_node)
{
    /* rule_node: children[0] = HEAD, children[1..] = body */
    if (rule_node->child_count < 1) return -1;

    wl_ast_node_t *head = rule_node->children[0];
    if (head->type != WL_NODE_HEAD || !head->name) return -1;

    /* Ensure relation exists */
    wl_relation_info_t *rel = find_relation(prog, head->name);
    if (!rel) {
        rel = add_relation(prog, head->name);
        if (!rel) return -1;
    }

    /* Add rule placeholder (IR tree will be built in Pass 2) */
    add_rule_placeholder(prog, head->name);

    return 0;
}

int
wl_program_collect_metadata(struct wirelog_program *program,
                            const wl_ast_node_t *ast)
{
    if (!program || !ast || ast->type != WL_NODE_PROGRAM) return -1;

    for (uint32_t i = 0; i < ast->child_count; i++) {
        wl_ast_node_t *child = ast->children[i];
        int rc = 0;

        switch (child->type) {
        case WL_NODE_DECL:
            rc = collect_decl(program, child);
            break;
        case WL_NODE_INPUT:
            rc = collect_input(program, child);
            break;
        case WL_NODE_OUTPUT:
            rc = collect_output(program, child);
            break;
        case WL_NODE_PRINTSIZE:
            rc = collect_printsize(program, child);
            break;
        case WL_NODE_RULE:
            rc = collect_rule(program, child);
            break;
        default:
            /* Ignore unknown node types */
            break;
        }

        if (rc != 0) return rc;
    }

    return 0;
}

/* ======================================================================== */
/* Schema / Stratum Synthesis                                               */
/* ======================================================================== */

void
wl_program_build_schemas(struct wirelog_program *program)
{
    if (!program || program->relation_count == 0) return;

    /* Free existing schemas */
    free(program->schemas);

    program->schemas = (wirelog_schema_t *)calloc(
        program->relation_count, sizeof(wirelog_schema_t));
    if (!program->schemas) return;

    for (uint32_t i = 0; i < program->relation_count; i++) {
        wl_relation_info_t *rel = &program->relations[i];
        program->schemas[i].relation_name = rel->name;  /* shared pointer */
        program->schemas[i].columns = rel->columns;      /* shared pointer */
        program->schemas[i].column_count = rel->column_count;
    }
}

void
wl_program_build_default_stratum(struct wirelog_program *program)
{
    if (!program) return;

    /* Free existing strata */
    if (program->strata) {
        for (uint32_t i = 0; i < program->stratum_count; i++) {
            free((void *)program->strata[i].rule_names);
        }
        free(program->strata);
    }

    program->stratum_count = 1;
    program->strata = (wirelog_stratum_t *)calloc(1, sizeof(wirelog_stratum_t));
    if (!program->strata) {
        program->stratum_count = 0;
        return;
    }

    program->strata[0].stratum_id = 0;
    program->strata[0].rule_count = program->rule_count;

    if (program->rule_count > 0) {
        program->strata[0].rule_names = (const char **)calloc(
            program->rule_count, sizeof(const char *));
        if (program->strata[0].rule_names) {
            for (uint32_t i = 0; i < program->rule_count; i++) {
                program->strata[0].rule_names[i] = program->rules[i].head_relation;
            }
        }
    }
}
