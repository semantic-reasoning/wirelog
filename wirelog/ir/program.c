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
#include "../passes/magic_sets.h"
#include "../intern.h"

#include <ctype.h>
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
    if (!type_name)
        return WIRELOG_TYPE_INT32;

    if (strcmp(type_name, "int32") == 0)
        return WIRELOG_TYPE_INT32;
    if (strcmp(type_name, "int64") == 0)
        return WIRELOG_TYPE_INT64;
    if (strcmp(type_name, "uint32") == 0)
        return WIRELOG_TYPE_UINT32;
    if (strcmp(type_name, "uint64") == 0)
        return WIRELOG_TYPE_UINT64;
    if (strcmp(type_name, "float") == 0)
        return WIRELOG_TYPE_FLOAT;
    if (strcmp(type_name, "string") == 0)
        return WIRELOG_TYPE_STRING;
    if (strcmp(type_name, "symbol") == 0)
        return WIRELOG_TYPE_STRING;
    if (strcmp(type_name, "bool") == 0)
        return WIRELOG_TYPE_BOOL;

    /* Default to int32 for unknown types */
    return WIRELOG_TYPE_INT32;
}

/* ======================================================================== */
/* Compound Column Metadata Parsing (Issue #531)                            */
/* ======================================================================== */

typedef struct {
    wirelog_compound_kind_t kind;
    uint32_t functor_id;
    uint32_t arity;
} compound_metadata_t;

/**
 * Parse compound metadata from type_name.
 * Syntax: "functor/arity" or "functor/arity side" or "functor/arity inline"
 * Examples: "f/2", "g/3 side", "h/2 inline"
 *
 * Returns: compound_metadata_t with kind=NONE if not a compound type.
 */
static compound_metadata_t
parse_compound_metadata(const char *type_name, wl_intern_t *intern)
{
    compound_metadata_t result = {0};
    result.kind = WIRELOG_COMPOUND_KIND_NONE;
    result.functor_id = 0;
    result.arity = 0;

    if (!type_name || !intern)
        return result;

    /* Look for '/' separator indicating compound syntax */
    const char *slash = strchr(type_name, '/');
    if (!slash)
        return result; /* Not a compound type */

    /* Extract functor name (everything before '/') */
    size_t functor_len = slash - type_name;
    char *functor_name = (char *)malloc(functor_len + 1);
    if (!functor_name)
        return result;

    memcpy(functor_name, type_name, functor_len);
    functor_name[functor_len] = '\0';

    /* Parse arity after '/' */
    char *arity_str = (char *)(slash + 1);
    char *arity_end = NULL;
    long arity_val = strtol(arity_str, &arity_end, 10);

    if (arity_end == arity_str || arity_val < 0 || arity_val > UINT32_MAX) {
        free(functor_name);
        return result; /* Invalid arity */
    }

    result.arity = (uint32_t)arity_val;

    /* Intern the functor name to get ID (default: side-relation for now) */
    int64_t functor_id = wl_intern_put(intern, functor_name);
    if (functor_id < 0) {
        free(functor_name);
        return result; /* Intern failed */
    }
    result.functor_id = (uint32_t)functor_id;
    result.kind = WIRELOG_COMPOUND_KIND_SIDE; /* Default to side-relation */

    /* Check for kind modifier (inline/side) after arity */
    char *kind_str = arity_end;
    while (*kind_str && isspace((unsigned char)*kind_str))
        kind_str++;

    if (strncmp(kind_str, "inline", 6) == 0
        && (kind_str[6] == '\0' || isspace((unsigned char)kind_str[6])))
        result.kind = WIRELOG_COMPOUND_KIND_INLINE;
    else if (strncmp(kind_str, "side", 4) == 0
        && (kind_str[4] == '\0' || isspace((unsigned char)kind_str[4])))
        result.kind = WIRELOG_COMPOUND_KIND_SIDE;

    free(functor_name);
    return result;
}

/* ======================================================================== */
/* Program Create / Free                                                    */
/* ======================================================================== */

struct wirelog_program *
wl_ir_program_create(void)
{
    struct wirelog_program *prog
        = (struct wirelog_program *)calloc(1, sizeof(struct wirelog_program));
    if (!prog)
        return NULL;

    prog->intern = wl_intern_create();
    if (!prog->intern) {
        free(prog);
        return NULL;
    }

    return prog;
}

static void
relation_info_free(wl_ir_relation_info_t *info)
{
    if (!info)
        return;
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
    free(info->input_io_scheme);
    free(info->output_file);
    free(info->fact_data);
}

static void
rule_ir_free(wl_ir_rule_ir_t *rule)
{
    if (!rule)
        return;
    free(rule->head_relation);
    wl_ir_node_free(rule->ir_root);
}

void
wl_ir_program_free(struct wirelog_program *program)
{
    if (!program)
        return;

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

    /* Free merged relation IRs (UNION wrappers only, before rules) */
    if (program->relation_irs) {
        for (uint32_t i = 0; i < program->relation_count; i++) {
            if (program->relation_irs[i]
                && program->relation_irs[i]->type == WIRELOG_IR_UNION) {
                /* Detach children (rule ir_roots) so they aren't freed here */
                free(program->relation_irs[i]->children);
                program->relation_irs[i]->children = NULL;
                program->relation_irs[i]->child_count = 0;
                wl_ir_node_free(program->relation_irs[i]);
            }
            /* Non-UNION entries are aliases to rule ir_roots, freed below */
        }
        free(program->relation_irs);
    }

    /* Free rules (owns ir_root nodes) */
    if (program->rules) {
        for (uint32_t i = 0; i < program->rule_count; i++) {
            rule_ir_free(&program->rules[i]);
        }
        free(program->rules);
    }

    /* Free AST */
    if (program->ast) {
        wl_parser_ast_node_free(program->ast);
    }

    /* Free intern table */
    wl_intern_free(program->intern);

    /* Free .query demands */
    if (program->demands) {
        for (uint32_t i = 0; i < program->demand_count; i++) {
            free((char *)program->demands[i].relation_name);
        }
        free(program->demands);
    }

    free(program);
}

/* ======================================================================== */
/* Relation Lookup / Add                                                    */
/* ======================================================================== */

static wl_ir_relation_info_t *
find_relation(struct wirelog_program *prog, const char *name)
{
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (prog->relations[i].name
            && strcmp(prog->relations[i].name, name) == 0) {
            return &prog->relations[i];
        }
    }
    return NULL;
}

static wl_ir_relation_info_t *
add_relation(struct wirelog_program *prog, const char *name)
{
    if (prog->relation_count >= prog->relation_capacity) {
        uint32_t new_cap = prog->relation_capacity == 0
                               ? INITIAL_CAPACITY
                               : prog->relation_capacity * 2;
        /* Shallow copy of wl_ir_relation_info_t is safe here: input_io_scheme
         * is a strdup'd string owned by the entry, mirroring the existing
         * input_param_names/values pattern. No double-free risk because the
         * old array is freed (not its elements) and the new array takes
         * ownership of all pointers. */
        wl_ir_relation_info_t *new_rels = (wl_ir_relation_info_t *)realloc(
            prog->relations, new_cap * sizeof(wl_ir_relation_info_t));
        if (!new_rels)
            return NULL;
        prog->relations = new_rels;
        prog->relation_capacity = new_cap;
    }

    wl_ir_relation_info_t *rel = &prog->relations[prog->relation_count++];
    memset(rel, 0, sizeof(wl_ir_relation_info_t));
    rel->name = strdup_safe(name);
    return rel;
}

static void
add_rule_placeholder(struct wirelog_program *prog, const char *head_name)
{
    if (prog->rule_count >= prog->rule_capacity) {
        uint32_t new_cap = prog->rule_capacity == 0 ? INITIAL_CAPACITY
                                                    : prog->rule_capacity * 2;
        wl_ir_rule_ir_t *new_rules = (wl_ir_rule_ir_t *)realloc(
            prog->rules, new_cap * sizeof(wl_ir_rule_ir_t));
        if (!new_rules)
            return;
        prog->rules = new_rules;
        prog->rule_capacity = new_cap;
    }

    wl_ir_rule_ir_t *rule = &prog->rules[prog->rule_count++];
    memset(rule, 0, sizeof(wl_ir_rule_ir_t));
    rule->head_relation = strdup_safe(head_name);
}

/* ======================================================================== */
/* Pass 1: Metadata Collection                                              */
/* ======================================================================== */

static int
collect_decl(struct wirelog_program *prog,
    const wl_parser_ast_node_t *decl_node)
{
    if (!decl_node->name)
        return -1;

    wl_ir_relation_info_t *rel = find_relation(prog, decl_node->name);
    if (!rel) {
        rel = add_relation(prog, decl_node->name);
        if (!rel)
            return -1;
    }

    /* Extract typed params as columns */
    uint32_t col_count = 0;
    for (uint32_t i = 0; i < decl_node->child_count; i++) {
        if (decl_node->children[i]->type == WL_PARSER_AST_NODE_TYPED_PARAM) {
            col_count++;
        }
    }

    if (col_count > 0) {
        rel->columns
            = (wirelog_column_t *)calloc(col_count, sizeof(wirelog_column_t));
        if (!rel->columns)
            return -1;
        rel->column_count = col_count;

        uint32_t idx = 0;
        for (uint32_t i = 0; i < decl_node->child_count; i++) {
            const wl_parser_ast_node_t *param = decl_node->children[i];
            if (param->type == WL_PARSER_AST_NODE_TYPED_PARAM) {
                rel->columns[idx].name = strdup_safe(param->name);
                rel->columns[idx].type
                    = type_name_to_column_type(param->type_name);

                /* Phase 1B: Extract compound metadata if present */
                compound_metadata_t meta = parse_compound_metadata(
                    param->type_name, prog->intern);
                rel->columns[idx].compound_kind = meta.kind;
                rel->columns[idx].compound_functor_id = meta.functor_id;
                rel->columns[idx].compound_arity = meta.arity;
                rel->columns[idx].compound_inline_col_offset = 0;

                idx++;
            }
        }
    }

    return 0;
}

static int
collect_input(struct wirelog_program *prog,
    const wl_parser_ast_node_t *input_node)
{
    if (!input_node->name)
        return -1;

    wl_ir_relation_info_t *rel = find_relation(prog, input_node->name);
    if (!rel) {
        rel = add_relation(prog, input_node->name);
        if (!rel)
            return -1;
    }

    rel->has_input = true;

    /* Extract input parameters.  The "io" key is consumed as a reserved
     * parameter to populate input_io_scheme and is NOT passed through
     * to input_param_names/values. */
    uint32_t param_count = 0;
    for (uint32_t i = 0; i < input_node->child_count; i++) {
        if (input_node->children[i]->type == WL_PARSER_AST_NODE_INPUT_PARAM) {
            if (input_node->children[i]->name
                && strncmp("io", input_node->children[i]->name, 3) == 0) {
                rel->input_io_scheme
                    = strdup_safe(input_node->children[i]->str_value);
            } else {
                param_count++;
            }
        }
    }

    if (param_count > 0) {
        rel->input_param_names = (char **)calloc(param_count, sizeof(char *));
        rel->input_param_values = (char **)calloc(param_count, sizeof(char *));
        if (!rel->input_param_names || !rel->input_param_values)
            return -1;
        rel->input_param_count = param_count;

        uint32_t idx = 0;
        for (uint32_t i = 0; i < input_node->child_count; i++) {
            const wl_parser_ast_node_t *param = input_node->children[i];
            if (param->type == WL_PARSER_AST_NODE_INPUT_PARAM) {
                if (param->name && strncmp(param->name, "io", 3) == 0)
                    continue;
                rel->input_param_names[idx] = strdup_safe(param->name);
                rel->input_param_values[idx] = strdup_safe(param->str_value);
                idx++;
            }
        }
    }

    return 0;
}

static int
collect_output(struct wirelog_program *prog,
    const wl_parser_ast_node_t *output_node)
{
    if (!output_node->name)
        return -1;

    wl_ir_relation_info_t *rel = find_relation(prog, output_node->name);
    if (!rel) {
        rel = add_relation(prog, output_node->name);
        if (!rel)
            return -1;
    }

    rel->has_output = true;

    /* Extract optional filename from OUTPUT_PARAM children */
    for (uint32_t i = 0; i < output_node->child_count; i++) {
        const wl_parser_ast_node_t *param = output_node->children[i];
        if (param->type == WL_PARSER_AST_NODE_OUTPUT_PARAM && param->name
            && strcmp(param->name, "filename") == 0 && param->str_value) {
            free(rel->output_file);
            rel->output_file = strdup_safe(param->str_value);
            break;
        }
    }

    return 0;
}

static int
collect_printsize(struct wirelog_program *prog,
    const wl_parser_ast_node_t *ps_node)
{
    if (!ps_node->name)
        return -1;

    wl_ir_relation_info_t *rel = find_relation(prog, ps_node->name);
    if (!rel) {
        rel = add_relation(prog, ps_node->name);
        if (!rel)
            return -1;
    }

    rel->has_printsize = true;
    return 0;
}

static int
collect_query(struct wirelog_program *prog,
    const wl_parser_ast_node_t *query_node)
{
    if (!query_node->name)
        return -1;

    if (prog->demand_count >= prog->demand_capacity) {
        uint32_t new_cap = prog->demand_capacity == 0
                               ? INITIAL_CAPACITY
                               : prog->demand_capacity * 2;
        wl_magic_demand_t *new_demands = (wl_magic_demand_t *)realloc(
            prog->demands, new_cap * sizeof(wl_magic_demand_t));
        if (!new_demands)
            return -1;
        prog->demands = new_demands;
        prog->demand_capacity = new_cap;
    }

    wl_magic_demand_t *d = &prog->demands[prog->demand_count++];
    d->relation_name = strdup_safe(query_node->name);
    d->bound_mask = (uint64_t)query_node->int_value;
    d->arity = query_node->query_arity; /* adornment count from parser */
    return 0;
}

static int
collect_rule(struct wirelog_program *prog,
    const wl_parser_ast_node_t *rule_node)
{
    /* rule_node: children[0] = HEAD, children[1..] = body */
    if (rule_node->child_count < 1)
        return -1;

    const wl_parser_ast_node_t *head = rule_node->children[0];
    if (head->type != WL_PARSER_AST_NODE_HEAD || !head->name)
        return -1;

    /* Ensure relation exists */
    const wl_ir_relation_info_t *rel = find_relation(prog, head->name);
    if (!rel) {
        rel = add_relation(prog, head->name);
        if (!rel)
            return -1;
    }

    /* Add rule placeholder (IR tree will be built in Pass 2) */
    add_rule_placeholder(prog, head->name);

    return 0;
}

static int
collect_fact(struct wirelog_program *prog,
    const wl_parser_ast_node_t *fact_node)
{
    if (!fact_node->name)
        return -1;

    wl_ir_relation_info_t *rel = find_relation(prog, fact_node->name);
    if (!rel) {
        rel = add_relation(prog, fact_node->name);
        if (!rel)
            return -1;
    }

    uint32_t ncols = fact_node->child_count;

    /* Grow fact_data buffer if needed */
    uint32_t needed = (rel->fact_count + 1) * ncols;
    if (needed > rel->fact_capacity) {
        uint32_t new_cap
            = rel->fact_capacity == 0 ? ncols * 8 : rel->fact_capacity * 2;
        if (new_cap < needed)
            new_cap = needed;
        int64_t *new_data
            = (int64_t *)realloc(rel->fact_data, new_cap * sizeof(int64_t));
        if (!new_data)
            return -1;
        rel->fact_data = new_data;
        rel->fact_capacity = new_cap;
    }

    /* Copy fact values into row-major layout */
    uint32_t offset = rel->fact_count * ncols;
    for (uint32_t i = 0; i < ncols; i++) {
        const wl_parser_ast_node_t *arg = fact_node->children[i];
        if (arg->type == WL_PARSER_AST_NODE_INTEGER) {
            rel->fact_data[offset + i] = arg->int_value;
        } else if (arg->type == WL_PARSER_AST_NODE_STRING) {
            rel->fact_data[offset + i]
                = wl_intern_put(prog->intern, arg->str_value);
        }
    }

    rel->fact_count++;
    return 0;
}

int
wl_ir_program_collect_metadata(struct wirelog_program *program,
    const wl_parser_ast_node_t *ast)
{
    if (!program || !ast || ast->type != WL_PARSER_AST_NODE_PROGRAM)
        return -1;

    for (uint32_t i = 0; i < ast->child_count; i++) {
        const wl_parser_ast_node_t *child = ast->children[i];
        int rc = 0;

        switch (child->type) {
        case WL_PARSER_AST_NODE_DECL:
            rc = collect_decl(program, child);
            break;
        case WL_PARSER_AST_NODE_INPUT:
            rc = collect_input(program, child);
            break;
        case WL_PARSER_AST_NODE_OUTPUT:
            rc = collect_output(program, child);
            break;
        case WL_PARSER_AST_NODE_PRINTSIZE:
            rc = collect_printsize(program, child);
            break;
        case WL_PARSER_AST_NODE_QUERY:
            rc = collect_query(program, child);
            break;
        case WL_PARSER_AST_NODE_RULE:
            rc = collect_rule(program, child);
            break;
        case WL_PARSER_AST_NODE_FACT:
            rc = collect_fact(program, child);
            break;
        default:
            /* Ignore unknown node types */
            break;
        }

        if (rc != 0)
            return rc;
    }

    return 0;
}

/* ======================================================================== */
/* Schema / Stratum Synthesis                                               */
/* ======================================================================== */

void
wl_ir_program_build_schemas(struct wirelog_program *program)
{
    if (!program || program->relation_count == 0)
        return;

    /* Free existing schemas */
    free(program->schemas);

    program->schemas = (wirelog_schema_t *)calloc(program->relation_count,
            sizeof(wirelog_schema_t));
    if (!program->schemas)
        return;

    for (uint32_t i = 0; i < program->relation_count; i++) {
        wl_ir_relation_info_t *rel = &program->relations[i];
        program->schemas[i].relation_name = rel->name; /* shared pointer */
        program->schemas[i].columns = rel->columns;    /* shared pointer */
        program->schemas[i].column_count = rel->column_count;
    }
}

void
wl_ir_program_build_default_stratum(struct wirelog_program *program)
{
    if (!program)
        return;

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
        program->strata[0].rule_names
            = (const char **)calloc(program->rule_count, sizeof(const char *));
        if (program->strata[0].rule_names) {
            for (uint32_t i = 0; i < program->rule_count; i++) {
                program->strata[0].rule_names[i]
                    = program->rules[i].head_relation;
            }
        }
    }
}

/* ======================================================================== */
/* Pass 2: AST-to-IR Rule Conversion                                        */
/* ======================================================================== */

/* ---- Expression Conversion (AST expr -> IR expr) ---- */

static wl_ir_expr_t *
convert_expr(const wl_parser_ast_node_t *node)
{
    if (!node)
        return NULL;

    switch (node->type) {
    case WL_PARSER_AST_NODE_VARIABLE: {
        wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_VAR);
        if (e)
            e->var_name = strdup_safe(node->name);
        return e;
    }
    case WL_PARSER_AST_NODE_INTEGER: {
        wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_CONST_INT);
        if (e)
            e->int_value = node->int_value;
        return e;
    }
    case WL_PARSER_AST_NODE_STRING: {
        wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_CONST_STR);
        if (e)
            e->str_value = strdup_safe(node->str_value);
        return e;
    }
    case WL_PARSER_AST_NODE_BINARY_EXPR: {
        wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_ARITH);
        if (!e)
            return NULL;
        e->arith_op = node->arith_op;
        for (uint32_t ci = 0; ci < node->child_count; ci++)
            wl_ir_expr_add_child(e, convert_expr(node->children[ci]));
        return e;
    }
    case WL_PARSER_AST_NODE_COMPARISON: {
        wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_CMP);
        if (!e)
            return NULL;
        e->cmp_op = node->cmp_op;
        if (node->child_count >= 2) {
            wl_ir_expr_add_child(e, convert_expr(node->children[0]));
            wl_ir_expr_add_child(e, convert_expr(node->children[1]));
        }
        return e;
    }
    case WL_PARSER_AST_NODE_AGGREGATE: {
        wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_AGG);
        if (!e)
            return NULL;
        e->agg_fn = node->agg_fn;
        if (node->child_count >= 1) {
            wl_ir_expr_add_child(e, convert_expr(node->children[0]));
        }
        return e;
    }
    case WL_PARSER_AST_NODE_BOOLEAN: {
        wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_BOOL);
        if (e)
            e->bool_value = node->bool_value;
        return e;
    }
    case WL_PARSER_AST_NODE_STR_FUNCTION: {
        wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_STR_FN);
        if (!e)
            return NULL;
        e->str_fn = node->str_fn;
        for (uint32_t ci = 0; ci < node->child_count; ci++) {
            wl_ir_expr_t *child = convert_expr(node->children[ci]);
            if (!child) {
                wl_ir_expr_free(e); return NULL;
            }
            wl_ir_expr_add_child(e, child);
        }
        return e;
    }
    default:
        return NULL;
    }
}

/* ---- Variable Name Tracking Helpers ---- */

static void
free_var_names(char **names, uint32_t count)
{
    if (!names)
        return;
    for (uint32_t i = 0; i < count; i++) {
        free(names[i]);
    }
    free(names);
}

static char **
merge_var_names(char **left, uint32_t left_count, char **right,
    uint32_t right_count, uint32_t *out_count)
{
    uint32_t max_count = left_count + right_count;
    char **merged
        = (char **)calloc(max_count > 0 ? max_count : 1, sizeof(char *));
    if (!merged) {
        *out_count = 0;
        return NULL;
    }

    uint32_t count = 0;

    for (uint32_t i = 0; i < left_count; i++) {
        if (!left[i])
            continue;
        bool dup = false;
        for (uint32_t j = 0; j < count; j++) {
            if (merged[j] && strcmp(merged[j], left[i]) == 0) {
                dup = true;
                break;
            }
        }
        if (!dup)
            merged[count++] = strdup_safe(left[i]);
    }

    for (uint32_t i = 0; i < right_count; i++) {
        if (!right[i])
            continue;
        bool dup = false;
        for (uint32_t j = 0; j < count; j++) {
            if (merged[j] && strcmp(merged[j], right[i]) == 0) {
                dup = true;
                break;
            }
        }
        if (!dup)
            merged[count++] = strdup_safe(right[i]);
    }

    *out_count = count;
    return merged;
}

/* ---- Join Key Extraction ---- */

static void
setup_join_keys(char **left_vars, uint32_t left_count, char **right_vars,
    uint32_t right_count, wirelog_ir_node_t *join)
{
    /* Count shared variables (exclude NULL = wildcard/constant) */
    uint32_t key_count = 0;
    for (uint32_t i = 0; i < left_count; i++) {
        if (!left_vars[i])
            continue;
        for (uint32_t j = 0; j < right_count; j++) {
            if (!right_vars[j])
                continue;
            if (strcmp(left_vars[i], right_vars[j]) == 0) {
                key_count++;
                break;
            }
        }
    }

    if (key_count == 0)
        return;

    join->join_left_keys = (char **)calloc(key_count, sizeof(char *));
    join->join_right_keys = (char **)calloc(key_count, sizeof(char *));
    if (!join->join_left_keys || !join->join_right_keys)
        return;
    join->join_key_count = key_count;

    uint32_t k = 0;
    for (uint32_t i = 0; i < left_count; i++) {
        if (!left_vars[i])
            continue;
        for (uint32_t j = 0; j < right_count; j++) {
            if (!right_vars[j])
                continue;
            if (strcmp(left_vars[i], right_vars[j]) == 0) {
                join->join_left_keys[k] = strdup_safe(left_vars[i]);
                join->join_right_keys[k] = strdup_safe(right_vars[j]);
                k++;
                break;
            }
        }
    }
}

/* ---- Scan Building with Intra-atom Filters ---- */

static wirelog_ir_node_t *
build_atom_scan(const wl_parser_ast_node_t *atom,
    const struct wirelog_program *prog, char ***out_var_names,
    uint32_t *out_var_count)
{
    wirelog_ir_node_t *scan = wl_ir_node_create(WIRELOG_IR_SCAN);
    if (!scan)
        return NULL;

    wl_ir_node_set_relation(scan, atom->name);

    uint32_t arg_count = atom->child_count;
    scan->column_names
        = (char **)calloc(arg_count > 0 ? arg_count : 1, sizeof(char *));
    scan->column_count = arg_count;

    char **var_names
        = (char **)calloc(arg_count > 0 ? arg_count : 1, sizeof(char *));
    if (!scan->column_names || !var_names) {
        free(var_names);
        wl_ir_node_free(scan);
        return NULL;
    }

    /* Collect column names from atom arguments */
    for (uint32_t i = 0; i < arg_count; i++) {
        const wl_parser_ast_node_t *arg = atom->children[i];
        if (arg->type == WL_PARSER_AST_NODE_VARIABLE) {
            scan->column_names[i] = strdup_safe(arg->name);
            var_names[i] = strdup_safe(arg->name);
        } else {
            /* Wildcard, integer, string -> NULL (anonymous position) */
            scan->column_names[i] = NULL;
            var_names[i] = NULL;
        }
    }

    wirelog_ir_node_t *result = scan;

    /*
     * Phase 2B: Compound column IR lowering (Issue #531).
     *
     * Annotate the SCAN node directly rather than wrapping it. Wrapping
     * placed a COMPOUND node above the SCAN, which broke downstream passes
     * (sip.c, magic_sets.c, exec_plan_gen.c) that descend to a SCAN leaf
     * by checking node->type == WIRELOG_IR_SCAN. The annotation approach
     * preserves the leaf's relation_name / column_names while flagging
     * the compound kind via the node type and metadata fields.
     *
     * Multi-compound-column atoms: the first compound column drives the
     * annotation (the compound_inline struct holds metadata for one
     * column). Additional compound columns retain the same scan-leaf
     * shape; richer multi-column lowering is deferred to Phase 2C.
     */
    if (prog && atom->name) {
        wl_ir_relation_info_t *rel_info = NULL;
        for (uint32_t i = 0; i < prog->relation_count; i++) {
            if (prog->relations[i].name
                && strcmp(prog->relations[i].name, atom->name) == 0) {
                rel_info = &prog->relations[i];
                break;
            }
        }

        if (rel_info && rel_info->columns) {
            for (uint32_t i = 0; i < arg_count && i < rel_info->column_count;
                i++) {
                const wl_parser_ast_node_t *arg = atom->children[i];
                const wirelog_column_t *col = &rel_info->columns[i];

                if (arg->type != WL_PARSER_AST_NODE_COMPOUND_TERM)
                    continue;
                if (col->compound_kind == WIRELOG_COMPOUND_KIND_NONE)
                    continue;

                /* First compound column annotates the SCAN. */
                if (scan->type == WIRELOG_IR_SCAN) {
                    scan->type = (col->compound_kind
                        == WIRELOG_COMPOUND_KIND_INLINE)
                        ? WIRELOG_IR_COMPOUND_INLINE
                        : WIRELOG_IR_COMPOUND_SIDE;
                    scan->compound_inline.functor_id
                        = col->compound_functor_id;
                    scan->compound_inline.arity = col->compound_arity;
                    scan->compound_inline.inline_col_offset
                        = col->compound_inline_col_offset;
                }
            }
        }
    }

    /* Step 1a: Intra-atom FILTER for duplicate variables */
    for (uint32_t i = 0; i < arg_count; i++) {
        if (!var_names[i])
            continue;
        for (uint32_t j = i + 1; j < arg_count; j++) {
            if (!var_names[j])
                continue;
            if (strcmp(var_names[i], var_names[j]) == 0) {
                wirelog_ir_node_t *f = wl_ir_node_create(WIRELOG_IR_FILTER);
                if (!f)
                    continue;

                wl_ir_expr_t *cmp = wl_ir_expr_create(WL_IR_EXPR_CMP);
                if (cmp) {
                    cmp->cmp_op = WIRELOG_CMP_EQ;
                    wl_ir_expr_t *lhs = wl_ir_expr_create(WL_IR_EXPR_VAR);
                    if (lhs)
                        lhs->var_name = strdup_safe(var_names[i]);
                    wl_ir_expr_t *rhs = wl_ir_expr_create(WL_IR_EXPR_VAR);
                    if (rhs)
                        rhs->var_name = strdup_safe(var_names[j]);
                    wl_ir_expr_add_child(cmp, lhs);
                    wl_ir_expr_add_child(cmp, rhs);
                }
                f->filter_expr = cmp;
                wl_ir_node_add_child(f, result);
                result = f;
            }
        }
    }

    /* Step 1b: Intra-atom FILTER for constants */
    for (uint32_t i = 0; i < arg_count; i++) {
        const wl_parser_ast_node_t *arg = atom->children[i];
        if (arg->type == WL_PARSER_AST_NODE_INTEGER) {
            wirelog_ir_node_t *f = wl_ir_node_create(WIRELOG_IR_FILTER);
            if (!f)
                continue;

            wl_ir_expr_t *cmp = wl_ir_expr_create(WL_IR_EXPR_CMP);
            if (cmp) {
                cmp->cmp_op = WIRELOG_CMP_EQ;
                wl_ir_expr_t *lhs = wl_ir_expr_create(WL_IR_EXPR_VAR);
                if (lhs) {
                    char col[32];
                    snprintf(col, sizeof(col), "col%u", i);
                    lhs->var_name = strdup_safe(col);
                }
                wl_ir_expr_t *rhs = wl_ir_expr_create(WL_IR_EXPR_CONST_INT);
                if (rhs)
                    rhs->int_value = arg->int_value;
                wl_ir_expr_add_child(cmp, lhs);
                wl_ir_expr_add_child(cmp, rhs);
            }
            f->filter_expr = cmp;
            wl_ir_node_add_child(f, result);
            result = f;
        } else if (arg->type == WL_PARSER_AST_NODE_STRING) {
            wirelog_ir_node_t *f = wl_ir_node_create(WIRELOG_IR_FILTER);
            if (!f)
                continue;

            wl_ir_expr_t *cmp = wl_ir_expr_create(WL_IR_EXPR_CMP);
            if (cmp) {
                cmp->cmp_op = WIRELOG_CMP_EQ;
                wl_ir_expr_t *lhs = wl_ir_expr_create(WL_IR_EXPR_VAR);
                if (lhs) {
                    char col[32];
                    snprintf(col, sizeof(col), "col%u", i);
                    lhs->var_name = strdup_safe(col);
                }
                wl_ir_expr_t *rhs = wl_ir_expr_create(WL_IR_EXPR_CONST_STR);
                if (rhs)
                    rhs->str_value = strdup_safe(arg->str_value);
                wl_ir_expr_add_child(cmp, lhs);
                wl_ir_expr_add_child(cmp, rhs);
            }
            f->filter_expr = cmp;
            wl_ir_node_add_child(f, result);
            result = f;
        }
    }

    *out_var_names = var_names;
    *out_var_count = arg_count;
    return result;
}

/* ---- Single Rule Conversion ---- */

static wirelog_ir_node_t *
convert_rule(const wl_parser_ast_node_t *rule_node,
    const struct wirelog_program *prog)
{
    if (!rule_node || rule_node->child_count < 1)
        return NULL;

    wl_parser_ast_node_t *head = rule_node->children[0];
    if (head->type != WL_PARSER_AST_NODE_HEAD)
        return NULL;

    uint32_t body_count = rule_node->child_count - 1;

    /* ---- Step 1: Collect positive atoms -> SCANs ---- */

    uint32_t scan_cap = body_count > 0 ? body_count : 1;
    wirelog_ir_node_t **scans
        = (wirelog_ir_node_t **)calloc(scan_cap, sizeof(wirelog_ir_node_t *));
    char ***scan_vars = (char ***)calloc(scan_cap, sizeof(char **));
    uint32_t *scan_vcounts = (uint32_t *)calloc(scan_cap, sizeof(uint32_t));
    if (!scans || !scan_vars || !scan_vcounts) {
        free(scans);
        free(scan_vars);
        free(scan_vcounts);
        return NULL;
    }

    uint32_t scan_count = 0;
    for (uint32_t i = 1; i < rule_node->child_count; i++) {
        const wl_parser_ast_node_t *b = rule_node->children[i];
        if (b->type == WL_PARSER_AST_NODE_ATOM) {
            scans[scan_count] = build_atom_scan(b, prog,
                    &scan_vars[scan_count], &scan_vcounts[scan_count]);
            scan_count++;
        }
    }

    /* ---- Step 2: JOIN across multiple scans ---- */

    wirelog_ir_node_t *current = NULL;
    char **cur_vars = NULL;
    uint32_t cur_vcount = 0;
    bool cur_vars_is_merged = false;

    if (scan_count == 1) {
        current = scans[0];
        cur_vars = scan_vars[0];
        cur_vcount = scan_vcounts[0];
    } else if (scan_count > 1) {
        current = scans[0];
        cur_vars = scan_vars[0];
        cur_vcount = scan_vcounts[0];

        for (uint32_t i = 1; i < scan_count; i++) {
            wirelog_ir_node_t *join = wl_ir_node_create(WIRELOG_IR_JOIN);
            if (!join)
                break;

            setup_join_keys(cur_vars, cur_vcount, scan_vars[i], scan_vcounts[i],
                join);

            wl_ir_node_add_child(join, current);
            wl_ir_node_add_child(join, scans[i]);

            uint32_t merged_count;
            char **merged = merge_var_names(cur_vars, cur_vcount, scan_vars[i],
                    scan_vcounts[i], &merged_count);
            if (cur_vars_is_merged) {
                free_var_names(cur_vars, cur_vcount);
            }

            cur_vars = merged;
            cur_vcount = merged_count;
            cur_vars_is_merged = true;
            current = join;
        }
    }

    /* ---- Step 3: FILTER for explicit comparisons ---- */

    for (uint32_t i = 1; i < rule_node->child_count; i++) {
        const wl_parser_ast_node_t *b = rule_node->children[i];
        if (b->type == WL_PARSER_AST_NODE_COMPARISON && current) {
            wirelog_ir_node_t *f = wl_ir_node_create(WIRELOG_IR_FILTER);
            if (!f)
                continue;
            f->filter_expr = convert_expr(b);
            wl_ir_node_add_child(f, current);
            current = f;
        } else if (b->type == WL_PARSER_AST_NODE_BOOLEAN) {
            if (!b->bool_value && current) {
                wirelog_ir_node_t *f = wl_ir_node_create(WIRELOG_IR_FILTER);
                if (!f)
                    continue;
                wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_BOOL);
                if (e)
                    e->bool_value = false;
                f->filter_expr = e;
                wl_ir_node_add_child(f, current);
                current = f;
            }
            /* Boolean True -> no-op */
        } else if (b->type == WL_PARSER_AST_NODE_STR_FUNCTION && current) {
            /* Standalone string predicate: contains(x, y), str_prefix(x, y), etc. */
            wirelog_ir_node_t *f = wl_ir_node_create(WIRELOG_IR_FILTER);
            if (!f)
                continue;
            f->filter_expr = convert_expr(b);
            wl_ir_node_add_child(f, current);
            current = f;
        }
    }

    /* ---- Step 4: ANTIJOIN for negations ---- */

    for (uint32_t i = 1; i < rule_node->child_count; i++) {
        const wl_parser_ast_node_t *b = rule_node->children[i];
        if (b->type == WL_PARSER_AST_NODE_NEGATION && b->child_count >= 1
            && current) {
            const wl_parser_ast_node_t *neg_atom = b->children[0];
            if (neg_atom->type != WL_PARSER_AST_NODE_ATOM)
                continue;

            char **neg_vars = NULL;
            uint32_t neg_vcount = 0;
            wirelog_ir_node_t *neg_scan
                = build_atom_scan(neg_atom, prog, &neg_vars, &neg_vcount);

            if (neg_scan) {
                wirelog_ir_node_t *aj = wl_ir_node_create(WIRELOG_IR_ANTIJOIN);
                if (aj) {
                    setup_join_keys(cur_vars, cur_vcount, neg_vars, neg_vcount,
                        aj);
                    wl_ir_node_add_child(aj, current);
                    wl_ir_node_add_child(aj, neg_scan);
                    current = aj;
                } else {
                    wl_ir_node_free(neg_scan);
                }
            }

            free_var_names(neg_vars, neg_vcount);
        }
    }

    /* ---- Step 5: PROJECT or AGGREGATE for head ---- */

    bool has_agg = false;
    const wl_parser_ast_node_t *agg_node = NULL;
    uint32_t non_agg_count = 0;

    for (uint32_t i = 0; i < head->child_count; i++) {
        if (head->children[i]->type == WL_PARSER_AST_NODE_AGGREGATE) {
            has_agg = true;
            agg_node = head->children[i];
        } else {
            non_agg_count++;
        }
    }

    wirelog_ir_node_t *root = NULL;

    if (has_agg && agg_node) {
        root = wl_ir_node_create(WIRELOG_IR_AGGREGATE);
        if (root) {
            wl_ir_node_set_relation(root, head->name);
            root->agg_fn = agg_node->agg_fn;

            if (agg_node->child_count >= 1) {
                root->agg_expr = convert_expr(agg_node->children[0]);
            }

            root->group_by_count = non_agg_count;
            if (non_agg_count > 0) {
                root->group_by_indices
                    = (uint32_t *)calloc(non_agg_count, sizeof(uint32_t));
                uint32_t gi = 0;
                for (uint32_t i = 0; i < head->child_count; i++) {
                    if (head->children[i]->type
                        != WL_PARSER_AST_NODE_AGGREGATE) {
                        /* Resolve head variable name to body column index */
                        const char *var = head->children[i]->name;
                        uint32_t col_idx = i; /* fallback to head position */
                        if (var) {
                            for (uint32_t j = 0; j < cur_vcount; j++) {
                                if (cur_vars[j]
                                    && strcmp(var, cur_vars[j]) == 0) {
                                    col_idx = j;
                                    break;
                                }
                            }
                        }
                        if (root->group_by_indices)
                            root->group_by_indices[gi++] = col_idx;
                    }
                }
            }

            if (current)
                wl_ir_node_add_child(root, current);
        }
    } else {
        root = wl_ir_node_create(WIRELOG_IR_PROJECT);
        if (root) {
            wl_ir_node_set_relation(root, head->name);
            root->project_count = head->child_count;

            if (head->child_count > 0) {
                root->project_exprs = (wl_ir_expr_t **)calloc(
                    head->child_count, sizeof(wl_ir_expr_t *));
                if (root->project_exprs) {
                    for (uint32_t i = 0; i < head->child_count; i++) {
                        root->project_exprs[i]
                            = convert_expr(head->children[i]);
                    }
                }
            }

            if (current)
                wl_ir_node_add_child(root, current);
        }
    }

    /* ---- Cleanup ---- */

    for (uint32_t i = 0; i < scan_count; i++) {
        free_var_names(scan_vars[i], scan_vcounts[i]);
    }
    if (cur_vars_is_merged) {
        free_var_names(cur_vars, cur_vcount);
    }

    free(scans);
    free(scan_vars);
    free(scan_vcounts);

    return root;
}

/* ---- Public Entry Point for Pass 2 ---- */

int
wl_ir_program_convert_rules(struct wirelog_program *program,
    const wl_parser_ast_node_t *ast)
{
    if (!program || !ast || ast->type != WL_PARSER_AST_NODE_PROGRAM)
        return -1;

    uint32_t rule_idx = 0;

    for (uint32_t i = 0; i < ast->child_count; i++) {
        const wl_parser_ast_node_t *child = ast->children[i];
        if (child->type != WL_PARSER_AST_NODE_RULE)
            continue;

        if (rule_idx >= program->rule_count)
            return -1;

        wirelog_ir_node_t *ir = convert_rule(child, program);
        if (!ir)
            return -1;

        program->rules[rule_idx].ir_root = ir;
        rule_idx++;
    }

    return 0;
}

/* ======================================================================== */
/* UNION Merging                                                            */
/* ======================================================================== */

int
wl_ir_program_merge_unions(struct wirelog_program *program)
{
    if (!program || program->relation_count == 0)
        return 0;

    program->relation_irs = (wirelog_ir_node_t **)calloc(
        program->relation_count, sizeof(wirelog_ir_node_t *));
    if (!program->relation_irs)
        return -1;

    for (uint32_t r = 0; r < program->relation_count; r++) {
        const char *rel_name = program->relations[r].name;
        if (!rel_name)
            continue;

        /* Count rules targeting this relation */
        uint32_t count = 0;
        for (uint32_t i = 0; i < program->rule_count; i++) {
            if (program->rules[i].head_relation
                && strcmp(program->rules[i].head_relation, rel_name) == 0) {
                count++;
            }
        }

        if (count == 0) {
            program->relation_irs[r] = NULL;
        } else if (count == 1) {
            /* Single rule: use directly (no UNION wrapper) */
            for (uint32_t i = 0; i < program->rule_count; i++) {
                if (program->rules[i].head_relation
                    && strcmp(program->rules[i].head_relation, rel_name) == 0) {
                    program->relation_irs[r] = program->rules[i].ir_root;
                    break;
                }
            }
        } else {
            /* Multiple rules: wrap in UNION */
            wirelog_ir_node_t *u = wl_ir_node_create(WIRELOG_IR_UNION);
            if (!u)
                return -1;
            wl_ir_node_set_relation(u, rel_name);

            for (uint32_t i = 0; i < program->rule_count; i++) {
                if (program->rules[i].head_relation
                    && strcmp(program->rules[i].head_relation, rel_name) == 0) {
                    wl_ir_node_add_child(u, program->rules[i].ir_root);
                }
            }

            program->relation_irs[r] = u;
        }
    }

    return 0;
}

/* ======================================================================== */
/* Magic Sets Support API                                                   */
/* ======================================================================== */

int
wl_ir_program_add_magic_relation(struct wirelog_program *prog, const char *name,
    uint32_t column_count)
{
    if (!prog || !name)
        return -1;

    /* No-op if already exists */
    if (find_relation(prog, name))
        return 0;

    wl_ir_relation_info_t *rel = add_relation(prog, name);
    if (!rel)
        return -1;

    /* Keep relation_irs in sync with relation_count so wl_ir_program_free
    * doesn't walk past the end of the array.  The new entry is NULL; it
    * will be filled in by the subsequent wl_ir_program_rebuild_relation_irs
    * call.  If realloc fails, NULL out relation_irs so free() is safe. */
    if (prog->relation_irs) {
        void *new_irs_v
            = realloc((void *)prog->relation_irs,
                prog->relation_count * sizeof(wirelog_ir_node_t *));
        if (new_irs_v) {
            prog->relation_irs = (wirelog_ir_node_t **)new_irs_v;
            prog->relation_irs[prog->relation_count - 1] = NULL;
        } else {
            free((void *)prog->relation_irs);
            prog->relation_irs = NULL;
        }
    }

    if (column_count > 0) {
        rel->columns = (wirelog_column_t *)calloc(column_count,
                sizeof(wirelog_column_t));
        if (!rel->columns)
            return -1;
        rel->column_count = column_count;
        for (uint32_t i = 0; i < column_count; i++) {
            char col_name[32];
            snprintf(col_name, sizeof(col_name), "c%u", i);
            rel->columns[i].name = strdup_safe(col_name);
            rel->columns[i].type = WIRELOG_TYPE_INT64;
        }
    }

    return 0;
}

int
wl_ir_program_add_magic_rule(struct wirelog_program *prog,
    const char *head_relation,
    wirelog_ir_node_t *ir_root)
{
    if (!prog || !head_relation || !ir_root)
        return -1;

    if (prog->rule_count >= prog->rule_capacity) {
        uint32_t new_cap = prog->rule_capacity == 0 ? INITIAL_CAPACITY
                                                    : prog->rule_capacity * 2;
        wl_ir_rule_ir_t *new_rules = (wl_ir_rule_ir_t *)realloc(
            prog->rules, new_cap * sizeof(wl_ir_rule_ir_t));
        if (!new_rules)
            return -1;
        prog->rules = new_rules;
        prog->rule_capacity = new_cap;
    }

    wl_ir_rule_ir_t *rule = &prog->rules[prog->rule_count++];
    memset(rule, 0, sizeof(wl_ir_rule_ir_t));
    rule->head_relation = strdup_safe(head_relation);
    rule->ir_root = ir_root;
    return 0;
}

int
wl_ir_program_rebuild_relation_irs(struct wirelog_program *prog)
{
    if (!prog)
        return -1;

    /* Free old relation_irs: UNION wrappers only, not rule ir_roots */
    if (prog->relation_irs) {
        for (uint32_t i = 0; i < prog->relation_count; i++) {
            if (prog->relation_irs[i]
                && prog->relation_irs[i]->type == WIRELOG_IR_UNION) {
                free(prog->relation_irs[i]->children);
                prog->relation_irs[i]->children = NULL;
                prog->relation_irs[i]->child_count = 0;
                wl_ir_node_free(prog->relation_irs[i]);
            }
        }
        free(prog->relation_irs);
        prog->relation_irs = NULL;
    }

    return wl_ir_program_merge_unions(prog);
}

void
wl_ir_program_free_strata(struct wirelog_program *prog)
{
    if (!prog || !prog->strata)
        return;

    for (uint32_t i = 0; i < prog->stratum_count; i++)
        free((void *)prog->strata[i].rule_names);

    free(prog->strata);
    prog->strata = NULL;
    prog->stratum_count = 0;
    prog->is_stratified = false;
}
