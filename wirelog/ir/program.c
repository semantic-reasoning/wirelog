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

/* ======================================================================== */
/* Pass 2: AST-to-IR Rule Conversion                                        */
/* ======================================================================== */

/* ---- Expression Conversion (AST expr -> IR expr) ---- */

static wl_ir_expr_t*
convert_expr(const wl_ast_node_t *node)
{
    if (!node) return NULL;

    switch (node->type) {
    case WL_NODE_VARIABLE: {
        wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_VAR);
        if (e) e->var_name = strdup_safe(node->name);
        return e;
    }
    case WL_NODE_INTEGER: {
        wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_CONST_INT);
        if (e) e->int_value = node->int_value;
        return e;
    }
    case WL_NODE_STRING: {
        wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_CONST_STR);
        if (e) e->str_value = strdup_safe(node->str_value);
        return e;
    }
    case WL_NODE_BINARY_EXPR: {
        wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_ARITH);
        if (!e) return NULL;
        e->arith_op = node->arith_op;
        if (node->child_count >= 2) {
            wl_ir_expr_add_child(e, convert_expr(node->children[0]));
            wl_ir_expr_add_child(e, convert_expr(node->children[1]));
        }
        return e;
    }
    case WL_NODE_COMPARISON: {
        wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_CMP);
        if (!e) return NULL;
        e->cmp_op = node->cmp_op;
        if (node->child_count >= 2) {
            wl_ir_expr_add_child(e, convert_expr(node->children[0]));
            wl_ir_expr_add_child(e, convert_expr(node->children[1]));
        }
        return e;
    }
    case WL_NODE_AGGREGATE: {
        wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_AGG);
        if (!e) return NULL;
        e->agg_fn = node->agg_fn;
        if (node->child_count >= 1) {
            wl_ir_expr_add_child(e, convert_expr(node->children[0]));
        }
        return e;
    }
    case WL_NODE_BOOLEAN: {
        wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_BOOL);
        if (e) e->bool_value = node->bool_value;
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
    if (!names) return;
    for (uint32_t i = 0; i < count; i++) {
        free(names[i]);
    }
    free(names);
}

static char**
merge_var_names(char **left, uint32_t left_count,
                char **right, uint32_t right_count,
                uint32_t *out_count)
{
    uint32_t max_count = left_count + right_count;
    char **merged = (char **)calloc(max_count > 0 ? max_count : 1, sizeof(char *));
    if (!merged) { *out_count = 0; return NULL; }

    uint32_t count = 0;

    for (uint32_t i = 0; i < left_count; i++) {
        if (!left[i]) continue;
        bool dup = false;
        for (uint32_t j = 0; j < count; j++) {
            if (merged[j] && strcmp(merged[j], left[i]) == 0) { dup = true; break; }
        }
        if (!dup) merged[count++] = strdup_safe(left[i]);
    }

    for (uint32_t i = 0; i < right_count; i++) {
        if (!right[i]) continue;
        bool dup = false;
        for (uint32_t j = 0; j < count; j++) {
            if (merged[j] && strcmp(merged[j], right[i]) == 0) { dup = true; break; }
        }
        if (!dup) merged[count++] = strdup_safe(right[i]);
    }

    *out_count = count;
    return merged;
}

/* ---- Join Key Extraction ---- */

static void
setup_join_keys(char **left_vars, uint32_t left_count,
                char **right_vars, uint32_t right_count,
                wirelog_ir_node_t *join)
{
    /* Count shared variables (exclude NULL = wildcard/constant) */
    uint32_t key_count = 0;
    for (uint32_t i = 0; i < left_count; i++) {
        if (!left_vars[i]) continue;
        for (uint32_t j = 0; j < right_count; j++) {
            if (!right_vars[j]) continue;
            if (strcmp(left_vars[i], right_vars[j]) == 0) {
                key_count++;
                break;
            }
        }
    }

    if (key_count == 0) return;

    join->join_left_keys = (char **)calloc(key_count, sizeof(char *));
    join->join_right_keys = (char **)calloc(key_count, sizeof(char *));
    if (!join->join_left_keys || !join->join_right_keys) return;
    join->join_key_count = key_count;

    uint32_t k = 0;
    for (uint32_t i = 0; i < left_count; i++) {
        if (!left_vars[i]) continue;
        for (uint32_t j = 0; j < right_count; j++) {
            if (!right_vars[j]) continue;
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

static wirelog_ir_node_t*
build_atom_scan(const wl_ast_node_t *atom,
                char ***out_var_names, uint32_t *out_var_count)
{
    wirelog_ir_node_t *scan = wl_ir_node_create(WIRELOG_IR_SCAN);
    if (!scan) return NULL;

    wl_ir_node_set_relation(scan, atom->name);

    uint32_t arg_count = atom->child_count;
    scan->column_names = (char **)calloc(arg_count > 0 ? arg_count : 1, sizeof(char *));
    scan->column_count = arg_count;

    char **var_names = (char **)calloc(arg_count > 0 ? arg_count : 1, sizeof(char *));
    if (!scan->column_names || !var_names) {
        free(var_names);
        wl_ir_node_free(scan);
        return NULL;
    }

    /* Collect column names from atom arguments */
    for (uint32_t i = 0; i < arg_count; i++) {
        wl_ast_node_t *arg = atom->children[i];
        if (arg->type == WL_NODE_VARIABLE) {
            scan->column_names[i] = strdup_safe(arg->name);
            var_names[i] = strdup_safe(arg->name);
        } else {
            /* Wildcard, integer, string -> NULL (anonymous position) */
            scan->column_names[i] = NULL;
            var_names[i] = NULL;
        }
    }

    wirelog_ir_node_t *result = scan;

    /* Step 1a: Intra-atom FILTER for duplicate variables */
    for (uint32_t i = 0; i < arg_count; i++) {
        if (!var_names[i]) continue;
        for (uint32_t j = i + 1; j < arg_count; j++) {
            if (!var_names[j]) continue;
            if (strcmp(var_names[i], var_names[j]) == 0) {
                wirelog_ir_node_t *f = wl_ir_node_create(WIRELOG_IR_FILTER);
                if (!f) continue;

                wl_ir_expr_t *cmp = wl_ir_expr_create(WL_IR_EXPR_CMP);
                if (cmp) {
                    cmp->cmp_op = WL_CMP_EQ;
                    wl_ir_expr_t *lhs = wl_ir_expr_create(WL_IR_EXPR_VAR);
                    if (lhs) lhs->var_name = strdup_safe(var_names[i]);
                    wl_ir_expr_t *rhs = wl_ir_expr_create(WL_IR_EXPR_VAR);
                    if (rhs) rhs->var_name = strdup_safe(var_names[j]);
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
        wl_ast_node_t *arg = atom->children[i];
        if (arg->type == WL_NODE_INTEGER) {
            wirelog_ir_node_t *f = wl_ir_node_create(WIRELOG_IR_FILTER);
            if (!f) continue;

            wl_ir_expr_t *cmp = wl_ir_expr_create(WL_IR_EXPR_CMP);
            if (cmp) {
                cmp->cmp_op = WL_CMP_EQ;
                wl_ir_expr_t *lhs = wl_ir_expr_create(WL_IR_EXPR_VAR);
                if (lhs) {
                    char col[32];
                    snprintf(col, sizeof(col), "col%u", i);
                    lhs->var_name = strdup_safe(col);
                }
                wl_ir_expr_t *rhs = wl_ir_expr_create(WL_IR_EXPR_CONST_INT);
                if (rhs) rhs->int_value = arg->int_value;
                wl_ir_expr_add_child(cmp, lhs);
                wl_ir_expr_add_child(cmp, rhs);
            }
            f->filter_expr = cmp;
            wl_ir_node_add_child(f, result);
            result = f;
        } else if (arg->type == WL_NODE_STRING) {
            wirelog_ir_node_t *f = wl_ir_node_create(WIRELOG_IR_FILTER);
            if (!f) continue;

            wl_ir_expr_t *cmp = wl_ir_expr_create(WL_IR_EXPR_CMP);
            if (cmp) {
                cmp->cmp_op = WL_CMP_EQ;
                wl_ir_expr_t *lhs = wl_ir_expr_create(WL_IR_EXPR_VAR);
                if (lhs) {
                    char col[32];
                    snprintf(col, sizeof(col), "col%u", i);
                    lhs->var_name = strdup_safe(col);
                }
                wl_ir_expr_t *rhs = wl_ir_expr_create(WL_IR_EXPR_CONST_STR);
                if (rhs) rhs->str_value = strdup_safe(arg->str_value);
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

static wirelog_ir_node_t*
convert_rule(const wl_ast_node_t *rule_node)
{
    if (!rule_node || rule_node->child_count < 1) return NULL;

    wl_ast_node_t *head = rule_node->children[0];
    if (head->type != WL_NODE_HEAD) return NULL;

    uint32_t body_count = rule_node->child_count - 1;

    /* ---- Step 1: Collect positive atoms -> SCANs ---- */

    uint32_t scan_cap = body_count > 0 ? body_count : 1;
    wirelog_ir_node_t **scans = (wirelog_ir_node_t **)calloc(
        scan_cap, sizeof(wirelog_ir_node_t *));
    char ***scan_vars = (char ***)calloc(scan_cap, sizeof(char **));
    uint32_t *scan_vcounts = (uint32_t *)calloc(scan_cap, sizeof(uint32_t));
    if (!scans || !scan_vars || !scan_vcounts) {
        free(scans); free(scan_vars); free(scan_vcounts);
        return NULL;
    }

    uint32_t scan_count = 0;
    for (uint32_t i = 1; i < rule_node->child_count; i++) {
        wl_ast_node_t *b = rule_node->children[i];
        if (b->type == WL_NODE_ATOM) {
            scans[scan_count] = build_atom_scan(b,
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
            if (!join) break;

            setup_join_keys(cur_vars, cur_vcount,
                            scan_vars[i], scan_vcounts[i], join);

            wl_ir_node_add_child(join, current);
            wl_ir_node_add_child(join, scans[i]);

            uint32_t merged_count;
            char **merged = merge_var_names(cur_vars, cur_vcount,
                                            scan_vars[i], scan_vcounts[i],
                                            &merged_count);
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
        wl_ast_node_t *b = rule_node->children[i];
        if (b->type == WL_NODE_COMPARISON && current) {
            wirelog_ir_node_t *f = wl_ir_node_create(WIRELOG_IR_FILTER);
            if (!f) continue;
            f->filter_expr = convert_expr(b);
            wl_ir_node_add_child(f, current);
            current = f;
        } else if (b->type == WL_NODE_BOOLEAN) {
            if (!b->bool_value && current) {
                wirelog_ir_node_t *f = wl_ir_node_create(WIRELOG_IR_FILTER);
                if (!f) continue;
                wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_BOOL);
                if (e) e->bool_value = false;
                f->filter_expr = e;
                wl_ir_node_add_child(f, current);
                current = f;
            }
            /* Boolean True -> no-op */
        }
    }

    /* ---- Step 4: ANTIJOIN for negations ---- */

    for (uint32_t i = 1; i < rule_node->child_count; i++) {
        wl_ast_node_t *b = rule_node->children[i];
        if (b->type == WL_NODE_NEGATION && b->child_count >= 1 && current) {
            wl_ast_node_t *neg_atom = b->children[0];
            if (neg_atom->type != WL_NODE_ATOM) continue;

            char **neg_vars = NULL;
            uint32_t neg_vcount = 0;
            wirelog_ir_node_t *neg_scan = build_atom_scan(neg_atom,
                &neg_vars, &neg_vcount);

            if (neg_scan) {
                wirelog_ir_node_t *aj = wl_ir_node_create(WIRELOG_IR_ANTIJOIN);
                if (aj) {
                    setup_join_keys(cur_vars, cur_vcount,
                                    neg_vars, neg_vcount, aj);
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
    wl_ast_node_t *agg_node = NULL;
    uint32_t non_agg_count = 0;

    for (uint32_t i = 0; i < head->child_count; i++) {
        if (head->children[i]->type == WL_NODE_AGGREGATE) {
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
                root->group_by_indices = (uint32_t *)calloc(
                    non_agg_count, sizeof(uint32_t));
                uint32_t gi = 0;
                for (uint32_t i = 0; i < head->child_count; i++) {
                    if (head->children[i]->type != WL_NODE_AGGREGATE) {
                        if (root->group_by_indices)
                            root->group_by_indices[gi++] = i;
                    }
                }
            }

            if (current) wl_ir_node_add_child(root, current);
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
                        root->project_exprs[i] = convert_expr(
                            head->children[i]);
                    }
                }
            }

            if (current) wl_ir_node_add_child(root, current);
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
wl_program_convert_rules(struct wirelog_program *program,
                         const wl_ast_node_t *ast)
{
    if (!program || !ast || ast->type != WL_NODE_PROGRAM) return -1;

    uint32_t rule_idx = 0;

    for (uint32_t i = 0; i < ast->child_count; i++) {
        wl_ast_node_t *child = ast->children[i];
        if (child->type != WL_NODE_RULE) continue;

        if (rule_idx >= program->rule_count) return -1;

        wirelog_ir_node_t *ir = convert_rule(child);
        if (!ir) return -1;

        program->rules[rule_idx].ir_root = ir;
        rule_idx++;
    }

    return 0;
}

/* ======================================================================== */
/* UNION Merging                                                            */
/* ======================================================================== */

int
wl_program_merge_unions(struct wirelog_program *program)
{
    if (!program || program->relation_count == 0) return 0;

    program->relation_irs = (wirelog_ir_node_t **)calloc(
        program->relation_count, sizeof(wirelog_ir_node_t *));
    if (!program->relation_irs) return -1;

    for (uint32_t r = 0; r < program->relation_count; r++) {
        const char *rel_name = program->relations[r].name;
        if (!rel_name) continue;

        /* Count rules targeting this relation */
        uint32_t count = 0;
        for (uint32_t i = 0; i < program->rule_count; i++) {
            if (program->rules[i].head_relation &&
                strcmp(program->rules[i].head_relation, rel_name) == 0) {
                count++;
            }
        }

        if (count == 0) {
            program->relation_irs[r] = NULL;
        } else if (count == 1) {
            /* Single rule: use directly (no UNION wrapper) */
            for (uint32_t i = 0; i < program->rule_count; i++) {
                if (program->rules[i].head_relation &&
                    strcmp(program->rules[i].head_relation, rel_name) == 0) {
                    program->relation_irs[r] = program->rules[i].ir_root;
                    break;
                }
            }
        } else {
            /* Multiple rules: wrap in UNION */
            wirelog_ir_node_t *u = wl_ir_node_create(WIRELOG_IR_UNION);
            if (!u) return -1;
            wl_ir_node_set_relation(u, rel_name);

            for (uint32_t i = 0; i < program->rule_count; i++) {
                if (program->rules[i].head_relation &&
                    strcmp(program->rules[i].head_relation, rel_name) == 0) {
                    wl_ir_node_add_child(u, program->rules[i].ir_root);
                }
            }

            program->relation_irs[r] = u;
        }
    }

    return 0;
}
