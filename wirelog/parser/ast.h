/*
 * ast.h - wirelog Abstract Syntax Tree
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
 * AST design based on FlowLog grammar analysis.
 * See discussion/FlowLog_Grammar_Analysis.md for grammar reference.
 */

#ifndef WL_PARSER_AST_H
#define WL_PARSER_AST_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include "../wirelog-types.h"

/* ======================================================================== */
/* AST Node Types                                                           */
/* ======================================================================== */

typedef enum {
    /* Top-level */
    WL_PARSER_AST_NODE_PROGRAM, /* children = declarations + rules */

    /* Declarations */
    WL_PARSER_AST_NODE_DECL,  /* .decl name(params): children = typed_params */
    WL_PARSER_AST_NODE_INPUT, /* .input name(params): children = input_params */
    WL_PARSER_AST_NODE_OUTPUT,    /* .output name */
    WL_PARSER_AST_NODE_PRINTSIZE, /* .printsize name */

    /* Rule structure */
    WL_PARSER_AST_NODE_RULE, /* children[0]=head, children[1..]=body; is_planning */
    WL_PARSER_AST_NODE_HEAD,     /* name=relation, children=head_args */
    WL_PARSER_AST_NODE_ATOM,     /* name=relation, children=atom_args */
    WL_PARSER_AST_NODE_NEGATION, /* !atom: children[0]=atom */
    WL_PARSER_AST_NODE_COMPARISON, /* cmp_op: children[0]=left, children[1]=right */
    WL_PARSER_AST_NODE_BOOLEAN, /* bool_value (True/False predicate) */

    /* Terms / Arguments */
    WL_PARSER_AST_NODE_VARIABLE,  /* name = variable name */
    WL_PARSER_AST_NODE_INTEGER,   /* int_value = integer constant */
    WL_PARSER_AST_NODE_STRING,    /* str_value = string constant */
    WL_PARSER_AST_NODE_WILDCARD,  /* _ placeholder */
    WL_PARSER_AST_NODE_AGGREGATE, /* agg_fn, children[0] = arithmetic expr */

    /* Expressions */
    WL_PARSER_AST_NODE_BINARY_EXPR, /* arith_op, children[0]=left, children[1]=right */

    /* Inline facts */
    WL_PARSER_AST_NODE_FACT, /* name=relation, children=constant args */

    /* Declaration parts */
    WL_PARSER_AST_NODE_TYPED_PARAM, /* name=attr_name, type_name=data_type */
    WL_PARSER_AST_NODE_INPUT_PARAM, /* name=param_name, str_value=param_value */
    WL_PARSER_AST_NODE_OUTPUT_PARAM, /* name=param_name, str_value=param_value */

    /* Query directive */
    WL_PARSER_AST_NODE_QUERY, /* .query name(b,f,...) .: name=relation, int_value=bound_mask */
} wl_parser_ast_node_type_t;

/* ======================================================================== */
/* AST Node                                                                 */
/* ======================================================================== */

typedef struct wl_parser_ast_node wl_parser_ast_node_t;

struct wl_parser_ast_node {
    wl_parser_ast_node_type_t type;

    /* Source location */
    uint32_t line;
    uint32_t col;

    /* Node data (usage depends on type) */
    char *name;        /* Relation/variable/attribute name */
    int64_t int_value; /* Integer constant value */
    char *str_value;   /* String constant or param value */
    bool bool_value;   /* Boolean predicate value */
    bool is_planning;  /* .plan optimization marker (rules only) */

    /* Operator (for comparisons, arithmetic, aggregates) */
    wirelog_cmp_op_t cmp_op;
    wirelog_arith_op_t arith_op;
    wirelog_agg_fn_t agg_fn;

    /* Type name for typed parameters (int32, int64, string) */
    char *type_name;

    /* Children */
    wl_parser_ast_node_t **children;
    uint32_t child_count;
    uint32_t child_capacity;

    /* Adornment count for WL_PARSER_AST_NODE_QUERY nodes */
    uint32_t query_arity;
};

/* ======================================================================== */
/* AST Construction API                                                     */
/* ======================================================================== */

wl_parser_ast_node_t *
wl_parser_ast_node_create(wl_parser_ast_node_type_t type, uint32_t line,
    uint32_t col);

void
wl_parser_ast_node_add_child(wl_parser_ast_node_t *parent,
    wl_parser_ast_node_t *child);

void
wl_parser_ast_node_set_name(wl_parser_ast_node_t *node, const char *name);

void
wl_parser_ast_node_set_str_value(wl_parser_ast_node_t *node, const char *value);

void
wl_parser_ast_node_set_type_name(wl_parser_ast_node_t *node,
    const char *type_name);

void
wl_parser_ast_node_free(wl_parser_ast_node_t *node);

/* ======================================================================== */
/* AST Printing / Debugging                                                 */
/* ======================================================================== */

void
wl_parser_ast_print(const wl_parser_ast_node_t *node, int indent);

const char *
wl_parser_ast_node_type_str(wl_parser_ast_node_type_t type);

#ifdef __cplusplus
}
#endif

#endif /* WL_PARSER_AST_H */
