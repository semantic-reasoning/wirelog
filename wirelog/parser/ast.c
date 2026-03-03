/*
 * ast.c - wirelog Abstract Syntax Tree Implementation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 */

#include "ast.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* AST Construction                                                         */
/* ======================================================================== */

#define INITIAL_CHILD_CAPACITY 4

wl_parser_ast_node_t *
wl_parser_ast_node_create(wl_parser_ast_node_type_t type, uint32_t line, uint32_t col)
{
    wl_parser_ast_node_t *node = (wl_parser_ast_node_t *)calloc(1, sizeof(wl_parser_ast_node_t));
    if (!node)
        return NULL;

    node->type = type;
    node->line = line;
    node->col = col;

    return node;
}

void
wl_parser_ast_node_add_child(wl_parser_ast_node_t *parent, wl_parser_ast_node_t *child)
{
    if (!parent || !child)
        return;

    if (parent->child_count >= parent->child_capacity) {
        uint32_t new_cap = parent->child_capacity == 0
                               ? INITIAL_CHILD_CAPACITY
                               : parent->child_capacity * 2;
        wl_parser_ast_node_t **new_children = (wl_parser_ast_node_t **)realloc(
            parent->children, new_cap * sizeof(wl_parser_ast_node_t *));
        if (!new_children)
            return;
        parent->children = new_children;
        parent->child_capacity = new_cap;
    }

    parent->children[parent->child_count++] = child;
}

static char *
strdup_safe(const char *s)
{
    if (!s)
        return NULL;
    size_t len = strlen(s);
    char *dup = (char *)malloc(len + 1);
    if (dup) {
        memcpy(dup, s, len + 1);
    }
    return dup;
}

void
wl_parser_ast_node_set_name(wl_parser_ast_node_t *node, const char *name)
{
    if (!node)
        return;
    free(node->name);
    node->name = strdup_safe(name);
}

void
wl_parser_ast_node_set_str_value(wl_parser_ast_node_t *node, const char *value)
{
    if (!node)
        return;
    free(node->str_value);
    node->str_value = strdup_safe(value);
}

void
wl_parser_ast_node_set_type_name(wl_parser_ast_node_t *node, const char *type_name)
{
    if (!node)
        return;
    free(node->type_name);
    node->type_name = strdup_safe(type_name);
}

void
wl_parser_ast_node_free(wl_parser_ast_node_t *node)
{
    if (!node)
        return;

    for (uint32_t i = 0; i < node->child_count; i++) {
        wl_parser_ast_node_free(node->children[i]);
    }
    free(node->children);
    free(node->name);
    free(node->str_value);
    free(node->type_name);
    free(node);
}

/* ======================================================================== */
/* AST Printing                                                             */
/* ======================================================================== */

const char *
wl_parser_ast_node_type_str(wl_parser_ast_node_type_t type)
{
    switch (type) {
    case WL_PARSER_AST_NODE_PROGRAM:
        return "PROGRAM";
    case WL_PARSER_AST_NODE_DECL:
        return "DECL";
    case WL_PARSER_AST_NODE_INPUT:
        return "INPUT";
    case WL_PARSER_AST_NODE_OUTPUT:
        return "OUTPUT";
    case WL_PARSER_AST_NODE_PRINTSIZE:
        return "PRINTSIZE";
    case WL_PARSER_AST_NODE_RULE:
        return "RULE";
    case WL_PARSER_AST_NODE_HEAD:
        return "HEAD";
    case WL_PARSER_AST_NODE_ATOM:
        return "ATOM";
    case WL_PARSER_AST_NODE_NEGATION:
        return "NEGATION";
    case WL_PARSER_AST_NODE_COMPARISON:
        return "COMPARISON";
    case WL_PARSER_AST_NODE_BOOLEAN:
        return "BOOLEAN";
    case WL_PARSER_AST_NODE_VARIABLE:
        return "VARIABLE";
    case WL_PARSER_AST_NODE_INTEGER:
        return "INTEGER";
    case WL_PARSER_AST_NODE_STRING:
        return "STRING";
    case WL_PARSER_AST_NODE_WILDCARD:
        return "WILDCARD";
    case WL_PARSER_AST_NODE_AGGREGATE:
        return "AGGREGATE";
    case WL_PARSER_AST_NODE_BINARY_EXPR:
        return "BINARY_EXPR";
    case WL_PARSER_AST_NODE_FACT:
        return "FACT";
    case WL_PARSER_AST_NODE_TYPED_PARAM:
        return "TYPED_PARAM";
    case WL_PARSER_AST_NODE_INPUT_PARAM:
        return "INPUT_PARAM";
    }
    return "UNKNOWN";
}

const char *
wirelog_cmp_op_str(wirelog_cmp_op_t op)
{
    switch (op) {
    case WIRELOG_CMP_EQ:
        return "=";
    case WIRELOG_CMP_NEQ:
        return "!=";
    case WIRELOG_CMP_LT:
        return "<";
    case WIRELOG_CMP_GT:
        return ">";
    case WIRELOG_CMP_LTE:
        return "<=";
    case WIRELOG_CMP_GTE:
        return ">=";
    }
    return "?";
}

const char *
wirelog_arith_op_str(wirelog_arith_op_t op)
{
    switch (op) {
    case WIRELOG_ARITH_ADD:
        return "+";
    case WIRELOG_ARITH_SUB:
        return "-";
    case WIRELOG_ARITH_MUL:
        return "*";
    case WIRELOG_ARITH_DIV:
        return "/";
    case WIRELOG_ARITH_MOD:
        return "%";
    }
    return "?";
}

const char *
wirelog_agg_fn_str(wirelog_agg_fn_t fn)
{
    switch (fn) {
    case WIRELOG_AGG_COUNT:
        return "count";
    case WIRELOG_AGG_SUM:
        return "sum";
    case WIRELOG_AGG_MIN:
        return "min";
    case WIRELOG_AGG_MAX:
        return "max";
    case WIRELOG_AGG_AVG:
        return "avg";
    }
    return "?";
}

void
wl_parser_ast_print(const wl_parser_ast_node_t *node, int indent)
{
    if (!node)
        return;

    for (int i = 0; i < indent; i++)
        fprintf(stderr, "  ");

    fprintf(stderr, "%s", wl_parser_ast_node_type_str(node->type));

    if (node->name)
        fprintf(stderr, " name=\"%s\"", node->name);
    if (node->type == WL_PARSER_AST_NODE_INTEGER)
        fprintf(stderr, " value=%lld", (long long)node->int_value);
    if (node->str_value)
        fprintf(stderr, " str=\"%s\"", node->str_value);
    if (node->type == WL_PARSER_AST_NODE_BOOLEAN)
        fprintf(stderr, " value=%s", node->bool_value ? "True" : "False");
    if (node->type == WL_PARSER_AST_NODE_COMPARISON)
        fprintf(stderr, " op=%s", wirelog_cmp_op_str(node->cmp_op));
    if (node->type == WL_PARSER_AST_NODE_BINARY_EXPR)
        fprintf(stderr, " op=%s", wirelog_arith_op_str(node->arith_op));
    if (node->type == WL_PARSER_AST_NODE_AGGREGATE)
        fprintf(stderr, " fn=%s", wirelog_agg_fn_str(node->agg_fn));
    if (node->type_name)
        fprintf(stderr, " type=%s", node->type_name);
    if (node->is_planning)
        fprintf(stderr, " [.plan]");

    fprintf(stderr, "\n");

    for (uint32_t i = 0; i < node->child_count; i++) {
        wl_parser_ast_print(node->children[i], indent + 1);
    }
}
