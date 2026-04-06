/*
 * ir.c - wirelog IR Node Implementation
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Implements IR node and expression creation, destruction,
 * and the 6 public API functions declared in wirelog-ir.h.
 */

#include "ir.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INITIAL_CHILD_CAPACITY 4

/* ======================================================================== */
/* Utility                                                                  */
/* ======================================================================== */

char *
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

/* ======================================================================== */
/* Expression API                                                           */
/* ======================================================================== */

wl_ir_expr_t *
wl_ir_expr_create(wl_ir_expr_type_t type)
{
    wl_ir_expr_t *expr = (wl_ir_expr_t *)calloc(1, sizeof(wl_ir_expr_t));
    if (!expr)
        return NULL;
    expr->type = type;
    return expr;
}

void
wl_ir_expr_add_child(wl_ir_expr_t *parent, wl_ir_expr_t *child)
{
    if (!parent || !child)
        return;

    if (parent->child_count >= parent->child_capacity) {
        uint32_t new_cap = parent->child_capacity == 0
                               ? INITIAL_CHILD_CAPACITY
                               : parent->child_capacity * 2;
        wl_ir_expr_t **new_children = (wl_ir_expr_t **)realloc(
            parent->children, new_cap * sizeof(wl_ir_expr_t *));
        if (!new_children)
            return;
        parent->children = new_children;
        parent->child_capacity = new_cap;
    }

    parent->children[parent->child_count++] = child;
}

void
wl_ir_expr_free(wl_ir_expr_t *expr)
{
    if (!expr)
        return;

    for (uint32_t i = 0; i < expr->child_count; i++) {
        wl_ir_expr_free(expr->children[i]);
    }
    free(expr->children);
    free(expr->var_name);
    free(expr->str_value);
    free(expr);
}

wl_ir_expr_t *
wl_ir_expr_clone(const wl_ir_expr_t *expr)
{
    if (!expr)
        return NULL;

    wl_ir_expr_t *clone = wl_ir_expr_create(expr->type);
    if (!clone)
        return NULL;

    clone->int_value = expr->int_value;
    clone->bool_value = expr->bool_value;
    clone->arith_op = expr->arith_op;
    clone->cmp_op = expr->cmp_op;
    clone->agg_fn = expr->agg_fn;
    clone->str_fn = expr->str_fn;

    if (expr->var_name) {
        clone->var_name = strdup_safe(expr->var_name);
        if (!clone->var_name) {
            wl_ir_expr_free(clone);
            return NULL;
        }
    }

    if (expr->str_value) {
        clone->str_value = strdup_safe(expr->str_value);
        if (!clone->str_value) {
            wl_ir_expr_free(clone);
            return NULL;
        }
    }

    for (uint32_t i = 0; i < expr->child_count; i++) {
        wl_ir_expr_t *child_clone = wl_ir_expr_clone(expr->children[i]);
        if (!child_clone) {
            wl_ir_expr_free(clone);
            return NULL;
        }
        wl_ir_expr_add_child(clone, child_clone);
    }

    return clone;
}

/* ======================================================================== */
/* IR Node Construction                                                     */
/* ======================================================================== */

wirelog_ir_node_t *
wl_ir_node_create(wirelog_ir_node_type_t type)
{
    wirelog_ir_node_t *node
        = (wirelog_ir_node_t *)calloc(1, sizeof(wirelog_ir_node_t));
    if (!node)
        return NULL;
    node->type = type;
    return node;
}

void
wl_ir_node_set_relation(wirelog_ir_node_t *node, const char *name)
{
    if (!node)
        return;
    free(node->relation_name);
    node->relation_name = strdup_safe(name);
}

void
wl_ir_node_add_child(wirelog_ir_node_t *node, wirelog_ir_node_t *child)
{
    if (!node || !child)
        return;

    if (node->child_count >= node->child_capacity) {
        uint32_t new_cap = node->child_capacity == 0 ? INITIAL_CHILD_CAPACITY
                                                     : node->child_capacity * 2;
        wirelog_ir_node_t **new_children = (wirelog_ir_node_t **)realloc(
            node->children, new_cap * sizeof(wirelog_ir_node_t *));
        if (!new_children)
            return;
        node->children = new_children;
        node->child_capacity = new_cap;
    }

    node->children[node->child_count++] = child;
}

void
wl_ir_node_free(wirelog_ir_node_t *node)
{
    if (!node)
        return;

    /* Free children recursively */
    for (uint32_t i = 0; i < node->child_count; i++) {
        wl_ir_node_free(node->children[i]);
    }
    free(node->children);

    /* Free relation name */
    free(node->relation_name);

    /* Free SCAN column names */
    if (node->column_names) {
        for (uint32_t i = 0; i < node->column_count; i++) {
            free(node->column_names[i]); /* NULL-safe: free(NULL) is no-op */
        }
        free(node->column_names);
    }

    /* Free PROJECT data */
    free(node->project_indices);
    if (node->project_exprs) {
        for (uint32_t i = 0; i < node->project_count; i++) {
            wl_ir_expr_free(node->project_exprs[i]);
        }
        free(node->project_exprs);
    }

    /* Free FILTER expression */
    wl_ir_expr_free(node->filter_expr);

    /* Free JOIN keys */
    if (node->join_left_keys) {
        for (uint32_t i = 0; i < node->join_key_count; i++) {
            free(node->join_left_keys[i]);
        }
        free(node->join_left_keys);
    }
    if (node->join_right_keys) {
        for (uint32_t i = 0; i < node->join_key_count; i++) {
            free(node->join_right_keys[i]);
        }
        free(node->join_right_keys);
    }

    /* Free AGGREGATE data */
    wl_ir_expr_free(node->agg_expr);
    free(node->group_by_indices);

    free(node);
}

/* ======================================================================== */
/* Public API (wirelog-ir.h)                                                */
/* ======================================================================== */

wirelog_ir_node_type_t
wirelog_ir_node_get_type(const wirelog_ir_node_t *node)
{
    if (!node)
        return WIRELOG_IR_SCAN;
    return node->type;
}

const char *
wirelog_ir_node_get_relation_name(const wirelog_ir_node_t *node)
{
    if (!node)
        return NULL;
    return node->relation_name;
}

uint32_t
wirelog_ir_node_get_child_count(const wirelog_ir_node_t *node)
{
    if (!node)
        return 0;
    return node->child_count;
}

const wirelog_ir_node_t *
wirelog_ir_node_get_child(const wirelog_ir_node_t *node, uint32_t index)
{
    if (!node || index >= node->child_count)
        return NULL;
    return node->children[index];
}

/* ======================================================================== */
/* IR Printing / Debugging                                                  */
/* ======================================================================== */

static const char *
ir_node_type_str(wirelog_ir_node_type_t type)
{
    switch (type) {
    case WIRELOG_IR_SCAN:
        return "SCAN";
    case WIRELOG_IR_PROJECT:
        return "PROJECT";
    case WIRELOG_IR_FILTER:
        return "FILTER";
    case WIRELOG_IR_JOIN:
        return "JOIN";
    case WIRELOG_IR_FLATMAP:
        return "FLATMAP";
    case WIRELOG_IR_AGGREGATE:
        return "AGGREGATE";
    case WIRELOG_IR_ANTIJOIN:
        return "ANTIJOIN";
    case WIRELOG_IR_UNION:
        return "UNION";
    case WIRELOG_IR_SEMIJOIN:
        return "SEMIJOIN";
    }
    return "UNKNOWN";
}

static void
ir_expr_to_buf(const wl_ir_expr_t *expr, char *buf, size_t bufsize, size_t *pos)
{
    if (!expr || *pos >= bufsize - 1)
        return;

    switch (expr->type) {
    case WL_IR_EXPR_VAR:
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, "%s",
                expr->var_name ? expr->var_name : "?");
        break;
    case WL_IR_EXPR_CONST_INT:
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, "%lld",
                (long long)expr->int_value);
        break;
    case WL_IR_EXPR_CONST_STR:
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, "\"%s\"",
                expr->str_value ? expr->str_value : "");
        break;
    case WL_IR_EXPR_ARITH:
        if (expr->child_count >= 2) {
            *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, "(");
            ir_expr_to_buf(expr->children[0], buf, bufsize, pos);
            *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, " %s ",
                    wirelog_arith_op_str(expr->arith_op));
            ir_expr_to_buf(expr->children[1], buf, bufsize, pos);
            *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, ")");
        }
        break;
    case WL_IR_EXPR_CMP:
        if (expr->child_count >= 2) {
            ir_expr_to_buf(expr->children[0], buf, bufsize, pos);
            *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, " %s ",
                    wirelog_cmp_op_str(expr->cmp_op));
            ir_expr_to_buf(expr->children[1], buf, bufsize, pos);
        }
        break;
    case WL_IR_EXPR_AGG:
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, "%s(",
                wirelog_agg_fn_str(expr->agg_fn));
        if (expr->child_count >= 1) {
            ir_expr_to_buf(expr->children[0], buf, bufsize, pos);
        }
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, ")");
        break;
    case WL_IR_EXPR_BOOL:
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, "%s",
                expr->bool_value ? "true" : "false");
        break;
    case WL_IR_EXPR_STR_FN:
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, "str_fn_%d(",
                (int)expr->str_fn);
        for (uint32_t ci = 0; ci < expr->child_count; ci++) {
            if (ci > 0)
                *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, ", ");
            ir_expr_to_buf(expr->children[ci], buf, bufsize, pos);
        }
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, ")");
        break;
    }
}

static void
ir_node_to_buf(const wirelog_ir_node_t *node, char *buf, size_t bufsize,
    size_t *pos, uint32_t indent)
{
    if (!node || *pos >= bufsize - 1)
        return;

    /* Indentation */
    for (uint32_t i = 0; i < indent; i++) {
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, "  ");
    }

    /* Node type */
    *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, "%s",
            ir_node_type_str(node->type));

    /* Relation name */
    if (node->relation_name) {
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, " \"%s\"",
                node->relation_name);
    }

    /* SCAN columns */
    if (node->type == WIRELOG_IR_SCAN && node->column_names) {
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, " [");
        for (uint32_t i = 0; i < node->column_count; i++) {
            if (i > 0)
                *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, ", ");
            if (node->column_names[i]) {
                *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, "%s",
                        node->column_names[i]);
            } else {
                *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, "_");
            }
        }
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, "]");
    }

    /* JOIN keys */
    if (node->type == WIRELOG_IR_JOIN && node->join_left_keys) {
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, " (key:");
        for (uint32_t i = 0; i < node->join_key_count; i++) {
            *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, " %s",
                    node->join_left_keys[i]);
        }
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, ")");
    }

    /* FILTER expression */
    if (node->type == WIRELOG_IR_FILTER && node->filter_expr) {
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, " (");
        ir_expr_to_buf(node->filter_expr, buf, bufsize, pos);
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, ")");
    }

    /* AGGREGATE */
    if (node->type == WIRELOG_IR_AGGREGATE && node->agg_expr) {
        *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, " ");
        ir_expr_to_buf(node->agg_expr, buf, bufsize, pos);
    }

    *pos += (size_t)snprintf(buf + *pos, bufsize - *pos, "\n");

    /* Children */
    for (uint32_t i = 0; i < node->child_count; i++) {
        ir_node_to_buf(node->children[i], buf, bufsize, pos, indent + 1);
    }
}

void
wirelog_ir_node_print(const wirelog_ir_node_t *node, uint32_t indent)
{
    if (!node)
        return;

    char buf[4096];
    size_t pos = 0;
    ir_node_to_buf(node, buf, sizeof(buf), &pos, indent);
    fprintf(stderr, "%s", buf);
}

char *
wirelog_ir_node_to_string(const wirelog_ir_node_t *node)
{
    if (!node)
        return NULL;

    char *buf = (char *)malloc(4096);
    if (!buf)
        return NULL;

    size_t pos = 0;
    ir_node_to_buf(node, buf, 4096, &pos, 0);

    return buf;
}
