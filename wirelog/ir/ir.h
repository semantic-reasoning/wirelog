/*
 * ir.h - wirelog IR Internal Structures
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 * Defines the internal structure of wirelog_ir_node_t (opaque in public header)
 * and the IR expression tree used for filters and projections.
 */

#ifndef WIRELOG_IR_INTERNAL_H
#define WIRELOG_IR_INTERNAL_H

#include "../wirelog-ir.h"
#include "../wirelog-types.h"

#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Expression Representation                                                */
/* ======================================================================== */

typedef enum {
    WL_IR_EXPR_VAR,       /* variable reference (by name) */
    WL_IR_EXPR_CONST_INT, /* integer constant */
    WL_IR_EXPR_CONST_STR, /* string constant */
    WL_IR_EXPR_ARITH,     /* binary arithmetic (+,-,*,/,%) */
    WL_IR_EXPR_CMP,       /* comparison (=,!=,<,>,<=,>=) */
    WL_IR_EXPR_AGG,       /* aggregate function */
    WL_IR_EXPR_BOOL,      /* boolean literal */
    WL_IR_EXPR_STR_FN,    /* string function (strlen, cat, substr, ...) */
} wl_ir_expr_type_t;

typedef struct wl_ir_expr wl_ir_expr_t;

struct wl_ir_expr {
    wl_ir_expr_type_t type;

    /* Data (usage depends on type) */
    char *var_name;    /* WL_IR_EXPR_VAR: variable name */
    int64_t int_value; /* WL_IR_EXPR_CONST_INT */
    char *str_value;   /* WL_IR_EXPR_CONST_STR */
    bool bool_value;   /* WL_IR_EXPR_BOOL */

    wirelog_arith_op_t arith_op; /* WL_IR_EXPR_ARITH */
    wirelog_cmp_op_t cmp_op;     /* WL_IR_EXPR_CMP */
    wirelog_agg_fn_t agg_fn;     /* WL_IR_EXPR_AGG */
    wirelog_str_fn_t str_fn;     /* WL_IR_EXPR_STR_FN */

    /* Children (binary ops have 2, aggregate has 1) */
    wl_ir_expr_t **children;
    uint32_t child_count;
    uint32_t child_capacity;
};

/* ======================================================================== */
/* IR Node Internal Definition                                              */
/* ======================================================================== */

struct wirelog_ir_node {
    wirelog_ir_node_type_t type;
    char *relation_name; /* output relation name */

    /* Operator-specific data */
    char **column_names;   /* SCAN: declared column names (NULL for wildcard) */
    uint32_t column_count; /* SCAN: column count */

    uint32_t *project_indices; /* PROJECT: column index mapping */
    uint32_t project_count;    /* PROJECT: number of projected columns */
    wl_ir_expr_t *
    *project_exprs;     /* PROJECT: expressions (for computed columns) */

    wl_ir_expr_t *filter_expr; /* FILTER: predicate expression */

    char **join_left_keys;   /* JOIN: left join key variable names */
    char **join_right_keys;  /* JOIN: right join key variable names */
    uint32_t join_key_count; /* JOIN: number of join keys */

    wirelog_agg_fn_t agg_fn;    /* AGGREGATE: function */
    wl_ir_expr_t *agg_expr;     /* AGGREGATE: expression to aggregate */
    uint32_t *group_by_indices; /* AGGREGATE: grouping column indices */
    uint32_t group_by_count;    /* AGGREGATE: number of grouping columns */

    /* Tree structure */
    wirelog_ir_node_t **children;
    uint32_t child_count;
    uint32_t child_capacity;
};

/* ======================================================================== */
/* Construction API (internal)                                              */
/* ======================================================================== */

wirelog_ir_node_t *
wl_ir_node_create(wirelog_ir_node_type_t type);
void
wl_ir_node_set_relation(wirelog_ir_node_t *node, const char *name);
void
wl_ir_node_add_child(wirelog_ir_node_t *node, wirelog_ir_node_t *child);
void
wl_ir_node_free(wirelog_ir_node_t *node);

/* ======================================================================== */
/* Expression API (internal)                                                */
/* ======================================================================== */

wl_ir_expr_t *
wl_ir_expr_create(wl_ir_expr_type_t type);
void
wl_ir_expr_add_child(wl_ir_expr_t *parent, wl_ir_expr_t *child);
void
wl_ir_expr_free(wl_ir_expr_t *expr);

/**
 * wl_ir_expr_clone:
 * @expr: Expression to clone (NULL-safe).
 *
 * Recursive deep copy of an expression tree.
 * All strings and child arrays are independently allocated.
 *
 * Returns: (transfer full): New expression tree, or NULL on error/NULL input.
 */
wl_ir_expr_t *
wl_ir_expr_clone(const wl_ir_expr_t *expr);

/* ======================================================================== */
/* Utility                                                                  */
/* ======================================================================== */

/**
 * strdup_safe:
 *
 * C11-safe string duplication (no POSIX strdup dependency).
 * Used by both parser and IR code.
 */
char *
strdup_safe(const char *s);

#endif /* WIRELOG_IR_INTERNAL_H */
