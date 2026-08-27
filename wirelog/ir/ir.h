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

/* A zero aggregate_index is a valid head position.  Keep legacy/internal
 * callers that do not specify one distinguishable from that position. */
#define WL_IR_AGGREGATE_INDEX_UNSET UINT32_MAX

/* ======================================================================== */
/* Expression Representation                                                */
/* ======================================================================== */

typedef enum {
    WL_IR_EXPR_VAR,       /* variable reference (by name) */
    WL_IR_EXPR_CONST_INT, /* integer constant */
    WL_IR_EXPR_CONST_FLOAT, /* finite binary64 constant */
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
    double float_value; /* WL_IR_EXPR_CONST_FLOAT */
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
/* Column Value Domain (Issue #962)                                         */
/* ======================================================================== */

/**
 * wl_ir_coltype_t:
 *
 * The value domain of a physical column, as far as expression typing cares.
 *
 * This deliberately is NOT wirelog_column_type_t.  WIRELOG_TYPE_INT32 == 0,
 * so an array of wirelog_column_type_t that was calloc()'d -- or that some
 * path forgot to fill in -- is bit-identical to "every column is INT32".
 * That is precisely the value that makes the comparison emitter keep the
 * integer opcode, so a plumbing gap would degrade silently to the very bug
 * being fixed and no test could tell the difference.
 *
 * Here UNKNOWN is 0 instead: a zero-filled or unpropagated array reads as
 * "type not established", which the emitter detects and reports.
 *
 * Only the string/non-string distinction is represented, because that is
 * the only distinction the comparison emitter can act on: every non-string
 * column is compared by value exactly as it is today.  Collapsing
 * int32/int64/float/bool into one SCALAR value also keeps the "both sides
 * known and in conflict" warning from firing on int32-vs-int64 pairs, which
 * are not a conflict at all.
 */
typedef enum {
    WL_IR_COLTYPE_UNKNOWN = 0, /* not established -- calloc() default */
    WL_IR_COLTYPE_SCALAR = 1,  /* int/uint/bool: compared by value */
    WL_IR_COLTYPE_STRING = 2,  /* interned symbol id: compared by content */
    WL_IR_COLTYPE_FLOAT = 3,   /* binary64 value, kept distinct for typing */
} wl_ir_coltype_t;

/**
 * wl_ir_coltype_from_column_type:
 *
 * Project a declared column type onto the comparison lattice.
 */
wl_ir_coltype_t
wl_ir_coltype_from_column_type(wirelog_column_type_t type);

/* ======================================================================== */
/* IR Node Internal Definition                                              */
/* ======================================================================== */

struct wirelog_ir_node {
    wirelog_ir_node_type_t type;
    char *relation_name; /* output relation name */

    /* Operator-specific data */
    char **column_names;   /* SCAN: declared column names (NULL for wildcard) */
    uint32_t column_count; /* SCAN: column count */
    /* SCAN: value domain of each column, parallel to column_names and the
     * same column_count entries.  NULL means "nothing established", which
     * every consumer must read as all-UNKNOWN rather than as a default
     * type (Issue #962). */
    wl_ir_coltype_t *column_types;

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
    uint32_t aggregate_index;   /* AGGREGATE: output position, or UNSET */

    /* Compound column IR (Issue #531) */
    struct {
        uint32_t functor_id;              /* COMPOUND_INLINE: which functor (intern'd) */
        uint32_t arity;                   /* COMPOUND_INLINE: arity of compound */
        wl_ir_expr_t **args;              /* COMPOUND_INLINE: lowered argument expressions */
        uint32_t arg_count;               /* COMPOUND_INLINE: == arity */
        uint32_t inline_col_offset;       /* COMPOUND_INLINE: physical column offset */
    } compound_inline;

    struct {
        wl_ir_expr_t *handle_expr;        /* COMPOUND_SIDE: IR for handle resolution */
    } compound_side;

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
/* Returns 0 after ownership transfers to @child's parent, or -1 when either
 * argument is NULL or the child-array growth allocation fails.  On failure,
 * the caller retains ownership of @child. */
int
wl_ir_node_add_child(wirelog_ir_node_t *node, wirelog_ir_node_t *child);
void
wl_ir_node_free(wirelog_ir_node_t *node);

/* ======================================================================== */
/* Expression API (internal)                                                */
/* ======================================================================== */

wl_ir_expr_t *
wl_ir_expr_create(wl_ir_expr_type_t type);
/* Returns 0 after ownership transfers to @child's parent, or -1 when either
 * argument is NULL or the child-array growth allocation fails.  On failure,
 * the caller retains ownership of @child. */
int
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
