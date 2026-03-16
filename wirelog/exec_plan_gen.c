/*
 * exec_plan_gen.c - wirelog Plan Generator from Parsed Program
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Converts a parsed+stratified wirelog_program_t into a wl_plan_t
 * execution plan.  Replaces the deleted DD plan generation path.
 */

#include "exec_plan_gen.h"

#include "columnar/columnar_nanoarrow.h"
#include "ir/ir.h"
#include "ir/program.h"
#include "wirelog-ir.h"
#include "wirelog-types.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* K-Fusion Feature Flag                                                    */
/* ======================================================================== */

/*
 * ENABLE_K_FUSION:
 *
 * When set to 1, rewrite_multiway_delta() emits a single WL_PLAN_OP_K_FUSION
 * operator (containing K independent operator sequences) instead of K
 * sequential copies joined by CONCAT+CONSOLIDATE.  The K_FUSION path enables
 * parallel workqueue execution in the columnar backend.
 *
 * Set to 0 (default) for the proven sequential expansion path.
 * Set to 1 for parallel K-fusion execution (Phase 2C performance testing).
 */
#ifndef ENABLE_K_FUSION
#define ENABLE_K_FUSION 1
#endif

/* ======================================================================== */
/* Internal helpers                                                         */
/* ======================================================================== */

static char *
dup_str(const char *s)
{
    if (!s)
        return NULL;
    size_t len = strlen(s);
    char *d = (char *)malloc(len + 1);
    if (!d)
        return NULL;
    memcpy(d, s, len + 1);
    return d;
}

/* ======================================================================== */
/* Expression serialization (IR expr tree -> postfix byte buffer)           */
/* ======================================================================== */

/* Dynamic byte buffer for building expression bytes */
typedef struct {
    uint8_t *data;
    uint32_t size;
    uint32_t capacity;
} expr_buf_t;

static int
expr_buf_init(expr_buf_t *buf)
{
    buf->capacity = 64;
    buf->size = 0;
    buf->data = (uint8_t *)malloc(buf->capacity);
    return buf->data ? 0 : -1;
}

static int
expr_buf_ensure(expr_buf_t *buf, uint32_t extra)
{
    uint32_t need = buf->size + extra;
    if (need <= buf->capacity)
        return 0;
    uint32_t cap = buf->capacity;
    while (cap < need)
        cap *= 2;
    uint8_t *p = (uint8_t *)realloc(buf->data, cap);
    if (!p)
        return -1;
    buf->data = p;
    buf->capacity = cap;
    return 0;
}

static int
expr_buf_push_u8(expr_buf_t *buf, uint8_t v)
{
    if (expr_buf_ensure(buf, 1) != 0)
        return -1;
    buf->data[buf->size++] = v;
    return 0;
}

static int
expr_buf_push_u16(expr_buf_t *buf, uint16_t v)
{
    if (expr_buf_ensure(buf, 2) != 0)
        return -1;
    /* little-endian */
    buf->data[buf->size++] = (uint8_t)(v & 0xFF);
    buf->data[buf->size++] = (uint8_t)((v >> 8) & 0xFF);
    return 0;
}

static int
expr_buf_push_i64(expr_buf_t *buf, int64_t v)
{
    if (expr_buf_ensure(buf, 8) != 0)
        return -1;
    /* little-endian */
    uint64_t u = (uint64_t)v;
    for (int i = 0; i < 8; i++) {
        buf->data[buf->size++] = (uint8_t)(u & 0xFF);
        u >>= 8;
    }
    return 0;
}

static int
expr_buf_push_bytes(expr_buf_t *buf, const uint8_t *data, uint32_t len)
{
    if (expr_buf_ensure(buf, len) != 0)
        return -1;
    memcpy(buf->data + buf->size, data, len);
    buf->size += len;
    return 0;
}

/* Map IR arith op -> plan expr tag */
static uint8_t
arith_to_tag(wirelog_arith_op_t op)
{
    switch (op) {
    case WIRELOG_ARITH_ADD:
        return WL_PLAN_EXPR_ARITH_ADD;
    case WIRELOG_ARITH_SUB:
        return WL_PLAN_EXPR_ARITH_SUB;
    case WIRELOG_ARITH_MUL:
        return WL_PLAN_EXPR_ARITH_MUL;
    case WIRELOG_ARITH_DIV:
        return WL_PLAN_EXPR_ARITH_DIV;
    case WIRELOG_ARITH_MOD:
        return WL_PLAN_EXPR_ARITH_MOD;
    case WIRELOG_ARITH_BAND:
        return WL_PLAN_EXPR_ARITH_BAND;
    case WIRELOG_ARITH_BOR:
        return WL_PLAN_EXPR_ARITH_BOR;
    case WIRELOG_ARITH_BXOR:
        return WL_PLAN_EXPR_ARITH_BXOR;
    case WIRELOG_ARITH_BNOT:
        return WL_PLAN_EXPR_ARITH_BNOT;
    case WIRELOG_ARITH_SHL:
        return WL_PLAN_EXPR_ARITH_SHL;
    case WIRELOG_ARITH_SHR:
        return WL_PLAN_EXPR_ARITH_SHR;
    case WIRELOG_ARITH_HASH:
        return WL_PLAN_EXPR_ARITH_HASH;
    case WIRELOG_ARITH_CRC32_ETH:
        return WL_PLAN_EXPR_ARITH_CRC32_ETH;
    case WIRELOG_ARITH_CRC32_CAST:
        return WL_PLAN_EXPR_ARITH_CRC32_CAST;
    case WIRELOG_ARITH_MD5:
        return WL_PLAN_EXPR_ARITH_MD5;
    case WIRELOG_ARITH_SHA1:
        return WL_PLAN_EXPR_ARITH_SHA1;
    case WIRELOG_ARITH_SHA256:
        return WL_PLAN_EXPR_ARITH_SHA256;
    case WIRELOG_ARITH_SHA512:
        return WL_PLAN_EXPR_ARITH_SHA512;
    case WIRELOG_ARITH_HMAC_SHA256:
        return WL_PLAN_EXPR_ARITH_HMAC_SHA256;
    }
    return WL_PLAN_EXPR_ARITH_ADD; /* fallback */
}

/* Map IR cmp op -> plan expr tag */
static uint8_t
cmp_to_tag(wirelog_cmp_op_t op)
{
    switch (op) {
    case WIRELOG_CMP_EQ:
        return WL_PLAN_EXPR_CMP_EQ;
    case WIRELOG_CMP_NEQ:
        return WL_PLAN_EXPR_CMP_NEQ;
    case WIRELOG_CMP_LT:
        return WL_PLAN_EXPR_CMP_LT;
    case WIRELOG_CMP_GT:
        return WL_PLAN_EXPR_CMP_GT;
    case WIRELOG_CMP_LTE:
        return WL_PLAN_EXPR_CMP_LTE;
    case WIRELOG_CMP_GTE:
        return WL_PLAN_EXPR_CMP_GTE;
    }
    return WL_PLAN_EXPR_CMP_EQ; /* fallback */
}

/* Map IR agg fn -> plan expr tag */
static uint8_t
agg_to_tag(wirelog_agg_fn_t fn)
{
    switch (fn) {
    case WIRELOG_AGG_COUNT:
        return WL_PLAN_EXPR_AGG_COUNT;
    case WIRELOG_AGG_SUM:
        return WL_PLAN_EXPR_AGG_SUM;
    case WIRELOG_AGG_MIN:
        return WL_PLAN_EXPR_AGG_MIN;
    case WIRELOG_AGG_MAX:
        return WL_PLAN_EXPR_AGG_MAX;
    case WIRELOG_AGG_AVG:
        return WL_PLAN_EXPR_AGG_SUM; /* approximate: no AVG tag */
    }
    return WL_PLAN_EXPR_AGG_COUNT; /* fallback */
}

/**
 * Serialize an IR expression tree into postfix (RPN) byte encoding.
 * Walks the tree recursively: children first (postfix), then operator.
 */
static int
serialize_expr(expr_buf_t *buf, const wl_ir_expr_t *expr, char **col_names,
               uint32_t col_count)
{
    if (!expr)
        return -1;

    switch (expr->type) {
    case WL_IR_EXPR_VAR: {
        if (!expr->var_name)
            return -1;
        /* Resolve variable name to "colN" using column name context */
        char resolved[32];
        const char *emit_name = expr->var_name;
        if (col_names && col_count > 0) {
            for (uint32_t c = 0; c < col_count; c++) {
                if (col_names[c] && strcmp(col_names[c], expr->var_name) == 0) {
                    snprintf(resolved, sizeof(resolved), "col%u", c);
                    emit_name = resolved;
                    break;
                }
            }
        }
        uint16_t len = (uint16_t)strlen(emit_name);
        if (expr_buf_push_u8(buf, WL_PLAN_EXPR_VAR) != 0)
            return -1;
        if (expr_buf_push_u16(buf, len) != 0)
            return -1;
        if (expr_buf_push_bytes(buf, (const uint8_t *)emit_name, len) != 0)
            return -1;
        return 0;
    }
    case WL_IR_EXPR_CONST_INT:
        if (expr_buf_push_u8(buf, WL_PLAN_EXPR_CONST_INT) != 0)
            return -1;
        return expr_buf_push_i64(buf, expr->int_value);

    case WL_IR_EXPR_CONST_STR: {
        if (!expr->str_value)
            return -1;
        uint16_t len = (uint16_t)strlen(expr->str_value);
        if (expr_buf_push_u8(buf, WL_PLAN_EXPR_CONST_STR) != 0)
            return -1;
        if (expr_buf_push_u16(buf, len) != 0)
            return -1;
        if (expr_buf_push_bytes(buf, (const uint8_t *)expr->str_value, len)
            != 0)
            return -1;
        return 0;
    }
    case WL_IR_EXPR_BOOL:
        if (expr_buf_push_u8(buf, WL_PLAN_EXPR_BOOL) != 0)
            return -1;
        return expr_buf_push_u8(buf, expr->bool_value ? 1 : 0);

    case WL_IR_EXPR_ARITH:
        /* Serialize children first (postfix) */
        for (uint32_t i = 0; i < expr->child_count; i++) {
            if (serialize_expr(buf, expr->children[i], col_names, col_count)
                != 0)
                return -1;
        }
        return expr_buf_push_u8(buf, arith_to_tag(expr->arith_op));

    case WL_IR_EXPR_CMP:
        for (uint32_t i = 0; i < expr->child_count; i++) {
            if (serialize_expr(buf, expr->children[i], col_names, col_count)
                != 0)
                return -1;
        }
        return expr_buf_push_u8(buf, cmp_to_tag(expr->cmp_op));

    case WL_IR_EXPR_AGG:
        for (uint32_t i = 0; i < expr->child_count; i++) {
            if (serialize_expr(buf, expr->children[i], col_names, col_count)
                != 0)
                return -1;
        }
        return expr_buf_push_u8(buf, agg_to_tag(expr->agg_fn));
    }

    return -1; /* unknown type */
}

/**
 * Serialize an IR expression into a wl_plan_expr_buffer_t.
 * col_names/col_count provide variable name -> column index resolution.
 * Returns 0 on success. On NULL expr, produces empty buffer (valid no-op).
 */
static int
serialize_expr_to_buffer_ctx(const wl_ir_expr_t *expr, char **col_names,
                             uint32_t col_count, wl_plan_expr_buffer_t *out_buf)
{
    out_buf->data = NULL;
    out_buf->size = 0;

    if (!expr)
        return 0; /* empty = no expression */

    expr_buf_t buf;
    if (expr_buf_init(&buf) != 0)
        return -1;

    if (serialize_expr(&buf, expr, col_names, col_count) != 0) {
        free(buf.data);
        return -1;
    }

    out_buf->data = buf.data;
    out_buf->size = buf.size;
    return 0;
}

/* ======================================================================== */
/* Operator list builder                                                    */
/* ======================================================================== */

typedef struct {
    wl_plan_op_t *ops;
    uint32_t count;
    uint32_t capacity;
} op_list_t;

static int
op_list_init(op_list_t *list)
{
    list->capacity = 8;
    list->count = 0;
    list->ops = (wl_plan_op_t *)calloc(list->capacity, sizeof(wl_plan_op_t));
    return list->ops ? 0 : -1;
}

static wl_plan_op_t *
op_list_push(op_list_t *list)
{
    if (list->count >= list->capacity) {
        uint32_t cap = list->capacity * 2;
        wl_plan_op_t *p
            = (wl_plan_op_t *)realloc(list->ops, cap * sizeof(wl_plan_op_t));
        if (!p)
            return NULL;
        list->ops = p;
        list->capacity = cap;
    }
    wl_plan_op_t *op = &list->ops[list->count];
    memset(op, 0, sizeof(*op));
    list->count++;
    return op;
}

/* Duplicate a uint32_t array */
static uint32_t *
dup_indices(const uint32_t *src, uint32_t count)
{
    if (!src || count == 0)
        return NULL;
    uint32_t *dst = (uint32_t *)malloc(count * sizeof(uint32_t));
    if (!dst)
        return NULL;
    memcpy(dst, src, count * sizeof(uint32_t));
    return dst;
}

/* ======================================================================== */
/* Column layout resolution                                                 */
/* ======================================================================== */

/**
 * Collect the output column names produced by an IR node.
 * For SCAN: returns the declared column_names.
 * For JOIN: concatenates left + right child column names.
 * For PROJECT/FILTER/FLATMAP/UNION: delegates to child[0].
 *
 * out_names and out_count are set on success (caller must free out_names
 * array AND each string in it).  Returns 0 on success, -1 on failure.
 */
static int
collect_output_columns(const wirelog_ir_node_t *node, char ***out_names,
                       uint32_t *out_count)
{
    if (!node) {
        *out_names = NULL;
        *out_count = 0;
        return -1;
    }

    switch (node->type) {
    case WIRELOG_IR_SCAN: {
        uint32_t nc = node->column_count;
        char **names = (char **)calloc(nc ? nc : 1, sizeof(char *));
        if (!names)
            return -1;
        for (uint32_t i = 0; i < nc; i++) {
            names[i]
                = dup_str(node->column_names ? node->column_names[i] : NULL);
        }
        *out_names = names;
        *out_count = nc;
        return 0;
    }

    case WIRELOG_IR_JOIN:
    case WIRELOG_IR_ANTIJOIN:
    case WIRELOG_IR_SEMIJOIN: {
        /* Concatenate left child columns + right child columns */
        char **left_names = NULL, **right_names = NULL;
        uint32_t left_count = 0, right_count = 0;
        if (node->child_count > 0)
            collect_output_columns(node->children[0], &left_names, &left_count);
        if (node->child_count > 1)
            collect_output_columns(node->children[1], &right_names,
                                   &right_count);

        uint32_t total = left_count + right_count;
        char **names = (char **)calloc(total ? total : 1, sizeof(char *));
        if (!names) {
            for (uint32_t i = 0; i < left_count; i++)
                free(left_names[i]);
            free((void *)left_names);
            for (uint32_t i = 0; i < right_count; i++)
                free(right_names[i]);
            free((void *)right_names);
            return -1;
        }
        for (uint32_t i = 0; i < left_count; i++)
            names[i] = left_names[i]; /* transfer ownership */
        for (uint32_t i = 0; i < right_count; i++)
            names[left_count + i] = right_names[i]; /* transfer ownership */
        free((void *)left_names);
        free((void *)right_names);
        *out_names = names;
        *out_count = total;
        return 0;
    }

    case WIRELOG_IR_PROJECT:
    case WIRELOG_IR_FILTER:
    case WIRELOG_IR_FLATMAP:
    case WIRELOG_IR_AGGREGATE:
    case WIRELOG_IR_UNION:
        /* Delegate to first child */
        if (node->child_count > 0)
            return collect_output_columns(node->children[0], out_names,
                                          out_count);
        *out_names = NULL;
        *out_count = 0;
        return -1;
    }

    *out_names = NULL;
    *out_count = 0;
    return -1;
}

/**
 * Resolve PROJECT expression variable names to column indices.
 * Uses the child node's output column layout to map var names to positions.
 * Returns allocated uint32_t array of indices, or NULL on failure.
 */
static uint32_t *
resolve_project_indices(const wirelog_ir_node_t *project_node)
{
    if (!project_node || project_node->project_count == 0
        || !project_node->project_exprs)
        return NULL;

    /* Get child's column layout */
    const wirelog_ir_node_t *child = NULL;
    if (project_node->child_count > 0)
        child = project_node->children[0];

    char **col_names = NULL;
    uint32_t col_count = 0;
    if (collect_output_columns(child, &col_names, &col_count) != 0)
        return NULL;

    uint32_t pc = project_node->project_count;
    uint32_t *indices = (uint32_t *)malloc(pc * sizeof(uint32_t));
    if (!indices) {
        for (uint32_t i = 0; i < col_count; i++)
            free(col_names[i]);
        free((void *)col_names);
        return NULL;
    }

    for (uint32_t i = 0; i < pc; i++) {
        indices[i] = i; /* fallback: identity */
        const wl_ir_expr_t *expr = project_node->project_exprs[i];
        if (expr && expr->type == WL_IR_EXPR_VAR && expr->var_name) {
            for (uint32_t c = 0; c < col_count; c++) {
                if (col_names[c] && strcmp(col_names[c], expr->var_name) == 0) {
                    indices[i] = c;
                    break;
                }
            }
        }
    }

    for (uint32_t i = 0; i < col_count; i++)
        free(col_names[i]);
    free((void *)col_names);
    return indices;
}

/**
 * Resolve a join key variable name to "colN" format using the child's
 * column layout.  Returns a newly allocated string like "col1".
 * Falls back to "col0" if the name is not found.
 */
static char *
resolve_key_to_colN(const char *key_name, const wirelog_ir_node_t *child)
{
    char buf[32];
    uint32_t idx = 0; /* fallback */

    if (key_name && child) {
        char **col_names = NULL;
        uint32_t col_count = 0;
        if (collect_output_columns(child, &col_names, &col_count) == 0) {
            for (uint32_t c = 0; c < col_count; c++) {
                if (col_names[c] && strcmp(col_names[c], key_name) == 0) {
                    idx = c;
                    break;
                }
            }
            for (uint32_t c = 0; c < col_count; c++)
                free(col_names[c]);
            free((void *)col_names);
        }
    }

    snprintf(buf, sizeof(buf), "col%u", idx);
    return dup_str(buf);
}

/**
 * Resolve an array of join key variable names to "colN" format.
 * Returns a newly allocated array of strings.
 */
static char **
resolve_keys_to_colN(char **keys, uint32_t count,
                     const wirelog_ir_node_t *child)
{
    if (!keys || count == 0)
        return NULL;
    char **out = (char **)malloc(count * sizeof(char *));
    if (!out)
        return NULL;
    for (uint32_t i = 0; i < count; i++) {
        out[i] = resolve_key_to_colN(keys[i], child);
        if (!out[i]) {
            for (uint32_t j = 0; j < i; j++)
                free(out[j]);
            free((void *)out);
            return NULL;
        }
    }
    return out;
}

/**
 * Recursively translate an IR node tree into plan operators.
 * Operators are emitted in post-order (children first) to form a
 * stack-machine sequence.
 */
static int
translate_ir_node(const wirelog_ir_node_t *node, op_list_t *ops)
{
    if (!node)
        return -1;

    switch (node->type) {
    case WIRELOG_IR_SCAN: {
        wl_plan_op_t *op = op_list_push(ops);
        if (!op)
            return -1;
        op->op = WL_PLAN_OP_VARIABLE;
        op->relation_name = dup_str(node->relation_name);
        if (node->relation_name && !op->relation_name)
            return -1;
        return 0;
    }

    case WIRELOG_IR_PROJECT: {
        /* Translate child first */
        if (node->child_count > 0) {
            if (translate_ir_node(node->children[0], ops) != 0)
                return -1;
        }
        wl_plan_op_t *op = op_list_push(ops);
        if (!op)
            return -1;
        op->op = WL_PLAN_OP_MAP;
        op->project_count = node->project_count;
        if (node->project_indices) {
            op->project_indices
                = dup_indices(node->project_indices, node->project_count);
            if (node->project_count > 0 && !op->project_indices)
                return -1;
        } else if (node->project_count > 0) {
            /* Resolve variable names to column indices from child layout */
            uint32_t *resolved = resolve_project_indices(node);
            if (resolved) {
                op->project_indices = resolved;
            } else {
                /* Fallback: synthesize identity [0..n-1] */
                uint32_t *ids = (uint32_t *)malloc(node->project_count
                                                   * sizeof(uint32_t));
                if (!ids)
                    return -1;
                for (uint32_t pi = 0; pi < node->project_count; pi++)
                    ids[pi] = pi;
                op->project_indices = ids;
            }
        }
        /* Serialize project expressions if present */
        if (node->project_exprs && node->project_count > 0) {
            op->map_exprs = (wl_plan_expr_buffer_t *)calloc(
                node->project_count, sizeof(wl_plan_expr_buffer_t));
            if (!op->map_exprs)
                return -1;
            op->map_expr_count = node->project_count;
            /* Collect child column names for variable name resolution */
            char **child_col_names = NULL;
            uint32_t child_col_count = 0;
            const wirelog_ir_node_t *child0
                = (node->child_count > 0) ? node->children[0] : NULL;
            collect_output_columns(child0, &child_col_names, &child_col_count);
            for (uint32_t i = 0; i < node->project_count; i++) {
                if (node->project_exprs[i]) {
                    if (serialize_expr_to_buffer_ctx(
                            node->project_exprs[i], child_col_names,
                            child_col_count, &op->map_exprs[i])
                        != 0) {
                        for (uint32_t c = 0; c < child_col_count; c++)
                            free(child_col_names[c]);
                        free((void *)child_col_names);
                        return -1;
                    }
                }
            }
            for (uint32_t c = 0; c < child_col_count; c++)
                free(child_col_names[c]);
            free((void *)child_col_names);
        }
        return 0;
    }

    case WIRELOG_IR_FILTER: {
        /* Translate child first */
        if (node->child_count > 0) {
            if (translate_ir_node(node->children[0], ops) != 0)
                return -1;
        }
        wl_plan_op_t *op = op_list_push(ops);
        if (!op)
            return -1;
        op->op = WL_PLAN_OP_FILTER;
        {
            char **filt_col_names = NULL;
            uint32_t filt_col_count = 0;
            const wirelog_ir_node_t *fchild
                = (node->child_count > 0) ? node->children[0] : NULL;
            collect_output_columns(fchild, &filt_col_names, &filt_col_count);
            int rc = serialize_expr_to_buffer_ctx(
                node->filter_expr, filt_col_names, filt_col_count,
                &op->filter_expr);
            for (uint32_t c = 0; c < filt_col_count; c++)
                free(filt_col_names[c]);
            free((void *)filt_col_names);
            if (rc != 0)
                return -1;
        }
        return 0;
    }

    case WIRELOG_IR_JOIN: {
        /* Left child first */
        if (node->child_count > 0) {
            if (translate_ir_node(node->children[0], ops) != 0)
                return -1;
        }
        wl_plan_op_t *op = op_list_push(ops);
        if (!op)
            return -1;
        op->op = WL_PLAN_OP_JOIN;
        /* Right relation is the second child's relation name */
        if (node->child_count > 1 && node->children[1]) {
            op->right_relation = dup_str(node->children[1]->relation_name);
        }
        /* Resolve join key variable names to "colN" positional format */
        op->left_keys = (const char *const *)resolve_keys_to_colN(
            node->join_left_keys, node->join_key_count,
            node->child_count > 0 ? node->children[0] : NULL);
        op->right_keys = (const char *const *)resolve_keys_to_colN(
            node->join_right_keys, node->join_key_count,
            node->child_count > 1 ? node->children[1] : NULL);
        op->key_count = node->join_key_count;
        return 0;
    }

    case WIRELOG_IR_ANTIJOIN: {
        /* Left child first */
        if (node->child_count > 0) {
            if (translate_ir_node(node->children[0], ops) != 0)
                return -1;
        }
        wl_plan_op_t *op = op_list_push(ops);
        if (!op)
            return -1;
        op->op = WL_PLAN_OP_ANTIJOIN;
        if (node->child_count > 1 && node->children[1]) {
            op->right_relation = dup_str(node->children[1]->relation_name);
        }
        op->left_keys = (const char *const *)resolve_keys_to_colN(
            node->join_left_keys, node->join_key_count,
            node->child_count > 0 ? node->children[0] : NULL);
        op->right_keys = (const char *const *)resolve_keys_to_colN(
            node->join_right_keys, node->join_key_count,
            node->child_count > 1 ? node->children[1] : NULL);
        op->key_count = node->join_key_count;
        return 0;
    }

    case WIRELOG_IR_SEMIJOIN: {
        /* Left child first */
        if (node->child_count > 0) {
            if (translate_ir_node(node->children[0], ops) != 0)
                return -1;
        }
        wl_plan_op_t *op = op_list_push(ops);
        if (!op)
            return -1;
        op->op = WL_PLAN_OP_SEMIJOIN;
        if (node->child_count > 1 && node->children[1]) {
            op->right_relation = dup_str(node->children[1]->relation_name);
        }
        op->left_keys = (const char *const *)resolve_keys_to_colN(
            node->join_left_keys, node->join_key_count,
            node->child_count > 0 ? node->children[0] : NULL);
        op->right_keys = (const char *const *)resolve_keys_to_colN(
            node->join_right_keys, node->join_key_count,
            node->child_count > 1 ? node->children[1] : NULL);
        op->key_count = node->join_key_count;
        op->project_indices
            = dup_indices(node->project_indices, node->project_count);
        op->project_count = node->project_count;
        return 0;
    }

    case WIRELOG_IR_AGGREGATE: {
        /* Translate child first */
        if (node->child_count > 0) {
            if (translate_ir_node(node->children[0], ops) != 0)
                return -1;
        }
        wl_plan_op_t *op = op_list_push(ops);
        if (!op)
            return -1;
        op->op = WL_PLAN_OP_REDUCE;
        op->agg_fn = node->agg_fn;
        op->group_by_indices
            = dup_indices(node->group_by_indices, node->group_by_count);
        op->group_by_count = node->group_by_count;
        return 0;
    }

    case WIRELOG_IR_UNION: {
        /* Each child is translated, then CONCAT + CONSOLIDATE */
        for (uint32_t i = 0; i < node->child_count; i++) {
            int crc = translate_ir_node(node->children[i], ops);
            if (crc != 0)
                return -1;
            /* After the second+ child, emit CONCAT to merge with previous */
            if (i > 0) {
                wl_plan_op_t *concat = op_list_push(ops);
                if (!concat)
                    return -1;
                concat->op = WL_PLAN_OP_CONCAT;
            }
        }
        /* Emit CONSOLIDATE to deduplicate */
        if (node->child_count > 0) {
            wl_plan_op_t *consol = op_list_push(ops);
            if (!consol)
                return -1;
            consol->op = WL_PLAN_OP_CONSOLIDATE;
        }
        return 0;
    }

    case WIRELOG_IR_FLATMAP: {
        /* Decompose into FILTER + MAP per ARCHITECTURE.md:267 */
        /* Translate child (source) first */
        if (node->child_count > 0) {
            if (translate_ir_node(node->children[0], ops) != 0)
                return -1;
        }

        /* FILTER op (from filter_expr) */
        if (node->filter_expr) {
            wl_plan_op_t *filt = op_list_push(ops);
            if (!filt)
                return -1;
            filt->op = WL_PLAN_OP_FILTER;
            char **filt2_names = NULL;
            uint32_t filt2_count = 0;
            const wirelog_ir_node_t *fchild2
                = (node->child_count > 0) ? node->children[0] : NULL;
            collect_output_columns(fchild2, &filt2_names, &filt2_count);
            int frc
                = serialize_expr_to_buffer_ctx(node->filter_expr, filt2_names,
                                               filt2_count, &filt->filter_expr);
            for (uint32_t c = 0; c < filt2_count; c++)
                free(filt2_names[c]);
            free((void *)filt2_names);
            if (frc != 0)
                return -1;
        }

        /* MAP op (from project_indices/project_count) */
        if (node->project_count > 0) {
            wl_plan_op_t *map = op_list_push(ops);
            if (!map)
                return -1;
            map->op = WL_PLAN_OP_MAP;
            map->project_count = node->project_count;
            if (node->project_indices) {
                map->project_indices
                    = dup_indices(node->project_indices, node->project_count);
                if (!map->project_indices)
                    return -1;
            } else {
                /* Synthesize identity indices */
                uint32_t *ids = (uint32_t *)malloc(node->project_count
                                                   * sizeof(uint32_t));
                if (!ids)
                    return -1;
                for (uint32_t pi = 0; pi < node->project_count; pi++)
                    ids[pi] = pi;
                map->project_indices = ids;
            }
            /* Serialize project expressions if present */
            if (node->project_exprs) {
                map->map_exprs = (wl_plan_expr_buffer_t *)calloc(
                    node->project_count, sizeof(wl_plan_expr_buffer_t));
                if (!map->map_exprs)
                    return -1;
                map->map_expr_count = node->project_count;
                /* Collect child column names for variable name resolution */
                char **child_col_names2 = NULL;
                uint32_t child_col_count2 = 0;
                const wirelog_ir_node_t *child2
                    = (node->child_count > 0) ? node->children[0] : NULL;
                collect_output_columns(child2, &child_col_names2,
                                       &child_col_count2);
                for (uint32_t i = 0; i < node->project_count; i++) {
                    if (node->project_exprs[i]) {
                        if (serialize_expr_to_buffer_ctx(
                                node->project_exprs[i], child_col_names2,
                                child_col_count2, &map->map_exprs[i])
                            != 0) {
                            for (uint32_t c = 0; c < child_col_count2; c++)
                                free(child_col_names2[c]);
                            free((void *)child_col_names2);
                            return -1;
                        }
                    }
                }
                for (uint32_t c = 0; c < child_col_count2; c++)
                    free(child_col_names2[c]);
                free((void *)child_col_names2);
            }
        }
        return 0;
    }
    }

    return -1; /* unknown node type */
}

/* ======================================================================== */
/* Free helpers                                                             */
/* ======================================================================== */

#if ENABLE_K_FUSION
static void
free_k_fusion_opaque(wl_plan_op_t *op); /* forward declaration */
#endif

static void
free_op(wl_plan_op_t *op)
{
    free((void *)op->relation_name);
    free((void *)op->right_relation);

    if (op->left_keys) {
        for (uint32_t i = 0; i < op->key_count; i++)
            free((void *)op->left_keys[i]);
        free((void *)op->left_keys);
    }
    if (op->right_keys) {
        for (uint32_t i = 0; i < op->key_count; i++)
            free((void *)op->right_keys[i]);
        free((void *)op->right_keys);
    }

    free((void *)op->project_indices);
    free(op->filter_expr.data);
    free((void *)op->group_by_indices);

    if (op->map_exprs) {
        for (uint32_t i = 0; i < op->map_expr_count; i++)
            free(op->map_exprs[i].data);
        free(op->map_exprs);
    }

#if ENABLE_K_FUSION
    if (op->opaque_data)
        free_k_fusion_opaque(op);
#endif
}

/* ======================================================================== */
/* Multi-Way Delta Expansion (Semi-Naive K-Atom Rewriting with CSE Hints)   */
/* ======================================================================== */

/**
 * Count "delta positions" in a relation plan: the initial VARIABLE op
 * (position 0) plus each subsequent JOIN op whose right_relation is an
 * IDB relation within this stratum.  Returns the count K, and fills
 * delta_pos[] with the op indices of the delta-eligible operators.
 * delta_pos must have room for at least op_count entries.
 */
static uint32_t
count_delta_positions(const wl_plan_op_t *ops, uint32_t op_count,
                      const char *const *idb_names, uint32_t idb_count,
                      uint32_t *delta_pos)
{
    uint32_t k = 0;
    for (uint32_t i = 0; i < op_count; i++) {
        if (ops[i].op == WL_PLAN_OP_VARIABLE) {
            bool is_idb = false;
            for (uint32_t r = 0; r < idb_count; r++) {
                if (ops[i].relation_name && idb_names[r]
                    && strcmp(ops[i].relation_name, idb_names[r]) == 0) {
                    is_idb = true;
                    break;
                }
            }
            if (is_idb)
                delta_pos[k++] = i;
        } else if (ops[i].op == WL_PLAN_OP_JOIN) {
            bool is_idb = false;
            for (uint32_t r = 0; r < idb_count; r++) {
                if (ops[i].right_relation && idb_names[r]
                    && strcmp(ops[i].right_relation, idb_names[r]) == 0) {
                    is_idb = true;
                    break;
                }
            }
            if (is_idb)
                delta_pos[k++] = i;
        }
    }
    return k;
}

/**
 * Deep-copy a single plan op (duplicates all owned strings/buffers).
 */
static int
clone_plan_op(const wl_plan_op_t *src, wl_plan_op_t *dst)
{
    memset(dst, 0, sizeof(*dst));
    dst->op = src->op;
    dst->delta_mode = src->delta_mode;
    dst->materialized = src->materialized;
    dst->agg_fn = src->agg_fn;
    dst->key_count = src->key_count;
    dst->project_count = src->project_count;
    dst->group_by_count = src->group_by_count;
    dst->map_expr_count = src->map_expr_count;

    if (src->relation_name) {
        dst->relation_name = dup_str(src->relation_name);
        if (!dst->relation_name)
            return -1;
    }
    if (src->right_relation) {
        dst->right_relation = dup_str(src->right_relation);
        if (!dst->right_relation)
            return -1;
    }
    if (src->left_keys && src->key_count > 0) {
        char **lk = (char **)malloc(src->key_count * sizeof(char *));
        if (!lk)
            return -1;
        for (uint32_t i = 0; i < src->key_count; i++) {
            lk[i] = dup_str(src->left_keys[i]);
            if (!lk[i]) {
                for (uint32_t j = 0; j < i; j++)
                    free(lk[j]);
                free(lk);
                return -1;
            }
        }
        dst->left_keys = (const char *const *)lk;
    }
    if (src->right_keys && src->key_count > 0) {
        char **rk = (char **)malloc(src->key_count * sizeof(char *));
        if (!rk)
            return -1;
        for (uint32_t i = 0; i < src->key_count; i++) {
            rk[i] = dup_str(src->right_keys[i]);
            if (!rk[i]) {
                for (uint32_t j = 0; j < i; j++)
                    free(rk[j]);
                free(rk);
                return -1;
            }
        }
        dst->right_keys = (const char *const *)rk;
    }
    dst->project_indices
        = dup_indices(src->project_indices, src->project_count);
    dst->group_by_indices
        = dup_indices(src->group_by_indices, src->group_by_count);

    if (src->filter_expr.data && src->filter_expr.size > 0) {
        dst->filter_expr.data = (uint8_t *)malloc(src->filter_expr.size);
        if (!dst->filter_expr.data)
            return -1;
        memcpy(dst->filter_expr.data, src->filter_expr.data,
               src->filter_expr.size);
        dst->filter_expr.size = src->filter_expr.size;
    }

    if (src->map_exprs && src->map_expr_count > 0) {
        dst->map_exprs = (wl_plan_expr_buffer_t *)calloc(
            src->map_expr_count, sizeof(wl_plan_expr_buffer_t));
        if (!dst->map_exprs)
            return -1;
        for (uint32_t i = 0; i < src->map_expr_count; i++) {
            if (src->map_exprs[i].data && src->map_exprs[i].size > 0) {
                dst->map_exprs[i].data
                    = (uint8_t *)malloc(src->map_exprs[i].size);
                if (!dst->map_exprs[i].data)
                    return -1;
                memcpy(dst->map_exprs[i].data, src->map_exprs[i].data,
                       src->map_exprs[i].size);
                dst->map_exprs[i].size = src->map_exprs[i].size;
            }
        }
    }
    return 0;
}

/**
 * Rewrite a single relation plan for multi-way delta expansion with
 * CSE materialization hints.
 *
 * Given original ops [0..op_count-1] with K delta positions, produce
 * K copies of the entire op sequence.  In copy d, delta_pos[d] gets
 * FORCE_DELTA and all other delta positions get FORCE_FULL.
 *
 * Materialization hints:  For the first K-2 delta positions (the
 * "shared prefix"), mark the corresponding JOIN ops as materialized
 * so the evaluator can cache and reuse intermediate join results
 * across copies.  This reduces effective work from K full passes to
 * ~2 passes for large K (e.g., 8-way DOOP CallGraphEdge).
 *
 * Returns new ops array (caller owns) and sets *out_count.
 * Returns NULL on failure.
 */
static wl_plan_op_t *
expand_multiway_delta(const wl_plan_op_t *ops, uint32_t op_count,
                      const uint32_t *delta_pos, uint32_t k,
                      uint32_t *out_count)
{
    /* Total ops: K copies of original + K CONCATs + 1 CONSOLIDATE
     * (each copy followed by a CONCAT, then CONSOLIDATE at the end) */
    uint32_t total = k * op_count + k + 1;
    wl_plan_op_t *new_ops = (wl_plan_op_t *)calloc(total, sizeof(wl_plan_op_t));
    if (!new_ops)
        return NULL;

    uint32_t wi = 0;

    for (uint32_t d = 0; d < k; d++) {
        for (uint32_t i = 0; i < op_count; i++) {
            if (clone_plan_op(&ops[i], &new_ops[wi]) != 0) {
                for (uint32_t j = 0; j < wi; j++)
                    free_op(&new_ops[j]);
                free(new_ops);
                return NULL;
            }

            /* Set delta_mode: position d gets FORCE_DELTA, all other
             * IDB positions get FORCE_FULL, non-IDB ops stay AUTO. */
            bool is_delta_pos = false;
            for (uint32_t p = 0; p < k; p++) {
                if (delta_pos[p] == i) {
                    new_ops[wi].delta_mode
                        = (p == d) ? WL_DELTA_FORCE_DELTA : WL_DELTA_FORCE_FULL;
                    is_delta_pos = true;
                    break;
                }
            }
            if (!is_delta_pos)
                new_ops[wi].delta_mode = WL_DELTA_AUTO;

            /* Materialization hint: mark the first K-2 JOIN operators
             * (among all delta positions) as materializable. These represent
             * the shared join prefix that the evaluator can cache and reuse
             * across all K copies. */
            if (is_delta_pos && new_ops[wi].op == WL_PLAN_OP_JOIN) {
                /* Count how many JOINs appear before this one in delta_pos */
                uint32_t join_idx = 0;
                for (uint32_t p = 0; p < k; p++) {
                    if (ops[delta_pos[p]].op == WL_PLAN_OP_JOIN) {
                        if (delta_pos[p] == i)
                            break;
                        join_idx++;
                    }
                }
                /* Materialize if this is one of the first K-2 JOINs */
                if (join_idx < k - 2)
                    new_ops[wi].materialized = true;
            }

            wi++;
        }

        /* Add CONCAT after each copy to mark boundaries for the evaluator.
         * The evaluator concatenates K copies before consolidation. */
        memset(&new_ops[wi], 0, sizeof(wl_plan_op_t));
        new_ops[wi].op = WL_PLAN_OP_CONCAT;
        wi++;
    }

    memset(&new_ops[wi], 0, sizeof(wl_plan_op_t));
    new_ops[wi].op = WL_PLAN_OP_CONSOLIDATE;
    wi++;

    *out_count = wi;
    return new_ops;
}

#if ENABLE_K_FUSION
/**
 * Free K-fusion metadata stored in op->opaque_data.
 * Releases the K operator sequences and the metadata struct itself.
 * The caller is responsible for the op itself.
 */
static void
free_k_fusion_opaque(wl_plan_op_t *op)
{
    if (!op->opaque_data)
        return;
    wl_plan_op_k_fusion_t *meta = (wl_plan_op_k_fusion_t *)op->opaque_data;
    if (meta->k_ops) {
        for (uint32_t d = 0; d < meta->k; d++) {
            if (meta->k_ops[d]) {
                uint32_t cnt = meta->k_op_counts ? meta->k_op_counts[d] : 0;
                for (uint32_t i = 0; i < cnt; i++)
                    free_op(&meta->k_ops[d][i]);
                free(meta->k_ops[d]);
            }
        }
        free(meta->k_ops);
    }
    free(meta->k_op_counts);
    free(meta);
    op->opaque_data = NULL;
}

/**
 * Rewrite a single relation plan for K-fusion parallel execution.
 *
 * Creates a single WL_PLAN_OP_K_FUSION operator whose opaque_data
 * points to a wl_plan_op_k_fusion_t containing K independent operator
 * sequences.  In sequence d, delta_pos[d] gets FORCE_DELTA and all
 * other delta positions get FORCE_FULL.  Non-delta ops stay AUTO.
 *
 * Materialization hints are applied identically to expand_multiway_delta():
 * the first K-2 JOIN delta positions are marked materialized for CSE reuse.
 *
 * Returns a 1-element ops array (caller owns) with *out_count = 1.
 * Returns NULL on allocation failure.
 */
static wl_plan_op_t *
expand_multiway_k_fusion(const wl_plan_op_t *ops, uint32_t op_count,
                         const uint32_t *delta_pos, uint32_t k,
                         uint32_t *out_count)
{
    wl_plan_op_t *result = (wl_plan_op_t *)calloc(1, sizeof(wl_plan_op_t));
    if (!result)
        return NULL;

    wl_plan_op_k_fusion_t *meta
        = (wl_plan_op_k_fusion_t *)calloc(1, sizeof(wl_plan_op_k_fusion_t));
    if (!meta) {
        free(result);
        return NULL;
    }

    meta->k = k;
    meta->k_ops = (wl_plan_op_t **)calloc(k, sizeof(wl_plan_op_t *));
    meta->k_op_counts = (uint32_t *)calloc(k, sizeof(uint32_t));
    if (!meta->k_ops || !meta->k_op_counts) {
        free(meta->k_ops);
        free(meta->k_op_counts);
        free(meta);
        free(result);
        return NULL;
    }

    for (uint32_t d = 0; d < k; d++) {
        /* Allocate op_count + 1: the extra slot holds a CONSOLIDATE op
         * so each worker produces sorted+deduped output, enabling the
         * K-fusion merge to skip an expensive post-completion qsort. */
        wl_plan_op_t *seq
            = (wl_plan_op_t *)calloc(op_count + 1, sizeof(wl_plan_op_t));
        if (!seq)
            goto fail;

        meta->k_ops[d] = seq;
        meta->k_op_counts[d] = op_count + 1;

        for (uint32_t i = 0; i < op_count; i++) {
            if (clone_plan_op(&ops[i], &seq[i]) != 0) {
                /* free successfully cloned ops in this sequence */
                for (uint32_t j = 0; j < i; j++)
                    free_op(&seq[j]);
                free(seq);
                meta->k_ops[d] = NULL;
                meta->k_op_counts[d] = 0;
                goto fail;
            }

            /* Apply delta_mode: position d gets FORCE_DELTA,
             * other IDB positions get FORCE_FULL, rest stay AUTO. */
            bool is_delta_pos = false;
            for (uint32_t p = 0; p < k; p++) {
                if (delta_pos[p] == i) {
                    seq[i].delta_mode
                        = (p == d) ? WL_DELTA_FORCE_DELTA : WL_DELTA_FORCE_FULL;
                    is_delta_pos = true;
                    break;
                }
            }
            if (!is_delta_pos)
                seq[i].delta_mode = WL_DELTA_AUTO;

            /* Materialization hint: first K-2 JOIN delta positions. */

            if (is_delta_pos && seq[i].op == WL_PLAN_OP_JOIN) {
                uint32_t join_idx = 0;
                for (uint32_t p = 0; p < k; p++) {
                    if (ops[delta_pos[p]].op == WL_PLAN_OP_JOIN) {
                        if (delta_pos[p] == i)
                            break;
                        join_idx++;
                    }
                }
                if (join_idx < k - 2)
                    seq[i].materialized = true;
            }
        }
        /* Append CONSOLIDATE so each worker produces sorted+deduped output.
         * This allows col_op_k_fusion to skip the post-completion qsort. */
        seq[op_count].op = WL_PLAN_OP_CONSOLIDATE;
    }

    result->op = WL_PLAN_OP_K_FUSION;
    result->opaque_data = (void *)meta;

    *out_count = 1;
    return result;

fail:
    /* Free any sequences allocated so far */
    for (uint32_t d = 0; d < k; d++) {
        if (meta->k_ops[d]) {
            for (uint32_t i = 0; i < meta->k_op_counts[d]; i++)
                free_op(&meta->k_ops[d][i]);
            free(meta->k_ops[d]);
        }
    }
    free(meta->k_ops);
    free(meta->k_op_counts);
    free(meta);
    free(result);
    return NULL;
}
#endif /* ENABLE_K_FUSION */

/**
 * Apply multi-way delta expansion to all recursive strata in the plan.
 * For each relation in a recursive stratum with K >= 2 IDB body atoms,
 * replaces the relation's ops with K expanded copies annotated with
 * delta_mode and materialization hints.
 */
static void
rewrite_multiway_delta(wl_plan_t *plan)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        wl_plan_stratum_t *st = (wl_plan_stratum_t *)&plan->strata[s];
        if (!st->is_recursive || !st->relations)
            continue;

        const char **idb_names
            = (const char **)malloc(st->relation_count * sizeof(char *));
        if (!idb_names)
            continue;
        for (uint32_t r = 0; r < st->relation_count; r++)
            idb_names[r] = st->relations[r].name;

        for (uint32_t r = 0; r < st->relation_count; r++) {
            wl_plan_relation_t *rel = (wl_plan_relation_t *)&st->relations[r];
            if (!rel->ops || rel->op_count == 0)
                continue;

            uint32_t *dpos
                = (uint32_t *)malloc(rel->op_count * sizeof(uint32_t));
            if (!dpos)
                continue;
            uint32_t k = count_delta_positions(
                rel->ops, rel->op_count, idb_names, st->relation_count, dpos);

            if (k >= 2) {
                uint32_t new_count = 0;
                wl_plan_op_t *new_ops;
#if ENABLE_K_FUSION
                new_ops = expand_multiway_k_fusion(rel->ops, rel->op_count,
                                                   dpos, k, &new_count);
#else
                new_ops = expand_multiway_delta(rel->ops, rel->op_count, dpos,
                                                k, &new_count);
#endif
                if (new_ops) {
                    for (uint32_t o = 0; o < rel->op_count; o++)
                        free_op((wl_plan_op_t *)&rel->ops[o]);
                    free((void *)rel->ops);
                    rel->ops = new_ops;
                    rel->op_count = new_count;
                }
            }
            free(dpos);
        }
        free(idb_names);
    }
}

/* ======================================================================== */
/* Public API                                                               */
/* ======================================================================== */

void
wl_plan_free(wl_plan_t *plan)
{
    if (!plan)
        return;

    if (plan->strata) {
        for (uint32_t s = 0; s < plan->stratum_count; s++) {
            wl_plan_stratum_t *st = (wl_plan_stratum_t *)&plan->strata[s];
            if (st->relations) {
                for (uint32_t r = 0; r < st->relation_count; r++) {
                    wl_plan_relation_t *rel
                        = (wl_plan_relation_t *)&st->relations[r];
                    free((void *)rel->name);
                    if (rel->ops) {
                        for (uint32_t o = 0; o < rel->op_count; o++)
                            free_op((wl_plan_op_t *)&rel->ops[o]);
                        free((void *)rel->ops);
                    }
                }
                free((void *)st->relations);
            }
        }
        free((void *)plan->strata);
    }

    if (plan->edb_relations) {
        for (uint32_t i = 0; i < plan->edb_count; i++)
            free((void *)plan->edb_relations[i]);
        free((void *)plan->edb_relations);
    }

    free(plan);
}

int
wl_plan_from_program(const struct wirelog_program *prog, wl_plan_t **out)
{
    if (!prog || !out)
        return -1;

    *out = NULL;

    wl_plan_t *plan = (wl_plan_t *)calloc(1, sizeof(wl_plan_t));
    if (!plan)
        return -1;

    /* ----------------------------------------------------------------
     * Build EDB relation list (relations with no rules / only facts)
     * ---------------------------------------------------------------- */
    uint32_t edb_cap = 8;
    char **edb_rels = (char **)malloc(edb_cap * sizeof(char *));
    if (!edb_rels) {
        free(plan);
        return -1;
    }
    uint32_t edb_count = 0;

    for (uint32_t i = 0; i < prog->relation_count; i++) {
        const wl_ir_relation_info_t *rel = &prog->relations[i];
        if (!rel->name)
            continue;

        /* Check if this relation is a pure IDB (has rules but no facts).
         * Relations with both rules and inline facts (e.g., reach(1).)
         * must still be pre-registered so that fact loading can insert
         * seed tuples before evaluation begins. */
        bool is_idb = false;
        bool has_facts = (rel->fact_count > 0 && rel->fact_data != NULL);
        for (uint32_t r = 0; r < prog->rule_count; r++) {
            if (prog->rules[r].head_relation
                && strcmp(prog->rules[r].head_relation, rel->name) == 0) {
                is_idb = true;
                break;
            }
        }
        if (!is_idb || has_facts) {
            if (edb_count >= edb_cap) {
                edb_cap *= 2;
                char **tmp
                    = (char **)realloc(edb_rels, edb_cap * sizeof(char *));
                if (!tmp) {
                    for (uint32_t j = 0; j < edb_count; j++)
                        free(edb_rels[j]);
                    free((void *)edb_rels);
                    free(plan);
                    return -1;
                }
                edb_rels = tmp;
            }
            edb_rels[edb_count] = dup_str(rel->name);
            if (!edb_rels[edb_count]) {
                for (uint32_t j = 0; j < edb_count; j++)
                    free(edb_rels[j]);
                free((void *)edb_rels);
                free(plan);
                return -1;
            }
            edb_count++;
        }
    }

    plan->edb_relations = (const char *const *)edb_rels;
    plan->edb_count = edb_count;

    /* ----------------------------------------------------------------
     * Build strata
     * ---------------------------------------------------------------- */
    uint32_t stratum_count = prog->stratum_count;
    if (stratum_count == 0)
        stratum_count = 1; /* guarantee at least 1 stratum */

    wl_plan_stratum_t *strata
        = (wl_plan_stratum_t *)calloc(stratum_count, sizeof(wl_plan_stratum_t));
    if (!strata) {
        wl_plan_free(plan);
        return -1;
    }

    for (uint32_t s = 0; s < prog->stratum_count; s++) {
        const wirelog_stratum_t *src = &prog->strata[s];
        wl_plan_stratum_t *dst = &strata[s];

        dst->stratum_id = src->stratum_id;
        dst->is_recursive = src->is_recursive;

        /* Issue #105: Determine if stratum is monotone (derives facts only,
         * no deletion via negation/antijoin). Conservative default: false.
         * Future: analyze operator trees for antijoin/semijoin/subtract operations
         * to set is_monotone = true when no negation is present. For now, we
         * mark only strata with zero negation rules. */
        dst->is_monotone = false; /* Conservative: requires operator analysis */

        /* Count unique relations in this stratum */
        /* Build per-relation plan from relation_irs[] */
        if (!src->rule_names || src->rule_count == 0) {
            dst->relations = NULL;
            dst->relation_count = 0;
            continue;
        }

        /* Collect unique relation names in this stratum */
        uint32_t max_rels = src->rule_count;
        char **unique_names = (char **)calloc(max_rels, sizeof(char *));
        if (!unique_names) {
            free(strata);
            wl_plan_free(plan);
            return -1;
        }
        uint32_t unique_count = 0;

        for (uint32_t r = 0; r < src->rule_count; r++) {
            const char *name = src->rule_names[r];
            if (!name)
                continue;
            bool found = false;
            for (uint32_t u = 0; u < unique_count; u++) {
                if (strcmp(unique_names[u], name) == 0) {
                    found = true;
                    break;
                }
            }
            if (!found)
                unique_names[unique_count++] = (char *)name;
        }

        wl_plan_relation_t *rels = (wl_plan_relation_t *)calloc(
            unique_count, sizeof(wl_plan_relation_t));
        if (!rels) {
            free((void *)unique_names);
            free(strata);
            wl_plan_free(plan);
            return -1;
        }

        for (uint32_t u = 0; u < unique_count; u++) {
            const char *rel_name = unique_names[u];

            /* Find the relation_irs[] index for this relation */
            wirelog_ir_node_t *ir_root = NULL;
            for (uint32_t ri = 0; ri < prog->relation_count; ri++) {
                if (prog->relations[ri].name
                    && strcmp(prog->relations[ri].name, rel_name) == 0
                    && prog->relation_irs && prog->relation_irs[ri]) {
                    ir_root = prog->relation_irs[ri];
                    break;
                }
            }

            rels[u].name = dup_str(rel_name);

            if (ir_root) {
                op_list_t ol;
                if (op_list_init(&ol) != 0) {
                    free((void *)unique_names);
                    /* Clean up already-built relations */
                    for (uint32_t v = 0; v < u; v++) {
                        free((void *)rels[v].name);
                        if (rels[v].ops) {
                            for (uint32_t o = 0; o < rels[v].op_count; o++)
                                free_op((wl_plan_op_t *)&rels[v].ops[o]);
                            free((void *)rels[v].ops);
                        }
                    }
                    free((void *)rels[u].name);
                    free(rels);
                    free(strata);
                    wl_plan_free(plan);
                    return -1;
                }

                int rc = translate_ir_node(ir_root, &ol);
                if (rc != 0) {
                    /* Clean up ops */
                    for (uint32_t o = 0; o < ol.count; o++)
                        free_op(&ol.ops[o]);
                    free(ol.ops);
                    free((void *)unique_names);
                    for (uint32_t v = 0; v < u; v++) {
                        free((void *)rels[v].name);
                        if (rels[v].ops) {
                            for (uint32_t o = 0; o < rels[v].op_count; o++)
                                free_op((wl_plan_op_t *)&rels[v].ops[o]);
                            free((void *)rels[v].ops);
                        }
                    }
                    free((void *)rels[u].name);
                    free(rels);
                    free(strata);
                    wl_plan_free(plan);
                    return -1;
                }

                rels[u].ops = ol.ops;
                rels[u].op_count = ol.count;
            } else {
                rels[u].ops = NULL;
                rels[u].op_count = 0;
            }
        }

        free((void *)unique_names);
        dst->relations = rels;
        dst->relation_count = unique_count;
    }

    plan->strata = strata;
    plan->stratum_count = stratum_count;

    /* Rewrite K-atom recursive rules for complete semi-naive evaluation.
     * For rules with K >= 2 IDB body atoms, emit K copies with CSE
     * materialization hints to avoid the regression seen without CSE. */
    rewrite_multiway_delta(plan);

    *out = plan;
    return 0;
}
