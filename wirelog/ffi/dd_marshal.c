/*
 * dd_marshal.c - wirelog DD FFI Marshalling Layer
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Converts internal DD plans (dd_plan.h) to FFI-safe representations
 * (dd_ffi.h) for transport across the C-to-Rust boundary.
 */

#include "dd_ffi.h"
#include "../ir/ir.h"

#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Expression Serialization Buffer                                          */
/* ======================================================================== */

#define EXPR_BUF_INITIAL_CAP 64

typedef struct {
    uint8_t *data;
    uint32_t size;
    uint32_t capacity;
} expr_buf_t;

static int
expr_buf_init(expr_buf_t *b)
{
    b->data = (uint8_t *)malloc(EXPR_BUF_INITIAL_CAP);
    if (!b->data)
        return -1;
    b->size = 0;
    b->capacity = EXPR_BUF_INITIAL_CAP;
    return 0;
}

static int
expr_buf_ensure(expr_buf_t *b, uint32_t additional)
{
    uint32_t needed = b->size + additional;
    if (needed <= b->capacity)
        return 0;

    uint32_t new_cap = b->capacity;
    while (new_cap < needed)
        new_cap *= 2;

    uint8_t *tmp = (uint8_t *)realloc(b->data, new_cap);
    if (!tmp)
        return -1;
    b->data = tmp;
    b->capacity = new_cap;
    return 0;
}

static int
expr_buf_write_u8(expr_buf_t *b, uint8_t val)
{
    if (expr_buf_ensure(b, 1) != 0)
        return -1;
    b->data[b->size++] = val;
    return 0;
}

static int
expr_buf_write_u16(expr_buf_t *b, uint16_t val)
{
    if (expr_buf_ensure(b, 2) != 0)
        return -1;
    /* little-endian */
    b->data[b->size++] = (uint8_t)(val & 0xFF);
    b->data[b->size++] = (uint8_t)((val >> 8) & 0xFF);
    return 0;
}

static int
expr_buf_write_i64(expr_buf_t *b, int64_t val)
{
    if (expr_buf_ensure(b, 8) != 0)
        return -1;
    memcpy(&b->data[b->size], &val, sizeof(int64_t));
    b->size += 8;
    return 0;
}

static int
expr_buf_write_bytes(expr_buf_t *b, const uint8_t *data, uint32_t len)
{
    if (expr_buf_ensure(b, len) != 0)
        return -1;
    memcpy(&b->data[b->size], data, len);
    b->size += len;
    return 0;
}

/* ======================================================================== */
/* Expression Serialization (Post-order / RPN)                              */
/* ======================================================================== */

/**
 * Recursively serialize an expression tree in post-order.
 * Children are serialized first (left to right), then the current node.
 *
 * Returns 0 on success, -1 on memory error, -3 on malformed tree.
 */
static int
serialize_expr_node(const wl_ir_expr_t *expr, expr_buf_t *b)
{
    if (!expr)
        return -3;

    /* Post-order: serialize children first */
    for (uint32_t i = 0; i < expr->child_count; i++) {
        int rc = serialize_expr_node(expr->children[i], b);
        if (rc != 0)
            return rc;
    }

    /* Serialize current node */
    switch (expr->type) {
    case WL_IR_EXPR_VAR: {
        if (!expr->var_name)
            return -3;
        uint16_t name_len = (uint16_t)strlen(expr->var_name);
        if (expr_buf_write_u8(b, WL_FFI_EXPR_VAR) != 0)
            return -1;
        if (expr_buf_write_u16(b, name_len) != 0)
            return -1;
        if (expr_buf_write_bytes(b, (const uint8_t *)expr->var_name, name_len)
            != 0)
            return -1;
        break;
    }

    case WL_IR_EXPR_CONST_INT:
        if (expr_buf_write_u8(b, WL_FFI_EXPR_CONST_INT) != 0)
            return -1;
        if (expr_buf_write_i64(b, expr->int_value) != 0)
            return -1;
        break;

    case WL_IR_EXPR_CONST_STR: {
        if (!expr->str_value)
            return -3;
        uint16_t str_len = (uint16_t)strlen(expr->str_value);
        if (expr_buf_write_u8(b, WL_FFI_EXPR_CONST_STR) != 0)
            return -1;
        if (expr_buf_write_u16(b, str_len) != 0)
            return -1;
        if (expr_buf_write_bytes(b, (const uint8_t *)expr->str_value, str_len)
            != 0)
            return -1;
        break;
    }

    case WL_IR_EXPR_BOOL:
        if (expr_buf_write_u8(b, WL_FFI_EXPR_BOOL) != 0)
            return -1;
        if (expr_buf_write_u8(b, expr->bool_value ? 1 : 0) != 0)
            return -1;
        break;

    case WL_IR_EXPR_ARITH: {
        uint8_t tag = (uint8_t)(WL_FFI_EXPR_ARITH_ADD + (int)expr->arith_op);
        if (expr_buf_write_u8(b, tag) != 0)
            return -1;
        break;
    }

    case WL_IR_EXPR_CMP: {
        uint8_t tag = (uint8_t)(WL_FFI_EXPR_CMP_EQ + (int)expr->cmp_op);
        if (expr_buf_write_u8(b, tag) != 0)
            return -1;
        break;
    }

    case WL_IR_EXPR_AGG: {
        uint8_t tag = (uint8_t)(WL_FFI_EXPR_AGG_COUNT + (int)expr->agg_fn);
        if (expr_buf_write_u8(b, tag) != 0)
            return -1;
        break;
    }

    default:
        return -3;
    }

    return 0;
}

int
wl_ffi_expr_serialize(const struct wl_ir_expr *expr, wl_ffi_expr_buffer_t *out)
{
    if (!expr) {
        out->data = NULL;
        out->size = 0;
        return 0;
    }

    expr_buf_t b;
    if (expr_buf_init(&b) != 0)
        return -1;

    int rc = serialize_expr_node(expr, &b);
    if (rc != 0) {
        free(b.data);
        return rc;
    }

    out->data = b.data;
    out->size = b.size;
    return 0;
}

/* ======================================================================== */
/* Operator Marshalling                                                     */
/* ======================================================================== */

/**
 * Marshal a single DD op into an FFI op.
 * All strings are deep-copied. Expression trees are serialized.
 *
 * Returns 0 on success, -1 on memory error, -3 on expr error.
 */
static int
marshal_op(const wl_dd_op_t *src, wl_ffi_op_t *dst)
{
    memset(dst, 0, sizeof(*dst));

    switch (src->op) {
    case WL_DD_VARIABLE:
        dst->op = WL_FFI_OP_VARIABLE;
        if (src->relation_name) {
            char *s = strdup_safe(src->relation_name);
            if (!s)
                return -1;
            dst->relation_name = s;
        }
        break;

    case WL_DD_MAP:
        dst->op = WL_FFI_OP_MAP;
        dst->project_count = src->project_count;
        if (src->project_count > 0 && src->project_indices) {
            uint32_t *idx
                = (uint32_t *)malloc(src->project_count * sizeof(uint32_t));
            if (!idx)
                return -1;
            memcpy(idx, src->project_indices,
                   src->project_count * sizeof(uint32_t));
            dst->project_indices = idx;
        }
        /* Serialize per-column expressions (for computed projections) */
        if (src->project_exprs && src->project_count > 0) {
            wl_ffi_expr_buffer_t *bufs = (wl_ffi_expr_buffer_t *)calloc(
                src->project_count, sizeof(wl_ffi_expr_buffer_t));
            if (!bufs)
                return -1;
            for (uint32_t i = 0; i < src->project_count; i++) {
                if (src->project_exprs[i]) {
                    int rc = wl_ffi_expr_serialize(src->project_exprs[i],
                                                   &bufs[i]);
                    if (rc != 0) {
                        for (uint32_t j = 0; j < i; j++)
                            free(bufs[j].data);
                        free(bufs);
                        return rc == -1 ? -1 : -3;
                    }
                }
                /* NULL expr -> bufs[i] remains {NULL, 0} (index-only col) */
            }
            dst->map_exprs = bufs;
            dst->map_expr_count = src->project_count;
        }
        break;

    case WL_DD_FILTER:
        dst->op = WL_FFI_OP_FILTER;
        if (src->filter_expr) {
            int rc = wl_ffi_expr_serialize(src->filter_expr, &dst->filter_expr);
            if (rc != 0)
                return rc == -1 ? -1 : -3;
        }
        break;

    case WL_DD_JOIN:
        dst->op = WL_FFI_OP_JOIN;
        if (src->right_relation) {
            char *s = strdup_safe(src->right_relation);
            if (!s)
                return -1;
            dst->right_relation = s;
        }
        dst->key_count = src->key_count;
        if (src->key_count > 0) {
            char **lk = (char **)calloc(src->key_count, sizeof(char *));
            char **rk = (char **)calloc(src->key_count, sizeof(char *));
            if (!lk || !rk) {
                free(lk);
                free(rk);
                return -1;
            }
            for (uint32_t k = 0; k < src->key_count; k++) {
                if (src->left_keys && src->left_keys[k]) {
                    lk[k] = strdup_safe(src->left_keys[k]);
                    if (!lk[k]) {
                        for (uint32_t j = 0; j < k; j++) {
                            free(lk[j]);
                            free(rk[j]);
                        }
                        free(lk);
                        free(rk);
                        return -1;
                    }
                }
                if (src->right_keys && src->right_keys[k]) {
                    rk[k] = strdup_safe(src->right_keys[k]);
                    if (!rk[k]) {
                        free(lk[k]); /* free the left key we just made */
                        for (uint32_t j = 0; j < k; j++) {
                            free(lk[j]);
                            free(rk[j]);
                        }
                        free(lk);
                        free(rk);
                        return -1;
                    }
                }
            }
            dst->left_keys = (const char *const *)lk;
            dst->right_keys = (const char *const *)rk;
        }
        break;

    case WL_DD_ANTIJOIN:
        dst->op = WL_FFI_OP_ANTIJOIN;
        if (src->right_relation) {
            char *s = strdup_safe(src->right_relation);
            if (!s)
                return -1;
            dst->right_relation = s;
        }
        dst->key_count = src->key_count;
        if (src->key_count > 0) {
            char **lk = (char **)calloc(src->key_count, sizeof(char *));
            char **rk = (char **)calloc(src->key_count, sizeof(char *));
            if (!lk || !rk) {
                free(lk);
                free(rk);
                return -1;
            }
            for (uint32_t k = 0; k < src->key_count; k++) {
                if (src->left_keys && src->left_keys[k]) {
                    lk[k] = strdup_safe(src->left_keys[k]);
                    if (!lk[k]) {
                        for (uint32_t j = 0; j < k; j++) {
                            free(lk[j]);
                            free(rk[j]);
                        }
                        free(lk);
                        free(rk);
                        return -1;
                    }
                }
                if (src->right_keys && src->right_keys[k]) {
                    rk[k] = strdup_safe(src->right_keys[k]);
                    if (!rk[k]) {
                        free(lk[k]);
                        for (uint32_t j = 0; j < k; j++) {
                            free(lk[j]);
                            free(rk[j]);
                        }
                        free(lk);
                        free(rk);
                        return -1;
                    }
                }
            }
            dst->left_keys = (const char *const *)lk;
            dst->right_keys = (const char *const *)rk;
        }
        break;

    case WL_DD_REDUCE:
        dst->op = WL_FFI_OP_REDUCE;
        dst->agg_fn = src->agg_fn;
        dst->group_by_count = src->group_by_count;
        if (src->group_by_count > 0 && src->group_by_indices) {
            uint32_t *gbi
                = (uint32_t *)malloc(src->group_by_count * sizeof(uint32_t));
            if (!gbi)
                return -1;
            memcpy(gbi, src->group_by_indices,
                   src->group_by_count * sizeof(uint32_t));
            dst->group_by_indices = gbi;
        }
        break;

    case WL_DD_CONCAT:
        dst->op = WL_FFI_OP_CONCAT;
        break;

    case WL_DD_CONSOLIDATE:
        dst->op = WL_FFI_OP_CONSOLIDATE;
        break;
    }

    return 0;
}

/* ======================================================================== */
/* FFI Op Free (single op)                                                  */
/* ======================================================================== */

static void
ffi_op_free_fields(wl_ffi_op_t *op)
{
    free((void *)op->relation_name);
    free((void *)op->right_relation);

    if (op->left_keys) {
        char **lk = (char **)(uintptr_t)op->left_keys;
        for (uint32_t i = 0; i < op->key_count; i++)
            free(lk[i]);
        free(lk);
    }
    if (op->right_keys) {
        char **rk = (char **)(uintptr_t)op->right_keys;
        for (uint32_t i = 0; i < op->key_count; i++)
            free(rk[i]);
        free(rk);
    }

    free((void *)op->project_indices);
    free((void *)op->group_by_indices);
    free(op->filter_expr.data);
    if (op->map_exprs) {
        for (uint32_t i = 0; i < op->map_expr_count; i++)
            free(op->map_exprs[i].data);
        free(op->map_exprs);
    }
}

/* ======================================================================== */
/* Plan Marshalling                                                         */
/* ======================================================================== */

int
wl_dd_marshal_plan(const wl_dd_plan_t *plan, wl_ffi_plan_t **out)
{
    if (!plan || !out)
        return -2;

    wl_ffi_plan_t *ffi = (wl_ffi_plan_t *)calloc(1, sizeof(wl_ffi_plan_t));
    if (!ffi)
        return -1;

    /* Marshal EDB relations */
    if (plan->edb_count > 0) {
        char **edb = (char **)calloc(plan->edb_count, sizeof(char *));
        if (!edb) {
            free(ffi);
            return -1;
        }
        for (uint32_t i = 0; i < plan->edb_count; i++) {
            edb[i] = strdup_safe(plan->edb_relations[i]);
            if (!edb[i]) {
                for (uint32_t j = 0; j < i; j++)
                    free(edb[j]);
                free(edb);
                free(ffi);
                return -1;
            }
        }
        ffi->edb_relations = (const char *const *)edb;
        ffi->edb_count = plan->edb_count;
    }

    /* Marshal strata */
    if (plan->stratum_count > 0) {
        wl_ffi_stratum_plan_t *strata = (wl_ffi_stratum_plan_t *)calloc(
            plan->stratum_count, sizeof(wl_ffi_stratum_plan_t));
        if (!strata) {
            wl_ffi_plan_free(ffi);
            return -1;
        }
        ffi->strata = strata;
        ffi->stratum_count = plan->stratum_count;

        for (uint32_t s = 0; s < plan->stratum_count; s++) {
            const wl_dd_stratum_plan_t *src_s = &plan->strata[s];
            wl_ffi_stratum_plan_t *dst_s = &strata[s];

            dst_s->stratum_id = src_s->stratum_id;
            dst_s->is_recursive = src_s->is_recursive;

            if (src_s->relation_count > 0) {
                wl_ffi_relation_plan_t *rels = (wl_ffi_relation_plan_t *)calloc(
                    src_s->relation_count, sizeof(wl_ffi_relation_plan_t));
                if (!rels) {
                    wl_ffi_plan_free(ffi);
                    return -1;
                }
                dst_s->relations = rels;
                dst_s->relation_count = src_s->relation_count;

                for (uint32_t r = 0; r < src_s->relation_count; r++) {
                    const wl_dd_relation_plan_t *src_r = &src_s->relations[r];
                    wl_ffi_relation_plan_t *dst_r = &rels[r];

                    /* Deep copy relation name */
                    if (src_r->name) {
                        char *n = strdup_safe(src_r->name);
                        if (!n) {
                            wl_ffi_plan_free(ffi);
                            return -1;
                        }
                        dst_r->name = n;
                    }

                    /* Marshal ops */
                    if (src_r->op_count > 0) {
                        wl_ffi_op_t *ops = (wl_ffi_op_t *)calloc(
                            src_r->op_count, sizeof(wl_ffi_op_t));
                        if (!ops) {
                            wl_ffi_plan_free(ffi);
                            return -1;
                        }
                        dst_r->ops = ops;
                        dst_r->op_count = src_r->op_count;

                        for (uint32_t o = 0; o < src_r->op_count; o++) {
                            int rc = marshal_op(&src_r->ops[o], &ops[o]);
                            if (rc != 0) {
                                wl_ffi_plan_free(ffi);
                                return rc;
                            }
                        }
                    }
                }
            }
        }
    }

    *out = ffi;
    return 0;
}

/* ======================================================================== */
/* FFI Plan Free                                                            */
/* ======================================================================== */

void
wl_ffi_plan_free(wl_ffi_plan_t *plan)
{
    if (!plan)
        return;

    /* Free strata */
    if (plan->strata) {
        wl_ffi_stratum_plan_t *strata
            = (wl_ffi_stratum_plan_t *)(uintptr_t)plan->strata;
        for (uint32_t s = 0; s < plan->stratum_count; s++) {
            wl_ffi_stratum_plan_t *sp = &strata[s];
            if (sp->relations) {
                wl_ffi_relation_plan_t *rels
                    = (wl_ffi_relation_plan_t *)(uintptr_t)sp->relations;
                for (uint32_t r = 0; r < sp->relation_count; r++) {
                    wl_ffi_relation_plan_t *rp = &rels[r];
                    free((void *)rp->name);
                    if (rp->ops) {
                        wl_ffi_op_t *ops = (wl_ffi_op_t *)(uintptr_t)rp->ops;
                        for (uint32_t o = 0; o < rp->op_count; o++)
                            ffi_op_free_fields(&ops[o]);
                        free(ops);
                    }
                }
                free(rels);
            }
        }
        free(strata);
    }

    /* Free EDB relations */
    if (plan->edb_relations) {
        char **edb = (char **)(uintptr_t)plan->edb_relations;
        for (uint32_t i = 0; i < plan->edb_count; i++)
            free(edb[i]);
        free(edb);
    }

    free(plan);
}
