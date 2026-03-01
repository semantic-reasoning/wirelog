/*
 * dd_plan.c - wirelog DD Execution Plan
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Translates stratified IR trees into DD execution plans.
 */

#include "dd_plan.h"
#include "../ir/ir.h"
#include "../ir/program.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define OP_INITIAL_CAPACITY 8

/* ======================================================================== */
/* Helpers                                                                  */
/* ======================================================================== */

/**
 * Add an operator to a relation plan.
 * Returns 0 on success, -1 on memory error.
 */
static int
relation_plan_add_op(wl_dd_relation_plan_t *rp, wl_dd_op_t op)
{
    if (rp->op_count >= rp->op_capacity) {
        uint32_t new_cap
            = rp->op_capacity == 0 ? OP_INITIAL_CAPACITY : rp->op_capacity * 2;
        wl_dd_op_t *tmp
            = (wl_dd_op_t *)realloc(rp->ops, new_cap * sizeof(wl_dd_op_t));
        if (!tmp)
            return -1;
        rp->ops = tmp;
        rp->op_capacity = new_cap;
    }

    rp->ops[rp->op_count++] = op;
    return 0;
}

/**
 * Free all owned memory in a single DD op.
 */
static void
dd_op_free_fields(wl_dd_op_t *op)
{
    free(op->relation_name);
    free(op->right_relation);

    for (uint32_t i = 0; i < op->key_count; i++) {
        free(op->left_keys[i]);
        free(op->right_keys[i]);
    }
    free(op->left_keys);
    free(op->right_keys);

    free(op->project_indices);
    if (op->project_exprs) {
        for (uint32_t i = 0; i < op->project_count; i++)
            wl_ir_expr_free(op->project_exprs[i]);
        free(op->project_exprs);
    }
    free(op->group_by_indices);

    if (op->filter_expr)
        wl_ir_expr_free(op->filter_expr);
}

/* ======================================================================== */
/* IR Tree Translation                                                      */
/* ======================================================================== */

/**
 * Column name context for tracking variable-to-column mappings
 * through the IR tree translation.  Updated by SCAN nodes (and extended
 * by JOIN with right-side non-key columns) so that FILTER and PROJECT
 * nodes can resolve variable names to positional column indices.
 */
#define IR_COL_CTX_MAX 32

typedef struct {
    const char *names[IR_COL_CTX_MAX];
    uint32_t count;
} ir_col_ctx_t;

/**
 * Recursively rewrite variable names in a cloned expression tree
 * from original Datalog names (e.g., "x") to positional names
 * (e.g., "col0") that match the Rust executor's convention.
 */
static void
rewrite_expr_vars(wl_ir_expr_t *expr, const char *const *column_names,
                  uint32_t column_count)
{
    if (!expr)
        return;

    if (expr->type == WL_IR_EXPR_VAR && expr->var_name) {
        for (uint32_t i = 0; i < column_count; i++) {
            if (column_names[i]
                && strcmp(expr->var_name, column_names[i]) == 0) {
                free(expr->var_name);
                char buf[16];
                snprintf(buf, sizeof(buf), "col%u", i);
                expr->var_name = strdup_safe(buf);
                break;
            }
        }
    }

    for (uint32_t i = 0; i < expr->child_count; i++)
        rewrite_expr_vars(expr->children[i], column_names, column_count);
}

/**
 * Translate an IR tree node into DD ops, appended to the relation plan.
 * Walks the tree in post-order (children first, then current node).
 *
 * @ctx tracks the current column name mapping flowing up from SCAN nodes
 * so that FILTER expressions can rewrite variable names to positional form.
 *
 * Returns 0 on success, -1 on memory error.
 */
static int
translate_ir_node(const wirelog_ir_node_t *node, wl_dd_relation_plan_t *rp,
                  const struct wirelog_program *prog, ir_col_ctx_t *ctx)
{
    if (!node)
        return 0;

    /* For JOIN/ANTIJOIN/SEMIJOIN, only translate the left child (child[0]).
     * The right child is referenced by name in the op;
     * translating it would add an extra VARIABLE that overwrites the
     * left side's data in the sequential interpreter. */
    uint32_t child_limit = node->child_count;
    if (node->type == WIRELOG_IR_JOIN || node->type == WIRELOG_IR_ANTIJOIN
        || node->type == WIRELOG_IR_SEMIJOIN)
        child_limit = (node->child_count > 0) ? 1 : 0;

    /* Translate children first (post-order) */
    for (uint32_t i = 0; i < child_limit; i++) {
        int rc = translate_ir_node(node->children[i], rp, prog, ctx);
        if (rc != 0)
            return rc;
    }

    /* Translate the current node */
    wl_dd_op_t op;
    memset(&op, 0, sizeof(op));

    switch (node->type) {
    case WIRELOG_IR_SCAN:
        op.op = WL_DD_VARIABLE;
        if (node->relation_name)
            op.relation_name = strdup_safe(node->relation_name);
        /* Update column context from SCAN's declared variable names */
        if (node->column_names) {
            ctx->count = 0;
            for (uint32_t i = 0; i < node->column_count && i < IR_COL_CTX_MAX;
                 i++)
                ctx->names[ctx->count++] = node->column_names[i];
        }
        break;

    case WIRELOG_IR_FILTER:
        op.op = WL_DD_FILTER;
        if (node->filter_expr) {
            op.filter_expr = wl_ir_expr_clone(node->filter_expr);
            if (!op.filter_expr)
                return -1;
            /* Rewrite variable names to positional col0, col1, ... */
            if (ctx->count > 0)
                rewrite_expr_vars(op.filter_expr,
                                  (const char *const *)ctx->names, ctx->count);
        }
        break;

    case WIRELOG_IR_PROJECT:
        op.op = WL_DD_MAP;
        op.project_count = node->project_count;
        if (node->project_count > 0 && node->project_indices) {
            /* Use explicit index mapping */
            op.project_indices
                = (uint32_t *)malloc(node->project_count * sizeof(uint32_t));
            if (!op.project_indices)
                return -1;
            memcpy(op.project_indices, node->project_indices,
                   node->project_count * sizeof(uint32_t));
        } else if (node->project_count > 0 && node->project_exprs
                   && ctx->count > 0) {
            /* Check whether any expression is non-trivial (not a simple
             * variable reference).  If so, we need per-column expression
             * trees in addition to the index array. */
            bool has_complex = false;
            for (uint32_t i = 0; i < node->project_count; i++) {
                wl_ir_expr_t *e = node->project_exprs[i];
                if (e && e->type != WL_IR_EXPR_VAR) {
                    has_complex = true;
                    break;
                }
            }

            /* Always build the index array (used for simple columns) */
            op.project_indices
                = (uint32_t *)malloc(node->project_count * sizeof(uint32_t));
            if (!op.project_indices)
                return -1;

            /* If complex expressions exist, allocate per-column expr array */
            if (has_complex) {
                op.project_exprs = (wl_ir_expr_t **)calloc(
                    node->project_count, sizeof(wl_ir_expr_t *));
                if (!op.project_exprs) {
                    free(op.project_indices);
                    return -1;
                }
            }

            for (uint32_t i = 0; i < node->project_count; i++) {
                wl_ir_expr_t *e = node->project_exprs[i];
                op.project_indices[i] = i; /* default: identity */

                if (e && e->type == WL_IR_EXPR_VAR && e->var_name) {
                    /* Simple variable -> resolve to column index */
                    for (uint32_t j = 0; j < ctx->count; j++) {
                        if (ctx->names[j]
                            && strcmp(e->var_name, ctx->names[j]) == 0) {
                            op.project_indices[i] = j;
                            break;
                        }
                    }
                } else if (e && e->type != WL_IR_EXPR_VAR && has_complex) {
                    /* Non-trivial expression -> clone and rewrite vars */
                    op.project_exprs[i] = wl_ir_expr_clone(e);
                    if (!op.project_exprs[i]) {
                        dd_op_free_fields(&op);
                        return -1;
                    }
                    rewrite_expr_vars(op.project_exprs[i],
                                      (const char *const *)ctx->names,
                                      ctx->count);
                }
            }
        }
        break;

    case WIRELOG_IR_JOIN:
        op.op = WL_DD_JOIN;
        /* Right relation name from second child */
        if (node->child_count >= 2 && node->children[1]
            && node->children[1]->relation_name) {
            op.right_relation = strdup_safe(node->children[1]->relation_name);
        }
        /* Copy join keys */
        if (node->join_key_count > 0) {
            op.key_count = node->join_key_count;
            op.left_keys = (char **)calloc(op.key_count, sizeof(char *));
            op.right_keys = (char **)calloc(op.key_count, sizeof(char *));
            if (!op.left_keys || !op.right_keys) {
                dd_op_free_fields(&op);
                return -1;
            }
            for (uint32_t k = 0; k < op.key_count; k++) {
                if (node->join_left_keys[k])
                    op.left_keys[k] = strdup_safe(node->join_left_keys[k]);
                if (node->join_right_keys[k])
                    op.right_keys[k] = strdup_safe(node->join_right_keys[k]);
            }
        }
        /* Resolve left key column positions for the Rust executor.
         * Stored in project_indices (reused; unused for JOIN otherwise). */
        if (op.key_count > 0 && ctx->count > 0) {
            op.project_indices
                = (uint32_t *)calloc(op.key_count, sizeof(uint32_t));
            if (op.project_indices) {
                op.project_count = op.key_count;
                for (uint32_t k = 0; k < op.key_count; k++) {
                    if (op.left_keys && op.left_keys[k]) {
                        for (uint32_t j = 0; j < ctx->count; j++) {
                            if (ctx->names[j]
                                && strcmp(op.left_keys[k], ctx->names[j])
                                       == 0) {
                                op.project_indices[k] = j;
                                break;
                            }
                        }
                    }
                }
            }
        }
        /* Update column context: join output = left_cols + right non-key cols */
        if (node->child_count >= 2 && node->children[1]
            && node->children[1]->column_names) {
            uint32_t rkey = node->join_key_count;
            const wirelog_ir_node_t *rc = node->children[1];
            for (uint32_t i = rkey;
                 i < rc->column_count && ctx->count < IR_COL_CTX_MAX; i++)
                ctx->names[ctx->count++] = rc->column_names[i];
        }
        break;

    case WIRELOG_IR_ANTIJOIN:
        op.op = WL_DD_ANTIJOIN;
        if (node->child_count >= 2 && node->children[1]
            && node->children[1]->relation_name) {
            op.right_relation = strdup_safe(node->children[1]->relation_name);
        }
        if (node->join_key_count > 0) {
            op.key_count = node->join_key_count;
            op.left_keys = (char **)calloc(op.key_count, sizeof(char *));
            op.right_keys = (char **)calloc(op.key_count, sizeof(char *));
            if (!op.left_keys || !op.right_keys) {
                dd_op_free_fields(&op);
                return -1;
            }
            for (uint32_t k = 0; k < op.key_count; k++) {
                if (node->join_left_keys[k])
                    op.left_keys[k] = strdup_safe(node->join_left_keys[k]);
                if (node->join_right_keys[k])
                    op.right_keys[k] = strdup_safe(node->join_right_keys[k]);
            }
        }
        break;

    case WIRELOG_IR_AGGREGATE: {
        /* Emit a MAP before REDUCE to project columns into the layout
         * expected by the Rust executor: [group_by_cols..., agg_value].
         * For min/max/sum the value column is the aggregate expression;
         * for count no value column is needed. */
        bool needs_value
            = (node->agg_fn != WL_AGG_COUNT && node->agg_fn != WL_AGG_AVG);
        uint32_t map_count = node->group_by_count + (needs_value ? 1 : 0);

        if (map_count > 0) {
            wl_dd_op_t map_op;
            memset(&map_op, 0, sizeof(map_op));
            map_op.op = WL_DD_MAP;
            map_op.project_count = map_count;
            map_op.project_indices
                = (uint32_t *)calloc(map_count, sizeof(uint32_t));
            if (!map_op.project_indices)
                return -1;

            /* Group-by columns use body column indices (already resolved
             * by program.c) */
            for (uint32_t i = 0; i < node->group_by_count; i++)
                map_op.project_indices[i] = node->group_by_indices[i];

            /* Value column for min/max/sum */
            if (needs_value && node->agg_expr) {
                if (node->agg_expr->type == WL_IR_EXPR_VAR) {
                    /* Simple variable — resolve to body column index */
                    uint32_t col_idx = 0;
                    if (node->agg_expr->var_name && ctx->count > 0) {
                        for (uint32_t j = 0; j < ctx->count; j++) {
                            if (ctx->names[j]
                                && strcmp(node->agg_expr->var_name,
                                          ctx->names[j])
                                       == 0) {
                                col_idx = j;
                                break;
                            }
                        }
                    }
                    map_op.project_indices[node->group_by_count] = col_idx;
                } else {
                    /* Complex expression (e.g. d+w) — use project_exprs */
                    map_op.project_exprs = (struct wl_ir_expr **)calloc(
                        map_count, sizeof(struct wl_ir_expr *));
                    if (!map_op.project_exprs) {
                        free(map_op.project_indices);
                        return -1;
                    }
                    map_op.project_exprs[node->group_by_count]
                        = wl_ir_expr_clone(node->agg_expr);
                    if (!map_op.project_exprs[node->group_by_count]) {
                        dd_op_free_fields(&map_op);
                        return -1;
                    }
                    rewrite_expr_vars(
                        map_op.project_exprs[node->group_by_count],
                        (const char *const *)ctx->names, ctx->count);
                    map_op.project_indices[node->group_by_count] = 0;
                }
            }

            int rc = relation_plan_add_op(rp, map_op);
            if (rc != 0) {
                dd_op_free_fields(&map_op);
                return rc;
            }
        }

        /* REDUCE with sequential group_by_indices (MAP already reordered) */
        op.op = WL_DD_REDUCE;
        op.agg_fn = node->agg_fn;
        op.group_by_count = node->group_by_count;
        if (node->group_by_count > 0) {
            op.group_by_indices
                = (uint32_t *)malloc(node->group_by_count * sizeof(uint32_t));
            if (!op.group_by_indices)
                return -1;
            for (uint32_t i = 0; i < node->group_by_count; i++)
                op.group_by_indices[i] = i;
        }
        break;
    }

    case WIRELOG_IR_FLATMAP: {
        /* FLATMAP = fused FILTER + PROJECT (created by fusion pass).
         * Emit a FILTER op first, then a MAP op. */

        /* 1. FILTER */
        if (node->filter_expr) {
            wl_dd_op_t filt_op;
            memset(&filt_op, 0, sizeof(filt_op));
            filt_op.op = WL_DD_FILTER;
            filt_op.filter_expr = wl_ir_expr_clone(node->filter_expr);
            if (!filt_op.filter_expr)
                return -1;
            if (ctx->count > 0)
                rewrite_expr_vars(filt_op.filter_expr,
                                  (const char *const *)ctx->names, ctx->count);
            int rc = relation_plan_add_op(rp, filt_op);
            if (rc != 0) {
                dd_op_free_fields(&filt_op);
                return rc;
            }
        }

        /* 2. MAP (same logic as PROJECT) */
        op.op = WL_DD_MAP;
        op.project_count = node->project_count;
        if (node->project_count > 0 && node->project_indices) {
            op.project_indices
                = (uint32_t *)malloc(node->project_count * sizeof(uint32_t));
            if (!op.project_indices)
                return -1;
            memcpy(op.project_indices, node->project_indices,
                   node->project_count * sizeof(uint32_t));
        } else if (node->project_count > 0 && node->project_exprs
                   && ctx->count > 0) {
            bool has_complex = false;
            for (uint32_t i = 0; i < node->project_count; i++) {
                wl_ir_expr_t *e = node->project_exprs[i];
                if (e && e->type != WL_IR_EXPR_VAR) {
                    has_complex = true;
                    break;
                }
            }

            op.project_indices
                = (uint32_t *)malloc(node->project_count * sizeof(uint32_t));
            if (!op.project_indices)
                return -1;

            if (has_complex) {
                op.project_exprs = (wl_ir_expr_t **)calloc(
                    node->project_count, sizeof(wl_ir_expr_t *));
                if (!op.project_exprs) {
                    free(op.project_indices);
                    return -1;
                }
            }

            for (uint32_t i = 0; i < node->project_count; i++) {
                wl_ir_expr_t *e = node->project_exprs[i];
                op.project_indices[i] = i;

                if (e && e->type == WL_IR_EXPR_VAR && e->var_name) {
                    for (uint32_t j = 0; j < ctx->count; j++) {
                        if (ctx->names[j]
                            && strcmp(e->var_name, ctx->names[j]) == 0) {
                            op.project_indices[i] = j;
                            break;
                        }
                    }
                } else if (e && e->type != WL_IR_EXPR_VAR && has_complex) {
                    op.project_exprs[i] = wl_ir_expr_clone(e);
                    if (!op.project_exprs[i]) {
                        dd_op_free_fields(&op);
                        return -1;
                    }
                    rewrite_expr_vars(op.project_exprs[i],
                                      (const char *const *)ctx->names,
                                      ctx->count);
                }
            }
        }
        break;
    }

    case WIRELOG_IR_SEMIJOIN:
        op.op = WL_DD_SEMIJOIN;
        if (node->child_count >= 2 && node->children[1]
            && node->children[1]->relation_name) {
            op.right_relation = strdup_safe(node->children[1]->relation_name);
        }
        if (node->join_key_count > 0) {
            op.key_count = node->join_key_count;
            op.left_keys = (char **)calloc(op.key_count, sizeof(char *));
            op.right_keys = (char **)calloc(op.key_count, sizeof(char *));
            if (!op.left_keys || !op.right_keys) {
                dd_op_free_fields(&op);
                return -1;
            }
            for (uint32_t k = 0; k < op.key_count; k++) {
                if (node->join_left_keys[k])
                    op.left_keys[k] = strdup_safe(node->join_left_keys[k]);
                if (node->join_right_keys[k])
                    op.right_keys[k] = strdup_safe(node->join_right_keys[k]);
            }
        }
        /* Resolve left key column positions for the Rust executor */
        if (op.key_count > 0 && ctx->count > 0) {
            op.project_indices
                = (uint32_t *)calloc(op.key_count, sizeof(uint32_t));
            if (op.project_indices) {
                op.project_count = op.key_count;
                for (uint32_t k = 0; k < op.key_count; k++) {
                    if (op.left_keys && op.left_keys[k]) {
                        for (uint32_t j = 0; j < ctx->count; j++) {
                            if (ctx->names[j]
                                && strcmp(op.left_keys[k], ctx->names[j])
                                       == 0) {
                                op.project_indices[k] = j;
                                break;
                            }
                        }
                    }
                }
            }
        }
        /* SEMIJOIN does not add columns to context (output = left input) */
        break;

    case WIRELOG_IR_UNION: {
        /* UNION -> CONCAT + CONSOLIDATE (two ops) */
        wl_dd_op_t concat_op;
        memset(&concat_op, 0, sizeof(concat_op));
        concat_op.op = WL_DD_CONCAT;
        int rc = relation_plan_add_op(rp, concat_op);
        if (rc != 0)
            return rc;

        wl_dd_op_t consol_op;
        memset(&consol_op, 0, sizeof(consol_op));
        consol_op.op = WL_DD_CONSOLIDATE;
        return relation_plan_add_op(rp, consol_op);
    }

    default:
        return 0;
    }

    return relation_plan_add_op(rp, op);
}

/* ======================================================================== */
/* Plan Generation                                                          */
/* ======================================================================== */

int
wl_dd_plan_generate(const struct wirelog_program *prog, wl_dd_plan_t **out)
{
    if (!prog || !out)
        return -2;

    if (!prog->is_stratified)
        return -2;

    *out = NULL;

    wl_dd_plan_t *plan = (wl_dd_plan_t *)calloc(1, sizeof(wl_dd_plan_t));
    if (!plan)
        return -1;

    /* Step 1: Collect EDB relations.
       A relation is EDB if relation_irs[r] == NULL (no defining rules). */
    if (prog->relation_irs) {
        uint32_t edb_count = 0;
        for (uint32_t r = 0; r < prog->relation_count; r++) {
            if (!prog->relation_irs[r])
                edb_count++;
        }

        if (edb_count > 0) {
            plan->edb_relations = (char **)calloc(edb_count, sizeof(char *));
            if (!plan->edb_relations) {
                wl_dd_plan_free(plan);
                return -1;
            }

            uint32_t idx = 0;
            for (uint32_t r = 0; r < prog->relation_count; r++) {
                if (!prog->relation_irs[r]) {
                    plan->edb_relations[idx]
                        = strdup_safe(prog->relations[r].name);
                    if (!plan->edb_relations[idx]) {
                        plan->edb_count = idx;
                        wl_dd_plan_free(plan);
                        return -1;
                    }
                    idx++;
                }
            }
            plan->edb_count = edb_count;
        }
    }

    /* Step 2: Create stratum plans */
    if (prog->stratum_count > 0) {
        plan->strata = (wl_dd_stratum_plan_t *)calloc(
            prog->stratum_count, sizeof(wl_dd_stratum_plan_t));
        if (!plan->strata) {
            wl_dd_plan_free(plan);
            return -1;
        }
        plan->stratum_count = prog->stratum_count;

        for (uint32_t s = 0; s < prog->stratum_count; s++) {
            const wirelog_stratum_t *src = &prog->strata[s];
            wl_dd_stratum_plan_t *sp = &plan->strata[s];

            sp->stratum_id = src->stratum_id;
            sp->is_recursive = src->is_recursive;

            /* Deduplicate rule_names to get unique relations */
            uint32_t unique_count = 0;
            char **unique_names
                = (char **)calloc(src->rule_count + 1, sizeof(char *));
            if (!unique_names && src->rule_count > 0) {
                wl_dd_plan_free(plan);
                return -1;
            }

            for (uint32_t r = 0; r < src->rule_count; r++) {
                bool dup = false;
                for (uint32_t u = 0; u < unique_count; u++) {
                    if (strcmp(src->rule_names[r], unique_names[u]) == 0) {
                        dup = true;
                        break;
                    }
                }
                if (!dup)
                    unique_names[unique_count++] = (char *)src->rule_names[r];
            }

            if (unique_count > 0) {
                sp->relations = (wl_dd_relation_plan_t *)calloc(
                    unique_count, sizeof(wl_dd_relation_plan_t));
                if (!sp->relations) {
                    free(unique_names);
                    wl_dd_plan_free(plan);
                    return -1;
                }
                sp->relation_count = unique_count;

                for (uint32_t u = 0; u < unique_count; u++) {
                    sp->relations[u].name = strdup_safe(unique_names[u]);

                    /* Find merged IR tree for this relation */
                    for (uint32_t ri = 0; ri < prog->relation_count; ri++) {
                        if (strcmp(prog->relations[ri].name, unique_names[u])
                            == 0) {
                            if (prog->relation_irs[ri]) {
                                ir_col_ctx_t ctx = { { 0 }, 0 };
                                int rc = translate_ir_node(
                                    prog->relation_irs[ri], &sp->relations[u],
                                    prog, &ctx);
                                if (rc != 0) {
                                    free(unique_names);
                                    wl_dd_plan_free(plan);
                                    return -1;
                                }
                            }
                            break;
                        }
                    }
                }
            }

            free(unique_names);
        }
    }

    *out = plan;
    return 0;
}

/* ======================================================================== */
/* Cleanup                                                                  */
/* ======================================================================== */

static void
relation_plan_free(wl_dd_relation_plan_t *rp)
{
    if (!rp)
        return;

    free(rp->name);

    for (uint32_t i = 0; i < rp->op_count; i++)
        dd_op_free_fields(&rp->ops[i]);
    free(rp->ops);
}

void
wl_dd_plan_free(wl_dd_plan_t *plan)
{
    if (!plan)
        return;

    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        wl_dd_stratum_plan_t *sp = &plan->strata[s];
        for (uint32_t r = 0; r < sp->relation_count; r++)
            relation_plan_free(&sp->relations[r]);
        free(sp->relations);
    }
    free(plan->strata);

    for (uint32_t i = 0; i < plan->edb_count; i++)
        free(plan->edb_relations[i]);
    free(plan->edb_relations);

    free(plan);
}

/* ======================================================================== */
/* Debug Utilities                                                          */
/* ======================================================================== */

const char *
wl_dd_op_type_str(wl_dd_op_type_t type)
{
    switch (type) {
    case WL_DD_VARIABLE:
        return "VARIABLE";
    case WL_DD_MAP:
        return "MAP";
    case WL_DD_FILTER:
        return "FILTER";
    case WL_DD_JOIN:
        return "JOIN";
    case WL_DD_ANTIJOIN:
        return "ANTIJOIN";
    case WL_DD_REDUCE:
        return "REDUCE";
    case WL_DD_CONCAT:
        return "CONCAT";
    case WL_DD_CONSOLIDATE:
        return "CONSOLIDATE";
    case WL_DD_SEMIJOIN:
        return "SEMIJOIN";
    }
    return "UNKNOWN";
}

void
wl_dd_plan_print(const wl_dd_plan_t *plan)
{
    if (!plan) {
        printf("DD Plan: (null)\n");
        return;
    }

    printf("DD Plan: %u strata, %u EDB relations\n", plan->stratum_count,
           plan->edb_count);

    for (uint32_t i = 0; i < plan->edb_count; i++)
        printf("  EDB: %s\n", plan->edb_relations[i]);

    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        wl_dd_stratum_plan_t *sp = &plan->strata[s];
        printf("  Stratum %u [%s] (%u relations):\n", sp->stratum_id,
               sp->is_recursive ? "recursive" : "non-recursive",
               sp->relation_count);

        for (uint32_t r = 0; r < sp->relation_count; r++) {
            wl_dd_relation_plan_t *rp = &sp->relations[r];
            printf("    %s (%u ops):\n", rp->name, rp->op_count);

            for (uint32_t o = 0; o < rp->op_count; o++) {
                wl_dd_op_t *op = &rp->ops[o];
                printf("      [%u] %s", o, wl_dd_op_type_str(op->op));
                if (op->relation_name)
                    printf("(%s)", op->relation_name);
                if (op->right_relation)
                    printf(" right=%s", op->right_relation);
                printf("\n");
            }
        }
    }
}
