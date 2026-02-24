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
#include "ir.h"
#include "program.h"

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
    free(op->group_by_indices);

    if (op->filter_expr)
        wl_ir_expr_free(op->filter_expr);
}

/* ======================================================================== */
/* IR Tree Translation                                                      */
/* ======================================================================== */

/**
 * Translate an IR tree node into DD ops, appended to the relation plan.
 * Walks the tree in post-order (children first, then current node).
 *
 * Returns 0 on success, -1 on memory error.
 */
static int
translate_ir_node(const wirelog_ir_node_t *node, wl_dd_relation_plan_t *rp,
                  const struct wirelog_program *prog)
{
    if (!node)
        return 0;

    /* Translate children first (post-order) */
    for (uint32_t i = 0; i < node->child_count; i++) {
        int rc = translate_ir_node(node->children[i], rp, prog);
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
        break;

    case WIRELOG_IR_FILTER:
        op.op = WL_DD_FILTER;
        if (node->filter_expr) {
            op.filter_expr = wl_ir_expr_clone(node->filter_expr);
            if (!op.filter_expr)
                return -1;
        }
        break;

    case WIRELOG_IR_PROJECT:
        op.op = WL_DD_MAP;
        op.project_count = node->project_count;
        if (node->project_count > 0 && node->project_indices) {
            op.project_indices
                = (uint32_t *)malloc(node->project_count * sizeof(uint32_t));
            if (!op.project_indices)
                return -1;
            memcpy(op.project_indices, node->project_indices,
                   node->project_count * sizeof(uint32_t));
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

    case WIRELOG_IR_AGGREGATE:
        op.op = WL_DD_REDUCE;
        op.agg_fn = node->agg_fn;
        if (node->group_by_count > 0 && node->group_by_indices) {
            op.group_by_count = node->group_by_count;
            op.group_by_indices
                = (uint32_t *)malloc(node->group_by_count * sizeof(uint32_t));
            if (!op.group_by_indices)
                return -1;
            memcpy(op.group_by_indices, node->group_by_indices,
                   node->group_by_count * sizeof(uint32_t));
        }
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
                                int rc = translate_ir_node(
                                    prog->relation_irs[ri], &sp->relations[u],
                                    prog);
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
