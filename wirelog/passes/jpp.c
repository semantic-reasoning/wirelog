/*
 * jpp.c - Join-Project Plan Optimization Pass
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Reorders multi-atom joins to minimize intermediate result sizes.
 * Operates in-place on the program's merged relation IR trees.
 */

#include "jpp.h"
#include "../ir/ir.h"
#include "../ir/program.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Internal: extract SCAN leaves from a left-deep JOIN chain                */
/* ======================================================================== */

/*
 * A left-deep JOIN chain:
 *   JOIN(JOIN(JOIN(S0, S1), S2), S3)
 *
 * Leaf order: [S0, S1, S2, S3]  (left-to-right)
 */
static uint32_t
collect_scans(wirelog_ir_node_t *node, wirelog_ir_node_t **out, uint32_t max)
{
    if (!node)
        return 0;
    if (node->type != WIRELOG_IR_JOIN) {
        /* Leaf (SCAN or FILTER wrapping a SCAN) */
        if (max > 0)
            out[0] = node;
        return 1;
    }
    /* Left child first, then right child */
    uint32_t n = 0;
    if (node->child_count >= 1)
        n += collect_scans(node->children[0], out, max);
    if (node->child_count >= 2 && n < max)
        n += collect_scans(node->children[1], out + n, max - n);
    return n;
}

/* ======================================================================== */
/* Internal: variable set operations                                        */
/* ======================================================================== */

/*
 * Get the variable names for a SCAN node.
 * Returns column_names and sets *count.
 */
static char **
scan_vars(const wirelog_ir_node_t *scan, uint32_t *count)
{
    if (!scan) {
        *count = 0;
        return NULL;
    }
    *count = scan->column_count;
    return scan->column_names;
}

/*
 * Count shared (non-NULL, non-wildcard) variable names between two sets.
 */
static uint32_t
count_shared_vars(char **a, uint32_t na, char **b, uint32_t nb)
{
    uint32_t shared = 0;
    for (uint32_t i = 0; i < na; i++) {
        if (!a[i])
            continue;
        for (uint32_t j = 0; j < nb; j++) {
            if (!b[j])
                continue;
            if (strcmp(a[i], b[j]) == 0) {
                shared++;
                break;
            }
        }
    }
    return shared;
}

/*
 * Check if a variable name is in a set.
 */
static bool
var_in_set(const char *var, char **set, uint32_t nset)
{
    if (!var)
        return false;
    for (uint32_t i = 0; i < nset; i++) {
        if (set[i] && strcmp(var, set[i]) == 0)
            return true;
    }
    return false;
}

/* ======================================================================== */
/* Internal: set up join keys on a JOIN node                                */
/* ======================================================================== */

/*
 * Mirrors the logic of setup_join_keys() in program.c but operates
 * on variable name arrays directly.
 */
static void
jpp_setup_join_keys(char **left_vars, uint32_t left_count, char **right_vars,
    uint32_t right_count, wirelog_ir_node_t *join)
{
    uint32_t key_count
        = count_shared_vars(left_vars, left_count, right_vars, right_count);
    if (key_count == 0)
        return;

    join->join_left_keys = (char **)calloc(key_count, sizeof(char *));
    join->join_right_keys = (char **)calloc(key_count, sizeof(char *));
    if (!join->join_left_keys || !join->join_right_keys)
        return;
    join->join_key_count = key_count;

    uint32_t k = 0;
    for (uint32_t i = 0; i < left_count; i++) {
        if (!left_vars[i])
            continue;
        for (uint32_t j = 0; j < right_count; j++) {
            if (!right_vars[j])
                continue;
            if (strcmp(left_vars[i], right_vars[j]) == 0) {
                join->join_left_keys[k] = strdup_safe(left_vars[i]);
                join->join_right_keys[k] = strdup_safe(right_vars[j]);
                k++;
                break;
            }
        }
    }
}

/* ======================================================================== */
/* Internal: merge two variable name sets (union, no duplicates)            */
/* ======================================================================== */

static char **
merge_vars(char **a, uint32_t na, char **b, uint32_t nb, uint32_t *out_count)
{
    /* Upper bound on merged size */
    char **merged = (char **)calloc(na + nb, sizeof(char *));
    if (!merged) {
        *out_count = 0;
        return NULL;
    }

    uint32_t n = 0;
    /* Copy all from a */
    for (uint32_t i = 0; i < na; i++) {
        merged[n++] = a[i]; /* alias, not strdup */
    }
    /* Add from b if not already in merged */
    for (uint32_t j = 0; j < nb; j++) {
        if (!b[j])
            continue;
        bool found = false;
        for (uint32_t k = 0; k < n; k++) {
            if (merged[k] && strcmp(merged[k], b[j]) == 0) {
                found = true;
                break;
            }
        }
        if (!found)
            merged[n++] = b[j]; /* alias */
    }
    *out_count = n;
    return merged;
}

/* ======================================================================== */
/* Internal: free old join keys on a node                                   */
/* ======================================================================== */

static void
free_join_keys(wirelog_ir_node_t *node)
{
    if (!node)
        return;
    for (uint32_t i = 0; i < node->join_key_count; i++) {
        free(node->join_left_keys[i]);
        free(node->join_right_keys[i]);
    }
    free(node->join_left_keys);
    free(node->join_right_keys);
    node->join_left_keys = NULL;
    node->join_right_keys = NULL;
    node->join_key_count = 0;
}

/* ======================================================================== */
/* Internal: IDB scan detection                                             */
/* ======================================================================== */

/*
 * Return true if the given scan node references an IDB relation.
 * A relation is IDB if it appears as the head of at least one rule.
 * idb_names/idb_count is the de-duplicated list of IDB relation names
 * extracted from the program's rule set.
 */
static bool
scan_is_idb(const wirelog_ir_node_t *scan, const char *const *idb_names,
    uint32_t idb_count)
{
    if (!scan || !scan->relation_name || !idb_names)
        return false;
    for (uint32_t i = 0; i < idb_count; i++) {
        if (idb_names[i]
            && strcmp(scan->relation_name, idb_names[i]) == 0)
            return true;
    }
    return false;
}

/* ======================================================================== */
/* Internal: greedy reorder a join chain                                    */
/* ======================================================================== */

/*
 * Given an array of SCAN leaves, compute the greedy ordering that
 * maximizes shared variables at each step.
 *
 * On a tie (equal shared-variable count), EDB atoms are preferred over
 * IDB atoms.  This avoids placing large recursive relations early in the
 * join chain (e.g. VarPointsTo in DOOP).
 *
 * idb_names/idb_count: de-duplicated IDB relation name list for tie-breaking.
 * May be NULL/0 to disable tie-breaking (falls back to index order).
 *
 * Returns true if the ordering changed from the original.
 */
static bool
greedy_order(wirelog_ir_node_t **scans, uint32_t nscan, uint32_t *order,
    const char *const *idb_names, uint32_t idb_count)
{
    if (nscan < 2) {
        for (uint32_t i = 0; i < nscan; i++)
            order[i] = i;
        return false;
    }

    bool *used = (bool *)calloc(nscan, sizeof(bool));
    if (!used) {
        for (uint32_t i = 0; i < nscan; i++)
            order[i] = i;
        return false;
    }

    /* Pre-compute IDB flags for all scans */
    bool *is_idb = (bool *)calloc(nscan, sizeof(bool));
    if (!is_idb) {
        free(used);
        for (uint32_t i = 0; i < nscan; i++)
            order[i] = i;
        return false;
    }
    for (uint32_t i = 0; i < nscan; i++)
        is_idb[i] = scan_is_idb(scans[i], idb_names, idb_count);

    /* Start with scan[0] */
    order[0] = 0;
    used[0] = true;

    /* Accumulated variable set */
    uint32_t acc_count;
    char **acc_vars = scan_vars(scans[0], &acc_count);
    /* We need a mutable copy for merging */
    char **acc = (char **)calloc(acc_count + nscan * 16, sizeof(char *));
    if (!acc) {
        free(is_idb);
        free(used);
        for (uint32_t i = 0; i < nscan; i++)
            order[i] = i;
        return false;
    }
    for (uint32_t i = 0; i < acc_count; i++)
        acc[i] = acc_vars[i];

    for (uint32_t step = 1; step < nscan; step++) {
        uint32_t best_idx = 0;
        uint32_t best_shared = 0;
        bool found_any = false;

        for (uint32_t j = 0; j < nscan; j++) {
            if (used[j])
                continue;
            uint32_t scount;
            char **svars = scan_vars(scans[j], &scount);
            uint32_t shared = count_shared_vars(acc, acc_count, svars, scount);
            /*
             * Pick j if:
             *   (a) strictly more shared variables, OR
             *   (b) equal shared variables AND j is EDB while current best
             *       is IDB (EDB tie-breaker).
             */
            if (!found_any || shared > best_shared
                || (shared == best_shared && !is_idb[j]
                && is_idb[best_idx])) {
                best_shared = shared;
                best_idx = j;
                found_any = true;
            }
        }

        order[step] = best_idx;
        used[best_idx] = true;

        /* Merge best's vars into accumulated set */
        uint32_t scount;
        char **svars = scan_vars(scans[best_idx], &scount);
        for (uint32_t j = 0; j < scount; j++) {
            if (!svars[j])
                continue;
            bool dup = false;
            for (uint32_t k = 0; k < acc_count; k++) {
                if (acc[k] && strcmp(acc[k], svars[j]) == 0) {
                    dup = true;
                    break;
                }
            }
            if (!dup)
                acc[acc_count++] = svars[j];
        }
    }

    free(acc);
    free(is_idb);
    free(used);

    /* Check if ordering changed */
    for (uint32_t i = 0; i < nscan; i++) {
        if (order[i] != i)
            return true;
    }
    return false;
}

/* ======================================================================== */
/* Internal: rebuild a left-deep JOIN chain from ordered scans              */
/* ======================================================================== */

/*
 * Rebuilds the JOIN chain in-place by reusing existing JOIN nodes.
 * The original JOIN nodes are collected, then reconnected in the new order.
 *
 * join_nodes[0] is the deepest (innermost) JOIN,
 * join_nodes[n-2] is the outermost JOIN.
 *
 * After rebuild:
 *   join_nodes[0] = JOIN(ordered[0], ordered[1])
 *   join_nodes[1] = JOIN(join_nodes[0], ordered[2])
 *   ...
 */
static void
rebuild_chain(wirelog_ir_node_t **join_nodes, uint32_t njoin,
    wirelog_ir_node_t **ordered_scans, uint32_t nscan)
{
    if (njoin == 0 || nscan < 2)
        return;

    /* First pass: clear old join keys on all JOIN nodes */
    for (uint32_t i = 0; i < njoin; i++)
        free_join_keys(join_nodes[i]);

    /* Build the chain bottom-up */
    /* Deepest join: children are ordered_scans[0] and ordered_scans[1] */
    join_nodes[0]->children[0] = ordered_scans[0];
    join_nodes[0]->children[1] = ordered_scans[1];

    /* Set up join keys for deepest */
    uint32_t lcount, rcount;
    char **lvars = scan_vars(ordered_scans[0], &lcount);
    char **rvars = scan_vars(ordered_scans[1], &rcount);
    jpp_setup_join_keys(lvars, lcount, rvars, rcount, join_nodes[0]);

    /* Accumulate variable set */
    uint32_t acc_count;
    char **acc = merge_vars(lvars, lcount, rvars, rcount, &acc_count);

    /* Remaining joins */
    for (uint32_t i = 1; i < njoin; i++) {
        join_nodes[i]->children[0] = join_nodes[i - 1];
        join_nodes[i]->children[1] = ordered_scans[i + 1];

        uint32_t scount;
        char **svars = scan_vars(ordered_scans[i + 1], &scount);
        jpp_setup_join_keys(acc, acc_count, svars, scount, join_nodes[i]);

        /* Merge into accumulated */
        uint32_t new_count;
        char **new_acc = merge_vars(acc, acc_count, svars, scount, &new_count);
        free(acc);
        acc = new_acc;
        acc_count = new_count;
    }

    free(acc);
}

/* ======================================================================== */
/* Internal: collect JOIN nodes from a left-deep chain (deepest first)      */
/* ======================================================================== */

static uint32_t
collect_joins(wirelog_ir_node_t *node, wirelog_ir_node_t **out, uint32_t max)
{
    if (!node || node->type != WIRELOG_IR_JOIN || max == 0)
        return 0;
    uint32_t n = 0;
    /* Recurse into left child first (deeper joins come first) */
    if (node->child_count >= 1)
        n = collect_joins(node->children[0], out, max);
    if (n < max)
        out[n++] = node;
    return n;
}

/* ======================================================================== */
/* Internal: collect head variable names from wrapper nodes                 */
/* ======================================================================== */

/*
 * Walk from the IR root through wrapper nodes to collect all variable
 * names that are referenced by the head projection or filter expressions.
 * This tells us which variables must survive to the top of the join chain.
 */
static void
collect_head_vars_from_expr(const wl_ir_expr_t *expr, char **out,
    uint32_t *count, uint32_t max)
{
    if (!expr || *count >= max)
        return;
    if (expr->type == WL_IR_EXPR_VAR && expr->var_name) {
        /* Check for duplicates */
        for (uint32_t i = 0; i < *count; i++) {
            if (out[i] && strcmp(out[i], expr->var_name) == 0)
                return;
        }
        out[(*count)++] = expr->var_name; /* alias */
    }
    for (uint32_t i = 0; i < expr->child_count; i++) {
        collect_head_vars_from_expr(expr->children[i], out, count, max);
    }
}

static uint32_t
collect_head_vars(wirelog_ir_node_t *ir, char **out, uint32_t max)
{
    uint32_t count = 0;
    wirelog_ir_node_t *node = ir;

    while (node) {
        if (node->type == WIRELOG_IR_PROJECT) {
            /* Collect from project_exprs */
            if (node->project_exprs) {
                for (uint32_t i = 0; i < node->project_count; i++) {
                    collect_head_vars_from_expr(node->project_exprs[i], out,
                        &count, max);
                }
            }
        } else if (node->type == WIRELOG_IR_FLATMAP) {
            /* Has both project_exprs and filter_expr */
            if (node->project_exprs) {
                for (uint32_t i = 0; i < node->project_count; i++) {
                    collect_head_vars_from_expr(node->project_exprs[i], out,
                        &count, max);
                }
            }
            if (node->filter_expr) {
                collect_head_vars_from_expr(node->filter_expr, out, &count,
                    max);
            }
        } else if (node->type == WIRELOG_IR_FILTER) {
            if (node->filter_expr) {
                collect_head_vars_from_expr(node->filter_expr, out, &count,
                    max);
            }
        } else if (node->type == WIRELOG_IR_ANTIJOIN) {
            /* ANTIJOIN join keys reference variables that must survive */
            for (uint32_t i = 0; i < node->join_key_count; i++) {
                if (node->join_left_keys[i]
                    && !var_in_set(node->join_left_keys[i], out, count)
                    && count < max) {
                    out[count++] = node->join_left_keys[i];
                }
            }
        } else {
            break;
        }

        if (node->child_count > 0)
            node = node->children[0];
        else
            break;
    }

    return count;
}

/* ======================================================================== */
/* Internal: insert intermediate projections in a join chain                */
/* ======================================================================== */

/*
 * For each intermediate JOIN in the chain (all except the outermost),
 * check if the accumulated variable set contains variables not needed
 * by any subsequent scan or the head. If so, insert a PROJECT node.
 *
 * Returns number of projections inserted.
 */
static uint32_t
insert_projections(wirelog_ir_node_t *join_root, char **head_vars,
    uint32_t head_var_count)
{
    if (!join_root || join_root->type != WIRELOG_IR_JOIN)
        return 0;

    /* Count depth and collect scans/joins */
    uint32_t depth = 0;
    {
        wirelog_ir_node_t *n = join_root;
        while (n && n->type == WIRELOG_IR_JOIN) {
            depth++;
            n = n->child_count > 0 ? n->children[0] : NULL;
        }
    }
    if (depth < 2)
        return 0; /* Need at least 3 atoms for intermediate projection */

    uint32_t nscan = depth + 1;
    wirelog_ir_node_t **scans
        = (wirelog_ir_node_t **)calloc(nscan, sizeof(wirelog_ir_node_t *));
    wirelog_ir_node_t **joins
        = (wirelog_ir_node_t **)calloc(depth, sizeof(wirelog_ir_node_t *));
    if (!scans || !joins) {
        free(scans);
        free(joins);
        return 0;
    }

    collect_scans(join_root, scans, nscan);
    collect_joins(join_root, joins, depth);

    uint32_t projections = 0;

    /* For each intermediate join (all except the outermost = joins[depth-1]),
     * check if we can project away variables. */
    uint32_t acc_count;
    char **acc_vars = scan_vars(scans[0], &acc_count);
    /* Build mutable accumulated set */
    char **acc = (char **)calloc(nscan * 16, sizeof(char *));
    if (!acc) {
        free(scans);
        free(joins);
        return 0;
    }
    for (uint32_t i = 0; i < acc_count; i++)
        acc[i] = acc_vars[i];

    /* Merge scan[1] */
    {
        uint32_t scount;
        char **svars = scan_vars(scans[1], &scount);
        for (uint32_t j = 0; j < scount; j++) {
            if (!svars[j])
                continue;
            if (!var_in_set(svars[j], acc, acc_count))
                acc[acc_count++] = svars[j];
        }
    }

    /* Track the actual physical column layout of the current join output.
     * Unlike acc[] (which is deduplicated), phys_names[] mirrors the true
     * columnar output: scan columns concatenated in order, join-key columns
     * appearing in both children.  When a PROJECT is inserted it shrinks the
     * layout; subsequent scans are appended on top.  This is the layout that
     * project_indices must reference. */
    char **phys_names = (char **)calloc(nscan * 32, sizeof(char *));
    uint32_t phys_count = 0;
    if (!phys_names) {
        free(acc);
        free(scans);
        free(joins);
        return 0;
    }
    /* Initial physical layout: S0 columns || S1 columns (with duplicates) */
    {
        uint32_t s0c;
        char **s0v = scan_vars(scans[0], &s0c);
        for (uint32_t j = 0; j < s0c; j++)
            phys_names[phys_count++] = s0v[j];
        uint32_t s1c;
        char **s1v = scan_vars(scans[1], &s1c);
        for (uint32_t j = 0; j < s1c; j++)
            phys_names[phys_count++] = s1v[j];
    }

    /* Now acc has the result of joins[0] (deepest join).
     * For each intermediate join i (0 to depth-2):
     *   - acc has the accumulated vars after joins[i]
     *   - Check which vars in acc are needed by scans[i+2..nscan-1]
     *     and head_vars
     *   - If any are dead, insert a PROJECT
     */
    for (uint32_t i = 0; i < depth - 1; i++) {
        /* Compute "needed" vars: head_vars + vars in future scans */
        /* Build needed set */
        char **needed = (char **)calloc(nscan * 16, sizeof(char *));
        uint32_t needed_count = 0;
        if (!needed)
            break;

        /* Add head vars */
        for (uint32_t h = 0; h < head_var_count; h++) {
            if (head_vars[h] && !var_in_set(head_vars[h], needed, needed_count))
                needed[needed_count++] = head_vars[h];
        }

        /* Add vars from future scans (scans[i+2] onward) */
        for (uint32_t s = i + 2; s < nscan; s++) {
            uint32_t scount;
            char **svars = scan_vars(scans[s], &scount);
            for (uint32_t j = 0; j < scount; j++) {
                if (svars[j] && !var_in_set(svars[j], needed, needed_count))
                    needed[needed_count++] = svars[j];
            }
        }

        /* Count how many accumulated vars are needed */
        uint32_t live_count = 0;
        for (uint32_t v = 0; v < acc_count; v++) {
            if (acc[v] && var_in_set(acc[v], needed, needed_count))
                live_count++;
        }

        if (live_count < acc_count && live_count > 0) {
            /* Some variables are dead; insert a PROJECT */
            wirelog_ir_node_t *proj = wl_ir_node_create(WIRELOG_IR_PROJECT);
            if (proj) {
                proj->project_count = live_count;
                proj->project_indices
                    = (uint32_t *)calloc(live_count, sizeof(uint32_t));
                proj->column_names
                    = (char **)calloc(live_count, sizeof(char *));
                if (proj->project_indices && proj->column_names) {
                    proj->column_count = live_count;
                    uint32_t p = 0;
                    for (uint32_t v = 0; v < acc_count; v++) {
                        if (acc[v]
                            && var_in_set(acc[v], needed, needed_count)) {
                            /* Find the first occurrence of acc[v] in the
                             * tracked physical column layout.  phys_names[]
                             * reflects the true columnar output after any
                             * prior PROJECTs, so indices are correct even
                             * when 2+ projections are inserted in one chain. */
                            uint32_t phys_idx = 0;
                            for (uint32_t ph = 0; ph < phys_count; ph++) {
                                if (phys_names[ph] && acc[v]
                                    && strcmp(phys_names[ph], acc[v]) == 0) {
                                    phys_idx = ph;
                                    break;
                                }
                            }
                            proj->project_indices[p] = phys_idx;
                            proj->column_names[p] = strdup_safe(acc[v]);
                            p++;
                        }
                    }

                    /* Insert: parent_join->children[0] = proj,
                     *          proj->child = current_join */
                    wl_ir_node_add_child(proj, joins[i]);
                    joins[i + 1]->children[0] = proj;

                    /* Update acc to reflect projection */
                    uint32_t new_acc = 0;
                    for (uint32_t v = 0; v < acc_count; v++) {
                        if (acc[v]
                            && var_in_set(acc[v], needed, needed_count)) {
                            acc[new_acc++] = acc[v];
                        }
                    }
                    acc_count = new_acc;

                    /* Update physical layout to match PROJECT output: the
                     * live acc columns, in order, with no duplicates. */
                    phys_count = 0;
                    for (uint32_t v = 0; v < acc_count; v++)
                        phys_names[phys_count++] = acc[v];

                    /* Recalculate join keys for parent join using PROJECTED acc.
                     * After projection, column indices change, so join keys must
                     * reference the projected columns in the PROJECT output. */
                    free_join_keys(joins[i + 1]);
                    uint32_t rscount;
                    char **rsvars = scan_vars(scans[i + 2], &rscount);
                    /* Use projected acc (current acc after shrinking) which has
                     * correct indices for the columns in the PROJECT output */
                    jpp_setup_join_keys(acc, acc_count, rsvars, rscount,
                        joins[i + 1]);

                    projections++;
                } else {
                    wl_ir_node_free(proj);
                }
            }
        }

        free(needed);

        /* Merge next scan's vars into acc and physical layout for the next
         * iteration.  phys_names gets ALL scan columns (join-key duplicates
         * included); acc gets only new (deduplicated) names. */
        if (i + 2 < nscan) {
            uint32_t scount;
            char **svars = scan_vars(scans[i + 2], &scount);
            for (uint32_t j = 0; j < scount; j++) {
                phys_names[phys_count++] = svars[j];
                if (svars[j] && !var_in_set(svars[j], acc, acc_count))
                    acc[acc_count++] = svars[j];
            }
        }
    }

    free(phys_names);
    free(acc);
    free(scans);
    free(joins);
    return projections;
}

/* ======================================================================== */
/* Internal: optimize a single join chain                                   */
/* ======================================================================== */

typedef struct {
    bool reordered;
    uint32_t projections_inserted;
} jpp_chain_result_t;

static jpp_chain_result_t
optimize_chain(wirelog_ir_node_t *join_root, char **head_vars,
    uint32_t head_var_count, const char *const *idb_names, uint32_t idb_count)
{
    jpp_chain_result_t result = { false, 0 };

    if (!join_root || join_root->type != WIRELOG_IR_JOIN)
        return result;

    /* Count depth */
    uint32_t depth = 0;
    {
        wirelog_ir_node_t *n = join_root;
        while (n && n->type == WIRELOG_IR_JOIN) {
            depth++;
            n = n->child_count > 0 ? n->children[0] : NULL;
        }
    }

    uint32_t nscan = depth + 1;
    if (nscan < 3)
        return result; /* Two-atom chains don't benefit from reordering */

    /* Collect SCAN leaves and JOIN nodes */
    wirelog_ir_node_t **scans
        = (wirelog_ir_node_t **)calloc(nscan, sizeof(wirelog_ir_node_t *));
    wirelog_ir_node_t **joins
        = (wirelog_ir_node_t **)calloc(depth, sizeof(wirelog_ir_node_t *));
    uint32_t *order = (uint32_t *)calloc(nscan, sizeof(uint32_t));
    if (!scans || !joins || !order) {
        free(scans);
        free(joins);
        free(order);
        return result;
    }

    uint32_t actual_scans = collect_scans(join_root, scans, nscan);
    uint32_t actual_joins = collect_joins(join_root, joins, depth);
    (void)actual_joins;

    if (actual_scans != nscan) {
        /* Not a clean left-deep chain; skip */
        free(scans);
        free(joins);
        free(order);
        return result;
    }

    /* Compute greedy ordering */
    bool changed = greedy_order(scans, nscan, order, idb_names, idb_count);

    if (changed) {
        /* Build ordered scan array */
        wirelog_ir_node_t **ordered
            = (wirelog_ir_node_t **)calloc(nscan, sizeof(wirelog_ir_node_t *));
        if (ordered) {
            for (uint32_t i = 0; i < nscan; i++)
                ordered[i] = scans[order[i]];
            rebuild_chain(joins, depth, ordered, nscan);
            free(ordered);
        }
        result.reordered = true;
    }

    /* Intermediate column projection elimination (Issue #191).
     * Called AFTER join reordering so projection decisions reflect the
     * final (possibly reordered) scan/join structure.
     */
    result.projections_inserted
        = insert_projections(join_root, head_vars, head_var_count);

    free(scans);
    free(joins);
    free(order);

    return result;
}

/* ======================================================================== */
/* Internal: find join root through wrapper nodes                           */
/* ======================================================================== */

/*
 * Descend through PROJECT, FLATMAP, FILTER, and ANTIJOIN wrappers
 * to find the JOIN chain root.
 */
static wirelog_ir_node_t *
find_join_chain(wirelog_ir_node_t *node)
{
    while (node) {
        switch (node->type) {
        case WIRELOG_IR_PROJECT:
        case WIRELOG_IR_FLATMAP:
        case WIRELOG_IR_FILTER:
        case WIRELOG_IR_ANTIJOIN:
            if (node->child_count > 0) {
                node = node->children[0];
                continue;
            }
            return NULL;
        case WIRELOG_IR_JOIN:
            return node;
        default:
            return NULL;
        }
    }
    return NULL;
}

/* ======================================================================== */
/* Internal: optimize a single IR tree (recurse into UNION children)        */
/* ======================================================================== */

static void
optimize_tree(wirelog_ir_node_t *ir, uint32_t *chains_examined,
    uint32_t *joins_reordered, uint32_t *projections_inserted,
    const char *const *idb_names, uint32_t idb_count)
{
    if (!ir)
        return;

    /* UNION: recurse into each child */
    if (ir->type == WIRELOG_IR_UNION) {
        for (uint32_t i = 0; i < ir->child_count; i++) {
            optimize_tree(ir->children[i], chains_examined, joins_reordered,
                projections_inserted, idb_names, idb_count);
        }
        return;
    }

    /* Find the join chain root through wrappers */
    wirelog_ir_node_t *root = find_join_chain(ir);
    if (!root)
        return;

    (*chains_examined)++;

    /* Collect head variables from wrapper nodes */
    char *head_vars[64];
    uint32_t head_var_count = collect_head_vars(ir, head_vars, 64);

    jpp_chain_result_t result
        = optimize_chain(root, head_vars, head_var_count, idb_names, idb_count);

    if (result.reordered)
        (*joins_reordered)++;
    *projections_inserted += result.projections_inserted;
}

/* ======================================================================== */
/* Public API                                                               */
/* ======================================================================== */

int
wl_jpp_apply(struct wirelog_program *prog, wl_jpp_stats_t *stats)
{
    if (!prog)
        return -2;

    if (!prog->relation_irs) {
        if (stats) {
            stats->joins_reordered = 0;
            stats->projections_inserted = 0;
            stats->chains_examined = 0;
        }
        return 0;
    }

    uint32_t chains_examined = 0;
    uint32_t joins_reordered = 0;
    uint32_t projections_inserted = 0;

    /* Build de-duplicated IDB relation name list for EDB tie-breaking.
     * A relation is IDB iff it appears as the head of at least one rule. */
    const char **idb_names = NULL;
    uint32_t idb_count = 0;
    if (prog->rule_count > 0) {
        idb_names
            = (const char **)calloc(prog->rule_count, sizeof(const char *));
        if (idb_names) {
            for (uint32_t i = 0; i < prog->rule_count; i++) {
                const char *h = prog->rules[i].head_relation;
                if (!h)
                    continue;
                bool dup = false;
                for (uint32_t j = 0; j < idb_count; j++) {
                    if (idb_names[j] && strcmp(idb_names[j], h) == 0) {
                        dup = true;
                        break;
                    }
                }
                if (!dup)
                    idb_names[idb_count++] = h;
            }
        }
    }

    for (uint32_t i = 0; i < prog->relation_count; i++) {
        optimize_tree(prog->relation_irs[i], &chains_examined, &joins_reordered,
            &projections_inserted, idb_names, idb_count);
    }

    free(idb_names);

    if (stats) {
        stats->joins_reordered = joins_reordered;
        stats->projections_inserted = projections_inserted;
        stats->chains_examined = chains_examined;
    }

    return 0;
}
