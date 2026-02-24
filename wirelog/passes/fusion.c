/*
 * fusion.c - Logic Fusion Optimization Pass
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Fuses adjacent FILTER + PROJECT IR nodes into a single FLATMAP node.
 * Operates in-place on the program's merged relation IR trees.
 *
 * Fusion pattern (bottom-up):
 *   PROJECT(FILTER(child))  ->  FLATMAP(child)
 *     filter_expr  := FILTER.filter_expr  (transferred, not copied)
 *     project_indices := PROJECT.project_indices  (kept in place)
 *     project_count   := PROJECT.project_count
 *
 * Memory ownership:
 *   relation_irs[] entries that are NOT UNION are aliases to
 *   rules[].ir_root (same pointer).  We must NOT free the PROJECT
 *   node; instead we mutate it in-place to FLATMAP.  Only the
 *   intermediate FILTER shell is freed (after detaching all owned
 *   data).
 */

#include "fusion.h"
#include "../ir/ir.h"
#include "../ir/program.h"

#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Node Counting                                                            */
/* ======================================================================== */

uint32_t
wl_fusion_count_nodes(const struct wirelog_ir_node *node)
{
    if (!node)
        return 0;
    uint32_t count = 1;
    for (uint32_t i = 0; i < node->child_count; i++) {
        count += wl_fusion_count_nodes(node->children[i]);
    }
    return count;
}

static uint32_t
count_all_nodes(const struct wirelog_program *prog)
{
    uint32_t total = 0;
    if (!prog || !prog->relation_irs)
        return 0;
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        total += wl_fusion_count_nodes(prog->relation_irs[i]);
    }
    return total;
}

/* ======================================================================== */
/* Fusion: in-place tree rewriting                                          */
/* ======================================================================== */

/*
 * try_fuse_node:
 *
 * Attempt to fuse the subtree rooted at *node_ptr.
 * Applies bottom-up: recurse into children first, then check the
 * current node for the PROJECT(FILTER(child)) pattern.
 *
 * When a match is found the PROJECT node is mutated in-place to
 * FLATMAP (preserving its identity / pointer).  The FILTER shell
 * is freed after transferring its filter_expr.
 *
 * Returns the number of fusions performed in this subtree.
 */
static uint32_t
try_fuse_node(wirelog_ir_node_t **node_ptr)
{
    if (!node_ptr || !*node_ptr)
        return 0;

    wirelog_ir_node_t *node = *node_ptr;
    uint32_t fusions = 0;

    /* Bottom-up: fuse children first */
    for (uint32_t i = 0; i < node->child_count; i++) {
        fusions += try_fuse_node(&node->children[i]);
    }

    /*
     * Pattern match: PROJECT(FILTER(child))
     *
     * node               = PROJECT  (project_indices, project_count)
     * node->children[0]  = FILTER   (filter_expr)
     * filter->children[0]= child    (SCAN, JOIN, etc.)
     */
    if (node->type == WIRELOG_IR_PROJECT && node->child_count == 1
        && node->children[0] && node->children[0]->type == WIRELOG_IR_FILTER
        && node->children[0]->child_count == 1) {

        wirelog_ir_node_t *filter = node->children[0];
        wirelog_ir_node_t *child = filter->children[0];

        /*
         * In-place mutation: convert PROJECT -> FLATMAP.
         * The node pointer is preserved so rules[].ir_root stays valid.
         */
        node->type = WIRELOG_IR_FLATMAP;
        /* project_indices and project_count stay in place (already on node) */

        /* Transfer filter_expr from FILTER to this node (move) */
        node->filter_expr = filter->filter_expr;
        filter->filter_expr = NULL;

        /* Re-point this node's child from FILTER to the grandchild */
        node->children[0] = child;

        /* Free the empty FILTER shell.
         * Detach its child first so wl_ir_node_free doesn't recurse. */
        filter->children[0] = NULL;
        filter->child_count = 0;
        /* relation_name on FILTER is typically NULL but clean up if set */
        wl_ir_node_free(filter);

        fusions++;
    }

    return fusions;
}

/* ======================================================================== */
/* Public API                                                               */
/* ======================================================================== */

int
wl_fusion_apply(struct wirelog_program *prog, wl_fusion_stats_t *stats)
{
    if (!prog)
        return -2;

    if (!prog->relation_irs) {
        /* No IR trees to optimize -- success with 0 fusions */
        if (stats) {
            stats->nodes_before = 0;
            stats->nodes_after = 0;
            stats->fusions_applied = 0;
        }
        return 0;
    }

    uint32_t nodes_before = count_all_nodes(prog);
    uint32_t total_fusions = 0;

    /* Walk each relation's merged IR tree */
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (prog->relation_irs[i]) {
            total_fusions += try_fuse_node(&prog->relation_irs[i]);
        }
    }

    uint32_t nodes_after = count_all_nodes(prog);

    if (stats) {
        stats->nodes_before = nodes_before;
        stats->nodes_after = nodes_after;
        stats->fusions_applied = total_fusions;
    }

    return 0;
}
