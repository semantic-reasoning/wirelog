/*
 * fusion.h - Logic Fusion Optimization Pass
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 *
 * Logic Fusion merges adjacent FILTER + PROJECT nodes into a single
 * FLATMAP node.  This reduces intermediate materializations and maps
 * directly to Differential Dataflow's flat_map() operator.
 *
 * Fusion patterns (applied bottom-up on each IR tree):
 *
 *   Pattern 1 - Filter+Project:
 *     PROJECT(FILTER(child))  ->  FLATMAP(child)
 *       with: filter_expr from FILTER, project_indices from PROJECT
 *
 *   Pattern 2 - Project-only (identity filter):
 *     PROJECT(child)  ->  FLATMAP(child)
 *       with: filter_expr = NULL, project_indices from PROJECT
 *       (only when child is SCAN or JOIN -- avoids double-fusion)
 *
 * The pass operates in-place on the program's merged relation IR trees.
 * Node counts before/after are returned via wl_fusion_stats_t for
 * observability.
 */

#ifndef WIRELOG_PASSES_FUSION_H
#define WIRELOG_PASSES_FUSION_H

#include <stdint.h>

/* Forward declarations */
struct wirelog_program;
struct wirelog_ir_node;

/* ======================================================================== */
/* Fusion Statistics                                                         */
/* ======================================================================== */

/**
 * wl_fusion_stats_t:
 *
 * Statistics collected during a fusion pass.
 *
 * @nodes_before:       Total IR nodes before fusion.
 * @nodes_after:        Total IR nodes after fusion.
 * @fusions_applied:    Number of FLATMAP fusions performed.
 */
typedef struct {
    uint32_t nodes_before;
    uint32_t nodes_after;
    uint32_t fusions_applied;
} wl_fusion_stats_t;

/* ======================================================================== */
/* Fusion API (internal)                                                    */
/* ======================================================================== */

/**
 * wl_fusion_apply:
 * @prog: Program whose relation IR trees will be optimized in-place.
 * @stats: (out) (optional): Fusion statistics.  May be NULL.
 *
 * Apply Logic Fusion to every merged relation IR tree in the program.
 * Trees are walked bottom-up; matching patterns are replaced with
 * FLATMAP nodes.
 *
 * Returns:
 *    0: Success (stats populated if non-NULL).
 *   -1: Memory allocation error.
 *   -2: Invalid program (NULL).
 */
int
wl_fusion_apply(struct wirelog_program *prog, wl_fusion_stats_t *stats);

/**
 * wl_fusion_count_nodes:
 * @node: Root of an IR subtree.
 *
 * Count the total number of nodes in an IR tree (recursive).
 *
 * Returns: Node count (0 if node is NULL).
 */
uint32_t
wl_fusion_count_nodes(const struct wirelog_ir_node *node);

#endif /* WIRELOG_PASSES_FUSION_H */
