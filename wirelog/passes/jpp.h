/*
 * jpp.h - Join-Project Plan Optimization Pass
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 *
 * JPP reorders multi-atom joins to minimize intermediate result sizes
 * and inserts intermediate projections to drop unneeded columns early.
 *
 * The pass operates in-place on the program's merged relation IR trees,
 * following the same pattern as Logic Fusion (fusion.c).
 *
 * Algorithm:
 *   1. Walk each relation's IR tree to find left-deep JOIN chains
 *   2. Extract SCAN leaves with their variable sets
 *   3. Greedy reorder: pick the next atom that maximizes shared
 *      variables with the accumulated result (avoids cross-products)
 *   4. Rebuild the JOIN chain with recalculated join keys
 *   5. Insert intermediate PROJECT nodes to drop columns not needed
 *      by subsequent joins or the head projection
 */

#ifndef WIRELOG_PASSES_JPP_H
#define WIRELOG_PASSES_JPP_H

#include <stdint.h>

/* Forward declarations */
struct wirelog_program;
struct wirelog_ir_node;

/* ======================================================================== */
/* JPP Statistics                                                            */
/* ======================================================================== */

/**
 * wl_jpp_stats_t:
 *
 * Statistics collected during a JPP pass.
 *
 * @joins_reordered:       Number of join chains that were reordered.
 * @projections_inserted:  Number of intermediate PROJECT nodes inserted.
 * @chains_examined:       Total join chains examined.
 */
typedef struct {
    uint32_t joins_reordered;
    uint32_t projections_inserted;
    uint32_t chains_examined;
} wl_jpp_stats_t;

/* ======================================================================== */
/* JPP API (internal)                                                       */
/* ======================================================================== */

/**
 * wl_jpp_apply:
 * @prog: Program whose relation IR trees will be optimized in-place.
 * @stats: (out) (optional): JPP statistics.  May be NULL.
 *
 * Apply Join-Project Plan optimization to every merged relation IR tree.
 * Left-deep JOIN chains are reordered to minimize cross-products by
 * maximizing shared join variables at each step.
 *
 * Returns:
 *    0: Success (stats populated if non-NULL).
 *   -1: Memory allocation error.
 *   -2: Invalid program (NULL).
 */
int
wl_jpp_apply(struct wirelog_program *prog, wl_jpp_stats_t *stats);

#endif /* WIRELOG_PASSES_JPP_H */
