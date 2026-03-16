/*
 * magic_sets.h - Magic Sets Demand-Driven Optimization Pass
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Implements Magic Sets as a source-to-source IR transformation.
 * Adds magic "demand" relations and rules that restrict evaluation
 * to only the tuples reachable from the query, reducing intermediate
 * result sizes for recursive programs.
 *
 * Pipeline position: runs after SIP, before plan generation.
 *
 *   Parse -> IR -> Stratify -> Fusion -> JPP -> SIP -> Magic Sets
 *         -> Re-stratify -> Plan Gen -> Backend
 *
 * Magic relation naming: "$m$<RelationName>_<adornment>"
 *   e.g. "$m$Path_bf" for Path with 1st arg bound, 2nd free.
 *
 * All-free optimization: when all demand roots have all-free adornment
 * (the default for .output relations with no .query directives), no
 * magic relations are generated and the pass is a no-op.
 */

#ifndef WIRELOG_PASSES_MAGIC_SETS_H
#define WIRELOG_PASSES_MAGIC_SETS_H

#include <stdint.h>
#include <stdbool.h>

struct wirelog_program;

/* ======================================================================== */
/* Statistics                                                               */
/* ======================================================================== */

/**
 * wl_magic_sets_stats_t:
 *
 * Statistics from a single Magic Sets pass invocation.
 *
 * @demand_roots:           Relations identified as demand roots.
 * @adorned_predicates:     Unique (relation, adornment) pairs with bound_mask != 0.
 * @magic_rules_generated:  Magic demand propagation rules added.
 * @original_rules_modified: Original rules with magic guards inserted.
 * @skipped_all_free:       Adorned predicates skipped (all-free adornment).
 */
typedef struct {
    uint32_t demand_roots;
    uint32_t adorned_predicates;
    uint32_t magic_rules_generated;
    uint32_t original_rules_modified;
    uint32_t skipped_all_free;
} wl_magic_sets_stats_t;

/* ======================================================================== */
/* Demand Specification (for explicit query demands)                        */
/* ======================================================================== */

/**
 * wl_magic_demand_t:
 *
 * Specifies a demand (query constraint) for a relation.
 *
 * @relation_name: Name of the demanded relation.
 * @bound_mask:    Bit i = 1 if position i is bound (query-constrained).
 *                 0 = all-free (no restriction, optimization skips this).
 * @arity:         Number of columns. 0 = auto-detect from program.
 */
typedef struct {
    const char *relation_name;
    uint64_t bound_mask;
    uint32_t arity;
} wl_magic_demand_t;

/* ======================================================================== */
/* Public API                                                               */
/* ======================================================================== */

/**
 * wl_magic_sets_apply:
 * @prog:  Program to transform (modified in-place).
 * @stats: (out) (nullable): Pass statistics.
 *
 * Apply Magic Sets transformation using .output and .printsize relations
 * as demand roots with all-free adornment.
 *
 * With all-free adornment (the default), the all-free optimization
 * skips magic relation generation entirely, making this a no-op.
 * Magic Sets only activates when explicit .query directives specify
 * bound positions (future: parser support for .query).
 *
 * Must be called AFTER: fusion, JPP, SIP.
 * Caller must call wl_ir_program_rebuild_relation_irs() and
 * wl_ir_stratify_program() after this if magic_sets_applied is true.
 *
 * Returns:
 *    0: Success.
 *   -1: Memory allocation error.
 *   -2: Invalid program (NULL).
 */
int
wl_magic_sets_apply(struct wirelog_program *prog, wl_magic_sets_stats_t *stats);

/**
 * wl_magic_sets_apply_with_demands:
 * @prog:         Program to transform (modified in-place).
 * @demands:      Array of demand specifications.
 * @demand_count: Number of demands.
 * @stats:        (out) (nullable): Pass statistics.
 *
 * Apply Magic Sets transformation with explicit demand specifications.
 * Demands with bound_mask == 0 are skipped (all-free optimization).
 *
 * Used for testing and for future .query directive support.
 *
 * Returns:
 *    0: Success.
 *   -1: Memory allocation error.
 *   -2: Invalid program (NULL).
 */
int
wl_magic_sets_apply_with_demands(struct wirelog_program *prog,
                                 const wl_magic_demand_t *demands,
                                 uint32_t demand_count,
                                 wl_magic_sets_stats_t *stats);

#endif /* WIRELOG_PASSES_MAGIC_SETS_H */
