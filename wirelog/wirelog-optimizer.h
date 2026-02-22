/*
 * wirelog-optimizer.h - wirelog Optimizer API
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.  If not, see
 * <https://www.gnu.org/licenses/lgpl-3.0.html>.
 */

#ifndef WIRELOG_OPTIMIZER_H
#define WIRELOG_OPTIMIZER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdbool.h>

/* ======================================================================== */
/* Optimization Pass Types                                                  */
/* ======================================================================== */

/**
 * wirelog_opt_pass_t:
 *
 * Optimization pass identifiers
 */
typedef enum {
    WIRELOG_OPT_LOGIC_FUSION = 0,      /* Fuse Join+Map+Filter operations */
    WIRELOG_OPT_JOIN_PROJECT_PLAN = 1, /* Optimize join ordering and projection */
    WIRELOG_OPT_SEMIJOIN = 2,          /* Apply semijoin pre-filtering */
    WIRELOG_OPT_SUBPLAN_SHARING = 3,   /* Share common subplans (CTEs) */
    WIRELOG_OPT_BOOLEAN_SPEC = 4,      /* Specialize Boolean operations */
} wirelog_opt_pass_t;

/**
 * wirelog_opt_config_t:
 *
 * Optimizer configuration
 */
typedef struct {
    bool enable_logic_fusion;
    bool enable_join_ordering;
    bool enable_semijoin;
    bool enable_subplan_sharing;
    bool enable_boolean_spec;
    uint32_t max_join_search_space;  /* Limit join ordering search space */
    bool debug_trace;                /* Enable debug output */
} wirelog_opt_config_t;

/* ======================================================================== */
/* Optimization Statistics                                                  */
/* ======================================================================== */

/**
 * wirelog_opt_stats_t:
 *
 * Optimization statistics
 */
typedef struct {
    uint32_t original_node_count;    /* IR nodes before optimization */
    uint32_t optimized_node_count;   /* IR nodes after optimization */
    uint32_t passes_applied;         /* Number of optimization passes run */
    uint32_t fusion_count;           /* Number of fusions performed */
    uint32_t join_reorders;          /* Number of join reorderings */
    double optimization_time_ms;     /* Time spent in optimization (ms) */
} wirelog_opt_stats_t;

/* ======================================================================== */
/* Optimization API                                                         */
/* ======================================================================== */

/**
 * wirelog_optimizer_get_default_config:
 *
 * Get the default optimizer configuration.
 *
 * Returns: Default configuration (all optimizations enabled)
 */
wirelog_opt_config_t
wirelog_optimizer_get_default_config(void);

/**
 * wirelog_optimize:
 * @program: Program to optimize
 * @error: (out) (optional): Error code
 *
 * Apply all enabled optimizations to a program.
 *
 * Optimizations applied (Phase 0+):
 * 1. Logic Fusion: Merge Join+Map+Filter into FlatMap
 * 2. Join-Project Plan: Determine optimal join order
 * 3. Semijoin Information Passing: Pre-filter with semijoins
 * 4. Subplan Sharing: Share common sub-expressions
 * 5. Boolean Specialization: Optimize diff field encoding
 *
 * Returns: true on success, false on error
 */
bool
wirelog_optimize(void *program, int *error);

/**
 * wirelog_optimize_with_config:
 * @program: Program to optimize
 * @config: Optimizer configuration
 * @error: (out) (optional): Error code
 *
 * Apply optimizations using custom configuration.
 *
 * Returns: true on success, false on error
 */
bool
wirelog_optimize_with_config(void *program,
                            const wirelog_opt_config_t *config,
                            int *error);

/**
 * wirelog_optimize_apply_pass:
 * @program: Program to optimize
 * @pass: Specific optimization pass to apply
 * @error: (out) (optional): Error code
 *
 * Apply a single optimization pass.
 *
 * Useful for selective optimization or debugging.
 *
 * Returns: true on success, false on error
 */
bool
wirelog_optimize_apply_pass(void *program,
                           wirelog_opt_pass_t pass,
                           int *error);

/* ======================================================================== */
/* Optimization Analysis                                                    */
/* ======================================================================== */

/**
 * wirelog_optimizer_get_stats:
 * @program: Optimized program
 * @stats: (out): Optimization statistics
 *
 * Get statistics about applied optimizations.
 *
 * Returns: true if stats were collected, false otherwise
 */
bool
wirelog_optimizer_get_stats(const void *program, wirelog_opt_stats_t *stats);

/**
 * wirelog_optimizer_debug_print:
 * @program: Program to debug
 *
 * Print detailed optimizer debug information.
 *
 * Output goes to stderr. Includes:
 * - Original IR structure
 * - Optimized IR structure
 * - Cost model decisions
 * - Join ordering rationale
 */
void
wirelog_optimizer_debug_print(const void *program);

/**
 * wirelog_optimizer_cost_estimate:
 * @program: Program to analyze
 *
 * Estimate the computational cost of evaluating the program.
 *
 * Returns: Estimated cost (arbitrary units), or 0 if not available
 */
uint64_t
wirelog_optimizer_cost_estimate(const void *program);

#ifdef __cplusplus
}
#endif

#endif /* WIRELOG_OPTIMIZER_H */
