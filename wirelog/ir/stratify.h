/*
 * stratify.h - wirelog Stratification & SCC Detection
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * INTERNAL HEADER - not installed, not part of public API.
 * Provides dependency graph construction, Tarjan's SCC detection,
 * and stratification for Datalog programs with negation.
 *
 * ========================================================================
 * Algorithm Overview
 * ========================================================================
 *
 * Stratification partitions a Datalog program's rules into ordered layers
 * (strata) such that negated or aggregated relations are fully computed
 * before any rule that depends on them through negation or aggregation.
 *
 * Pipeline:
 *   1. Dependency Graph Construction
 *      - Walk each rule's IR tree to extract dependency edges.
 *      - SCAN nodes produce POSITIVE edges (head depends on scanned relation).
 *      - ANTIJOIN children[1] produce NEGATION edges.
 *      - AGGREGATE children produce AGGREGATION edges.
 *      - Only IDB relations (those appearing as rule heads) become nodes.
 *      - EDB-only relations (input facts, no defining rules) are excluded.
 *
 *   2. SCC Detection (Tarjan's Algorithm, Iterative)
 *      - Finds strongly connected components in the dependency graph.
 *      - Uses an explicit call stack instead of recursion (embedded-safe).
 *      - Builds an adjacency list for O(V+E) traversal.
 *      - SCCs are numbered such that sink nodes (no outgoing dependencies
 *        to other SCCs) receive lower IDs, matching execution order.
 *
 *   3. Stratifiability Validation
 *      - A program is NOT stratifiable if a NEGATION edge exists within
 *        a single SCC (negation cycle: A depends on !B, B depends on A).
 *      - Self-negation (p :- !p) is a special case detected here.
 *      - AGGREGATION validation is deferred to a future pass.
 *
 *   4. Stratum Assignment
 *      - Each SCC becomes one stratum.
 *      - Stratum IDs follow the SCC numbering (lower ID = computed earlier).
 *      - Rules are assigned to their head relation's stratum.
 *      - 0-rule programs produce 1 empty stratum (>= 1 contract).
 *
 * Complexity: O(V + E) time and space, where V = number of IDB relations
 * and E = number of dependency edges.
 *
 * ========================================================================
 * Stratifiability Rules
 * ========================================================================
 *
 * A Datalog program is stratifiable if and only if:
 *   - No negation cycle exists: for every NEGATION edge (A -> !B),
 *     A and B must be in DIFFERENT strongly connected components.
 *   - Equivalently: negation is only permitted across strata, not within.
 *
 * Examples:
 *   STRATIFIABLE:
 *     p(x) :- base(x).
 *     r(x) :- base(x), !p(x).     -- p and r in different SCCs
 *
 *   NOT STRATIFIABLE:
 *     p(x) :- base(x), !q(x).
 *     q(x) :- base(x), !p(x).     -- p and q in same SCC (cycle)
 *
 *   NOT STRATIFIABLE (self-negation):
 *     p(x) :- base(x), !p(x).     -- p negates itself
 *
 * ========================================================================
 * Usage (internal)
 * ========================================================================
 *
 * Called by wirelog_parse_string() after rule conversion:
 *
 *   int rc = wl_program_stratify(program);
 *   if (rc == -2) { ... not stratifiable ... }
 *   if (rc == -1) { ... memory error ... }
 *   // rc == 0: program->strata populated, program->is_stratified = true
 *
 * For unit testing, the dependency graph and SCC APIs can be used directly:
 *
 *   wl_dep_graph_t *g = wl_dep_graph_build(prog);
 *   wl_scc_result_t *scc = wl_scc_detect(g);
 *   // inspect g->edges, scc->scc_id, etc.
 *   wl_scc_free(scc);
 *   wl_dep_graph_free(g);
 */

#ifndef WIRELOG_STRATIFY_H
#define WIRELOG_STRATIFY_H

#include "../wirelog-types.h"

#include <stdbool.h>
#include <stdint.h>

/* Forward declaration — stratify must NOT include parser/ast.h */
struct wirelog_program;

/* ======================================================================== */
/* Dependency Edge Types                                                    */
/* ======================================================================== */

/**
 * wl_dep_type_t:
 *
 * Edge types in the dependency graph, representing how one relation
 * depends on another within a Datalog rule.
 *
 * WL_DEP_POSITIVE:    Standard positive dependency. The head relation
 *                     references the body relation through a SCAN node.
 *                     Example: r(x) :- a(x).  produces  r --POSITIVE--> a
 *
 * WL_DEP_NEGATION:    Negated dependency. The head relation references
 *                     the body relation through an ANTIJOIN node.
 *                     Example: r(x) :- a(x), !b(x).  produces  r --NEGATION--> b
 *                     Negation within an SCC makes the program unstratifiable.
 *
 * WL_DEP_AGGREGATION: Aggregation dependency. The head relation aggregates
 *                     over the body relation through an AGGREGATE node.
 *                     Example: r(x, min(d)) :- s(x, d).  produces  r --AGGREGATION--> s
 *                     Validation deferred to a future pass.
 */
typedef enum {
    WL_DEP_POSITIVE,    /* A depends on B (positive body atom) */
    WL_DEP_NEGATION,    /* A depends on !B (negation) */
    WL_DEP_AGGREGATION, /* A aggregates over B (future validation) */
} wl_dep_type_t;

/* ======================================================================== */
/* Dependency Graph                                                         */
/* ======================================================================== */

/**
 * wl_dep_edge_t:
 *
 * Single directed edge in the dependency graph.
 *
 * @from: Graph-local index of the head (defining) relation.
 *        Maps to program->relations[graph->relation_map[from]].
 * @to:   Graph-local index of the body (referenced) relation.
 *        Maps to program->relations[graph->relation_map[to]].
 * @type: How the head depends on the body (positive, negation, aggregation).
 */
typedef struct {
    uint32_t from;
    uint32_t to;
    wl_dep_type_t type;
} wl_dep_edge_t;

/**
 * wl_dep_graph_t:
 *
 * Dependency graph for IDB (intensional database) relations.
 *
 * Design decisions:
 * - Uses indices into program->relations rather than duplicating names.
 * - Only IDB relations (those appearing as rule heads) are graph nodes.
 * - EDB-only relations (input facts with no defining rules) are excluded.
 * - Edges between IDB and EDB relations are not stored (EDB has no
 *   graph node index). This is correct for stratification since EDB
 *   relations are always available before any stratum executes.
 *
 * @relation_count: Number of IDB relations (graph nodes).
 * @relation_map:   relation_map[i] = index into program->relations.
 *                  Allows name lookup: program->relations[relation_map[i]].name
 * @edges:          Array of dependency edges.
 * @edge_count:     Number of edges currently stored.
 * @edge_capacity:  Allocated capacity of edges array.
 */
typedef struct {
    uint32_t relation_count;
    uint32_t *relation_map;

    wl_dep_edge_t *edges;
    uint32_t edge_count;
    uint32_t edge_capacity;
} wl_dep_graph_t;

/* ======================================================================== */
/* SCC Detection Result                                                     */
/* ======================================================================== */

/**
 * wl_scc_result_t:
 *
 * Result of Tarjan's SCC detection.
 *
 * @scc_id:    Array of size graph->relation_count.
 *             scc_id[i] = SCC number that graph node i belongs to.
 *             Lower SCC IDs correspond to components that should be
 *             computed earlier (sink nodes in the condensation DAG).
 * @scc_count: Total number of distinct SCCs found.
 *             Equal to the number of strata in the stratified program.
 */
typedef struct {
    uint32_t *scc_id;
    uint32_t scc_count;
} wl_scc_result_t;

/* ======================================================================== */
/* Internal API                                                             */
/* ======================================================================== */

/**
 * wl_dep_graph_build:
 * @prog: Program with converted rules (rule IR trees must exist)
 *
 * Build a dependency graph from the program's rule IR trees.
 * Walks each rule's IR tree recursively to find SCAN, ANTIJOIN, and
 * AGGREGATE nodes, creating corresponding POSITIVE, NEGATION, and
 * AGGREGATION edges.
 *
 * Only IDB relations (those appearing as rule heads) become graph nodes.
 * Dependencies on EDB-only relations are silently ignored since EDB
 * is always available before any stratum executes.
 *
 * Returns: (transfer full): Dependency graph, or NULL if:
 *          - prog is NULL
 *          - prog has 0 rules
 *          - No IDB relations found
 *          - Memory allocation failed
 */
wl_dep_graph_t *
wl_dep_graph_build(const struct wirelog_program *prog);

/**
 * wl_dep_graph_free:
 * @graph: (transfer full): Graph to free (NULL-safe)
 *
 * Free a dependency graph and all associated memory.
 */
void
wl_dep_graph_free(wl_dep_graph_t *graph);

/**
 * wl_scc_detect:
 * @graph: Dependency graph (must not be NULL, relation_count > 0)
 *
 * Detect strongly connected components using iterative Tarjan's algorithm.
 * Uses an explicit call stack instead of recursion for embedded-target safety.
 * Builds an adjacency list for O(V+E) traversal.
 *
 * SCC numbering: sink components (no outgoing edges to other SCCs in the
 * condensation DAG) receive lower IDs. This directly maps to execution
 * order — stratum 0 has no IDB dependencies and executes first.
 *
 * Returns: (transfer full): SCC result, or NULL on error
 */
wl_scc_result_t *
wl_scc_detect(const wl_dep_graph_t *graph);

/**
 * wl_scc_free:
 * @result: (transfer full): SCC result to free (NULL-safe)
 *
 * Free an SCC result and its scc_id array.
 */
void
wl_scc_free(wl_scc_result_t *result);

/**
 * wl_program_stratify:
 * @program: Program to stratify (modified in place)
 *
 * Full stratification pipeline:
 *   1. Build dependency graph from rule IR trees
 *   2. Detect SCCs using iterative Tarjan's algorithm
 *   3. Map SCC IDs to stratum IDs (direct mapping, no reversal needed)
 *   4. Validate: negation within SCC = not stratifiable
 *   5. Assign strata: build wirelog_stratum_t array with rule names
 *
 * On success, populates program->strata, program->stratum_count,
 * and sets program->is_stratified = true.
 *
 * Special cases:
 *   - 0-rule programs: produces 1 empty stratum (honors >= 1 contract)
 *   - Programs with only EDB dependencies: 1 stratum per IDB relation
 *
 * Returns:
 *    0: Success. program->strata populated.
 *   -1: Memory allocation error.
 *   -2: Not stratifiable (negation cycle detected within an SCC).
 *        The program contains mutual negation that cannot be resolved
 *        by any ordering of strata.
 */
int
wl_program_stratify(struct wirelog_program *program);

#endif /* WIRELOG_STRATIFY_H */
