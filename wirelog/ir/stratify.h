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
 * Single edge in the dependency graph.
 * from/to are indices into the graph's relation_map.
 */
typedef struct {
    uint32_t from; /* Index of head relation in graph node array */
    uint32_t to;   /* Index of body relation in graph node array */
    wl_dep_type_t type;
} wl_dep_edge_t;

/**
 * wl_dep_graph_t:
 *
 * Dependency graph for IDB relations.
 * Uses indices into program->relations (no name duplication).
 * EDB-only relations (no rules defining them) do NOT appear as nodes.
 */
typedef struct {
    uint32_t relation_count; /* Number of IDB relations (rule heads only) */
    uint32_t *relation_map;  /* relation_map[i] = index into program->relations */

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
 * scc_id[i] = which SCC graph node i belongs to.
 */
typedef struct {
    uint32_t *scc_id;   /* scc_id[i] = SCC number for relation i */
    uint32_t scc_count; /* Total number of SCCs */
} wl_scc_result_t;

/* ======================================================================== */
/* Internal API                                                             */
/* ======================================================================== */

/**
 * wl_dep_graph_build:
 * @prog: Program with converted rules
 *
 * Build a dependency graph from the program's rule IR trees.
 * Only IDB relations (those appearing as rule heads) become graph nodes.
 *
 * Returns: (transfer full): Dependency graph, or NULL if no rules/memory error
 */
wl_dep_graph_t *
wl_dep_graph_build(const struct wirelog_program *prog);

/**
 * wl_dep_graph_free:
 * @graph: (transfer full): Graph to free (NULL-safe)
 */
void
wl_dep_graph_free(wl_dep_graph_t *graph);

/**
 * wl_scc_detect:
 * @graph: Dependency graph
 *
 * Detect strongly connected components using iterative Tarjan's algorithm.
 * SCCs are numbered in reverse topological order (Tarjan's natural output).
 *
 * Returns: (transfer full): SCC result, or NULL on error
 */
wl_scc_result_t *
wl_scc_detect(const wl_dep_graph_t *graph);

/**
 * wl_scc_free:
 * @result: (transfer full): SCC result to free (NULL-safe)
 */
void
wl_scc_free(wl_scc_result_t *result);

/**
 * wl_program_stratify:
 * @program: Program to stratify (modified in place)
 *
 * Full stratification pipeline:
 *   1. Build dependency graph
 *   2. Detect SCCs (Tarjan's iterative)
 *   3. Topological sort (reverse Tarjan output)
 *   4. Validate (negation within SCC = not stratifiable)
 *   5. Assign strata
 *
 * Returns: 0 = success, -1 = memory error, -2 = not stratifiable (negation cycle)
 */
int
wl_program_stratify(struct wirelog_program *program);

#endif /* WIRELOG_STRATIFY_H */
