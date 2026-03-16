/*
 * subsumption.h - Type Hierarchy Subsumption Optimization
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Removes subsumed rules from type hierarchies (e.g., SubtypeOf in DOOP).
 * Detects when a rule is logically subsumed by another and eliminates the
 * redundant rule, reducing tuple generation without affecting semantics.
 *
 * Example (DOOP SubtypeOf):
 *   SubtypeOf(s, s) :- isClassType(s).     [subsumed by next rule]
 *   SubtypeOf(t, t) :- isType(t).          [general case]
 *
 * Since isClassType(X) ⊆ isType(X), the first rule is redundant.
 *
 * Pipeline position: runs early after Stratify, before JPP.
 *
 *   Parse -> IR -> Stratify -> SUBSUMPTION -> Fusion -> JPP -> ...
 *
 * Focuses on DOOP workload type hierarchy optimization.
 */

#ifndef WIRELOG_SUBSUMPTION_H
#define WIRELOG_SUBSUMPTION_H

#include <stdint.h>

struct wirelog_program;

/**
 * wl_subsumption_stats_t:
 *
 * Statistics from subsumption optimization pass.
 *
 * @rules_examined:  Number of rules analyzed.
 * @rules_removed:   Number of rules eliminated as subsumed.
 * @relations:       Number of relations optimized.
 */
typedef struct {
    uint32_t rules_examined;
    uint32_t rules_removed;
    uint32_t relations;
} wl_subsumption_stats_t;

/**
 * wl_subsumption_apply:
 * @prog:  (borrow): Program to optimize. Must not be NULL.
 * @stats: (out) (nullable): If non-NULL, receives pass statistics.
 *
 * Detect and remove subsumed rules from type hierarchy relations.
 * Specifically targets DOOP SubtypeOf and similar relations where
 * predicate hierarchy enables rule subsumption elimination.
 *
 * Operates in-place on the program's IR.
 *
 * Returns:
 *    0: Success (possibly zero removals).
 *   -2: Invalid input (NULL program).
 */
int
wl_subsumption_apply(struct wirelog_program *prog,
                     wl_subsumption_stats_t *stats);

#endif /* WIRELOG_SUBSUMPTION_H */
