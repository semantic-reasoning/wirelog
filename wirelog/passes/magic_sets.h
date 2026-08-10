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
 * @arity_mismatch_skipped: Demands skipped due to adornment count/arity mismatch.
 * @skipped_aggregate:     Aggregate rules skipped because Magic Sets cannot
 *                         safely insert magic guards into them yet.
 * @skipped_constant_head: Rules left unguarded because no bound head position
 *                         holds a variable the guard could be keyed on.
 *                         Read the name loosely.  A literal constant in the
 *                         bound position is the common case and is a genuine
 *                         policy skip -- the rule is well-formed and the pass
 *                         declines.  But an arithmetic head expression in the
 *                         bound position (`p(x + 1, y) :- ...` under
 *                         `.query p(b, f)`) also lands here, and that is
 *                         neither a constant nor a decision: the pass simply
 *                         cannot key a guard on a computed head column yet.
 *                         The counter conflates the two.  Only the *bound*
 *                         positions are read, so `p(x, y + 1)` under the same
 *                         query is guarded normally -- position 0 is the plain
 *                         variable x.  Before #990 an arithmetic head in a
 *                         bound position reached this only when unfused, and
 *                         counted as an unsupported head when fused; now it
 *                         counts here either way, which is at least
 *                         consistent, but still mislabeled.
 * @skipped_unsupported_head: Rules left unguarded because the pass found no
 *                         head variable names to key the guard on.  A
 *                         *capability* gap.  Two shapes reach it, and the
 *                         counter does not separate them: a head wider than
 *                         the 64-bit adornment mask (more than 64 columns),
 *                         and a root carrying no head projection at all.
 *                         Since #990 a rule fused to a FLATMAP root -- once
 *                         the common way to get here -- keeps its
 *                         project_exprs and is guarded normally.  An AGGREGATE
 *                         root cannot reach here either:
 *                         relation_has_aggregate_rule() filters such relations
 *                         before adornment, in Phase 1 and again in the Phase
 *                         2 BFS, so they never enter `processed` and the guard
 *                         loop never sees them.  The wide head is what remains
 *                         reachable in practice.
 *
 * Both skip counters describe a rule that is evaluated unrestricted, which is
 * sound but unoptimized.  They are the only signal that a demand did not reach
 * it -- but only for a caller that supplies a stats struct.  The library's own
 * pipeline passes NULL (api_facade.c), so in a shipping build nothing reads
 * them; they are observability for tests and embedders, not a runtime warning.
 * A rule missing from both counters is guarded, which is not the same as
 * correctly restricted: a guard prunes only what its demand relation was
 * populated with, so a rule guarded on a demand no propagation rule ever fills
 * loses answers instead of saving work (#1027).  Guarding is complete only
 * when the demand actually propagates; these counters measure the first half.
 * They are kept apart because they are reported at different points, not
 * because the split is clean: as noted above, one capability gap is currently
 * counted on the policy side.
 *
 * @original_rules_modified counts guards actually inserted.  A rule counted in
 * either skip counter is not counted there.
 */
typedef struct {
    uint32_t demand_roots;
    uint32_t adorned_predicates;
    uint32_t magic_rules_generated;
    uint32_t original_rules_modified;
    uint32_t skipped_all_free;
    uint32_t arity_mismatch_skipped;
    uint32_t skipped_aggregate;
    uint32_t skipped_constant_head;
    uint32_t skipped_unsupported_head;
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
 * The public optimizer defers parsed bound `.query` directives until their
 * seed values can be supplied.  Explicit callers may use this lower-level
 * entry point and populate the generated demand relations themselves.
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
 * Used for testing and for explicit callers that provide demand seeds.
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
