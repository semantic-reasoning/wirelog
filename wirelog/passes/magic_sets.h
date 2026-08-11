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
 * @adorned_predicates:     Unique (relation, adornment) pairs with bound_mask
 *                         != 0 that survived the guard-viability closure --
 *                         that is, the ones a magic relation was created for.
 *                         A pair the closure dropped is counted only by
 *                         @unrestrictable_relations (#1027).
 * @magic_rules_generated:  Magic demand propagation rules added.
 * @original_rules_modified: Original rules with magic guards inserted.
 * @skipped_all_free:       Demand *roots* skipped because the demand itself is
 *                         all-free (bound_mask == 0), the default for a
 *                         .output relation with no bound .query.  A Phase 1
 *                         event and a genuine optimisation skip: there is no
 *                         restriction to propagate, so the pass declines and
 *                         the program is left as written.  Nothing is lost.
 *                         An all-free IDB *body occurrence* is a different
 *                         event and is counted by @unrestrictable_relations
 *                         below; it used to share this counter, which
 *                         conflated a no-op with a correctness constraint.
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
 * @unrestrictable_relations: Relations excluded from the transformation
 *                         entirely by the guard-viability closure (#1027).
 *                         Seeded by an IDB body occurrence that binds nothing
 *                         -- such an occurrence needs every tuple of the
 *                         relation, and this pass cannot hand one occurrence
 *                         an unguarded copy, because insert_magic_guard()
 *                         rewrites the rule root in place and one set of rules
 *                         serves every adornment.  Closed under "R unguarded
 *                         implies every IDB R reads is unguarded", since an
 *                         unguarded R evaluated against a guarded child has
 *                         its own fixpoint cut.  Every member gets no magic
 *                         relation, no demand rule and no guard.
 *                         This is an over-approximation, not a lost-answers
 *                         count.  It is seeded from an occurrence's binding
 *                         pattern alone, and a binding pattern does not decide
 *                         whether the occurrence contributes anything for the
 *                         seeded demand -- a rule whose body is empty for that
 *                         demand is guarded soundly and completely despite
 *                         reading its relation all-free.  The closure has no
 *                         way to tell those apart, so it excludes both.  What
 *                         the counter reports is how much of the program the
 *                         pass declined to optimise in order to stay correct.
 *
 *                         That cost is not small, and its dominant trigger has
 *                         a name: a rule with a constant in the bound head
 *                         position, `X(1, y) :- ...` under `.query X(b, f)`.
 *                         get_head_vars() yields no variable for a constant
 *                         position, so Phase 2's `bound` set starts empty,
 *                         so *every* IDB in that rule's body binds nothing and
 *                         seeds the closure, and the transitive step unguards
 *                         the whole subprogram reachable from there.  One
 *                         character does it: changing `X(a, b) :- p(a, b),
 *                         q(a, b).` to `X(1, b) :- p(a, b), q(a, b).` takes
 *                         the same program from 4 adorned predicates and 4
 *                         guards to 2 and 1.
 *
 *                         Measured over a 342-program census on which the pass
 *                         with and without this closure are both sound and
 *                         query-complete: the unguarded oracle derives 1785
 *                         rows, the pass without the closure 820, and with it
 *                         1085 -- so about a third of the pruning is given
 *                         back.  It derives more than the pass without it on
 *                         94 of the 342, roughly one program in five, which is
 *                         not a corner case.  A separate sample found the
 *                         constant-in-bound-head shape in almost every program
 *                         the closure made lossier; that sample and the 342
 *                         census are different runs and their counts do not
 *                         combine, so take the shape as the dominant trigger
 *                         and the 94 as the rate.
 *
 *                         Seeds also cover IDBs behind unguarded
 *                         .output/.printsize consumers, IDBs under negation
 *                         (#1047), and IDBs feeding aggregate rules (#1048).
 *                         The closure is conservative: every such relation
 *                         and every positive IDB dependency it reads is left
 *                         unrestricted.
 *
 * The two skip counters describe a rule the pass declined to guard.  That is
 * sound *in isolation* but is not a "sound but unoptimized" outcome in
 * general: an unrestricted rule that reads a relation the pass did guard is
 * reading a partial relation, which is unsound under negation (#1047) and
 * wrong-valued under aggregation (#1048).  Correctness comes from the whole
 * assignment of guards being consistent, not from any one rule's skip; the
 * guard-viability closure above is what enforces that for the shapes it can
 * see, and the two issues named are the shapes it cannot.
 *
 * They are the only signal that a demand did not reach a rule -- but only for
 * a caller that supplies a stats struct.  The library's own pipeline passes
 * NULL (api_facade.c), so in a shipping build nothing reads them; they are
 * observability for tests and embedders, not a runtime warning.  A rule
 * missing from both counters is not necessarily guarded either: a rule of a
 * relation in @unrestrictable_relations is unguarded and is counted by none of
 * the three, because Phase 4b never walks it.  Being guarded is in turn not
 * the same as being correctly restricted: a guard prunes only what its demand
 * relation was populated with, so a rule guarded on a demand no propagation
 * rule ever fills loses answers instead of saving work.  That is exactly the
 * failure #1027 fixed, and the closure now removes the relation from the
 * transformation rather than guarding it on a demand that stays empty.  The
 * two skip counters are kept apart because they are reported at different
 * points, not because the split is clean: as noted above, one capability gap
 * is currently counted on the policy side.
 *
 * @original_rules_modified counts guards actually inserted.  A rule counted in
 * either skip counter is not counted there, and neither is a rule of a
 * relation in @unrestrictable_relations -- Phase 4b never reaches it.
 *
 * The pass can also decline outright, leaving the program exactly as written:
 * that happens when the guard-viability closure cannot be completed, which
 * today means a rule with more than MS_MAX_ATOMS body atoms somewhere under an
 * unguardable relation.  It returns 0 with `magic_sets_applied` still false and
 * both @adorned_predicates and @unrestrictable_relations at 0, and warns on
 * stderr.  Declining is deliberate: the closure walks rules Phase 2 never
 * reached, so it is the first thing in the pass able to trip over such a rule
 * in a subprogram the demand never touched, and returning an error there would
 * surface through api_facade.c as WIRELOG_ERR_MEMORY and fail
 * wirelog_optimize() for a program that optimized before.
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
    uint32_t unrestrictable_relations;
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
