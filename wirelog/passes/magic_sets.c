/*
 * magic_sets.c - Magic Sets Demand-Driven Optimization Pass
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Implements Magic Sets as a source-to-source IR transformation.
 *
 * Algorithm overview:
 *  Phase 1: Identify demand roots (from explicit demands or .output relations).
 *  Phase 2: BFS to compute adorned program (which relations need magic guards).
 *  Phase 2b: Guard-viability closure -- drop the relations that must stay
 *            unguarded, and everything they read (#1027).
 *  Phase 3: Create magic relations ($m$<name>_<adornment>).
 *  Phase 4: Generate demand propagation rules and insert magic guards.
 *
 * Each magic relation $m$P_bf captures "which values of P's bound arguments
 * are currently demanded". A magic guard in rule P's body ensures only
 * demanded tuples are derived, pruning the fixpoint computation.
 *
 * References:
 *  - Bancilhon et al. (1986), "Magic Sets and Other Strange Ways to
 *    Implement Logic Programs"
 *  - Architecture: .omc/plans/magic_sets_architecture.md
 */

#include "magic_sets.h"
#include "../ir/ir.h"
#include "../ir/program.h"
#include "../util/log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

/* ======================================================================== */
/* Internal Limits                                                          */
/* ======================================================================== */

#define MS_MAX_ATOMS 64    /* max body atoms per rule */
#define MS_MAX_ADORNED 512 /* max adorned predicates */
#define MS_MAX_VARS 128    /* max bound variables tracked per rule */

/* ======================================================================== */
/* Variable Set (tracks bound variable names)                               */
/* ======================================================================== */

typedef struct {
    const char *vars[MS_MAX_VARS]; /* borrowed pointers from IR nodes */
    uint32_t count;
    bool overflow;
} ms_varset_t;

static void
varset_clear(ms_varset_t *vs)
{
    vs->count = 0;
    vs->overflow = false;
}

static bool
varset_contains(const ms_varset_t *vs, const char *var)
{
    if (!var)
        return false;
    for (uint32_t i = 0; i < vs->count; i++) {
        if (vs->vars[i] && strcmp(vs->vars[i], var) == 0)
            return true;
    }
    return false;
}

static void
varset_add(ms_varset_t *vs, const char *var)
{
    if (!var || varset_contains(vs, var))
        return;
    if (vs->count >= MS_MAX_VARS) {
        vs->overflow = true;
        return;
    }
    vs->vars[vs->count++] = var;
}

/* ======================================================================== */
/* Atom Info (shallow view into an IR SCAN node)                           */
/* ======================================================================== */

typedef struct {
    const char *rel_name;   /* borrowed */
    const char **col_names; /* borrowed array from SCAN node */
    uint32_t col_count;
} ms_atom_t;

/* ======================================================================== */
/* Adorned Predicate Set                                                    */
/* ======================================================================== */

typedef struct {
    const char *rel_name; /* borrowed from the program IR */
    uint64_t bound_mask;
    uint32_t arity;
} ms_adorned_t;

typedef struct {
    ms_adorned_t *items;
    uint32_t count;
    bool overflow;
} ms_adorned_set_t;

static bool
adorned_set_init(ms_adorned_set_t *s)
{
    memset(s, 0, sizeof(*s));
    s->items = (ms_adorned_t *)calloc(MS_MAX_ADORNED, sizeof(*s->items));
    return s->items != NULL;
}

static void
adorned_set_free(ms_adorned_set_t *s)
{
    free(s->items);
    s->items = NULL;
    s->count = 0;
}

static bool
adorned_contains(const ms_adorned_set_t *s, const char *name, uint64_t mask)
{
    for (uint32_t i = 0; i < s->count; i++) {
        if (s->items[i].bound_mask == mask
            && strcmp(s->items[i].rel_name, name) == 0)
            return true;
    }
    return false;
}

/* Returns true if newly added, false if already present or table full. */
static bool
adorned_add(ms_adorned_set_t *s, const char *name, uint64_t mask,
    uint32_t arity)
{
    if (adorned_contains(s, name, mask))
        return false;
    if (s->count >= MS_MAX_ADORNED) {
        s->overflow = true;
        return false;
    }
    s->items[s->count].rel_name = name;
    s->items[s->count].bound_mask = mask;
    s->items[s->count].arity = arity;
    s->count++;
    return true;
}

/* ======================================================================== */
/* Unrestrictable Relation Set (Issue #1027)                                */
/* ======================================================================== */

/*
 * Relations this pass must leave entirely unguarded.
 *
 * Textbook Magic Sets answers "one occurrence needs all of R, another needs
 * only the demanded part" by splitting R into separate adorned predicates.
 * This pass does not split: insert_magic_guard() mutates the rule root in
 * place, so one set of rules serves every adornment of R.  Under that
 * architecture the requirement collapses to all-or-nothing -- if any
 * occurrence needs R unrestricted, R must not be guarded at all.
 *
 * Membership is by name.  Both sites that add to the set do so only under
 * is_idb(), so every member is the head relation of some rule, and the set can
 * therefore never hold more distinct names than the program has rules.  That
 * bound is an invariant of this file -- it holds because of the two is_idb()
 * guards a few lines away -- which is why the capacity is sized from
 * prog->rule_count rather than from prog->relation_count.  Sizing it from the
 * relation table would instead rest on what the parser chooses to put there,
 * which is not this pass's to promise.
 */
typedef struct {
    const char **names; /* borrowed from the program IR */
    uint32_t count;
    uint32_t capacity;
} ms_relset_t;

static bool
relset_init(ms_relset_t *s, uint32_t capacity)
{
    memset(s, 0, sizeof(*s));
    s->capacity = (capacity > 0) ? capacity : 1;
    s->names = (const char **)calloc(s->capacity, sizeof(*s->names));
    return s->names != NULL;
}

static void
relset_free(ms_relset_t *s)
{
    /* Explicit cast: the array holds `const char *` elements, and an implicit
     * `const char ** -> void *` conversion is a clang-tidy finding
     * (bugprone-multi-level-implicit-pointer-conversion).  Same convention as
     * exec_plan_gen.c's free() calls on borrowed-name arrays. */
    free((void *)s->names);
    s->names = NULL;
    s->count = 0;
    s->capacity = 0;
}

static bool
relset_contains(const ms_relset_t *s, const char *name)
{
    if (!name)
        return false;
    for (uint32_t i = 0; i < s->count; i++) {
        if (s->names[i] && strcmp(s->names[i], name) == 0)
            return true;
    }
    return false;
}

/*
 * Returns false only if the set is full, which the capacity bound above makes
 * unreachable: the caller sizes it at prog->rule_count + 1 and every name
 * added is a rule head.  The branch is kept rather than asserted because the
 * consequence of a silent drop would be a relation guarded that must not be,
 * i.e. lost answers; callers turn a false return into a declined pass.
 */
static bool
relset_add(ms_relset_t *s, const char *name)
{
    if (!name || relset_contains(s, name))
        return true;
    if (s->count >= s->capacity)
        return false;
    s->names[s->count++] = name;
    return true;
}

/* ======================================================================== */
/* Magic Relation Name                                                      */
/* ======================================================================== */

/*
 * Generate "$m$<rel>_<adornment_string>".
 * Returns heap-allocated string (caller must free).
 */
static char *
make_magic_name(const char *rel, uint64_t mask, uint32_t arity)
{
    uint32_t n = (arity < 64) ? arity : 64;
    char adorn[65];
    for (uint32_t i = 0; i < n; i++)
        adorn[i] = (mask & (1ULL << i)) ? 'b' : 'f';
    adorn[n] = '\0';

    /* "$m$" (3) + rel + "_" (1) + adorn (n) + NUL (1) */
    size_t len = 3 + strlen(rel) + 1 + n + 1;
    char *name = (char *)malloc(len);
    if (name)
        snprintf(name, len, "$m$%s_%s", rel, adorn);
    return name;
}

/* ======================================================================== */
/* IDB Detection                                                            */
/* ======================================================================== */

static bool
is_idb(const struct wirelog_program *prog, const char *rel_name)
{
    if (!rel_name)
        return false;
    for (uint32_t i = 0; i < prog->rule_count; i++) {
        if (prog->rules[i].head_relation
            && strcmp(prog->rules[i].head_relation, rel_name) == 0)
            return true;
    }
    return false;
}

/*
 * Whether any rule defining @rel_name has an AGGREGATE root.
 *
 * The type test is the right one here and must stay: this asks what kind of
 * rule it is, not what the root's payload holds.  It is also load-bearing in a
 * way that is not local -- since #990, get_head_vars() no longer names
 * AGGREGATE, and returns 0 for such a root only because an AGGREGATE root
 * carries no project_exprs.  This function is therefore the sole thing keeping
 * aggregate rules out of the guard loop; it is called before adornment in
 * Phase 1 and again in the Phase 2 BFS, so such relations never enter
 * `processed`.  Widening AGGREGATE to carry a head projection would need this
 * to still hold.
 */
static bool
relation_has_aggregate_rule(const struct wirelog_program *prog,
    const char *rel_name)
{
    if (!prog || !rel_name)
        return false;
    for (uint32_t i = 0; i < prog->rule_count; i++) {
        if (prog->rules[i].head_relation
            && strcmp(prog->rules[i].head_relation, rel_name) == 0
            && prog->rules[i].ir_root
            && prog->rules[i].ir_root->type == WIRELOG_IR_AGGREGATE)
            return true;
    }
    return false;
}

static uint32_t
get_arity(const struct wirelog_program *prog, const char *rel_name)
{
    if (!rel_name)
        return 0;
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (prog->relations[i].name
            && strcmp(prog->relations[i].name, rel_name) == 0)
            return prog->relations[i].column_count;
    }
    return 0;
}

/* ======================================================================== */
/* Body Atom Collection (DFS left-to-right, respecting negation)           */
/* ======================================================================== */

static void
collect_scans_r(const wirelog_ir_node_t *node, ms_atom_t *atoms,
    uint32_t *count, bool *overflow)
{
    if (!node)
        return;
    if (*count >= MS_MAX_ATOMS) {
        *overflow = true;
        return;
    }

    switch (node->type) {
    case WIRELOG_IR_SCAN:
    case WIRELOG_IR_COMPOUND_INLINE:
    case WIRELOG_IR_COMPOUND_SIDE:
        atoms[*count].rel_name = node->relation_name;
        atoms[*count].col_names = (const char **)node->column_names;
        atoms[*count].col_count = node->column_count;
        (*count)++;
        break;

    case WIRELOG_IR_ANTIJOIN:
    case WIRELOG_IR_SEMIJOIN:
        /*
         * ANTIJOIN: only positive (left) child; demand does NOT flow
         *           through negation (right child).
         * SEMIJOIN: only left child; right is a clone used for
         *           filtering only, not a real body atom.
         */
        if (node->child_count > 0)
            collect_scans_r(node->children[0], atoms, count, overflow);
        break;

    default:
        for (uint32_t i = 0; i < node->child_count; i++)
            collect_scans_r(node->children[i], atoms, count, overflow);
        break;
    }
}

/*
 * Collect body atoms from the rule's IR tree.
 * The body is child[0] of the PROJECT/AGGREGATE root.
 * Returns the number of atoms collected.
 */
static uint32_t
collect_body_atoms(const wirelog_ir_node_t *ir_root, ms_atom_t *atoms,
    bool *overflow)
{
    if (!ir_root || ir_root->child_count == 0)
        return 0;
    uint32_t count = 0;
    collect_scans_r(ir_root->children[0], atoms, &count, overflow);
    return count;
}

/* Collect every relation scan, including scans below an anti-join.  This is
 * deliberately separate from collect_scans_r(): demand propagation must not
 * cross negation, while guard viability must see the complete relation a
 * negated or aggregate consumer reads. */
static void
collect_all_scans_r(const wirelog_ir_node_t *node, ms_atom_t *atoms,
    uint32_t *count, bool *overflow)
{
    if (!node)
        return;
    if (*count >= MS_MAX_ATOMS) {
        *overflow = true;
        return;
    }

    if (node->type == WIRELOG_IR_SCAN
        || node->type == WIRELOG_IR_COMPOUND_INLINE
        || node->type == WIRELOG_IR_COMPOUND_SIDE) {
        atoms[*count].rel_name = node->relation_name;
        atoms[*count].col_names = (const char **)node->column_names;
        atoms[*count].col_count = node->column_count;
        (*count)++;
        return;
    }

    for (uint32_t i = 0; i < node->child_count; i++)
        collect_all_scans_r(node->children[i], atoms, count, overflow);
}

/* Find scans used as the right-hand side of any anti-join.  The left side is
 * still traversed because it can contain another nested anti-join. */
static void
collect_negated_scans_r(const wirelog_ir_node_t *node, ms_atom_t *atoms,
    uint32_t *count, bool *overflow)
{
    if (!node)
        return;

    if (node->type == WIRELOG_IR_ANTIJOIN) {
        if (node->child_count > 0)
            collect_negated_scans_r(node->children[0], atoms, count,
                overflow);
        if (node->child_count > 1)
            collect_all_scans_r(node->children[1], atoms, count, overflow);
        return;
    }

    for (uint32_t i = 0; i < node->child_count; i++)
        collect_negated_scans_r(node->children[i], atoms, count, overflow);
}

static bool
seed_unrestrictable_scans(const struct wirelog_program *prog,
    const wirelog_ir_node_t *root, bool all_scans, ms_relset_t *u)
{
    if (!root || root->child_count == 0)
        return true;

    ms_atom_t atoms[MS_MAX_ATOMS];
    uint32_t count = 0;
    bool overflow = false;
    if (all_scans)
        collect_all_scans_r(root->children[0], atoms, &count, &overflow);
    else
        collect_negated_scans_r(root->children[0], atoms, &count, &overflow);
    if (overflow)
        return false;

    for (uint32_t i = 0; i < count; i++) {
        if (atoms[i].rel_name && is_idb(prog, atoms[i].rel_name)
            && !relset_add(u, atoms[i].rel_name))
            return false;
    }
    return true;
}

/* Seed consumers whose semantics require a complete producer relation. */
static bool
seed_guard_viability_roots(const struct wirelog_program *prog,
    const wl_magic_demand_t *demands, uint32_t demand_count, ms_relset_t *u)
{
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        const wl_ir_relation_info_t *rel = &prog->relations[i];
        bool explicitly_restricted = false;
        for (uint32_t di = 0; di < demand_count; di++) {
            if (demands[di].relation_name
                && strcmp(demands[di].relation_name, rel->name) == 0
                && demands[di].bound_mask != 0) {
                explicitly_restricted = true;
                break;
            }
        }
        if ((rel->has_output || rel->has_printsize)
            && !explicitly_restricted && is_idb(prog, rel->name)
            && !relset_add(u, rel->name))
            return false;
    }

    for (uint32_t i = 0; i < prog->rule_count; i++) {
        const wirelog_ir_node_t *root = prog->rules[i].ir_root;
        if (!root)
            continue;

        /* A negated relation must be complete before anti-join evaluation. */
        if (!seed_unrestrictable_scans(prog, root, false, u))
            return false;

        /* Aggregate heads are intentionally not adorned.  Their inputs must
         * nevertheless be complete or the aggregate value is computed over a
         * demand-restricted subset. */
        if (root->type == WIRELOG_IR_AGGREGATE
            && !seed_unrestrictable_scans(prog, root, true, u))
            return false;
    }
    return true;
}

/*
 * Close @u under "R unguarded implies every IDB R reads is unguarded".
 *
 * @u arrives seeded with every relation whose IDB body occurrence bound
 * nothing at the Phase 2 site.  Such an occurrence needs the whole relation,
 * and the pass cannot give it an unguarded copy, so the relation must be left
 * unguarded outright (see ms_relset_t).
 *
 * Being unguarded is contagious.  An unguarded R is evaluated in full, so
 * every IDB in R's rule bodies must be evaluated in full too -- otherwise R's
 * own fixpoint is cut through the guarded child and the answers are lost one
 * level down instead of at R.  Hence the closure.
 *
 * The separate seed walker above handles negated and aggregate consumers
 * before this transitive pass runs.  Consequently this function only needs
 * to follow ordinary positive IDB dependencies from each already-unsafe
 * relation; demand propagation continues to use collect_body_atoms().
 * Keeping those walkers separate is important: a negated relation must be
 * complete for soundness, but no demand may propagate through the negated
 * atom.
 *
 * Terminates: the bound is re-read each iteration but relset_add() appends a
 * name at most once, so each relation is dequeued exactly once.
 *
 * Returns false if the closure could not be completed -- a rule with more than
 * MS_MAX_ATOMS body atoms, or a full @u -- in which case the caller must not
 * rely on @u and declines the transformation.
 */
static bool
close_unrestrictable(const struct wirelog_program *prog, ms_relset_t *u)
{
    for (uint32_t ui = 0; ui < u->count; ui++) {
        const char *rel = u->names[ui];

        for (uint32_t ri = 0; ri < prog->rule_count; ri++) {
            if (!prog->rules[ri].head_relation
                || strcmp(prog->rules[ri].head_relation, rel) != 0)
                continue;

            const wirelog_ir_node_t *ir_root = prog->rules[ri].ir_root;
            if (!ir_root)
                continue;

            ms_atom_t atoms[MS_MAX_ATOMS];
            bool atom_overflow = false;
            uint32_t atom_count = collect_body_atoms(ir_root, atoms,
                    &atom_overflow);
            if (atom_overflow) {
                fprintf(stderr,
                    "warning: magic sets declined: a rule with more than %u "
                    "body atoms is reachable from an unguardable relation\n",
                    MS_MAX_ATOMS);
                return false;
            }

            for (uint32_t ai = 0; ai < atom_count; ai++) {
                if (!atoms[ai].rel_name || !is_idb(prog, atoms[ai].rel_name)
                    || relation_has_aggregate_rule(prog, atoms[ai].rel_name))
                    continue;
                if (!relset_add(u, atoms[ai].rel_name))
                    return false;
            }
        }
    }
    return true;
}

/*
 * Same-relation multiple adornments are not safe under this pass's current
 * one-rule-set-per-relation rewrite (#995).  Inserting one guard per
 * adornment conjoins demands that should be unioned, so downgrade the relation
 * to the same all-or-nothing path used by guard viability before closure.
 */
static bool
seed_multi_adornment_relations(const struct wirelog_program *prog,
    const ms_adorned_set_t *processed, ms_relset_t *u, uint32_t *count)
{
    if (count)
        *count = 0;

    for (uint32_t i = 0; i < processed->count; i++) {
        const ms_adorned_t *ap = &processed->items[i];
        if (!ap->rel_name || ap->bound_mask == 0
            || !is_idb(prog, ap->rel_name))
            continue;

        bool already_checked = false;
        for (uint32_t j = 0; j < i; j++) {
            if (processed->items[j].rel_name
                && strcmp(processed->items[j].rel_name, ap->rel_name) == 0) {
                already_checked = true;
                break;
            }
        }
        if (already_checked)
            continue;

        bool found_different_mask = false;
        for (uint32_t j = i + 1; j < processed->count; j++) {
            const ms_adorned_t *other = &processed->items[j];
            if (!other->rel_name || other->bound_mask == 0
                || strcmp(other->rel_name, ap->rel_name) != 0)
                continue;
            if (other->bound_mask != ap->bound_mask) {
                found_different_mask = true;
                break;
            }
        }

        if (found_different_mask) {
            if (count)
                (*count)++;
            if (!relset_add(u, ap->rel_name))
                return false;
        }
    }

    return true;
}

/*
 * The value domain of variable @var as bound somewhere under @node.
 *
 * Used to type the magic guard SCAN, whose columns hold the same values as
 * the body variables they are named after.
 *
 * The guard SCAN is joined in as the RIGHT child (Issue #989), so its
 * columns come *last* in the rule's column layout, after the body's.  Every
 * guard column is named after a bound head variable, which by construction
 * also names an earlier body column, and collect_output_columns() /
 * serialize_expr() both resolve a name to its first match -- so a comparison
 * on a bound variable now resolves against the body's column, not the
 * guard's.  These types are therefore unreachable by name.  They remain
 * reachable positionally: col_ctx_lookup_type() falls back to parsing "colN"
 * against the concatenated layout, whose tail is the guard.  Leaving them
 * untyped would silently return a string comparison landing there to
 * comparing intern ids (Issue #962), so they are still filled in.
 *
 * First match wins, which is the same rule collect_output_columns() and
 * serialize_expr() use to resolve a name to a column.
 */
static wl_ir_coltype_t
ms_lookup_var_type(const wirelog_ir_node_t *node, const char *var)
{
    if (!node || !var)
        return WL_IR_COLTYPE_UNKNOWN;

    if (node->column_names && node->column_types) {
        for (uint32_t i = 0; i < node->column_count; i++) {
            if (node->column_names[i]
                && strcmp(node->column_names[i], var) == 0)
                return node->column_types[i];
        }
    }
    for (uint32_t i = 0; i < node->child_count; i++) {
        wl_ir_coltype_t t = ms_lookup_var_type(node->children[i], var);
        if (t != WL_IR_COLTYPE_UNKNOWN)
            return t;
    }
    return WL_IR_COLTYPE_UNKNOWN;
}

/* ======================================================================== */
/* Head Variable Extraction                                                 */
/* ======================================================================== */

/*
 * Extract variable names from the rule's head projection.
 * vars[i] = variable name at head position i, or NULL for constants/wildcards.
 * Returns the head arity (number of head arguments).
 *
 * Returns 0 for AGGREGATE rules, but only as a consequence of the test below:
 * an AGGREGATE root carries project_count == 0 and project_exprs == NULL, so
 * it has no head projection to read.  Nothing here names the type.  If an
 * AGGREGATE root ever grows project_exprs, this returns its head variables
 * and the pass will guard aggregate rules -- see the note on
 * relation_has_aggregate_rule(), which is what actually keeps them out.
 *
 * Also returns 0 for a head wider than @max_vars, which is a different thing:
 * there the head projection is present and readable, and the pass declines it
 * because the adornment mask is 64 bits wide.  Callers see one 0 for both.
 */
static uint32_t
get_head_vars(const wirelog_ir_node_t *ir_root, const char **vars,
    uint32_t max_vars)
{
    if (!ir_root)
        return 0;

    /*
     * Key off the head projection payload, not the node type (Issue #990).
     *
     * A rule root does not keep the type it was parsed with.  Logic Fusion
     * rewrites PROJECT -> FLATMAP in place (fusion.c), leaving project_exprs /
     * project_count untouched, and it runs before Magic Sets in the shipping
     * pipeline -- so testing for WIRELOG_IR_PROJECT silently skipped every
     * filtered rule, losing answers.  Nor is fusion the only rewriter:
     * program.c re-types SCAN -> COMPOUND_INLINE/_SIDE the same way.
     * Enumerating root types is a losing game; the payload is what this
     * function actually needs.
     *
     * This is the existing convention, not a new one: exec_plan_gen.c decides
     * a node projects with literally `node->project_exprs &&
     * node->project_count > 0`, keying off the pointer rather than the type.
     *
     * Safe against the one PROJECT built without project_exprs -- jpp.c's
     * dead-variable projection -- because that one is spliced strictly
     * between JOINs in a chain and never becomes a rule root.
     */
    if (ir_root->project_exprs && ir_root->project_count > 0) {
        if (ir_root->project_count > max_vars) {
            fprintf(stderr,
                "warning: magic sets skipped rule with %u head columns; "
                "the 64-bit adornment mask supports at most %u\n",
                ir_root->project_count, max_vars);
            return 0;
        }
        uint32_t n = (ir_root->project_count < max_vars)
                         ? ir_root->project_count
                         : max_vars;
        for (uint32_t i = 0; i < n; i++) {
            if (ir_root->project_exprs && ir_root->project_exprs[i]
                && ir_root->project_exprs[i]->type == WL_IR_EXPR_VAR)
                vars[i] = ir_root->project_exprs[i]->var_name;
            else
                vars[i] = NULL;
        }
        return n;
    }

    /* No head projection to read variable names from.  Two shapes reach here:
     * an AGGREGATE root, excluded upstream by relation_has_aggregate_rule(),
     * and a zero-arity head (`.decl p()`), which parses to a PROJECT with
     * project_count == 0 and has no position a demand could bind.  The
     * over-wide head above returns 0 without reaching this line, so a 0
     * return does not imply this path. */
    return 0;
}

/* ======================================================================== */
/* Popcount                                                                 */
/* ======================================================================== */

static uint32_t
popcount64(uint64_t x)
{
    uint32_t n = 0;
    while (x) {
        n += (uint32_t)(x & 1u);
        x >>= 1;
    }
    return n;
}

/* ======================================================================== */
/* Build Demand Propagation Rule IR                                         */
/* ======================================================================== */

/*
 * Build the IR tree for a magic demand propagation rule:
 *
 *   $m$Bj_adj(bound_args_of_Bj) :-
 *       $m$P_adorn(guard_bound_vars),
 *       prefix_atoms[0](...),
 *       ...,
 *       prefix_atoms[prefix_count-1](...).
 *
 * The prefix atoms are the body atoms before Bj that bind variables
 * needed to determine which values of Bj are demanded.
 *
 * Returns: (transfer full) IR tree, or NULL on error.
 */
static wirelog_ir_node_t *
build_demand_rule_ir(const char *body_magic_name, const char *guard_magic_name,
    const char **guard_bound_vars, uint32_t guard_bound_count,
    const ms_atom_t *prefix_atoms, uint32_t prefix_count,
    const ms_atom_t *target_atom, uint64_t target_bound_mask)
{
    /*
     * === Step 1: Start with the guard magic SCAN ===
     *
     * The SCANs built here carry names but no column_types.  A demand rule
     * is only SCAN/JOIN/PROJECT -- it holds no filter or projection
     * expression -- so nothing in it consults a column type.  (The guard
     * inserted into the ORIGINAL rule does, and is typed; see
     * insert_magic_guard.)
     */
    wirelog_ir_node_t *current = wl_ir_node_create(WIRELOG_IR_SCAN);
    if (!current)
        return NULL;
    wl_ir_node_set_relation(current, guard_magic_name);

    if (guard_bound_count > 0) {
        current->column_names
            = (char **)calloc(guard_bound_count, sizeof(char *));
        if (!current->column_names) {
            wl_ir_node_free(current);
            return NULL;
        }
        current->column_count = guard_bound_count;
        for (uint32_t i = 0; i < guard_bound_count; i++) {
            if (guard_bound_vars[i])
                current->column_names[i] = strdup_safe(guard_bound_vars[i]);
            else {
                char constant_name[32];
                snprintf(constant_name, sizeof(constant_name),
                    "$magic_const_%u", i);
                current->column_names[i] = strdup_safe(constant_name);
            }
        }
    }

    /* Track bound variables for join key computation */
    ms_varset_t bound;
    varset_clear(&bound);
    for (uint32_t i = 0; i < guard_bound_count; i++) {
        if (guard_bound_vars[i])
            varset_add(&bound, guard_bound_vars[i]);
    }

    /* === Step 2: JOIN with each prefix atom === */

    for (uint32_t pi = 0; pi < prefix_count; pi++) {
        const ms_atom_t *pa = &prefix_atoms[pi];

        /* Clone the prefix atom's SCAN */
        wirelog_ir_node_t *scan = wl_ir_node_create(WIRELOG_IR_SCAN);
        if (!scan) {
            wl_ir_node_free(current);
            return NULL;
        }
        wl_ir_node_set_relation(scan, pa->rel_name);
        if (pa->col_count > 0) {
            scan->column_names = (char **)calloc(pa->col_count, sizeof(char *));
            if (!scan->column_names) {
                wl_ir_node_free(scan);
                wl_ir_node_free(current);
                return NULL;
            }
            scan->column_count = pa->col_count;
            for (uint32_t ci = 0; ci < pa->col_count; ci++) {
                if (pa->col_names && pa->col_names[ci])
                    scan->column_names[ci] = strdup_safe(pa->col_names[ci]);
            }
        }

        /* Create JOIN(current, scan) with keys on shared bound variables */
        wirelog_ir_node_t *join = wl_ir_node_create(WIRELOG_IR_JOIN);
        if (!join) {
            wl_ir_node_free(scan);
            wl_ir_node_free(current);
            return NULL;
        }

        /* Count shared variables first */
        uint32_t key_count = 0;
        for (uint32_t ci = 0; ci < pa->col_count && ci < 64; ci++) {
            if (pa->col_names && pa->col_names[ci]
                && varset_contains(&bound, pa->col_names[ci]))
                key_count++;
        }

        if (key_count > 0) {
            join->join_left_keys = (char **)calloc(key_count, sizeof(char *));
            join->join_right_keys = (char **)calloc(key_count, sizeof(char *));
            if (!join->join_left_keys || !join->join_right_keys) {
                wl_ir_node_free(join);
                wl_ir_node_free(scan);
                wl_ir_node_free(current);
                return NULL;
            }
            join->join_key_count = key_count;
            uint32_t k = 0;
            for (uint32_t ci = 0; ci < pa->col_count && ci < 64; ci++) {
                if (pa->col_names && pa->col_names[ci]
                    && varset_contains(&bound, pa->col_names[ci])) {
                    join->join_left_keys[k] = strdup_safe(pa->col_names[ci]);
                    join->join_right_keys[k] = strdup_safe(pa->col_names[ci]);
                    k++;
                }
            }
        }

        if (wl_ir_node_add_child(join, current) != 0) {
            wl_ir_node_free(join);
            wl_ir_node_free(scan);
            wl_ir_node_free(current);
            return NULL;
        }
        if (wl_ir_node_add_child(join, scan) != 0) {
            join->children[0] = NULL;
            join->child_count = 0;
            wl_ir_node_free(join);
            wl_ir_node_free(scan);
            wl_ir_node_free(current);
            return NULL;
        }
        current = join;

        /* Add this prefix atom's variables to the bound set */
        for (uint32_t ci = 0; ci < pa->col_count; ci++) {
            if (pa->col_names && pa->col_names[ci])
                varset_add(&bound, pa->col_names[ci]);
        }
    }

    /* === Step 3: PROJECT head outputs bound args of Bj === */

    uint32_t bound_count = popcount64(target_bound_mask);

    wirelog_ir_node_t *root = wl_ir_node_create(WIRELOG_IR_PROJECT);
    if (!root) {
        wl_ir_node_free(current);
        return NULL;
    }
    wl_ir_node_set_relation(root, body_magic_name);
    root->project_count = bound_count;

    if (bound_count > 0) {
        root->project_exprs
            = (wl_ir_expr_t **)calloc(bound_count, sizeof(wl_ir_expr_t *));
        if (!root->project_exprs) {
            wl_ir_node_free(root);
            wl_ir_node_free(current);
            return NULL;
        }
        uint32_t ei = 0;
        for (uint32_t i = 0;
            i < target_atom->col_count && i < 64 && ei < bound_count; i++) {
            if (target_bound_mask & (1ULL << i)) {
                wl_ir_expr_t *e = wl_ir_expr_create(WL_IR_EXPR_VAR);
                if (e && target_atom->col_names && target_atom->col_names[i])
                    e->var_name = strdup_safe(target_atom->col_names[i]);
                root->project_exprs[ei++] = e;
            }
        }
    }

    if (wl_ir_node_add_child(root, current) != 0) {
        wl_ir_node_free(root);
        wl_ir_node_free(current);
        return NULL;
    }
    return root;
}

/* ======================================================================== */
/* Magic Guard Insertion                                                    */
/* ======================================================================== */

/* Outcome of insert_magic_guard(); see the counters in magic_sets.h. */
typedef enum {
    MS_GUARD_ERROR = -1,
    MS_GUARD_INSERTED = 0,
    /* Every bound head position holds a constant, so there is no variable
     * to key the guard on and the rule is left unrestricted. */
    MS_GUARD_SKIPPED_CONSTANT_HEAD = 1,
    /* Malformed input: nothing to attach a guard to. */
    MS_GUARD_SKIPPED_NO_BODY = 2,
} ms_guard_result_t;

/*
 * Insert a magic guard JOIN at the top of a rule's body.
 *
 * Transforms:
 *   PROJECT(head) -> body_tree
 * into:
 *   PROJECT(head) -> JOIN(body_tree, SCAN($magic, [bound_positions]))
 *
 * The JOIN filters the body to only tuples where the bound variables
 * appear in the magic demand relation.
 *
 * The guard SCAN is the RIGHT child, not the left (Issue #989).  A JOIN's
 * right child is not a subtree in the execution plan: wl_plan_op_t carries
 * a `right_relation` *name*, and translate_ir_node() collapses children[1]
 * to its relation_name.  Putting the body on the right therefore produces
 * right_relation = NULL whenever the body is composite (a JOIN chain, an
 * ANTIJOIN from a negated atom, a SIP SEMIJOIN), and the rule derives
 * nothing.  Every other producer of JOIN nodes -- the parser, compound
 * side-bindings, jpp's chain rebuild -- already builds left-deep; this
 * keeps that invariant.
 */
static ms_guard_result_t
insert_magic_guard(wirelog_ir_node_t *ir_root, const char *magic_name,
    const char **bound_vars, uint32_t bound_count)
{
    if (!ir_root || !magic_name)
        return MS_GUARD_SKIPPED_NO_BODY;
    if (ir_root->child_count == 0)
        return MS_GUARD_SKIPPED_NO_BODY;

    uint32_t variable_count = 0;
    for (uint32_t i = 0; i < bound_count; i++) {
        if (bound_vars[i])
            variable_count++;
    }
    if (variable_count == 0)
        return MS_GUARD_SKIPPED_CONSTANT_HEAD;

    wirelog_ir_node_t *body = ir_root->children[0];

    /* SCAN($m$rel_adorn, column_names = [bound_var_0, ...]) */
    wirelog_ir_node_t *magic_scan = wl_ir_node_create(WIRELOG_IR_SCAN);
    if (!magic_scan)
        return MS_GUARD_ERROR;
    wl_ir_node_set_relation(magic_scan, magic_name);

    magic_scan->column_names = (char **)calloc(bound_count, sizeof(char *));
    magic_scan->column_types
        = (wl_ir_coltype_t *)calloc(bound_count, sizeof(wl_ir_coltype_t));
    if (!magic_scan->column_names || !magic_scan->column_types) {
        wl_ir_node_free(magic_scan);
        return MS_GUARD_ERROR;
    }
    magic_scan->column_count = bound_count;
    for (uint32_t i = 0; i < bound_count; i++) {
        if (bound_vars[i]) {
            magic_scan->column_names[i] = strdup_safe(bound_vars[i]);
            /* The magic relation carries the same values as the body
             * variables it is keyed on, so it inherits their types.
             * Unreachable by name now that the guard is the right child --
             * the body's identically named column resolves first (#989) --
             * but still reachable through the "colN" positional fallback,
             * where a wrong type would be a silent mistype (Issue #962). */
            magic_scan->column_types[i]
                = ms_lookup_var_type(body, bound_vars[i]);
        } else {
            char constant_name[32];
            snprintf(constant_name, sizeof(constant_name),
                "$magic_const_%u", i);
            magic_scan->column_names[i] = strdup_safe(constant_name);
        }
    }

    /* JOIN(body, magic_scan) keyed on bound variables */
    wirelog_ir_node_t *guard_join = wl_ir_node_create(WIRELOG_IR_JOIN);
    if (!guard_join) {
        wl_ir_node_free(magic_scan);
        return MS_GUARD_ERROR;
    }

    guard_join->join_left_keys
        = (char **)calloc(variable_count, sizeof(char *));
    guard_join->join_right_keys
        = (char **)calloc(variable_count, sizeof(char *));
    if (!guard_join->join_left_keys || !guard_join->join_right_keys) {
        wl_ir_node_free(guard_join);
        wl_ir_node_free(magic_scan);
        return MS_GUARD_ERROR;
    }
    guard_join->join_key_count = variable_count;
    uint32_t key = 0;
    for (uint32_t i = 0; i < bound_count; i++) {
        if (bound_vars[i]) {
            guard_join->join_left_keys[key] = strdup_safe(bound_vars[i]);
            guard_join->join_right_keys[key] = strdup_safe(bound_vars[i]);
            key++;
        }
    }

    /* Left-deep: the composite body stays on the left, where the plan can
     * express it as a subtree.  The right child must be a single relation
     * (Issue #989). */
    if (wl_ir_node_add_child(guard_join, body) != 0) {
        wl_ir_node_free(guard_join);
        wl_ir_node_free(magic_scan);
        return MS_GUARD_ERROR;
    }
    if (wl_ir_node_add_child(guard_join, magic_scan) != 0) {
        guard_join->children[0] = NULL;
        guard_join->child_count = 0;
        wl_ir_node_free(guard_join);
        wl_ir_node_free(magic_scan);
        return MS_GUARD_ERROR;
    }

    /* Replace body in parent */
    ir_root->children[0] = guard_join;
    return MS_GUARD_INSERTED;
}

/* ======================================================================== */
/* Public API: apply with explicit demands                                  */
/* ======================================================================== */

int
wl_magic_sets_apply_with_demands(struct wirelog_program *prog,
    const wl_magic_demand_t *demands,
    uint32_t demand_count,
    wl_magic_sets_stats_t *stats)
{
    if (!prog)
        return -2;
    if (prog->magic_sets_applied)
        return 0; /* Idempotency guard: already transformed */
    if (!demands || demand_count == 0)
        return 0;

    if (stats) {
        stats->demand_roots = 0;
        stats->adorned_predicates = 0;
        stats->magic_rules_generated = 0;
        stats->original_rules_modified = 0;
        stats->skipped_all_free = 0;
        stats->arity_mismatch_skipped = 0;
        stats->skipped_aggregate = 0;
        stats->skipped_constant_head = 0;
        stats->skipped_unsupported_head = 0;
        stats->multi_adornment_relations = 0;
        stats->unrestrictable_relations = 0;
    }

    /* === Phase 1: Seed the worklist from explicit demands === */

    ms_adorned_set_t processed;
    if (!adorned_set_init(&processed))
        return -1;

    /* Relations that must not be guarded at all (#1027).  Seeded in Phase 2,
     * closed before Phase 3.  Every member is an IDB, so the distinct names
     * cannot outnumber the program's rules; +1 keeps the allocation non-zero
     * for a program with none.  See ms_relset_t for why the bound is taken
     * from the rule table and not the relation table. */
    ms_relset_t unrestrictable;
    if (!relset_init(&unrestrictable, prog->rule_count + 1)) {
        adorned_set_free(&processed);
        return -1;
    }
    bool closure_incomplete = false;

    /* These consumers are unrestricted by construction.  Seed their IDB
     * inputs before the adorned BFS so the same closure also removes any
     * guards that would make their results incomplete, unsound, or wrong.
     * This walker is intentionally separate from Phase 2's demand walker:
     * negation must not propagate demand, but it must constrain viability. */
    if (!seed_guard_viability_roots(prog, demands, demand_count,
        &unrestrictable))
        closure_incomplete = true;

    /* Simple queue over processed items (indices 0..count-1) */
    uint32_t wl_head = 0; /* index of next item to process */

    for (uint32_t i = 0; i < demand_count; i++) {
        const wl_magic_demand_t *d = &demands[i];
        if (!d->relation_name)
            continue;

        if (stats)
            stats->demand_roots++;

        if (d->bound_mask == 0) {
            if (stats)
                stats->skipped_all_free++;
            continue; /* All-free optimization: skip */
        }

        if (relation_has_aggregate_rule(prog, d->relation_name)) {
            if (stats)
                stats->skipped_aggregate++;
            continue;
        }

        uint32_t arity = d->arity;
        if (arity == 0) {
            arity = get_arity(prog, d->relation_name);
        } else {
            uint32_t actual = get_arity(prog, d->relation_name);
            if (actual != 0 && actual != arity) {
                fprintf(stderr,
                    "warning: .query %s has %u adornment(s) but relation "
                    "has %u column(s); demand skipped\n",
                    d->relation_name, arity, actual);
                if (stats)
                    stats->arity_mismatch_skipped++;
                continue;
            }
        }

        adorned_add(&processed, d->relation_name, d->bound_mask, arity);
        if (processed.overflow) {
            fprintf(stderr,
                "warning: magic sets skipped program with more than %u "
                "adorned predicates\n", MS_MAX_ADORNED);
            adorned_set_free(&processed);
            relset_free(&unrestrictable);
            return -1;
        }
    }

    /* === Phase 2: BFS adorned program === */

    while (wl_head < processed.count) {
        const ms_adorned_t *ap = &processed.items[wl_head++];

        /* Walk every rule defining this relation */
        for (uint32_t ri = 0; ri < prog->rule_count; ri++) {
            if (!prog->rules[ri].head_relation
                || strcmp(prog->rules[ri].head_relation, ap->rel_name) != 0)
                continue;

            wirelog_ir_node_t *ir_root = prog->rules[ri].ir_root;
            if (!ir_root)
                continue;

            /* Head variable names, read off whatever head projection the root
             * carries -- not off its node type (#990). */
            const char *head_vars[64] = { 0 };
            uint32_t head_arity = get_head_vars(ir_root, head_vars, 64);
            if (head_arity == 0)
                continue;

            /* Init bound_vars from head's bound positions */
            ms_varset_t bound;
            varset_clear(&bound);
            for (uint32_t i = 0; i < head_arity && i < 64; i++) {
                if ((ap->bound_mask & (1ULL << i)) && head_vars[i])
                    varset_add(&bound, head_vars[i]);
            }

            /* Collect body atoms in join order */
            ms_atom_t atoms[MS_MAX_ATOMS];
            bool atom_overflow = false;
            uint32_t atom_count = collect_body_atoms(ir_root, atoms,
                    &atom_overflow);
            if (atom_overflow) {
                fprintf(stderr,
                    "warning: magic sets skipped rule with more than %u "
                    "body atoms\n", MS_MAX_ATOMS);
                adorned_set_free(&processed);
                relset_free(&unrestrictable);
                return -1;
            }

            for (uint32_t ai = 0; ai < atom_count; ai++) {
                const ms_atom_t *atom = &atoms[ai];
                if (!atom->rel_name)
                    goto next_atom;

                if (is_idb(prog, atom->rel_name)) {
                    if (relation_has_aggregate_rule(prog, atom->rel_name)) {
                        if (stats)
                            stats->skipped_aggregate++;
                        goto next_atom;
                    }

                    /* Compute adornment of this IDB body atom */
                    uint64_t atom_mask = 0;
                    for (uint32_t ci = 0; ci < atom->col_count && ci < 64;
                        ci++) {
                        if (atom->col_names && atom->col_names[ci]
                            && varset_contains(&bound, atom->col_names[ci]))
                            atom_mask |= (1ULL << ci);
                    }

                    if (atom_mask != 0) {
                        adorned_add(&processed, atom->rel_name, atom_mask,
                            atom->col_count);
                        if (processed.overflow) {
                            fprintf(stderr,
                                "warning: magic sets skipped program with "
                                "more than %u adorned predicates\n",
                                MS_MAX_ADORNED);
                            adorned_set_free(&processed);
                            relset_free(&unrestrictable);
                            return -1;
                        }
                    } else {
                        /* This occurrence binds nothing, so it needs every
                         * tuple of the relation.  No adorned predicate is
                         * added, so Phase 3 creates no demand relation and
                         * Phase 4a generates no rule to fill one -- and a
                         * guard over a demand nothing populates does not
                         * prune, it cuts the recursion (#1027).  Seed the
                         * guard-viability closure with the relation instead;
                         * it will be excluded from guarding altogether.
                         *
                         * Not counted as skipped_all_free.  That counter names
                         * the Phase 1 event -- a demand root adorned all-free,
                         * a genuine optimisation skip -- and this is a
                         * different thing entirely. */
                        if (!relset_add(&unrestrictable, atom->rel_name))
                            closure_incomplete = true;
                    }
                }

next_atom:
                /* Add all vars of this atom to bound set */
                for (uint32_t ci = 0; ci < atom->col_count; ci++) {
                    if (atom->col_names && atom->col_names[ci])
                        varset_add(&bound, atom->col_names[ci]);
                }
                if (bound.overflow) {
                    fprintf(stderr,
                        "warning: magic sets skipped rule with more than "
                        "%u variables\n", MS_MAX_VARS);
                    adorned_set_free(&processed);
                    relset_free(&unrestrictable);
                    return -1;
                }
            }
        }
    }

    uint32_t multi_adornment_relations = 0;
    if (!seed_multi_adornment_relations(prog, &processed, &unrestrictable,
        &multi_adornment_relations))
        closure_incomplete = true;
    if (stats)
        stats->multi_adornment_relations = multi_adornment_relations;

    /*
     * === Guard-viability closure (Issue #1027) ===
     *
     * Between the BFS and any mutation.  Phase 2 seeded `unrestrictable` with
     * the relations some occurrence needs whole; close it so that everything
     * they read is unguarded too, then drop every adornment of every member.
     *
     * Dropping them here is the single point that skips all three of the
     * things the pass would otherwise do with an adorned predicate: Phase 3
     * creates no magic relation for it, Phase 4a generates no demand rule
     * keyed on it, and Phase 4b inserts no guard into its rules.  All three
     * iterate `processed`.  The one site that does not is the Phase 4a
     * *target*, which keys off the body atom it is looking at rather than off
     * `processed`; that one is skipped explicitly below.
     *
     * If the closure cannot be completed the pass declines rather than
     * failing.  This walk reaches rules of relations Phase 2 never touched, so
     * it is the first thing in the pass that can trip over a rule with more
     * than MS_MAX_ATOMS body atoms in a subprogram the demand never reached.
     * Returning -1 there would surface through api_facade.c as
     * WIRELOG_ERR_MEMORY and fail wirelog_optimize() outright for a program
     * that optimized before, naming a cause that is not the real one.  An
     * incomplete closure is not an error, it just means no assignment of
     * guards can be shown consistent -- so leave the program as written.
     * Nothing has been mutated at this point: Phase 3 is the first writer.
     */

    if (!close_unrestrictable(prog, &unrestrictable))
        closure_incomplete = true;

    if (closure_incomplete) {
        /*
         * Declined: `magic_sets_applied` stays false and the IR is untouched.
         * Closure-derived counters are zeroed because none describes anything
         * that happened -- `unrestrictable` is a partial set here.
         */
        if (stats) {
            stats->adorned_predicates = 0;
            stats->multi_adornment_relations = 0;
            stats->unrestrictable_relations = 0;
        }
        adorned_set_free(&processed);
        relset_free(&unrestrictable);
        return 0;
    }
    if (stats)
        stats->unrestrictable_relations = unrestrictable.count;

    if (unrestrictable.count > 0) {
        uint32_t kept = 0;
        for (uint32_t i = 0; i < processed.count; i++) {
            if (relset_contains(&unrestrictable, processed.items[i].rel_name))
                continue;
            processed.items[kept++] = processed.items[i];
        }
        processed.count = kept;
    }

    if (stats)
        stats->adorned_predicates = processed.count;

    if (processed.count == 0){
        adorned_set_free(&processed);
        relset_free(&unrestrictable);
        return 0; /* Nothing to do */
    }

    /* === Phase 3: Create magic relations === */

    for (uint32_t i = 0; i < processed.count; i++) {
        const ms_adorned_t *ap = &processed.items[i];
        uint32_t bound_count = popcount64(ap->bound_mask);

        char *mname = make_magic_name(ap->rel_name, ap->bound_mask, ap->arity);
        if (!mname) {
            adorned_set_free(&processed);
            relset_free(&unrestrictable);
            return -1;
        }

        int rc = wl_ir_program_add_magic_relation(prog, mname, bound_count);
        if (rc != 0) {
            free(mname);
            adorned_set_free(&processed);
            relset_free(&unrestrictable);
            return -1;
        }
        free(mname);
        prog->magic_relation_count++;
    }

    /*
     * === Phase 4a: Generate demand propagation rules (READ-ONLY) ===
     *
     * Walk original rule bodies BEFORE any IR mutation.  Inserting magic
     * guards (Phase 4b) adds JOIN nodes wrapping the body; if we collected
     * body atoms after that, we would see the guard SCAN as an extra atom
     * and compute incorrect adornments for subsequent adorned predicates.
     */

    uint32_t orig_rule_count = prog->rule_count; /* snapshot before new rules */

    for (uint32_t pi = 0; pi < processed.count; pi++) {
        const ms_adorned_t *ap = &processed.items[pi];

        char *guard_magic
            = make_magic_name(ap->rel_name, ap->bound_mask, ap->arity);
        if (!guard_magic) {
            adorned_set_free(&processed);
            relset_free(&unrestrictable);
            return -1;
        }

        for (uint32_t ri = 0; ri < orig_rule_count; ri++) {
            if (!prog->rules[ri].head_relation
                || strcmp(prog->rules[ri].head_relation, ap->rel_name) != 0)
                continue;

            wirelog_ir_node_t *ir_root = prog->rules[ri].ir_root;
            if (!ir_root)
                continue;

            const char *head_vars[64] = { 0 };
            uint32_t head_arity = get_head_vars(ir_root, head_vars, 64);
            if (head_arity == 0)
                continue;

            const char *guard_bvars[64] = { 0 };
            uint32_t guard_bcount = 0;
            for (uint32_t i = 0; i < head_arity && i < 64; i++) {
                if (ap->bound_mask & (1ULL << i))
                    guard_bvars[guard_bcount++] = head_vars[i];
            }

            ms_atom_t atoms[MS_MAX_ATOMS];
            bool atom_overflow = false;
            uint32_t atom_count = collect_body_atoms(ir_root, atoms,
                    &atom_overflow);
            if (atom_overflow) {
                fprintf(stderr,
                    "warning: magic sets skipped rule with more than %u "
                    "body atoms\n", MS_MAX_ATOMS);
                free(guard_magic);
                adorned_set_free(&processed);
                relset_free(&unrestrictable);
                return -1;
            }

            ms_varset_t bound;
            varset_clear(&bound);
            for (uint32_t i = 0; i < guard_bcount; i++) {
                if (guard_bvars[i])
                    varset_add(&bound, guard_bvars[i]);
            }

            for (uint32_t ai = 0; ai < atom_count; ai++) {
                const ms_atom_t *atom = &atoms[ai];
                if (!atom->rel_name)
                    goto next_4a_atom;

                /*
                 * The guard-viability skip has to be applied here as well as
                 * on `ap` (Issue #1027).  This loop is keyed on the *body
                 * atom's* relation while `ap` names the *head's*, so an
                 * unrestrictable body atom under a perfectly restrictable head
                 * would otherwise still get a demand rule -- one whose head
                 * names a magic relation Phase 3 declined to create.  Nothing
                 * downstream reports that: wl_ir_program_rebuild_relation_irs()
                 * iterates relations, so a rule with no declared head relation
                 * is silently dropped.
                 *
                 * There is no demand to propagate to an unguarded relation in
                 * any case; it is evaluated in full.
                 *
                 * Aggregate body atoms use the same exclusion: Phase 2 skips
                 * aggregate relations, so Phase 3 creates no magic relation
                 * for them.  Keep Phase 4a in agreement or it emits an orphan
                 * demand rule that is silently dropped during IR rebuild.
                 */
                if (is_idb(prog, atom->rel_name)
                    && !relset_contains(&unrestrictable, atom->rel_name)
                    && !relation_has_aggregate_rule(prog, atom->rel_name)) {
                    uint64_t atom_mask = 0;
                    for (uint32_t ci = 0; ci < atom->col_count && ci < 64;
                        ci++) {
                        if (atom->col_names && atom->col_names[ci]
                            && varset_contains(&bound, atom->col_names[ci]))
                            atom_mask |= (1ULL << ci);
                    }

                    if (atom_mask != 0) {
                        char *body_magic = make_magic_name(
                            atom->rel_name, atom_mask, atom->col_count);
                        if (!body_magic) {
                            free(guard_magic);
                            adorned_set_free(&processed);
                            relset_free(&unrestrictable);
                            return -1;
                        }

                        wirelog_ir_node_t *demand_ir = build_demand_rule_ir(
                            body_magic, guard_magic, guard_bvars, guard_bcount,
                            atoms, ai, atom, atom_mask);

                        if (demand_ir) {
                            int rc = wl_ir_program_add_magic_rule(
                                prog, body_magic, demand_ir);
                            if (rc != 0) {
                                free(body_magic);
                                free(guard_magic);
                                wl_ir_node_free(demand_ir);
                                adorned_set_free(&processed);
                                relset_free(&unrestrictable);
                                return -1;
                            }
                            if (stats)
                                stats->magic_rules_generated++;
                        }
                        free(body_magic);
                    }
                }

next_4a_atom:
                for (uint32_t ci = 0; ci < atom->col_count; ci++) {
                    if (atom->col_names && atom->col_names[ci])
                        varset_add(&bound, atom->col_names[ci]);
                }
                if (bound.overflow) {
                    fprintf(stderr,
                        "warning: magic sets skipped rule with more than "
                        "%u variables\n", MS_MAX_VARS);
                    free(guard_magic);
                    adorned_set_free(&processed);
                    relset_free(&unrestrictable);
                    return -1;
                }
            }
        }

        free(guard_magic);
    }

    /*
     * === Phase 4b: Insert magic guards into original rules (MUTATING) ===
     *
     * Now that all demand propagation rules have been generated from the
     * original (unmodified) IR, we can safely mutate the original rule bodies
     * by splicing in JOIN(body, SCAN($m$rel)).  The guard SCAN is the right
     * child and the body stays on the left; see insert_magic_guard() for why
     * the other order does not survive plan translation (#989).
     *
     * Every adornment left in `processed` is one Phase 3 created a relation
     * for and Phase 4a can propagate a demand into -- except the demand roots
     * themselves, which by construction no propagation rule fills, because
     * nothing above them demands anything.  Seeding those is #989.  For every
     * other member the guard-viability closure removed the ones whose demand
     * could not be filled before Phase 3 ran, so this loop no longer guards a
     * rule on a demand nothing fills (#1027).
     */

    for (uint32_t pi = 0; pi < processed.count; pi++) {
        const ms_adorned_t *ap = &processed.items[pi];

        char *guard_magic
            = make_magic_name(ap->rel_name, ap->bound_mask, ap->arity);
        if (!guard_magic) {
            adorned_set_free(&processed);
            relset_free(&unrestrictable);
            return -1;
        }

        for (uint32_t ri = 0; ri < orig_rule_count; ri++) {
            if (!prog->rules[ri].head_relation
                || strcmp(prog->rules[ri].head_relation, ap->rel_name) != 0)
                continue;

            wirelog_ir_node_t *ir_root = prog->rules[ri].ir_root;
            if (!ir_root)
                continue;

            const char *head_vars[64] = { 0 };
            uint32_t head_arity = get_head_vars(ir_root, head_vars, 64);
            if (head_arity == 0) {
                /* No head variable names the guard could be keyed on, so the
                 * rule runs unrestricted.  A capability gap, not a policy
                 * decision.
                 *
                 * Reachable.  get_head_vars() also returns 0 for a head wider
                 * than the 64-bit adornment mask, and that path needs only a
                 * bound demand on a relation with more than 64 columns; it is
                 * pinned by test_wide_head_is_an_unsupported_head().  What
                 * #990 changed is that a rule fused to a FLATMAP root no
                 * longer lands here: it keeps its project_exprs and is guarded
                 * normally.  An AGGREGATE root cannot reach here either --
                 * relation_has_aggregate_rule() excludes those relations
                 * before they can enter `processed`.
                 *
                 * Leaving one rule of a guarded relation unrestricted is not
                 * automatically sound.  The rule still reads whatever its body
                 * names, and a body atom on a *guarded* relation reads a
                 * partial relation -- which is unsound under negation (#1047)
                 * and wrong-valued under aggregation (#1048).  The
                 * guard-viability closure (#1027) is what keeps the assignment
                 * of guards consistent for the shapes it can see; this skip is
                 * outside it, and the head arity that lands here is the one
                 * shape where the pass has no guard to offer either way. */
                if (stats)
                    stats->skipped_unsupported_head++;
                WL_LOG(WL_LOG_SEC_EVAL, WL_LOG_WARN,
                    "magic sets left rule for '%s' unguarded: its head has no "
                    "variable names the demand relation '%s' can be keyed on",
                    ap->rel_name, guard_magic);
                continue;
            }

            const char *guard_bvars[64] = { 0 };
            uint32_t guard_bcount = 0;
            for (uint32_t i = 0; i < head_arity && i < 64; i++) {
                if (ap->bound_mask & (1ULL << i))
                    guard_bvars[guard_bcount++] = head_vars[i];
            }

            ms_guard_result_t gr = insert_magic_guard(ir_root, guard_magic,
                    guard_bvars, guard_bcount);
            if (gr == MS_GUARD_ERROR) {
                free(guard_magic);
                adorned_set_free(&processed);
                relset_free(&unrestrictable);
                return -1;
            }
            if (stats) {
                /* Count guards actually inserted.  A rule whose bound head
                 * positions are all constants keeps running unrestricted;
                 * that is what skipped_constant_head records. */
                if (gr == MS_GUARD_INSERTED)
                    stats->original_rules_modified++;
                else if (gr == MS_GUARD_SKIPPED_CONSTANT_HEAD)
                    stats->skipped_constant_head++;
            }
        }

        free(guard_magic);
    }

    prog->magic_sets_applied = true;
    adorned_set_free(&processed);
    relset_free(&unrestrictable);
    return 0;
}

/* ======================================================================== */
/* Public API: apply from .output/.printsize demand roots                  */
/* ======================================================================== */

int
wl_magic_sets_apply(struct wirelog_program *prog, wl_magic_sets_stats_t *stats)
{
    if (!prog)
        return -2;

    /* Collect demand roots from .output and .printsize relations */
    wl_magic_demand_t demands[64];
    uint32_t demand_count = 0;

    for (uint32_t i = 0; i < prog->relation_count && demand_count < 64; i++) {
        if (prog->relations[i].has_output || prog->relations[i].has_printsize) {
            demands[demand_count].relation_name = prog->relations[i].name;
            demands[demand_count].bound_mask = 0; /* All-free */
            demands[demand_count].arity = prog->relations[i].column_count;
            demand_count++;
        }
    }

    /* With all-free adornment for all roots, all-free optimization
     * kicks in inside wl_magic_sets_apply_with_demands → no-op. */
    return wl_magic_sets_apply_with_demands(prog, demands, demand_count, stats);
}
