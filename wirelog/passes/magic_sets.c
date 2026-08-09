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
 * Extract variable names from the rule's PROJECT head.
 * vars[i] = variable name at head position i, or NULL for constants/wildcards.
 * Returns the head arity (number of head arguments).
 * Returns 0 for AGGREGATE rules (not currently supported for magic guards).
 */
static uint32_t
get_head_vars(const wirelog_ir_node_t *ir_root, const char **vars,
    uint32_t max_vars)
{
    if (!ir_root)
        return 0;

    if (ir_root->type == WIRELOG_IR_PROJECT) {
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

    /* AGGREGATE rules: skip magic guard insertion for now */
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

        wl_ir_node_add_child(join, current);
        wl_ir_node_add_child(join, scan);
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

    wl_ir_node_add_child(root, current);
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
 *   PROJECT(head) -> JOIN(body_tree, SCAN($magic, [bound_vars]))
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
    if (bound_count == 0)
        return MS_GUARD_SKIPPED_CONSTANT_HEAD;
    if (ir_root->child_count == 0)
        return MS_GUARD_SKIPPED_NO_BODY;

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
        }
    }

    /* JOIN(body, magic_scan) keyed on bound variables */
    wirelog_ir_node_t *guard_join = wl_ir_node_create(WIRELOG_IR_JOIN);
    if (!guard_join) {
        wl_ir_node_free(magic_scan);
        return MS_GUARD_ERROR;
    }

    guard_join->join_left_keys = (char **)calloc(bound_count, sizeof(char *));
    guard_join->join_right_keys = (char **)calloc(bound_count, sizeof(char *));
    if (!guard_join->join_left_keys || !guard_join->join_right_keys) {
        wl_ir_node_free(guard_join);
        wl_ir_node_free(magic_scan);
        return MS_GUARD_ERROR;
    }
    guard_join->join_key_count = bound_count;
    for (uint32_t i = 0; i < bound_count; i++) {
        if (bound_vars[i]) {
            guard_join->join_left_keys[i] = strdup_safe(bound_vars[i]);
            guard_join->join_right_keys[i] = strdup_safe(bound_vars[i]);
        }
    }

    /* Left-deep: the composite body stays on the left, where the plan can
     * express it as a subtree.  The right child must be a single relation
     * (Issue #989). */
    wl_ir_node_add_child(guard_join, body);       /* left: original body */
    wl_ir_node_add_child(guard_join, magic_scan); /* right: magic demand */

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
    }

    /* === Phase 1: Seed the worklist from explicit demands === */

    ms_adorned_set_t processed;
    if (!adorned_set_init(&processed))
        return -1;

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

            /* Get head variable names (PROJECT only) */
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
                            return -1;
                        }
                    } else {
                        if (stats)
                            stats->skipped_all_free++;
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
                    return -1;
                }
            }
        }
    }

    if (stats)
        stats->adorned_predicates = processed.count;

    if (processed.count == 0){
        adorned_set_free(&processed);
        return 0; /* Nothing to do */
    }

    /* === Phase 3: Create magic relations === */

    for (uint32_t i = 0; i < processed.count; i++) {
        const ms_adorned_t *ap = &processed.items[i];
        uint32_t bound_count = popcount64(ap->bound_mask);

        char *mname = make_magic_name(ap->rel_name, ap->bound_mask, ap->arity);
        if (!mname)
            return -1;

        int rc = wl_ir_program_add_magic_relation(prog, mname, bound_count);
        if (rc != 0) {
            free(mname);
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
        if (!guard_magic)
            return -1;

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

            const char *guard_bvars[64];
            uint32_t guard_bcount = 0;
            for (uint32_t i = 0; i < head_arity && i < 64; i++) {
                if ((ap->bound_mask & (1ULL << i)) && head_vars[i])
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
                adorned_set_free(&processed);
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

                if (is_idb(prog, atom->rel_name)) {
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
                    adorned_set_free(&processed);
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
     * original (unmodified) IR, we can safely mutate the original rule
     * bodies by prepending JOIN(SCAN($m$rel), body).
     */

    for (uint32_t pi = 0; pi < processed.count; pi++) {
        const ms_adorned_t *ap = &processed.items[pi];

        char *guard_magic
            = make_magic_name(ap->rel_name, ap->bound_mask, ap->arity);
        if (!guard_magic)
            return -1;

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
                /* No PROJECT head to read variable names from, so the guard
                 * cannot be keyed and the rule runs unrestricted.  This is a
                 * capability gap, not a policy decision: a rule fused to a
                 * FLATMAP root lands here -- Logic Fusion runs before Magic
                 * Sets in the shipping pipeline and any rule with a filter is
                 * a candidate.  That is the only shape that reaches this in
                 * practice; an AGGREGATE root cannot, because
                 * relation_has_aggregate_rule() filters those relations before
                 * adornment so they never enter `processed`.  Counting it is
                 * what makes the omission visible at all (see #990). */
                if (stats)
                    stats->skipped_unsupported_head++;
                continue;
            }

            const char *guard_bvars[64];
            uint32_t guard_bcount = 0;
            for (uint32_t i = 0; i < head_arity && i < 64; i++) {
                if ((ap->bound_mask & (1ULL << i)) && head_vars[i])
                    guard_bvars[guard_bcount++] = head_vars[i];
            }

            ms_guard_result_t gr = insert_magic_guard(ir_root, guard_magic,
                    guard_bvars, guard_bcount);
            if (gr == MS_GUARD_ERROR) {
                free(guard_magic);
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
