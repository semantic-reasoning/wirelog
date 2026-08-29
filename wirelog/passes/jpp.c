/*
 * jpp.c - Join-Project Plan Optimization Pass
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Reorders multi-atom joins to minimize intermediate result sizes.
 * Operates in-place on the program's merged relation IR trees.
 */

#include "jpp.h"
#include "../ir/ir.h"
#include "../ir/program.h"
#include "../util/log.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Internal: extract SCAN leaves from a left-deep JOIN chain                */
/* ======================================================================== */

/*
 * A left-deep JOIN chain:
 *   JOIN(JOIN(JOIN(S0, S1), S2), S3)
 *
 * Leaf order: [S0, S1, S2, S3]  (left-to-right)
 */
static uint32_t
collect_scans(wirelog_ir_node_t *node, wirelog_ir_node_t **out, uint32_t max)
{
    if (!node)
        return 0;
    if (node->type != WIRELOG_IR_JOIN) {
        /* Leaf (SCAN or FILTER wrapping a SCAN) */
        if (max > 0)
            out[0] = node;
        return 1;
    }
    /* Left child first, then right child */
    uint32_t n = 0;
    if (node->child_count >= 1)
        n += collect_scans(node->children[0], out, max);
    if (node->child_count >= 2 && n < max)
        n += collect_scans(node->children[1], out + n, max - n);
    return n;
}

/* Count every non-JOIN leaf without the bounded output-array semantics of
* collect_scans().  JPP only supports left-deep JOIN chains; this count is
* used to reject bushy trees before any scratch allocation or mutation. */
static uint32_t
count_join_leaves(const wirelog_ir_node_t *node)
{
    if (!node)
        return 0;
    if (node->type != WIRELOG_IR_JOIN)
        return 1;

    uint32_t total = 0;
    for (uint32_t i = 0; i < node->child_count; i++) {
        uint32_t child = count_join_leaves(node->children[i]);
        if (child > UINT32_MAX - total)
            return UINT32_MAX;
        total += child;
    }
    return total;
}

/* ======================================================================== */
/* Internal: variable set operations                                        */
/* ======================================================================== */

static const wirelog_ir_node_t *
wl_passes_jpp_data_node(const wirelog_ir_node_t *node)
{
    while (node && node->type == WIRELOG_IR_FILTER && node->child_count > 0)
        node = node->children[0];
    return node;
}

/*
 * Get the variable names for a SCAN node.
 * Returns column_names and sets *count.
 */
static char **
scan_vars(const wirelog_ir_node_t *scan, uint32_t *count)
{
    scan = wl_passes_jpp_data_node(scan);
    if (!scan) {
        *count = 0;
        return NULL;
    }
    *count = scan->column_count;
    return scan->column_names;
}

/* Largest element count a char *[] allocation can express. */
#define JPP_PTR_ARRAY_MAX ((uint64_t)(SIZE_MAX / sizeof(char *)))

/*
 * Total number of scan columns in a chain, for sizing the scratch arrays.
 *
 * The count MUST come from scan_vars(), not from scans[i]->column_count: a
 * leaf may be a FILTER wrapping the SCAN (program.c wraps an atom that
 * repeats a variable), and such a leaf carries column_count == 0 while the
 * inner SCAN holds the real width.
 *
 * Returns false only when the element count would exceed what a char *[]
 * allocation can express; that is the one case a caller must treat as "give
 * up".  On success *out is at least 1.
 *
 * A total of 0 is reachable and is NOT a failure: `.decl p()` is legal and
 * leaves column_count == 0 (see wirelog/ir/program.h), so a chain of nullary
 * atoms totals 0 while still being a chain the pass should optimize.  *out is
 * clamped to 1 there rather than reported as failure, because calloc(0, ..)
 * may return NULL and a caller cannot tell that apart from OOM.
 *
 * The clamp is safe for the two arrays sized directly from this value --
 * acc[] and phys_names[] -- because every write into those is bounded by some
 * scan's own column count, so a chain totalling 0 columns performs no writes
 * at all.  It is NOT the whole story for needed[], which also receives the
 * head variables and so can take a write even when this returns 0-clamped-to-1
 * (`o(x) :- a(), b(), e().` over nullary a/b/e reaches exactly that).  That
 * array is sized head_var_count + this value; see needed_cap in
 * insert_projections().
 */
static bool
scan_columns_total(wirelog_ir_node_t **scans, uint32_t nscan, size_t *out)
{
    uint64_t total = 0;

    for (uint32_t i = 0; i < nscan; i++) {
        uint32_t count = 0;
        (void)scan_vars(scans[i], &count);
        total += count;
        if (total > JPP_PTR_ARRAY_MAX) {
            WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_WARN,
                "jpp: join chain of %u scans exceeds %llu columns; scratch "
                "arrays cannot be sized, leaving the chain unoptimized",
                nscan, (unsigned long long)JPP_PTR_ARRAY_MAX);
            return false;
        }
    }

    *out = total > 0 ? (size_t)total : (size_t)1;
    return true;
}

/*
 * Count shared (non-NULL, non-wildcard) variable names between two sets.
 */
static uint32_t
count_shared_vars(char **a, uint32_t na, char **b, uint32_t nb)
{
    uint32_t shared = 0;
    for (uint32_t i = 0; i < na; i++) {
        if (!a[i])
            continue;
        for (uint32_t j = 0; j < nb; j++) {
            if (!b[j])
                continue;
            if (strcmp(a[i], b[j]) == 0) {
                shared++;
                break;
            }
        }
    }
    return shared;
}

/*
 * Check if a variable name is in a set.
 */
static bool
var_in_set(const char *var, char **set, uint32_t nset)
{
    if (!var)
        return false;
    for (uint32_t i = 0; i < nset; i++) {
        if (set[i] && strcmp(var, set[i]) == 0)
            return true;
    }
    return false;
}

/* ======================================================================== */
/* Internal: set up join keys on a JOIN node                                */
/* ======================================================================== */

/*
 * A join key set that has been fully built but not yet installed on a node.
 *
 * Key construction is split into a build step that allocates but mutates no
 * IR node, and an install step that mutates one node but allocates nothing.
 * That split is what lets a caller be all-or-nothing: it can build every key
 * set it needs first, and only start destroying the old keys once every
 * replacement is in hand.  Building in place instead -- destroy, then
 * allocate -- leaves a JOIN with zero keys when the allocation fails, and a
 * zero-key JOIN is executed as a cross product, so the query silently returns
 * wrong answers with exit status 0 (issue #1111).
 *
 * The strings are OWNED copies, never aliases into a SCAN's column_names:
 * install hands them straight to node->join_left_keys/join_right_keys, whose
 * elements are freed by free_join_keys() and wl_ir_node_free().
 */
typedef struct {
    char **left;
    char **right;
    uint32_t count;
} jpp_staged_keys_t;

/*
 * Release a staged key set that was never installed.  Safe on a zeroed
 * struct and on one that install() has already emptied.
 */
static void
jpp_free_staged_keys(jpp_staged_keys_t *staged)
{
    if (!staged)
        return;
    for (uint32_t i = 0; i < staged->count; i++) {
        free(staged->left[i]);
        free(staged->right[i]);
    }
    free((void *)staged->left);
    free((void *)staged->right);
    staged->left = NULL;
    staged->right = NULL;
    staged->count = 0;
}

/*
 * Build the join key set for a JOIN of @left_vars against @right_vars.
 * Mirrors the logic of setup_join_keys() in program.c but operates on
 * variable name arrays directly and touches no IR node.
 *
 * Returns false ONLY on allocation failure; *out is empty in that case and
 * needs no cleanup.  A true return with out->count == 0 means the two sides
 * share no variable, which is a legitimate cross product -- `p(x,y) :- a(x),
 * b(y).` has no join key and ir/program.c's setup_join_keys() produces the
 * same empty set.  Keeping those two outcomes on separate channels (the bool
 * and the count) is the point: the old in-place builder reported both as
 * "node now has zero keys".
 */
static bool
jpp_build_join_keys(char **left_vars, uint32_t left_count, char **right_vars,
    uint32_t right_count, jpp_staged_keys_t *out)
{
    out->left = NULL;
    out->right = NULL;
    out->count = 0;

    uint32_t key_count
        = count_shared_vars(left_vars, left_count, right_vars, right_count);
    if (key_count == 0)
        return true;

    /*
     * Size both arrays from left_count, not from key_count.
     *
     * key_count is the exact answer, and count_shared_vars() computed it
     * with a walk that is character-identical to the fill loop below, over
     * the same left_vars, right_vars, left_count and right_count -- nothing
     * mutates any of them in between -- so k reaches exactly key_count.
     * Sizing from left_count instead makes the bound STRUCTURAL: the fill
     * loop iterates left_vars[0 .. left_count) and appends at most one key
     * per iteration, so k < left_count at every store no matter what the
     * two walks agree on.  That is a property of the loop a reader can see
     * in one place, and it does not depend on re-deriving an equality
     * through a second traversal.  left_count >= key_count >= 1 here (a
     * non-zero key_count needs at least one non-NULL left variable), so
     * this never asks calloc for zero elements either.
     *
     * The arrays are therefore allocated LONGER than out->count whenever a
     * left variable is unshared or NULL.  A DUPLICATED left variable is not
     * a source of slack: count_shared_vars() counts it and the fill loop
     * stores it, so it consumes a slot and yields a duplicate key entry.
     *
     * The slack is not negligible, and the honest worst case is large.
     * Measured across the FULL test suite it peaks at 199 spare pointers
     * per array -- left_count == 200 against key_count == 1, from the
     * recursive 3-atom self-join over 200 columns in
     * tests/test_wide_relation.c, where the accumulator carries every
     * column and the atoms share a single variable.  The 32- and 33-column
     * widths of that same shape give 31 and 32.  Within tests/test_jpp.c
     * alone it peaks at three, which is why the figure has to be quoted
     * against the whole suite and not the pass's own tests.
     *
     * That is wasted space, not a leak or a hazard.  The spare elements
     * stay NULL from calloc, the array is released by the same free()
     * under either sizing, and nothing truncates: out->count is k either
     * way, and every consumer of a JOIN's key arrays iterates
     * [0, join_key_count) -- free_join_keys() and jpp_free_staged_keys()
     * in this file, wl_ir_node_free() and the IR printer in ir/ir.c, the
     * key copy in passes/sip.c, and the three resolution loops in
     * exec_plan_gen.c, which take the array and the count as a pair.  The
     * ir/ir.c pair is the one a reader is least likely to think to check;
     * both walk join_key_count, never the allocation.
     *
     * This does leave jpp-produced JOIN nodes as the only ones whose key
     * arrays are longer than join_key_count: ir/program.c's
     * setup_join_keys() and passes/magic_sets.c both calloc(key_count) and
     * set join_key_count = key_count.  Nothing appends to these arrays in
     * place, so the asymmetry is unobservable today; anything that ever
     * wanted to would have to carry the capacity, which only this builder
     * knows.
     */
    char **left = (char **)calloc(left_count, sizeof(char *));
    char **right = (char **)calloc(left_count, sizeof(char *));
    if (!left || !right) {
        free((void *)left);
        free((void *)right);
        return false;
    }

    uint32_t k = 0;
    for (uint32_t i = 0; i < left_count; i++) {
        if (!left_vars[i])
            continue;
        for (uint32_t j = 0; j < right_count; j++) {
            if (!right_vars[j])
                continue;
            if (strcmp(left_vars[i], right_vars[j]) == 0) {
                left[k] = strdup_safe(left_vars[i]);
                right[k] = strdup_safe(right_vars[j]);
                if (!left[k] || !right[k]) {
                    /* A NULL key name is not a benign short key list:
                     * exec_plan_gen.c's resolve_key_to_colN() falls back to
                     * column 0 for a NULL name, turning the join into a
                     * silently WRONG join rather than a cross product, and
                     * ir.c prints key names through %s. */
                    free(left[k]);
                    free(right[k]);
                    for (uint32_t f = 0; f < k; f++) {
                        free(left[f]);
                        free(right[f]);
                    }
                    free((void *)left);
                    free((void *)right);
                    return false;
                }
                k++;
                break;
            }
        }
    }

    out->left = left;
    out->right = right;
    out->count = k;
    return true;
}

/* ======================================================================== */
/* Internal: merge two variable name sets (union, no duplicates)            */
/* ======================================================================== */

static char **
merge_vars(char **a, uint32_t na, char **b, uint32_t nb, uint32_t *out_count)
{
    /*
     * Upper bound on merged size, plus one element that is never written.
     *
     * na + nb == 0 is reachable and legitimate: `.decl p()` is legal and
     * leaves column_count == 0, so an all-nullary chain merges two empty
     * sets.  C permits calloc(0, n) to return NULL and a caller cannot tell
     * that apart from OOM, which would decline the reorder for a chain that
     * is perfectly optimizable.  The `+ 1` is what keeps this allocation
     * away from zero, and it does so BY CONSTRUCTION rather than by a
     * `cap > 0 ? cap : 1` clamp at the call: there is no zero-size leg left
     * to get wrong, and no conditional for a reader to check.  The cost is
     * one pointer per merge.
     *
     * The width is more than sufficient for the writes: the first loop
     * stores na elements and the second at most nb, so n <= na + nb <
     * merged_cap on every path.  It cannot wrap.  na and nb count the
     * elements of `char *` arrays that already exist, so each is at most
     * SIZE_MAX / sizeof(char *) and the sum plus one stays well inside
     * size_t on every target -- the addition is performed in size_t, both
     * operands having been widened before it.
     */
    size_t merged_cap = (size_t)na + (size_t)nb + 1u;
    char **merged = (char **)calloc(merged_cap, sizeof(char *));
    if (!merged) {
        *out_count = 0;
        return NULL;
    }

    uint32_t n = 0;
    /* Copy all from a */
    for (uint32_t i = 0; i < na; i++) {
        merged[n++] = a[i]; /* alias, not strdup */
    }
    /* Add from b if not already in merged */
    for (uint32_t j = 0; j < nb; j++) {
        if (!b[j])
            continue;
        bool found = false;
        for (uint32_t k = 0; k < n; k++) {
            if (merged[k] && strcmp(merged[k], b[j]) == 0) {
                found = true;
                break;
            }
        }
        if (!found)
            merged[n++] = b[j]; /* alias */
    }
    *out_count = n;
    return merged;
}

/* ======================================================================== */
/* Internal: free old join keys on a node                                   */
/* ======================================================================== */

static void
free_join_keys(wirelog_ir_node_t *node)
{
    if (!node)
        return;
    for (uint32_t i = 0; i < node->join_key_count; i++) {
        free(node->join_left_keys[i]);
        free(node->join_right_keys[i]);
    }
    free((void *)node->join_left_keys);
    free((void *)node->join_right_keys);
    node->join_left_keys = NULL;
    node->join_right_keys = NULL;
    node->join_key_count = 0;
}

/* ======================================================================== */
/* Internal: install a staged key set on a JOIN node                        */
/* ======================================================================== */

/*
 * Replace @node's join keys with the previously built @staged set.
 * Allocates nothing and therefore cannot fail: every caller may treat this
 * as the commit half of a build-then-install pair.  Ownership of the staged
 * arrays and strings transfers to @node, and @staged is left empty.
 */
static void
jpp_install_join_keys(wirelog_ir_node_t *node, jpp_staged_keys_t *staged)
{
    free_join_keys(node);
    node->join_left_keys = staged->left;
    node->join_right_keys = staged->right;
    node->join_key_count = staged->count;
    staged->left = NULL;
    staged->right = NULL;
    staged->count = 0;
}

/* ======================================================================== */
/* Internal: IDB scan detection                                             */
/* ======================================================================== */

/*
 * Return true if the given scan node references an IDB relation.
 * A relation is IDB if it appears as the head of at least one rule.
 * idb_names/idb_count is the de-duplicated list of IDB relation names
 * extracted from the program's rule set.
 */
static bool
scan_is_idb(const wirelog_ir_node_t *scan, const char *const *idb_names,
    uint32_t idb_count)
{
    scan = wl_passes_jpp_data_node(scan);
    if (!scan || !scan->relation_name || !idb_names)
        return false;
    for (uint32_t i = 0; i < idb_count; i++) {
        if (idb_names[i]
            && strcmp(scan->relation_name, idb_names[i]) == 0)
            return true;
    }
    return false;
}

static bool
expr_contains_extension_call(const wl_ir_expr_t *expr)
{
    if (!expr)
        return false;
    if (expr->type == WL_IR_EXPR_EXTENSION_CALL)
        return true;
    for (uint32_t i = 0; i < expr->child_count; i++)
        if (expr_contains_extension_call(expr->children[i]))
            return true;
    return false;
}

/* JPP has no registry snapshot at optimizer time, so it cannot distinguish
* a trusted PURE/DETERMINISTIC attestation from a legacy or partial one.
* Keep extension-containing trees in source order until an optimizer pass
* with capability metadata exists.  This prevents join reordering and
* projection changes from duplicating, moving, or eliminating callbacks. */
static bool
node_contains_extension_call(const wirelog_ir_node_t *node)
{
    if (!node)
        return false;
    if (expr_contains_extension_call(node->filter_expr)
        || expr_contains_extension_call(node->agg_expr)
        || expr_contains_extension_call(node->compound_side.handle_expr))
        return true;
    if (node->project_exprs) {
        for (uint32_t i = 0; i < node->project_count; i++)
            if (expr_contains_extension_call(node->project_exprs[i]))
                return true;
    }
    for (uint32_t i = 0; i < node->child_count; i++)
        if (node_contains_extension_call(node->children[i]))
            return true;
    return false;
}

/* ======================================================================== */
/* Internal: greedy reorder a join chain                                    */
/* ======================================================================== */

/*
 * Given an array of SCAN leaves, compute the greedy ordering that
 * maximizes shared variables at each step.
 *
 * On a tie (equal shared-variable count), EDB atoms are preferred over
 * IDB atoms.  This avoids placing large recursive relations early in the
 * join chain (e.g. VarPointsTo in DOOP).
 *
 * idb_names/idb_count: de-duplicated IDB relation name list for tie-breaking.
 * May be NULL/0 to disable tie-breaking (falls back to index order).
 *
 * Returns true if the ordering changed from the original.
 */
static bool
greedy_order(wirelog_ir_node_t **scans, uint32_t nscan, uint32_t *order,
    const char *const *idb_names, uint32_t idb_count)
{
    if (nscan < 2) {
        for (uint32_t i = 0; i < nscan; i++)
            order[i] = i;
        return false;
    }

    bool *used = (bool *)calloc(nscan, sizeof(bool));
    if (!used) {
        for (uint32_t i = 0; i < nscan; i++)
            order[i] = i;
        return false;
    }

    /* Pre-compute IDB flags for all scans */
    bool *is_idb = (bool *)calloc(nscan, sizeof(bool));
    if (!is_idb) {
        free(used);
        for (uint32_t i = 0; i < nscan; i++)
            order[i] = i;
        return false;
    }
    for (uint32_t i = 0; i < nscan; i++)
        is_idb[i] = scan_is_idb(scans[i], idb_names, idb_count);

    /* Start with scan[0] */
    order[0] = 0;
    used[0] = true;

    /* Accumulated variable set */
    uint32_t acc_count;
    char **acc_vars = scan_vars(scans[0], &acc_count);
    /* We need a mutable copy for merging.  The chain's total column count is
     * an exact upper bound -- but note the bound is TOTAL columns, not
     * DISTINCT ones, and it has to be.  acc[] is seeded with scans[0]'s
     * columns verbatim, duplicates and NULLs included (an atom that repeats a
     * variable, r0(p, p, q), carries cols=3 [p,p,q]); only the columns merged
     * from later scans are deduplicated.  Do not tighten this to a distinct
     * count -- that reintroduces the overflow this pass was fixed for. */
    size_t acc_cap;
    if (!scan_columns_total(scans, nscan, &acc_cap)) {
        free(is_idb);
        free(used);
        for (uint32_t i = 0; i < nscan; i++)
            order[i] = i;
        return false;
    }
    char **acc = (char **)calloc(acc_cap, sizeof(char *));
    if (!acc) {
        free(is_idb);
        free(used);
        for (uint32_t i = 0; i < nscan; i++)
            order[i] = i;
        return false;
    }
    for (uint32_t i = 0; i < acc_count; i++)
        acc[i] = acc_vars[i];

    for (uint32_t step = 1; step < nscan; step++) {
        uint32_t best_idx = 0;
        uint32_t best_shared = 0;
        bool found_any = false;

        for (uint32_t j = 0; j < nscan; j++) {
            if (used[j])
                continue;
            uint32_t scount;
            char **svars = scan_vars(scans[j], &scount);
            uint32_t shared = count_shared_vars(acc, acc_count, svars, scount);
            /*
             * Pick j if:
             *   (a) strictly more shared variables, OR
             *   (b) equal shared variables AND j is EDB while current best
             *       is IDB (EDB tie-breaker).
             */
            if (!found_any || shared > best_shared
                || (shared == best_shared && !is_idb[j]
                && is_idb[best_idx])) {
                best_shared = shared;
                best_idx = j;
                found_any = true;
            }
        }

        order[step] = best_idx;
        used[best_idx] = true;

        /* Merge best's vars into accumulated set */
        uint32_t scount;
        char **svars = scan_vars(scans[best_idx], &scount);
        for (uint32_t j = 0; j < scount; j++) {
            if (!svars[j])
                continue;
            bool dup = false;
            for (uint32_t k = 0; k < acc_count; k++) {
                if (acc[k] && strcmp(acc[k], svars[j]) == 0) {
                    dup = true;
                    break;
                }
            }
            if (!dup)
                acc[acc_count++] = svars[j];
        }
    }

    free((void *)acc);
    free(is_idb);
    free(used);

    /* Check if ordering changed */
    for (uint32_t i = 0; i < nscan; i++) {
        if (order[i] != i)
            return true;
    }
    return false;
}

/* ======================================================================== */
/* Internal: rebuild a left-deep JOIN chain from ordered scans              */
/* ======================================================================== */

/*
 * Rebuilds the JOIN chain in-place by reusing existing JOIN nodes.
 * The original JOIN nodes are collected, then reconnected in the new order.
 *
 * join_nodes[0] is the deepest (innermost) JOIN,
 * join_nodes[n-2] is the outermost JOIN.
 *
 * After rebuild:
 *   join_nodes[0] = JOIN(ordered[0], ordered[1])
 *   join_nodes[1] = JOIN(join_nodes[0], ordered[2])
 *   ...
 *
 * All-or-nothing.  The work is split into two phases:
 *
 *   Phase 1 computes and allocates every replacement key set but mutates
 *   nothing, so a failure anywhere in it can be reported with the chain
 *   still exactly as it was found.  Hoisting the computation ahead of the
 *   mutation is sound because every input to it comes from
 *   scan_vars(ordered_scans[k]) or from the accumulator, and the two node
 *   sets are disjoint: collect_scans() yields only non-JOIN nodes and
 *   collect_joins() only JOIN nodes, so nothing here reads a join_nodes[i]
 *   child that phase 2 goes on to overwrite.
 *
 *   Phase 2 rewires the children and installs the staged keys.  It performs
 *   no allocation and cannot fail.
 *
 * Returns true when the chain was rebuilt, false when it was left untouched.
 * The caller must not count a false return as a reorder (issue #1111).
 */
static bool
rebuild_chain(wirelog_ir_node_t **join_nodes, uint32_t njoin,
    wirelog_ir_node_t **ordered_scans, uint32_t nscan)
{
    if (njoin == 0 || nscan < 2)
        return false;

    /* ---- Phase 1: compute only.  Allocates; mutates nothing. ---- */

    jpp_staged_keys_t *staged
        = (jpp_staged_keys_t *)calloc(njoin, sizeof(jpp_staged_keys_t));
    if (!staged) {
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_WARN,
            "jpp: cannot stage join keys for a chain of %u joins; leaving "
            "the chain in its original order",
            njoin);
        return false;
    }

    /* Deepest join: children will be ordered_scans[0] and ordered_scans[1] */
    uint32_t lcount, rcount;
    char **lvars = scan_vars(ordered_scans[0], &lcount);
    char **rvars = scan_vars(ordered_scans[1], &rcount);
    bool ok = jpp_build_join_keys(lvars, lcount, rvars, rcount, &staged[0]);

    /* Accumulate variable set */
    uint32_t acc_count = 0;
    char **acc = NULL;
    if (ok) {
        acc = merge_vars(lvars, lcount, rvars, rcount, &acc_count);
        ok = acc != NULL;
    }

    /* Remaining joins */
    for (uint32_t i = 1; ok && i < njoin; i++) {
        uint32_t scount;
        char **svars = scan_vars(ordered_scans[i + 1], &scount);
        ok = jpp_build_join_keys(acc, acc_count, svars, scount, &staged[i]);
        if (!ok)
            break;

        /* Merge into accumulated */
        uint32_t new_count;
        char **new_acc = merge_vars(acc, acc_count, svars, scount, &new_count);
        if (!new_acc) {
            ok = false;
            break;
        }
        free((void *)acc);
        acc = new_acc;
        acc_count = new_count;
    }

    free((void *)acc);

    if (!ok) {
        for (uint32_t i = 0; i < njoin; i++)
            jpp_free_staged_keys(&staged[i]);
        free(staged);
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_WARN,
            "jpp: cannot rebuild the join keys for a chain of %u scans; "
            "leaving the chain in its original order",
            nscan);
        return false;
    }

    /* ---- Phase 2: commit only.  No allocation; cannot fail. ---- */

    join_nodes[0]->children[0] = ordered_scans[0];
    join_nodes[0]->children[1] = ordered_scans[1];
    jpp_install_join_keys(join_nodes[0], &staged[0]);

    for (uint32_t i = 1; i < njoin; i++) {
        join_nodes[i]->children[0] = join_nodes[i - 1];
        join_nodes[i]->children[1] = ordered_scans[i + 1];
        jpp_install_join_keys(join_nodes[i], &staged[i]);
    }

    free(staged);
    return true;
}

/* ======================================================================== */
/* Internal: collect JOIN nodes from a left-deep chain (deepest first)      */
/* ======================================================================== */

static uint32_t
collect_joins(wirelog_ir_node_t *node, wirelog_ir_node_t **out, uint32_t max)
{
    if (!node || node->type != WIRELOG_IR_JOIN || max == 0)
        return 0;
    uint32_t n = 0;
    /* Recurse into left child first (deeper joins come first) */
    if (node->child_count >= 1)
        n = collect_joins(node->children[0], out, max);
    if (n < max)
        out[n++] = node;
    return n;
}

/* ======================================================================== */
/* Internal: collect head variable names from wrapper nodes                 */
/* ======================================================================== */

/*
 * Walk from the IR root through wrapper nodes to collect all variable
 * names that are referenced by the head projection or filter expressions.
 * This tells us which variables must survive to the top of the join chain.
 *
 * DO NOT REMOVE the `max` checks here (the `*count >= max` guard below and
 * the `count < max` guard in collect_head_vars()'s ANTIJOIN arm).  The caller
 * sizes `out` from count_head_vars(), which is intended to be an exact upper
 * bound, so the checks look redundant -- and they are, as long as that
 * function stays in step with this one.  The two walk the same node types by
 * hand, in two places; if they ever diverge and count_head_vars() undercounts,
 * these checks are the only thing that makes the consequence a truncated head
 * set (a wrong plan) instead of a heap-buffer-overflow WRITE.  That is the
 * failure mode issue #1002 was filed for.
 */
static void
collect_head_vars_from_expr(const wl_ir_expr_t *expr, char **out,
    uint32_t *count, uint32_t max)
{
    if (!expr || *count >= max)
        return;
    if (expr->type == WL_IR_EXPR_VAR && expr->var_name) {
        /* Check for duplicates */
        for (uint32_t i = 0; i < *count; i++) {
            if (out[i] && strcmp(out[i], expr->var_name) == 0)
                return;
        }
        out[(*count)++] = expr->var_name; /* alias */
    }
    for (uint32_t i = 0; i < expr->child_count; i++) {
        collect_head_vars_from_expr(expr->children[i], out, count, max);
    }
}

static uint32_t
collect_head_vars(wirelog_ir_node_t *ir, char **out, uint32_t max)
{
    uint32_t count = 0;
    wirelog_ir_node_t *node = ir;

    while (node) {
        if (node->type == WIRELOG_IR_PROJECT) {
            /* Collect from project_exprs */
            if (node->project_exprs) {
                for (uint32_t i = 0; i < node->project_count; i++) {
                    collect_head_vars_from_expr(node->project_exprs[i], out,
                        &count, max);
                }
            }
        } else if (node->type == WIRELOG_IR_FLATMAP) {
            /* Has both project_exprs and filter_expr */
            if (node->project_exprs) {
                for (uint32_t i = 0; i < node->project_count; i++) {
                    collect_head_vars_from_expr(node->project_exprs[i], out,
                        &count, max);
                }
            }
            if (node->filter_expr) {
                collect_head_vars_from_expr(node->filter_expr, out, &count,
                    max);
            }
        } else if (node->type == WIRELOG_IR_FILTER) {
            if (node->filter_expr) {
                collect_head_vars_from_expr(node->filter_expr, out, &count,
                    max);
            }
        } else if (node->type == WIRELOG_IR_ANTIJOIN) {
            /* ANTIJOIN join keys reference variables that must survive */
            for (uint32_t i = 0; i < node->join_key_count; i++) {
                if (node->join_left_keys[i]
                    && !var_in_set(node->join_left_keys[i], out, count)
                    && count < max) {
                    out[count++] = node->join_left_keys[i];
                }
            }
        } else {
            break;
        }

        if (node->child_count > 0)
            node = node->children[0];
        else
            break;
    }

    return count;
}

static uint64_t
count_head_vars_in_expr(const wl_ir_expr_t *expr)
{
    if (!expr)
        return 0;
    uint64_t count
        = (expr->type == WL_IR_EXPR_VAR && expr->var_name) ? 1u : 0u;
    for (uint32_t i = 0; i < expr->child_count; i++)
        count += count_head_vars_in_expr(expr->children[i]);
    return count;
}

/*
 * Upper bound on what collect_head_vars() will return.
 *
 * Walks exactly the same wrapper chain and counts every VAR leaf plus every
 * ANTIJOIN join key, without collect_head_vars()'s deduplication, so the
 * result is always >= the deduplicated count.  Used to size the output array
 * up front: a fixed cap silently truncates, and a dropped head variable that
 * does not also appear in a later scan of the chain -- the "needed" loop in
 * insert_projections() re-adds the ones that do -- is projected away, which
 * yields wrong column values with a zero exit status and no diagnostic.
 *
 * Accumulates in uint64_t and saturates at UINT32_MAX.  Saturation needs more
 * than 2^32 VAR nodes in one rule and so is unreachable in practice; it is
 * here so that the bound can never wrap to a small number, which would put us
 * straight back into the undersized-array failure this pass exists to avoid.
 * A saturated count is still a valid upper bound, so it is safe either way:
 * the calloc() in optimize_tree() will usually fail at that size and leave the
 * chain unoptimized, but it may also succeed under Linux overcommit, and a
 * 2^32-entry array is not too small.
 */
static uint32_t
count_head_vars(const wirelog_ir_node_t *ir)
{
    uint64_t count = 0;
    const wirelog_ir_node_t *node = ir;

    while (node) {
        if (node->type == WIRELOG_IR_PROJECT) {
            if (node->project_exprs) {
                for (uint32_t i = 0; i < node->project_count; i++)
                    count += count_head_vars_in_expr(node->project_exprs[i]);
            }
        } else if (node->type == WIRELOG_IR_FLATMAP) {
            if (node->project_exprs) {
                for (uint32_t i = 0; i < node->project_count; i++)
                    count += count_head_vars_in_expr(node->project_exprs[i]);
            }
            if (node->filter_expr)
                count += count_head_vars_in_expr(node->filter_expr);
        } else if (node->type == WIRELOG_IR_FILTER) {
            if (node->filter_expr)
                count += count_head_vars_in_expr(node->filter_expr);
        } else if (node->type == WIRELOG_IR_ANTIJOIN) {
            count += node->join_key_count;
        } else {
            break;
        }

        if (node->child_count > 0)
            node = node->children[0];
        else
            break;
    }

    return count > UINT32_MAX ? UINT32_MAX : (uint32_t)count;
}

/* ======================================================================== */
/* Internal: insert intermediate projections in a join chain                */
/* ======================================================================== */

/*
 * For each intermediate JOIN in the chain (all except the outermost),
 * check if the accumulated variable set contains variables not needed
 * by any subsequent scan or the head. If so, insert a PROJECT node.
 *
 * Returns number of projections inserted.
 */
static uint32_t
insert_projections(wirelog_ir_node_t *join_root, char **head_vars,
    uint32_t head_var_count)
{
    if (!join_root || join_root->type != WIRELOG_IR_JOIN)
        return 0;

    /* Count depth and collect scans/joins */
    uint32_t depth = 0;
    {
        wirelog_ir_node_t *n = join_root;
        while (n && n->type == WIRELOG_IR_JOIN) {
            depth++;
            n = n->child_count > 0 ? n->children[0] : NULL;
        }
    }
    if (depth < 2)
        return 0; /* Need at least 3 atoms for intermediate projection */

    uint32_t nscan = depth + 1;
    if (count_join_leaves(join_root) != nscan)
        return 0; /* Bushy JOINs are outside the projection contract. */
    wirelog_ir_node_t **scans
        = (wirelog_ir_node_t **)calloc(nscan, sizeof(wirelog_ir_node_t *));
    wirelog_ir_node_t **joins
        = (wirelog_ir_node_t **)calloc(depth, sizeof(wirelog_ir_node_t *));
    if (!scans || !joins) {
        free((void *)scans);
        free((void *)joins);
        return 0;
    }

    collect_scans(join_root, scans, nscan);
    collect_joins(join_root, joins, depth);

    uint32_t projections = 0;

    /* Bound for acc[] and phys_names[] below: neither ever holds more than
     * one alias per scan column in the chain.  needed[] is sized separately
     * -- it holds the head variables as well, so its bound adds
     * head_var_count on top of this. */
    /*
     * Six stores into acc[] and phys_names[], and one into needed[], carry
     * NOLINTs for clang-analyzer-security.ArrayBound below.  All seven are
     * analyzer limitations rather than real bounds; the proof lives here,
     * once, so that the suppressions themselves can stay bare.
     *
     * total_cols is scan_columns_total(): the sum of the scan_vars() counts
     * over every entry of scans[].  Call a "chain column" one column of one
     * of those scans; there are exactly total_cols of them.
     *
     *   - acc[] has total_cols elements.  It is filled to scans[0]'s column
     *     count, then acc_count is incremented once per column of a later
     *     scan not already present, and is otherwise only ever reset
     *     DOWNWARD, by the PROJECT rewrite.  Every increment consumes a
     *     distinct chain column that no earlier increment consumed, so at
     *     each store the index acc_count is at most the number of chain
     *     columns already consumed -- leaving at least the one being stored
     *     -- hence acc_count < total_cols.
     *   - phys_names[] has total_cols elements.  phys_count is incremented
     *     once per column of scans[0], scans[1] and each later scans[i + 2],
     *     and is reset to acc_count (itself <= total_cols) whenever a
     *     PROJECT shrinks the layout.  Each increment likewise consumes a
     *     distinct chain column, so phys_count < total_cols at each store.
     *   - needed[] has head_var_count + total_cols elements.  needed_count
     *     is incremented at most once per head variable and at most once per
     *     chain column, each behind a var_in_set() duplicate check, so
     *     needed_count < needed_cap at each store.
     *
     * The counts are uint32_t and total_cols is size_t, so "count <=
     * total_cols" is a complete argument only together with total_cols <=
     * UINT32_MAX.  That holds: total_cols is a sum of uint32_t column_count
     * values over nscan scans, and a chain totalling more than UINT32_MAX
     * columns could not have been built at all -- phys_names[] alone would
     * be one allocation of that many pointers.
     *
     * The analyzer reports these because it does not inline
     * scan_columns_total(): a total accumulated over a loop is outside the
     * constraint manager's reach, so it explores paths on which total_cols
     * is unrelated to the per-scan counts (total_cols == 1 against a scan of
     * many columns).  Two stronger experiments confirm that diagnosis rather
     * than a mere size budget -- inlining the entire summation loop into
     * this function by hand, giving the analyzer maximal visibility, and
     * deleting the WL_LOG from the helper to rule out "the helper is too big
     * to inline".  Both still report the same seven sites.
     */
    size_t total_cols;
    if (!scan_columns_total(scans, nscan, &total_cols)) {
        free((void *)scans);
        free((void *)joins);
        return 0;
    }

    /* For each intermediate join (all except the outermost = joins[depth-1]),
     * check if we can project away variables. */
    uint32_t acc_count;
    char **acc_vars = scan_vars(scans[0], &acc_count);
    /* Build mutable accumulated set */
    char **acc = (char **)calloc(total_cols, sizeof(char *));
    if (!acc) {
        free((void *)scans);
        free((void *)joins);
        return 0;
    }
    for (uint32_t i = 0; i < acc_count; i++)
        // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
        acc[i] = acc_vars[i];

    /* Merge scan[1] */
    {
        uint32_t scount;
        char **svars = scan_vars(scans[1], &scount);
        for (uint32_t j = 0; j < scount; j++) {
            if (!svars[j])
                continue;
            if (!var_in_set(svars[j], acc, acc_count))
                // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
                acc[acc_count++] = svars[j];
        }
    }

    /* Track the actual physical column layout of the current join output.
     * Unlike acc[] (which is deduplicated), phys_names[] mirrors the true
     * columnar output: scan columns concatenated in order, join-key columns
     * appearing in both children.  When a PROJECT is inserted it shrinks the
     * layout; subsequent scans are appended on top.  This is the layout that
     * project_indices must reference. */
    char **phys_names = (char **)calloc(total_cols, sizeof(char *));
    uint32_t phys_count = 0;
    if (!phys_names) {
        free((void *)acc);
        free((void *)scans);
        free((void *)joins);
        return 0;
    }
    /* Initial physical layout: S0 columns || S1 columns (with duplicates) */
    {
        uint32_t s0c;
        char **s0v = scan_vars(scans[0], &s0c);
        for (uint32_t j = 0; j < s0c; j++)
            // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
            phys_names[phys_count++] = s0v[j];
        uint32_t s1c;
        char **s1v = scan_vars(scans[1], &s1c);
        for (uint32_t j = 0; j < s1c; j++)
            // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
            phys_names[phys_count++] = s1v[j];
    }

    /* Now acc has the result of joins[0] (deepest join).
     * For each intermediate join i (0 to depth-2):
     *   - acc has the accumulated vars after joins[i]
     *   - Check which vars in acc are needed by scans[i+2..nscan-1]
     *     and head_vars
     *   - If any are dead, insert a PROJECT
     */
    /* The needed set holds at most every head variable plus every scan
     * column, deduplicated.  0 means only that the size would overflow;
     * total_cols is already at least 1, so 0 is never a legitimate bound. */
    uint64_t needed_total = (uint64_t)head_var_count + (uint64_t)total_cols;
    size_t needed_cap = 0;
    if (needed_total > JPP_PTR_ARRAY_MAX) {
        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_WARN,
            "jpp: %u head variables plus %zu scan columns exceed %llu; the "
            "needed set cannot be sized, leaving the chain unoptimized",
            head_var_count, total_cols,
            (unsigned long long)JPP_PTR_ARRAY_MAX);
    } else {
        needed_cap = (size_t)needed_total;
    }

    for (uint32_t i = 0; i < depth - 1; i++) {
        /* Compute "needed" vars: head_vars + vars in future scans */
        /* Build needed set */
        char **needed = needed_cap > 0
            ? (char **)calloc(needed_cap, sizeof(char *))
            : NULL;
        uint32_t needed_count = 0;
        if (!needed)
            break;

        /* Add head vars */
        for (uint32_t h = 0; h < head_var_count; h++) {
            if (head_vars[h] && !var_in_set(head_vars[h], needed, needed_count))
                needed[needed_count++] = head_vars[h];
        }

        /* Add vars from future scans (scans[i+2] onward) */
        for (uint32_t s = i + 2; s < nscan; s++) {
            uint32_t scount;
            char **svars = scan_vars(scans[s], &scount);
            for (uint32_t j = 0; j < scount; j++) {
                if (svars[j] && !var_in_set(svars[j], needed, needed_count))
                    // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
                    needed[needed_count++] = svars[j];
            }
        }

        /* Count how many accumulated vars are needed */
        uint32_t live_count = 0;
        for (uint32_t v = 0; v < acc_count; v++) {
            if (acc[v] && var_in_set(acc[v], needed, needed_count))
                live_count++;
        }

        if (live_count < acc_count && live_count > 0) {
            /* Some variables are dead; insert a PROJECT */
            wirelog_ir_node_t *proj = wl_ir_node_create(WIRELOG_IR_PROJECT);
            if (proj) {
                proj->project_count = live_count;
                proj->project_indices
                    = (uint32_t *)calloc(live_count, sizeof(uint32_t));
                proj->column_names
                    = (char **)calloc(live_count, sizeof(char *));
                if (proj->project_indices && proj->column_names) {
                    proj->column_count = live_count;
                    /*
                     * The store into proj->project_indices[] below carries
                     * an eighth NOLINT, and it is PRE-EMPTIVE: the site does
                     * not fire at the analyzer's default max-inlinable-size
                     * of 100 CFG blocks, at which insert_projections() is
                     * too large to inline into its caller.  Raise that
                     * threshold to 140 and the reported set for this file
                     * does not drift, it FLIPS: the seven sites above all
                     * disappear and this one appears alone in their place.
                     *
                     * insert_projections() sits only ~31-40 blocks above the
                     * default, and the direction of risk is SHRINKING it --
                     * extracting a helper is the ordinary refactor here.
                     * Such a refactor would take this file from
                     * suppressed-clean to RED at a site nothing had flagged.
                     *
                     * Other stores in this file are latent too, and none of
                     * them is suppressed.  greedy_order()'s two acc[] writes
                     * and rebuild_chain()'s children[0] assignments sit
                     * OUTSIDE insert_projections(), so no refactor of the
                     * kind above can surface them.
                     *
                     * That argument does not reach the PROJECT rewrite lower
                     * in this same function -- acc[new_acc++] and the
                     * phys_names[] refill that follows it are inside
                     * insert_projections() -- so for those two the reason is
                     * different, and it is empirical rather than
                     * structural: neither is reported at 100, at 140 or at
                     * 200, whereas the store below is reported at 140 and at
                     * 200.  Both are safe by the bounds already established
                     * above -- new_acc only trails v across the same
                     * [0, acc_count) walk, and the refill writes exactly
                     * acc_count entries into an array of total_cols.
                     * Suppressing a site that no measured configuration
                     * reports would spend a baseline slot and buy nothing.
                     *
                     * The invariant is the same identical-predicate shape as
                     * jpp_build_join_keys(): project_indices[] and
                     * column_names[] both have live_count elements, and p is
                     * incremented once per v in [0, acc_count) satisfying
                     * `acc[v] && var_in_set(acc[v], needed, needed_count)`
                     * -- character-identical to the predicate the live_count
                     * loop just counted with, over the same acc[], needed[]
                     * and needed_count, none of which is mutated in between.
                     * So p reaches exactly live_count, and every store is at
                     * an index p < live_count.
                     */
                    uint32_t p = 0;
                    for (uint32_t v = 0; v < acc_count; v++) {
                        if (acc[v]
                            && var_in_set(acc[v], needed, needed_count)) {
                            /* Find the first occurrence of acc[v] in the
                             * tracked physical column layout.  phys_names[]
                             * reflects the true columnar output after any
                             * prior PROJECTs, so indices are correct even
                             * when 2+ projections are inserted in one chain. */
                            uint32_t phys_idx = 0;
                            for (uint32_t ph = 0; ph < phys_count; ph++) {
                                if (phys_names[ph] && acc[v]
                                    && strcmp(phys_names[ph], acc[v]) == 0) {
                                    phys_idx = ph;
                                    break;
                                }
                            }
                            // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
                            proj->project_indices[p] = phys_idx;
                            proj->column_names[p] = strdup_safe(acc[v]);
                            if (!proj->column_names[p]) {
                                /* A half-named PROJECT is not a cosmetic
                                 * defect: exec_plan_gen.c returns
                                 * column_names[] verbatim as this node's
                                 * output layout, and resolve_key_to_colN()
                                 * answers "col0" for any key it cannot find
                                 * there -- so where the missing name is a
                                 * join key of the JOIN above, the join reads
                                 * the WRONG column and still reports success.
                                 *
                                 * Declining is exact here: proj is still
                                 * detached, and neither the children[0]
                                 * splice nor the acc[]/phys_names[] rewrites
                                 * below have run, so nothing outside proj has
                                 * been touched yet. */
                                WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_WARN,
                                    "jpp: cannot name the columns of an "
                                    "inserted projection; leaving this level "
                                    "unprojected");
                                wl_ir_node_free(proj);
                                goto next_level;
                            }
                            p++;
                        }
                    }

                    /* Insert: parent_join->children[0] = proj,
                     *          proj->child = current_join */
                    if (wl_ir_node_add_child(proj, joins[i]) != 0) {
                        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_WARN,
                            "jpp: cannot attach an inserted projection; "
                            "leaving this level unprojected");
                        wl_ir_node_free(proj);
                        goto next_level;
                    }
                    joins[i + 1]->children[0] = proj;

                    /* Update acc to reflect projection */
                    uint32_t new_acc = 0;
                    for (uint32_t v = 0; v < acc_count; v++) {
                        if (acc[v]
                            && var_in_set(acc[v], needed, needed_count)) {
                            acc[new_acc++] = acc[v];
                        }
                    }
                    acc_count = new_acc;

                    /* Update physical layout to match PROJECT output: the
                     * live acc columns, in order, with no duplicates. */
                    phys_count = 0;
                    for (uint32_t v = 0; v < acc_count; v++)
                        phys_names[phys_count++] = acc[v];

                    /* Recalculate join keys for parent join using PROJECTED acc.
                     * After projection, column indices change, so join keys must
                     * reference the projected columns in the PROJECT output.
                     *
                     * Build first, install second, for the same reason as
                     * rebuild_chain(): freeing the old keys up front and then
                     * failing to allocate the new ones leaves this JOIN with
                     * zero keys, which executes as a cross product (#1111).
                     * Keeping the existing keys is correct as a fallback -- a
                     * variable shared with scans[i + 2] is by construction in
                     * the `needed` set and so survives the PROJECT, making the
                     * recomputed key NAMES identical to the ones already on
                     * the node. */
                    uint32_t rscount;
                    char **rsvars = scan_vars(scans[i + 2], &rscount);
                    /* Use projected acc (current acc after shrinking) which has
                     * correct indices for the columns in the PROJECT output */
                    jpp_staged_keys_t rkeys = { 0 };
                    if (jpp_build_join_keys(acc, acc_count, rsvars, rscount,
                        &rkeys)) {
                        jpp_install_join_keys(joins[i + 1], &rkeys);
                    } else {
                        WL_LOG(WL_LOG_SEC_JOIN, WL_LOG_WARN,
                            "jpp: cannot recompute the join keys above an "
                            "inserted projection; keeping the existing keys");
                    }

                    projections++;
                } else {
                    wl_ir_node_free(proj);
                }
            }
        }

        /*
         * Abandoning ONE projection must not abandon the loop's bookkeeping.
         * `continue` here would skip both the free() below and the tail merge
         * of scans[i + 2] into acc[] and phys_names[], leaving the next
         * iteration to run on an accumulator that no longer describes the
         * chain: jpp_build_join_keys() would then find no shared variable,
         * succeed with count == 0, and jpp_install_join_keys() would replace
         * that JOIN's correct keys with an empty set -- a cross product, and
         * exactly the #1111 signature.  Jump past the projection work only.
         */
next_level:
        free((void *)needed);

        /* Merge next scan's vars into acc and physical layout for the next
         * iteration.  phys_names gets ALL scan columns (join-key duplicates
         * included); acc gets only new (deduplicated) names. */
        if (i + 2 < nscan) {
            uint32_t scount;
            char **svars = scan_vars(scans[i + 2], &scount);
            for (uint32_t j = 0; j < scount; j++) {
                // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
                phys_names[phys_count++] = svars[j];
                if (svars[j] && !var_in_set(svars[j], acc, acc_count))
                    // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
                    acc[acc_count++] = svars[j];
            }
        }
    }

    free((void *)phys_names);
    free((void *)acc);
    free((void *)scans);
    free((void *)joins);
    return projections;
}

/* ======================================================================== */
/* Internal: optimize a single join chain                                   */
/* ======================================================================== */

typedef struct {
    bool reordered;
    uint32_t projections_inserted;
} jpp_chain_result_t;

static jpp_chain_result_t
optimize_chain(wirelog_ir_node_t *join_root, char **head_vars,
    uint32_t head_var_count, const char *const *idb_names, uint32_t idb_count)
{
    jpp_chain_result_t result = { false, 0 };

    if (!join_root || join_root->type != WIRELOG_IR_JOIN)
        return result;

    /* Count depth */
    uint32_t depth = 0;
    {
        wirelog_ir_node_t *n = join_root;
        while (n && n->type == WIRELOG_IR_JOIN) {
            depth++;
            n = n->child_count > 0 ? n->children[0] : NULL;
        }
    }

    uint32_t nscan = depth + 1;
    if (nscan < 3)
        return result; /* Two-atom chains don't benefit from reordering */
    if (count_join_leaves(join_root) != nscan)
        return result; /* Only optimize complete left-deep chains. */

    /* Collect SCAN leaves and JOIN nodes */
    wirelog_ir_node_t **scans
        = (wirelog_ir_node_t **)calloc(nscan, sizeof(wirelog_ir_node_t *));
    wirelog_ir_node_t **joins
        = (wirelog_ir_node_t **)calloc(depth, sizeof(wirelog_ir_node_t *));
    uint32_t *order = (uint32_t *)calloc(nscan, sizeof(uint32_t));
    if (!scans || !joins || !order) {
        free((void *)scans);
        free((void *)joins);
        free(order);
        return result;
    }

    uint32_t actual_scans = collect_scans(join_root, scans, nscan);
    uint32_t actual_joins = collect_joins(join_root, joins, depth);
    (void)actual_joins;

    if (actual_scans != nscan) {
        /* Not a clean left-deep chain; skip */
        free((void *)scans);
        free((void *)joins);
        free(order);
        return result;
    }

    /* Compute greedy ordering */
    bool changed = greedy_order(scans, nscan, order, idb_names, idb_count);

    if (changed) {
        /* Build ordered scan array */
        wirelog_ir_node_t **ordered
            = (wirelog_ir_node_t **)calloc(nscan, sizeof(wirelog_ir_node_t *));
        if (ordered) {
            for (uint32_t i = 0; i < nscan; i++)
                ordered[i] = scans[order[i]];
            /* Only a chain that was actually rebuilt counts as reordered.
             * Reporting the reorder unconditionally overstated the statistic
             * both when rebuild_chain() declined and when this calloc failed
             * so it never ran at all (issue #1111). */
            result.reordered = rebuild_chain(joins, depth, ordered, nscan);
            free((void *)ordered);
        }
    }

    /* Intermediate column projection elimination (Issue #191).
     * Called AFTER join reordering so projection decisions reflect the
     * final (possibly reordered) scan/join structure.
     */
    result.projections_inserted
        = insert_projections(join_root, head_vars, head_var_count);

    free((void *)scans);
    free((void *)joins);
    free(order);

    return result;
}

/* ======================================================================== */
/* Internal: find join root through wrapper nodes                           */
/* ======================================================================== */

/*
 * Descend through PROJECT, FLATMAP, FILTER, and ANTIJOIN wrappers
 * to find the JOIN chain root.
 */
static wirelog_ir_node_t *
find_join_chain(wirelog_ir_node_t *node)
{
    while (node) {
        switch (node->type) {
        case WIRELOG_IR_PROJECT:
        case WIRELOG_IR_FLATMAP:
        case WIRELOG_IR_FILTER:
        case WIRELOG_IR_ANTIJOIN:
            if (node->child_count > 0) {
                node = node->children[0];
                continue;
            }
            return NULL;
        case WIRELOG_IR_JOIN:
            return node;
        default:
            return NULL;
        }
    }
    return NULL;
}

/* ======================================================================== */
/* Internal: optimize a single IR tree (recurse into UNION children)        */
/* ======================================================================== */

static void
optimize_tree(wirelog_ir_node_t *ir, uint32_t *chains_examined,
    uint32_t *joins_reordered, uint32_t *projections_inserted,
    const char *const *idb_names, uint32_t idb_count)
{
    if (!ir)
        return;

    if (node_contains_extension_call(ir))
        return;

    /* UNION: recurse into each child */
    if (ir->type == WIRELOG_IR_UNION) {
        for (uint32_t i = 0; i < ir->child_count; i++) {
            optimize_tree(ir->children[i], chains_examined, joins_reordered,
                projections_inserted, idb_names, idb_count);
        }
        return;
    }

    /* Find the join chain root through wrappers */
    wirelog_ir_node_t *root = find_join_chain(ir);
    if (!root)
        return;

    (*chains_examined)++;

    /* Collect head variables from wrapper nodes, into an array sized by a
     * counting pre-pass so that none is dropped. */
    uint32_t head_var_max = count_head_vars(ir);
    char **head_vars = NULL;
    if (head_var_max > 0) {
        head_vars = (char **)calloc(head_var_max, sizeof(char *));
        if (!head_vars) {
            /* Without the head variables every accumulated variable looks
             * dead, which would project away live columns.  Leave this tree
             * unoptimized instead. */
            return;
        }
    }
    uint32_t head_var_count
        = head_vars ? collect_head_vars(ir, head_vars, head_var_max) : 0;

    jpp_chain_result_t result
        = optimize_chain(root, head_vars, head_var_count, idb_names, idb_count);

    free((void *)head_vars);

    if (result.reordered)
        (*joins_reordered)++;
    *projections_inserted += result.projections_inserted;
}

static bool
contains_aggregate(const wirelog_ir_node_t *ir)
{
    if (!ir)
        return false;
    if (ir->type == WIRELOG_IR_AGGREGATE)
        return true;
    for (uint32_t i = 0; i < ir->child_count; i++) {
        if (contains_aggregate(ir->children[i]))
            return true;
    }
    return false;
}

/* ======================================================================== */
/* Public API                                                               */
/* ======================================================================== */

int
wl_jpp_apply(struct wirelog_program *prog, wl_jpp_stats_t *stats)
{
    if (!prog)
        return -2;

    if (!prog->relation_irs) {
        if (stats) {
            stats->joins_reordered = 0;
            stats->projections_inserted = 0;
            stats->chains_examined = 0;
            stats->skipped_aggregate = 0;
        }
        return 0;
    }

    uint32_t chains_examined = 0;
    uint32_t joins_reordered = 0;
    uint32_t projections_inserted = 0;
    uint32_t skipped_aggregate = 0;

    /* Build de-duplicated IDB relation name list for EDB tie-breaking.
     * A relation is IDB iff it appears as the head of at least one rule. */
    const char **idb_names = NULL;
    uint32_t idb_count = 0;
    if (prog->rule_count > 0) {
        idb_names
            = (const char **)calloc(prog->rule_count, sizeof(const char *));
        if (idb_names) {
            for (uint32_t i = 0; i < prog->rule_count; i++) {
                const char *h = prog->rules[i].head_relation;
                if (!h)
                    continue;
                bool dup = false;
                for (uint32_t j = 0; j < idb_count; j++) {
                    if (idb_names[j] && strcmp(idb_names[j], h) == 0) {
                        dup = true;
                        break;
                    }
                }
                if (!dup)
                    idb_names[idb_count++] = h;
            }
        }
    }

    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (contains_aggregate(prog->relation_irs[i])) {
            skipped_aggregate++;
            continue;
        }
        optimize_tree(prog->relation_irs[i], &chains_examined, &joins_reordered,
            &projections_inserted, idb_names, idb_count);
    }

    free((void *)idb_names);

    if (stats) {
        stats->joins_reordered = joins_reordered;
        stats->projections_inserted = projections_inserted;
        stats->chains_examined = chains_examined;
        stats->skipped_aggregate = skipped_aggregate;
    }

    return 0;
}
