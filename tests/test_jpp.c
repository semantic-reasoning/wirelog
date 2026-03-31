/*
 * test_jpp.c - Tests for Join-Project Plan Optimization Pass
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "../wirelog/passes/jpp.h"
#include "../wirelog/ir/ir.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/wirelog-parser.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Helpers                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                            \
        do {                                      \
            tests_run++;                          \
            printf("  [%d] %s", tests_run, name); \
        } while (0)

#define PASS()                 \
        do {                       \
            tests_passed++;        \
            printf(" ... PASS\n"); \
        } while (0)

#define FAIL(msg)                         \
        do {                                  \
            tests_failed++;                   \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)

/* ======================================================================== */
/* Helper: find relation IR by name                                         */
/* ======================================================================== */

static wirelog_ir_node_t *
find_relation_ir(struct wirelog_program *prog, const char *name)
{
    if (!prog || !prog->relation_irs)
        return NULL;
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (prog->relations[i].name
            && strcmp(prog->relations[i].name, name) == 0) {
            return prog->relation_irs[i];
        }
    }
    return NULL;
}

/* ======================================================================== */
/* Helper: find the root of a join chain (skip PROJECT/FLATMAP wrapper)     */
/* ======================================================================== */

static wirelog_ir_node_t *
find_join_root(wirelog_ir_node_t *ir)
{
    if (!ir)
        return NULL;
    if (ir->type == WIRELOG_IR_PROJECT || ir->type == WIRELOG_IR_FLATMAP) {
        if (ir->child_count > 0)
            return find_join_root(ir->children[0]);
    }
    return ir;
}

/* ======================================================================== */
/* Helper: find the deepest (innermost) JOIN in a left-deep chain           */
/* ======================================================================== */

static wirelog_ir_node_t *
find_deepest_join(wirelog_ir_node_t *node)
{
    if (!node || node->type != WIRELOG_IR_JOIN)
        return NULL;
    /* Descend into left child, skipping any intermediate PROJECT nodes
     * that insert_projections may have added between JOIN levels. */
    wirelog_ir_node_t *left
        = node->child_count > 0 ? node->children[0] : NULL;
    while (left && left->type == WIRELOG_IR_PROJECT && left->child_count > 0)
        left = left->children[0];
    if (left && left->type == WIRELOG_IR_JOIN)
        return find_deepest_join(left);
    return node;
}

/* ======================================================================== */
/* Helper: count nodes of a specific type in an IR tree                     */
/* ======================================================================== */

static uint32_t
count_type_in_tree(const wirelog_ir_node_t *node, wirelog_ir_node_type_t type)
{
    if (!node)
        return 0;
    uint32_t count = (node->type == type) ? 1 : 0;
    for (uint32_t i = 0; i < node->child_count; i++) {
        count += count_type_in_tree(node->children[i], type);
    }
    return count;
}

/* ======================================================================== */
/* Error Handling Tests                                                     */
/* ======================================================================== */

static void
test_jpp_null_program(void)
{
    TEST("jpp: NULL program returns -2");

    wl_jpp_stats_t stats = { 0, 0, 0 };
    int rc = wl_jpp_apply(NULL, &stats);

    if (rc != -2) {
        FAIL("expected -2");
        return;
    }
    PASS();
}

static void
test_jpp_null_stats(void)
{
    TEST("jpp: NULL stats works (no crash)");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl edge(x: int32, y: int32)\n"
            "edge(1, 2).\n",
            &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    int rc = wl_jpp_apply(prog, NULL);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_jpp_no_ir_trees(void)
{
    TEST("jpp: program with only EDB returns 0 with zero stats");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl edge(x: int32, y: int32)\n"
            "edge(1, 2).\n",
            &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_jpp_stats_t stats = { 99, 99, 99 };
    int rc = wl_jpp_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.joins_reordered != 0 || stats.projections_inserted != 0
        || stats.chains_examined != 0) {
        FAIL("expected all stats = 0 for EDB-only program");
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* No-op Tests                                                              */
/* ======================================================================== */

static void
test_jpp_single_atom_noop(void)
{
    TEST("jpp: single-atom rule is no-op (no JOINs)");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl edge(x: int32, y: int32)\n"
            ".decl node(x: int32)\n"
            "node(x) :- edge(x, _).\n",
            &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_jpp_stats_t stats = { 0, 0, 0 };
    int rc = wl_jpp_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.joins_reordered != 0) {
        FAIL("single-atom rule should not be reordered");
        wirelog_program_free(prog);
        return;
    }

    /* Verify no JOINs exist in the IR */
    wirelog_ir_node_t *ir = find_relation_ir(prog, "node");
    if (!ir) {
        FAIL("no IR for 'node'");
        wirelog_program_free(prog);
        return;
    }

    uint32_t join_count = count_type_in_tree(ir, WIRELOG_IR_JOIN);
    if (join_count != 0) {
        FAIL("expected 0 JOINs for single-atom rule");
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_jpp_two_atom_noop(void)
{
    TEST("jpp: two-atom rule is no-op (already optimal)");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl edge(x: int32, y: int32)\n"
            ".decl tc(x: int32, y: int32)\n"
            "tc(x, z) :- tc(x, y), edge(y, z).\n",
            &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_jpp_stats_t stats = { 0, 0, 0 };
    int rc = wl_jpp_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.joins_reordered != 0) {
        FAIL("two-atom rule should not be reordered");
        wirelog_program_free(prog);
        return;
    }

    /* Should have exactly 1 JOIN */
    wirelog_ir_node_t *ir = find_relation_ir(prog, "tc");
    if (!ir) {
        FAIL("no IR for 'tc'");
        wirelog_program_free(prog);
        return;
    }

    uint32_t join_count = count_type_in_tree(ir, WIRELOG_IR_JOIN);
    if (join_count != 1) {
        FAIL("expected 1 JOIN for two-atom rule");
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Reorder Tests                                                            */
/* ======================================================================== */

static void
test_jpp_three_atom_reorder(void)
{
    TEST("jpp: 3-atom rule reordered to avoid cross-product");

    /*
     * path(x, z) :- a(x, y), c(w, z), b(y, w).
     *
     * Naive left-to-right:
     *   JOIN(SCAN(a), SCAN(c))  -- join_key_count=0 (cross-product!)
     *   JOIN(above, SCAN(b))
     *
     * Optimal greedy:
     *   Start with a(x,y), next b(y,w) shares y, then c(w,z) shares w.
     *   JOIN(SCAN(a), SCAN(b))  -- join on y
     *   JOIN(above, SCAN(c))    -- join on w
     *   No cross-products.
     */
    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
            ".decl b(y: int32, w: int32)\n"
            ".decl c(w: int32, z: int32)\n"
            ".decl path(x: int32, z: int32)\n"
            "path(x, z) :- a(x, y), c(w, z), b(y, w).\n",
            &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_jpp_stats_t stats = { 0, 0, 0 };
    int rc = wl_jpp_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.joins_reordered != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected joins_reordered=1, got %u",
            stats.joins_reordered);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    /* Verify: no cross-products in the reordered tree.
    * The deepest JOIN must have join_key_count > 0. */
    wirelog_ir_node_t *ir = find_relation_ir(prog, "path");
    if (!ir) {
        FAIL("no IR for 'path'");
        wirelog_program_free(prog);
        return;
    }

    wirelog_ir_node_t *join_root = find_join_root(ir);
    if (!join_root || join_root->type != WIRELOG_IR_JOIN) {
        FAIL("expected JOIN at root of chain");
        wirelog_program_free(prog);
        return;
    }

    wirelog_ir_node_t *deep = find_deepest_join(join_root);
    if (!deep) {
        FAIL("no deepest JOIN found");
        wirelog_program_free(prog);
        return;
    }

    if (deep->join_key_count == 0) {
        FAIL("deepest JOIN has no join keys (cross-product not eliminated)");
        wirelog_program_free(prog);
        return;
    }

    /* Outer JOIN should also have keys */
    if (join_root->join_key_count == 0) {
        FAIL("outer JOIN has no join keys (cross-product not eliminated)");
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Stats Tests                                                              */
/* ======================================================================== */

static void
test_jpp_already_optimal_three_atom(void)
{
    TEST("jpp: already-optimal 3-atom rule not reordered");

    /*
     * tc(x, z) :- edge(x, y), edge(y, w), edge(w, z).
     *
     * Naive left-to-right:
     *   JOIN(SCAN(edge), SCAN(edge))  -- shares y
     *   JOIN(above, SCAN(edge))       -- shares w
     *   Already optimal (no cross-products).
     */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl edge(x: int32, y: int32)\n"
        ".decl tc(x: int32, z: int32)\n"
        "tc(x, z) :- edge(x, y), edge(y, w), edge(w, z).\n",
        &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_jpp_stats_t stats = { 0, 0, 0 };
    int rc = wl_jpp_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.chains_examined != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected chains_examined=1, got %u",
            stats.chains_examined);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    if (stats.joins_reordered != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf),
            "expected joins_reordered=0 (already optimal), got %u",
            stats.joins_reordered);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_jpp_chains_examined_count(void)
{
    TEST("jpp: chains_examined counts all 3+ atom chains");

    /*
     * Two IDB relations with 3-atom rules:
     *   path: a(x,y), c(w,z), b(y,w) -- needs reorder
     *   tc:   edge(x,y), edge(y,w), edge(w,z) -- already optimal
     *
     * chains_examined should be 2, joins_reordered should be 1.
     */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, w: int32)\n"
        ".decl c(w: int32, z: int32)\n"
        ".decl edge(x: int32, y: int32)\n"
        ".decl path(x: int32, z: int32)\n"
        ".decl tc(x: int32, z: int32)\n"
        "path(x, z) :- a(x, y), c(w, z), b(y, w).\n"
        "tc(x, z) :- edge(x, y), edge(y, w), edge(w, z).\n",
        &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_jpp_stats_t stats = { 0, 0, 0 };
    int rc = wl_jpp_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.chains_examined != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected chains_examined=2, got %u",
            stats.chains_examined);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    if (stats.joins_reordered != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected joins_reordered=1, got %u",
            stats.joins_reordered);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* UNION and ANTIJOIN Tests                                                 */
/* ======================================================================== */

static void
test_jpp_union_recurse(void)
{
    TEST("jpp: recurse into UNION children");

    /*
     * Two rules for 'path', each with 3 atoms needing reorder:
     *   path(x, z) :- a(x, y), c(w, z), b(y, w).
     *   path(x, z) :- a(x, y), d(v, z), e(y, v).
     *
     * Both should be reordered. The relation IR is UNION(rule1, rule2).
     * chains_examined should be 2 (one per UNION child).
     */
    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
            ".decl b(y: int32, w: int32)\n"
            ".decl c(w: int32, z: int32)\n"
            ".decl d(v: int32, z: int32)\n"
            ".decl e(y: int32, v: int32)\n"
            ".decl path(x: int32, z: int32)\n"
            "path(x, z) :- a(x, y), c(w, z), b(y, w).\n"
            "path(x, z) :- a(x, y), d(v, z), e(y, v).\n",
            &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    /* Verify the relation IR is a UNION */
    wirelog_ir_node_t *ir = find_relation_ir(prog, "path");
    if (!ir || ir->type != WIRELOG_IR_UNION) {
        FAIL("expected UNION IR for 'path' with two rules");
        wirelog_program_free(prog);
        return;
    }

    wl_jpp_stats_t stats = { 0, 0, 0 };
    int rc = wl_jpp_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.chains_examined != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected chains_examined=2, got %u",
            stats.chains_examined);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    if (stats.joins_reordered != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected joins_reordered=2, got %u",
            stats.joins_reordered);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_jpp_antijoin_preserved(void)
{
    TEST("jpp: ANTIJOIN wrapper preserved, join chain underneath optimized");

    /*
     * path(x, z) :- a(x, y), c(w, z), b(y, w), !neg(x, z).
     *
     * IR structure:
     *   PROJECT(ANTIJOIN(JOIN(JOIN(SCAN(a), SCAN(c)), SCAN(b)), SCAN(neg)))
     *
     * JPP should:
     *   - Preserve the ANTIJOIN node
     *   - Reorder the JOIN chain underneath (children[0] of ANTIJOIN)
     *   - After: no cross-product in the join chain
     */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, w: int32)\n"
        ".decl c(w: int32, z: int32)\n"
        ".decl neg(x: int32, z: int32)\n"
        ".decl path(x: int32, z: int32)\n"
        "path(x, z) :- a(x, y), c(w, z), b(y, w), !neg(x, z).\n",
        &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_jpp_stats_t stats = { 0, 0, 0 };
    int rc = wl_jpp_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.joins_reordered != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected joins_reordered=1, got %u",
            stats.joins_reordered);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    /* Verify ANTIJOIN is still present */
    wirelog_ir_node_t *ir = find_relation_ir(prog, "path");
    if (!ir) {
        FAIL("no IR for 'path'");
        wirelog_program_free(prog);
        return;
    }

    uint32_t aj_count = count_type_in_tree(ir, WIRELOG_IR_ANTIJOIN);
    if (aj_count != 1) {
        FAIL("expected 1 ANTIJOIN preserved");
        wirelog_program_free(prog);
        return;
    }

    /* The join chain underneath should have no cross-products */
    wirelog_ir_node_t *root = find_join_root(ir);
    /* root may be ANTIJOIN; look at its left child for the join chain */
    if (root && root->type == WIRELOG_IR_ANTIJOIN && root->child_count > 0)
        root = root->children[0];
    wirelog_ir_node_t *deep = find_deepest_join(root);
    if (!deep || deep->join_key_count == 0) {
        FAIL("deepest JOIN has no join keys after reorder");
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Projection Insertion Tests                                               */
/* ======================================================================== */

static void
test_jpp_intermediate_projection(void)
{
    TEST("jpp: intermediate projection enabled (Issue #191)");

    /*
     * Intermediate column projection elimination is now enabled.
     * For a 3-atom rule with head needing {x, z}, projections are
     * inserted after scans to eliminate unused columns.
     * DD backend removed in Phase 2C - original blocking issue resolved.
     */
    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
            ".decl b(y: int32, w: int32)\n"
            ".decl c(w: int32, z: int32)\n"
            ".decl path(x: int32, z: int32)\n"
            "path(x, z) :- a(x, y), b(y, w), c(w, z).\n",
            &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_jpp_stats_t stats = { 0, 0, 0 };
    int rc = wl_jpp_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.projections_inserted == 0) {
        FAIL("expected projections_inserted > 0 (now enabled)");
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Idempotency and End-to-End Tests                                         */
/* ======================================================================== */

static void
test_jpp_idempotent(void)
{
    TEST("jpp: second pass is idempotent (no further reordering)");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
            ".decl b(y: int32, w: int32)\n"
            ".decl c(w: int32, z: int32)\n"
            ".decl path(x: int32, z: int32)\n"
            "path(x, z) :- a(x, y), c(w, z), b(y, w).\n",
            &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    /* First pass: should reorder */
    wl_jpp_stats_t stats1 = { 0, 0, 0 };
    int rc = wl_jpp_apply(prog, &stats1);
    if (rc != 0) {
        FAIL("first pass failed");
        wirelog_program_free(prog);
        return;
    }
    if (stats1.joins_reordered != 1) {
        FAIL("first pass should reorder");
        wirelog_program_free(prog);
        return;
    }

    /* Second pass: should be no-op */
    wl_jpp_stats_t stats2 = { 0, 0, 0 };
    rc = wl_jpp_apply(prog, &stats2);
    if (rc != 0) {
        FAIL("second pass failed");
        wirelog_program_free(prog);
        return;
    }
    if (stats2.joins_reordered != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "second pass should not reorder, got %u",
            stats2.joins_reordered);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_jpp_four_atom_reorder(void)
{
    TEST("jpp: 4-atom rule reordered correctly");

    /*
     * out(x, v) :- a(x, y), d(u, v), b(y, w), c(w, u).
     *
     * Naive left-to-right:
     *   JOIN(SCAN(a), SCAN(d)) -- 0 shared (cross-product!)
     *
     * Greedy:
     *   a(x,y)            -> {x,y}
     *   b(y,w) shares y   -> {x,y,w}
     *   c(w,u) shares w   -> {x,y,w,u}
     *   d(u,v) shares u   -> {x,y,w,u,v}
     *   No cross-products.
     */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, w: int32)\n"
        ".decl c(w: int32, u: int32)\n"
        ".decl d(u: int32, v: int32)\n"
        ".decl out(x: int32, v: int32)\n"
        "out(x, v) :- a(x, y), d(u, v), b(y, w), c(w, u).\n",
        &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_jpp_stats_t stats = { 0, 0, 0 };
    int rc = wl_jpp_apply(prog, &stats);

    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    if (stats.joins_reordered != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected joins_reordered=1, got %u",
            stats.joins_reordered);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    /* Verify no cross-products: every JOIN should have key_count > 0 */
    wirelog_ir_node_t *ir = find_relation_ir(prog, "out");
    if (!ir) {
        FAIL("no IR for 'out'");
        wirelog_program_free(prog);
        return;
    }

    wirelog_ir_node_t *root = find_join_root(ir);
    if (!root || root->type != WIRELOG_IR_JOIN) {
        FAIL("expected JOIN root");
        wirelog_program_free(prog);
        return;
    }

    /* Walk all JOINs and check each has keys */
    wirelog_ir_node_t *n = root;
    while (n && n->type == WIRELOG_IR_JOIN) {
        if (n->join_key_count == 0) {
            FAIL("found JOIN with no keys (cross-product)");
            wirelog_program_free(prog);
            return;
        }
        n = n->child_count > 0 ? n->children[0] : NULL;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* EDB Tie-Breaker Tests                                                   */
/* ======================================================================== */

/*
 * Return the relation_name of a scan node, descending through any intra-atom
 * FILTER wrapper that may have been inserted for duplicate-variable detection.
 */
static const char *
get_scan_relation(const wirelog_ir_node_t *node)
{
    while (node) {
        if (node->type == WIRELOG_IR_SCAN)
            return node->relation_name;
        if (node->type == WIRELOG_IR_FILTER && node->child_count > 0)
            node = node->children[0];
        else
            break;
    }
    return NULL;
}

static void
test_jpp_edb_tiebreak(void)
{
    TEST("jpp: EDB atom preferred over IDB on shared-var tie (issue #394)");

    /*
     * out(x, z) :- idb(x, y), IdbB(x, z), EdbA(y, z).
     *
     * idb  = IDB (derived by: idb(x,y)   :- seed_edb(x,y).)
     * IdbB = IDB (derived by: IdbB(x,z)  :- EdbA(x,z).)
     * EdbA = EDB (no rules)
     *
     * Greedy seed = idb (scan[0]), accumulated = {x, y}
     * Step 2 tie:
     *   IdbB(x,z) shares {x}  -> shared=1  (IDB)
     *   EdbA(y,z) shares {y}  -> shared=1  (EDB)
     * Without tie-break: IdbB wins (lower index 1).
     * With EDB tie-break: EdbA wins (EDB beats IDB on equal shared count).
     *
     * Verify: deepest JOIN's right child is EdbA, not IdbB.
     */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl seed_edb(x: int32, y: int32)\n"
        ".decl EdbA(y: int32, z: int32)\n"
        ".decl IdbB(x: int32, z: int32)\n"
        ".decl idb(x: int32, y: int32)\n"
        ".decl out(x: int32, z: int32)\n"
        "IdbB(x, z) :- EdbA(x, z).\n"
        "idb(x, y) :- seed_edb(x, y).\n"
        "out(x, z) :- idb(x, y), IdbB(x, z), EdbA(y, z).\n",
        &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_jpp_stats_t stats = { 0, 0, 0 };
    int rc = wl_jpp_apply(prog, &stats);
    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    wirelog_ir_node_t *ir = find_relation_ir(prog, "out");
    if (!ir) {
        FAIL("no IR for 'out'");
        wirelog_program_free(prog);
        return;
    }

    wirelog_ir_node_t *join_root = find_join_root(ir);
    if (!join_root || join_root->type != WIRELOG_IR_JOIN) {
        FAIL("expected JOIN at root");
        wirelog_program_free(prog);
        return;
    }

    /* Deepest join: children[1] is the 2nd atom in greedy order */
    wirelog_ir_node_t *deep = find_deepest_join(join_root);
    if (!deep || deep->child_count < 2) {
        FAIL("no deepest join or missing children");
        wirelog_program_free(prog);
        return;
    }

    const char *second = get_scan_relation(deep->children[1]);
    if (!second) {
        FAIL("could not get relation name of second scan");
        wirelog_program_free(prog);
        return;
    }

    if (strcmp(second, "EdbA") != 0) {
        char buf[128];
        snprintf(buf, sizeof(buf),
            "expected EdbA (EDB) as 2nd atom, got '%s' (EDB tie-break not applied)",
            second);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_jpp_idb_idb_tie_unchanged(void)
{
    TEST("jpp: two-IDB tie leaves original index order (no regression)");

    /*
     * out(x, z) :- idb(x, y), idb2(x, z), idb3(y, z).
     *
     * All three are IDB. At step 2 idb2 and idb3 both share 1 var.
     * The tie-breaker does NOT apply (neither side is EDB), so the
     * lower-index atom (idb2, index 1) is still chosen — unchanged
     * from pre-fix behavior.
     */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(
        ".decl edb(x: int32, y: int32)\n"
        ".decl idb(x: int32, y: int32)\n"
        ".decl idb2(x: int32, z: int32)\n"
        ".decl idb3(y: int32, z: int32)\n"
        ".decl out(x: int32, z: int32)\n"
        "idb(x, y) :- edb(x, y).\n"
        "idb2(x, z) :- edb(x, z).\n"
        "idb3(y, z) :- edb(y, z).\n"
        "out(x, z) :- idb(x, y), idb2(x, z), idb3(y, z).\n",
        &err);

    if (!prog) {
        FAIL("parse failed");
        return;
    }

    wl_jpp_stats_t stats = { 0, 0, 0 };
    int rc = wl_jpp_apply(prog, &stats);
    if (rc != 0) {
        FAIL("expected 0");
        wirelog_program_free(prog);
        return;
    }

    /* Both idb2 and idb3 are IDB, so tie-break should not apply.
     * The deepest join's right child should be idb2 (lower index 1). */
    wirelog_ir_node_t *ir = find_relation_ir(prog, "out");
    if (!ir) {
        FAIL("no IR for 'out'");
        wirelog_program_free(prog);
        return;
    }

    wirelog_ir_node_t *join_root = find_join_root(ir);
    if (!join_root || join_root->type != WIRELOG_IR_JOIN) {
        FAIL("expected JOIN at root");
        wirelog_program_free(prog);
        return;
    }

    wirelog_ir_node_t *deep = find_deepest_join(join_root);
    if (!deep || deep->child_count < 2) {
        FAIL("no deepest join or missing children");
        wirelog_program_free(prog);
        return;
    }

    const char *second = get_scan_relation(deep->children[1]);
    if (!second) {
        FAIL("could not get relation name of second scan");
        wirelog_program_free(prog);
        return;
    }

    if (strcmp(second, "idb2") != 0) {
        char buf[128];
        snprintf(buf, sizeof(buf),
            "expected idb2 as 2nd atom (IDB-IDB tie unchanged), got '%s'",
            second);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== test_jpp ===\n");

    /* Error handling */
    test_jpp_null_program();
    test_jpp_null_stats();
    test_jpp_no_ir_trees();

    /* No-op cases */
    test_jpp_single_atom_noop();
    test_jpp_two_atom_noop();

    /* Reorder */
    test_jpp_three_atom_reorder();

    /* Stats */
    test_jpp_already_optimal_three_atom();
    test_jpp_chains_examined_count();

    /* UNION and ANTIJOIN */
    test_jpp_union_recurse();
    test_jpp_antijoin_preserved();

    /* Projection insertion */
    test_jpp_intermediate_projection();

    /* Idempotency and end-to-end */
    test_jpp_idempotent();
    test_jpp_four_atom_reorder();

    /* EDB tie-breaker (issue #394) */
    test_jpp_edb_tiebreak();
    test_jpp_idb_idb_tie_unchanged();

    printf("\n  Total: %d  Passed: %d  Failed: %d\n\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
