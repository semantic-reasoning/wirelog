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

#include <stdarg.h>
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
/* Wide-relation scratch sizing (Issue #1002)                               */
/* ======================================================================== */

/*
 * jpp sized its scratch arrays from fixed per-scan constants (nscan * 16,
 * nscan * 32) rather than from the chain's actual column count, so a chain
 * of three or more atoms over wide relations wrote past the end of them.
 *
 * The shapes below are far too wide to write out literally, so the sources
 * are generated.  This target links no evaluator, so these cases can only
 * assert that wl_jpp_apply() completes and reports sane statistics -- never
 * answer correctness.  tests/test_wide_relation.c covers the answers.
 */

typedef struct {
    char *data;
    size_t len;
    size_t cap;
    int oom;
} sbuf_t;

static void
sb_init(sbuf_t *s)
{
    s->data = NULL;
    s->len = 0;
    s->cap = 0;
    s->oom = 0;
}

static void
sb_free(sbuf_t *s)
{
    free(s->data);
    sb_init(s);
}

static void
sb_reserve(sbuf_t *s, size_t extra)
{
    if (s->oom)
        return;
    if (s->len + extra + 1 <= s->cap)
        return;
    size_t cap = s->cap ? s->cap : 1024;
    while (cap < s->len + extra + 1)
        cap *= 2;
    char *p = (char *)realloc(s->data, cap);
    if (!p) {
        s->oom = 1;
        return;
    }
    s->data = p;
    s->cap = cap;
}

static void
sb_puts(sbuf_t *s, const char *text)
{
    size_t n = strlen(text);
    sb_reserve(s, n);
    if (s->oom)
        return;
    memcpy(s->data + s->len, text, n);
    s->len += n;
    s->data[s->len] = '\0';
}

static void
sb_printf(sbuf_t *s, const char *fmt, ...)
{
    char tmp[256];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    if (n < 0 || (size_t)n >= sizeof(tmp)) {
        s->oom = 1;
        return;
    }
    sb_puts(s, tmp);
}

/*
 * One body atom.  Column j is named:
 *   - the same as column 0, when @dup is set and j == 1 (a variable repeated
 *     inside a single atom, which program.c wraps in a FILTER -- the leaf
 *     collect_scans() then returns carries column_count == 0);
 *   - "s<j>"      when j < @shared (drawn from the pool shared by all atoms);
 *   - "v<k>_<j>"  otherwise (private to atom k).
 */
typedef struct {
    unsigned cols;
    unsigned shared;
    int dup;
} atom_t;

static void
sb_var(sbuf_t *s, unsigned k, const atom_t *a, unsigned j)
{
    unsigned src = (a->dup && j == 1) ? 0u : j;
    if (src < a->shared)
        sb_printf(s, "s%u", src);
    else
        sb_printf(s, "v%u_%u", k, src);
}

static void
sb_decl(sbuf_t *s, const char *name, unsigned n)
{
    sb_printf(s, ".decl %s(", name);
    for (unsigned i = 0; i < n; i++)
        sb_printf(s, "%sc%u: int64", i ? ", " : "", i);
    sb_puts(s, ")\n");
}

/*
 * ".decl r0(..)  ..  .decl o(..)   o(<head_args>) :- r0(..), .., r{n-1}(..)."
 */
static void
build_join_src(sbuf_t *s, const atom_t *atoms, unsigned natoms,
    const char *head_args, unsigned head_cols)
{
    for (unsigned k = 0; k < natoms; k++) {
        char name[16];
        snprintf(name, sizeof(name), "r%u", k);
        sb_decl(s, name, atoms[k].cols);
    }
    sb_decl(s, "o", head_cols);

    /* sb_puts, not sb_printf: a 51-variable head is longer than sb_printf's
     * formatting buffer. */
    sb_puts(s, "o(");
    sb_puts(s, head_args);
    sb_puts(s, ") :- ");
    for (unsigned k = 0; k < natoms; k++) {
        sb_printf(s, "%sr%u(", k ? ", " : "", k);
        for (unsigned j = 0; j < atoms[k].cols; j++) {
            if (j)
                sb_puts(s, ", ");
            sb_var(s, k, &atoms[k], j);
        }
        sb_puts(s, ")");
    }
    sb_puts(s, ".\n");
}

/*
 * Parse @src and run wl_jpp_apply on it.  Returns 0 and fills @stats on
 * success; on failure fills @msg and returns -1.
 */
static int
run_jpp(const char *src, wl_jpp_stats_t *stats, char *msg, size_t msg_size)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);

    if (!prog) {
        snprintf(msg, msg_size, "parse failed");
        return -1;
    }

    wl_jpp_stats_t local = { 0, 0, 0 };
    int rc = wl_jpp_apply(prog, &local);
    wirelog_program_free(prog);

    if (rc != 0) {
        snprintf(msg, msg_size, "wl_jpp_apply returned %d, expected 0", rc);
        return -1;
    }
    *stats = local;
    return 0;
}

/*
 * Shared driver: build the shape, run the pass, and require that it survives
 * with chains_examined == 1 and exactly @expect_projections intermediate
 * PROJECTs.
 *
 * Asserting projections_inserted is the load-bearing half.  chains_examined
 * is incremented in optimize_tree() before any of the scratch allocations,
 * so it stays 1 even when every one of them fails and the pass silently
 * gives up on the chain -- it cannot tell "optimized correctly" from "did
 * nothing".  projections_inserted can.
 */
static void
check_wide_shape(const char *name, const atom_t *atoms, unsigned natoms,
    const char *head_args, unsigned head_cols, uint32_t expect_projections)
{
    TEST(name);

    sbuf_t s;
    sb_init(&s);
    build_join_src(&s, atoms, natoms, head_args, head_cols);
    if (s.oom) {
        sb_free(&s);
        FAIL("source generation OOM");
        return;
    }

    wl_jpp_stats_t stats = { 0, 0, 0 };
    char msg[256];
    if (run_jpp(s.data, &stats, msg, sizeof(msg)) != 0) {
        sb_free(&s);
        FAIL(msg);
        return;
    }
    sb_free(&s);

    if (stats.chains_examined != 1
        || stats.projections_inserted != expect_projections) {
        char buf[160];
        snprintf(buf, sizeof(buf),
            "expected chains=1 projections=%u, got chains=%u projections=%u",
            expect_projections, stats.chains_examined,
            stats.projections_inserted);
        FAIL(buf);
        return;
    }
    PASS();
}

/* n atoms of @cols all-distinct columns; head projects one body variable. */
static void
check_all_distinct(unsigned natoms, unsigned cols, uint32_t expect_projections)
{
    atom_t atoms[8];
    char name[160];

    /* Caller bug, not a shape the pass declines to handle: silently
     * returning here would report a pass the test never ran. */
    if (natoms > sizeof(atoms) / sizeof(atoms[0])) {
        TEST("jpp #1002: check_all_distinct atom budget");
        FAIL("natoms exceeds the atoms[] buffer; raise it");
        return;
    }
    for (unsigned k = 0; k < natoms; k++) {
        atoms[k].cols = cols;
        atoms[k].shared = 0;
        atoms[k].dup = 0;
    }
    snprintf(name, sizeof(name),
        "jpp #1002: %u atoms x %u all-distinct columns", natoms, cols);
    check_wide_shape(name, atoms, natoms, "v0_0", 1, expect_projections);
}

static void
test_jpp_wide_greedy_acc(void)
{
    /*
     * greedy_order()'s acc[] was sized `acc_count + nscan * 16` and then
     * filled with one entry per column of every later-selected scan, so it
     * overruns once the chain's total column count passes that bound.
     * 3 atoms: clean at 24 columns, heap-buffer-overflow WRITE at 25.
     * 4 atoms: clean at 21, overflow at 22.
     */
    check_all_distinct(3, 24, 1);
    check_all_distinct(3, 25, 1);
    check_all_distinct(4, 21, 2);
    check_all_distinct(4, 22, 2);
}

static void
test_jpp_wide_projection_seed(void)
{
    /*
     * insert_projections()'s acc[] was sized `nscan * 16` with no room for
     * the seed copy of scans[0]'s columns, so a 3-atom chain whose first
     * atom alone is wider than 3 * 16 overruns on the seed copy -- before
     * any merging happens.
     */
    atom_t atoms[3] = { { 50, 0, 0 }, { 2, 0, 0 }, { 2, 0, 0 } };

    check_wide_shape(
        "jpp #1002: 3-atom chain, 50-column first atom (seed copy)",
        atoms, 3, "v0_0", 1, 1);
}

static void
test_jpp_wide_projection_merge(void)
{
    /*
     * The same acc[]: seed copy fits (40 <= 48) but merging the second
     * atom's 11 new variables runs past the end.
     */
    atom_t atoms[3] = { { 40, 1, 0 }, { 12, 1, 0 }, { 4, 0, 0 } };

    check_wide_shape("jpp #1002: 3-atom chain, 40+12 columns (acc merge)",
        atoms, 3, "s0", 1, 1);
}

static void
test_jpp_wide_physical_layout(void)
{
    /*
     * phys_names[] was sized `nscan * 32` (96 here) but receives every
     * column of every scan: 40 + 40 for the first two atoms, then 30 more
     * for the third -- 110 writes.
     *
     * Every accumulated variable is live here (the head keeps the 11 that
     * the third atom does not mention), so no PROJECT is inserted and the
     * layout is never compacted.
     *
     * This one overruns phys_names[]. The sanitizer job
     * (-Db_sanitize=address,undefined) is the reliable detector and the one
     * to trust.  A release run is not guaranteed to be silent either:
     * measured here with only this site reverted, the release build aborted
     * at exactly this case with glibc's "corrupted size vs. prev_size"
     * (rc=134).  Whether glibc trips at all depends on its version and on
     * the allocation layout, so a green release run is not evidence the fix
     * is present -- but neither is a red one a surprise.
     */
    atom_t atoms[3] = { { 40, 39, 0 }, { 40, 39, 0 }, { 30, 30, 0 } };
    sbuf_t head;

    sb_init(&head);
    for (unsigned i = 30; i < 39; i++)
        sb_printf(&head, "%ss%u", i > 30 ? ", " : "", i);
    sb_puts(&head, ", v0_39, v1_39");
    if (head.oom) {
        sb_free(&head);
        TEST("jpp #1002: 3-atom chain, physical layout tracking");
        FAIL("head generation OOM");
        return;
    }

    check_wide_shape("jpp #1002: 3-atom chain, physical layout tracking",
        atoms, 3, head.data, 11, 0);
    sb_free(&head);
}

static void
test_jpp_wide_needed_set(void)
{
    /*
     * The per-iteration needed[] was sized `nscan * 16` (48 here) but is
     * filled with the head variables first -- its bound omitted
     * head_var_count entirely, so a head of 51 variables overruns it before
     * a single future-scan column is added.
     *
     * Measured pre-fix: heap-buffer-overflow WRITE at jpp.c:638 under ASAN.
     * As with the case above, the sanitizer job is the reliable detector.
     * With only this site reverted the release build also aborted (glibc
     * "malloc(): unaligned tcache chunk detected", rc=134) -- but after this
     * case had already reported PASS, at the next allocation, not here.  With
     * the site above reverted as well the run dies earlier still, at that
     * case, and never reaches this one.  Allocator-layout luck, not a
     * guarantee: do not read a green non-sanitizer run as coverage.
     */
    atom_t atoms[3] = { { 17, 0, 0 }, { 17, 0, 0 }, { 17, 0, 0 } };
    sbuf_t head;

    sb_init(&head);
    for (unsigned k = 0; k < 3; k++) {
        for (unsigned j = 0; j < 17; j++)
            sb_printf(&head, "%sv%u_%u", (k || j) ? ", " : "", k, j);
    }
    if (head.oom) {
        sb_free(&head);
        TEST("jpp #1002: 3-atom chain, 51-variable head");
        FAIL("head generation OOM");
        return;
    }

    check_wide_shape("jpp #1002: 3-atom chain, 51-variable head (needed set)",
        atoms, 3, head.data, 51, 0);
    sb_free(&head);
}

static void
test_jpp_wide_filter_wrapped_leaves(void)
{
    /*
     * Atoms 2 and 3 repeat their first variable, so program.c wraps those
     * SCANs in a FILTER and collect_scans() returns the FILTER as the leaf.
     * Such a leaf carries column_count == 0, so any capacity computed from
     * the leaf's own column_count would be far too small; the width has to
     * come through scan_vars(), which skips the wrapper.
     *
     * The shape is deliberately MIXED -- atom 1 is a bare SCAN, atoms 2 and
     * 3 are wrapped -- and that matters for what this case can prove.
     *
     * An all-wrapped chain makes the mis-computed total 0, which is now
     * clamped to 1 (see scan_columns_total): still undersized, so it would
     * also be caught.  But it would be caught only via that clamp.  Before
     * the clamp existed, a 0 total was treated as an allocation failure and
     * the pass declined the chain, so the all-wrapped shape sailed past a
     * mutation of scan_columns_total() to scans[i]->column_count with
     * nothing but chains_examined to show for it.  The mixed shape does not
     * depend on that: one bare 60-column leaf makes the mis-computed total
     * 60 against a real 180, an ordinary undersized array that gets written
     * past no matter how the zero case is handled.  It is also the shape
     * real programs produce -- some atoms repeat a variable, most do not.
     *
     * Measured with the mutation applied: heap-buffer-overflow WRITE of
     * size 8 in greedy_order, from this case.
     */
    atom_t atoms[3] = { { 60, 0, 0 }, { 60, 0, 1 }, { 60, 0, 1 } };

    check_wide_shape("jpp #1002: 3-atom chain, bare + 2 FILTER-wrapped "
        "60-column leaves", atoms, 3, "v0_0", 1, 1);
}

static void
test_jpp_wide_two_atom_noop(void)
{
    /*
     * Negative control.  optimize_chain() returns before any of the scratch
     * allocations when nscan < 3, so a two-atom chain is clean at every
     * width.  That contradicts issue #1002's claim that "wider relations
     * will fail at 2 [atoms]": the early return is at the top of
     * optimize_chain(), so no scratch array is ever allocated, let alone
     * overrun, below three atoms.  chains_examined still counts the chain.
     */
    atom_t atoms[2] = { { 40, 0, 0 }, { 40, 0, 0 } };

    check_wide_shape("jpp #1002: 2-atom chain x 40 columns is unaffected",
        atoms, 2, "v0_0", 1, 0);
}

/* Append the SCAN relation names of @n, left to right, to @s. */
static void
sb_scan_order(sbuf_t *s, const wirelog_ir_node_t *n)
{
    if (!n)
        return;
    if (n->type == WIRELOG_IR_SCAN) {
        sb_printf(s, "%s%s", s->len ? " " : "",
            n->relation_name ? n->relation_name : "(null)");
        return;
    }
    for (uint32_t i = 0; i < n->child_count; i++)
        sb_scan_order(s, n->children[i]);
}

static void
test_jpp_nullary_chain(void)
{
    TEST("jpp #1002: nullary chain still reordered (.decl p() is legal)");

    /*
     * `.decl p()` parses and leaves column_count == 0 (wirelog/ir/program.h),
     * and a zero-arity fact against it is accepted (tests/test_program.c), so
     * a chain whose every atom is nullary totals ZERO scan columns.
     *
     * That is a legitimate chain, not an error.  Sizing the scratch arrays
     * from the column total must not fold "total == 0" into the
     * allocation-failure path: doing so silently disables the pass for these
     * programs.  Measured against unpatched origin/main, that regressed this
     * exact rule from reordered=1 / scan order "a e b" to reordered=0 / scan
     * order "a b e" -- the EDB tie-break of issue #394 no longer firing,
     * because e (an EDB) was never preferred over b (an IDB) on the
     * zero-shared-variable tie.
     *
     * Answers are unaffected here (a nullary join is a cross product), so
     * nothing but the plan shows the difference.  Pin the plan.
     */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl e()\n"
            ".decl a()\n"
            ".decl b()\n"
            ".decl o(x: int64)\n"
            "e().\n"
            "a() :- e().\n"
            "b() :- e().\n"
            "o(1) :- a(), b(), e().\n",
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

    if (stats.chains_examined != 1 || stats.joins_reordered != 1
        || stats.projections_inserted != 0) {
        char buf[160];
        snprintf(buf, sizeof(buf),
            "expected chains=1 reordered=1 projections=0, got %u/%u/%u",
            stats.chains_examined, stats.joins_reordered,
            stats.projections_inserted);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    sbuf_t order;
    sb_init(&order);
    sb_scan_order(&order, find_relation_ir(prog, "o"));
    if (order.oom || !order.data || strcmp(order.data, "a e b") != 0) {
        char buf[160];
        snprintf(buf, sizeof(buf), "expected scan order \"a e b\", got \"%s\"",
            order.data ? order.data : "(oom)");
        FAIL(buf);
        sb_free(&order);
        wirelog_program_free(prog);
        return;
    }
    sb_free(&order);

    wirelog_program_free(prog);
    PASS();
}

static void
test_jpp_plan_unchanged_narrow(void)
{
    TEST("jpp: narrow 3-atom plan (order and projection count) unchanged");

    /*
     * Plan-equivalence guard for the re-sizing above.  Correct capacities
     * must not change any decision the pass makes, so pin both outputs on a
     * shape narrow enough that neither the old nor the new bound is ever
     * reached: the greedy order (a, b, c -- c last, since it shares nothing
     * with a) and the exact number of intermediate projections.
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

    if (stats.chains_examined != 1 || stats.joins_reordered != 1
        || stats.projections_inserted != 1) {
        char buf[160];
        snprintf(buf, sizeof(buf),
            "expected chains=1 reordered=1 projections=1, got %u/%u/%u",
            stats.chains_examined, stats.joins_reordered,
            stats.projections_inserted);
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    wirelog_ir_node_t *ir = find_relation_ir(prog, "path");
    wirelog_ir_node_t *join_root = ir ? find_join_root(ir) : NULL;
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

    const char *first = get_scan_relation(deep->children[0]);
    const char *second = get_scan_relation(deep->children[1]);
    const char *third = join_root->child_count >= 2
        ? get_scan_relation(join_root->children[1]) : NULL;

    if (!first || !second || !third || strcmp(first, "a") != 0
        || strcmp(second, "b") != 0 || strcmp(third, "c") != 0) {
        char buf[160];
        snprintf(buf, sizeof(buf),
            "expected scan order a, b, c; got %s, %s, %s",
            first ? first : "(null)", second ? second : "(null)",
            third ? third : "(null)");
        FAIL(buf);
        wirelog_program_free(prog);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/*
 * The fifth site: head_vars[] in optimize_tree(), which was a fixed
 * char *[64] on the stack.  This one case pins both halves of it.
 *
 *   o(h0..h64) :- r0(h0..h31, j), r1(j, h32..h64, k), r2(j), !neg(k).
 *
 *   IR: PROJECT(ANTIJOIN(JOIN(JOIN(r0, r1), r2), SCAN(neg)))
 *
 * head_vars must end up holding 66 names: the 65 projected h* variables,
 * plus k, which only the ANTIJOIN's join key contributes.  The chain's
 * accumulated set after the first join is exactly those 66 plus j, and j is
 * kept alive by r2, so every accumulated variable is live and NO
 * intermediate PROJECT is inserted.  projections_inserted == 0 is therefore
 * the assertion that the head set was collected in full.
 *
 * Drop any one name from it and the corresponding column looks dead, so a
 * PROJECT appears and the count goes to 1.  Two independent ways to drop
 * one:
 *
 *   - a head_vars capacity below 66 (the old fixed 64 truncates two: h64
 *     and k).  This is the only case in this file with a head wider than
 *     64, so without it site 5 is pinned nowhere in its own test target.
 *   - count_head_vars() failing to account for the ANTIJOIN join keys
 *     (`count += node->join_key_count`).  The 65 PROJECT variables fill the
 *     array exactly, and collect_head_vars()'s `count < max` guard then
 *     discards k.
 *
 * Both are undercounts of the head set, which is the failure this pre-pass
 * exists to prevent.  Note what the guard converts them into: truncation and
 * a wrong plan, not a buffer overrun -- see the comment on
 * collect_head_vars().
 */
static void
test_jpp_wide_head_antijoin_key(void)
{
    TEST("jpp #1002: 65-variable head plus an ANTIJOIN-only variable");

    sbuf_t s;
    sb_init(&s);

    /* r0(h0..h31, j) */
    sb_puts(&s, ".decl r0(");
    for (unsigned i = 0; i < 32; i++)
        sb_printf(&s, "c%u: int64, ", i);
    sb_puts(&s, "cj: int64)\n");
    /* r1(j, h32..h64, k) */
    sb_puts(&s, ".decl r1(cj: int64, ");
    for (unsigned i = 32; i < 65; i++)
        sb_printf(&s, "c%u: int64, ", i);
    sb_puts(&s, "ck: int64)\n");
    sb_puts(&s, ".decl r2(cj: int64)\n");
    sb_puts(&s, ".decl neg(ck: int64)\n");
    sb_puts(&s, ".decl o(");
    for (unsigned i = 0; i < 65; i++)
        sb_printf(&s, "%sc%u: int64", i ? ", " : "", i);
    sb_puts(&s, ")\n");

    sb_puts(&s, "o(");
    for (unsigned i = 0; i < 65; i++)
        sb_printf(&s, "%sh%u", i ? ", " : "", i);
    sb_puts(&s, ") :- r0(");
    for (unsigned i = 0; i < 32; i++)
        sb_printf(&s, "h%u, ", i);
    sb_puts(&s, "j), r1(j, ");
    for (unsigned i = 32; i < 65; i++)
        sb_printf(&s, "h%u, ", i);
    sb_puts(&s, "k), r2(j), !neg(k).\n");

    if (s.oom) {
        sb_free(&s);
        FAIL("source generation OOM");
        return;
    }

    wl_jpp_stats_t stats = { 0, 0, 0 };
    char msg[256];
    if (run_jpp(s.data, &stats, msg, sizeof(msg)) != 0) {
        sb_free(&s);
        FAIL(msg);
        return;
    }
    sb_free(&s);

    if (stats.chains_examined != 1 || stats.projections_inserted != 0) {
        char buf[192];
        snprintf(buf, sizeof(buf),
            "expected chains=1 projections=0 (every accumulated variable "
            "live), got chains=%u projections=%u",
            stats.chains_examined, stats.projections_inserted);
        FAIL(buf);
        return;
    }
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

    /* Wide-relation scratch sizing (issue #1002) */
    test_jpp_wide_greedy_acc();
    test_jpp_wide_projection_seed();
    test_jpp_wide_projection_merge();
    test_jpp_wide_physical_layout();
    test_jpp_wide_needed_set();
    test_jpp_wide_filter_wrapped_leaves();
    test_jpp_wide_two_atom_noop();
    test_jpp_wide_head_antijoin_key();
    test_jpp_nullary_chain();
    test_jpp_plan_unchanged_narrow();

    printf("\n  Total: %d  Passed: %d  Failed: %d\n\n", tests_run, tests_passed,
        tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
