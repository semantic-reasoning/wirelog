/*
 * test_fusion.c - Tests for Logic Fusion Optimization Pass
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 */

#include "../wirelog/passes/fusion.h"
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
/* Helper: find relation IR by name (uses internal struct)                  */
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

static uint32_t
count_type_in_program(struct wirelog_program *prog, wirelog_ir_node_type_t type)
{
    uint32_t total = 0;
    if (!prog || !prog->relation_irs)
        return 0;
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        total += count_type_in_tree(prog->relation_irs[i], type);
    }
    return total;
}

/* ======================================================================== */
/* Error Handling Tests                                                     */
/* ======================================================================== */

static void
test_fusion_null_program(void)
{
    TEST("fusion: NULL program returns -2");

    wl_fusion_stats_t stats = { 0, 0, 0 };
    int rc = wl_fusion_apply(NULL, &stats);

    if (rc != -2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected rc=-2, got %d", rc);
        FAIL(buf);
        return;
    }

    PASS();
}

static void
test_fusion_null_stats(void)
{
    TEST("fusion: NULL stats pointer accepted (no crash)");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n"
                                                   ".decl r(x: int32)\n"
                                                   "r(x) :- a(x).\n",
                                                   &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    int rc = wl_fusion_apply(prog, NULL);

    if (rc != 0) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected rc=0, got %d", rc);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Node Count Utility Tests                                                 */
/* ======================================================================== */

static void
test_count_nodes_null(void)
{
    TEST("count_nodes: NULL returns 0");

    uint32_t count = wl_fusion_count_nodes(NULL);

    if (count != 0) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 0, got %u", count);
        FAIL(buf);
        return;
    }

    PASS();
}

static void
test_count_nodes_simple(void)
{
    TEST("count_nodes: r(x) :- a(x). -> PROJECT(SCAN) = 2 nodes");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n"
                                                   ".decl r(x: int32)\n"
                                                   "r(x) :- a(x).\n",
                                                   &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wirelog_ir_node_t *ir = find_relation_ir(prog, "r");
    if (!ir) {
        wirelog_program_free(prog);
        FAIL("no relation IR for 'r'");
        return;
    }

    uint32_t count = wl_fusion_count_nodes(ir);

    /* Should be PROJECT(SCAN) = 2 nodes */
    if (count != 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 2, got %u", count);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Pattern 1: FILTER + PROJECT -> FLATMAP                                   */
/* ======================================================================== */

static void
test_fusion_filter_project(void)
{
    TEST("fusion: PROJECT(FILTER(SCAN)) -> FLATMAP(SCAN)");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl r(x: int32)\n"
                               "r(x) :- a(x, y), y > 0.\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    /* Before fusion: should have FILTER and PROJECT nodes, no FLATMAP */
    uint32_t flatmap_before = count_type_in_program(prog, WIRELOG_IR_FLATMAP);
    uint32_t filter_before = count_type_in_program(prog, WIRELOG_IR_FILTER);
    uint32_t project_before = count_type_in_program(prog, WIRELOG_IR_PROJECT);

    if (flatmap_before != 0) {
        wirelog_program_free(prog);
        FAIL("expected 0 FLATMAP before fusion");
        return;
    }

    if (filter_before < 1) {
        wirelog_program_free(prog);
        FAIL("expected >= 1 FILTER before fusion");
        return;
    }

    if (project_before < 1) {
        wirelog_program_free(prog);
        FAIL("expected >= 1 PROJECT before fusion");
        return;
    }

    /* Apply fusion */
    wl_fusion_stats_t stats = { 0, 0, 0 };
    int rc = wl_fusion_apply(prog, &stats);

    if (rc != 0) {
        char buf[100];
        snprintf(buf, sizeof(buf), "fusion failed: rc=%d", rc);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    /* After fusion: FILTER+PROJECT should be replaced by FLATMAP */
    uint32_t flatmap_after = count_type_in_program(prog, WIRELOG_IR_FLATMAP);
    uint32_t filter_after = count_type_in_program(prog, WIRELOG_IR_FILTER);
    uint32_t project_after = count_type_in_program(prog, WIRELOG_IR_PROJECT);

    if (flatmap_after < 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected >= 1 FLATMAP after fusion, got %u",
                 flatmap_after);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    if (filter_after != 0) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 0 FILTER after fusion, got %u",
                 filter_after);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    if (project_after != 0) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 0 PROJECT after fusion, got %u",
                 project_after);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    /* Stats should reflect 1 fusion */
    if (stats.fusions_applied < 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected >= 1 fusions_applied, got %u",
                 stats.fusions_applied);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_fusion_filter_project_preserves_scan(void)
{
    TEST("fusion: FLATMAP child is still SCAN after fusion");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl r(x: int32)\n"
                               "r(x) :- a(x, y), y > 0.\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    int rc = wl_fusion_apply(prog, NULL);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("fusion failed");
        return;
    }

    /* The relation IR root should be FLATMAP */
    wirelog_ir_node_t *ir = find_relation_ir(prog, "r");
    if (!ir) {
        wirelog_program_free(prog);
        FAIL("no relation IR after fusion");
        return;
    }

    if (ir->type != WIRELOG_IR_FLATMAP) {
        wirelog_program_free(prog);
        FAIL("root should be FLATMAP after fusion");
        return;
    }

    /* FLATMAP should have 1 child: the SCAN */
    if (ir->child_count != 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "FLATMAP should have 1 child, got %u",
                 ir->child_count);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    if (!ir->children[0] || ir->children[0]->type != WIRELOG_IR_SCAN) {
        wirelog_program_free(prog);
        FAIL("FLATMAP child should be SCAN");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Stats Accuracy Tests                                                     */
/* ======================================================================== */

static void
test_fusion_stats_node_reduction(void)
{
    TEST("fusion: stats show node count reduction");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl r(x: int32)\n"
                               "r(x) :- a(x, y), y > 0.\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_fusion_stats_t stats = { 0, 0, 0 };
    int rc = wl_fusion_apply(prog, &stats);

    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("fusion failed");
        return;
    }

    /* Before: PROJECT(FILTER(SCAN)) = 3 nodes
     * After: FLATMAP(SCAN) = 2 nodes */
    if (stats.nodes_before != 3) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected nodes_before=3, got %u",
                 stats.nodes_before);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    if (stats.nodes_after != 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected nodes_after=2, got %u",
                 stats.nodes_after);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    if (stats.nodes_after >= stats.nodes_before) {
        wirelog_program_free(prog);
        FAIL("node count should decrease after fusion");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* No-Op Cases (fusion should not apply)                                    */
/* ======================================================================== */

static void
test_fusion_scan_only_noop(void)
{
    TEST("fusion: r(x) :- a(x). -> no fusion (PROJECT(SCAN) kept)");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n"
                                                   ".decl r(x: int32)\n"
                                                   "r(x) :- a(x).\n",
                                                   &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_fusion_stats_t stats = { 0, 0, 0 };
    int rc = wl_fusion_apply(prog, &stats);

    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("fusion should succeed even with nothing to fuse");
        return;
    }

    /* PROJECT(SCAN) without FILTER -- no fusion should apply
     * (Pattern 1 requires FILTER+PROJECT) */
    if (stats.fusions_applied != 0) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 0 fusions for simple scan, got %u",
                 stats.fusions_applied);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_fusion_aggregate_noop(void)
{
    TEST("fusion: aggregate rule -> no fusion");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl r(x: int32, c: int32)\n"
                               "r(x, count(y)) :- a(x, y).\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_fusion_stats_t stats = { 0, 0, 0 };
    int rc = wl_fusion_apply(prog, &stats);

    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("fusion should succeed with no fusible patterns");
        return;
    }

    /* AGGREGATE nodes should not be fused */
    if (stats.fusions_applied != 0) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 0 fusions for aggregate, got %u",
                 stats.fusions_applied);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Join + Filter + Project Fusion                                           */
/* ======================================================================== */

static void
test_fusion_join_filter_project(void)
{
    TEST("fusion: PROJECT(FILTER(JOIN)) -> FLATMAP(JOIN)");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl b(y: int32, z: int32)\n"
                               ".decl r(x: int32)\n"
                               "r(x) :- a(x, y), b(y, z), z > 0.\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    /* Apply fusion */
    wl_fusion_stats_t stats = { 0, 0, 0 };
    int rc = wl_fusion_apply(prog, &stats);

    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("fusion failed");
        return;
    }

    /* After fusion: should have FLATMAP, no standalone FILTER+PROJECT */
    uint32_t flatmap_count = count_type_in_program(prog, WIRELOG_IR_FLATMAP);
    uint32_t filter_count = count_type_in_program(prog, WIRELOG_IR_FILTER);
    uint32_t project_count = count_type_in_program(prog, WIRELOG_IR_PROJECT);

    if (flatmap_count < 1) {
        wirelog_program_free(prog);
        FAIL("expected FLATMAP after join+filter+project fusion");
        return;
    }

    if (filter_count != 0) {
        wirelog_program_free(prog);
        FAIL("FILTER should be consumed by fusion");
        return;
    }

    if (project_count != 0) {
        wirelog_program_free(prog);
        FAIL("PROJECT should be consumed by fusion");
        return;
    }

    if (stats.fusions_applied < 1) {
        wirelog_program_free(prog);
        FAIL("expected >= 1 fusions_applied");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* UNION with fusible children                                              */
/* ======================================================================== */

static void
test_fusion_union_children(void)
{
    TEST("fusion: UNION of filter+project rules -> 2 FLATMAP children");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl b(x: int32, z: int32)\n"
                               ".decl r(x: int32)\n"
                               "r(x) :- a(x, y), y > 0.\n"
                               "r(x) :- b(x, z), z > 0.\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_fusion_stats_t stats = { 0, 0, 0 };
    int rc = wl_fusion_apply(prog, &stats);

    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("fusion failed");
        return;
    }

    /* Both branches should be fused */
    if (stats.fusions_applied < 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected >= 2 fusions for union, got %u",
                 stats.fusions_applied);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    /* No remaining FILTER or PROJECT */
    uint32_t filter_count = count_type_in_program(prog, WIRELOG_IR_FILTER);
    uint32_t project_count = count_type_in_program(prog, WIRELOG_IR_PROJECT);

    if (filter_count != 0 || project_count != 0) {
        wirelog_program_free(prog);
        FAIL("all FILTER/PROJECT should be fused in both branches");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* FLATMAP Data Integrity                                                   */
/* ======================================================================== */

static void
test_fusion_flatmap_has_filter_expr(void)
{
    TEST("fusion: FLATMAP inherits filter_expr from FILTER");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl r(x: int32)\n"
                               "r(x) :- a(x, y), y > 0.\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    int rc = wl_fusion_apply(prog, NULL);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("fusion failed");
        return;
    }

    wirelog_ir_node_t *ir = find_relation_ir(prog, "r");
    if (!ir || ir->type != WIRELOG_IR_FLATMAP) {
        wirelog_program_free(prog);
        FAIL("root should be FLATMAP");
        return;
    }

    if (!ir->filter_expr) {
        wirelog_program_free(prog);
        FAIL("FLATMAP should have non-NULL filter_expr");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_fusion_flatmap_has_project_indices(void)
{
    TEST("fusion: FLATMAP inherits project_indices from PROJECT");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl r(x: int32)\n"
                               "r(x) :- a(x, y), y > 0.\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    int rc = wl_fusion_apply(prog, NULL);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("fusion failed");
        return;
    }

    wirelog_ir_node_t *ir = find_relation_ir(prog, "r");
    if (!ir || ir->type != WIRELOG_IR_FLATMAP) {
        wirelog_program_free(prog);
        FAIL("root should be FLATMAP");
        return;
    }

    if (ir->project_count == 0) {
        wirelog_program_free(prog);
        FAIL("FLATMAP should have project_count > 0");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Idempotency                                                              */
/* ======================================================================== */

static void
test_fusion_idempotent(void)
{
    TEST("fusion: applying twice yields same result");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl a(x: int32, y: int32)\n"
                               ".decl r(x: int32)\n"
                               "r(x) :- a(x, y), y > 0.\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    wl_fusion_stats_t stats1 = { 0, 0, 0 };
    int rc = wl_fusion_apply(prog, &stats1);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("first fusion failed");
        return;
    }

    wl_fusion_stats_t stats2 = { 0, 0, 0 };
    rc = wl_fusion_apply(prog, &stats2);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("second fusion failed");
        return;
    }

    /* Second pass should apply 0 fusions (already fused) */
    if (stats2.fusions_applied != 0) {
        char buf[100];
        snprintf(buf, sizeof(buf), "second pass should apply 0 fusions, got %u",
                 stats2.fusions_applied);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    /* Node counts should be identical */
    if (stats2.nodes_before != stats2.nodes_after) {
        wirelog_program_free(prog);
        FAIL("second pass should not change node count");
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
    printf("\n=== wirelog Logic Fusion Tests ===\n\n");

    /* Error handling */
    test_fusion_null_program();
    test_fusion_null_stats();

    /* Node count utility */
    test_count_nodes_null();
    test_count_nodes_simple();

    /* Pattern 1: FILTER + PROJECT -> FLATMAP */
    test_fusion_filter_project();
    test_fusion_filter_project_preserves_scan();

    /* Stats accuracy */
    test_fusion_stats_node_reduction();

    /* No-op cases */
    test_fusion_scan_only_noop();
    test_fusion_aggregate_noop();

    /* Join + Filter + Project */
    test_fusion_join_filter_project();

    /* UNION children */
    test_fusion_union_children();

    /* FLATMAP data integrity */
    test_fusion_flatmap_has_filter_expr();
    test_fusion_flatmap_has_project_indices();

    /* Idempotency */
    test_fusion_idempotent();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
