/*
 * test_ir.c - wirelog IR Node Test Suite
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests written first (TDD) before IR implementation.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/wirelog-ir.h"
#include "../wirelog/ir/ir.h"

/* ======================================================================== */
/* Test Helpers                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                      \
    do {                                \
        tests_run++;                    \
        printf("  [TEST] %-55s", name); \
        fflush(stdout);                 \
    } while (0)

#define PASS()             \
    do {                   \
        tests_passed++;    \
        printf(" PASS\n"); \
    } while (0)

#define FAIL(msg)                   \
    do {                            \
        tests_failed++;             \
        printf(" FAIL: %s\n", msg); \
    } while (0)

/* ======================================================================== */
/* IR Node Creation Tests                                                   */
/* ======================================================================== */

static void
test_create_scan_node(void)
{
    TEST("Create SCAN node with relation name");

    wirelog_ir_node_t *node = wl_ir_node_create(WIRELOG_IR_SCAN);
    if (!node) {
        FAIL("node is NULL");
        return;
    }

    wl_ir_node_set_relation(node, "Arc");

    if (wirelog_ir_node_get_type(node) != WIRELOG_IR_SCAN) {
        wl_ir_node_free(node);
        FAIL("type is not SCAN");
        return;
    }

    const char *name = wirelog_ir_node_get_relation_name(node);
    if (!name || strcmp(name, "Arc") != 0) {
        wl_ir_node_free(node);
        FAIL("relation name mismatch");
        return;
    }

    if (wirelog_ir_node_get_child_count(node) != 0) {
        wl_ir_node_free(node);
        FAIL("SCAN should have 0 children");
        return;
    }

    wl_ir_node_free(node);
    PASS();
}

static void
test_create_project_node(void)
{
    TEST("Create PROJECT node with child");

    wirelog_ir_node_t *scan = wl_ir_node_create(WIRELOG_IR_SCAN);
    wl_ir_node_set_relation(scan, "Arc");

    wirelog_ir_node_t *proj = wl_ir_node_create(WIRELOG_IR_PROJECT);
    wl_ir_node_set_relation(proj, "Tc");
    wl_ir_node_add_child(proj, scan);

    if (wirelog_ir_node_get_type(proj) != WIRELOG_IR_PROJECT) {
        wl_ir_node_free(proj);
        FAIL("type is not PROJECT");
        return;
    }

    if (wirelog_ir_node_get_child_count(proj) != 1) {
        wl_ir_node_free(proj);
        FAIL("PROJECT should have 1 child");
        return;
    }

    const wirelog_ir_node_t *child = wirelog_ir_node_get_child(proj, 0);
    if (!child || wirelog_ir_node_get_type(child) != WIRELOG_IR_SCAN) {
        wl_ir_node_free(proj);
        FAIL("child should be SCAN");
        return;
    }

    wl_ir_node_free(proj);
    PASS();
}

static void
test_create_join_node(void)
{
    TEST("Create JOIN node with two children");

    wirelog_ir_node_t *scan1 = wl_ir_node_create(WIRELOG_IR_SCAN);
    wl_ir_node_set_relation(scan1, "Tc");

    wirelog_ir_node_t *scan2 = wl_ir_node_create(WIRELOG_IR_SCAN);
    wl_ir_node_set_relation(scan2, "Arc");

    wirelog_ir_node_t *join = wl_ir_node_create(WIRELOG_IR_JOIN);
    wl_ir_node_add_child(join, scan1);
    wl_ir_node_add_child(join, scan2);

    if (wirelog_ir_node_get_child_count(join) != 2) {
        wl_ir_node_free(join);
        FAIL("JOIN should have 2 children");
        return;
    }

    const wirelog_ir_node_t *left = wirelog_ir_node_get_child(join, 0);
    const wirelog_ir_node_t *right = wirelog_ir_node_get_child(join, 1);

    if (!left || strcmp(wirelog_ir_node_get_relation_name(left), "Tc") != 0) {
        wl_ir_node_free(join);
        FAIL("left child should be Tc");
        return;
    }

    if (!right
        || strcmp(wirelog_ir_node_get_relation_name(right), "Arc") != 0) {
        wl_ir_node_free(join);
        FAIL("right child should be Arc");
        return;
    }

    wl_ir_node_free(join);
    PASS();
}

static void
test_create_filter_node(void)
{
    TEST("Create FILTER node with expression");

    wirelog_ir_node_t *scan = wl_ir_node_create(WIRELOG_IR_SCAN);
    wl_ir_node_set_relation(scan, "edge");

    /* Build expression: x != y */
    wl_ir_expr_t *left = wl_ir_expr_create(WL_IR_EXPR_VAR);
    left->var_name = strdup_safe("x");

    wl_ir_expr_t *right = wl_ir_expr_create(WL_IR_EXPR_VAR);
    right->var_name = strdup_safe("y");

    wl_ir_expr_t *cmp = wl_ir_expr_create(WL_IR_EXPR_CMP);
    cmp->cmp_op = WIRELOG_CMP_NEQ;
    wl_ir_expr_add_child(cmp, left);
    wl_ir_expr_add_child(cmp, right);

    wirelog_ir_node_t *filter = wl_ir_node_create(WIRELOG_IR_FILTER);
    filter->filter_expr = cmp;
    wl_ir_node_add_child(filter, scan);

    if (wirelog_ir_node_get_type(filter) != WIRELOG_IR_FILTER) {
        wl_ir_node_free(filter);
        FAIL("type is not FILTER");
        return;
    }

    if (!filter->filter_expr) {
        wl_ir_node_free(filter);
        FAIL("filter_expr is NULL");
        return;
    }

    if (filter->filter_expr->cmp_op != WIRELOG_CMP_NEQ) {
        wl_ir_node_free(filter);
        FAIL("filter cmp_op should be NEQ");
        return;
    }

    if (filter->filter_expr->child_count != 2) {
        wl_ir_node_free(filter);
        FAIL("filter expr should have 2 children");
        return;
    }

    wl_ir_node_free(filter);
    PASS();
}

static void
test_create_aggregate_node(void)
{
    TEST("Create AGGREGATE node with agg_fn and group_by");

    wirelog_ir_node_t *scan = wl_ir_node_create(WIRELOG_IR_SCAN);
    wl_ir_node_set_relation(scan, "sssp2");

    wl_ir_expr_t *agg_expr = wl_ir_expr_create(WL_IR_EXPR_VAR);
    agg_expr->var_name = strdup_safe("d");

    wirelog_ir_node_t *agg = wl_ir_node_create(WIRELOG_IR_AGGREGATE);
    wl_ir_node_set_relation(agg, "sssp");
    agg->agg_fn = WIRELOG_AGG_MIN;
    agg->agg_expr = agg_expr;

    /* group_by indices */
    agg->group_by_indices = (uint32_t *)malloc(sizeof(uint32_t));
    agg->group_by_indices[0] = 0;
    agg->group_by_count = 1;

    wl_ir_node_add_child(agg, scan);

    if (agg->agg_fn != WIRELOG_AGG_MIN) {
        wl_ir_node_free(agg);
        FAIL("agg_fn should be MIN");
        return;
    }

    if (agg->group_by_count != 1 || agg->group_by_indices[0] != 0) {
        wl_ir_node_free(agg);
        FAIL("group_by mismatch");
        return;
    }

    if (!agg->agg_expr || agg->agg_expr->type != WL_IR_EXPR_VAR) {
        wl_ir_node_free(agg);
        FAIL("agg_expr mismatch");
        return;
    }

    wl_ir_node_free(agg);
    PASS();
}

static void
test_create_antijoin_node(void)
{
    TEST("Create ANTIJOIN node with two children");

    wirelog_ir_node_t *scan1 = wl_ir_node_create(WIRELOG_IR_SCAN);
    wl_ir_node_set_relation(scan1, "node");

    wirelog_ir_node_t *scan2 = wl_ir_node_create(WIRELOG_IR_SCAN);
    wl_ir_node_set_relation(scan2, "edge");

    wirelog_ir_node_t *antijoin = wl_ir_node_create(WIRELOG_IR_ANTIJOIN);
    wl_ir_node_add_child(antijoin, scan1);
    wl_ir_node_add_child(antijoin, scan2);

    if (wirelog_ir_node_get_type(antijoin) != WIRELOG_IR_ANTIJOIN) {
        wl_ir_node_free(antijoin);
        FAIL("type is not ANTIJOIN");
        return;
    }

    if (wirelog_ir_node_get_child_count(antijoin) != 2) {
        wl_ir_node_free(antijoin);
        FAIL("ANTIJOIN should have 2 children");
        return;
    }

    wl_ir_node_free(antijoin);
    PASS();
}

static void
test_create_union_node(void)
{
    TEST("Create UNION node with multiple children");

    wirelog_ir_node_t *proj1 = wl_ir_node_create(WIRELOG_IR_PROJECT);
    wl_ir_node_set_relation(proj1, "Tc");

    wirelog_ir_node_t *proj2 = wl_ir_node_create(WIRELOG_IR_PROJECT);
    wl_ir_node_set_relation(proj2, "Tc");

    wirelog_ir_node_t *proj3 = wl_ir_node_create(WIRELOG_IR_PROJECT);
    wl_ir_node_set_relation(proj3, "Tc");

    wirelog_ir_node_t *un = wl_ir_node_create(WIRELOG_IR_UNION);
    wl_ir_node_set_relation(un, "Tc");
    wl_ir_node_add_child(un, proj1);
    wl_ir_node_add_child(un, proj2);
    wl_ir_node_add_child(un, proj3);

    if (wirelog_ir_node_get_child_count(un) != 3) {
        wl_ir_node_free(un);
        FAIL("UNION should have 3 children");
        return;
    }

    wl_ir_node_free(un);
    PASS();
}

/* ======================================================================== */
/* Expression Tests                                                         */
/* ======================================================================== */

static void
test_create_expression_tree(void)
{
    TEST("Create expression tree (var, const, arith, cmp)");

    /* Build: (x + 1) < y */
    wl_ir_expr_t *var_x = wl_ir_expr_create(WL_IR_EXPR_VAR);
    var_x->var_name = strdup_safe("x");

    wl_ir_expr_t *const_1 = wl_ir_expr_create(WL_IR_EXPR_CONST_INT);
    const_1->int_value = 1;

    wl_ir_expr_t *arith = wl_ir_expr_create(WL_IR_EXPR_ARITH);
    arith->arith_op = WIRELOG_ARITH_ADD;
    wl_ir_expr_add_child(arith, var_x);
    wl_ir_expr_add_child(arith, const_1);

    wl_ir_expr_t *var_y = wl_ir_expr_create(WL_IR_EXPR_VAR);
    var_y->var_name = strdup_safe("y");

    wl_ir_expr_t *cmp = wl_ir_expr_create(WL_IR_EXPR_CMP);
    cmp->cmp_op = WIRELOG_CMP_LT;
    wl_ir_expr_add_child(cmp, arith);
    wl_ir_expr_add_child(cmp, var_y);

    /* Verify structure */
    if (cmp->type != WL_IR_EXPR_CMP) {
        FAIL("root should be CMP");
        wl_ir_expr_free(cmp);
        return;
    }
    if (cmp->cmp_op != WIRELOG_CMP_LT) {
        FAIL("cmp_op should be LT");
        wl_ir_expr_free(cmp);
        return;
    }
    if (cmp->child_count != 2) {
        FAIL("cmp should have 2 children");
        wl_ir_expr_free(cmp);
        return;
    }

    wl_ir_expr_t *left = cmp->children[0];
    if (left->type != WL_IR_EXPR_ARITH) {
        FAIL("left should be ARITH");
        wl_ir_expr_free(cmp);
        return;
    }
    if (left->arith_op != WIRELOG_ARITH_ADD) {
        FAIL("arith_op should be ADD");
        wl_ir_expr_free(cmp);
        return;
    }
    if (left->child_count != 2) {
        FAIL("arith should have 2 children");
        wl_ir_expr_free(cmp);
        return;
    }

    wl_ir_expr_t *x = left->children[0];
    if (x->type != WL_IR_EXPR_VAR) {
        FAIL("should be VAR");
        wl_ir_expr_free(cmp);
        return;
    }
    if (strcmp(x->var_name, "x") != 0) {
        FAIL("var_name should be x");
        wl_ir_expr_free(cmp);
        return;
    }

    wl_ir_expr_t *one = left->children[1];
    if (one->type != WL_IR_EXPR_CONST_INT) {
        FAIL("should be CONST_INT");
        wl_ir_expr_free(cmp);
        return;
    }
    if (one->int_value != 1) {
        FAIL("int_value should be 1");
        wl_ir_expr_free(cmp);
        return;
    }

    wl_ir_expr_t *y = cmp->children[1];
    if (y->type != WL_IR_EXPR_VAR) {
        FAIL("should be VAR");
        wl_ir_expr_free(cmp);
        return;
    }
    if (strcmp(y->var_name, "y") != 0) {
        FAIL("var_name should be y");
        wl_ir_expr_free(cmp);
        return;
    }

    wl_ir_expr_free(cmp);
    PASS();
}

static void
test_expr_string_constant(void)
{
    TEST("Create string constant expression");

    wl_ir_expr_t *str = wl_ir_expr_create(WL_IR_EXPR_CONST_STR);
    str->str_value = strdup_safe("hello");

    if (str->type != WL_IR_EXPR_CONST_STR) {
        FAIL("type mismatch");
        wl_ir_expr_free(str);
        return;
    }
    if (strcmp(str->str_value, "hello") != 0) {
        FAIL("str_value mismatch");
        wl_ir_expr_free(str);
        return;
    }

    wl_ir_expr_free(str);
    PASS();
}

static void
test_expr_bool(void)
{
    TEST("Create boolean expression");

    wl_ir_expr_t *b = wl_ir_expr_create(WL_IR_EXPR_BOOL);
    b->bool_value = true;

    if (b->type != WL_IR_EXPR_BOOL) {
        FAIL("type mismatch");
        wl_ir_expr_free(b);
        return;
    }
    if (!b->bool_value) {
        FAIL("bool_value should be true");
        wl_ir_expr_free(b);
        return;
    }

    wl_ir_expr_free(b);
    PASS();
}

static void
test_expr_aggregate(void)
{
    TEST("Create aggregate expression (min)");

    wl_ir_expr_t *inner = wl_ir_expr_create(WL_IR_EXPR_VAR);
    inner->var_name = strdup_safe("d");

    wl_ir_expr_t *agg = wl_ir_expr_create(WL_IR_EXPR_AGG);
    agg->agg_fn = WIRELOG_AGG_MIN;
    wl_ir_expr_add_child(agg, inner);

    if (agg->type != WL_IR_EXPR_AGG) {
        FAIL("type mismatch");
        wl_ir_expr_free(agg);
        return;
    }
    if (agg->agg_fn != WIRELOG_AGG_MIN) {
        FAIL("agg_fn should be MIN");
        wl_ir_expr_free(agg);
        return;
    }
    if (agg->child_count != 1) {
        FAIL("should have 1 child");
        wl_ir_expr_free(agg);
        return;
    }

    wl_ir_expr_free(agg);
    PASS();
}

/* ======================================================================== */
/* Public API Tests                                                         */
/* ======================================================================== */

static void
test_get_child_out_of_bounds(void)
{
    TEST("get_child returns NULL for out-of-bounds index");

    wirelog_ir_node_t *node = wl_ir_node_create(WIRELOG_IR_SCAN);
    wl_ir_node_set_relation(node, "test");

    const wirelog_ir_node_t *child = wirelog_ir_node_get_child(node, 0);
    if (child != NULL) {
        wl_ir_node_free(node);
        FAIL("should return NULL for index 0 on leaf node");
        return;
    }

    child = wirelog_ir_node_get_child(node, 99);
    if (child != NULL) {
        wl_ir_node_free(node);
        FAIL("should return NULL for index 99");
        return;
    }

    wl_ir_node_free(node);
    PASS();
}

static void
test_ir_node_to_string(void)
{
    TEST("IR node to_string output");

    wirelog_ir_node_t *scan = wl_ir_node_create(WIRELOG_IR_SCAN);
    wl_ir_node_set_relation(scan, "Arc");

    char *str = wirelog_ir_node_to_string(scan);
    if (!str) {
        wl_ir_node_free(scan);
        FAIL("to_string returned NULL");
        return;
    }

    /* Should contain SCAN and Arc */
    if (!strstr(str, "SCAN")) {
        free(str);
        wl_ir_node_free(scan);
        FAIL("should contain SCAN");
        return;
    }

    if (!strstr(str, "Arc")) {
        free(str);
        wl_ir_node_free(scan);
        FAIL("should contain Arc");
        return;
    }

    free(str);
    wl_ir_node_free(scan);
    PASS();
}

static void
test_ir_node_to_string_tree(void)
{
    TEST("IR node to_string for tree structure");

    wirelog_ir_node_t *scan1 = wl_ir_node_create(WIRELOG_IR_SCAN);
    wl_ir_node_set_relation(scan1, "Tc");

    wirelog_ir_node_t *scan2 = wl_ir_node_create(WIRELOG_IR_SCAN);
    wl_ir_node_set_relation(scan2, "Arc");

    wirelog_ir_node_t *join = wl_ir_node_create(WIRELOG_IR_JOIN);
    wl_ir_node_add_child(join, scan1);
    wl_ir_node_add_child(join, scan2);

    wirelog_ir_node_t *proj = wl_ir_node_create(WIRELOG_IR_PROJECT);
    wl_ir_node_set_relation(proj, "Tc");
    wl_ir_node_add_child(proj, join);

    char *str = wirelog_ir_node_to_string(proj);
    if (!str) {
        wl_ir_node_free(proj);
        FAIL("to_string returned NULL");
        return;
    }

    /* Should contain all node types */
    if (!strstr(str, "PROJECT")) {
        free(str);
        wl_ir_node_free(proj);
        FAIL("should contain PROJECT");
        return;
    }

    if (!strstr(str, "JOIN")) {
        free(str);
        wl_ir_node_free(proj);
        FAIL("should contain JOIN");
        return;
    }

    free(str);
    wl_ir_node_free(proj);
    PASS();
}

static void
test_ir_node_print(void)
{
    TEST("IR node print output (smoke test)");

    wirelog_ir_node_t *scan = wl_ir_node_create(WIRELOG_IR_SCAN);
    wl_ir_node_set_relation(scan, "test_rel");

    /* Just verify it doesn't crash */
    wirelog_ir_node_print(scan, 0);

    wl_ir_node_free(scan);
    PASS();
}

static void
test_ir_node_free_null(void)
{
    TEST("IR node free handles NULL safely");

    /* Should not crash */
    wl_ir_node_free(NULL);

    PASS();
}

static void
test_ir_node_get_type_all(void)
{
    TEST("All 8 IR node types are distinct");

    wirelog_ir_node_type_t types[]
        = { WIRELOG_IR_SCAN,     WIRELOG_IR_PROJECT, WIRELOG_IR_FILTER,
            WIRELOG_IR_JOIN,     WIRELOG_IR_FLATMAP, WIRELOG_IR_AGGREGATE,
            WIRELOG_IR_ANTIJOIN, WIRELOG_IR_UNION };

    for (int i = 0; i < 8; i++) {
        wirelog_ir_node_t *node = wl_ir_node_create(types[i]);
        if (!node) {
            FAIL("create failed");
            return;
        }
        if (wirelog_ir_node_get_type(node) != types[i]) {
            wl_ir_node_free(node);
            FAIL("type mismatch for node type");
            return;
        }
        wl_ir_node_free(node);
    }

    PASS();
}

static void
test_scan_column_names(void)
{
    TEST("SCAN node with column names");

    wirelog_ir_node_t *scan = wl_ir_node_create(WIRELOG_IR_SCAN);
    wl_ir_node_set_relation(scan, "Arc");

    /* Set column names */
    scan->column_count = 2;
    scan->column_names = (char **)malloc(2 * sizeof(char *));
    scan->column_names[0] = strdup_safe("x");
    scan->column_names[1] = strdup_safe("y");

    if (scan->column_count != 2) {
        wl_ir_node_free(scan);
        FAIL("column_count should be 2");
        return;
    }

    if (strcmp(scan->column_names[0], "x") != 0) {
        wl_ir_node_free(scan);
        FAIL("column 0 should be x");
        return;
    }

    if (strcmp(scan->column_names[1], "y") != 0) {
        wl_ir_node_free(scan);
        FAIL("column 1 should be y");
        return;
    }

    wl_ir_node_free(scan);
    PASS();
}

static void
test_join_keys(void)
{
    TEST("JOIN node with join keys");

    wirelog_ir_node_t *join = wl_ir_node_create(WIRELOG_IR_JOIN);

    join->join_key_count = 1;
    join->join_left_keys = (char **)malloc(sizeof(char *));
    join->join_right_keys = (char **)malloc(sizeof(char *));
    join->join_left_keys[0] = strdup_safe("z");
    join->join_right_keys[0] = strdup_safe("z");

    if (join->join_key_count != 1) {
        wl_ir_node_free(join);
        FAIL("join_key_count should be 1");
        return;
    }

    if (strcmp(join->join_left_keys[0], "z") != 0) {
        wl_ir_node_free(join);
        FAIL("left key should be z");
        return;
    }

    if (strcmp(join->join_right_keys[0], "z") != 0) {
        wl_ir_node_free(join);
        FAIL("right key should be z");
        return;
    }

    wl_ir_node_free(join);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("\n=== wirelog IR Node Tests ===\n\n");

    /* IR Node creation */
    test_create_scan_node();
    test_create_project_node();
    test_create_join_node();
    test_create_filter_node();
    test_create_aggregate_node();
    test_create_antijoin_node();
    test_create_union_node();

    /* Expression tree */
    test_create_expression_tree();
    test_expr_string_constant();
    test_expr_bool();
    test_expr_aggregate();

    /* Public API */
    test_get_child_out_of_bounds();
    test_ir_node_to_string();
    test_ir_node_to_string_tree();
    test_ir_node_print();
    test_ir_node_free_null();
    test_ir_node_get_type_all();

    /* Operator-specific fields */
    test_scan_column_names();
    test_join_keys();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
