/*
 * test_hash_parser.c - Hash Function Parser Tests (Issue #144)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Verifies that the parser recognizes the hash() built-in function and
 * creates correct BINARY_EXPR AST nodes with WIRELOG_ARITH_HASH op.
 *
 * Syntax: hash(expr)  -- unary, like bnot(expr)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/parser/parser.h"

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

#define PARSE(src)                \
    char errbuf[512] = { 0 };     \
    wl_parser_ast_node_t *program \
        = wl_parser_parse_string(src, errbuf, sizeof(errbuf))

#define ASSERT_PARSED()                                             \
    do {                                                            \
        if (!program) {                                             \
            char buf[600];                                          \
            snprintf(buf, sizeof(buf), "parse failed: %s", errbuf); \
            FAIL(buf);                                              \
            return;                                                 \
        }                                                           \
        if (program->type != WL_PARSER_AST_NODE_PROGRAM) {          \
            wl_parser_ast_node_free(program);                       \
            FAIL("root is not PROGRAM");                            \
            return;                                                 \
        }                                                           \
    } while (0)

#define CLEANUP() wl_parser_ast_node_free(program)

static const wl_parser_ast_node_t *
child(const wl_parser_ast_node_t *node, uint32_t index)
{
    if (!node || index >= node->child_count)
        return NULL;
    return node->children[index];
}

/* ======================================================================== */
/* Helper: extract first head arg from first rule                          */
/* ======================================================================== */

static const wl_parser_ast_node_t *
first_head_arg(const wl_parser_ast_node_t *program)
{
    const wl_parser_ast_node_t *rule = child(program, 0);
    if (!rule || rule->type != WL_PARSER_AST_NODE_RULE)
        return NULL;
    const wl_parser_ast_node_t *head = child(rule, 0);
    if (!head || head->type != WL_PARSER_AST_NODE_HEAD)
        return NULL;
    return child(head, 0);
}

/* ======================================================================== */
/* Helper: extract first body comparison left child                        */
/* ======================================================================== */

static const wl_parser_ast_node_t *
first_body_cmp_left(const wl_parser_ast_node_t *program)
{
    const wl_parser_ast_node_t *rule = child(program, 0);
    if (!rule)
        return NULL;
    for (uint32_t i = 1; i < rule->child_count; i++) {
        const wl_parser_ast_node_t *b = child(rule, i);
        if (b && b->type == WL_PARSER_AST_NODE_COMPARISON)
            return child(b, 0);
    }
    return NULL;
}

/* ======================================================================== */
/* hash() Parser Tests                                                     */
/* ======================================================================== */

static void
test_parse_hash_in_head(void)
{
    TEST("hash(x) in head creates BINARY_EXPR with HASH op");
    PARSE("r(hash(x)) :- a(x).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR) {
        CLEANUP();
        FAIL("expected BINARY_EXPR in head");
        return;
    }
    if (expr->arith_op != WIRELOG_ARITH_HASH) {
        CLEANUP();
        FAIL("expected arith_op == WIRELOG_ARITH_HASH");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_hash_has_one_child(void)
{
    TEST("hash(x) is unary: exactly 1 child");
    PARSE("r(hash(x)) :- a(x).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || expr->arith_op != WIRELOG_ARITH_HASH) {
        CLEANUP();
        FAIL("expected HASH BINARY_EXPR");
        return;
    }
    if (expr->child_count != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 child, got %u",
                 expr->child_count);
        CLEANUP();
        FAIL(buf);
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_hash_child_is_variable(void)
{
    TEST("hash(x) child is VARIABLE node 'x'");
    PARSE("r(hash(x)) :- a(x).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || expr->arith_op != WIRELOG_ARITH_HASH) {
        CLEANUP();
        FAIL("expected HASH BINARY_EXPR");
        return;
    }
    const wl_parser_ast_node_t *operand = child(expr, 0);
    if (!operand || operand->type != WL_PARSER_AST_NODE_VARIABLE
        || strcmp(operand->name, "x") != 0) {
        CLEANUP();
        FAIL("child should be VARIABLE 'x'");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_hash_with_integer_literal(void)
{
    TEST("hash(42) child is INTEGER 42");
    PARSE("r(hash(42)) :- a().");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || expr->arith_op != WIRELOG_ARITH_HASH) {
        CLEANUP();
        FAIL("expected HASH BINARY_EXPR");
        return;
    }
    const wl_parser_ast_node_t *operand = child(expr, 0);
    if (!operand || operand->type != WL_PARSER_AST_NODE_INTEGER
        || operand->int_value != 42) {
        CLEANUP();
        FAIL("child should be INTEGER 42");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_hash_in_filter(void)
{
    TEST("hash(x) = y in body creates COMPARISON with HASH left");
    PARSE("r(x) :- a(x, y), hash(x) = y.");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *lhs = first_body_cmp_left(program);
    if (!lhs || lhs->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || lhs->arith_op != WIRELOG_ARITH_HASH) {
        CLEANUP();
        FAIL("expected HASH expr on left side of comparison");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_hash_modulo_in_head(void)
{
    TEST("hash(x) % 16 in head: HASH nested inside MOD");
    PARSE("r(hash(x) % 16) :- a(x).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *outer = first_head_arg(program);
    if (!outer || outer->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || outer->arith_op != WIRELOG_ARITH_MOD) {
        CLEANUP();
        FAIL("outer expr should be MOD");
        return;
    }
    const wl_parser_ast_node_t *inner = child(outer, 0);
    if (!inner || inner->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || inner->arith_op != WIRELOG_ARITH_HASH) {
        CLEANUP();
        FAIL("left child of mod should be HASH expr");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_hash_nested_in_band(void)
{
    TEST("band(hash(x), 255) - HASH nested inside BAND");
    PARSE("r(band(hash(x), 255)) :- a(x).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *outer = first_head_arg(program);
    if (!outer || outer->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || outer->arith_op != WIRELOG_ARITH_BAND) {
        CLEANUP();
        FAIL("outer should be BAND");
        return;
    }
    const wl_parser_ast_node_t *inner = child(outer, 0);
    if (!inner || inner->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || inner->arith_op != WIRELOG_ARITH_HASH) {
        CLEANUP();
        FAIL("left child of band should be HASH expr");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_hash_with_arithmetic_arg(void)
{
    TEST("hash(x + 1) - HASH with arithmetic sub-expression");
    PARSE("r(hash(x + 1)) :- a(x).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || expr->arith_op != WIRELOG_ARITH_HASH) {
        CLEANUP();
        FAIL("expected HASH BINARY_EXPR");
        return;
    }
    const wl_parser_ast_node_t *arg = child(expr, 0);
    if (!arg || arg->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || arg->arith_op != WIRELOG_ARITH_ADD) {
        CLEANUP();
        FAIL("hash arg should be ADD expr");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_hash_in_rule_program(void)
{
    TEST("program with .decl and hash() rule parses successfully");
    PARSE(".decl a(x: int64)\n"
          ".decl r(z: int64)\n"
          "r(hash(x)) :- a(x).\n");
    ASSERT_PARSED();

    /* 2 decls + 1 rule = 3 children */
    if (program->child_count != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 children, got %u",
                 program->child_count);
        CLEANUP();
        FAIL(buf);
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_hash_filter_neq(void)
{
    TEST("hash(x) != hash(y) in body comparison works");
    PARSE("r(x) :- a(x, y), hash(x) != hash(y).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *lhs = first_body_cmp_left(program);
    if (!lhs || lhs->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || lhs->arith_op != WIRELOG_ARITH_HASH) {
        CLEANUP();
        FAIL("expected HASH expr on left side of != comparison");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_hash_bucket_assignment(void)
{
    TEST("bucket(hash(id) % 16) :- item(id) - partitioning pattern");
    PARSE(".decl item(id: int64)\n"
          ".decl bucket(b: int64)\n"
          "bucket(hash(id) % 16) :- item(id).\n");
    ASSERT_PARSED();

    if (program->child_count != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 children, got %u",
                 program->child_count);
        CLEANUP();
        FAIL(buf);
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== wirelog Hash Parser Tests (Issue #144) ===\n\n");

    printf("--- hash() Basic Parsing ---\n");
    test_parse_hash_in_head();
    test_parse_hash_has_one_child();
    test_parse_hash_child_is_variable();
    test_parse_hash_with_integer_literal();

    printf("\n--- hash() in Body Filters ---\n");
    test_parse_hash_in_filter();
    test_parse_hash_filter_neq();

    printf("\n--- hash() Nested Expressions ---\n");
    test_parse_hash_modulo_in_head();
    test_parse_hash_nested_in_band();
    test_parse_hash_with_arithmetic_arg();

    printf("\n--- hash() Full Programs ---\n");
    test_parse_hash_in_rule_program();
    test_parse_hash_bucket_assignment();

    printf("\n=== Results: %d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf(" ===\n");

    return tests_failed > 0 ? 1 : 0;
}
