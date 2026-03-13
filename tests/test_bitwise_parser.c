/*
 * test_bitwise_parser.c - Bitwise Operator Parser Tests (Issue #72)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * TDD RED phase: verifies that the parser recognizes bitwise operator
 * keywords (band, bor, bxor, bnot, bshl, bshr) and creates correct
 * BINARY_EXPR AST nodes with the appropriate arith_op values.
 *
 * Syntax: function-call form — band(x, y), bor(x, y), bnot(x), etc.
 *
 * These tests FAIL until US-002 (parser support) is implemented.
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

#define ASSERT_PARSED()                                               \
    do {                                                              \
        if (!program) {                                               \
            char buf[600];                                            \
            snprintf(buf, sizeof(buf), "parse failed: %s", errbuf);  \
            FAIL(buf);                                                \
            return;                                                   \
        }                                                             \
        if (program->type != WL_PARSER_AST_NODE_PROGRAM) {           \
            wl_parser_ast_node_free(program);                         \
            FAIL("root is not PROGRAM");                              \
            return;                                                   \
        }                                                             \
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
/* Helper: extract first body comparison left child from first rule       */
/* ======================================================================== */

static const wl_parser_ast_node_t *
first_body_cmp_left(const wl_parser_ast_node_t *program)
{
    const wl_parser_ast_node_t *rule = child(program, 0);
    if (!rule)
        return NULL;
    /* body starts at child index 1 (index 0 is head) */
    for (uint32_t i = 1; i < rule->child_count; i++) {
        const wl_parser_ast_node_t *b = child(rule, i);
        if (b && b->type == WL_PARSER_AST_NODE_COMPARISON)
            return child(b, 0);
    }
    return NULL;
}

/* ======================================================================== */
/* Binary Bitwise Operator Tests: band                                     */
/* ======================================================================== */

static void
test_parse_band_in_head(void)
{
    TEST("band(x, y) in head creates BINARY_EXPR with BAND op");
    PARSE("r(band(x, y)) :- a(x, y).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR) {
        CLEANUP();
        FAIL("expected BINARY_EXPR in head");
        return;
    }
    if (expr->arith_op != WIRELOG_ARITH_BAND) {
        CLEANUP();
        FAIL("expected arith_op == WIRELOG_ARITH_BAND");
        return;
    }
    if (expr->child_count != 2) {
        CLEANUP();
        FAIL("band must have 2 children");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_band_operands_are_variables(void)
{
    TEST("band(x, y) children are VARIABLE nodes x and y");
    PARSE("r(band(x, y)) :- a(x, y).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR) {
        CLEANUP();
        FAIL("expected BINARY_EXPR");
        return;
    }
    const wl_parser_ast_node_t *lhs = child(expr, 0);
    const wl_parser_ast_node_t *rhs = child(expr, 1);
    if (!lhs || lhs->type != WL_PARSER_AST_NODE_VARIABLE
        || strcmp(lhs->name, "x") != 0) {
        CLEANUP();
        FAIL("left child should be VARIABLE 'x'");
        return;
    }
    if (!rhs || rhs->type != WL_PARSER_AST_NODE_VARIABLE
        || strcmp(rhs->name, "y") != 0) {
        CLEANUP();
        FAIL("right child should be VARIABLE 'y'");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_band_with_integer_literal(void)
{
    TEST("band(x, 255) in head stores integer 255 as right child");
    PARSE("r(band(x, 255)) :- a(x).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || expr->arith_op != WIRELOG_ARITH_BAND) {
        CLEANUP();
        FAIL("expected BINARY_EXPR with BAND op");
        return;
    }
    const wl_parser_ast_node_t *rhs = child(expr, 1);
    if (!rhs || rhs->type != WL_PARSER_AST_NODE_INTEGER
        || rhs->int_value != 255) {
        CLEANUP();
        FAIL("right child should be INTEGER 255");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_band_in_filter(void)
{
    TEST("band(x, 0xFF) != 0 in body creates COMPARISON with BAND left");
    PARSE("r(x) :- a(x), band(x, 255) != 0.");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *lhs = first_body_cmp_left(program);
    if (!lhs || lhs->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || lhs->arith_op != WIRELOG_ARITH_BAND) {
        CLEANUP();
        FAIL("expected BAND expr on left side of comparison");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Binary Bitwise Operator Tests: bor                                      */
/* ======================================================================== */

static void
test_parse_bor_in_head(void)
{
    TEST("bor(x, y) in head creates BINARY_EXPR with BOR op");
    PARSE("r(bor(x, y)) :- a(x, y).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || expr->arith_op != WIRELOG_ARITH_BOR) {
        CLEANUP();
        FAIL("expected BINARY_EXPR with BOR op");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_bor_in_filter(void)
{
    TEST("bor(x, y) != 0 in body filter works");
    PARSE("r(x) :- a(x, y), bor(x, y) != 0.");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *lhs = first_body_cmp_left(program);
    if (!lhs || lhs->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || lhs->arith_op != WIRELOG_ARITH_BOR) {
        CLEANUP();
        FAIL("expected BOR expr in body comparison");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Binary Bitwise Operator Tests: bxor                                     */
/* ======================================================================== */

static void
test_parse_bxor_in_head(void)
{
    TEST("bxor(x, y) in head creates BINARY_EXPR with BXOR op");
    PARSE("r(bxor(x, y)) :- a(x, y).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || expr->arith_op != WIRELOG_ARITH_BXOR) {
        CLEANUP();
        FAIL("expected BINARY_EXPR with BXOR op");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_bxor_with_constants(void)
{
    TEST("bxor(170, 85) in head: 0xAA ^ 0x55 operands stored");
    PARSE("r(bxor(170, 85)) :- a().");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || expr->arith_op != WIRELOG_ARITH_BXOR) {
        CLEANUP();
        FAIL("expected BINARY_EXPR with BXOR op");
        return;
    }
    const wl_parser_ast_node_t *lhs = child(expr, 0);
    const wl_parser_ast_node_t *rhs = child(expr, 1);
    if (!lhs || lhs->type != WL_PARSER_AST_NODE_INTEGER || lhs->int_value != 170) {
        CLEANUP();
        FAIL("left child should be INTEGER 170");
        return;
    }
    if (!rhs || rhs->type != WL_PARSER_AST_NODE_INTEGER || rhs->int_value != 85) {
        CLEANUP();
        FAIL("right child should be INTEGER 85");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Unary Bitwise Operator Tests: bnot                                      */
/* ======================================================================== */

static void
test_parse_bnot_in_head(void)
{
    TEST("bnot(x) in head creates BINARY_EXPR with BNOT op, 1 child");
    PARSE("r(bnot(x)) :- a(x).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR) {
        CLEANUP();
        FAIL("expected BINARY_EXPR node");
        return;
    }
    if (expr->arith_op != WIRELOG_ARITH_BNOT) {
        CLEANUP();
        FAIL("expected arith_op == WIRELOG_ARITH_BNOT");
        return;
    }
    /* bnot is unary: right child holds the operand (lhs unused/NULL) */
    const wl_parser_ast_node_t *operand = child(expr, 0);
    if (!operand || operand->type != WL_PARSER_AST_NODE_VARIABLE
        || strcmp(operand->name, "x") != 0) {
        CLEANUP();
        FAIL("bnot child should be VARIABLE 'x'");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_bnot_with_constant(void)
{
    TEST("bnot(0) in head: unary complement of integer constant");
    PARSE("r(bnot(0)) :- a().");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || expr->arith_op != WIRELOG_ARITH_BNOT) {
        CLEANUP();
        FAIL("expected BINARY_EXPR with BNOT op");
        return;
    }
    const wl_parser_ast_node_t *operand = child(expr, 0);
    if (!operand || operand->type != WL_PARSER_AST_NODE_INTEGER
        || operand->int_value != 0) {
        CLEANUP();
        FAIL("bnot child should be INTEGER 0");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_bnot_in_filter(void)
{
    TEST("bnot(x) = y in body comparison works");
    PARSE("r(x) :- a(x, y), bnot(x) = y.");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *lhs = first_body_cmp_left(program);
    if (!lhs || lhs->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || lhs->arith_op != WIRELOG_ARITH_BNOT) {
        CLEANUP();
        FAIL("expected BNOT expr in body comparison");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Shift Operator Tests: bshl, bshr                                        */
/* ======================================================================== */

static void
test_parse_bshl_in_head(void)
{
    TEST("bshl(x, 4) in head creates BINARY_EXPR with SHL op");
    PARSE("r(bshl(x, 4)) :- a(x).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || expr->arith_op != WIRELOG_ARITH_SHL) {
        CLEANUP();
        FAIL("expected BINARY_EXPR with SHL op");
        return;
    }
    const wl_parser_ast_node_t *rhs = child(expr, 1);
    if (!rhs || rhs->type != WL_PARSER_AST_NODE_INTEGER || rhs->int_value != 4) {
        CLEANUP();
        FAIL("right child should be INTEGER 4 (shift amount)");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_bshr_in_head(void)
{
    TEST("bshr(x, 2) in head creates BINARY_EXPR with SHR op");
    PARSE("r(bshr(x, 2)) :- a(x).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || expr->arith_op != WIRELOG_ARITH_SHR) {
        CLEANUP();
        FAIL("expected BINARY_EXPR with SHR op");
        return;
    }
    const wl_parser_ast_node_t *rhs = child(expr, 1);
    if (!rhs || rhs->type != WL_PARSER_AST_NODE_INTEGER || rhs->int_value != 2) {
        CLEANUP();
        FAIL("right child should be INTEGER 2 (shift amount)");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_bshl_in_filter(void)
{
    TEST("bshl(x, 8) > 0 in body filter works");
    PARSE("r(x) :- a(x), bshl(x, 8) > 0.");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *lhs = first_body_cmp_left(program);
    if (!lhs || lhs->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || lhs->arith_op != WIRELOG_ARITH_SHL) {
        CLEANUP();
        FAIL("expected SHL expr in body comparison");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_bshr_in_filter(void)
{
    TEST("bshr(x, 3) = 1 in body filter works");
    PARSE("r(x) :- a(x), bshr(x, 3) = 1.");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *lhs = first_body_cmp_left(program);
    if (!lhs || lhs->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || lhs->arith_op != WIRELOG_ARITH_SHR) {
        CLEANUP();
        FAIL("expected SHR expr in body comparison");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Nested / Combined Bitwise Expression Tests                              */
/* ======================================================================== */

static void
test_parse_nested_band_bshr(void)
{
    TEST("band(bshr(x, 4), 15) - nested shift+mask expression parses");
    PARSE("r(band(bshr(x, 4), 15)) :- a(x).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *outer = first_head_arg(program);
    if (!outer || outer->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || outer->arith_op != WIRELOG_ARITH_BAND) {
        CLEANUP();
        FAIL("outer expr should be BAND");
        return;
    }
    const wl_parser_ast_node_t *inner = child(outer, 0);
    if (!inner || inner->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || inner->arith_op != WIRELOG_ARITH_SHR) {
        CLEANUP();
        FAIL("inner expr should be SHR");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_bor_bshl_combine(void)
{
    TEST("bor(bshl(hi, 8), lo) - reconstruct 16-bit value parses");
    PARSE("r(bor(bshl(hi, 8), lo)) :- a(hi, lo).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *outer = first_head_arg(program);
    if (!outer || outer->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || outer->arith_op != WIRELOG_ARITH_BOR) {
        CLEANUP();
        FAIL("outer expr should be BOR");
        return;
    }
    const wl_parser_ast_node_t *inner = child(outer, 0);
    if (!inner || inner->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || inner->arith_op != WIRELOG_ARITH_SHL) {
        CLEANUP();
        FAIL("inner expr should be SHL");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_all_binary_bitwise_ops_roundtrip(void)
{
    TEST("all 5 binary bitwise ops parse without error in one program");
    PARSE(".decl a(x: int64, y: int64)\n"
          ".decl r(z: int64)\n"
          "r(band(x, y)) :- a(x, y).\n"
          "r(bor(x, y))  :- a(x, y).\n"
          "r(bxor(x, y)) :- a(x, y).\n"
          "r(bshl(x, y)) :- a(x, y).\n"
          "r(bshr(x, y)) :- a(x, y).\n");
    ASSERT_PARSED();

    /* 2 decls + 5 rules = 7 children */
    if (program->child_count != 7) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 7 children, got %u",
                 program->child_count);
        CLEANUP();
        FAIL(buf);
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_bnot_combined_with_band(void)
{
    TEST("band(bnot(x), 255) - bnot nested inside band parses");
    PARSE("r(band(bnot(x), 255)) :- a(x).");
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
        || inner->arith_op != WIRELOG_ARITH_BNOT) {
        CLEANUP();
        FAIL("left child of band should be BNOT expr");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_bitwise_with_arithmetic_mixed(void)
{
    TEST("band(x + 1, 15) - bitwise op with arithmetic sub-expression");
    PARSE("r(band(x + 1, 15)) :- a(x).");
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
        || inner->arith_op != WIRELOG_ARITH_ADD) {
        CLEANUP();
        FAIL("left child of band should be ADD expr");
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
    printf("=== wirelog Bitwise Parser Tests (Issue #72) ===\n\n");

    printf("--- band (bitwise AND) ---\n");
    test_parse_band_in_head();
    test_parse_band_operands_are_variables();
    test_parse_band_with_integer_literal();
    test_parse_band_in_filter();

    printf("\n--- bor (bitwise OR) ---\n");
    test_parse_bor_in_head();
    test_parse_bor_in_filter();

    printf("\n--- bxor (bitwise XOR) ---\n");
    test_parse_bxor_in_head();
    test_parse_bxor_with_constants();

    printf("\n--- bnot (bitwise NOT, unary) ---\n");
    test_parse_bnot_in_head();
    test_parse_bnot_with_constant();
    test_parse_bnot_in_filter();

    printf("\n--- bshl / bshr (shift) ---\n");
    test_parse_bshl_in_head();
    test_parse_bshr_in_head();
    test_parse_bshl_in_filter();
    test_parse_bshr_in_filter();

    printf("\n--- Nested / Combined ---\n");
    test_parse_nested_band_bshr();
    test_parse_bor_bshl_combine();
    test_parse_all_binary_bitwise_ops_roundtrip();
    test_parse_bnot_combined_with_band();
    test_parse_bitwise_with_arithmetic_mixed();

    printf("\n=== Results: %d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf(" ===\n");

    return tests_failed > 0 ? 1 : 0;
}
