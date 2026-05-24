/*
 * test_crc32_parser.c - CRC-32 Built-in Parser Tests (Issue #884)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Verifies that the parser recognizes the crc32_ethernet() and
 * crc32_castagnoli() built-in functions and creates correct BINARY_EXPR AST
 * nodes with WIRELOG_ARITH_CRC32_ETH / WIRELOG_ARITH_CRC32_CAST ops.
 *
 * Syntax: crc32_ethernet(expr), crc32_castagnoli(expr)  -- unary, like md5(expr).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/parser/parser.h"

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                      \
        do {                                \
            tests_run++;                    \
            printf("  [TEST] %-60s", name); \
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
/* crc32_ethernet() Parser Tests                                            */
/* ======================================================================== */

static void
test_parse_crc32_ethernet_in_head(void)
{
    TEST("crc32_ethernet(x) in head creates BINARY_EXPR with CRC32_ETH op");
    PARSE("r(crc32_ethernet(x)) :- a(x).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR) {
        CLEANUP();
        FAIL("expected BINARY_EXPR in head");
        return;
    }
    if (expr->arith_op != WIRELOG_ARITH_CRC32_ETH) {
        CLEANUP();
        FAIL("expected arith_op == WIRELOG_ARITH_CRC32_ETH");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_crc32_ethernet_has_one_child(void)
{
    TEST("crc32_ethernet(x) is unary: exactly 1 child");
    PARSE("r(crc32_ethernet(x)) :- a(x).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->arith_op != WIRELOG_ARITH_CRC32_ETH
        || expr->child_count != 1) {
        CLEANUP();
        FAIL("expected unary CRC32_ETH expression");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_crc32_ethernet_in_filter(void)
{
    TEST(
        "crc32_ethernet(x) = y in body creates COMPARISON with CRC32_ETH left");
    PARSE("r(x) :- a(x, y), crc32_ethernet(x) = y.");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *lhs = first_body_cmp_left(program);
    if (!lhs || lhs->type != WL_PARSER_AST_NODE_BINARY_EXPR
        || lhs->arith_op != WIRELOG_ARITH_CRC32_ETH) {
        CLEANUP();
        FAIL("expected CRC32_ETH expr on left side of comparison");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_crc32_ethernet_filter_neq(void)
{
    TEST("crc32_ethernet(x) != y in body comparison parses cleanly");
    PARSE("r(x) :- a(x, y), crc32_ethernet(x) != y.");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *lhs = first_body_cmp_left(program);
    if (!lhs || lhs->arith_op != WIRELOG_ARITH_CRC32_ETH) {
        CLEANUP();
        FAIL("expected CRC32_ETH expr on left side of != comparison");
        return;
    }
    CLEANUP();
    PASS();
}

/*
 * Issue #884 minimal reproducer: the original symptom was that the parser
 * could not consume crc32_ethernet on either side of an equality.  Pin the
 * minimal program so regressions stay visible.
 */
static void
test_parse_crc32_ethernet_issue_884_reproducer(void)
{
    TEST("Issue #884 minimal repro: in(s,c) + crc32_ethernet(S) = C");
    PARSE(".decl in(s: symbol, c: int64)\n"
        ".decl out(s: symbol)\n"
        "in(\"abc\", 123).\n"
        "out(S) :- in(S, C), crc32_ethernet(S) = C.\n");
    ASSERT_PARSED();
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* crc32_castagnoli() Parser Tests                                          */
/* ======================================================================== */

static void
test_parse_crc32_castagnoli_in_head(void)
{
    TEST("crc32_castagnoli(x) in head creates BINARY_EXPR with CRC32_CAST op");
    PARSE("r(crc32_castagnoli(x)) :- a(x).");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *expr = first_head_arg(program);
    if (!expr || expr->type != WL_PARSER_AST_NODE_BINARY_EXPR) {
        CLEANUP();
        FAIL("expected BINARY_EXPR in head");
        return;
    }
    if (expr->arith_op != WIRELOG_ARITH_CRC32_CAST) {
        CLEANUP();
        FAIL("expected arith_op == WIRELOG_ARITH_CRC32_CAST");
        return;
    }
    CLEANUP();
    PASS();
}

static void
test_parse_crc32_castagnoli_in_filter(void)
{
    TEST("crc32_castagnoli(x) = y on left side of comparison");
    PARSE("r(x) :- a(x, y), crc32_castagnoli(x) = y.");
    ASSERT_PARSED();

    const wl_parser_ast_node_t *lhs = first_body_cmp_left(program);
    if (!lhs || lhs->arith_op != WIRELOG_ARITH_CRC32_CAST) {
        CLEANUP();
        FAIL("expected CRC32_CAST expr on left side of comparison");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Distinctness sanity                                                      */
/* ======================================================================== */

static void
test_parse_crc32_variants_distinct_ops(void)
{
    TEST("crc32_ethernet and crc32_castagnoli map to distinct arith_ops");
    if (WIRELOG_ARITH_CRC32_ETH == WIRELOG_ARITH_CRC32_CAST) {
        FAIL("CRC32_ETH and CRC32_CAST collide in arith_op enum");
        return;
    }
    PASS();
}

int
main(void)
{
    printf("=== wirelog CRC-32 Parser Tests (Issue #884) ===\n\n");

    printf("--- crc32_ethernet() ---\n");
    test_parse_crc32_ethernet_in_head();
    test_parse_crc32_ethernet_has_one_child();
    test_parse_crc32_ethernet_in_filter();
    test_parse_crc32_ethernet_filter_neq();
    test_parse_crc32_ethernet_issue_884_reproducer();

    printf("\n--- crc32_castagnoli() ---\n");
    test_parse_crc32_castagnoli_in_head();
    test_parse_crc32_castagnoli_in_filter();

    printf("\n--- Sanity ---\n");
    test_parse_crc32_variants_distinct_ops();

    printf("\n=== Results: %d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf(" ===\n");

    return tests_failed > 0 ? 1 : 0;
}
