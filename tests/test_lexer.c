/*
 * test_lexer.c - wirelog Lexer Test Suite
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Comprehensive lexer tests based on FlowLog grammar.
 * Tests written first (TDD) before lexer implementation.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "../wirelog/parser/lexer.h"

/* ======================================================================== */
/* Test Helpers                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) \
    do { \
        tests_run++; \
        printf("  [TEST] %-50s", name); \
        fflush(stdout); \
    } while (0)

#define PASS() \
    do { \
        tests_passed++; \
        printf(" PASS\n"); \
    } while (0)

#define FAIL(msg) \
    do { \
        tests_failed++; \
        printf(" FAIL: %s\n", msg); \
    } while (0)

#define ASSERT_TOK(lexer, expected_type) \
    do { \
        wl_token_t tok = wl_lexer_next_token(&(lexer)); \
        if (tok.type != (expected_type)) { \
            char buf[256]; \
            snprintf(buf, sizeof(buf), \
                "expected %s, got %s (line %u, col %u)", \
                wl_token_type_str(expected_type), \
                wl_token_type_str(tok.type), \
                tok.line, tok.col); \
            FAIL(buf); \
            return; \
        } \
    } while (0)

#define ASSERT_TOK_VAL(lexer, expected_type, expected_val) \
    do { \
        wl_token_t tok = wl_lexer_next_token(&(lexer)); \
        if (tok.type != (expected_type)) { \
            char buf[256]; \
            snprintf(buf, sizeof(buf), \
                "expected %s, got %s", \
                wl_token_type_str(expected_type), \
                wl_token_type_str(tok.type)); \
            FAIL(buf); \
            return; \
        } \
        if (tok.int_value != (expected_val)) { \
            char buf[256]; \
            snprintf(buf, sizeof(buf), \
                "expected value %lld, got %lld", \
                (long long)(expected_val), (long long)tok.int_value); \
            FAIL(buf); \
            return; \
        } \
    } while (0)

#define ASSERT_TOK_STR(lexer, expected_type, expected_str) \
    do { \
        wl_token_t tok = wl_lexer_next_token(&(lexer)); \
        if (tok.type != (expected_type)) { \
            char buf[256]; \
            snprintf(buf, sizeof(buf), \
                "expected %s, got %s", \
                wl_token_type_str(expected_type), \
                wl_token_type_str(tok.type)); \
            FAIL(buf); \
            return; \
        } \
        char *s = wl_token_to_string(&tok); \
        if (strcmp(s, expected_str) != 0) { \
            char buf[256]; \
            snprintf(buf, sizeof(buf), \
                "expected \"%s\", got \"%s\"", expected_str, s); \
            free(s); \
            FAIL(buf); \
            return; \
        } \
        free(s); \
    } while (0)

/* ======================================================================== */
/* Lexer: Basic Token Tests                                                 */
/* ======================================================================== */

static void test_empty_input(void) {
    TEST("empty input yields EOF");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "");
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_whitespace_only(void) {
    TEST("whitespace-only yields EOF");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "   \t\n  \n  ");
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_identifier_simple(void) {
    TEST("simple identifier");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "edge");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "edge");
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_identifier_with_underscore_prefix(void) {
    TEST("identifier with underscore prefix");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "_tmp");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "_tmp");
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_identifier_with_digits(void) {
    TEST("identifier with digits");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "node42");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "node42");
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_identifier_with_underscores(void) {
    TEST("identifier with underscores");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "var_points_to");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "var_points_to");
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_standalone_underscore(void) {
    TEST("standalone underscore is wildcard");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "_");
    ASSERT_TOK(lex, WL_TOK_UNDERSCORE);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_integer_literal(void) {
    TEST("integer literal");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "42");
    ASSERT_TOK_VAL(lex, WL_TOK_INTEGER, 42);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_integer_zero(void) {
    TEST("integer zero");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "0");
    ASSERT_TOK_VAL(lex, WL_TOK_INTEGER, 0);
    PASS();
}

static void test_integer_large(void) {
    TEST("large integer");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "2147483647");
    ASSERT_TOK_VAL(lex, WL_TOK_INTEGER, 2147483647LL);
    PASS();
}

static void test_string_literal(void) {
    TEST("string literal");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "\"hello world\"");
    ASSERT_TOK_STR(lex, WL_TOK_STRING, "hello world");
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_string_empty(void) {
    TEST("empty string literal");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "\"\"");
    ASSERT_TOK_STR(lex, WL_TOK_STRING, "");
    PASS();
}

/* ======================================================================== */
/* Lexer: Boolean Literals                                                  */
/* ======================================================================== */

static void test_true_literal(void) {
    TEST("True boolean literal");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "True");
    ASSERT_TOK(lex, WL_TOK_TRUE);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_false_literal(void) {
    TEST("False boolean literal");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "False");
    ASSERT_TOK(lex, WL_TOK_FALSE);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

/* ======================================================================== */
/* Lexer: Keywords                                                          */
/* ======================================================================== */

static void test_aggregate_keywords_lowercase(void) {
    TEST("aggregate keywords (lowercase)");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "count sum min max average");
    ASSERT_TOK(lex, WL_TOK_COUNT);
    ASSERT_TOK(lex, WL_TOK_SUM);
    ASSERT_TOK(lex, WL_TOK_MIN);
    ASSERT_TOK(lex, WL_TOK_MAX);
    ASSERT_TOK(lex, WL_TOK_AVG);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_aggregate_keywords_uppercase(void) {
    TEST("aggregate keywords (uppercase)");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "COUNT SUM MIN MAX AVG");
    ASSERT_TOK(lex, WL_TOK_COUNT);
    ASSERT_TOK(lex, WL_TOK_SUM);
    ASSERT_TOK(lex, WL_TOK_MIN);
    ASSERT_TOK(lex, WL_TOK_MAX);
    ASSERT_TOK(lex, WL_TOK_AVG);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_type_keywords(void) {
    TEST("type keywords");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "int32 int64 string");
    ASSERT_TOK(lex, WL_TOK_INT32);
    ASSERT_TOK(lex, WL_TOK_INT64);
    ASSERT_TOK(lex, WL_TOK_STRING_TYPE);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_keyword_like_identifiers(void) {
    TEST("keyword-like identifiers not confused");
    wl_lexer_t lex;
    /* "counter" starts with "count" but is a longer identifier */
    wl_lexer_init(&lex, "counter minimum int32x stringy");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "counter");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "minimum");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "int32x");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "stringy");
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

/* ======================================================================== */
/* Lexer: Punctuation                                                       */
/* ======================================================================== */

static void test_punctuation(void) {
    TEST("punctuation tokens");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "( ) , : !");
    ASSERT_TOK(lex, WL_TOK_LPAREN);
    ASSERT_TOK(lex, WL_TOK_RPAREN);
    ASSERT_TOK(lex, WL_TOK_COMMA);
    ASSERT_TOK(lex, WL_TOK_COLON);
    ASSERT_TOK(lex, WL_TOK_BANG);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_dot_standalone(void) {
    TEST("standalone dot");
    wl_lexer_t lex;
    wl_lexer_init(&lex, ".");
    ASSERT_TOK(lex, WL_TOK_DOT);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

/* ======================================================================== */
/* Lexer: Operators                                                         */
/* ======================================================================== */

static void test_horn_clause(void) {
    TEST("horn clause operator :-");
    wl_lexer_t lex;
    wl_lexer_init(&lex, ":-");
    ASSERT_TOK(lex, WL_TOK_HORN);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_comparison_operators(void) {
    TEST("comparison operators");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "= != < > <= >=");
    ASSERT_TOK(lex, WL_TOK_EQ);
    ASSERT_TOK(lex, WL_TOK_NEQ);
    ASSERT_TOK(lex, WL_TOK_LT);
    ASSERT_TOK(lex, WL_TOK_GT);
    ASSERT_TOK(lex, WL_TOK_LTE);
    ASSERT_TOK(lex, WL_TOK_GTE);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_arithmetic_operators(void) {
    TEST("arithmetic operators");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "+ - * / %");
    ASSERT_TOK(lex, WL_TOK_PLUS);
    ASSERT_TOK(lex, WL_TOK_MINUS);
    ASSERT_TOK(lex, WL_TOK_STAR);
    ASSERT_TOK(lex, WL_TOK_SLASH);
    ASSERT_TOK(lex, WL_TOK_PERCENT);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

/* ======================================================================== */
/* Lexer: Directives                                                        */
/* ======================================================================== */

static void test_directive_decl(void) {
    TEST("directive .decl");
    wl_lexer_t lex;
    wl_lexer_init(&lex, ".decl");
    ASSERT_TOK(lex, WL_TOK_DECL);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_directive_input(void) {
    TEST("directive .input");
    wl_lexer_t lex;
    wl_lexer_init(&lex, ".input");
    ASSERT_TOK(lex, WL_TOK_INPUT);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_directive_output(void) {
    TEST("directive .output");
    wl_lexer_t lex;
    wl_lexer_init(&lex, ".output");
    ASSERT_TOK(lex, WL_TOK_OUTPUT);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_directive_printsize(void) {
    TEST("directive .printsize");
    wl_lexer_t lex;
    wl_lexer_init(&lex, ".printsize");
    ASSERT_TOK(lex, WL_TOK_PRINTSIZE);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_directive_plan(void) {
    TEST("directive .plan");
    wl_lexer_t lex;
    wl_lexer_init(&lex, ".plan");
    ASSERT_TOK(lex, WL_TOK_PLAN);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_dot_followed_by_non_directive(void) {
    TEST("dot followed by non-directive is DOT + IDENT");
    wl_lexer_t lex;
    wl_lexer_init(&lex, ".foo");
    ASSERT_TOK(lex, WL_TOK_DOT);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "foo");
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

/* ======================================================================== */
/* Lexer: Comments                                                          */
/* ======================================================================== */

static void test_hash_comment(void) {
    TEST("hash comment");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "# this is a comment\nfoo");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "foo");
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_double_slash_comment(void) {
    TEST("double-slash comment");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "// this is a comment\nbar");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "bar");
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_comment_at_end_of_line(void) {
    TEST("comment at end of line");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "foo # comment\nbar");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "foo");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "bar");
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_comment_only(void) {
    TEST("comment-only input");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "# just a comment");
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

/* ======================================================================== */
/* Lexer: Line/Column Tracking                                              */
/* ======================================================================== */

static void test_line_tracking(void) {
    TEST("line number tracking");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "foo\nbar\nbaz");
    wl_token_t t1 = wl_lexer_next_token(&lex);
    wl_token_t t2 = wl_lexer_next_token(&lex);
    wl_token_t t3 = wl_lexer_next_token(&lex);
    if (t1.line != 1 || t2.line != 2 || t3.line != 3) {
        char buf[128];
        snprintf(buf, sizeof(buf), "lines: %u,%u,%u expected 1,2,3",
                 t1.line, t2.line, t3.line);
        FAIL(buf);
        return;
    }
    PASS();
}

static void test_column_tracking(void) {
    TEST("column tracking");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "  foo bar");
    wl_token_t t1 = wl_lexer_next_token(&lex);
    wl_token_t t2 = wl_lexer_next_token(&lex);
    if (t1.col != 3 || t2.col != 7) {
        char buf[128];
        snprintf(buf, sizeof(buf), "cols: %u,%u expected 3,7",
                 t1.col, t2.col);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Lexer: Peek                                                              */
/* ======================================================================== */

static void test_peek_does_not_advance(void) {
    TEST("peek does not advance position");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "foo bar");
    wl_token_t t1 = wl_lexer_peek_token(&lex);
    wl_token_t t2 = wl_lexer_peek_token(&lex);
    wl_token_t t3 = wl_lexer_next_token(&lex);
    if (t1.type != WL_TOK_IDENT || t2.type != WL_TOK_IDENT ||
        t3.type != WL_TOK_IDENT) {
        FAIL("peek should return same token");
        return;
    }
    char *s1 = wl_token_to_string(&t1);
    char *s3 = wl_token_to_string(&t3);
    if (strcmp(s1, "foo") != 0 || strcmp(s3, "foo") != 0) {
        free(s1); free(s3);
        FAIL("peek and next should return 'foo'");
        return;
    }
    free(s1); free(s3);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "bar");
    PASS();
}

/* ======================================================================== */
/* Lexer: Complex Token Sequences                                           */
/* ======================================================================== */

static void test_declaration_tokens(void) {
    TEST("declaration token sequence");
    wl_lexer_t lex;
    wl_lexer_init(&lex, ".decl Arc(x: int32, y: int32)");
    ASSERT_TOK(lex, WL_TOK_DECL);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "Arc");
    ASSERT_TOK(lex, WL_TOK_LPAREN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "x");
    ASSERT_TOK(lex, WL_TOK_COLON);
    ASSERT_TOK(lex, WL_TOK_INT32);
    ASSERT_TOK(lex, WL_TOK_COMMA);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "y");
    ASSERT_TOK(lex, WL_TOK_COLON);
    ASSERT_TOK(lex, WL_TOK_INT32);
    ASSERT_TOK(lex, WL_TOK_RPAREN);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_input_directive_tokens(void) {
    TEST("input directive token sequence");
    wl_lexer_t lex;
    wl_lexer_init(&lex,
        ".input Arc(IO=\"file\", filename=\"Arc.csv\", delimiter=\",\")");
    ASSERT_TOK(lex, WL_TOK_INPUT);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "Arc");
    ASSERT_TOK(lex, WL_TOK_LPAREN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "IO");
    ASSERT_TOK(lex, WL_TOK_EQ);
    ASSERT_TOK_STR(lex, WL_TOK_STRING, "file");
    ASSERT_TOK(lex, WL_TOK_COMMA);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "filename");
    ASSERT_TOK(lex, WL_TOK_EQ);
    ASSERT_TOK_STR(lex, WL_TOK_STRING, "Arc.csv");
    ASSERT_TOK(lex, WL_TOK_COMMA);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "delimiter");
    ASSERT_TOK(lex, WL_TOK_EQ);
    ASSERT_TOK_STR(lex, WL_TOK_STRING, ",");
    ASSERT_TOK(lex, WL_TOK_RPAREN);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_rule_tokens(void) {
    TEST("rule token sequence");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "Tc(x, y) :- Arc(x, y).");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "Tc");
    ASSERT_TOK(lex, WL_TOK_LPAREN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "x");
    ASSERT_TOK(lex, WL_TOK_COMMA);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "y");
    ASSERT_TOK(lex, WL_TOK_RPAREN);
    ASSERT_TOK(lex, WL_TOK_HORN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "Arc");
    ASSERT_TOK(lex, WL_TOK_LPAREN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "x");
    ASSERT_TOK(lex, WL_TOK_COMMA);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "y");
    ASSERT_TOK(lex, WL_TOK_RPAREN);
    ASSERT_TOK(lex, WL_TOK_DOT);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_negation_tokens(void) {
    TEST("negation token sequence");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "r(x) :- a(x), !b(x).");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "r");
    ASSERT_TOK(lex, WL_TOK_LPAREN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "x");
    ASSERT_TOK(lex, WL_TOK_RPAREN);
    ASSERT_TOK(lex, WL_TOK_HORN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "a");
    ASSERT_TOK(lex, WL_TOK_LPAREN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "x");
    ASSERT_TOK(lex, WL_TOK_RPAREN);
    ASSERT_TOK(lex, WL_TOK_COMMA);
    ASSERT_TOK(lex, WL_TOK_BANG);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "b");
    ASSERT_TOK(lex, WL_TOK_LPAREN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "x");
    ASSERT_TOK(lex, WL_TOK_RPAREN);
    ASSERT_TOK(lex, WL_TOK_DOT);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_aggregate_tokens(void) {
    TEST("aggregate expression tokens");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "sssp(x, min(d)) :- sssp2(x, d).");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "sssp");
    ASSERT_TOK(lex, WL_TOK_LPAREN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "x");
    ASSERT_TOK(lex, WL_TOK_COMMA);
    ASSERT_TOK(lex, WL_TOK_MIN);
    ASSERT_TOK(lex, WL_TOK_LPAREN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "d");
    ASSERT_TOK(lex, WL_TOK_RPAREN);
    ASSERT_TOK(lex, WL_TOK_RPAREN);
    ASSERT_TOK(lex, WL_TOK_HORN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "sssp2");
    ASSERT_TOK(lex, WL_TOK_LPAREN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "x");
    ASSERT_TOK(lex, WL_TOK_COMMA);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "d");
    ASSERT_TOK(lex, WL_TOK_RPAREN);
    ASSERT_TOK(lex, WL_TOK_DOT);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_arithmetic_in_aggregate(void) {
    TEST("arithmetic inside aggregate");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "min(d1 + d2)");
    ASSERT_TOK(lex, WL_TOK_MIN);
    ASSERT_TOK(lex, WL_TOK_LPAREN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "d1");
    ASSERT_TOK(lex, WL_TOK_PLUS);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "d2");
    ASSERT_TOK(lex, WL_TOK_RPAREN);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_comparison_tokens(void) {
    TEST("comparison expression tokens");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "x != y, a >= 10, b < c");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "x");
    ASSERT_TOK(lex, WL_TOK_NEQ);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "y");
    ASSERT_TOK(lex, WL_TOK_COMMA);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "a");
    ASSERT_TOK(lex, WL_TOK_GTE);
    ASSERT_TOK_VAL(lex, WL_TOK_INTEGER, 10);
    ASSERT_TOK(lex, WL_TOK_COMMA);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "b");
    ASSERT_TOK(lex, WL_TOK_LT);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "c");
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_wildcard_in_atom(void) {
    TEST("wildcard in atom arguments");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "edge(_, y)");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "edge");
    ASSERT_TOK(lex, WL_TOK_LPAREN);
    ASSERT_TOK(lex, WL_TOK_UNDERSCORE);
    ASSERT_TOK(lex, WL_TOK_COMMA);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "y");
    ASSERT_TOK(lex, WL_TOK_RPAREN);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

static void test_plan_after_rule(void) {
    TEST(".plan after rule terminator");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "r(x) :- a(x). .plan");
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "r");
    ASSERT_TOK(lex, WL_TOK_LPAREN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "x");
    ASSERT_TOK(lex, WL_TOK_RPAREN);
    ASSERT_TOK(lex, WL_TOK_HORN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "a");
    ASSERT_TOK(lex, WL_TOK_LPAREN);
    ASSERT_TOK_STR(lex, WL_TOK_IDENT, "x");
    ASSERT_TOK(lex, WL_TOK_RPAREN);
    ASSERT_TOK(lex, WL_TOK_DOT);
    ASSERT_TOK(lex, WL_TOK_PLAN);
    ASSERT_TOK(lex, WL_TOK_EOF);
    PASS();
}

/* ======================================================================== */
/* Lexer: Error Cases                                                       */
/* ======================================================================== */

static void test_unterminated_string(void) {
    TEST("unterminated string produces error");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "\"hello");
    wl_token_t tok = wl_lexer_next_token(&lex);
    if (tok.type != WL_TOK_ERROR) {
        FAIL("expected WL_TOK_ERROR for unterminated string");
        return;
    }
    PASS();
}

static void test_unknown_character(void) {
    TEST("unknown character produces error");
    wl_lexer_t lex;
    wl_lexer_init(&lex, "@");
    wl_token_t tok = wl_lexer_next_token(&lex);
    if (tok.type != WL_TOK_ERROR) {
        FAIL("expected WL_TOK_ERROR for unknown char");
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int main(void) {
    printf("=== wirelog Lexer Tests ===\n\n");

    printf("--- Basic Tokens ---\n");
    test_empty_input();
    test_whitespace_only();
    test_identifier_simple();
    test_identifier_with_underscore_prefix();
    test_identifier_with_digits();
    test_identifier_with_underscores();
    test_standalone_underscore();
    test_integer_literal();
    test_integer_zero();
    test_integer_large();
    test_string_literal();
    test_string_empty();

    printf("\n--- Boolean Literals ---\n");
    test_true_literal();
    test_false_literal();

    printf("\n--- Keywords ---\n");
    test_aggregate_keywords_lowercase();
    test_aggregate_keywords_uppercase();
    test_type_keywords();
    test_keyword_like_identifiers();

    printf("\n--- Punctuation ---\n");
    test_punctuation();
    test_dot_standalone();

    printf("\n--- Operators ---\n");
    test_horn_clause();
    test_comparison_operators();
    test_arithmetic_operators();

    printf("\n--- Directives ---\n");
    test_directive_decl();
    test_directive_input();
    test_directive_output();
    test_directive_printsize();
    test_directive_plan();
    test_dot_followed_by_non_directive();

    printf("\n--- Comments ---\n");
    test_hash_comment();
    test_double_slash_comment();
    test_comment_at_end_of_line();
    test_comment_only();

    printf("\n--- Line/Column Tracking ---\n");
    test_line_tracking();
    test_column_tracking();

    printf("\n--- Peek ---\n");
    test_peek_does_not_advance();

    printf("\n--- Complex Sequences ---\n");
    test_declaration_tokens();
    test_input_directive_tokens();
    test_rule_tokens();
    test_negation_tokens();
    test_aggregate_tokens();
    test_arithmetic_in_aggregate();
    test_comparison_tokens();
    test_wildcard_in_atom();
    test_plan_after_rule();

    printf("\n--- Error Cases ---\n");
    test_unterminated_string();
    test_unknown_character();

    printf("\n=== Results: %d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf(" ===\n");

    return tests_failed > 0 ? 1 : 0;
}
