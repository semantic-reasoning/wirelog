/*
 * test_string_parser.c - Parser Tests for String Function Syntax (Issue #143)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests that all 13 string function keywords are recognized by the lexer/parser
 * and that expression syntax (nested, mixed with arithmetic) parses correctly.
 * Also verifies that syntax errors (missing parens, missing commas) are detected.
 */

#include "../wirelog/wirelog.h"

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
/* Parse Helpers                                                            */
/* ======================================================================== */

static void
assert_parses(const char *test_name, const char *src)
{
    TEST(test_name);
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        FAIL("expected parse success but got NULL");
        return;
    }
    wirelog_program_free(prog);
    PASS();
}

static void
assert_parse_fails(const char *test_name, const char *src)
{
    TEST(test_name);
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (prog) {
        wirelog_program_free(prog);
        FAIL("expected parse failure but got success");
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Valid Syntax: All 13 String Functions                                   */
/* ======================================================================== */

static void
test_parse_strlen(void)
{
    const char *src =
        ".decl a(x: symbol)\n"
        ".decl r(z: int64)\n"
        "r(strlen(x)) :- a(x).\n";
    assert_parses("strlen(var) parses successfully", src);
}

static void
test_parse_cat(void)
{
    const char *src =
        ".decl a(x: symbol, y: symbol)\n"
        ".decl r(z: symbol)\n"
        "r(cat(x, y)) :- a(x, y).\n";
    assert_parses("cat(a, b) parses successfully", src);
}

static void
test_parse_substr(void)
{
    const char *src =
        ".decl a(x: symbol)\n"
        ".decl r(z: symbol)\n"
        "r(substr(x, 0, 5)) :- a(x).\n";
    assert_parses("substr(x, 0, 5) parses successfully", src);
}

static void
test_parse_contains(void)
{
    const char *src =
        ".decl a(x: symbol, y: symbol)\n"
        ".decl r(z: symbol)\n"
        "r(x) :- a(x, y), contains(x, y).\n";
    assert_parses("contains(a, b) as filter predicate parses successfully",
        src);
}

static void
test_parse_str_prefix(void)
{
    const char *src =
        ".decl a(x: symbol, y: symbol)\n"
        ".decl r(z: symbol)\n"
        "r(x) :- a(x, y), str_prefix(x, y).\n";
    assert_parses("str_prefix(a, b) parses successfully", src);
}

static void
test_parse_str_suffix(void)
{
    const char *src =
        ".decl a(x: symbol, y: symbol)\n"
        ".decl r(z: symbol)\n"
        "r(x) :- a(x, y), str_suffix(x, y).\n";
    assert_parses("str_suffix(a, b) parses successfully", src);
}

static void
test_parse_str_ord(void)
{
    const char *src =
        ".decl a(x: symbol)\n"
        ".decl r(z: int64)\n"
        "r(str_ord(x)) :- a(x).\n";
    assert_parses("str_ord(x) parses successfully", src);
}

static void
test_parse_to_upper(void)
{
    const char *src =
        ".decl a(x: symbol)\n"
        ".decl r(z: symbol)\n"
        "r(to_upper(x)) :- a(x).\n";
    assert_parses("to_upper(x) parses successfully", src);
}

static void
test_parse_to_lower(void)
{
    const char *src =
        ".decl a(x: symbol)\n"
        ".decl r(z: symbol)\n"
        "r(to_lower(x)) :- a(x).\n";
    assert_parses("to_lower(x) parses successfully", src);
}

static void
test_parse_str_replace(void)
{
    const char *src =
        ".decl a(x: symbol, y: symbol, z: symbol)\n"
        ".decl r(w: symbol)\n"
        "r(str_replace(x, y, z)) :- a(x, y, z).\n";
    assert_parses("str_replace(x, y, z) parses successfully", src);
}

static void
test_parse_trim(void)
{
    const char *src =
        ".decl a(x: symbol)\n"
        ".decl r(z: symbol)\n"
        "r(trim(x)) :- a(x).\n";
    assert_parses("trim(x) parses successfully", src);
}

static void
test_parse_to_string(void)
{
    const char *src =
        ".decl a(x: int64)\n"
        ".decl r(z: symbol)\n"
        "r(to_string(x)) :- a(x).\n";
    assert_parses("to_string(x) parses successfully", src);
}

static void
test_parse_to_number(void)
{
    const char *src =
        ".decl a(x: symbol)\n"
        ".decl r(z: int64)\n"
        "r(to_number(x)) :- a(x).\n";
    assert_parses("to_number(x) parses successfully", src);
}

/* ======================================================================== */
/* Nested and Complex Expressions                                           */
/* ======================================================================== */

static void
test_parse_nested_cat_substr(void)
{
    const char *src =
        ".decl a(x: symbol, y: symbol)\n"
        ".decl r(z: symbol)\n"
        "r(cat(substr(x, 0, 2), y)) :- a(x, y).\n";
    assert_parses("cat(substr(x, 0, 2), y) nested expression parses", src);
}

static void
test_parse_nested_upper_lower(void)
{
    const char *src =
        ".decl a(x: symbol)\n"
        ".decl r(z: symbol)\n"
        "r(to_lower(to_upper(x))) :- a(x).\n";
    assert_parses("to_lower(to_upper(x)) double-nested parses", src);
}

static void
test_parse_strlen_arithmetic(void)
{
    const char *src =
        ".decl a(x: symbol)\n"
        ".decl r(z: int64)\n"
        "r(strlen(x) + 1) :- a(x).\n";
    assert_parses("strlen(x) + 1 mixed with arithmetic parses", src);
}

static void
test_parse_nested_to_number_to_string(void)
{
    const char *src =
        ".decl a(x: int64)\n"
        ".decl r(z: int64)\n"
        "r(to_number(to_string(x))) :- a(x).\n";
    assert_parses("to_number(to_string(x)) round-trip nesting parses", src);
}

static void
test_parse_contains_with_to_string(void)
{
    const char *src =
        ".decl a(x: int64, y: int64)\n"
        ".decl r(z: int64)\n"
        "r(x) :- a(x, y), contains(to_string(x), to_string(y)).\n";
    assert_parses("contains(to_string(x), to_string(y)) parses", src);
}

/* ======================================================================== */
/* Syntax Error Detection                                                   */
/* ======================================================================== */

static void
test_parse_error_strlen_missing_paren(void)
{
    const char *src =
        ".decl a(x: symbol)\n"
        ".decl r(z: int64)\n"
        "r(strlen x) :- a(x).\n";
    assert_parse_fails("strlen without parens is a parse error", src);
}

static void
test_parse_error_substr_missing_comma(void)
{
    const char *src =
        ".decl a(x: symbol)\n"
        ".decl r(z: symbol)\n"
        "r(substr(x 0 5)) :- a(x).\n";
    assert_parse_fails("substr missing comma separators is a parse error", src);
}

static void
test_parse_error_cat_missing_closing_paren(void)
{
    const char *src =
        ".decl a(x: symbol, y: symbol)\n"
        ".decl r(z: symbol)\n"
        "r(cat(x, y) :- a(x, y).\n";
    assert_parse_fails("cat missing closing paren is a parse error", src);
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== String Function Parser Tests (Issue #143) ===\n");

    printf("\n--- Valid Syntax: All 13 Functions ---\n");
    test_parse_strlen();
    test_parse_cat();
    test_parse_substr();
    test_parse_contains();
    test_parse_str_prefix();
    test_parse_str_suffix();
    test_parse_str_ord();
    test_parse_to_upper();
    test_parse_to_lower();
    test_parse_str_replace();
    test_parse_trim();
    test_parse_to_string();
    test_parse_to_number();

    printf("\n--- Nested and Complex Expressions ---\n");
    test_parse_nested_cat_substr();
    test_parse_nested_upper_lower();
    test_parse_strlen_arithmetic();
    test_parse_nested_to_number_to_string();
    test_parse_contains_with_to_string();

    printf("\n--- Syntax Error Detection ---\n");
    test_parse_error_strlen_missing_paren();
    test_parse_error_substr_missing_comma();
    test_parse_error_cat_missing_closing_paren();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
