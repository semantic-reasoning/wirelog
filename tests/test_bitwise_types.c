/*
 * test_bitwise_types.c - Bitwise Operator Enum Type Tests (Issue #72)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * TDD RED phase: verifies that bitwise operator enum values are defined
 * in wirelog_arith_op_t and wirelog_arith_op_str() handles them.
 *
 * These tests FAIL until US-001 (Define bitwise operator enums) is complete.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/wirelog-types.h"

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
/* Enum Value Existence Tests                                               */
/* ======================================================================== */

static void
test_band_enum_defined(void)
{
    TEST("WIRELOG_ARITH_BAND enum value is defined");
    wirelog_arith_op_t op = WIRELOG_ARITH_BAND;
    /* Just assigning it proves it compiles; cast to int for comparison */
    if ((int)op < 0) {
        FAIL("WIRELOG_ARITH_BAND must be non-negative");
        return;
    }
    PASS();
}

static void
test_bor_enum_defined(void)
{
    TEST("WIRELOG_ARITH_BOR enum value is defined");
    wirelog_arith_op_t op = WIRELOG_ARITH_BOR;
    if ((int)op < 0) {
        FAIL("WIRELOG_ARITH_BOR must be non-negative");
        return;
    }
    PASS();
}

static void
test_bxor_enum_defined(void)
{
    TEST("WIRELOG_ARITH_BXOR enum value is defined");
    wirelog_arith_op_t op = WIRELOG_ARITH_BXOR;
    if ((int)op < 0) {
        FAIL("WIRELOG_ARITH_BXOR must be non-negative");
        return;
    }
    PASS();
}

static void
test_bnot_enum_defined(void)
{
    TEST("WIRELOG_ARITH_BNOT enum value is defined");
    wirelog_arith_op_t op = WIRELOG_ARITH_BNOT;
    if ((int)op < 0) {
        FAIL("WIRELOG_ARITH_BNOT must be non-negative");
        return;
    }
    PASS();
}

static void
test_shl_enum_defined(void)
{
    TEST("WIRELOG_ARITH_SHL enum value is defined");
    wirelog_arith_op_t op = WIRELOG_ARITH_SHL;
    if ((int)op < 0) {
        FAIL("WIRELOG_ARITH_SHL must be non-negative");
        return;
    }
    PASS();
}

static void
test_shr_enum_defined(void)
{
    TEST("WIRELOG_ARITH_SHR enum value is defined");
    wirelog_arith_op_t op = WIRELOG_ARITH_SHR;
    if ((int)op < 0) {
        FAIL("WIRELOG_ARITH_SHR must be non-negative");
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Enum Distinctness Tests                                                  */
/* ======================================================================== */

static void
test_bitwise_ops_distinct_from_each_other(void)
{
    TEST("all 6 bitwise enum values are distinct");
    int band = (int)WIRELOG_ARITH_BAND;
    int bor  = (int)WIRELOG_ARITH_BOR;
    int bxor = (int)WIRELOG_ARITH_BXOR;
    int bnot = (int)WIRELOG_ARITH_BNOT;
    int shl  = (int)WIRELOG_ARITH_SHL;
    int shr  = (int)WIRELOG_ARITH_SHR;

    if (band == bor || band == bxor || band == bnot || band == shl
        || band == shr) {
        FAIL("BAND collides with another bitwise op");
        return;
    }
    if (bor == bxor || bor == bnot || bor == shl || bor == shr) {
        FAIL("BOR collides with another bitwise op");
        return;
    }
    if (bxor == bnot || bxor == shl || bxor == shr) {
        FAIL("BXOR collides with another bitwise op");
        return;
    }
    if (bnot == shl || bnot == shr) {
        FAIL("BNOT collides with SHL or SHR");
        return;
    }
    if (shl == shr) {
        FAIL("SHL collides with SHR");
        return;
    }
    PASS();
}

static void
test_bitwise_ops_distinct_from_arithmetic_ops(void)
{
    TEST("bitwise enum values do not collide with ADD/SUB/MUL/DIV/MOD");
    int existing[] = {
        (int)WIRELOG_ARITH_ADD,
        (int)WIRELOG_ARITH_SUB,
        (int)WIRELOG_ARITH_MUL,
        (int)WIRELOG_ARITH_DIV,
        (int)WIRELOG_ARITH_MOD,
    };
    int bitwise[] = {
        (int)WIRELOG_ARITH_BAND,
        (int)WIRELOG_ARITH_BOR,
        (int)WIRELOG_ARITH_BXOR,
        (int)WIRELOG_ARITH_BNOT,
        (int)WIRELOG_ARITH_SHL,
        (int)WIRELOG_ARITH_SHR,
    };

    for (int i = 0; i < 6; i++) {
        for (int j = 0; j < 5; j++) {
            if (bitwise[i] == existing[j]) {
                char buf[80];
                snprintf(buf, sizeof(buf),
                         "bitwise op %d collides with existing arith op %d",
                         bitwise[i], existing[j]);
                FAIL(buf);
                return;
            }
        }
    }
    PASS();
}

/* ======================================================================== */
/* String Conversion Tests                                                  */
/* ======================================================================== */

static void
test_arith_op_str_band(void)
{
    TEST("wirelog_arith_op_str(BAND) returns non-null non-empty string");
    const char *s = wirelog_arith_op_str(WIRELOG_ARITH_BAND);
    if (!s || s[0] == '\0') {
        FAIL("expected non-empty string for BAND");
        return;
    }
    PASS();
}

static void
test_arith_op_str_bor(void)
{
    TEST("wirelog_arith_op_str(BOR) returns non-null non-empty string");
    const char *s = wirelog_arith_op_str(WIRELOG_ARITH_BOR);
    if (!s || s[0] == '\0') {
        FAIL("expected non-empty string for BOR");
        return;
    }
    PASS();
}

static void
test_arith_op_str_bxor(void)
{
    TEST("wirelog_arith_op_str(BXOR) returns non-null non-empty string");
    const char *s = wirelog_arith_op_str(WIRELOG_ARITH_BXOR);
    if (!s || s[0] == '\0') {
        FAIL("expected non-empty string for BXOR");
        return;
    }
    PASS();
}

static void
test_arith_op_str_bnot(void)
{
    TEST("wirelog_arith_op_str(BNOT) returns non-null non-empty string");
    const char *s = wirelog_arith_op_str(WIRELOG_ARITH_BNOT);
    if (!s || s[0] == '\0') {
        FAIL("expected non-empty string for BNOT");
        return;
    }
    PASS();
}

static void
test_arith_op_str_shl(void)
{
    TEST("wirelog_arith_op_str(SHL) returns non-null non-empty string");
    const char *s = wirelog_arith_op_str(WIRELOG_ARITH_SHL);
    if (!s || s[0] == '\0') {
        FAIL("expected non-empty string for SHL");
        return;
    }
    PASS();
}

static void
test_arith_op_str_shr(void)
{
    TEST("wirelog_arith_op_str(SHR) returns non-null non-empty string");
    const char *s = wirelog_arith_op_str(WIRELOG_ARITH_SHR);
    if (!s || s[0] == '\0') {
        FAIL("expected non-empty string for SHR");
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
    printf("=== wirelog Bitwise Types Tests (Issue #72) ===\n\n");

    printf("--- Enum Definitions ---\n");
    test_band_enum_defined();
    test_bor_enum_defined();
    test_bxor_enum_defined();
    test_bnot_enum_defined();
    test_shl_enum_defined();
    test_shr_enum_defined();

    printf("\n--- Enum Distinctness ---\n");
    test_bitwise_ops_distinct_from_each_other();
    test_bitwise_ops_distinct_from_arithmetic_ops();

    printf("\n--- String Conversion ---\n");
    test_arith_op_str_band();
    test_arith_op_str_bor();
    test_arith_op_str_bxor();
    test_arith_op_str_bnot();
    test_arith_op_str_shl();
    test_arith_op_str_shr();

    printf("\n=== Results: %d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf(" ===\n");

    return tests_failed > 0 ? 1 : 0;
}
