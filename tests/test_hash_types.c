/*
 * test_hash_types.c - Hash Function Enum Type Tests (Issue #144)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Verifies that WIRELOG_ARITH_HASH is defined in wirelog_arith_op_t,
 * is distinct from all other arith ops, and wirelog_arith_op_str()
 * returns a non-empty string for it.
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
test_hash_enum_defined(void)
{
    TEST("WIRELOG_ARITH_HASH enum value is defined");
    wirelog_arith_op_t op = WIRELOG_ARITH_HASH;
    if ((int)op < 0) {
        FAIL("WIRELOG_ARITH_HASH must be non-negative");
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Enum Distinctness Tests                                                  */
/* ======================================================================== */

static void
test_hash_distinct_from_arithmetic_ops(void)
{
    TEST("WIRELOG_ARITH_HASH does not collide with ADD/SUB/MUL/DIV/MOD");
    int existing[] = {
        (int)WIRELOG_ARITH_ADD, (int)WIRELOG_ARITH_SUB, (int)WIRELOG_ARITH_MUL,
        (int)WIRELOG_ARITH_DIV, (int)WIRELOG_ARITH_MOD,
    };
    int hash_val = (int)WIRELOG_ARITH_HASH;
    for (int j = 0; j < 5; j++) {
        if (hash_val == existing[j]) {
            FAIL("WIRELOG_ARITH_HASH collides with arithmetic op");
            return;
        }
    }
    PASS();
}

static void
test_hash_distinct_from_bitwise_ops(void)
{
    TEST("WIRELOG_ARITH_HASH does not collide with bitwise ops");
    int bitwise[] = {
        (int)WIRELOG_ARITH_BAND, (int)WIRELOG_ARITH_BOR,
        (int)WIRELOG_ARITH_BXOR, (int)WIRELOG_ARITH_BNOT,
        (int)WIRELOG_ARITH_SHL,  (int)WIRELOG_ARITH_SHR,
    };
    int hash_val = (int)WIRELOG_ARITH_HASH;
    for (int j = 0; j < 6; j++) {
        if (hash_val == bitwise[j]) {
            FAIL("WIRELOG_ARITH_HASH collides with bitwise op");
            return;
        }
    }
    PASS();
}

/* ======================================================================== */
/* String Conversion Tests                                                  */
/* ======================================================================== */

static void
test_arith_op_str_hash(void)
{
    TEST("wirelog_arith_op_str(HASH) returns non-null non-empty string");
    const char *s = wirelog_arith_op_str(WIRELOG_ARITH_HASH);
    if (!s || s[0] == '\0') {
        FAIL("expected non-empty string for HASH");
        return;
    }
    PASS();
}

static void
test_arith_op_str_hash_is_hash(void)
{
    TEST("wirelog_arith_op_str(HASH) returns \"hash\"");
    const char *s = wirelog_arith_op_str(WIRELOG_ARITH_HASH);
    if (!s || strcmp(s, "hash") != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected \"hash\", got \"%s\"",
                 s ? s : "(null)");
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
    printf("=== wirelog Hash Types Tests (Issue #144) ===\n\n");

    printf("--- Enum Definitions ---\n");
    test_hash_enum_defined();

    printf("\n--- Enum Distinctness ---\n");
    test_hash_distinct_from_arithmetic_ops();
    test_hash_distinct_from_bitwise_ops();

    printf("\n--- String Conversion ---\n");
    test_arith_op_str_hash();
    test_arith_op_str_hash_is_hash();

    printf("\n=== Results: %d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf(" ===\n");

    return tests_failed > 0 ? 1 : 0;
}
