/*
 * test_csv.c - CSV reader unit tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests the CSV reader: line parsing, file reading, delimiter handling.
 */

#include "../wirelog/io/csv_reader.h"

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
/* Test: wl_csv_parse_line                                                  */
/* ======================================================================== */

static void
test_parse_line_basic(void)
{
    TEST("parse_line: comma-separated integers");

    int64_t values[16];
    uint32_t count = 0;
    int rc = wl_csv_parse_line("1,2,3", ',', values, 16, &count);

    if (rc != 0) {
        FAIL("returned non-zero");
        return;
    }
    if (count != 3) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 3 values, got %u", count);
        FAIL(msg);
        return;
    }
    if (values[0] != 1 || values[1] != 2 || values[2] != 3) {
        FAIL("wrong values");
        return;
    }
    PASS();
}

static void
test_parse_line_single_value(void)
{
    TEST("parse_line: single value");

    int64_t values[16];
    uint32_t count = 0;
    int rc = wl_csv_parse_line("42", ',', values, 16, &count);

    if (rc != 0) {
        FAIL("returned non-zero");
        return;
    }
    if (count != 1 || values[0] != 42) {
        FAIL("expected single value 42");
        return;
    }
    PASS();
}

static void
test_parse_line_negative(void)
{
    TEST("parse_line: negative numbers");

    int64_t values[16];
    uint32_t count = 0;
    int rc = wl_csv_parse_line("-1,0,-999", ',', values, 16, &count);

    if (rc != 0) {
        FAIL("returned non-zero");
        return;
    }
    if (count != 3) {
        FAIL("expected 3 values");
        return;
    }
    if (values[0] != -1 || values[1] != 0 || values[2] != -999) {
        FAIL("wrong values");
        return;
    }
    PASS();
}

static void
test_parse_line_whitespace(void)
{
    TEST("parse_line: whitespace around values");

    int64_t values[16];
    uint32_t count = 0;
    int rc = wl_csv_parse_line(" 1 , 2 , 3 ", ',', values, 16, &count);

    if (rc != 0) {
        FAIL("returned non-zero");
        return;
    }
    if (count != 3 || values[0] != 1 || values[1] != 2 || values[2] != 3) {
        FAIL("whitespace not handled");
        return;
    }
    PASS();
}

static void
test_parse_line_tab_delimiter(void)
{
    TEST("parse_line: tab delimiter");

    int64_t values[16];
    uint32_t count = 0;
    int rc = wl_csv_parse_line("10\t20\t30", '\t', values, 16, &count);

    if (rc != 0) {
        FAIL("returned non-zero");
        return;
    }
    if (count != 3 || values[0] != 10 || values[1] != 20 || values[2] != 30) {
        FAIL("tab delimiter not handled");
        return;
    }
    PASS();
}

static void
test_parse_line_empty(void)
{
    TEST("parse_line: empty string returns 0 count");

    int64_t values[16];
    uint32_t count = 99;
    int rc = wl_csv_parse_line("", ',', values, 16, &count);

    if (rc != 0) {
        FAIL("returned non-zero");
        return;
    }
    if (count != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 0 values, got %u", count);
        FAIL(msg);
        return;
    }
    PASS();
}

static void
test_parse_line_null_input(void)
{
    TEST("parse_line: NULL input returns error");

    int64_t values[16];
    uint32_t count = 0;
    int rc = wl_csv_parse_line(NULL, ',', values, 16, &count);

    if (rc == 0) {
        FAIL("should return error for NULL");
        return;
    }
    PASS();
}

static void
test_parse_line_overflow(void)
{
    TEST("parse_line: buffer too small returns error");

    int64_t values[2];
    uint32_t count = 0;
    int rc = wl_csv_parse_line("1,2,3", ',', values, 2, &count);

    if (rc == 0) {
        FAIL("should return error when buffer too small");
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
    printf("\n=== wirelog CSV Reader Tests ===\n\n");

    printf("--- Line Parsing ---\n");
    test_parse_line_basic();
    test_parse_line_single_value();
    test_parse_line_negative();
    test_parse_line_whitespace();
    test_parse_line_tab_delimiter();
    test_parse_line_empty();
    test_parse_line_null_input();
    test_parse_line_overflow();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
