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
#include "test_tmpdir.h"

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
/* Test: wl_csv_read_file                                                   */
/* ======================================================================== */

static void
test_read_file_basic(void)
{
    TEST("read_file: 3-row 2-column CSV");

    char path[512];
    test_tmppath(path, sizeof(path), "wirelog_test_csv_basic.csv");
    FILE *f = fopen(path, "w");
    if (!f) {
        FAIL("cannot create temp file");
        return;
    }
    fprintf(f, "1,2\n3,4\n5,6\n");
    fclose(f);

    int64_t *data = NULL;
    uint32_t nrows = 0, ncols = 0;
    int rc = wl_csv_read_file(path, ',', &data, &nrows, &ncols);
    remove(path);

    if (rc != 0) {
        FAIL("returned non-zero");
        return;
    }
    if (nrows != 3 || ncols != 2) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 3x2, got %ux%u", nrows, ncols);
        free(data);
        FAIL(msg);
        return;
    }
    /* Row-major: data[row * ncols + col] */
    if (data[0] != 1 || data[1] != 2 || data[2] != 3 || data[3] != 4
        || data[4] != 5 || data[5] != 6) {
        free(data);
        FAIL("wrong data values");
        return;
    }
    free(data);
    PASS();
}

static void
test_read_file_single_column(void)
{
    TEST("read_file: single-column CSV");

    char path[512];
    test_tmppath(path, sizeof(path), "wirelog_test_csv_1col.csv");
    FILE *f = fopen(path, "w");
    if (!f) {
        FAIL("cannot create temp file");
        return;
    }
    fprintf(f, "10\n20\n30\n");
    fclose(f);

    int64_t *data = NULL;
    uint32_t nrows = 0, ncols = 0;
    int rc = wl_csv_read_file(path, ',', &data, &nrows, &ncols);
    remove(path);

    if (rc != 0) {
        FAIL("returned non-zero");
        return;
    }
    if (nrows != 3 || ncols != 1) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 3x1, got %ux%u", nrows, ncols);
        free(data);
        FAIL(msg);
        return;
    }
    if (data[0] != 10 || data[1] != 20 || data[2] != 30) {
        free(data);
        FAIL("wrong data values");
        return;
    }
    free(data);
    PASS();
}

static void
test_read_file_empty_lines(void)
{
    TEST("read_file: skips empty lines and trailing newline");

    char path[512];
    test_tmppath(path, sizeof(path), "wirelog_test_csv_empty.csv");
    FILE *f = fopen(path, "w");
    if (!f) {
        FAIL("cannot create temp file");
        return;
    }
    fprintf(f, "1,2\n\n3,4\n\n");
    fclose(f);

    int64_t *data = NULL;
    uint32_t nrows = 0, ncols = 0;
    int rc = wl_csv_read_file(path, ',', &data, &nrows, &ncols);
    remove(path);

    if (rc != 0) {
        FAIL("returned non-zero");
        return;
    }
    if (nrows != 2) {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected 2 rows, got %u", nrows);
        free(data);
        FAIL(msg);
        return;
    }
    free(data);
    PASS();
}

static void
test_read_file_nonexistent(void)
{
    TEST("read_file: nonexistent file returns error");

    char path[512];
    test_tmppath(path, sizeof(path), "wirelog_no_such_csv.csv");

    int64_t *data = NULL;
    uint32_t nrows = 0, ncols = 0;
    int rc = wl_csv_read_file(path, ',', &data, &nrows, &ncols);

    if (rc == 0) {
        free(data);
        FAIL("should return error for nonexistent file");
        return;
    }
    PASS();
}

static void
test_read_file_null_args(void)
{
    TEST("read_file: NULL arguments return error");

    int64_t *data = NULL;
    uint32_t nrows = 0, ncols = 0;

    if (wl_csv_read_file(NULL, ',', &data, &nrows, &ncols) == 0) {
        FAIL("should error for NULL path");
        return;
    }

    char path[512];
    test_tmppath(path, sizeof(path), "x.csv");
    if (wl_csv_read_file(path, ',', NULL, &nrows, &ncols) == 0) {
        FAIL("should error for NULL data");
        return;
    }
    PASS();
}

static void
test_read_file_inconsistent_cols(void)
{
    TEST("read_file: inconsistent column count returns error");

    char path[512];
    test_tmppath(path, sizeof(path), "wirelog_test_csv_incon.csv");
    FILE *f = fopen(path, "w");
    if (!f) {
        FAIL("cannot create temp file");
        return;
    }
    fprintf(f, "1,2\n3,4,5\n");
    fclose(f);

    int64_t *data = NULL;
    uint32_t nrows = 0, ncols = 0;
    int rc = wl_csv_read_file(path, ',', &data, &nrows, &ncols);
    remove(path);

    if (rc == 0) {
        free(data);
        FAIL("should return error for inconsistent columns");
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

    printf("\n--- File Reading ---\n");
    test_read_file_basic();
    test_read_file_single_column();
    test_read_file_empty_lines();
    test_read_file_nonexistent();
    test_read_file_null_args();
    test_read_file_inconsistent_cols();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
