/*
 * test_cli.c - CLI driver unit tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests the CLI driver components: file reading, pipeline execution,
 * and output formatting.
 */

#include "../wirelog/ffi/dd_ffi.h"
#include "../wirelog/wirelog-parser.h"
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
/* CLI internals under test                                                 */
/* ======================================================================== */

#include "../wirelog/cli/driver.h"

/* ======================================================================== */
/* Test: wl_read_file                                                       */
/* ======================================================================== */

static void
test_read_file_basic(void)
{
    TEST("wl_read_file reads a .dl file");

    /* Write a temp file */
    const char *path = "/tmp/wirelog_test_read.dl";
    FILE *f = fopen(path, "w");
    if (!f) {
        FAIL("cannot create temp file");
        return;
    }
    fprintf(f, ".decl edge(x: int32, y: int32)\nedge(1, 2).\n");
    fclose(f);

    char *contents = wl_read_file(path);
    if (!contents) {
        FAIL("wl_read_file returned NULL");
        return;
    }

    if (strstr(contents, ".decl edge") == NULL) {
        free(contents);
        FAIL("contents missing .decl edge");
        return;
    }

    if (strstr(contents, "edge(1, 2).") == NULL) {
        free(contents);
        FAIL("contents missing edge(1, 2).");
        return;
    }

    free(contents);
    remove(path);
    PASS();
}

static void
test_read_file_nonexistent(void)
{
    TEST("wl_read_file returns NULL for nonexistent file");

    char *contents = wl_read_file("/tmp/wirelog_no_such_file_xyz.dl");
    if (contents != NULL) {
        free(contents);
        FAIL("expected NULL for nonexistent file");
        return;
    }

    PASS();
}

static void
test_read_file_null_path(void)
{
    TEST("wl_read_file returns NULL for NULL path");

    char *contents = wl_read_file(NULL);
    if (contents != NULL) {
        free(contents);
        FAIL("expected NULL for NULL path");
        return;
    }

    PASS();
}

/* ======================================================================== */
/* Test: wl_print_tuple (output formatting callback)                        */
/* ======================================================================== */

static void
test_print_tuple_basic(void)
{
    TEST("wl_print_tuple formats relation(v1, v2)");

    const char *path = "/tmp/wirelog_test_output.txt";
    FILE *f = fopen(path, "w");
    if (!f) {
        FAIL("cannot create temp file");
        return;
    }

    int64_t row[] = { 1, 2 };
    wl_print_tuple("edge", row, 2, f);
    fclose(f);

    char *contents = wl_read_file(path);
    if (!contents) {
        FAIL("cannot read output file");
        return;
    }

    if (strcmp(contents, "edge(1, 2)\n") != 0) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected 'edge(1, 2)\\n', got '%s'",
                 contents);
        free(contents);
        remove(path);
        FAIL(msg);
        return;
    }

    free(contents);
    remove(path);
    PASS();
}

static void
test_print_tuple_single_col(void)
{
    TEST("wl_print_tuple formats single column");

    const char *path = "/tmp/wirelog_test_output2.txt";
    FILE *f = fopen(path, "w");
    if (!f) {
        FAIL("cannot create temp file");
        return;
    }

    int64_t row[] = { 42 };
    wl_print_tuple("node", row, 1, f);
    fclose(f);

    char *contents = wl_read_file(path);
    if (!contents) {
        FAIL("cannot read output file");
        return;
    }

    if (strcmp(contents, "node(42)\n") != 0) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected 'node(42)\\n', got '%s'",
                 contents);
        free(contents);
        remove(path);
        FAIL(msg);
        return;
    }

    free(contents);
    remove(path);
    PASS();
}

/* ======================================================================== */
/* Test: wl_run_pipeline (full execution pipeline)                          */
/* ======================================================================== */

static void
test_run_pipeline_tc(void)
{
    TEST("wl_run_pipeline executes transitive closure");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "edge(1, 2).\n"
                      "edge(2, 3).\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    const char *outpath = "/tmp/wirelog_test_pipeline.txt";
    FILE *f = fopen(outpath, "w");
    if (!f) {
        FAIL("cannot create output file");
        return;
    }

    int rc = wl_run_pipeline(src, 1, f);
    fclose(f);

    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "wl_run_pipeline returned %d", rc);
        remove(outpath);
        FAIL(msg);
        return;
    }

    char *output = wl_read_file(outpath);
    if (!output) {
        remove(outpath);
        FAIL("cannot read output file");
        return;
    }

    /* Should have 3 tc tuples: (1,2), (2,3), (1,3) */
    int count = 0;
    const char *p = output;
    while ((p = strstr(p, "tc(")) != NULL) {
        count++;
        p++;
    }

    if (count != 3) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected 3 tc tuples, got %d\n%s", count,
                 output);
        free(output);
        remove(outpath);
        FAIL(msg);
        return;
    }

    free(output);
    remove(outpath);
    PASS();
}

static void
test_run_pipeline_null_source(void)
{
    TEST("wl_run_pipeline rejects NULL source");

    int rc = wl_run_pipeline(NULL, 1, stdout);
    if (rc == 0) {
        FAIL("expected non-zero for NULL source");
        return;
    }

    PASS();
}

static void
test_run_pipeline_parse_error(void)
{
    TEST("wl_run_pipeline returns error for invalid source");

    int rc = wl_run_pipeline("this is not valid datalog @@#$", 1, stdout);
    if (rc == 0) {
        FAIL("expected non-zero for invalid source");
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
    printf("\n=== wirelog CLI Driver Tests ===\n\n");

    printf("--- File Reading ---\n");
    test_read_file_basic();
    test_read_file_nonexistent();
    test_read_file_null_path();

    printf("\n--- Output Formatting ---\n");
    test_print_tuple_basic();
    test_print_tuple_single_col();

    printf("\n--- Pipeline ---\n");
    test_run_pipeline_tc();
    test_run_pipeline_null_source();
    test_run_pipeline_parse_error();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
