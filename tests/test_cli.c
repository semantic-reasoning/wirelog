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

#include "../wirelog/intern.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"

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
    char path[512];
    test_tmppath(path, sizeof(path), "wirelog_test_read.dl");
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

    char nxpath[512];
    test_tmppath(nxpath, sizeof(nxpath), "wirelog_no_such_file_xyz.dl");
    char *contents = wl_read_file(nxpath);
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

    char path[512];
    test_tmppath(path, sizeof(path), "wirelog_test_output.txt");
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

    char path[512];
    test_tmppath(path, sizeof(path), "wirelog_test_output2.txt");
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

    char outpath[512];
    test_tmppath(outpath, sizeof(outpath), "wirelog_test_pipeline.txt");
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
/* Test: pipeline with .input directive (CSV loading)                        */
/* ======================================================================== */

static void
test_run_pipeline_csv_input(void)
{
    TEST("wl_run_pipeline with .input CSV loads external data");

    /* Create CSV file */
    char csv_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wirelog_test_pipeline_edges.csv");
    FILE *csv = fopen(csv_path, "w");
    if (!csv) {
        FAIL("cannot create CSV file");
        return;
    }
    fprintf(csv, "1,2\n2,3\n3,4\n");
    fclose(csv);

    char src[1024];
    snprintf(src, sizeof(src),
             ".decl edge(x: int32, y: int32)\n"
             ".input edge(filename=\"%s\", delimiter=\",\")\n"
             ".decl tc(x: int32, y: int32)\n"
             "tc(x, y) :- edge(x, y).\n"
             "tc(x, z) :- tc(x, y), edge(y, z).\n",
             csv_path);

    char outpath[512];
    test_tmppath(outpath, sizeof(outpath), "wirelog_test_pipeline_csv_out.txt");
    FILE *f = fopen(outpath, "w");
    if (!f) {
        remove(csv_path);
        FAIL("cannot create output file");
        return;
    }

    int rc = wl_run_pipeline(src, 1, f);
    fclose(f);

    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "wl_run_pipeline returned %d", rc);
        remove(outpath);
        remove(csv_path);
        FAIL(msg);
        return;
    }

    char *output = wl_read_file(outpath);
    if (!output) {
        remove(outpath);
        remove(csv_path);
        FAIL("cannot read output file");
        return;
    }

    /* Should have 6 tc tuples */
    int count = 0;
    const char *p = output;
    while ((p = strstr(p, "tc(")) != NULL) {
        count++;
        p++;
    }

    if (count != 6) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected 6 tc tuples, got %d\n%s", count,
                 output);
        free(output);
        remove(outpath);
        remove(csv_path);
        FAIL(msg);
        return;
    }

    free(output);
    remove(outpath);
    remove(csv_path);
    PASS();
}

static void
test_run_pipeline_csv_missing_file(void)
{
    TEST("wl_run_pipeline fails when .input CSV file is missing");

    char nxcsv[512];
    test_tmppath(nxcsv, sizeof(nxcsv), "wirelog_no_such_file_xyz.csv");
    char src[1024];
    snprintf(src, sizeof(src),
             ".decl edge(x: int32, y: int32)\n"
             ".input edge(filename=\"%s\", delimiter=\",\")\n"
             ".decl tc(x: int32, y: int32)\n"
             "tc(x, y) :- edge(x, y).\n",
             nxcsv);

    int rc = wl_run_pipeline(src, 1, stdout);
    if (rc == 0) {
        FAIL("should fail for missing CSV file");
        return;
    }
    PASS();
}

static void
test_run_pipeline_csv_tab_delimiter(void)
{
    TEST("wl_run_pipeline with tab-delimited CSV");

    char csv_path[512];
    test_tmppath(csv_path, sizeof(csv_path), "wirelog_test_tab.csv");
    FILE *csv = fopen(csv_path, "w");
    if (!csv) {
        FAIL("cannot create CSV file");
        return;
    }
    fprintf(csv, "1\t2\n2\t3\n");
    fclose(csv);

    char src[1024];
    snprintf(src, sizeof(src),
             ".decl edge(x: int32, y: int32)\n"
             ".input edge(filename=\"%s\", delimiter=\"\\t\")\n"
             ".decl r(x: int32, y: int32)\n"
             "r(x, y) :- edge(x, y).\n",
             csv_path);

    char outpath[512];
    test_tmppath(outpath, sizeof(outpath), "wirelog_test_tab_out.txt");
    FILE *f = fopen(outpath, "w");
    if (!f) {
        remove(csv_path);
        FAIL("cannot create output file");
        return;
    }

    int rc = wl_run_pipeline(src, 1, f);
    fclose(f);

    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "wl_run_pipeline returned %d", rc);
        remove(outpath);
        remove(csv_path);
        FAIL(msg);
        return;
    }

    char *output = wl_read_file(outpath);
    if (!output) {
        remove(outpath);
        remove(csv_path);
        FAIL("cannot read output");
        return;
    }

    int count = 0;
    const char *p = output;
    while ((p = strstr(p, "r(")) != NULL) {
        count++;
        p++;
    }

    if (count != 2) {
        char msg[128];
        snprintf(msg, sizeof(msg), "expected 2 r tuples, got %d\n%s", count,
                 output);
        free(output);
        remove(outpath);
        remove(csv_path);
        FAIL(msg);
        return;
    }

    free(output);
    remove(outpath);
    remove(csv_path);
    PASS();
}

/* ======================================================================== */
/* Test: pipeline with string-typed output (symbol interning)                */
/* ======================================================================== */

static void
test_run_pipeline_string_output(void)
{
    TEST(
        "wl_run_pipeline outputs strings (not integer IDs) for string columns");

    const char *src = ".decl edge(x: string, y: string)\n"
                      "edge(\"A\", \"B\").\n"
                      "edge(\"B\", \"C\").\n"
                      ".decl tc(x: string, y: string)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    char outpath[512];
    test_tmppath(outpath, sizeof(outpath), "wirelog_test_string_out.txt");
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

    /* Output should contain string values like "A", "B", "C"
     * rather than integer IDs like 0, 1, 2 */
    if (strstr(output, "\"A\"") == NULL) {
        char msg[256];
        snprintf(msg, sizeof(msg), "output should contain '\"A\"' but got: %s",
                 output);
        free(output);
        remove(outpath);
        FAIL(msg);
        return;
    }
    if (strstr(output, "\"B\"") == NULL) {
        free(output);
        remove(outpath);
        FAIL("output should contain '\"B\"'");
        return;
    }
    if (strstr(output, "\"C\"") == NULL) {
        free(output);
        remove(outpath);
        FAIL("output should contain '\"C\"'");
        return;
    }

    /* Should have 3 tc tuples: (A,B), (B,C), (A,C) */
    int count = 0;
    const char *p = output;
    while ((p = strstr(p, "tc(")) != NULL) {
        count++;
        p++;
    }

    if (count != 3) {
        char msg[256];
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
test_run_pipeline_mixed_type_output(void)
{
    TEST("wl_run_pipeline outputs mixed string/int columns correctly");

    const char *src = ".decl person(name: string, age: int32)\n"
                      "person(\"Alice\", 30).\n"
                      "person(\"Bob\", 25).\n"
                      ".decl result(name: string, age: int32)\n"
                      "result(n, a) :- person(n, a).\n";

    char outpath[512];
    test_tmppath(outpath, sizeof(outpath), "wirelog_test_mixed_out.txt");
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

    /* Should contain string "Alice" and integer 30 */
    if (strstr(output, "\"Alice\"") == NULL) {
        char msg[256];
        snprintf(msg, sizeof(msg),
                 "output should contain '\"Alice\"' but got: %s", output);
        free(output);
        remove(outpath);
        FAIL(msg);
        return;
    }
    if (strstr(output, "30") == NULL) {
        free(output);
        remove(outpath);
        FAIL("output should contain integer 30");
        return;
    }

    free(output);
    remove(outpath);
    PASS();
}

/* ======================================================================== */
/* Test: .output directive filters output to marked relations               */
/* ======================================================================== */

static void
test_run_pipeline_output_filter(void)
{
    TEST("wl_run_pipeline outputs only .output-marked relations");

    /* Two IDB relations: tc and reach.  Only tc has .output. */
    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      ".output tc\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n"
                      ".decl reach(x: int32)\n"
                      "reach(1).\n"
                      "reach(y) :- reach(x), edge(x, y).\n";

    char outpath[512];
    test_tmppath(outpath, sizeof(outpath), "wirelog_test_output_filter.txt");
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

    /* tc should be in output */
    if (strstr(output, "tc(") == NULL) {
        char msg[256];
        snprintf(msg, sizeof(msg), "output should contain tc tuples: %s",
                 output);
        free(output);
        remove(outpath);
        FAIL(msg);
        return;
    }

    /* reach should NOT be in output (IDB but no .output directive) */
    if (strstr(output, "reach(") != NULL) {
        char msg[256];
        snprintf(msg, sizeof(msg), "output should NOT contain reach tuples: %s",
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
test_run_pipeline_no_output_directive(void)
{
    TEST("wl_run_pipeline outputs all relations when no .output is used");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    char outpath[512];
    test_tmppath(outpath, sizeof(outpath), "wirelog_test_no_output_dir.txt");
    FILE *f = fopen(outpath, "w");
    if (!f) {
        FAIL("cannot create output file");
        return;
    }

    int rc = wl_run_pipeline(src, 1, f);
    fclose(f);

    if (rc != 0) {
        remove(outpath);
        FAIL("wl_run_pipeline failed");
        return;
    }

    char *output = wl_read_file(outpath);
    if (!output) {
        remove(outpath);
        FAIL("cannot read output file");
        return;
    }

    /* Both tc and edge should be in output (no .output filtering) */
    if (strstr(output, "tc(") == NULL) {
        free(output);
        remove(outpath);
        FAIL("output should contain tc tuples");
        return;
    }

    free(output);
    remove(outpath);
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

    printf("\n--- Pipeline with .input CSV ---\n");
    test_run_pipeline_csv_input();
    test_run_pipeline_csv_missing_file();
    test_run_pipeline_csv_tab_delimiter();

    printf("\n--- Pipeline with String Output ---\n");
    test_run_pipeline_string_output();
    test_run_pipeline_mixed_type_output();

    printf("\n--- Pipeline with .output Directive ---\n");
    test_run_pipeline_output_filter();
    test_run_pipeline_no_output_directive();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
