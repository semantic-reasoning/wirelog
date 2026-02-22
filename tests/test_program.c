/*
 * test_program.c - wirelog Program + IR Conversion Test Suite
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests written first (TDD) before program implementation.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/parser/parser.h"
#include "../wirelog/ir/program.h"

/* ======================================================================== */
/* Test Helpers                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) \
    do { \
        tests_run++; \
        printf("  [TEST] %-55s", name); \
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

/* Helper: parse string and build program */
static struct wirelog_program*
make_program(const char *source)
{
    char errbuf[512] = {0};
    wl_ast_node_t *ast = wl_parse_string(source, errbuf, sizeof(errbuf));
    if (!ast) return NULL;

    struct wirelog_program *prog = wl_program_create();
    if (!prog) {
        wl_ast_node_free(ast);
        return NULL;
    }

    prog->ast = ast;
    if (wl_program_collect_metadata(prog, ast) != 0) {
        wl_program_free(prog);
        return NULL;
    }

    return prog;
}

/* ======================================================================== */
/* Metadata Collection Tests                                                */
/* ======================================================================== */

static void
test_decl_single_relation(void)
{
    TEST("Parse .decl with 2 columns");

    struct wirelog_program *prog = make_program(
        ".decl Arc(x: int32, y: int32)\n"
    );

    if (!prog) { FAIL("program is NULL"); return; }

    if (prog->relation_count != 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 1 relation, got %u", prog->relation_count);
        wl_program_free(prog);
        FAIL(buf);
        return;
    }

    if (strcmp(prog->relations[0].name, "Arc") != 0) {
        wl_program_free(prog);
        FAIL("relation name should be Arc");
        return;
    }

    if (prog->relations[0].column_count != 2) {
        wl_program_free(prog);
        FAIL("should have 2 columns");
        return;
    }

    if (strcmp(prog->relations[0].columns[0].name, "x") != 0) {
        wl_program_free(prog);
        FAIL("column 0 name should be x");
        return;
    }

    wl_program_free(prog);
    PASS();
}

static void
test_input_directive(void)
{
    TEST("Parse .input marks relation has_input");

    struct wirelog_program *prog = make_program(
        ".decl Arc(x: int32, y: int32)\n"
        ".input Arc(filename=\"data.csv\", delimiter=\"\\t\")\n"
    );

    if (!prog) { FAIL("program is NULL"); return; }

    if (!prog->relations[0].has_input) {
        wl_program_free(prog);
        FAIL("has_input should be true");
        return;
    }

    if (prog->relations[0].input_param_count < 1) {
        wl_program_free(prog);
        FAIL("should have input params");
        return;
    }

    wl_program_free(prog);
    PASS();
}

static void
test_output_directive(void)
{
    TEST("Parse .output marks relation has_output");

    struct wirelog_program *prog = make_program(
        ".decl Reach(x: int32, y: int32)\n"
        ".output Reach\n"
    );

    if (!prog) { FAIL("program is NULL"); return; }

    if (!prog->relations[0].has_output) {
        wl_program_free(prog);
        FAIL("has_output should be true");
        return;
    }

    wl_program_free(prog);
    PASS();
}

static void
test_printsize_directive(void)
{
    TEST("Parse .printsize marks relation has_printsize");

    struct wirelog_program *prog = make_program(
        ".decl Tc(x: int32, y: int32)\n"
        ".printsize Tc\n"
    );

    if (!prog) { FAIL("program is NULL"); return; }

    if (!prog->relations[0].has_printsize) {
        wl_program_free(prog);
        FAIL("has_printsize should be true");
        return;
    }

    wl_program_free(prog);
    PASS();
}

static void
test_full_tc_metadata(void)
{
    TEST("Full TC program: 2 relations with correct metadata");

    struct wirelog_program *prog = make_program(
        ".decl Arc(x: int32, y: int32)\n"
        ".decl Tc(x: int32, y: int32)\n"
        ".input Arc(filename=\"arc.csv\")\n"
        ".output Tc\n"
        "Tc(x, y) :- Arc(x, y).\n"
        "Tc(x, y) :- Tc(x, z), Arc(z, y).\n"
    );

    if (!prog) { FAIL("program is NULL"); return; }

    if (prog->relation_count != 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 2 relations, got %u", prog->relation_count);
        wl_program_free(prog);
        FAIL(buf);
        return;
    }

    /* Find Arc and Tc */
    int arc_idx = -1, tc_idx = -1;
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (strcmp(prog->relations[i].name, "Arc") == 0) arc_idx = (int)i;
        if (strcmp(prog->relations[i].name, "Tc") == 0) tc_idx = (int)i;
    }

    if (arc_idx < 0 || tc_idx < 0) {
        wl_program_free(prog);
        FAIL("should have Arc and Tc relations");
        return;
    }

    if (!prog->relations[arc_idx].has_input) {
        wl_program_free(prog);
        FAIL("Arc should have has_input");
        return;
    }

    if (!prog->relations[tc_idx].has_output) {
        wl_program_free(prog);
        FAIL("Tc should have has_output");
        return;
    }

    if (prog->rule_count != 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 2 rules, got %u", prog->rule_count);
        wl_program_free(prog);
        FAIL(buf);
        return;
    }

    wl_program_free(prog);
    PASS();
}

static void
test_no_rules_program(void)
{
    TEST("Program with no rules has 0 rule_count");

    struct wirelog_program *prog = make_program(
        ".decl Arc(x: int32, y: int32)\n"
        ".input Arc(filename=\"data.csv\")\n"
    );

    if (!prog) { FAIL("program is NULL"); return; }

    if (prog->rule_count != 0) {
        wl_program_free(prog);
        FAIL("rule_count should be 0");
        return;
    }

    wl_program_free(prog);
    PASS();
}

static void
test_schema_synthesis(void)
{
    TEST("Schema synthesis from relation metadata");

    struct wirelog_program *prog = make_program(
        ".decl Arc(x: int32, y: int32)\n"
    );

    if (!prog) { FAIL("program is NULL"); return; }

    wl_program_build_schemas(prog);

    if (!prog->schemas) {
        wl_program_free(prog);
        FAIL("schemas should not be NULL");
        return;
    }

    if (strcmp(prog->schemas[0].relation_name, "Arc") != 0) {
        wl_program_free(prog);
        FAIL("schema relation_name should be Arc");
        return;
    }

    if (prog->schemas[0].column_count != 2) {
        wl_program_free(prog);
        FAIL("schema column_count should be 2");
        return;
    }

    if (strcmp(prog->schemas[0].columns[0].name, "x") != 0) {
        wl_program_free(prog);
        FAIL("schema column 0 name should be x");
        return;
    }

    wl_program_free(prog);
    PASS();
}

static void
test_default_stratum(void)
{
    TEST("Default stratum contains all rule head names");

    struct wirelog_program *prog = make_program(
        ".decl Arc(x: int32, y: int32)\n"
        ".decl Tc(x: int32, y: int32)\n"
        "Tc(x, y) :- Arc(x, y).\n"
        "Tc(x, y) :- Tc(x, z), Arc(z, y).\n"
    );

    if (!prog) { FAIL("program is NULL"); return; }

    wl_program_build_default_stratum(prog);

    if (prog->stratum_count != 1) {
        wl_program_free(prog);
        FAIL("stratum_count should be 1");
        return;
    }

    if (prog->strata[0].stratum_id != 0) {
        wl_program_free(prog);
        FAIL("stratum_id should be 0");
        return;
    }

    if (prog->strata[0].rule_count != 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 2 rule names, got %u", prog->strata[0].rule_count);
        wl_program_free(prog);
        FAIL(buf);
        return;
    }

    wl_program_free(prog);
    PASS();
}

static void
test_program_free_null(void)
{
    TEST("Program free handles NULL safely");

    wl_program_free(NULL);

    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("\n=== wirelog Program Tests ===\n\n");

    /* Metadata collection */
    test_decl_single_relation();
    test_input_directive();
    test_output_directive();
    test_printsize_directive();
    test_full_tc_metadata();
    test_no_rules_program();

    /* Schema and stratum synthesis */
    test_schema_synthesis();
    test_default_stratum();

    /* Safety */
    test_program_free_null();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
