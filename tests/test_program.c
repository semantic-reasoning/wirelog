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
#include "../wirelog/intern.h"
#include "../wirelog/wirelog-parser.h"

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

/* Helper: parse string and build program */
static struct wirelog_program *
make_program(const char *source)
{
    char errbuf[512] = { 0 };
    wl_parser_ast_node_t *ast
        = wl_parser_parse_string(source, errbuf, sizeof(errbuf));
    if (!ast)
        return NULL;

    struct wirelog_program *prog = wl_ir_program_create();
    if (!prog) {
        wl_parser_ast_node_free(ast);
        return NULL;
    }

    prog->ast = ast;
    if (wl_ir_program_collect_metadata(prog, ast) != 0) {
        wl_ir_program_free(prog);
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

    struct wirelog_program *prog
        = make_program(".decl Arc(x: int32, y: int32)\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (prog->relation_count != 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 1 relation, got %u",
            prog->relation_count);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    if (strcmp(prog->relations[0].name, "Arc") != 0) {
        wl_ir_program_free(prog);
        FAIL("relation name should be Arc");
        return;
    }

    if (prog->relations[0].column_count != 2) {
        wl_ir_program_free(prog);
        FAIL("should have 2 columns");
        return;
    }

    if (strcmp(prog->relations[0].columns[0].name, "x") != 0) {
        wl_ir_program_free(prog);
        FAIL("column 0 name should be x");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_input_directive(void)
{
    TEST("Parse .input marks relation has_input");

    struct wirelog_program *prog = make_program(
        ".decl Arc(x: int32, y: int32)\n"
        ".input Arc(filename=\"data.csv\", delimiter=\"\\t\")\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (!prog->relations[0].has_input) {
        wl_ir_program_free(prog);
        FAIL("has_input should be true");
        return;
    }

    if (prog->relations[0].input_param_count < 1) {
        wl_ir_program_free(prog);
        FAIL("should have input params");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_input_directive_param_names_values(void)
{
    TEST(
        "wl_ir_from_program extracts input_param_names and input_param_values");

    struct wirelog_program *prog = make_program(
        ".decl Arc(x: int32, y: int32)\n"
        ".input Arc(IO=\"file\", filename=\"test.csv\", delimiter=\",\")\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (!prog->relations[0].has_input) {
        wl_ir_program_free(prog);
        FAIL("has_input should be true");
        return;
    }

    if (prog->relations[0].input_param_count != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 params, got %u",
            prog->relations[0].input_param_count);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    /* Verify param names are stored */
    const char *expected_names[] = { "IO", "filename", "delimiter" };
    const char *expected_values[] = { "file", "test.csv", "," };
    for (uint32_t i = 0; i < 3; i++) {
        if (!prog->relations[0].input_param_names[i]
            || strcmp(prog->relations[0].input_param_names[i],
            expected_names[i])
            != 0) {
            char buf[128];
            snprintf(buf, sizeof(buf), "param %u name: expected '%s', got '%s'",
                i, expected_names[i],
                prog->relations[0].input_param_names[i]
                         ? prog->relations[0].input_param_names[i]
                         : "(null)");
            wl_ir_program_free(prog);
            FAIL(buf);
            return;
        }
        if (!prog->relations[0].input_param_values[i]
            || strcmp(prog->relations[0].input_param_values[i],
            expected_values[i])
            != 0) {
            char buf[128];
            snprintf(buf, sizeof(buf),
                "param %u value: expected '%s', got '%s'", i,
                expected_values[i],
                prog->relations[0].input_param_values[i]
                         ? prog->relations[0].input_param_values[i]
                         : "(null)");
            wl_ir_program_free(prog);
            FAIL(buf);
            return;
        }
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_input_io_scheme_present(void)
{
    TEST("input_io_scheme populated from io param");

    struct wirelog_program *prog = make_program(
        ".decl R(x: int32, y: int32)\n"
        ".input R(io=\"pcap\", filename=\"x.pcap\")\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (!prog->relations[0].input_io_scheme
        || strcmp(prog->relations[0].input_io_scheme, "pcap") != 0) {
        wl_ir_program_free(prog);
        FAIL("input_io_scheme should be \"pcap\"");
        return;
    }

    /* "io" must NOT appear in input_param_names */
    if (prog->relations[0].input_param_count != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 passthrough param, got %u",
            prog->relations[0].input_param_count);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    if (!prog->relations[0].input_param_names[0]
        || strcmp(prog->relations[0].input_param_names[0], "filename") != 0) {
        wl_ir_program_free(prog);
        FAIL("passthrough param should be \"filename\"");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_input_io_scheme_absent(void)
{
    TEST("input_io_scheme NULL when io param absent");

    struct wirelog_program *prog = make_program(
        ".decl R(x: int32, y: int32)\n"
        ".input R(filename=\"x.csv\")\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (prog->relations[0].input_io_scheme != NULL) {
        wl_ir_program_free(prog);
        FAIL("input_io_scheme should be NULL");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_output_directive(void)
{
    TEST("Parse .output marks relation has_output");

    struct wirelog_program *prog
        = make_program(".decl Reach(x: int32, y: int32)\n"
            ".output Reach\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (!prog->relations[0].has_output) {
        wl_ir_program_free(prog);
        FAIL("has_output should be true");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_output_directive_with_filename(void)
{
    TEST("Parse .output(filename=...) stores output_file");

    struct wirelog_program *prog
        = make_program(".decl Reach(x: int32, y: int32)\n"
            ".output Reach(filename=\"reach.csv\")\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (!prog->relations[0].has_output) {
        wl_ir_program_free(prog);
        FAIL("has_output should be true");
        return;
    }

    if (!prog->relations[0].output_file
        || strcmp(prog->relations[0].output_file, "reach.csv") != 0) {
        wl_ir_program_free(prog);
        FAIL("output_file should be \"reach.csv\"");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_printsize_directive(void)
{
    TEST("Parse .printsize marks relation has_printsize");

    struct wirelog_program *prog = make_program(".decl Tc(x: int32, y: int32)\n"
            ".printsize Tc\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (!prog->relations[0].has_printsize) {
        wl_ir_program_free(prog);
        FAIL("has_printsize should be true");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_full_tc_metadata(void)
{
    TEST("Full TC program: 2 relations with correct metadata");

    struct wirelog_program *prog
        = make_program(".decl Arc(x: int32, y: int32)\n"
            ".decl Tc(x: int32, y: int32)\n"
            ".input Arc(filename=\"arc.csv\")\n"
            ".output Tc\n"
            "Tc(x, y) :- Arc(x, y).\n"
            "Tc(x, y) :- Tc(x, z), Arc(z, y).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (prog->relation_count != 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 2 relations, got %u",
            prog->relation_count);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    /* Find Arc and Tc */
    int arc_idx = -1, tc_idx = -1;
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (strcmp(prog->relations[i].name, "Arc") == 0)
            arc_idx = (int)i;
        if (strcmp(prog->relations[i].name, "Tc") == 0)
            tc_idx = (int)i;
    }

    if (arc_idx < 0 || tc_idx < 0) {
        wl_ir_program_free(prog);
        FAIL("should have Arc and Tc relations");
        return;
    }

    if (!prog->relations[arc_idx].has_input) {
        wl_ir_program_free(prog);
        FAIL("Arc should have has_input");
        return;
    }

    if (!prog->relations[tc_idx].has_output) {
        wl_ir_program_free(prog);
        FAIL("Tc should have has_output");
        return;
    }

    if (prog->rule_count != 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 2 rules, got %u",
            prog->rule_count);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_no_rules_program(void)
{
    TEST("Program with no rules has 0 rule_count");

    struct wirelog_program *prog
        = make_program(".decl Arc(x: int32, y: int32)\n"
            ".input Arc(filename=\"data.csv\")\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (prog->rule_count != 0) {
        wl_ir_program_free(prog);
        FAIL("rule_count should be 0");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_schema_synthesis(void)
{
    TEST("Schema synthesis from relation metadata");

    struct wirelog_program *prog
        = make_program(".decl Arc(x: int32, y: int32)\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wl_ir_program_build_schemas(prog);

    if (!prog->schemas) {
        wl_ir_program_free(prog);
        FAIL("schemas should not be NULL");
        return;
    }

    if (strcmp(prog->schemas[0].relation_name, "Arc") != 0) {
        wl_ir_program_free(prog);
        FAIL("schema relation_name should be Arc");
        return;
    }

    if (prog->schemas[0].column_count != 2) {
        wl_ir_program_free(prog);
        FAIL("schema column_count should be 2");
        return;
    }

    if (strcmp(prog->schemas[0].columns[0].name, "x") != 0) {
        wl_ir_program_free(prog);
        FAIL("schema column 0 name should be x");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_default_stratum(void)
{
    TEST("Default stratum contains all rule head names");

    struct wirelog_program *prog
        = make_program(".decl Arc(x: int32, y: int32)\n"
            ".decl Tc(x: int32, y: int32)\n"
            "Tc(x, y) :- Arc(x, y).\n"
            "Tc(x, y) :- Tc(x, z), Arc(z, y).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wl_ir_program_build_default_stratum(prog);

    if (prog->stratum_count != 1) {
        wl_ir_program_free(prog);
        FAIL("stratum_count should be 1");
        return;
    }

    if (prog->strata[0].stratum_id != 0) {
        wl_ir_program_free(prog);
        FAIL("stratum_id should be 0");
        return;
    }

    if (prog->strata[0].rule_count != 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 2 rule names, got %u",
            prog->strata[0].rule_count);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_program_free_null(void)
{
    TEST("Program free handles NULL safely");

    wl_ir_program_free(NULL);

    PASS();
}

/* Helper: parse string, build program, and convert rules */
static struct wirelog_program *
make_program_with_rules(const char *source)
{
    struct wirelog_program *prog = make_program(source);
    if (!prog)
        return NULL;

    if (wl_ir_program_convert_rules(prog, prog->ast) != 0) {
        wl_ir_program_free(prog);
        return NULL;
    }

    return prog;
}

/* ======================================================================== */
/* Rule Conversion Tests                                                    */
/* ======================================================================== */

static void
test_simple_rule(void)
{
    TEST("Simple rule r(x) :- a(x). -> PROJECT over SCAN");

    struct wirelog_program *prog = make_program_with_rules(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }
    if (prog->rule_count != 1 || !prog->rules[0].ir_root) {
        wl_ir_program_free(prog);
        FAIL("should have 1 rule with IR");
        return;
    }

    wirelog_ir_node_t *root = prog->rules[0].ir_root;
    if (root->type != WIRELOG_IR_PROJECT) {
        wl_ir_program_free(prog);
        FAIL("root should be PROJECT");
        return;
    }

    if (root->child_count != 1 || root->children[0]->type != WIRELOG_IR_SCAN) {
        wl_ir_program_free(prog);
        FAIL("PROJECT child should be SCAN");
        return;
    }

    if (strcmp(root->children[0]->relation_name, "a") != 0) {
        wl_ir_program_free(prog);
        FAIL("SCAN relation should be a");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_two_body_join(void)
{
    TEST("Two-body join: PROJECT over JOIN(SCAN, SCAN)");

    struct wirelog_program *prog
        = make_program_with_rules(".decl Tc(x: int32, y: int32)\n"
            ".decl Arc(x: int32, y: int32)\n"
            "Tc(x, y) :- Tc(x, z), Arc(z, y).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wirelog_ir_node_t *root = prog->rules[0].ir_root;
    if (root->type != WIRELOG_IR_PROJECT) {
        wl_ir_program_free(prog);
        FAIL("root should be PROJECT");
        return;
    }

    wirelog_ir_node_t *join = root->children[0];
    if (join->type != WIRELOG_IR_JOIN) {
        wl_ir_program_free(prog);
        FAIL("child should be JOIN");
        return;
    }

    if (join->child_count != 2) {
        wl_ir_program_free(prog);
        FAIL("JOIN should have 2 children");
        return;
    }

    if (join->children[0]->type != WIRELOG_IR_SCAN
        || join->children[1]->type != WIRELOG_IR_SCAN) {
        wl_ir_program_free(prog);
        FAIL("JOIN children should be SCANs");
        return;
    }

    /* Verify join key is z */
    if (join->join_key_count != 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 1 join key, got %u",
            join->join_key_count);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    if (strcmp(join->join_left_keys[0], "z") != 0) {
        wl_ir_program_free(prog);
        FAIL("join key should be z");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_comparison_filter(void)
{
    TEST("Comparison: PROJECT over FILTER over JOIN");

    struct wirelog_program *prog = make_program_with_rules(
        ".decl edge(x: int32, y: int32)\n"
        ".decl sg(x: int32, y: int32)\n"
        "sg(x, y) :- edge(z, x), edge(z, y), x != y.\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wirelog_ir_node_t *root = prog->rules[0].ir_root;
    if (root->type != WIRELOG_IR_PROJECT) {
        wl_ir_program_free(prog);
        FAIL("root should be PROJECT");
        return;
    }

    wirelog_ir_node_t *filter = root->children[0];
    if (filter->type != WIRELOG_IR_FILTER) {
        wl_ir_program_free(prog);
        FAIL("child should be FILTER");
        return;
    }

    if (!filter->filter_expr || filter->filter_expr->type != WL_IR_EXPR_CMP) {
        wl_ir_program_free(prog);
        FAIL("filter should have CMP expression");
        return;
    }

    if (filter->filter_expr->cmp_op != WIRELOG_CMP_NEQ) {
        wl_ir_program_free(prog);
        FAIL("cmp_op should be NEQ");
        return;
    }

    wirelog_ir_node_t *join = filter->children[0];
    if (join->type != WIRELOG_IR_JOIN) {
        wl_ir_program_free(prog);
        FAIL("filter child should be JOIN");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_negation_antijoin(void)
{
    TEST("Negation: PROJECT over ANTIJOIN(SCAN, SCAN)");

    struct wirelog_program *prog
        = make_program_with_rules(".decl node(x: int32)\n"
            ".decl edge(x: int32, y: int32)\n"
            ".decl isolated(x: int32)\n"
            "isolated(x) :- node(x), !edge(x, _).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wirelog_ir_node_t *root = prog->rules[0].ir_root;
    if (root->type != WIRELOG_IR_PROJECT) {
        wl_ir_program_free(prog);
        FAIL("root should be PROJECT");
        return;
    }

    wirelog_ir_node_t *antijoin = root->children[0];
    if (antijoin->type != WIRELOG_IR_ANTIJOIN) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected ANTIJOIN, got type %d",
            antijoin->type);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    if (antijoin->child_count != 2) {
        wl_ir_program_free(prog);
        FAIL("ANTIJOIN should have 2 children");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_aggregation_simple(void)
{
    TEST("Aggregation: AGGREGATE over SCAN");

    struct wirelog_program *prog
        = make_program_with_rules(".decl sssp2(x: int32, d: int32)\n"
            ".decl sssp(x: int32, d: int32)\n"
            "sssp(x, min(d)) :- sssp2(x, d).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wirelog_ir_node_t *root = prog->rules[0].ir_root;
    if (root->type != WIRELOG_IR_AGGREGATE) {
        wl_ir_program_free(prog);
        FAIL("root should be AGGREGATE");
        return;
    }

    if (root->agg_fn != WIRELOG_AGG_MIN) {
        wl_ir_program_free(prog);
        FAIL("agg_fn should be MIN");
        return;
    }

    if (root->child_count != 1 || root->children[0]->type != WIRELOG_IR_SCAN) {
        wl_ir_program_free(prog);
        FAIL("AGGREGATE child should be SCAN");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_aggregation_with_join(void)
{
    TEST("Aggregation+join: AGGREGATE over JOIN");

    struct wirelog_program *prog = make_program_with_rules(
        ".decl sssp2(x: int32, d: int32)\n"
        ".decl arc(x: int32, y: int32, d: int32)\n"
        "sssp2(y, min(d1 + d2)) :- sssp2(x, d1), arc(x, y, d2).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wirelog_ir_node_t *root = prog->rules[0].ir_root;
    if (root->type != WIRELOG_IR_AGGREGATE) {
        wl_ir_program_free(prog);
        FAIL("root should be AGGREGATE");
        return;
    }

    if (!root->agg_expr) {
        wl_ir_program_free(prog);
        FAIL("agg_expr should not be NULL");
        return;
    }

    /* agg_expr should be an arithmetic expression (d1 + d2) */
    if (root->agg_expr->type != WL_IR_EXPR_ARITH) {
        wl_ir_program_free(prog);
        FAIL("agg_expr should be ARITH");
        return;
    }

    wirelog_ir_node_t *join = root->children[0];
    if (join->type != WIRELOG_IR_JOIN) {
        wl_ir_program_free(prog);
        FAIL("AGGREGATE child should be JOIN");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_aggregation_constant(void)
{
    TEST("Aggregation with constant: min(0)");

    struct wirelog_program *prog
        = make_program_with_rules(".decl id(x: int32)\n"
            ".decl r(x: int32, d: int32)\n"
            "r(x, min(0)) :- id(x).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wirelog_ir_node_t *root = prog->rules[0].ir_root;
    if (root->type != WIRELOG_IR_AGGREGATE) {
        wl_ir_program_free(prog);
        FAIL("root should be AGGREGATE");
        return;
    }

    if (!root->agg_expr || root->agg_expr->type != WL_IR_EXPR_CONST_INT) {
        wl_ir_program_free(prog);
        FAIL("agg_expr should be CONST_INT");
        return;
    }

    if (root->agg_expr->int_value != 0) {
        wl_ir_program_free(prog);
        FAIL("agg_expr value should be 0");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_three_body_join(void)
{
    TEST("Three-body join: left-deep JOIN tree");

    struct wirelog_program *prog
        = make_program_with_rules(".decl a(x: int32, y: int32)\n"
            ".decl b(y: int32, z: int32)\n"
            ".decl c(z: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x, y), b(y, z), c(z).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wirelog_ir_node_t *root = prog->rules[0].ir_root;
    if (root->type != WIRELOG_IR_PROJECT) {
        wl_ir_program_free(prog);
        FAIL("root should be PROJECT");
        return;
    }

    /* Should be: PROJECT -> JOIN(JOIN(SCAN_a, SCAN_b), SCAN_c) */
    wirelog_ir_node_t *outer_join = root->children[0];
    if (outer_join->type != WIRELOG_IR_JOIN) {
        wl_ir_program_free(prog);
        FAIL("child should be JOIN");
        return;
    }

    if (outer_join->child_count != 2) {
        wl_ir_program_free(prog);
        FAIL("outer JOIN should have 2 children");
        return;
    }

    wirelog_ir_node_t *inner_join = outer_join->children[0];
    if (inner_join->type != WIRELOG_IR_JOIN) {
        wl_ir_program_free(prog);
        FAIL("left child should be inner JOIN");
        return;
    }

    if (outer_join->children[1]->type != WIRELOG_IR_SCAN) {
        wl_ir_program_free(prog);
        FAIL("right child should be SCAN");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_duplicate_variable(void)
{
    TEST("Duplicate variable a(x,x) -> FILTER(col0=col1)");

    struct wirelog_program *prog
        = make_program_with_rules(".decl a(x: int32, y: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x, x).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wirelog_ir_node_t *root = prog->rules[0].ir_root;
    if (root->type != WIRELOG_IR_PROJECT) {
        wl_ir_program_free(prog);
        FAIL("root should be PROJECT");
        return;
    }

    /* Should be PROJECT -> FILTER -> SCAN */
    wirelog_ir_node_t *filter = root->children[0];
    if (filter->type != WIRELOG_IR_FILTER) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected FILTER, got type %d",
            filter->type);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    if (!filter->filter_expr || filter->filter_expr->type != WL_IR_EXPR_CMP) {
        wl_ir_program_free(prog);
        FAIL("filter should have CMP expr (col0 = col1)");
        return;
    }

    if (filter->filter_expr->cmp_op != WIRELOG_CMP_EQ) {
        wl_ir_program_free(prog);
        FAIL("cmp_op should be EQ");
        return;
    }

    if (filter->children[0]->type != WIRELOG_IR_SCAN) {
        wl_ir_program_free(prog);
        FAIL("FILTER child should be SCAN");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_constant_in_atom(void)
{
    TEST("Constant in atom a(x, 42) -> FILTER(col1=42)");

    struct wirelog_program *prog
        = make_program_with_rules(".decl a(x: int32, y: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x, 42).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wirelog_ir_node_t *root = prog->rules[0].ir_root;
    if (root->type != WIRELOG_IR_PROJECT) {
        wl_ir_program_free(prog);
        FAIL("root should be PROJECT");
        return;
    }

    wirelog_ir_node_t *filter = root->children[0];
    if (filter->type != WIRELOG_IR_FILTER) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected FILTER, got type %d",
            filter->type);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    /* The filter expression should compare a var with the constant 42 */
    if (!filter->filter_expr || filter->filter_expr->type != WL_IR_EXPR_CMP) {
        wl_ir_program_free(prog);
        FAIL("filter should have CMP expr");
        return;
    }

    /* One child should be CONST_INT with value 42 */
    wl_ir_expr_t *expr = filter->filter_expr;
    bool found_42 = false;
    for (uint32_t i = 0; i < expr->child_count; i++) {
        if (expr->children[i]->type == WL_IR_EXPR_CONST_INT
            && expr->children[i]->int_value == 42) {
            found_42 = true;
        }
    }

    if (!found_42) {
        wl_ir_program_free(prog);
        FAIL("filter should reference constant 42");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_wildcard_not_join_key(void)
{
    TEST("Wildcard _ excluded from join keys");

    struct wirelog_program *prog
        = make_program_with_rules(".decl a(x: int32, y: int32)\n"
            ".decl b(z: int32, w: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x, _), b(_, x).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wirelog_ir_node_t *root = prog->rules[0].ir_root;
    /* Should be PROJECT -> JOIN with key x only */
    wirelog_ir_node_t *join_node = root->children[0];

    /* May have filters wrapping, find the JOIN */
    while (join_node && join_node->type != WIRELOG_IR_JOIN
        && join_node->child_count > 0) {
        join_node = join_node->children[0];
    }

    if (!join_node || join_node->type != WIRELOG_IR_JOIN) {
        wl_ir_program_free(prog);
        FAIL("should have a JOIN node");
        return;
    }

    /* Join key should be x, not _ */
    if (join_node->join_key_count != 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 1 join key, got %u",
            join_node->join_key_count);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    if (strcmp(join_node->join_left_keys[0], "x") != 0) {
        char buf[100];
        snprintf(buf, sizeof(buf), "join key should be x, got %s",
            join_node->join_left_keys[0]);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_wildcard_in_scan(void)
{
    TEST("Wildcard in SCAN stored as NULL column");

    struct wirelog_program *prog
        = make_program_with_rules(".decl a(x: int32, y: int32, z: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x, _, _).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    /* Find the SCAN node */
    wirelog_ir_node_t *root = prog->rules[0].ir_root;
    wirelog_ir_node_t *scan = root;
    while (scan && scan->type != WIRELOG_IR_SCAN && scan->child_count > 0) {
        scan = scan->children[0];
    }

    if (!scan || scan->type != WIRELOG_IR_SCAN) {
        wl_ir_program_free(prog);
        FAIL("should have SCAN node");
        return;
    }

    if (scan->column_count != 3) {
        wl_ir_program_free(prog);
        FAIL("SCAN should have 3 columns");
        return;
    }

    /* column 0 should be "x", columns 1 and 2 should be NULL (wildcard) */
    if (!scan->column_names[0] || strcmp(scan->column_names[0], "x") != 0) {
        wl_ir_program_free(prog);
        FAIL("column 0 should be x");
        return;
    }

    if (scan->column_names[1] != NULL) {
        wl_ir_program_free(prog);
        FAIL("column 1 should be NULL (wildcard)");
        return;
    }

    if (scan->column_names[2] != NULL) {
        wl_ir_program_free(prog);
        FAIL("column 2 should be NULL (wildcard)");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_boolean_true_noop(void)
{
    TEST("Boolean True in body -> no FILTER added");

    struct wirelog_program *prog
        = make_program_with_rules(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x), True.\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wirelog_ir_node_t *root = prog->rules[0].ir_root;
    /* Should be PROJECT -> SCAN (no FILTER for True) */
    if (root->type != WIRELOG_IR_PROJECT) {
        wl_ir_program_free(prog);
        FAIL("root should be PROJECT");
        return;
    }

    if (root->children[0]->type != WIRELOG_IR_SCAN) {
        char buf[100];
        snprintf(buf, sizeof(buf),
            "expected SCAN, got type %d (True should be no-op)",
            root->children[0]->type);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_boolean_false_filter(void)
{
    TEST("Boolean False in body -> FILTER(false) added");

    struct wirelog_program *prog
        = make_program_with_rules(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x), False.\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wirelog_ir_node_t *root = prog->rules[0].ir_root;
    if (root->type != WIRELOG_IR_PROJECT) {
        wl_ir_program_free(prog);
        FAIL("root should be PROJECT");
        return;
    }

    wirelog_ir_node_t *filter = root->children[0];
    if (filter->type != WIRELOG_IR_FILTER) {
        wl_ir_program_free(prog);
        FAIL("child should be FILTER");
        return;
    }

    if (!filter->filter_expr || filter->filter_expr->type != WL_IR_EXPR_BOOL) {
        wl_ir_program_free(prog);
        FAIL("filter should have BOOL expr");
        return;
    }

    if (filter->filter_expr->bool_value != false) {
        wl_ir_program_free(prog);
        FAIL("bool_value should be false");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

/* Helper: full pipeline (parse + metadata + convert + merge + schemas + strata) */
static struct wirelog_program *
make_full_program(const char *source)
{
    struct wirelog_program *prog = make_program_with_rules(source);
    if (!prog)
        return NULL;

    if (wl_ir_program_merge_unions(prog) != 0) {
        wl_ir_program_free(prog);
        return NULL;
    }

    wl_ir_program_build_schemas(prog);
    wl_ir_program_build_default_stratum(prog);

    return prog;
}

/* ======================================================================== */
/* Phase 2B: Compound Column IR Lowering Tests (Issue #531/#539)            */
/*                                                                          */
/* The parser does not yet accept "f/N inline" / "f/N side" syntax in       */
/* .decl bodies, so these tests build a program with regular column types   */
/* and then patch the relation's column metadata before convert_rules() to  */
/* simulate compound declarations. This isolates the IR lowering path that  */
/* runs inside build_atom_scan().                                           */
/* ======================================================================== */

static void
patch_compound_column(struct wirelog_program *prog, const char *relation,
    uint32_t col_idx, wirelog_compound_kind_t kind, const char *functor,
    uint32_t arity, uint32_t inline_offset)
{
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (prog->relations[i].name
            && strcmp(prog->relations[i].name, relation) == 0
            && col_idx < prog->relations[i].column_count) {
            wirelog_column_t *col = &prog->relations[i].columns[col_idx];
            col->compound_kind = kind;
            col->compound_arity = arity;
            col->compound_inline_col_offset = inline_offset;
            int64_t fid = wl_intern_put(prog->intern, functor);
            col->compound_functor_id = (fid >= 0) ? (uint32_t)fid : 0;
            return;
        }
    }
}

/*
 * Walk an IR subtree and count nodes whose type matches `target`.
 * Used to verify Phase 2B annotates the leaf SCAN in place rather than
 * wrapping it in an additional COMPOUND_INLINE/SIDE parent.
 */
static uint32_t
count_ir_nodes_of_type(const wirelog_ir_node_t *node,
    wirelog_ir_node_type_t target)
{
    if (!node)
        return 0;
    uint32_t total = (node->type == target) ? 1 : 0;
    for (uint32_t i = 0; i < node->child_count; i++) {
        total += count_ir_nodes_of_type(node->children[i], target);
    }
    return total;
}

/* Descend through wrappers (PROJECT, FILTER, etc.) to the relation leaf. */
static const wirelog_ir_node_t *
find_relation_leaf(const wirelog_ir_node_t *node, const char *relation)
{
    if (!node)
        return NULL;
    if (node->relation_name && strcmp(node->relation_name, relation) == 0
        && node->child_count == 0) {
        return node;
    }
    for (uint32_t i = 0; i < node->child_count; i++) {
        const wirelog_ir_node_t *r
            = find_relation_leaf(node->children[i], relation);
        if (r)
            return r;
    }
    return NULL;
}

static void
test_compound_inline_annotation(void)
{
    TEST("INLINE compound: scan->type becomes COMPOUND_INLINE with metadata");

    struct wirelog_program *prog
        = make_program(".decl pred(a: int32, b: int32)\n"
            ".decl r(b: int32)\n"
            "r(b) :- pred(f(x), b).\n");
    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    patch_compound_column(prog, "pred", 0, WIRELOG_COMPOUND_KIND_INLINE, "f", 1,
        7);
    if (wl_ir_program_convert_rules(prog, prog->ast) != 0) {
        wl_ir_program_free(prog);
        FAIL("convert_rules failed");
        return;
    }

    const wirelog_ir_node_t *leaf
        = find_relation_leaf(prog->rules[0].ir_root, "pred");
    if (!leaf) {
        wl_ir_program_free(prog);
        FAIL("could not locate pred leaf in IR");
        return;
    }

    if (leaf->type != WIRELOG_IR_COMPOUND_INLINE) {
        wl_ir_program_free(prog);
        FAIL("leaf type should be COMPOUND_INLINE");
        return;
    }

    int64_t expected_fid = wl_intern_get(prog->intern, "f");
    if (leaf->compound_inline.functor_id != (uint32_t)expected_fid
        || leaf->compound_inline.arity != 1
        || leaf->compound_inline.inline_col_offset != 7) {
        wl_ir_program_free(prog);
        FAIL("compound metadata incorrect on annotated leaf");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_compound_side_annotation(void)
{
    TEST("SIDE compound: scan->type becomes COMPOUND_SIDE with metadata");

    struct wirelog_program *prog
        = make_program(".decl pred(a: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- pred(g(x)).\n");
    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    patch_compound_column(prog, "pred", 0, WIRELOG_COMPOUND_KIND_SIDE, "g", 1,
        0);
    if (wl_ir_program_convert_rules(prog, prog->ast) != 0) {
        wl_ir_program_free(prog);
        FAIL("convert_rules failed");
        return;
    }

    const wirelog_ir_node_t *leaf
        = find_relation_leaf(prog->rules[0].ir_root, "pred");
    if (!leaf || leaf->type != WIRELOG_IR_COMPOUND_SIDE) {
        wl_ir_program_free(prog);
        FAIL("leaf type should be COMPOUND_SIDE");
        return;
    }

    int64_t expected_fid = wl_intern_get(prog->intern, "g");
    if (leaf->compound_inline.functor_id != (uint32_t)expected_fid
        || leaf->compound_inline.arity != 1) {
        wl_ir_program_free(prog);
        FAIL("compound metadata incorrect on annotated leaf");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_compound_annotation_not_wrapped(void)
{
    TEST("Compound annotation: no extra COMPOUND wrapper above the SCAN");

    struct wirelog_program *prog
        = make_program(".decl pred(a: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- pred(f(x)).\n");
    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    patch_compound_column(prog, "pred", 0, WIRELOG_COMPOUND_KIND_INLINE, "f", 1,
        0);
    if (wl_ir_program_convert_rules(prog, prog->ast) != 0) {
        wl_ir_program_free(prog);
        FAIL("convert_rules failed");
        return;
    }

    const wirelog_ir_node_t *root = prog->rules[0].ir_root;
    uint32_t inline_count
        = count_ir_nodes_of_type(root, WIRELOG_IR_COMPOUND_INLINE);
    uint32_t scan_count = count_ir_nodes_of_type(root, WIRELOG_IR_SCAN);

    /* Annotation must produce exactly one COMPOUND_INLINE leaf and zero
       residual SCAN nodes for the compound atom. */
    if (inline_count != 1 || scan_count != 0) {
        char buf[128];
        snprintf(buf, sizeof(buf),
            "expected 1 COMPOUND_INLINE + 0 SCAN, got %u + %u",
            inline_count, scan_count);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    const wirelog_ir_node_t *leaf = find_relation_leaf(root, "pred");
    if (!leaf || leaf->child_count != 0) {
        wl_ir_program_free(prog);
        FAIL("annotated leaf must have no children");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_compound_mixed_columns(void)
{
    TEST("Mixed columns: first compound column drives annotation");

    struct wirelog_program *prog
        = make_program(".decl pred(a: int32, b: int32, c: int32)\n"
            ".decl r(y: int32)\n"
            "r(y) :- pred(f(x), y, g(z)).\n");
    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    /* col 0: INLINE f/1, col 1: regular, col 2: SIDE g/1 */
    patch_compound_column(prog, "pred", 0, WIRELOG_COMPOUND_KIND_INLINE, "f", 1,
        3);
    patch_compound_column(prog, "pred", 2, WIRELOG_COMPOUND_KIND_SIDE, "g", 1,
        0);

    if (wl_ir_program_convert_rules(prog, prog->ast) != 0) {
        wl_ir_program_free(prog);
        FAIL("convert_rules failed");
        return;
    }

    const wirelog_ir_node_t *leaf
        = find_relation_leaf(prog->rules[0].ir_root, "pred");
    if (!leaf || leaf->type != WIRELOG_IR_COMPOUND_INLINE) {
        wl_ir_program_free(prog);
        FAIL("leaf should be annotated by FIRST compound column (INLINE)");
        return;
    }

    int64_t fid = wl_intern_get(prog->intern, "f");
    if (leaf->compound_inline.functor_id != (uint32_t)fid
        || leaf->compound_inline.arity != 1
        || leaf->compound_inline.inline_col_offset != 3) {
        wl_ir_program_free(prog);
        FAIL("metadata should reflect FIRST compound column (f/1, offset 3)");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_compound_regular_atom_unchanged(void)
{
    TEST("Regular atom (no compound column declared) stays as SCAN");

    struct wirelog_program *prog
        = make_program(".decl pred(a: int32, b: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- pred(x, y).\n");
    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (wl_ir_program_convert_rules(prog, prog->ast) != 0) {
        wl_ir_program_free(prog);
        FAIL("convert_rules failed");
        return;
    }

    const wirelog_ir_node_t *leaf
        = find_relation_leaf(prog->rules[0].ir_root, "pred");
    if (!leaf || leaf->type != WIRELOG_IR_SCAN) {
        wl_ir_program_free(prog);
        FAIL("leaf should remain SCAN when no compound metadata");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_compound_participates_in_join(void)
{
    TEST("Compound annotation participates in JOIN chain (well-formed IR)");

    struct wirelog_program *prog
        = make_program(".decl pred(a: int32, b: int32)\n"
            ".decl other(b: int32, c: int32)\n"
            ".decl r(x: int32, c: int32)\n"
            "r(x, c) :- pred(f(x), y), other(y, c).\n");
    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    patch_compound_column(prog, "pred", 0, WIRELOG_COMPOUND_KIND_INLINE, "f", 1,
        0);
    if (wl_ir_program_convert_rules(prog, prog->ast) != 0) {
        wl_ir_program_free(prog);
        FAIL("convert_rules failed");
        return;
    }

    const wirelog_ir_node_t *root = prog->rules[0].ir_root;
    if (!root || root->type != WIRELOG_IR_PROJECT) {
        wl_ir_program_free(prog);
        FAIL("root should be PROJECT");
        return;
    }
    const wirelog_ir_node_t *join = root->children[0];
    if (!join || join->type != WIRELOG_IR_JOIN || join->child_count != 2) {
        wl_ir_program_free(prog);
        FAIL("PROJECT child should be a 2-child JOIN");
        return;
    }
    if (join->children[0]->type != WIRELOG_IR_COMPOUND_INLINE
        || join->children[1]->type != WIRELOG_IR_SCAN) {
        wl_ir_program_free(prog);
        FAIL("JOIN children should be [COMPOUND_INLINE pred, SCAN other]");
        return;
    }
    if (join->join_key_count != 1) {
        wl_ir_program_free(prog);
        FAIL("expected join key on shared variable y");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* UNION Merge Tests                                                        */
/* ======================================================================== */

static void
test_union_merge_tc(void)
{
    TEST("TC 2 rules same head -> UNION with 2 children");

    struct wirelog_program *prog
        = make_full_program(".decl Arc(x: int32, y: int32)\n"
            ".decl Tc(x: int32, y: int32)\n"
            "Tc(x, y) :- Arc(x, y).\n"
            "Tc(x, y) :- Tc(x, z), Arc(z, y).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    /* Find Tc relation index */
    int tc_idx = -1;
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (strcmp(prog->relations[i].name, "Tc") == 0) {
            tc_idx = (int)i;
            break;
        }
    }

    if (tc_idx < 0 || !prog->relation_irs) {
        wl_ir_program_free(prog);
        FAIL("Tc relation or relation_irs not found");
        return;
    }

    wirelog_ir_node_t *tc_ir = prog->relation_irs[tc_idx];
    if (!tc_ir || tc_ir->type != WIRELOG_IR_UNION) {
        wl_ir_program_free(prog);
        FAIL("Tc should be UNION");
        return;
    }

    if (tc_ir->child_count != 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "UNION should have 2 children, got %u",
            tc_ir->child_count);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_union_single_rule(void)
{
    TEST("Single-rule relation -> no UNION wrapping");

    struct wirelog_program *prog = make_full_program(".decl a(x: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- a(x).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    /* Find r relation index */
    int r_idx = -1;
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (strcmp(prog->relations[i].name, "r") == 0) {
            r_idx = (int)i;
            break;
        }
    }

    if (r_idx < 0 || !prog->relation_irs) {
        wl_ir_program_free(prog);
        FAIL("r relation or relation_irs not found");
        return;
    }

    wirelog_ir_node_t *r_ir = prog->relation_irs[r_idx];
    if (!r_ir) {
        wl_ir_program_free(prog);
        FAIL("r IR should not be NULL");
        return;
    }

    /* Single rule: should NOT be UNION */
    if (r_ir->type == WIRELOG_IR_UNION) {
        wl_ir_program_free(prog);
        FAIL("single rule should NOT be wrapped in UNION");
        return;
    }

    if (r_ir->type != WIRELOG_IR_PROJECT) {
        wl_ir_program_free(prog);
        FAIL("single rule should be PROJECT");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Public API Tests                                                         */
/* ======================================================================== */

static void
test_api_parse_string(void)
{
    TEST("wirelog_parse_string returns valid program");

    wirelog_error_t err = WIRELOG_ERR_UNKNOWN;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl Arc(x: int32, y: int32)\n"
            ".decl Tc(x: int32, y: int32)\n"
            "Tc(x, y) :- Arc(x, y).\n"
            "Tc(x, y) :- Tc(x, z), Arc(z, y).\n",
            &err);

    if (!prog) {
        FAIL("program is NULL");
        return;
    }
    if (err != WIRELOG_OK) {
        wirelog_program_free(prog);
        FAIL("error should be OK");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_api_parse_string_error(void)
{
    TEST("wirelog_parse_string returns NULL for invalid input");

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog
        = wirelog_parse_string("this is not valid datalog {{{}}}", &err);

    if (prog != NULL) {
        wirelog_program_free(prog);
        FAIL("should return NULL for invalid input");
        return;
    }

    if (err != WIRELOG_ERR_PARSE) {
        FAIL("error should be WIRELOG_ERR_PARSE");
        return;
    }

    PASS();
}

static void
test_api_get_rule_count(void)
{
    TEST("wirelog_program_get_rule_count returns correct count");

    wirelog_program_t *prog
        = wirelog_parse_string(".decl Arc(x: int32, y: int32)\n"
            ".decl Tc(x: int32, y: int32)\n"
            "Tc(x, y) :- Arc(x, y).\n"
            "Tc(x, y) :- Tc(x, z), Arc(z, y).\n",
            NULL);

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    uint32_t count = wirelog_program_get_rule_count(prog);
    if (count != 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 2 rules, got %u", count);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_api_get_schema(void)
{
    TEST("wirelog_program_get_schema returns correct info");

    wirelog_program_t *prog
        = wirelog_parse_string(".decl Arc(x: int32, y: int32)\n", NULL);

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    const wirelog_schema_t *schema = wirelog_program_get_schema(prog, "Arc");
    if (!schema) {
        wirelog_program_free(prog);
        FAIL("schema for Arc should not be NULL");
        return;
    }

    if (strcmp(schema->relation_name, "Arc") != 0) {
        wirelog_program_free(prog);
        FAIL("relation_name should be Arc");
        return;
    }

    if (schema->column_count != 2) {
        wirelog_program_free(prog);
        FAIL("should have 2 columns");
        return;
    }

    if (strcmp(schema->columns[0].name, "x") != 0) {
        wirelog_program_free(prog);
        FAIL("column 0 name should be x");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_api_get_schema_null(void)
{
    TEST("wirelog_program_get_schema(nonexistent) returns NULL");

    wirelog_program_t *prog
        = wirelog_parse_string(".decl Arc(x: int32, y: int32)\n", NULL);

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    const wirelog_schema_t *schema
        = wirelog_program_get_schema(prog, "nonexistent");
    if (schema != NULL) {
        wirelog_program_free(prog);
        FAIL("should return NULL for nonexistent relation");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_api_get_stratum_count(void)
{
    TEST("wirelog_program_get_stratum_count returns 1");

    wirelog_program_t *prog
        = wirelog_parse_string(".decl Arc(x: int32, y: int32)\n"
            ".decl Tc(x: int32, y: int32)\n"
            "Tc(x, y) :- Arc(x, y).\n",
            NULL);

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    uint32_t count = wirelog_program_get_stratum_count(prog);
    if (count != 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 1, got %u", count);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_api_get_stratum(void)
{
    TEST("wirelog_program_get_stratum(0) returns valid stratum");

    wirelog_program_t *prog
        = wirelog_parse_string(".decl Arc(x: int32, y: int32)\n"
            ".decl Tc(x: int32, y: int32)\n"
            "Tc(x, y) :- Arc(x, y).\n"
            "Tc(x, y) :- Tc(x, z), Arc(z, y).\n",
            NULL);

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    const wirelog_stratum_t *stratum = wirelog_program_get_stratum(prog, 0);
    if (!stratum) {
        wirelog_program_free(prog);
        FAIL("stratum should not be NULL");
        return;
    }

    if (stratum->rule_count != 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 2 rules, got %u",
            stratum->rule_count);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_api_get_stratum_oob(void)
{
    TEST("wirelog_program_get_stratum(99) returns NULL");

    wirelog_program_t *prog
        = wirelog_parse_string(".decl Arc(x: int32, y: int32)\n", NULL);

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    const wirelog_stratum_t *stratum = wirelog_program_get_stratum(prog, 99);
    if (stratum != NULL) {
        wirelog_program_free(prog);
        FAIL("should return NULL for out-of-bounds");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_api_is_stratified(void)
{
    TEST("wirelog_program_is_stratified returns true (stub)");

    wirelog_program_t *prog
        = wirelog_parse_string(".decl Arc(x: int32, y: int32)\n", NULL);

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (!wirelog_program_is_stratified(prog)) {
        wirelog_program_free(prog);
        FAIL("should return true");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_api_parse_file_stub(void)
{
    TEST("wirelog_parse returns NULL with WIRELOG_ERR_IO");

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse("nonexistent.dl", &err);

    if (prog != NULL) {
        wirelog_program_free(prog);
        FAIL("should return NULL (stub)");
        return;
    }

    if (err != WIRELOG_ERR_IO) {
        FAIL("error should be WIRELOG_ERR_IO");
        return;
    }

    PASS();
}

static void
test_api_parse_with_error_stub(void)
{
    TEST("wirelog_parse_with_error_info returns NULL + error_code");

    wirelog_parse_error_t info;
    memset(&info, 0, sizeof(info));

    wirelog_program_t *prog
        = wirelog_parse_with_error_info("nonexistent.dl", &info);

    if (prog != NULL) {
        wirelog_program_free(prog);
        FAIL("should return NULL (stub)");
        return;
    }

    if (info.error_code != WIRELOG_ERR_IO) {
        FAIL("error_code should be WIRELOG_ERR_IO");
        return;
    }

    PASS();
}

static void
test_api_end_to_end(void)
{
    TEST("End-to-end: parse -> inspect IR -> schema -> free");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl Arc(x: int32, y: int32)\n"
            ".decl Tc(x: int32, y: int32)\n"
            ".input Arc(filename=\"arc.csv\")\n"
            ".output Tc\n"
            "Tc(x, y) :- Arc(x, y).\n"
            "Tc(x, y) :- Tc(x, z), Arc(z, y).\n",
            &err);

    if (!prog || err != WIRELOG_OK) {
        FAIL("parse failed");
        return;
    }

    /* Check rule count */
    if (wirelog_program_get_rule_count(prog) != 2) {
        wirelog_program_free(prog);
        FAIL("rule count should be 2");
        return;
    }

    /* Check schema */
    const wirelog_schema_t *arc_schema
        = wirelog_program_get_schema(prog, "Arc");
    if (!arc_schema || arc_schema->column_count != 2) {
        wirelog_program_free(prog);
        FAIL("Arc schema incorrect");
        return;
    }

    /* Check stratum */
    if (wirelog_program_get_stratum_count(prog) != 1) {
        wirelog_program_free(prog);
        FAIL("stratum count should be 1");
        return;
    }

    const wirelog_stratum_t *s = wirelog_program_get_stratum(prog, 0);
    if (!s || s->rule_count != 2) {
        wirelog_program_free(prog);
        FAIL("stratum should have 2 rules");
        return;
    }

    /* Check stratification */
    if (!wirelog_program_is_stratified(prog)) {
        wirelog_program_free(prog);
        FAIL("should be stratified");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Fact Collection Tests                                                    */
/* ======================================================================== */

static void
test_fact_collection_single_relation(void)
{
    TEST("Fact collection: edge(1,2). edge(2,3). stored in relation");

    struct wirelog_program *prog
        = make_program(".decl edge(src: int32, dst: int32)\n"
            "edge(1, 2).\n"
            "edge(2, 3).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (prog->relation_count != 1) {
        wl_ir_program_free(prog);
        FAIL("expected 1 relation");
        return;
    }

    wl_ir_relation_info_t *rel = &prog->relations[0];
    if (rel->fact_count != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 2 facts, got %u", rel->fact_count);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    /* fact_data layout: [1, 2, 2, 3] (row-major, 2 cols per row) */
    if (!rel->fact_data) {
        wl_ir_program_free(prog);
        FAIL("fact_data should not be NULL");
        return;
    }

    if (rel->fact_data[0] != 1 || rel->fact_data[1] != 2) {
        wl_ir_program_free(prog);
        FAIL("fact 0 should be (1, 2)");
        return;
    }

    if (rel->fact_data[2] != 2 || rel->fact_data[3] != 3) {
        wl_ir_program_free(prog);
        FAIL("fact 1 should be (2, 3)");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Fact Extraction API Tests                                                */
/* ======================================================================== */

static void
test_api_get_facts(void)
{
    TEST("wirelog_program_get_facts returns correct data");

    wirelog_program_t *prog
        = wirelog_parse_string(".decl edge(src: int32, dst: int32)\n"
            "edge(1, 2).\n"
            "edge(2, 3).\n"
            "edge(3, 4).\n",
            NULL);
    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    int64_t *data = NULL;
    uint32_t num_rows = 0, num_cols = 0;
    int rc
        = wirelog_program_get_facts(prog, "edge", &data, &num_rows, &num_cols);
    if (rc != 0) {
        wirelog_program_free(prog);
        FAIL("get_facts should return 0");
        return;
    }

    if (num_rows != 3 || num_cols != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3x2, got %ux%u", num_rows,
            num_cols);
        free(data);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    /* Verify data: {1,2, 2,3, 3,4} */
    if (data[0] != 1 || data[1] != 2 || data[2] != 2 || data[3] != 3
        || data[4] != 3 || data[5] != 4) {
        free(data);
        wirelog_program_free(prog);
        FAIL("fact data mismatch");
        return;
    }

    free(data);
    wirelog_program_free(prog);
    PASS();
}

static void
test_api_get_facts_no_facts(void)
{
    TEST("wirelog_program_get_facts returns 1 for no-facts relation");

    wirelog_program_t *prog
        = wirelog_parse_string(".decl edge(src: int32, dst: int32)\n", NULL);
    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    int64_t *data = NULL;
    uint32_t num_rows = 0, num_cols = 0;
    int rc
        = wirelog_program_get_facts(prog, "edge", &data, &num_rows, &num_cols);
    if (rc != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected rc=1, got %d", rc);
        free(data);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_api_get_facts_unknown_relation(void)
{
    TEST("wirelog_program_get_facts returns -1 for unknown relation");

    wirelog_program_t *prog
        = wirelog_parse_string(".decl edge(src: int32, dst: int32)\n", NULL);
    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    int64_t *data = NULL;
    uint32_t num_rows = 0, num_cols = 0;
    int rc = wirelog_program_get_facts(prog, "nonexistent", &data, &num_rows,
            &num_cols);
    if (rc != -1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected rc=-1, got %d", rc);
        free(data);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

static void
test_string_fact_interning(void)
{
    TEST("string fact interning via parser");

    const char *source = ".decl name(x: string)\n"
        "name(\"Alice\").\n"
        "name(\"Bob\").\n"
        "name(\"Alice\").\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(source, &err);
    if (!prog) {
        FAIL("parse failed");
        return;
    }

    /* Check fact count */
    int64_t *data = NULL;
    uint32_t nrows = 0, ncols = 0;
    int rc = wirelog_program_get_facts(prog, "name", &data, &nrows, &ncols);
    if (rc != 0) {
        FAIL("get_facts failed");
        wirelog_program_free(prog);
        return;
    }

    if (nrows != 3) {
        FAIL("expected 3 facts");
        free(data);
        wirelog_program_free(prog);
        return;
    }

    if (ncols != 1) {
        FAIL("expected 1 column");
        free(data);
        wirelog_program_free(prog);
        return;
    }

    /* "Alice" and "Bob" should get different IDs */
    if (data[0] == data[1]) {
        FAIL("Alice and Bob should have different IDs");
        free(data);
        wirelog_program_free(prog);
        return;
    }

    /* Both "Alice" facts should have the same ID */
    if (data[0] != data[2]) {
        FAIL("duplicate Alice should have same ID");
        free(data);
        wirelog_program_free(prog);
        return;
    }

    /* Reverse lookup via intern table */
    const wl_intern_t *intern = wirelog_program_get_intern(prog);
    if (!intern) {
        FAIL("intern table is NULL");
        free(data);
        wirelog_program_free(prog);
        return;
    }

    const char *s0 = wl_intern_reverse(intern, data[0]);
    const char *s1 = wl_intern_reverse(intern, data[1]);
    if (!s0 || strcmp(s0, "Alice") != 0) {
        FAIL("reverse of first ID should be 'Alice'");
        free(data);
        wirelog_program_free(prog);
        return;
    }
    if (!s1 || strcmp(s1, "Bob") != 0) {
        FAIL("reverse of second ID should be 'Bob'");
        free(data);
        wirelog_program_free(prog);
        return;
    }

    free(data);
    wirelog_program_free(prog);
    PASS();
}

int
main(void)
{
    printf("\n=== wirelog Program Tests ===\n\n");

    /* Metadata collection */
    test_decl_single_relation();
    test_input_directive();
    test_input_directive_param_names_values();
    test_input_io_scheme_present();
    test_input_io_scheme_absent();
    test_output_directive();
    test_output_directive_with_filename();
    test_printsize_directive();
    test_full_tc_metadata();
    test_no_rules_program();

    /* Schema and stratum synthesis */
    test_schema_synthesis();
    test_default_stratum();

    /* Safety */
    test_program_free_null();

    /* Rule conversion */
    test_simple_rule();
    test_two_body_join();
    test_comparison_filter();
    test_negation_antijoin();
    test_aggregation_simple();
    test_aggregation_with_join();
    test_aggregation_constant();
    test_three_body_join();
    test_duplicate_variable();
    test_constant_in_atom();
    test_wildcard_not_join_key();
    test_wildcard_in_scan();
    test_boolean_true_noop();
    test_boolean_false_filter();

    /* Phase 2B: compound column IR lowering (Issue #531/#539) */
    test_compound_inline_annotation();
    test_compound_side_annotation();
    test_compound_annotation_not_wrapped();
    test_compound_mixed_columns();
    test_compound_regular_atom_unchanged();
    test_compound_participates_in_join();

    /* UNION merge */
    test_union_merge_tc();
    test_union_single_rule();

    /* Fact collection */
    test_fact_collection_single_relation();

    /* Fact extraction API */
    test_api_get_facts();
    test_api_get_facts_no_facts();
    test_api_get_facts_unknown_relation();

    /* String interning */
    test_string_fact_interning();

    /* Public API */
    test_api_parse_string();
    test_api_parse_string_error();
    test_api_get_rule_count();
    test_api_get_schema();
    test_api_get_schema_null();
    test_api_get_stratum_count();
    test_api_get_stratum();
    test_api_get_stratum_oob();
    test_api_is_stratified();
    test_api_parse_file_stub();
    test_api_parse_with_error_stub();
    test_api_end_to_end();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
        tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
