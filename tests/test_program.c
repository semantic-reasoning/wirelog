/*
 * test_program.c - wirelog Program + IR Conversion Test Suite
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests written first (TDD) before program implementation.
 */

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/parser/parser.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/intern.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/util/log.h"

#if defined(_MSC_VER) && !defined(__clang__)
#  include <process.h>
static int
wl_test_setenv_(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, (value && *value) ? value : "1");
}
static int
wl_test_unsetenv_(const char *name)
{
    return _putenv_s(name, "");
}
#  define setenv   wl_test_setenv_
#  define unsetenv wl_test_unsetenv_
#  define getpid   _getpid
#else
#  include <unistd.h>
#endif

#include <time.h>

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
test_decl_compound_column_metadata(void)
{
    TEST("Compound declarations populate relation metadata");

    struct wirelog_program *prog
        = make_program(".decl Event(id: int64, payload: metadata/4, "
            "inline_payload: metadata/4 inline, "
            "side_payload: metadata/2 side)\n");
    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    const wl_ir_relation_info_t *rel = &prog->relations[0];
    if (rel->column_count != 4) {
        wl_ir_program_free(prog);
        FAIL("expected 4 columns");
        return;
    }

    int64_t metadata_id = wl_intern_get(prog->intern, "metadata");
    if (metadata_id < 0) {
        wl_ir_program_free(prog);
        FAIL("metadata functor was not interned");
        return;
    }

    if (rel->columns[1].type != WIRELOG_TYPE_INT64
        || rel->columns[1].compound_kind != WIRELOG_COMPOUND_KIND_SIDE
        || rel->columns[1].compound_functor_id != (uint32_t)metadata_id
        || rel->columns[1].compound_arity != 4) {
        wl_ir_program_free(prog);
        FAIL("default side compound metadata is incorrect");
        return;
    }

    if (rel->columns[2].type != WIRELOG_TYPE_INT64
        || rel->columns[2].compound_kind != WIRELOG_COMPOUND_KIND_INLINE
        || rel->columns[2].compound_functor_id != (uint32_t)metadata_id
        || rel->columns[2].compound_arity != 4
        || rel->columns[2].compound_inline_col_offset != 2) {
        wl_ir_program_free(prog);
        FAIL("inline compound metadata is incorrect");
        return;
    }

    if (rel->columns[3].compound_kind != WIRELOG_COMPOUND_KIND_SIDE
        || rel->columns[3].compound_arity != 2
        || rel->columns[3].compound_inline_col_offset != 0) {
        wl_ir_program_free(prog);
        FAIL("explicit side compound metadata is incorrect");
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

/* Issue #973: a rule head may carry at most one aggregate.  The AGGREGATE IR
 * node has one agg_fn / one agg_expr, so lowering used to keep only the last
 * aggregate and emit a tuple of the wrong arity with no diagnostic.
 *
 * make_program_with_rules() calls wl_ir_program_convert_rules() directly, so a
 * NULL return here is exactly the lowering rejection and not some later stage.
 * Each test pairs its negatives with a positive control so that a blanket
 * parser breakage cannot make the negatives pass for the wrong reason. */
static void
test_aggregation_multi_head_rejected(void)
{
    TEST("Aggregation: multiple aggregates in one head are rejected");

    /* Positive control: a single aggregate must still lower correctly. */
    struct wirelog_program *ok
        = make_program_with_rules(".decl val(g: int32, v: int32)\n"
            ".decl t(g: int32, a: int32)\n"
            "t(g, min(v)) :- val(g, v).\n");
    if (!ok) {
        FAIL("control: single-aggregate head should still lower");
        return;
    }
    if (ok->rules[0].ir_root->type != WIRELOG_IR_AGGREGATE
        || ok->rules[0].ir_root->agg_fn != WIRELOG_AGG_MIN
        || ok->rules[0].ir_root->group_by_count != 1) {
        wl_ir_program_free(ok);
        FAIL("control: expected AGGREGATE/MIN/group_by_count==1");
        return;
    }
    wl_ir_program_free(ok);

    /* Negative: two different aggregates, with a group-by column. */
    struct wirelog_program *bad
        = make_program_with_rules(".decl val(g: int32, v: int32)\n"
            ".decl t(g: int32, a: int32, b: int32)\n"
            "t(g, min(v), max(v)) :- val(g, v).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("t(g, min(v), max(v)) should be rejected");
        return;
    }

    /* Negative: two aggregates and no group-by column at all. */
    bad = make_program_with_rules(".decl val(g: int32, v: int32)\n"
            ".decl t(a: int32, b: int32)\n"
            "t(min(v), max(v)) :- val(g, v).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("t(min(v), max(v)) should be rejected");
        return;
    }

    /* Negative: three aggregates. */
    bad = make_program_with_rules(".decl val(g: int32, v: int32)\n"
            ".decl t(g: int32, a: int32, b: int32, c: int32)\n"
            "t(g, min(v), max(v), sum(v)) :- val(g, v).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("three aggregates should be rejected");
        return;
    }

    /* Negative: same agg_fn twice.  This one passes the downstream
     * mixed-agg_fn guard (#692 B5) -- since #975 that guard lives in
     * agg_spec_observe() (exec_plan_gen.c), which records the relation's
     * whole-relation reduction at lowering rather than rediscovering it by
     * scanning ops -- and would canonicalise only one column, so it must be
     * caught here. */
    bad = make_program_with_rules(".decl val(g: int32, v: int32)\n"
            ".decl t(g: int32, a: int32, b: int32)\n"
            "t(g, min(v), min(v)) :- val(g, v).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("t(g, min(v), min(v)) should be rejected");
        return;
    }

    /* Negative: the recursive shape.  This one is the memory-safety case --
     * before the fix it reached col_op_reduce()/col_rel_append_all() with an
     * emitted arity of 2 against a 3-arity .decl and segfaulted during
     * col_eval_stratum().  Rejecting at lowering keeps it from ever getting
     * that far.  (The single-aggregate variant
     * "cc(y, min(c)) :- cc(x, c, d), edge(x, y)." crashes the same way but
     * has nothing wrong with its aggregate count, so this check cannot see
     * it; it is rejected earlier by the #977 rule-head arity pass -- see
     * test_head_arity_recursive_aggregate_rejected().  This head has three
     * arguments against a 3-arity .decl, so that pass accepts it and it does
     * reach this check.) */
    bad = make_program_with_rules(".decl edge(x: int64, y: int64)\n"
            ".decl cc(n: int64, lo: int64, hi: int64)\n"
            "cc(y, min(c), max(c)) :- cc(x, c, d), edge(x, y).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("recursive multi-aggregate head should be rejected");
        return;
    }

    /* Control again: the documented workaround -- one aggregate per rule,
     * joined -- must still lower cleanly. */
    struct wirelog_program *workaround
        = make_program_with_rules(".decl val(g: int32, v: int32)\n"
            ".decl tmin(g: int32, a: int32)\n"
            ".decl tmax(g: int32, b: int32)\n"
            ".decl t(g: int32, a: int32, b: int32)\n"
            "tmin(g, min(v)) :- val(g, v).\n"
            "tmax(g, max(v)) :- val(g, v).\n"
            "t(g, a, b) :- tmin(g, a), tmax(g, b).\n");
    if (!workaround) {
        FAIL("control: documented per-rule workaround must lower");
        return;
    }
    if (workaround->rules[0].ir_root->type != WIRELOG_IR_AGGREGATE
        || workaround->rules[1].ir_root->type != WIRELOG_IR_AGGREGATE
        || workaround->rules[2].ir_root->type != WIRELOG_IR_PROJECT) {
        wl_ir_program_free(workaround);
        FAIL("control: workaround should lower to AGGREGATE/AGGREGATE/PROJECT");
        return;
    }
    wl_ir_program_free(workaround);

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
/* Compound Column IR Lowering Tests (Issue #531/#539)                      */
/*                                                                          */
/* These tests synthesize compound column metadata directly on              */
/* wirelog_column_t before convert_rules() in order to isolate the IR       */
/* lowering path that runs inside build_atom_scan(). The parser accepts     */
/* the "f/N inline" / "f/N side" declaration syntax (see                    */
/* test_parse_decl_compound_types in test_parser.c), but the synthetic      */
/* helper is retained because several cases below need either:              */
/*                                                                          */
/*   - non-default inline_col_offset values that the parser's natural       */
/*     prefix-sum (ir/program.c) does not produce for the test schema, or   */
/*   - parser-unreachable states such as compound_arity = 0 or an           */
/*     unknown compound_kind enum value, which exist solely to verify       */
/*     build_atom_scan()'s defense against corrupt IR metadata.             */
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
test_compound_inline_annotation_from_declaration(void)
{
    TEST("INLINE compound declaration annotates matching body pattern");

    struct wirelog_program *prog
        = make_program(".decl pred(id: int32, payload: f/1 inline)\n"
            ".decl r(x: int32)\n"
            "r(x) :- pred(id, f(x)).\n");
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
    if (!leaf || leaf->type != WIRELOG_IR_COMPOUND_INLINE) {
        wl_ir_program_free(prog);
        FAIL("leaf type should be COMPOUND_INLINE");
        return;
    }

    int64_t expected_fid = wl_intern_get(prog->intern, "f");
    if (leaf->compound_inline.functor_id != (uint32_t)expected_fid
        || leaf->compound_inline.arity != 1
        || leaf->compound_inline.inline_col_offset != 1) {
        wl_ir_program_free(prog);
        FAIL("declared compound metadata incorrect on annotated leaf");
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

/*
 * Issue #539 Phase 3: observability coverage. Drives compound IR lowering
 * with WL_LOG=COMPOUND:5 pointed at a tempfile and asserts that the
 * build_atom_scan entry, metadata, and SCAN-annotation lines are all
 * captured through the logger.
 */
static const char *
obs_tmpdir_(void)
{
    const char *d = getenv("TMPDIR");
    if (d && *d) return d;
#if defined(_WIN32)
    d = getenv("TEMP");
    if (d && *d) return d;
    d = getenv("TMP");
    if (d && *d) return d;
    return ".";
#else
    return "/tmp";
#endif
}

static void
test_compound_observability_logging(void)
{
    TEST("WL_LOG captures COMPOUND section during IR lowering");

    char tmp_path[256];
    const char *d = obs_tmpdir_();
#if defined(_WIN32)
    const char sep = '\\';
#else
    const char sep = '/';
#endif
    snprintf(tmp_path, sizeof(tmp_path),
        "%s%cwl_compound_obs_%ld_%ld.log",
        d, sep, (long)getpid(), (long)time(NULL));
    (void)remove(tmp_path);

    setenv("WL_LOG_FILE", tmp_path, 1);
    setenv("WL_LOG", "COMPOUND:5", 1);
    wl_log_init();

    struct wirelog_program *prog
        = make_program(".decl pred(a: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- pred(f(x)).\n");
    if (!prog) {
        wl_log_shutdown();
        unsetenv("WL_LOG");
        unsetenv("WL_LOG_FILE");
        FAIL("program is NULL");
        return;
    }

    patch_compound_column(prog, "pred", 0, WIRELOG_COMPOUND_KIND_INLINE, "f", 1,
        0);
    if (wl_ir_program_convert_rules(prog, prog->ast) != 0) {
        wl_ir_program_free(prog);
        wl_log_shutdown();
        unsetenv("WL_LOG");
        unsetenv("WL_LOG_FILE");
        FAIL("convert_rules failed");
        return;
    }

    wl_ir_program_free(prog);
    wl_log_shutdown();

    /* Read the log file and verify key markers are present. */
    char buf[4096] = { 0 };
    FILE *f = fopen(tmp_path, "r");
    if (!f) {
        unsetenv("WL_LOG");
        unsetenv("WL_LOG_FILE");
        FAIL("could not open log file");
        return;
    }
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    (void)remove(tmp_path);
    unsetenv("WL_LOG");
    unsetenv("WL_LOG_FILE");

    if (n == 0) {
        FAIL("log file is empty");
        return;
    }
    if (!strstr(buf, "[COMPOUND]")) {
        FAIL("log missing [COMPOUND] section tag");
        return;
    }
    if (!strstr(buf, "build_atom_scan: enter")) {
        FAIL("log missing build_atom_scan entry message");
        return;
    }
    if (!strstr(buf, "compound metadata:")) {
        FAIL("log missing compound metadata message");
        return;
    }
    if (!strstr(buf, "SCAN annotated as COMPOUND_INLINE")) {
        FAIL("log missing SCAN annotation INFO message");
        return;
    }

    PASS();
}

/* ======================================================================== */
/* Phase 3: Error handling / validation tests (Issue #539)                   */
/* ======================================================================== */

/*
 * Passing NULL program or NULL ast to convert_rules must return an error
 * code, not segfault.
 */
static void
test_convert_rules_null_inputs(void)
{
    TEST("wl_ir_program_convert_rules rejects NULL inputs");

    if (wl_ir_program_convert_rules(NULL, NULL) == 0) {
        FAIL("expected non-zero return for NULL program and ast");
        return;
    }

    struct wirelog_program *prog
        = make_program(".decl pred(a: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- pred(x).\n");
    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (wl_ir_program_convert_rules(prog, NULL) == 0) {
        wl_ir_program_free(prog);
        FAIL("expected non-zero return for NULL ast");
        return;
    }

    if (wl_ir_program_convert_rules(NULL, prog->ast) == 0) {
        wl_ir_program_free(prog);
        FAIL("expected non-zero return for NULL program");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

/*
 * Corrupt column metadata with an out-of-range compound_kind value.
 * build_atom_scan must skip the annotation and leave the leaf as SCAN
 * instead of producing a malformed node type.
 */
static void
test_compound_invalid_kind_skipped(void)
{
    TEST("Invalid compound_kind value leaves leaf as SCAN");

    struct wirelog_program *prog
        = make_program(".decl pred(a: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- pred(f(x)).\n");
    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    /* Inject an out-of-range kind (cast from int). */
    patch_compound_column(prog, "pred", 0,
        (wirelog_compound_kind_t)99, "f", 1, 0);

    if (wl_ir_program_convert_rules(prog, prog->ast) != 0) {
        wl_ir_program_free(prog);
        FAIL("convert_rules must tolerate corrupt compound_kind");
        return;
    }

    const wirelog_ir_node_t *leaf
        = find_relation_leaf(prog->rules[0].ir_root, "pred");
    if (!leaf) {
        wl_ir_program_free(prog);
        FAIL("could not locate pred leaf");
        return;
    }

    if (leaf->type != WIRELOG_IR_SCAN) {
        wl_ir_program_free(prog);
        FAIL("leaf must remain SCAN when compound_kind is invalid");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

/*
 * INLINE compounds must carry a non-zero arity; arity=0 is treated as
 * corrupt metadata and the annotation is skipped so downstream passes
 * don't see a compound with no payload.
 */
static void
test_compound_inline_zero_arity_skipped(void)
{
    TEST("INLINE compound with arity=0 leaves leaf as SCAN");

    struct wirelog_program *prog
        = make_program(".decl pred(a: int32)\n"
            ".decl r(x: int32)\n"
            "r(x) :- pred(f(x)).\n");
    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    /* arity=0 is invalid for an INLINE compound. */
    patch_compound_column(prog, "pred", 0, WIRELOG_COMPOUND_KIND_INLINE, "f", 0,
        0);

    if (wl_ir_program_convert_rules(prog, prog->ast) != 0) {
        wl_ir_program_free(prog);
        FAIL("convert_rules must tolerate arity=0");
        return;
    }

    const wirelog_ir_node_t *leaf
        = find_relation_leaf(prog->rules[0].ir_root, "pred");
    if (!leaf || leaf->type != WIRELOG_IR_SCAN) {
        wl_ir_program_free(prog);
        FAIL("leaf must remain SCAN when INLINE arity is 0");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

/*
 * A program with zero relations still exercises convert_rules without
 * hitting the compound annotation path. Sanity check that the empty/edge
 * case doesn't regress.
 */
static void
test_compound_no_relations_safe(void)
{
    TEST("convert_rules safe when program has no rules or relations");

    struct wirelog_program *prog = wl_ir_program_create();
    if (!prog) {
        FAIL("program_create returned NULL");
        return;
    }

    /* Build a minimal empty AST-like stub by parsing an empty program. */
    char errbuf[128] = { 0 };
    wl_parser_ast_node_t *ast
        = wl_parser_parse_string("", errbuf, sizeof(errbuf));
    if (!ast) {
        wl_ir_program_free(prog);
        FAIL("failed to parse empty program");
        return;
    }
    prog->ast = ast;

    if (wl_ir_program_convert_rules(prog, ast) != 0) {
        wl_ir_program_free(prog);
        FAIL("convert_rules failed on empty program");
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
test_api_get_schema_compound_columns(void)
{
    TEST("wirelog_program_get_schema exposes compound column metadata");

    wirelog_program_t *prog = wirelog_parse_string(
        ".decl Event(id: int64, payload: metadata/4 inline)\n", NULL);
    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    const wirelog_schema_t *schema
        = wirelog_program_get_schema(prog, "Event");
    if (!schema || schema->column_count != 2) {
        wirelog_program_free(prog);
        FAIL("schema for Event should have 2 columns");
        return;
    }

    const wirelog_column_t *payload = &schema->columns[1];
    if (payload->type != WIRELOG_TYPE_INT64
        || payload->compound_kind != WIRELOG_COMPOUND_KIND_INLINE
        || payload->compound_arity != 4
        || payload->compound_inline_col_offset != 1) {
        wirelog_program_free(prog);
        FAIL("payload compound metadata is incorrect");
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
test_api_get_relation_ir_single_rule(void)
{
    TEST("wirelog_program_get_relation_ir returns single-rule root");

    wirelog_program_t *prog
        = wirelog_parse_string(".decl Arc(x: int32, y: int32)\n"
            ".decl Reach(x: int32, y: int32)\n"
            "Reach(x, y) :- Arc(x, y).\n",
            NULL);

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    const wirelog_ir_node_t *root
        = wirelog_program_get_relation_ir(prog, "Reach");
    if (!root) {
        wirelog_program_free(prog);
        FAIL("Reach IR should not be NULL");
        return;
    }

    if (wirelog_ir_node_get_type(root) != WIRELOG_IR_PROJECT) {
        wirelog_program_free(prog);
        FAIL("single-rule relation should expose PROJECT root");
        return;
    }

    const char *relation = wirelog_ir_node_get_relation_name(root);
    if (!relation || strcmp(relation, "Reach") != 0) {
        wirelog_program_free(prog);
        FAIL("root relation name should be Reach");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_api_get_relation_ir_multi_rule_union(void)
{
    TEST(
        "wirelog_program_get_relation_ir returns UNION for multi-rule relation");

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

    const wirelog_ir_node_t *root = wirelog_program_get_relation_ir(prog, "Tc");
    if (!root) {
        wirelog_program_free(prog);
        FAIL("Tc IR should not be NULL");
        return;
    }

    if (wirelog_ir_node_get_type(root) != WIRELOG_IR_UNION) {
        wirelog_program_free(prog);
        FAIL("multi-rule relation should expose UNION root");
        return;
    }

    if (wirelog_ir_node_get_child_count(root) != 2) {
        wirelog_program_free(prog);
        FAIL("UNION root should have 2 children");
        return;
    }

    if (!wirelog_ir_node_get_child(root, 0)
        || !wirelog_ir_node_get_child(root, 1)) {
        wirelog_program_free(prog);
        FAIL("UNION children should be accessible");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_api_get_relation_ir_null_cases(void)
{
    TEST("wirelog_program_get_relation_ir returns NULL for absent IR roots");

    wirelog_program_t *prog
        = wirelog_parse_string(".decl Arc(x: int32, y: int32)\n", NULL);

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (wirelog_program_get_relation_ir(prog, "Arc") != NULL) {
        wirelog_program_free(prog);
        FAIL("EDB-only relation should return NULL");
        return;
    }

    if (wirelog_program_get_relation_ir(prog, "Missing") != NULL) {
        wirelog_program_free(prog);
        FAIL("unknown relation should return NULL");
        return;
    }

    if (wirelog_program_get_relation_ir(NULL, "Arc") != NULL
        || wirelog_program_get_relation_ir(prog, NULL) != NULL) {
        wirelog_program_free(prog);
        FAIL("NULL inputs should return NULL");
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
/* Issue #977 (unit 2): inline fact arity must match the .decl              */
/* ======================================================================== */

/* collect_fact() packs fact_data using the *fact's own* child_count as the
 * row stride, while both readers of that buffer stride by the *declared*
 * rel->column_count: wl_session_load_facts (session_facts.c) and
 * wirelog_program_get_facts (the public API, ir/api.c).
 * Nothing reconciled the two, so an arity disagreement produced a heap
 * over-read, an uninitialised read inside the allocation, or a silently
 * fabricated tuple -- depending on which side was wider.
 *
 * Entry point: these tests go through make_program(), which calls
 * wl_ir_program_collect_metadata() directly.  That is exactly where the
 * validation pass lives (tail of the function, after the source-order
 * loop), so a NULL return here is this rejection and not a later stage.
 * ir/api.c maps the same non-zero return to WIRELOG_ERR_PARSE, which
 * test_fact_arity_mismatch_maps_to_parse_error() pins separately.
 *
 * The check deliberately does NOT live inside collect_fact():
 * wl_ir_program_collect_metadata() is a single source-order loop, so a
 * .decl written after its facts has not been seen when collect_fact() runs.
 * test_fact_arity_decl_after_facts_rejected() is the case that a
 * collect_fact-internal check would wrongly accept. */

static void
test_fact_arity_narrower_than_decl_rejected(void)
{
    TEST("Fact arity: fact narrower than .decl is rejected");

    /* 1-arity fact against a 9-column .decl.  Pre-fix the readers strided
     * the declared width per row over a buffer packed at the fact's own
     * width.  collect_fact seeds fact_capacity at ncols * 8, so with a
     * 1-arity fact the buffer holds 8 slots: a 9-column read runs one slot
     * past the allocation -- heap-buffer-overflow in col_rel_row_copy_in
     * <- col_rel_append_row <- col_session_insert <- wl_session_load_facts.
     *
     * The width has to exceed 8 for that.  At exactly .decl p/8 the read
     * lands on the last slot rather than past it, so ASAN stays silent and
     * the program exits 0 emitting uninitialised heap as answers -- the
     * quieter defect, covered by the second case below. */
    struct wirelog_program *bad = make_program(
        ".decl p(a: int32, b: int32, c: int32, d: int32, e: int32,"
        " f: int32, g: int32, h: int32, i: int32)\n"
        "p(1).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("p(1). against .decl p/9 should be rejected");
        return;
    }

    /* Two 1-arity facts against .decl p/8: 2 rows * 8 columns = 16 slots
     * read from the same 8-slot buffer, so this over-reads too even though
     * the declared width does not exceed the seed capacity. */
    bad = make_program(
        ".decl p(a: int32, b: int32, c: int32, d: int32,"
        " e: int32, f: int32, g: int32, h: int32)\n"
        "p(1). p(2).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("p(1). p(2). against .decl p/8 should be rejected");
        return;
    }

    /* The ASAN-silent shape: one 1-arity fact against .decl p/8 reads
     * within the allocation but past what was written. */
    bad = make_program(
        ".decl p(a: int32, b: int32, c: int32, d: int32,"
        " e: int32, f: int32, g: int32, h: int32)\n"
        "p(1).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("p(1). against .decl p/8 should be rejected");
        return;
    }

    PASS();
}

static void
test_fact_arity_decl_after_facts_rejected(void)
{
    TEST("Fact arity: .decl written after the facts is still checked");

    /* Same mismatch as above, source order reversed.  This is the case
     * that proves the pass must re-walk the AST after the collection loop
     * rather than validate inside collect_fact(). */
    struct wirelog_program *bad = make_program(
        "p(1).\n"
        ".decl p(a: int32, b: int32, c: int32, d: int32,"
        " e: int32, f: int32, g: int32, h: int32)\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("p(1). before .decl p/8 should be rejected");
        return;
    }

    PASS();
}

static void
test_fact_arity_wider_than_decl_rejected(void)
{
    TEST("Fact arity: fact wider than .decl is rejected");

    /* Pre-fix this was the silent-corruption case: val(1,5,7). val(2,6,8).
     * packed 3 values per row but was read back 2 per row, so the program
     * evaluated to t(1,5) and t(7,2) -- a tuple present in no source fact --
     * and dropped (6,8) entirely.  Exit status was 0. */
    struct wirelog_program *bad
        = make_program(".decl val(g: int32, v: int32)\n"
            "val(1, 5, 7).\n"
            "val(2, 6, 8).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("val(1,5,7). against .decl val/2 should be rejected");
        return;
    }

    /* Inline-compound columns.  `.decl p(id, lbl: pair/2 inline)` has a
     * *logical* width of 2 and a *physical* width of 3, and this fact is
     * written flat.  It is rejected, because facts are compared logically:
     * collect_fact packs one slot per written argument and both readers
     * stride by rel->column_count, which is the logical width.
     *
     * Pre-fix this was accepted and silently dropped the third value.
     * Note this is the one place where the fact rule and the rule-head rule
     * diverge: validate_head_arities() (#977 unit 3) compares *physical*
     * width via atom_physical_column_count(), so the head spelling of this
     * same shape -- pred(x,y,z) against .decl pred(id, payload: f/2 inline)
     * -- is accepted, and is pinned by
     * test_head_arity_inline_compound_accepted().  The divergence is not an
     * oversight: the head grammar has no compound-term production, so the
     * flattened spelling is the only one available to a rule, while facts
     * are packed and read back at the logical width.  If compound terms ever
     * become expressible in facts, this comparison has to move with it. */
    bad = make_program(".decl p(id: int64, lbl: pair/2 inline)\n"
            "p(1, 2, 3).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("flat p(1,2,3). against inline-compound .decl p/2 rejected");
        return;
    }

    PASS();
}

static void
test_fact_arity_mixed_across_facts_rejected(void)
{
    TEST("Fact arity: mixed arities across facts are rejected");

    /* collect_fact() computes offset = fact_count * ncols with a *per-fact*
     * ncols, so p(1). writes index 0 and p(2,3). writes indices 2..3,
     * leaving index 1 never written.  realloc does not zero, so the reader
     * returned uninitialised heap (observed: 0xBEBEBEBEBEBEBEBE) with ASAN
     * silent and exit 0, because the read stayed inside the allocation. */
    struct wirelog_program *bad = make_program(".decl p(a: int32, b: int32)\n"
            "p(1).\n"
            "p(2, 3).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("p(1). p(2,3). against .decl p/2 should be rejected");
        return;
    }

    PASS();
}

static void
test_fact_arity_zero_param_decl_rejected(void)
{
    TEST("Fact arity: .decl p() with a 1-arity fact is rejected");

    /* `.decl p()` parses and leaves column_count == 0 (see
     * test_parse_decl_empty_params in test_parser.c), which is precisely
     * why the guard keys off has_decl rather than the column_count > 0
     * proxy -- the proxy would skip exactly these relations.
     *
     * Deliberate decision: p(1). against a zero-column declaration IS an
     * arity mismatch and is rejected.  Pre-fix it reached the session and
     * died at runtime with "error: failed to load facts for 'p'" (exit 1),
     * so nothing that used to work stops working; the diagnostic just moves
     * to parse time and names both arities. */
    struct wirelog_program *bad = make_program(".decl p()\n"
            "p(1).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("p(1). against .decl p() should be rejected");
        return;
    }

    /* A zero-arity fact against a zero-column .decl matches and is
     * accepted.  Note what "accepted" means here: collect_fact computes
     * needed = fact_count * 0 == 0 and never allocates, so fact_data stays
     * NULL and wl_session_load_facts skips the relation entirely.  The fact
     * is silently *dropped*, and p() evaluates to false -- pre-existing
     * behaviour this check neither causes nor fixes.  Do not read this test
     * as evidence that zero-arity facts work. */
    struct wirelog_program *ok = make_program(".decl p()\n"
            "p().\n");
    if (!ok) {
        FAIL("p(). against .decl p() should be accepted");
        return;
    }
    wl_ir_program_free(ok);

    PASS();
}

static void
test_fact_arity_matching_accepted(void)
{
    TEST("Fact arity: matching facts still load and lower correctly");

    /* Positive control: the exact shape of the wider-fact case above, with
     * the correct arity.  Assert the stored values, not just non-NULL --
     * a pass that rejected everything would still return non-NULL here if
     * we only checked for a program. */
    struct wirelog_program *prog
        = make_program_with_rules(".decl val(g: int32, v: int32)\n"
            ".decl t(x: int32, y: int32)\n"
            "val(1, 2).\n"
            "val(3, 4).\n"
            "t(x, y) :- val(x, y).\n");
    if (!prog) {
        FAIL("matching-arity program should be accepted");
        return;
    }

    const wl_ir_relation_info_t *val = NULL;
    for (uint32_t i = 0; i < prog->relation_count; i++) {
        if (strcmp(prog->relations[i].name, "val") == 0)
            val = &prog->relations[i];
    }
    if (!val || val->column_count != 2 || val->fact_count != 2
        || !val->fact_data) {
        wl_ir_program_free(prog);
        FAIL("val should hold 2 facts of 2 columns");
        return;
    }
    if (val->fact_data[0] != 1 || val->fact_data[1] != 2
        || val->fact_data[2] != 3 || val->fact_data[3] != 4) {
        wl_ir_program_free(prog);
        FAIL("val fact_data should be {1,2,3,4}");
        return;
    }
    if (prog->rule_count != 1 || !prog->rules[0].ir_root) {
        wl_ir_program_free(prog);
        FAIL("the rule should still lower");
        return;
    }
    wl_ir_program_free(prog);

    PASS();
}

static void
test_fact_arity_undeclared_relations_unaffected(void)
{
    TEST("Fact arity: undeclared relations keep their current behaviour");

    /* An undeclared *derived* head is widespread and legitimate
     * (bench/workloads/doop.dl alone has ~90).  It must not be rejected. */
    struct wirelog_program *derived
        = make_program_with_rules(".decl val(g: int32, v: int32)\n"
            "val(1, 2).\n"
            "t(g) :- val(g, v).\n");
    if (!derived) {
        FAIL("undeclared derived head must still be accepted");
        return;
    }
    wl_ir_program_free(derived);

    /* An undeclared relation *with facts* keeps working through metadata
     * collection: column_count is 0 because no .decl was seen, and the
     * guard keys off has_decl, so the pass stays out of the way.  Such a
     * program still fails later at session load ("failed to load facts for
     * 'q'", exit 1) -- facts on undeclared relations are still unvalidated
     * under #977, and remain so after the rule-head pass (unit 3) landed.
     * This case is what pins the has_decl condition: drop it and this
     * program is rejected at parse time instead. */
    struct wirelog_program *undecl = make_program("q(1, 2).\n"
            "q(3, 4).\n");
    if (!undecl) {
        FAIL("undeclared relation with facts must still collect");
        return;
    }
    wl_ir_program_free(undecl);

    PASS();
}

static void
test_fact_arity_mismatch_maps_to_parse_error(void)
{
    TEST("Fact arity: public API reports WIRELOG_ERR_PARSE");

    /* The public reader wirelog_program_get_facts() memcpy's
     * fact_count * column_count from the mismatched buffer, so an embedder
     * hit the over-read with no session involved at all.  Confirm the
     * program never gets built, and that the error is the documented one. */
    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *bad
        = wirelog_parse_string(".decl val(g: int32, v: int32)\n"
            "val(1, 5, 7).\n",
            &err);
    if (bad) {
        wirelog_program_free(bad);
        FAIL("public API should reject the arity mismatch");
        return;
    }
    if (err != WIRELOG_ERR_PARSE) {
        FAIL("error should be WIRELOG_ERR_PARSE");
        return;
    }

    /* Control: the matching program still round-trips through the public
     * reader with the right values. */
    wirelog_program_t *ok = wirelog_parse_string(
        ".decl val(g: int32, v: int32)\n"
        "val(1, 2).\n"
        "val(3, 4).\n",
        &err);
    if (!ok || err != WIRELOG_OK) {
        if (ok)
            wirelog_program_free(ok);
        FAIL("control: matching program should parse");
        return;
    }
    int64_t *data = NULL;
    uint32_t nrows = 0, ncols = 0;
    if (wirelog_program_get_facts(ok, "val", &data, &nrows, &ncols) != 0) {
        wirelog_program_free(ok);
        FAIL("control: get_facts should succeed");
        return;
    }
    if (nrows != 2 || ncols != 2 || data[0] != 1 || data[1] != 2
        || data[2] != 3 || data[3] != 4) {
        free(data);
        wirelog_program_free(ok);
        FAIL("control: get_facts should return {1,2,3,4}");
        return;
    }
    free(data);
    wirelog_program_free(ok);

    PASS();
}

/* ======================================================================== */
/* Issue #977 (unit 3): rule-head arity must match the .decl                */
/* ======================================================================== */

/* A rule head whose arity disagrees with its relation's `.decl` was accepted
 * silently.  In a recursive stratum that is a heap over-read: col_op_reduce()
 * (columnar/ops.c) sizes its output region as group_by_count + 1 while
 * col_rel_append_all() (columnar/relation.c) copies dst->ncols columns from
 * it, unclamped against src->ncols -- ASAN reports
 * "heap-buffer-overflow READ of size 8" in col_rel_append_all under
 * col_eval_stratum() (columnar/eval.c), exit 139.  Outside a recursive
 * stratum it is a silent wrong answer instead.
 *
 * The predicate compares PHYSICAL width, not logical, and that is the whole
 * subtlety of this unit.  Unit 2 deliberately does the opposite for facts:
 * collect_fact() packs one slot per written argument and both readers stride
 * by the logical rel->column_count, so facts compare logically.  A rule head
 * is different -- it is written flat, one head argument per *physical*
 * column, because the head grammar has no compound-term production at all
 * (parse_head_arg() dispatches only to aggregate and arithmetic expressions,
 * so `pred(x, f(y,z))` is a parse error).  The only spelling available for
 * an inline-compound relation is the expanded one, `pred(x,y,z)`, which is
 * three head arguments against a two-column .decl.  A logical
 * `child_count != column_count` check would reject working code;
 * test_head_arity_inline_compound_accepted() is the case that pins this.
 *
 * Entry point: these tests go through make_program(), which calls
 * wl_ir_program_collect_metadata() directly -- the same tail pass that holds
 * the fact check -- so a NULL return is this rejection and not a later stage.
 * The positive controls use make_program_with_rules() so they additionally
 * prove the rule still lowers. */

static void
test_head_arity_recursive_aggregate_rejected(void)
{
    TEST("Head arity: the recursive single-aggregate crash shape is rejected");

    /* THE memory-safety case.  Before this check the program below
     * segfaulted (exit 139) inside col_eval_stratum().  Issue #973 rejects
     * the *multi*-aggregate spelling of the same crash; this single-aggregate
     * spelling reached evaluation untouched, because it is not the aggregate
     * count that is wrong -- the head emits 2 columns into a 3-column
     * relation that facts have already materialised at width 3. */
    struct wirelog_program *bad
        = make_program(".decl edge(x: int64, y: int64)\n"
            ".decl cc(n: int64, lo: int64, hi: int64)\n"
            "edge(1,2). edge(2,3). edge(3,1).\n"
            "cc(1,10,10).\n"
            "cc(y, min(c)) :- cc(x, c, d), edge(x, y).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("cc(y, min(c)) against a 3-column .decl should be rejected");
        return;
    }

    PASS();
}

static void
test_head_arity_narrower_than_decl_rejected(void)
{
    TEST("Head arity: head narrower than .decl is rejected");

    /* This program has no producer of t at the declared width, so pre-fix it
     * evaluated cleanly with exit 0 and emitted one column into a relation
     * declared with three -- a silent wrong answer.  Add a `t(9,9,9).` fact
     * and the same non-recursive shape becomes a heap over-read through
     * col_op_map(); recursion is not what makes this unsafe, a
     * declared-width producer is. */
    struct wirelog_program *bad
        = make_program(".decl val(g: int64, v: int64)\n"
            ".decl t(a: int64, b: int64, c: int64)\n"
            "val(1,10). val(2,20).\n"
            "t(g) :- val(g, v).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("t(g) against .decl t/3 should be rejected");
        return;
    }

    PASS();
}

static void
test_head_arity_wider_than_decl_rejected(void)
{
    TEST("Head arity: head wider than .decl is rejected");

    /* Emitted four columns against a two-column .decl, exit 0, printing
     * t(1, 10, 1, 10). */
    struct wirelog_program *bad
        = make_program(".decl val(g: int64, v: int64)\n"
            ".decl t(a: int64, b: int64)\n"
            "val(1,10). val(2,20).\n"
            "t(g, v, g, v) :- val(g, v).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("t(g,v,g,v) against .decl t/2 should be rejected");
        return;
    }

    /* The .decl may follow its rules, exactly as it may follow its facts;
     * the pass runs after the whole source-order collection loop. */
    bad = make_program("t(g, v, g, v) :- val(g, v).\n"
            ".decl val(g: int64, v: int64)\n"
            ".decl t(a: int64, b: int64)\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL(".decl written after the rule should still be checked");
        return;
    }

    PASS();
}

static void
test_head_arity_inline_compound_accepted(void)
{
    TEST("Head arity: flattened inline-compound head is accepted");

    /* POSITIVE CONTROL for the physical-width predicate.  `pred` is declared
    * with two *logical* columns and three *physical* ones (id, plus f/2
    * expanded into two slots).  The head writes three arguments, which is
    * the only spelling the grammar allows, and evaluation produces
    * pred(1, 10, 20).
    *
    * If anyone "simplifies" the predicate to the logical comparison unit 2
    * uses for facts (head->child_count != rel->column_count), this test
    * fails with "flattened inline-compound head should be accepted" --
    * 3 != 2.  That is the entire reason this test exists.
    *
    * test_wirelog_easy_inline_facts.c (T4) carries the evaluation half:
    * this binary links parser/ir/io/thread only and cannot run a program. */
    struct wirelog_program *ok
        = make_program_with_rules(".decl src(a: int64, b: int64, c: int64)\n"
            ".decl pred(id: int64, payload: f/2 inline)\n"
            "src(1,10,20).\n"
            "pred(x, y, z) :- src(x, y, z).\n");
    if (!ok) {
        FAIL("flattened inline-compound head should be accepted");
        return;
    }
    if (ok->rule_count != 1 || !ok->rules[0].ir_root
        || ok->rules[0].ir_root->type != WIRELOG_IR_PROJECT
        || ok->rules[0].ir_root->project_count != 3) {
        wl_ir_program_free(ok);
        FAIL("head should lower to a PROJECT of 3 physical columns");
        return;
    }
    wl_ir_program_free(ok);

    PASS();
}

static void
test_head_arity_compound_handle_form_rejected(void)
{
    TEST("Head arity: handle-form head into a compound relation is rejected");

    /* The open question this unit had to settle.  `pred(1, 99).` is a legal
     * *fact* -- unit 2 compares facts logically, and 2 == 2 -- so it is not
     * obvious that the head analogue `pred(x, y) :- src(x, y).` should be
     * illegal.  It is, and the reason is not symmetry but a fabricated
     * value: the head leaves the second inline slot never written, and a
     * body pattern that destructures the column reads it anyway.
     *
     *   .decl src(a: int64, b: int64)
     *   .decl pred(id: int64, payload: f/2 inline)
     *   .decl outr(id: int64, p: int64, q: int64)
     *   src(1,99).
     *   pred(x, y)        :- src(x, y).
     *   outr(id, p, q)    :- pred(id, f(p, q)).
     *
     * evaluated to outr(1, 99, 0) with exit 0 and ASAN silent -- the 0 is
     * an unwritten physical slot, present in no source data.  The
     * three-argument spelling of the same program yields outr(1, 10, 20)
     * from src(1,10,20), which is correct.  So the physical width is the
     * one the relation must be written at, and this shape is rejected.
     *
     * (The identical defect via the *fact* form -- `pred(1, 99).` plus the
     * same destructuring rule, also outr(1, 99, 0) -- is still accepted,
     * because unit 2 must compare facts logically: wl_session_load_facts()
     * and wirelog_program_get_facts() both stride by column_count.  That
     * asymmetry is real and is not resolved here -- issue #985 tracks it,
     * along with the fact that an inline-compound relation cannot be
     * populated correctly by an inline fact at all.) */
    struct wirelog_program *bad
        = make_program(".decl src(a: int64, b: int64)\n"
            ".decl pred(id: int64, payload: f/2 inline)\n"
            "src(1,99).\n"
            "pred(x, y) :- src(x, y).\n");
    if (bad) {
        wl_ir_program_free(bad);
        FAIL("2-argument head into an inline f/2 relation should be rejected");
        return;
    }

    /* A `side` compound occupies one physical slot, not compound_arity of
     * them, so the handle form is the *only* form there and must pass.
     * This is what stops the physical-width sum from being written as a
     * blanket `+= compound_arity`. */
    struct wirelog_program *ok
        = make_program_with_rules(".decl src(a: int64, b: int64)\n"
            ".decl sref(id: int64, meta: m/4 side)\n"
            "src(1,99).\n"
            "sref(x, y) :- src(x, y).\n");
    if (!ok) {
        FAIL("2-argument head into a side-compound relation must be accepted");
        return;
    }
    wl_ir_program_free(ok);

    PASS();
}

static void
test_head_arity_undeclared_head_accepted(void)
{
    TEST("Head arity: undeclared heads keep their current behaviour");

    /* POSITIVE CONTROL for the has_decl guard.  Undeclared derived heads are
     * widespread and legitimate -- bench/workloads/doop.dl alone has ~90 --
     * and the check must never see them.  Drop has_decl from the guard and
     * this fails: column_count is 0 for an undeclared relation, so the
     * comparison becomes 1 != 0. */
    struct wirelog_program *ok
        = make_program_with_rules(".decl val(g: int64, v: int64)\n"
            "val(1,10).\n"
            "t(g) :- val(g, v).\n");
    if (!ok) {
        FAIL("undeclared head must still be accepted");
        return;
    }
    wl_ir_program_free(ok);

    PASS();
}

static void
test_head_arity_matching_accepted(void)
{
    TEST("Head arity: matching plain and aggregate heads are accepted");

    /* Plain head, matching arity. */
    struct wirelog_program *plain
        = make_program_with_rules(".decl val(g: int64, v: int64)\n"
            ".decl t(a: int64, b: int64)\n"
            "val(1,10).\n"
            "t(g, v) :- val(g, v).\n");
    if (!plain) {
        FAIL("matching plain head should be accepted");
        return;
    }
    wl_ir_program_free(plain);

    /* Aggregate head, matching arity.  An AGGREGATE head argument is one
     * direct HEAD child (parse_aggregate_expr() is reached only from
     * parse_head_arg()), so t(g, min(v)) has child_count 2 and lowers to
     * group_by_count + 1 == 2 emitted columns.  The head arity is therefore
     * counted the same way for aggregate and non-aggregate heads, and the
     * check needs no aggregate-specific case. */
    struct wirelog_program *agg
        = make_program_with_rules(".decl val(g: int64, v: int64)\n"
            ".decl t(a: int64, b: int64)\n"
            "val(1,10).\n"
            "t(g, min(v)) :- val(g, v).\n");
    if (!agg) {
        FAIL("matching single-aggregate head should be accepted");
        return;
    }
    if (agg->rules[0].ir_root->type != WIRELOG_IR_AGGREGATE
        || agg->rules[0].ir_root->group_by_count != 1) {
        wl_ir_program_free(agg);
        FAIL("aggregate head should lower to AGGREGATE/group_by_count==1");
        return;
    }
    wl_ir_program_free(agg);

    /* The two live single-aggregate workloads, bench/workloads/cc.dl:6 and
     * sssp.dl:5, in their exact declared shapes.  Both are recursive and
     * both must keep working. */
    struct wirelog_program *cc
        = make_program_with_rules(".decl edge(x: int32, y: int32)\n"
            ".decl cc(x: int32, c: int32)\n"
            "cc(x, x) :- edge(x, _).\n"
            "cc(x, x) :- edge(_, x).\n"
            "cc(y, min(c)) :- cc(x, c), edge(x, y).\n");
    if (!cc) {
        FAIL("bench/workloads/cc.dl head shape must still lower");
        return;
    }
    wl_ir_program_free(cc);

    struct wirelog_program *sssp
        = make_program_with_rules(".decl wedge(x: int32, y: int32, w: int32)\n"
            ".decl dist(x: int32, d: int32)\n"
            "dist(1, 0).\n"
            "dist(y, min(d + w)) :- dist(x, d), wedge(x, y, w).\n");
    if (!sssp) {
        FAIL("bench/workloads/sssp.dl head shape must still lower");
        return;
    }
    wl_ir_program_free(sssp);

    /* Issue #980: aggregate *position* is ignored -- t(min(v), g) lowers
     * group-by-first regardless of how it was written.  That is out of
     * scope here, but the check must not accidentally depend on position:
     * both spellings have the same arity and both must be accepted. */
    struct wirelog_program *swapped
        = make_program_with_rules(".decl val(g: int64, v: int64)\n"
            ".decl t(a: int64, b: int64)\n"
            "val(1,10).\n"
            "t(min(v), g) :- val(g, v).\n");
    if (!swapped) {
        FAIL("aggregate-first head has matching arity and must be accepted");
        return;
    }
    wl_ir_program_free(swapped);

    PASS();
}

static void
test_head_arity_mismatch_maps_to_parse_error(void)
{
    TEST("Head arity: public API reports WIRELOG_ERR_PARSE");

    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *bad
        = wirelog_parse_string(".decl edge(x: int64, y: int64)\n"
            ".decl cc(n: int64, lo: int64, hi: int64)\n"
            "edge(1,2).\n"
            "cc(1,10,10).\n"
            "cc(y, min(c)) :- cc(x, c, d), edge(x, y).\n",
            &err);
    if (bad) {
        wirelog_program_free(bad);
        FAIL("public API should reject the head arity mismatch");
        return;
    }
    if (err != WIRELOG_ERR_PARSE) {
        FAIL("error should be WIRELOG_ERR_PARSE");
        return;
    }

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

/* ======================================================================== */
/* Issue #535: RDF Named-Graph column detection                             */
/* ======================================================================== */

static void
test_rdf_graph_column_flag_set(void)
{
    TEST("__graph_id column sets has_graph_column + graph_column_index");

    struct wirelog_program *prog = make_program(
        ".decl fact(a: int64, b: int64, __graph_id: int64)\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (prog->relation_count != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 relation, got %u",
            prog->relation_count);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    if (!prog->relations[0].has_graph_column) {
        wl_ir_program_free(prog);
        FAIL("has_graph_column should be true");
        return;
    }

    if (prog->relations[0].graph_column_index != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected graph_column_index=2, got %u",
            prog->relations[0].graph_column_index);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    if (prog->relations[0].column_count != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 columns, got %u",
            prog->relations[0].column_count);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_rdf_graph_column_flag_absent(void)
{
    TEST("relation without __graph_id has has_graph_column == false");

    struct wirelog_program *prog = make_program(
        ".decl fact(a: int64, b: int64)\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (prog->relation_count != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 relation, got %u",
            prog->relation_count);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    if (prog->relations[0].has_graph_column) {
        wl_ir_program_free(prog);
        FAIL("has_graph_column should be false when __graph_id absent");
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_rdf_graph_column_at_index_zero(void)
{
    TEST(
        "__graph_id at column 0 sets index=0 (not confused with default zero)");

    struct wirelog_program *prog = make_program(
        ".decl edge(__graph_id: int64, src: int64, dst: int64)\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    if (prog->relation_count != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 relation, got %u",
            prog->relation_count);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    if (!prog->relations[0].has_graph_column) {
        wl_ir_program_free(prog);
        FAIL("has_graph_column should be true");
        return;
    }

    if (prog->relations[0].graph_column_index != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected graph_column_index=0, got %u",
            prog->relations[0].graph_column_index);
        wl_ir_program_free(prog);
        FAIL(buf);
        return;
    }

    wl_ir_program_free(prog);
    PASS();
}

static void
test_rdf_graph_metadata_reserved_arity_enforced(void)
{
    TEST("user .decl __graph_metadata with wrong arity is rejected");

    /* make_program returns NULL when the parser or metadata pass fails.
     * Two columns is not the reserved 6-column schema. */
    struct wirelog_program *prog = make_program(
        ".decl __graph_metadata(graph_id: int64, tenant: int64)\n");

    if (prog != NULL) {
        wl_ir_program_free(prog);
        FAIL("__graph_metadata with 2 columns should be rejected");
        return;
    }
    PASS();
}

static void
test_rdf_graph_metadata_reserved_arity_accepted(void)
{
    TEST("user .decl __graph_metadata with 6 columns is accepted");

    struct wirelog_program *prog = make_program(
        ".decl __graph_metadata(graph_id: int64, tenant: int64,"
        " timestamp: int64, location: int64, risk: int64,"
        " description: int64)\n");

    if (prog == NULL) {
        FAIL("__graph_metadata with 6 columns should be accepted");
        return;
    }
    wl_ir_program_free(prog);
    PASS();
}

int
main(void)
{
    printf("\n=== wirelog Program Tests ===\n\n");

    /* Metadata collection */
    test_decl_single_relation();
    test_decl_compound_column_metadata();
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
    test_aggregation_multi_head_rejected();
    test_three_body_join();
    test_duplicate_variable();
    test_constant_in_atom();
    test_wildcard_not_join_key();
    test_wildcard_in_scan();
    test_boolean_true_noop();
    test_boolean_false_filter();

    /* Phase 2B: compound column IR lowering (Issue #531/#539) */
    test_compound_inline_annotation();
    test_compound_inline_annotation_from_declaration();
    test_compound_side_annotation();
    test_compound_annotation_not_wrapped();
    test_compound_mixed_columns();
    test_compound_regular_atom_unchanged();
    test_compound_participates_in_join();

    /* Phase 3 (Issue #539): observability via WL_LOG */
    test_compound_observability_logging();

    /* Phase 3: error handling / validation (Issue #539) */
    test_convert_rules_null_inputs();
    test_compound_invalid_kind_skipped();
    test_compound_inline_zero_arity_skipped();
    test_compound_no_relations_safe();

    /* Issue #535: RDF named-graph column detection */
    test_rdf_graph_column_flag_set();
    test_rdf_graph_column_flag_absent();
    test_rdf_graph_column_at_index_zero();
    test_rdf_graph_metadata_reserved_arity_enforced();
    test_rdf_graph_metadata_reserved_arity_accepted();

    /* UNION merge */
    test_union_merge_tc();
    test_union_single_rule();

    /* Fact collection */
    test_fact_collection_single_relation();

    /* Issue #977 unit 2: inline fact arity vs .decl */
    test_fact_arity_narrower_than_decl_rejected();
    test_fact_arity_decl_after_facts_rejected();
    test_fact_arity_wider_than_decl_rejected();
    test_fact_arity_mixed_across_facts_rejected();
    test_fact_arity_zero_param_decl_rejected();
    test_fact_arity_matching_accepted();
    test_fact_arity_undeclared_relations_unaffected();
    test_fact_arity_mismatch_maps_to_parse_error();

    /* Issue #977 unit 3: rule-head arity vs .decl */
    test_head_arity_recursive_aggregate_rejected();
    test_head_arity_narrower_than_decl_rejected();
    test_head_arity_wider_than_decl_rejected();
    test_head_arity_inline_compound_accepted();
    test_head_arity_compound_handle_form_rejected();
    test_head_arity_undeclared_head_accepted();
    test_head_arity_matching_accepted();
    test_head_arity_mismatch_maps_to_parse_error();

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
    test_api_get_schema_compound_columns();
    test_api_get_schema_null();
    test_api_get_relation_ir_single_rule();
    test_api_get_relation_ir_multi_rule_union();
    test_api_get_relation_ir_null_cases();
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
