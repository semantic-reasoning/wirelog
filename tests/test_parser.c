/*
 * test_parser.c - wirelog Parser Test Suite
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Comprehensive parser tests covering all FlowLog grammar features.
 * Tests written first (TDD) before parser implementation.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/parser/parser.h"

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

#define PARSE(src) \
    char errbuf[512] = {0}; \
    wl_ast_node_t *program = wl_parse_string(src, errbuf, sizeof(errbuf))

#define ASSERT_PARSED() \
    do { \
        if (!program) { \
            char buf[600]; \
            snprintf(buf, sizeof(buf), "parse failed: %s", errbuf); \
            FAIL(buf); \
            return; \
        } \
        if (program->type != WL_NODE_PROGRAM) { \
            wl_ast_node_free(program); \
            FAIL("root is not PROGRAM"); \
            return; \
        } \
    } while (0)

#define CLEANUP() wl_ast_node_free(program)

/* Helper to get nth child */
static const wl_ast_node_t*
child(const wl_ast_node_t *node, uint32_t index)
{
    if (!node || index >= node->child_count) return NULL;
    return node->children[index];
}

/* ======================================================================== */
/* Parser: Empty and Minimal Programs                                       */
/* ======================================================================== */

static void test_parse_empty(void) {
    TEST("empty program");
    PARSE("");
    ASSERT_PARSED();
    if (program->child_count != 0) {
        CLEANUP();
        FAIL("expected 0 children");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_comments_only(void) {
    TEST("comments-only program");
    PARSE("# comment line 1\n// comment line 2\n");
    ASSERT_PARSED();
    if (program->child_count != 0) {
        CLEANUP();
        FAIL("expected 0 children");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Parser: Declarations                                                     */
/* ======================================================================== */

static void test_parse_simple_decl(void) {
    TEST("simple .decl with two attributes");
    PARSE(".decl Arc(x: int32, y: int32)");
    ASSERT_PARSED();
    if (program->child_count != 1) {
        CLEANUP();
        FAIL("expected 1 child");
        return;
    }
    const wl_ast_node_t *decl = child(program, 0);
    if (decl->type != WL_NODE_DECL) {
        CLEANUP();
        FAIL("expected DECL node");
        return;
    }
    if (strcmp(decl->name, "Arc") != 0) {
        CLEANUP();
        FAIL("expected name 'Arc'");
        return;
    }
    if (decl->child_count != 2) {
        CLEANUP();
        FAIL("expected 2 typed params");
        return;
    }
    const wl_ast_node_t *p0 = child(decl, 0);
    if (p0->type != WL_NODE_TYPED_PARAM ||
        strcmp(p0->name, "x") != 0 ||
        strcmp(p0->type_name, "int32") != 0) {
        CLEANUP();
        FAIL("first param should be x: int32");
        return;
    }
    const wl_ast_node_t *p1 = child(decl, 1);
    if (p1->type != WL_NODE_TYPED_PARAM ||
        strcmp(p1->name, "y") != 0 ||
        strcmp(p1->type_name, "int32") != 0) {
        CLEANUP();
        FAIL("second param should be y: int32");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_decl_int64(void) {
    TEST(".decl with int64 type");
    PARSE(".decl BigNum(val: int64)");
    ASSERT_PARSED();
    const wl_ast_node_t *decl = child(program, 0);
    const wl_ast_node_t *p0 = child(decl, 0);
    if (strcmp(p0->type_name, "int64") != 0) {
        CLEANUP();
        FAIL("expected type int64");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_decl_string_type(void) {
    TEST(".decl with string type");
    PARSE(".decl Name(id: int32, label: string)");
    ASSERT_PARSED();
    const wl_ast_node_t *decl = child(program, 0);
    const wl_ast_node_t *p1 = child(decl, 1);
    if (strcmp(p1->name, "label") != 0 ||
        strcmp(p1->type_name, "string") != 0) {
        CLEANUP();
        FAIL("expected label: string");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_decl_empty_params(void) {
    TEST(".decl with empty params");
    PARSE(".decl Empty()");
    ASSERT_PARSED();
    const wl_ast_node_t *decl = child(program, 0);
    if (decl->child_count != 0) {
        CLEANUP();
        FAIL("expected 0 params");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_decl_three_attrs(void) {
    TEST(".decl with three attributes");
    PARSE(".decl Arc(src: int32, dest: int32, weight: int32)");
    ASSERT_PARSED();
    const wl_ast_node_t *decl = child(program, 0);
    if (decl->child_count != 3) {
        CLEANUP();
        FAIL("expected 3 params");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Parser: Input/Output/PrintSize Directives                                */
/* ======================================================================== */

static void test_parse_input_directive(void) {
    TEST(".input with parameters");
    PARSE(".input Arc(IO=\"file\", filename=\"Arc.csv\", delimiter=\",\")");
    ASSERT_PARSED();
    const wl_ast_node_t *inp = child(program, 0);
    if (inp->type != WL_NODE_INPUT) {
        CLEANUP();
        FAIL("expected INPUT node");
        return;
    }
    if (strcmp(inp->name, "Arc") != 0) {
        CLEANUP();
        FAIL("expected name 'Arc'");
        return;
    }
    if (inp->child_count != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 params, got %u",
                 inp->child_count);
        CLEANUP();
        FAIL(buf);
        return;
    }
    const wl_ast_node_t *p0 = child(inp, 0);
    if (p0->type != WL_NODE_INPUT_PARAM ||
        strcmp(p0->name, "IO") != 0 ||
        strcmp(p0->str_value, "file") != 0) {
        CLEANUP();
        FAIL("first param should be IO=\"file\"");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_output_directive(void) {
    TEST(".output directive");
    PARSE(".output Reach");
    ASSERT_PARSED();
    const wl_ast_node_t *out = child(program, 0);
    if (out->type != WL_NODE_OUTPUT ||
        strcmp(out->name, "Reach") != 0) {
        CLEANUP();
        FAIL("expected OUTPUT 'Reach'");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_printsize_directive(void) {
    TEST(".printsize directive");
    PARSE(".printsize Tc");
    ASSERT_PARSED();
    const wl_ast_node_t *ps = child(program, 0);
    if (ps->type != WL_NODE_PRINTSIZE ||
        strcmp(ps->name, "Tc") != 0) {
        CLEANUP();
        FAIL("expected PRINTSIZE 'Tc'");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Parser: Simple Rules                                                     */
/* ======================================================================== */

static void test_parse_simple_rule(void) {
    TEST("simple rule: Tc(x,y) :- Arc(x,y).");
    PARSE("Tc(x, y) :- Arc(x, y).");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    if (rule->type != WL_NODE_RULE) {
        CLEANUP();
        FAIL("expected RULE node");
        return;
    }
    /* head is first child */
    const wl_ast_node_t *head = child(rule, 0);
    if (head->type != WL_NODE_HEAD ||
        strcmp(head->name, "Tc") != 0) {
        CLEANUP();
        FAIL("expected HEAD 'Tc'");
        return;
    }
    if (head->child_count != 2) {
        CLEANUP();
        FAIL("head should have 2 args");
        return;
    }
    /* body atom is second child */
    const wl_ast_node_t *body_atom = child(rule, 1);
    if (body_atom->type != WL_NODE_ATOM ||
        strcmp(body_atom->name, "Arc") != 0) {
        CLEANUP();
        FAIL("expected body ATOM 'Arc'");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_recursive_rule(void) {
    TEST("recursive rule: Tc(x,y) :- Tc(x,z), Arc(z,y).");
    PARSE("Tc(x, y) :- Tc(x, z), Arc(z, y).");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    /* head + 2 body predicates = 3 children */
    if (rule->child_count != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 children, got %u",
                 rule->child_count);
        CLEANUP();
        FAIL(buf);
        return;
    }
    const wl_ast_node_t *b0 = child(rule, 1);
    const wl_ast_node_t *b1 = child(rule, 2);
    if (strcmp(b0->name, "Tc") != 0 || strcmp(b1->name, "Arc") != 0) {
        CLEANUP();
        FAIL("body atoms should be Tc and Arc");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_rule_with_constants(void) {
    TEST("rule with integer constant");
    PARSE("r(x) :- a(x, 42).");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *body = child(rule, 1);
    const wl_ast_node_t *arg1 = child(body, 1);
    if (arg1->type != WL_NODE_INTEGER || arg1->int_value != 42) {
        CLEANUP();
        FAIL("expected integer constant 42");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_rule_with_string_constant(void) {
    TEST("rule with string constant");
    PARSE("r(x) :- name(x, \"hello\").");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *body = child(rule, 1);
    const wl_ast_node_t *arg1 = child(body, 1);
    if (arg1->type != WL_NODE_STRING ||
        strcmp(arg1->str_value, "hello") != 0) {
        CLEANUP();
        FAIL("expected string constant 'hello'");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_rule_with_wildcard(void) {
    TEST("rule with wildcard");
    PARSE("r(x) :- edge(x, _).");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *body = child(rule, 1);
    const wl_ast_node_t *arg1 = child(body, 1);
    if (arg1->type != WL_NODE_WILDCARD) {
        CLEANUP();
        FAIL("expected WILDCARD");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Parser: Negation                                                         */
/* ======================================================================== */

static void test_parse_negation(void) {
    TEST("negated atom in body");
    PARSE("r(x) :- a(x), !b(x).");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *neg = child(rule, 2);
    if (neg->type != WL_NODE_NEGATION) {
        CLEANUP();
        FAIL("expected NEGATION node");
        return;
    }
    const wl_ast_node_t *inner = child(neg, 0);
    if (inner->type != WL_NODE_ATOM ||
        strcmp(inner->name, "b") != 0) {
        CLEANUP();
        FAIL("negation should wrap atom 'b'");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Parser: Comparisons                                                      */
/* ======================================================================== */

static void test_parse_equality_constraint(void) {
    TEST("equality constraint x = y");
    PARSE("r(x) :- a(x, y), x = y.");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *cmp = child(rule, 2);
    if (cmp->type != WL_NODE_COMPARISON || cmp->cmp_op != WL_CMP_EQ) {
        CLEANUP();
        FAIL("expected COMPARISON with EQ");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_inequality_constraint(void) {
    TEST("inequality constraint x != y");
    PARSE("r(x) :- a(x, y), x != y.");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *cmp = child(rule, 2);
    if (cmp->type != WL_NODE_COMPARISON || cmp->cmp_op != WL_CMP_NEQ) {
        CLEANUP();
        FAIL("expected COMPARISON with NEQ");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_less_than(void) {
    TEST("less than constraint x < 10");
    PARSE("r(x) :- a(x), x < 10.");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *cmp = child(rule, 2);
    if (cmp->type != WL_NODE_COMPARISON || cmp->cmp_op != WL_CMP_LT) {
        CLEANUP();
        FAIL("expected COMPARISON with LT");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_greater_equal(void) {
    TEST("greater-equal constraint x >= y");
    PARSE("r(x) :- a(x, y), x >= y.");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *cmp = child(rule, 2);
    if (cmp->cmp_op != WL_CMP_GTE) {
        CLEANUP();
        FAIL("expected GTE");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Parser: Arithmetic in Comparisons                                        */
/* ======================================================================== */

static void test_parse_arithmetic_comparison(void) {
    TEST("arithmetic in comparison: x + 1 < y");
    PARSE("r(x) :- a(x, y), x + 1 < y.");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *cmp = child(rule, 2);
    if (cmp->type != WL_NODE_COMPARISON || cmp->cmp_op != WL_CMP_LT) {
        CLEANUP();
        FAIL("expected COMPARISON with LT");
        return;
    }
    /* Left side should be binary expr (x + 1) */
    const wl_ast_node_t *left = child(cmp, 0);
    if (left->type != WL_NODE_BINARY_EXPR ||
        left->arith_op != WL_ARITH_ADD) {
        CLEANUP();
        FAIL("left should be binary ADD");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Parser: Aggregation                                                      */
/* ======================================================================== */

static void test_parse_min_aggregate(void) {
    TEST("min aggregate in head");
    PARSE("sssp(x, min(d)) :- sssp2(x, d).");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *head = child(rule, 0);
    /* Second head arg should be aggregate */
    const wl_ast_node_t *agg = child(head, 1);
    if (agg->type != WL_NODE_AGGREGATE || agg->agg_fn != WL_AGG_MIN) {
        CLEANUP();
        FAIL("expected AGGREGATE MIN");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_count_aggregate(void) {
    TEST("count aggregate in head");
    PARSE("cnt(count(x)) :- node(x).");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *head = child(rule, 0);
    const wl_ast_node_t *agg = child(head, 0);
    if (agg->type != WL_NODE_AGGREGATE || agg->agg_fn != WL_AGG_COUNT) {
        CLEANUP();
        FAIL("expected AGGREGATE COUNT");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_sum_aggregate(void) {
    TEST("SUM aggregate (uppercase)");
    PARSE("total(SUM(w)) :- edge(_, _, w).");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *head = child(rule, 0);
    const wl_ast_node_t *agg = child(head, 0);
    if (agg->agg_fn != WL_AGG_SUM) {
        CLEANUP();
        FAIL("expected SUM");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_max_aggregate(void) {
    TEST("max aggregate");
    PARSE("biggest(max(v)) :- data(v).");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *head = child(rule, 0);
    const wl_ast_node_t *agg = child(head, 0);
    if (agg->agg_fn != WL_AGG_MAX) {
        CLEANUP();
        FAIL("expected MAX");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_avg_aggregate(void) {
    TEST("AVG aggregate");
    PARSE("avg_val(AVG(v)) :- data(v).");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *head = child(rule, 0);
    const wl_ast_node_t *agg = child(head, 0);
    if (agg->agg_fn != WL_AGG_AVG) {
        CLEANUP();
        FAIL("expected AVG");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_aggregate_with_arithmetic(void) {
    TEST("aggregate with arithmetic: min(d1 + d2)");
    PARSE("r(y, min(d1 + d2)) :- s(x, d1), a(x, y, d2).");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *head = child(rule, 0);
    const wl_ast_node_t *agg = child(head, 1);
    if (agg->type != WL_NODE_AGGREGATE || agg->agg_fn != WL_AGG_MIN) {
        CLEANUP();
        FAIL("expected AGGREGATE MIN");
        return;
    }
    const wl_ast_node_t *expr = child(agg, 0);
    if (expr->type != WL_NODE_BINARY_EXPR ||
        expr->arith_op != WL_ARITH_ADD) {
        CLEANUP();
        FAIL("aggregate body should be ADD");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_aggregate_with_constant(void) {
    TEST("aggregate with constant: min(0)");
    PARSE("r(x, min(0)) :- id(x).");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *head = child(rule, 0);
    const wl_ast_node_t *agg = child(head, 1);
    if (agg->type != WL_NODE_AGGREGATE) {
        CLEANUP();
        FAIL("expected AGGREGATE");
        return;
    }
    const wl_ast_node_t *inner = child(agg, 0);
    if (inner->type != WL_NODE_INTEGER || inner->int_value != 0) {
        CLEANUP();
        FAIL("aggregate body should be integer 0");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Parser: Boolean Predicates                                               */
/* ======================================================================== */

static void test_parse_boolean_true(void) {
    TEST("boolean True predicate in body");
    PARSE("r(x) :- a(x), True.");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *bp = child(rule, 2);
    if (bp->type != WL_NODE_BOOLEAN || bp->bool_value != true) {
        CLEANUP();
        FAIL("expected BOOLEAN True");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_boolean_false(void) {
    TEST("boolean False predicate in body");
    PARSE("r(x) :- a(x), False.");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *bp = child(rule, 2);
    if (bp->type != WL_NODE_BOOLEAN || bp->bool_value != false) {
        CLEANUP();
        FAIL("expected BOOLEAN False");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Parser: Arithmetic in Head                                               */
/* ======================================================================== */

static void test_parse_arithmetic_head(void) {
    TEST("arithmetic expression in head");
    PARSE("r(x + 1) :- a(x).");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *head = child(rule, 0);
    const wl_ast_node_t *arg0 = child(head, 0);
    if (arg0->type != WL_NODE_BINARY_EXPR ||
        arg0->arith_op != WL_ARITH_ADD) {
        CLEANUP();
        FAIL("head arg should be ADD expression");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Parser: .plan Optimization Marker                                        */
/* ======================================================================== */

static void test_parse_plan_marker(void) {
    TEST(".plan optimization marker on rule");
    PARSE("r(x) :- a(x). .plan");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    if (!rule->is_planning) {
        CLEANUP();
        FAIL("expected is_planning=true");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_rule_without_plan(void) {
    TEST("rule without .plan has is_planning=false");
    PARSE("r(x) :- a(x).");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    if (rule->is_planning) {
        CLEANUP();
        FAIL("expected is_planning=false");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Parser: Complete Programs (Real FlowLog Examples)                        */
/* ======================================================================== */

static void test_parse_transitive_closure(void) {
    TEST("transitive closure program");
    PARSE(
        ".decl Arc(x: int32, y: int32)\n"
        ".input Arc(IO=\"file\", filename=\"Arc.csv\", delimiter=\",\")\n"
        "\n"
        ".decl Tc(x: int32, y: int32)\n"
        "\n"
        "Tc(x, y) :- Arc(x, y).\n"
        "Tc(x, y) :- Tc(x, z), Arc(z, y).\n"
        "\n"
        ".printsize Tc\n"
    );
    ASSERT_PARSED();
    /* Expected: .decl Arc, .input Arc, .decl Tc, rule1, rule2, .printsize */
    if (program->child_count != 6) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 6 children, got %u",
                 program->child_count);
        CLEANUP();
        FAIL(buf);
        return;
    }
    /* Check structure */
    if (child(program, 0)->type != WL_NODE_DECL) {
        CLEANUP(); FAIL("child 0 should be DECL"); return;
    }
    if (child(program, 1)->type != WL_NODE_INPUT) {
        CLEANUP(); FAIL("child 1 should be INPUT"); return;
    }
    if (child(program, 2)->type != WL_NODE_DECL) {
        CLEANUP(); FAIL("child 2 should be DECL"); return;
    }
    if (child(program, 3)->type != WL_NODE_RULE) {
        CLEANUP(); FAIL("child 3 should be RULE"); return;
    }
    if (child(program, 4)->type != WL_NODE_RULE) {
        CLEANUP(); FAIL("child 4 should be RULE"); return;
    }
    if (child(program, 5)->type != WL_NODE_PRINTSIZE) {
        CLEANUP(); FAIL("child 5 should be PRINTSIZE"); return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_reachability(void) {
    TEST("reachability program");
    PARSE(
        ".decl Source(id: int32)\n"
        ".input Source(IO=\"file\", filename=\"Source.csv\", delimiter=\",\")\n"
        ".decl Arc(x: int32, y: int32)\n"
        ".input Arc(IO=\"file\", filename=\"Arc.csv\", delimiter=\",\")\n"
        "\n"
        ".decl Reach(id: int32)\n"
        "\n"
        "Reach(y) :- Source(y).\n"
        "Reach(y) :- Reach(x), Arc(x, y).\n"
        "\n"
        ".printsize Reach\n"
    );
    ASSERT_PARSED();
    /* .decl Source, .input Source, .decl Arc, .input Arc,
       .decl Reach, rule1, rule2, .printsize Reach = 8 */
    if (program->child_count != 8) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 8 children, got %u",
                 program->child_count);
        CLEANUP();
        FAIL(buf);
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_sssp(void) {
    TEST("SSSP program with aggregation");
    PARSE(
        ".decl arc(src: int32, dest: int32, weight: int32)\n"
        ".decl id(src: int32)\n"
        ".decl sssp2(x: int32, y: int32)\n"
        ".decl sssp(x: int32, y: int32)\n"
        "\n"
        ".input arc(IO=\"file\", filename=\"Arc.csv\", delimiter=\",\")\n"
        ".input id(IO=\"file\", filename=\"Id.csv\", delimiter=\",\")\n"
        "\n"
        "sssp2(x, min(0)) :- id(x).\n"
        "sssp2(y, min(d1 + d2)) :- sssp2(x, d1), arc(x, y, d2).\n"
        "sssp(x, min(d)) :- sssp2(x, d).\n"
        "\n"
        ".printsize sssp\n"
    );
    ASSERT_PARSED();
    /* 4 decls + 2 inputs + 3 rules + 1 printsize = 10 */
    if (program->child_count != 10) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 10 children, got %u",
                 program->child_count);
        CLEANUP();
        FAIL(buf);
        return;
    }
    /* Check that the second rule has an aggregate with arithmetic */
    const wl_ast_node_t *rule2 = child(program, 7);
    if (rule2->type != WL_NODE_RULE) {
        CLEANUP();
        FAIL("child 7 should be RULE");
        return;
    }
    const wl_ast_node_t *head = child(rule2, 0);
    const wl_ast_node_t *agg = child(head, 1);
    if (agg->type != WL_NODE_AGGREGATE || agg->agg_fn != WL_AGG_MIN) {
        CLEANUP();
        FAIL("head arg 1 should be min aggregate");
        return;
    }
    const wl_ast_node_t *arith = child(agg, 0);
    if (arith->type != WL_NODE_BINARY_EXPR ||
        arith->arith_op != WL_ARITH_ADD) {
        CLEANUP();
        FAIL("aggregate should contain d1 + d2");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_negation_program(void) {
    TEST("program with negation");
    PARSE(
        ".decl edge(x: int32, y: int32)\n"
        ".decl node(x: int32)\n"
        ".decl isolated(x: int32)\n"
        "\n"
        "isolated(x) :- node(x), !edge(x, _).\n"
        "\n"
        ".output isolated\n"
    );
    ASSERT_PARSED();
    /* 3 decls + 1 rule + 1 output = 5 */
    if (program->child_count != 5) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 5 children, got %u",
                 program->child_count);
        CLEANUP();
        FAIL(buf);
        return;
    }
    const wl_ast_node_t *rule = child(program, 3);
    /* head + 2 body predicates (node, !edge) */
    if (rule->child_count != 3) {
        CLEANUP();
        FAIL("rule should have 3 children (head + 2 body)");
        return;
    }
    const wl_ast_node_t *neg = child(rule, 2);
    if (neg->type != WL_NODE_NEGATION) {
        CLEANUP();
        FAIL("second body pred should be NEGATION");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_comparison_program(void) {
    TEST("program with comparisons");
    PARSE(
        ".decl edge(x: int32, y: int32)\n"
        ".decl sg(x: int32, y: int32)\n"
        "\n"
        "sg(x, y) :- edge(z, x), edge(z, y), x != y.\n"
        "\n"
        ".printsize sg\n"
    );
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 2);
    if (rule->type != WL_NODE_RULE) {
        CLEANUP();
        FAIL("expected RULE");
        return;
    }
    /* head + 3 body predicates */
    if (rule->child_count != 4) {
        CLEANUP();
        FAIL("rule should have 4 children");
        return;
    }
    const wl_ast_node_t *cmp = child(rule, 3);
    if (cmp->type != WL_NODE_COMPARISON || cmp->cmp_op != WL_CMP_NEQ) {
        CLEANUP();
        FAIL("third body pred should be COMPARISON NEQ");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Parser: Error Cases                                                      */
/* ======================================================================== */

static void test_parse_error_missing_horn(void) {
    TEST("error: missing :- in rule");
    PARSE("r(x) a(x).");
    if (program != NULL) {
        CLEANUP();
        FAIL("expected parse failure");
        return;
    }
    PASS();
}

static void test_parse_error_missing_dot(void) {
    TEST("error: missing . at end of rule");
    PARSE("r(x) :- a(x)");
    if (program != NULL) {
        CLEANUP();
        FAIL("expected parse failure");
        return;
    }
    PASS();
}

static void test_parse_error_missing_rparen(void) {
    TEST("error: missing ) in declaration");
    PARSE(".decl Arc(x: int32");
    if (program != NULL) {
        CLEANUP();
        FAIL("expected parse failure");
        return;
    }
    PASS();
}

static void test_parse_error_invalid_type(void) {
    TEST("error: invalid type name");
    PARSE(".decl Foo(x: float32)");
    if (program != NULL) {
        CLEANUP();
        FAIL("expected parse failure");
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Parser: Multiple Arithmetic Operators                                    */
/* ======================================================================== */

static void test_parse_chained_arithmetic(void) {
    TEST("chained arithmetic: a + b * c");
    /* FlowLog arithmetic is left-associative flat: factor (op factor)* */
    PARSE("r(x + y + z) :- a(x, y, z).");
    ASSERT_PARSED();
    const wl_ast_node_t *rule = child(program, 0);
    const wl_ast_node_t *head = child(rule, 0);
    const wl_ast_node_t *expr = child(head, 0);
    /* Should be binary tree: (x + y) + z */
    if (expr->type != WL_NODE_BINARY_EXPR) {
        CLEANUP();
        FAIL("expected BINARY_EXPR");
        return;
    }
    CLEANUP();
    PASS();
}

static void test_parse_all_arithmetic_ops(void) {
    TEST("all arithmetic operators in comparisons");
    PARSE(
        "r(x) :- a(x, y), x + y > 0.\n"
        "r(x) :- a(x, y), x - y < 0.\n"
        "r(x) :- a(x, y), x * y = 0.\n"
        "r(x) :- a(x, y), x / y >= 1.\n"
        "r(x) :- a(x, y), x % y <= 2.\n"
    );
    ASSERT_PARSED();
    if (program->child_count != 5) {
        CLEANUP();
        FAIL("expected 5 rules");
        return;
    }
    CLEANUP();
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int main(void) {
    printf("=== wirelog Parser Tests ===\n\n");

    printf("--- Empty/Minimal ---\n");
    test_parse_empty();
    test_parse_comments_only();

    printf("\n--- Declarations ---\n");
    test_parse_simple_decl();
    test_parse_decl_int64();
    test_parse_decl_string_type();
    test_parse_decl_empty_params();
    test_parse_decl_three_attrs();

    printf("\n--- Directives ---\n");
    test_parse_input_directive();
    test_parse_output_directive();
    test_parse_printsize_directive();

    printf("\n--- Simple Rules ---\n");
    test_parse_simple_rule();
    test_parse_recursive_rule();
    test_parse_rule_with_constants();
    test_parse_rule_with_string_constant();
    test_parse_rule_with_wildcard();

    printf("\n--- Negation ---\n");
    test_parse_negation();

    printf("\n--- Comparisons ---\n");
    test_parse_equality_constraint();
    test_parse_inequality_constraint();
    test_parse_less_than();
    test_parse_greater_equal();

    printf("\n--- Arithmetic ---\n");
    test_parse_arithmetic_comparison();
    test_parse_arithmetic_head();
    test_parse_chained_arithmetic();
    test_parse_all_arithmetic_ops();

    printf("\n--- Aggregation ---\n");
    test_parse_min_aggregate();
    test_parse_count_aggregate();
    test_parse_sum_aggregate();
    test_parse_max_aggregate();
    test_parse_avg_aggregate();
    test_parse_aggregate_with_arithmetic();
    test_parse_aggregate_with_constant();

    printf("\n--- Boolean Predicates ---\n");
    test_parse_boolean_true();
    test_parse_boolean_false();

    printf("\n--- Plan Marker ---\n");
    test_parse_plan_marker();
    test_parse_rule_without_plan();

    printf("\n--- Complete Programs ---\n");
    test_parse_transitive_closure();
    test_parse_reachability();
    test_parse_sssp();
    test_parse_negation_program();
    test_parse_comparison_program();

    printf("\n--- Error Cases ---\n");
    test_parse_error_missing_horn();
    test_parse_error_missing_dot();
    test_parse_error_missing_rparen();
    test_parse_error_invalid_type();

    printf("\n=== Results: %d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf(" ===\n");

    return tests_failed > 0 ? 1 : 0;
}
