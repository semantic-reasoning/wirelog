/*
 * test_stratify.c - wirelog Stratification & SCC Test Suite
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests written first (TDD) before stratification implementation.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/parser/parser.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/ir/stratify.h"
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

/* Helper: parse string, build program, convert rules, merge unions */
static struct wirelog_program *
make_full_program(const char *source)
{
    char errbuf[512] = { 0 };
    wl_ast_node_t *ast = wl_parse_string(source, errbuf, sizeof(errbuf));
    if (!ast)
        return NULL;

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

    if (wl_program_convert_rules(prog, ast) != 0) {
        wl_program_free(prog);
        return NULL;
    }

    if (wl_program_merge_unions(prog) != 0) {
        wl_program_free(prog);
        return NULL;
    }

    return prog;
}

/* ======================================================================== */
/* Dependency Graph Tests                                                   */
/* ======================================================================== */

static void
test_dep_graph_simple(void)
{
    TEST("Dep graph: r(x) :- a(x). -> 1 POSITIVE edge");

    struct wirelog_program *prog = make_full_program(".decl a(x: int32)\n"
                                                     ".decl r(x: int32)\n"
                                                     "r(x) :- a(x).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wl_dep_graph_t *g = wl_dep_graph_build(prog);
    if (!g) {
        wl_program_free(prog);
        FAIL("dep graph is NULL");
        return;
    }

    /* r is IDB (has rule), a is EDB (no rule) */
    if (g->relation_count != 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 1 IDB relation, got %u",
                 g->relation_count);
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL(buf);
        return;
    }

    /* r depends on a (EDB), so no edges between IDB nodes.
       Edge count should be 0 since a is not in the graph. */
    if (g->edge_count != 0) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 0 edges (EDB dep), got %u",
                 g->edge_count);
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL(buf);
        return;
    }

    wl_dep_graph_free(g);
    wl_program_free(prog);
    PASS();
}

static void
test_dep_graph_negation(void)
{
    TEST("Dep graph: negation edge between IDB relations");

    /* r depends on p (POSITIVE) and negates q.
       p and q are also IDB so edges appear in graph. */
    struct wirelog_program *prog = make_full_program(".decl base(x: int32)\n"
                                                     ".decl p(x: int32)\n"
                                                     ".decl q(x: int32)\n"
                                                     ".decl r(x: int32)\n"
                                                     "p(x) :- base(x).\n"
                                                     "q(x) :- base(x).\n"
                                                     "r(x) :- p(x), !q(x).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wl_dep_graph_t *g = wl_dep_graph_build(prog);
    if (!g) {
        wl_program_free(prog);
        FAIL("dep graph is NULL");
        return;
    }

    /* p, q, r are IDB (3 relations) */
    if (g->relation_count != 3) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 3 IDB relations, got %u",
                 g->relation_count);
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL(buf);
        return;
    }

    /* Should have: r->p (POSITIVE), r->q (NEGATION) = at least 2 edges */
    if (g->edge_count < 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected >= 2 edges, got %u",
                 g->edge_count);
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL(buf);
        return;
    }

    /* Find the NEGATION edge */
    bool found_neg = false;
    for (uint32_t i = 0; i < g->edge_count; i++) {
        if (g->edges[i].type == WL_DEP_NEGATION) {
            found_neg = true;
            break;
        }
    }

    if (!found_neg) {
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL("no NEGATION edge found");
        return;
    }

    wl_dep_graph_free(g);
    wl_program_free(prog);
    PASS();
}

static void
test_dep_graph_recursive(void)
{
    TEST("Dep graph: TC recursive -> self-loop edge");

    struct wirelog_program *prog
        = make_full_program(".decl Arc(x: int32, y: int32)\n"
                            ".decl Tc(x: int32, y: int32)\n"
                            "Tc(x, y) :- Arc(x, y).\n"
                            "Tc(x, y) :- Tc(x, z), Arc(z, y).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wl_dep_graph_t *g = wl_dep_graph_build(prog);
    if (!g) {
        wl_program_free(prog);
        FAIL("dep graph is NULL");
        return;
    }

    /* Tc is IDB. Should have self-loop: Tc -> Tc (from recursive rule) */
    bool found_self = false;
    for (uint32_t i = 0; i < g->edge_count; i++) {
        if (g->edges[i].from == g->edges[i].to
            && g->edges[i].type == WL_DEP_POSITIVE) {
            found_self = true;
            break;
        }
    }

    if (!found_self) {
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL("no self-loop edge found for Tc");
        return;
    }

    wl_dep_graph_free(g);
    wl_program_free(prog);
    PASS();
}

static void
test_dep_graph_multiple(void)
{
    TEST("Dep graph: multiple IDB relations, correct edge count");

    struct wirelog_program *prog = make_full_program(".decl a(x: int32)\n"
                                                     ".decl b(x: int32)\n"
                                                     ".decl c(x: int32)\n"
                                                     "b(x) :- a(x).\n"
                                                     "c(x) :- b(x).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wl_dep_graph_t *g = wl_dep_graph_build(prog);
    if (!g) {
        wl_program_free(prog);
        FAIL("dep graph is NULL");
        return;
    }

    /* b and c are IDB, a is EDB */
    if (g->relation_count != 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 2 IDB relations, got %u",
                 g->relation_count);
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL(buf);
        return;
    }

    /* Edges: c->b (POSITIVE, both IDB). b->a skipped (a is EDB).
       So exactly 1 edge between IDB nodes. */
    if (g->edge_count != 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 1 IDB-IDB edge, got %u",
                 g->edge_count);
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL(buf);
        return;
    }

    wl_dep_graph_free(g);
    wl_program_free(prog);
    PASS();
}

static void
test_dep_graph_empty(void)
{
    TEST("Dep graph: 0 rules -> NULL graph");

    struct wirelog_program *prog = make_full_program(".decl a(x: int32)\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wl_dep_graph_t *g = wl_dep_graph_build(prog);

    /* 0 rules = no IDB relations = NULL graph is acceptable */
    if (g != NULL) {
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL("expected NULL graph for 0-rule program");
        return;
    }

    wl_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* SCC Detection Tests                                                      */
/* ======================================================================== */

static void
test_scc_no_recursion(void)
{
    TEST("SCC: no recursion -> all singletons");

    /* b(x) :- a(x). c(x) :- b(x). No cycles = each in own SCC */
    struct wirelog_program *prog = make_full_program(".decl a(x: int32)\n"
                                                     ".decl b(x: int32)\n"
                                                     ".decl c(x: int32)\n"
                                                     "b(x) :- a(x).\n"
                                                     "c(x) :- b(x).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wl_dep_graph_t *g = wl_dep_graph_build(prog);
    if (!g) {
        wl_program_free(prog);
        FAIL("dep graph is NULL");
        return;
    }

    wl_scc_result_t *scc = wl_scc_detect(g);
    if (!scc) {
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL("SCC result is NULL");
        return;
    }

    /* 2 IDB relations, no cycles -> 2 SCCs (each singleton) */
    if (scc->scc_count != 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 2 SCCs, got %u", scc->scc_count);
        wl_scc_free(scc);
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL(buf);
        return;
    }

    /* b and c must be in different SCCs */
    if (scc->scc_id[0] == scc->scc_id[1]) {
        wl_scc_free(scc);
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL("b and c should be in different SCCs");
        return;
    }

    wl_scc_free(scc);
    wl_dep_graph_free(g);
    wl_program_free(prog);
    PASS();
}

static void
test_scc_direct_recursion(void)
{
    TEST("SCC: Tc recursive -> Tc in own SCC");

    struct wirelog_program *prog
        = make_full_program(".decl Arc(x: int32, y: int32)\n"
                            ".decl Tc(x: int32, y: int32)\n"
                            "Tc(x, y) :- Arc(x, y).\n"
                            "Tc(x, y) :- Tc(x, z), Arc(z, y).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wl_dep_graph_t *g = wl_dep_graph_build(prog);
    if (!g) {
        wl_program_free(prog);
        FAIL("dep graph is NULL");
        return;
    }

    wl_scc_result_t *scc = wl_scc_detect(g);
    if (!scc) {
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL("SCC result is NULL");
        return;
    }

    /* Tc self-loops -> 1 SCC containing Tc */
    if (scc->scc_count != 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 1 SCC, got %u", scc->scc_count);
        wl_scc_free(scc);
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL(buf);
        return;
    }

    wl_scc_free(scc);
    wl_dep_graph_free(g);
    wl_program_free(prog);
    PASS();
}

static void
test_scc_mutual_recursion(void)
{
    TEST("SCC: mutual recursion -> a,b in same SCC");

    /* a and b mutually depend on each other */
    struct wirelog_program *prog = make_full_program(".decl base(x: int32)\n"
                                                     ".decl a(x: int32)\n"
                                                     ".decl b(x: int32)\n"
                                                     "a(x) :- b(x).\n"
                                                     "b(x) :- a(x).\n"
                                                     "a(x) :- base(x).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wl_dep_graph_t *g = wl_dep_graph_build(prog);
    if (!g) {
        wl_program_free(prog);
        FAIL("dep graph is NULL");
        return;
    }

    wl_scc_result_t *scc = wl_scc_detect(g);
    if (!scc) {
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL("SCC result is NULL");
        return;
    }

    /* a and b form a cycle -> 1 SCC */
    if (scc->scc_count != 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 1 SCC, got %u", scc->scc_count);
        wl_scc_free(scc);
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL(buf);
        return;
    }

    /* Both a and b should be in same SCC */
    if (g->relation_count >= 2 && scc->scc_id[0] != scc->scc_id[1]) {
        wl_scc_free(scc);
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL("a and b should be in the same SCC");
        return;
    }

    wl_scc_free(scc);
    wl_dep_graph_free(g);
    wl_program_free(prog);
    PASS();
}

static void
test_scc_two_sccs(void)
{
    TEST("SCC: two separate cycles -> 2 SCCs");

    /* {a,b} cycle and {d,e} cycle, connected by c */
    struct wirelog_program *prog = make_full_program(".decl base(x: int32)\n"
                                                     ".decl a(x: int32)\n"
                                                     ".decl b(x: int32)\n"
                                                     ".decl c(x: int32)\n"
                                                     ".decl d(x: int32)\n"
                                                     ".decl e(x: int32)\n"
                                                     "a(x) :- b(x).\n"
                                                     "b(x) :- a(x).\n"
                                                     "a(x) :- base(x).\n"
                                                     "c(x) :- a(x).\n"
                                                     "d(x) :- e(x).\n"
                                                     "e(x) :- d(x).\n"
                                                     "d(x) :- c(x).\n");

    if (!prog) {
        FAIL("program is NULL");
        return;
    }

    wl_dep_graph_t *g = wl_dep_graph_build(prog);
    if (!g) {
        wl_program_free(prog);
        FAIL("dep graph is NULL");
        return;
    }

    wl_scc_result_t *scc = wl_scc_detect(g);
    if (!scc) {
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL("SCC result is NULL");
        return;
    }

    /* {a,b} + {c singleton} + {d,e} = 3 SCCs */
    if (scc->scc_count != 3) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 3 SCCs, got %u", scc->scc_count);
        wl_scc_free(scc);
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL(buf);
        return;
    }

    wl_scc_free(scc);
    wl_dep_graph_free(g);
    wl_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Stratification Integration Tests                                         */
/* ======================================================================== */

static void
test_stratify_simple(void)
{
    TEST("Stratify: simple EDB-only deps -> 1 stratum");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n"
                                                   ".decl r(x: int32)\n"
                                                   "r(x) :- a(x).\n",
                                                   &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    if (wirelog_program_get_stratum_count(prog) != 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 1 stratum, got %u",
                 wirelog_program_get_stratum_count(prog));
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    if (!wirelog_program_is_stratified(prog)) {
        wirelog_program_free(prog);
        FAIL("program should be stratified");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_stratify_recursive_tc(void)
{
    TEST("Stratify: TC recursive -> 1 stratum");

    wirelog_error_t err;
    wirelog_program_t *prog
        = wirelog_parse_string(".decl Arc(x: int32, y: int32)\n"
                               ".decl Tc(x: int32, y: int32)\n"
                               "Tc(x, y) :- Arc(x, y).\n"
                               "Tc(x, y) :- Tc(x, z), Arc(z, y).\n",
                               &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    if (wirelog_program_get_stratum_count(prog) != 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 1 stratum, got %u",
                 wirelog_program_get_stratum_count(prog));
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    const wirelog_stratum_t *s0 = wirelog_program_get_stratum(prog, 0);
    if (!s0 || s0->rule_count != 2) {
        wirelog_program_free(prog);
        FAIL("stratum 0 should have 2 rules");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_stratify_two_strata(void)
{
    TEST("Stratify: chain b->a, c->b -> 2 strata ordered");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n"
                                                   ".decl b(x: int32)\n"
                                                   ".decl c(x: int32)\n"
                                                   "b(x) :- a(x).\n"
                                                   "c(x) :- b(x).\n",
                                                   &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    if (wirelog_program_get_stratum_count(prog) != 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 2 strata, got %u",
                 wirelog_program_get_stratum_count(prog));
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    /* Stratum 0 should contain b (computed first, depends on EDB a).
       Stratum 1 should contain c (depends on IDB b). */
    const wirelog_stratum_t *s0 = wirelog_program_get_stratum(prog, 0);
    const wirelog_stratum_t *s1 = wirelog_program_get_stratum(prog, 1);

    if (!s0 || !s1) {
        wirelog_program_free(prog);
        FAIL("strata should not be NULL");
        return;
    }

    if (s0->rule_count != 1 || s1->rule_count != 1) {
        wirelog_program_free(prog);
        FAIL("each stratum should have 1 rule");
        return;
    }

    /* b must be in earlier stratum than c */
    bool b_first = (strcmp(s0->rule_names[0], "b") == 0
                    && strcmp(s1->rule_names[0], "c") == 0);
    if (!b_first) {
        char buf[200];
        snprintf(buf, sizeof(buf),
                 "expected b in s0, c in s1; got s0=%s, s1=%s",
                 s0->rule_names[0], s1->rule_names[0]);
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_stratify_unstratifiable(void)
{
    TEST("Stratify: negation cycle -> parse fails");

    /* p depends on !q, q depends on !p -> negation cycle within SCC */
    wirelog_error_t err = WIRELOG_OK;
    wirelog_program_t *prog = wirelog_parse_string(".decl base(x: int32)\n"
                                                   ".decl p(x: int32)\n"
                                                   ".decl q(x: int32)\n"
                                                   "p(x) :- base(x), !q(x).\n"
                                                   "q(x) :- base(x), !p(x).\n",
                                                   &err);

    if (prog != NULL) {
        wirelog_program_free(prog);
        FAIL("expected parse to fail for unstratifiable program");
        return;
    }

    if (err != WIRELOG_ERR_PARSE) {
        FAIL("error should be WIRELOG_ERR_PARSE");
        return;
    }

    PASS();
}

static void
test_stratify_multiple_sccs(void)
{
    TEST("Stratify: 3 SCCs correct ordering");

    /* {a,b} mutual recursion, c depends on a, {d,e} mutual recursion depends on c */
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl base(x: int32)\n"
                                                   ".decl a(x: int32)\n"
                                                   ".decl b(x: int32)\n"
                                                   ".decl c(x: int32)\n"
                                                   ".decl d(x: int32)\n"
                                                   ".decl e(x: int32)\n"
                                                   "a(x) :- b(x).\n"
                                                   "b(x) :- a(x).\n"
                                                   "a(x) :- base(x).\n"
                                                   "c(x) :- a(x).\n"
                                                   "d(x) :- e(x).\n"
                                                   "e(x) :- d(x).\n"
                                                   "d(x) :- c(x).\n",
                                                   &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    if (wirelog_program_get_stratum_count(prog) != 3) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected 3 strata, got %u",
                 wirelog_program_get_stratum_count(prog));
        wirelog_program_free(prog);
        FAIL(buf);
        return;
    }

    if (!wirelog_program_is_stratified(prog)) {
        wirelog_program_free(prog);
        FAIL("program should be stratified");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

static void
test_stratify_no_rules(void)
{
    TEST("Stratify: 0 rules -> 1 stratum (>= 1 contract)");

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(".decl a(x: int32)\n", &err);

    if (!prog) {
        FAIL("parse returned NULL");
        return;
    }

    if (wirelog_program_get_stratum_count(prog) < 1) {
        wirelog_program_free(prog);
        FAIL("stratum_count should be >= 1");
        return;
    }

    wirelog_program_free(prog);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("\n=== wirelog Stratification Tests ===\n\n");

    /* Dependency graph */
    test_dep_graph_simple();
    test_dep_graph_negation();
    test_dep_graph_recursive();
    test_dep_graph_multiple();
    test_dep_graph_empty();

    /* SCC detection */
    test_scc_no_recursion();
    test_scc_direct_recursion();
    test_scc_mutual_recursion();
    test_scc_two_sccs();

    /* Stratification integration */
    test_stratify_simple();
    test_stratify_recursive_tc();
    test_stratify_two_strata();
    test_stratify_unstratifiable();
    test_stratify_multiple_sccs();
    test_stratify_no_rules();

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
