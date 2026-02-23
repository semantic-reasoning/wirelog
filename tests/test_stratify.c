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

    /* Edge: r -> a (but a is EDB so may not appear as edge target node,
       however the edge should still be recorded with the EDB relation) */
    if (g->edge_count < 1) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected >= 1 edge, got %u", g->edge_count);
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL(buf);
        return;
    }

    if (g->edges[0].type != WL_DEP_POSITIVE) {
        wl_dep_graph_free(g);
        wl_program_free(prog);
        FAIL("edge type should be POSITIVE");
        return;
    }

    wl_dep_graph_free(g);
    wl_program_free(prog);
    PASS();
}

static void
test_dep_graph_negation(void)
{
    TEST("Dep graph: r(x) :- a(x), !b(x). -> POSITIVE + NEGATION");

    struct wirelog_program *prog = make_full_program(".decl a(x: int32)\n"
                                                     ".decl b(x: int32)\n"
                                                     ".decl r(x: int32)\n"
                                                     "r(x) :- a(x), !b(x).\n");

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

    /* Should have at least 2 edges: r->a (POSITIVE), r->b (NEGATION) */
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

    /* Edges: b->a (POSITIVE), c->b (POSITIVE) = at least 2 */
    if (g->edge_count < 2) {
        char buf[100];
        snprintf(buf, sizeof(buf), "expected >= 2 edges, got %u",
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

    printf("\n=== Results: %d passed, %d failed, %d total ===\n\n",
           tests_passed, tests_failed, tests_run);

    return tests_failed > 0 ? 1 : 0;
}
