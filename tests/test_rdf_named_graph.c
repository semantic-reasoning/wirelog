/*
 * test_rdf_named_graph.c - Integration tests for RDF named-graph semantics
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * End-to-end tests covering parse -> plan -> session -> step -> snapshot
 * for the __graph_id column and __graph_metadata auto-created relation.
 *
 * Issue #535: RDF Named-Graph support
 *
 *   TC-1  test_graph_id_column_stored_in_session
 *           EDB-only .decl with __graph_id: has_graph_column==true,
 *           graph_col_idx==2 on the col_rel_t.
 *
 *   TC-2  test_single_graph_facts_visible
 *           Three triples all in graph 42 project through a rule that
 *           discards the graph column; result has 3 rows.
 *
 *   TC-3  test_multi_graph_all_facts_visible_without_filter
 *           Triples spread across three graphs: all 3 appear in result
 *           (graph_id is not an implicit filter).
 *
 *   TC-4  test_graph_metadata_filter_by_tenant
 *           Join triple against __graph_metadata on graph_id; only rows
 *           whose tenant==99 reach the "trusted" relation.
 *
 *   TC-5  test_cross_graph_join_on_non_graph_column
 *           Two relations with independent __graph_id columns; join on
 *           a shared non-graph key produces the expected result count.
 *
 *   TC-6  test_backward_compat_relation_without_graph_column
 *           Classic TC (edge + tc, no __graph_id) still works and
 *           has_graph_column == false on both relations.
 */

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test harness                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                              \
        do {                                        \
            tests_run++;                            \
            printf("  [%d] %s", tests_run, name);   \
        } while (0)

#define PASS()                                  \
        do {                                        \
            tests_passed++;                         \
            printf(" ... PASS\n");                  \
        } while (0)

#define FAIL(msg)                               \
        do {                                        \
            tests_failed++;                         \
            printf(" ... FAIL: %s\n", (msg));       \
            goto cleanup;                           \
        } while (0)

#define ASSERT(cond, msg)                       \
        do {                                        \
            if (!(cond)) {                          \
                FAIL(msg);                          \
            }                                       \
        } while (0)

/* ======================================================================== */
/* Session helpers                                                          */
/* ======================================================================== */

/*
 * Build a session from a Datalog source string.  Applies all standard
 * optimizer passes (fusion, jpp, sip) before plan generation.
 * Returns the col_session cast; caller frees via cleanup_session().
 */
static wl_col_session_t *
make_session(const char *src, wl_plan_t **plan_out,
    wirelog_program_t **prog_out)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);

    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;

    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return NULL;
    }

    wl_session_t *sess = NULL;

    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return NULL;
    }

    *plan_out = plan;
    *prog_out = prog;
    return COL_SESSION(sess);
}

static void
cleanup_session(wl_col_session_t *sess, wl_plan_t *plan,
    wirelog_program_t *prog)
{
    if (sess)
        wl_session_destroy(&sess->base);
    if (plan)
        wl_plan_free(plan);
    if (prog)
        wirelog_program_free(prog);
}

/* ======================================================================== */
/* TC-1: graph_id column flag stored on col_rel_t after session create     */
/* ======================================================================== */

static void
test_graph_id_column_stored_in_session(void)
{
    TEST("TC-1: __graph_id column stored in session col_rel_t");

    wl_plan_t         *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t  *sess = NULL;

    const char *src =
        ".decl triple(s: int64, p: int64, __graph_id: int64)\n";

    sess = make_session(src, &plan, &prog);
    ASSERT(sess != NULL, "make_session failed");

    col_rel_t *r = session_find_rel(sess, "triple");
    ASSERT(r != NULL, "triple relation not found in session");
    ASSERT(r->has_graph_column == true,
        "has_graph_column should be true for __graph_id decl");
    ASSERT(r->graph_col_idx == 2,
        "graph_col_idx should be 2 (third column)");

    PASS();
cleanup:
    cleanup_session(sess, plan, prog);
}

/* ======================================================================== */
/* TC-2: single-graph facts all visible in derived relation                */
/* ======================================================================== */

static void
test_single_graph_facts_visible(void)
{
    TEST("TC-2: single-graph triples project into result (3 rows)");

    wl_plan_t         *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t  *sess = NULL;

    const char *src =
        ".decl triple(s: int64, p: int64, __graph_id: int64)\n"
        ".decl result(s: int64, p: int64)\n"
        "result(s, p) :- triple(s, p, g).\n";

    sess = make_session(src, &plan, &prog);
    ASSERT(sess != NULL, "make_session failed");

    /* Insert 3 triples all in graph 42 */
    int64_t triples[] = {
        1, 10, 42,
        2, 20, 42,
        3, 30, 42,
    };
    int rc = wl_session_insert(&sess->base, "triple", triples, 3, 3);
    ASSERT(rc == 0, "insert failed");

    rc = wl_session_step(&sess->base);
    ASSERT(rc == 0, "session_step failed");

    col_rel_t *result = session_find_rel(sess, "result");
    ASSERT(result != NULL, "result relation not found");

    char msg[64];
    snprintf(msg, sizeof(msg), "expected 3 rows, got %u", result->nrows);
    ASSERT(result->nrows == 3, msg);

    PASS();
cleanup:
    cleanup_session(sess, plan, prog);
}

/* ======================================================================== */
/* TC-3: multi-graph facts all visible without implicit graph filter       */
/* ======================================================================== */

static void
test_multi_graph_all_facts_visible_without_filter(void)
{
    TEST("TC-3: triples in different graphs all project (3 rows, no gating)");

    wl_plan_t         *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t  *sess = NULL;

    const char *src =
        ".decl triple(s: int64, p: int64, __graph_id: int64)\n"
        ".decl result(s: int64, p: int64)\n"
        "result(s, p) :- triple(s, p, g).\n";

    sess = make_session(src, &plan, &prog);
    ASSERT(sess != NULL, "make_session failed");

    /* Three triples spread across graphs 42, 43, 44 */
    int64_t triples[] = {
        1, 10, 42,
        2, 20, 43,
        3, 30, 44,
    };
    int rc = wl_session_insert(&sess->base, "triple", triples, 3, 3);
    ASSERT(rc == 0, "insert failed");

    rc = wl_session_step(&sess->base);
    ASSERT(rc == 0, "session_step failed");

    col_rel_t *result = session_find_rel(sess, "result");
    ASSERT(result != NULL, "result relation not found");

    char msg[64];
    snprintf(msg, sizeof(msg), "expected 3 rows, got %u", result->nrows);
    ASSERT(result->nrows == 3, msg);

    PASS();
cleanup:
    cleanup_session(sess, plan, prog);
}

/* ======================================================================== */
/* TC-4: __graph_metadata tenant filter selects subset of triples          */
/* ======================================================================== */

static void
test_graph_metadata_filter_by_tenant(void)
{
    TEST("TC-4: join on __graph_metadata tenant column filters to 2 rows");

    wl_plan_t         *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t  *sess = NULL;

    /*
     * trusted(s,p) derives from triples whose graph_id appears in
     * __graph_metadata with tenant==99.
     * Use distinct dummy variable names (t0..t3) for metadata columns
     * other than the join key and tenant so the parser does not trip
     * on repeated wildcards.
     */
    const char *src =
        ".decl triple(s: int64, p: int64, __graph_id: int64)\n"
        ".decl __graph_metadata(graph_id: int64, tenant: int64,"
        " timestamp: int64, location: int64, risk: int64,"
        " description: int64)\n"
        ".decl trusted(s: int64, p: int64)\n"
        "trusted(s, p) :- triple(s, p, g),"
        " __graph_metadata(g, 99, t0, t1, t2, t3).\n";

    sess = make_session(src, &plan, &prog);
    ASSERT(sess != NULL, "make_session failed");

    /* Triples in graphs 1, 2, 3 */
    int64_t triples[] = {
        1, 10, 1,
        2, 20, 2,
        3, 30, 3,
    };
    int rc = wl_session_insert(&sess->base, "triple", triples, 3, 3);
    ASSERT(rc == 0, "insert triples failed");

    /*
     * Graph metadata: graph 1 => tenant 99, graph 2 => tenant 77,
     *                 graph 3 => tenant 99.
     * Expected: rows for graphs 1 and 3 match tenant==99, so trusted
     * should contain 2 rows.
     */
    int64_t meta[] = {
        1, 99, 0, 0, 0, 0,
        2, 77, 0, 0, 0, 0,
        3, 99, 0, 0, 0, 0,
    };
    rc = wl_session_insert(&sess->base, "__graph_metadata", meta, 3, 6);
    ASSERT(rc == 0, "insert __graph_metadata failed");

    rc = wl_session_step(&sess->base);
    ASSERT(rc == 0, "session_step failed");

    col_rel_t *trusted = session_find_rel(sess, "trusted");
    ASSERT(trusted != NULL, "trusted relation not found");

    char msg[64];
    snprintf(msg, sizeof(msg), "expected 2 rows, got %u", trusted->nrows);
    ASSERT(trusted->nrows == 2, msg);

    PASS();
cleanup:
    cleanup_session(sess, plan, prog);
}

/* ======================================================================== */
/* TC-5: cross-graph join on non-graph key column                          */
/* ======================================================================== */

static void
test_cross_graph_join_on_non_graph_column(void)
{
    TEST("TC-5: cross-graph join on key column produces 2 rows");

    wl_plan_t         *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t  *sess = NULL;

    /*
     * joined(k,v,i) :- left_rel(k,v,g1), right_rel(k,i,g2).
     * g1 and g2 are distinct variables so the graph columns don't
     * constrain the join — only k (the first column) does.
     */
    const char *src =
        ".decl left_rel(key: int64, val: int64, __graph_id: int64)\n"
        ".decl right_rel(key: int64, info: int64, __graph_id: int64)\n"
        ".decl joined(key: int64, val: int64, info: int64)\n"
        "joined(k, v, i) :- left_rel(k, v, g1), right_rel(k, i, g2).\n";

    sess = make_session(src, &plan, &prog);
    ASSERT(sess != NULL, "make_session failed");

    /* left rows: keys 10 and 20, in different graphs */
    int64_t left_rows[] = {
        10, 100, 42,
        20, 200, 43,
    };
    int rc = wl_session_insert(&sess->base, "left_rel", left_rows, 2, 3);
    ASSERT(rc == 0, "insert left_rel failed");

    /* right rows: same keys 10 and 20, in yet other graphs */
    int64_t right_rows[] = {
        10, 999, 50,
        20, 888, 51,
    };
    rc = wl_session_insert(&sess->base, "right_rel", right_rows, 2, 3);
    ASSERT(rc == 0, "insert right_rel failed");

    rc = wl_session_step(&sess->base);
    ASSERT(rc == 0, "session_step failed");

    col_rel_t *joined = session_find_rel(sess, "joined");
    ASSERT(joined != NULL, "joined relation not found");

    char msg[64];
    snprintf(msg, sizeof(msg), "expected 2 rows, got %u", joined->nrows);
    ASSERT(joined->nrows == 2, msg);

    PASS();
cleanup:
    cleanup_session(sess, plan, prog);
}

/* ======================================================================== */
/* TC-6: backward-compat — classic TC without __graph_id                   */
/* ======================================================================== */

static void
test_backward_compat_relation_without_graph_column(void)
{
    TEST("TC-6: classic TC (no __graph_id) works; has_graph_column==false");

    wl_plan_t         *plan = NULL;
    wirelog_program_t *prog = NULL;
    wl_col_session_t  *sess = NULL;

    const char *src =
        ".decl edge(x: int64, y: int64)\n"
        ".decl tc(x: int64, y: int64)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    sess = make_session(src, &plan, &prog);
    ASSERT(sess != NULL, "make_session failed");

    /* edge: 1->2, 2->3, 3->4 */
    int64_t edges[] = {
        1, 2,
        2, 3,
        3, 4,
    };
    int rc = wl_session_insert(&sess->base, "edge", edges, 3, 2);
    ASSERT(rc == 0, "insert edges failed");

    rc = wl_session_step(&sess->base);
    ASSERT(rc == 0, "session_step failed");

    col_rel_t *tc = session_find_rel(sess, "tc");
    ASSERT(tc != NULL, "tc relation not found");

    /* TC on 1->2->3->4: 6 pairs */
    char msg[64];
    snprintf(msg, sizeof(msg), "expected 6 TC rows, got %u", tc->nrows);
    ASSERT(tc->nrows == 6, msg);

    /* Neither edge nor tc should have has_graph_column set */
    col_rel_t *edge_rel = session_find_rel(sess, "edge");
    ASSERT(edge_rel != NULL, "edge relation not found");
    ASSERT(edge_rel->has_graph_column == false,
        "edge.has_graph_column should be false");
    ASSERT(tc->has_graph_column == false,
        "tc.has_graph_column should be false");

    PASS();
cleanup:
    cleanup_session(sess, plan, prog);
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_rdf_named_graph (Issue #535)\n");
    printf("==================================\n");

    test_graph_id_column_stored_in_session();
    test_single_graph_facts_visible();
    test_multi_graph_all_facts_visible_without_filter();
    test_graph_metadata_filter_by_tenant();
    test_cross_graph_join_on_non_graph_column();
    test_backward_compat_relation_without_graph_column();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
