/*
 * test_rule_level_frontier.c - Integration tests for rule-level frontier
 * correctness (Phase 4, US-4-006)
 *
 * Tests that rule-level frontier tracking correctly identifies affected rules,
 * preserves unaffected rule frontiers across incremental inserts, and produces
 * output identical to a full-reset baseline.
 *
 * Test cases:
 *   1. Rule A depends on inserted fact -> affected; Rule B unaffected -> not set
 *   2. Rule A->Rule B dependency chain -> transitive detection marks both
 *   3. Affected rule frontier reset; unaffected stratum frontier preserved
 *   4. Output correctness vs baseline (no skip) after incremental insert
 *   5. Edge cases: empty delta and null insertion
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../wirelog/backend/columnar_nanoarrow.h"
#include "../wirelog/exec_plan.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog.h"

/* ======================================================================== */
/* TEST HARNESS MACROS                                                       */
/* ======================================================================== */

#define TEST(name)                       \
    do {                                 \
        printf("  [TEST] %-62s ", name); \
        fflush(stdout);                  \
    } while (0)

#define PASS              \
    do {                  \
        printf("PASS\n"); \
        tests_passed++;   \
    } while (0)

#define FAIL(msg)                  \
    do {                           \
        printf("FAIL: %s\n", msg); \
        tests_failed++;            \
    } while (0)

static int tests_passed = 0;
static int tests_failed = 0;

/* ======================================================================== */
/* PLAN BUILDER HELPERS                                                      */
/* ======================================================================== */

/*
 * Build a minimal wl_plan_op_t VARIABLE operator referencing rel_name.
 */
static wl_plan_op_t
make_var_op(const char *rel_name)
{
    wl_plan_op_t op;
    memset(&op, 0, sizeof(op));
    op.op = WL_PLAN_OP_VARIABLE;
    op.relation_name = rel_name;
    return op;
}

/*
 * Initialise a wl_plan_relation_t whose single operator is VARIABLE(dep_rel).
 * rel_name is the IDB head relation; op_storage provides backing for the op.
 */
static void
init_relation(wl_plan_relation_t *pr, const char *rel_name, const char *dep_rel,
              wl_plan_op_t *op_storage)
{
    op_storage[0] = make_var_op(dep_rel);
    pr->name = rel_name;
    pr->ops = op_storage;
    pr->op_count = 1;
}

/* ======================================================================== */
/* DATALOG PLAN BUILDER HELPERS                                              */
/* ======================================================================== */

/*
 * Build a wl_plan_t from a Datalog source string, applying fusion, JPP,
 * and SIP passes.  Returns NULL on failure.  Caller must wl_plan_free().
 */
static wl_plan_t *
build_plan(const char *src)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    if (rc != 0)
        return NULL;
    return plan;
}

/*
 * Return the index of the first recursive stratum, or UINT32_MAX if none.
 */
static uint32_t
find_recursive_stratum(const wl_plan_t *plan)
{
    for (uint32_t i = 0; i < plan->stratum_count; i++) {
        if (plan->strata[i].is_recursive)
            return i;
    }
    return UINT32_MAX;
}

/* ======================================================================== */
/* TUPLE COUNTING CALLBACK                                                   */
/* ======================================================================== */

static void
count_cb(const char *relation, const int64_t *row, uint32_t ncols,
         void *user_data)
{
    int64_t *count = (int64_t *)user_data;
    (*count)++;
    (void)relation;
    (void)row;
    (void)ncols;
}

/* ======================================================================== */
/* TC1: Direct dependency — Rule A affected, Rule B unaffected               */
/* ======================================================================== */

/*
 * Plan layout (one stratum, two rules):
 *   rule 0: "idb_a"  depends on EDB "edb_edge"
 *   rule 1: "idb_b"  depends on EDB "edb_node"  (independent)
 *
 * Inserting into "edb_edge" must set bit 0 only.
 * Inserting into "edb_node" must set bit 1 only.
 */
static void
test_direct_rule_dependency_affects_single_rule(void)
{
    TEST("direct dependency: rule A affected, rule B unaffected");

    wl_plan_op_t ops[2][1];
    wl_plan_relation_t rels[2];
    wl_plan_stratum_t strata[1];
    wl_plan_t plan;

    init_relation(&rels[0], "idb_a", "edb_edge", ops[0]);
    init_relation(&rels[1], "idb_b", "edb_node", ops[1]);

    memset(&strata[0], 0, sizeof(strata[0]));
    strata[0].stratum_id = 0;
    strata[0].is_recursive = false;
    strata[0].relations = rels;
    strata[0].relation_count = 2;

    memset(&plan, 0, sizeof(plan));
    plan.strata = strata;
    plan.stratum_count = 1;

    wl_session_t *sess = NULL;
    const wl_compute_backend_t *backend = wl_backend_columnar();
    int rc = backend->session_create(&plan, 1, &sess);
    if (rc != 0 || !sess) {
        FAIL("session_create failed");
        return;
    }

    uint64_t mask_edge = col_compute_affected_rules(sess, "edb_edge");
    uint64_t mask_node = col_compute_affected_rules(sess, "edb_node");
    backend->session_destroy(sess);

    if (mask_edge != (uint64_t)0x1) {
        char buf[80];
        snprintf(buf, sizeof(buf), "edge: expected 0x1, got 0x%" PRIx64,
                 mask_edge);
        FAIL(buf);
        return;
    }
    if (mask_node != (uint64_t)0x2) {
        char buf[80];
        snprintf(buf, sizeof(buf), "node: expected 0x2, got 0x%" PRIx64,
                 mask_node);
        FAIL(buf);
        return;
    }
    PASS;
}

/* ======================================================================== */
/* TC2: Transitive rule dependency — Rule A -> Rule B chain                 */
/* ======================================================================== */

/*
 * Plan layout (two strata):
 *   stratum 0, rule 0: "idb_mid"  depends on EDB "edb_base"
 *   stratum 1, rule 1: "idb_a"    depends on "idb_mid"   (transitive)
 *   stratum 1, rule 2: "idb_b"    depends on EDB "edb_other" (unrelated)
 *
 * Global rule indices (declaration order):
 *   rule 0 = strat0/idb_mid, rule 1 = strat1/idb_a, rule 2 = strat1/idb_b.
 *
 * Inserting "edb_base" must set bits 0,1 only; bit 2 must remain clear.
 */
static void
test_transitive_rule_dependency_marks_chain(void)
{
    TEST("transitive dependency: A->B chain both marked, C unaffected");

    wl_plan_op_t ops0[1][1], ops1[2][1];
    wl_plan_relation_t rels0[1], rels1[2];
    wl_plan_stratum_t strata[2];
    wl_plan_t plan;

    init_relation(&rels0[0], "idb_mid", "edb_base", ops0[0]);

    init_relation(&rels1[0], "idb_a", "idb_mid", ops1[0]);
    init_relation(&rels1[1], "idb_b", "edb_other", ops1[1]);

    memset(&strata[0], 0, sizeof(strata[0]));
    strata[0].stratum_id = 0;
    strata[0].is_recursive = false;
    strata[0].relations = rels0;
    strata[0].relation_count = 1;

    memset(&strata[1], 0, sizeof(strata[1]));
    strata[1].stratum_id = 1;
    strata[1].is_recursive = false;
    strata[1].relations = rels1;
    strata[1].relation_count = 2;

    memset(&plan, 0, sizeof(plan));
    plan.strata = strata;
    plan.stratum_count = 2;

    wl_session_t *sess = NULL;
    const wl_compute_backend_t *backend = wl_backend_columnar();
    int rc = backend->session_create(&plan, 1, &sess);
    if (rc != 0 || !sess) {
        FAIL("session_create failed");
        return;
    }

    uint64_t mask = col_compute_affected_rules(sess, "edb_base");
    backend->session_destroy(sess);

    /* bits 0,1 set; bit 2 clear */
    uint64_t expected = (uint64_t)0x3;
    if (mask == expected) {
        PASS;
    } else {
        char buf[80];
        snprintf(buf, sizeof(buf), "expected 0x%" PRIx64 ", got 0x%" PRIx64,
                 expected, mask);
        FAIL(buf);
    }
}

/* ======================================================================== */
/* TC3: Unaffected stratum frontier preserved after incremental insert       */
/* ======================================================================== */

/*
 * Program: path(x,y) :- edge(x,y). path(x,z) :- path(x,y), edge(y,z).
 *
 * Step 1: insert edges (1->2, 2->3), call wl_session_step.
 * Step 2: snapshot frontier for the recursive stratum (should be set).
 * Step 3: call col_session_insert_incremental for the same relation.
 * Assert: frontier for the recursive stratum is UNCHANGED after the insert;
 *         col_session_insert_incremental must not reset any frontier.
 */
static void
test_unaffected_stratum_frontier_preserved_after_insert(void)
{
    TEST("unaffected stratum frontier preserved after incremental insert");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      ".decl path(x: int32, y: int32)\n"
                      "path(x, y) :- edge(x, y).\n"
                      "path(x, z) :- path(x, y), edge(y, z).\n";

    wl_plan_t *plan = build_plan(src);
    if (!plan) {
        FAIL("build_plan failed");
        return;
    }

    uint32_t rec_si = find_recursive_stratum(plan);
    if (rec_si == UINT32_MAX) {
        wl_plan_free(plan);
        FAIL("no recursive stratum found");
        return;
    }

    wl_session_t *sess = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    if (rc != 0 || !sess) {
        wl_plan_free(plan);
        FAIL("wl_session_create failed");
        return;
    }

    /* 3-edge chain ensures recursive stratum runs multiple iterations,
     * setting a non-zero frontier before the incremental insert. */
    int64_t edges[] = { 1, 2, 2, 3, 3, 4 };
    rc = wl_session_insert(sess, "edge", edges, 3, 2);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("wl_session_insert failed");
        return;
    }

    rc = wl_session_step(sess);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("wl_session_step failed");
        return;
    }

    col_frontier_t f_before;
    rc = col_session_get_frontier(sess, rec_si, &f_before);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("col_session_get_frontier failed after step");
        return;
    }

    /* Frontier must be set after evaluation (not the zero-init default) */
    if (f_before.iteration == 0 && f_before.stratum == 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("frontier not set after wl_session_step");
        return;
    }

    int64_t new_edge[] = { 3, 4 };
    rc = col_session_insert_incremental(sess, "edge", new_edge, 1, 2);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "col_session_insert_incremental returned %d",
                 rc);
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL(msg);
        return;
    }

    col_frontier_t f_after;
    rc = col_session_get_frontier(sess, rec_si, &f_after);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("col_session_get_frontier failed after incremental insert");
        return;
    }

    if (f_after.iteration != f_before.iteration
        || f_after.stratum != f_before.stratum) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "frontier changed by insert: before=(%u,%u) after=(%u,%u)",
                 f_before.iteration, f_before.stratum, f_after.iteration,
                 f_after.stratum);
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL(msg);
        return;
    }

    wl_session_destroy(sess);
    wl_plan_free(plan);
    PASS;
}

/* ======================================================================== */
/* TC4: Output correctness — incremental matches full-reset baseline         */
/* ======================================================================== */

/*
 * Verify that rule-level frontier skip produces the same derived tuples
 * as a fresh full evaluation.
 *
 * Baseline session:
 *   insert all three edges (1->2, 2->3, 3->4) at once, step, count paths.
 *   Expected: 6 paths (1->2, 2->3, 3->4, 1->3, 2->4, 1->4).
 *
 * Incremental session:
 *   insert (1->2, 2->3), step -> 3 paths,
 *   incremental-insert (3->4), step -> must yield 6 paths total.
 *
 * If rule-level skip incorrectly skips the recursive rule, the incremental
 * session will produce fewer than 6 paths (regression).
 */
static void
test_output_correctness_incremental_matches_baseline(void)
{
    TEST("output correctness: incremental matches full-reset baseline (6 "
         "paths)");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      ".decl path(x: int32, y: int32)\n"
                      "path(x, y) :- edge(x, y).\n"
                      "path(x, z) :- path(x, y), edge(y, z).\n";

    /* ---- Baseline session: insert all edges at once ---- */
    wl_plan_t *plan_base = build_plan(src);
    if (!plan_base) {
        FAIL("build_plan (baseline) failed");
        return;
    }

    wl_session_t *sess_base = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan_base, 1, &sess_base);
    if (rc != 0 || !sess_base) {
        wl_plan_free(plan_base);
        FAIL("wl_session_create (baseline) failed");
        return;
    }

    int64_t all_edges[] = { 1, 2, 2, 3, 3, 4 };
    rc = wl_session_insert(sess_base, "edge", all_edges, 3, 2);
    if (rc != 0) {
        wl_session_destroy(sess_base);
        wl_plan_free(plan_base);
        FAIL("wl_session_insert (baseline) failed");
        return;
    }

    rc = wl_session_step(sess_base);
    if (rc != 0) {
        wl_session_destroy(sess_base);
        wl_plan_free(plan_base);
        FAIL("wl_session_step (baseline) failed");
        return;
    }

    int64_t baseline_count = 0;
    rc = wl_session_snapshot(sess_base, count_cb, &baseline_count);
    wl_session_destroy(sess_base);
    wl_plan_free(plan_base);

    if (rc != 0) {
        FAIL("wl_session_snapshot (baseline) failed");
        return;
    }

    /* ---- Incremental session: insert in two phases ---- */
    wl_plan_t *plan_inc = build_plan(src);
    if (!plan_inc) {
        FAIL("build_plan (incremental) failed");
        return;
    }

    wl_session_t *sess_inc = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan_inc, 1, &sess_inc);
    if (rc != 0 || !sess_inc) {
        wl_plan_free(plan_inc);
        FAIL("wl_session_create (incremental) failed");
        return;
    }

    int64_t initial_edges[] = { 1, 2, 2, 3 };
    rc = wl_session_insert(sess_inc, "edge", initial_edges, 2, 2);
    if (rc != 0) {
        wl_session_destroy(sess_inc);
        wl_plan_free(plan_inc);
        FAIL("wl_session_insert phase 1 failed");
        return;
    }

    rc = wl_session_step(sess_inc);
    if (rc != 0) {
        wl_session_destroy(sess_inc);
        wl_plan_free(plan_inc);
        FAIL("wl_session_step phase 1 failed");
        return;
    }

    int64_t extra_edge[] = { 3, 4 };
    rc = col_session_insert_incremental(sess_inc, "edge", extra_edge, 1, 2);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "col_session_insert_incremental returned %d",
                 rc);
        wl_session_destroy(sess_inc);
        wl_plan_free(plan_inc);
        FAIL(msg);
        return;
    }

    rc = wl_session_step(sess_inc);
    if (rc != 0) {
        wl_session_destroy(sess_inc);
        wl_plan_free(plan_inc);
        FAIL("wl_session_step phase 2 failed");
        return;
    }

    int64_t incremental_count = 0;
    rc = wl_session_snapshot(sess_inc, count_cb, &incremental_count);
    wl_session_destroy(sess_inc);
    wl_plan_free(plan_inc);

    if (rc != 0) {
        FAIL("wl_session_snapshot (incremental) failed");
        return;
    }

    if (incremental_count != baseline_count) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "count mismatch: baseline=%" PRId64 " incremental=%" PRId64,
                 baseline_count, incremental_count);
        FAIL(msg);
        return;
    }

    PASS;
}

/* ======================================================================== */
/* TC5a: Empty delta — no rules affected, function returns safely            */
/* ======================================================================== */

/*
 * A relation name not present in any rule body must return mask 0.
 * This verifies the "empty delta" scenario where inserted EDB relation
 * is unknown to the plan.
 */
static void
test_empty_delta_unknown_relation_affects_no_rules(void)
{
    TEST("empty delta: unknown relation affects no rules (mask 0)");

    wl_plan_op_t ops[2][1];
    wl_plan_relation_t rels[2];
    wl_plan_stratum_t strata[1];
    wl_plan_t plan;

    init_relation(&rels[0], "idb_a", "edb_edge", ops[0]);
    init_relation(&rels[1], "idb_b", "edb_node", ops[1]);

    memset(&strata[0], 0, sizeof(strata[0]));
    strata[0].stratum_id = 0;
    strata[0].is_recursive = false;
    strata[0].relations = rels;
    strata[0].relation_count = 2;

    memset(&plan, 0, sizeof(plan));
    plan.strata = strata;
    plan.stratum_count = 1;

    wl_session_t *sess = NULL;
    const wl_compute_backend_t *backend = wl_backend_columnar();
    int rc = backend->session_create(&plan, 1, &sess);
    if (rc != 0 || !sess) {
        FAIL("session_create failed");
        return;
    }

    uint64_t mask = col_compute_affected_rules(sess, "edb_unknown");
    backend->session_destroy(sess);

    if (mask == 0) {
        PASS;
    } else {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0, got 0x%" PRIx64, mask);
        FAIL(buf);
    }
}

/* ======================================================================== */
/* TC5b: Null insertion — NULL session or relation returns 0 safely         */
/* ======================================================================== */

/*
 * col_compute_affected_rules() must handle NULL session and NULL relation
 * gracefully, returning 0 without crashing.
 */
static void
test_null_insertion_returns_zero_mask(void)
{
    TEST("null insertion: NULL session or relation returns mask 0");

    wl_plan_op_t ops[1];
    wl_plan_relation_t rels[1];
    wl_plan_stratum_t strata[1];
    wl_plan_t plan;

    init_relation(&rels[0], "idb_a", "edb_edge", ops);

    memset(&strata[0], 0, sizeof(strata[0]));
    strata[0].stratum_id = 0;
    strata[0].is_recursive = false;
    strata[0].relations = rels;
    strata[0].relation_count = 1;

    memset(&plan, 0, sizeof(plan));
    plan.strata = strata;
    plan.stratum_count = 1;

    wl_session_t *sess = NULL;
    const wl_compute_backend_t *backend = wl_backend_columnar();
    int rc = backend->session_create(&plan, 1, &sess);
    if (rc != 0 || !sess) {
        FAIL("session_create failed");
        return;
    }

    uint64_t m1 = col_compute_affected_rules(NULL, "edb_edge");
    uint64_t m2 = col_compute_affected_rules(sess, NULL);
    backend->session_destroy(sess);

    if (m1 == 0 && m2 == 0) {
        PASS;
    } else {
        char buf[80];
        snprintf(buf, sizeof(buf), "expected 0,0 got 0x%" PRIx64 ",0x%" PRIx64,
                 m1, m2);
        FAIL(buf);
    }
}

/* ======================================================================== */
/* TC6: Affected rule mask uses rule indices globally across strata         */
/* ======================================================================== */

/*
 * Verify that the rule index encoding is globally contiguous across strata,
 * not reset per-stratum.  With:
 *   strat 0: rule 0 (idb_a <- edb_edge),  rule 1 (idb_b <- edb_node)
 *   strat 1: rule 2 (idb_c <- idb_b)
 *
 * Inserting "edb_node" must set bits 1 and 2 (rule 1 directly, rule 2
 * transitively through idb_b), leaving bit 0 clear.
 */
static void
test_global_rule_indices_across_strata(void)
{
    TEST("global rule indices: cross-stratum bit assignment is contiguous");

    wl_plan_op_t ops0[2][1], ops1[1][1];
    wl_plan_relation_t rels0[2], rels1[1];
    wl_plan_stratum_t strata[2];
    wl_plan_t plan;

    init_relation(&rels0[0], "idb_a", "edb_edge", ops0[0]); /* rule 0 */
    init_relation(&rels0[1], "idb_b", "edb_node", ops0[1]); /* rule 1 */
    init_relation(&rels1[0], "idb_c", "idb_b", ops1[0]);    /* rule 2 */

    memset(&strata[0], 0, sizeof(strata[0]));
    strata[0].stratum_id = 0;
    strata[0].is_recursive = false;
    strata[0].relations = rels0;
    strata[0].relation_count = 2;

    memset(&strata[1], 0, sizeof(strata[1]));
    strata[1].stratum_id = 1;
    strata[1].is_recursive = false;
    strata[1].relations = rels1;
    strata[1].relation_count = 1;

    memset(&plan, 0, sizeof(plan));
    plan.strata = strata;
    plan.stratum_count = 2;

    wl_session_t *sess = NULL;
    const wl_compute_backend_t *backend = wl_backend_columnar();
    int rc = backend->session_create(&plan, 1, &sess);
    if (rc != 0 || !sess) {
        FAIL("session_create failed");
        return;
    }

    uint64_t mask = col_compute_affected_rules(sess, "edb_node");
    backend->session_destroy(sess);

    /* bits 1,2 set; bit 0 clear */
    uint64_t expected = (uint64_t)0x6;
    if (mask == expected) {
        PASS;
    } else {
        char buf[80];
        snprintf(buf, sizeof(buf), "expected 0x%" PRIx64 ", got 0x%" PRIx64,
                 expected, mask);
        FAIL(buf);
    }
}

/* ======================================================================== */
/* TC7: Multiple incremental inserts — frontier preserved across all         */
/* ======================================================================== */

/*
 * Pattern: eval, inc-insert, eval, inc-insert, eval.
 * After each col_session_insert_incremental the recursive stratum's
 * frontier must remain non-zero (not reset to UINT32_MAX or zero).
 * This guards against frontier corruption over repeated incremental cycles.
 */
static void
test_multiple_incremental_inserts_preserve_frontier(void)
{
    TEST("multiple incremental inserts: frontier preserved across all cycles");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      ".decl path(x: int32, y: int32)\n"
                      "path(x, y) :- edge(x, y).\n"
                      "path(x, z) :- path(x, y), edge(y, z).\n";

    wl_plan_t *plan = build_plan(src);
    if (!plan) {
        FAIL("build_plan failed");
        return;
    }

    uint32_t rec_si = find_recursive_stratum(plan);
    if (rec_si == UINT32_MAX) {
        wl_plan_free(plan);
        FAIL("no recursive stratum found");
        return;
    }

    wl_session_t *sess = NULL;
    int rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    if (rc != 0 || !sess) {
        wl_plan_free(plan);
        FAIL("wl_session_create failed");
        return;
    }

    int64_t e1[] = { 1, 2 };
    rc = wl_session_insert(sess, "edge", e1, 1, 2);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("wl_session_insert failed");
        return;
    }
    rc = wl_session_step(sess);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("step 1 failed");
        return;
    }

    col_frontier_t f1;
    rc = col_session_get_frontier(sess, rec_si, &f1);
    if (rc != 0 || f1.iteration == UINT32_MAX) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("frontier not set after step 1");
        return;
    }

    /* First incremental insert: must preserve frontier */
    int64_t e2[] = { 2, 3 };
    rc = col_session_insert_incremental(sess, "edge", e2, 1, 2);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("incremental insert 1 failed");
        return;
    }

    col_frontier_t f_mid;
    rc = col_session_get_frontier(sess, rec_si, &f_mid);
    if (rc != 0 || f_mid.iteration != f1.iteration
        || f_mid.stratum != f1.stratum) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("frontier changed after incremental insert 1");
        return;
    }

    rc = wl_session_step(sess);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("step 2 failed");
        return;
    }

    /* Second incremental insert: frontier must still be valid */
    int64_t e3[] = { 3, 4 };
    rc = col_session_insert_incremental(sess, "edge", e3, 1, 2);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("incremental insert 2 failed");
        return;
    }

    col_frontier_t f_final;
    rc = col_session_get_frontier(sess, rec_si, &f_final);
    if (rc != 0 || f_final.iteration == UINT32_MAX) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("frontier invalid after incremental insert 2");
        return;
    }

    rc = wl_session_step(sess);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        FAIL("step 3 failed");
        return;
    }

    wl_session_destroy(sess);
    wl_plan_free(plan);
    PASS;
}

/* ======================================================================== */
/* MAIN                                                                      */
/* ======================================================================== */

int
main(void)
{
    printf("\n=== Rule-Level Frontier Integration Tests (Phase 4, US-4-006) "
           "===\n\n");

    test_direct_rule_dependency_affects_single_rule();
    test_transitive_rule_dependency_marks_chain();
    test_unaffected_stratum_frontier_preserved_after_insert();
    test_output_correctness_incremental_matches_baseline();
    test_empty_delta_unknown_relation_affects_no_rules();
    test_null_insertion_returns_zero_mask();
    test_global_rule_indices_across_strata();
    test_multiple_incremental_inserts_preserve_frontier();

    printf("\n=== Results: %d/%d passed ===\n\n", tests_passed,
           tests_passed + tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
