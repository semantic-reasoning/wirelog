/*
 * test_affected_rules.c - Tests for col_compute_affected_rules() (Phase 4,
 * US-4-003)
 *
 * Validates that col_compute_affected_rules() correctly identifies which rules
 * need re-evaluation after new facts are inserted into an EDB relation.
 * Covers direct, transitive, cross-stratum, and SIMD code path correctness.
 *
 * Rule indices are assigned globally across strata in declaration order:
 *   stratum 0 relation 0 -> rule 0
 *   stratum 0 relation 1 -> rule 1
 *   stratum 1 relation 0 -> rule 2
 *   ... etc.
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/exec_plan.h"

/* ======================================================================== */
/* TEST HARNESS MACROS                                                       */
/* ======================================================================== */

#define TEST(name)                       \
    do {                                 \
        printf("  [TEST] %-60s ", name); \
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
 * Build a wl_plan_relation_t with a single VARIABLE op pointing to dep_rel.
 * The relation itself is named rel_name (the IDB relation produced / rule head).
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
/* TEST 1: Direct dependency — single rule in one stratum                   */
/* ======================================================================== */

static void
test_direct_dependency(void)
{
    TEST("direct_dependency: EDB insertion marks only the dependent rule");

    /*
     * Plan layout (one stratum, two relations/rules):
     *   stratum 0, rule 0: "idb_A" depends on EDB "edge"
     *   stratum 0, rule 1: "idb_B" depends on EDB "node"  (unrelated)
     *
     * Inserting into "edge" should set bit 0 only.
     */
    wl_plan_op_t ops[2][1];
    wl_plan_relation_t rels[2];
    wl_plan_stratum_t strata[1];
    wl_plan_t plan;

    init_relation(&rels[0], "idb_A", "edge", ops[0]);
    init_relation(&rels[1], "idb_B", "node", ops[1]);

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

    uint64_t mask = col_compute_affected_rules(sess, "edge");
    backend->session_destroy(sess);

    /* Only rule 0 (idb_A <- edge) should be set. */
    if (mask == (uint64_t)0x1) {
        PASS;
    } else {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0x1, got 0x%" PRIx64, mask);
        FAIL(buf);
    }
}

/* ======================================================================== */
/* TEST 2: Transitive dependency across rules in the same stratum           */
/* ======================================================================== */

static void
test_transitive_same_stratum(void)
{
    TEST("transitive_same_stratum: rule chain A->B->C all marked");

    /*
     * Plan layout (one stratum, three rules):
     *   rule 0: "rel_C" depends on EDB "edb_x"
     *   rule 1: "rel_B" depends on "rel_C"
     *   rule 2: "rel_A" depends on "rel_B"
     *
     * Inserting "edb_x" must mark rules 0, 1, 2.
     */
    wl_plan_op_t ops[3][1];
    wl_plan_relation_t rels[3];
    wl_plan_stratum_t strata[1];
    wl_plan_t plan;

    init_relation(&rels[0], "rel_C", "edb_x", ops[0]);
    init_relation(&rels[1], "rel_B", "rel_C", ops[1]);
    init_relation(&rels[2], "rel_A", "rel_B", ops[2]);

    memset(&strata[0], 0, sizeof(strata[0]));
    strata[0].stratum_id = 0;
    strata[0].is_recursive = false;
    strata[0].relations = rels;
    strata[0].relation_count = 3;

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

    uint64_t mask = col_compute_affected_rules(sess, "edb_x");
    backend->session_destroy(sess);

    uint64_t expected = (uint64_t)0x7; /* bits 0,1,2 */
    if (mask == expected) {
        PASS;
    } else {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0x%" PRIx64 ", got 0x%" PRIx64,
                 expected, mask);
        FAIL(buf);
    }
}

/* ======================================================================== */
/* TEST 3: Transitive dependency across strata                              */
/* ======================================================================== */

static void
test_transitive_cross_strata(void)
{
    TEST("transitive_cross_strata: dependency through stratum boundary");

    /*
     * Plan layout (two strata):
     *   stratum 0, rule 0: "idb_0" depends on EDB "base"
     *   stratum 1, rule 1: "idb_1" depends on "idb_0"
     *   stratum 1, rule 2: "idb_2" depends on EDB "other"  (unrelated)
     *
     * Global rule indices: rule 0 = strat0/rel0, rule 1 = strat1/rel0,
     *                      rule 2 = strat1/rel1.
     *
     * Inserting "base" must mark rules 0 and 1 (bits 0,1); rule 2 must not
     * be marked.
     */
    wl_plan_op_t ops0[1][1], ops1[2][1];
    wl_plan_relation_t rels0[1], rels1[2];
    wl_plan_stratum_t strata[2];
    wl_plan_t plan;

    init_relation(&rels0[0], "idb_0", "base", ops0[0]);

    init_relation(&rels1[0], "idb_1", "idb_0", ops1[0]);
    init_relation(&rels1[1], "idb_2", "other", ops1[1]);

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

    uint64_t mask = col_compute_affected_rules(sess, "base");
    backend->session_destroy(sess);

    /* rules 0 (idb_0<-base) and 1 (idb_1<-idb_0) affected; rule 2 not. */
    uint64_t expected = (uint64_t)0x3; /* bits 0,1 */
    if (mask == expected) {
        PASS;
    } else {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0x%" PRIx64 ", got 0x%" PRIx64,
                 expected, mask);
        FAIL(buf);
    }
}

/* ======================================================================== */
/* TEST 4: Unrelated insertion yields mask 0                                */
/* ======================================================================== */

static void
test_unrelated_insertion(void)
{
    TEST("unrelated_insertion: inserting unrelated EDB yields mask 0");

    /*
     * Plan:
     *   stratum 0, rule 0: "idb_A" depends on EDB "edge"
     *
     * Inserting into "vertex" (unreferenced) must return 0.
     */
    wl_plan_op_t ops0[1];
    wl_plan_relation_t rels0[1];
    wl_plan_stratum_t strata[1];
    wl_plan_t plan;

    init_relation(&rels0[0], "idb_A", "edge", ops0);

    memset(&strata[0], 0, sizeof(strata[0]));
    strata[0].stratum_id = 0;
    strata[0].is_recursive = false;
    strata[0].relations = rels0;
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

    uint64_t mask = col_compute_affected_rules(sess, "vertex");
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
/* TEST 5: SIMD path — >8 rules, sparse pattern                            */
/* ======================================================================== */

static void
test_simd_matches_scalar(void)
{
    TEST(
        "simd_matches_scalar: SIMD bitmask result matches scalar for 10 rules");

    /*
     * Build a plan with one stratum and 10 rules.
     * Rules 0, 3, 7, 9 reference "edb_k"; others reference "other_edb".
     * No transitive dependencies.
     *
     * Expected mask: bits 0,3,7,9 = 0x289.
     */
#define N_RULES 10
    wl_plan_op_t ops[N_RULES][1];
    wl_plan_relation_t rels[N_RULES];
    wl_plan_stratum_t strata[1];
    wl_plan_t plan;

    const bool uses_edb[N_RULES]
        = { true, false, false, true, false, false, false, true, false, true };
    static const char *idb_names[N_RULES] = {
        "idb_r0", "idb_r1", "idb_r2", "idb_r3", "idb_r4",
        "idb_r5", "idb_r6", "idb_r7", "idb_r8", "idb_r9",
    };

    for (int i = 0; i < N_RULES; i++) {
        const char *dep = uses_edb[i] ? "edb_k" : "other_edb";
        init_relation(&rels[i], idb_names[i], dep, ops[i]);
    }

    memset(&strata[0], 0, sizeof(strata[0]));
    strata[0].stratum_id = 0;
    strata[0].is_recursive = false;
    strata[0].relations = rels;
    strata[0].relation_count = N_RULES;

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

    uint64_t mask = col_compute_affected_rules(sess, "edb_k");
    backend->session_destroy(sess);

    /* Bits 0,3,7,9 = 0x289 */
    uint64_t expected = ((uint64_t)1 << 0) | ((uint64_t)1 << 3)
                        | ((uint64_t)1 << 7) | ((uint64_t)1 << 9);
    if (mask == expected) {
        PASS;
    } else {
        char buf[80];
        snprintf(buf, sizeof(buf), "expected 0x%" PRIx64 ", got 0x%" PRIx64,
                 expected, mask);
        FAIL(buf);
    }
#undef N_RULES
}

/* ======================================================================== */
/* TEST 6: Invalid input returns 0                                          */
/* ======================================================================== */

static void
test_null_inputs(void)
{
    TEST("null_inputs: NULL session or relation returns 0");

    wl_plan_op_t ops0[1];
    wl_plan_relation_t rels0[1];
    wl_plan_stratum_t strata[1];
    wl_plan_t plan;

    init_relation(&rels0[0], "idb_A", "edge", ops0);

    memset(&strata[0], 0, sizeof(strata[0]));
    strata[0].stratum_id = 0;
    strata[0].is_recursive = false;
    strata[0].relations = rels0;
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

    uint64_t m1 = col_compute_affected_rules(NULL, "edge");
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
/* TEST 7: JOIN right_relation dependency detection                         */
/* ======================================================================== */

static void
test_join_right_relation_dependency(void)
{
    TEST("join_right_relation: dependency via JOIN right_relation field");

    /*
     * Plan layout (one stratum, one rule):
     *   stratum 0, rule 0: "path" depends on "edge" (via VARIABLE)
     *                      and "path" (via JOIN right_relation)
     *
     * Create a rule: path(x,z) :- path(x,y), edge(y,z)
     * This has:
     *   - VARIABLE op for "path" (left child of join)
     *   - JOIN op with right_relation = "edge"
     *
     * Inserting "edge" must mark rule 0.
     * This test validates the JOIN right_relation fix in
     * rule_references_relation() and col_compute_affected_rules().
     */
    wl_plan_op_t ops[2];
    wl_plan_relation_t rels[1];
    wl_plan_stratum_t strata[1];
    wl_plan_t plan;

    memset(&ops[0], 0, sizeof(ops[0]));
    ops[0].op = WL_PLAN_OP_VARIABLE;
    ops[0].relation_name = "path";

    memset(&ops[1], 0, sizeof(ops[1]));
    ops[1].op = WL_PLAN_OP_JOIN;
    ops[1].right_relation = "edge"; /* Dependency via JOIN right_relation */

    memset(&rels[0], 0, sizeof(rels[0]));
    rels[0].name = "path";
    rels[0].ops = ops;
    rels[0].op_count = 2;

    memset(&strata[0], 0, sizeof(strata[0]));
    strata[0].stratum_id = 0;
    strata[0].is_recursive = true;
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

    /* Inserting "edge" should mark rule 0 */
    uint64_t mask = col_compute_affected_rules(sess, "edge");
    backend->session_destroy(sess);

    uint64_t expected = (uint64_t)0x1; /* bit 0 */
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
/* MAIN                                                                      */
/* ======================================================================== */

int
main(void)
{
    printf("\n=== Affected Rule Detection Tests (Phase 4, US-4-003) ===\n\n");

    test_direct_dependency();
    test_transitive_same_stratum();
    test_transitive_cross_strata();
    test_unrelated_insertion();
    test_simd_matches_scalar();
    test_null_inputs();
    test_join_right_relation_dependency();

    printf("\n=== Results: %d/%d passed ===\n\n", tests_passed,
           tests_passed + tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
