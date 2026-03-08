/*
 * test_affected_strata.c - Tests for col_compute_affected_strata() (Phase 4)
 *
 * Validates that col_compute_affected_strata() correctly identifies which
 * strata need re-evaluation after new facts are inserted into an EDB relation.
 * Covers direct, transitive, and SIMD code path correctness.
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

#include "../wirelog/backend/columnar_nanoarrow.h"
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
 * The relation itself is named rel_name (the IDB relation produced).
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
/* TEST 1: EDB insertion marks only directly dependent IDB strata           */
/* ======================================================================== */

static void
test_direct_dependency(void)
{
    TEST("direct_dependency: EDB insertion marks only dependent IDB strata");

    /*
     * Plan layout:
     *   stratum 0: relation "idb_A" depends on EDB "edge"
     *   stratum 1: relation "idb_B" depends on EDB "node"  (unrelated)
     *
     * Inserting into "edge" should set bit 0 only.
     */
    wl_plan_op_t ops0[1], ops1[1];
    wl_plan_relation_t rels0[1], rels1[1];
    wl_plan_stratum_t strata[2];
    wl_plan_t plan;

    init_relation(&rels0[0], "idb_A", "edge", ops0);
    init_relation(&rels1[0], "idb_B", "node", ops1);

    memset(&strata[0], 0, sizeof(strata[0]));
    strata[0].stratum_id = 0;
    strata[0].is_recursive = false;
    strata[0].relations = rels0;
    strata[0].relation_count = 1;

    memset(&strata[1], 0, sizeof(strata[1]));
    strata[1].stratum_id = 1;
    strata[1].is_recursive = false;
    strata[1].relations = rels1;
    strata[1].relation_count = 1;

    memset(&plan, 0, sizeof(plan));
    plan.strata = strata;
    plan.stratum_count = 2;

    /* Create a real session so col_compute_affected_strata can use it. */
    wl_session_t *sess = NULL;
    const wl_compute_backend_t *backend = wl_backend_columnar();
    int rc = backend->session_create(&plan, 1, &sess);
    if (rc != 0 || !sess) {
        FAIL("session_create failed");
        return;
    }

    uint64_t mask = col_compute_affected_strata(sess, "edge");
    backend->session_destroy(sess);

    if (mask == (uint64_t)0x1) {
        PASS;
    } else {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0x1, got 0x%" PRIx64, mask);
        FAIL(buf);
    }
}

/* ======================================================================== */
/* TEST 2: Lower stratum insertion marks all higher dependent strata         */
/* ======================================================================== */

static void
test_higher_strata_affected(void)
{
    TEST("higher_strata_affected: lower stratum marks higher dependent strata");

    /*
     * Plan layout (linear chain via VARIABLE ops):
     *   stratum 0: "idb_0" depends on EDB "base"
     *   stratum 1: "idb_1" depends on "idb_0"
     *   stratum 2: "idb_2" depends on "idb_1"
     *   stratum 3: "idb_3" depends on EDB "other"  (unrelated)
     *
     * Inserting into "base" should mark strata 0, 1, 2 (bits 0,1,2).
     * Stratum 3 is unrelated and must NOT be marked.
     */
    wl_plan_op_t ops[4][1];
    wl_plan_relation_t rels[4][1];
    wl_plan_stratum_t strata[4];
    wl_plan_t plan;

    init_relation(&rels[0][0], "idb_0", "base", ops[0]);
    init_relation(&rels[1][0], "idb_1", "idb_0", ops[1]);
    init_relation(&rels[2][0], "idb_2", "idb_1", ops[2]);
    init_relation(&rels[3][0], "idb_3", "other", ops[3]);

    for (int i = 0; i < 4; i++) {
        memset(&strata[i], 0, sizeof(strata[i]));
        strata[i].stratum_id = (uint32_t)i;
        strata[i].is_recursive = false;
        strata[i].relations = rels[i];
        strata[i].relation_count = 1;
    }

    memset(&plan, 0, sizeof(plan));
    plan.strata = strata;
    plan.stratum_count = 4;

    wl_session_t *sess = NULL;
    const wl_compute_backend_t *backend = wl_backend_columnar();
    int rc = backend->session_create(&plan, 1, &sess);
    if (rc != 0 || !sess) {
        FAIL("session_create failed");
        return;
    }

    uint64_t mask = col_compute_affected_strata(sess, "base");
    backend->session_destroy(sess);

    /* Expect bits 0, 1, 2 set; bit 3 clear. */
    uint64_t expected = (uint64_t)0x7;
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
/* TEST 3: Transitive dependency (A -> B -> C chain)                        */
/* ======================================================================== */

static void
test_transitive_dependency(void)
{
    TEST("transitive_dependency: A depends on B depends on C");

    /*
     * Plan:
     *   stratum 0: "rel_C" depends on EDB "edb_x"
     *   stratum 1: "rel_B" depends on "rel_C"
     *   stratum 2: "rel_A" depends on "rel_B"
     *
     * Inserting into "edb_x" must mark all three strata (bits 0,1,2).
     */
    wl_plan_op_t ops[3][1];
    wl_plan_relation_t rels[3][1];
    wl_plan_stratum_t strata[3];
    wl_plan_t plan;

    init_relation(&rels[0][0], "rel_C", "edb_x", ops[0]);
    init_relation(&rels[1][0], "rel_B", "rel_C", ops[1]);
    init_relation(&rels[2][0], "rel_A", "rel_B", ops[2]);

    for (int i = 0; i < 3; i++) {
        memset(&strata[i], 0, sizeof(strata[i]));
        strata[i].stratum_id = (uint32_t)i;
        strata[i].is_recursive = false;
        strata[i].relations = rels[i];
        strata[i].relation_count = 1;
    }

    memset(&plan, 0, sizeof(plan));
    plan.strata = strata;
    plan.stratum_count = 3;

    wl_session_t *sess = NULL;
    const wl_compute_backend_t *backend = wl_backend_columnar();
    int rc = backend->session_create(&plan, 1, &sess);
    if (rc != 0 || !sess) {
        FAIL("session_create failed");
        return;
    }

    uint64_t mask = col_compute_affected_strata(sess, "edb_x");
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
/* TEST 4: Unrelated stratum produces bitmask = 0                           */
/* ======================================================================== */

static void
test_unrelated_insertion(void)
{
    TEST("unrelated_insertion: inserting unrelated EDB yields mask 0");

    /*
     * Plan:
     *   stratum 0: "idb_A" depends on EDB "edge"
     *
     * Inserting into "vertex" (which no stratum references) must return 0.
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

    uint64_t mask = col_compute_affected_strata(sess, "vertex");
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
/* TEST 5: SIMD path matches scalar fallback for >8 strata                  */
/* ======================================================================== */

static void
test_simd_matches_scalar(void)
{
    TEST("simd_matches_scalar: SIMD bitmask result matches scalar for 10 "
         "strata");

    /*
     * Build a plan with 10 strata where strata 0,3,7,9 reference "edb_k".
     * Verify col_compute_affected_strata returns exactly bits 0,3,7,9.
     * This exercises bitmask_or_simd() on a wide-enough bitmask.
     */
#define N_STRATA 10
    wl_plan_op_t ops[N_STRATA][2];
    wl_plan_relation_t rels[N_STRATA][1];
    wl_plan_stratum_t strata[N_STRATA];
    wl_plan_t plan;

    /* Relations that reference "edb_k" directly: strata 0, 3, 7, 9 */
    const bool uses_edb[N_STRATA]
        = { true, false, false, true, false, false, false, true, false, true };
    const char *dep_rels[N_STRATA];
    for (int i = 0; i < N_STRATA; i++) {
        dep_rels[i] = uses_edb[i] ? "edb_k" : "other_edb";
    }

    /* Give each stratum a unique IDB name and wire ops. */
    static const char *idb_names[N_STRATA] = {
        "idb_s0", "idb_s1", "idb_s2", "idb_s3", "idb_s4",
        "idb_s5", "idb_s6", "idb_s7", "idb_s8", "idb_s9",
    };

    for (int i = 0; i < N_STRATA; i++) {
        init_relation(&rels[i][0], idb_names[i], dep_rels[i], &ops[i][0]);
        memset(&strata[i], 0, sizeof(strata[i]));
        strata[i].stratum_id = (uint32_t)i;
        strata[i].is_recursive = false;
        strata[i].relations = rels[i];
        strata[i].relation_count = 1;
    }

    memset(&plan, 0, sizeof(plan));
    plan.strata = strata;
    plan.stratum_count = N_STRATA;

    wl_session_t *sess = NULL;
    const wl_compute_backend_t *backend = wl_backend_columnar();
    int rc = backend->session_create(&plan, 1, &sess);
    if (rc != 0 || !sess) {
        FAIL("session_create failed");
        return;
    }

    uint64_t mask = col_compute_affected_strata(sess, "edb_k");
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
#undef N_STRATA
}

/* ======================================================================== */
/* MAIN                                                                      */
/* ======================================================================== */

int
main(void)
{
    printf("\n=== Affected Strata Detection Tests (Phase 4) ===\n\n");

    test_direct_dependency();
    test_higher_strata_affected();
    test_transitive_dependency();
    test_unrelated_insertion();
    test_simd_matches_scalar();

    printf("\n=== Results: %d/%d passed ===\n\n", tests_passed,
           tests_passed + tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
