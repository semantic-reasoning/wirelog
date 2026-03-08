/*
 * test_option2_cse.c - Tests for K-way delta expansion with CSE hints
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates that the plan rewriter correctly emits K copies for
 * recursive rules with K >= 3 IDB body atoms, with proper
 * delta_mode and materialization hints.
 */

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ========================================================================
 * Test Framework
 * ======================================================================== */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                      \
    do {                                                \
        test_count++;                                   \
        printf("TEST %d: %s ... ", test_count, (name)); \
    } while (0)

#define PASS()            \
    do {                  \
        pass_count++;     \
        printf("PASS\n"); \
    } while (0)

#define FAIL(msg)                    \
    do {                             \
        fail_count++;                \
        printf("FAIL: %s\n", (msg)); \
    } while (0)

#define ASSERT(cond, msg) \
    do {                  \
        if (!(cond)) {    \
            FAIL(msg);    \
            return;       \
        }                 \
    } while (0)

/* ========================================================================
 * K-FUSION mode detection
 * ======================================================================== */

#ifdef ENABLE_K_FUSION
static bool
using_k_fusion(void)
{
    return ENABLE_K_FUSION;
}
#else
static bool
using_k_fusion(void)
{
    return false;
}
#endif

/* ========================================================================
 * Helpers
 * ======================================================================== */

static wl_plan_t *
make_plan(const char *src)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return NULL;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    wl_plan_from_program(prog, &plan);
    wirelog_program_free(prog);
    return plan;
}

static const wl_plan_relation_t *
find_relation(const wl_plan_t *plan, const char *name)
{
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            if (plan->strata[s].relations[r].name
                && strcmp(plan->strata[s].relations[r].name, name) == 0)
                return &plan->strata[s].relations[r];
        }
    }
    return NULL;
}

static uint32_t
count_ops(const wl_plan_relation_t *rel, wl_plan_op_type_t type)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < rel->op_count; i++) {
        if (rel->ops[i].op == type)
            n++;
    }
    return n;
}

static uint32_t
count_delta_mode(const wl_plan_relation_t *rel, wl_delta_mode_t mode)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < rel->op_count; i++) {
        if (rel->ops[i].delta_mode == mode)
            n++;
    }
    return n;
}

static uint32_t
count_materialized(const wl_plan_relation_t *rel)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < rel->op_count; i++) {
        if (rel->ops[i].materialized)
            n++;
    }
    return n;
}

struct count_ctx {
    int64_t count;
};

static void
count_cb(const char *relation, const int64_t *row, uint32_t ncols,
         void *user_data)
{
    struct count_ctx *ctx = (struct count_ctx *)user_data;
    ctx->count++;
    (void)relation;
    (void)row;
    (void)ncols;
}

static int64_t
run_program(const char *src, uint32_t num_workers)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0 || !plan) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, num_workers, &sess);
    if (rc != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    rc = wl_session_load_facts(sess, prog);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    struct count_ctx ctx = { 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);

    if (rc != 0)
        return -1;
    return ctx.count;
}

/* ========================================================================
 * PLAN STRUCTURE TESTS: K-way delta expansion
 * ======================================================================== */

static void
test_2atom_no_expansion(void)
{
    TEST("2-atom rule (TC) is not expanded");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wl_plan_t *plan = make_plan(src);
    ASSERT(plan != NULL, "plan generation failed");

    const wl_plan_relation_t *tc = find_relation(plan, "tc");
    ASSERT(tc != NULL, "tc not found");

    uint32_t force_delta = count_delta_mode(tc, WL_DELTA_FORCE_DELTA);
    ASSERT(force_delta == 0, "2-atom rule should not have FORCE_DELTA ops");

    wl_plan_free(plan);
    PASS();
}

static void
test_3atom_expansion(void)
{
    TEST("3-atom recursive rule produces K=3 copies");

    const char *src = ".decl a(x: int32, y: int32)\n"
                      ".decl b(x: int32, y: int32)\n"
                      ".decl c(x: int32, y: int32)\n"
                      ".decl r(x: int32, w: int32)\n"
                      "a(1, 2). b(2, 3). c(3, 4).\n"
                      "r(x, w) :- a(x, y), b(y, z), c(z, w).\n"
                      "a(x, y) :- r(x, y).\n"
                      "b(x, y) :- r(x, y).\n"
                      "c(x, y) :- r(x, y).\n";

    wl_plan_t *plan = make_plan(src);
    ASSERT(plan != NULL, "plan generation failed");

    const wl_plan_relation_t *r = find_relation(plan, "r");
    ASSERT(r != NULL, "r not found");

    if (using_k_fusion()) {
        uint32_t k_fusions = count_ops(r, WL_PLAN_OP_K_FUSION);
        ASSERT(k_fusions >= 1, "expected K_FUSION operator with ENABLE_K_FUSION=1");
    } else {
        uint32_t force_delta = count_delta_mode(r, WL_DELTA_FORCE_DELTA);
        ASSERT(force_delta == 3, "expected 3 FORCE_DELTA ops for 3-atom rule");

        uint32_t concats = count_ops(r, WL_PLAN_OP_CONCAT);
        ASSERT(concats >= 2, "expected at least 2 CONCAT ops");

        uint32_t consols = count_ops(r, WL_PLAN_OP_CONSOLIDATE);
        ASSERT(consols >= 1, "expected CONSOLIDATE op");
    }

    wl_plan_free(plan);
    PASS();
}

static void
test_3atom_materialization_hints(void)
{
    TEST("3-atom rule has materialization hints on first K-2 joins");

    const char *src = ".decl a(x: int32, y: int32)\n"
                      ".decl b(x: int32, y: int32)\n"
                      ".decl c(x: int32, y: int32)\n"
                      ".decl r(x: int32, w: int32)\n"
                      "a(1, 2). b(2, 3). c(3, 4).\n"
                      "r(x, w) :- a(x, y), b(y, z), c(z, w).\n"
                      "a(x, y) :- r(x, y).\n"
                      "b(x, y) :- r(x, y).\n"
                      "c(x, y) :- r(x, y).\n";

    wl_plan_t *plan = make_plan(src);
    ASSERT(plan != NULL, "plan generation failed");

    const wl_plan_relation_t *r = find_relation(plan, "r");
    ASSERT(r != NULL, "r not found");

    if (using_k_fusion()) {
        /* K_FUSION encapsulates all copies; materialization hints are internal */
        uint32_t k_fusions = count_ops(r, WL_PLAN_OP_K_FUSION);
        ASSERT(k_fusions >= 1, "expected K_FUSION operator with ENABLE_K_FUSION=1");
    } else {
        /* K=3: first K-2=1 JOIN position is materialized per copy.
         * 3 copies × 1 materialized JOIN = 3 total. */
        uint32_t mat = count_materialized(r);
        ASSERT(mat == 3, "expected 3 materialized hints (1 per copy × 3 copies)");
    }

    wl_plan_free(plan);
    PASS();
}

static void
test_3atom_force_full(void)
{
    TEST("3-atom rule has correct FORCE_FULL count");

    const char *src = ".decl a(x: int32, y: int32)\n"
                      ".decl b(x: int32, y: int32)\n"
                      ".decl c(x: int32, y: int32)\n"
                      ".decl r(x: int32, w: int32)\n"
                      "a(1, 2). b(2, 3). c(3, 4).\n"
                      "r(x, w) :- a(x, y), b(y, z), c(z, w).\n"
                      "a(x, y) :- r(x, y).\n"
                      "b(x, y) :- r(x, y).\n"
                      "c(x, y) :- r(x, y).\n";

    wl_plan_t *plan = make_plan(src);
    ASSERT(plan != NULL, "plan generation failed");

    const wl_plan_relation_t *r = find_relation(plan, "r");
    ASSERT(r != NULL, "r not found");

    if (using_k_fusion()) {
        /* K_FUSION encapsulates all copies; FORCE_FULL is internal */
        uint32_t k_fusions = count_ops(r, WL_PLAN_OP_K_FUSION);
        ASSERT(k_fusions >= 1, "expected K_FUSION operator with ENABLE_K_FUSION=1");
    } else {
        /* Each copy: 1 FORCE_DELTA + 2 FORCE_FULL = K*(K-1) = 6 FORCE_FULL */
        uint32_t force_full = count_delta_mode(r, WL_DELTA_FORCE_FULL);
        ASSERT(force_full == 6,
               "expected 6 FORCE_FULL ops (2 per copy × 3 copies)");
    }

    wl_plan_free(plan);
    PASS();
}

static void
test_nonrecursive_no_rewrite(void)
{
    TEST("non-recursive stratum is not rewritten");

    const char *src = ".decl a(x: int32, y: int32)\n"
                      ".decl b(x: int32, y: int32)\n"
                      ".decl c(x: int32, y: int32)\n"
                      ".decl r(x: int32, w: int32)\n"
                      "a(1, 2). b(2, 3). c(3, 4).\n"
                      "r(x, w) :- a(x, y), b(y, z), c(z, w).\n";

    wl_plan_t *plan = make_plan(src);
    ASSERT(plan != NULL, "plan generation failed");

    const wl_plan_relation_t *r = find_relation(plan, "r");
    ASSERT(r != NULL, "r not found");

    uint32_t force_delta = count_delta_mode(r, WL_DELTA_FORCE_DELTA);
    ASSERT(force_delta == 0, "non-recursive should not have FORCE_DELTA");

    wl_plan_free(plan);
    PASS();
}

static void
test_delta_and_full_invariant(void)
{
    TEST("K=3 expansion: FORCE_DELTA + FORCE_FULL = K*K");

    const char *src = ".decl a(x: int32, y: int32)\n"
                      ".decl b(x: int32, y: int32)\n"
                      ".decl c(x: int32, y: int32)\n"
                      ".decl r(x: int32, w: int32)\n"
                      "a(1, 2). b(2, 3). c(3, 4).\n"
                      "r(x, w) :- a(x, y), b(y, z), c(z, w).\n"
                      "a(x, y) :- r(x, y).\n"
                      "b(x, y) :- r(x, y).\n"
                      "c(x, y) :- r(x, y).\n";

    wl_plan_t *plan = make_plan(src);
    ASSERT(plan != NULL, "plan generation failed");

    const wl_plan_relation_t *r = find_relation(plan, "r");
    ASSERT(r != NULL, "r not found");

    if (using_k_fusion()) {
        /* K_FUSION encapsulates all copies; fd/ff counts are internal */
        uint32_t k_fusions = count_ops(r, WL_PLAN_OP_K_FUSION);
        ASSERT(k_fusions >= 1, "expected K_FUSION operator with ENABLE_K_FUSION=1");
    } else {
        /* For K copies, each copy has K IDB positions:
         * 1 FORCE_DELTA + (K-1) FORCE_FULL = K per copy.
         * Total: K FORCE_DELTA + K*(K-1) FORCE_FULL = K*K annotated ops.
         * K=3: 3 + 6 = 9 total annotated IDB ops. */
        uint32_t fd = count_delta_mode(r, WL_DELTA_FORCE_DELTA);
        uint32_t ff = count_delta_mode(r, WL_DELTA_FORCE_FULL);
        char msg[128];
        snprintf(msg, sizeof(msg), "expected fd+ff=9 (K*K), got fd=%u ff=%u",
                 fd, ff);
        ASSERT(fd + ff == 9, msg);
    }

    wl_plan_free(plan);
    PASS();
}

static void
test_expanded_plan_free(void)
{
    TEST("wl_plan_free handles expanded plan without crash");

    const char *src = ".decl a(x: int32, y: int32)\n"
                      ".decl b(x: int32, y: int32)\n"
                      ".decl c(x: int32, y: int32)\n"
                      ".decl r(x: int32, w: int32)\n"
                      "a(1, 2). b(2, 3). c(3, 4).\n"
                      "r(x, w) :- a(x, y), b(y, z), c(z, w).\n"
                      "a(x, y) :- r(x, y).\n"
                      "b(x, y) :- r(x, y).\n"
                      "c(x, y) :- r(x, y).\n";

    wl_plan_t *plan = make_plan(src);
    ASSERT(plan != NULL, "plan generation failed");
    wl_plan_free(plan);
    PASS();
}

/* ========================================================================
 * K=2 DELTA EXPANSION TESTS (TDD red phase)
 *
 * The K=2 program: a(x,z) :- a(x,y), b(y,z) with b(x,y) :- a(x,y)
 * creates a 2-atom IDB rule.  With the threshold lowered to K >= 2,
 * the plan rewriter should emit exactly 2 FORCE_DELTA copies.
 *
 * These tests FAIL until exec_plan_gen.c lowers the guard to k >= 2.
 * ======================================================================== */

static const char *k2_src = ".decl a(x: int32, y: int32)\n"
                            ".decl b(x: int32, y: int32)\n"
                            "a(1, 2). b(2, 3).\n"
                            "a(x, z) :- a(x, y), b(y, z).\n"
                            "b(x, y) :- a(x, y).\n";

static void
test_2atom_k2_expansion(void)
{
    TEST("K=2: 2-atom rule produces EXACTLY 2 FORCE_DELTA copies");

    wl_plan_t *plan = make_plan(k2_src);
    ASSERT(plan != NULL, "plan generation failed");

    const wl_plan_relation_t *a = find_relation(plan, "a");
    ASSERT(a != NULL, "relation a not found");

    if (using_k_fusion()) {
        uint32_t k_fusions = count_ops(a, WL_PLAN_OP_K_FUSION);
        ASSERT(k_fusions >= 1, "expected K_FUSION operator with ENABLE_K_FUSION=1");
    } else {
        uint32_t force_delta = count_delta_mode(a, WL_DELTA_FORCE_DELTA);
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "expected >= 2 FORCE_DELTA ops for K=2 rule, got %u",
                 force_delta);
        ASSERT(force_delta >= 2, msg);
    }

    wl_plan_free(plan);
    PASS();
}

static void
test_2atom_k2_force_full(void)
{
    TEST("K=2: 2-atom rule has correct FORCE_FULL count");

    wl_plan_t *plan = make_plan(k2_src);
    ASSERT(plan != NULL, "plan generation failed");

    const wl_plan_relation_t *a = find_relation(plan, "a");
    ASSERT(a != NULL, "relation a not found");

    if (using_k_fusion()) {
        /* K_FUSION encapsulates all copies; FORCE_FULL is internal */
        uint32_t k_fusions = count_ops(a, WL_PLAN_OP_K_FUSION);
        ASSERT(k_fusions >= 1, "expected K_FUSION operator with ENABLE_K_FUSION=1");
    } else {
        /* K=2: each copy has 1 FORCE_DELTA + 1 FORCE_FULL.
         * 2 copies => 2 FORCE_FULL total. */
        uint32_t force_full = count_delta_mode(a, WL_DELTA_FORCE_FULL);
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "expected >= 2 FORCE_FULL ops for K=2 rule, got %u",
                 force_full);
        ASSERT(force_full >= 2, msg);
    }

    wl_plan_free(plan);
    PASS();
}

static void
test_2atom_k2_concat_consolidate(void)
{
    TEST("K=2: 2-atom rule has CONCAT and CONSOLIDATE operators");

    wl_plan_t *plan = make_plan(k2_src);
    ASSERT(plan != NULL, "plan generation failed");

    const wl_plan_relation_t *a = find_relation(plan, "a");
    ASSERT(a != NULL, "relation a not found");

    if (using_k_fusion()) {
        /* K_FUSION replaces CONCAT+CONSOLIDATE structure */
        uint32_t k_fusions = count_ops(a, WL_PLAN_OP_K_FUSION);
        ASSERT(k_fusions >= 1, "expected K_FUSION operator with ENABLE_K_FUSION=1");
    } else {
        uint32_t concats = count_ops(a, WL_PLAN_OP_CONCAT);
        char msg_concat[128];
        snprintf(msg_concat, sizeof(msg_concat),
                 "expected >= 2 CONCAT ops for K=2 rule, got %u", concats);
        ASSERT(concats >= 2, msg_concat);

        uint32_t consols = count_ops(a, WL_PLAN_OP_CONSOLIDATE);
        char msg_consol[128];
        snprintf(msg_consol, sizeof(msg_consol),
                 "expected >= 1 CONSOLIDATE op for K=2 rule, got %u", consols);
        ASSERT(consols >= 1, msg_consol);
    }

    wl_plan_free(plan);
    PASS();
}

static void
test_2atom_k2_invariant(void)
{
    TEST("K=2: FORCE_DELTA + FORCE_FULL == K*K == 4");

    wl_plan_t *plan = make_plan(k2_src);
    ASSERT(plan != NULL, "plan generation failed");

    const wl_plan_relation_t *a = find_relation(plan, "a");
    ASSERT(a != NULL, "relation a not found");

    if (using_k_fusion()) {
        /* K_FUSION encapsulates all copies; fd/ff counts are internal */
        uint32_t k_fusions = count_ops(a, WL_PLAN_OP_K_FUSION);
        ASSERT(k_fusions >= 1, "expected K_FUSION operator with ENABLE_K_FUSION=1");
    } else {
        uint32_t fd = count_delta_mode(a, WL_DELTA_FORCE_DELTA);
        uint32_t ff = count_delta_mode(a, WL_DELTA_FORCE_FULL);
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "expected fd+ff=4 (K*K for K=2), got fd=%u ff=%u", fd, ff);
        ASSERT(fd + ff == 4, msg);
    }

    wl_plan_free(plan);
    PASS();
}

static void
test_2atom_k2_no_materialization(void)
{
    TEST("K=2: 2-atom rule has zero materialization hints (K-2=0)");

    wl_plan_t *plan = make_plan(k2_src);
    ASSERT(plan != NULL, "plan generation failed");

    const wl_plan_relation_t *a = find_relation(plan, "a");
    ASSERT(a != NULL, "relation a not found");

    if (using_k_fusion()) {
        /* K_FUSION encapsulates all copies; materialization is internal */
        uint32_t k_fusions = count_ops(a, WL_PLAN_OP_K_FUSION);
        ASSERT(k_fusions >= 1, "expected K_FUSION operator with ENABLE_K_FUSION=1");
    } else {
        /* K=2: K-2 = 0 intermediate joins to materialize per copy. */
        uint32_t mat = count_materialized(a);
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "expected 0 materialized hints for K=2 rule, got %u", mat);
        ASSERT(mat == 0, msg);
    }

    wl_plan_free(plan);
    PASS();
}

static void
test_k1_k3_unaffected(void)
{
    TEST("K=1 not expanded; K=3 still produces 3 FORCE_DELTA copies");

    /* K=1: single-IDB-atom rule (transitive closure with EDB join).
     * The body has only 1 IDB atom (tc) so it should NOT be expanded. */
    const char *tc_src = ".decl edge(x: int32, y: int32)\n"
                         "edge(1, 2). edge(2, 3).\n"
                         ".decl tc(x: int32, y: int32)\n"
                         "tc(x, y) :- edge(x, y).\n"
                         "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wl_plan_t *plan_tc = make_plan(tc_src);
    ASSERT(plan_tc != NULL, "TC plan generation failed");

    const wl_plan_relation_t *tc = find_relation(plan_tc, "tc");
    ASSERT(tc != NULL, "tc not found");

    uint32_t fd_k1 = count_delta_mode(tc, WL_DELTA_FORCE_DELTA);
    char msg_k1[128];
    snprintf(msg_k1, sizeof(msg_k1),
             "K=1 TC should have 0 FORCE_DELTA ops, got %u", fd_k1);
    ASSERT(fd_k1 == 0, msg_k1);

    wl_plan_free(plan_tc);

    /* K=3: 3-atom recursive rule should still produce exactly 3 copies. */
    const char *cspa_src = ".decl a(x: int32, y: int32)\n"
                           ".decl b(x: int32, y: int32)\n"
                           ".decl c(x: int32, y: int32)\n"
                           ".decl r(x: int32, w: int32)\n"
                           "a(1, 2). b(2, 3). c(3, 4).\n"
                           "r(x, w) :- a(x, y), b(y, z), c(z, w).\n"
                           "a(x, y) :- r(x, y).\n"
                           "b(x, y) :- r(x, y).\n"
                           "c(x, y) :- r(x, y).\n";

    wl_plan_t *plan_cspa = make_plan(cspa_src);
    ASSERT(plan_cspa != NULL, "CSPA plan generation failed");

    const wl_plan_relation_t *r = find_relation(plan_cspa, "r");
    ASSERT(r != NULL, "r not found");

    if (using_k_fusion()) {
        uint32_t k_fusions = count_ops(r, WL_PLAN_OP_K_FUSION);
        char msg_k3[128];
        snprintf(msg_k3, sizeof(msg_k3),
                 "K=3 rule should have K_FUSION operator, got %u k_fusions",
                 k_fusions);
        ASSERT(k_fusions >= 1, msg_k3);
    } else {
        uint32_t fd_k3 = count_delta_mode(r, WL_DELTA_FORCE_DELTA);
        char msg_k3[128];
        snprintf(msg_k3, sizeof(msg_k3),
                 "K=3 rule should have 3 FORCE_DELTA ops, got %u", fd_k3);
        ASSERT(fd_k3 == 3, msg_k3);
    }

    wl_plan_free(plan_cspa);
    PASS();
}

/* ========================================================================
 * INTEGRATION TESTS: correctness preservation
 * ======================================================================== */

static void
test_integ_two_way_join_regression(void)
{
    TEST("integ: 2-way TC unaffected by delta expansion (regression guard)");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl tc(x: int32, y: int32)\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n";

    int64_t count = run_program(src, 1);
    ASSERT(count >= 0, "TC evaluation failed");

    char msg[128];
    snprintf(msg, sizeof(msg), "expected 6 TC tuples, got %" PRId64, count);
    ASSERT(count == 6, msg);

    PASS();
}

static void
test_integ_three_way_join_correctness(void)
{
    TEST("integ: 3-way join (3-hop path) correctness");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl path3(x: int32, z: int32)\n"
                      "path3(x, z) :- edge(x, y), edge(y, w), edge(w, z).\n";

    int64_t count = run_program(src, 1);
    ASSERT(count >= 0, "program evaluation failed");

    char msg[128];
    snprintf(msg, sizeof(msg), "expected 1 path3 fact, got %" PRId64, count);
    ASSERT(count == 1, msg);

    PASS();
}

static void
test_integ_cspa_memory_alias(void)
{
    TEST("integ: CSPA memoryAlias 3-atom recursive rule correctness");

    const char *src = ".decl pointsTo(ptr: int32, cell: int32)\n"
                      "pointsTo(1, 10). pointsTo(2, 10). pointsTo(3, 20). "
                      "pointsTo(4, 20).\n"
                      ".decl memoryAlias(x: int32, y: int32)\n"
                      "memoryAlias(x, y) :- pointsTo(x, a), pointsTo(y, a).\n"
                      "memoryAlias(x, z) :- pointsTo(x, a), memoryAlias(a, b), "
                      "pointsTo(z, b).\n";

    int64_t count = run_program(src, 1);
    ASSERT(count >= 0, "CSPA evaluation failed");

    char msg[128];
    snprintf(msg, sizeof(msg), "expected 8 memoryAlias facts, got %" PRId64,
             count);
    ASSERT(count == 8, msg);

    PASS();
}

/* ========================================================================
 * main
 * ======================================================================== */

int
main(void)
{
    printf("=== Option 2 CSE Plan Rewriting Tests ===\n\n");

    /* Plan structure tests */
    test_2atom_no_expansion();
    test_3atom_expansion();
    test_3atom_materialization_hints();
    test_3atom_force_full();
    test_nonrecursive_no_rewrite();
    test_delta_and_full_invariant();
    test_expanded_plan_free();

    /* K=2 delta expansion tests (TDD red phase) */
    printf("\n--- K=2 Delta Expansion Tests (red phase) ---\n");
    test_2atom_k2_expansion();
    test_2atom_k2_force_full();
    test_2atom_k2_concat_consolidate();
    test_2atom_k2_invariant();
    test_2atom_k2_no_materialization();
    test_k1_k3_unaffected();

    /* Integration correctness tests */
    printf("\n--- Integration Tests ---\n");
    test_integ_two_way_join_regression();
    test_integ_three_way_join_correctness();
    test_integ_cspa_memory_alias();

    printf("\n%d tests: %d passed, %d failed\n", test_count, pass_count,
           fail_count);
    return fail_count > 0 ? 1 : 0;
}
