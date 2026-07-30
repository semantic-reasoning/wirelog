/*
 * test_plan_gen.c - Unit tests for wl_plan_from_program() and wl_plan_free()
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/backend.h"
#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/intern.h"
#include "../wirelog/ir/program.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ----------------------------------------------------------------
 * Test framework
 * ---------------------------------------------------------------- */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                    \
        do {                                              \
            test_count++;                                 \
            printf("TEST %d: %s ... ", test_count, name); \
        } while (0)

#define PASS()            \
        do {                  \
            pass_count++;     \
            printf("PASS\n"); \
        } while (0)

#define FAIL(msg)                  \
        do {                           \
            fail_count++;              \
            printf("FAIL: %s\n", msg); \
        } while (0)

#define ASSERT(cond, msg) \
        do {                  \
            if (!(cond)) {    \
                FAIL(msg);    \
                return;       \
            }                 \
        } while (0)

/* ----------------------------------------------------------------
 * Snapshot counting callback
 * ---------------------------------------------------------------- */

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

/* ----------------------------------------------------------------
 * Test: Simple TC program generates valid plan
 * ---------------------------------------------------------------- */

static void
test_tc_plan_generation(void)
{
    TEST("TC plan generation");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2). edge(2, 3). edge(3, 4).\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "wl_plan_from_program failed");
    ASSERT(plan != NULL, "plan is NULL");

    /* Should have at least 1 stratum */
    ASSERT(plan->stratum_count >= 1, "no strata");

    /* Should have at least 1 relation (tc) in some stratum */
    int found_tc = 0;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            if (plan->strata[s].relations[r].name
                && strcmp(plan->strata[s].relations[r].name, "tc") == 0) {
                found_tc = 1;
                ASSERT(plan->strata[s].relations[r].op_count > 0,
                    "tc has no ops");
            }
        }
    }
    ASSERT(found_tc, "tc relation not found in plan");

    /* edge should be an EDB */
    int found_edge_edb = 0;
    for (uint32_t i = 0; i < plan->edb_count; i++) {
        if (plan->edb_relations[i]
            && strcmp(plan->edb_relations[i], "edge") == 0)
            found_edge_edb = 1;
    }
    ASSERT(found_edge_edb, "edge not in EDB list");

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_plan_generation_preinterns_static_string_literals(void)
{
    TEST("plan generation pre-interns static string literals");

    const char *src = ".decl trigger(value: symbol)\n"
        ".decl unused(value: symbol, plane: symbol)\n"
        "unused(X, \"data\") :- trigger(X).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");
    ASSERT(wl_intern_get(prog->intern, "data") < 0,
        "parser unexpectedly interned projection literal");

    ASSERT(wl_fusion_apply(prog, NULL) == 0, "fusion failed");
    ASSERT(wl_jpp_apply(prog, NULL) == 0, "JPP failed");
    ASSERT(wl_sip_apply(prog, NULL) == 0, "SIP failed");

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");
    ASSERT(wl_intern_get(prog->intern, "data") >= 0,
        "plan generation did not intern projection literal");

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ----------------------------------------------------------------
 * Test: TC end-to-end via columnar session
 * ---------------------------------------------------------------- */

static void
test_tc_end_to_end(void)
{
    TEST("TC end-to-end columnar session");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2). edge(2, 3). edge(3, 4).\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
    ASSERT(rc == 0, "session create failed");

    rc = wl_session_load_facts(sess, prog);
    ASSERT(rc == 0, "load facts failed");

    struct count_ctx ctx = { 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    ASSERT(rc == 0, "snapshot failed");
    ASSERT(ctx.count > 0, "no tuples produced");

    /* TC on 1->2->3->4 should produce 6 tuples:
     * tc(1,2), tc(2,3), tc(3,4), tc(1,3), tc(2,4), tc(1,4) */
    ASSERT(ctx.count == 6, "expected 6 TC tuples");

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_join_project_fusion_plan_shape(void)
{
    TEST("JOIN project fusion plan shape");

    const char *src = ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, z: int32)\n"
        ".decl r(x: int32, z: int32)\n"
        "r(x, z) :- a(x, y), b(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    const wl_plan_relation_t *rp = NULL;
    for (uint32_t s = 0; s < plan->stratum_count && !rp; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            const wl_plan_relation_t *cand = &plan->strata[s].relations[r];
            if (cand->name && strcmp(cand->name, "r") == 0) {
                rp = cand;
                break;
            }
        }
    }
    ASSERT(rp != NULL, "relation r not found");

    const wl_plan_op_t *join = NULL;
    for (uint32_t i = 0; i < rp->op_count; i++) {
        ASSERT(rp->ops[i].op != WL_PLAN_OP_MAP,
            "pure join projection should not leave MAP");
        if (rp->ops[i].op == WL_PLAN_OP_JOIN)
            join = &rp->ops[i];
    }
    ASSERT(join != NULL, "JOIN not found");
    ASSERT(join->project_count == 2, "JOIN should carry projection width");
    ASSERT(join->project_indices != NULL, "JOIN projection indices missing");
    ASSERT(join->project_indices[0] == 0, "first projected column should be x");
    ASSERT(join->project_indices[1] == 3,
        "second projected column should be z");

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_join_project_fusion_keeps_computed_map(void)
{
    TEST("JOIN project fusion keeps computed MAP");

    const char *src = ".decl a(x: int32, y: int32)\n"
        ".decl b(y: int32, z: int32)\n"
        ".decl r(x: int32, z: int32)\n"
        "r(x + 1, z) :- a(x, y), b(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    bool found_map = false;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count; r++) {
            const wl_plan_relation_t *rp = &plan->strata[s].relations[r];
            if (!rp->name || strcmp(rp->name, "r") != 0)
                continue;
            for (uint32_t i = 0; i < rp->op_count; i++)
                if (rp->ops[i].op == WL_PLAN_OP_MAP)
                    found_map = true;
        }
    }
    ASSERT(found_map, "computed projection should keep MAP");

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

static void
test_k_fusion_sequences_fuse_join_project(void)
{
    TEST("K_FUSION sequences fuse JOIN project");

    const char *src = ".decl p(x: int32, y: int32)\n"
        "p(1, 2).\n"
        "p(x, z) :- p(x, y), p(y, z).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    ASSERT(prog != NULL, "parse failed");

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    int rc = wl_plan_from_program(prog, &plan);
    ASSERT(rc == 0, "plan generation failed");

    const wl_plan_op_k_fusion_t *kf = NULL;
    for (uint32_t s = 0; s < plan->stratum_count && !kf; s++) {
        for (uint32_t r = 0; r < plan->strata[s].relation_count && !kf; r++) {
            const wl_plan_relation_t *rel = &plan->strata[s].relations[r];
            if (!rel->name || strcmp(rel->name, "p") != 0)
                continue;
            for (uint32_t i = 0; i < rel->op_count; i++) {
                if (rel->ops[i].op == WL_PLAN_OP_K_FUSION) {
                    kf = (const wl_plan_op_k_fusion_t *)
                        rel->ops[i].opaque_data;
                    break;
                }
            }
        }
    }
    ASSERT(kf != NULL, "K_FUSION not found");
    ASSERT(kf->k >= 2, "K_FUSION should contain delta copies");

    for (uint32_t d = 0; d < kf->k; d++) {
        bool projected_join = false;
        for (uint32_t i = 0; i < kf->k_op_counts[d]; i++) {
            const wl_plan_op_t *op = &kf->k_ops[d][i];
            ASSERT(op->op != WL_PLAN_OP_MAP,
                "pure projection MAP should be fused inside K_FUSION");
            if (op->op == WL_PLAN_OP_JOIN && op->project_count == 2
                && op->project_indices)
                projected_join = true;
        }
        ASSERT(projected_join, "projected JOIN not found in K_FUSION copy");
    }

    wl_plan_free(plan);
    wirelog_program_free(prog);
    PASS();
}

/* ----------------------------------------------------------------
 * Test: wl_plan_free NULL-safe
 * ---------------------------------------------------------------- */

static void
test_plan_free_null(void)
{
    TEST("wl_plan_free NULL-safe");
    wl_plan_free(NULL); /* should not crash */
    PASS();
}

/* ----------------------------------------------------------------
 * Test: wl_session_load_facts NULL-safe
 * ---------------------------------------------------------------- */

static void
test_load_facts_null_safe(void)
{
    TEST("wl_session_load_facts NULL-safe");
    int rc = wl_session_load_facts(NULL, NULL);
    ASSERT(rc == -1, "expected -1 for NULL args");
    PASS();
}

/* ----------------------------------------------------------------
 * Main
 * ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== Plan Generator Tests ===\n");

    test_tc_plan_generation();
    test_plan_generation_preinterns_static_string_literals();
    test_tc_end_to_end();
    test_join_project_fusion_plan_shape();
    test_join_project_fusion_keeps_computed_map();
    test_k_fusion_sequences_fuse_join_project();
    test_plan_free_null();
    test_load_facts_null_safe();

    printf("\n%d tests: %d passed, %d failed\n", test_count, pass_count,
        fail_count);
    return fail_count > 0 ? 1 : 0;
}
