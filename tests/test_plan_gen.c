/*
 * test_plan_gen.c - Unit tests for wl_plan_from_program() and wl_plan_free()
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 */

#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/backend.h"
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
    test_tc_end_to_end();
    test_plan_free_null();
    test_load_facts_null_safe();

    printf("\n%d tests: %d passed, %d failed\n", test_count, pass_count,
           fail_count);
    return fail_count > 0 ? 1 : 0;
}
