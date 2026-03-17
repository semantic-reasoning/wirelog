/*
 * test_lftj_integration.c - End-to-end integration tests for LFTJ (Issue #195)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates that:
 *   1. wl_plan_from_program emits WL_PLAN_OP_LFTJ for eligible 3+ EDB chains
 *   2. wl_plan_from_program does NOT rewrite 2-way EDB joins (k < 3)
 *   3. 3-way join via session API produces correct results
 *   4. 4-way join via session API produces correct results
 *   5. No false positive: IDB relations are not rewritten as LFTJ
 */

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ----------------------------------------------------------------
 * Test framework
 * ---------------------------------------------------------------- */

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
        return;                      \
    } while (0)

#define ASSERT(cond, msg) \
    do {                  \
        if (!(cond))      \
            FAIL(msg);    \
    } while (0)

/* ----------------------------------------------------------------
 * Tuple collector
 * ---------------------------------------------------------------- */

#define MAX_ROWS 256
#define MAX_NCOLS 16

typedef struct {
    uint32_t count;
    int64_t rows[MAX_ROWS][MAX_NCOLS];
    uint32_t ncols[MAX_ROWS];
    int oom;
} collect_t;

static void
collect_cb(const char *relation, const int64_t *row, uint32_t ncols, void *user)
{
    (void)relation;
    collect_t *c = (collect_t *)user;
    if (c->count >= MAX_ROWS) {
        c->oom = 1;
        return;
    }
    uint32_t idx = c->count++;
    c->ncols[idx] = ncols < MAX_NCOLS ? ncols : MAX_NCOLS;
    for (uint32_t i = 0; i < c->ncols[idx]; i++)
        c->rows[idx][i] = row[i];
}

/* ----------------------------------------------------------------
 * Session helpers
 * ---------------------------------------------------------------- */

/*
 * make_plan: parse + apply all optimizer passes + generate plan.
 * Note: JPP and SIP passes interact with SEMIJOIN schema tracking in a way
 * that can produce incorrect project_indices for 4+ way EDB joins.
 * Use make_plan_no_opt for LFTJ-specific tests to avoid that interference.
 */
static int
make_plan(const char *src, wl_plan_t **out_plan, wirelog_program_t **out_prog)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    *out_plan = plan;
    *out_prog = prog;
    return 0;
}

/*
 * make_plan_no_opt: parse + fusion only (no JPP/SIP) + generate plan.
 * Use for LFTJ tests: JPP/SIP insert SEMIJOIN ops that break the
 * consecutive JOIN chain the LFTJ detector looks for.
 */
static int
make_plan_no_opt(const char *src, wl_plan_t **out_plan,
                 wirelog_program_t **out_prog)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;

    wl_fusion_apply(prog, NULL);

    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    *out_plan = plan;
    *out_prog = prog;
    return 0;
}

static int
run_session_no_opt(const char *src, collect_t *out)
{
    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;

    if (make_plan_no_opt(src, &plan, &prog) != 0)
        return -1;

    wl_session_t *sess = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    if (wl_session_load_facts(sess, prog) != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    memset(out, 0, sizeof(*out));
    int rc = wl_session_snapshot(sess, collect_cb, out);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc;
}

/* ----------------------------------------------------------------
 * Plan inspection helpers
 * ---------------------------------------------------------------- */

/* Count WL_PLAN_OP_LFTJ operators across all strata and relations. */
static uint32_t
count_lftj_ops(const wl_plan_t *plan)
{
    uint32_t n = 0;
    for (uint32_t s = 0; s < plan->stratum_count; s++) {
        const wl_plan_stratum_t *st = &plan->strata[s];
        for (uint32_t r = 0; r < st->relation_count; r++) {
            const wl_plan_relation_t *rel = &st->relations[r];
            for (uint32_t o = 0; o < rel->op_count; o++) {
                if (rel->ops[o].op == WL_PLAN_OP_LFTJ)
                    n++;
            }
        }
    }
    return n;
}

/* ----------------------------------------------------------------
 * Tests
 * ---------------------------------------------------------------- */

/* Test 1: 3-way EDB join is rewritten to LFTJ in the generated plan. */
static void
test_plan_has_lftj(void)
{
    TEST("Plan contains WL_PLAN_OP_LFTJ for 3-way EDB join");

    const char *src = ".decl r1(x: int32, a: int32)\n"
                      ".decl r2(x: int32, b: int32)\n"
                      ".decl r3(x: int32, c: int32)\n"
                      ".decl out(x: int32, a: int32, b: int32, c: int32)\n"
                      "r1(1, 10). r2(1, 100). r3(1, 1000).\n"
                      "out(x, a, b, c) :- r1(x, a), r2(x, b), r3(x, c).\n";

    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    ASSERT(make_plan_no_opt(src, &plan, &prog) == 0, "make_plan_no_opt failed");

    uint32_t n = count_lftj_ops(plan);
    wl_plan_free(plan);
    wirelog_program_free(prog);

    ASSERT(n >= 1, "expected >= 1 WL_PLAN_OP_LFTJ, got 0");
    PASS();
}

/* Test 2: 2-way EDB join is NOT rewritten to LFTJ (k < 3). */
static void
test_plan_no_lftj_for_binary(void)
{
    TEST("Plan does NOT contain WL_PLAN_OP_LFTJ for 2-way EDB join");

    const char *src = ".decl r1(x: int32, a: int32)\n"
                      ".decl r2(x: int32, b: int32)\n"
                      ".decl out(x: int32, a: int32, b: int32)\n"
                      "r1(1, 10). r2(1, 100).\n"
                      "out(x, a, b) :- r1(x, a), r2(x, b).\n";

    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    ASSERT(make_plan(src, &plan, &prog) == 0, "make_plan failed");

    uint32_t n = count_lftj_ops(plan);
    wl_plan_free(plan);
    wirelog_program_free(prog);

    ASSERT(n == 0, "2-way EDB join incorrectly rewritten as LFTJ");
    PASS();
}

/* Test 3: 3-way join produces correct result tuples via session. */
static void
test_3way_join_result(void)
{
    TEST("3-way EDB join produces correct result via session");

    /*
     * r1(x, a): (1,10) (2,20) (3,30)
     * r2(x, b): (1,100) (2,200) (4,400)
     * r3(x, c): (1,1000) (3,3000) (5,5000)
     *
     * out(x, a, b, c) :- r1(x,a), r2(x,b), r3(x,c)
     * Only x=1 is in all three: out(1, 10, 100, 1000)
     */
    const char *src = ".decl r1(x: int32, a: int32)\n"
                      ".decl r2(x: int32, b: int32)\n"
                      ".decl r3(x: int32, c: int32)\n"
                      ".decl out(x: int32, a: int32, b: int32, c: int32)\n"
                      "r1(1, 10). r1(2, 20). r1(3, 30).\n"
                      "r2(1, 100). r2(2, 200). r2(4, 400).\n"
                      "r3(1, 1000). r3(3, 3000). r3(5, 5000).\n"
                      "out(x, a, b, c) :- r1(x, a), r2(x, b), r3(x, c).\n";

    collect_t coll;
    ASSERT(run_session_no_opt(src, &coll) == 0, "run_session_no_opt failed");
    ASSERT(!coll.oom, "tuple collector overflowed");

    /* Filter to 4-column result tuples (r1/r2/r3 have 2 cols each). */
    uint32_t out_count = 0;
    bool found_expected = false;
    for (uint32_t i = 0; i < coll.count; i++) {
        if (coll.ncols[i] != 4)
            continue;
        out_count++;
        if (coll.rows[i][0] == 1 && coll.rows[i][1] == 10
            && coll.rows[i][2] == 100 && coll.rows[i][3] == 1000)
            found_expected = true;
    }

    ASSERT(out_count == 1, "expected exactly 1 output tuple");
    ASSERT(found_expected, "expected out(1,10,100,1000) not found");
    PASS();
}

/* Test 4: 4-way join produces correct result via session. */
static void
test_4way_join_result(void)
{
    TEST("4-way EDB join produces correct result via session");

    /*
     * r1(x, a): (1,11) (2,21) (3,31)
     * r2(x, b): (1,12) (2,22) (4,42)
     * r3(x, c): (1,13) (3,33) (5,53)
     * r4(x, d): (1,14) (2,24) (6,64)
     *
     * out(x,a,b,c,d) :- r1(x,a), r2(x,b), r3(x,c), r4(x,d)
     * x=1 is in all four: out(1, 11, 12, 13, 14)
     */
    const char *src
        = ".decl r1(x: int32, a: int32)\n"
          ".decl r2(x: int32, b: int32)\n"
          ".decl r3(x: int32, c: int32)\n"
          ".decl r4(x: int32, d: int32)\n"
          ".decl out(x: int32, a: int32, b: int32, c: int32, d: int32)\n"
          "r1(1, 11). r1(2, 21). r1(3, 31).\n"
          "r2(1, 12). r2(2, 22). r2(4, 42).\n"
          "r3(1, 13). r3(3, 33). r3(5, 53).\n"
          "r4(1, 14). r4(2, 24). r4(6, 64).\n"
          "out(x, a, b, c, d) :- r1(x, a), r2(x, b), r3(x, c), r4(x, d).\n";

    collect_t coll;
    ASSERT(run_session_no_opt(src, &coll) == 0, "run_session_no_opt failed");
    ASSERT(!coll.oom, "tuple collector overflowed");

    uint32_t out_count = 0;
    bool found_expected = false;
    for (uint32_t i = 0; i < coll.count; i++) {
        if (coll.ncols[i] != 5)
            continue;
        out_count++;
        if (coll.rows[i][0] == 1 && coll.rows[i][1] == 11
            && coll.rows[i][2] == 12 && coll.rows[i][3] == 13
            && coll.rows[i][4] == 14)
            found_expected = true;
    }

    ASSERT(out_count == 1, "expected exactly 1 output tuple");
    ASSERT(found_expected, "expected out(1,11,12,13,14) not found");
    PASS();
}

/* Test 5: IDB relation in chain prevents LFTJ rewrite. */
static void
test_idb_not_rewritten(void)
{
    TEST("IDB right_relation in chain is not rewritten as LFTJ");

    /*
     * tc is IDB (derived from edge), so the join chain
     * r1 ⋈ tc ⋈ r2 is NOT eligible for LFTJ.
     */
    const char *src = ".decl edge(x: int32, y: int32)\n"
                      ".decl r1(x: int32, a: int32)\n"
                      ".decl r2(x: int32, b: int32)\n"
                      ".decl tc(x: int32, y: int32)\n"
                      ".decl out(x: int32)\n"
                      "edge(1, 2). edge(2, 3).\n"
                      "r1(1, 10). r2(1, 100).\n"
                      "tc(x, y) :- edge(x, y).\n"
                      "tc(x, z) :- tc(x, y), edge(y, z).\n"
                      "out(x) :- r1(x, a), tc(x, y), r2(x, b).\n";

    wirelog_program_t *prog = NULL;
    wl_plan_t *plan = NULL;
    ASSERT(make_plan(src, &plan, &prog) == 0, "make_plan failed");

    uint32_t n = count_lftj_ops(plan);
    wl_plan_free(plan);
    wirelog_program_free(prog);

    /* The out-relation plan contains tc (IDB) -> no LFTJ rewrite for out */
    ASSERT(n == 0, "IDB-containing chain incorrectly rewritten as LFTJ");
    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== LFTJ Integration Tests (Issue #195) ===\n");

    test_plan_has_lftj();
    test_plan_no_lftj_for_binary();
    test_3way_join_result();
    test_4way_join_result();
    test_idb_not_rewritten();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n");

    return fail_count > 0 ? 1 : 0;
}
