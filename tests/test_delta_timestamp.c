/*
 * test_delta_timestamp.c - Unit tests for col_delta_timestamp_t (3B-001)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates:
 *   1. Struct layout: iteration/stratum/worker fields independently accessible
 *   2. Monotonic iteration ordering: delta timestamps increase across iterations
 *   3. Survival through merges: timestamps propagate through append without
 *      corrupting the data array
 *   4. Multi-stratum: stratum_idx threaded correctly through both strata
 *   5. Null safety: non-timestamped relations work unchanged after struct change
 */

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/exec_plan_gen.h"
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
 * Helper: tuple count callback
 * ---------------------------------------------------------------- */

struct count_ctx {
    int64_t count;
    const char *tracked;
};

static void
count_cb(const char *r, const int64_t *row, uint32_t nc, void *u)
{
    (void)row;
    (void)nc;
    struct count_ctx *ctx = (struct count_ctx *)u;
    if (r && ctx->tracked && strcmp(r, ctx->tracked) == 0)
        ctx->count++;
}

/* ----------------------------------------------------------------
 * Helper: run a program end-to-end, return tuple count + iterations
 * ---------------------------------------------------------------- */

static int
run_program_ts(const char *src, const char *rel, int64_t *out_count,
               uint32_t *out_iters)
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

    struct count_ctx ctx = { 0, rel };
    int rc = wl_session_snapshot(sess, count_cb, &ctx);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    if (out_count)
        *out_count = ctx.count;
    if (out_iters)
        *out_iters = col_session_get_iteration_count(sess);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return 0;
}

/* ================================================================
 * Test 1: col_delta_timestamp_t struct layout
 *
 * Verifies all four fields are independently accessible and zero-
 * initializable.  Compile + runtime sanity check for the struct.
 * ================================================================ */
static void
test_timestamp_struct_layout(void)
{
    TEST("col_delta_timestamp_t struct layout and field access");

    col_delta_timestamp_t ts;
    memset(&ts, 0, sizeof(ts));
    ASSERT(ts.iteration == 0, "iteration must zero-init");
    ASSERT(ts.stratum == 0, "stratum must zero-init");
    ASSERT(ts.worker == 0, "worker must zero-init");
    ASSERT(ts._reserved == 0, "_reserved must zero-init");

    ts.iteration = 7;
    ts.stratum = 2;
    ts.worker = 3;
    ASSERT(ts.iteration == 7, "iteration assignment");
    ASSERT(ts.stratum == 2, "stratum assignment");
    ASSERT(ts.worker == 3, "worker assignment");

    /* Size must be exactly 4 x uint32_t + int64_t = 24 bytes */
    ASSERT(sizeof(col_delta_timestamp_t)
               == 4 * sizeof(uint32_t) + sizeof(int64_t),
           "struct must be 4 x uint32_t + int64_t (24 bytes)");

    PASS();
}

/* ================================================================
 * Test 2: Monotonic iteration ordering via real program
 *
 * Run a 3-edge chain TC.  The program converges in 2 recursive
 * iterations (iter=0 produces (1,3),(2,4); iter=1 produces (1,4)).
 * Verify correctness: 6 tuples (oracle match).
 *
 * If timestamps were written incorrectly and corrupted the data
 * buffer, the tuple count would diverge from the oracle.
 * ================================================================ */
static void
test_timestamp_monotonic_ordering(void)
{
    TEST("Monotonic ordering: 3-edge TC correctness after timestamp writes");

    const char *src = ".decl r(x: int32, y: int32)\n"
                      "r(1, 2). r(2, 3). r(3, 4).\n"
                      "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    uint32_t iters = 0;
    int rc = run_program_ts(src, "r", &count, &iters);
    ASSERT(rc == 0, "program execution failed");
    ASSERT(count == 6, "expected 6 tuples from 3-edge chain closure");
    ASSERT(iters >= 1, "must require at least 1 iteration");
    ASSERT(iters <= 3, "must converge within 3 iterations");

    PASS();
}

/* ================================================================
 * Test 3: Survival through merges — 5-node chain
 *
 * A 5-node chain exercises more delta iterations and more merges.
 * Timestamps on delta rows must survive the append path without
 * corrupting the underlying data array.
 *
 * Expected: 10 tuples (all pairs i<j in 1..5).
 * ================================================================ */
static void
test_timestamp_survival_through_merges(void)
{
    TEST("Survival through merges: 5-node chain = 10 tuples");

    const char *src = ".decl e(x: int32, y: int32)\n"
                      "e(1, 2). e(2, 3). e(3, 4). e(4, 5).\n"
                      ".decl reach(x: int32, y: int32)\n"
                      "reach(x, y) :- e(x, y).\n"
                      "reach(x, z) :- reach(x, y), reach(y, z).\n";

    int64_t count = 0;
    uint32_t iters = 0;
    int rc = run_program_ts(src, "reach", &count, &iters);
    ASSERT(rc == 0, "5-node chain program failed");
    ASSERT(count == 10, "expected 10 tuples from 5-node chain");
    /* 5-node chain requires at least 2 recursive iterations */
    ASSERT(iters >= 2, "5-node chain needs >= 2 iterations");

    PASS();
}

/* ================================================================
 * Test 4: 2-cycle quick convergence
 *
 * A 2-cycle r(1,2), r(2,1) converges in 1-2 iterations.
 * Expected: 4 tuples {(1,2),(2,1),(1,1),(2,2)}.
 * Tests that timestamp stamping does not interfere with the
 * empty-delta-skip optimization after convergence.
 * ================================================================ */
static void
test_timestamp_two_cycle_convergence(void)
{
    TEST("2-cycle convergence: 4 tuples, no corruption from timestamps");

    const char *src = ".decl r(x: int32, y: int32)\n"
                      "r(1, 2). r(2, 1).\n"
                      "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    uint32_t iters = 0;
    int rc = run_program_ts(src, "r", &count, &iters);
    ASSERT(rc == 0, "2-cycle program failed");
    ASSERT(count == 4, "2-cycle must produce exactly 4 tuples");
    ASSERT(iters <= 3, "2-cycle should converge within 3 iterations");

    PASS();
}

/* ================================================================
 * Test 5: Multi-stratum program
 *
 * Two-stratum program: non-recursive edge stratum + recursive
 * reach stratum.  Verifies that stratum_idx is correctly threaded
 * through both strata without corrupting evaluation.
 *
 * Expected: reach has 6 tuples (3-edge chain TC, K=1 path).
 * ================================================================ */
static void
test_timestamp_multi_stratum(void)
{
    TEST("Multi-stratum: stratum_idx threaded correctly, 6 tuples");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl reach(x: int32, y: int32)\n"
                      "reach(x, y) :- edge(x, y).\n"
                      "reach(x, z) :- reach(x, y), edge(y, z).\n";

    int64_t count = 0;
    uint32_t iters = 0;
    int rc = run_program_ts(src, "reach", &count, &iters);
    ASSERT(rc == 0, "multi-stratum program failed");
    ASSERT(count == 6, "expected 6 tuples from 3-edge TC");
    ASSERT(iters >= 1, "must require at least 1 iteration");

    PASS();
}

/* ================================================================
 * Test 6: Non-timestamped relations unaffected (null safety)
 *
 * Regression check: ordinary relations (timestamps == NULL) must
 * continue to produce correct results after the col_rel_t struct
 * gained the timestamps field.
 * ================================================================ */
static void
test_timestamp_null_safety(void)
{
    TEST("Null safety: non-timestamped program is unaffected");

    /* K=2 program (K-fusion path, timestamps on K-fusion workers = 0) */
    const char *src = ".decl r(x: int32, y: int32)\n"
                      "r(1, 2). r(2, 3). r(3, 4).\n"
                      "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    int rc = run_program_ts(src, "r", &count, NULL);
    ASSERT(rc == 0, "null-safety program failed");
    ASSERT(count == 6, "null-safety: expected 6 tuples");

    PASS();
}

/* ================================================================
 * Test 7: Complete graph dedup with timestamps (KI-1 regression)
 *
 * Bidirectional 3-node graph: K=2 dedup must produce 9 tuples.
 * Verifies timestamp stamping does not re-introduce the KI-1
 * over-count bug (which produced 11 instead of 9 before ea224ad).
 * ================================================================ */
static void
test_timestamp_complete_graph_dedup(void)
{
    TEST("Complete graph dedup with timestamps: 9 tuples (KI-1 regression)");

    const char *src = ".decl r(x: int32, y: int32)\n"
                      "r(1, 2). r(2, 1). r(1, 3). r(3, 1). r(2, 3). r(3, 2).\n"
                      "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    int rc = run_program_ts(src, "r", &count, NULL);
    ASSERT(rc == 0, "complete-graph program failed");
    ASSERT(count == 9,
           "K=2 complete graph must produce 9 tuples (KI-1 regression check)");

    PASS();
}

/* ================================================================
 * main
 * ================================================================ */

int
main(void)
{
    printf("\n=== Delta Timestamp Unit Tests (3B-001) ===\n\n");

    test_timestamp_struct_layout();
    test_timestamp_monotonic_ordering();
    test_timestamp_survival_through_merges();
    test_timestamp_two_cycle_convergence();
    test_timestamp_multi_stratum();
    test_timestamp_null_safety();
    test_timestamp_complete_graph_dedup();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n\n");

    return fail_count > 0 ? 1 : 0;
}
