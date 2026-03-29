/*
 * test_k_fusion_e2e.c - K-Fusion End-to-End Integration Tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Tests that K-fusion dispatch (col_op_k_fusion) produces correct results
 * via the real session API. Validates correctness of K=2 parallel evaluation
 * against known baselines, not just mock structures.
 *
 * Test cases:
 *   1. K=2 recursive join produces correct tuple count
 *   2. K=2 vs K=1 parity (same TC result via different paths)
 *   3. Iteration count correctness (fixed-point semantics preserved)
 *   4. Empty-delta skip works with K-fusion (no extra iterations)
 *   5. Larger graph K=2 correctness (5-node chain)
 *   6. Convergence: K=2 produces no duplicate tuples
 */

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/exec_plan_gen.h"
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
 * Test framework (matches wirelog convention)
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
 * Tuple counting callback
 * ---------------------------------------------------------------- */

struct result_ctx {
    int64_t total;
    int64_t rel_count; /* tuples in tracked relation */
    const char *tracked_rel;
};

static void
count_tuples_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct result_ctx *ctx = (struct result_ctx *)user_data;
    ctx->total++;
    if (relation && ctx->tracked_rel && strcmp(relation, ctx->tracked_rel) == 0)
        ctx->rel_count++;
    (void)row;
    (void)ncols;
}

/* ----------------------------------------------------------------
 * Helper: run a program end-to-end, return tuple count + iterations
 * ---------------------------------------------------------------- */

static int
run_program(const char *src, const char *tracked_rel, int64_t *out_count,
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
    int rc = wl_plan_from_program(prog, &plan);
    if (rc != 0) {
        wirelog_program_free(prog);
        return -1;
    }

    wl_session_t *sess = NULL;
    rc = wl_session_create(wl_backend_columnar(), plan, 1, &sess);
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

    struct result_ctx ctx = { 0, 0, tracked_rel };
    rc = wl_session_snapshot(sess, count_tuples_cb, &ctx);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    if (out_count)
        *out_count = ctx.rel_count;
    if (out_iters)
        *out_iters = col_session_get_iteration_count(sess);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return 0;
}

/* ================================================================
 * Test 1: K=2 recursive join produces correct tuple count
 *
 * r(x,z) :- r(x,y), r(y,z).  <- K=2 body (two r atoms -> K-fusion)
 *
 * Base: r(1,2), r(2,3), r(3,4)
 * Expected closure: {(1,2),(2,3),(3,4),(1,3),(2,4),(1,4)} = 6 tuples
 *
 * This is the primary K-fusion correctness test: verifies that parallel
 * evaluation of two r-copy variants produces the same result as the
 * sequential semi-naive baseline.
 * ================================================================ */
static void
test_k2_recursive_join_correct_count(void)
{
    TEST("K=2 recursive join produces 6-tuple closure");

    const char *src = ".decl r(x: int32, y: int32)\n"
        "r(1, 2). r(2, 3). r(3, 4).\n"
        "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    int rc = run_program(src, "r", &count, NULL);
    ASSERT(rc == 0, "K=2 program execution failed");
    ASSERT(count == 6, "expected 6 tuples from 3-edge chain K=2 closure");

    PASS();
}

/* ================================================================
 * Test 2: K=1 vs K=2 parity on TC problem
 *
 * K=1 path: tc(x,z) :- tc(x,y), edge(y,z).  -- no K-fusion
 * K=2 path: tc(x,z) :- tc(x,y), tc(y,z).    -- K-fusion triggered
 *
 * Both programs on edge(1,2), edge(2,3), edge(3,4) should produce
 * the same 6-tuple TC result. Verifies K-fusion parity with non-fusion.
 * ================================================================ */
static void
test_k1_vs_k2_tc_parity(void)
{
    TEST("K=1 and K=2 TC produce same result (parity)");

    /* K=1: single tc in body, no K-fusion */
    const char *src_k1 = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2). edge(2, 3). edge(3, 4).\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    /* K=2: two tc atoms in body, triggers K-fusion */
    const char *src_k2 = ".decl e(x: int32, y: int32)\n"
        "e(1, 2). e(2, 3). e(3, 4).\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- e(x, y).\n"
        "tc(x, z) :- tc(x, y), tc(y, z).\n";

    int64_t count_k1 = 0, count_k2 = 0;
    int rc1 = run_program(src_k1, "tc", &count_k1, NULL);
    int rc2 = run_program(src_k2, "tc", &count_k2, NULL);

    ASSERT(rc1 == 0, "K=1 program failed");
    ASSERT(rc2 == 0, "K=2 program failed");
    ASSERT(count_k1 == 6, "K=1 expected 6 tuples");
    ASSERT(count_k2 == 6, "K=2 expected 6 tuples");
    ASSERT(count_k1 == count_k2, "K=1 and K=2 must agree on tuple count");

    PASS();
}

/* ================================================================
 * Test 3: Iteration count correctness
 *
 * K=2 evaluation must converge in the same number of iterations
 * as the sequential baseline. For a 3-edge chain TC, the K=2
 * closure converges in at most 3 iterations.
 * ================================================================ */
static void
test_k2_iteration_count(void)
{
    TEST("K=2 converges in correct iteration count");

    const char *src = ".decl r(x: int32, y: int32)\n"
        "r(1, 2). r(2, 3). r(3, 4).\n"
        "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    uint32_t iters = 0;
    int rc = run_program(src, "r", &count, &iters);
    ASSERT(rc == 0, "K=2 program failed");
    ASSERT(count == 6, "expected 6 tuples");
    ASSERT(iters <= 3, "K=2 should converge within 3 iterations");
    ASSERT(iters >= 1, "must require at least 1 iteration");

    PASS();
}

/* ================================================================
 * Test 4: Empty delta skip works with K-fusion
 *
 * Once a K=2 recursive relation converges, further iterations
 * should not produce extra tuples. The empty-delta skip optimization
 * must cooperate with K-fusion dispatch.
 *
 * Verify: tuple count does not change after fixpoint.
 * ================================================================ */
static void
test_k2_empty_delta_skip(void)
{
    TEST("K=2 empty delta skip: no extra tuples after fixpoint");

    /* Program where the recursive relation converges quickly */
    const char *src = ".decl r(x: int32, y: int32)\n"
        "r(1, 2). r(2, 1).\n"
        "r(x, z) :- r(x, y), r(y, z).\n";

    /* r(1,2) and r(2,1) form a 2-cycle.
     * Expected closure: {(1,2),(2,1),(1,1),(2,2)} = 4 tuples */
    int64_t count = 0;
    uint32_t iters = 0;
    int rc = run_program(src, "r", &count, &iters);
    ASSERT(rc == 0, "K=2 2-cycle program failed");
    ASSERT(count == 4, "2-cycle K=2 should produce 4 tuples");
    ASSERT(iters <= 3, "2-cycle should converge within 3 iterations");

    PASS();
}

/* ================================================================
 * Test 5: Larger graph K=2 correctness (5-node chain)
 *
 * 5-node chain: e(1,2), e(2,3), e(3,4), e(4,5)
 * Full closure has 10 tuples: all pairs (i,j) with i<j in 1..5
 * ================================================================ */
static void
test_k2_larger_graph(void)
{
    TEST("K=2 larger graph: 5-node chain produces 10 tuples");

    const char *src = ".decl e(x: int32, y: int32)\n"
        "e(1, 2). e(2, 3). e(3, 4). e(4, 5).\n"
        ".decl reach(x: int32, y: int32)\n"
        "reach(x, y) :- e(x, y).\n"
        "reach(x, z) :- reach(x, y), reach(y, z).\n";

    int64_t count = 0;
    int rc = run_program(src, "reach", &count, NULL);
    ASSERT(rc == 0, "5-node chain K=2 program failed");
    ASSERT(count == 10, "5-node chain K=2 should produce 10 tuples");

    PASS();
}

/* ================================================================
 * Test 6: K=2 produces no duplicate tuples (complete graph dedup)
 *
 * The K-fusion merge step must deduplicate results from K workers.
 * A complete bidirectional 3-node graph maximally exercises dedup:
 * both K=2 workers produce large overlapping result sets.
 *
 * Previously produced 11 tuples (KI-1 bug). Fixed in Phase 3A by
 * one-time sort of pre-existing IDB data in col_eval_stratum().
 * Correct answer: 6 base + 3 self-loops = 9 unique tuples.
 * ================================================================ */
static void
test_k2_complete_graph_dedup(void)
{
    TEST("K=2 complete graph dedup: 3-node bidirectional graph = 9 tuples");

    const char *src = ".decl r(x: int32, y: int32)\n"
        "r(1, 2). r(2, 1). r(1, 3). r(3, 1). r(2, 3). r(3, 2).\n"
        "r(x, z) :- r(x, y), r(y, z).\n";

    /* Complete bidirectional graph: adds self-loops (1,1),(2,2),(3,3).
     * 6 base + 3 self-loops = 9 unique tuples. KI-1 fix required. */
    int64_t count = 0;
    int rc = run_program(src, "r", &count, NULL);
    ASSERT(rc == 0, "K=2 complete-graph program failed");
    ASSERT(
        count == 9,
        "K=2 complete graph should produce 9 tuples (KI-1 fix verification)");

    PASS();
}

/* ================================================================
 * Test 7: Single-node self-loop (K=2 trivial case)
 *
 * r(1,1). r(x,z) :- r(x,y), r(y,z).
 * Expected: {(1,1)} = 1 tuple (no new tuples added by recursive rule)
 * ================================================================ */
static void
test_k2_single_self_loop(void)
{
    TEST("K=2 single self-loop: converges to 1 tuple");

    const char *src = ".decl r(x: int32, y: int32)\n"
        "r(1, 1).\n"
        "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    int rc = run_program(src, "r", &count, NULL);
    ASSERT(rc == 0, "K=2 self-loop program failed");
    ASSERT(count == 1, "self-loop K=2 should produce exactly 1 tuple");

    PASS();
}

/* ================================================================
 * Test 8: Multi-rule UNION with K=2 self-join + K=1 EDB-IDB rule
 *
 * Issue #370: CSPA pattern — a relation with multiple recursive rules
 * where one has a K=2 self-join and another has a K=1 EDB-IDB join.
 * The per-segment delta skip must neuter the K=1 segment in workers
 * handling K=2 delta positions and vice versa, while preserving
 * correctness (EDB base-case rules must still fire at iteration 0).
 *
 * vF(y,x) :- assign(y,x).           -- EDB base case
 * vF(x,y) :- vF(x,z), vF(z,y).      -- K=2 self-join (transitive closure)
 * vF(x,y) :- assign(x,z), mA(z,y).  -- K=1 EDB-IDB
 * mA(x,x) :- assign(_,x).           -- base case for mA
 *
 * assign: (1,2), (2,3), (3,4)
 * Expected: vF produces transitive closure of assign + mA contributions.
 * ================================================================ */
static void
test_k2_multi_rule_union_cspa_pattern(void)
{
    TEST("K=2 multi-rule UNION (CSPA pattern) correctness");

    const char *src =
        ".decl assign(x: int32, y: int32)\n"
        "assign(1, 2). assign(2, 3). assign(3, 4).\n"
        ".decl mA(x: int32, y: int32)\n"
        "mA(x, x) :- assign(_, x).\n"
        ".decl vF(x: int32, y: int32)\n"
        "vF(y, x) :- assign(y, x).\n"
        "vF(x, y) :- vF(x, z), vF(z, y).\n"
        "vF(x, y) :- assign(x, z), mA(z, y).\n";

    int64_t count = 0;
    uint32_t iters = 0;
    int rc = run_program(src, "vF", &count, &iters);
    ASSERT(rc == 0, "CSPA-pattern program failed");

    /* assign = {(1,2),(2,3),(3,4)}
     * mA = {(2,2),(3,3),(4,4)}  (from assign(_,x))
     *
     * vF base from assign: {(1,2),(2,3),(3,4)}
     * vF from assign(x,z),mA(z,y): assign(1,2)->mA(2,2)->(1,2),
     *   assign(2,3)->mA(3,3)->(2,3), assign(3,4)->mA(4,4)->(3,4)
     *   (all already in base)
     * vF from self-join (transitive closure of {(1,2),(2,3),(3,4)}):
     *   {(1,2),(2,3),(3,4),(1,3),(2,4),(1,4)} = 6 tuples */
    ASSERT(count == 6, "CSPA-pattern vF should produce 6 tuples");
    ASSERT(iters >= 1, "must require at least 1 iteration");

    PASS();
}

/* ================================================================
 * Test 9: Multi-rule UNION where K=1 rule contributes unique tuples
 *
 * Verifies that the EDB-IDB rule in a multi-rule UNION still fires
 * and contributes tuples that the self-join alone would not produce.
 *
 * r(x,y) :- e(x,y).             -- EDB base
 * r(x,y) :- r(x,z), r(z,y).    -- K=2 self-join
 * r(x,y) :- e(x,z), s(z,y).    -- K=1 EDB-IDB (adds cross-graph edges)
 * s(2, 10). s(3, 20).           -- unique targets
 *
 * Without issue #370 fix, the K=1 rule's segment could be evaluated
 * with full joins in non-delta workers, wasting O(N^2) work. With
 * the fix, the segment is neutered in workers for the K=2 rule and
 * only contributes through AUTO delta mode.
 * ================================================================ */
static void
test_k2_multi_rule_union_unique_contributions(void)
{
    TEST("K=2 multi-rule UNION: K=1 rule contributes unique tuples");

    const char *src =
        ".decl e(x: int32, y: int32)\n"
        "e(1, 2). e(2, 3).\n"
        ".decl s(x: int32, y: int32)\n"
        "s(2, 10). s(3, 20).\n"
        ".decl r(x: int32, y: int32)\n"
        "r(x, y) :- e(x, y).\n"
        "r(x, y) :- r(x, z), r(z, y).\n"
        "r(x, y) :- e(x, z), s(z, y).\n";

    int64_t count = 0;
    int rc = run_program(src, "r", &count, NULL);
    ASSERT(rc == 0, "multi-rule UNION program failed");

    /* e = {(1,2),(2,3)}
     * s = {(2,10),(3,20)}
     *
     * r base from e: {(1,2),(2,3)}
     * r from e(x,z),s(z,y): (1,2)->s(2,10)->(1,10), (2,3)->s(3,20)->(2,20)
     * r from self-join: (1,2)+(2,3)->(1,3), (1,2)+(2,20)->(1,20),
     *   (1,3)+(3,20)->(1,20)[dup], (1,2)+(2,10)... wait, need (x,z),(z,y)
     * Let me enumerate systematically:
     *   After base+K=1: {(1,2),(2,3),(1,10),(2,20)}
     *   Self-join iter 1: (1,2)x(2,3)->(1,3), (1,2)x(2,20)->(1,20)
     *   After iter 1: {(1,2),(2,3),(1,10),(2,20),(1,3),(1,20)}
     *   Self-join iter 2: (1,3)x(3,20)->(1,20)[dup]
     *   Converge. Total = 6 tuples. */
    ASSERT(count == 6, "multi-rule UNION should produce 6 tuples");

    /* Verify the K=1 rule contributed unique tuples (1,10) and (2,20)
     * that would not exist from the self-join alone. The self-join of
     * {(1,2),(2,3)} only produces {(1,2),(2,3),(1,3)} = 3 tuples. */
    ASSERT(count > 3, "K=1 rule must contribute tuples beyond self-join");

    PASS();
}

/* ================================================================
 * main
 * ================================================================ */

int
main(void)
{
    printf("\n=== K-Fusion End-to-End Integration Tests ===\n\n");

    test_k2_recursive_join_correct_count();
    test_k1_vs_k2_tc_parity();
    test_k2_iteration_count();
    test_k2_empty_delta_skip();
    test_k2_larger_graph();
    test_k2_complete_graph_dedup();
    test_k2_single_self_loop();
    test_k2_multi_rule_union_cspa_pattern();
    test_k2_multi_rule_union_unique_contributions();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n\n");

    return fail_count > 0 ? 1 : 0;
}
