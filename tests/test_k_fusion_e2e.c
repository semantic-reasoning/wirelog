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
#ifndef _WIN32
#define _DEFAULT_SOURCE 1
#endif

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/internal.h"
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

#ifdef _WIN32
static int
wl_test_setenv_(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, (value && *value) ? value : "1");
}

static int
wl_test_unsetenv_(const char *name)
{
    return _putenv_s(name, "");
}

#define setenv wl_test_setenv_
#define unsetenv wl_test_unsetenv_
#endif

/* Defined under WL_SESSION_TEST_HOOKS in testlib_prod. */
extern void (*wl_columnar_kfusion_test_clone)(wl_col_session_t *parent,
    wl_col_session_t *child, bool before);
extern int (*wl_columnar_kfusion_test_submit)(wl_work_queue_t *wq,
    void (*work_fn)(void *ctx), void *ctx);

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
run_program_with_workers(const char *src, const char *tracked_rel,
    uint32_t workers, int64_t *out_count, uint32_t *out_iters)
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
    rc = wl_session_create(wl_backend_columnar(), plan, workers, &sess);
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

static int
run_program(const char *src, const char *tracked_rel, int64_t *out_count,
    uint32_t *out_iters)
{
    return run_program_with_workers(src, tracked_rel, 1, out_count, out_iters);
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
 * Test 10: All inactive K-fusion branches keep target schema
 *
 * In iteration > 0, FORCE_DELTA branches whose delta relation is absent
 * are inactive. The parallel K-fusion path should short-circuit without
 * allocating worker sessions, but the produced empty relation must still
 * carry the target relation's column count.
 * ================================================================ */
static void
test_k2_all_inactive_branches_preserve_target_schema(void)
{
    TEST("K=2 all inactive branches preserve target schema");

    wl_col_session_t sess;
    memset(&sess, 0, sizeof(sess));
    sess.num_workers = 2;
    sess.current_iteration = 1;

    col_rel_t *target = col_rel_new_auto("out", 3);
    ASSERT(target != NULL, "target relation allocation failed");
    int rc = session_add_rel(&sess, target);
    ASSERT(rc == 0, "target relation registration failed");

    wl_plan_op_t branch0[] = {
        {
            .op = WL_PLAN_OP_VARIABLE,
            .relation_name = "r",
            .delta_mode = WL_DELTA_FORCE_DELTA,
        },
    };
    wl_plan_op_t branch1[] = {
        {
            .op = WL_PLAN_OP_VARIABLE,
            .relation_name = "r",
            .delta_mode = WL_DELTA_FORCE_DELTA,
        },
    };
    wl_plan_op_t *k_ops[] = { branch0, branch1 };
    uint32_t k_counts[] = { 1, 1 };
    wl_plan_op_k_fusion_t meta = {
        .k = 2,
        .k_ops = k_ops,
        .k_op_counts = k_counts,
    };
    wl_plan_op_t op = {
        .op = WL_PLAN_OP_K_FUSION,
        .relation_name = "out",
        .opaque_data = &meta,
    };

    eval_stack_t stack;
    eval_stack_init(&stack);
    rc = col_op_k_fusion(&op, &stack, &sess);
    ASSERT(rc == 0, "K-fusion all-inactive path failed");
    ASSERT(stack.top == 1, "K-fusion should push one empty result");

    eval_entry_t result = eval_stack_pop(&stack);
    ASSERT(result.rel != NULL, "K-fusion result missing");
    ASSERT(result.owned, "K-fusion empty result should be owned");
    ASSERT(result.rel->nrows == 0, "K-fusion result should be empty");
    ASSERT(result.rel->ncols == 3, "empty result must keep target schema");
    col_rel_destroy(result.rel);
    eval_stack_drain(&stack);

    col_rel_destroy(target);
    wl_kfusion_adaptive_destroy(sess.kfusion_adaptive);
    free(sess.rels);
    session_rel_free_hash(&sess);
    PASS();
}

/* ================================================================
 * Test 11: W>1, K below parallel threshold uses the same semantics
 *
 * K=2 is below WL_KFUSION_MIN_PARALLEL_K. A multi-worker session should
 * still produce the same closure while using the serial K-fusion evaluator
 * instead of allocating branch worker sessions.
 * ================================================================ */
static void
test_k2_multiworker_threshold_serial_semantics(void)
{
    TEST("K=2 W=8 threshold serial path preserves closure");

    const char *src = ".decl r(x: int32, y: int32)\n"
        "r(1, 2). r(2, 3). r(3, 4).\n"
        "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    int rc = run_program_with_workers(src, "r", 8, &count, NULL);
    ASSERT(rc == 0, "K=2 W=8 program execution failed");
    ASSERT(count == 6, "K=2 W=8 should produce 6 tuples");

    PASS();
}

/* ================================================================
 * #1382: with WIRELOG_JOIN_BATCH_BYTES set, K-Fusion runs its branches
 * serially on the calling session.  Parallel branch sessions are shallow
 * copies with no coordinator, so before this they passed bounded-join
 * eligibility and ran the bounded producer concurrently, outside the
 * per-session accounting the knob is defined by.  A K=4 rule at four
 * workers clones branches deterministically with the knob off, and must
 * clone none with it on; the closure is the same either way.
 * ================================================================ */
static unsigned kfusion_clones;

static void
count_kfusion_clone(wl_col_session_t *parent, wl_col_session_t *child,
    bool before)
{
    (void)parent;
    (void)child;
    if (before)
        kfusion_clones++;
}

struct pair_ctx {
    uint32_t count;
    uint32_t mismatches;
};

/* r(a, b) holds exactly for 0 <= a < b <= 12 with (b - a) % 3 == 1: the
 * 1-, 4-, 7- and 10-step paths of the chain e(i, i + 1). */
static void
check_pair_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct pair_ctx *ctx = (struct pair_ctx *)user_data;
    if (!relation || strcmp(relation, "r") != 0 || ncols != 2)
        return;
    ctx->count++;
    if (!(row[0] >= 0 && row[0] < row[1] && row[1] <= 12
        && (row[1] - row[0]) % 3 == 1))
        ctx->mismatches++;
}

static void
test_k4_join_batch_knob_runs_branches_serially(void)
{
    TEST("K=4 W=4 with the join-batch knob clones no branch session");

    const char *src = ".decl e(x: int32, y: int32)\n"
        ".decl r(x: int32, y: int32)\n"
        "e(0, 1). e(1, 2). e(2, 3). e(3, 4). e(4, 5). e(5, 6).\n"
        "e(6, 7). e(7, 8). e(8, 9). e(9, 10). e(10, 11). e(11, 12).\n"
        "r(x, y) :- e(x, y).\n"
        "r(a, f) :- r(a, b), r(b, c), r(c, d), r(d, f).\n";
    unsigned clones[2] = { 0, 0 };
    struct pair_ctx pairs[2];
    int rcs[2] = { -1, -1 };

    memset(pairs, 0, sizeof(pairs));
    wl_columnar_kfusion_test_clone = count_kfusion_clone;
    for (int knob = 0; knob < 2; knob++) {
        wirelog_error_t err;
        wirelog_program_t *prog = wirelog_parse_string(src, &err);
        wl_plan_t *plan = NULL;
        wl_session_t *sess = NULL;

        if (knob)
            setenv("WIRELOG_JOIN_BATCH_BYTES", "224", 1);
        else
            unsetenv("WIRELOG_JOIN_BATCH_BYTES");
        unsetenv("WIRELOG_JOIN_BATCH_STRICT");
        kfusion_clones = 0;
        if (prog && wl_plan_from_program(prog, &plan) == 0
            && wl_session_create(wl_backend_columnar(), plan, 4, &sess) == 0
            && wl_session_load_facts(sess, prog) == 0)
            rcs[knob] = wl_session_snapshot(sess, check_pair_cb,
                    &pairs[knob]);
        clones[knob] = kfusion_clones;
        if (sess)
            wl_session_destroy(sess);
        if (plan)
            wl_plan_free(plan);
        if (prog)
            wirelog_program_free(prog);
    }
    wl_columnar_kfusion_test_clone = NULL;
    unsetenv("WIRELOG_JOIN_BATCH_BYTES");
    if (rcs[0] != 0 || rcs[1] != 0)
        printf("(rc %d/%d) ", rcs[0], rcs[1]);
    printf("(clones off=%u on=%u) ", clones[0], clones[1]);
    ASSERT(rcs[0] == 0 && rcs[1] == 0, "snapshot failed");
    ASSERT(pairs[0].count == 30 && pairs[0].mismatches == 0
        && pairs[1].count == 30 && pairs[1].mismatches == 0,
        "closure differs from the 30 expected pairs");
    ASSERT(clones[0] > 0, "the knob-off run cloned no branch session");
    ASSERT(clones[1] == 0, "a branch session was cloned with the knob on");
    PASS();
}

/* ================================================================
 * Issue #2129: a K-Fusion branch session is a shallow copy of the
 * coordinator.  It must not inherit the coordinator's filt_cache array
 * reservation, which the branch would release when it grows its own cache,
 * and every reservation the branch takes for its own cache must be returned
 * to the governor when the cohort is cleaned up.
 * ================================================================ */

static wl_columnar_memory_governor_ref_t *
create_test_governor(void)
{
    wl_columnar_memory_resolution_t resolution;

    memset(&resolution, 0, sizeof(resolution));
    resolution.budget_bytes = (uint64_t)256u * 1024u * 1024u;
    resolution.usable_bytes = (uint64_t)256u * 1024u * 1024u;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    return wl_columnar_memory_governor_ref_create(&resolution);
}

static uint64_t
governor_reserved(wl_columnar_memory_governor_ref_t *governor)
{
    return wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(governor));
}

struct branch_token_probe {
    wl_col_session_t *parent;
    wl_col_session_t *child;
    wl_columnar_memory_reservation_t *token;
    uint64_t token_bytes;
    uint64_t token_state;
    unsigned primed_clones;
    unsigned inherited;
    unsigned grown_after_primed_parent;
    unsigned parent_changed;
};

static struct branch_token_probe token_probe;

/* Runs right after the shallow copy of branch d, before its resets. */
static void
record_parent_token(wl_col_session_t *parent, wl_col_session_t *child,
    bool before)
{
    if (before)
        return;
    token_probe.parent = parent;
    token_probe.child = child;
    token_probe.token = parent->filt_cache_array_reservation;
    if (token_probe.token) {
        token_probe.primed_clones++;
        token_probe.token_bytes = token_probe.token->bytes;
        token_probe.token_state = atomic_load_explicit(
            &token_probe.token->state, memory_order_acquire);
    }
}

/* Runs branch d inline on the coordinator thread, after all of its resets.
 * Clearing an inherited token here keeps the unfixed code from releasing
 * the coordinator's token, so the token reads below and in the next
 * branch's record_parent_token stay valid, and the test reports the
 * inheritance instead of crashing. */
static int
run_branch_inline(wl_work_queue_t *wq, void (*work_fn)(void *ctx), void *ctx)
{
    wl_col_session_t *child = token_probe.child;
    wl_columnar_memory_reservation_t *token = token_probe.token;

    (void)wq;
    if (child->filt_cache_array_reservation != NULL) {
        token_probe.inherited++;
        child->filt_cache_array_reservation = NULL;
    }
    work_fn(ctx);
    if (child->filt_cache_cap > 0 && token != NULL)
        token_probe.grown_after_primed_parent++;
    if (token_probe.parent->filt_cache_array_reservation != token
        || (token != NULL && (token->bytes != token_probe.token_bytes
        || atomic_load_explicit(&token->state, memory_order_acquire)
        != token_probe.token_state)))
        token_probe.parent_changed++;
    return 0;
}

static void
test_k4_branch_filter_cache_tokens_are_branch_owned(void)
{
    TEST("K=4 branches own their filter-cache reservations (#2129)");

    /* s is a lower stratum, so the coordinator fills its own filter cache
     * for k(y, 1) before dispatch; each branch then filters k(b, 1) into a
     * cache of its own. */
    const char *src = ".decl e(x: int32, y: int32)\n"
        ".decl k(x: int32, y: int32)\n"
        ".decl s(x: int32, y: int32)\n"
        ".decl r(x: int32, y: int32)\n"
        "e(0, 1). e(1, 2). e(2, 3). e(3, 4). e(4, 5). e(5, 6).\n"
        "e(6, 7). e(7, 8). e(8, 9). e(9, 10). e(10, 11). e(11, 12).\n"
        "k(0, 1). k(1, 1). k(2, 1). k(3, 1). k(4, 1). k(5, 1). k(6, 1).\n"
        "k(7, 1). k(8, 1). k(9, 1). k(10, 1). k(11, 1). k(12, 1).\n"
        "s(x, y) :- e(x, y), k(y, 1).\n"
        "r(x, y) :- s(x, y).\n"
        "r(a, f) :- r(a, b), k(b, 1), r(b, c), r(c, d), r(d, f).\n";
    /* W=1 runs the branches serially without cloning: the control. */
    static const uint32_t workers[] = { 1, 4, 2 };
    char why[256];

    why[0] = '\0';
    unsetenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    unsetenv("WIRELOG_JOIN_BATCH_BYTES");
    unsetenv("WIRELOG_JOIN_BATCH_STRICT");
    for (size_t wi = 0; wi < sizeof(workers) / sizeof(workers[0]); wi++) {
        uint32_t w = workers[wi];
        wl_columnar_memory_governor_ref_t *governor = create_test_governor();
        wirelog_error_t err;
        wirelog_program_t *prog = NULL;
        wl_plan_t *plan = NULL;
        wl_session_t *sess = NULL;
        wl_session_options_t options;
        struct pair_ctx pairs = { 0, 0 };
        uint64_t baseline = 0;
        uint64_t after = 0;
        int rc = -1;

        if (!governor) {
            snprintf(why, sizeof(why), "W=%u: governor allocation failed",
                w);
            break;
        }
        baseline = governor_reserved(governor);
        memset(&token_probe, 0, sizeof(token_probe));
        wl_columnar_kfusion_test_clone = record_parent_token;
        wl_columnar_kfusion_test_submit = run_branch_inline;
        wl_session_options_init(&options);
        options.memory_governor = governor;
        prog = wirelog_parse_string(src, &err);
        if (prog && wl_plan_from_program(prog, &plan) == 0
            && wl_session_create_with_options(wl_backend_columnar(), plan, w,
            &options, &sess) == 0
            && wl_session_load_facts(sess, prog) == 0)
            rc = wl_session_snapshot(sess, check_pair_cb, &pairs);
        if (sess)
            wl_session_destroy(sess);
        if (plan)
            wl_plan_free(plan);
        if (prog)
            wirelog_program_free(prog);
        wl_columnar_kfusion_test_clone = NULL;
        wl_columnar_kfusion_test_submit = NULL;
        after = governor_reserved(governor);
        wl_columnar_memory_governor_ref_release(governor);

        printf("\n    W=%u primed=%u inherited=%u grown=%u "
            "parent_changed=%u residue=%" PRId64 " ", w,
            token_probe.primed_clones, token_probe.inherited,
            token_probe.grown_after_primed_parent,
            token_probe.parent_changed, (int64_t)(after - baseline));
        if (rc != 0)
            snprintf(why, sizeof(why), "W=%u: snapshot failed (rc %d)", w,
                rc);
        else if (pairs.count != 30 || pairs.mismatches != 0)
            snprintf(why, sizeof(why),
                "W=%u: closure differs from the 30 expected pairs", w);
        else if (w > 1 && token_probe.grown_after_primed_parent < 2)
            snprintf(why, sizeof(why), "W=%u: fewer than two branches grew "
                "a filter cache while the coordinator held a token", w);
        else if (token_probe.inherited != 0)
            snprintf(why, sizeof(why), "W=%u: a branch session inherited "
                "the coordinator's filter-cache array token", w);
        else if (token_probe.parent_changed != 0)
            snprintf(why, sizeof(why), "W=%u: a branch changed the "
                "coordinator's filter-cache array token", w);
        else if (after != baseline)
            snprintf(why, sizeof(why), "W=%u: the governor still holds "
                "reservations after the session was destroyed", w);
        if (why[0] != '\0')
            break;
    }
    printf("\n    ");
    ASSERT(why[0] == '\0', why);
    PASS();
}

/* Multiset digest of every tuple a snapshot reports: independent of the
 * order of tuples, dependent on the order of columns within a tuple. */
struct model_digest {
    uint64_t rows;
    uint64_t sum;
};

static void
digest_tuple_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct model_digest *digest = (struct model_digest *)user_data;
    uint64_t h = 1469598103934665603ull;

    for (const char *c = relation; c && *c; c++)
        h = (h ^ (uint8_t)*c) * 1099511628211ull;
    for (uint32_t i = 0; i < ncols; i++)
        h = (h ^ (uint64_t)row[i]) * 1099511628211ull;
    digest->rows++;
    digest->sum += h;
}

struct fact_rows {
    const char *relation;
    const int64_t *rows;
    uint32_t count;
    uint32_t ncols;
};

static int
insert_fact_rows(wl_session_t *sess, const struct fact_rows *facts,
    size_t nfacts)
{
    for (size_t f = 0; f < nfacts; f++) {
        for (uint32_t i = 0; i < facts[f].count; i++) {
            int rc = wl_session_insert(sess, facts[f].relation,
                    facts[f].rows + (size_t)i * facts[f].ncols, 1,
                    facts[f].ncols);
            if (rc != 0)
                return rc;
        }
    }
    return 0;
}

static void
test_issue_2129_program_same_model_at_every_width(void)
{
    TEST("#2129 program: same model and no reservation residue at W=1..8");

    const char *src = ".decl e0(c0: int64, c1: int64, c2: int64)\n"
        ".decl e1(c0: int64, c1: int64)\n"
        ".decl e2(c0: int64, c1: int64, c2: int64)\n"
        ".decl e3(c0: int64, c1: int64)\n"
        ".decl a(c0: int64, c1: int64)\n"
        ".decl b(c0: int64, c1: int64)\n"
        ".decl l0(c0: int64, c1: int64)\n"
        "l0(x0,x1) :- e1(x0,x1).\n"
        "a(x1,x1) :- b(x0,x1), e1(x0,3).\n"
        "b(x0,x0) :- a(x0,x0), e3(x0,x0), !e0(x0,x0,x0).\n"
        "b(x2,x2) :- a(x0,x0), b(x0,x1), e0(x2,x3,x4), !e3(x1,x2).\n"
        "a(x1,x0) :- b(x0,x1).\n"
        "b(x0,x0) :- a(x0,x1), e1(x0,x1).\n"
        "b(x2,x2) :- b(x0,x1), a(x1,x2), b(x0,x1), !l0(x1,x1), x1 != x2.\n";
    static const int64_t e0_round1[] = { 3, 2, 0, 6, 4, 3, 5, 3, 1, 2, 6, 6,
                                         4, 2, 1 };
    static const int64_t e1_round1[] = { 3, 4, 2, 7, 6, 0 };
    static const int64_t e2_round1[] = { 4, 2, 2, 6, 0, 7 };
    static const int64_t e3_round1[] = { 7, 0, 6, 1, 4, 6, 7, 1, 6, 6 };
    static const int64_t e1_round2[] = { 0, 4, 4, 5 };
    static const int64_t e3_round2[] = { 0, 3 };
    static const struct fact_rows round1[] = {
        { "e0", e0_round1, 5, 3 },
        { "e1", e1_round1, 3, 2 },
        { "e2", e2_round1, 2, 3 },
        { "e3", e3_round1, 5, 2 },
    };
    static const struct fact_rows round2[] = {
        { "e1", e1_round2, 2, 2 },
        { "e3", e3_round2, 1, 2 },
    };
    static const uint32_t workers[] = { 1, 2, 4, 8 };
    struct model_digest reference[2];
    char why[256];

    memset(reference, 0, sizeof(reference));
    why[0] = '\0';
    setenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER", "1", 1);
    unsetenv("WIRELOG_JOIN_BATCH_BYTES");
    unsetenv("WIRELOG_JOIN_BATCH_STRICT");
    for (size_t wi = 0; wi < sizeof(workers) / sizeof(workers[0]); wi++) {
        uint32_t w = workers[wi];
        wl_columnar_memory_governor_ref_t *governor = create_test_governor();
        wirelog_error_t err;
        wirelog_program_t *prog = NULL;
        wl_plan_t *plan = NULL;
        wl_session_t *sess = NULL;
        wl_session_options_t options;
        struct model_digest got[2];
        uint64_t baseline = 0;
        uint64_t after = 0;
        int rc = -1;

        memset(got, 0, sizeof(got));
        if (!governor) {
            snprintf(why, sizeof(why), "W=%u: governor allocation failed",
                w);
            break;
        }
        baseline = governor_reserved(governor);
        wl_session_options_init(&options);
        options.memory_governor = governor;
        prog = wirelog_parse_string(src, &err);
        if (prog && wl_plan_from_program(prog, &plan) == 0
            && wl_session_create_with_options(wl_backend_columnar(), plan, w,
            &options, &sess) == 0) {
            rc = insert_fact_rows(sess, round1,
                    sizeof(round1) / sizeof(round1[0]));
            if (rc == 0)
                rc = wl_session_snapshot(sess, digest_tuple_cb, &got[0]);
            if (rc == 0)
                rc = insert_fact_rows(sess, round2,
                        sizeof(round2) / sizeof(round2[0]));
            if (rc == 0)
                rc = wl_session_snapshot(sess, digest_tuple_cb, &got[1]);
        }
        if (sess)
            wl_session_destroy(sess);
        if (plan)
            wl_plan_free(plan);
        if (prog)
            wirelog_program_free(prog);
        after = governor_reserved(governor);
        wl_columnar_memory_governor_ref_release(governor);

        printf("\n    W=%u rows=%" PRIu64 "/%" PRIu64 " residue=%" PRId64
            " ", w, got[0].rows, got[1].rows, (int64_t)(after - baseline));
        if (w == 1)
            memcpy(reference, got, sizeof(reference));
        if (rc != 0)
            snprintf(why, sizeof(why), "W=%u: insert or snapshot failed "
                "(rc %d)", w, rc);
        else if (reference[1].rows == 0)
            snprintf(why, sizeof(why), "W=%u: the reference model is empty",
                w);
        else if (memcmp(got, reference, sizeof(reference)) != 0)
            snprintf(why, sizeof(why), "W=%u: the model differs from W=1",
                w);
        else if (after != baseline)
            snprintf(why, sizeof(why), "W=%u: the governor still holds "
                "reservations after the session was destroyed", w);
        if (why[0] != '\0')
            break;
    }
    unsetenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    printf("\n    ");
    ASSERT(why[0] == '\0', why);
    PASS();
}

/* ================================================================
 * Issue #2261: a K-Fusion branch session must not share the coordinator's
 * sorted-arrangement (LFTJ) cache.  Pin counts on its entries are plain
 * integers, so branches pinning the same entry on worker threads race, and
 * a branch that grew the array would reallocate the coordinator's storage.
 * ================================================================ */

struct branch_sarr_probe {
    wl_col_session_t *parent;
    wl_col_session_t *child;
    col_sorted_arr_entry_t *parent_entries;
    uint32_t parent_count;
    uint32_t parent_cap;
    uint32_t parent_pins;
    unsigned clones;
    unsigned primed;
    unsigned inherited;
    unsigned child_built;
    unsigned parent_changed;
};

static struct branch_sarr_probe sarr_probe;

/* Runs right after the shallow copy of branch d, before its resets. */
static void
record_parent_sarr(wl_col_session_t *parent, wl_col_session_t *child,
    bool before)
{
    if (before)
        return;
    sarr_probe.parent = parent;
    sarr_probe.child = child;
    sarr_probe.clones++;
    sarr_probe.parent_entries = parent->sarr_entries;
    sarr_probe.parent_count = parent->sarr_count;
    sarr_probe.parent_cap = parent->sarr_cap;
    sarr_probe.parent_pins = parent->sarr_active_pins;
    if (parent->sarr_count > 0)
        sarr_probe.primed++;
}

/* Runs branch d inline on the coordinator thread, after all of its resets.
 * Clearing an inherited cache here keeps the unfixed code from pinning or
 * growing the coordinator's entries, so the test reports the inheritance
 * instead of corrupting the coordinator's cache. */
static int
run_sarr_branch_inline(wl_work_queue_t *wq, void (*work_fn)(void *ctx),
    void *ctx)
{
    wl_col_session_t *child = sarr_probe.child;
    wl_col_session_t *parent = sarr_probe.parent;

    (void)wq;
    if (child->sarr_entries != NULL || child->sarr_count != 0
        || child->sarr_cap != 0 || child->sarr_active_pins != 0) {
        sarr_probe.inherited++;
        child->sarr_entries = NULL;
        child->sarr_count = 0;
        child->sarr_cap = 0;
        child->sarr_active_pins = 0;
    }
    work_fn(ctx);
    if (child->sarr_count > 0)
        sarr_probe.child_built++;
    if (parent->sarr_entries != sarr_probe.parent_entries
        || parent->sarr_count != sarr_probe.parent_count
        || parent->sarr_cap != sarr_probe.parent_cap
        || parent->sarr_active_pins != sarr_probe.parent_pins)
        sarr_probe.parent_changed++;
    for (uint32_t i = 0; i < parent->sarr_count; i++) {
        if (parent->sarr_entries[i].sarr.pin_count != 0)
            sarr_probe.parent_changed++;
    }
    return 0;
}

/* Runs src at width w without the fusion/JPP/SIP passes, which would break
 * the EDB join chains that the LFTJ rewrite looks for.  Returns the snapshot
 * rc and stores the number of tuples of r. */
static int
run_lftj_program(const char *src, uint32_t w, int64_t *out_count)
{
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    wl_plan_t *plan = NULL;
    wl_session_t *sess = NULL;
    struct result_ctx ctx = { 0, 0, "r" };
    int rc = -1;

    if (prog && wl_plan_from_program(prog, &plan) == 0
        && wl_session_create(wl_backend_columnar(), plan, w, &sess) == 0
        && wl_session_load_facts(sess, prog) == 0)
        rc = wl_session_snapshot(sess, count_tuples_cb, &ctx);
    if (sess)
        wl_session_destroy(sess);
    if (plan)
        wl_plan_free(plan);
    if (prog)
        wirelog_program_free(prog);
    *out_count = ctx.rel_count;
    return rc;
}

static void
test_k4_branch_sorted_arrangements_are_branch_owned(void)
{
    TEST("K-Fusion branches own their sorted arrangements (#2261)");

    /* s is a lower stratum whose a/b/c chain becomes an LFTJ operator, so
     * the coordinator holds sorted arrangements before dispatch; the
     * recursive rule's a/b/c chain is an LFTJ operator inside every
     * branch.  Four r atoms make K=4, which dispatches in parallel without
     * consulting the adaptive scheduler.  That scheduler times each run,
     * and in the MSVC CI job a K=2 version of this fixture never
     * dispatched in parallel. */
    const char *src = ".decl a(k: int32, x: int32)\n"
        ".decl b(k: int32, x: int32)\n"
        ".decl c(k: int32, x: int32)\n"
        ".decl e(x: int32, y: int32)\n"
        ".decl s(x: int32)\n"
        ".decl r(x: int32, y: int32)\n"
        "a(1, 1). a(2, 2). a(3, 3). b(1, 1). b(2, 2). b(3, 3).\n"
        "c(1, 1). c(2, 2). c(3, 3).\n"
        "e(1, 2). e(2, 3). e(3, 4). e(4, 5).\n"
        "s(x) :- a(k, x), b(k, y), c(k, z).\n"
        "r(x, y) :- e(x, y).\n"
        "r(x, w) :- a(x, k), b(y, k), c(z, k), r(x, t), r(t, u), r(u, v),"
        " r(v, w).\n";
    /* W=1 runs the branches serially without cloning: the control. */
    static const uint32_t workers[] = { 1, 4, 2 };
    int64_t control = -1;
    int64_t count = 0;
    char why[256];
    int rc;

    why[0] = '\0';
    unsetenv("WIRELOG_TDD_MIN_ROWS_PER_WORKER");
    unsetenv("WIRELOG_JOIN_BATCH_BYTES");
    unsetenv("WIRELOG_JOIN_BATCH_STRICT");
    for (size_t wi = 0; wi < sizeof(workers) / sizeof(workers[0]); wi++) {
        uint32_t w = workers[wi];

        memset(&sarr_probe, 0, sizeof(sarr_probe));
        wl_columnar_kfusion_test_clone = record_parent_sarr;
        wl_columnar_kfusion_test_submit = run_sarr_branch_inline;
        count = 0;
        rc = run_lftj_program(src, w, &count);
        wl_columnar_kfusion_test_clone = NULL;
        wl_columnar_kfusion_test_submit = NULL;
        if (w == 1)
            control = count;

        printf("\n    W=%u count=%" PRId64 " clones=%u primed=%u "
            "inherited=%u built=%u parent_changed=%u ", w, count,
            sarr_probe.clones, sarr_probe.primed, sarr_probe.inherited,
            sarr_probe.child_built, sarr_probe.parent_changed);
        if (rc != 0)
            snprintf(why, sizeof(why), "W=%u: snapshot failed (rc %d)", w,
                rc);
        else if (count != control || count <= 0)
            snprintf(why, sizeof(why), "W=%u: %" PRId64 " tuples of r, "
                "W=1 derived %" PRId64, w, count, control);
        else if (w == 1 && sarr_probe.clones != 0)
            snprintf(why, sizeof(why), "W=1 cloned a branch session");
        else if (w > 1 && (sarr_probe.clones == 0 || sarr_probe.primed == 0))
            snprintf(why, sizeof(why), "W=%u: no branch was cloned while "
                "the coordinator held sorted arrangements", w);
        else if (w > 1 && sarr_probe.child_built == 0)
            snprintf(why, sizeof(why), "W=%u: no branch built a sorted "
                "arrangement", w);
        else if (sarr_probe.inherited != 0)
            snprintf(why, sizeof(why), "W=%u: a branch session inherited "
                "the coordinator's sorted-arrangement cache", w);
        else if (sarr_probe.parent_changed != 0)
            snprintf(why, sizeof(why), "W=%u: a branch changed the "
                "coordinator's sorted-arrangement cache", w);
        if (why[0] != '\0')
            break;
    }

    /* Real worker threads, for ThreadSanitizer: before the fix, branches
     * pinned and unpinned the coordinator's entries concurrently.  Runs
     * even after a failure above, so the race stays visible. */
    {
        count = 0;
        rc = run_lftj_program(src, 4, &count);
        printf("\n    W=4 threaded count=%" PRId64 " ", count);
        if (why[0] == '\0' && (rc != 0 || count != control))
            snprintf(why, sizeof(why), "threaded W=4: rc %d, %" PRId64
                " tuples of r, W=1 derived %" PRId64, rc, count, control);
    }
    printf("\n    ");
    ASSERT(why[0] == '\0', why);
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
    test_k2_all_inactive_branches_preserve_target_schema();
    test_k2_multiworker_threshold_serial_semantics();
    test_k4_join_batch_knob_runs_branches_serially();
    test_k4_branch_filter_cache_tokens_are_branch_owned();
    test_issue_2129_program_same_model_at_every_width();
    test_k4_branch_sorted_arrangements_are_branch_owned();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n\n");

    return fail_count > 0 ? 1 : 0;
}
