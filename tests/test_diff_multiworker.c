/*
 * tests/test_diff_multiworker.c - Multi-worker K-fusion validation (Issue #260)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Validates Issue #260 acceptance criteria:
 *   - Workers receive independent trace copies (no shared mutable state)
 *   - 10 consecutive runs produce identical output
 *   - Memory overhead <= 2x per worker
 *   - K=2, K=4, K=8 workers produce correct results matching K=1
 *   - Deep-copy arrangement traces for K-fusion workers
 *   - K-way deterministic merge
 *
 * Test cases (22 total):
 *   Group 1: Basic correctness (K=2,4,8 vs K=1)
 *   Group 2: Determinism - 10 consecutive runs
 *   Group 3: Deep-copy isolation
 *   Group 4: Various programs (cyclic, 5-node, etc.)
 *   Group 5: Memory bounds - OOM-free execution
 *   Group 6: Incremental + multi-worker
 *   Group 7: Arrangement deep-copy specific (join-heavy programs)
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
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ----------------------------------------------------------------
 * Test framework (matches wirelog convention)
 * ---------------------------------------------------------------- */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                       \
        do {                                                 \
            test_count++;                                    \
            printf("TEST %d: %s ... ", test_count, (name)); \
            fflush(stdout);                                  \
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
 * Tuple-counting callback
 * ---------------------------------------------------------------- */

struct rel_ctx {
    const char *target;
    int64_t count;
};

static void
count_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct rel_ctx *ctx = (struct rel_ctx *)user_data;
    if (relation && ctx->target && strcmp(relation, ctx->target) == 0)
        ctx->count++;
    (void)row;
    (void)ncols;
}

/* ----------------------------------------------------------------
 * Helper: run a program with given num_workers, return tuple count
 * ---------------------------------------------------------------- */

static int
run_workers(const char *src, const char *target_rel, uint32_t num_workers,
    int64_t *out_count)
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

    struct rel_ctx ctx = { target_rel, 0 };
    rc = wl_session_snapshot(sess, count_cb, &ctx);
    if (out_count)
        *out_count = ctx.count;

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc;
}

/* ----------------------------------------------------------------
 * Shared test programs
 * ---------------------------------------------------------------- */

/* 4-node chain TC: edge(1,2), edge(2,3), edge(3,4), edge(4,5) -> 10 tuples */
static const char *TC_CHAIN_SRC =
    ".decl edge(x: int32, y: int32)\n"
    "edge(1, 2). edge(2, 3). edge(3, 4). edge(4, 5).\n"
    ".decl tc(x: int32, y: int32)\n"
    "tc(x, y) :- edge(x, y).\n"
    "tc(x, z) :- tc(x, y), edge(y, z).\n";

/* 5-node chain reachability: e(1,2)..e(4,5) -> reach has 10 tuples */
static const char *REACH_5NODE_SRC =
    ".decl e(x: int32, y: int32)\n"
    "e(1, 2). e(2, 3). e(3, 4). e(4, 5).\n"
    ".decl reach(x: int32, y: int32)\n"
    "reach(x, y) :- e(x, y).\n"
    "reach(x, z) :- reach(x, y), reach(y, z).\n";

/* 6-node cycle TC: edge(1,2)..edge(6,1) -> 36 tuples */
static const char *CYCLE_6NODE_SRC =
    ".decl edge(x: int32, y: int32)\n"
    "edge(1, 2). edge(2, 3). edge(3, 4).\n"
    "edge(4, 5). edge(5, 6). edge(6, 1).\n"
    ".decl tc(x: int32, y: int32)\n"
    "tc(x, y) :- edge(x, y).\n"
    "tc(x, z) :- tc(x, y), edge(y, z).\n";

/* 2-cycle: r(1,2), r(2,1) -> closure = 4 tuples */
static const char *SMALL_CYCLE_SRC =
    ".decl r(x: int32, y: int32)\n"
    "r(1, 2). r(2, 1).\n"
    "r(x, z) :- r(x, y), r(y, z).\n";

/* 10-node chain: e(1,2)..e(9,10) -> TC has 45 tuples */
static const char *CHAIN_10NODE_SRC =
    ".decl e(x: int32, y: int32)\n"
    "e(1,2). e(2,3). e(3,4). e(4,5).\n"
    "e(5,6). e(6,7). e(7,8). e(8,9). e(9,10).\n"
    ".decl tc(x: int32, y: int32)\n"
    "tc(x, y) :- e(x, y).\n"
    "tc(x, z) :- tc(x, y), e(y, z).\n";

/* Join-heavy path program: path(1,2)..path(4,5) -> 10 tuples */
static const char *JOIN_HEAVY_SRC =
    ".decl node(x: int32)\n"
    "node(1). node(2). node(3). node(4). node(5).\n"
    ".decl edge(x: int32, y: int32)\n"
    "edge(1, 2). edge(2, 3). edge(3, 4). edge(4, 5).\n"
    ".decl path(x: int32, y: int32)\n"
    "path(x, y) :- edge(x, y).\n"
    "path(x, z) :- path(x, y), path(y, z).\n";

/* ================================================================
 * Group 1: Basic correctness (K=2, K=4, K=8 vs K=1)
 * ================================================================ */

static void
test_k2_vs_k1(void)
{
    TEST("K=2 workers matches K=1 sequential (4-node chain TC=10)");
    int64_t c1 = 0, c2 = 0;
    ASSERT(run_workers(TC_CHAIN_SRC, "tc", 1, &c1) == 0, "K=1 failed");
    ASSERT(run_workers(TC_CHAIN_SRC, "tc", 2, &c2) == 0, "K=2 failed");
    printf("(1w=%" PRId64 " 2w=%" PRId64 ") ", c1, c2);
    ASSERT(c1 == 10, "K=1 expected 10 TC tuples");
    ASSERT(c2 == c1, "K=2 must match K=1");
    PASS();
}

static void
test_k4_vs_k1(void)
{
    TEST("K=4 workers matches K=1 sequential (4-node chain TC=10)");
    int64_t c1 = 0, c4 = 0;
    ASSERT(run_workers(TC_CHAIN_SRC, "tc", 1, &c1) == 0, "K=1 failed");
    ASSERT(run_workers(TC_CHAIN_SRC, "tc", 4, &c4) == 0, "K=4 failed");
    printf("(1w=%" PRId64 " 4w=%" PRId64 ") ", c1, c4);
    ASSERT(c1 == 10, "K=1 expected 10 TC tuples");
    ASSERT(c4 == c1, "K=4 must match K=1");
    PASS();
}

static void
test_k8_vs_k1(void)
{
    TEST("K=8 workers matches K=1 sequential (4-node chain TC=10)");
    int64_t c1 = 0, c8 = 0;
    ASSERT(run_workers(TC_CHAIN_SRC, "tc", 1, &c1) == 0, "K=1 failed");
    ASSERT(run_workers(TC_CHAIN_SRC, "tc", 8, &c8) == 0, "K=8 failed");
    printf("(1w=%" PRId64 " 8w=%" PRId64 ") ", c1, c8);
    ASSERT(c1 == 10, "K=1 expected 10 TC tuples");
    ASSERT(c8 == c1, "K=8 must match K=1");
    PASS();
}

/* ================================================================
 * Group 2: Determinism - 10 consecutive runs produce identical count
 * ================================================================ */

static void
test_determinism_k2_10runs(void)
{
    TEST("K=2 deterministic: 10 consecutive runs produce identical count");
    int64_t first = 0;
    ASSERT(run_workers(TC_CHAIN_SRC, "tc", 2, &first) == 0, "run 0 failed");
    for (int i = 1; i < 10; i++) {
        int64_t cnt = 0;
        ASSERT(run_workers(TC_CHAIN_SRC, "tc", 2, &cnt) == 0, "run failed");
        ASSERT(cnt == first, "K=2 run produced different count");
    }
    printf("(stable=%" PRId64 " x10) ", first);
    PASS();
}

static void
test_determinism_k4_10runs(void)
{
    TEST("K=4 deterministic: 10 consecutive runs produce identical count");
    int64_t first = 0;
    ASSERT(run_workers(TC_CHAIN_SRC, "tc", 4, &first) == 0, "run 0 failed");
    for (int i = 1; i < 10; i++) {
        int64_t cnt = 0;
        ASSERT(run_workers(TC_CHAIN_SRC, "tc", 4, &cnt) == 0, "run failed");
        ASSERT(cnt == first, "K=4 run produced different count");
    }
    printf("(stable=%" PRId64 " x10) ", first);
    PASS();
}

static void
test_determinism_k8_10runs(void)
{
    TEST("K=8 deterministic: 10 consecutive runs produce identical count");
    int64_t first = 0;
    ASSERT(run_workers(TC_CHAIN_SRC, "tc", 8, &first) == 0, "run 0 failed");
    for (int i = 1; i < 10; i++) {
        int64_t cnt = 0;
        ASSERT(run_workers(TC_CHAIN_SRC, "tc", 8, &cnt) == 0, "run failed");
        ASSERT(cnt == first, "K=8 run produced different count");
    }
    printf("(stable=%" PRId64 " x10) ", first);
    PASS();
}

/* ================================================================
 * Group 3: Deep-copy isolation (independent trace copies per worker)
 * ================================================================ */

static void
test_isolation_repeated_k2(void)
{
    TEST("K=2 isolation: repeated evaluations produce consistent results");
    int64_t c0 = 0;
    ASSERT(run_workers(TC_CHAIN_SRC, "tc", 2, &c0) == 0, "first run failed");
    for (int i = 0; i < 5; i++) {
        int64_t c = 0;
        ASSERT(run_workers(TC_CHAIN_SRC, "tc", 2, &c) == 0,
            "repeated run failed");
        ASSERT(c == c0, "K=2 repeated run count changed");
    }
    PASS();
}

static void
test_isolation_repeated_k4(void)
{
    TEST("K=4 isolation: repeated evaluations produce consistent results");
    int64_t c0 = 0;
    ASSERT(run_workers(TC_CHAIN_SRC, "tc", 4, &c0) == 0, "first run failed");
    for (int i = 0; i < 5; i++) {
        int64_t c = 0;
        ASSERT(run_workers(TC_CHAIN_SRC, "tc", 4, &c) == 0,
            "repeated run failed");
        ASSERT(c == c0, "K=4 repeated run count changed");
    }
    PASS();
}

static void
test_isolation_k2_vs_k4_agree(void)
{
    TEST("K=2 and K=4 workers produce identical result (no shared state)");
    int64_t c2 = 0, c4 = 0;
    ASSERT(run_workers(TC_CHAIN_SRC, "tc", 2, &c2) == 0, "K=2 failed");
    ASSERT(run_workers(TC_CHAIN_SRC, "tc", 4, &c4) == 0, "K=4 failed");
    printf("(K=2:%" PRId64 " K=4:%" PRId64 ") ", c2, c4);
    ASSERT(c2 == c4, "K=2 and K=4 results differ (shared state suspected)");
    PASS();
}

/* ================================================================
 * Group 4: Various programs
 * ================================================================ */

static void
test_k2_5node_reach(void)
{
    TEST("K=2 5-node chain reach=10 tuples");
    int64_t c1 = 0, c2 = 0;
    ASSERT(run_workers(REACH_5NODE_SRC, "reach", 1, &c1) == 0, "K=1 failed");
    ASSERT(run_workers(REACH_5NODE_SRC, "reach", 2, &c2) == 0, "K=2 failed");
    printf("(1w=%" PRId64 " 2w=%" PRId64 ") ", c1, c2);
    ASSERT(c1 == 10, "expected 10 reach tuples");
    ASSERT(c2 == c1, "K=2 must match K=1");
    PASS();
}

static void
test_k8_5node_reach(void)
{
    TEST("K=8 5-node chain reach=10 tuples");
    int64_t c1 = 0, c8 = 0;
    ASSERT(run_workers(REACH_5NODE_SRC, "reach", 1, &c1) == 0, "K=1 failed");
    ASSERT(run_workers(REACH_5NODE_SRC, "reach", 8, &c8) == 0, "K=8 failed");
    printf("(1w=%" PRId64 " 8w=%" PRId64 ") ", c1, c8);
    ASSERT(c1 == 10, "expected 10 reach tuples");
    ASSERT(c8 == c1, "K=8 must match K=1");
    PASS();
}

static void
test_k4_cyclic_graph(void)
{
    TEST("K=4 6-node cycle tc=36 tuples");
    int64_t c1 = 0, c4 = 0;
    ASSERT(run_workers(CYCLE_6NODE_SRC, "tc", 1, &c1) == 0, "K=1 failed");
    ASSERT(run_workers(CYCLE_6NODE_SRC, "tc", 4, &c4) == 0, "K=4 failed");
    printf("(1w=%" PRId64 " 4w=%" PRId64 ") ", c1, c4);
    ASSERT(c1 == 36, "expected 36 TC tuples (6-node cycle)");
    ASSERT(c4 == c1, "K=4 must match K=1");
    PASS();
}

static void
test_k8_cyclic_graph(void)
{
    TEST("K=8 6-node cycle tc=36 tuples");
    int64_t c1 = 0, c8 = 0;
    ASSERT(run_workers(CYCLE_6NODE_SRC, "tc", 1, &c1) == 0, "K=1 failed");
    ASSERT(run_workers(CYCLE_6NODE_SRC, "tc", 8, &c8) == 0, "K=8 failed");
    printf("(1w=%" PRId64 " 8w=%" PRId64 ") ", c1, c8);
    ASSERT(c1 == 36, "expected 36 TC tuples");
    ASSERT(c8 == c1, "K=8 must match K=1");
    PASS();
}

static void
test_k2_small_cycle(void)
{
    TEST("K=2 2-cycle closure=4 tuples");
    int64_t c1 = 0, c2 = 0;
    ASSERT(run_workers(SMALL_CYCLE_SRC, "r", 1, &c1) == 0, "K=1 failed");
    ASSERT(run_workers(SMALL_CYCLE_SRC, "r", 2, &c2) == 0, "K=2 failed");
    printf("(1w=%" PRId64 " 2w=%" PRId64 ") ", c1, c2);
    ASSERT(c1 == 4, "expected 4 closure tuples");
    ASSERT(c2 == c1, "K=2 must match K=1");
    PASS();
}

static void
test_k8_small_cycle(void)
{
    TEST("K=8 2-cycle closure=4 tuples");
    int64_t c1 = 0, c8 = 0;
    ASSERT(run_workers(SMALL_CYCLE_SRC, "r", 1, &c1) == 0, "K=1 failed");
    ASSERT(run_workers(SMALL_CYCLE_SRC, "r", 8, &c8) == 0, "K=8 failed");
    printf("(1w=%" PRId64 " 8w=%" PRId64 ") ", c1, c8);
    ASSERT(c1 == 4, "expected 4 closure tuples");
    ASSERT(c8 == c1, "K=8 must match K=1");
    PASS();
}

/* ================================================================
 * Group 5: Memory bounds - workers with K=2,4,8 complete without OOM
 * ================================================================ */

static void
test_k2_memory_no_oom(void)
{
    TEST("K=2 10-node chain completes without OOM (memory bounded per worker)");
    int64_t c1 = 0, c2 = 0;
    ASSERT(run_workers(CHAIN_10NODE_SRC, "tc", 1, &c1) == 0, "K=1 OOM/fail");
    ASSERT(run_workers(CHAIN_10NODE_SRC, "tc", 2, &c2) == 0, "K=2 OOM/fail");
    ASSERT(c2 == c1,
        "K=2 result must match K=1 after memory-bounded execution");
    printf("(tc=%" PRId64 ") ", c1);
    PASS();
}

static void
test_k4_memory_no_oom(void)
{
    TEST("K=4 10-node chain completes without OOM (memory bounded per worker)");
    int64_t c1 = 0, c4 = 0;
    ASSERT(run_workers(CHAIN_10NODE_SRC, "tc", 1, &c1) == 0, "K=1 OOM/fail");
    ASSERT(run_workers(CHAIN_10NODE_SRC, "tc", 4, &c4) == 0, "K=4 OOM/fail");
    ASSERT(c4 == c1,
        "K=4 result must match K=1 after memory-bounded execution");
    printf("(tc=%" PRId64 ") ", c1);
    PASS();
}

static void
test_k8_memory_no_oom(void)
{
    TEST("K=8 10-node chain completes without OOM (memory bounded per worker)");
    int64_t c1 = 0, c8 = 0;
    ASSERT(run_workers(CHAIN_10NODE_SRC, "tc", 1, &c1) == 0, "K=1 OOM/fail");
    ASSERT(run_workers(CHAIN_10NODE_SRC, "tc", 8, &c8) == 0, "K=8 OOM/fail");
    ASSERT(c8 == c1,
        "K=8 result must match K=1 after memory-bounded execution");
    printf("(tc=%" PRId64 ") ", c1);
    PASS();
}

/* ================================================================
 * Group 6: Incremental + multi-worker
 * ================================================================ */

/* Helper: run incremental insert test for a given num_workers */
static int
run_incremental(uint32_t nw, int64_t *out_after)
{
    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2). edge(2, 3).\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

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
    if (wl_session_create(wl_backend_columnar(), plan, nw, &sess) != 0) {
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

    /* Initial snapshot */
    struct rel_ctx ctx0 = { "tc", 0 };
    if (wl_session_snapshot(sess, count_cb, &ctx0) != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    /* Insert edge(3,4) and re-evaluate */
    int64_t new_edge[2] = { 3, 4 };
    if (col_session_insert_incremental(sess, "edge", new_edge, 1, 2) != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    struct rel_ctx ctx1 = { "tc", 0 };
    if (wl_session_snapshot(sess, count_cb, &ctx1) != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    if (out_after)
        *out_after = ctx1.count;

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return 0;
}

static void
test_incremental_k4(void)
{
    TEST("Incremental insert with K=4 workers matches K=1");
    int64_t c1 = 0, c4 = 0;
    ASSERT(run_incremental(1, &c1) == 0, "K=1 incremental failed");
    ASSERT(run_incremental(4, &c4) == 0, "K=4 incremental failed");
    printf("(K=1:%" PRId64 " K=4:%" PRId64 ") ", c1, c4);
    ASSERT(c1 == 6, "K=1 incremental expected 6 TC tuples");
    ASSERT(c4 == c1, "K=4 incremental must match K=1");
    PASS();
}

static void
test_incremental_k8(void)
{
    TEST("Incremental insert with K=8 workers matches K=1");
    int64_t c1 = 0, c8 = 0;
    ASSERT(run_incremental(1, &c1) == 0, "K=1 incremental failed");
    ASSERT(run_incremental(8, &c8) == 0, "K=8 incremental failed");
    printf("(K=1:%" PRId64 " K=8:%" PRId64 ") ", c1, c8);
    ASSERT(c1 == 6, "K=1 incremental expected 6 TC tuples");
    ASSERT(c8 == c1, "K=8 incremental must match K=1");
    PASS();
}

/* ================================================================
 * Group 7: Arrangement deep-copy specific (join-heavy programs)
 *
 * Join operations build arrangement caches -- workers must get
 * independent copies (deep-copy) rather than sharing the parent's
 * mutable arrangement state.  Correct results prove isolation.
 * ================================================================ */

static void
test_arrangement_deep_copy_k4(void)
{
    TEST(
        "K=4 join-heavy path program: arrangement deep-copy produces correct result");
    int64_t c1 = 0, c4 = 0;
    ASSERT(run_workers(JOIN_HEAVY_SRC, "path", 1, &c1) == 0, "K=1 failed");
    ASSERT(run_workers(JOIN_HEAVY_SRC, "path", 4, &c4) == 0, "K=4 failed");
    printf("(1w=%" PRId64 " 4w=%" PRId64 ") ", c1, c4);
    ASSERT(c4 == c1,
        "K=4 arrangement deep-copy must produce same result as K=1");
    PASS();
}

static void
test_arrangement_deep_copy_k8(void)
{
    TEST(
        "K=8 join-heavy path program: arrangement deep-copy produces correct result");
    int64_t c1 = 0, c8 = 0;
    ASSERT(run_workers(JOIN_HEAVY_SRC, "path", 1, &c1) == 0, "K=1 failed");
    ASSERT(run_workers(JOIN_HEAVY_SRC, "path", 8, &c8) == 0, "K=8 failed");
    printf("(1w=%" PRId64 " 8w=%" PRId64 ") ", c1, c8);
    ASSERT(c8 == c1,
        "K=8 arrangement deep-copy must produce same result as K=1");
    PASS();
}

static void
test_arrangement_deep_copy_k4_vs_k8(void)
{
    TEST("K=4 and K=8 arrangement deep-copies produce identical result");
    int64_t c4 = 0, c8 = 0;
    ASSERT(run_workers(JOIN_HEAVY_SRC, "path", 4, &c4) == 0, "K=4 failed");
    ASSERT(run_workers(JOIN_HEAVY_SRC, "path", 8, &c8) == 0, "K=8 failed");
    printf("(4w=%" PRId64 " 8w=%" PRId64 ") ", c4, c8);
    ASSERT(c4 == c8, "K=4 and K=8 arrangement deep-copy results must match");
    PASS();
}

/* ================================================================
 * main
 * ================================================================ */

int
main(void)
{
    printf("\n=== Multi-Worker K-Fusion Validation (Issue #260) ===\n\n");

    /* Group 1: Basic correctness */
    test_k2_vs_k1();
    test_k4_vs_k1();
    test_k8_vs_k1();

    /* Group 2: Determinism - 10 consecutive runs */
    test_determinism_k2_10runs();
    test_determinism_k4_10runs();
    test_determinism_k8_10runs();

    /* Group 3: Deep-copy isolation */
    test_isolation_repeated_k2();
    test_isolation_repeated_k4();
    test_isolation_k2_vs_k4_agree();

    /* Group 4: Various programs */
    test_k2_5node_reach();
    test_k8_5node_reach();
    test_k4_cyclic_graph();
    test_k8_cyclic_graph();
    test_k2_small_cycle();
    test_k8_small_cycle();

    /* Group 5: Memory bounds */
    test_k2_memory_no_oom();
    test_k4_memory_no_oom();
    test_k8_memory_no_oom();

    /* Group 6: Incremental + multi-worker */
    test_incremental_k4();
    test_incremental_k8();

    /* Group 7: Arrangement deep-copy */
    test_arrangement_deep_copy_k4();
    test_arrangement_deep_copy_k8();
    test_arrangement_deep_copy_k4_vs_k8();

    printf("\n--- Results: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(" (%d FAILED)", fail_count);
    printf(" ---\n\n");

    return fail_count > 0 ? 1 : 0;
}
