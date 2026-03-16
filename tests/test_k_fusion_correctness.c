/*
 * test_k_fusion_correctness.c - K-Fusion Correctness Tests (Phase 5)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Verifies that K-fusion parallel evaluation produces byte-for-byte
 * equivalent output across K=1 (baseline), K=2, K=4, and K=8 workers.
 *
 * Hash strategy: for each tuple in the snapshot, compute a FNV-1a hash
 * over its raw int64_t bytes, then XOR all tuple hashes together.
 * XOR is commutative+associative => order-independent, so different
 * worker scheduling that produces the same set of tuples yields the
 * same fingerprint.
 *
 * Tests:
 *   1. test_k_fusion_correctness_K1  - baseline hash (single worker)
 *   2. test_k_fusion_correctness_K2  - 2 workers == K1
 *   3. test_k_fusion_correctness_K4  - 4 workers == K1
 *   4. test_k_fusion_correctness_K8  - 8 workers == K1
 */

#include "../wirelog/backend.h"
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
 * Order-independent tuple fingerprint
 *
 * FNV-1a over the raw bytes of each int64_t column, then XOR all
 * per-tuple hashes together.  XOR is commutative + associative so
 * the result is independent of tuple delivery order.
 * ---------------------------------------------------------------- */

#define FNV_OFFSET_BASIS UINT64_C(14695981039346656037)
#define FNV_PRIME        UINT64_C(1099511628211)

struct hash_ctx {
    uint64_t fingerprint; /* XOR of per-tuple FNV-1a hashes */
    int64_t  count;
};

static void
hash_cb(const char *relation, const int64_t *row, uint32_t ncols,
        void *user_data)
{
    struct hash_ctx *ctx = (struct hash_ctx *)user_data;

    /* FNV-1a over relation name + all column bytes */
    uint64_t h = FNV_OFFSET_BASIS;

    /* Mix relation name */
    if (relation) {
        for (const char *p = relation; *p; p++) {
            h ^= (uint64_t)(unsigned char)*p;
            h *= FNV_PRIME;
        }
    }

    /* Mix each column value's bytes */
    for (uint32_t c = 0; c < ncols; c++) {
        const unsigned char *bytes = (const unsigned char *)&row[c];
        for (uint32_t b = 0; b < sizeof(int64_t); b++) {
            h ^= (uint64_t)bytes[b];
            h *= FNV_PRIME;
        }
    }

    ctx->fingerprint ^= h;
    ctx->count++;
}

/* ----------------------------------------------------------------
 * Helper: evaluate a program with the given worker count,
 * return fingerprint + tuple count.
 * ---------------------------------------------------------------- */

static int
eval_fingerprint(const char *src, uint32_t num_workers,
                 uint64_t *out_fp, int64_t *out_count)
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

    struct hash_ctx ctx = { 0, 0 };
    rc = wl_session_snapshot(sess, hash_cb, &ctx);
    if (rc != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    if (out_fp)
        *out_fp = ctx.fingerprint;
    if (out_count)
        *out_count = ctx.count;

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return 0;
}

/* ----------------------------------------------------------------
 * Shared program: TC over a 7-node chain.
 *
 *   edge(1,2). edge(2,3). edge(3,4). edge(4,5). edge(5,6). edge(6,7).
 *   tc(x,y) :- edge(x,y).
 *   tc(x,z) :- tc(x,y), edge(y,z).
 *
 * Expected TC tuples: 7*(7-1)/2 = 21.
 * Expected total (edge + tc): 6 + 21 = 27.
 * ---------------------------------------------------------------- */

static const char *TC_SRC =
    ".decl edge(x: int32, y: int32)\n"
    "edge(1, 2). edge(2, 3). edge(3, 4).\n"
    "edge(4, 5). edge(5, 6). edge(6, 7).\n"
    ".decl tc(x: int32, y: int32)\n"
    "tc(x, y) :- edge(x, y).\n"
    "tc(x, z) :- tc(x, y), edge(y, z).\n";

/* ================================================================
 * Test 1: K=1 baseline — establishes the reference fingerprint
 * ================================================================ */

static uint64_t g_fp_k1 = 0;
static int64_t  g_count_k1 = 0;

static void
test_k_fusion_correctness_K1(void)
{
    TEST("K=1 (baseline): TC 7-node chain hash established");

    int rc = eval_fingerprint(TC_SRC, 1, &g_fp_k1, &g_count_k1);
    ASSERT(rc == 0, "K=1 evaluation failed");
    ASSERT(g_count_k1 > 0, "K=1 produced zero tuples");

    /* TC over 7-node chain: 7*(7-1)/2 = 21 pairs.
     * Snapshot returns IDB (derived) relations only. */
    ASSERT(g_count_k1 == 21, "K=1 expected 21 tc tuples (7-node chain)");

    printf("(fp=0x%016" PRIx64 " count=%" PRId64 ") ", g_fp_k1, g_count_k1);

    PASS();
}

/* ================================================================
 * Test 2: K=2 — output must match K=1 fingerprint
 * ================================================================ */

static void
test_k_fusion_correctness_K2(void)
{
    TEST("K=2 output matches K=1 fingerprint");

    uint64_t fp2 = 0;
    int64_t  cnt2 = 0;
    int rc = eval_fingerprint(TC_SRC, 2, &fp2, &cnt2);
    ASSERT(rc == 0, "K=2 evaluation failed");

    printf("(fp=0x%016" PRIx64 " count=%" PRId64 ") ", fp2, cnt2);

    char msg[128];
    snprintf(msg, sizeof(msg),
             "K=2 count %" PRId64 " != K=1 count %" PRId64, cnt2, g_count_k1);
    ASSERT(cnt2 == g_count_k1, msg);

    snprintf(msg, sizeof(msg),
             "K=2 fp 0x%016" PRIx64 " != K=1 fp 0x%016" PRIx64, fp2, g_fp_k1);
    ASSERT(fp2 == g_fp_k1, msg);

    PASS();
}

/* ================================================================
 * Test 3: K=4 — output must match K=1 fingerprint
 * ================================================================ */

static void
test_k_fusion_correctness_K4(void)
{
    TEST("K=4 output matches K=1 fingerprint");

    uint64_t fp4 = 0;
    int64_t  cnt4 = 0;
    int rc = eval_fingerprint(TC_SRC, 4, &fp4, &cnt4);
    ASSERT(rc == 0, "K=4 evaluation failed");

    printf("(fp=0x%016" PRIx64 " count=%" PRId64 ") ", fp4, cnt4);

    char msg[128];
    snprintf(msg, sizeof(msg),
             "K=4 count %" PRId64 " != K=1 count %" PRId64, cnt4, g_count_k1);
    ASSERT(cnt4 == g_count_k1, msg);

    snprintf(msg, sizeof(msg),
             "K=4 fp 0x%016" PRIx64 " != K=1 fp 0x%016" PRIx64, fp4, g_fp_k1);
    ASSERT(fp4 == g_fp_k1, msg);

    PASS();
}

/* ================================================================
 * Test 4: K=8 — output must match K=1 fingerprint (DOOP scale)
 * ================================================================ */

static void
test_k_fusion_correctness_K8(void)
{
    TEST("K=8 output matches K=1 fingerprint");

    uint64_t fp8 = 0;
    int64_t  cnt8 = 0;
    int rc = eval_fingerprint(TC_SRC, 8, &fp8, &cnt8);
    ASSERT(rc == 0, "K=8 evaluation failed");

    printf("(fp=0x%016" PRIx64 " count=%" PRId64 ") ", fp8, cnt8);

    char msg[128];
    snprintf(msg, sizeof(msg),
             "K=8 count %" PRId64 " != K=1 count %" PRId64, cnt8, g_count_k1);
    ASSERT(cnt8 == g_count_k1, msg);

    snprintf(msg, sizeof(msg),
             "K=8 fp 0x%016" PRIx64 " != K=1 fp 0x%016" PRIx64, fp8, g_fp_k1);
    ASSERT(fp8 == g_fp_k1, msg);

    PASS();
}

/* ================================================================
 * Test 5: cyclic graph — K=1,2,4,8 all match (harder convergence)
 *
 *   edge: 1->2, 2->3, 3->4, 4->5, 5->1  (5-node cycle)
 *   All pairs reachable => 5*5 = 25 tc tuples + 5 edge = 30 total.
 * ================================================================ */

static void
test_k_fusion_correctness_cyclic(void)
{
    TEST("cyclic graph: K=2,4,8 all match K=1");

    static const char *cyc_src =
        ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2). edge(2, 3). edge(3, 4). edge(4, 5). edge(5, 1).\n"
        ".decl tc(x: int32, y: int32)\n"
        "tc(x, y) :- edge(x, y).\n"
        "tc(x, z) :- tc(x, y), edge(y, z).\n";

    uint64_t fp1 = 0; int64_t cnt1 = 0;
    uint64_t fp2 = 0; int64_t cnt2 = 0;
    uint64_t fp4 = 0; int64_t cnt4 = 0;
    uint64_t fp8 = 0; int64_t cnt8 = 0;

    ASSERT(eval_fingerprint(cyc_src, 1, &fp1, &cnt1) == 0, "K=1 failed");
    ASSERT(eval_fingerprint(cyc_src, 2, &fp2, &cnt2) == 0, "K=2 failed");
    ASSERT(eval_fingerprint(cyc_src, 4, &fp4, &cnt4) == 0, "K=4 failed");
    ASSERT(eval_fingerprint(cyc_src, 8, &fp8, &cnt8) == 0, "K=8 failed");

    /* 5-node cycle: all nodes reach all others => 5*5 = 25 tc tuples (IDB only) */
    ASSERT(cnt1 == 25, "K=1 cyclic: expected 25 tc tuples");

    printf("(cnt=%"PRId64" fp2=%s fp4=%s fp8=%s) ",
           cnt1,
           fp2 == fp1 ? "match" : "MISMATCH",
           fp4 == fp1 ? "match" : "MISMATCH",
           fp8 == fp1 ? "match" : "MISMATCH");

    ASSERT(cnt2 == cnt1, "K=2 cyclic count mismatch");
    ASSERT(cnt4 == cnt1, "K=4 cyclic count mismatch");
    ASSERT(cnt8 == cnt1, "K=8 cyclic count mismatch");
    ASSERT(fp2 == fp1,   "K=2 cyclic fingerprint mismatch");
    ASSERT(fp4 == fp1,   "K=4 cyclic fingerprint mismatch");
    ASSERT(fp8 == fp1,   "K=8 cyclic fingerprint mismatch");

    PASS();
}

/* ----------------------------------------------------------------
 * main
 * ---------------------------------------------------------------- */

int
main(void)
{
    printf("=== K-Fusion Correctness Tests (Phase 5) ===\n\n");

    /* K1 must run first — sets g_fp_k1 / g_count_k1 baseline */
    test_k_fusion_correctness_K1();
    test_k_fusion_correctness_K2();
    test_k_fusion_correctness_K4();
    test_k_fusion_correctness_K8();
    test_k_fusion_correctness_cyclic();

    printf("\n=== Results: %d passed, %d failed (of %d) ===\n",
           pass_count, fail_count, test_count);

    return fail_count > 0 ? 1 : 0;
}
