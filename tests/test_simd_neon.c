/*
 * test_simd_neon.c - ARM NEON SIMD Hash and Key-Match Tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates correctness of the NEON-accelerated hash (hash_int64_keys_neon)
 * and key-match (keys_match_neon) paths used in the JOIN, anti-join, and
 * semi-join operators.  Tests are performed via the real session API so that
 * both the build phase (hash) and the probe phase (key comparison) are
 * exercised together.
 *
 * On non-ARM builds the same tests run through the scalar fallback path,
 * verifying that the dispatcher selects a correct implementation regardless
 * of architecture.
 *
 * Test cases:
 *   1. kc=1  JOIN: scalar FNV-1a path (kc < 2 fallback)
 *   2. kc=2  JOIN: two-key join -- first NEON iteration
 *   3. kc=4  JOIN: four-key join -- two full NEON iterations
 *   4. kc=2  JOIN no match: disjoint keys produce empty result
 *   5. kc=1  JOIN negative values: hash correctness with negatives
 *   6. kc=2  JOIN partial match: only subset of rows match
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
 * Helper: tuple counter callback
 * ---------------------------------------------------------------- */

struct count_ctx {
    int64_t count;
    const char *tracked;
};

static void
count_cb(const char *rel, const int64_t *row, uint32_t nc, void *u)
{
    (void)row;
    (void)nc;
    struct count_ctx *ctx = (struct count_ctx *)u;
    if (rel && ctx->tracked && strcmp(rel, ctx->tracked) == 0)
        ctx->count++;
}

/* ----------------------------------------------------------------
 * Helper: run a Datalog program, return tuple count for `rel`
 * ---------------------------------------------------------------- */

static int
run_program(const char *src, const char *rel, int64_t *out_count)
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

    if (out_count)
        *out_count = ctx.count;

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return rc;
}

/* ================================================================
 * Test 1: kc=1 JOIN (scalar FNV-1a path, kc < 2 fallback)
 *
 * Transitive closure: r(1,2), r(2,3), r(3,4) -> 6 tuples.
 * JOIN on 1 key column (kc=1 < 2 falls through to scalar FNV-1a).
 * ================================================================ */
static void
test_kc1_join_tc_3edge(void)
{
    TEST("kc=1 JOIN (scalar path): TC 3-edge produces 6 tuples");

    const char *src = ".decl r(x: int32, y: int32)\n"
                      "r(1, 2). r(2, 3). r(3, 4).\n"
                      "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    int rc = run_program(src, "r", &count);
    ASSERT(rc == 0, "program failed");
    ASSERT(count == 6, "expected 6 tuples");

    PASS();
}

/* ================================================================
 * Test 2: kc=2 JOIN (two-column key -- first NEON iteration)
 *
 * join on (a,b): out(a,b,e,f) :- lhs(a,b,e), rhs(a,b,f).
 * lhs: (1,2,10), (3,4,20), (5,6,30)
 * rhs: (1,2,100), (5,6,200)
 * Expected: 2 matching tuples.
 * NEON path: hash_int64_keys_neon processes both key cols in one vceqq_s64;
 * keys_match_neon compares both cols in one NEON iteration.
 * ================================================================ */
static void
test_kc2_join_neon_path(void)
{
    TEST("kc=2 JOIN (NEON path): two-key join produces 2 tuples");

    const char *src = ".decl lhs(a: int32, b: int32, e: int32)\n"
                      ".decl rhs(a: int32, b: int32, f: int32)\n"
                      ".decl out(a: int32, b: int32, e: int32, f: int32)\n"
                      "lhs(1, 2, 10). lhs(3, 4, 20). lhs(5, 6, 30).\n"
                      "rhs(1, 2, 100). rhs(5, 6, 200).\n"
                      "out(a, b, e, f) :- lhs(a, b, e), rhs(a, b, f).\n";

    int64_t count = 0;
    int rc = run_program(src, "out", &count);
    ASSERT(rc == 0, "program failed");
    ASSERT(count == 2, "expected 2 tuples");

    PASS();
}

/* ================================================================
 * Test 3: kc=4 JOIN (four-column key -- two full NEON iterations)
 *
 * join on (a,b,c,d): out(a,b,c,d,e,f) :- lhs(a,b,c,d,e), rhs(a,b,c,d,f).
 * lhs: (1,2,3,4,10), (1,2,3,4,11), (5,6,7,8,20)
 * rhs: (1,2,3,4,100), (9,9,9,9,200)
 * Expected: 2 tuples (both lhs rows with key (1,2,3,4) match).
 * NEON path: two iterations of 2-key NEON comparison cover all 4 columns.
 * ================================================================ */
static void
test_kc4_join_two_neon_iters(void)
{
    TEST("kc=4 JOIN (2x NEON iterations): four-key join produces 2 tuples");

    const char *src
        = ".decl lhs(a: int32, b: int32, c: int32, d: int32, e: int32)\n"
          ".decl rhs(a: int32, b: int32, c: int32, d: int32, f: int32)\n"
          ".decl out(a: int32, b: int32, c: int32, d: int32, e: int32, "
          "f: int32)\n"
          "lhs(1, 2, 3, 4, 10). lhs(1, 2, 3, 4, 11). lhs(5, 6, 7, 8, 20).\n"
          "rhs(1, 2, 3, 4, 100). rhs(9, 9, 9, 9, 200).\n"
          "out(a, b, c, d, e, f) :- lhs(a, b, c, d, e), rhs(a, b, c, d, f).\n";

    int64_t count = 0;
    int rc = run_program(src, "out", &count);
    ASSERT(rc == 0, "program failed");
    ASSERT(count == 2, "expected 2 tuples");

    PASS();
}

/* ================================================================
 * Test 4: kc=2 JOIN with no matching keys -- empty result
 *
 * lhs and rhs have disjoint two-column keys; NEON comparison detects mismatch.
 * ================================================================ */
static void
test_kc2_join_no_match(void)
{
    TEST("kc=2 JOIN (NEON path): no matching keys produces 0 tuples");

    const char *src = ".decl lhs(a: int32, b: int32, e: int32)\n"
                      ".decl rhs(a: int32, b: int32, f: int32)\n"
                      ".decl out(a: int32, b: int32, e: int32, f: int32)\n"
                      "lhs(1, 2, 10). lhs(3, 4, 20).\n"
                      "rhs(9, 9, 100). rhs(0, 0, 200).\n"
                      "out(a, b, e, f) :- lhs(a, b, e), rhs(a, b, f).\n";

    int64_t count = 0;
    int rc = run_program(src, "out", &count);
    ASSERT(rc == 0, "program failed");
    ASSERT(count == 0, "expected 0 tuples (no key match)");

    PASS();
}

/* ================================================================
 * Test 5: kc=1 JOIN with large values (hi32 bit path)
 *
 * Hash must correctly process values > INT32_MAX to exercise the high32
 * FNV-1a round in the hash function.
 * TC on a 2-cycle with large positive int64 node IDs.
 * ================================================================ */
static void
test_kc1_join_large_values(void)
{
    TEST("kc=1 JOIN (large values, hi32 path): 2-cycle produces 4 tuples");

    /* 3000000000 > 2^31-1, so hi32 != 0 -- exercises hi32 FNV-1a round. */
    const char *src = ".decl r(x: int64, y: int64)\n"
                      "r(3000000000, 4000000000). r(4000000000, 3000000000).\n"
                      "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    int rc = run_program(src, "r", &count);
    ASSERT(rc == 0, "program failed");
    ASSERT(count == 4, "expected 4 tuples from large-value 2-cycle");

    PASS();
}

/* ================================================================
 * Test 6: kc=2 JOIN partial match -- only a subset of rows match
 *
 * 4 lhs rows, 2 rhs rows; only 2 lhs rows match any rhs row.
 * Validates that the NEON comparison correctly rejects non-matching rows.
 * ================================================================ */
static void
test_kc2_join_partial_match(void)
{
    TEST("kc=2 JOIN (NEON path): partial match produces 2 tuples");

    const char *src = ".decl lhs(a: int32, b: int32, e: int32)\n"
                      ".decl rhs(a: int32, b: int32, f: int32)\n"
                      ".decl out(a: int32, b: int32, e: int32, f: int32)\n"
                      "lhs(1, 2, 10).\n"
                      "lhs(1, 3, 11).\n" /* differs at col b */
                      "lhs(5, 6, 20).\n"
                      "lhs(2, 2, 30).\n" /* differs at col a */
                      "rhs(1, 2, 100).\n"
                      "rhs(5, 6, 200).\n"
                      "out(a, b, e, f) :- lhs(a, b, e), rhs(a, b, f).\n";

    int64_t count = 0;
    int rc = run_program(src, "out", &count);
    ASSERT(rc == 0, "program failed");
    ASSERT(count == 2, "expected 2 tuples (only 2 lhs rows match any rhs)");

    PASS();
}

/* ================================================================
 * main
 * ================================================================ */

int
main(void)
{
    printf("\n=== ARM NEON SIMD Hash and Key-Match Tests ===\n\n");

#ifdef __ARM_NEON__
    printf("INFO: ARM NEON available -- NEON paths will be exercised\n\n");
#elif defined(__AVX2__)
    printf("INFO: AVX2 available (not ARM) -- AVX2 paths will be used\n\n");
#else
    printf("INFO: No SIMD -- scalar fallback paths will be used\n\n");
#endif

    test_kc1_join_tc_3edge();
    test_kc2_join_neon_path();
    test_kc4_join_two_neon_iters();
    test_kc2_join_no_match();
    test_kc1_join_large_values();
    test_kc2_join_partial_match();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n\n");

    return fail_count > 0 ? 1 : 0;
}
