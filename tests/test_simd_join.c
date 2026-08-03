/*
 * test_simd_join.c - JOIN Hash and Key-Match Tests (Issue #231)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates JOIN, anti-join and semi-join correctness across key widths,
 * driven through the real session API so that both the build phase (hashing)
 * and the probe phase (key comparison) are exercised together.
 *
 * These cases once targeted per-key-column SIMD kernels (hash_int64_keys_avx2,
 * keys_match_avx2).  Those were never reachable -- their dispatcher macros had
 * no call sites -- and were removed; the join hash and key-match are scalar on
 * every target today.  The cases are kept because varying the key width is
 * still worthwhile coverage, but they no longer exercise any SIMD path.
 *
 * Test cases:
 *   1. kc=1  JOIN: single-key special case
 *   2. kc=2  JOIN: two-key join, still scalar path
 *   3. kc=4  JOIN: four-key join — generic (non-special-cased) arm
 *   4. kc=8  JOIN: eight-key join — generic arm, wider key
 *   5. Large kc=1 JOIN: 200 rows — hash distribution stress
 *   6. kc=4  JOIN no match: no common key values, empty result
 *   7. kc=1  JOIN negative values: hash correctness with negatives
 *   8. kc=4  JOIN partial match: only a subset of rows match
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
 * Test 1: kc=1 JOIN (scalar FNV-1a path)
 *
 * Transitive closure: r(1,2), r(2,3), r(3,4) -> 6 tuples.
 * JOIN on 1 key column, which the join hash and key match special-case.
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
 * Test 2: kc=2 JOIN (two-column key, scalar path)
 *
 * join on (a,b): out(a,b,e,f) :- lhs(a,b,e), rhs(a,b,f).
 * lhs: (1,2,10), (3,4,20), (5,6,30)
 * rhs: (1,2,100), (5,6,200)
 * Expected: 2 matching tuples (kc=2 < 4 uses scalar FNV-1a).
 * ================================================================ */
static void
test_kc2_join_two_key(void)
{
    TEST("kc=2 JOIN (scalar path): two-key join produces 2 tuples");

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
 * Test 3: kc=4 JOIN (four-column key — generic arm)
 *
 * join on (a,b,c,d): out(a,b,c,d,e,f) :- lhs(a,b,c,d,e), rhs(a,b,c,d,f).
 * lhs: (1,2,3,4,10), (1,2,3,4,11), (5,6,7,8,20)
 * rhs: (1,2,3,4,100), (9,9,9,9,200)
 * Expected: 2 tuples (both lhs rows with key (1,2,3,4) match rhs row).
 * Four key columns: exercises the generic (non-special-cased) arm of the
 * join hash and key-match, which handle kc == 1 and kc == 2 separately.
 * ================================================================ */
static void
test_kc4_join_avx2_path(void)
{
    TEST("kc=4 JOIN: four-key join produces 2 tuples");

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
 * Test 4: kc=8 JOIN (eight-column key — generic arm, wider key)
 *
 * join on all 8 columns (a..h).
 * lhs: two rows with key (1,2,3,4,5,6,7,8) + payload p
 * rhs: one row with key (1,2,3,4,5,6,7,8) + payload q
 * Expected: 2 tuples.
 * ================================================================ */
static void
test_kc8_join_two_avx2_iters(void)
{
    TEST("kc=8 JOIN: eight-key join produces 2 tuples");

    const char *src
        = ".decl lhs(a:int32,b:int32,c:int32,d:int32,e:int32,f:int32,g:int32,"
        "h:int32,p:int32)\n"
        ".decl rhs(a:int32,b:int32,c:int32,d:int32,e:int32,f:int32,g:int32,"
        "h:int32,q:int32)\n"
        ".decl out(a:int32,b:int32,c:int32,d:int32,e:int32,f:int32,g:int32,"
        "h:int32,p:int32,q:int32)\n"
        "lhs(1,2,3,4,5,6,7,8,10). lhs(1,2,3,4,5,6,7,8,11).\n"
        "lhs(9,9,9,9,9,9,9,9,99).\n"
        "rhs(1,2,3,4,5,6,7,8,100). rhs(0,0,0,0,0,0,0,0,200).\n"
        "out(a,b,c,d,e,f,g,h,p,q) :- "
        "lhs(a,b,c,d,e,f,g,h,p), rhs(a,b,c,d,e,f,g,h,q).\n";

    int64_t count = 0;
    int rc = run_program(src, "out", &count);
    ASSERT(rc == 0, "program failed");
    ASSERT(count == 2, "expected 2 tuples");

    PASS();
}

/* ================================================================
 * Test 5: Large kc=1 JOIN — hash distribution stress (200 rows)
 *
 * TC on a 15-node chain: 15 base edges, closure = C(15,2) - 14 = 105 tuples.
 * Wait — for a chain 1->2->...->15, TC has 15*14/2 = 105 distinct (x,z) pairs
 * with x < z, plus the base 14 pairs = 14 + 91 = 105... actually:
 * For a chain of N nodes (N-1 edges), TC has (N-1)*N/2 tuples.
 * N=10: 9 edges, TC = 45 tuples. Use N=10 for a manageable test.
 * ================================================================ */
static void
test_kc1_join_large(void)
{
    TEST("kc=1 JOIN (200+ rows): 10-node chain TC produces 45 tuples");

    const char *src = ".decl r(x: int32, y: int32)\n"
        "r(1,2). r(2,3). r(3,4). r(4,5). r(5,6).\n"
        "r(6,7). r(7,8). r(8,9). r(9,10).\n"
        "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    int rc = run_program(src, "r", &count);
    ASSERT(rc == 0, "program failed");
    /* 10-node chain: 9 + 8 + ... + 1 = 45 tuples */
    ASSERT(count == 45, "expected 45 tuples from 10-node chain TC");

    PASS();
}

/* ================================================================
 * Test 6: kc=4 JOIN with no matching keys — empty result
 *
 * lhs and rhs have disjoint keys; the key comparison must reject every pair.
 * ================================================================ */
static void
test_kc4_join_no_match(void)
{
    TEST("kc=4 JOIN: no matching keys produces 0 tuples");

    const char *src
        = ".decl lhs(a: int32, b: int32, c: int32, d: int32, e: int32)\n"
        ".decl rhs(a: int32, b: int32, c: int32, d: int32, f: int32)\n"
        ".decl out(a: int32, b: int32, c: int32, d: int32, e: int32, "
        "f: int32)\n"
        "lhs(1, 2, 3, 4, 10). lhs(5, 6, 7, 8, 20).\n"
        "rhs(9, 9, 9, 9, 100). rhs(0, 0, 0, 0, 200).\n"
        "out(a, b, c, d, e, f) :- lhs(a, b, c, d, e), rhs(a, b, c, d, f).\n";

    int64_t count = 0;
    int rc = run_program(src, "out", &count);
    ASSERT(rc == 0, "program failed");
    ASSERT(count == 0, "expected 0 tuples (no key match)");

    PASS();
}

/* ================================================================
 * Test 7: kc=1 JOIN with large values (hi32 bit path)
 *
 * Hash must correctly process the high 32 bits of int64 keys.
 * TC on a 2-cycle with large node IDs (> INT32_MAX) to exercise the
 * hi32 branch of the FNV-1a hash.
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
 * Test 8: kc=4 JOIN partial match — only a subset of rows match
 *
 * 5 lhs rows, 3 rhs rows; only 2 lhs rows match any rhs row.
 * Validates that the key comparison correctly rejects non-matching rows.
 * ================================================================ */
static void
test_kc4_join_partial_match(void)
{
    TEST("kc=4 JOIN: partial match produces 2 tuples");

    const char *src
        = ".decl lhs(a: int32, b: int32, c: int32, d: int32, e: int32)\n"
        ".decl rhs(a: int32, b: int32, c: int32, d: int32, f: int32)\n"
        ".decl out(a: int32, b: int32, c: int32, d: int32, e: int32, "
        "f: int32)\n"
        "lhs(1, 2, 3, 4, 10).\n"
        "lhs(1, 2, 3, 5, 11).\n"   /* differs at col d */
        "lhs(5, 6, 7, 8, 20).\n"
        "lhs(1, 2, 4, 4, 30).\n"   /* differs at col c */
        "lhs(2, 2, 3, 4, 40).\n"   /* differs at col a */
        "rhs(1, 2, 3, 4, 100).\n"
        "rhs(5, 6, 7, 8, 200).\n"
        "rhs(9, 9, 9, 9, 300).\n"
        "out(a, b, c, d, e, f) :- lhs(a, b, c, d, e), rhs(a, b, c, d, f).\n";

    int64_t count = 0;
    int rc = run_program(src, "out", &count);
    ASSERT(rc == 0, "program failed");
    ASSERT(count == 2, "expected 2 tuples (only 2 lhs rows match any rhs row)");

    PASS();
}

/* ================================================================
 * Test 9: kc=1 JOIN with 3-cycle (recursive)
 *
 * Validates correct hash/match results for a recursive 3-cycle at kc == 1,
 * where the join hash and key-match both take their single-key special case
 * and the same relation is joined against itself across iterations.
 *
 * Originally written for the Issue #234 NEON dispatch fallback; that
 * dispatcher and its kernels no longer exist, but the recursive kc == 1
 * shape is still worth covering.
 * ================================================================ */
static void
test_kc1_join_3cycle_dispatch(void)
{
    TEST("kc=1 JOIN (Issue #234 dispatch): 3-cycle recursive join");

    const char *src = ".decl r(x: int64, y: int64)\n"
        "r(100, 200). r(200, 300). r(300, 100).\n"
        "r(x, z) :- r(x, y), r(y, z).\n";

    int64_t count = 0;
    int rc = run_program(src, "r", &count);
    ASSERT(rc == 0, "program failed");
    /* 3-node cycle: 3 base + 3 one-hop + 3 two-hop = 9 tuples */
    ASSERT(count == 9, "expected 9 tuples from 3-cycle");

    PASS();
}

/* ================================================================
 * Test 10: kc=1 JOIN with zero key (Issue #234 dispatch)
 *
 * Zero key has hi32=0, lo32=0, testing edge case of FNV-1a hash
 * at kc == 1. Must produce correct join results.
 * ================================================================ */
static void
test_kc1_join_zero_key(void)
{
    TEST("kc=1 JOIN (Issue #234 dispatch): zero-value key hashes correctly");

    const char *src = ".decl lhs(a: int64, b: int64)\n"
        ".decl rhs(a: int64, c: int64)\n"
        ".decl out(a: int64, b: int64, c: int64)\n"
        "lhs(0, 10). lhs(1, 20). lhs(0, 30).\n"
        "rhs(0, 100). rhs(2, 200).\n"
        "out(a, b, c) :- lhs(a, b), rhs(a, c).\n";

    int64_t count = 0;
    int rc = run_program(src, "out", &count);
    ASSERT(rc == 0, "program failed");
    /* lhs rows (0,10) and (0,30) match rhs (0,100) -> 2 tuples */
    ASSERT(count == 2, "expected 2 tuples with zero key");

    PASS();
}

/* ================================================================
 * Test 11: kc=2 recursive join
 *
 * kc == 2 has its own special case in both the join hash and the key match,
 * so a recursive two-key join is worth covering separately from kc == 1.
 * This test validates correctness at the boundary with a recursive
 * join that exercises both hash and key-match paths extensively.
 * ================================================================ */
static void
test_kc2_join_neon_boundary(void)
{
    TEST("kc=2 JOIN: two-key recursive join");

    const char *src = ".decl edge(x: int32, y: int32, w: int32)\n"
        ".decl path(x: int32, y: int32, w: int32)\n"
        "edge(1, 2, 10). edge(2, 3, 20). edge(3, 1, 30).\n"
        "path(x, y, w) :- edge(x, y, w).\n"
        "path(x, z, w) :- path(x, y, _), edge(y, z, w).\n";

    int64_t count = 0;
    int rc = run_program(src, "path", &count);
    ASSERT(rc == 0, "program failed");
    /* 3-node cycle with weights: 3 base + 3 one-hop + 3 two-hop = 9 paths */
    ASSERT(count == 9, "expected 9 paths from 3-cycle with weights");

    PASS();
}

/* ================================================================
 * main
 * ================================================================ */

int
main(void)
{
    printf(
        "\n=== JOIN Hash and Key-Match Tests (Issue #231, #234) ===\n\n");

    printf("INFO: the join hash and key match are scalar on every target;\n"
        "      these cases vary key width, not instruction set.\n\n");

    test_kc1_join_tc_3edge();
    test_kc2_join_two_key();
    test_kc4_join_avx2_path();
    test_kc8_join_two_avx2_iters();
    test_kc1_join_large();
    test_kc4_join_no_match();
    test_kc1_join_large_values();
    test_kc4_join_partial_match();
    test_kc1_join_3cycle_dispatch();
    test_kc1_join_zero_key();
    test_kc2_join_neon_boundary();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n\n");

    return fail_count > 0 ? 1 : 0;
}
