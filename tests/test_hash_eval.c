/*
 * test_hash_eval.c - Hash Function Evaluator Tests (Issue #144)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * End-to-end tests that parse a Datalog program using hash(),
 * run it through the columnar evaluator, and verify the output
 * tuples contain correct xxHash3-computed values.
 *
 * Expected hash values (XXH3_64bits over int64_t, interpreted as int64_t):
 *   hash(0)   = (int64_t)14374147212387527897 = -4072596861321023719
 *   hash(1)   = (int64_t)3439722301264460078  = 3439722301264460078
 *   hash(42)  = (int64_t)15395265915043915720 = -3051478158665635896
 *   hash(100) = (int64_t)13278640519796112475 = -5168103553913439141
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

/* ======================================================================== */
/* Test Helpers                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                      \
    do {                                \
        tests_run++;                    \
        printf("  [TEST] %-55s", name); \
        fflush(stdout);                 \
    } while (0)

#define PASS()             \
    do {                   \
        tests_passed++;    \
        printf(" PASS\n"); \
    } while (0)

#define FAIL(msg)                   \
    do {                            \
        tests_failed++;             \
        printf(" FAIL: %s\n", msg); \
    } while (0)

/* ======================================================================== */
/* Expected hash values (precomputed via (int64_t)XXH3_64bits over int64_t) */
/* ======================================================================== */

#define HASH_OF_0 ((int64_t) - 4072596861322023719LL)
#define HASH_OF_1 ((int64_t)3439722301264460078LL)
#define HASH_OF_42 ((int64_t) - 3051478158665635896LL)
#define HASH_OF_100 ((int64_t) - 5168103553913439141LL)

/* ======================================================================== */
/* Result Capture Callback                                                 */
/* ======================================================================== */

struct result_ctx {
    int64_t values[32];
    uint32_t count;
};

static void
capture_cb(const char *relation, const int64_t *row, uint32_t ncols,
           void *user_data)
{
    struct result_ctx *ctx = (struct result_ctx *)user_data;
    (void)relation;
    if (ctx->count < 32 && ncols >= 1)
        ctx->values[ctx->count++] = row[0];
}

/* ======================================================================== */
/* Helper: run program, return count of output tuples                      */
/* ======================================================================== */

static int
run_hash_program(const char *src, int64_t *out_values, int max_values)
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

    struct result_ctx ctx = { .count = 0 };
    if (wl_session_snapshot(sess, capture_cb, &ctx) != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    int n = (int)ctx.count;
    if (n > max_values)
        n = max_values;
    for (int i = 0; i < n; i++)
        out_values[i] = ctx.values[i];

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return n;
}

/* ======================================================================== */
/* Determinism Tests                                                        */
/* ======================================================================== */

static void
test_eval_hash_zero(void)
{
    TEST("hash(0) produces deterministic xxHash3 value");

    const char *src = ".decl a(x: int64)\n"
                      "a(0).\n"
                      ".decl r(z: int64)\n"
                      "r(hash(x)) :- a(x).\n";

    int64_t vals[4];
    int n = run_hash_program(src, vals, 4);

    if (n < 0) {
        FAIL("evaluation failed");
        return;
    }
    if (n != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 tuple, got %d", n);
        FAIL(buf);
        return;
    }
    if (vals[0] != HASH_OF_0) {
        char buf[128];
        snprintf(buf, sizeof(buf), "expected %" PRId64 ", got %" PRId64,
                 HASH_OF_0, vals[0]);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_hash_one(void)
{
    TEST("hash(1) produces deterministic xxHash3 value");

    const char *src = ".decl a(x: int64)\n"
                      "a(1).\n"
                      ".decl r(z: int64)\n"
                      "r(hash(x)) :- a(x).\n";

    int64_t vals[4];
    int n = run_hash_program(src, vals, 4);

    if (n != 1 || vals[0] != HASH_OF_1) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "expected %" PRId64 ", got %" PRId64 " (n=%d)", HASH_OF_1,
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_hash_forty_two(void)
{
    TEST("hash(42) produces deterministic xxHash3 value");

    const char *src = ".decl a(x: int64)\n"
                      "a(42).\n"
                      ".decl r(z: int64)\n"
                      "r(hash(x)) :- a(x).\n";

    int64_t vals[4];
    int n = run_hash_program(src, vals, 4);

    if (n != 1 || vals[0] != HASH_OF_42) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "expected %" PRId64 ", got %" PRId64 " (n=%d)", HASH_OF_42,
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_hash_hundred(void)
{
    TEST("hash(100) produces deterministic xxHash3 value");

    const char *src = ".decl a(x: int64)\n"
                      "a(100).\n"
                      ".decl r(z: int64)\n"
                      "r(hash(x)) :- a(x).\n";

    int64_t vals[4];
    int n = run_hash_program(src, vals, 4);

    if (n != 1 || vals[0] != HASH_OF_100) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "expected %" PRId64 ", got %" PRId64 " (n=%d)", HASH_OF_100,
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Distribution Tests                                                       */
/* ======================================================================== */

static void
test_eval_hash_different_inputs_different_hashes(void)
{
    TEST("hash: different inputs produce different hash values");

    /* hash(0), hash(1), hash(42) should all be distinct */
    const char *src = ".decl a(x: int64)\n"
                      "a(0). a(1). a(42).\n"
                      ".decl r(z: int64)\n"
                      "r(hash(x)) :- a(x).\n";

    int64_t vals[8];
    int n = run_hash_program(src, vals, 8);

    if (n != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 tuples, got %d", n);
        FAIL(buf);
        return;
    }
    /* Check all three are distinct */
    if (vals[0] == vals[1] || vals[0] == vals[2] || vals[1] == vals[2]) {
        FAIL("hash collision: different inputs produced same hash");
        return;
    }
    PASS();
}

static void
test_eval_hash_same_input_same_hash(void)
{
    TEST("hash: same input always produces same hash (idempotent)");

    /* hash(x) = hash(x) always */
    const char *src = ".decl a(x: int64)\n"
                      "a(42).\n"
                      ".decl r(z: int64)\n"
                      "r(1) :- a(x), hash(x) = hash(x).\n";

    int64_t vals[4];
    int n = run_hash_program(src, vals, 4);

    if (n != 1 || vals[0] != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 tuple with val 1, got n=%d", n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Filter / Comparison Tests                                               */
/* ======================================================================== */

static void
test_eval_hash_filter_equality(void)
{
    TEST("hash(x) = hash(y) filter passes when x = y");

    /* x=42, y=42 -> hash(42)=hash(42), x=42,y=1 -> hash(42)!=hash(1) */
    const char *src = ".decl a(x: int64, y: int64)\n"
                      "a(42, 42). a(42, 1).\n"
                      ".decl r(x: int64)\n"
                      "r(x) :- a(x, y), hash(x) = hash(y).\n";

    int64_t vals[4];
    int n = run_hash_program(src, vals, 4);

    if (n != 1 || vals[0] != 42) {
        char buf[64];
        snprintf(buf, sizeof(buf),
                 "expected 1 tuple with val 42, got n=%d val=%" PRId64, n,
                 n > 0 ? vals[0] : -1LL);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Modulo / Bucketing Tests                                                */
/* ======================================================================== */

static void
test_eval_hash_modulo_bucket(void)
{
    TEST("hash(x) % 16 produces deterministic signed-modulo result");

    /* hash(0) = -4072596861322023719; -4072596861322023719 % 16 = -7 */
    const char *src = ".decl a(x: int64)\n"
                      "a(0).\n"
                      ".decl r(z: int64)\n"
                      "r(hash(x) % 16) :- a(x).\n";

    int64_t vals[4];
    int n = run_hash_program(src, vals, 4);

    if (n != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 tuple, got %d", n);
        FAIL(buf);
        return;
    }
    /* C signed modulo: HASH_OF_0 % 16 = -7 */
    int64_t expected = HASH_OF_0 % 16;
    if (vals[0] != expected) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected %" PRId64 ", got %" PRId64,
                 expected, vals[0]);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_hash_combined_with_band(void)
{
    TEST("band(hash(x), 15) extracts low nibble of hash");

    const char *src = ".decl a(x: int64)\n"
                      "a(42).\n"
                      ".decl r(z: int64)\n"
                      "r(band(hash(x), 15)) :- a(x).\n";

    int64_t vals[4];
    int n = run_hash_program(src, vals, 4);

    if (n != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 tuple, got %d", n);
        FAIL(buf);
        return;
    }
    /* band with 15 masks low nibble: result must be in [0, 15] */
    if (vals[0] < 0 || vals[0] > 15) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected value in [0,15], got %" PRId64,
                 vals[0]);
        FAIL(buf);
        return;
    }
    /* Verify: band(hash(42), 15) = HASH_OF_42 & 15 */
    int64_t expected = HASH_OF_42 & 15;
    if (vals[0] != expected) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected %" PRId64 ", got %" PRId64,
                 expected, vals[0]);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== wirelog Hash Eval Tests (Issue #144) ===\n\n");

    printf("--- Determinism Tests ---\n");
    test_eval_hash_zero();
    test_eval_hash_one();
    test_eval_hash_forty_two();
    test_eval_hash_hundred();

    printf("\n--- Distribution Tests ---\n");
    test_eval_hash_different_inputs_different_hashes();
    test_eval_hash_same_input_same_hash();

    printf("\n--- Filter / Comparison Tests ---\n");
    test_eval_hash_filter_equality();

    printf("\n--- Modulo / Bucketing Tests ---\n");
    test_eval_hash_modulo_bucket();
    test_eval_hash_combined_with_band();

    printf("\n=== Results: %d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf(" ===\n");

    return tests_failed > 0 ? 1 : 0;
}
