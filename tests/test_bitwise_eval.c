/*
 * test_bitwise_eval.c - Bitwise Operator Evaluator Tests (Issue #72)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * TDD RED phase: end-to-end tests that parse a Datalog program using
 * bitwise operators, run it through the columnar evaluator, and verify
 * the output tuples contain correct bitwise-computed values.
 *
 * Evaluation pattern:
 *   .decl a(x: int64, y: int64)
 *   a(X, Y).    <- inline fact with known values
 *   .decl r(z: int64)
 *   r(band(x, y)) :- a(x, y).
 *   -> snapshot produces r(X & Y)
 *
 * These tests FAIL until US-001 + US-002 + US-003 + US-004 are implemented.
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
/* Result Capture Callback                                                 */
/* ======================================================================== */

struct result_ctx {
    int64_t values[32]; /* captured first column values */
    uint32_t count;
    int error;
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
/* Helper: run a single-rule program, return first output value            */
/* ======================================================================== */

/*
 * run_bitwise_program:
 *   Parses @src, applies optimization passes, runs snapshot, and stores
 *   all output first-column values in @out_values (up to @max_values).
 *   Returns the number of output tuples, or -1 on evaluation error.
 */
static int
run_bitwise_program(const char *src, int64_t *out_values, int max_values)
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

    struct result_ctx ctx = { .count = 0, .error = 0 };
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
/* band tests                                                               */
/* ======================================================================== */

static void
test_eval_band_0xFF_and_0x0F(void)
{
    TEST("band: 0xFF & 0x0F = 0x0F (255 & 15 = 15)");

    const char *src = ".decl a(x: int64)\n"
                      "a(255).\n"
                      ".decl r(z: int64)\n"
                      "r(band(x, 15)) :- a(x).\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

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
    if (vals[0] != 15) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 15, got %" PRId64, vals[0]);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_band_0xAA_and_0xF0(void)
{
    TEST("band: 0xAA & 0xF0 = 0xA0 (170 & 240 = 160)");

    const char *src = ".decl a(x: int64, y: int64)\n"
                      "a(170, 240).\n"
                      ".decl r(z: int64)\n"
                      "r(band(x, y)) :- a(x, y).\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

    if (n != 1 || vals[0] != 160) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 160, got %" PRId64 " (n=%d)",
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_band_zero_mask(void)
{
    TEST("band: any & 0 = 0");

    const char *src = ".decl a(x: int64)\n"
                      "a(12345).\n"
                      ".decl r(z: int64)\n"
                      "r(band(x, 0)) :- a(x).\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

    if (n != 1 || vals[0] != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0, got %" PRId64 " (n=%d)",
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* bor tests                                                                */
/* ======================================================================== */

static void
test_eval_bor_0xF0_or_0x0F(void)
{
    TEST("bor: 0xF0 | 0x0F = 0xFF (240 | 15 = 255)");

    const char *src = ".decl a(x: int64, y: int64)\n"
                      "a(240, 15).\n"
                      ".decl r(z: int64)\n"
                      "r(bor(x, y)) :- a(x, y).\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

    if (n != 1 || vals[0] != 255) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 255, got %" PRId64 " (n=%d)",
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_bor_identity_zero(void)
{
    TEST("bor: x | 0 = x (identity element)");

    const char *src = ".decl a(x: int64)\n"
                      "a(42).\n"
                      ".decl r(z: int64)\n"
                      "r(bor(x, 0)) :- a(x).\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

    if (n != 1 || vals[0] != 42) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 42, got %" PRId64 " (n=%d)",
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* bxor tests                                                               */
/* ======================================================================== */

static void
test_eval_bxor_0xFF_xor_0x0F(void)
{
    TEST("bxor: 0xFF ^ 0x0F = 0xF0 (255 ^ 15 = 240)");

    const char *src = ".decl a(x: int64, y: int64)\n"
                      "a(255, 15).\n"
                      ".decl r(z: int64)\n"
                      "r(bxor(x, y)) :- a(x, y).\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

    if (n != 1 || vals[0] != 240) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 240, got %" PRId64 " (n=%d)",
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_bxor_self_is_zero(void)
{
    TEST("bxor: x ^ x = 0 (self-cancellation)");

    const char *src = ".decl a(x: int64)\n"
                      "a(99).\n"
                      ".decl r(z: int64)\n"
                      "r(bxor(x, x)) :- a(x).\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

    if (n != 1 || vals[0] != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0, got %" PRId64 " (n=%d)",
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* bnot tests                                                               */
/* ======================================================================== */

static void
test_eval_bnot_zero(void)
{
    TEST("bnot: ~0 = -1 (all bits set in int64)");

    const char *src = ".decl a(x: int64)\n"
                      "a(0).\n"
                      ".decl r(z: int64)\n"
                      "r(bnot(x)) :- a(x).\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

    if (n != 1 || vals[0] != (int64_t)(-1)) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected -1, got %" PRId64 " (n=%d)",
                 n > 0 ? vals[0] : 0LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_bnot_involution(void)
{
    TEST("bnot: ~~x = x (double complement is identity)");

    /* We verify by checking bnot(bnot(x)) = x */
    const char *src = ".decl a(x: int64)\n"
                      "a(42).\n"
                      ".decl r(z: int64)\n"
                      "r(bnot(bnot(x))) :- a(x).\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

    if (n != 1 || vals[0] != 42) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 42, got %" PRId64 " (n=%d)",
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* bshl tests                                                               */
/* ======================================================================== */

static void
test_eval_bshl_1_by_4(void)
{
    TEST("bshl: 1 << 4 = 16");

    const char *src = ".decl a(x: int64)\n"
                      "a(1).\n"
                      ".decl r(z: int64)\n"
                      "r(bshl(x, 4)) :- a(x).\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

    if (n != 1 || vals[0] != 16) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 16, got %" PRId64 " (n=%d)",
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_bshl_byte_to_high(void)
{
    TEST("bshl: 1 << 8 = 256 (byte-level shift)");

    const char *src = ".decl a(x: int64)\n"
                      "a(1).\n"
                      ".decl r(z: int64)\n"
                      "r(bshl(x, 8)) :- a(x).\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

    if (n != 1 || vals[0] != 256) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 256, got %" PRId64 " (n=%d)",
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* bshr tests                                                               */
/* ======================================================================== */

static void
test_eval_bshr_256_by_4(void)
{
    TEST("bshr: 256 >> 4 = 16");

    const char *src = ".decl a(x: int64)\n"
                      "a(256).\n"
                      ".decl r(z: int64)\n"
                      "r(bshr(x, 4)) :- a(x).\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

    if (n != 1 || vals[0] != 16) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 16, got %" PRId64 " (n=%d)",
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_bshr_nibble_extraction(void)
{
    TEST("bshr: 0xAB >> 4 = 0x0A (extract high nibble)");

    /* 0xAB = 171, 0x0A = 10 */
    const char *src = ".decl a(x: int64)\n"
                      "a(171).\n"
                      ".decl r(z: int64)\n"
                      "r(bshr(x, 4)) :- a(x).\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

    if (n != 1 || vals[0] != 10) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 10, got %" PRId64 " (n=%d)",
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Compound / Protocol Analysis Pattern Tests                              */
/* ======================================================================== */

static void
test_eval_extract_nibble_pattern(void)
{
    TEST("extract nibble: band(bshr(x, 4), 15) on 0xAB = 0x0A = 10");

    /* 0xAB = 171; high nibble = (171 >> 4) & 0x0F = 10 & 15 = 10 */
    const char *src = ".decl a(x: int64)\n"
                      "a(171).\n"
                      ".decl r(z: int64)\n"
                      "r(band(bshr(x, 4), 15)) :- a(x).\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

    if (n != 1 || vals[0] != 10) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 10, got %" PRId64 " (n=%d)",
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_reconstruct_16bit_value(void)
{
    TEST("reconstruct 16-bit: bor(bshl(hi, 8), lo) on (0x0A, 0xBC) = 0x0ABC");

    /* hi=10 (0x0A), lo=188 (0xBC); result = (10 << 8) | 188 = 2560 + 188 = 2748 = 0x0ABC */
    const char *src = ".decl a(hi: int64, lo: int64)\n"
                      "a(10, 188).\n"
                      ".decl r(z: int64)\n"
                      "r(bor(bshl(hi, 8), lo)) :- a(hi, lo).\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

    if (n != 1 || vals[0] != 2748) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 2748, got %" PRId64 " (n=%d)",
                 n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_check_error_flag(void)
{
    TEST("check error flag: band(x, 4) filter passes when bit 2 set");

    /* error_flags = 7 (bits 0,1,2 set); band(7, 4) = 4 != 0 -> tuple included */
    const char *src = ".decl a(x: int64)\n"
                      "a(7). a(2). a(1).\n"
                      ".decl r(z: int64)\n"
                      "r(x) :- a(x), band(x, 4) != 0.\n";

    int64_t vals[4];
    int n = run_bitwise_program(src, vals, 4);

    /* Only x=7 has bit 2 set (7 & 4 = 4 != 0); x=2 -> 2&4=0, x=1 -> 1&4=0 */
    if (n != 1 || vals[0] != 7) {
        char buf[64];
        snprintf(buf, sizeof(buf),
                 "expected 1 tuple with value 7, got n=%d val=%" PRId64, n,
                 n > 0 ? vals[0] : -1LL);
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
    printf("=== wirelog Bitwise Eval Tests (Issue #72) ===\n\n");

    printf("--- band (bitwise AND) ---\n");
    test_eval_band_0xFF_and_0x0F();
    test_eval_band_0xAA_and_0xF0();
    test_eval_band_zero_mask();

    printf("\n--- bor (bitwise OR) ---\n");
    test_eval_bor_0xF0_or_0x0F();
    test_eval_bor_identity_zero();

    printf("\n--- bxor (bitwise XOR) ---\n");
    test_eval_bxor_0xFF_xor_0x0F();
    test_eval_bxor_self_is_zero();

    printf("\n--- bnot (bitwise NOT) ---\n");
    test_eval_bnot_zero();
    test_eval_bnot_involution();

    printf("\n--- bshl (left shift) ---\n");
    test_eval_bshl_1_by_4();
    test_eval_bshl_byte_to_high();

    printf("\n--- bshr (right shift) ---\n");
    test_eval_bshr_256_by_4();
    test_eval_bshr_nibble_extraction();

    printf("\n--- Protocol Analysis Patterns ---\n");
    test_eval_extract_nibble_pattern();
    test_eval_reconstruct_16bit_value();
    test_eval_check_error_flag();

    printf("\n=== Results: %d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf(" ===\n");

    return tests_failed > 0 ? 1 : 0;
}
