/*
 * test_cryptographic_hashes.c - Cryptographic Hash Function Tests (Issue #73)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Unit tests for md5(), sha1(), sha256(), sha512(), hmac_sha256() functions.
 *
 * These functions operate on int64_t inputs: the raw 8 bytes of the integer
 * are passed through the respective mbedTLS hash algorithm, and the digest
 * is then folded via XXH3_64bits() to produce a deterministic int64_t result.
 *
 * Actual behavior per function:
 *   md5(), sha1(), hmac_sha256(): parse OK, plan OK.
 *     - mbedTLS enabled:  evaluate correctly, produce deterministic int64_t.
 *     - mbedTLS disabled: evaluator hits error path (goto bad), snapshot
 *       still succeeds (rc=0) but tuple value is 0 (fallback).
 *   sha256(), sha512(): tokens exist in lexer but parser does not yet handle
 *     them; wirelog_parse_string() fails for programs using them (returns -1).
 *
 * Compilation guards:
 *   WL_MBEDTLS_ENABLED defined:   full determinism and integration tests.
 *   WL_MBEDTLS_ENABLED not defined: runtime behavior tests for each function.
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
/* Test Framework                                                           */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                        \
    do {                                  \
        tests_run++;                      \
        printf("  [TEST] %-60s", (name)); \
        fflush(stdout);                   \
    } while (0)

#define PASS()             \
    do {                   \
        tests_passed++;    \
        printf(" PASS\n"); \
    } while (0)

#define FAIL(msg)                     \
    do {                              \
        tests_failed++;               \
        printf(" FAIL: %s\n", (msg)); \
        return;                       \
    } while (0)

/* ======================================================================== */
/* Result Capture                                                           */
/* ======================================================================== */

struct result_ctx {
    int64_t col0[64];
    int64_t col1[64];
    uint32_t count;
    uint32_t ncols_seen;
    int snapshot_rc; /* return code from wl_session_snapshot */
};

static void
capture_cb(const char *relation, const int64_t *row, uint32_t ncols,
           void *user_data)
{
    struct result_ctx *ctx = (struct result_ctx *)user_data;
    (void)relation;
    if (ctx->count < 64) {
        ctx->col0[ctx->count] = row[0];
        if (ncols >= 2)
            ctx->col1[ctx->count] = row[1];
        ctx->ncols_seen = ncols;
        ctx->count++;
    }
}

/*
 * run_program_full:
 * Run a Datalog program and fill in ctx.  Returns:
 *   -1  if parse, plan, or session creation/load fails
 *    0  if everything succeeded (ctx->snapshot_rc holds the snapshot rc)
 */
static int
run_program_full(const char *src, struct result_ctx *ctx)
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

    ctx->count = 0;
    ctx->ncols_seen = 0;
    ctx->snapshot_rc = wl_session_snapshot(sess, capture_cb, ctx);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return 0;
}

/* ======================================================================== */
/* Shared test helpers used regardless of mbedTLS availability             */
/* ======================================================================== */

/*
 * sha256() and sha512() are fully implemented: lexer, parser, and backend.
 * These determinism tests are unconditional (run with or without mbedTLS).
 * Without mbedTLS, parse/plan succeed but snapshot emits tuple value 0 (fallback).
 */
static void
test_sha256_determinism(void)
{
    TEST("sha256(): two evaluations of sha256(0) produce the same result");

    const char *src = ".decl a(x: int64)\n"
                      "a(0).\n"
                      ".decl r(z: int64)\n"
                      "r(sha256(x)) :- a(x).\n";

    struct result_ctx ctx1, ctx2;
    int rc1 = run_program_full(src, &ctx1);
    int rc2 = run_program_full(src, &ctx2);
    if (rc1 != 0) {
        FAIL("first evaluation failed (parse/plan/session error)");
    }
    if (rc2 != 0) {
        FAIL("second evaluation failed (parse/plan/session error)");
    }
    if (ctx1.count != 1 || ctx2.count != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 tuple each run, got %u and %u",
                 ctx1.count, ctx2.count);
        FAIL(buf);
    }
    if (ctx1.col0[0] != ctx2.col0[0]) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "sha256(0) not deterministic: %" PRId64 " != %" PRId64,
                 ctx1.col0[0], ctx2.col0[0]);
        FAIL(buf);
    }
    PASS();
}

static void
test_sha512_determinism(void)
{
    TEST("sha512(): two evaluations of sha512(0) produce the same result");

    const char *src = ".decl a(x: int64)\n"
                      "a(0).\n"
                      ".decl r(z: int64)\n"
                      "r(sha512(x)) :- a(x).\n";

    struct result_ctx ctx1, ctx2;
    int rc1 = run_program_full(src, &ctx1);
    int rc2 = run_program_full(src, &ctx2);
    if (rc1 != 0) {
        FAIL("first evaluation failed (parse/plan/session error)");
    }
    if (rc2 != 0) {
        FAIL("second evaluation failed (parse/plan/session error)");
    }
    if (ctx1.count != 1 || ctx2.count != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 tuple each run, got %u and %u",
                 ctx1.count, ctx2.count);
        FAIL(buf);
    }
    if (ctx1.col0[0] != ctx2.col0[0]) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "sha512(0) not deterministic: %" PRId64 " != %" PRId64,
                 ctx1.col0[0], ctx2.col0[0]);
        FAIL(buf);
    }
    PASS();
}

#ifdef WL_MBEDTLS_ENABLED

/* ======================================================================== */
/* mbedTLS-enabled: Determinism tests                                      */
/*                                                                          */
/* Since expected output values depend on the mbedTLS+XXH3 pipeline, we    */
/* verify determinism by running the same program twice and comparing,     */
/* and verify distinctness by running with multiple distinct inputs.        */
/* ======================================================================== */

static void
test_md5_determinism_zero(void)
{
    TEST("md5(0): two evaluations produce the same result");

    const char *src = ".decl a(x: int64)\n"
                      "a(0).\n"
                      ".decl r(z: int64)\n"
                      "r(md5(x)) :- a(x).\n";

    struct result_ctx ctx1, ctx2;
    int rc1 = run_program_full(src, &ctx1);
    int rc2 = run_program_full(src, &ctx2);
    if (rc1 != 0 || ctx1.snapshot_rc != 0) {
        FAIL("first evaluation failed");
    }
    if (rc2 != 0 || ctx2.snapshot_rc != 0) {
        FAIL("second evaluation failed");
    }
    if (ctx1.count != 1 || ctx2.count != 1) {
        FAIL("expected 1 tuple each run");
    }
    if (ctx1.col0[0] != ctx2.col0[0]) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "md5(0) not deterministic: %" PRId64 " != %" PRId64,
                 ctx1.col0[0], ctx2.col0[0]);
        FAIL(buf);
    }
    PASS();
}

static void
test_md5_determinism_one(void)
{
    TEST("md5(1): two evaluations produce the same result");

    const char *src = ".decl a(x: int64)\n"
                      "a(1).\n"
                      ".decl r(z: int64)\n"
                      "r(md5(x)) :- a(x).\n";

    struct result_ctx ctx1, ctx2;
    if (run_program_full(src, &ctx1) != 0 || ctx1.snapshot_rc != 0) {
        FAIL("first evaluation failed");
    }
    if (run_program_full(src, &ctx2) != 0 || ctx2.snapshot_rc != 0) {
        FAIL("second evaluation failed");
    }
    if (ctx1.count != 1 || ctx2.count != 1) {
        FAIL("expected 1 tuple each run");
    }
    if (ctx1.col0[0] != ctx2.col0[0]) {
        FAIL("md5(1) not deterministic across runs");
    }
    PASS();
}

static void
test_md5_determinism_fortytwo(void)
{
    TEST("md5(42): two evaluations produce the same result");

    const char *src = ".decl a(x: int64)\n"
                      "a(42).\n"
                      ".decl r(z: int64)\n"
                      "r(md5(x)) :- a(x).\n";

    struct result_ctx ctx1, ctx2;
    if (run_program_full(src, &ctx1) != 0 || ctx1.snapshot_rc != 0) {
        FAIL("first evaluation failed");
    }
    if (run_program_full(src, &ctx2) != 0 || ctx2.snapshot_rc != 0) {
        FAIL("second evaluation failed");
    }
    if (ctx1.count != 1 || ctx2.count != 1) {
        FAIL("expected 1 tuple each run");
    }
    if (ctx1.col0[0] != ctx2.col0[0]) {
        FAIL("md5(42) not deterministic across runs");
    }
    PASS();
}

static void
test_md5_distinct_inputs_distinct_outputs(void)
{
    TEST("md5: distinct inputs (0, 1, 42) produce distinct outputs");

    const char *src = ".decl a(x: int64)\n"
                      "a(0). a(1). a(42).\n"
                      ".decl r(z: int64)\n"
                      "r(md5(x)) :- a(x).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 tuples, got %u", ctx.count);
        FAIL(buf);
    }
    if (ctx.col0[0] == ctx.col0[1] || ctx.col0[0] == ctx.col0[2]
        || ctx.col0[1] == ctx.col0[2]) {
        FAIL("md5 collision: distinct inputs produced same output");
    }
    PASS();
}

static void
test_md5_idempotent(void)
{
    TEST("md5: md5(x) = md5(x) filter passes for same variable");

    const char *src = ".decl a(x: int64)\n"
                      "a(0).\n"
                      ".decl r(ok: int64)\n"
                      "r(1) :- a(x), md5(x) = md5(x).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 1 || ctx.col0[0] != 1) {
        FAIL("md5(x) = md5(x) filter should always pass");
    }
    PASS();
}

static void
test_sha1_determinism_zero(void)
{
    TEST("sha1(0): two evaluations produce the same result");

    const char *src = ".decl a(x: int64)\n"
                      "a(0).\n"
                      ".decl r(z: int64)\n"
                      "r(sha1(x)) :- a(x).\n";

    struct result_ctx ctx1, ctx2;
    if (run_program_full(src, &ctx1) != 0 || ctx1.snapshot_rc != 0) {
        FAIL("first evaluation failed");
    }
    if (run_program_full(src, &ctx2) != 0 || ctx2.snapshot_rc != 0) {
        FAIL("second evaluation failed");
    }
    if (ctx1.count != 1 || ctx2.count != 1) {
        FAIL("expected 1 tuple each run");
    }
    if (ctx1.col0[0] != ctx2.col0[0]) {
        FAIL("sha1(0) not deterministic across runs");
    }
    PASS();
}

static void
test_sha1_determinism_one(void)
{
    TEST("sha1(1): two evaluations produce the same result");

    const char *src = ".decl a(x: int64)\n"
                      "a(1).\n"
                      ".decl r(z: int64)\n"
                      "r(sha1(x)) :- a(x).\n";

    struct result_ctx ctx1, ctx2;
    if (run_program_full(src, &ctx1) != 0 || ctx1.snapshot_rc != 0) {
        FAIL("first evaluation failed");
    }
    if (run_program_full(src, &ctx2) != 0 || ctx2.snapshot_rc != 0) {
        FAIL("second evaluation failed");
    }
    if (ctx1.count != 1 || ctx2.count != 1) {
        FAIL("expected 1 tuple each run");
    }
    if (ctx1.col0[0] != ctx2.col0[0]) {
        FAIL("sha1(1) not deterministic across runs");
    }
    PASS();
}

static void
test_sha1_determinism_fortytwo(void)
{
    TEST("sha1(42): two evaluations produce the same result");

    const char *src = ".decl a(x: int64)\n"
                      "a(42).\n"
                      ".decl r(z: int64)\n"
                      "r(sha1(x)) :- a(x).\n";

    struct result_ctx ctx1, ctx2;
    if (run_program_full(src, &ctx1) != 0 || ctx1.snapshot_rc != 0) {
        FAIL("first evaluation failed");
    }
    if (run_program_full(src, &ctx2) != 0 || ctx2.snapshot_rc != 0) {
        FAIL("second evaluation failed");
    }
    if (ctx1.count != 1 || ctx2.count != 1) {
        FAIL("expected 1 tuple each run");
    }
    if (ctx1.col0[0] != ctx2.col0[0]) {
        FAIL("sha1(42) not deterministic across runs");
    }
    PASS();
}

static void
test_sha1_distinct_inputs_distinct_outputs(void)
{
    TEST("sha1: distinct inputs (0, 1, 42) produce distinct outputs");

    const char *src = ".decl a(x: int64)\n"
                      "a(0). a(1). a(42).\n"
                      ".decl r(z: int64)\n"
                      "r(sha1(x)) :- a(x).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 tuples, got %u", ctx.count);
        FAIL(buf);
    }
    if (ctx.col0[0] == ctx.col0[1] || ctx.col0[0] == ctx.col0[2]
        || ctx.col0[1] == ctx.col0[2]) {
        FAIL("sha1 collision: distinct inputs produced same output");
    }
    PASS();
}

static void
test_sha1_idempotent(void)
{
    TEST("sha1: sha1(x) = sha1(x) filter passes for same variable");

    const char *src = ".decl a(x: int64)\n"
                      "a(1).\n"
                      ".decl r(ok: int64)\n"
                      "r(1) :- a(x), sha1(x) = sha1(x).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 1 || ctx.col0[0] != 1) {
        FAIL("sha1(x) = sha1(x) filter should always pass");
    }
    PASS();
}

/*
 * hmac_sha256() determinism tests.
 * Binary function: hmac_sha256(msg, key).
 */
static void
test_hmac_sha256_determinism_msg0_key1(void)
{
    TEST("hmac_sha256(0, 1): two evaluations produce the same result");

    const char *src = ".decl a(msg: int64, key: int64)\n"
                      "a(0, 1).\n"
                      ".decl r(z: int64)\n"
                      "r(hmac_sha256(msg, key)) :- a(msg, key).\n";

    struct result_ctx ctx1, ctx2;
    if (run_program_full(src, &ctx1) != 0 || ctx1.snapshot_rc != 0) {
        FAIL("first evaluation failed");
    }
    if (run_program_full(src, &ctx2) != 0 || ctx2.snapshot_rc != 0) {
        FAIL("second evaluation failed");
    }
    if (ctx1.count != 1 || ctx2.count != 1) {
        FAIL("expected 1 tuple each run");
    }
    if (ctx1.col0[0] != ctx2.col0[0]) {
        FAIL("hmac_sha256(0,1) not deterministic across runs");
    }
    PASS();
}

static void
test_hmac_sha256_determinism_msg1_key42(void)
{
    TEST("hmac_sha256(1, 42): two evaluations produce the same result");

    const char *src = ".decl a(msg: int64, key: int64)\n"
                      "a(1, 42).\n"
                      ".decl r(z: int64)\n"
                      "r(hmac_sha256(msg, key)) :- a(msg, key).\n";

    struct result_ctx ctx1, ctx2;
    if (run_program_full(src, &ctx1) != 0 || ctx1.snapshot_rc != 0) {
        FAIL("first evaluation failed");
    }
    if (run_program_full(src, &ctx2) != 0 || ctx2.snapshot_rc != 0) {
        FAIL("second evaluation failed");
    }
    if (ctx1.count != 1 || ctx2.count != 1) {
        FAIL("expected 1 tuple each run");
    }
    if (ctx1.col0[0] != ctx2.col0[0]) {
        FAIL("hmac_sha256(1,42) not deterministic across runs");
    }
    PASS();
}

static void
test_hmac_sha256_determinism_msg42_key0(void)
{
    TEST("hmac_sha256(42, 0): two evaluations produce the same result");

    const char *src = ".decl a(msg: int64, key: int64)\n"
                      "a(42, 0).\n"
                      ".decl r(z: int64)\n"
                      "r(hmac_sha256(msg, key)) :- a(msg, key).\n";

    struct result_ctx ctx1, ctx2;
    if (run_program_full(src, &ctx1) != 0 || ctx1.snapshot_rc != 0) {
        FAIL("first evaluation failed");
    }
    if (run_program_full(src, &ctx2) != 0 || ctx2.snapshot_rc != 0) {
        FAIL("second evaluation failed");
    }
    if (ctx1.count != 1 || ctx2.count != 1) {
        FAIL("expected 1 tuple each run");
    }
    if (ctx1.col0[0] != ctx2.col0[0]) {
        FAIL("hmac_sha256(42,0) not deterministic across runs");
    }
    PASS();
}

static void
test_hmac_sha256_key_sensitivity(void)
{
    TEST("hmac_sha256: different keys produce different outputs for same msg");

    const char *src = ".decl a(msg: int64, key: int64)\n"
                      "a(0, 1). a(0, 2).\n"
                      ".decl r(z: int64)\n"
                      "r(hmac_sha256(msg, key)) :- a(msg, key).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 2 tuples, got %u", ctx.count);
        FAIL(buf);
    }
    if (ctx.col0[0] == ctx.col0[1]) {
        FAIL("hmac_sha256 key insensitive: same msg + different keys gave same "
             "output");
    }
    PASS();
}

static void
test_hmac_sha256_msg_sensitivity(void)
{
    TEST("hmac_sha256: different msgs produce different outputs for same key");

    const char *src = ".decl a(msg: int64, key: int64)\n"
                      "a(0, 99). a(1, 99).\n"
                      ".decl r(z: int64)\n"
                      "r(hmac_sha256(msg, key)) :- a(msg, key).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 2 tuples, got %u", ctx.count);
        FAIL(buf);
    }
    if (ctx.col0[0] == ctx.col0[1]) {
        FAIL("hmac_sha256 msg insensitive: different msgs + same key gave same "
             "output");
    }
    PASS();
}

static void
test_hmac_sha256_asymmetric(void)
{
    TEST("hmac_sha256(msg, key) != hmac_sha256(key, msg) in general");

    const char *src = ".decl a(msg: int64, key: int64)\n"
                      "a(0, 42). a(42, 0).\n"
                      ".decl r(z: int64)\n"
                      "r(hmac_sha256(msg, key)) :- a(msg, key).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 2 tuples, got %u", ctx.count);
        FAIL(buf);
    }
    if (ctx.col0[0] == ctx.col0[1]) {
        FAIL("hmac_sha256(0,42) == hmac_sha256(42,0): unexpectedly symmetric");
    }
    PASS();
}

/* ======================================================================== */
/* Edge-case tests (mbedTLS enabled)                                       */
/* ======================================================================== */

static void
test_md5_edge_zero_input(void)
{
    TEST("md5(0): zero integer input produces a non-zero hash value");

    const char *src = ".decl a(x: int64)\n"
                      "a(0).\n"
                      ".decl r(z: int64)\n"
                      "r(md5(x)) :- a(x).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 1) {
        FAIL("expected 1 tuple");
    }
    /* The md5 of the zero int64_t bytes folded via XXH3 should be non-zero */
    if (ctx.col0[0] == 0) {
        FAIL("md5(0) produced 0: likely evaluator error path (mbedTLS issue)");
    }
    PASS();
}

static void
test_sha1_edge_negative_input(void)
{
    TEST("sha1(-1): negative integer input produces a non-zero hash value");

    const char *src = ".decl a(x: int64)\n"
                      "a(-1).\n"
                      ".decl r(z: int64)\n"
                      "r(sha1(x)) :- a(x).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 1) {
        FAIL("expected 1 tuple");
    }
    if (ctx.col0[0] == 0) {
        FAIL("sha1(-1) produced 0: likely evaluator error path");
    }
    PASS();
}

static void
test_md5_edge_max_int64(void)
{
    TEST("md5(9223372036854775807): max int64 input hashes without error");

    const char *src = ".decl a(x: int64)\n"
                      "a(9223372036854775807).\n"
                      ".decl r(z: int64)\n"
                      "r(md5(x)) :- a(x).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 1) {
        FAIL("expected 1 tuple");
    }
    PASS();
}

static void
test_sha1_edge_min_int64(void)
{
    TEST("sha1(-9223372036854775808): min int64 input hashes without error");

    const char *src = ".decl a(x: int64)\n"
                      "a(-9223372036854775808).\n"
                      ".decl r(z: int64)\n"
                      "r(sha1(x)) :- a(x).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 1) {
        FAIL("expected 1 tuple");
    }
    PASS();
}

static void
test_hmac_sha256_edge_zero_key(void)
{
    TEST("hmac_sha256(42, 0): zero key produces a non-zero hash value");

    const char *src = ".decl a(msg: int64, key: int64)\n"
                      "a(42, 0).\n"
                      ".decl r(z: int64)\n"
                      "r(hmac_sha256(msg, key)) :- a(msg, key).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 1) {
        FAIL("expected 1 tuple");
    }
    if (ctx.col0[0] == 0) {
        FAIL("hmac_sha256(42,0) produced 0: likely evaluator error path");
    }
    PASS();
}

/* ======================================================================== */
/* Integration tests: cryptographic hashes in Datalog rules                */
/* ======================================================================== */

static void
test_integration_md5_datalog_rule(void)
{
    TEST("Integration: md5(id) in Datalog rule produces 5 distinct "
         "fingerprints");

    const char *src = ".decl item(id: int64)\n"
                      "item(0). item(1). item(2). item(3). item(4).\n"
                      ".decl fingerprint(fp: int64)\n"
                      "fingerprint(md5(id)) :- item(id).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 5) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 5 distinct tuples, got %u",
                 ctx.count);
        FAIL(buf);
    }
    for (uint32_t i = 0; i < ctx.count; i++) {
        for (uint32_t j = i + 1; j < ctx.count; j++) {
            if (ctx.col0[i] == ctx.col0[j]) {
                FAIL("md5 collision in Datalog rule output");
            }
        }
    }
    PASS();
}

static void
test_integration_sha1_datalog_filter(void)
{
    TEST("Integration: sha1(x) = sha1(y) filter passes only when x == y");

    const char *src = ".decl pair(x: int64, y: int64)\n"
                      "pair(1, 1). pair(1, 2).\n"
                      ".decl match(x: int64)\n"
                      "match(x) :- pair(x, y), sha1(x) = sha1(y).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf),
                 "expected 1 match tuple (x=y case only), got %u", ctx.count);
        FAIL(buf);
    }
    if (ctx.col0[0] != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected x=1 in result, got %" PRId64,
                 ctx.col0[0]);
        FAIL(buf);
    }
    PASS();
}

static void
test_integration_hmac_sha256_datalog_rule(void)
{
    TEST("Integration: hmac_sha256(msg, key) produces 3 distinct auth tokens");

    const char *src = ".decl auth(msg: int64, key: int64)\n"
                      "auth(0, 100). auth(1, 100). auth(2, 100).\n"
                      ".decl token(t: int64)\n"
                      "token(hmac_sha256(msg, key)) :- auth(msg, key).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 distinct HMAC tokens, got %u",
                 ctx.count);
        FAIL(buf);
    }
    for (uint32_t i = 0; i < ctx.count; i++) {
        for (uint32_t j = i + 1; j < ctx.count; j++) {
            if (ctx.col0[i] == ctx.col0[j]) {
                FAIL("hmac_sha256 collision in Datalog rule output");
            }
        }
    }
    PASS();
}

#else /* !WL_MBEDTLS_ENABLED */

/* ======================================================================== */
/* mbedTLS-disabled: runtime behavior tests                                */
/*                                                                          */
/* md5(), sha1(), hmac_sha256() parse and plan successfully but hit the     */
/* evaluator error path (goto bad) at runtime.  The snapshot still returns  */
/* 0 (success) and emits 1 tuple, but the tuple value is 0 (fallback).     */
/* ======================================================================== */

static void
test_md5_evaluates_with_zero_fallback_no_mbedtls(void)
{
    TEST("md5(): without mbedTLS snapshot succeeds, tuple value is 0 "
         "(fallback)");

    const char *src = ".decl a(x: int64)\n"
                      "a(0).\n"
                      ".decl r(z: int64)\n"
                      "r(md5(x)) :- a(x).\n";

    struct result_ctx ctx;
    int rc = run_program_full(src, &ctx);
    if (rc != 0) {
        FAIL("expected parse/plan/session to succeed even without mbedTLS");
    }
    /* snapshot succeeds (rc=0) with 1 tuple; tuple value is 0 (fallback) */
    if (ctx.snapshot_rc != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "snapshot_rc=%d, expected 0",
                 ctx.snapshot_rc);
        FAIL(buf);
    }
    if (ctx.count != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 tuple, got %u", ctx.count);
        FAIL(buf);
    }
    if (ctx.col0[0] != 0) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "expected fallback value 0 without mbedTLS, got %" PRId64,
                 ctx.col0[0]);
        FAIL(buf);
    }
    PASS();
}

static void
test_sha1_evaluates_with_zero_fallback_no_mbedtls(void)
{
    TEST("sha1(): without mbedTLS snapshot succeeds, tuple value is 0 "
         "(fallback)");

    const char *src = ".decl a(x: int64)\n"
                      "a(0).\n"
                      ".decl r(z: int64)\n"
                      "r(sha1(x)) :- a(x).\n";

    struct result_ctx ctx;
    int rc = run_program_full(src, &ctx);
    if (rc != 0) {
        FAIL("expected parse/plan/session to succeed even without mbedTLS");
    }
    if (ctx.snapshot_rc != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "snapshot_rc=%d, expected 0",
                 ctx.snapshot_rc);
        FAIL(buf);
    }
    if (ctx.count != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 tuple, got %u", ctx.count);
        FAIL(buf);
    }
    if (ctx.col0[0] != 0) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "expected fallback value 0 without mbedTLS, got %" PRId64,
                 ctx.col0[0]);
        FAIL(buf);
    }
    PASS();
}

static void
test_hmac_sha256_evaluates_with_zero_fallback_no_mbedtls(void)
{
    TEST("hmac_sha256(): without mbedTLS snapshot succeeds, value is 0 "
         "(fallback)");

    const char *src = ".decl a(msg: int64, key: int64)\n"
                      "a(0, 1).\n"
                      ".decl r(z: int64)\n"
                      "r(hmac_sha256(msg, key)) :- a(msg, key).\n";

    struct result_ctx ctx;
    int rc = run_program_full(src, &ctx);
    if (rc != 0) {
        FAIL("expected parse/plan/session to succeed even without mbedTLS");
    }
    if (ctx.snapshot_rc != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "snapshot_rc=%d, expected 0",
                 ctx.snapshot_rc);
        FAIL(buf);
    }
    if (ctx.count != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 tuple, got %u", ctx.count);
        FAIL(buf);
    }
    if (ctx.col0[0] != 0) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "expected fallback value 0 without mbedTLS, got %" PRId64,
                 ctx.col0[0]);
        FAIL(buf);
    }
    PASS();
}

#endif /* WL_MBEDTLS_ENABLED */

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
#ifdef WL_MBEDTLS_ENABLED
    printf("=== wirelog Cryptographic Hash Tests (Issue #73) [mbedTLS enabled] "
           "===\n\n");

    printf("--- md5() Determinism Tests ---\n");
    test_md5_determinism_zero();
    test_md5_determinism_one();
    test_md5_determinism_fortytwo();
    test_md5_distinct_inputs_distinct_outputs();
    test_md5_idempotent();

    printf("\n--- sha1() Determinism Tests ---\n");
    test_sha1_determinism_zero();
    test_sha1_determinism_one();
    test_sha1_determinism_fortytwo();
    test_sha1_distinct_inputs_distinct_outputs();
    test_sha1_idempotent();

    printf("\n--- hmac_sha256() Determinism Tests ---\n");
    test_hmac_sha256_determinism_msg0_key1();
    test_hmac_sha256_determinism_msg1_key42();
    test_hmac_sha256_determinism_msg42_key0();
    test_hmac_sha256_key_sensitivity();
    test_hmac_sha256_msg_sensitivity();
    test_hmac_sha256_asymmetric();

    printf("\n--- Edge Case Tests ---\n");
    test_md5_edge_zero_input();
    test_sha1_edge_negative_input();
    test_md5_edge_max_int64();
    test_sha1_edge_min_int64();
    test_hmac_sha256_edge_zero_key();

    printf("\n--- Integration Tests ---\n");
    test_integration_md5_datalog_rule();
    test_integration_sha1_datalog_filter();
    test_integration_hmac_sha256_datalog_rule();

#else
    printf("=== wirelog Cryptographic Hash Tests (Issue #73) [mbedTLS "
           "DISABLED] ===\n\n");

    printf("--- Runtime Behavior Tests (mbedTLS unavailable) ---\n");
    test_md5_evaluates_with_zero_fallback_no_mbedtls();
    test_sha1_evaluates_with_zero_fallback_no_mbedtls();
    test_hmac_sha256_evaluates_with_zero_fallback_no_mbedtls();
#endif

    printf("\n--- sha256()/sha512() Determinism Tests (always run) ---\n");
    test_sha256_determinism();
    test_sha512_determinism();

    printf("\n=== Results: %d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf(" ===\n");

    return tests_failed > 0 ? 1 : 0;
}
