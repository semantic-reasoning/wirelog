/*
 * test_cryptographic_hashes.c - Cryptographic Hash Function Tests (Issue #73)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Unit tests for md5(), sha1(), sha256(), sha512(), hmac_sha256(), uuid4(),
 * and uuid5() functions.
 *
 * These functions operate on int64_t inputs: the raw 8 bytes of the integer
 * are passed through the respective mbedTLS hash algorithm, and the digest
 * is then folded via XXH3_64bits() to produce a deterministic int64_t result.
 *
 * Actual behavior per function:
 *   md5(), sha1(), sha256(), sha512(), hmac_sha256(), uuid4(), uuid5():
 *     parse OK, plan OK.
 *     - mbedTLS enabled:  evaluate correctly and produce int64_t results.
 *     - mbedTLS disabled: evaluator hits error path (goto bad), snapshot
 *       fails closed and no tuple is emitted.
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
 * Without mbedTLS, parse/plan succeed but snapshot fails closed.
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
#ifndef WL_MBEDTLS_ENABLED
    if (ctx1.snapshot_rc == 0 || ctx2.snapshot_rc == 0) {
        FAIL("expected fail-closed snapshot errors without mbedTLS");
    }
    if (ctx1.count != 0 || ctx2.count != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0 tuples each run, got %u and %u",
            ctx1.count, ctx2.count);
        FAIL(buf);
    }
    PASS();
    return;
#endif
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
#ifndef WL_MBEDTLS_ENABLED
    if (ctx1.snapshot_rc == 0 || ctx2.snapshot_rc == 0) {
        FAIL("expected fail-closed snapshot errors without mbedTLS");
    }
    if (ctx1.count != 0 || ctx2.count != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0 tuples each run, got %u and %u",
            ctx1.count, ctx2.count);
        FAIL(buf);
    }
    PASS();
    return;
#endif
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

static unsigned
uuid_returned_prefix_version(int64_t value)
{
    unsigned char bytes[8];
    memcpy(bytes, &value, sizeof(bytes));
    return (unsigned)(bytes[6] >> 4);
}

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

static void
test_sha256_enabled_nonzero_distinct(void)
{
    TEST("sha256: enabled path returns non-zero distinct values");

    const char *src = ".decl a(x: int64)\n"
        "a(0). a(1). a(42).\n"
        ".decl r(z: int64)\n"
        "r(sha256(x)) :- a(x).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 tuples, got %u", ctx.count);
        FAIL(buf);
    }
    for (uint32_t i = 0; i < ctx.count; i++) {
        if (ctx.col0[i] == 0) {
            FAIL("sha256 produced 0: likely evaluator error path");
        }
        for (uint32_t j = i + 1; j < ctx.count; j++) {
            if (ctx.col0[i] == ctx.col0[j]) {
                FAIL("sha256 collision: distinct inputs produced same output");
            }
        }
    }
    PASS();
}

static void
test_sha512_enabled_nonzero_distinct(void)
{
    TEST("sha512: enabled path returns non-zero distinct values");

    const char *src = ".decl a(x: int64)\n"
        "a(0). a(1). a(42).\n"
        ".decl r(z: int64)\n"
        "r(sha512(x)) :- a(x).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 tuples, got %u", ctx.count);
        FAIL(buf);
    }
    for (uint32_t i = 0; i < ctx.count; i++) {
        if (ctx.col0[i] == 0) {
            FAIL("sha512 produced 0: likely evaluator error path");
        }
        for (uint32_t j = i + 1; j < ctx.count; j++) {
            if (ctx.col0[i] == ctx.col0[j]) {
                FAIL("sha512 collision: distinct inputs produced same output");
            }
        }
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

static void
test_uuid4_enabled_random_version_prefix(void)
{
    TEST("uuid4(): enabled path returns non-zero v4 UUID prefixes");

    const char *src = ".decl a(x: int64)\n"
        "a(0).\n"
        ".decl r(z: int64)\n"
        "r(uuid4()) :- a(x).\n";

    int64_t values[4];
    for (uint32_t i = 0; i < 4; i++) {
        struct result_ctx ctx;
        if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
            FAIL("evaluation failed");
        }
        if (ctx.count != 1) {
            FAIL("expected 1 tuple");
        }
        if (ctx.col0[0] == 0) {
            FAIL("uuid4 produced 0: likely evaluator error path");
        }
        if (uuid_returned_prefix_version(ctx.col0[0]) != 4) {
            FAIL("uuid4 returned prefix does not carry version nibble 4");
        }
        values[i] = ctx.col0[0];
    }

    if (values[0] == values[1] && values[0] == values[2]
        && values[0] == values[3]) {
        FAIL("uuid4 produced the same prefix across four evaluations");
    }
    PASS();
}

static void
test_uuid5_enabled_deterministic_version_prefix(void)
{
    TEST("uuid5(ns, name): same inputs are deterministic v5 prefixes");

    const char *src = ".decl a(ns: int64, name: int64)\n"
        "a(1234, 5678).\n"
        ".decl r(z: int64)\n"
        "r(uuid5(ns, name)) :- a(ns, name).\n";

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
    if (ctx1.col0[0] == 0) {
        FAIL("uuid5 produced 0: likely evaluator error path");
    }
    if (ctx1.col0[0] != ctx2.col0[0]) {
        FAIL("uuid5 was not deterministic for identical namespace/name");
    }
    if (uuid_returned_prefix_version(ctx1.col0[0]) != 5) {
        FAIL("uuid5 returned prefix does not carry version nibble 5");
    }
    PASS();
}

static void
test_uuid5_enabled_namespace_name_sensitivity(void)
{
    TEST("uuid5(ns, name): namespace and name change output");

    const char *src = ".decl a(ns: int64, name: int64)\n"
        "a(1234, 5678). a(1234, 5679). a(1235, 5678).\n"
        ".decl r(z: int64)\n"
        "r(uuid5(ns, name)) :- a(ns, name).\n";

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 tuples, got %u", ctx.count);
        FAIL(buf);
    }
    for (uint32_t i = 0; i < ctx.count; i++) {
        if (ctx.col0[i] == 0) {
            FAIL("uuid5 produced 0: likely evaluator error path");
        }
        if (uuid_returned_prefix_version(ctx.col0[i]) != 5) {
            FAIL("uuid5 returned prefix does not carry version nibble 5");
        }
        for (uint32_t j = i + 1; j < ctx.count; j++) {
            if (ctx.col0[i] == ctx.col0[j]) {
                FAIL("uuid5 collision across changed namespace/name inputs");
            }
        }
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

/* TODO: Fix sha1() negative input handling; function disabled for now */
__attribute__((unused)) static void
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

/* TODO: Fix sha1() negative input handling; function disabled for now */
__attribute__((unused)) static void
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

/* ======================================================================== */
/* Symbol operands (Issue #963)                                             */
/*                                                                          */
/* The digests above take the 8-byte int64 representation of their operand. */
/* When the operand is a symbol that int64 is an *intern id*, so the digest */
/* answered a question about the intern table, not about the string: the    */
/* same string digested differently depending on what had been interned     */
/* first.  Symbol operands now digest the string's own bytes -- strlen      */
/* many, no NUL -- which is what an external tool sees.                     */
/*                                                                          */
/* Every expected value below was derived outside wirelog, e.g.             */
/*                                                                          */
/*     printf 'aa' | sha256sum | cut -d' ' -f1 | xxd -r -p | xxhsum -H3     */
/*                                                                          */
/* which is also the two-step recipe docs/SYNTAX.md documents.  Note what   */
/* it says about these built-ins: sha256("aa") is not SHA-256 of "aa", it   */
/* is XXH3_64bits of the SHA-256 digest of "aa".                            */
/* ======================================================================== */

#define XXH3_OF_MD5_AA     8108801538665929855LL
#define XXH3_OF_SHA1_AA    4228114194375067801LL
#define XXH3_OF_SHA256_AA  (-8977746440570084450LL)
#define XXH3_OF_SHA512_AA  (-3394976671127927984LL)
/* HMAC-SHA-256, msg first and key second, in the four operand typings. */
#define XXH3_OF_HMAC_SS    1125073340110602348LL  /* msg "aa", key "zz"   */
#define XXH3_OF_HMAC_SI    (-1543635408773734377LL) /* msg "aa", key 1    */
#define XXH3_OF_HMAC_IS    (-5755376871629748970LL) /* msg 1,    key "zz" */
#define XXH3_OF_HMAC_II    5350477797202862412LL  /* msg 1,    key 1      */
#define XXH3_OF_HMAC_EMPTY_KEY 5217900878614730541LL /* msg "msg", key "" */
/*
 * uuid5 over symbols frames each operand as
 *
 *     tag || u64le(len(operand)) || operand
 *
 * where tag is 'S' for a symbol's bytes and 'I' for an int64's eight
 * little-endian bytes, and returns the first 8 bytes of the version-tagged
 * SHA-1 of the two frames, as int64.  In Python:
 *
 *   frame = lambda t, b: t + pack('<Q', len(b)) + b
 *   buf = frame(b'S', ns) + frame(b'I', pack('<q', name))   # the _SI case
 *   d = bytearray(sha1(buf).digest())
 *   d[6] = (d[6] & 0x0F) | 0x50; d[8] = (d[8] & 0x3F) | 0x80
 *   unpack('<q', bytes(d[:8]))[0]
 *
 * Only the three symbol-bearing opcodes frame.  The int64-only opcode
 * (0x2A) still hashes a bare u64le(ns) || u64le(name), so uuid5(int, int)
 * keeps every value it has ever returned.
 */
#define UUID5_SS_AA_ZZ     (-551394562050983893LL)
#define UUID5_SI_AA_1      (-7614036125174443443LL)
#define UUID5_IS_1_ZZ      (-5165379894741549620LL)
#define UUID5_AB_C         (-3651962211908811388LL)
#define UUID5_A_BC         (-1560628122420165596LL)
/*
 * The two operand pairs that the domain tag exists to separate.
 *
 *   UUID5_SS_EMPTY  uuid5("", "")   -- two empty symbols
 *   UUID5_II_ZERO   uuid5(0, 0)     -- the 0x2A opcode, value unchanged
 *
 * Without the tag both hash sixteen zero bytes: an empty framed pair was
 * u64le(0) || "" || u64le(0) || "", which is exactly what two zero int64s
 * concatenate to.
 *
 *   UUID5_SS_8BYTE  uuid5("abcdefgh", "x")
 *   UUID5_IS_8BYTE  uuid5(7523094288207667809, "x")
 *
 * 7523094288207667809 is unpack('<q', b"abcdefgh"), so without the tag the
 * two hash identical bytes however they are length-prefixed.
 */
#define UUID5_SS_EMPTY     (-5593758115287104118LL)
#define UUID5_II_ZERO      6655197997870229985LL
#define UUID5_SS_8BYTE     1250797450875576375LL
#define UUID5_IS_8BYTE     4854796543189122013LL

/* Run @src and require exactly one row whose first column is @want. */
static void
expect_single_value(const char *name, const char *src, int64_t want)
{
    TEST(name);

    struct result_ctx ctx;
    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 row, got %u", ctx.count);
        FAIL(buf);
    }
    if (ctx.col0[0] != want) {
        char buf[128];
        snprintf(buf, sizeof(buf),
            "expected %" PRId64 ", got %" PRId64, want, ctx.col0[0]);
        FAIL(buf);
    }
    PASS();
}

/*
 * Run @src and hand back the single value it produced.  Returns 0 on
 * success.  Unlike expect_single_value() this asserts nothing about the
 * value, so callers can compare two evaluator results against each other
 * rather than comparing two compile-time constants -- which is what makes
 * an inequality test able to fail when the evaluator changes.
 */
static int
eval_single_value(const char *src, int64_t *out)
{
    struct result_ctx ctx;

    if (run_program_full(src, &ctx) != 0 || ctx.snapshot_rc != 0)
        return -1;
    if (ctx.count != 1)
        return -1;
    *out = ctx.col0[0];
    return 0;
}

#define SYM_FACT ".decl s(v: symbol)\ns(\"aa\").\n"

static void
test_digests_of_symbol_operands(void)
{
    expect_single_value("md5(sym) digests the string's bytes",
        SYM_FACT ".decl r(z: int64)\nr(md5(v)) :- s(v).\n",
        XXH3_OF_MD5_AA);
    expect_single_value("sha1(sym) digests the string's bytes",
        SYM_FACT ".decl r(z: int64)\nr(sha1(v)) :- s(v).\n",
        XXH3_OF_SHA1_AA);
    expect_single_value("sha256(sym) digests the string's bytes",
        SYM_FACT ".decl r(z: int64)\nr(sha256(v)) :- s(v).\n",
        XXH3_OF_SHA256_AA);
    expect_single_value("sha512(sym) digests the string's bytes",
        SYM_FACT ".decl r(z: int64)\nr(sha512(v)) :- s(v).\n",
        XXH3_OF_SHA512_AA);
}

/*
 * hmac_sha256() and uuid5() take two operands that type independently, so
 * each has three symbol-bearing opcodes rather than one.  #962's rule for
 * comparisons -- a one-sided type match keeps the integer opcode -- is
 * deliberately not followed here: a comparison needs both ids reversed to
 * mean anything, while each digest operand contributes its own bytes, so
 * applying that rule would leave the mixed cases digesting an id.
 */
static void
test_two_operand_digests_type_each_operand(void)
{
    expect_single_value("hmac_sha256(sym, sym) keys and messages with bytes",
        SYM_FACT ".decl k(v: symbol)\nk(\"zz\").\n"
        ".decl r(z: int64)\n"
        "r(hmac_sha256(m, key)) :- s(m), k(key).\n",
        XXH3_OF_HMAC_SS);
    expect_single_value("hmac_sha256(sym, int) uses the int64 key bytes",
        SYM_FACT ".decl r(z: int64)\n"
        "r(hmac_sha256(v, 1)) :- s(v).\n",
        XXH3_OF_HMAC_SI);
    expect_single_value("hmac_sha256(int, sym) uses the int64 message bytes",
        ".decl k(v: symbol)\nk(\"zz\").\n"
        ".decl r(z: int64)\n"
        "r(hmac_sha256(1, key)) :- k(key).\n",
        XXH3_OF_HMAC_IS);
    expect_single_value("hmac_sha256(int, int) is unchanged",
        ".decl n(x: int64)\nn(1).\n"
        ".decl r(z: int64)\n"
        "r(hmac_sha256(x, 1)) :- n(x).\n",
        XXH3_OF_HMAC_II);
    /*
     * An empty symbol key is zero bytes, which PSA will not import; before
     * #963 both operands were 8 bytes so it could not arise.  RFC 2104
     * zero-pads the key, so this must equal Python's hmac.new(b"", ...) and
     * not fail the query.
     */
    expect_single_value("hmac_sha256(sym, \"\") uses the RFC 2104 empty key",
        ".decl p(m: symbol, k: symbol)\n"
        "p(\"msg\", \"\").\n"
        ".decl r(z: int64)\n"
        "r(hmac_sha256(m, k)) :- p(m, k).\n",
        XXH3_OF_HMAC_EMPTY_KEY);

    expect_single_value("uuid5(sym, sym) hashes both strings' bytes",
        SYM_FACT ".decl k(v: symbol)\nk(\"zz\").\n"
        ".decl r(z: int64)\n"
        "r(uuid5(ns, nm)) :- s(ns), k(nm).\n",
        UUID5_SS_AA_ZZ);
    expect_single_value("uuid5(sym, int) hashes bytes then int64",
        SYM_FACT ".decl r(z: int64)\n"
        "r(uuid5(v, 1)) :- s(v).\n",
        UUID5_SI_AA_1);
    expect_single_value("uuid5(int, sym) hashes int64 then bytes",
        ".decl k(v: symbol)\nk(\"zz\").\n"
        ".decl r(z: int64)\n"
        "r(uuid5(1, nm)) :- k(nm).\n",
        UUID5_IS_1_ZZ);
}

/*
 * Distinct typed (namespace, name) pairs produce distinct digest inputs.
 *
 * Giving uuid5() variable-length operands is what makes this possible to
 * get wrong, in two independent ways.
 *
 * The split.  A bare concatenation of "ab" + "c" and of "a" + "bc" is the
 * same three bytes, so both calls would hash identical input and return one
 * value -- a collision that could not arise while both operands were
 * fixed-width int64s.  A length prefix per operand fixes it.
 *
 * The type.  A length prefix says nothing about what an operand *is*, so
 * the symbol "abcdefgh" and the int64 whose eight little-endian bytes spell
 * it are still the same framed operand.  A one-byte domain tag per operand
 * -- 'S' or 'I' -- fixes that, and as a side effect separates uuid5("", "")
 * from uuid5(0, 0), whose framings were both sixteen zero bytes.
 *
 * Both the inequalities and the values are asserted.  The inequalities
 * alone would be satisfied by any construction that merely differs, and
 * pinned values alone would not say what property they encode.
 *
 * Unambiguous is still not injective, and not RFC 4122.  The digest input
 * is one-to-one with the typed operand pair, but only 8 of the 16 digest
 * bytes are returned and 4 bits of those are overwritten with the version
 * nibble, so uuid5() has at most 2^60 outputs and collides at scale like
 * any 60-bit fingerprint.  Separately, the namespace need not be a UUID.
 * Those are separate claims from the ones tested here.
 */
static void
test_uuid5_distinguishes_operand_boundaries(void)
{
    expect_single_value("uuid5(\"ab\",\"c\") length-frames its operands",
        ".decl p(a: symbol, b: symbol)\n"
        "p(\"ab\", \"c\").\n"
        ".decl r(z: int64)\n"
        "r(uuid5(a, b)) :- p(a, b).\n",
        UUID5_AB_C);
    expect_single_value("uuid5(\"a\",\"bc\") differs from uuid5(\"ab\",\"c\")",
        ".decl q(a: symbol, b: symbol)\n"
        "q(\"a\", \"bc\").\n"
        ".decl r(z: int64)\n"
        "r(uuid5(a, b)) :- q(a, b).\n",
        UUID5_A_BC);

    /*
     * Stated as a property too.  Comparing the two #defines would be a
     * preprocessor comparison that no source change can ever falsify, so
     * re-evaluate both programs instead.
     */
    TEST("uuid5 operand boundaries are unambiguous");
    int64_t ab_c = 0, a_bc = 0;
    if (eval_single_value(
            ".decl p(a: symbol, b: symbol)\n"
            "p(\"ab\", \"c\").\n"
            ".decl r(z: int64)\n"
            "r(uuid5(a, b)) :- p(a, b).\n", &ab_c) != 0
        || eval_single_value(
            ".decl p(a: symbol, b: symbol)\n"
            "p(\"a\", \"bc\").\n"
            ".decl r(z: int64)\n"
            "r(uuid5(a, b)) :- p(a, b).\n", &a_bc) != 0) {
        FAIL("evaluation failed");
    } else if (ab_c == a_bc) {
        FAIL("uuid5(\"ab\",\"c\") still equals uuid5(\"a\",\"bc\")");
    } else {
        PASS();
    }
}

/*
 * The operand's *type* is part of the digest input too (Issue #968).
 *
 * These are the two pairs that a length prefix alone cannot separate.  Each
 * is asserted as a value and then as an inequality, so removing the domain
 * tag fails the property and not only the pin.
 *
 * uuid5(0, 0) goes through the int64-only opcode 0x2A, which is deliberately
 * left unframed and untagged: it is the one uuid5 encoding that has shipped,
 * and UUID5_II_ZERO is the value it returned before #968.
 */
static void
test_uuid5_distinguishes_operand_types(void)
{
    /* Two empty symbols; framed, they were sixteen zero bytes. */
    static const char *const src_ss_empty =
        ".decl p(a: symbol, b: symbol)\n"
        "p(\"\", \"\").\n"
        ".decl r(z: int64)\n"
        "r(uuid5(a, b)) :- p(a, b).\n";
    /* Two zero int64s, via the unframed 0x2A opcode. */
    static const char *const src_ii_zero =
        ".decl n(a: int64, b: int64)\n"
        "n(0, 0).\n"
        ".decl r(z: int64)\n"
        "r(uuid5(a, b)) :- n(a, b).\n";
    /* An 8-byte symbol namespace... */
    static const char *const src_ss_8byte =
        ".decl p(a: symbol, b: symbol)\n"
        "p(\"abcdefgh\", \"x\").\n"
        ".decl r(z: int64)\n"
        "r(uuid5(a, b)) :- p(a, b).\n";
    /* ...and the int64 whose little-endian bytes are that same "abcdefgh",
     * i.e. unpack('<q', b"abcdefgh") == 7523094288207667809. */
    static const char *const src_is_8byte =
        ".decl k(b: symbol)\n"
        "k(\"x\").\n"
        ".decl r(z: int64)\n"
        "r(uuid5(7523094288207667809, b)) :- k(b).\n";

    /*
     * The tag comes from what filt_digest_bytes() actually emitted, not
     * from the operand's declared type -- and that distinction is the
     * whole fix.  .decl types are not enforced, so a symbol-declared
     * column can hold a value that names no interned string; the digest
     * then falls back to the int64 bytes.  Tagging that 'S' because the
     * column says symbol would put those bytes back in the symbol domain
     * and restore the very collision this test pins apart.  Without this
     * case that mutation passes the entire suite.
     */
    static const char *const src_mistyped =
        ".decl S(s: symbol)\n"
        "S(7523094288207667809).\n"
        ".decl k(b: symbol)\n"
        "k(\"x\").\n"
        ".decl r(z: int64)\n"
        "r(uuid5(s, b)) :- S(s), k(b).\n";

    expect_single_value("uuid5(\"\",\"\") tags its empty symbol operands",
        src_ss_empty, UUID5_SS_EMPTY);
    expect_single_value("uuid5(0,0) keeps the unframed int64 encoding",
        src_ii_zero, UUID5_II_ZERO);
    expect_single_value("uuid5(sym, sym) tags an 8-byte symbol namespace",
        src_ss_8byte, UUID5_SS_8BYTE);
    expect_single_value("uuid5(int, sym) tags the int64 namespace",
        src_is_8byte, UUID5_IS_8BYTE);
    /* The mistyped column: declared symbol, holding a value that names no
     * interned string.  It must land in the int64 domain, i.e. agree with
     * the genuine int64 call above rather than the 8-byte symbol one. */
    expect_single_value("a symbol column holding an un-interned value is "
        "tagged int64", src_mistyped, UUID5_IS_8BYTE);

    /*
     * The same two facts as properties.  These re-evaluate rather than
     * comparing the pinned macros above: a macro-vs-macro assertion is
     * decided by the preprocessor and cannot fail however the encoding
     * changes, so it would not catch the domain tag being dropped.
     */
    int64_t ss_empty = 0, ii_zero = 0, ss_8byte = 0, is_8byte = 0;

    TEST("uuid5(\"\",\"\") differs from uuid5(0,0)");
    if (eval_single_value(src_ss_empty, &ss_empty) != 0
        || eval_single_value(src_ii_zero, &ii_zero) != 0) {
        FAIL("evaluation failed");
    }
    if (ss_empty == ii_zero) {
        FAIL("empty symbols and zero int64s hash alike: the tag is missing");
    }
    PASS();

    TEST("uuid5(\"abcdefgh\",x) differs from uuid5(<same bytes as int64>,x)");
    if (eval_single_value(src_ss_8byte, &ss_8byte) != 0
        || eval_single_value(src_is_8byte, &is_8byte) != 0) {
        FAIL("evaluation failed");
    }
    if (ss_8byte == is_8byte) {
        FAIL("a symbol and the int64 spelling it hash alike: tag is missing");
    }
    PASS();
}

#else /* !WL_MBEDTLS_ENABLED */

/* ======================================================================== */
/* mbedTLS-disabled: runtime behavior tests                                */
/*                                                                          */
/* Crypto built-ins parse and plan successfully but hit the evaluator error */
/* path (goto bad) at runtime. The snapshot returns an error and emits no   */
/* tuple, matching the columnar fail-closed expression policy.              */
/* ======================================================================== */

static void
test_unavailable_builtin_fails_closed(const char *name, const char *src)
{
    TEST(name);

    struct result_ctx ctx;
    int rc = run_program_full(src, &ctx);
    if (rc != 0) {
        FAIL("expected parse/plan/session to succeed even without mbedTLS");
    }
    if (ctx.snapshot_rc == 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "snapshot_rc=%d, expected non-zero",
            ctx.snapshot_rc);
        FAIL(buf);
    }
    if (ctx.count != 0) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 0 tuples, got %u", ctx.count);
        FAIL(buf);
    }
    PASS();
}

static void
test_md5_fails_closed_no_mbedtls(void)
{
    const char *src = ".decl a(x: int64)\n"
        "a(0).\n"
        ".decl r(z: int64)\n"
        "r(md5(x)) :- a(x).\n";
    test_unavailable_builtin_fails_closed(
        "md5(): without mbedTLS fails closed",
        src);
}

static void
test_sha1_fails_closed_no_mbedtls(void)
{
    const char *src = ".decl a(x: int64)\n"
        "a(0).\n"
        ".decl r(z: int64)\n"
        "r(sha1(x)) :- a(x).\n";
    test_unavailable_builtin_fails_closed(
        "sha1(): without mbedTLS fails closed",
        src);
}

static void
test_sha256_fails_closed_no_mbedtls(void)
{
    const char *src = ".decl a(x: int64)\n"
        "a(0).\n"
        ".decl r(z: int64)\n"
        "r(sha256(x)) :- a(x).\n";
    test_unavailable_builtin_fails_closed(
        "sha256(): without mbedTLS fails closed",
        src);
}

static void
test_sha512_fails_closed_no_mbedtls(void)
{
    const char *src = ".decl a(x: int64)\n"
        "a(0).\n"
        ".decl r(z: int64)\n"
        "r(sha512(x)) :- a(x).\n";
    test_unavailable_builtin_fails_closed(
        "sha512(): without mbedTLS fails closed",
        src);
}

static void
test_hmac_sha256_fails_closed_no_mbedtls(void)
{
    const char *src = ".decl a(msg: int64, key: int64)\n"
        "a(0, 1).\n"
        ".decl r(z: int64)\n"
        "r(hmac_sha256(msg, key)) :- a(msg, key).\n";
    test_unavailable_builtin_fails_closed(
        "hmac_sha256(): without mbedTLS fails closed",
        src);
}

static void
test_uuid4_fails_closed_no_mbedtls(void)
{
    const char *src = ".decl a(x: int64)\n"
        "a(0).\n"
        ".decl r(z: int64)\n"
        "r(uuid4()) :- a(x).\n";
    test_unavailable_builtin_fails_closed(
        "uuid4(): without mbedTLS fails closed",
        src);
}

static void
test_uuid5_fails_closed_no_mbedtls(void)
{
    const char *src = ".decl a(ns: int64, name: int64)\n"
        "a(1234, 5678).\n"
        ".decl r(z: int64)\n"
        "r(uuid5(ns, name)) :- a(ns, name).\n";
    test_unavailable_builtin_fails_closed(
        "uuid5(): without mbedTLS fails closed",
        src);
}

/*
 * The symbol-operand opcodes (#963) are emitted unconditionally -- only the
 * evaluator arms behind them are #ifdef'd -- so the new opcodes must fail
 * closed here too, not fall through to some permissive default.
 */
static void
test_symbol_operand_digests_fail_closed_no_mbedtls(void)
{
    test_unavailable_builtin_fails_closed(
        "sha256(sym): without mbedTLS fails closed",
        ".decl s(v: symbol)\n"
        "s(\"aa\").\n"
        ".decl r(z: int64)\n"
        "r(sha256(v)) :- s(v).\n");
    test_unavailable_builtin_fails_closed(
        "hmac_sha256(sym, sym): without mbedTLS fails closed",
        ".decl p(m: symbol, k: symbol)\n"
        "p(\"aa\", \"zz\").\n"
        ".decl r(z: int64)\n"
        "r(hmac_sha256(m, k)) :- p(m, k).\n");
    test_unavailable_builtin_fails_closed(
        "uuid5(sym, sym): without mbedTLS fails closed",
        ".decl p(a: symbol, b: symbol)\n"
        "p(\"aa\", \"zz\").\n"
        ".decl r(z: int64)\n"
        "r(uuid5(a, b)) :- p(a, b).\n");
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

    printf("\n--- sha256()/sha512() Enabled Coverage Tests ---\n");
    test_sha256_enabled_nonzero_distinct();
    test_sha512_enabled_nonzero_distinct();

    printf("\n--- hmac_sha256() Determinism Tests ---\n");
    test_hmac_sha256_determinism_msg0_key1();
    test_hmac_sha256_determinism_msg1_key42();
    test_hmac_sha256_determinism_msg42_key0();
    test_hmac_sha256_key_sensitivity();
    test_hmac_sha256_msg_sensitivity();
    test_hmac_sha256_asymmetric();

    printf("\n--- UUID Built-in Tests ---\n");
    /* uuid4()/uuid5() return the first 8 UUID bytes as int64_t. The version
     * byte is visible in that prefix; the RFC variant byte is not. */
    test_uuid4_enabled_random_version_prefix();
    test_uuid5_enabled_deterministic_version_prefix();
    test_uuid5_enabled_namespace_name_sensitivity();

    printf("\n--- Edge Case Tests ---\n");
    test_md5_edge_zero_input();
    /* TODO: Fix sha1() negative input handling (Issue #???)
     * test_sha1_edge_negative_input();
     */
    test_md5_edge_max_int64();
    /* TODO: Fix sha1() negative input handling (Issue #???)
     * test_sha1_edge_min_int64();
     */
    test_hmac_sha256_edge_zero_key();

    printf("\n--- Integration Tests ---\n");
    test_integration_md5_datalog_rule();
    test_integration_sha1_datalog_filter();
    test_integration_hmac_sha256_datalog_rule();

    printf("\n--- Symbol Operand Tests (Issue #963) ---\n");
    test_digests_of_symbol_operands();
    test_two_operand_digests_type_each_operand();
    test_uuid5_distinguishes_operand_boundaries();
    test_uuid5_distinguishes_operand_types();

#else
    printf("=== wirelog Cryptographic Hash Tests (Issue #73) [mbedTLS "
        "DISABLED] ===\n\n");

    printf("--- Runtime Behavior Tests (mbedTLS unavailable) ---\n");
    test_md5_fails_closed_no_mbedtls();
    test_sha1_fails_closed_no_mbedtls();
    test_sha256_fails_closed_no_mbedtls();
    test_sha512_fails_closed_no_mbedtls();
    test_hmac_sha256_fails_closed_no_mbedtls();
    test_uuid4_fails_closed_no_mbedtls();
    test_uuid5_fails_closed_no_mbedtls();
    test_symbol_operand_digests_fail_closed_no_mbedtls();
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
