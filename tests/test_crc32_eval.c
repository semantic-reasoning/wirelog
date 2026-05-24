/*
 * test_crc32_eval.c - CRC-32 Built-in Evaluator Tests (Issue #884)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * End-to-end tests for crc32_ethernet() / crc32_castagnoli() Datalog
 * built-ins.  Verifies that the columnar evaluator computes the operator
 * (no fail-closed path), that distinct int64 inputs produce distinct
 * outputs, that the two variants disagree on the same input, and that
 * the comparison form used in examples/05-crc32-checksum/crc32_validate.dl
 * (crc32_ethernet(x) = c filter) drives the result set deterministically.
 *
 * The tests use int64 inputs the same way the existing cryptographic
 * hash tests do: the evaluator pops the int64 from the filter stack and
 * hashes its 8 little-endian bytes.
 */

#include "../wirelog/backend.h"
#include "../wirelog/crc32.h"
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

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                            \
        do {                                  \
            tests_run++;                      \
            printf("  [TEST] %-60s", (name)); \
            fflush(stdout);                   \
        } while (0)

#define PASS()             \
        do {               \
            tests_passed++; \
            printf(" PASS\n"); \
        } while (0)

#define FAIL(msg)                         \
        do {                              \
            tests_failed++;               \
            printf(" FAIL: %s\n", (msg)); \
            return;                       \
        } while (0)

struct result_ctx {
    int64_t col0[64];
    uint32_t count;
    int snapshot_rc;
};

static void
capture_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct result_ctx *ctx = (struct result_ctx *)user_data;
    (void)relation;
    (void)ncols;
    if (ctx->count < 64) {
        ctx->col0[ctx->count] = row[0];
        ctx->count++;
    }
}

struct two_col_ctx {
    int64_t col0;
    int64_t col1;
    uint32_t count;
};

static void
capture_two_cols_cb(const char *relation, const int64_t *row, uint32_t ncols,
    void *user_data)
{
    struct two_col_ctx *ctx = (struct two_col_ctx *)user_data;
    (void)relation;
    if (ncols >= 2 && ctx->count == 0) {
        ctx->col0 = row[0];
        ctx->col1 = row[1];
    }
    ctx->count++;
}

static int
run_program(const char *src, struct result_ctx *ctx)
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
    ctx->snapshot_rc = wl_session_snapshot(sess, capture_cb, ctx);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    return 0;
}

/* ======================================================================== */
/* crc32_ethernet() evaluator tests                                         */
/* ======================================================================== */

static void
test_crc32_ethernet_determinism(void)
{
    TEST("crc32_ethernet(0): two evaluations produce the same result");

    const char *src = ".decl a(x: int64)\n"
        "a(0).\n"
        ".decl r(z: int64)\n"
        "r(crc32_ethernet(x)) :- a(x).\n";

    struct result_ctx ctx1, ctx2;
    if (run_program(src, &ctx1) != 0 || ctx1.snapshot_rc != 0) {
        FAIL("first evaluation failed");
    }
    if (run_program(src, &ctx2) != 0 || ctx2.snapshot_rc != 0) {
        FAIL("second evaluation failed");
    }
    if (ctx1.count != 1 || ctx2.count != 1) {
        FAIL("expected 1 tuple each run");
    }
    if (ctx1.col0[0] != ctx2.col0[0]) {
        FAIL("crc32_ethernet(0) not deterministic across runs");
    }
    PASS();
}

static void
test_crc32_ethernet_distinct_inputs(void)
{
    TEST("crc32_ethernet: distinct inputs produce distinct outputs");

    const char *src = ".decl a(x: int64)\n"
        "a(0). a(1). a(42).\n"
        ".decl r(z: int64)\n"
        "r(crc32_ethernet(x)) :- a(x).\n";

    struct result_ctx ctx;
    if (run_program(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 3) {
        FAIL("expected 3 tuples");
    }
    if (ctx.col0[0] == ctx.col0[1] || ctx.col0[0] == ctx.col0[2]
        || ctx.col0[1] == ctx.col0[2]) {
        FAIL("crc32_ethernet collision on distinct inputs");
    }
    PASS();
}

static void
test_crc32_ethernet_matches_engine(void)
{
    TEST("crc32_ethernet(0x123456789ABCDEF0) matches engine output");

    const int64_t input = (int64_t)0x123456789ABCDEF0LL;
    uint8_t bytes[sizeof(input)];
    memcpy(bytes, &input, sizeof(input));
    uint32_t expected = ethernet_crc32(bytes, sizeof(bytes));

    char src[256];
    snprintf(src, sizeof(src),
        ".decl a(x: int64)\n"
        "a(%" PRId64 ").\n"
        ".decl r(z: int64)\n"
        "r(crc32_ethernet(x)) :- a(x).\n",
        input);

    struct result_ctx ctx;
    if (run_program(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 1) {
        FAIL("expected 1 tuple");
    }
    if ((uint32_t)ctx.col0[0] != expected) {
        char buf[128];
        snprintf(buf, sizeof(buf),
            "evaluator=0x%08x engine=0x%08x",
            (uint32_t)ctx.col0[0], expected);
        FAIL(buf);
    }
    PASS();
}

static void
test_crc32_ethernet_filter_matches(void)
{
    TEST("crc32_ethernet(x) = c filter selects only matching rows");

    /* Compute the Ethernet CRC-32 of the int64 12345 so the rule can
     * pin a single matching row. */
    const int64_t input = 12345;
    uint8_t bytes[sizeof(input)];
    memcpy(bytes, &input, sizeof(input));
    int64_t target = (int64_t)ethernet_crc32(bytes, sizeof(bytes));

    char src[384];
    snprintf(src, sizeof(src),
        ".decl pair(x: int64, c: int64)\n"
        "pair(12345, %" PRId64 "). pair(54321, 0).\n"
        ".decl r(x: int64)\n"
        "r(x) :- pair(x, c), crc32_ethernet(x) = c.\n",
        target);

    struct result_ctx ctx;
    if (run_program(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 1 || ctx.col0[0] != 12345) {
        char buf[64];
        snprintf(buf, sizeof(buf), "count=%u first=%" PRId64,
            ctx.count, ctx.count ? ctx.col0[0] : 0);
        FAIL(buf);
    }
    PASS();
}

/* ======================================================================== */
/* crc32_castagnoli() evaluator tests                                       */
/* ======================================================================== */

static void
test_crc32_castagnoli_determinism(void)
{
    TEST("crc32_castagnoli(0): two evaluations produce the same result");

    const char *src = ".decl a(x: int64)\n"
        "a(0).\n"
        ".decl r(z: int64)\n"
        "r(crc32_castagnoli(x)) :- a(x).\n";

    struct result_ctx ctx1, ctx2;
    if (run_program(src, &ctx1) != 0 || ctx1.snapshot_rc != 0) {
        FAIL("first evaluation failed");
    }
    if (run_program(src, &ctx2) != 0 || ctx2.snapshot_rc != 0) {
        FAIL("second evaluation failed");
    }
    if (ctx1.count != 1 || ctx2.count != 1) {
        FAIL("expected 1 tuple each run");
    }
    if (ctx1.col0[0] != ctx2.col0[0]) {
        FAIL("crc32_castagnoli(0) not deterministic across runs");
    }
    PASS();
}

static void
test_crc32_castagnoli_matches_engine(void)
{
    TEST("crc32_castagnoli(0x123456789ABCDEF0) matches engine output");

    const int64_t input = (int64_t)0x123456789ABCDEF0LL;
    uint8_t bytes[sizeof(input)];
    memcpy(bytes, &input, sizeof(input));
    uint32_t expected = castagnoli_crc32(bytes, sizeof(bytes));

    char src[256];
    snprintf(src, sizeof(src),
        ".decl a(x: int64)\n"
        "a(%" PRId64 ").\n"
        ".decl r(z: int64)\n"
        "r(crc32_castagnoli(x)) :- a(x).\n",
        input);

    struct result_ctx ctx;
    if (run_program(src, &ctx) != 0 || ctx.snapshot_rc != 0) {
        FAIL("evaluation failed");
    }
    if (ctx.count != 1) {
        FAIL("expected 1 tuple");
    }
    if ((uint32_t)ctx.col0[0] != expected) {
        char buf[128];
        snprintf(buf, sizeof(buf),
            "evaluator=0x%08x engine=0x%08x",
            (uint32_t)ctx.col0[0], expected);
        FAIL(buf);
    }
    PASS();
}

/* ======================================================================== */
/* Variant separation: Ethernet vs Castagnoli                              */
/* ======================================================================== */

static void
test_crc32_variants_differ(void)
{
    TEST("crc32_ethernet and crc32_castagnoli disagree on a non-trivial input");

    /* CRC-32 Ethernet (poly 0x04C11DB7) and Castagnoli (poly 0x1EDC6F41)
     * are different polynomials.  For any non-zero input they should
     * produce different residues. */
    const char *src = ".decl a(x: int64)\n"
        "a(42).\n"
        ".decl r(eth: int64, cast: int64)\n"
        "r(crc32_ethernet(x), crc32_castagnoli(x)) :- a(x).\n";

    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog) {
        FAIL("parse failed");
    }
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);
    wl_plan_t *plan = NULL;
    if (wl_plan_from_program(prog, &plan) != 0) {
        wirelog_program_free(prog);
        FAIL("plan failed");
    }
    wl_session_t *sess = NULL;
    if (wl_session_create(wl_backend_columnar(), plan, 1, &sess) != 0) {
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("session create failed");
    }
    if (wl_session_load_facts(sess, prog) != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("session load failed");
    }

    struct two_col_ctx captured = { 0, 0, 0 };
    int snap_rc = wl_session_snapshot(sess, capture_two_cols_cb, &captured);

    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);

    if (snap_rc != 0 || captured.count != 1) {
        FAIL("snapshot did not return a single row");
    }
    if (captured.col0 == captured.col1) {
        FAIL("crc32_ethernet and crc32_castagnoli produced identical output");
    }
    PASS();
}

int
main(void)
{
    printf("=== wirelog CRC-32 Evaluator Tests (Issue #884) ===\n\n");

    printf("--- crc32_ethernet() ---\n");
    test_crc32_ethernet_determinism();
    test_crc32_ethernet_distinct_inputs();
    test_crc32_ethernet_matches_engine();
    test_crc32_ethernet_filter_matches();

    printf("\n--- crc32_castagnoli() ---\n");
    test_crc32_castagnoli_determinism();
    test_crc32_castagnoli_matches_engine();

    printf("\n--- Variant separation ---\n");
    test_crc32_variants_differ();

    printf("\n=== Results: %d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0)
        printf(", %d FAILED", tests_failed);
    printf(" ===\n");

    return tests_failed > 0 ? 1 : 0;
}
