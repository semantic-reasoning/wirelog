/*
 * test_string_eval.c - Evaluator Integration Tests for String Functions (Issue #143)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * End-to-end tests: parse Datalog programs using string functions, run through
 * the columnar evaluator, and verify output tuples contain correct values.
 *
 * Strategy: all test programs produce int64-typed output so results can be
 * verified directly from the snapshot callback without intern table access.
 * Symbol-producing functions (cat, substr, to_upper, etc.) are exercised via
 * chaining with strlen, to_number, or str_ord.
 *
 * Evaluation pattern:
 *   .decl a(x: int64)
 *   a(42).
 *   .decl r(z: int64)
 *   r(strlen(to_string(x))) :- a(x).
 *   -> snapshot produces r(2)  [strlen("42") = 2]
 */

#include "../wirelog/backend.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/columnar/memory_governor.h"
#include "../wirelog/ir/program.h"

#include <errno.h>
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
    int64_t values[32];
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
/* Helper: run program and collect first-column output values              */
/* ======================================================================== */

static int
run_string_program(const char *src, int64_t *out_values, int max_values)
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
/* Issue #1470: a denied interning fails the step, never emits id -1        */
/* ======================================================================== */

static wl_columnar_memory_governor_ref_t *
denial_governor(uint64_t usable_bytes)
{
    wl_columnar_memory_resolution_t resolution = {
        .budget_bytes = usable_bytes,
        .headroom_bytes = 0,
        .usable_bytes = usable_bytes,
        .mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING,
        .source = WL_COLUMNAR_MEMORY_SOURCE_ENV,
        .status = WL_COLUMNAR_MEMORY_OK,
    };
    return wl_columnar_memory_governor_ref_create(&resolution);
}

/* Attach @intern to a governor that admits exactly its current footprint,
 * so the next unique string is denied.  Returns the retained governor
 * reference (caller releases) or NULL. */
static wl_columnar_memory_governor_ref_t *
attach_exact_governor(wl_intern_t *intern)
{
    wl_columnar_memory_governor_ref_t *probe = denial_governor(1u << 20);
    wl_columnar_memory_governor_ref_t *exact = NULL;
    uint64_t footprint;
    if (!probe)
        return NULL;
    if (wl_intern_attach_memory_governor(intern, probe) != 0) {
        wl_columnar_memory_governor_ref_release(probe);
        return NULL;
    }
    footprint = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(probe));
    if (wl_intern_detach_memory_governor(intern, probe) != 0) {
        wl_columnar_memory_governor_ref_release(probe);
        return NULL;
    }
    wl_columnar_memory_governor_ref_release(probe);
    exact = denial_governor(footprint);
    if (exact && wl_intern_attach_memory_governor(intern, exact) != 0) {
        wl_columnar_memory_governor_ref_release(exact);
        return NULL;
    }
    return exact;
}

static uint32_t
put_bytes(uint8_t *buf, uint32_t at, const uint8_t *src, uint32_t n)
{
    memcpy(buf + at, src, n);
    return at + n;
}

/* Evaluator level: VAR col0, CONST_STR "-suffix", STR_FN_CAT over a row
 * whose result is unique fails with ALLOCATION_FAILURE and out_val 0; a
 * CONST_STR that itself needs interning fails the same way.  On the
 * previous code both returned OK with out_val -1. */
static void
test_eval_denied_put_fails_evaluation(void)
{
    TEST("denied interning fails the expression with ALLOCATION_FAILURE");
    wl_intern_t *intern = wl_intern_create();
    wl_columnar_memory_governor_ref_t *exact = NULL;
    uint8_t buf[64];
    uint32_t size = 0;
    int64_t alpha, out_val = 7;
    int64_t row[1];
    uint32_t count;
    wl_columnar_expr_status_t status = WL_COLUMNAR_EXPR_OK;
    wl_columnar_expr_context_t ctx;
    int rc, ok;
    if (!intern) {
        FAIL("intern create failed");
        return;
    }
    alpha = wl_intern_put(intern, "alpha");
    /* The literal is interned ahead of time, as the planner does. */
    if (alpha < 0 || wl_intern_put(intern, "-suffix") < 0
        || !(exact = attach_exact_governor(intern))) {
        FAIL("fixture: exact governor attach failed");
        wl_intern_free(intern);
        return;
    }
    count = wl_intern_count(intern);
    memset(&ctx, 0, sizeof(ctx));
    ctx.intern = intern;
    ctx.configured_worker_count = 1;
    ctx.active_worker_count = 1;
    /* VAR "col0"; CONST_STR "-suffix"; STR_FN_CAT */
    {
        const uint8_t var[] = { WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0' };
        const uint8_t lit[] = { WL_PLAN_EXPR_CONST_STR, 7, 0,
                                '-', 's', 'u', 'f', 'f', 'i', 'x' };
        const uint8_t cat[] = { WL_PLAN_EXPR_STR_FN_CAT };
        size = put_bytes(buf, size, var, sizeof(var));
        size = put_bytes(buf, size, lit, sizeof(lit));
        size = put_bytes(buf, size, cat, sizeof(cat));
    }
    row[0] = alpha;
    rc = wl_columnar_expr_eval_run_ctx(buf, size, row, 1, &out_val, &ctx,
            &status);
    ok = rc == WL_COLUMNAR_EXPR_ALLOCATION_FAILURE
        && status == WL_COLUMNAR_EXPR_ALLOCATION_FAILURE && out_val == 0
        && wl_intern_count(intern) == count;
    /* A literal that is not pre-interned is refused the same way. */
    {
        const uint8_t lit[] = { WL_PLAN_EXPR_CONST_STR, 9, 0,
                                'b', 'r', 'a', 'n', 'd', '-', 'n', 'e', 'w' };
        size = put_bytes(buf, 0, lit, sizeof(lit));
    }
    status = WL_COLUMNAR_EXPR_OK;
    out_val = 7;
    rc = wl_columnar_expr_eval_run_ctx(buf, size, row, 1, &out_val, &ctx,
            &status);
    ok = ok && rc == WL_COLUMNAR_EXPR_ALLOCATION_FAILURE
        && status == WL_COLUMNAR_EXPR_ALLOCATION_FAILURE && out_val == 0
        && wl_intern_count(intern) == count;
    /* An invalid operand is still an ordinary -1 result, not a failure. */
    {
        const uint8_t var[] = { WL_PLAN_EXPR_VAR, 4, 0, 'c', 'o', 'l', '0' };
        const uint8_t lit[] = { WL_PLAN_EXPR_CONST_STR, 7, 0,
                                '-', 's', 'u', 'f', 'f', 'i', 'x' };
        const uint8_t cat[] = { WL_PLAN_EXPR_STR_FN_CAT };
        size = put_bytes(buf, 0, var, sizeof(var));
        size = put_bytes(buf, size, lit, sizeof(lit));
        size = put_bytes(buf, size, cat, sizeof(cat));
    }
    row[0] = 12345; /* not an interned id */
    status = WL_COLUMNAR_EXPR_OK;
    rc = wl_columnar_expr_eval_run_ctx(buf, size, row, 1, &out_val, &ctx,
            &status);
    ok = ok && rc == WL_COLUMNAR_EXPR_OK && status == WL_COLUMNAR_EXPR_OK
        && out_val == -1;
    wl_intern_free(intern);
    wl_columnar_memory_governor_ref_release(exact);
    if (!ok) {
        FAIL("denied interning did not fail the evaluation cleanly");
        return;
    }
    PASS();
}

/* Session level: r(cat(x, "-suffix")) :- a(x) with the program's intern
 * table attached to a governor that admits exactly its footprint.  The
 * snapshot must fail with ENOMEM, emit no row (in particular none with a
 * negative id), and leave the table and its reservation unchanged; the
 * same program under a normal budget yields the two suffixed strings.
 * On the previous code the snapshot returned 0 with rows carrying id -1. */
static void
test_session_denied_put_fails_step(uint32_t workers)
{
    char name[96];
    snprintf(name, sizeof(name),
        "denied interning fails the step with ENOMEM (%u worker%s)",
        workers, workers == 1 ? "" : "s");
    TEST(name);
    const char *src =
        ".decl a(x: symbol)\n"
        "a(\"alpha\"). a(\"beta\").\n"
        ".decl r(y: symbol)\n"
        "r(cat(x, \"-suffix\")) :- a(x).\n";
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    wl_plan_t *plan = NULL;
    wl_session_t *sess = NULL;
    wl_columnar_memory_governor_ref_t *exact = NULL;
    struct result_ctx ctx = { .count = 0, .error = 0 };
    uint32_t count;
    uint64_t reserved;
    int rc, ok;
    if (!prog) {
        FAIL("parse failed");
        return;
    }
    wl_fusion_apply(prog, NULL);
    wl_jpp_apply(prog, NULL);
    wl_sip_apply(prog, NULL);
    /* The plan is built first: it pre-interns the "-suffix" literal, so
     * the first charged put is the builtin's result.  The test keeps its
     * own reference to the exact governor until the program is freed;
     * under the #1469 rebind an unreferenced governor would otherwise be
     * taken over by the session's governor and the denial would vanish. */
    if (wl_plan_from_program(prog, &plan) != 0
        || !(exact = attach_exact_governor(prog->intern))) {
        if (plan)
            wl_plan_free(plan);
        wirelog_program_free(prog);
        FAIL("fixture: plan or exact governor attach failed");
        return;
    }
    count = wl_intern_count(prog->intern);
    reserved = wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(exact));
    if (wl_session_create(wl_backend_columnar(), plan, workers, &sess) != 0
        || wl_session_load_facts(sess, prog) != 0) {
        if (sess)
            wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        wl_columnar_memory_governor_ref_release(exact);
        FAIL("fixture: session create or fact load failed");
        return;
    }
    rc = wl_session_snapshot(sess, capture_cb, &ctx);
    ok = rc == ENOMEM && ctx.count == 0
        && wl_intern_count(prog->intern) == count
        && wl_columnar_memory_reserved(
        wl_columnar_memory_governor_ref_get(exact)) == reserved;
    for (uint32_t i = 0; i < ctx.count; i++)
        if (ctx.values[i] < 0)
            ok = 0;
    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
    wl_columnar_memory_governor_ref_release(exact);
    if (!ok) {
        char msg[96];
        snprintf(msg, sizeof(msg),
            "snapshot rc=%d rows=%u (expected ENOMEM and no rows)", rc,
            ctx.count);
        FAIL(msg);
        return;
    }
    /* The same program under a normal budget produces both strings. */
    {
        int64_t values[4];
        int n = run_string_program(src, values, 4);
        if (n != 2 || values[0] < 0 || values[1] < 0
            || values[0] == values[1]) {
            FAIL("rerun under a normal budget did not yield two strings");
            return;
        }
    }
    PASS();
}

#ifdef WL_MBEDTLS_ENABLED
static void
test_eval_uuid5_rfc_known_vector(void)
{
    TEST("uuid5_rfc matches RFC 4122 known vector");

    const char *src =
        ".decl a(ns: symbol, name: symbol)\n"
        "a(\"6ba7b810-9dad-11d1-80b4-00c04fd430c8\", \"python.org\").\n"
        ".decl r(ok: int64)\n"
        "r(1) :- a(ns, name), uuid5_rfc(ns, name) = "
        "\"886313e1-3b8a-5372-9b90-0c9aee199e5d\".\n";
    int64_t values[2];
    int n = run_string_program(src, values, 2);
    if (n != 1 || values[0] != 1) {
        FAIL("RFC 4122 UUID v5 known vector did not match");
        return;
    }
    PASS();
}
#endif

/* ======================================================================== */
/* strlen tests                                                             */
/* ======================================================================== */

static void
test_eval_strlen_via_to_string_2digits(void)
{
    TEST("strlen(to_string(42)) = 2");

    const char *src =
        ".decl a(x: int64)\n"
        "a(42).\n"
        ".decl r(z: int64)\n"
        "r(strlen(to_string(x))) :- a(x).\n";

    int64_t vals[4];
    int n = run_string_program(src, vals, 4);

    if (n != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 tuple, got %d", n);
        FAIL(buf);
        return;
    }
    if (vals[0] != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 2, got %" PRId64, vals[0]);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_strlen_via_to_string_3digits(void)
{
    TEST("strlen(to_string(100)) = 3");

    const char *src =
        ".decl a(x: int64)\n"
        "a(100).\n"
        ".decl r(z: int64)\n"
        "r(strlen(to_string(x))) :- a(x).\n";

    int64_t vals[4];
    int n = run_string_program(src, vals, 4);

    if (n != 1 || vals[0] != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3, got %" PRId64 " (n=%d)",
            n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_strlen_negative_number_has_sign(void)
{
    TEST("strlen(to_string(-5)) = 2 (sign counts)");

    /* Use 0-5 to produce -5 since negative literal facts may not parse */
    const char *src =
        ".decl a(x: int64)\n"
        "a(5).\n"
        ".decl r(z: int64)\n"
        "r(strlen(to_string(0 - x))) :- a(x).\n";

    int64_t vals[4];
    int n = run_string_program(src, vals, 4);

    if (n != 1 || vals[0] != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 2, got %" PRId64 " (n=%d)",
            n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* to_number round-trip tests                                              */
/* ======================================================================== */

static void
test_eval_to_number_to_string_roundtrip(void)
{
    TEST("to_number(to_string(42)) = 42 (round-trip)");

    const char *src =
        ".decl a(x: int64)\n"
        "a(42).\n"
        ".decl r(z: int64)\n"
        "r(to_number(to_string(x))) :- a(x).\n";

    int64_t vals[4];
    int n = run_string_program(src, vals, 4);

    if (n != 1 || vals[0] != 42) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 42, got %" PRId64 " (n=%d)",
            n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_to_number_to_string_zero(void)
{
    TEST("to_number(to_string(0)) = 0");

    const char *src =
        ".decl a(x: int64)\n"
        "a(0).\n"
        ".decl r(z: int64)\n"
        "r(to_number(to_string(x))) :- a(x).\n";

    int64_t vals[4];
    int n = run_string_program(src, vals, 4);

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
/* str_ord tests                                                            */
/* ======================================================================== */

static void
test_eval_str_ord_of_digit_string(void)
{
    /* to_string(65) = "65", str_ord("65") = ord('6') = 54 */
    TEST("str_ord(to_string(65)) = 54 (ord of '6')");

    const char *src =
        ".decl a(x: int64)\n"
        "a(65).\n"
        ".decl r(z: int64)\n"
        "r(str_ord(to_string(x))) :- a(x).\n";

    int64_t vals[4];
    int n = run_string_program(src, vals, 4);

    if (n != 1 || vals[0] != 54) {  /* '6' = 54 */
        char buf[64];
        snprintf(buf, sizeof(buf),
            "expected 54 (ord '6'), got %" PRId64 " (n=%d)",
            n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_str_ord_single_digit(void)
{
    /* to_string(5) = "5", str_ord("5") = 53 (ASCII '5') */
    TEST("str_ord(to_string(5)) = 53 (ord of '5')");

    const char *src =
        ".decl a(x: int64)\n"
        "a(5).\n"
        ".decl r(z: int64)\n"
        "r(str_ord(to_string(x))) :- a(x).\n";

    int64_t vals[4];
    int n = run_string_program(src, vals, 4);

    if (n != 1 || vals[0] != 53) {  /* '5' = 53 */
        char buf[64];
        snprintf(buf, sizeof(buf),
            "expected 53 (ord '5'), got %" PRId64 " (n=%d)",
            n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* contains filter tests                                                   */
/* ======================================================================== */

static void
test_eval_contains_filter_selects_matching(void)
{
    /* to_string(100) = "100" contains to_string(1) = "1" -> true  */
    /* to_string(200) = "200" contains to_string(1) = "1" -> false */
    TEST(
        "contains filter: only tuple where to_string(x) contains to_string(y) passes");

    const char *src =
        ".decl a(x: int64)\n"
        "a(100).\n"
        "a(200).\n"
        ".decl b(y: int64)\n"
        "b(1).\n"
        ".decl r(z: int64)\n"
        "r(x) :- a(x), b(y), contains(to_string(x), to_string(y)).\n";

    int64_t vals[8];
    int n = run_string_program(src, vals, 8);

    if (n < 0) {
        FAIL("evaluation failed");
        return;
    }
    if (n != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 output tuple, got %d", n);
        FAIL(buf);
        return;
    }
    if (vals[0] != 100) {
        char buf[64];
        snprintf(buf, sizeof(buf),
            "expected x=100 to pass filter, got %" PRId64, vals[0]);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* str_prefix filter tests                                                 */
/* ======================================================================== */

static void
test_eval_str_prefix_filter(void)
{
    /* to_string(100) = "100" starts with to_string(10) = "10" -> true  */
    /* to_string(200) = "200" starts with to_string(10) = "10" -> false */
    TEST("str_prefix filter: selects only tuple with matching prefix");

    const char *src =
        ".decl a(x: int64)\n"
        "a(100).\n"
        "a(200).\n"
        ".decl b(y: int64)\n"
        "b(10).\n"
        ".decl r(z: int64)\n"
        "r(x) :- a(x), b(y), str_prefix(to_string(x), to_string(y)).\n";

    int64_t vals[8];
    int n = run_string_program(src, vals, 8);

    if (n < 0) {
        FAIL("evaluation failed");
        return;
    }
    if (n != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 output tuple, got %d", n);
        FAIL(buf);
        return;
    }
    if (vals[0] != 100) {
        char buf[64];
        snprintf(buf, sizeof(buf),
            "expected x=100 passes prefix filter, got %" PRId64, vals[0]);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* str_suffix filter tests                                                 */
/* ======================================================================== */

static void
test_eval_str_suffix_filter(void)
{
    /* to_string(110) = "110" ends with to_string(0) = "0" -> true  */
    /* to_string(211) = "211" ends with to_string(0) = "0" -> false */
    TEST("str_suffix filter: selects only tuple with matching suffix");

    const char *src =
        ".decl a(x: int64)\n"
        "a(110).\n"
        "a(211).\n"
        ".decl b(y: int64)\n"
        "b(0).\n"
        ".decl r(z: int64)\n"
        "r(x) :- a(x), b(y), str_suffix(to_string(x), to_string(y)).\n";

    int64_t vals[8];
    int n = run_string_program(src, vals, 8);

    if (n < 0) {
        FAIL("evaluation failed");
        return;
    }
    if (n != 1) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 1 output tuple, got %d", n);
        FAIL(buf);
        return;
    }
    if (vals[0] != 110) {
        char buf[64];
        snprintf(buf, sizeof(buf),
            "expected x=110 passes suffix filter, got %" PRId64, vals[0]);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* to_upper / to_lower via strlen (codepoint count preserved)             */
/* ======================================================================== */

static void
test_eval_to_upper_preserves_strlen(void)
{
    /* strlen(to_upper(to_string(42))) = strlen("42") = 2 */
    TEST("strlen(to_upper(to_string(42))) = 2 (length preserved)");

    const char *src =
        ".decl a(x: int64)\n"
        "a(42).\n"
        ".decl r(z: int64)\n"
        "r(strlen(to_upper(to_string(x)))) :- a(x).\n";

    int64_t vals[4];
    int n = run_string_program(src, vals, 4);

    if (n != 1 || vals[0] != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 2, got %" PRId64 " (n=%d)",
            n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

static void
test_eval_to_lower_preserves_strlen(void)
{
    /* strlen(to_lower(to_string(42))) = strlen("42") = 2 */
    TEST("strlen(to_lower(to_string(42))) = 2 (length preserved)");

    const char *src =
        ".decl a(x: int64)\n"
        "a(42).\n"
        ".decl r(z: int64)\n"
        "r(strlen(to_lower(to_string(x)))) :- a(x).\n";

    int64_t vals[4];
    int n = run_string_program(src, vals, 4);

    if (n != 1 || vals[0] != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 2, got %" PRId64 " (n=%d)",
            n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* trim via strlen (trimming digits-only string is a no-op)               */
/* ======================================================================== */

static void
test_eval_trim_no_whitespace_preserves_strlen(void)
{
    /* strlen(trim(to_string(42))) = strlen(trim("42")) = strlen("42") = 2 */
    TEST("strlen(trim(to_string(42))) = 2 (no whitespace to trim)");

    const char *src =
        ".decl a(x: int64)\n"
        "a(42).\n"
        ".decl r(z: int64)\n"
        "r(strlen(trim(to_string(x)))) :- a(x).\n";

    int64_t vals[4];
    int n = run_string_program(src, vals, 4);

    if (n != 1 || vals[0] != 2) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 2, got %" PRId64 " (n=%d)",
            n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* cat via strlen (sum of lengths)                                        */
/* ======================================================================== */

static void
test_eval_strlen_of_cat_equals_sum(void)
{
    /* strlen(cat(to_string(x), to_string(y))) = strlen("12") + strlen("34") = 2+2 = 4 */
    TEST("strlen(cat(to_string(x), to_string(y))) = sum of individual lengths");

    const char *src =
        ".decl a(x: int64, y: int64)\n"
        "a(12, 34).\n"
        ".decl r(z: int64)\n"
        "r(strlen(cat(to_string(x), to_string(y)))) :- a(x, y).\n";

    int64_t vals[4];
    int n = run_string_program(src, vals, 4);

    if (n != 1 || vals[0] != 4) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 4, got %" PRId64 " (n=%d)",
            n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* substr via strlen                                                        */
/* ======================================================================== */

static void
test_eval_strlen_of_substr(void)
{
    /* substr(to_string(12345), 1, 3) -> substr("12345", 1, 3) = "234", strlen = 3 */
    TEST("strlen(substr(to_string(12345), 1, 3)) = 3");

    const char *src =
        ".decl a(x: int64)\n"
        "a(12345).\n"
        ".decl r(z: int64)\n"
        "r(strlen(substr(to_string(x), 1, 3))) :- a(x).\n";

    int64_t vals[4];
    int n = run_string_program(src, vals, 4);

    if (n != 1 || vals[0] != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3, got %" PRId64 " (n=%d)",
            n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* str_replace via to_number                                               */
/* ======================================================================== */

static void
test_eval_str_replace_and_to_number(void)
{
    /* to_string(123) = "123", str_replace("123", to_string(2), to_string(9))
     * = "193", to_number("193") = 193 */
    TEST(
        "to_number(str_replace(to_string(123), to_string(2), to_string(9))) = 193");

    const char *src =
        ".decl a(x: int64)\n"
        "a(123).\n"
        ".decl r(z: int64)\n"
        "r(to_number(str_replace(to_string(x), to_string(2), to_string(9)))) :- a(x).\n";

    int64_t vals[4];
    int n = run_string_program(src, vals, 4);

    if (n != 1 || vals[0] != 193) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 193, got %" PRId64 " (n=%d)",
            n > 0 ? vals[0] : -1LL, n);
        FAIL(buf);
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Multiple input tuples produce multiple output tuples                    */
/* ======================================================================== */

static void
test_eval_strlen_multiple_inputs(void)
{
    TEST(
        "strlen(to_string(x)) applied to multiple inputs produces multiple outputs");

    const char *src =
        ".decl a(x: int64)\n"
        "a(1).\n"    /* to_string(1) = "1", strlen = 1 */
        "a(10).\n"   /* to_string(10) = "10", strlen = 2 */
        "a(100).\n"  /* to_string(100) = "100", strlen = 3 */
        ".decl r(z: int64)\n"
        "r(strlen(to_string(x))) :- a(x).\n";

    int64_t vals[8];
    int n = run_string_program(src, vals, 8);

    if (n < 0) {
        FAIL("evaluation failed");
        return;
    }
    if (n != 3) {
        char buf[64];
        snprintf(buf, sizeof(buf), "expected 3 output tuples, got %d", n);
        FAIL(buf);
        return;
    }
    /* Verify all three expected values appear (order may vary) */
    int found1 = 0, found2 = 0, found3 = 0;
    for (int i = 0; i < n; i++) {
        if (vals[i] == 1) found1 = 1;
        if (vals[i] == 2) found2 = 1;
        if (vals[i] == 3) found3 = 1;
    }
    if (!found1 || !found2 || !found3) {
        FAIL("expected output values {1, 2, 3} not all present");
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
    printf(
        "=== String Function Evaluator Integration Tests (Issue #143) ===\n");

    printf("\n--- strlen via to_string ---\n");
    test_eval_strlen_via_to_string_2digits();
    test_eval_strlen_via_to_string_3digits();
    test_eval_strlen_negative_number_has_sign();

    printf("\n--- to_number / to_string round-trip ---\n");
    test_eval_to_number_to_string_roundtrip();
    test_eval_to_number_to_string_zero();

    printf("\n--- str_ord ---\n");
    test_eval_str_ord_of_digit_string();
    test_eval_str_ord_single_digit();

    printf("\n--- contains filter ---\n");
    test_eval_contains_filter_selects_matching();

    printf("\n--- str_prefix filter ---\n");
    test_eval_str_prefix_filter();

    printf("\n--- str_suffix filter ---\n");
    test_eval_str_suffix_filter();

    printf("\n--- to_upper / to_lower ---\n");
    test_eval_to_upper_preserves_strlen();
    test_eval_to_lower_preserves_strlen();

    printf("\n--- trim ---\n");
    test_eval_trim_no_whitespace_preserves_strlen();

    printf("\n--- cat ---\n");
    test_eval_strlen_of_cat_equals_sum();

    printf("\n--- substr ---\n");
    test_eval_strlen_of_substr();

    printf("\n--- str_replace ---\n");
    test_eval_str_replace_and_to_number();

    printf("\n--- multiple input tuples ---\n");
    test_eval_strlen_multiple_inputs();
    test_eval_denied_put_fails_evaluation();
    test_session_denied_put_fails_step(1);
    test_session_denied_put_fails_step(2);

#ifdef WL_MBEDTLS_ENABLED
    printf("\n--- uuid5_rfc ---\n");
    test_eval_uuid5_rfc_known_vector();
#endif

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
