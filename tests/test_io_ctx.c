/*
 * test_io_ctx.c - I/O context accessor test scaffold (TDD gate)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Seven test scenarios for wl_io_ctx_t accessors: relation_name,
 * num_cols, col_type, param_lookup, param_null_key, intern_string,
 * platform_ctx. All guarded behind TEST_CTX_PRESENT and currently SKIP
 * until #453 implements the context.
 *
 * Part of #446 (I/O adapter umbrella).
 */

#define TEST_CTX_PRESENT

#include <stdio.h>
#include <string.h>

#include "wirelog/io/io_ctx_internal.h"

/* ======================================================================== */
/* Mock Helpers                                                             */
/* ======================================================================== */

static wl_intern_t *g_intern = NULL;

static wl_io_ctx_t *
create_mock_ctx(const char *relation_name,
    const wirelog_column_type_t *col_types, uint32_t num_cols,
    const char **param_keys, const char **param_values,
    uint32_t num_params)
{
    if (!g_intern)
        g_intern = wl_intern_create();
    return wl_io_ctx_create_test(relation_name, col_types, num_cols,
               param_keys, param_values, num_params,
               g_intern);
}

static void
destroy_mock_ctx(wl_io_ctx_t *ctx)
{
    wl_io_ctx_destroy(ctx);
}

/* ======================================================================== */
/* Test Harness                                                             */
/* ======================================================================== */

#define TEST(name) do { printf("  [TEST] %-50s ", name); } while (0)
#define PASS()     do { printf("PASS\n"); passed++; } while (0)
#define FAIL(msg)  do { printf("FAIL: %s\n", msg); failed++; } while (0)
#define SKIP(msg)  do { printf("SKIP: %s\n", msg); skipped++; } while (0)

static int passed = 0, failed = 0, skipped = 0;

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static void
test_relation_name(void)
{
    TEST("wl_io_ctx_relation_name returns correct name");
#ifdef TEST_CTX_PRESENT
    wirelog_column_type_t cols[] = { WIRELOG_TYPE_INT32 };
    wl_io_ctx_t *ctx = create_mock_ctx("events", cols, 1, NULL, NULL, 0);
    if (strcmp(wl_io_ctx_relation_name(ctx), "events") != 0) {
        FAIL("relation name mismatch"); destroy_mock_ctx(ctx); return;
    }
    PASS();
    destroy_mock_ctx(ctx);
#else
    SKIP("context accessors not yet implemented (#453)");
#endif
}

static void
test_num_cols(void)
{
    TEST("wl_io_ctx_num_cols returns correct count");
#ifdef TEST_CTX_PRESENT
    wirelog_column_type_t cols[] = {
        WIRELOG_TYPE_STRING, WIRELOG_TYPE_INT32, WIRELOG_TYPE_STRING
    };
    wl_io_ctx_t *ctx = create_mock_ctx("rel3", cols, 3, NULL, NULL, 0);
    if (wl_io_ctx_num_cols(ctx) != 3) {
        FAIL("expected 3 columns"); destroy_mock_ctx(ctx); return;
    }
    PASS();
    destroy_mock_ctx(ctx);
#else
    SKIP("context accessors not yet implemented (#453)");
#endif
}

static void
test_col_type(void)
{
    TEST("wl_io_ctx_col_type returns correct types");
#ifdef TEST_CTX_PRESENT
    wirelog_column_type_t cols[] = {
        WIRELOG_TYPE_STRING, WIRELOG_TYPE_INT32, WIRELOG_TYPE_STRING
    };
    wl_io_ctx_t *ctx = create_mock_ctx("typed", cols, 3, NULL, NULL, 0);
    if (wl_io_ctx_col_type(ctx, 0) != WIRELOG_TYPE_STRING) {
        FAIL("col 0 type mismatch"); destroy_mock_ctx(ctx); return;
    }
    if (wl_io_ctx_col_type(ctx, 1) != WIRELOG_TYPE_INT32) {
        FAIL("col 1 type mismatch"); destroy_mock_ctx(ctx); return;
    }
    if (wl_io_ctx_col_type(ctx, 2) != WIRELOG_TYPE_STRING) {
        FAIL("col 2 type mismatch"); destroy_mock_ctx(ctx); return;
    }
    PASS();
    destroy_mock_ctx(ctx);
#else
    SKIP("context accessors not yet implemented (#453)");
#endif
}

static void
test_param_lookup(void)
{
    TEST("wl_io_ctx_param returns correct values");
#ifdef TEST_CTX_PRESENT
    wirelog_column_type_t cols[] = { WIRELOG_TYPE_INT32 };
    const char *keys[] = { "filename", "delimiter" };
    const char *values[] = { "data.csv", "," };
    wl_io_ctx_t *ctx = create_mock_ctx("params", cols, 1, keys, values, 2);
    if (strcmp(wl_io_ctx_param(ctx, "filename"), "data.csv") != 0) {
        FAIL("filename param mismatch"); destroy_mock_ctx(ctx); return;
    }
    if (strcmp(wl_io_ctx_param(ctx, "delimiter"), ",") != 0) {
        FAIL("delimiter param mismatch"); destroy_mock_ctx(ctx); return;
    }
    if (wl_io_ctx_param(ctx, "nonexistent") != NULL) {
        FAIL("expected NULL for unknown param"); destroy_mock_ctx(ctx); return;
    }
    PASS();
    destroy_mock_ctx(ctx);
#else
    SKIP("context accessors not yet implemented (#453)");
#endif
}

static void
test_param_null_key(void)
{
    TEST("wl_io_ctx_param with NULL key returns NULL");
#ifdef TEST_CTX_PRESENT
    wirelog_column_type_t cols[] = { WIRELOG_TYPE_INT32 };
    wl_io_ctx_t *ctx = create_mock_ctx("nullkey", cols, 1, NULL, NULL, 0);
    if (wl_io_ctx_param(ctx, NULL) != NULL) {
        FAIL("expected NULL for NULL key"); destroy_mock_ctx(ctx); return;
    }
    PASS();
    destroy_mock_ctx(ctx);
#else
    SKIP("context accessors not yet implemented (#453)");
#endif
}

static void
test_intern_string(void)
{
    TEST("wl_io_ctx_intern_string returns consistent IDs");
#ifdef TEST_CTX_PRESENT
    wirelog_column_type_t cols[] = { WIRELOG_TYPE_STRING };
    wl_io_ctx_t *ctx = create_mock_ctx("intern", cols, 1, NULL, NULL, 0);
    int64_t id1 = wl_io_ctx_intern_string(ctx, "hello");
    if (id1 <= 0) {
        FAIL("first intern returned non-positive ID"); destroy_mock_ctx(ctx);
        return;
    }
    int64_t id2 = wl_io_ctx_intern_string(ctx, "hello");
    if (id2 != id1) {
        FAIL("second intern returned different ID"); destroy_mock_ctx(ctx);
        return;
    }
    PASS();
    destroy_mock_ctx(ctx);
#else
    SKIP("context accessors not yet implemented (#453)");
#endif
}

static void
test_platform_ctx(void)
{
    TEST("wl_io_ctx_platform get/set round-trip");
#ifdef TEST_CTX_PRESENT
    wirelog_column_type_t cols[] = { WIRELOG_TYPE_INT32 };
    wl_io_ctx_t *ctx = create_mock_ctx("plat", cols, 1, NULL, NULL, 0);
    if (wl_io_ctx_platform(ctx) != NULL) {
        FAIL("expected NULL initial platform"); destroy_mock_ctx(ctx); return;
    }
    wl_io_ctx_set_platform(ctx, (void *)0xCAFE);
    if (wl_io_ctx_platform(ctx) != (void *)0xCAFE) {
        FAIL("platform pointer mismatch"); destroy_mock_ctx(ctx); return;
    }
    PASS();
    destroy_mock_ctx(ctx);
#else
    SKIP("context accessors not yet implemented (#453)");
#endif
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int main(void) {
    printf("=== test_io_ctx ===\n");
    test_relation_name();
    test_num_cols();
    test_col_type();
    test_param_lookup();
    test_param_null_key();
    test_intern_string();
    test_platform_ctx();
    printf("=== Results: %d passed, %d failed, %d skipped ===\n",
        passed, failed, skipped);
    if (g_intern) {
        wl_intern_free(g_intern);
        g_intern = NULL;
    }
    return failed > 0 ? 1 : 0;
}
