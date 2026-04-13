/*
 * test_io_adapter_asan.c - Mock adapter error-path tests for ASan
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Exercises error and boundary paths in the adapter registry.
 * Designed to surface memory errors under ASan (Issue #459):
 *   - NULL inputs to all three public API functions
 *   - Scheme at SCHEME_MAX_LEN-1 boundary (max safe length)
 *   - Scheme one byte over SCHEME_MAX_LEN (must be truncated safely)
 *   - Fill registry to capacity then verify "registry full" error
 *   - wl_io_last_error() valid after each error path
 *
 * Part of #459 (ASan + TSan CI gates).
 */

#include "wirelog/io/io_adapter.h"

#include <stdio.h>
#include <string.h>

/* ======================================================================== */
/* Test Harness                                                              */
/* ======================================================================== */

#define TEST(name) do { printf("  [TEST] %-60s ", name); } while (0)
#define PASS()     do { printf("PASS\n"); passed++; } while (0)
#define FAIL(msg)  do { printf("FAIL: %s\n", msg); failed++; } while (0)

static int passed = 0, failed = 0;

/* ======================================================================== */
/* Helpers                                                                  */
/* ======================================================================== */

/* Scheme that is exactly SCHEME_MAX_LEN-1 characters (63 chars + NUL). */
#define SCHEME_MAX_LEN 64
static char s_long_scheme[SCHEME_MAX_LEN];   /* 63 'a' chars + NUL */

/* Scheme one byte longer than allowed: 64 'b' chars + NUL. The registry
 * must truncate or reject it without writing past its internal buffer. */
static char s_overlong_scheme[SCHEME_MAX_LEN + 2];  /* 64 'b' + NUL */

static void
build_boundary_schemes(void)
{
    memset(s_long_scheme, 'a', SCHEME_MAX_LEN - 1);
    s_long_scheme[SCHEME_MAX_LEN - 1] = '\0';

    memset(s_overlong_scheme, 'b', SCHEME_MAX_LEN);
    s_overlong_scheme[SCHEME_MAX_LEN] = '\0';
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static void
test_null_adapter(void)
{
    TEST("register NULL adapter returns -1");
    int rc = wl_io_register_adapter(NULL);
    if (rc != -1) {
        FAIL("expected -1"); return;
    }
    const char *err = wl_io_last_error();
    if (err && err[0] != '\0') PASS();
    else FAIL("expected non-empty error string after NULL adapter");
}

static void
test_null_scheme_register(void)
{
    TEST("register adapter with NULL scheme returns -1");
    wl_io_adapter_t a;
    memset(&a, 0, sizeof(a));
    a.scheme = NULL;
    a.abi_version = WL_IO_ABI_VERSION;
    int rc = wl_io_register_adapter(&a);
    if (rc != -1) {
        FAIL("expected -1"); return;
    }
    const char *err = wl_io_last_error();
    if (err && err[0] != '\0') PASS();
    else FAIL("expected non-empty error string");
}

static void
test_null_find(void)
{
    TEST("find NULL scheme returns NULL");
    const wl_io_adapter_t *found = wl_io_find_adapter(NULL);
    if (found == NULL) PASS();
    else FAIL("expected NULL for NULL scheme");
}

static void
test_null_unregister(void)
{
    TEST("unregister NULL scheme returns -1");
    int rc = wl_io_unregister_adapter(NULL);
    if (rc != -1) {
        FAIL("expected -1"); return;
    }
    const char *err = wl_io_last_error();
    if (err && err[0] != '\0') PASS();
    else FAIL("expected non-empty error string");
}

static void
test_abi_mismatch(void)
{
    TEST("register adapter with wrong ABI version returns -1");
    wl_io_adapter_t a;
    memset(&a, 0, sizeof(a));
    a.scheme = "mock_bad_abi2";
    a.abi_version = WL_IO_ABI_VERSION + 99u;
    int rc = wl_io_register_adapter(&a);
    if (rc == -1) PASS();
    else FAIL("expected -1 for ABI mismatch");
}

static void
test_boundary_scheme_length(void)
{
    TEST("register/find/unregister scheme at max length boundary");
    wl_io_adapter_t a;
    memset(&a, 0, sizeof(a));
    a.scheme = s_long_scheme;        /* 63 chars */
    a.abi_version = WL_IO_ABI_VERSION;

    int rc = wl_io_register_adapter(&a);
    if (rc != 0) {
        FAIL("register failed for max-length scheme"); return;
    }

    const wl_io_adapter_t *found = wl_io_find_adapter(s_long_scheme);
    if (found == NULL) {
        FAIL("find failed for max-length scheme"); return;
    }

    rc = wl_io_unregister_adapter(s_long_scheme);
    if (rc == 0) PASS();
    else FAIL("unregister failed for max-length scheme");
}

static void
test_overlong_scheme_safe(void)
{
    TEST("register scheme over max length is safe (no buffer overrun)");
    /* s_overlong_scheme is 64 'b' chars + NUL.  The registry copies with
     * strncpy(dst, src, SCHEME_MAX_LEN-1) so it will be truncated to 63 'b'
     * chars.  We just verify the call does not crash (ASan would detect any
     * out-of-bounds write). */
    wl_io_adapter_t a;
    memset(&a, 0, sizeof(a));
    a.scheme = s_overlong_scheme;
    a.abi_version = WL_IO_ABI_VERSION;

    /* May succeed or fail (truncated scheme might collide or not), but must
     * not crash or write out of bounds. */
    int rc = wl_io_register_adapter(&a);
    if (rc == 0) {
        /* Clean up: look up by first SCHEME_MAX_LEN-1 chars */
        char truncated[SCHEME_MAX_LEN];
        memset(truncated, 'b', SCHEME_MAX_LEN - 1);
        truncated[SCHEME_MAX_LEN - 1] = '\0';
        wl_io_unregister_adapter(truncated);
    }
    /* If rc == -1, that's also fine (duplicate or other error). */
    PASS();
}

static void
test_registry_full(void)
{
    TEST("register past WL_IO_MAX_ADAPTERS returns -1 with error");

    /* csv is already registered (slot 0).  Register adapters to fill the
     * remaining 31 slots, then attempt one more. */
    wl_io_adapter_t adapters[WL_IO_MAX_ADAPTERS];
    char schemes[WL_IO_MAX_ADAPTERS][16];
    int registered = 0;

    for (int i = 0; i < WL_IO_MAX_ADAPTERS - 1; i++) {
        snprintf(schemes[i], sizeof(schemes[i]), "fill_%d", i);
        memset(&adapters[i], 0, sizeof(adapters[i]));
        adapters[i].scheme = schemes[i];
        adapters[i].abi_version = WL_IO_ABI_VERSION;
        if (wl_io_register_adapter(&adapters[i]) == 0)
            registered++;
    }

    /* Try to register one more beyond capacity */
    wl_io_adapter_t overflow;
    memset(&overflow, 0, sizeof(overflow));
    overflow.scheme = "overflow_scheme";
    overflow.abi_version = WL_IO_ABI_VERSION;
    int rc = wl_io_register_adapter(&overflow);

    /* Cleanup: unregister all fill_ adapters we registered */
    for (int i = 0; i < WL_IO_MAX_ADAPTERS - 1; i++) {
        if (adapters[i].scheme)
            wl_io_unregister_adapter(schemes[i]);
    }

    if (rc == -1 && registered == WL_IO_MAX_ADAPTERS - 1) PASS();
    else if (rc == -1) {
        /* Registry may have been partially filled by prior test state */
        PASS();
    } else {
        FAIL("expected -1 when registry is full");
    }
}

static void
test_unregister_nonexistent(void)
{
    TEST("unregister non-existent scheme returns -1");
    int rc = wl_io_unregister_adapter("scheme_that_was_never_registered");
    if (rc == -1) PASS();
    else FAIL("expected -1 for unknown scheme");
}

static void
test_error_string_after_success(void)
{
    TEST("wl_io_last_error() is empty after successful operation");
    wl_io_adapter_t a;
    memset(&a, 0, sizeof(a));
    a.scheme = "mock_err_clear";
    a.abi_version = WL_IO_ABI_VERSION;

    int rc = wl_io_register_adapter(&a);
    if (rc != 0) {
        FAIL("register failed unexpectedly"); return;
    }

    const char *err = wl_io_last_error();
    int ok = (err == NULL || err[0] == '\0');

    wl_io_unregister_adapter("mock_err_clear");

    if (ok) PASS();
    else FAIL("expected empty error string after success");
}

/* ======================================================================== */
/* Main                                                                      */
/* ======================================================================== */

int
main(void)
{
    printf("=== test_io_adapter_asan (Issue #459) ===\n");

    build_boundary_schemes();

    test_null_adapter();
    test_null_scheme_register();
    test_null_find();
    test_null_unregister();
    test_abi_mismatch();
    test_boundary_scheme_length();
    test_overlong_scheme_safe();
    test_registry_full();
    test_unregister_nonexistent();
    test_error_string_after_success();

    printf("=== Results: %d passed, %d failed ===\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
