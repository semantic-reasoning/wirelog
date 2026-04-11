/*
 * test_io_adapter.c - I/O adapter registry test scaffold (TDD gate)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Six test scenarios for the I/O adapter registry. All guarded behind
 * TEST_REGISTRY_PRESENT and currently SKIP until #451 implements the
 * registry.
 *
 * Part of #446 (I/O adapter umbrella).
 */

#include <stdio.h>
#include <string.h>

/* The io_adapter.h header exists (placeholder from #449) but the
 * actual API functions are not yet implemented (#451). Guard all
 * test bodies behind TEST_REGISTRY_PRESENT so they compile as
 * SKIP until the implementation lands. */
#include "wirelog/io/io_adapter.h"

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
test_register_and_find(void)
{
    TEST("register_and_find");
#ifdef TEST_REGISTRY_PRESENT
    wl_io_adapter_t adapter;
    memset(&adapter, 0, sizeof(adapter));
    adapter.scheme = "mock";
    adapter.abi_version = WL_IO_ABI_VERSION;

    int rc = wl_io_register_adapter(&adapter);
    if (rc != 0) {
        FAIL("register returned non-zero"); return;
    }

    const wl_io_adapter_t *found = wl_io_find_adapter("mock");
    if (found == &adapter) PASS();
    else FAIL("found pointer does not match registered adapter");
#else
    SKIP("registry not yet implemented (#451)");
#endif
}

static void
test_find_unknown_scheme(void)
{
    TEST("find_unknown_scheme");
#ifdef TEST_REGISTRY_PRESENT
    const wl_io_adapter_t *found = wl_io_find_adapter("nonexistent");
    if (found == NULL) PASS();
    else FAIL("expected NULL for unknown scheme");
#else
    SKIP("registry not yet implemented (#451)");
#endif
}

static void
test_duplicate_register_error(void)
{
    TEST("duplicate_register_error");
#ifdef TEST_REGISTRY_PRESENT
    wl_io_adapter_t adapter;
    memset(&adapter, 0, sizeof(adapter));
    adapter.scheme = "mock_dup";
    adapter.abi_version = WL_IO_ABI_VERSION;

    int rc1 = wl_io_register_adapter(&adapter);
    if (rc1 != 0) {
        FAIL("first register failed"); return;
    }

    int rc2 = wl_io_register_adapter(&adapter);
    if (rc2 == -1) PASS();
    else FAIL("expected -1 for duplicate registration");
#else
    SKIP("registry not yet implemented (#451)");
#endif
}

static void
test_builtin_csv_autoload(void)
{
    TEST("builtin_csv_autoload");
#ifdef TEST_REGISTRY_PRESENT
    const wl_io_adapter_t *found = wl_io_find_adapter("csv");
    if (found != NULL) PASS();
    else FAIL("expected non-NULL for built-in csv adapter");
#else
    SKIP("registry not yet implemented (#451)");
#endif
}

static void
test_unregister_then_find(void)
{
    TEST("unregister_then_find");
#ifdef TEST_REGISTRY_PRESENT
    wl_io_adapter_t adapter;
    memset(&adapter, 0, sizeof(adapter));
    adapter.scheme = "mock_unreg";
    adapter.abi_version = WL_IO_ABI_VERSION;

    int rc = wl_io_register_adapter(&adapter);
    if (rc != 0) {
        FAIL("register failed"); return;
    }

    rc = wl_io_unregister_adapter("mock_unreg");
    if (rc != 0) {
        FAIL("unregister failed"); return;
    }

    const wl_io_adapter_t *found = wl_io_find_adapter("mock_unreg");
    if (found != NULL) {
        FAIL("expected NULL after unregister"); return;
    }

    rc = wl_io_register_adapter(&adapter);
    if (rc == 0) PASS();
    else FAIL("re-register after unregister failed");
#else
    SKIP("registry not yet implemented (#451)");
#endif
}

static void
test_abi_version_mismatch(void)
{
    TEST("abi_version_mismatch");
#ifdef TEST_REGISTRY_PRESENT
    wl_io_adapter_t adapter;
    memset(&adapter, 0, sizeof(adapter));
    adapter.scheme = "mock_bad_abi";
    adapter.abi_version = 999;

    int rc = wl_io_register_adapter(&adapter);
    if (rc == -1) PASS();
    else FAIL("expected -1 for ABI version mismatch");
#else
    SKIP("registry not yet implemented (#451)");
#endif
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int main(void) {
    printf("=== test_io_adapter ===\n");
    test_register_and_find();
    test_find_unknown_scheme();
    test_duplicate_register_error();
    test_builtin_csv_autoload();
    test_unregister_then_find();
    test_abi_version_mismatch();
    printf("=== Results: %d passed, %d failed, %d skipped ===\n",
        passed, failed, skipped);
    return failed > 0 ? 1 : 0;
}
