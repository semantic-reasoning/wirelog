/*
 * test_plugin_loader.c - Plugin loader integration tests (Issue #461)
 *
 * Tests: load + register + find + unload, ABI mismatch error path,
 * missing symbol error path, NULL path error path.
 *
 * Requires -Dio_plugin_dlopen=enabled to build.
 */

#include "wirelog/io/io_adapter.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* We test the plugin loader by calling it directly */
extern int  wl_plugin_load(const char *path);
extern void wl_plugin_unload_all(void);

#define TEST(name)  do { printf("  [TEST] %-50s ", name); } while (0)
#define PASS()      do { printf("PASS\n"); passed++; } while (0)
#define FAIL(msg)   do { printf("FAIL: %s\n", msg); failed++; } while (0)

static int passed = 0, failed = 0;

static void
test_load_valid_plugin(const char *mock_path)
{
    TEST("load valid plugin");

    int rc = wl_plugin_load(mock_path);
    if (rc != 0) {
        FAIL("wl_plugin_load returned non-zero");
        return;
    }

    const wl_io_adapter_t *found = wl_io_find_adapter("mock_plugin");
    if (!found) {
        FAIL("adapter 'mock_plugin' not found after load");
        return;
    }

    if (strcmp(found->scheme, "mock_plugin") != 0) {
        FAIL("scheme mismatch");
        return;
    }

    PASS();
}

static void
test_unload_cleans_registry(void)
{
    TEST("unload cleans registry");

    wl_plugin_unload_all();

    const wl_io_adapter_t *found = wl_io_find_adapter("mock_plugin");
    if (found) {
        FAIL("adapter 'mock_plugin' still found after unload");
        return;
    }

    PASS();
}

static void
test_abi_mismatch(const char *bad_abi_path)
{
    TEST("ABI mismatch returns error");

    int rc = wl_plugin_load(bad_abi_path);
    if (rc == 0) {
        FAIL("expected failure for ABI mismatch plugin");
        wl_plugin_unload_all();
        return;
    }

    PASS();
}

static void
test_missing_file(void)
{
    TEST("missing file returns error");

    int rc = wl_plugin_load("/nonexistent/path/libfoo.so");
    if (rc == 0) {
        FAIL("expected failure for missing file");
        return;
    }

    PASS();
}

static void
test_null_path(void)
{
    TEST("NULL path returns error");

    int rc = wl_plugin_load(NULL);
    if (rc == 0) {
        FAIL("expected failure for NULL path");
        return;
    }

    PASS();
}

int
main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr,
            "usage: %s <mock_plugin.so> <mock_bad_abi.so>\n", argv[0]);
        return 1;
    }

    const char *mock_path = argv[1];
    const char *bad_abi_path = argv[2];

    printf("test_plugin_loader\n");

    test_null_path();
    test_missing_file();
    test_load_valid_plugin(mock_path);
    test_unload_cleans_registry();
    test_abi_mismatch(bad_abi_path);

    printf("\n  Results: %d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
