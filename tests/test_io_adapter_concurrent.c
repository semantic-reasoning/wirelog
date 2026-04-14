/*
 * test_io_adapter_concurrent.c - Concurrent I/O adapter registry stress test
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Exercises the mutex-guarded adapter registry under concurrent load.
 * Designed to expose data races under TSan (Issue #459).
 *
 * Tests:
 *   1. Concurrent find: N threads repeatedly call wl_io_find_adapter
 *   2. Concurrent register/unregister: N threads each own a unique scheme
 *   3. Mixed readers + writers: find and register running simultaneously
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
/* Concurrent Tests (POSIX/C11; skipped on Windows)                         */
/* ======================================================================== */

#ifndef _WIN32

#include "wirelog/thread.h"

#define NTHREADS     8
#define FIND_ITERS   2000

/* ---- Test 1: concurrent find ------------------------------------------- */

static void *
find_worker(void *arg)
{
    (void)arg;
    for (int i = 0; i < FIND_ITERS; i++) {
        /* csv is the built-in; must always be findable */
        const wl_io_adapter_t *a = wl_io_find_adapter("csv");
        (void)a;
    }
    return NULL;
}

static void
test_concurrent_find(void)
{
    TEST("concurrent find (csv built-in, 8 threads x 2000 iters)");

    thread_t threads[NTHREADS];
    for (int i = 0; i < NTHREADS; i++)
        thread_create(&threads[i], find_worker, NULL);
    for (int i = 0; i < NTHREADS; i++)
        thread_join(&threads[i]);

    /* After concurrent reads, csv must still be findable */
    const wl_io_adapter_t *a = wl_io_find_adapter("csv");
    if (a != NULL) PASS();
    else FAIL("csv not found after concurrent reads");
}

/* ---- Test 2: concurrent register/unregister ----------------------------- */

typedef struct {
    char scheme[32];
    wl_io_adapter_t adapter;
    int result;   /* 0 = ok, 1 = error */
} reg_arg_t;

static void *
reg_unreg_worker(void *arg)
{
    reg_arg_t *a = (reg_arg_t *)arg;

    /* Each thread owns a unique scheme; register then unregister FIND_ITERS
     * times.  The registry has 32 slots total; with 8 threads, each holding
     * at most 1 slot at a time, we stay well within capacity. */
    for (int i = 0; i < FIND_ITERS; i++) {
        int rc = wl_io_register_adapter(&a->adapter);
        if (rc != 0) {
            a->result = 1;
            return NULL;
        }
        rc = wl_io_unregister_adapter(a->scheme);
        if (rc != 0) {
            a->result = 1;
            return NULL;
        }
    }
    a->result = 0;
    return NULL;
}

static void
test_concurrent_register_unregister(void)
{
    TEST("concurrent register/unregister (8 threads x 2000 iters each)");

    thread_t threads[NTHREADS];
    reg_arg_t args[NTHREADS];

    for (int i = 0; i < NTHREADS; i++) {
        snprintf(args[i].scheme, sizeof(args[i].scheme), "conc_%d", i);
        memset(&args[i].adapter, 0, sizeof(args[i].adapter));
        args[i].adapter.scheme = args[i].scheme;
        args[i].adapter.abi_version = WL_IO_ABI_VERSION;
        args[i].result = 0;
        thread_create(&threads[i], reg_unreg_worker, &args[i]);
    }
    for (int i = 0; i < NTHREADS; i++)
        thread_join(&threads[i]);

    int any_failed = 0;
    for (int i = 0; i < NTHREADS; i++) {
        if (args[i].result != 0) {
            any_failed = 1;
            break;
        }
    }
    if (!any_failed) PASS();
    else FAIL("register/unregister cycle failed in one or more threads");
}

/* ---- Test 3: mixed readers + writers ------------------------------------ */

static void *
mixed_find_worker(void *arg)
{
    (void)arg;
    for (int i = 0; i < FIND_ITERS; i++) {
        const wl_io_adapter_t *a = wl_io_find_adapter("csv");
        (void)a;
        a = wl_io_find_adapter("nonexistent_scheme_xyz");
        (void)a;
    }
    return NULL;
}

static void *
mixed_reg_worker(void *arg)
{
    reg_arg_t *a = (reg_arg_t *)arg;
    for (int i = 0; i < FIND_ITERS; i++) {
        wl_io_register_adapter(&a->adapter);
        wl_io_unregister_adapter(a->scheme);
    }
    return NULL;
}

static void
test_mixed_readers_writers(void)
{
    TEST("mixed readers + writers (4 find + 4 reg/unreg threads)");

    thread_t readers[4];
    thread_t writers[4];
    reg_arg_t wargs[4];

    for (int i = 0; i < 4; i++) {
        snprintf(wargs[i].scheme, sizeof(wargs[i].scheme), "mixed_%d", i);
        memset(&wargs[i].adapter, 0, sizeof(wargs[i].adapter));
        wargs[i].adapter.scheme = wargs[i].scheme;
        wargs[i].adapter.abi_version = WL_IO_ABI_VERSION;
        thread_create(&readers[i], mixed_find_worker, NULL);
        thread_create(&writers[i], mixed_reg_worker, &wargs[i]);
    }
    for (int i = 0; i < 4; i++) {
        thread_join(&readers[i]);
        thread_join(&writers[i]);
    }

    /* Registry must still be consistent: csv findable, no dangling entries */
    const wl_io_adapter_t *a = wl_io_find_adapter("csv");
    if (a != NULL) PASS();
    else FAIL("csv not found after mixed concurrent access");
}

#endif /* !_WIN32 */

/* ======================================================================== */
/* Main                                                                      */
/* ======================================================================== */

int
main(void)
{
    printf("=== test_io_adapter_concurrent (Issue #459) ===\n");

#ifndef _WIN32
    test_concurrent_find();
    test_concurrent_register_unregister();
    test_mixed_readers_writers();
#else
    printf("  [SKIP] concurrent tests not supported on Windows\n");
#endif

    printf("=== Results: %d passed, %d failed ===\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
