/*
 * test_intern.c - Symbol intern table unit tests
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Tests the symbol intern table: insert, dedup, reverse lookup, capacity.
 */

#include "../wirelog/intern.h"

#include "../wirelog/thread.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test Helpers                                                             */
/* ======================================================================== */

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name)                            \
        do {                                      \
            tests_run++;                          \
            printf("  [%d] %s", tests_run, name); \
        } while (0)

#define PASS()                 \
        do {                       \
            tests_passed++;        \
            printf(" ... PASS\n"); \
        } while (0)

#define FAIL(msg)                         \
        do {                                  \
            tests_failed++;                   \
            printf(" ... FAIL: %s\n", (msg)); \
        } while (0)

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static void
test_create_and_free(void)
{
    TEST("create and free intern table");

    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("wl_intern_create returned NULL");
        return;
    }

    if (wl_intern_count(intern) != 0) {
        FAIL("new table should have count 0");
        wl_intern_free(intern);
        return;
    }

    wl_intern_free(intern);
    PASS();
}

static void
test_put_returns_sequential_ids(void)
{
    TEST("put returns sequential IDs");

    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed");
        return;
    }

    int64_t id0 = wl_intern_put(intern, "hello");
    int64_t id1 = wl_intern_put(intern, "world");
    int64_t id2 = wl_intern_put(intern, "foo");

    if (id0 != 0 || id1 != 1 || id2 != 2) {
        FAIL("expected sequential IDs 0, 1, 2");
        wl_intern_free(intern);
        return;
    }

    if (wl_intern_count(intern) != 3) {
        FAIL("count should be 3");
        wl_intern_free(intern);
        return;
    }

    wl_intern_free(intern);
    PASS();
}

static void
test_put_dedup(void)
{
    TEST("put same string returns same ID (dedup)");

    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed");
        return;
    }

    int64_t id_a1 = wl_intern_put(intern, "Alice");
    int64_t id_b = wl_intern_put(intern, "Bob");
    int64_t id_a2 = wl_intern_put(intern, "Alice");

    if (id_a1 != id_a2) {
        FAIL("same string should return same ID");
        wl_intern_free(intern);
        return;
    }

    if (id_a1 == id_b) {
        FAIL("different strings should return different IDs");
        wl_intern_free(intern);
        return;
    }

    if (wl_intern_count(intern) != 2) {
        FAIL("count should be 2 (dedup)");
        wl_intern_free(intern);
        return;
    }

    wl_intern_free(intern);
    PASS();
}

static void
test_get_existing(void)
{
    TEST("get returns ID for existing string");

    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed");
        return;
    }

    int64_t put_id = wl_intern_put(intern, "test");
    int64_t get_id = wl_intern_get(intern, "test");

    if (put_id != get_id) {
        FAIL("get should return same ID as put");
        wl_intern_free(intern);
        return;
    }

    wl_intern_free(intern);
    PASS();
}

static void
test_get_not_found(void)
{
    TEST("get returns -1 for unknown string");

    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed");
        return;
    }

    int64_t id = wl_intern_get(intern, "nonexistent");
    if (id != -1) {
        FAIL("get on unknown string should return -1");
        wl_intern_free(intern);
        return;
    }

    wl_intern_free(intern);
    PASS();
}

static void
test_reverse_lookup(void)
{
    TEST("reverse lookup by ID");

    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed");
        return;
    }

    wl_intern_put(intern, "alpha");
    wl_intern_put(intern, "beta");
    wl_intern_put(intern, "gamma");

    const char *s0 = wl_intern_reverse(intern, 0);
    const char *s1 = wl_intern_reverse(intern, 1);
    const char *s2 = wl_intern_reverse(intern, 2);
    const char *s3 = wl_intern_reverse(intern, 3);
    const char *sm = wl_intern_reverse(intern, -1);

    if (!s0 || strcmp(s0, "alpha") != 0) {
        FAIL("reverse(0) should be 'alpha'");
        wl_intern_free(intern);
        return;
    }
    if (!s1 || strcmp(s1, "beta") != 0) {
        FAIL("reverse(1) should be 'beta'");
        wl_intern_free(intern);
        return;
    }
    if (!s2 || strcmp(s2, "gamma") != 0) {
        FAIL("reverse(2) should be 'gamma'");
        wl_intern_free(intern);
        return;
    }
    if (s3 != NULL) {
        FAIL("reverse(3) should be NULL (out of range)");
        wl_intern_free(intern);
        return;
    }
    if (sm != NULL) {
        FAIL("reverse(-1) should be NULL (negative ID)");
        wl_intern_free(intern);
        return;
    }

    wl_intern_free(intern);
    PASS();
}

static void
test_null_and_empty(void)
{
    TEST("NULL and empty string handling");

    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed");
        return;
    }

    /* NULL input should return -1 */
    int64_t id_null = wl_intern_put(intern, NULL);
    if (id_null != -1) {
        FAIL("put(NULL) should return -1");
        wl_intern_free(intern);
        return;
    }

    /* Empty string is a valid string */
    int64_t id_empty = wl_intern_put(intern, "");
    if (id_empty < 0) {
        FAIL("put(\"\") should succeed");
        wl_intern_free(intern);
        return;
    }

    const char *rev = wl_intern_reverse(intern, id_empty);
    if (!rev || strcmp(rev, "") != 0) {
        FAIL("reverse of empty string should be \"\"");
        wl_intern_free(intern);
        return;
    }

    /* NULL get should return -1 */
    int64_t get_null = wl_intern_get(intern, NULL);
    if (get_null != -1) {
        FAIL("get(NULL) should return -1");
        wl_intern_free(intern);
        return;
    }

    wl_intern_free(intern);
    PASS();
}

static void
test_many_strings(void)
{
    TEST("intern many strings (triggers resize)");

    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed");
        return;
    }

    /* Insert 200 strings to trigger hash table resize */
    char buf[32];
    for (int i = 0; i < 200; i++) {
        snprintf(buf, sizeof(buf), "str_%d", i);
        int64_t id = wl_intern_put(intern, buf);
        if (id != (int64_t)i) {
            FAIL("unexpected ID after resize");
            wl_intern_free(intern);
            return;
        }
    }

    if (wl_intern_count(intern) != 200) {
        FAIL("count should be 200");
        wl_intern_free(intern);
        return;
    }

    /* Verify all strings can be looked up */
    for (int i = 0; i < 200; i++) {
        snprintf(buf, sizeof(buf), "str_%d", i);
        int64_t id = wl_intern_get(intern, buf);
        if (id != (int64_t)i) {
            FAIL("get after resize returned wrong ID");
            wl_intern_free(intern);
            return;
        }
        const char *rev = wl_intern_reverse(intern, (int64_t)i);
        if (!rev || strcmp(rev, buf) != 0) {
            FAIL("reverse after resize returned wrong string");
            wl_intern_free(intern);
            return;
        }
    }

    wl_intern_free(intern);
    PASS();
}

/*
 * Issue #958: one intern table is shared, unsynchronized, by every
 * parallel evaluation worker.  Two threads interning the same strings
 * must agree on every id, and reverse lookups issued while the other
 * thread is still interning must return complete strings.
 *
 * Before the fix this reported a count overshoot (e.g. 2083 for 2000
 * distinct strings, because count was a non-atomic read-modify-write),
 * disagreeing ids, and -- driven by the realloc() of the id -> string
 * array under a concurrent reader -- heap corruption and SIGSEGV.
 */

#define CONCURRENT_STRINGS 2000
#define CONCURRENT_THREADS 4

typedef struct {
    wl_intern_t *intern;
    int64_t ids[CONCURRENT_STRINGS];
    int bad_reverse; /* reverse() disagreed with the string just interned */
    int bad_put;     /* put() failed outright */
} concurrent_ctx_t;

static void *
concurrent_put_worker(void *arg)
{
    concurrent_ctx_t *ctx = (concurrent_ctx_t *)arg;
    char buf[32];

    for (int i = 0; i < CONCURRENT_STRINGS; i++) {
        snprintf(buf, sizeof(buf), "sym_%d", i);
        int64_t id = wl_intern_put(ctx->intern, buf);
        ctx->ids[i] = id;
        if (id < 0) {
            ctx->bad_put++;
            continue;
        }
        /* Lock-free read racing the other thread's writes. */
        const char *rev = wl_intern_reverse(ctx->intern, id);
        if (!rev || strcmp(rev, buf) != 0)
            ctx->bad_reverse++;
    }

    return NULL;
}

static void
test_concurrent_put(void)
{
    TEST("concurrent put from two threads (Issue #958)");

    wl_intern_t *intern = wl_intern_create();
    if (!intern) {
        FAIL("create failed");
        return;
    }

    concurrent_ctx_t *ctx
        = (concurrent_ctx_t *)calloc(CONCURRENT_THREADS, sizeof(*ctx));
    if (!ctx) {
        FAIL("calloc failed");
        wl_intern_free(intern);
        return;
    }

    thread_t threads[CONCURRENT_THREADS];
    int started = 0;
    for (int t = 0; t < CONCURRENT_THREADS; t++) {
        ctx[t].intern = intern;
        if (thread_create(&threads[t], concurrent_put_worker, &ctx[t]) != 0)
            break;
        started++;
    }
    for (int t = 0; t < started; t++)
        thread_join(&threads[t]);

    if (started != CONCURRENT_THREADS) {
        FAIL("thread_create failed");
        free(ctx);
        wl_intern_free(intern);
        return;
    }

    /* Every string is interned exactly once, however the threads raced. */
    if (wl_intern_count(intern) != (uint32_t)CONCURRENT_STRINGS) {
        FAIL("count != number of distinct strings");
        free(ctx);
        wl_intern_free(intern);
        return;
    }

    for (int t = 0; t < CONCURRENT_THREADS; t++) {
        if (ctx[t].bad_put) {
            FAIL("put returned -1");
            free(ctx);
            wl_intern_free(intern);
            return;
        }
        if (ctx[t].bad_reverse) {
            FAIL("concurrent reverse returned the wrong string");
            free(ctx);
            wl_intern_free(intern);
            return;
        }
    }

    /* Both threads must have been handed the same id for each string,
     * and that id must still reverse-map correctly afterwards. */
    char buf[32];
    char *seen = (char *)calloc(CONCURRENT_STRINGS, 1);
    if (!seen) {
        FAIL("calloc failed");
        free(ctx);
        wl_intern_free(intern);
        return;
    }

    for (int i = 0; i < CONCURRENT_STRINGS; i++) {
        int64_t id = ctx[0].ids[i];

        snprintf(buf, sizeof(buf), "sym_%d", i);
        for (int t = 1; t < CONCURRENT_THREADS; t++) {
            if (ctx[t].ids[i] != id) {
                FAIL("threads disagree on the id of a string");
                free(seen);
                free(ctx);
                wl_intern_free(intern);
                return;
            }
        }
        if (id < 0 || id >= CONCURRENT_STRINGS || seen[id]) {
            FAIL("ids are not a permutation of 0..N-1");
            free(seen);
            free(ctx);
            wl_intern_free(intern);
            return;
        }
        seen[id] = 1;

        const char *rev = wl_intern_reverse(intern, id);
        if (!rev || strcmp(rev, buf) != 0) {
            FAIL("reverse returned the wrong string");
            free(seen);
            free(ctx);
            wl_intern_free(intern);
            return;
        }
        if (wl_intern_get(intern, buf) != id) {
            FAIL("get returned an id other than the one put returned");
            free(seen);
            free(ctx);
            wl_intern_free(intern);
            return;
        }
    }

    free(seen);
    free(ctx);
    wl_intern_free(intern);
    PASS();
}

static void
test_free_null(void)
{
    TEST("free(NULL) is safe");

    wl_intern_free(NULL);
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("=== Symbol Intern Table Tests ===\n");

    test_create_and_free();
    test_put_returns_sequential_ids();
    test_put_dedup();
    test_get_existing();
    test_get_not_found();
    test_reverse_lookup();
    test_null_and_empty();
    test_many_strings();
    test_concurrent_put();
    test_free_null();

    printf("\nResults: %d/%d passed, %d failed\n", tests_passed, tests_run,
        tests_failed);

    return tests_failed > 0 ? 1 : 0;
}
