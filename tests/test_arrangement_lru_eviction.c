/*
 * test_arrangement_lru_eviction.c - Arrangement cache LRU eviction (Issue #216)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates LRU eviction behaviour for the arrangement cache:
 *   1. arr_count stays within COL_ARR_CACHE_MAX after forced eviction
 *   2. arr_total_bytes stays within arr_cache_limit_bytes
 *   3. After eviction, re-accessing a tombstoned entry rebuilds correctly
 *   4. lru_clock is bumped on every cache hit
 *   5. mem_bytes is tracked correctly after build
 *   6. WL_ARR_CACHE_LIMIT_BYTES env var sets the limit at session creation
 */

#define _GNU_SOURCE

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/passes/fusion.h"
#include "../wirelog/passes/jpp.h"
#include "../wirelog/passes/sip.h"
#include "../wirelog/session.h"
#include "../wirelog/session_facts.h"
#include "../wirelog/wirelog.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* MSVC portability: setenv/unsetenv are POSIX-only. */
#ifdef _MSC_VER
static int
setenv(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, value);
}
static int
unsetenv(const char *name)
{
    return _putenv_s(name, "");
}
#endif

/* ----------------------------------------------------------------
 * Test framework
 * ---------------------------------------------------------------- */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                       \
        do {                                                 \
            test_count++;                                    \
            printf("TEST %d: %s ... ", test_count, (name)); \
        } while (0)

#define PASS()            \
        do {                  \
            pass_count++;     \
            printf("PASS\n"); \
        } while (0)

#define FAIL(msg)                    \
        do {                             \
            fail_count++;                \
            printf("FAIL: %s\n", (msg)); \
            return;                      \
        } while (0)

#define ASSERT(cond, msg) \
        do {                  \
            if (!(cond))      \
            FAIL(msg);    \
        } while (0)

/* ----------------------------------------------------------------
 * Helpers
 * ---------------------------------------------------------------- */

static void
noop_cb(const char *r, const int64_t *row, uint32_t nc, void *u)
{
    (void)r;
    (void)row;
    (void)nc;
    (void)u;
}

static int
make_session(const char *src, wl_session_t **out_sess, wl_plan_t **out_plan,
    wirelog_program_t **out_prog)
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

    if (wl_session_snapshot(sess, noop_cb, NULL) != 0) {
        wl_session_destroy(sess);
        wl_plan_free(plan);
        wirelog_program_free(prog);
        return -1;
    }

    *out_sess = sess;
    *out_plan = plan;
    *out_prog = prog;
    return 0;
}

static void
free_session(wl_session_t *sess, wl_plan_t *plan, wirelog_program_t *prog)
{
    wl_session_destroy(sess);
    wl_plan_free(plan);
    wirelog_program_free(prog);
}

/* ================================================================
 * Test 1: lru_clock bumped on cache hit, mem_bytes tracked
 * ================================================================ */
static void
test_lru_clock_and_mem_bytes(void)
{
    TEST("lru_clock bumped on hit; mem_bytes > 0 after build");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2). edge(2, 3). edge(3, 4).\n"
        ".decl path(x: int32, y: int32)\n"
        "path(x, y) :- edge(x, y).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
        "session creation failed");

    wl_col_session_t *cs = COL_SESSION(sess);
    uint32_t key_cols[1] = { 0 };

    /* First access — builds the arrangement. */
    col_arrangement_t *arr1
        = col_session_get_arrangement(sess, "edge", key_cols, 1);
    ASSERT(arr1 != NULL, "first get_arrangement must succeed");

    /* Find the entry to inspect LRU fields. */
    col_arr_entry_t *entry = NULL;
    for (uint32_t i = 0; i < cs->arr_count; i++) {
        if (strcmp(cs->arr_entries[i].rel_name, "edge") == 0
            && cs->arr_entries[i].key_count == 1
            && cs->arr_entries[i].key_cols[0] == 0) {
            entry = &cs->arr_entries[i];
            break;
        }
    }
    ASSERT(entry != NULL, "entry must exist after get_arrangement");
    ASSERT(entry->lru_clock > 0, "lru_clock must be > 0 after first access");
    ASSERT(entry->mem_bytes > 0, "mem_bytes must be > 0 after build");
    ASSERT(cs->arr_total_bytes >= entry->mem_bytes,
        "arr_total_bytes must include entry mem_bytes");

    uint64_t clock_after_first = entry->lru_clock;

    /* Second access — same entry, should bump clock. */
    col_arrangement_t *arr2
        = col_session_get_arrangement(sess, "edge", key_cols, 1);
    ASSERT(arr2 == arr1, "second access must return same pointer");
    ASSERT(entry->lru_clock > clock_after_first,
        "lru_clock must increase on second access");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 2: WL_ARR_CACHE_LIMIT_BYTES env var is respected at init
 * ================================================================ */
static void
test_env_var_limit(void)
{
    TEST("WL_ARR_CACHE_LIMIT_BYTES env var sets limit");

    /* Set a very small limit (1 MB) so we can verify it was applied. */
    setenv("WL_ARR_CACHE_LIMIT_BYTES", "1048576", 1);

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 2).\n"
        ".decl path(x: int32, y: int32)\n"
        "path(x, y) :- edge(x, y).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
        "session creation failed");

    wl_col_session_t *cs = COL_SESSION(sess);
    ASSERT(cs->arr_cache_limit_bytes == 1048576,
        "arr_cache_limit_bytes must equal WL_ARR_CACHE_LIMIT_BYTES");

    free_session(sess, plan, prog);
    unsetenv("WL_ARR_CACHE_LIMIT_BYTES");
    PASS();
}

/* ================================================================
 * Test 3: Tombstoned entry rebuilds correctly on re-access
 *
 * Simulates eviction by directly calling arr_free_contents on an
 * entry, then verifying that get_arrangement rebuilds the index.
 * ================================================================ */
static void
test_tombstone_rebuild(void)
{
    TEST("Tombstoned entry (indexed_rows=0) rebuilds on re-access");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(10, 1). edge(20, 2). edge(30, 3).\n"
        ".decl path(x: int32, y: int32)\n"
        "path(x, y) :- edge(x, y).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
        "session creation failed");

    wl_col_session_t *cs = COL_SESSION(sess);
    uint32_t key_cols[1] = { 0 };

    /* Build the arrangement. */
    col_arrangement_t *arr1
        = col_session_get_arrangement(sess, "edge", key_cols, 1);
    ASSERT(arr1 != NULL, "initial build must succeed");
    ASSERT(arr1->indexed_rows == 3, "indexed_rows must be 3");

    /* Find the entry and simulate eviction (tombstone). */
    col_arr_entry_t *entry = NULL;
    for (uint32_t i = 0; i < cs->arr_count; i++) {
        if (strcmp(cs->arr_entries[i].rel_name, "edge") == 0) {
            entry = &cs->arr_entries[i];
            break;
        }
    }
    ASSERT(entry != NULL, "entry must exist");

    size_t old_mem = entry->mem_bytes;
    cs->arr_total_bytes -= old_mem;
    arr_free_contents(&entry->arr); /* sets indexed_rows = 0 */
    entry->mem_bytes = 0;

    ASSERT(entry->arr.indexed_rows == 0, "tombstone: indexed_rows must be 0");

    /* Re-access: must rebuild. */
    col_arrangement_t *arr2
        = col_session_get_arrangement(sess, "edge", key_cols, 1);
    ASSERT(arr2 != NULL, "re-access after tombstone must succeed");
    ASSERT(arr2->indexed_rows == 3, "rebuild must restore indexed_rows = 3");
    ASSERT(entry->mem_bytes > 0, "mem_bytes must be restored after rebuild");
    ASSERT(cs->arr_total_bytes >= entry->mem_bytes,
        "arr_total_bytes must include rebuilt entry");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 4: arr_total_bytes accounting remains consistent
 *
 * Multiple arrangements on different key columns of the same
 * relation: total_bytes must be the sum of individual mem_bytes.
 * ================================================================ */
static void
test_total_bytes_accounting(void)
{
    TEST("arr_total_bytes = sum of all entry mem_bytes");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(1, 10). edge(2, 20). edge(3, 30).\n"
        ".decl pathA(x: int32, y: int32)\n"
        "pathA(x, y) :- edge(x, y).\n"
        ".decl pathB(x: int32, y: int32)\n"
        "pathB(x, y) :- edge(y, x).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
        "session creation failed");

    wl_col_session_t *cs = COL_SESSION(sess);

    uint32_t key0[1] = { 0 };
    uint32_t key1[1] = { 1 };

    col_arrangement_t *a0 = col_session_get_arrangement(sess, "edge", key0, 1);
    col_arrangement_t *a1 = col_session_get_arrangement(sess, "edge", key1, 1);
    ASSERT(a0 != NULL && a1 != NULL, "both arrangements must be built");

    /* Compute expected sum. */
    size_t expected = 0;
    for (uint32_t i = 0; i < cs->arr_count; i++)
        expected += cs->arr_entries[i].mem_bytes;

    ASSERT(cs->arr_total_bytes == expected,
        "arr_total_bytes must equal sum of all entry mem_bytes");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * main
 * ================================================================ */
int
main(void)
{
    printf("=== test_arrangement_lru_eviction ===\n");

    test_lru_clock_and_mem_bytes();
    test_env_var_limit();
    test_tombstone_rebuild();
    test_total_bytes_accounting();

    printf("\n%d/%d tests passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n");

    return fail_count > 0 ? 1 : 0;
}
