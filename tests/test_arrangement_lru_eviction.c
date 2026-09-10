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
    ASSERT(entry->mem_bytes
        == (size_t)arr1->nbuckets * sizeof(uint64_t)
        + (size_t)arr1->ht_cap * sizeof(uint32_t),
        "mem_bytes must include tagged bucket heads and chain storage");
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
 * Test 5: pinned arrangements defer invalidation until the lease ends
 * ================================================================ */
static void
test_pinned_invalidation(void)
{
    TEST("pinned arrangement defers invalidation until release");

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
    col_arrangement_pin_t pin;
    col_arrangement_t *arr = col_session_get_arrangement(sess, "edge",
            key_cols, 1);
    ASSERT(arr != NULL && arr->indexed_rows == 3,
        "arrangement must be built before pinning");
    ASSERT(col_session_pin_arrangement(sess, "edge", key_cols, 1, &pin) == 0,
        "arrangement pin must succeed");

    /* Force the registry to need a realloc while the only resident entry is
     * pinned.  It must refuse the growth rather than move the lease target. */
    cs->arr_cap = cs->arr_count;
    uint32_t alternate_key[1] = { 1 };
    ASSERT(col_session_get_arrangement(sess, "edge", alternate_key, 1)
        == NULL,
        "registry growth must be deferred while an entry is pinned");
    ASSERT(pin.entry->mem_bytes > 0 && pin.entry->pin_count == 1
        && pin.entry->arr.indexed_rows == 3,
        "a pinned arrangement must remain valid while growth is deferred");

    col_session_invalidate_arrangements(sess, "edge");
    ASSERT(pin.entry != NULL && pin.entry->pin_count == 1
        && pin.entry->rebuild_deferred,
        "invalidation must be deferred while pinned");
    ASSERT(arr->indexed_rows == 3,
        "pinned arrangement must remain readable until release");

    col_arrangement_pin_release(&pin);
    ASSERT(pin.active == false, "release must deactivate the lease");
    ASSERT(arr->indexed_rows == 0,
        "deferred invalidation must take effect on final release");

    arr = col_session_get_arrangement(sess, "edge", key_cols, 1);
    ASSERT(arr != NULL && arr->indexed_rows == 3,
        "next access must rebuild the invalidated arrangement");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 6: token mismatch under a lease defers the rebuild (#1435)
 * ================================================================ */
static void
test_pinned_rebuild_deferred(void)
{
    TEST("pinned arrangement is never rebuilt in place on a stale token");

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
    col_rel_t *edge = session_find_rel(cs, "edge");
    uint32_t key_cols[1] = { 0 };
    col_arrangement_pin_t pin;
    col_arrangement_pin_t second;
    ASSERT(edge != NULL && edge->nrows == 3, "edge must hold three rows");
    ASSERT(col_session_pin_arrangement(sess, "edge", key_cols, 1, &pin) == 0,
        "arrangement pin must succeed");
    col_arr_entry_t *entry = pin.entry;
    const uint64_t *head_before = entry->arr.ht_head;
    const uint32_t *next_before = entry->arr.ht_next;
    uint32_t generation_before = entry->arr.generation;
    uint32_t nbuckets_before = entry->arr.nbuckets;
    uint32_t ht_cap_before = entry->arr.ht_cap;
    size_t mem_bytes_before = entry->mem_bytes;
    size_t total_before = cs->arr_total_bytes;
    uint64_t clock_before = entry->lru_clock;
    ASSERT(entry->arr.indexed_rows == 3, "index must cover the three rows");

    /* Same-row-count mutation: key 20 becomes 40.  The token no longer
     * matches, but the reader holds a lease, so the index must stay
     * exactly as it is and the lookup must report it unavailable. */
    ASSERT(col_rel_set(edge, 1, 0, 40) == 0, "in-place set failed");
    ASSERT(col_session_get_arrangement(sess, "edge", key_cols, 1) == NULL,
        "stale leased entry must be reported unavailable");
    ASSERT(entry->rebuild_deferred && entry->pin_count == 1,
        "rebuild must be deferred while pinned");
    ASSERT(entry->arr.ht_head == head_before &&
        entry->arr.ht_next == next_before
        && entry->arr.generation == generation_before
        && entry->arr.nbuckets == nbuckets_before
        && entry->arr.ht_cap == ht_cap_before
        && entry->arr.indexed_rows == 3
        && entry->mem_bytes == mem_bytes_before
        && cs->arr_total_bytes == total_before
        && entry->lru_clock == clock_before,
        "deferral must leave buffers, accounting and clock untouched");
    ASSERT(col_session_pin_arrangement(sess, "edge", key_cols, 1, &second)
        == EBUSY && entry->pin_count == 1 && !second.active,
        "a second lease on a stale entry must report EBUSY");

    /* Growth without a token change cannot happen through the relation
     * API on main (an append publishes a new view generation), so the
     * incremental clause is exercised white-box: refresh the token after
     * an append to simulate a consumer that refreshed it without
     * rebuilding.  indexed_rows < nrows must defer just like a mismatch. */
    int64_t row[2] = { 50, 5 };
    ASSERT(col_rel_append_row(edge, row) == 0, "append failed");
    entry->rebuild_deferred = false;
    entry->source_snapshot = wl_columnar_relation_snapshot(edge);
    ASSERT(col_session_get_arrangement(sess, "edge", key_cols, 1) == NULL
        && entry->rebuild_deferred && entry->arr.indexed_rows == 3
        && entry->arr.ht_head == head_before,
        "growth under a lease must defer the incremental update");

    col_arrangement_pin_release(&pin);
    ASSERT(!pin.active && entry->pin_count == 0
        && entry->arr.indexed_rows == 0,
        "last release must apply the deferred invalidation");
    col_arrangement_t *arr = col_session_get_arrangement(sess, "edge",
            key_cols, 1);
    ASSERT(arr != NULL && arr->indexed_rows == 4,
        "next lookup after release must rebuild over four rows");
    int64_t probe_new[2] = { 40, 0 };
    int64_t probe_old[2] = { 20, 0 };
    ASSERT(col_arrangement_find_first_typed(arr, edge, probe_new)
        != UINT32_MAX,
        "rebuilt index must find the new key");
    ASSERT(col_arrangement_find_first_typed(arr, edge, probe_old)
        == UINT32_MAX,
        "rebuilt index must not find the replaced key");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 7: a pinned, token-fresh, empty index is still handed out
 * ================================================================ */
static void
test_pinned_empty_index_available(void)
{
    TEST("pinned empty arrangement stays available to a second lease");

    const char *src = ".decl edge(x: int32, y: int32)\n"
        ".decl seed(x: int32, y: int32)\n"
        "seed(1, 2).\n"
        ".decl path(x: int32, y: int32)\n"
        "path(x, y) :- seed(x, z), edge(z, y).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
        "session creation failed");

    wl_col_session_t *cs = COL_SESSION(sess);
    col_rel_t *edge = session_find_rel(cs, "edge");
    uint32_t key_cols[1] = { 0 };
    col_arrangement_pin_t first;
    col_arrangement_pin_t second;
    ASSERT(edge != NULL && edge->nrows == 0, "edge must be empty");
    /* A session relation gets its column layout at the first insert; give
     * the empty relation its schema so the lookup validates the key column
     * and indexes zero rows, which is the shape a bounded join sees for an
     * empty right relation. */
    ASSERT(col_rel_set_schema(edge, 2, NULL) == 0, "schema failed");
    ASSERT(col_session_pin_arrangement(sess, "edge", key_cols, 1, &first) == 0
        && first.entry->arr.indexed_rows == 0,
        "an empty relation must still get a lease");
    const uint64_t *empty_head = first.entry->arr.ht_head;
    uint32_t empty_buckets = first.entry->arr.nbuckets;
    size_t empty_mem = first.entry->mem_bytes;
    ASSERT(col_session_get_arrangement(sess, "edge", key_cols, 1)
        == first.arr && !first.entry->rebuild_deferred,
        "a pinned, fresh, empty index must be returned, not deferred");
    /* The empty index is handed out through the same-size rebuild path
     * today; it must never reallocate the buffers under the lease. */
    ASSERT(first.entry->arr.ht_head == empty_head
        && first.entry->arr.nbuckets == empty_buckets
        && first.entry->mem_bytes == empty_mem,
        "looking up a pinned empty index must not touch its buffers");
    ASSERT(col_session_pin_arrangement(sess, "edge", key_cols, 1, &second)
        == 0 && first.entry->pin_count == 2,
        "a second lease on a fresh empty index must succeed");
    col_arrangement_pin_release(&second);
    col_arrangement_pin_release(&first);
    ASSERT(first.entry == NULL && cs->arr_count > 0,
        "both leases must be released");

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
    test_pinned_invalidation();
    test_pinned_rebuild_deferred();
    test_pinned_empty_index_available();

    printf("\n%d/%d tests passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n");

    return fail_count > 0 ? 1 : 0;
}
