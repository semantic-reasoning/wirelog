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
    TEST(
        "Tombstoned entry (buffers freed, token cleared) rebuilds on re-access");

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
    ASSERT(wl_columnar_relation_snapshot_valid(entry->source_snapshot),
        "rebuild must leave a valid source token");
    /* Issue #1500: a real LRU eviction clears the token as well, so the
     * tombstone reads as unbuilt by token and by freed buffers.  Lease the
     * entry through the pressured lookup so it is deferred rather than
     * reused as the new entry's slot; the release then performs the
     * deferred eviction with no lookup to reuse the tombstone. */
    col_arrangement_pin_t pin = { 0 };
    ASSERT(col_session_pin_arrangement(sess, "edge", key_cols, 1, &pin) == 0
        && pin.entry == entry, "pin before pressure");
    uint32_t alternate[1] = { 1 };
    cs->arr_cache_limit_bytes = 0;
    (void)col_session_get_arrangement(sess, "edge", alternate, 1);
    ASSERT(entry->evict_deferred && entry->mem_bytes > 0,
        "pressure must defer eviction of the leased entry");
    col_arrangement_pin_release(&pin);
    ASSERT(entry->mem_bytes == 0 && entry->arr.nbuckets == 0
        && !wl_columnar_relation_snapshot_valid(entry->source_snapshot),
        "deferred eviction must free the buffers and clear the source token");

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

    col_arr_entry_t *pinned_entry = pin.entry;
    col_arrangement_pin_release(&pin);
    ASSERT(pin.active == false, "release must deactivate the lease");
    ASSERT(arr->indexed_rows == 0,
        "deferred invalidation must take effect on final release");
    /* Issue #1500: the deferred invalidation clears the token, which is
     * what makes the entry unbuilt for the next lookup. */
    ASSERT(!wl_columnar_relation_snapshot_valid(pinned_entry->source_snapshot),
        "deferred release must clear the source token");

    arr = col_session_get_arrangement(sess, "edge", key_cols, 1);
    ASSERT(arr != NULL && arr->indexed_rows == 3,
        "next access must rebuild the invalidated arrangement");
    ASSERT(wl_columnar_relation_snapshot_valid(pinned_entry->source_snapshot),
        "rebuild must restore a valid source token");

    free_session(sess, plan, prog);
    PASS();
}

/* No probe is performed against changed source columns until all old leases
 * end. These cases test index storage, not source-reader isolation. */
static void
test_pinned_rebuild(unsigned scenario)
{
    const char *names[] = {
        "same-count mutation", "append within capacity", "append with growth",
        "synthetic incremental append", "synthetic incremental growth",
        "explicit invalidation", "deferred eviction",
    };
    TEST(names[scenario]);
    const char *src = ".decl edge(x: int32, y: int32)\n"
        "edge(10, 1). edge(20, 2). edge(30, 3).\n"
        ".decl path(x: int32, y: int32)\n"
        "path(x, y) :- edge(x, y).\n";
    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    col_arrangement_pin_t first = { 0 }, second = { 0 }, denied = { 0 };
    uint64_t *heads = NULL;
    uint32_t *chains = NULL;
    const char *failure = NULL;
#define PIN_CHECK(condition, message) do { \
            if (!(condition)) { failure = message; goto cleanup; } \
} while (0)
    PIN_CHECK(make_session(src, &sess, &plan, &prog) == 0, "session setup");
    wl_col_session_t *cs = COL_SESSION(sess);
    col_rel_t *rel = session_find_rel(cs, "edge");
    uint32_t key[1] = { 0 };
    PIN_CHECK(rel != NULL, "source relation missing");
    PIN_CHECK(col_session_pin_arrangement(sess, "edge", key, 1, &first) == 0,
        "first pin");
    PIN_CHECK(col_session_pin_arrangement(sess, "edge", key, 1,
        &second) == 0,
        "fresh nested pin");
    col_arr_entry_t *entry = first.entry;
    col_arrangement_t *arr = first.arr;
    if (scenario == 0 || scenario == 6)
        PIN_CHECK(col_rel_set(rel, 0, 0, 11) == 0, "same-count mutation");
    if (scenario >= 1 && scenario <= 4) {
        unsigned count = (scenario == 2 || scenario == 4) ? 40 : 1;
        for (unsigned i = 0; i < count; i++) {
            int64_t row[2] = { 100 + i, 1000 + i };
            PIN_CHECK(col_rel_append_row(rel, row) == 0, "source append");
        }
        if (scenario >= 3) {
            /* Defensive incremental branch: real appends change the token. */
            entry->source_snapshot = wl_columnar_relation_snapshot(rel);
        }
    }
    if (scenario == 5)
        col_session_invalidate_arrangements(sess, "edge");
    if (scenario == 6) {
        uint32_t alternate[1] = { 1 };
        cs->arr_cache_limit_bytes = 0;
        cs->arr_cap = cs->arr_count;
        PIN_CHECK(col_session_get_arrangement(sess, "edge", alternate, 1)
            == NULL && entry->evict_deferred,
            "pressure must defer eviction and registry growth");
    }
    /* Capture after source allocation; only arrangement accounting is fixed. */
    col_arrangement_t saved = *arr;
    col_relation_snapshot_t snapshot = entry->source_snapshot;
    size_t bytes = entry->mem_bytes, total = cs->arr_total_bytes;
    heads = malloc(arr->nbuckets * sizeof(*heads));
    chains = malloc((arr->indexed_rows + 1u) * sizeof(*chains));
    PIN_CHECK(heads && chains, "snapshot allocation");
    memcpy(heads, arr->ht_head, arr->nbuckets * sizeof(*heads));
    if (arr->indexed_rows)
        memcpy(chains, arr->ht_next, arr->indexed_rows * sizeof(*chains));
    PIN_CHECK(col_session_get_arrangement(sess, "edge", key, 1) == NULL,
        "lookup rebuilt or returned a stale pinned index");
    PIN_CHECK(col_session_pin_arrangement(sess, "edge", key, 1, &denied) != 0
        && !denied.active, "stale acquisition must remain inactive");
    PIN_CHECK(entry->pin_count == 2u
        && entry->rebuild_deferred, "deferred lease count");
    PIN_CHECK(arr->ht_head == saved.ht_head && arr->ht_next == saved.ht_next
        && arr->nbuckets == saved.nbuckets && arr->ht_cap == saved.ht_cap
        && arr->generation == saved.generation
        && arr->indexed_rows == saved.indexed_rows
        && entry->mem_bytes == bytes && cs->arr_total_bytes == total
        && arr->reservation.bytes == saved.reservation.bytes
        && wl_columnar_relation_snapshot_equal(entry->source_snapshot, snapshot)
        && memcmp(heads, arr->ht_head, arr->nbuckets * sizeof(*heads)) == 0
        && (!arr->indexed_rows || memcmp(chains, arr->ht_next,
        arr->indexed_rows * sizeof(*chains)) == 0),
        "denied lookup changed index storage, token or accounting");
    col_arrangement_pin_release(&second);
    PIN_CHECK(entry->pin_count == 1 && entry->rebuild_deferred
        && arr->indexed_rows == saved.indexed_rows,
        "nonfinal release invalidated the index");
    col_arrangement_pin_release(&first);
    PIN_CHECK(entry->pin_count == 0 && !entry->rebuild_deferred
        && arr->indexed_rows == 0
        && (scenario == 6 ? entry->mem_bytes == 0
            : arr->generation == saved.generation),
        "final release must invalidate without rebuilding");
    arr = col_session_get_arrangement(sess, "edge", key, 1);
    PIN_CHECK(arr && arr->indexed_rows == rel->nrows
        && wl_columnar_relation_snapshot_equal(entry->source_snapshot,
        wl_columnar_relation_snapshot(rel)), "lazy rebuild freshness");
    for (uint32_t i = 0; i < rel->nrows; i++) {
        int64_t row[2] = { rel->columns[0][i], rel->columns[1][i] };
        uint32_t found = col_arrangement_find_first(arr, rel->columns, 2, row);
        PIN_CHECK(found == i, "rebuilt key mismatch");
        unsigned matches = 0, visited = 0;
        while (found != UINT32_MAX && visited++ < rel->nrows) {
            PIN_CHECK(found < rel->nrows, "rebuilt chain out of bounds");
            if (rel->columns[0][found] == row[0])
                matches++;
            found = col_arrangement_find_next(arr, found);
        }
        PIN_CHECK(found == UINT32_MAX && matches == 1,
            "rebuilt key multiplicity or cycle mismatch");
    }
    if (scenario == 0 || scenario == 6) {
        int64_t old[2] = { 10, 1 };
        PIN_CHECK(col_arrangement_find_first(arr, rel->columns, 2, old)
            == UINT32_MAX, "removed key still indexed");
    }
    size_t sum = 0;
    for (uint32_t i = 0; i < cs->arr_count; i++)
        sum += cs->arr_entries[i].mem_bytes;
    PIN_CHECK(sum == cs->arr_total_bytes, "rebuild accounting sum");
    {
        uint32_t generation = arr->generation;
        PIN_CHECK(col_session_get_arrangement(sess, "edge", key, 1) == arr
            && arr->generation == generation,
            "fresh lookup rebuilt unnecessarily");
    }
cleanup:
    col_arrangement_pin_release(&denied);
    col_arrangement_pin_release(&second);
    col_arrangement_pin_release(&first);
    free(heads);
    free(chains);
    if (sess)
        free_session(sess, plan, prog);
    if (failure)
        FAIL(failure);
    PASS();
#undef PIN_CHECK
}

/* ================================================================
 * Issue #1500: a built empty index is fresh, not unbuilt.
 *
 * Fixture: a declared EDB with no facts.  Session creation registers it
 * with its schema and a valid token; the fallback below covers a build
 * that leaves ncols at 0 (as seen for int-only EDBs elsewhere).
 * ================================================================ */
static int
make_empty_edge_session(wl_session_t **sess, wl_plan_t **plan,
    wirelog_program_t **prog, col_rel_t **edge_out)
{
    const char *src = ".decl edge(x: int32, y: int32)\n"
        ".decl path(x: int32, y: int32)\n"
        "path(x, y) :- edge(x, y).\n";
    if (make_session(src, sess, plan, prog) != 0)
        return -1;
    col_rel_t *edge = session_find_rel(COL_SESSION(*sess), "edge");
    if (!edge)
        return -1;
    if (edge->ncols == 0 && col_rel_set_schema(edge, 2, NULL) != 0)
        return -1;
    if (edge->ncols != 2 || edge->nrows != 0
        || !wl_columnar_relation_snapshot_valid(
            wl_columnar_relation_snapshot(edge)))
        return -1;
    *edge_out = edge;
    return 0;
}

static col_arr_entry_t *
find_entry(wl_col_session_t *cs, const char *name)
{
    for (uint32_t i = 0; i < cs->arr_count; i++) {
        if (cs->arr_entries[i].rel_name
            && strcmp(cs->arr_entries[i].rel_name, name) == 0)
            return &cs->arr_entries[i];
    }
    return NULL;
}

/* A leased empty index is returned as is by a later lookup, admits a
 * second lease, and is neither deferred nor invalidated by the releases.
 * On the previous clause the lookup returned NULL and the second lease
 * failed. */
static void
test_pinned_empty_index(void)
{
    TEST("pinned empty index stays available and admits a second lease");
    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    col_rel_t *edge = NULL;
    ASSERT(make_empty_edge_session(&sess, &plan, &prog, &edge) == 0,
        "empty-relation fixture");
    wl_col_session_t *cs = COL_SESSION(sess);
    uint32_t key[1] = { 0 };
    col_arrangement_pin_t first = { 0 }, second = { 0 };
    int ok = col_session_pin_arrangement(sess, "edge", key, 1, &first) == 0;
    col_arr_entry_t *entry = ok ? first.entry : NULL;
    col_arrangement_t *arr = ok ? first.arr : NULL;
    if (ok)
        ok = entry && arr && arr->indexed_rows == 0 && arr->nbuckets == 16
            && arr->ht_cap == 0 && entry->mem_bytes == 128
            && wl_columnar_relation_snapshot_valid(entry->source_snapshot);
    uint64_t *heads = ok ? arr->ht_head : NULL;
    uint32_t generation = ok ? arr->generation : 0;
    size_t bytes = ok ? entry->mem_bytes : 0, total = cs->arr_total_bytes;
    if (ok)
        ok = col_session_get_arrangement(sess, "edge", key, 1) == arr
            && !entry->rebuild_deferred && arr->ht_head == heads
            && arr->generation == generation && arr->nbuckets == 16
            && entry->mem_bytes == bytes && cs->arr_total_bytes == total;
    if (ok)
        ok = col_session_pin_arrangement(sess, "edge", key, 1, &second) == 0
            && second.arr == arr && entry->pin_count == 2;
    col_arrangement_pin_release(&second);
    col_arrangement_pin_release(&first);
    if (ok)
        ok = entry->pin_count == 0 && !entry->rebuild_deferred
            && arr->indexed_rows == 0 && arr->generation == generation
            && wl_columnar_relation_snapshot_valid(entry->source_snapshot);
    free_session(sess, plan, prog);
    ASSERT(ok, "pinned empty index was deferred, rebuilt or refused a lease");
    PASS();
}

/* An unpinned empty index is not rebuilt on every lookup; invalidation
 * clears its token and the next lookup rebuilds.  On the previous clause
 * the second lookup re-entered the full build (generation bumped). */
static void
test_unpinned_empty_index_no_rebuild(void)
{
    TEST("unpinned empty index is not rebuilt on every lookup");
    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    col_rel_t *edge = NULL;
    ASSERT(make_empty_edge_session(&sess, &plan, &prog, &edge) == 0,
        "empty-relation fixture");
    wl_col_session_t *cs = COL_SESSION(sess);
    uint32_t key[1] = { 0 };
    col_arrangement_t *arr = col_session_get_arrangement(sess, "edge", key, 1);
    col_arr_entry_t *entry = find_entry(cs, "edge");
    int ok = arr && entry && &entry->arr == arr && arr->indexed_rows == 0
        && arr->nbuckets == 16 && entry->mem_bytes == 128
        && wl_columnar_relation_snapshot_valid(entry->source_snapshot);
    uint64_t *heads = ok ? arr->ht_head : NULL;
    uint32_t generation = ok ? arr->generation : 0;
    size_t total = cs->arr_total_bytes;
    if (ok)
        ok = col_session_get_arrangement(sess, "edge", key, 1) == arr
            && arr->generation == generation && arr->ht_head == heads
            && entry->mem_bytes == 128 && cs->arr_total_bytes == total;
    if (ok) {
        col_session_invalidate_arrangements(sess, "edge");
        ok = !wl_columnar_relation_snapshot_valid(entry->source_snapshot)
            && arr->indexed_rows == 0;
    }
    if (ok)
        ok = col_session_get_arrangement(sess, "edge", key, 1) == arr
            && arr->generation == generation + 1
            && wl_columnar_relation_snapshot_valid(entry->source_snapshot)
            && cs->arr_total_bytes == total;
    free_session(sess, plan, prog);
    ASSERT(ok,
        "unpinned empty index was rebuilt needlessly or not after invalidation");
    PASS();
}

/* ================================================================
 * main
 * ================================================================ */
/* ================================================================
 * Issue #1515: a failed tombstone-slot reuse must leave the registry
 * scannable and the slot a well-formed tombstone.
 * ================================================================ */

/* An enforcing governor that denies every arrangement build: a 16-bucket
 * table needs far more than one byte. */
static wl_columnar_memory_governor_ref_t *
tight_governor(void)
{
    wl_columnar_memory_resolution_t resolution = { 0 };
    resolution.budget_bytes = 1u;
    resolution.usable_bytes = 1u;
    resolution.mode = WL_COLUMNAR_MEMORY_MODE_ENFORCING;
    resolution.source = WL_COLUMNAR_MEMORY_SOURCE_ENV;
    resolution.status = WL_COLUMNAR_MEMORY_OK;
    return wl_columnar_memory_governor_ref_create(&resolution);
}

static int
find_entry_index(const wl_col_session_t *cs, const char *rel_name,
    uint32_t key_col)
{
    for (uint32_t i = 0; i < cs->arr_count; i++) {
        const col_arr_entry_t *e = &cs->arr_entries[i];
        if (e->rel_name && strcmp(e->rel_name, rel_name) == 0
            && e->key_count == 1u && e->key_cols[0] == key_col)
            return (int)i;
    }
    return -1;
}

static const char *REUSE_FAILURE_SRC = ".decl edge(x: int32, y: int32)\n"
    "edge(10, 1). edge(20, 2). edge(30, 3).\n"
    ".decl node(x: int32)\n"
    "node(1). node(2).\n"
    ".decl pathA(x: int32, y: int32)\n"
    "pathA(x, y) :- edge(x, y).\n"
    ".decl n(x: int32)\n"
    "n(x) :- node(x).\n";

/* @tombstone_last selects where the tombstone sits: index 0 (a following
 * scan must walk past the failed slot) or the last index (the failure
 * tail must not mistake a reused last slot for an appended one). */
static void
test_tombstone_reuse_failure(bool tombstone_last)
{
    TEST(tombstone_last
        ? "Failed tombstone reuse (last slot) keeps a rebuildable tombstone"
        : "Failed tombstone reuse (first slot) keeps the registry scannable");

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(REUSE_FAILURE_SRC, &sess, &plan, &prog) == 0,
        "session creation failed");

    wl_col_session_t *cs = COL_SESSION(sess);
    uint32_t key0[1] = { 0 };
    uint32_t key1[1] = { 1 };
    col_arrangement_t *a_edge = NULL;
    col_arrangement_t *a_node = NULL;
    if (tombstone_last) {
        a_node = col_session_get_arrangement(sess, "node", key0, 1);
        a_edge = col_session_get_arrangement(sess, "edge", key0, 1);
    } else {
        a_edge = col_session_get_arrangement(sess, "edge", key0, 1);
        a_node = col_session_get_arrangement(sess, "node", key0, 1);
    }
    ASSERT(a_edge != NULL && a_node != NULL, "initial builds must succeed");
    int edge_idx = find_entry_index(cs, "edge", 0);
    ASSERT(edge_idx == (tombstone_last ? 1 : 0), "unexpected edge slot");

    /* Manufacture an eviction tombstone on the edge entry. */
    col_arr_entry_t *entry = &cs->arr_entries[edge_idx];
    cs->arr_total_bytes -= entry->mem_bytes;
    arr_free_contents(&entry->arr);
    entry->mem_bytes = 0;
    uint32_t count_before = cs->arr_count;
    size_t total_before = cs->arr_total_bytes;

    /* Only later attaches see the swapped session governor; every existing
     * holder keeps its own retained reference. */
    wl_columnar_memory_governor_ref_t *tight = tight_governor();
    ASSERT(tight != NULL, "tight governor allocation failed");
    wl_columnar_memory_governor_ref_t *orig = cs->memory_governor;
    cs->memory_governor = tight;

    /* A miss on a new key reuses the tombstone; the build is denied. */
    ASSERT(col_session_get_arrangement(sess, "edge", key1, 1) == NULL,
        "denied build must fail the lookup");
    ASSERT(cs->arr_count == count_before, "failure must not change arr_count");
    ASSERT(cs->arr_total_bytes == total_before,
        "failure must not change arr_total_bytes");
    entry = &cs->arr_entries[edge_idx];
    /* The registry stays scannable: lookup, pin and invalidation walk past
     * the failed slot. */
    ASSERT(col_session_get_arrangement(sess, "node", key0, 1) == a_node,
        "lookup of another relation must still hit");
    col_arrangement_pin_t pin;
    ASSERT(col_session_pin_arrangement(sess, "node", key0, 1, &pin) == 0,
        "pin of another relation must succeed");
    col_arrangement_pin_release(&pin);
    col_session_invalidate_arrangements(sess, "edge");

    ASSERT(entry->rel_name != NULL && strcmp(entry->rel_name, "edge") == 0,
        "failed slot must keep a name");
    ASSERT(entry->key_count == 1u && entry->key_cols != NULL
        && entry->key_cols[0] == 1u, "failed slot must keep the new key");
    ASSERT(entry->arr.indexed_rows == 0 && entry->mem_bytes == 0
        && entry->arr.ht_head == NULL && entry->arr.reserved_bytes == 0
        && entry->pin_count == 0, "failed slot must be a tombstone");
    ASSERT(entry->arr.memory_governor == tight,
        "failed slot must keep the governor it attached");
    col_session_invalidate_arrangements(sess, "edge");
    ASSERT(entry->arr.indexed_rows == 0, "tombstone stays invalidated");
    ASSERT(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(tight)) == 0,
        "denied build must not leave a reservation");

    /* A same-key retry takes the hit path; the slot's own governor still
     * denies, and the tombstone is unchanged. */
    ASSERT(col_session_get_arrangement(sess, "edge", key1, 1) == NULL,
        "same-key retry must still be denied");
    ASSERT(cs->arr_count == count_before
        && cs->arr_total_bytes == total_before
        && entry->rel_name != NULL && entry->key_cols[0] == 1u
        && entry->mem_bytes == 0, "denied retry must leave the tombstone");

    /* Restore the session governor; the tombstone keeps its own reference
     * until it is reused. */
    cs->memory_governor = orig;
    wl_columnar_memory_governor_ref_release(tight);

    /* A later miss reuses the failed slot with the session governor. */
    col_arrangement_t *a_again
        = col_session_get_arrangement(sess, "edge", key0, 1);
    ASSERT(a_again != NULL, "reuse after restore must build");
    ASSERT(a_again->indexed_rows == 3, "rebuilt arrangement must index rows");
    ASSERT(cs->arr_count == count_before, "reuse must not grow arr_count");
    entry = &cs->arr_entries[edge_idx];
    ASSERT(a_again == &entry->arr && entry->key_cols[0] == 0u,
        "reuse must take the failed slot");
    ASSERT(entry->arr.memory_governor == orig,
        "reused slot must attach the session governor");
    ASSERT(cs->arr_total_bytes == total_before + entry->mem_bytes,
        "arr_total_bytes must account the rebuilt entry only");

    free_session(sess, plan, prog);
    PASS();
}

/* The appended case keeps today's rollback: a denied first build on a
 * fresh registry leaves nothing behind. */
static void
test_append_failure_rolls_back(void)
{
    TEST("Denied first build on an appended slot rolls back");

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(REUSE_FAILURE_SRC, &sess, &plan, &prog) == 0,
        "session creation failed");

    wl_col_session_t *cs = COL_SESSION(sess);
    uint32_t key0[1] = { 0 };
    uint32_t count_before = cs->arr_count;
    size_t total_before = cs->arr_total_bytes;
    wl_columnar_memory_governor_ref_t *tight = tight_governor();
    ASSERT(tight != NULL, "tight governor allocation failed");
    wl_columnar_memory_governor_ref_t *orig = cs->memory_governor;
    cs->memory_governor = tight;

    ASSERT(col_session_get_arrangement(sess, "edge", key0, 1) == NULL,
        "denied build must fail the lookup");
    ASSERT(cs->arr_count == count_before && cs->arr_total_bytes == total_before,
        "appended failure must roll back");
    ASSERT(wl_columnar_memory_reserved(
            wl_columnar_memory_governor_ref_get(tight)) == 0,
        "denied build must not leave a reservation");

    cs->memory_governor = orig;
    wl_columnar_memory_governor_ref_release(tight);
    col_arrangement_t *arr = col_session_get_arrangement(sess, "edge", key0, 1);
    ASSERT(arr != NULL && arr->indexed_rows == 3,
        "build after restore must succeed");
    ASSERT(cs->arr_count == count_before + 1u, "append must add one entry");

    free_session(sess, plan, prog);
    PASS();
}

int
main(void)
{
    printf("=== test_arrangement_lru_eviction ===\n");

    test_lru_clock_and_mem_bytes();
    test_env_var_limit();
    test_tombstone_rebuild();
    test_tombstone_reuse_failure(false);
    test_tombstone_reuse_failure(true);
    test_append_failure_rolls_back();
    test_total_bytes_accounting();
    test_pinned_invalidation();
    for (unsigned scenario = 0; scenario < 7; scenario++)
        test_pinned_rebuild(scenario);
    test_pinned_empty_index();
    test_unpinned_empty_index_no_rebuild();

    printf("\n%d/%d tests passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n");

    return fail_count > 0 ? 1 : 0;
}
