/*
 * test_arrangement.c - Unit tests for col_arrangement_t (Phase 3C-001)
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * Validates the persistent hash-index arrangement layer:
 *   1. col_arrangement_t struct size (16-byte sentinel check)
 *   2. col_session_get_arrangement: lazy creation, indexed_rows = nrows
 *   3. col_arrangement_find_first: known key found, correct row returned
 *   4. col_arrangement_find_first: unknown key returns UINT32_MAX
 *   5. col_arrangement_find_next: single-match key returns UINT32_MAX
 *   6. Multiple rows same key: find_next traverses full chain
 *   7. col_session_invalidate_arrangements: resets indexed_rows to 0
 *   8. Second get_arrangement call returns cached pointer (registry reuse)
 *
 * Private struct access:
 *   col_rel_t and wl_col_session_t are private to columnar_nanoarrow.c.
 *   We mirror only the first few stable fields needed to obtain rel->data
 *   and rel->ncols for passing to col_arrangement_find_first.
 *   If the private struct layout changes, update the mirrors below.
 */

#include "../wirelog/columnar/columnar_nanoarrow.h"
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

/* ----------------------------------------------------------------
 * Test framework
 * ---------------------------------------------------------------- */

static int test_count = 0;
static int pass_count = 0;
static int fail_count = 0;

#define TEST(name)                                      \
    do {                                                \
        test_count++;                                   \
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
 * Private struct mirrors
 *
 * We mirror only the initial, stable fields of col_rel_t and
 * wl_col_session_t to extract rel->data and rel->ncols.
 *
 * Layout must match columnar_nanoarrow.c exactly.
 * Keep in sync when those structs change.
 * ---------------------------------------------------------------- */

/*
 * Mirror of col_rel_t (first 5 fields only).
 * Full definition is in columnar_nanoarrow.c:49-62.
 *
 * Memory layout (x86-64, LP64):
 *   char     *name      → 8 bytes  (offset  0)
 *   uint32_t  ncols     → 4 bytes  (offset  8)
 *   [padding            → 4 bytes  (offset 12)]
 *   int64_t  *data      → 8 bytes  (offset 16)
 *   uint32_t  nrows     → 4 bytes  (offset 24)
 *   uint32_t  capacity  → 4 bytes  (offset 28)
 */
typedef struct {
    char *name;
    uint32_t ncols;
    uint32_t _ncols_pad; /* explicit alignment padding */
    int64_t *data;
    uint32_t nrows;
    uint32_t capacity;
    /* omitted: col_names, schema, schema_ok, timestamps */
} test_col_rel_mirror_t;

/*
 * Mirror of wl_col_session_t (first 6 fields only).
 * Full definition is in columnar_nanoarrow.c:670-690.
 *
 * Memory layout (x86-64):
 *   wl_session_t   base     (sizeof(wl_session_t) bytes, offset 0)
 *   const void    *plan     (8 bytes)
 *   col_rel_t    **rels     (8 bytes)
 *   uint32_t       nrels    (4 bytes)
 *   uint32_t       rel_cap  (4 bytes)
 */
typedef struct {
    wl_session_t base;
    const void *plan;
    test_col_rel_mirror_t **rels;
    uint32_t nrels;
    uint32_t rel_cap;
    /* omitted: delta_cb, delta_data, eval_arena, mat_cache, etc. */
} test_col_session_mirror_t;

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

/* build and evaluate a simple program, return open session.
 * Caller owns *out_sess, *out_plan, *out_prog and must free them. */
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

/*
 * find_rel_mirror: locate a relation by name in the mirrored session.
 * Returns NULL if not found.
 */
static test_col_rel_mirror_t *
find_rel_mirror(wl_session_t *sess, const char *rel_name)
{
    test_col_session_mirror_t *ms = (test_col_session_mirror_t *)sess;
    for (uint32_t i = 0; i < ms->nrels; i++) {
        test_col_rel_mirror_t *r = ms->rels[i];
        if (r && r->name && strcmp(r->name, rel_name) == 0)
            return r;
    }
    return NULL;
}

/* ================================================================
 * Test 1: col_arrangement_t struct size
 *
 * The public struct col_arrangement_t must have a stable known size.
 * This test acts as a sentinel: if any field is added/removed/resized,
 * the test breaks and the developer notices immediately.
 *
 * Expected size (x86-64, LP64):
 *   uint32_t *key_cols     8  (offset  0)
 *   uint32_t  key_count    4  (offset  8)
 *   uint32_t  nbuckets     4  (offset 12) — packed with key_count, no pad
 *   uint32_t *ht_head      8  (offset 16)
 *   uint32_t *ht_next      8  (offset 24)
 *   uint32_t  ht_cap       4  (offset 32)
 *   uint32_t  indexed_rows 4  (offset 36)
 *   uint64_t  content_hash 8  (offset 40)
 *                         = 48 bytes total
 * ================================================================ */
static void
test_arrangement_struct_size(void)
{
    TEST("col_arrangement_t struct size (layout sentinel)");

    /* 48 bytes: 3 pointers (24) + 4 uint32 (16) + 1 uint64 (8) */
    ASSERT(sizeof(col_arrangement_t) == 48,
           "col_arrangement_t must be 48 bytes; update if struct changes");

    PASS();
}

/* ================================================================
 * Test 2: col_session_get_arrangement creates and populates index
 *
 * After snapshot on a 3-edge TC program, request an arrangement
 * on the base relation.  The arrangement must:
 *   - be non-NULL
 *   - have indexed_rows equal to the number of EDB facts
 *   - have nbuckets > 0 (power of 2)
 * ================================================================ */
static void
test_arrangement_creation(void)
{
    TEST(
        "col_session_get_arrangement: creates index with correct indexed_rows");

    /* 3 facts in edge, 2 columns (x, y) */
    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl reach(x: int32, y: int32)\n"
                      "reach(x, y) :- edge(x, y).\n"
                      "reach(x, z) :- reach(x, y), edge(y, z).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    /* Arrange on col 0 of 'edge' */
    uint32_t key_cols[] = { 0 };
    col_arrangement_t *arr
        = col_session_get_arrangement(sess, "edge", key_cols, 1);

    ASSERT(arr != NULL, "arrangement must be non-NULL for existing relation");
    ASSERT(arr->indexed_rows == 3,
           "indexed_rows must equal EDB fact count (3)");
    ASSERT(arr->nbuckets > 0, "nbuckets must be > 0");
    ASSERT((arr->nbuckets & (arr->nbuckets - 1)) == 0,
           "nbuckets must be a power of 2");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 3: col_arrangement_find_first locates a known key
 *
 * Load edge(1,2), edge(2,3), edge(3,4).  Build arrangement on col 0.
 * Query key_row = {2, *} — should find row where col 0 == 2 (edge(2,3)).
 * ================================================================ */
static void
test_arrangement_find_first_hit(void)
{
    TEST("col_arrangement_find_first: known key returns valid row index");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl reach(x: int32, y: int32)\n"
                      "reach(x, y) :- edge(x, y).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    uint32_t key_cols[] = { 0 };
    col_arrangement_t *arr
        = col_session_get_arrangement(sess, "edge", key_cols, 1);
    ASSERT(arr != NULL, "arrangement must be non-NULL");

    test_col_rel_mirror_t *rel = find_rel_mirror(sess, "edge");
    ASSERT(rel != NULL, "edge relation not found in session");
    ASSERT(rel->ncols == 2, "edge must have 2 columns");

    /* key_row: col 0 = 2 (any value in col 1 is ignored by find_first) */
    int64_t key_row[2] = { 2, 0 };
    uint32_t row_idx
        = col_arrangement_find_first(arr, rel->data, rel->ncols, key_row);

    ASSERT(row_idx != UINT32_MAX, "find_first must find row with key col0=2");

    /* Verify the returned row actually has col 0 == 2 */
    const int64_t *rp = rel->data + (size_t)row_idx * rel->ncols;
    ASSERT(rp[0] == 2, "returned row must have col 0 == 2");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 4: col_arrangement_find_first returns UINT32_MAX for missing key
 *
 * Query for key col0=99 which does not exist in edge.
 * ================================================================ */
static void
test_arrangement_find_first_miss(void)
{
    TEST("col_arrangement_find_first: unknown key returns UINT32_MAX");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl reach(x: int32, y: int32)\n"
                      "reach(x, y) :- edge(x, y).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    uint32_t key_cols[] = { 0 };
    col_arrangement_t *arr
        = col_session_get_arrangement(sess, "edge", key_cols, 1);
    ASSERT(arr != NULL, "arrangement must be non-NULL");

    test_col_rel_mirror_t *rel = find_rel_mirror(sess, "edge");
    ASSERT(rel != NULL, "edge relation not found");

    int64_t key_row[2] = { 99, 0 }; /* col0=99 does not exist */
    uint32_t row_idx
        = col_arrangement_find_first(arr, rel->data, rel->ncols, key_row);

    ASSERT(row_idx == UINT32_MAX,
           "find_first must return UINT32_MAX for non-existent key");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 5: find_next at end of single-match chain returns UINT32_MAX
 *
 * In edge(1,2), edge(2,3), edge(3,4) keyed on col 0:
 * each key value (1, 2, 3) appears exactly once.
 * After find_first, find_next must return UINT32_MAX.
 * ================================================================ */
static void
test_arrangement_find_next_singleton(void)
{
    TEST("col_arrangement_find_next: singleton chain terminates with "
         "UINT32_MAX");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl reach(x: int32, y: int32)\n"
                      "reach(x, y) :- edge(x, y).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    uint32_t key_cols[] = { 0 };
    col_arrangement_t *arr
        = col_session_get_arrangement(sess, "edge", key_cols, 1);
    ASSERT(arr != NULL, "arrangement must be non-NULL");

    test_col_rel_mirror_t *rel = find_rel_mirror(sess, "edge");
    ASSERT(rel != NULL, "edge relation not found");

    int64_t key_row[2] = { 1, 0 };
    uint32_t first
        = col_arrangement_find_first(arr, rel->data, rel->ncols, key_row);
    ASSERT(first != UINT32_MAX, "find_first must find key col0=1");

    uint32_t next = col_arrangement_find_next(arr, first);
    /* The chain may contain hash-collision false positives from other rows,
     * but since edge keys (1,2,3) are distinct primes with 8+ buckets,
     * the singleton property holds in practice.  Verify next terminates. */
    while (next != UINT32_MAX) {
        /* If there's a collision, the next row's col 0 must NOT equal 1
         * (it's a false positive from a different key). */
        const int64_t *rp = rel->data + (size_t)next * rel->ncols;
        ASSERT(
            rp[0] != 1,
            "find_next continuation must be a hash-collision false positive");
        next = col_arrangement_find_next(arr, next);
    }

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 6: Multiple rows with same key — find_next traverses all
 *
 * Load r(1,2), r(1,3), r(1,4), r(2,5).
 * Arrange on col 0.  Key col0=1 should match 3 rows.
 * find_first + find_next chain must yield all 3 matches.
 * ================================================================ */
static void
test_arrangement_find_next_chain(void)
{
    TEST("col_arrangement_find_next: multi-row key traverses full chain");

    const char *src = ".decl r(x: int32, y: int32)\n"
                      "r(1, 2). r(1, 3). r(1, 4). r(2, 5).\n"
                      ".decl s(x: int32, y: int32)\n"
                      "s(x, y) :- r(x, y).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    uint32_t key_cols[] = { 0 };
    col_arrangement_t *arr
        = col_session_get_arrangement(sess, "r", key_cols, 1);
    ASSERT(arr != NULL, "arrangement must be non-NULL");

    test_col_rel_mirror_t *rel = find_rel_mirror(sess, "r");
    ASSERT(rel != NULL, "r relation not found");
    ASSERT(rel->nrows == 4, "r must have 4 rows");

    int64_t key_row[2] = { 1, 0 };
    uint32_t count = 0;
    uint32_t row
        = col_arrangement_find_first(arr, rel->data, rel->ncols, key_row);
    while (row != UINT32_MAX) {
        const int64_t *rp = rel->data + (size_t)row * rel->ncols;
        if (rp[0] == 1)
            count++;
        row = col_arrangement_find_next(arr, row);
    }

    ASSERT(count == 3, "find chain must visit exactly 3 rows with col0=1");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 7: col_session_invalidate_arrangements resets indexed_rows
 *
 * After invalidation, indexed_rows must be 0.
 * After a subsequent get_arrangement call, it must be rebuilt.
 * ================================================================ */
static void
test_arrangement_invalidate(void)
{
    TEST("col_session_invalidate_arrangements: resets indexed_rows to 0");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl reach(x: int32, y: int32)\n"
                      "reach(x, y) :- edge(x, y).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    uint32_t key_cols[] = { 0 };
    col_arrangement_t *arr
        = col_session_get_arrangement(sess, "edge", key_cols, 1);
    ASSERT(arr != NULL, "arrangement must be non-NULL");
    ASSERT(arr->indexed_rows == 3,
           "indexed_rows must be 3 before invalidation");

    /* Invalidate */
    col_session_invalidate_arrangements(sess, "edge");
    ASSERT(arr->indexed_rows == 0,
           "indexed_rows must be 0 immediately after invalidation");

    /* Re-request: should trigger full rebuild */
    col_arrangement_t *arr2
        = col_session_get_arrangement(sess, "edge", key_cols, 1);
    ASSERT(arr2 != NULL, "arrangement must be non-NULL after rebuild");
    ASSERT(arr2->indexed_rows == 3,
           "indexed_rows must be restored to 3 after rebuild");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * Test 8: Registry caching — second call returns same pointer
 *
 * Two consecutive calls to col_session_get_arrangement with identical
 * arguments must return the same col_arrangement_t pointer (no
 * duplication in the registry).
 * ================================================================ */
static void
test_arrangement_registry_cache(void)
{
    TEST(
        "col_session_get_arrangement: registry caches — same pointer returned");

    const char *src = ".decl edge(x: int32, y: int32)\n"
                      "edge(1, 2). edge(2, 3). edge(3, 4).\n"
                      ".decl reach(x: int32, y: int32)\n"
                      "reach(x, y) :- edge(x, y).\n";

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(src, &sess, &plan, &prog) == 0,
           "session creation failed");

    uint32_t key_cols[] = { 0 };
    col_arrangement_t *arr1
        = col_session_get_arrangement(sess, "edge", key_cols, 1);
    col_arrangement_t *arr2
        = col_session_get_arrangement(sess, "edge", key_cols, 1);

    ASSERT(arr1 != NULL, "first arrangement must be non-NULL");
    ASSERT(arr2 != NULL, "second arrangement must be non-NULL");
    ASSERT(arr1 == arr2,
           "second call must return the same pointer (registry cache hit)");

    /* Different key_cols must produce a separate entry */
    uint32_t key_cols2[] = { 1 };
    col_arrangement_t *arr3
        = col_session_get_arrangement(sess, "edge", key_cols2, 1);
    ASSERT(arr3 != NULL, "arrangement on col 1 must be non-NULL");
    ASSERT(arr3 != arr1, "different key_cols must produce a distinct entry");

    free_session(sess, plan, prog);
    PASS();
}

/* ================================================================
 * main
 * ================================================================ */

int
main(void)
{
    printf("\n=== Arrangement Layer Unit Tests (Phase 3C-001) ===\n\n");

    test_arrangement_struct_size();
    test_arrangement_creation();
    test_arrangement_find_first_hit();
    test_arrangement_find_first_miss();
    test_arrangement_find_next_singleton();
    test_arrangement_find_next_chain();
    test_arrangement_invalidate();
    test_arrangement_registry_cache();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n\n");

    return fail_count > 0 ? 1 : 0;
}
