/*
 * test_arrangement_pow2_overflow.c - Issue #1074
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 *
 * arr_next_pow2() smears the low bits of n-1 upward and returns n + 1.  For
 * n > 0x80000000 the smear saturates to 0xFFFFFFFF and the return wraps to 0,
 * so the "next power of two" of a large row count is zero.  Two call sites in
 * wirelog/columnar/arrangement.c consume that:
 *
 *   1. arr_build_full() computes nbuckets = arr_next_pow2(nrows * 2).  With
 *      nbuckets == 0 and a freshly zeroed arrangement, `nbuckets !=
 *      arr->nbuckets` is false, the bucket-head allocation is skipped, and
 *      ht_head stays NULL.  arr_index_rows() then loads arr->ht_head[bucket]
 *      through a null pointer.  Sweeping all 2^32 row counts, nbuckets is 0
 *      for nrows in [0x40000001, 0x7FFFFFFF] and again in
 *      [0xC0000001, 0xFFFFFFFF] -- 2147483646 values, about half the space.
 *
 *   2. arr_update_incremental() sizes the chain array as nrows * 2, which
 *      wraps to 0 for nrows >= 0x80000000 and is then clamped up to the
 *      16-entry floor.  A 64-byte chain array for a two-billion-row relation
 *      is a heap-buffer-overflow WRITE in arr_index_rows().  arr_next_pow2(0)
 *      is 16, so `needed != arr->nbuckets` does not fire and the full rebuild
 *      -- and case 1's guard with it -- is never reached.
 *
 * Both cases are unrepresentable relations, so both must be rejected rather
 * than indexed.
 *
 * Column sizing: ARR_HASH_TILE is 1024, and arr_hash_rows_batch() hashes a
 * whole tile before arr_index_rows() touches any bucket.  A column shorter
 * than one tile would fault inside the batch hash instead of at the site
 * under test, so the fixture supplies 4096 rows of real backing store and
 * only the row *count* is oversized.
 *
 * Private struct access:
 *   col_rel_t and wl_col_session_t are private to the columnar backend.  The
 *   mirrors below copy the initial, stable fields, exactly as
 *   tests/test_arrangement.c does.  Keep them in sync.
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
 * Fixture sizing
 * ---------------------------------------------------------------- */

/* Real rows of backing store per column: >= ARR_HASH_TILE (1024) so the
 * batch hash stays in bounds for the first tile of every case below. */
#define FIXTURE_ROWS 4096u

/* Smallest nrows for which arr_next_pow2(nrows * 2) == 0. */
#define NROWS_ZERO_BUCKETS 0x40000001u

/* Smallest nrows for which nrows * 2 wraps, undersizing the chain array. */
#define NROWS_CAP_WRAP 0x80000000u

/* ----------------------------------------------------------------
 * Private struct mirrors (see tests/test_arrangement.c)
 * ---------------------------------------------------------------- */

typedef struct {
    char *name;
    uint32_t ncols;
    uint32_t _ncols_pad; /* explicit alignment padding */
    int64_t **columns;
    void *column_types;
    uint32_t nrows;
    uint32_t capacity;
    /* omitted: col_names, schema, schema_ok, timestamps */
} test_col_rel_mirror_t;

typedef struct {
    wl_session_t base;
    const void *frontier_ops;
    const void *rotation_ops;
    const void *plan;
    const void *intern;
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

static const char *const FIXTURE_SRC = ".decl edge(x: int32, y: int32)\n"
    "edge(1, 2). edge(2, 3). edge(3, 4).\n"
    ".decl reach(x: int32, y: int32)\n"
    "reach(x, y) :- edge(x, y).\n";

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

/*
 * oversize_rel: point every column at FIXTURE_ROWS rows of test-owned backing
 * store and claim `nrows` rows.  The relation keeps ownership of its own
 * buffers, which are handed back by restore_rel() before the session is
 * destroyed, so nothing here changes what the session frees.
 *
 * Returns 0 on success.  `saved` receives the original column pointers and
 * must have room for rel->ncols entries.
 */
static int
oversize_rel(test_col_rel_mirror_t *rel, uint32_t nrows, int64_t **saved,
    int64_t **backing)
{
    for (uint32_t c = 0; c < rel->ncols; c++) {
        backing[c] = (int64_t *)malloc(FIXTURE_ROWS * sizeof(int64_t));
        if (!backing[c]) {
            for (uint32_t d = 0; d < c; d++)
                free(backing[d]);
            return -1;
        }
        for (uint32_t i = 0; i < FIXTURE_ROWS; i++)
            backing[c][i] = (int64_t)i + (int64_t)c;
    }
    for (uint32_t c = 0; c < rel->ncols; c++) {
        saved[c] = rel->columns[c];
        rel->columns[c] = backing[c];
    }
    rel->nrows = nrows;
    rel->capacity = nrows;
    return 0;
}

static void
restore_rel(test_col_rel_mirror_t *rel, uint32_t nrows, uint32_t capacity,
    int64_t **saved, int64_t **backing)
{
    for (uint32_t c = 0; c < rel->ncols; c++) {
        rel->columns[c] = saved[c];
        free(backing[c]);
    }
    rel->nrows = nrows;
    rel->capacity = capacity;
}

/* ================================================================
 * Test 1: arr_build_full must reject a row count whose bucket count
 * overflows to zero, instead of indexing through a NULL ht_head.
 * ================================================================ */
static void
test_build_full_rejects_zero_nbuckets(void)
{
    TEST("arr_build_full: nrows with zero bucket count is rejected");

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(FIXTURE_SRC, &sess, &plan, &prog) == 0,
        "session creation failed");

    test_col_rel_mirror_t *rel = find_rel_mirror(sess, "edge");
    if (!rel) {
        free_session(sess, plan, prog);
        FAIL("relation 'edge' not found");
    }

    uint32_t orig_nrows = rel->nrows;
    uint32_t orig_capacity = rel->capacity;
    int64_t *saved[2] = { NULL, NULL };
    int64_t *backing[2] = { NULL, NULL };
    if (rel->ncols != 2
        || oversize_rel(rel, NROWS_ZERO_BUCKETS, saved, backing) != 0) {
        free_session(sess, plan, prog);
        FAIL("fixture setup failed");
    }

    uint32_t key_cols[] = { 0 };
    col_arrangement_t *arr
        = col_session_get_arrangement(sess, "edge", key_cols, 1);

    restore_rel(rel, orig_nrows, orig_capacity, saved, backing);
    free_session(sess, plan, prog);

    ASSERT(arr == NULL,
        "an unrepresentable row count must not produce an arrangement");
    PASS();
}

/* ================================================================
 * Test 2: arr_update_incremental must reject a row count whose chain
 * array size wraps, instead of writing past a 16-entry allocation.
 *
 * The arrangement is first built at the relation's real size, so
 * arr->nbuckets is the 16-entry floor.  arr_next_pow2(nrows * 2) is
 * also 16 once nrows * 2 wraps to 0, so the load-factor check does
 * not divert to arr_build_full and this path must guard itself.
 * ================================================================ */
static void
test_update_incremental_rejects_wrapped_cap(void)
{
    TEST("arr_update_incremental: nrows with wrapped chain cap is rejected");

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    ASSERT(make_session(FIXTURE_SRC, &sess, &plan, &prog) == 0,
        "session creation failed");

    test_col_rel_mirror_t *rel = find_rel_mirror(sess, "edge");
    if (!rel) {
        free_session(sess, plan, prog);
        FAIL("relation 'edge' not found");
    }

    uint32_t key_cols[] = { 0 };
    col_arrangement_t *arr
        = col_session_get_arrangement(sess, "edge", key_cols, 1);
    if (!arr || arr->nbuckets != 16u) {
        free_session(sess, plan, prog);
        FAIL("baseline arrangement must exist with the 16-bucket floor");
    }

    uint32_t orig_nrows = rel->nrows;
    uint32_t orig_capacity = rel->capacity;
    int64_t *saved[2] = { NULL, NULL };
    int64_t *backing[2] = { NULL, NULL };
    if (rel->ncols != 2
        || oversize_rel(rel, NROWS_CAP_WRAP, saved, backing) != 0) {
        free_session(sess, plan, prog);
        FAIL("fixture setup failed");
    }

    col_arrangement_t *grown
        = col_session_get_arrangement(sess, "edge", key_cols, 1);

    restore_rel(rel, orig_nrows, orig_capacity, saved, backing);
    free_session(sess, plan, prog);

    ASSERT(grown == NULL,
        "an unrepresentable row count must not extend an arrangement");
    PASS();
}

/* ================================================================
 * main
 * ================================================================ */

int
main(void)
{
    printf("\n=== Arrangement next-pow2 Overflow Tests (Issue #1074) ===\n\n");

    test_build_full_rejects_zero_nbuckets();
    test_update_incremental_rejects_wrapped_cap();

    printf("\nResults: %d/%d passed", pass_count, test_count);
    if (fail_count > 0)
        printf(", %d FAILED", fail_count);
    printf("\n\n");

    return fail_count > 0 ? 1 : 0;
}
