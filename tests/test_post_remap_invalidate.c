/*
 * test_post_remap_invalidate.c - Issue #591 acceptance harness
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Verifies wl_handle_remap_invalidate_side_relation_caches:
 *
 *   1. Calls col_session_invalidate_arrangements per side-relation,
 *      confirmed by arr_entries[].arr.indexed_rows being reset to 0
 *      after a populated arrangement is invalidated.
 *
 *   2. Frees + zeroes per-relation dedup_slots without leaking.
 *      The leak avoidance is verified structurally (free() is called
 *      before NULL) and observably (build-san run is ASAN-clean even
 *      after multiple invalidate cycles on the same relation).
 *
 *   3. Skips non-side-relations: a user-named relation registered
 *      alongside the __compound_* table is NOT touched.
 *
 *   4. NULL session is rejected with EINVAL.
 *
 * The test does NOT also call the apply pass; the invalidation
 * helper is independent and the rotation helper (#550-C) calls it
 * after a successful apply.  Calling it standalone here pins the
 * pure-invalidation behaviour without entangling #589/#590 again.
 */

#include "../wirelog/columnar/columnar_nanoarrow.h"
#include "../wirelog/columnar/compound_side.h"
#include "../wirelog/columnar/handle_remap_apply_side.h"
#include "../wirelog/columnar/internal.h"
#include "../wirelog/exec_plan_gen.h"
#include "../wirelog/session.h"
#include "../wirelog/wirelog-parser.h"
#include "../wirelog/wirelog.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Test harness                                                             */
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
            goto cleanup;                     \
        } while (0)

#define ASSERT(cond, msg)                 \
        do {                                  \
            if (!(cond)) {                    \
                FAIL(msg);                    \
            }                                 \
        } while (0)

/* ======================================================================== */
/* Session fixture                                                          */
/* ======================================================================== */

static int
make_session(wl_session_t **out_sess, wl_plan_t **out_plan,
    wirelog_program_t **out_prog)
{
    const char *src =
        ".decl a(x: int32)\n"
        ".decl r(x: int32)\n"
        "r(x) :- a(x).\n";
    wirelog_error_t err;
    wirelog_program_t *prog = wirelog_parse_string(src, &err);
    if (!prog)
        return -1;
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
    *out_sess = sess;
    *out_plan = plan;
    *out_prog = prog;
    return 0;
}

static void
free_session(wl_session_t *sess, wl_plan_t *plan, wirelog_program_t *prog)
{
    if (sess)
        wl_session_destroy(sess);
    if (plan)
        wl_plan_free(plan);
    if (prog)
        wirelog_program_free(prog);
}

/* Locate the arr_entries slot covering @rel_name keyed on the given
 * column.  Returns NULL if no entry exists -- meaning the
 * arrangement has not been built or has been invalidated and freed
 * (which is the case after col_session_invalidate_arrangements
 * because that helper sets indexed_rows=0 but keeps the slot). */
static const col_arr_entry_t *
find_arr_entry(const wl_col_session_t *cs, const char *rel_name,
    uint32_t key_col)
{
    for (uint32_t i = 0; i < cs->arr_count; i++) {
        if (strcmp(cs->arr_entries[i].rel_name, rel_name) != 0)
            continue;
        if (cs->arr_entries[i].key_count != 1u)
            continue;
        if (cs->arr_entries[i].key_cols[0] != key_col)
            continue;
        return &cs->arr_entries[i];
    }
    return NULL;
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static void
test_invalidate_arrangement_and_dedup(void)
{
    TEST("#591: invalidate clears arrangements and frees dedup_slots");

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    if (make_session(&sess, &plan, &prog) != 0) {
        tests_failed++;
        printf(" ... FAIL: make_session\n");
        return;
    }
    wl_col_session_t *cs = (wl_col_session_t *)sess;

    /* Register a side-relation and append a few rows so the
     * arrangement actually has something to index. */
    col_rel_t *rel = NULL;
    int rc = wl_compound_side_ensure(cs, "f", 2u, &rel);
    ASSERT(rc == 0 && rel != NULL, "ensure side rel");
    /* Schema: handle, arg0, arg1.  Append 4 rows. */
    for (uint32_t i = 0; i < 4u; i++) {
        int64_t row[3] = { (int64_t)(i + 1), (int64_t)(10 + i),
                           (int64_t)(100 + i) };
        ASSERT(col_rel_append_row(rel, row) == 0, "append");
    }

    /* Build a column-0 arrangement; this populates arr_entries[]. */
    uint32_t kc[1] = { 0u };
    col_arrangement_t *arr =
        col_session_get_arrangement(sess, rel->name, kc, 1u);
    ASSERT(arr != NULL, "get_arrangement");
    const col_arr_entry_t *e =
        find_arr_entry(cs, rel->name, 0u);
    ASSERT(e != NULL, "arr_entries slot for side rel");
    ASSERT(e->arr.indexed_rows == rel->nrows,
        "arrangement should be fully indexed before invalidate");

    /* Manually allocate dedup_slots so we can prove free() runs.
     * The eval.c lazy-build path normally seeds this; bypassing it
     * keeps the test self-contained.  rel->dedup_cap and
     * dedup_count are set so the helper's reset-to-zero is
     * observable. */
    rel->dedup_cap = 8u;
    rel->dedup_count = 3u;
    rel->dedup_slots = (uint64_t *)calloc(rel->dedup_cap,
            sizeof(uint64_t));
    ASSERT(rel->dedup_slots != NULL, "calloc dedup_slots");
    rel->dedup_slots[0] = 0xDEADBEEFu;

    /* Run the invalidation pass. */
    uint64_t out_rels = 0;
    rc = wl_handle_remap_invalidate_side_relation_caches(cs, &out_rels);
    ASSERT(rc == 0, "invalidate rc");
    ASSERT(out_rels == 1u,
        "exactly one __compound_ relation should be touched");

    /* (1) Arrangement cache cleared. */
    e = find_arr_entry(cs, rel->name, 0u);
    ASSERT(e != NULL, "arr_entries slot retained");
    ASSERT(e->arr.indexed_rows == 0u,
        "indexed_rows must reset to 0 after invalidate");

    /* (2) Dedup slots freed and zeroed.  ASAN/leak detection
     * catches the missing free; the asserts here pin the
     * post-condition. */
    ASSERT(rel->dedup_slots == NULL,
        "dedup_slots must be NULL after invalidate");
    ASSERT(rel->dedup_cap == 0u, "dedup_cap must be 0");
    ASSERT(rel->dedup_count == 0u, "dedup_count must be 0");

    /* Idempotency: a second call must be a no-op (free(NULL) is
     * legal; arrangement helper is idempotent too). */
    rc = wl_handle_remap_invalidate_side_relation_caches(cs, &out_rels);
    ASSERT(rc == 0, "second invalidate rc");
    ASSERT(out_rels == 1u, "second invalidate still finds the side rel");
    ASSERT(rel->dedup_slots == NULL, "dedup_slots stays NULL");

    PASS();
cleanup:
    free_session(sess, plan, prog);
}

static void
test_invalidate_skips_non_side_relations(void)
{
    TEST("#591: non-side relations are not touched by invalidate");

    wl_session_t *sess = NULL;
    wl_plan_t *plan = NULL;
    wirelog_program_t *prog = NULL;
    if (make_session(&sess, &plan, &prog) != 0) {
        tests_failed++;
        printf(" ... FAIL: make_session\n");
        return;
    }
    wl_col_session_t *cs = (wl_col_session_t *)sess;

    /* The session created by make_session() registers EDB rel "a"
     * and IDB rel "r" -- neither prefixed __compound_.  Plant
     * a fake dedup_slots on one of them and verify the invalidate
     * pass leaves it alone. */
    col_rel_t *user_rel = session_find_rel(cs, "a");
    ASSERT(user_rel != NULL, "find EDB rel 'a'");
    user_rel->dedup_cap = 4u;
    user_rel->dedup_count = 2u;
    user_rel->dedup_slots
        = (uint64_t *)calloc(user_rel->dedup_cap, sizeof(uint64_t));
    ASSERT(user_rel->dedup_slots != NULL, "calloc plant");
    uint64_t *planted = user_rel->dedup_slots;

    uint64_t out_rels = 0;
    int rc = wl_handle_remap_invalidate_side_relation_caches(cs, &out_rels);
    ASSERT(rc == 0, "rc");
    ASSERT(out_rels == 0u, "no __compound_ relations -> 0");

    ASSERT(user_rel->dedup_slots == planted,
        "user-rel dedup_slots must NOT be freed");
    ASSERT(user_rel->dedup_cap == 4u, "user-rel dedup_cap preserved");
    ASSERT(user_rel->dedup_count == 2u,
        "user-rel dedup_count preserved");

    /* Tear-down: free our plant before destroy so the session's
     * usual free path doesn't hit a now-disowned pointer. */
    free(user_rel->dedup_slots);
    user_rel->dedup_slots = NULL;
    user_rel->dedup_cap = 0;
    user_rel->dedup_count = 0;

    PASS();
cleanup:
    if (user_rel) {
        free(user_rel->dedup_slots);
        user_rel->dedup_slots = NULL;
        user_rel->dedup_cap = 0;
        user_rel->dedup_count = 0;
    }
    free_session(sess, plan, prog);
}

static void
test_invalidate_einval(void)
{
    TEST("#591: NULL session rejected with EINVAL");
    uint64_t out = 99u;
    int rc =
        wl_handle_remap_invalidate_side_relation_caches(NULL, &out);
    if (rc != EINVAL) {
        tests_failed++;
        printf(" ... FAIL: NULL sess not rejected\n");
        return;
    }
    if (out != 0u) {
        tests_failed++;
        printf(" ... FAIL: out_rels_invalidated must be zeroed on EINVAL\n");
        return;
    }
    PASS();
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_post_remap_invalidate (Issue #591)\n");
    printf("=======================================\n");

    test_invalidate_arrangement_and_dedup();
    test_invalidate_skips_non_side_relations();
    test_invalidate_einval();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
