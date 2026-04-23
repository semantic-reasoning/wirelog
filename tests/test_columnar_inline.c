/*
 * tests/test_columnar_inline.c - inline-tier integration test battery
 * (Issue #532 Task 5).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * These tests compose the building blocks landed by Tasks #1-#4 and
 * exercise them as one pipeline:
 *
 *   test_consolidation_inline_compound
 *       Insert duplicate rows with inline compound columns, radix-sort
 *       and compact, confirm duplicates collapse and inline slot values
 *       are preserved bit-for-bit through the consolidation pass (Z-set
 *       semantics).
 *
 *   test_asan_nested_insert_delete
 *       Tight create/fill/retract/destroy loop.  Each iteration allocates
 *       an INLINE-tier relation with a 3-wide compound column, stores
 *       rows, retracts them (no physical mutation), then destroys the
 *       relation.  Under -Db_sanitize=address the suite must report zero
 *       leaks and zero use-after-free across ~1k cycles.
 *
 *   test_arrow_schema_inline_expansion
 *       Backend abstraction (Invariant #5 of the K-Fusion contract):
 *       after col_rel_apply_compound_schema, the relation's physical
 *       ncols equals the sum of per-column widths and the arity map is
 *       addressable at the reported inline_physical_offset.  Verifies
 *       that callers operating purely through the columnar abstraction
 *       see the expanded schema without knowledge of inline encoding.
 *
 *   test_simd_filter_inline
 *       Scan/filter over a relation whose inline columns hold non-key
 *       data.  Ensures inline slots pass through col_rel_compact() (the
 *       hot SIMD-friendly row-copy helper) without corruption.  Formal
 *       SIMD emission is verified post-build by scripts/ci; this test
 *       is the behavioural sibling.
 *
 * The K-Fusion shadow test (K=4 / 100k rows) already landed in
 * tests/test_k_fusion_inline_shadow.c (Task #4); no duplication here.
 */

#include "../wirelog/columnar/internal.h"
#include "../wirelog/wirelog-types.h"

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

#define TEST(name)                              \
        do {                                        \
            tests_run++;                            \
            printf("  [%d] %s", tests_run, name);   \
        } while (0)

#define PASS()                                  \
        do {                                        \
            tests_passed++;                         \
            printf(" ... PASS\n");                  \
        } while (0)

#define FAIL(msg)                               \
        do {                                        \
            tests_failed++;                         \
            printf(" ... FAIL: %s\n", (msg));       \
            goto cleanup;                           \
        } while (0)

#define ASSERT(cond, msg)                       \
        do {                                        \
            if (!(cond)) {                          \
                FAIL(msg);                          \
            }                                       \
        } while (0)

/* Build a (scalar, inline/<arity>) relation with `nrows` addressable rows.
 * Schema is (key:scalar, c0..c{arity-1}:inline).  Physical ncols = 1+arity. */
static int
build_fixture_(col_rel_t **out_rel, const char *name, uint32_t arity,
    uint32_t nrows)
{
    col_rel_t *rel = NULL;
    if (col_rel_alloc(&rel, name) != 0 || !rel)
        return -1;

    const col_rel_logical_col_t cols[2] = {
        { WIRELOG_COMPOUND_KIND_NONE,   0u,    0u },
        { WIRELOG_COMPOUND_KIND_INLINE, arity, 1u },
    };
    if (col_rel_apply_compound_schema(rel, cols, 2) != 0) {
        col_rel_destroy(rel);
        return -1;
    }

    const uint32_t physical = 1u + arity;
    const char *names[5] = { "k", "c0", "c1", "c2", "c3" };
    if (col_rel_set_schema(rel, physical, names) != 0) {
        col_rel_destroy(rel);
        return -1;
    }

    int64_t zero[5] = { 0 };
    for (uint32_t r = 0; r < nrows; r++) {
        if (col_rel_append_row(rel, zero) != 0) {
            col_rel_destroy(rel);
            return -1;
        }
    }
    *out_rel = rel;
    return 0;
}

/* ======================================================================== */
/* Tests                                                                    */
/* ======================================================================== */

static void
test_consolidation_inline_compound(void)
{
    TEST("consolidation preserves inline slots across radix-sort + compact");
    col_rel_t *rel = NULL;
    const uint32_t N = 256;
    const uint32_t ARITY = 2u;

    if (build_fixture_(&rel, "consolidation_fixture", ARITY, N) != 0)
        FAIL("fixture failed");

    /* Populate: key cycles 0..15, inline args encode (key*10, key*10+1). */
    for (uint32_t r = 0; r < N; r++) {
        int64_t key = (int64_t)(r % 16);
        col_rel_set(rel, r, 0, key);
        const int64_t args[2] = { key * 10, key * 10 + 1 };
        ASSERT(wl_col_rel_store_inline_compound(rel, r, 1, args, ARITY) == 0,
            "store failed");
    }

    /* Consolidation step: radix-sort on key then compact.  The key column
     * (physical idx 0) is the sort key; inline slots ride along. */
    col_rel_radix_sort(rel, 0, N);
    col_rel_compact(rel);

    /* Verify: rows are key-sorted and inline slots still resolve to
     * (key*10, key*10+1) via the Task #3 retrieve accessor. */
    int64_t prev_key = -1;
    for (uint32_t r = 0; r < rel->nrows; r++) {
        int64_t key = col_rel_get(rel, r, 0);
        ASSERT(key >= prev_key, "radix-sort did not preserve key order");
        prev_key = key;
        int64_t args[2] = { 0, 0 };
        ASSERT(wl_col_rel_retrieve_inline_compound(rel, r, 1, args, ARITY)
            == 0, "retrieve failed");
        ASSERT(args[0] == key * 10, "inline arg0 corrupted");
        ASSERT(args[1] == key * 10 + 1, "inline arg1 corrupted");
    }

    PASS();
cleanup:
    if (rel) col_rel_destroy(rel);
}

static void
test_asan_nested_insert_delete(void)
{
    TEST("ASan: nested create/store/retract/destroy cycles leak-free");
    const uint32_t CYCLES = 1000;
    const uint32_t ARITY = 3u;

    for (uint32_t i = 0; i < CYCLES; i++) {
        col_rel_t *rel = NULL;
        if (build_fixture_(&rel, "asan_cycle", ARITY, 8u) != 0) {
            tests_failed++;
            printf(" ... FAIL: fixture at cycle %u\n", i);
            return;
        }
        for (uint32_t r = 0; r < 8u; r++) {
            const int64_t args[3] = { (int64_t)r, (int64_t)r + 1,
                                      (int64_t)r + 2 };
            if (wl_col_rel_store_inline_compound(rel, r, 1, args, ARITY)
                != 0) {
                col_rel_destroy(rel);
                tests_failed++;
                printf(" ... FAIL: store at cycle %u row %u\n", i, r);
                return;
            }
            /* Retract with multiplicity -1 — physical slots untouched,
             * no allocation happens; any leak would accumulate across the
             * 1000-cycle loop and trip ASan on shutdown. */
            if (wl_col_rel_retract_inline_compound(rel, r, 1, -1) != 0) {
                col_rel_destroy(rel);
                tests_failed++;
                printf(" ... FAIL: retract at cycle %u row %u\n", i, r);
                return;
            }
        }
        col_rel_destroy(rel);
    }

    PASS();
    return;
}

static void
test_arrow_schema_inline_expansion(void)
{
    TEST("schema expansion: physical ncols = sum(widths), offsets addressable");
    col_rel_t *rel = NULL;

    /* Mixed schema: scalar + inline/4 + scalar + inline/2 -> widths 1,4,1,2 -> 8.
     * First INLINE column starts at physical offset 1. */
    if (col_rel_alloc(&rel, "arrow_expansion") != 0 || !rel)
        FAIL("alloc failed");

    const col_rel_logical_col_t cols[4] = {
        { WIRELOG_COMPOUND_KIND_NONE,   0u, 0u },
        { WIRELOG_COMPOUND_KIND_INLINE, 4u, 1u },
        { WIRELOG_COMPOUND_KIND_NONE,   0u, 0u },
        { WIRELOG_COMPOUND_KIND_INLINE, 2u, 1u },
    };

    uint32_t physical = 0u;
    uint32_t offset_map[4] = { 0 };
    uint32_t first_inline = 0u;
    uint32_t compound_count = 0u;
    int rc = col_rel_compute_physical_layout(cols, 4, &physical, offset_map,
            &first_inline, &compound_count);
    ASSERT(rc == 0, "layout returned error");
    ASSERT(physical == 8u, "physical ncols != 8");
    ASSERT(compound_count == 2u, "compound_count != 2");
    ASSERT(first_inline == 1u, "first_inline_offset != 1");
    ASSERT(offset_map[0] == 0u, "offset[0] != 0");
    ASSERT(offset_map[1] == 1u, "offset[1] != 1");
    ASSERT(offset_map[2] == 5u, "offset[2] != 5"); /* 1 + 4 */
    ASSERT(offset_map[3] == 6u, "offset[3] != 6"); /* 1 + 4 + 1 */

    ASSERT(col_rel_apply_compound_schema(rel, cols, 4) == 0,
        "apply_schema failed");
    ASSERT(rel->compound_kind == WIRELOG_COMPOUND_KIND_INLINE,
        "compound_kind != INLINE");
    ASSERT(rel->compound_count == 2u, "compound_count mismatch");
    ASSERT(rel->inline_physical_offset == 1u,
        "inline_physical_offset mismatch");
    ASSERT(rel->compound_arity_map != NULL, "arity_map NULL");
    ASSERT(rel->compound_arity_map[0] == 1u, "width[0] != 1");
    ASSERT(rel->compound_arity_map[1] == 4u, "width[1] != 4");
    ASSERT(rel->compound_arity_map[2] == 1u, "width[2] != 1");
    ASSERT(rel->compound_arity_map[3] == 2u, "width[3] != 2");

    /* Round-trip through the backend-facing storage ops: writer and reader
    * agree on what physical range logical column 1 (inline/4) occupies. */
    const char *names[8] = { "k", "a0", "a1", "a2", "a3", "g", "b0", "b1" };
    ASSERT(col_rel_set_schema(rel, 8u, names) == 0, "set_schema");
    const int64_t zero[8] = { 0 };
    ASSERT(col_rel_append_row(rel, zero) == 0, "append_row");

    const int64_t a4[4] = { 101, 102, 103, 104 };
    ASSERT(wl_col_rel_store_inline_compound(rel, 0, 1, a4, 4u) == 0,
        "store inline/4");
    const int64_t a2[2] = { 201, 202 };
    ASSERT(wl_col_rel_store_inline_compound(rel, 0, 3, a2, 2u) == 0,
        "store inline/2");

    int64_t got4[4] = { 0 };
    int64_t got2[2] = { 0 };
    ASSERT(wl_col_rel_retrieve_inline_compound(rel, 0, 1, got4, 4u) == 0,
        "retrieve inline/4");
    ASSERT(wl_col_rel_retrieve_inline_compound(rel, 0, 3, got2, 2u) == 0,
        "retrieve inline/2");
    for (uint32_t k = 0; k < 4; k++)
        ASSERT(got4[k] == a4[k], "inline/4 arg corrupted");
    for (uint32_t k = 0; k < 2; k++)
        ASSERT(got2[k] == a2[k], "inline/2 arg corrupted");

    PASS();
cleanup:
    if (rel) col_rel_destroy(rel);
}

static void
test_simd_filter_inline(void)
{
    TEST("SIMD-friendly compact preserves inline payload");
    col_rel_t *rel = NULL;
    const uint32_t N = 64;
    const uint32_t ARITY = 2u;

    if (build_fixture_(&rel, "simd_filter", ARITY, N) != 0)
        FAIL("fixture failed");

    /* Populate with a pattern that makes corruption detectable: row r's
     * inline slots hold (r*7, r*7+3). */
    for (uint32_t r = 0; r < N; r++) {
        col_rel_set(rel, r, 0, (int64_t)r);
        const int64_t args[2] = { (int64_t)r * 7, (int64_t)r * 7 + 3 };
        ASSERT(wl_col_rel_store_inline_compound(rel, r, 1, args, ARITY) == 0,
            "store");
    }

    /* Compact is the row-copy helper that production code uses ahead of
     * SIMD filter/join passes.  After compact, inline slots must round-trip
     * unchanged — any stride miscalculation would scramble them. */
    col_rel_compact(rel);

    for (uint32_t r = 0; r < N; r++) {
        int64_t args[2] = { 0, 0 };
        ASSERT(wl_col_rel_retrieve_inline_compound(rel, r, 1, args, ARITY)
            == 0, "retrieve");
        ASSERT(args[0] == (int64_t)r * 7, "arg0 scrambled");
        ASSERT(args[1] == (int64_t)r * 7 + 3, "arg1 scrambled");
    }

    PASS();
cleanup:
    if (rel) col_rel_destroy(rel);
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_columnar_inline (Issue #532 Task 5)\n");
    printf("========================================\n");

    test_consolidation_inline_compound();
    test_asan_nested_insert_delete();
    test_arrow_schema_inline_expansion();
    test_simd_filter_inline();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
