/*
 * tests/test_inline_compound_wiring.c - FILTER/PROJECT/LFTJ wiring unit tests
 * (Issue #534 Task #1).
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * These tests exercise the inline-tier resolver / comparator / copy
 * helpers added in Task #1 of Issue #534 ("FILTER/PROJECT/LFTJ wiring
 * for inline compound columns"). They cover three surfaces:
 *
 *   test_locate_translates_logical_to_physical
 *       wl_col_rel_inline_locate returns (offset, width) matching the
 *       prefix-sum of compound_arity_map. Catches schema drift between
 *       ops.c (physical index) and IR lowering (logical index).
 *
 *   test_filter_inline_compound_equals
 *       wl_col_rel_inline_compound_equals reports exact N-tuple match;
 *       rejects arity mismatch, row-out-of-range, and per-arg differences.
 *       Mirrors the hot path `p(_, f(a,b), _)` authorization predicate.
 *
 *   test_filter_multiplicity_preserved
 *       Invariant #1 (Z-set): when FILTER copies a matching row into its
 *       output relation, the copy uses col_rel_append_row so the Z-set
 *       multiplicity encoded by the timestamps/delta layer stays intact.
 *       We verify the output carries inherited compound metadata so a
 *       downstream consolidation pass can still address the inline slots.
 *
 *   test_project_inline_copies_full_arity
 *       wl_col_rel_inline_project_column copies the entire arity range
 *       between matching logical columns; refuses width-mismatched dst.
 *
 *   test_lftj_arg_physical_col_resolution
 *       wl_col_rel_inline_arg_physical_col returns the physical col for
 *       the i-th argument of an inline compound, allowing LFTJ callers
 *       to join on any single inline argument. Rejects arg_idx out of
 *       range with EINVAL (no silent clamp).
 *
 *   test_lftj_join_on_inline_arg_multiplicity
 *       End-to-end Z-set check (Invariant #1): joining two relations on
 *       an inline compound argument yields an output whose row count
 *       equals Σ(outer_mult_key * inner_mult_key) for each matching key.
 *       Here we use multiplicity = 1 for every row so the expected count
 *       simplifies to outer_matches × inner_matches per key.
 *
 * K-Fusion isolation (Invariant #4): every test uses freshly allocated
 * relations per test, never shares buffers across workers, and the dst
 * relations in project/LFTJ are owned by the test body — this mirrors
 * the per-worker ownership contract validated by Task #2.
 *
 * Invariant audit (5-Invariant Checklist — Issue #530 template):
 *   #1 Timely-Differential (Z-set)      — see test_filter_multiplicity_preserved
 *                                          and test_lftj_join_on_inline_arg_multiplicity.
 *   #2 Pure C11                         — no VLAs, no GNU extensions, -Wpedantic clean.
 *   #3 Columnar Storage / SIMD          — helpers access columns[col][row] via the
 *                                          accessor layer (col_rel_get/col_rel_set);
 *                                          they add no indirection in the hot path.
 *   #4 K-Fusion isolation               — see block comment above.
 *   #5 Backend Abstraction              — helpers use only col_rel_t API; no backend-
 *                                          specific calls are introduced.
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

/* Build a (scalar, inline/<arity>, scalar) relation pre-populated with
 * `nrows` zero rows. Logical schema -> (k, f/arity, g). Physical ncols =
 * 1 + arity + 1. Returns 0 on success, -1 on any failure; *out_rel holds
 * the fresh relation (caller owns). */
static int
build_three_col_fixture(col_rel_t **out_rel, const char *name, uint32_t arity,
    uint32_t nrows)
{
    col_rel_t *rel = NULL;
    if (col_rel_alloc(&rel, name) != 0 || !rel)
        return -1;

    const col_rel_logical_col_t cols[3] = {
        { WIRELOG_COMPOUND_KIND_NONE,   0u,    0u },
        { WIRELOG_COMPOUND_KIND_INLINE, arity, 1u },
        { WIRELOG_COMPOUND_KIND_NONE,   0u,    0u },
    };
    if (col_rel_apply_compound_schema(rel, cols, 3) != 0) {
        col_rel_destroy(rel);
        return -1;
    }

    const uint32_t physical = 1u + arity + 1u;
    const char *names[6] = { "k", "a0", "a1", "a2", "a3", "g" };
    if (col_rel_set_schema(rel, physical, names) != 0) {
        col_rel_destroy(rel);
        return -1;
    }

    int64_t zero[6] = { 0 };
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
/* locate: logical -> (offset, width) translation                           */
/* ======================================================================== */

static void
test_locate_translates_logical_to_physical(void)
{
    TEST("locate returns prefix-sum offset + width");
    col_rel_t *rel = NULL;
    if (build_three_col_fixture(&rel, "locate", 3u, 1u) != 0)
        FAIL("fixture failed");

    uint32_t offset = 0xDEADBEEFu;
    uint32_t width = 0xDEADBEEFu;

    /* Logical 0 -> scalar -> offset=0, width=1. */
    ASSERT(wl_col_rel_inline_locate(rel, 0u, &offset, &width) == 0,
        "locate(0) error");
    ASSERT(offset == 0u && width == 1u, "locate(0) mismatch");

    /* Logical 1 -> inline/3 -> offset=1, width=3. */
    ASSERT(wl_col_rel_inline_locate(rel, 1u, &offset, &width) == 0,
        "locate(1) error");
    ASSERT(offset == 1u && width == 3u, "locate(1) mismatch");

    /* Logical 2 -> scalar -> offset=4, width=1. */
    ASSERT(wl_col_rel_inline_locate(rel, 2u, &offset, &width) == 0,
        "locate(2) error");
    ASSERT(offset == 4u && width == 1u, "locate(2) mismatch");

    /* Logical col 99 is out of range -> EINVAL, outputs untouched. */
    offset = 0xDEADBEEFu;
    width = 0xDEADBEEFu;
    int rc = wl_col_rel_inline_locate(rel, 99u, &offset, &width);
    ASSERT(rc == EINVAL, "locate(99) should fail EINVAL");

    /* NULL output pointers rejected. */
    ASSERT(wl_col_rel_inline_locate(rel, 1u, NULL, &width) == EINVAL,
        "locate NULL offset should fail");
    ASSERT(wl_col_rel_inline_locate(rel, 1u, &offset, NULL) == EINVAL,
        "locate NULL width should fail");
    ASSERT(wl_col_rel_inline_locate(NULL, 0u, &offset, &width) == EINVAL,
        "locate NULL rel should fail");

    PASS();
cleanup:
    if (rel) col_rel_destroy(rel);
}

/* ======================================================================== */
/* FILTER: inline compound equality predicate                               */
/* ======================================================================== */

static void
test_filter_inline_compound_equals(void)
{
    TEST("FILTER: compound_equals matches N-tuple constants");
    col_rel_t *rel = NULL;
    if (build_three_col_fixture(&rel, "filter_eq", 2u, 4u) != 0)
        FAIL("fixture failed");

    /* Populate:
     *   row 0: f(1, 2)
     *   row 1: f(1, 3)
     *   row 2: f(10, 20)
     *   row 3: f(1, 2)     — duplicate of row 0
     */
    const int64_t r0[2] = { 1, 2 };
    const int64_t r1[2] = { 1, 3 };
    const int64_t r2[2] = { 10, 20 };
    const int64_t r3[2] = { 1, 2 };
    ASSERT(wl_col_rel_store_inline_compound(rel, 0u, 1u, r0, 2u) == 0, "s0");
    ASSERT(wl_col_rel_store_inline_compound(rel, 1u, 1u, r1, 2u) == 0, "s1");
    ASSERT(wl_col_rel_store_inline_compound(rel, 2u, 1u, r2, 2u) == 0, "s2");
    ASSERT(wl_col_rel_store_inline_compound(rel, 3u, 1u, r3, 2u) == 0, "s3");

    const int64_t needle[2] = { 1, 2 };
    ASSERT(wl_col_rel_inline_compound_equals(rel, 0u, 1u, needle, 2u) != 0,
        "row 0 should match");
    ASSERT(wl_col_rel_inline_compound_equals(rel, 1u, 1u, needle, 2u) == 0,
        "row 1 must NOT match (arg1 differs)");
    ASSERT(wl_col_rel_inline_compound_equals(rel, 2u, 1u, needle, 2u) == 0,
        "row 2 must NOT match (both differ)");
    ASSERT(wl_col_rel_inline_compound_equals(rel, 3u, 1u, needle, 2u) != 0,
        "row 3 should match (duplicate of row 0)");

    /* Arity mismatch — always false. */
    const int64_t needle3[3] = { 1, 2, 3 };
    ASSERT(wl_col_rel_inline_compound_equals(rel, 0u, 1u, needle3, 3u) == 0,
        "arity-3 needle must NOT match arity-2 compound");

    /* Row OOR — false. */
    ASSERT(wl_col_rel_inline_compound_equals(rel, 99u, 1u, needle, 2u) == 0,
        "OOR row must NOT match");

    /* NULL input — false. */
    ASSERT(wl_col_rel_inline_compound_equals(rel, 0u, 1u, NULL, 2u) == 0,
        "NULL expect must NOT match");

    PASS();
cleanup:
    if (rel) col_rel_destroy(rel);
}

/* Verifies the Z-set multiplicity invariant: when FILTER produces an output
 * via col_rel_pool_new_like / col_rel_new_like and then col_rel_append_row,
 * each selected source row contributes exactly +1 to the output (mirroring
 * the Delta-semantics of col_delta_timestamp_t). Since our test does not
 * spin up a session/timestamps layer, the proof is structural:
 *   (a) the output relation inherits compound_kind/arity_map so the
 *       downstream consolidation pass can still address inline slots, and
 *   (b) inline args round-trip through retrieve after filtering.
 */
static void
test_filter_multiplicity_preserved(void)
{
    TEST("FILTER: output inherits INLINE schema + row multiplicity preserved");
    col_rel_t *src = NULL;
    col_rel_t *out = NULL;

    if (build_three_col_fixture(&src, "filter_src", 2u, 3u) != 0)
        FAIL("fixture failed");

    /* Populate: f(1,2), f(1,2), f(9,9).  The first two are duplicates so
     * post-filter Z-set multiplicity should be 2 if we count copies. */
    const int64_t p[2] = { 1, 2 };
    ASSERT(wl_col_rel_store_inline_compound(src, 0u, 1u, p, 2u) == 0, "s0");
    ASSERT(wl_col_rel_store_inline_compound(src, 1u, 1u, p, 2u) == 0, "s1");
    const int64_t q[2] = { 9, 9 };
    ASSERT(wl_col_rel_store_inline_compound(src, 2u, 1u, q, 2u) == 0, "s2");

    /* Allocate output via col_rel_new_like — it must inherit compound
     * metadata so the inline slots remain addressable downstream. */
    out = col_rel_new_like("$filter_out", src);
    ASSERT(out != NULL, "new_like failed");
    ASSERT(out->compound_kind == WIRELOG_COMPOUND_KIND_INLINE,
        "output compound_kind not INLINE");
    ASSERT(out->compound_count == 1u, "output compound_count wrong");
    ASSERT(out->inline_physical_offset == 1u,
        "output inline_physical_offset wrong");
    ASSERT(out->compound_arity_map != NULL, "output arity_map NULL");
    ASSERT(out->compound_arity_map[0] == 1u, "arity_map[0] != 1");
    ASSERT(out->compound_arity_map[1] == 2u, "arity_map[1] != 2");
    ASSERT(out->compound_arity_map[2] == 1u, "arity_map[2] != 1");

    /* Simulate the FILTER hot path: for each row matching f(1,2), copy
     * the full physical row into out. The # of output rows equals the
     * Z-set multiplicity for key f(1,2). */
    const int64_t needle[2] = { 1, 2 };
    int64_t row_buf[4] = { 0 };
    for (uint32_t r = 0; r < src->nrows; r++) {
        if (wl_col_rel_inline_compound_equals(src, r, 1u, needle, 2u)) {
            col_rel_row_copy_out(src, r, row_buf);
            ASSERT(col_rel_append_row(out, row_buf) == 0, "append_row");
        }
    }

    ASSERT(out->nrows == 2u,
        "multiplicity wrong: expected 2 matches for f(1,2)");

    /* Round-trip: the first two output rows must retrieve f(1,2). */
    int64_t got[2] = { 0 };
    ASSERT(wl_col_rel_retrieve_inline_compound(out, 0u, 1u, got, 2u) == 0,
        "retrieve out[0]");
    ASSERT(got[0] == 1 && got[1] == 2, "out[0] inline args wrong");
    ASSERT(wl_col_rel_retrieve_inline_compound(out, 1u, 1u, got, 2u) == 0,
        "retrieve out[1]");
    ASSERT(got[0] == 1 && got[1] == 2, "out[1] inline args wrong");

    PASS();
cleanup:
    if (out) col_rel_destroy(out);
    if (src) col_rel_destroy(src);
}

/* ======================================================================== */
/* PROJECT: whole-arity copy between like-schema relations                  */
/* ======================================================================== */

static void
test_project_inline_copies_full_arity(void)
{
    TEST("PROJECT: project_column copies all arity slots");
    col_rel_t *src = NULL;
    col_rel_t *dst = NULL;

    if (build_three_col_fixture(&src, "project_src", 3u, 2u) != 0)
        FAIL("src fixture failed");

    /* Pre-fill src rows with distinguishable payload. */
    const int64_t r0[3] = { 11, 22, 33 };
    const int64_t r1[3] = { 44, 55, 66 };
    ASSERT(wl_col_rel_store_inline_compound(src, 0u, 1u, r0, 3u) == 0, "s0");
    ASSERT(wl_col_rel_store_inline_compound(src, 1u, 1u, r1, 3u) == 0, "s1");

    /* Build dst with matching schema (arity=3) and pre-extend nrows so the
     * project helper has an addressable target row. The direct allocation
     * route (build_three_col_fixture) gives us a dst with nrows>=N. */
    if (build_three_col_fixture(&dst, "project_dst", 3u, 2u) != 0)
        FAIL("dst fixture failed");

    ASSERT(wl_col_rel_inline_project_column(dst, 0u, src, 1u, 1u) == 0,
        "project src[1] -> dst[0]");
    ASSERT(wl_col_rel_inline_project_column(dst, 1u, src, 0u, 1u) == 0,
        "project src[0] -> dst[1]");

    int64_t got[3] = { 0 };
    ASSERT(wl_col_rel_retrieve_inline_compound(dst, 0u, 1u, got, 3u) == 0,
        "retrieve dst[0]");
    ASSERT(got[0] == 44 && got[1] == 55 && got[2] == 66,
        "dst[0] not r1");
    ASSERT(wl_col_rel_retrieve_inline_compound(dst, 1u, 1u, got, 3u) == 0,
        "retrieve dst[1]");
    ASSERT(got[0] == 11 && got[1] == 22 && got[2] == 33,
        "dst[1] not r0");

    /* Width-mismatch dst (arity 2) must be rejected. */
    col_rel_t *narrow = NULL;
    if (build_three_col_fixture(&narrow, "project_narrow", 2u, 1u) == 0) {
        int rc = wl_col_rel_inline_project_column(narrow, 0u, src, 0u, 1u);
        ASSERT(rc == EINVAL, "width-mismatch dst must fail EINVAL");
        col_rel_destroy(narrow);
    }

    /* Row OOR rejected. */
    ASSERT(wl_col_rel_inline_project_column(dst, 99u, src, 0u, 1u) == EINVAL,
        "OOR dst_row must fail EINVAL");
    ASSERT(wl_col_rel_inline_project_column(dst, 0u, src, 99u, 1u) == EINVAL,
        "OOR src_row must fail EINVAL");

    /* NULL args rejected. */
    ASSERT(wl_col_rel_inline_project_column(NULL, 0u, src, 0u, 1u) == EINVAL,
        "NULL dst must fail EINVAL");
    ASSERT(wl_col_rel_inline_project_column(dst, 0u, NULL, 0u, 1u) == EINVAL,
        "NULL src must fail EINVAL");

    PASS();
cleanup:
    if (dst) col_rel_destroy(dst);
    if (src) col_rel_destroy(src);
}

/* ======================================================================== */
/* LFTJ: inline arg -> physical col resolution                              */
/* ======================================================================== */

static void
test_lftj_arg_physical_col_resolution(void)
{
    TEST("LFTJ: arg_physical_col returns offset + arg_idx");
    col_rel_t *rel = NULL;
    if (build_three_col_fixture(&rel, "lftj_arg", 4u, 1u) != 0)
        FAIL("fixture failed");
    /* Schema: [k, a0, a1, a2, a3, g] -> inline@1 occupies physical 1..4. */

    uint32_t physical = 0u;
    ASSERT(wl_col_rel_inline_arg_physical_col(rel, 1u, 0u, &physical) == 0,
        "arg 0 resolve");
    ASSERT(physical == 1u, "arg 0 -> physical 1");
    ASSERT(wl_col_rel_inline_arg_physical_col(rel, 1u, 2u, &physical) == 0,
        "arg 2 resolve");
    ASSERT(physical == 3u, "arg 2 -> physical 3");
    ASSERT(wl_col_rel_inline_arg_physical_col(rel, 1u, 3u, &physical) == 0,
        "arg 3 resolve");
    ASSERT(physical == 4u, "arg 3 -> physical 4");

    /* arg_idx >= width must fail. */
    ASSERT(wl_col_rel_inline_arg_physical_col(rel, 1u, 4u, &physical)
        == EINVAL, "arg 4 must fail EINVAL");
    ASSERT(wl_col_rel_inline_arg_physical_col(rel, 1u, 99u, &physical)
        == EINVAL, "arg 99 must fail EINVAL");

    /* NULL out fails. */
    ASSERT(wl_col_rel_inline_arg_physical_col(rel, 1u, 0u, NULL) == EINVAL,
        "NULL out must fail");

    PASS();
cleanup:
    if (rel) col_rel_destroy(rel);
}

/* End-to-end multiplicity check: build two relations, join them on a
 * single inline compound argument via a hand-rolled nested loop (standing
 * in for the LFTJ cross-product emission), and verify that the output
 * row count equals Σ(outer_mult × inner_mult) per matching key. With
 * unit multiplicity this reduces to the Cartesian product of matching
 * groups — the classic Z-set join semantics. */
static void
test_lftj_join_on_inline_arg_multiplicity(void)
{
    TEST("LFTJ: join on inline arg yields outer × inner multiplicity");
    col_rel_t *left = NULL;
    col_rel_t *right = NULL;

    if (build_three_col_fixture(&left, "lftj_left", 2u, 4u) != 0)
        FAIL("left fixture failed");
    if (build_three_col_fixture(&right, "lftj_right", 2u, 4u) != 0)
        FAIL("right fixture failed");

    /* left: arg0 cycle 1,2,1,2 -> key 1 appears twice, key 2 appears twice.
     * right: arg0 cycle 1,1,2,3 -> key 1 twice, key 2 once, key 3 once.
     * Join on arg0 of the inline compound (logical col 1, arg 0).
     * Expected per-key matches:
     *   key 1: 2 × 2 = 4
     *   key 2: 2 × 1 = 2
     *   key 3: 0 × 1 = 0
     * Expected total multiplicity = 6.
     */
    const int64_t l_args[4][2] = { {1, 10}, {2, 20}, {1, 11}, {2, 21} };
    const int64_t r_args[4][2] = { {1, 100}, {1, 101}, {2, 200}, {3, 300} };
    for (uint32_t i = 0; i < 4u; i++) {
        ASSERT(wl_col_rel_store_inline_compound(left, i, 1u, l_args[i], 2u)
            == 0, "l store");
        ASSERT(wl_col_rel_store_inline_compound(right, i, 1u, r_args[i], 2u)
            == 0, "r store");
    }

    uint32_t left_key_col = 0u;
    uint32_t right_key_col = 0u;
    ASSERT(wl_col_rel_inline_arg_physical_col(left, 1u, 0u, &left_key_col)
        == 0, "resolve left key");
    ASSERT(wl_col_rel_inline_arg_physical_col(right, 1u, 0u, &right_key_col)
        == 0, "resolve right key");
    ASSERT(left_key_col == 1u && right_key_col == 1u, "key cols wrong");

    /* Nested-loop join using the resolved physical columns. Under LFTJ,
     * the same keys would be co-iterated via leapfrog seek; the result
     * multiplicity is identical modulo row emission order. */
    uint32_t total = 0u;
    for (uint32_t li = 0; li < left->nrows; li++) {
        int64_t lk = col_rel_get(left, li, left_key_col);
        for (uint32_t ri = 0; ri < right->nrows; ri++) {
            int64_t rk = col_rel_get(right, ri, right_key_col);
            if (lk == rk)
                total++;
        }
    }
    ASSERT(total == 6u, "join multiplicity != 6");

    PASS();
cleanup:
    if (right) col_rel_destroy(right);
    if (left) col_rel_destroy(left);
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf("test_inline_compound_wiring (Issue #534 Task #1)\n");
    printf("=================================================\n");

    test_locate_translates_logical_to_physical();
    test_filter_inline_compound_equals();
    test_filter_multiplicity_preserved();
    test_project_inline_copies_full_arity();
    test_lftj_arg_physical_col_resolution();
    test_lftj_join_on_inline_arg_multiplicity();

    printf("\nResults: %d/%d passed, %d failed\n",
        tests_passed, tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
