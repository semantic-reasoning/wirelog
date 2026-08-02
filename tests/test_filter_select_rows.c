/*
 * tests/test_filter_select_rows.c - column-native filter kernel equivalence
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * col_filter_select_rows() has a scalar body and, on AVX2 targets, a
 * vectorized body that processes 8 rows per iteration and left-packs the
 * surviving row indices through a 256-entry lookup table.  The two must
 * agree exactly, for every opcode and every selection pattern.
 *
 * This is a differential test: a deliberately naive reference implementation
 * lives here, and every case compares the kernel against it.  The reference
 * never changes when the kernel is tuned, so it stays an independent oracle.
 *
 *   test_all_leftpack_masks
 *       Drives all 256 distinct 8-row pass/fail patterns.  On AVX2 builds
 *       each one selects a different left-pack LUT entry, so this covers
 *       the table exhaustively rather than sampling it.
 *
 *   test_opcodes_and_lengths
 *       All six opcodes across row counts that straddle the 8-row vector
 *       boundary (0,1,7,8,9,15,16,17,...), both const-compare and
 *       column-compare forms.
 *
 *   test_extreme_values
 *       INT64_MIN/INT64_MAX/negatives, where a signed compare implemented
 *       with _mm256_cmpgt_epi64 could diverge from C semantics.
 *
 *   test_unknown_opcode
 *       An unrecognized opcode must select no rows, matching the scalar
 *       helper's reject-the-row default.
 *
 *   test_selection_buffer_slack
 *       The vector store writes 8 lanes even when fewer are selected.
 *       Guard bytes past nrows + COL_FILTER_SEL_SLACK must be untouched.
 */

#include "../wirelog/columnar/internal.h"

#include <inttypes.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures;

#define CHECK(cond, ...)                                                      \
        do {                                                                      \
            if (!(cond)) {                                                        \
                printf("  FAIL %s:%d: ", __FILE__, __LINE__);                     \
                printf(__VA_ARGS__);                                              \
                printf("\n");                                                     \
                failures++;                                                       \
            }                                                                     \
        } while (0)

static const wl_plan_expr_tag_t ALL_OPS[] = {
    WL_PLAN_EXPR_CMP_EQ, WL_PLAN_EXPR_CMP_NEQ, WL_PLAN_EXPR_CMP_LT,
    WL_PLAN_EXPR_CMP_LTE, WL_PLAN_EXPR_CMP_GT, WL_PLAN_EXPR_CMP_GTE,
};
#define NOPS (sizeof(ALL_OPS) / sizeof(ALL_OPS[0]))

static const char *
op_name(wl_plan_expr_tag_t op)
{
    switch (op) {
    case WL_PLAN_EXPR_CMP_EQ:
        return "EQ";
    case WL_PLAN_EXPR_CMP_NEQ:
        return "NEQ";
    case WL_PLAN_EXPR_CMP_LT:
        return "LT";
    case WL_PLAN_EXPR_CMP_LTE:
        return "LTE";
    case WL_PLAN_EXPR_CMP_GT:
        return "GT";
    case WL_PLAN_EXPR_CMP_GTE:
        return "GTE";
    default:
        return "?";
    }
}

/*
 * Reference oracle: the most obvious loop that could possibly work.  Kept
 * deliberately unoptimized and independent of the kernel's structure.
 */
static uint32_t
ref_select_rows(const int64_t *col_a, const int64_t *col_b, int64_t const_b,
    uint32_t nrows, wl_plan_expr_tag_t cmp_op, uint32_t *out_sel)
{
    uint32_t out = 0;
    for (uint32_t r = 0; r < nrows; r++) {
        int64_t a = col_a[r];
        int64_t b = col_b ? col_b[r] : const_b;
        bool pass;
        switch (cmp_op) {
        case WL_PLAN_EXPR_CMP_EQ:
            pass = (a == b);
            break;
        case WL_PLAN_EXPR_CMP_NEQ:
            pass = (a != b);
            break;
        case WL_PLAN_EXPR_CMP_LT:
            pass = (a < b);
            break;
        case WL_PLAN_EXPR_CMP_LTE:
            pass = (a <= b);
            break;
        case WL_PLAN_EXPR_CMP_GT:
            pass = (a > b);
            break;
        case WL_PLAN_EXPR_CMP_GTE:
            pass = (a >= b);
            break;
        default:
            pass = false;
            break;
        }
        if (pass)
            out_sel[out++] = r;
    }
    return out;
}

/* Run both implementations and assert identical output. */
static void
compare_case(const char *what, const int64_t *col_a, const int64_t *col_b,
    int64_t const_b, uint32_t nrows, wl_plan_expr_tag_t cmp_op)
{
    size_t cap = (size_t)nrows + COL_FILTER_SEL_SLACK;
    uint32_t *got = (uint32_t *)malloc(cap * sizeof(uint32_t));
    uint32_t *want = (uint32_t *)malloc(cap * sizeof(uint32_t));
    if (!got || !want) {
        printf("  FAIL %s: allocation\n", what);
        failures++;
        free(got);
        free(want);
        return;
    }

    uint32_t n_want = ref_select_rows(col_a, col_b, const_b, nrows, cmp_op,
            want);
    uint32_t n_got = col_filter_select_rows(col_a, col_b, const_b, nrows,
            cmp_op, got);

    CHECK(n_got == n_want, "%s [%s nrows=%u]: count %u, expected %u", what,
        op_name(cmp_op), nrows, n_got, n_want);
    if (n_got == n_want) {
        for (uint32_t i = 0; i < n_want; i++) {
            if (got[i] != want[i]) {
                CHECK(false, "%s [%s nrows=%u]: sel[%u]=%u, expected %u", what,
                    op_name(cmp_op), nrows, i, got[i], want[i]);
                break;
            }
        }
    }
    free(got);
    free(want);
}

/*
 * Every 8-row pass/fail pattern.  Column values are 1 where the pattern bit
 * is set and 0 elsewhere; an EQ-against-1 predicate then reproduces the
 * pattern exactly, selecting LUT entry `mask` on AVX2 builds.
 */
static void
test_all_leftpack_masks(void)
{
    printf("test_all_leftpack_masks\n");
    for (uint32_t mask = 0; mask < 256; mask++) {
        int64_t col[8];
        for (uint32_t j = 0; j < 8; j++)
            col[j] = ((mask >> j) & 1u) ? 1 : 0;
        compare_case("leftpack", col, NULL, 1, 8, WL_PLAN_EXPR_CMP_EQ);
    }

    /* Same patterns offset into a longer column, so the vector loop runs
     * several iterations before the pattern under test. */
    for (uint32_t mask = 0; mask < 256; mask++) {
        int64_t col[24];
        for (uint32_t j = 0; j < 24; j++)
            col[j] = 0;
        for (uint32_t j = 0; j < 8; j++)
            col[16 + j] = ((mask >> j) & 1u) ? 1 : 0;
        compare_case("leftpack_offset", col, NULL, 1, 24,
            WL_PLAN_EXPR_CMP_EQ);
    }
}

static void
test_opcodes_and_lengths(void)
{
    printf("test_opcodes_and_lengths\n");
    static const uint32_t LENGTHS[] = { 0, 1, 2, 7, 8, 9, 15, 16, 17, 31, 32,
                                        33, 64, 1000, 1001 };
    const uint32_t nlen = sizeof(LENGTHS) / sizeof(LENGTHS[0]);
    const uint32_t maxn = 1001;

    int64_t *a = (int64_t *)malloc(maxn * sizeof(int64_t));
    int64_t *b = (int64_t *)malloc(maxn * sizeof(int64_t));
    if (!a || !b) {
        printf("  FAIL allocation\n");
        failures++;
        free(a);
        free(b);
        return;
    }

    /* Deterministic pseudo-random values in a small range so that every
     * opcode yields a non-trivial mix of passing and failing rows. */
    uint64_t s = 0x9E3779B97F4A7C15ull;
    for (uint32_t i = 0; i < maxn; i++) {
        s = s * 6364136223846793005ull + 1442695040888963407ull;
        a[i] = (int64_t)((s >> 33) % 11) - 5;
        s = s * 6364136223846793005ull + 1442695040888963407ull;
        b[i] = (int64_t)((s >> 33) % 11) - 5;
    }

    for (uint32_t li = 0; li < nlen; li++) {
        for (uint32_t oi = 0; oi < NOPS; oi++) {
            /* colA CMP const */
            compare_case("const_cmp", a, NULL, 0, LENGTHS[li], ALL_OPS[oi]);
            /* colA CMP colB */
            compare_case("col_cmp", a, b, 0, LENGTHS[li], ALL_OPS[oi]);
        }
    }

    /* Selectivity extremes: all rows pass, no rows pass. */
    for (uint32_t i = 0; i < maxn; i++)
        a[i] = 7;
    for (uint32_t oi = 0; oi < NOPS; oi++) {
        compare_case("all_pass", a, NULL, 7, 1000, ALL_OPS[oi]);
        compare_case("none_pass", a, NULL, 9999, 1000, ALL_OPS[oi]);
    }

    free(a);
    free(b);
}

static void
test_extreme_values(void)
{
    printf("test_extreme_values\n");
    static const int64_t VALUES[] = { INT64_MIN, INT64_MIN + 1, -2147483649LL,
                                      -1, 0, 1, 2147483648LL, INT64_MAX - 1,
                                      INT64_MAX };
    const uint32_t nv = sizeof(VALUES) / sizeof(VALUES[0]);

    /* Column repeats the extreme set so it spans several vector iterations. */
    const uint32_t nrows = nv * 4;
    int64_t *col = (int64_t *)malloc(nrows * sizeof(int64_t));
    if (!col) {
        printf("  FAIL allocation\n");
        failures++;
        return;
    }
    for (uint32_t i = 0; i < nrows; i++)
        col[i] = VALUES[i % nv];

    for (uint32_t vi = 0; vi < nv; vi++) {
        for (uint32_t oi = 0; oi < NOPS; oi++)
            compare_case("extremes", col, NULL, VALUES[vi], nrows,
                ALL_OPS[oi]);
    }

    /* Column-vs-column with extremes on both sides: catches a signed
     * comparison implemented as an unsigned one. */
    int64_t *other = (int64_t *)malloc(nrows * sizeof(int64_t));
    if (other) {
        for (uint32_t i = 0; i < nrows; i++)
            other[i] = VALUES[(i + 3) % nv];
        for (uint32_t oi = 0; oi < NOPS; oi++)
            compare_case("extremes_colcol", col, other, 0, nrows,
                ALL_OPS[oi]);
        free(other);
    }
    free(col);
}

static void
test_unknown_opcode(void)
{
    printf("test_unknown_opcode\n");
    int64_t col[16];
    for (uint32_t i = 0; i < 16; i++)
        col[i] = (int64_t)i;

    uint32_t sel[16 + COL_FILTER_SEL_SLACK];
    /* WL_PLAN_EXPR_CMP_* are 0x22..0x27; 0x7F is not a comparison opcode. */
    uint32_t n = col_filter_select_rows(col, NULL, 0, 16,
            (wl_plan_expr_tag_t)0x7F, sel);
    CHECK(n == 0, "unknown opcode selected %u rows, expected 0", n);
}

static void
test_selection_buffer_slack(void)
{
    printf("test_selection_buffer_slack\n");
    /* nrows deliberately a multiple of 8 with every row passing, so the
     * final vector iteration stores a full 8 lanes at the highest offset. */
    const uint32_t nrows = 64;
    int64_t col[64];
    for (uint32_t i = 0; i < nrows; i++)
        col[i] = 5;

    const uint32_t cap = nrows + COL_FILTER_SEL_SLACK;
    const uint32_t guard = 16;
    uint32_t *buf = (uint32_t *)malloc((cap + guard) * sizeof(uint32_t));
    if (!buf) {
        printf("  FAIL allocation\n");
        failures++;
        return;
    }
    for (uint32_t i = 0; i < cap + guard; i++)
        buf[i] = 0xDEADBEEFu;

    uint32_t n = col_filter_select_rows(col, NULL, 5, nrows,
            WL_PLAN_EXPR_CMP_EQ, buf);
    CHECK(n == nrows, "expected all %u rows, got %u", nrows, n);
    for (uint32_t i = 0; i < n; i++)
        CHECK(buf[i] == i, "sel[%u]=%u, expected %u", i, buf[i], i);

    /* Nothing past the documented capacity may be written. */
    for (uint32_t i = cap; i < cap + guard; i++)
        CHECK(buf[i] == 0xDEADBEEFu,
            "guard slot %u overwritten with %u", i, buf[i]);

    free(buf);
}

int
main(void)
{
    printf("=== col_filter_select_rows equivalence ===\n");
#ifdef __AVX2__
    printf("(AVX2 kernel compiled in)\n");
#else
    printf("(scalar kernel only)\n");
#endif

    test_all_leftpack_masks();
    test_opcodes_and_lengths();
    test_extreme_values();
    test_unknown_opcode();
    test_selection_buffer_slack();

    if (failures) {
        printf("=== %d FAILURE(S) ===\n", failures);
        return 1;
    }
    printf("=== all passed ===\n");
    return 0;
}
