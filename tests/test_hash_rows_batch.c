/*
 * tests/test_hash_rows_batch.c - batch join-hash kernel equivalence
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * col_hash_rows_batch() has a scalar body and, on AVX2 targets, a vectorized
 * body that hashes 8 rows per iteration with the low and high halves of each
 * int64_t deinterleaved into separate lanes.  The two must agree with
 * col_join_hash_rel_keys() bit for bit.
 *
 * That equality is not cosmetic.  The diff arrangement is cached in the
 * session and extended incrementally, so rows indexed in one iteration are
 * probed in a later one, potentially through a different code path.  A single
 * differing bit strands the earlier rows in buckets computed under the other
 * function and loses join results with no crash and no error -- which is why
 * these tests assert exact uint32_t equality rather than "the join returned
 * some rows".
 *
 *   test_matches_oracle
 *       Differential against an FNV-1a written here from the specification,
 *       independent of the kernel's structure, over randomized data.
 *
 *   test_golden_values
 *       Hardcoded uint32_t results.  The oracle and the kernel could in
 *       principle drift together (a changed multiplier in both); only pinned
 *       constants catch that, and a changed hash is exactly the silent
 *       data-loss event above.
 *
 *   test_row_order
 *       Every row hashes differently, so a lane-permutation mistake in the
 *       deinterleave shows up as a misplaced result.  This is the detector
 *       for the blend-cannot-swap-128-bit-halves trap.
 *
 *   test_lengths / test_row_begin_offset
 *       Counts straddling the 8-row vector width, and non-zero row_begin so
 *       the vector loop starts away from the column head.
 *
 *   test_kc_zero
 *       Zero key columns must yield the bare FNV offset basis, matching the
 *       scalar function's zero-trip loop.
 *
 *   test_extremes / test_repeated_and_reordered_key_cols
 *       INT64 boundary values, and key column lists that repeat or reorder.
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

#define CHECK(cond, ...)                                              \
        do {                                                              \
            if (!(cond)) {                                                \
                printf("  FAIL %s:%d: ", __FILE__, __LINE__);             \
                printf(__VA_ARGS__);                                      \
                printf("\n");                                             \
                failures++;                                               \
            }                                                             \
        } while (0)

/*
 * Reference FNV-1a, written from the specification rather than derived from
 * the kernel: 32-bit offset basis, per 64-bit key the low word then the high
 * word, each xored in and then multiplied by the prime.
 */
static uint32_t
ref_hash_row(const col_rel_t *rel, uint32_t row, const uint32_t *key_cols,
    uint32_t kc)
{
    uint32_t h = 2166136261u;
    for (uint32_t i = 0; i < kc; i++) {
        uint64_t v = (uint64_t)rel->columns[key_cols[i]][row];
        h ^= (uint32_t)(v & 0xffffffffu);
        h *= 16777619u;
        h ^= (uint32_t)(v >> 32);
        h *= 16777619u;
    }
    return h;
}

/* Build a relation with @ncols columns and @nrows rows, values from cb. */
static col_rel_t *
make_rel(uint32_t nrows, uint32_t ncols, int64_t (*cb)(uint32_t r, uint32_t c))
{
    col_rel_t *rel = col_rel_new_auto("t", ncols);
    if (!rel)
        return NULL;
    int64_t row[16];
    for (uint32_t r = 0; r < nrows; r++) {
        for (uint32_t c = 0; c < ncols && c < 16; c++)
            row[c] = cb(r, c);
        if (col_rel_append_row(rel, row) != 0) {
            col_rel_destroy(rel);
            return NULL;
        }
    }
    return rel;
}

/* Compare the batch kernel against the oracle over one range. */
static void
compare_range(const char *what, const col_rel_t *rel, uint32_t begin,
    uint32_t end, const uint32_t *key_cols, uint32_t kc)
{
    uint32_t n = (end > begin) ? end - begin : 0;
    uint32_t *got = (uint32_t *)malloc((n ? n : 1) * sizeof(uint32_t));
    if (!got) {
        printf("  FAIL %s: allocation\n", what);
        failures++;
        return;
    }
    memset(got, 0xAB, (n ? n : 1) * sizeof(uint32_t));

    col_hash_rows_batch(rel, begin, end, key_cols, kc, got);

    for (uint32_t j = 0; j < n; j++) {
        uint32_t want = ref_hash_row(rel, begin + j, key_cols, kc);
        if (got[j] != want) {
            CHECK(false,
                "%s [kc=%u begin=%u n=%u]: out[%u]=0x%08" PRIx32
                ", expected 0x%08" PRIx32, what, kc, begin, n, j, got[j],
                want);
            break;
        }
    }
    free(got);
}

static int64_t
mix_cb(uint32_t r, uint32_t c)
{
    /* Deterministic, spread across the full int64 range including negatives. */
    uint64_t s = (uint64_t)r * 6364136223846793005ull
        + (uint64_t)c * 1442695040888963407ull + 0x9E3779B97F4A7C15ull;
    s ^= s >> 29;
    s *= 0xBF58476D1CE4E5B9ull;
    s ^= s >> 32;
    return (int64_t)s;
}

static void
test_matches_oracle(void)
{
    printf("test_matches_oracle\n");
    static const uint32_t KEYSETS[][4] = {
        { 0 }, { 1 }, { 0, 1 }, { 2, 0 }, { 0, 1, 2 }, { 3, 2, 1, 0 },
    };
    static const uint32_t KCS[] = { 1, 1, 2, 2, 3, 4 };

    col_rel_t *rel = make_rel(2000, 4, mix_cb);
    if (!rel) {
        printf("  FAIL relation build\n");
        failures++;
        return;
    }
    for (uint32_t k = 0; k < 6; k++)
        compare_range("oracle", rel, 0, 2000, KEYSETS[k], KCS[k]);
    col_rel_destroy(rel);
}

/*
 * Golden values pin the hash itself.  Computed from the FNV-1a definition:
 * a changed multiplier or basis would move these even if kernel and oracle
 * changed together.
 */
static int64_t
golden_cb(uint32_t r, uint32_t c)
{
    (void)c;
    return (int64_t)r;
}

static void
test_golden_values(void)
{
    printf("test_golden_values\n");
    col_rel_t *rel = make_rel(8, 1, golden_cb);
    if (!rel) {
        printf("  FAIL relation build\n");
        failures++;
        return;
    }
    const uint32_t k0[] = { 0 };
    uint32_t got[8];
    col_hash_rows_batch(rel, 0, 8, k0, 1, got);

    /* h = basis; h ^= lo32(v); h *= prime; h ^= hi32(v); h *= prime */
    for (uint32_t r = 0; r < 8; r++) {
        uint32_t h = 2166136261u;
        h ^= r;
        h *= 16777619u;
        h ^= 0u;
        h *= 16777619u;
        CHECK(got[r] == h, "golden row %u: 0x%08" PRIx32 " != 0x%08" PRIx32,
            r, got[r], h);
    }
    /*
     * Fully hardcoded anchors, computed independently from the FNV-1a
     * definition rather than captured from this kernel.  The loop above
     * shares its formula with the implementation; these do not, so they are
     * what actually pins the offset basis and the prime.
     */
    static const uint32_t GOLDEN[4] = {
        0x117697CDu, 0xEB741D64u, 0x5D7B8C9Fu, 0x37791236u,
    };
    for (uint32_t r = 0; r < 4; r++)
        CHECK(got[r] == GOLDEN[r],
            "golden anchor row %u: 0x%08" PRIx32 " != 0x%08" PRIx32, r,
            got[r], GOLDEN[r]);
    col_rel_destroy(rel);
}

static int64_t
distinct_cb(uint32_t r, uint32_t c)
{
    (void)c;
    /* Distinct high AND low words per row, so a lane mix-up cannot alias. */
    return (int64_t)(((uint64_t)(r + 1) << 32) | (uint64_t)(0xFEED0000u + r));
}

static void
test_row_order(void)
{
    printf("test_row_order\n");
    col_rel_t *rel = make_rel(64, 1, distinct_cb);
    if (!rel) {
        printf("  FAIL relation build\n");
        failures++;
        return;
    }
    const uint32_t k0[] = { 0 };
    uint32_t got[64];
    col_hash_rows_batch(rel, 0, 64, k0, 1, got);

    for (uint32_t r = 0; r < 64; r++) {
        uint32_t want = ref_hash_row(rel, r, k0, 1);
        CHECK(got[r] == want,
            "row %u landed wrong: 0x%08" PRIx32 " != 0x%08" PRIx32, r, got[r],
            want);
        if (got[r] != want)
            break;
    }
    col_rel_destroy(rel);
}

static void
test_lengths(void)
{
    printf("test_lengths\n");
    static const uint32_t LENGTHS[] = { 0, 1, 2, 7, 8, 9, 15, 16, 17, 31, 32,
                                        33, 63, 64, 65, 1023, 1024, 1025 };
    const uint32_t nlen = sizeof(LENGTHS) / sizeof(LENGTHS[0]);
    const uint32_t k01[] = { 0, 1 };

    col_rel_t *rel = make_rel(1025, 2, mix_cb);
    if (!rel) {
        printf("  FAIL relation build\n");
        failures++;
        return;
    }
    for (uint32_t i = 0; i < nlen; i++) {
        compare_range("len", rel, 0, LENGTHS[i], k01, 1);
        compare_range("len", rel, 0, LENGTHS[i], k01, 2);
    }
    col_rel_destroy(rel);
}

static void
test_row_begin_offset(void)
{
    printf("test_row_begin_offset\n");
    const uint32_t k0[] = { 0 };
    col_rel_t *rel = make_rel(600, 1, mix_cb);
    if (!rel) {
        printf("  FAIL relation build\n");
        failures++;
        return;
    }
    /* Offsets that are and are not multiples of the vector width, including
     * ranges that end exactly at nrows. */
    static const uint32_t BEGINS[] = { 1, 3, 7, 8, 9, 16, 100, 511, 592, 599 };
    for (uint32_t i = 0; i < sizeof(BEGINS) / sizeof(BEGINS[0]); i++) {
        compare_range("offset", rel, BEGINS[i], 600, k0, 1);
        compare_range("offset_mid", rel, BEGINS[i],
            BEGINS[i] + 17 < 600 ? BEGINS[i] + 17 : 600, k0, 1);
    }
    /* Empty and inverted ranges must be no-ops. */
    compare_range("empty", rel, 42, 42, k0, 1);
    compare_range("inverted", rel, 100, 50, k0, 1);
    col_rel_destroy(rel);
}

static void
test_kc_zero(void)
{
    printf("test_kc_zero\n");
    col_rel_t *rel = make_rel(20, 2, mix_cb);
    if (!rel) {
        printf("  FAIL relation build\n");
        failures++;
        return;
    }
    uint32_t got[20];
    memset(got, 0xAB, sizeof(got));
    col_hash_rows_batch(rel, 0, 20, NULL, 0, got);
    for (uint32_t r = 0; r < 20; r++)
        CHECK(got[r] == 2166136261u,
            "kc=0 row %u: 0x%08" PRIx32 " != offset basis", r, got[r]);
    col_rel_destroy(rel);
}

static int64_t extreme_vals[9] = { INT64_MIN, INT64_MIN + 1, -2147483649LL, -1,
                                   0, 1, 2147483648LL, INT64_MAX - 1,
                                   INT64_MAX };

static int64_t
extreme_cb(uint32_t r, uint32_t c)
{
    return extreme_vals[(r + c * 3u) % 9u];
}

static void
test_extremes(void)
{
    printf("test_extremes\n");
    const uint32_t k01[] = { 0, 1 };
    col_rel_t *rel = make_rel(90, 2, extreme_cb);
    if (!rel) {
        printf("  FAIL relation build\n");
        failures++;
        return;
    }
    compare_range("extremes", rel, 0, 90, k01, 1);
    compare_range("extremes", rel, 0, 90, k01, 2);
    col_rel_destroy(rel);
}

static void
test_repeated_and_reordered_key_cols(void)
{
    printf("test_repeated_and_reordered_key_cols\n");
    static const uint32_t REPEAT[] = { 2, 2 };
    static const uint32_t REORDER[] = { 3, 1 };
    static const uint32_t REVERSE[] = { 3, 2, 1, 0 };

    col_rel_t *rel = make_rel(300, 4, mix_cb);
    if (!rel) {
        printf("  FAIL relation build\n");
        failures++;
        return;
    }
    compare_range("repeat", rel, 0, 300, REPEAT, 2);
    compare_range("reorder", rel, 0, 300, REORDER, 2);
    compare_range("reverse", rel, 0, 300, REVERSE, 4);
    col_rel_destroy(rel);
}

int
main(void)
{
    printf("=== col_hash_rows_batch equivalence ===\n");
#ifdef __AVX2__
    printf("(AVX2 kernel compiled in)\n");
#else
    printf("(scalar kernel only)\n");
#endif

    test_matches_oracle();
    test_golden_values();
    test_row_order();
    test_lengths();
    test_row_begin_offset();
    test_kc_zero();
    test_extremes();
    test_repeated_and_reordered_key_cols();

    if (failures) {
        printf("=== %d FAILURE(S) ===\n", failures);
        return 1;
    }
    printf("=== all passed ===\n");
    return 0;
}
