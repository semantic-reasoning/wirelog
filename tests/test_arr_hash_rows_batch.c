/*
 * tests/test_arr_hash_rows_batch.c - arrangement batch hash equivalence
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * arr_hash_rows_batch() must reproduce the arrangement's hash exactly. Both
 * paths use the shared 32-bit value hash, including canonical float keys.
 *
 * These tests exist to hold that argument to account. The oracle is written
 * independently from the shared 32-bit value-hash specification.
 *
 * Why bucket equality matters: arr_update_incremental() extends a cached
 * arrangement in place, so rows indexed on one call are probed against rows
 * indexed on another. A divergence leaves earlier rows in buckets the probe
 * never visits, and join matches disappear with no crash and no error.
 *
 *   test_matches_oracle
 *       Kernel output vs the 32-bit value-hash oracle, over randomized data,
 *       every key width, and every plausible nbuckets.
 *
 *   test_golden_values
 *       Hardcoded results computed independently from the FNV-1a definition,
 *       pinning the basis and the prime themselves.
 *
 *   test_full_32_bits / test_float_keys
 *       The raw output must equal the complete value hash, and float batches
 *       must canonicalize +0.0 and -0.0 identically.
 *
 *   test_row_order / test_lengths / test_row_begin_offset
 *       Lane placement, counts straddling the 8-row vector width, and a
 *       non-zero start so the vector loop begins away from the column head.
 *
 *   test_extremes / test_repeated_and_reordered_key_cols
 *       INT64 boundaries, and key lists that repeat or reorder columns.
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
 * The arrangement's hash, written independently from the shared value-hash
 * specification. Float zero is canonicalized before hashing.
 */
static uint64_t
ref_hash64(const col_rel_t *rel, uint32_t row, const uint32_t *key_cols,
    uint32_t key_count)
{
    uint64_t h = 14695981039346656037ULL;
    for (uint32_t k = 0; k < key_count; k++) {
        uint64_t v = (uint64_t)rel->columns[key_cols[k]][row];
        if (rel->column_types
            && rel->column_types[key_cols[k]] == WIRELOG_TYPE_FLOAT
            && v == UINT64_C(0x8000000000000000))
            v = 0;
        for (int b = 0; b < 8; b++) {
            h ^= v & 0xffu;
            h *= 1099511628211ULL;
            v >>= 8;
        }
    }
    return h;
}

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

static int64_t
mix_cb(uint32_t r, uint32_t c)
{
    uint64_t s = (uint64_t)r * 6364136223846793005ull
        + (uint64_t)c * 1442695040888963407ull + 0x9E3779B97F4A7C15ull;
    s ^= s >> 29;
    s *= 0xBF58476D1CE4E5B9ull;
    s ^= s >> 32;
    return (int64_t)s;
}

/* Compare kernel output against the 64-bit oracle over one range. */
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

    arr_hash_rows_batch(rel, begin, end, key_cols, kc, got);

    /* Every power-of-two table size the arrangement can produce. */
    static const uint32_t NBUCKETS[] = { 16u, 256u, 4096u, 65536u,
                                         1u << 24, 1u << 31 };

    for (uint32_t j = 0; j < n; j++) {
        uint64_t ref = ref_hash64(rel, begin + j, key_cols, kc);

        if (got[j] != (uint32_t)(ref & 0xffffffffu)) {
            CHECK(false,
                "%s [kc=%u begin=%u]: raw out[%u]=0x%08" PRIx32
                ", low32(ref)=0x%08" PRIx32, what, kc, begin, j, got[j],
                (uint32_t)(ref & 0xffffffffu));
            break;
        }
        bool bad = false;
        for (uint32_t bi = 0; bi < sizeof(NBUCKETS) / sizeof(NBUCKETS[0]);
            bi++) {
            uint32_t nb = NBUCKETS[bi];
            uint32_t want = (uint32_t)(ref & (uint64_t)(nb - 1));
            if ((got[j] & (nb - 1)) != want) {
                CHECK(false, "%s [kc=%u nbuckets=%u]: bucket %u != %u", what,
                    kc, nb, got[j] & (nb - 1), want);
                bad = true;
                break;
            }
        }
        if (bad)
            break;
    }
    free(got);
}

static void
test_matches_64bit_oracle(void)
{
    printf("test_matches_64bit_oracle\n");
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
 * The raw output must equal low32 of the 64-bit chain, not merely agree once
 * masked. compare_range already asserts this; this case states it separately
 * so a regression names the right thing.
 */
static void
test_full_32_bits(void)
{
    printf("test_full_32_bits\n");
    const uint32_t k0[] = { 0 };
    col_rel_t *rel = make_rel(256, 1, mix_cb);
    if (!rel) {
        printf("  FAIL relation build\n");
        failures++;
        return;
    }
    uint32_t got[256];
    arr_hash_rows_batch(rel, 0, 256, k0, 1, got);
    for (uint32_t r = 0; r < 256; r++) {
        uint32_t want = (uint32_t)(ref_hash64(rel, r, k0, 1)
            & 0xffffffffu);
        CHECK(got[r] == want,
            "row %u: 0x%08" PRIx32 " != low32 0x%08" PRIx32, r, got[r], want);
        if (got[r] != want)
            break;
    }
    col_rel_destroy(rel);
}

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
    arr_hash_rows_batch(rel, 0, 8, k0, 1, got);

    /*
     * Computed independently from the 32-bit two-word FNV-1a definition, not
     * captured from this kernel. These pin the basis and the prime themselves.
     */
    static const uint32_t GOLDEN[8] = {
        0x281A39C5u, 0x1D2AEFA4u, 0x3DF8CE07u, 0x330983E6u,
        0xFC5D1141u, 0xF16DC720u, 0x123BA583u, 0x074C5B62u,
    };
    for (uint32_t r = 0; r < 8; r++)
        CHECK(got[r] == GOLDEN[r],
            "golden row %u: 0x%08" PRIx32 " != 0x%08" PRIx32, r, got[r],
            GOLDEN[r]);
    col_rel_destroy(rel);
}

static int64_t
distinct_cb(uint32_t r, uint32_t c)
{
    (void)c;
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
    arr_hash_rows_batch(rel, 0, 64, k0, 1, got);
    for (uint32_t r = 0; r < 64; r++) {
        uint32_t want = (uint32_t)(ref_hash64(rel, r, k0, 1)
            & 0xffffffffu);
        CHECK(got[r] == want, "row %u landed wrong: 0x%08" PRIx32
            " != 0x%08" PRIx32, r, got[r], want);
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
    const uint32_t k01[] = { 0, 1 };

    col_rel_t *rel = make_rel(1025, 2, mix_cb);
    if (!rel) {
        printf("  FAIL relation build\n");
        failures++;
        return;
    }
    for (uint32_t i = 0; i < sizeof(LENGTHS) / sizeof(LENGTHS[0]); i++) {
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
    static const uint32_t BEGINS[] = { 1, 3, 7, 8, 9, 16, 100, 511, 592, 599 };
    for (uint32_t i = 0; i < sizeof(BEGINS) / sizeof(BEGINS[0]); i++) {
        compare_range("offset", rel, BEGINS[i], 600, k0, 1);
        compare_range("offset_mid", rel, BEGINS[i],
            BEGINS[i] + 17 < 600 ? BEGINS[i] + 17 : 600, k0, 1);
    }
    compare_range("empty", rel, 42, 42, k0, 1);
    compare_range("inverted", rel, 100, 50, k0, 1);
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

static void
test_float_keys(void)
{
    printf("test_float_keys\n");
    const uint32_t k0[] = { 0 };
    const wirelog_column_type_t types[] = { WIRELOG_TYPE_FLOAT };
    col_rel_t *rel = col_rel_new_auto("float", 1);
    if (!rel || col_rel_set_column_types(rel, types, 1) != 0) {
        printf("  FAIL relation build\n");
        failures++;
        col_rel_destroy(rel);
        return;
    }

    static const uint64_t values[] = {
        UINT64_C(0x0000000000000000), /* +0.0 */
        UINT64_C(0x8000000000000000), /* -0.0 */
        UINT64_C(0x3ff0000000000000), /* 1.0 */
        UINT64_C(0xbff0000000000000), /* -1.0 */
        UINT64_C(0x4004000000000000), /* 2.5 */
        UINT64_C(0xc004000000000000), /* -2.5 */
        UINT64_C(0x3fd5555555555555),
        UINT64_C(0xbfd5555555555555),
        UINT64_C(0x4014000000000000),
        UINT64_C(0xc014000000000000),
        UINT64_C(0x4024000000000000),
        UINT64_C(0xc024000000000000),
        UINT64_C(0x4034000000000000),
        UINT64_C(0xc034000000000000),
        UINT64_C(0x4044000000000000),
        UINT64_C(0xc044000000000000),
    };
    for (uint32_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
        int64_t row[1];
        memcpy(row, &values[i], sizeof(row[0]));
        if (col_rel_append_row(rel, row) != 0) {
            printf("  FAIL append row %u\n", i);
            failures++;
            col_rel_destroy(rel);
            return;
        }
    }

    uint32_t got[sizeof(values) / sizeof(values[0])];
    arr_hash_rows_batch(rel, 0, (uint32_t)(sizeof(values) / sizeof(values[0])),
        k0, 1, got);
    for (uint32_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
        uint32_t want = (uint32_t)(ref_hash64(rel, i, k0, 1)
            & 0xffffffffu);
        CHECK(got[i] == want, "float row %u: 0x%08" PRIx32
            " != 0x%08" PRIx32, i, got[i], want);
    }
    CHECK(got[0] == got[1], "+0.0 and -0.0 hash differently");
    col_rel_destroy(rel);
}

static void
test_float_typed_arrangement_probe(void)
{
    printf("test_float_typed_arrangement_probe\n");
    const uint32_t key_cols[] = { 0 };
    const wirelog_column_type_t types[] = { WIRELOG_TYPE_FLOAT };
    col_rel_t *rel = col_rel_new_auto("float_probe", 1);
    col_arrangement_t arr = { 0 };
    if (!rel || col_rel_set_column_types(rel, types, 1) != 0) {
        printf("  FAIL relation build\n");
        failures++;
        col_rel_destroy(rel);
        return;
    }
    int64_t zero[1] = { wl_columnar_float_to_bits(0.0) };
    int64_t negative_zero[1] = { wl_columnar_float_to_bits(-0.0) };
    if (col_rel_append_row(rel, zero) != 0
        || col_rel_append_row(rel, negative_zero) != 0) {
        printf("  FAIL append\n");
        failures++;
        col_rel_destroy(rel);
        return;
    }

    arr.key_cols = (uint32_t *)key_cols;
    arr.key_count = 1;
    arr.nbuckets = 16;
    arr.ht_cap = rel->nrows;
    arr.indexed_rows = rel->nrows;
    arr.ht_head = (uint64_t *)malloc(arr.nbuckets * sizeof(uint64_t));
    arr.ht_next = (uint32_t *)malloc(arr.ht_cap * sizeof(uint32_t));
    if (!arr.ht_head || !arr.ht_next) {
        printf("  FAIL arrangement allocation\n");
        failures++;
        free(arr.ht_head);
        free(arr.ht_next);
        col_rel_destroy(rel);
        return;
    }
    for (uint32_t i = 0; i < arr.nbuckets; i++)
        arr.ht_head[i] = UINT64_C(0xFFFFFFFF);
    uint32_t hashes[2];
    arr_hash_rows_batch(rel, 0, rel->nrows, key_cols, 1, hashes);
    for (uint32_t row = 0; row < rel->nrows; row++) {
        uint32_t bucket = hashes[row] & (arr.nbuckets - 1u);
        arr.ht_next[row] = (uint32_t)arr.ht_head[bucket];
        arr.ht_head[bucket] = row;
    }
    uint32_t row = col_arrangement_find_first_typed(&arr, rel, negative_zero);
    CHECK(row != UINT32_MAX, "typed arrangement probe missed signed zero");
    free(arr.ht_head);
    free(arr.ht_next);
    col_rel_destroy(rel);
}

int
main(void)
{
    printf("=== arr_hash_rows_batch equivalence ===\n");
#ifdef __AVX2__
    printf("(AVX2 kernel compiled in)\n");
#else
    printf("(scalar kernel only)\n");
#endif

    test_matches_64bit_oracle();
    test_full_32_bits();
    test_golden_values();
    test_row_order();
    test_lengths();
    test_row_begin_offset();
    test_extremes();
    test_repeated_and_reordered_key_cols();
    test_float_keys();
    test_float_typed_arrangement_probe();

    if (failures) {
        printf("=== %d FAILURE(S) ===\n", failures);
        return 1;
    }
    printf("=== all passed ===\n");
    return 0;
}
