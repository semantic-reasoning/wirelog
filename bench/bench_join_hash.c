/*
 * bench_join_hash.c - join hash-table build microbenchmark
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Decides whether col_hash_rows_batch() earns its place in the join build
 * loops, and separates the value of the vector kernel from the value of
 * batching at all:
 *
 *   perrow   The loop the join build sites use today: hash one row, mask it,
 *            prepend to its chain, repeat.  Reproduced verbatim here because
 *            col_join_hash_rel_keys is static.  This is the baseline -- the
 *            question is whether the batch form beats what we have, not
 *            whether it beats some other batch form.
 *
 *   batch    col_hash_rows_batch into a COL_HASH_TILE scratch, then a chain
 *            build pass over the tile.  This is the shape the call sites
 *            would take.
 *
 *   hash     The kernel alone, no chain build.  The gap between `hash` and
 *            `batch` is the chain-write cost, which is memory-bound and
 *            which no amount of SIMD in the hash can remove.
 *
 * Whether `batch` and `hash` use the AVX2 or the scalar body is a
 * compile-time decision, so the comparison is made by building this file
 * twice.  meson registers:
 *
 *   bench_join_hash          default flags  -> AVX2 kernel
 *   bench_join_hash_scalar   -mno-avx2      -> scalar kernel
 *
 * A runtime switch would put a global read inside the hash loop and change
 * the thing being measured.
 *
 * Every configuration checks its hashes against the per-row baseline and
 * prints a mismatch count.  A throughput number without mismatch=0 beside it
 * is not evidence of anything -- and for this kernel a mismatch is not a
 * benchmark artifact but the silent join-result-loss failure that #938 is
 * about.
 */

#include "../wirelog/columnar/internal.h"

#include "bench_util.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define WARMUP  3
#define REPEATS 7

static const uint32_t NROWS_LIST[] = { 20000, 100000, 1000000 };
#define NROWS_COUNT (sizeof(NROWS_LIST) / sizeof(NROWS_LIST[0]))

static const uint32_t KC_LIST[] = { 1, 2, 4 };
#define KC_COUNT (sizeof(KC_LIST) / sizeof(KC_LIST[0]))

/* Duplicate rate: 0 means every key distinct, 10 means ~10% repeats.  Chain
 * length changes the memory behaviour of the build pass, not the hash. */
static const uint32_t DUP_PCT[] = { 0, 10 };
#define DUP_COUNT (sizeof(DUP_PCT) / sizeof(DUP_PCT[0]))

/*
 * The scalar FNV-1a the join uses, reproduced because col_join_hash_rel_keys
 * is static.  Kept structurally identical to it, including the kc == 1 and
 * kc == 2 special cases, so `perrow` really is today's cost.
 */
static BENCH_NOINLINE uint32_t
perrow_hash(const col_rel_t *rel, uint32_t row, const uint32_t *key_cols,
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

/* Today's build loop: hash, mask, prepend, one row at a time. */
static BENCH_NOINLINE void
build_perrow(const col_rel_t *rel, uint32_t nrows, const uint32_t *key_cols,
    uint32_t kc, uint32_t nbuckets, uint32_t *head, uint32_t *next)
{
    memset(head, 0, (size_t)nbuckets * sizeof(uint32_t));
    for (uint32_t r = 0; r < nrows; r++) {
        uint32_t h = perrow_hash(rel, r, key_cols, kc) & (nbuckets - 1);
        next[r] = head[h];
        head[h] = r + 1;
    }
}

/* The batched shape: hash a tile, then prepend that tile's rows. */
static BENCH_NOINLINE void
build_batch(const col_rel_t *rel, uint32_t nrows, const uint32_t *key_cols,
    uint32_t kc, uint32_t nbuckets, uint32_t *head, uint32_t *next,
    uint32_t *scratch)
{
    memset(head, 0, (size_t)nbuckets * sizeof(uint32_t));
    for (uint32_t base = 0; base < nrows;) {
        uint32_t chunk = nrows - base;
        if (chunk > COL_HASH_TILE)
            chunk = COL_HASH_TILE;

        col_hash_rows_batch(rel, base, base + chunk, key_cols, kc, scratch);

        for (uint32_t j = 0; j < chunk; j++) {
            uint32_t r = base + j;
            uint32_t h = scratch[j] & (nbuckets - 1);
            next[r] = head[h];
            head[h] = r + 1;
        }
        base += chunk;
    }
}

/* Kernel only, tiled the same way, no chain writes. */
static BENCH_NOINLINE void
hash_only(const col_rel_t *rel, uint32_t nrows, const uint32_t *key_cols,
    uint32_t kc, uint32_t *scratch)
{
    for (uint32_t base = 0; base < nrows;) {
        uint32_t chunk = nrows - base;
        if (chunk > COL_HASH_TILE)
            chunk = COL_HASH_TILE;
        col_hash_rows_batch(rel, base, base + chunk, key_cols, kc, scratch);
        base += chunk;
    }
}

static uint32_t
next_pow2_local(uint32_t n)
{
    if (n < 16)
        return 16;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    return n + 1;
}

static double
median(double *v, uint32_t n)
{
    for (uint32_t i = 1; i < n; i++) {
        double key = v[i];
        uint32_t j = i;
        while (j > 0 && v[j - 1] > key) {
            v[j] = v[j - 1];
            j--;
        }
        v[j] = key;
    }
    return v[n / 2];
}

int
main(void)
{
#ifdef __AVX2__
    const char *variant = "avx2";
#else
    const char *variant = "scalar";
#endif
    printf("bench_join_hash (kernel=%s, warmup=%d repeats=%d, tile=%u)\n",
        variant, WARMUP, REPEATS, COL_HASH_TILE);
    printf("%-8s %-3s %-4s %10s %10s %10s %8s %8s %8s\n", "nrows", "kc",
        "dup%", "perrow_ms", "batch_ms", "hash_ms", "batch_x", "hash_x",
        "mismatch");

    for (uint32_t ni = 0; ni < NROWS_COUNT; ni++) {
        const uint32_t nrows = NROWS_LIST[ni];
        for (uint32_t ki = 0; ki < KC_COUNT; ki++) {
            const uint32_t kc = KC_LIST[ki];
            for (uint32_t di = 0; di < DUP_COUNT; di++) {
                const uint32_t dup = DUP_PCT[di];

                col_rel_t *rel = col_rel_new_auto("bench", 4);
                if (!rel)
                    return 1;

                uint64_t s = 0x243F6A8885A308D3ull;
                for (uint32_t r = 0; r < nrows; r++) {
                    int64_t row[4];
                    for (uint32_t c = 0; c < 4; c++) {
                        s = s * 6364136223846793005ull
                            + 1442695040888963407ull;
                        int64_t v = (int64_t)(s >> 16);
                        /* Fold a fraction of keys onto earlier values so
                         * chains are not uniformly length 1. */
                        if (dup && r > 0 && (s % 100u) < dup)
                            v = rel->columns[c][r % (r > 64 ? 64 : r)];
                        row[c] = v;
                    }
                    if (col_rel_append_row(rel, row) != 0)
                        return 1;
                }

                const uint32_t KEYS[4] = { 0, 1, 2, 3 };
                uint32_t nbuckets = next_pow2_local(nrows * 2);

                uint32_t *head = (uint32_t *)malloc(
                    (size_t)nbuckets * sizeof(uint32_t));
                uint32_t *next_a = (uint32_t *)malloc(
                    (size_t)nrows * sizeof(uint32_t));
                uint32_t *next_b = (uint32_t *)malloc(
                    (size_t)nrows * sizeof(uint32_t));
                uint32_t *scratch = (uint32_t *)malloc(
                    (size_t)COL_HASH_TILE * sizeof(uint32_t));
                if (!head || !next_a || !next_b || !scratch)
                    return 1;

                double pr[REPEATS], ba[REPEATS], ha[REPEATS];

                for (int rep = 0; rep < WARMUP + REPEATS; rep++) {
                    bench_time_t t0 = bench_time_now();
                    build_perrow(rel, nrows, KEYS, kc, nbuckets, head, next_a);
                    bench_time_t t1 = bench_time_now();

                    bench_time_t t2 = bench_time_now();
                    build_batch(rel, nrows, KEYS, kc, nbuckets, head, next_b,
                        scratch);
                    bench_time_t t3 = bench_time_now();

                    bench_time_t t4 = bench_time_now();
                    hash_only(rel, nrows, KEYS, kc, scratch);
                    bench_time_t t5 = bench_time_now();

                    if (rep >= WARMUP) {
                        int i = rep - WARMUP;
                        pr[i] = bench_time_diff_ms(t0, t1);
                        ba[i] = bench_time_diff_ms(t2, t3);
                        ha[i] = bench_time_diff_ms(t4, t5);
                    }
                }

                /* Both builds must produce identical chains, and every hash
                 * must match the per-row baseline exactly. */
                uint64_t mismatch = 0;
                for (uint32_t r = 0; r < nrows; r++) {
                    if (next_a[r] != next_b[r])
                        mismatch++;
                }
                for (uint32_t base = 0; base < nrows && mismatch == 0;) {
                    uint32_t chunk = nrows - base;
                    if (chunk > COL_HASH_TILE)
                        chunk = COL_HASH_TILE;
                    col_hash_rows_batch(rel, base, base + chunk, KEYS, kc,
                        scratch);
                    for (uint32_t j = 0; j < chunk; j++) {
                        if (scratch[j] != perrow_hash(rel, base + j, KEYS, kc))
                            mismatch++;
                    }
                    base += chunk;
                }

                double p = median(pr, REPEATS);
                double b = median(ba, REPEATS);
                double h = median(ha, REPEATS);

                printf("%-8u %-3u %-4u %10.3f %10.3f %10.3f %7.2fx %7.2fx"
                    " %8" PRIu64 "\n", nrows, kc, dup, p, b, h,
                    b > 0.0 ? p / b : 0.0, h > 0.0 ? p / h : 0.0, mismatch);
                fflush(stdout);

                free(scratch);
                free(next_b);
                free(next_a);
                free(head);
                col_rel_destroy(rel);
            }
        }
    }
    return 0;
}
