/*
 * bench_filter_scan.c - column-native filter scan microbenchmark
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Measures the filter fast path of col_op_filter() three ways, so the value
 * of the SIMD kernel can be separated from the value of restructuring the
 * loop:
 *
 *   fused    The loop col_op_filter() used before the kernel existed:
 *            one pass that tests a row and immediately materializes it.
 *            Reproduced verbatim here so it stays measurable after the
 *            original was replaced.
 *
 *   tiled    Scan COL_FILTER_TILE rows, materialize those, repeat.
 *
 *   adapt    Tiled, except a first tile that keeps nearly every row falls
 *            back to `fused` for the whole scan.  This is what col_op_filter()
 *            ships; `adapt_x` is the number that matters.
 *
 *   scan     col_filter_select_rows() alone, without materializing.  This
 *            isolates the kernel; the gap between `scan` and `tiled` is the
 *            materialize cost that no amount of SIMD in the scan can remove.
 *
 * Whether `tiled`/`adapt` use the AVX2 or the scalar body of the kernel is decided
 * at compile time by __AVX2__, so the comparison is made by building this
 * file twice.  meson registers:
 *
 *   bench_filter_scan          default flags  -> AVX2 kernel
 *   bench_filter_scan_scalar   -mno-avx2      -> scalar kernel
 *
 * Two binaries rather than a runtime switch: a runtime flag would put a
 * global read inside the scan loop and change the very thing being measured.
 *
 * Every configuration verifies its output against the fused variant and
 * prints mismatch counts.  A throughput number without `mismatch=0` beside
 * it is not evidence of anything.
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

/* Selectivity drives the left-pack path hard at the middle values and
 * degenerates to all-skip / all-copy at the ends. */
static const int SELECTIVITY_PCT[] = { 1, 25, 50, 99 };
#define SEL_COUNT (sizeof(SELECTIVITY_PCT) / sizeof(SELECTIVITY_PCT[0]))

static const uint32_t NCOLS_LIST[] = { 2, 8 };
#define NCOLS_COUNT (sizeof(NCOLS_LIST) / sizeof(NCOLS_LIST[0]))

/*
 * The pre-kernel loop from col_op_filter(), reproduced exactly: scan the
 * key column and materialize passing rows in the same pass.
 */
static BENCH_NOINLINE uint32_t
filter_fused(int64_t *const *columns, uint32_t col_a_idx, int64_t const_b,
    uint32_t nrows, uint32_t ncols, int64_t *tmp)
{
    const int64_t *col_a = columns[col_a_idx];
    uint32_t nout = 0;
    for (uint32_t r = 0; r < nrows; r++) {
        if (col_a[r] < const_b) {
            for (uint32_t c = 0; c < ncols; c++)
                tmp[(size_t)nout * ncols + c] = columns[c][r];
            nout++;
        }
    }
    return nout;
}

/*
 * The tiled form col_op_filter() uses: scan a tile, materialize the rows it
 * selected, move on.
 */
static BENCH_NOINLINE uint32_t
filter_tiled(int64_t *const *columns, uint32_t col_a_idx, int64_t const_b,
    uint32_t nrows, uint32_t ncols, uint32_t *sel, int64_t *tmp)
{
    const int64_t *col_a = columns[col_a_idx];
    uint32_t nout = 0;
    for (uint32_t base = 0; base < nrows;) {
        uint32_t chunk = nrows - base;
        if (chunk > COL_FILTER_TILE)
            chunk = COL_FILTER_TILE;

        uint32_t nsel = col_filter_select_rows(col_a + base, NULL, const_b,
                chunk, WL_PLAN_EXPR_CMP_LT, sel);

        for (uint32_t i = 0; i < nsel; i++) {
            uint32_t r = base + sel[i];
            for (uint32_t c = 0; c < ncols; c++)
                tmp[(size_t)nout * ncols + c] = columns[c][r];
            nout++;
        }
        base += chunk;
    }
    return nout;
}

/*
 * What col_op_filter() ships: tiled selection, except that a first tile which
 * keeps nearly everything switches the whole scan to the fused loop.  Mirrors
 * the threshold in ops.c.
 */
static BENCH_NOINLINE uint32_t
filter_adaptive(int64_t *const *columns, uint32_t col_a_idx, int64_t const_b,
    uint32_t nrows, uint32_t ncols, uint32_t *sel, int64_t *tmp)
{
#ifndef __AVX2__
    /* Mirrors ops.c: with no vectorized scan the selection vector is never
     * built at all, so not even one tile is scanned. */
    (void)sel;
    return filter_fused(columns, col_a_idx, const_b, nrows, ncols, tmp);
#else
    const int64_t *col_a = columns[col_a_idx];
    uint32_t nout = 0;

    for (uint32_t base = 0; base < nrows;) {
        uint32_t chunk = nrows - base;
        if (chunk > COL_FILTER_TILE)
            chunk = COL_FILTER_TILE;

        uint32_t nsel = col_filter_select_rows(col_a + base, NULL, const_b,
                chunk, WL_PLAN_EXPR_CMP_LT, sel);

        if (base == 0 && nsel > chunk - (chunk / 8))
            return filter_fused(columns, col_a_idx, const_b, nrows, ncols,
                       tmp);

        for (uint32_t i = 0; i < nsel; i++) {
            uint32_t r = base + sel[i];
            for (uint32_t c = 0; c < ncols; c++)
                tmp[(size_t)nout * ncols + c] = columns[c][r];
            nout++;
        }
        base += chunk;
    }
    return nout;
#endif
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
    printf("bench_filter_scan (kernel=%s, warmup=%d repeats=%d)\n", variant,
        WARMUP, REPEATS);
    printf("%-8s %-5s %-5s %9s %9s %9s %9s %8s %8s\n", "nrows", "ncol",
        "sel%", "fused_ms", "tiled_ms", "adapt_ms", "scan_ms", "adapt_x",
        "mismatch");

    for (uint32_t ni = 0; ni < NROWS_COUNT; ni++) {
        const uint32_t nrows = NROWS_LIST[ni];
        for (uint32_t ci = 0; ci < NCOLS_COUNT; ci++) {
            const uint32_t ncols = NCOLS_LIST[ci];
            for (uint32_t si = 0; si < SEL_COUNT; si++) {
                const int sel_pct = SELECTIVITY_PCT[si];

                /* Column-major storage, one contiguous array per column. */
                int64_t **columns = (int64_t **)malloc(
                    ncols * sizeof(int64_t *));
                if (!columns)
                    return 1;
                for (uint32_t c = 0; c < ncols; c++) {
                    columns[c] = (int64_t *)malloc(
                        (size_t)nrows * sizeof(int64_t));
                    if (!columns[c])
                        return 1;
                }

                /*
                 * Key column holds values 0..99 in a fixed pseudo-random
                 * order; the predicate is `< sel_pct`, so selectivity is
                 * sel_pct% and the passing rows are scattered rather than
                 * clustered.
                 */
                uint64_t s = 0x243F6A8885A308D3ull;
                for (uint32_t r = 0; r < nrows; r++) {
                    s = s * 6364136223846793005ull + 1442695040888963407ull;
                    columns[0][r] = (int64_t)((s >> 33) % 100);
                    for (uint32_t c = 1; c < ncols; c++)
                        columns[c][r] = (int64_t)r + (int64_t)c;
                }

                int64_t *tmp_a = (int64_t *)malloc(
                    (size_t)nrows * ncols * sizeof(int64_t));
                int64_t *tmp_b = (int64_t *)malloc(
                    (size_t)nrows * ncols * sizeof(int64_t));
                int64_t *tmp_c = (int64_t *)malloc(
                    (size_t)nrows * ncols * sizeof(int64_t));
                uint32_t *sel = (uint32_t *)malloc(
                    ((size_t)nrows + COL_FILTER_SEL_SLACK)
                    * sizeof(uint32_t));
                if (!tmp_a || !tmp_b || !tmp_c || !sel)
                    return 1;

                double fused_ms[REPEATS], adapt_ms[REPEATS];
                double tiled_ms[REPEATS], scan_ms[REPEATS];
                uint32_t n_fused = 0, n_adapt = 0, n_tiled = 0;
                uint64_t mismatch = 0;

                for (int rep = 0; rep < WARMUP + REPEATS; rep++) {
                    bench_time_t t0 = bench_time_now();
                    n_fused = filter_fused(columns, 0, sel_pct, nrows, ncols,
                            tmp_a);
                    bench_time_t t1 = bench_time_now();

                    bench_time_t t2 = bench_time_now();
                    (void)col_filter_select_rows(columns[0], NULL, sel_pct,
                        nrows, WL_PLAN_EXPR_CMP_LT, sel);
                    bench_time_t t3 = bench_time_now();

                    bench_time_t t7 = bench_time_now();
                    n_tiled = filter_tiled(columns, 0, sel_pct, nrows, ncols,
                            sel, tmp_b);
                    bench_time_t t8 = bench_time_now();

                    bench_time_t t5 = bench_time_now();
                    n_adapt = filter_adaptive(columns, 0, sel_pct, nrows,
                            ncols, sel, tmp_c);
                    bench_time_t t6 = bench_time_now();

                    if (rep >= WARMUP) {
                        int i = rep - WARMUP;
                        fused_ms[i] = bench_time_diff_ms(t0, t1);
                        tiled_ms[i] = bench_time_diff_ms(t7, t8);
                        adapt_ms[i] = bench_time_diff_ms(t5, t6);
                        scan_ms[i] = bench_time_diff_ms(t2, t3);
                    }
                }

                /* Correctness: all three variants must produce the same rows. */
                if (n_fused != n_adapt || n_fused != n_tiled) {
                    mismatch = 1;
                } else {
                    size_t nvals = (size_t)n_fused * ncols;
                    for (size_t i = 0; i < nvals; i++) {
                        if (tmp_a[i] != tmp_b[i] || tmp_a[i] != tmp_c[i])
                            mismatch++;
                    }
                }

                double f = median(fused_ms, REPEATS);
                double ti = median(tiled_ms, REPEATS);
                double ad = median(adapt_ms, REPEATS);
                double sc = median(scan_ms, REPEATS);

                printf("%-8u %-5u %-5d %9.3f %9.3f %9.3f %9.3f %7.2fx"
                    " %8" PRIu64 "\n", nrows, ncols, sel_pct, f, ti, ad, sc,
                    ad > 0.0 ? f / ad : 0.0, mismatch);
                fflush(stdout);

                free(sel);
                free(tmp_c);
                free(tmp_b);
                free(tmp_a);
                for (uint32_t c = 0; c < ncols; c++)
                    free(columns[c]);
                free(columns);
            }
        }
    }
    return 0;
}
