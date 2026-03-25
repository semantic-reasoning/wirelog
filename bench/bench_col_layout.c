/*
 * bench_col_layout.c - Column-major migration performance baseline
 *
 * Copyright (C) CleverPlant
 * Licensed under LGPL-3.0
 * For commercial licenses, contact: inquiry@cleverplant.com
 *
 * Microbenchmarks measuring operations affected by row-major vs column-major
 * storage layout.  Establishes baselines before #325 migration begins.
 *
 * Benchmarks:
 *   1. col_scan       - Strided column scan vs contiguous array scan
 *   2. row_reconstruct - Gather columns into contiguous row buffer
 *   3. partition      - col_rel_partition_by_key throughput
 *   4. radix_sort     - col_rel_radix_sort_int64 throughput
 *   5. append_row     - col_rel_append_row throughput (write path)
 *
 * Issue #327 (sub-issue of #325)
 */

#include "../wirelog/columnar/internal.h"

#include "bench_util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ======================================================================== */
/* Configuration                                                            */
/* ======================================================================== */

#define WARMUP  3
#define REPEATS 7

static const uint32_t NROWS_LIST[] = { 20000, 100000, 500000 };
static const uint32_t NCOLS_LIST[] = { 2, 4, 8 };

#define NROWS_COUNT (sizeof(NROWS_LIST) / sizeof(NROWS_LIST[0]))
#define NCOLS_COUNT (sizeof(NCOLS_LIST) / sizeof(NCOLS_LIST[0]))

/* ======================================================================== */
/* Helpers                                                                  */
/* ======================================================================== */

static col_rel_t *
make_rel_random(uint32_t nrows, uint32_t ncols, uint32_t seed)
{
    col_rel_t *r = col_rel_new_auto("bench", ncols);
    if (!r)
        return NULL;

    srand(seed);
    int64_t *row = (int64_t *)malloc(ncols * sizeof(int64_t));
    for (uint32_t i = 0; i < nrows; i++) {
        for (uint32_t c = 0; c < ncols; c++)
            row[c] = (int64_t)rand();
        col_rel_append_row(r, row);
    }
    free(row);
    return r;
}

static double
median_of(double *vals, int n)
{
    qsort(vals, (size_t)n, sizeof(double), bench_cmp_double);
    return vals[n / 2];
}

/* Prevent compiler from optimizing away reads */
static volatile int64_t g_sink;

/* ======================================================================== */
/* Benchmark 1: Column Scan (strided vs contiguous)                         */
/* ======================================================================== */

static double
bench_col_scan_strided(col_rel_t *r, uint32_t key_col)
{
    bench_time_t t0 = bench_time_now();
    int64_t sum = 0;
    for (uint32_t i = 0; i < r->nrows; i++)
        sum += r->data[(size_t)i * r->ncols + key_col];
    g_sink = sum;
    bench_time_t t1 = bench_time_now();
    return bench_time_diff_ms(t0, t1);
}

static double
bench_col_scan_contiguous(const int64_t *col_array, uint32_t nrows)
{
    bench_time_t t0 = bench_time_now();
    int64_t sum = 0;
    for (uint32_t i = 0; i < nrows; i++)
        sum += col_array[i];
    g_sink = sum;
    bench_time_t t1 = bench_time_now();
    return bench_time_diff_ms(t0, t1);
}

/* ======================================================================== */
/* Benchmark 2: Row Reconstruction (column-major cost simulation)           */
/* ======================================================================== */

static double
bench_row_reconstruct(const int64_t **col_arrays, uint32_t nrows,
    uint32_t ncols)
{
    int64_t *row_buf = (int64_t *)malloc(ncols * sizeof(int64_t));
    bench_time_t t0 = bench_time_now();
    int64_t sum = 0;
    for (uint32_t i = 0; i < nrows; i++) {
        for (uint32_t c = 0; c < ncols; c++)
            row_buf[c] = col_arrays[c][i];
        sum += row_buf[0];
    }
    g_sink = sum;
    bench_time_t t1 = bench_time_now();
    free(row_buf);
    return bench_time_diff_ms(t0, t1);
}

/* ======================================================================== */
/* Benchmark 3: Partition-by-key                                            */
/* ======================================================================== */

static double
bench_partition(col_rel_t *r)
{
    uint32_t key_cols[] = { 0 };
    col_rel_t *parts[4] = { NULL };

    bench_time_t t0 = bench_time_now();
    col_rel_partition_by_key(r, key_cols, 1, 4, parts);
    bench_time_t t1 = bench_time_now();

    for (uint32_t w = 0; w < 4; w++) {
        if (parts[w])
            col_rel_destroy(parts[w]);
    }
    return bench_time_diff_ms(t0, t1);
}

/* ======================================================================== */
/* Benchmark 4: Radix Sort                                                  */
/* ======================================================================== */

static double
bench_radix_sort(uint32_t nrows, uint32_t ncols)
{
    /* Fresh copy each time since sort is in-place */
    col_rel_t *r = make_rel_random(nrows, ncols, 42);
    if (!r)
        return -1.0;

    bench_time_t t0 = bench_time_now();
    col_rel_radix_sort_int64(r);
    bench_time_t t1 = bench_time_now();

    col_rel_destroy(r);
    return bench_time_diff_ms(t0, t1);
}

/* ======================================================================== */
/* Benchmark 5: Append Row (write path throughput)                          */
/* ======================================================================== */

static double
bench_append_row(uint32_t nrows, uint32_t ncols)
{
    col_rel_t *r = col_rel_new_auto("bench_append", ncols);
    if (!r)
        return -1.0;

    int64_t *row = (int64_t *)malloc(ncols * sizeof(int64_t));
    for (uint32_t c = 0; c < ncols; c++)
        row[c] = (int64_t)c;

    bench_time_t t0 = bench_time_now();
    for (uint32_t i = 0; i < nrows; i++)
        col_rel_append_row(r, row);
    bench_time_t t1 = bench_time_now();

    free(row);
    col_rel_destroy(r);
    return bench_time_diff_ms(t0, t1);
}

/* ======================================================================== */
/* Runner                                                                   */
/* ======================================================================== */

static void
run_benchmark(const char *name, uint32_t nrows, uint32_t ncols,
    double (*fn)(void *ctx), void *ctx)
{
    double times[WARMUP + REPEATS];

    for (int i = 0; i < WARMUP + REPEATS; i++)
        times[i] = fn(ctx);

    double med = median_of(times + WARMUP, REPEATS);
    double min_t = times[WARMUP];
    double max_t = times[WARMUP];
    for (int i = WARMUP + 1; i < WARMUP + REPEATS; i++) {
        if (times[i] < min_t)
            min_t = times[i];
        if (times[i] > max_t)
            max_t = times[i];
    }

    printf("%s\t%u\t%u\t%.3f\t%.3f\t%.3f\n",
        name, nrows, ncols, min_t, med, max_t);
}

/* Wrapper contexts for uniform benchmark interface */
typedef struct {
    col_rel_t *rel;
    uint32_t key_col;
} scan_ctx_t;

typedef struct {
    const int64_t **col_arrays;
    uint32_t nrows;
    uint32_t ncols;
} reconstruct_ctx_t;

typedef struct {
    uint32_t nrows;
    uint32_t ncols;
} size_ctx_t;

static double
fn_scan_strided(void *ctx)
{
    scan_ctx_t *s = (scan_ctx_t *)ctx;
    return bench_col_scan_strided(s->rel, s->key_col);
}

static double
fn_scan_contiguous(void *ctx)
{
    scan_ctx_t *s = (scan_ctx_t *)ctx;
    /* Extract column 0 into contiguous array */
    int64_t *col = (int64_t *)malloc(s->rel->nrows * sizeof(int64_t));
    for (uint32_t i = 0; i < s->rel->nrows; i++)
        col[i] = s->rel->data[(size_t)i * s->rel->ncols + s->key_col];
    double t = bench_col_scan_contiguous(col, s->rel->nrows);
    free(col);
    return t;
}

static double
fn_row_reconstruct(void *ctx)
{
    reconstruct_ctx_t *r = (reconstruct_ctx_t *)ctx;
    return bench_row_reconstruct(r->col_arrays, r->nrows, r->ncols);
}

static double
fn_partition(void *ctx)
{
    return bench_partition((col_rel_t *)ctx);
}

static double
fn_radix_sort(void *ctx)
{
    size_ctx_t *s = (size_ctx_t *)ctx;
    return bench_radix_sort(s->nrows, s->ncols);
}

static double
fn_append_row(void *ctx)
{
    size_ctx_t *s = (size_ctx_t *)ctx;
    return bench_append_row(s->nrows, s->ncols);
}

/* ======================================================================== */
/* Main                                                                     */
/* ======================================================================== */

int
main(void)
{
    printf(
        "# bench_col_layout - Column-major migration baseline (Issue #327)\n");
    printf("# warmup=%d repeats=%d\n", WARMUP, REPEATS);
    printf("benchmark\tnrows\tncols\tmin_ms\tmedian_ms\tmax_ms\n");

    for (size_t ni = 0; ni < NROWS_COUNT; ni++) {
        uint32_t nrows = NROWS_LIST[ni];

        for (size_t ci = 0; ci < NCOLS_COUNT; ci++) {
            uint32_t ncols = NCOLS_LIST[ci];

            /* Build test relation */
            col_rel_t *r = make_rel_random(nrows, ncols, 42);
            if (!r)
                continue;

            /* 1. Column scan (strided - current row-major cost) */
            scan_ctx_t scan_ctx = { .rel = r, .key_col = 0 };
            run_benchmark("col_scan_strided", nrows, ncols,
                fn_scan_strided, &scan_ctx);

            /* 2. Column scan (contiguous - column-major benefit) */
            run_benchmark("col_scan_contiguous", nrows, ncols,
                fn_scan_contiguous, &scan_ctx);

            /* 3. Row reconstruction (column-major cost) */
            int64_t **col_arrays
                = (int64_t **)malloc(ncols * sizeof(int64_t *));
            for (uint32_t c = 0; c < ncols; c++) {
                col_arrays[c]
                    = (int64_t *)malloc(nrows * sizeof(int64_t));
                for (uint32_t i = 0; i < nrows; i++)
                    col_arrays[c][i]
                        = r->data[(size_t)i * ncols + c];
            }
            reconstruct_ctx_t recon_ctx = {
                .col_arrays = (const int64_t **)col_arrays,
                .nrows = nrows,
                .ncols = ncols,
            };
            run_benchmark("row_reconstruct", nrows, ncols,
                fn_row_reconstruct, &recon_ctx);

            for (uint32_t c = 0; c < ncols; c++)
                free(col_arrays[c]);
            free(col_arrays);

            /* 4. Partition-by-key (W=4) */
            run_benchmark("partition_w4", nrows, ncols,
                fn_partition, r);

            /* 5. Radix sort */
            size_ctx_t size_ctx = { .nrows = nrows, .ncols = ncols };
            run_benchmark("radix_sort", nrows, ncols,
                fn_radix_sort, &size_ctx);

            /* 6. Append row */
            run_benchmark("append_row", nrows, ncols,
                fn_append_row, &size_ctx);

            col_rel_destroy(r);
        }
    }

    printf("# peak_rss_kb=%ld\n", bench_peak_rss_kb());
    return 0;
}
